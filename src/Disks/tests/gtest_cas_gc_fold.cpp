#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/ProfileEvents.h>
#include "cas_test_helpers.h"

#include <algorithm>
#include <array>
#include <map>
#include <mutex>
#include <set>

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace ProfileEvents
{
    extern const Event CASGCCondemnMarkerUnconfirmedCarry;
    extern const Event CASGCReadAheadHit;
    extern const Event CASGCReadAheadMiss;
    extern const Event CASGCReadAheadWasted;
    extern const Event CASRefManifestBodyFoldGets;
    extern const Event CASRefManifestBodyMemoHits;
}

namespace
{
const UInt128 kGc = hexToU128("00000000000000000000000000000001");
ManifestRef ref(const String &, uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}

std::optional<DB::Cas::Object> readOf(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

bool headExists(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).head(key, Retry::standard()).has_value();
}
}

/// Committed new_manifest => +1 per blob entry (BlobInDegreeMatchesActiveManifests).
/// After a fold, gc/state records snap_attempt == the folding leader's lease.seq, and the fold seal
/// lives under (snap_generation, snap_attempt).
TEST(CASGCFold, FoldAdoptsAttemptEqualsLeaseSeq)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();

    const auto st = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);
    EXPECT_EQ(st.snap_attempt, st.lease.seq);
    EXPECT_GT(st.snap_generation, 0u);
    /// The one-pass round's fold seal is durable under (snap_generation, snap_attempt) — the adopted
    /// attempt locates it (a seal under any other attempt would be unadopted debris).
    EXPECT_TRUE(headExists(*backend, store->layout().foldSealKey(st.snap_generation, st.snap_attempt)));
}

TEST(CASGCFold, CommittedAddEmitsPlusOnePerBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r,
        {blobEntryFor("a", DB::UInt128(1)), blobEntryFor("b", DB::UInt128(2))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();

    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(2)), 1);
}

/// Owner removal => -1 per blob entry; in-degree returns to 0.
TEST(CASGCFold, RemovalEmitsMinusOne)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
}

/// Precommit with a PRESENT, valid body => +1.
TEST(CASGCFold, PrecommitBodyPresentEmitsPlusOne)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    addPrecommitTransition(*backend, store->layout(), ns, DB::UInt128(7), "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
}

/// Precommit whose body is ABSENT => NO delta (control #4); the 404 must NOT throw.
TEST(CASGCFold, PrecommitMissingBodyEmitsNoDelta)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    addPrecommitTransition(*backend, store->layout(), ns, DB::UInt128(7), "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    EXPECT_NO_THROW(gc.runRegularRound());
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
}

/// FOLD BARRIER (control #23): a LIVE precommit binding whose body is missing does NOT advance the
/// durable fold cursor past its activation event; when the body appears the cursor advances.
TEST(CASGCFold, FoldBarrierHaltsCursorAtLiveMissingBodyPrecommit)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const uint64_t v = addPrecommitTransition(*backend, store->layout(), ns, DB::UInt128(7), "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    EXPECT_NO_THROW(gc.runRegularRound());
    EXPECT_LT(foldCursorOf(*backend, store->layout(), ns, 0), v);   // barrier: halted at the activation

    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_GE(foldCursorOf(*backend, store->layout(), ns, 0), v);   // barrier lifted by activation
}

/// Promote of an already-activated precommit is a PURE OWNER MOVE: NO delta, body not condemned.
TEST(CASGCFold, PromoteOfActivatedPrecommitEmitsNoDelta)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    addPrecommitTransition(*backend, store->layout(), ns, DB::UInt128(7), "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    promoteTransition(*backend, store->layout(), ns, DB::UInt128(7), "tbl", r);
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);   // unchanged, still pinned
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})));   // not condemned
}

/// Committed add naming a MISSING body (404) => clamp + anomaly, never a guessed +1, never a throw.
TEST(CASGCFold, CommittedMissingBodyClampsCursorAndRecordsAnomaly)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const uint64_t v = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);  // no body
    Gc gc(store, kGc);
    RoundReport report;
    EXPECT_NO_THROW(report = gc.runRegularRound());
    EXPECT_TRUE(report.hasAnomaly(ns, /*shard*/0));
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
    EXPECT_LT(foldCursorOf(*backend, store->layout(), ns, 0), v);
}

/// A body whose self-ref disagrees (PRESENT but INVALID) => hard fail closed (controls #19/#20).
TEST(CASGCFold, RefMismatchFailsClosed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    PartManifest bad;
    bad.ref = ref("srv-a:1", 1, 0xBB);   // != r
    bad.root_namespace_id = ns;
    bad.entries = {blobEntryFor("a", DB::UInt128(1))};
    bad.payload_digest = computePayloadDigest(bad);
    {
        OperationForTest op(*backend);
        (*op).create(store->layout().manifestKey(ManifestId{ns, r}), encodePartManifest(bad), Retry::standard());
    }
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]{ gc.runRegularRound(); });
}

/// Owner-removal whose OLD committed body is gone at removal-fold => clamp + anomaly, no partial -1.
TEST(CASGCFold, RemovalWithMissingOldBodyClampsAndRecordsAnomaly)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    gc.runRegularRound();   // +1; blob 1 in-degree 1

    const uint64_t removal_version = dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    deleteManifestBody(*backend, store->layout(), ManifestId{ns, r});   // body gone before its decrement

    RoundReport report;
    EXPECT_NO_THROW(report = gc.runRegularRound());
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);   // unchanged: no silent -1
    EXPECT_TRUE(report.hasAnomaly(ns, /*shard*/0));
    EXPECT_LT(foldCursorOf(*backend, store->layout(), ns, 0), removal_version);
}

/// (The two `CASGCFold.IncarnationMismatchRestartsFoldAtZero*` tests were removed with the snapshot+log
/// ref model: they injected a stale per-shard fold cursor beyond the live mutable shard's version and
/// asserted the fold RESET the cursor to 0 on an incarnation mismatch. There is no mutable per-shard
/// cursor to stale-reset anymore -- the durable cursor is a strictly-increasing `RefTxnId`, and a
/// recreated namespace uses a GREATER `writer_epoch`, so the ABA hazard is impossible by construction.
/// The ref-model equivalent -- `remove_namespace` then a later `namespace_birth` with a greater id folds
/// normally -- is covered by `gtest_cas_gc_shard_incarnation.cpp` and `gtest_cas_ref_gc.cpp`.)

/// T0 (2026-07-02 snapshot-streaming): an idle round — no journal changes, no retired entries — touches
/// ZERO run objects. After one populated round, reset the counters and run a no-op round; the fold must
/// carry the parent generation's `RunRef` verbatim into the new fold_seal (same key, same checksum, same
/// generation) and NOT read or write any `.../blob_target/...` object.
TEST(CASGCFold, EmptyDeltaShardCarriesParentRunRef)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    /// gc_fold_max_defer_rounds=0 forces fold-every-round: this test exercises the pure-ref-carry FOLD
    /// path on an idle round; without it the round would DEFER (re-adopt the sealed generation) and never
    /// mint the carried generation this test inspects.
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();   // round 1: folds the +1, seals the gen-1 blob_target run

    const auto st1 = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);
    const auto parent_seal = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st1.snap_generation, st1.snap_attempt))->bytes);
    ASSERT_EQ(parent_seal.blob_target_runs.size(), 1u);
    const RunRef parent_ref = parent_seal.blob_target_runs.front();

    backend->resetCounts();
    gc.runRegularRound();   // round 2: no changes => pure ref-carry, zero run I/O

    EXPECT_EQ(backend->ioCountForKeysContaining("/blob_target/"), 0u)
        << "idle round must not GET/getStream/PUT any blob_target run object";

    const auto st2 = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);
    EXPECT_GT(st2.snap_generation, st1.snap_generation);
    const auto new_seal = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st2.snap_generation, st2.snap_attempt))->bytes);
    ASSERT_EQ(new_seal.blob_target_runs.size(), 1u);
    const RunRef carried = new_seal.blob_target_runs.front();
    EXPECT_EQ(carried.key, parent_ref.key) << "carried ref points at the PARENT generation's run key";
    EXPECT_EQ(carried.checksum, parent_ref.checksum);
    EXPECT_EQ(carried.shard, 0u);
    EXPECT_EQ(carried.key_generation, st1.snap_generation)
        << "the carried ref names the generation whose key namespace physically holds the object";
}

/// The round AFTER a ref-carry, with a real delta, folds THROUGH the carried ref: the new generation's
/// run is produced from the OLD-generation run (resolved via the carried ref, not by key construction)
/// merged with the delta, and the resulting in-degree is correct.
TEST(CASGCFold, FoldResolvesThroughCarriedRef)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref("srv-a:1", 1, 0xAA);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    gc.runRegularRound();   // gen 1: blob 1 in-degree 1
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    gc.runRegularRound();   // gen 2: no delta => carries the gen-1 ref
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1)
        << "in-degree resolves through the carried parent ref";

    // A real delta on the NEXT round must fold through the carried ref and drop blob 1 to zero.
    const ManifestRef r2 = ref("srv-a:2", 2, 0xBB);
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", r1, r2);

    gc.runRegularRound();   // gen 3: -1 on blob 1 (old owner dropped), +1 on blob 2
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0)
        << "fold through the carried ref applied the -1 correctly";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(2)), 1);
}

/// previewDeletes resolves runs through the current seal's refs, not by key construction. After a
/// pure ref-carry round the current seal's `blob_target_runs` point at an OLDER generation's key; the
/// preview must open that physical object via the ref and report the correct in-degree — here blob 1 is
/// still referenced, so its carried-ref-resolved in-degree is 1 and it is NOT surfaced as a candidate.
/// (A carried ref that the preview failed to resolve would mis-open the run and either throw or spuriously
/// surface the still-referenced blob.)
TEST(CASGCFold, PreviewResolvesCarriedRef)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// gc_fold_max_defer_rounds=0 forces the idle second round to FOLD (pure ref-carry) rather than
    /// DEFER, so the current seal's `blob_target_runs` point at the parent generation's key (the carried
    /// ref this test resolves through).
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();   // gen 1: blob referenced, in-degree 1
    const auto st1 = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);

    gc.runRegularRound();   // gen 2: no delta, no retired => pure ref-carry (ref points back at gen 1)
    const auto st2 = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);
    ASSERT_GT(st2.snap_generation, st1.snap_generation);
    const auto seal2 = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st2.snap_generation, st2.snap_attempt))->bytes);
    ASSERT_EQ(seal2.blob_target_runs.size(), 1u);
    ASSERT_EQ(seal2.blob_target_runs.front().key_generation, st1.snap_generation)
        << "the current seal's ref physically lives at the parent generation (carried, not reconstructed)";

    // The preview resolves the carried ref (a gen-1 physical key) and computes in-degree 1 => blob 1 is
    // not a delete candidate. Resolution-by-ref is the property under test.
    const auto preview = gc.previewDeletes();
    for (const auto & e : preview)
        EXPECT_NE(e.ref, (DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(blob)})) << "still-referenced blob must not be surfaced (carried ref resolved to in-degree 1)";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), blob), 1)
        << "in-degree through the carried parent ref is 1";
}

/// Per-consumer whole-file seal-checksum RED tests (codecs-v3 phase 5, Task 6) at the seal-driven
/// consumers. Setup: fold one referenced blob into a sealed generation, then corrupt the persisted
/// seal's blob_target_runs[0].checksum (the stored run bytes stay valid), so the abort comes from the
/// seal-checksum verify, not a row invariant.
namespace
{
String corruptSealedRunChecksum(InMemoryBackend & backend, const Layout & layout, const GcState & st)
{
    const String sk = layout.foldSealKey(st.snap_generation, st.snap_attempt);
    const auto existing = readOf(backend, sk);
    auto seal = decodeFoldSeal(existing->bytes);
    if (seal.blob_target_runs.empty())
        return {};
    const String run_key = seal.blob_target_runs.front().key;
    seal.blob_target_runs.front().checksum = seal.blob_target_runs.front().checksum + 1;
    OperationForTest op(backend);
    (*op).replace(sk, encodeFoldSeal(seal), existing->etag, Retry::standard());
    return run_key;
}
}

TEST(CASGCFold, PreviewDeletesSealChecksumMismatchFailsClosed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();   // seals gen-1 with one blob_target run
    const auto st = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);
    ASSERT_FALSE(corruptSealedRunChecksum(*backend, store->layout(), st).empty());

    // A deletion preview must never be derived from an unverified run: fail closed.
    Gc gc2(store, kGc);   // fresh read of the corrupted seal
    EXPECT_THROW(gc2.previewDeletes(), DB::Exception);
}

TEST(CASGCFold, FsckSealChecksumMismatchCataloguedAndAuditCompletes)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    replaceRecoverableCkptForRawFixture(
        *backend, store->layout(), ns,
        RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 1},
                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});

    Gc gc(store, kGc);
    gc.runRegularRound();
    const auto st = decodeGcState(readOf(*backend, store->layout().gcStateKey())->bytes);

    /// A present-but-unreferenced blob (written AFTER the round so GC never touches it) is what makes
    /// fsck enter its GC-pipeline classification path (guarded by a non-empty unreferenced set), which
    /// is where it streams + seal-checksum-verifies the snapshot runs.
    writeBlobBody(*backend, store->layout(), DB::UInt128(2));

    const String bad_run_key = corruptSealedRunChecksum(*backend, store->layout(), st);
    ASSERT_FALSE(bad_run_key.empty());

    // fsck is a read-only auditor: it must CATALOGUE the corrupt run and COMPLETE, not abort the scan.
    FsckReport report;
    EXPECT_NO_THROW(report = runFsck(*store, /*detail*/ true));
    EXPECT_GE(report.corrupted_runs, 1u);
    bool catalogued = false;
    for (const auto & o : report.objects)
        if (o.cls == FsckClass::CorruptedRun && o.key == bad_run_key)
            catalogued = true;
    EXPECT_TRUE(catalogued) << "the corrupt run must be catalogued with its key";
}

/// A mid-log clamp must be RECOVERABLE (spec §Step 3 transaction atomicity). A single log carrying two
/// ops -- [drop committed A (a `-1` whose body is present at removal-fold), add precommit B (whose body is
/// transiently absent)] -- clamps on B. The `-1` on A must NOT be merged into the round's owner-removed
/// cleanup, because the post-CAS body delete would then reclaim A's body while A's edge stays unfolded
/// behind the clamp; the next re-fold of that same log would then find A's body missing and clamp forever
/// (a permanent pool-wide destructive freeze). With per-log staging, A's body survives the clamp round and
/// the log folds cleanly once B's body reappears.
TEST(CASGCFold, MidLogClampPreservesEarlierRemovalBodyAndRecovers)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef a = ref("srv-a:1", 1, 0xAA);
    const ManifestRef b = ref("srv-a:2", 2, 0xBB);

    /// Round 0: commit A (references blob 1). A's body is present and folds a +1.
    writeManifestRaw(*backend, store->layout(), ns, a, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "r1", std::nullopt, a);
    Gc gc(store, kGc);
    gc.runRegularRound();
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    /// ONE log with two ops: drop committed A (`-1`, body present), then add precommit B (`+1`, body
    /// staged then removed => a transient 404 clamps the log after A's `-1` already folded).
    writeManifestRaw(*backend, store->layout(), ns, b, {blobEntryFor("b", DB::UInt128(2))});
    deleteManifestBody(*backend, store->layout(), ManifestId{ns, b});   // B's body absent => clamp
    const uint64_t log_seq = appendRefLogSeed(*backend, store->layout(), ns,
        {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r1", a}, std::nullopt),
         ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r2", b})});
    advanceRecoverableCkptForRawFixture(*backend, store->layout(), ns, RefTxnId{1, log_seq});

    const RoundReport clamp_report = gc.runRegularRound();
    EXPECT_TRUE(clamp_report.hasAnomaly(ns, /*shard*/0)) << "the missing B body must clamp this log";
    EXPECT_LT(foldCursorOf(*backend, store->layout(), ns, 0), log_seq) << "the clamp halts the cursor below the log";
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, a})))
        << "A's body must survive the clamp round: its `-1` was staged, not merged, so no post-CAS delete "
           "reclaimed it -- otherwise the re-fold would clamp on A's missing body forever";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1) << "A's `-1` was not adopted (clamp)";

    /// The transient 404 heals: B's body reappears. The next round re-folds the SAME log cleanly.
    writeManifestRaw(*backend, store->layout(), ns, b, {blobEntryFor("b", DB::UInt128(2))});
    const RoundReport clean_report = gc.runRegularRound();
    EXPECT_FALSE(clean_report.hasAnomaly(ns, /*shard*/0)) << "with both bodies present the log folds; no clamp";
    EXPECT_GE(foldCursorOf(*backend, store->layout(), ns, 0), log_seq) << "the cursor advanced past the recovered log";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0) << "A's `-1` applied";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(2)), 1) << "B's `+1` applied";
}

/// A `+1` precommit whose body is PERMANENTLY absent and whose build is below the durable watermark floor
/// (provably dead -- the exact fact the orphan sweep uses to reclaim the body) must be SKIPPED, not held on
/// the fold barrier forever. Without a terminal rule this table clamps every round with no resolution (a
/// late-predecessor precommit whose body was already reclaimed). The watermark is seeded so the precommit's
/// build is dead; the fold must advance the cursor past the log and record no clamp anomaly.
TEST(CASGCFold, DeadPrecommitWithMissingBodyIsSkippedNotClampedForever)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    /// The namespace's server-root prefix is "srv"; seed its watermark floor so build_sequence 5 is retired.
    const RootNamespace ns{"srv/tbl"};
    setWatermarkMinActive(*backend, store->layout(), "srv", /*writer_epoch*/1, /*min_active_build_sequence*/10);

    /// A precommit naming a build (writer_epoch 1, build_sequence 5) whose body is never written.
    const ManifestRef dead = ManifestRef{.writer_epoch = 1, .build_sequence = 5, .manifest_ordinal = 1};
    const uint64_t log_seq =
        addPrecommitTransition(*backend, store->layout(), ns, DB::UInt128(7), "r1", std::nullopt, dead);

    Gc gc(store, kGc);
    const RoundReport report = gc.runRegularRound();
    EXPECT_FALSE(report.hasAnomaly(ns, /*shard*/0))
        << "a provably-dead precommit's missing body is skipped, not clamped";
    EXPECT_GE(foldCursorOf(*backend, store->layout(), ns, 0), log_seq)
        << "the fold advanced past the log instead of holding the barrier forever";

    /// A second identical round stays clean (terminal resolution, not a recurring clamp).
    const RoundReport report2 = gc.runRegularRound();
    EXPECT_FALSE(report2.hasAnomaly(ns, /*shard*/0)) << "the resolution is terminal: no recurring clamp";
}

/// A10: a single clamp anomaly must suppress ALL destructive actions in the round — the merge-side
/// deletes AND the post-CAS ref/namespace cleanup — from ONE decision, not two independent recomputes
/// of !report.anomalies.empty() that a future edit could desync (over-delete class). This pins that a
/// clamped round reclaims nothing.
TEST(CASGCFold, SingleAnomalySuppressesEveryDestructiveActionInTheRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef a = ref("srv-a:1", 1, 0xAA);
    const ManifestRef b = ref("srv-a:2", 2, 0xBB);

    /// Round 0: commit A (references blob 1); its body folds a +1.
    writeManifestRaw(*backend, store->layout(), ns, a, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "r1", std::nullopt, a);
    Gc gc(store, kGc);
    gc.runRegularRound();
    ASSERT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    /// One log: drop committed A (`-1`, body present) then add precommit B whose body is absent -> the
    /// missing B body clamps the log AFTER A's `-1` folded.
    writeManifestRaw(*backend, store->layout(), ns, b, {blobEntryFor("b", DB::UInt128(2))});
    deleteManifestBody(*backend, store->layout(), ManifestId{ns, b});
    const uint64_t log_seq = appendRefLogSeed(*backend, store->layout(), ns,
        {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r1", a}, std::nullopt),
         ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r2", b})});
    advanceRecoverableCkptForRawFixture(*backend, store->layout(), ns, RefTxnId{1, log_seq});

    const RoundReport rep = gc.runRegularRound();
    ASSERT_TRUE(rep.hasAnomaly(ns, /*shard*/0)) << "the missing B body must clamp this round";
    /// The clamp suppresses the WHOLE destructive pipeline this round: no deletes, no redeletes, and
    /// A's `-1` stays unadopted (its body must survive, else the re-fold clamps on it forever).
    EXPECT_EQ(rep.deleted, 0u);
    EXPECT_EQ(rep.redeleted, 0u);
    EXPECT_EQ(rep.graduated, 0u);
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, a})));
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
}

/// A10 follow-up: the round-side destructive gates -- the perpetual dead-life janitor AND
/// `cleanupRefObjects`' covered ref-object deletion -- must ALSO honor the round's ONE
/// `suppress_destructive` decision, not just fold()'s merge-side reducers pinned above. A clamp anomaly in
/// one namespace must suppress destructive cleanup POOL-WIDE: dead-life physical debris must not be
/// swept, and an unrelated live
/// table's snapshot-covered ref-log must not be deleted, in the SAME clamped round. A clean round
/// afterward proves the setup really was cleanup-eligible, not vacuously untouched.
TEST(CASGCFold, RoundSideAnomalySuppressesRefLogCleanupWhileRemovalDebrisStaysJanitorWork)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout & layout = store->layout();
    Gc gc(store, kGc);

    /// Namespace 1: the clamp trigger (same construction as
    /// SingleAnomalySuppressesEveryDestructiveActionInTheRound above).
    const RootNamespace ns_clamp{"00/aa@cas@"};
    const ManifestRef a = ref("srv-a:1", 1, 0xAA);
    const ManifestRef b = ref("srv-a:2", 2, 0xBB);
    writeManifestRaw(*backend, layout, ns_clamp, a, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns_clamp, "r1", std::nullopt, a);
    runRegularRoundReclaiming(gc);   /// folds A cleanly; establishes the baseline before the clamp

    /// Namespace 2: a namespace mid-removal with physical manifest and verbatim-file debris. Generation
    /// 7 has no lifecycle-specific cleanup pass: terminal folding records evidence, while these bytes
    /// remain inert work for the perpetual janitor and orphan-manifest sweep.
    const RootNamespace ns_removed{"00/cc@cas@"};
    RefOp remove_op;
    remove_op.kind = RefOpKind::RemoveNamespace;
    const uint64_t removal_log_seq = appendRefLogSeed(*backend, layout, ns_removed, {remove_op});
    writeRecoverableCkptForRawFixture(
        *backend, layout, ns_removed,
        RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, removal_log_seq},
                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
    /// Keyed at the life the CATALOG names for this namespace (`appendRefLogSeed` admitted it above),
    /// which is the physical life that owns the eventual janitor work. Spelling the sentinel here instead
    /// would plant debris under the wrong life and make the retention assertion vacuous.
    const String debris_key
        = layout.namespaceFilesPrefix(CasRefCatalog::lifeIfCataloged(op, layout, ns_removed).value())
        + "leftover_verbatim_file";
    {
        OperationForTest debris_op(*backend);
        (*debris_op).create(debris_key, "debris", Retry::standard());
    }
    const ManifestRef removed_body = ref("srv-r:1", 1, 0xEE);
    writeManifestRaw(*backend, layout, ns_removed, removed_body, {blobEntryFor("r", DB::UInt128(9))});
    const String debris_manifest_key = layout.manifestKey(ManifestId{ns_removed, removed_body});

    /// Namespace 3: a live table with an exact checkpoint-named recovery triple -- exactly what a
    /// clamp-free round's `cleanupRefObjects` may clean below that base.
    const RootNamespace ns_covered{"00/dd@cas@"};
    const ManifestRef c1 = ref("srv-c:1", 1, 0xCC);
    const ManifestRef c2 = ref("srv-c:2", 2, 0xDD);
    writeManifestRaw(*backend, layout, ns_covered, c1, {blobEntryFor("c", DB::UInt128(3))});
    writeManifestRaw(*backend, layout, ns_covered, c2, {blobEntryFor("d", DB::UInt128(4))});
    const uint64_t cv1 = publishCommittedTransition(*backend, layout, ns_covered, "t1", std::nullopt, c1);
    const uint64_t cv2 = publishCommittedTransition(*backend, layout, ns_covered, "t2", std::nullopt, c2);
    writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns_covered.string(), RefTxnId{1, cv2},
        {committedRow("t1", c1), committedRow("t2", c2)}));
    replaceRecoverableCkptForRawFixture(*backend, layout, ns_covered, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, cv2},
        .checkpoint_snapshot_id = RefTxnId{1, cv2},
        .last_epoch_seal = std::nullopt,
    });
    const String covered_log_key = layout.refLogKey(fixture::fixtureLife(ns_covered), RefTxnId{1, cv1});
    ASSERT_TRUE(headExists(*backend, covered_log_key));

    /// Trigger the clamp in ns_clamp: drop committed A, add precommit B whose body is absent.
    writeManifestRaw(*backend, layout, ns_clamp, b, {blobEntryFor("b", DB::UInt128(2))});
    deleteManifestBody(*backend, layout, ManifestId{ns_clamp, b});
    const uint64_t clamp_log_seq = appendRefLogSeed(*backend, layout, ns_clamp,
        {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r1", a}, std::nullopt),
         ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r2", b})});
    advanceRecoverableCkptForRawFixture(*backend, layout, ns_clamp, RefTxnId{1, clamp_log_seq});

    const RoundReport rep = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(rep.hasAnomaly(ns_clamp, /*shard*/0)) << "the missing B body must clamp this round";
    EXPECT_EQ(rep.deleted, 0u);
    EXPECT_EQ(rep.redeleted, 0u);
    EXPECT_EQ(rep.graduated, 0u);

    /// Removal folding never performs lifecycle-specific physical cleanup, with or without a clamp.
    EXPECT_TRUE(headExists(*backend, debris_manifest_key))
        << "removed manifest debris remains ordinary orphan-sweep work";
    EXPECT_TRUE(headExists(*backend, debris_key))
        << "removed verbatim-file debris remains ordinary janitor work";

    /// `cleanupRefObjects` must not have deleted anything anywhere this round.
    EXPECT_TRUE(headExists(*backend, covered_log_key))
        << "a clamp anywhere in the round must suppress ref-log cleanup pool-wide, even for an unrelated live table";

    /// Heal the clamp and run a clean round. Ordinary ref-log cleanup resumes, while removal debris
    /// remains physically untouched by the lifecycle path.
    writeManifestRaw(*backend, layout, ns_clamp, b, {blobEntryFor("b", DB::UInt128(2))});
    const RoundReport clean_rep = runRegularRoundReclaiming(gc);
    EXPECT_FALSE(clean_rep.hasAnomaly(ns_clamp, /*shard*/0));
    EXPECT_TRUE(headExists(*backend, debris_manifest_key))
        << "a clamp-free fold still performs no lifecycle-specific manifest deletion";
    EXPECT_TRUE(headExists(*backend, debris_key))
        << "a clamp-free fold still performs no lifecycle-specific verbatim-file deletion";
    EXPECT_FALSE(headExists(*backend, covered_log_key)) << "a clamp-free round cleans the covered ref-log";
}

/// ---- persisted condemn-marker confirmation ----
///
/// A leader under a graduation budget of 1 confirms the condemn marker of three condemned blobs; one
/// graduates and two are carried. The leader and its pool then go away, so every later round runs in a
/// fresh `Gc` whose in-memory confirmation memo is empty, under an unbounded budget. Each test runs the
/// scenario twice: with the carried rows as written (`Persisted`), and with their confirmation cleared
/// before the fresh leader (`Cleared`), which is the row a carry wrote before the flag was persisted.
namespace
{
enum class CarryPath : uint8_t { Persisted, Cleared };

/// Counts reads of `.meta` keys and, when asked, throws on reads of one of them.
class MetaReadProbeBackend : public InMemoryBackend
{
public:
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key.ends_with(".meta"))
        {
            std::lock_guard lock(mutex);
            ++meta_reads[key];
            if (key == failing_key)
                throw std::runtime_error("injected fault: blob meta read failed");
        }
        return InMemoryBackend::read(key, access);
    }

    size_t metaReads(const String & key)
    {
        std::lock_guard lock(mutex);
        return meta_reads[key];
    }

    void resetMetaReads()
    {
        std::lock_guard lock(mutex);
        meta_reads.clear();
    }

    void failMetaReads(const String & key)
    {
        std::lock_guard lock(mutex);
        failing_key = key;
    }

private:
    std::mutex mutex;
    std::map<String, size_t> meta_reads;
    String failing_key;
};

BlobRef blobRefOf(const UInt128 & hash)
{
    return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)};
}

PoolPtr openPoolWithGraduationBudget(const std::shared_ptr<MetaReadProbeBackend> & backend, uint64_t budget)
{
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_round_graduation_budget = budget});
}

std::optional<RetiredEntry> retiredRowOf(Backend & backend, const Layout & layout, const UInt128 & hash)
{
    for (const RetiredEntry & e : currentRetiredSet(backend, layout, /*shard*/0))
        if (e.ref == blobRefOf(hash))
            return e;
    return std::nullopt;
}

void setMetaClean(Backend & backend, const Layout & layout, const UInt128 & hash)
{
    OperationForTest op(backend);
    const BlobRef blob = blobRefOf(hash);
    const auto lm = loadMeta(*op, layout, blob);
    if (!lm)
    {
        writeMetaClean(backend, layout, hash, /*size*/1);
        return;
    }
    BlobMeta clean = lm->meta;
    clean.state = MetaState::Clean;
    ASSERT_TRUE(std::holds_alternative<Committed>(casMeta(*op, layout, blob, lm->etag, clean)));
}

std::optional<MetaState> metaStateOf(Backend & backend, const Layout & layout, const UInt128 & hash)
{
    const auto lm = loadMetaForTest(backend, layout, hash);
    if (!lm)
        return std::nullopt;
    return lm->meta.state;
}

/// Rewrites the adopted seal's runs with `marker_confirmed` cleared on every row not yet `delete_pending`.
void clearCarriedConfirmations(Backend & backend, const Layout & layout)
{
    OperationForTest op(backend);
    const GcState st = decodeGcState((*op).read(layout.gcStateKey(), Retry::standard())->bytes);
    const String seal_key = layout.foldSealKey(st.snap_generation, st.snap_attempt);
    const auto seal_obj = (*op).read(seal_key, Retry::standard());
    ASSERT_TRUE(seal_obj.has_value());
    CasFoldSeal seal = decodeFoldSeal(seal_obj->bytes);
    for (RunRef & run : seal.blob_target_runs)
    {
        const auto run_obj = (*op).read(run.key, Retry::standard());
        ASSERT_TRUE(run_obj.has_value());
        DB::ReadBufferFromString in(run_obj->bytes);
        SourceEdgeRunReader reader(in);
        DB::WriteBufferFromOwnString out;
        SourceEdgeRunWriter writer(out);
        while (true)
        {
            SourceEdgeRecord rec;
            if (!reader.next(rec))
                break;
            if (rec.marker == RunMarker::Condemned && !rec.delete_pending)
                rec.marker_confirmed = false;
            writer.append(rec);
        }
        writer.finish();
        out.finalize();
        const String bytes = out.str();
        orThrow((*op).replace(run.key, bytes, run_obj->etag, Retry::standard()), "rewrite run " + run.key);
        run.checksum = sourceEdgeRunChecksum(bytes);
    }
    orThrow((*op).replace(seal_key, encodeFoldSeal(seal), seal_obj->etag, Retry::standard()), "rewrite seal " + seal_key);
}

/// Three condemned blobs, `blobs` sorted in the fold's settle order, each owned by its own table.
struct CondemnedCohort
{
    std::shared_ptr<MetaReadProbeBackend> backend = std::make_shared<MetaReadProbeBackend>();
    const Layout layout{"p"};
    const RootNamespace ns{"00/aa@cas@"};
    std::vector<UInt128> blobs;

    /// Publishes the three blobs, folds them, drops every owner and runs the condemning round.
    void condemn(Gc & gc)
    {
        for (uint64_t i = 1; i <= 3; ++i)
            blobs.push_back(UInt128(i));
        std::sort(blobs.begin(), blobs.end(), [](const UInt128 & a, const UInt128 & b) { return blobRefOf(a) < blobRefOf(b); });
        std::vector<ManifestRef> refs;
        for (size_t i = 0; i < blobs.size(); ++i)
        {
            const ManifestRef r = ref("srv-a:1", i + 1, static_cast<uint32_t>(i + 1));
            refs.push_back(r);
            writeBlobBody(*backend, layout, blobs[i]);
            writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", blobs[i])});
            publishCommittedTransition(*backend, layout, ns, "tbl" + std::to_string(i), std::nullopt, r);
        }
        runRegularRoundReclaiming(gc);
        for (size_t i = 0; i < blobs.size(); ++i)
            dropRefTransition(*backend, layout, ns, "tbl" + std::to_string(i), refs[i]);
        const RoundReport rep = runRegularRoundReclaiming(gc);
        ASSERT_EQ(rep.condemned, blobs.size());
    }

    /// The whole confirming leader: condemns, then runs one graduation round under a budget of 1 in the
    /// same process, which confirms all three from its memo, graduates `blobs[0]` and carries the rest.
    void confirmAndCarry()
    {
        auto store = openPoolWithGraduationBudget(backend, /*budget*/1);
        Gc gc(store, kGc);
        condemn(gc);
        const RoundReport rep = runRegularRoundReclaiming(gc);
        ASSERT_EQ(rep.graduated, 1u);
    }

    String metaKey(const UInt128 & hash) const { return layout.blobMetaKey(blobRefOf(hash)); }
};

void expectSameRow(const std::optional<RetiredEntry> & a, const std::optional<RetiredEntry> & b)
{
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a->delete_pending, b->delete_pending);
    EXPECT_EQ(a->marker_confirmed, b->marker_confirmed);
    EXPECT_EQ(a->token.dialect, b->token.dialect);
    EXPECT_EQ(a->token.value, b->token.value);
    EXPECT_EQ(a->condemn_round, b->condemn_round);
    EXPECT_EQ(a->size, b->size);
}
}

/// Oracle row "Marker `Clean` after a writer replaced T with T'": the carried row graduates and its
/// delete ends `Replaced`; the old path carried it and re-stamped the marker. The fresh leader holds
/// an empty memo. The writer's edge for T' lands after both rounds' intake cuts, so neither fold sees it.
TEST(CASGCFold, PersistedConfirmationGraduatesAfterRepublishAndClear)
{
    struct Outcome
    {
        std::optional<RetiredEntry> carried_before;
        std::optional<RetiredEntry> flagged_after;
        std::optional<RetiredEntry> sibling_after;
        std::optional<MetaState> flagged_meta_after;
        size_t sibling_meta_reads = 0;
        RoundReport delete_round;
        std::optional<Meta> flagged_body_after_delete;
        std::optional<Etag> replacement;
    };
    const auto run = [](CarryPath path)
    {
        CondemnedCohort c;
        c.confirmAndCarry();
        const UInt128 flagged = c.blobs[1];
        const UInt128 sibling = c.blobs[2];
        Outcome o;
        o.carried_before = retiredRowOf(*c.backend, c.layout, flagged);
        if (path == CarryPath::Cleared)
            clearCarriedConfirmations(*c.backend, c.layout);

        o.replacement = displaceBlobToken(*c.backend, c.layout, blobRefOf(flagged));
        setMetaClean(*c.backend, c.layout, flagged);

        auto store = openPoolWithGraduationBudget(c.backend, /*unbounded*/0);
        Gc gc(store, kGc);
        c.backend->resetMetaReads();
        runRegularRoundReclaiming(gc);
        o.flagged_after = retiredRowOf(*c.backend, c.layout, flagged);
        o.sibling_after = retiredRowOf(*c.backend, c.layout, sibling);
        o.flagged_meta_after = metaStateOf(*c.backend, c.layout, flagged);
        o.sibling_meta_reads = c.backend->metaReads(c.metaKey(sibling));

        o.delete_round = runRegularRoundReclaiming(gc);
        OperationForTest op(*c.backend);
        o.flagged_body_after_delete = (*op).head(c.layout.blobKey(blobRefOf(flagged)), Retry::standard());
        return o;
    };
    const Outcome persisted = run(CarryPath::Persisted);
    const Outcome cleared = run(CarryPath::Cleared);

    ASSERT_TRUE(persisted.carried_before.has_value());
    EXPECT_FALSE(persisted.carried_before->delete_pending);
    EXPECT_TRUE(persisted.carried_before->marker_confirmed) << "the carry must persist the confirmation";

    ASSERT_TRUE(cleared.flagged_after.has_value());
    EXPECT_FALSE(cleared.flagged_after->delete_pending) << "old path: the Clean marker refuses graduation";
    EXPECT_EQ(cleared.flagged_meta_after, MetaState::Condemned) << "old path: the refused gate re-stamps the marker";

    ASSERT_TRUE(persisted.flagged_after.has_value());
    EXPECT_TRUE(persisted.flagged_after->delete_pending) << "the persisted confirmation graduates the row";
    EXPECT_EQ(persisted.flagged_meta_after, MetaState::Clean) << "graduation reads and writes no marker";
    EXPECT_EQ(persisted.delete_round.replaced, 1u) << "the exact-token delete of T finds T'";
    ASSERT_TRUE(persisted.flagged_body_after_delete.has_value());
    EXPECT_EQ(persisted.flagged_body_after_delete->etag, *persisted.replacement) << "T' must survive";

    expectSameRow(persisted.sibling_after, cleared.sibling_after);
    EXPECT_TRUE(persisted.sibling_after->delete_pending);
    EXPECT_EQ(persisted.sibling_meta_reads, 0u);
    EXPECT_EQ(cleared.sibling_meta_reads, 1u);
}

/// Oracle row "Meta GET throws": the carried row graduates although its meta read throws, and the next
/// round deletes the present T; the old path carried it. The fresh leader holds an empty memo; no
/// writer takes part.
TEST(CASGCFold, PersistedConfirmationGraduatesWithUnreadableMeta)
{
    struct Outcome
    {
        std::optional<RetiredEntry> flagged_after;
        std::optional<RetiredEntry> sibling_after;
        uint64_t unconfirmed_carries = 0;
        bool flagged_present_after_delete = true;
    };
    const auto run = [](CarryPath path)
    {
        CondemnedCohort c;
        c.confirmAndCarry();
        const UInt128 flagged = c.blobs[1];
        const UInt128 sibling = c.blobs[2];
        if (path == CarryPath::Cleared)
            clearCarriedConfirmations(*c.backend, c.layout);

        auto store = openPoolWithGraduationBudget(c.backend, /*unbounded*/0);
        Gc gc(store, kGc);
        c.backend->failMetaReads(c.metaKey(flagged));
        Outcome o;
        const auto carries_before = ProfileEvents::global_counters[ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry].load();
        runRegularRoundReclaiming(gc);
        o.unconfirmed_carries = ProfileEvents::global_counters[ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry].load() - carries_before;
        c.backend->failMetaReads({});
        o.flagged_after = retiredRowOf(*c.backend, c.layout, flagged);
        o.sibling_after = retiredRowOf(*c.backend, c.layout, sibling);

        runRegularRoundReclaiming(gc);
        OperationForTest op(*c.backend);
        o.flagged_present_after_delete = (*op).head(c.layout.blobKey(blobRefOf(flagged)), Retry::standard()).has_value();
        return o;
    };
    const Outcome persisted = run(CarryPath::Persisted);
    const Outcome cleared = run(CarryPath::Cleared);

    ASSERT_TRUE(cleared.flagged_after.has_value());
    EXPECT_FALSE(cleared.flagged_after->delete_pending) << "old path: an unreadable meta refuses graduation";
    EXPECT_EQ(cleared.unconfirmed_carries, 1u);

    ASSERT_TRUE(persisted.flagged_after.has_value());
    EXPECT_TRUE(persisted.flagged_after->delete_pending) << "the persisted confirmation needs no meta read";
    EXPECT_EQ(persisted.unconfirmed_carries, 0u);
    EXPECT_FALSE(persisted.flagged_present_after_delete) << "the next round deletes the present T";

    expectSameRow(persisted.sibling_after, cleared.sibling_after);
    EXPECT_TRUE(persisted.sibling_after->delete_pending);
}

/// Safety point "delayed marker retry": T's marker is lost, a writer replaces T with T' and marks it
/// `Clean`, and the gate's retry then stamps `Condemned` over T' and records T in the second leader's
/// memo. That leader carries the row past an exhausted budget; a third leader with an empty memo
/// graduates it from the persisted flag, and the delete of T ends `Replaced`. The writer's edge for T'
/// lands after every intake cut in the test.
TEST(CASGCFold, DelayedMarkerRetryOverReplacementDoesNotDeleteIt)
{
    struct Outcome
    {
        std::optional<MetaState> meta_after_retry;
        std::optional<RetiredEntry> carried;
        std::optional<RetiredEntry> flagged_after;
        size_t flagged_meta_reads = 0;
        RoundReport delete_round;
        std::optional<Meta> flagged_body_after_delete;
        std::optional<MetaState> meta_after_delete;
        std::optional<Etag> replacement;
    };
    const auto run = [](CarryPath path)
    {
        CondemnedCohort c;
        /// The last blob in settle order, so both graduation rounds below spend their budget first.
        UInt128 flagged;
        Outcome o;
        {
            auto store = openPoolWithGraduationBudget(c.backend, /*budget*/1);
            {
                Gc first(store, kGc);
                c.condemn(first);
            }
            flagged = c.blobs[2];
            OperationForTest op(*c.backend);
            const auto lm = loadMeta(*op, c.layout, blobRefOf(flagged));
            EXPECT_TRUE(lm.has_value());
            if (lm)
                deleteMetaExact(*op, c.layout, blobRefOf(flagged), lm->etag);
            o.replacement = displaceBlobToken(*c.backend, c.layout, blobRefOf(flagged));
            setMetaClean(*c.backend, c.layout, flagged);

            Gc second(store, kGc);
            runRegularRoundReclaiming(second);
            o.meta_after_retry = metaStateOf(*c.backend, c.layout, flagged);
            runRegularRoundReclaiming(second);
            o.carried = retiredRowOf(*c.backend, c.layout, flagged);
        }
        if (path == CarryPath::Cleared)
            clearCarriedConfirmations(*c.backend, c.layout);

        auto store = openPoolWithGraduationBudget(c.backend, /*unbounded*/0);
        Gc third(store, kGc);
        c.backend->resetMetaReads();
        runRegularRoundReclaiming(third);
        o.flagged_after = retiredRowOf(*c.backend, c.layout, flagged);
        o.flagged_meta_reads = c.backend->metaReads(c.metaKey(flagged));
        o.delete_round = runRegularRoundReclaiming(third);
        OperationForTest op(*c.backend);
        o.flagged_body_after_delete = (*op).head(c.layout.blobKey(blobRefOf(flagged)), Retry::standard());
        o.meta_after_delete = metaStateOf(*c.backend, c.layout, flagged);
        return o;
    };
    const Outcome persisted = run(CarryPath::Persisted);
    const Outcome cleared = run(CarryPath::Cleared);

    EXPECT_EQ(persisted.meta_after_retry, MetaState::Condemned) << "the retry stamps Condemned over T'";
    ASSERT_TRUE(persisted.carried.has_value());
    EXPECT_FALSE(persisted.carried->delete_pending) << "precondition: the budget carried the row";
    EXPECT_TRUE(persisted.carried->marker_confirmed) << "the carry persists the retry's confirmation of T";
    EXPECT_FALSE(persisted.carried->token.matches(*persisted.replacement)) << "the row still names T";

    ASSERT_TRUE(persisted.flagged_after.has_value());
    ASSERT_TRUE(cleared.flagged_after.has_value());
    EXPECT_TRUE(persisted.flagged_after->delete_pending);
    EXPECT_TRUE(cleared.flagged_after->delete_pending) << "old path: the stray Condemned marker confirms T";
    EXPECT_EQ(persisted.flagged_meta_reads, 0u);
    EXPECT_EQ(cleared.flagged_meta_reads, 1u);

    for (const Outcome * o : {&persisted, &cleared})
    {
        EXPECT_EQ(o->delete_round.replaced, 1u) << "the exact-token delete of T finds T'";
        ASSERT_TRUE(o->flagged_body_after_delete.has_value());
        EXPECT_EQ(o->flagged_body_after_delete->etag, *o->replacement) << "T' must survive";
        EXPECT_EQ(o->meta_after_delete, MetaState::Condemned);
    }
}

/// Oracle row "a supersede to T'": the superseding row starts unflagged, because its marker confirms
/// nothing about T'. The fresh leader holds an empty memo. Unlike the other tests, the writer's add and
/// drop of its T' reference both land before the round's intake cut: that touch is what triggers the supersede.
TEST(CASGCFold, SupersededRowStartsUnconfirmed)
{
    struct Outcome
    {
        std::optional<RetiredEntry> carried_before;
        std::optional<RetiredEntry> flagged_after;
        std::optional<RetiredEntry> sibling_after;
        std::optional<Etag> replacement;
    };
    const auto run = [](CarryPath path)
    {
        CondemnedCohort c;
        c.confirmAndCarry();
        const UInt128 flagged = c.blobs[1];
        const UInt128 sibling = c.blobs[2];
        Outcome o;
        o.carried_before = retiredRowOf(*c.backend, c.layout, flagged);
        if (path == CarryPath::Cleared)
            clearCarriedConfirmations(*c.backend, c.layout);

        o.replacement = displaceBlobToken(*c.backend, c.layout, blobRefOf(flagged));
        setMetaClean(*c.backend, c.layout, flagged);
        const ManifestRef writer_ref = ref("srv-a:1", 10, 10);
        writeManifestRaw(*c.backend, c.layout, c.ns, writer_ref, {blobEntryFor("a", flagged)});
        publishCommittedTransition(*c.backend, c.layout, c.ns, "writer", std::nullopt, writer_ref);
        dropRefTransition(*c.backend, c.layout, c.ns, "writer", writer_ref);

        auto store = openPoolWithGraduationBudget(c.backend, /*unbounded*/0);
        Gc gc(store, kGc);
        runRegularRoundReclaiming(gc);
        o.flagged_after = retiredRowOf(*c.backend, c.layout, flagged);
        o.sibling_after = retiredRowOf(*c.backend, c.layout, sibling);
        return o;
    };
    const Outcome persisted = run(CarryPath::Persisted);
    const Outcome cleared = run(CarryPath::Cleared);

    ASSERT_TRUE(persisted.carried_before.has_value());
    EXPECT_TRUE(persisted.carried_before->marker_confirmed) << "precondition: the superseded row was flagged";

    for (const Outcome * o : {&persisted, &cleared})
    {
        ASSERT_TRUE(o->flagged_after.has_value());
        EXPECT_TRUE(o->flagged_after->token.matches(*o->replacement)) << "the row now names T'";
        EXPECT_FALSE(o->flagged_after->delete_pending);
        EXPECT_FALSE(o->flagged_after->marker_confirmed) << "a superseding row must not inherit the flag";
    }
    EXPECT_EQ(persisted.flagged_after->condemn_round, cleared.flagged_after->condemn_round);
    expectSameRow(persisted.sibling_after, cleared.sibling_after);
    EXPECT_TRUE(persisted.sibling_after->delete_pending);
}

/// Oracle row "Marker absent after T was deleted": the carried row graduates and its delete ends
/// absent; the old path carried it and re-created the marker. The fresh leader holds an empty memo; no
/// writer takes part.
TEST(CASGCFold, PersistedConfirmationGraduatesWithMetaAbsent)
{
    struct Outcome
    {
        std::optional<RetiredEntry> flagged_after;
        std::optional<RetiredEntry> sibling_after;
        size_t flagged_meta_reads = 0;
        uint64_t unconfirmed_carries = 0;
        std::optional<MetaState> flagged_meta_after;
        RoundReport delete_round;
    };
    const auto run = [](CarryPath path)
    {
        CondemnedCohort c;
        c.confirmAndCarry();
        const UInt128 flagged = c.blobs[1];
        const UInt128 sibling = c.blobs[2];
        if (path == CarryPath::Cleared)
            clearCarriedConfirmations(*c.backend, c.layout);
        {
            OperationForTest op(*c.backend);
            const String key = c.layout.blobKey(blobRefOf(flagged));
            const auto body = (*op).head(key, Retry::standard());
            EXPECT_TRUE(body.has_value());
            if (body)
                EXPECT_EQ((*op).remove(key, body->etag, Retry::once()), Removal::Removed);
            const auto lm = loadMeta(*op, c.layout, blobRefOf(flagged));
            EXPECT_TRUE(lm.has_value());
            if (lm)
                deleteMetaExact(*op, c.layout, blobRefOf(flagged), lm->etag);
        }

        auto store = openPoolWithGraduationBudget(c.backend, /*unbounded*/0);
        Gc gc(store, kGc);
        c.backend->resetMetaReads();
        Outcome o;
        const auto carries_before = ProfileEvents::global_counters[ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry].load();
        runRegularRoundReclaiming(gc);
        o.unconfirmed_carries = ProfileEvents::global_counters[ProfileEvents::CASGCCondemnMarkerUnconfirmedCarry].load() - carries_before;
        o.flagged_after = retiredRowOf(*c.backend, c.layout, flagged);
        o.sibling_after = retiredRowOf(*c.backend, c.layout, sibling);
        o.flagged_meta_reads = c.backend->metaReads(c.metaKey(flagged));
        o.flagged_meta_after = metaStateOf(*c.backend, c.layout, flagged);
        o.delete_round = runRegularRoundReclaiming(gc);
        return o;
    };
    const Outcome persisted = run(CarryPath::Persisted);
    const Outcome cleared = run(CarryPath::Cleared);

    ASSERT_TRUE(cleared.flagged_after.has_value());
    EXPECT_FALSE(cleared.flagged_after->delete_pending) << "old path: an absent marker refuses graduation";
    EXPECT_EQ(cleared.unconfirmed_carries, 1u);
    EXPECT_EQ(cleared.flagged_meta_after, MetaState::Condemned) << "old path: the refused gate re-creates the marker";

    ASSERT_TRUE(persisted.flagged_after.has_value());
    EXPECT_TRUE(persisted.flagged_after->delete_pending) << "the persisted confirmation graduates the row";
    EXPECT_EQ(persisted.flagged_meta_reads, 0u);
    EXPECT_EQ(persisted.unconfirmed_carries, 0u);
    EXPECT_FALSE(persisted.flagged_meta_after.has_value()) << "graduation writes no marker";
    EXPECT_EQ(persisted.delete_round.absent, 1u) << "the delete of the already-deleted T ends absent";
    EXPECT_EQ(persisted.delete_round.replaced, 0u);

    expectSameRow(persisted.sibling_after, cleared.sibling_after);
    EXPECT_TRUE(persisted.sibling_after->delete_pending);
}

namespace
{

/// Records which watched manifest keys the running fold read, and fails the test on a delete of one of
/// them while the fold still runs. The window is the phase-sink interval from the end of
/// `fold_ref_group` to the end of `fold_reduce`, which contains every read of the ref intake.
class FoldGuardBackend : public InMemoryBackend
{
public:
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto got = InMemoryBackend::read(key, access);
        std::function<void()> hook;
        {
            std::lock_guard lock(mutex);
            ++reads_of[key];
            if (got && fold_running && watched.contains(key))
                folded.insert(key);
            if (const auto it = read_hooks.find(key); it != read_hooks.end())
            {
                hook = std::move(it->second);
                read_hooks.erase(it);
            }
        }
        if (hook)
            hook();
        return got;
    }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        checkDelete(key);
        return InMemoryBackend::remove(key, expected_value, access);
    }

    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override
    {
        for (const WriteOnceKey & key : keys)
            checkDelete(key.str());
        InMemoryBackend::removeManyWriteOnce(keys, access);
    }

    void watch(const String & key)
    {
        std::lock_guard lock(mutex);
        watched.insert(key);
    }
    /// Runs `hook` once, right after the first read of `key` returns.
    void onRead(const String & key, std::function<void()> hook)
    {
        std::lock_guard lock(mutex);
        read_hooks[key] = std::move(hook);
    }
    GcPhaseSink phaseSink()
    {
        return [this](const GcPhaseRecord & record)
        {
            std::lock_guard lock(mutex);
            if (record.phase == "fold_ref_group")
                fold_running = true;
            else if (record.phase == "fold_reduce")
                fold_running = false;
        };
    }
    void setFailOnGuardedDelete(bool fail)
    {
        std::lock_guard lock(mutex);
        fail_on_guarded_delete = fail;
    }
    std::set<String> foldedKeys() const
    {
        std::lock_guard lock(mutex);
        return folded;
    }
    std::vector<String> guardedDeletes() const
    {
        std::lock_guard lock(mutex);
        return guarded_deletes;
    }
    size_t readsOf(const String & key) const
    {
        std::lock_guard lock(mutex);
        const auto it = reads_of.find(key);
        return it == reads_of.end() ? 0 : it->second;
    }

private:
    void checkDelete(const String & key)
    {
        std::lock_guard lock(mutex);
        if (!fold_running || !folded.contains(key))
            return;
        guarded_deletes.push_back(key);
        if (fail_on_guarded_delete)
            ADD_FAILURE() << "a manifest body the running fold has folded was deleted: " << key;
    }

    mutable std::mutex mutex;
    std::set<String> watched;
    std::set<String> folded;
    std::vector<String> guarded_deletes;
    std::map<String, size_t> reads_of;
    std::map<String, std::function<void()>> read_hooks;
    bool fold_running = false;
    bool fail_on_guarded_delete = true;
};

/// Everything a fold run leaves behind that the memo could change: the whole store, the folded edge
/// events, the manifest deletes with their tokens, each round's clamp decision and the read counters.
struct MemoRun
{
    std::map<String, String> objects;
    std::vector<String> fold_events;
    std::vector<bool> clamped;
    uint64_t body_gets = 0;
    uint64_t memo_hits = 0;
    uint64_t read_ahead_misses = 0;
    uint64_t read_ahead_wasted = 0;
};

struct MemoOptions
{
    bool memo = true;
    uint64_t io_concurrency = 16;
    std::optional<size_t> memo_budget = std::nullopt;
};

struct MemoFixture
{
    std::shared_ptr<FoldGuardBackend> backend = std::make_shared<FoldGuardBackend>();
    std::shared_ptr<SharedEventLog> events = std::make_shared<SharedEventLog>();
    PoolPtr store;
    std::unique_ptr<Gc> gc;

    explicit MemoFixture(const MemoOptions & options)
    {
        store = Pool::open(backend, PoolConfig{
            .pool_prefix = "p",
            .server_root_id = "test",
            .gc_fold_max_defer_rounds = 0,
            .gc_io_concurrency = options.io_concurrency,
            .event_sink = [log = events](CasEvent event) { log->push(std::move(event)); }});
        gc = std::make_unique<Gc>(store, kGc);
        gc->setManifestMemoForTest(options.memo);
        if (options.memo_budget)
            gc->setManifestMemoBudgetForTest(*options.memo_budget);
        gc->setPhaseSink(backend->phaseSink());
    }
    const Layout & layout() const { return store->layout(); }
};

uint64_t counter(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event].load();
}

std::map<String, String> objectsOf(Backend & backend)
{
    std::map<String, String> objects;
    OperationForTest op(backend);
    String cursor;
    while (true)
    {
        const ListPage page = (*op).list("", cursor, 1000, Retry::standard());
        for (const ListedKey & listed : page.keys)
        {
            /// Per-pool identity (a random pool id, the mount lease's clock) differs between any two pools.
            if (listed.key == "p/_pool_meta" || listed.key.starts_with("p/gc/server-roots/"))
                continue;
            if (const auto got = (*op).read(listed.key, Retry::standard()))
                objects[listed.key] = got->bytes;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return objects;
}

std::vector<String> foldEventsOf(const SharedEventLog & events)
{
    std::vector<String> rendered;
    for (const CasEvent & ev : events.snapshot())
    {
        if (ev.type == CasEventType::ManifestDelete)
        {
            rendered.push_back(fmt::format("manifest_delete|{}|{}|{}|{}", ev.namespace_, ev.object_hash, ev.token, ev.outcome));
            continue;
        }
        if (ev.type != CasEventType::RootAdd && ev.type != CasEventType::RootRemove)
            continue;
        String line = fmt::format("{}|{}|{}|{}|{}", ev.type == CasEventType::RootAdd ? "add" : "remove",
                                  ev.namespace_, ev.object_hash, ev.outcome, ev.reason);
        for (const auto & [name, value] : ev.detail)
            line += fmt::format("|{}={}", name, value);
        rendered.push_back(std::move(line));
    }
    return rendered;
}

/// Runs `rounds` regular rounds on a fresh pool, calling `stage(fixture, round)` before each.
MemoRun runMemoScenario(const MemoOptions & options, size_t rounds,
                        const std::function<void(MemoFixture &, size_t)> & stage)
{
    MemoFixture f(options);
    MemoRun run;
    const uint64_t gets_before = counter(ProfileEvents::CASRefManifestBodyFoldGets);
    const uint64_t hits_before = counter(ProfileEvents::CASRefManifestBodyMemoHits);
    const uint64_t misses_before = counter(ProfileEvents::CASGCReadAheadMiss);
    const uint64_t wasted_before = counter(ProfileEvents::CASGCReadAheadWasted);
    for (size_t round = 0; round < rounds; ++round)
    {
        stage(f, round);
        run.clamped.push_back(!f.gc->runRegularRound().anomalies.empty());
    }
    run.body_gets = counter(ProfileEvents::CASRefManifestBodyFoldGets) - gets_before;
    run.memo_hits = counter(ProfileEvents::CASRefManifestBodyMemoHits) - hits_before;
    run.read_ahead_misses = counter(ProfileEvents::CASGCReadAheadMiss) - misses_before;
    run.read_ahead_wasted = counter(ProfileEvents::CASGCReadAheadWasted) - wasted_before;
    run.objects = objectsOf(*f.backend);
    run.fold_events = foldEventsOf(*f.events);
    return run;
}

/// The memo must not change anything a fold decides: same store, same edge events, same clamps.
void expectSameOutcome(const MemoRun & memo, const MemoRun & oracle)
{
    std::vector<String> differing;
    for (const auto & [key, bytes] : memo.objects)
        if (!oracle.objects.contains(key) || oracle.objects.at(key) != bytes)
            differing.push_back(key);
    for (const auto & [key, bytes] : oracle.objects)
        if (!memo.objects.contains(key))
            differing.push_back(key);
    EXPECT_EQ(differing, std::vector<String>{}) << "the store after the rounds differs from the no-memo fold";
    EXPECT_EQ(memo.fold_events, oracle.fold_events);
    EXPECT_EQ(memo.clamped, oracle.clamped);
    EXPECT_EQ(memo.body_gets + memo.memo_hits, oracle.body_gets)
        << "every body the no-memo fold consumed is consumed once, from a read or from the memo";
    EXPECT_EQ(oracle.memo_hits, 0u);
}

const RootNamespace kMemoNs{"00/aa@cas@"};

/// Returns the ref-log key of the drop.
String publishThenDrop(MemoFixture & f, const ManifestRef & a)
{
    writeManifestRaw(*f.backend, f.layout(), kMemoNs, a,
        {blobEntryFor("a", DB::UInt128(1)), blobEntryFor("b", DB::UInt128(2))});
    f.backend->watch(f.layout().manifestKey(ManifestId{kMemoNs, a}));
    publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", std::nullopt, a);
    const uint64_t drop = dropRefTransition(*f.backend, f.layout(), kMemoNs, "r1", a);
    return f.layout().refLogKey(
        CasRefCatalog::lifeIfCataloged(*OperationForTest(*f.backend), f.layout(), kMemoNs).value(), RefTxnId{1, drop});
}

}

/// A part published and dropped in one round: its body is read once, and the drop's hit reproduces
/// the `-1` deltas and the exact-incarnation cleanup that deletes the body after the round.
TEST(CASGCFold, ManifestFoldedForPublishAndDropIsReadOnce)
{
    const ManifestRef a = ref("", 1, 1);
    std::vector<String> a_tokens;
    const auto stage = [&](MemoFixture & f, size_t)
    {
        publishThenDrop(f, a);
        a_tokens.push_back(
            (*OperationForTest(*f.backend)).read(f.layout().manifestKey(ManifestId{kMemoNs, a}), Retry::standard())->etag.render());
    };
    const MemoRun memo = runMemoScenario({}, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(oracle.body_gets, 2u);
    EXPECT_EQ(memo.body_gets, 1u);
    EXPECT_EQ(memo.memo_hits, 1u);
    ASSERT_EQ(memo.fold_events.size(), 5u) << "two +1 and two -1 blob edges, then the body delete";
    ASSERT_EQ(a_tokens.size(), 2u);
    EXPECT_EQ(memo.fold_events.back(),
              fmt::format("manifest_delete|{}|{}|{}|deleted_or_absent", kMemoNs.string(), manifestRefDebugString(a), a_tokens[0]))
        << "the delete carries the incarnation of the body the memo read";
    EXPECT_FALSE(memo.objects.contains(Layout("p").manifestKey(ManifestId{kMemoNs, a})))
        << "the drop's cleanup token came from the memo and still deleted the body";
}

/// Memoized manifests across several transactions and a repoint: each hit folds with the sign and
/// the ordinal of the edge in front of it. B's drop is its log's only edge, so a hit carrying another
/// transaction's ordinal leaves that log unapplied. A later edge that clamps its log discards that
/// log's staged hit and its cleanup.
TEST(CASGCFold, ManifestMemoHitUsesTheCurrentSignAndOrdinal)
{
    const ManifestRef a = ref("", 1, 1);
    const ManifestRef b = ref("", 2, 1);
    const ManifestRef c = ref("", 3, 1);
    const ManifestRef d = ref("", 4, 1);
    const Layout layout("p");
    const String d_key = layout.manifestKey(ManifestId{kMemoNs, d});
    /// The in-degree of D's blob before each round after the first.
    std::vector<int64_t> d_in_degree;
    const auto stage = [&](MemoFixture & f, size_t round)
    {
        if (round == 0)
        {
            writeManifestRaw(*f.backend, f.layout(), kMemoNs, a, {blobEntryFor("a", DB::UInt128(1))});
            writeManifestRaw(*f.backend, f.layout(), kMemoNs, b, {blobEntryFor("b", DB::UInt128(2))});
            writeManifestRaw(*f.backend, f.layout(), kMemoNs, d, {blobEntryFor("d", DB::UInt128(4))});
            publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r3", std::nullopt, d);
            publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", std::nullopt, a);
            publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", a, b);   /// repoint: A's `-1` is a hit
            dropRefTransition(*f.backend, f.layout(), kMemoNs, "r1", b);               /// a log of one hit
            /// C's body is absent: the log clamps after D's `-1` was staged from the memo.
            const uint64_t seq = appendRefLogSeed(*f.backend, f.layout(), kMemoNs,
                {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r3", d}, std::nullopt),
                 ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r2", c})});
            advanceRecoverableCkptForRawFixture(*f.backend, f.layout(), kMemoNs, RefTxnId{1, seq});
        }
        else
        {
            d_in_degree.push_back(inDegreeOf(*f.backend, f.layout(), DB::UInt128(4)));
            if (round == 1)
                writeManifestRaw(*f.backend, f.layout(), kMemoNs, c, {blobEntryFor("c", DB::UInt128(3))});
        }
    };
    const MemoRun memo = runMemoScenario({}, 3, stage);
    EXPECT_EQ(d_in_degree, (std::vector<int64_t>{1, 0}))
        << "the clamp discarded D's `-1` staged from the memo; the next round applied it";
    d_in_degree.clear();
    const MemoRun oracle = runMemoScenario({.memo = false}, 3, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(memo.clamped, (std::vector<bool>{true, false, false}));
    EXPECT_EQ(memo.memo_hits, 3u) << "the removals of A, B and D in the first round";
    EXPECT_FALSE(memo.objects.contains(d_key));
}

/// The hint loop skips a memoized body. When the take evicts it before its edge folds, the fold
/// reads it inline: one extra read-ahead miss and one GET, same outcome.
TEST(CASGCFold, ManifestMemoSkipsHintsAndFallsBackInline)
{
    const ManifestRef a = ref("", 1, 1);
    const ManifestRef b = ref("", 2, 1);

    /// A budget that holds one of the two manifests but not both.
    size_t budget = 0;
    {
        InMemoryBackend scratch;
        const auto charge = [&](const std::vector<ManifestRef> & refs)
        {
            GcManifestMemo probe;
            for (const ManifestRef & r : refs)
            {
                const ManifestId id{kMemoNs, r};
                const String key = Layout("p").manifestKey(id);
                OperationForTest op(scratch);
                if (!(*op).read(key, Retry::standard()))
                    (*op).create(key, "x", Retry::standard());
                probe.insert(id, ManifestFold{.etag = (*op).read(key, Retry::standard())->etag, .entries = {
                    ManifestFoldEntry{.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(DB::UInt128(1))},
                                      .source_id = sourceEdgeId(id, "a"), .path = "a"}}});
            }
            return probe.charged();
        };
        const size_t one = charge({a});
        const size_t two = charge({a, b});
        budget = one + (two - one) / 2;
        ASSERT_LT(budget, two);
    }

    const auto stage = [&](MemoFixture & f, size_t)
    {
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, a, {blobEntryFor("a", DB::UInt128(1))});
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, b, {blobEntryFor("a", DB::UInt128(1))});
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", std::nullopt, a);
        /// B folds first and its insert evicts A, whose hint the memo hit had skipped.
        const uint64_t seq = appendRefLogSeed(*f.backend, f.layout(), kMemoNs,
            {ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r2", b}),
             ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r1", a}, std::nullopt)});
        advanceRecoverableCkptForRawFixture(*f.backend, f.layout(), kMemoNs, RefTxnId{1, seq});
    };
    const MemoRun memo = runMemoScenario({.memo_budget = budget}, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(memo.body_gets, 3u);
    EXPECT_EQ(memo.memo_hits, 0u);
    EXPECT_EQ(memo.read_ahead_misses, oracle.read_ahead_misses + 1) << "A's inline read after its eviction";
}

/// A repeated manifest whose charge exceeds the budget is never stored: two GETs, zero hits.
TEST(CASGCFold, ManifestLargerThanTheMemoBudgetIsReadEachTime)
{
    const ManifestRef a = ref("", 1, 1);
    const auto stage = [&](MemoFixture & f, size_t) { publishThenDrop(f, a); };
    const MemoRun memo = runMemoScenario({.memo_budget = 64}, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(memo.body_gets, 2u);
    EXPECT_EQ(memo.memo_hits, 0u);
}

/// An absent body is not memoized: the next edge naming it reads it again and sees it present.
TEST(CASGCFold, AbsentManifestBodyIsReprobed)
{
    const RootNamespace ns{"srv/tbl"};
    const ManifestRef p = ManifestRef{.writer_epoch = 1, .build_sequence = 5, .manifest_ordinal = 1};
    const String p_key = Layout("p").manifestKey(ManifestId{ns, p});
    const auto stage = [&](MemoFixture & f, size_t)
    {
        /// Build 5 is below the floor, so the first edge's absent body is skipped rather than clamped.
        setWatermarkMinActive(*f.backend, f.layout(), "srv", /*writer_epoch*/ 1, /*min_active_build_sequence*/ 10);
        addPrecommitTransition(*f.backend, f.layout(), ns, DB::UInt128(7), "r1", std::nullopt, p);
        const uint64_t seq = appendRefLogSeed(*f.backend, f.layout(), ns, publishCommittedOps("r2", p));
        advanceRecoverableCkptForRawFixture(*f.backend, f.layout(), ns, RefTxnId{1, seq});
        const String second_log = f.layout().refLogKey(
            CasRefCatalog::lifeIfCataloged(*OperationForTest(*f.backend), f.layout(), ns).value(), RefTxnId{1, seq});
        f.backend->onRead(second_log, [&f, ns, p]
            { writeManifestRaw(*f.backend, f.layout(), ns, p, {blobEntryFor("a", DB::UInt128(1))}); });
    };
    const MemoOptions sequential{.io_concurrency = 1};
    MemoFixture probe(sequential);
    stage(probe, 0);
    probe.gc->runRegularRound();
    const size_t p_reads = probe.backend->readsOf(p_key);

    const MemoRun memo = runMemoScenario(sequential, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false, .io_concurrency = 1}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(p_reads, 2u) << "absent on the first edge, read again and present on the second";
    EXPECT_EQ(memo.body_gets, 1u) << "an absent read is not a consumed body";
    EXPECT_EQ(memo.memo_hits, 0u);
    EXPECT_EQ(inDegreeOf(*probe.backend, probe.layout(), DB::UInt128(1)), 1);
}

/// A memo hit is a fresh read only while no folded body is deleted during the fold. Inside the fold,
/// after both A and the writer's precommitted M were memoized, the orphan sweep runs over A's build
/// prefix and the writer abandons M's build. Each deletes other debris and neither deletes a folded
/// body; a direct delete of A in the same window is caught.
TEST(CASGCFold, FoldedManifestBodyIsNeverDeletedDuringTheFold)
{
    /// Far from the build sequences the writer below mints, so the two never share a key.
    const ManifestRef a = ref("", 1000, 1);
    const ManifestRef orphan = ref("", 1000, 2);
    const auto run = [&](bool delete_a_directly) -> std::vector<String>
    {
        MemoFixture f(MemoOptions{.io_concurrency = 1});
        f.backend->setFailOnGuardedDelete(!delete_a_directly);
        const String a_key = f.layout().manifestKey(ManifestId{kMemoNs, a});
        const String orphan_key = f.layout().manifestKey(ManifestId{kMemoNs, orphan});
        setWatermarkMinActive(*f.backend, f.layout(), "00", /*writer_epoch*/ 1, /*min_active_build_sequence*/ 2000);
        /// A first round folds past the seal of epoch 1. The sweep then has authority over epoch-1 builds,
        /// and A, published and dropped in epoch 2, is protected only by its unfolded drop.
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, ref("", 3000, 1), {blobEntryFor("f", DB::UInt128(6))});
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "filler", std::nullopt, ref("", 3000, 1));
        writeSealAt(*f.backend, f.layout(), kMemoNs, RefTxnId{1, 2});
        const ManifestRef filler2{.writer_epoch = 2, .build_sequence = 1, .manifest_ordinal = 1};
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, filler2, {blobEntryFor("f", DB::UInt128(6))});
        writeTxnAt(*f.backend, f.layout(), kMemoNs, RefTxnId{2, 1}, publishCommittedOps("filler2", filler2), RefTxnId{1, 2});
        const auto checkpoint = [&](const RefTxnId & through)
        {
            replaceRecoverableCkptForRawFixture(*f.backend, f.layout(), kMemoNs, RefCkpt{
                .life_epoch = 1, .committed_through = through, .checkpoint_snapshot_id = std::nullopt,
                .last_epoch_seal = RefTxnId{1, 2}});
        };
        checkpoint(RefTxnId{2, 1});
        f.gc->runRegularRound();

        /// The writer owns its namespace's ref stream, which the raw fixtures below must not share. That
        /// namespace sorts before A's, so the fold memoizes M before the hook runs.
        const RootNamespace writer_ns{"00/a0@cas@"};
        PartWriteInfo info;
        info.intended_namespace = writer_ns;
        info.intended_ref = writer_ns.string() + "/m";
        auto build = f.store->beginPartWrite(info);
        const ManifestId m = build->stageManifest({blobEntryFor("m", DB::UInt128(8))});
        build->precommitAdd(writer_ns, "m", m);
        const ManifestId debris = build->stageManifest({blobEntryFor("d", DB::UInt128(9))});
        const String m_key = f.layout().manifestKey(m);
        f.backend->watch(m_key);

        writeManifestRaw(*f.backend, f.layout(), kMemoNs, orphan, {blobEntryFor("o", DB::UInt128(7))});
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, a, {blobEntryFor("a", DB::UInt128(1))});
        f.backend->watch(a_key);
        writeTxnAt(*f.backend, f.layout(), kMemoNs, RefTxnId{2, 2}, publishCommittedOps("r1", a));
        writeTxnAt(*f.backend, f.layout(), kMemoNs, RefTxnId{2, 3},
            {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r1", a}, std::nullopt)});
        checkpoint(RefTxnId{2, 3});
        const String drop_log = f.layout().refLogKey(
            CasRefCatalog::lifeIfCataloged(*OperationForTest(*f.backend), f.layout(), kMemoNs).value(), RefTxnId{2, 3});

        uint64_t swept = 0;
        bool steps_ran = false;
        f.backend->onRead(drop_log, [&]
        {
            swept = sweepNamespace(*f.store, kMemoNs, BuildPrefix{.writer_epoch = 1, .build_sequence = 1000});
            build->abandon();
            if (delete_a_directly)
                deleteManifestBody(*f.backend, f.layout(), ManifestId{kMemoNs, a});
            steps_ran = true;
        });

        const uint64_t hits_before = counter(ProfileEvents::CASRefManifestBodyMemoHits);
        f.gc->runRegularRound();
        EXPECT_TRUE(steps_ran);
        EXPECT_EQ(f.backend->foldedKeys(), (std::set<String>{a_key, m_key})) << "the fold read A and M before the hook";
        EXPECT_EQ(counter(ProfileEvents::CASRefManifestBodyMemoHits) - hits_before, 1u) << "the drop folded A from the memo";
        EXPECT_GE(swept, 1u) << "the sweep ran over A's prefix";
        EXPECT_FALSE(headExists(*f.backend, orphan_key)) << "the sweep deleted A's never-owned sibling";
        EXPECT_FALSE(headExists(*f.backend, f.layout().manifestKey(debris))) << "the writer cleanup ran";
        EXPECT_TRUE(headExists(*f.backend, m_key)) << "the writer cleanup skips a precommitted body";
        return f.backend->guardedDeletes();
    };

    EXPECT_EQ(run(/*delete_a_directly*/ false), std::vector<String>{});
    EXPECT_EQ(run(/*delete_a_directly*/ true).size(), 1u) << "the guard catches a delete of a folded body";
}

/// One manifest folded under several owners in one round: an adoption (a precommit of the live body
/// under another ref), its promotion, a publication under a third ref, a repoint away and back, and a
/// drop. Every fold after the first is a hit and equals the no-memo fold.
TEST(CASGCFold, ManifestMemoHitsAcrossAdoptionAttachAndRepublication)
{
    const ManifestRef a = ref("", 1, 1);
    const ManifestRef b = ref("", 2, 1);
    const auto stage = [&](MemoFixture & f, size_t)
    {
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, a, {blobEntryFor("a", DB::UInt128(1))});
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, b, {blobEntryFor("b", DB::UInt128(2))});
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", std::nullopt, a);                  /// read A
        addPrecommitTransition(*f.backend, f.layout(), kMemoNs, DB::UInt128(5), "r2", std::nullopt, a);     /// adopt: hit
        promoteTransition(*f.backend, f.layout(), kMemoNs, DB::UInt128(5), "r2", a);                         /// no edge
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r3", std::nullopt, a);                  /// attach: hit
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", a, b);                             /// hit, read B
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", b, a);                             /// two hits
        dropRefTransition(*f.backend, f.layout(), kMemoNs, "r2", a);                                         /// hit
    };
    const MemoRun memo = runMemoScenario({}, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(memo.clamped, std::vector<bool>{false});
    EXPECT_EQ(oracle.body_gets, 8u);
    EXPECT_EQ(memo.body_gets, 2u) << "A and B, once each";
    EXPECT_EQ(memo.memo_hits, 6u);
}

/// Inline bytes are not kept by the memo and so not charged: a manifest with a 1 MiB inline entry fits
/// a 4 KiB memo and its drop is a hit.
TEST(CASGCFold, ManifestInlinePayloadIsNotCharged)
{
    const ManifestRef a = ref("", 1, 1);
    const auto stage = [&](MemoFixture & f, size_t)
    {
        ManifestEntry big;
        big.path = "big";
        big.placement = EntryPlacement::Inline;
        big.inline_bytes = String(1 << 20, 'x');
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, a, {blobEntryFor("a", DB::UInt128(1)), big});
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", std::nullopt, a);
        dropRefTransition(*f.backend, f.layout(), kMemoNs, "r1", a);
    };
    const MemoRun memo = runMemoScenario({.memo_budget = 4096}, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(memo.body_gets, 1u);
    EXPECT_EQ(memo.memo_hits, 1u);
}

/// A clamp leaves the rest of its log's hinted bodies untaken: they count as wasted read-ahead and in
/// neither body counter. The memoized E is not hinted, so the memo run wastes one read fewer.
TEST(CASGCFold, ManifestReadsUntakenAfterAClampCountInNeitherCounter)
{
    const ManifestRef c = ref("", 3, 1);
    const ManifestRef e = ref("", 5, 1);
    const ManifestRef g = ref("", 6, 1);
    const auto stage = [&](MemoFixture & f, size_t)
    {
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, e, {blobEntryFor("e", DB::UInt128(5))});
        writeManifestRaw(*f.backend, f.layout(), kMemoNs, g, {blobEntryFor("g", DB::UInt128(6))});
        publishCommittedTransition(*f.backend, f.layout(), kMemoNs, "r1", std::nullopt, e);
        /// C's body is absent, so the log clamps at its first edge.
        const uint64_t seq = appendRefLogSeed(*f.backend, f.layout(), kMemoNs,
            {ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r2", c}),
             ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, "r1", e}, std::nullopt),
             ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "r3", g})});
        advanceRecoverableCkptForRawFixture(*f.backend, f.layout(), kMemoNs, RefTxnId{1, seq});
    };
    const MemoRun memo = runMemoScenario({}, 1, stage);
    const MemoRun oracle = runMemoScenario({.memo = false}, 1, stage);

    expectSameOutcome(memo, oracle);
    EXPECT_EQ(memo.clamped, std::vector<bool>{true});
    EXPECT_EQ(oracle.body_gets, 1u) << "E's publication only";
    EXPECT_EQ(memo.body_gets, 1u);
    EXPECT_EQ(memo.memo_hits, 0u);
    EXPECT_GE(oracle.read_ahead_wasted, 2u) << "E's and G's untaken reads";
    EXPECT_EQ(memo.read_ahead_wasted + 1, oracle.read_ahead_wasted);
}

namespace
{
/// How the reduce phase meets one blob that a drop brought to a removal.
enum class HeadEntry
{
    Taken,      /// a candidate the merge HEADs
    Passed,     /// a candidate that keeps a prior edge, so the merge never HEADs it
    Unlisted,   /// HEADed by the merge but left out of the candidates
};

struct HeadCounts
{
    uint64_t hit = 0;
    uint64_t miss = 0;
    uint64_t wasted = 0;
    bool operator==(const HeadCounts &) const = default;
};

void PrintTo(const HeadCounts & c, std::ostream * os)
{
    *os << "{hit " << c.hit << ", miss " << c.miss << ", wasted " << c.wasted << "}";
}

/// The positional rule: `s` is the first candidate position not yet passed and the outstanding hints
/// are `[s, s + window)`. `entries` is one shard in `BlobRef` order.
void modelShard(const std::vector<HeadEntry> & entries, size_t window, HeadCounts & counts)
{
    const size_t n = std::count_if(entries.begin(), entries.end(), [](HeadEntry e) { return e != HeadEntry::Unlisted; });
    size_t s = 0;
    size_t pos = 0;   /// the lower bound of the next entry among the candidates
    for (const HeadEntry entry : entries)
    {
        if (entry == HeadEntry::Passed)
        {
            ++pos;
            continue;
        }
        const size_t outstanding_end = std::min(s + window, n);
        counts.wasted += std::min(pos, outstanding_end) - std::min(s, outstanding_end);
        if (entry == HeadEntry::Taken)
        {
            ++(pos < outstanding_end ? counts.hit : counts.miss);
            s = ++pos;
        }
        else
        {
            ++counts.miss;
            s = pos;
        }
    }
    counts.wasted += std::min(s + window, n) - std::min(s, n);
}

struct HeadWindowOptions
{
    uint64_t io_concurrency = 2;
    uint64_t gc_shards = 1;
    /// Manifest reads a clamped log leaves hinted and untaken in the round that folds the drop.
    size_t untaken_reads = 0;
};

struct HeadWindowRun
{
    std::map<String, String> objects;
    std::vector<String> head_events;
    std::vector<bool> clamped;
    HeadCounts counts;   /// read-ahead counter deltas of the reduce phases, where only HEADs are taken
};

BlobRef headRef(BlobHashAlgo algo, uint64_t high, uint64_t low)
{
    return BlobRef{algo, BlobDigest::fromU128((static_cast<DB::UInt128>(high) << 64) | low)};
}

void writeBlobAt(Backend & backend, const Layout & layout, const BlobRef & ref, uint64_t blob_header_len)
{
    EnvelopeHeader header;
    header.kind = ObjectKind::Blob;
    OperationForTest op(backend);
    (*op).create(layout.blobKey(ref), encodeEnvelopeHeader(header, static_cast<uint32_t>(blob_header_len)) + "x",
                 Retry::standard());
}

const RootNamespace kHeadNs{"00/aa@cas@"};
const RootNamespace kClampNs{"00/bb@cas@"};

/// Round 0 publishes `M` over every blob and `K` over the passed ones; round 1 drops `M`.
HeadWindowRun runHeadWindowScenario(const std::vector<std::pair<BlobRef, HeadEntry>> & blobs, const HeadWindowOptions & options)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto events = std::make_shared<SharedEventLog>();
    PoolPtr store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_root_id = "test",
        .gc_shards = options.gc_shards,
        .gc_fold_max_defer_rounds = 0,
        .gc_io_concurrency = options.io_concurrency,
        .event_sink = [log = events](CasEvent event) { log->push(std::move(event)); }});
    const Layout & layout = store->layout();
    std::set<BlobHashAlgo> algos;
    for (const auto & [blob, entry] : blobs)
        algos.insert(blob.algo);
    for (const BlobHashAlgo algo : algos)
        Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = fmt::format("admit-{}", blobHashAlgoName(algo)),
                                       .blob_hash_algo = algo, .blob_hash_allow_new = true});
    Gc gc(store, kGc);

    std::set<BlobRef> unlisted;
    for (const auto & [blob, entry] : blobs)
        if (entry == HeadEntry::Unlisted)
            unlisted.insert(blob);
    gc.setHeadCandidateFilterForTest([unlisted](const BlobRef & blob) { return unlisted.contains(blob); });

    HeadWindowRun run;
    std::array<uint64_t, 3> at_intake_end{};
    gc.setPhaseSink([&](const GcPhaseRecord & record)
    {
        const std::array<uint64_t, 3> now{counter(ProfileEvents::CASGCReadAheadHit), counter(ProfileEvents::CASGCReadAheadMiss),
                                          counter(ProfileEvents::CASGCReadAheadWasted)};
        if (record.phase == "fold_ref_intake")
            at_intake_end = now;
        else if (record.phase == "fold_reduce")
        {
            run.counts.hit += now[0] - at_intake_end[0];
            run.counts.miss += now[1] - at_intake_end[1];
            run.counts.wasted += now[2] - at_intake_end[2];
        }
    });

    const ManifestRef m = ref("", 1, 1);
    const ManifestRef k = ref("", 2, 1);
    std::vector<ManifestEntry> all;
    std::vector<ManifestEntry> kept;
    for (const auto & [blob, entry] : blobs)
    {
        writeBlobAt(*backend, layout, blob, store->poolMeta().blob_header_len);
        ManifestEntry e = blobEntryFor(fmt::format("f{}", all.size()), 0);
        e.ref = blob;
        all.push_back(e);
        if (entry == HeadEntry::Passed)
            kept.push_back(e);
    }
    writeManifestRaw(*backend, layout, kHeadNs, m, all);
    publishCommittedTransition(*backend, layout, kHeadNs, "m", std::nullopt, m);
    if (!kept.empty())
    {
        writeManifestRaw(*backend, layout, kHeadNs, k, kept);
        publishCommittedTransition(*backend, layout, kHeadNs, "k", std::nullopt, k);
    }
    if (options.untaken_reads > 0)
    {
        const ManifestRef e = ref("", 99, 1);
        writeManifestRaw(*backend, layout, kClampNs, e, {blobEntryFor("e", DB::UInt128(0xE))});
        publishCommittedTransition(*backend, layout, kClampNs, "e", std::nullopt, e);
    }
    run.clamped.push_back(!gc.runRegularRound().anomalies.empty());

    dropRefTransition(*backend, layout, kHeadNs, "m", m);
    if (options.untaken_reads > 0)
    {
        /// The first precommit's body is absent, so the log clamps before taking any of the others.
        std::vector<RefOp> ops{ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, "c", ref("", 100, 1)})};
        for (size_t i = 0; i < options.untaken_reads; ++i)
        {
            const ManifestRef g = ref("", 101 + i, 1);
            writeManifestRaw(*backend, layout, kClampNs, g, {blobEntryFor("g", DB::UInt128(0xC000 + i))});
            ops.push_back(ownerTransitionOp(std::nullopt, RefOwnerBinding{RefOwnerKind::Precommit, fmt::format("g{}", i), g}));
        }
        const uint64_t seq = appendRefLogSeed(*backend, layout, kClampNs, std::move(ops));
        advanceRecoverableCkptForRawFixture(*backend, layout, kClampNs, RefTxnId{1, seq});
    }
    run.clamped.push_back(!gc.runRegularRound().anomalies.empty());

    run.objects = objectsOf(*backend);
    for (const CasEvent & ev : events->snapshot())
        if (ev.type == CasEventType::IndegZero || ev.type == CasEventType::GcRetireObserve || ev.type == CasEventType::BlobRetire)
            run.head_events.push_back(fmt::format("{}|{}|{}|{}", static_cast<int>(ev.type), ev.object_hash, ev.token, ev.outcome));
    return run;
}

/// Runs the scenario at `options` and at concurrency 1: same store, same HEAD trail, same clamps, and
/// the read-ahead counters the positional model predicts.
void expectHeadWindow(const std::vector<std::pair<BlobRef, HeadEntry>> & blobs, const HeadWindowOptions & options)
{
    std::vector<std::pair<BlobRef, HeadEntry>> ordered = blobs;
    std::sort(ordered.begin(), ordered.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
    std::vector<std::vector<HeadEntry>> shards(options.gc_shards);
    for (const auto & [blob, entry] : ordered)
        shards[blobShard(blob, options.gc_shards)].push_back(entry);
    HeadCounts expected;
    for (const std::vector<HeadEntry> & shard : shards)
        modelShard(shard, 4 * options.io_concurrency, expected);

    const HeadWindowRun run = runHeadWindowScenario(blobs, options);
    HeadWindowOptions sequential = options;
    sequential.io_concurrency = 1;
    const HeadWindowRun oracle = runHeadWindowScenario(blobs, sequential);

    EXPECT_EQ(run.objects, oracle.objects);
    EXPECT_EQ(run.head_events, oracle.head_events);
    EXPECT_EQ(run.clamped, oracle.clamped);
    EXPECT_EQ(run.clamped.back(), options.untaken_reads > 0);
    EXPECT_EQ(run.counts, expected);
    EXPECT_EQ(oracle.counts.hit + oracle.counts.wasted, 0u);
    EXPECT_EQ(oracle.counts.miss, expected.hit + expected.miss) << "every take reads inline at concurrency 1";
}

std::vector<std::pair<BlobRef, HeadEntry>> headPattern(
    BlobHashAlgo algo, uint64_t high, const std::vector<std::pair<size_t, HeadEntry>> & runs)
{
    std::vector<std::pair<BlobRef, HeadEntry>> blobs;
    for (const auto & [count, entry] : runs)
        for (size_t i = 0; i < count; ++i)
            blobs.emplace_back(headRef(algo, high, blobs.size() + 1), entry);
    return blobs;
}
}

/// Single passed candidates, one after each take: each would otherwise keep its hint forever.
TEST(CASGCFold, HeadWindowSingleGapsDoNotPinIt)
{
    std::vector<std::pair<size_t, HeadEntry>> runs;
    for (size_t i = 0; i < 12; ++i)
    {
        runs.emplace_back(1, HeadEntry::Taken);
        runs.emplace_back(1, HeadEntry::Passed);
    }
    runs.emplace_back(4, HeadEntry::Taken);
    const auto blobs = headPattern(BlobHashAlgo::CityHash128, 0, runs);
    expectHeadWindow(blobs, {});
}

/// A gap shorter than the window, then a gap of more than three windows.
TEST(CASGCFold, HeadWindowLongGapsAreDiscarded)
{
    const auto blobs = headPattern(BlobHashAlgo::CityHash128, 0,
        {{3, HeadEntry::Taken}, {5, HeadEntry::Passed}, {6, HeadEntry::Taken}, {30, HeadEntry::Passed}, {10, HeadEntry::Taken}});
    expectHeadWindow(blobs, {});
}

/// `XXH3_128` sorts before `Sha256` as a `BlobRef` while its key string sorts after: the window
/// follows the merge's `BlobRef` order across the algorithm boundary.
TEST(CASGCFold, HeadWindowFollowsBlobRefOrderAcrossAlgorithms)
{
    ASSERT_LT(headRef(BlobHashAlgo::XXH3_128, 9, 9), headRef(BlobHashAlgo::Sha256, 0, 1));
    ASSERT_GT(Layout("p").blobKey(headRef(BlobHashAlgo::XXH3_128, 9, 9)), Layout("p").blobKey(headRef(BlobHashAlgo::Sha256, 0, 1)));
    auto blobs = headPattern(BlobHashAlgo::XXH3_128, 0, {{4, HeadEntry::Taken}, {9, HeadEntry::Passed}});
    for (auto & blob : headPattern(BlobHashAlgo::Sha256, 0, {{3, HeadEntry::Passed}, {8, HeadEntry::Taken}}))
        blobs.push_back(blob);
    expectHeadWindow(blobs, {});
}

/// Shard 0 holds the higher `BlobRef`s and ends in a gap; its outstanding hints are discarded before
/// shard 1 starts from its own lowest candidate.
TEST(CASGCFold, HeadWindowRestartsAtEachShard)
{
    constexpr size_t shard0_blobs = 22;
    std::vector<std::pair<BlobRef, HeadEntry>> blobs;
    for (size_t i = 0; i < shard0_blobs; ++i)
        blobs.emplace_back(headRef(BlobHashAlgo::CityHash128, 1000 + 2 * i, 1),
                           i < 18 && i % 2 == 0 ? HeadEntry::Taken : HeadEntry::Passed);
    for (size_t i = 0; i < 10; ++i)
        blobs.emplace_back(headRef(BlobHashAlgo::CityHash128, 1 + 2 * i, 1), HeadEntry::Taken);
    for (size_t i = 0; i < blobs.size(); ++i)
        ASSERT_EQ(blobShard(blobs[i].first, 2), i < shard0_blobs ? 0u : 1u);
    expectHeadWindow(blobs, {.gc_shards = 2});
}

/// A take nobody listed keeps its successor hintable, so the successor hits after a long gap.
TEST(CASGCFold, HeadWindowSuccessorOfAnUnlistedTakeHits)
{
    const auto blobs = headPattern(BlobHashAlgo::CityHash128, 0,
        {{2, HeadEntry::Taken}, {10, HeadEntry::Passed}, {1, HeadEntry::Unlisted}, {6, HeadEntry::Taken}});
    expectHeadWindow(blobs, {});
}

/// Reads a clamped log never takes do not count against the HEAD window.
TEST(CASGCFold, HeadWindowIgnoresUntakenReads)
{
    const auto blobs = headPattern(BlobHashAlgo::CityHash128, 0, {{12, HeadEntry::Taken}});
    expectHeadWindow(blobs, {.untaken_reads = 9});
}
