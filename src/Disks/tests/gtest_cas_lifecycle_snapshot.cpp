#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>

/// Task 12 (rev.7 spec §7, [C5]-visibility): the NON-GATED lifecycle snapshot backing
/// `system.cas_mounts`. A Factory-class read (spec §1): I/O-free, no `store()`/`poolAccess`,
/// truthful in EVERY state — so a not-live / stopped / vanished / never-started disk stays VISIBLE to the
/// operator instead of silently missing from the table. These tests exercise the accessor directly (the
/// SQL-level assertions land in Task 14): `ContentAddressedMetadataStorage::lifecycleSnapshot` at the
/// storage level, and `Pool::lifecycleSnapshot` at the pool level (including the zero-backend-op proof).
/// Harness patterns follow gtest_cas_operation_gate.cpp / gtest_cas_forget.cpp.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB;
using DB::Cas::PoolLifecycle;
using DB::Cas::tests::CountingBackend;

namespace
{

const std::string kSrid = "test";

/// A live table dir + committed part reused by the storage-level tests (the shape
/// gtest_cas_operation_gate.cpp / gtest_cas_forget.cpp use).
const std::string kTableDir = "sn0/sn0sn0s0-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

std::shared_ptr<ContentAddressedMetadataStorage> openSnapshotStorage()
{
    auto settings = Cas::tests::makeSettingsForTest(
        kSrid, std::filesystem::temp_directory_path() / "ca_snapshot_scratch");
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

void commitOnePart(ContentAddressedMetadataStorage & storage)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(kTableDir + "/tmp_insert_all_1_1_0/data.bin", 65536, WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(kTableDir + "/tmp_insert_all_1_1_0", kPartDir);
    tx->commit(NoCommitOptions{});
}

}

/// (a) Live: the snapshot reads `live` with no reason and no `since`, and always carries the disk's
/// last-known identity (pool_id + server_root_id).
TEST(CASLifecycleSnapshot, LiveIsTruthfulWithIdentity)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);

    const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
    EXPECT_EQ(snap.lifecycle, "live");
    EXPECT_TRUE(snap.reason.empty()) << snap.reason;
    EXPECT_TRUE(snap.detail.empty()) << snap.detail;
    EXPECT_EQ(snap.since, 0) << "a live pool has no lifecycle `since`";
    EXPECT_EQ(snap.server_root_id, storage->serverRootId());
    EXPECT_FALSE(snap.pool_id.empty()) << "a started disk knows its pool identity";
    EXPECT_EQ(snap.pool_id, storage->getPoolUUID());
}

/// (c) VanishedForgotten via the REAL verb (`storage->forgetDisk()`): the snapshot reads `closed` with the
/// enum-clean `reason` word `forgotten` (so Task 14's `lifecycle || '(' || lifecycle_reason || ')'` reads
/// EXACTLY `closed(forgotten)`), the `detail` carrying the operator's decommission timestamp, `since`
/// set, and the identity still present.
TEST(CASLifecycleSnapshot, VanishedForgottenIsEnumCleanWithTimestampedDetail)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);
    const String pool_id_before = storage->getPoolUUID();

    storage->forgetDisk();

    const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
    EXPECT_EQ(snap.lifecycle, "closed");
    EXPECT_EQ(snap.reason, "forgotten");
    /// Task 14's teardown check depends on this exact concatenation.
    EXPECT_EQ(snap.lifecycle + "(" + snap.reason + ")", "closed(forgotten)");
    EXPECT_NE(snap.detail.find("SYSTEM CAS FORGET at "), std::string::npos) << snap.detail;
    EXPECT_NE(snap.detail.find("erasure was NOT verified"), std::string::npos) << snap.detail;
    EXPECT_NE(snap.since, 0);
    /// The disk stays registered and introspectable under its identity after FORGET.
    EXPECT_EQ(snap.pool_id, pool_id_before);
    EXPECT_EQ(snap.server_root_id, storage->serverRootId());
}

/// (d) A null pool never crashes the accessor and reports the storage-level lifecycle: `constructing`
/// before the first startup, `shutdown` after teardown. reason/since stay empty/0 (no terminal cause).
TEST(CASLifecycleSnapshot, NullPoolReportsConstructingThenShutdown)
{
    auto settings = Cas::tests::makeSettingsForTest(
        kSrid, std::filesystem::temp_directory_path() / "ca_snapshot_null_scratch");
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);

    /// Constructed but never started: no pool published.
    const CasLifecycleSnapshot before = storage->lifecycleSnapshot();
    EXPECT_EQ(before.lifecycle, "constructing");
    EXPECT_TRUE(before.reason.empty());
    EXPECT_TRUE(before.detail.empty());
    EXPECT_EQ(before.since, 0);
    EXPECT_TRUE(before.pool_id.empty()) << "no identity before startup";
    EXPECT_EQ(before.server_root_id, kSrid) << "the identity is known from config even pre-startup";

    storage->startup();
    ASSERT_EQ(storage->lifecycleSnapshot().lifecycle, "live");

    storage->shutdown();
    const CasLifecycleSnapshot after = storage->lifecycleSnapshot();
    EXPECT_EQ(after.lifecycle, "shutdown") << "a torn-down disk is distinguishable from a never-started one";
    EXPECT_FALSE(after.pool_id.empty()) << "the last-known identity survives shutdown";
}

/// (e) The accessor is I/O-free (spec §1 Factory class): NO backend op runs, in any lifecycle state. Proven
/// against a `CountingBackend` — the totals recorded after open do not move across snapshot reads, whether
/// the pool is Live or forced terminal.
TEST(CASLifecycleSnapshot, PerformsZeroBackendOps)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    const uint64_t head0 = backend->headTotal();
    const uint64_t get0 = backend->getTotal();
    const uint64_t put0 = backend->putTotal();
    const uint64_t getstream0 = backend->getStreamTotal();
    const uint64_t list0 = backend->listTotal();

    const auto assertNoIo = [&](const char * where)
    {
        EXPECT_EQ(backend->headTotal(), head0) << where;
        EXPECT_EQ(backend->getTotal(), get0) << where;
        EXPECT_EQ(backend->putTotal(), put0) << where;
        EXPECT_EQ(backend->getStreamTotal(), getstream0) << where;
        EXPECT_EQ(backend->listTotal(), list0) << where;
    };

    /// Live snapshot: zero I/O.
    (void)store->lifecycleSnapshot();
    assertNoIo("live snapshot must not touch the backend");

    /// Forced terminal snapshot (the very state the store()-class surface refuses): still zero I/O.
    store->setLifecycleForTest(PoolLifecycle::VanishedReplaced);
    const DB::Cas::Pool::LifecycleSnapshot vanished = store->lifecycleSnapshot();
    assertNoIo("a vanished-pool snapshot must not touch the backend");
    EXPECT_EQ(vanished.lifecycle, PoolLifecycle::VanishedReplaced);
}

/// An expired lease while the lifecycle stays `Live`: `paused` / `lease_expired`, `since` is the
/// deadline as wall time, and a deadline back in the future returns the row to `live`.
TEST(CASLifecycleSnapshot, ExpiredLeaseIsPausedUntilRestored)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);
    auto pool = storage->store();
    const auto wall_now_s = []
    {
        return static_cast<int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    };

    const int64_t before_s = wall_now_s();
    pool->setMountDeadline(pool->bootMsNow() - 5'000);
    const CasLifecycleSnapshot expired = storage->lifecycleSnapshot();
    const int64_t after_s = wall_now_s();

    EXPECT_EQ(expired.lifecycle, "paused");
    EXPECT_EQ(expired.reason, "lease_expired");
    EXPECT_GE(static_cast<int64_t>(expired.since), before_s - 6);
    EXPECT_LE(static_cast<int64_t>(expired.since), after_s - 4);
    EXPECT_EQ(pool->lifecycle(), PoolLifecycle::Live) << "the lifecycle enum is not touched";

    pool->setMountDeadline(pool->bootMsNow() + 60'000);
    const CasLifecycleSnapshot restored = storage->lifecycleSnapshot();
    EXPECT_EQ(restored.lifecycle, "live");
    EXPECT_TRUE(restored.reason.empty()) << restored.reason;
    EXPECT_EQ(restored.since, 0);
}

/// Lease left but not more than a conditional change's reserve: `paused` / `renewal_overdue`, `since` is
/// the instant the reserve was crossed.
TEST(CASLifecycleSnapshot, RenewalOverdueIsPausedBeforeTheLeaseEnds)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);
    auto pool = storage->store();
    const auto & budget = pool->poolConfig().cas_request_budget;
    const uint64_t reserve = 2 * budget.attemptEnvelopeMs() + budget.lease_safety_margin_ms;
    const int64_t before_s = ::time(nullptr);
    pool->setMountDeadline(pool->bootMsNow() + reserve - 1'000);
    const CasLifecycleSnapshot overdue = storage->lifecycleSnapshot();
    const int64_t after_s = ::time(nullptr);
    EXPECT_EQ(overdue.lifecycle, "paused");
    EXPECT_EQ(overdue.reason, "renewal_overdue");
    EXPECT_GE(static_cast<int64_t>(overdue.since), before_s - 2);
    EXPECT_LE(static_cast<int64_t>(overdue.since), after_s - 1);
    pool->setMountDeadline(pool->bootMsNow() + reserve + 60'000);
    EXPECT_EQ(storage->lifecycleSnapshot().lifecycle, "live");
}

/// Today's lifecycle values in the new vocabulary.
TEST(CASLifecycleSnapshot, OldLifecycleValuesMapToTheNewVocabulary)
{
    const auto read = [](PoolLifecycle lc)
    {
        auto storage = openSnapshotStorage();
        storage->store()->setLifecycleForTest(lc);
        const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
        return snap.lifecycle + "(" + snap.reason + ")";
    };
    EXPECT_EQ(read(PoolLifecycle::TransientNotLive), "paused(fenced_by_gc)");
    EXPECT_EQ(read(PoolLifecycle::IdentityLost), "identity_lost()");
    EXPECT_EQ(read(PoolLifecycle::VanishedReplaced), "vanished(replaced)");
}

/// The local row's body is the last confirmed one; reading it sends nothing while a renewal is stuck.
TEST(CASLifecycleSnapshot, ConfirmedBodyIsReadWithoutIoWhileARenewalIsStuck)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    const auto requests = [&] { return backend->getTotal() + backend->headTotal() + backend->putTotal() + backend->listTotal(); };
    const std::optional<DB::Cas::MountLease> first = store->confirmedMountBody();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->writer_epoch, store->writerEpoch());
    DB::Cas::tests::ManualBarrier in_put;
    backend->onBeforeWrite(store->layout().mountKey(kSrid), [&] { in_put.arriveAndWait(); });
    std::thread renewal([&] { store->renewWatermarkOnce(); });
    in_put.waitUntilArrived();
    const uint64_t before = requests();
    const std::optional<DB::Cas::MountLease> during = store->confirmedMountBody();
    (void)store->lifecycleSnapshot();
    (void)store->renewalProgress();
    EXPECT_EQ(requests(), before);
    ASSERT_TRUE(during.has_value());
    EXPECT_EQ(during->seq, first->seq) << "a renewal in flight is not confirmed yet";
    in_put.release();
    renewal.join();
    backend->onBeforeWrite(store->layout().mountKey(kSrid), {});
    EXPECT_EQ(store->confirmedMountBody()->seq, first->seq + 1);
}
