#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcManifestMemo.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const Layout kLayout("p");

ManifestId idOf(const String & ns, uint64_t seq)
{
    return ManifestId{RootNamespace{ns}, ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = 1}};
}

/// A real incarnation of the manifest key: `Etag` is minted only by an admitted request.
Etag etagOf(InMemoryBackend & backend, const ManifestId & id)
{
    const String key = kLayout.manifestKey(id);
    OperationForTest op(backend);
    if (auto got = (*op).read(key, Retry::standard()))
        return got->etag;
    (*op).create(key, "x", Retry::standard());
    return (*op).read(key, Retry::standard())->etag;
}

ManifestFold foldOf(InMemoryBackend & backend, const ManifestId & id, size_t entries, size_t path_len)
{
    ManifestFold fold{.etag = etagOf(backend, id), .entries = {}};
    fold.entries.reserve(entries);
    for (size_t i = 0; i < entries; ++i)
    {
        String path(path_len, 'p');
        path.replace(0, std::min(path_len, size_t{8}), fmt::format("{:08}", i).substr(0, std::min(path_len, size_t{8})));
        fold.entries.push_back(ManifestFoldEntry{
            .ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(DB::UInt128(i + 1))},
            .source_id = sourceEdgeId(id, path),
            .path = std::move(path)});
    }
    return fold;
}

}

TEST(CASGCManifestMemo, InsertThenFindHits)
{
    InMemoryBackend backend;
    GcManifestMemo memo;
    const ManifestId id = idOf("00/aa@cas@", 1);
    const ManifestFold fold = foldOf(backend, id, 2, 10);
    ASSERT_TRUE(memo.insert(id, fold));

    const ManifestFold * hit = memo.find(id);
    ASSERT_NE(hit, nullptr);
    EXPECT_EQ(hit->etag, fold.etag);
    ASSERT_EQ(hit->entries.size(), 2u);
    EXPECT_EQ(hit->entries[1].path, fold.entries[1].path);
    EXPECT_EQ(hit->entries[1].source_id, fold.entries[1].source_id);
    EXPECT_EQ(memo.hits(), 1u);
    EXPECT_GT(memo.charged(), 0u);
}

TEST(CASGCManifestMemo, MissOnOtherRefOrNamespace)
{
    InMemoryBackend backend;
    GcManifestMemo memo;
    const ManifestId id = idOf("00/aa@cas@", 1);
    ASSERT_TRUE(memo.insert(id, foldOf(backend, id, 1, 10)));

    EXPECT_EQ(memo.find(idOf("00/aa@cas@", 2)), nullptr);
    EXPECT_EQ(memo.find(idOf("00/bb@cas@", 1)), nullptr);
    EXPECT_FALSE(memo.contains(idOf("00/bb@cas@", 1)));
    EXPECT_TRUE(memo.contains(id));
    EXPECT_EQ(memo.hits(), 0u) << "neither a miss nor `contains` counts as a hit";
}

TEST(CASGCManifestMemo, EvictsOldestFirstPastTheBudget)
{
    InMemoryBackend backend;
    const ManifestId a = idOf("00/aa@cas@", 1);
    const ManifestId b = idOf("00/aa@cas@", 2);
    const ManifestId c = idOf("00/aa@cas@", 3);

    size_t two = 0;
    {
        GcManifestMemo probe;
        probe.insert(a, foldOf(backend, a, 3, 40));
        probe.insert(b, foldOf(backend, b, 3, 40));
        two = probe.charged();
    }

    GcManifestMemo memo(two);
    ASSERT_TRUE(memo.insert(a, foldOf(backend, a, 3, 40)));
    ASSERT_TRUE(memo.insert(b, foldOf(backend, b, 3, 40)));
    EXPECT_EQ(memo.evictions(), 0u);
    ASSERT_TRUE(memo.insert(c, foldOf(backend, c, 3, 40)));
    EXPECT_EQ(memo.evictions(), 1u);
    EXPECT_FALSE(memo.contains(a)) << "the oldest insert goes first";
    EXPECT_TRUE(memo.contains(b));
    EXPECT_TRUE(memo.contains(c));
    EXPECT_LE(memo.charged(), two);
}

TEST(CASGCManifestMemo, EntryLargerThanTheBudgetIsRefused)
{
    InMemoryBackend backend;
    const ManifestId small = idOf("00/aa@cas@", 1);
    const ManifestId big = idOf("00/aa@cas@", 2);
    GcManifestMemo memo(4096);
    ASSERT_TRUE(memo.insert(small, foldOf(backend, small, 1, 10)));
    const size_t before = memo.charged();

    EXPECT_FALSE(memo.insert(big, foldOf(backend, big, 64, 100)));
    EXPECT_EQ(memo.charged(), before);
    EXPECT_FALSE(memo.contains(big));
    EXPECT_TRUE(memo.contains(small)) << "a refused insert evicts nothing";
    EXPECT_EQ(memo.evictions(), 0u);
}

TEST(CASGCManifestMemo, EmptyManifestsAreChargedAndEvict)
{
    InMemoryBackend backend;
    const size_t budget = 64 * 1024;
    GcManifestMemo memo(budget);
    /// Enough inserts to charge twice the budget at 256 B each; a fixed count keeps a broken bound finite.
    const uint64_t inserts = 2 * budget / 256;
    size_t inserted_before_eviction = 0;
    for (uint64_t seq = 1; seq <= inserts; ++seq)
    {
        const ManifestId id = idOf("00/aa@cas@", seq);
        const size_t before = memo.charged();
        ASSERT_TRUE(memo.insert(id, ManifestFold{.etag = etagOf(backend, id), .entries = {}}));
        ASSERT_LE(memo.charged(), budget) << "after insert " << seq;
        if (memo.evictions() == 0)
        {
            EXPECT_GE(memo.charged() - before, 256u) << "an empty manifest still costs its node, key and etag";
            ++inserted_before_eviction;
        }
    }
    EXPECT_GT(memo.evictions(), 0u) << "empty manifests filled the budget";
    EXPECT_LE(inserted_before_eviction * 256, budget);
}

TEST(CASGCManifestMemo, SharedNamespaceIsChargedOnce)
{
    InMemoryBackend backend;
    const String ns(kMaxNamespaceBytes, 'n');
    GcManifestMemo memo;

    const ManifestId first = idOf(ns, 1);
    ASSERT_TRUE(memo.insert(first, ManifestFold{.etag = etagOf(backend, first), .entries = {}}));
    /// Bucket arrays grow in steps; they are measured apart from what each manifest pays.
    const auto held = [&memo] { return memo.charged() - memo.bucketBytes(); };
    const size_t after_first = held();
    const ManifestId second = idOf(ns, 2);
    ASSERT_TRUE(memo.insert(second, ManifestFold{.etag = etagOf(backend, second), .entries = {}}));
    const size_t per_manifest = held() - after_first;
    EXPECT_GE(after_first - per_manifest, kMaxNamespaceBytes) << "the first insert paid for the namespace";

    for (uint64_t seq = 3; seq <= 1000; ++seq)
    {
        const ManifestId id = idOf(ns, seq);
        ASSERT_TRUE(memo.insert(id, ManifestFold{.etag = etagOf(backend, id), .entries = {}}));
    }
    EXPECT_EQ(held(), after_first + 999 * per_manifest)
        << "each further manifest pays its node and etag (whose key embeds the namespace), never the interned namespace again";
}

TEST(CASGCManifestMemo, LongPathsAreChargedByCapacity)
{
    InMemoryBackend backend;
    const ManifestId short_id = idOf("00/aa@cas@", 1);
    const ManifestId long_id = idOf("00/aa@cas@", 2);
    const ManifestFold short_fold = foldOf(backend, short_id, 4, 8);
    const ManifestFold long_fold = foldOf(backend, long_id, 4, 4000);

    size_t long_path_capacity = 0;
    size_t short_path_capacity = 0;
    for (size_t i = 0; i < 4; ++i)
    {
        long_path_capacity += long_fold.entries[i].path.capacity();
        short_path_capacity += short_fold.entries[i].path.capacity();
    }

    GcManifestMemo short_memo;
    GcManifestMemo long_memo;
    ASSERT_TRUE(short_memo.insert(short_id, short_fold));
    ASSERT_TRUE(long_memo.insert(long_id, long_fold));
    EXPECT_EQ(long_memo.charged() - short_memo.charged(), long_path_capacity - short_path_capacity);
}

namespace
{

/// Fills a default-budget memo past the budget with manifests of `entries` paths of `path_bytes`
/// each, spread over 512-byte namespaces, and returns the thread tracker's net allocation and the
/// memo's charge at the end. The tracker sees the allocator's size classes, which is the figure the
/// budget exists to cap.
std::pair<Int64, size_t> allocationAndCharge(size_t entries, size_t path_bytes)
{
    DB::MainThreadStatus::getInstance();
    InMemoryBackend backend;

    constexpr size_t kNamespaces = 16;
    std::vector<String> namespaces;
    std::vector<ManifestFold> templates;
    for (size_t n = 0; n < kNamespaces; ++n)
    {
        namespaces.push_back(String(kMaxNamespaceBytes - 2, 'n') + fmt::format("{:02}", n));
        templates.push_back(foldOf(backend, idOf(namespaces.back(), 1), entries, path_bytes));
    }

    auto & tracker = DB::CurrentThread::get().memory_tracker;
    DB::CurrentThread::flushUntrackedMemory();
    const Int64 before = tracker.get();
    GcManifestMemo memo;
    /// Every manifest is charged at least 256 B, so this fixed count charges at least twice the budget.
    /// The tracker, not the charge under test, stops the fill if the bound is broken.
    constexpr uint64_t kInserts = 2 * GcManifestMemo::kBudgetBytes / 256;
    for (uint64_t seq = 1; seq <= kInserts; ++seq)
    {
        const size_t n = seq % kNamespaces;
        /// A copy allocates what a decode hands the memo: a fresh etag key, entry vector and paths.
        ManifestFold fold = templates[n];
        EXPECT_TRUE(memo.insert(idOf(String(namespaces[n]), seq), std::move(fold)));
        if (seq % 1024 != 0)
            continue;
        DB::CurrentThread::flushUntrackedMemory();
        if (tracker.get() - before > static_cast<Int64>(2 * GcManifestMemo::kBudgetBytes))
        {
            ADD_FAILURE() << "allocation " << tracker.get() - before << " passed twice the budget after insert " << seq;
            break;
        }
        if (memo.charged() > GcManifestMemo::kBudgetBytes)
        {
            ADD_FAILURE() << "charge " << memo.charged() << " passed the budget after insert " << seq;
            break;
        }
    }
    EXPECT_GT(memo.evictions(), 0u) << "the fill reached the budget";
    DB::CurrentThread::flushUntrackedMemory();
    const Int64 allocation = tracker.get() - before;
    std::cerr << "memo with " << entries << " entries of " << path_bytes << " B: allocation " << allocation
              << " B, charge " << memo.charged() << " B, ratio "
              << static_cast<double>(allocation) / static_cast<double>(memo.charged()) << '\n';
    return {allocation, memo.charged()};
}

}

TEST(CASGCManifestMemo, AllocationStaysWithinTheCharge)
{
    for (const auto [entries, path_bytes] : {std::pair<size_t, size_t>{0, 0}, {4, 200}})
    {
        SCOPED_TRACE(fmt::format("{} entries of {} B", entries, path_bytes));
        const auto [allocation, charged] = allocationAndCharge(entries, path_bytes);
        EXPECT_LE(charged, GcManifestMemo::kBudgetBytes);
        EXPECT_GE(charged, GcManifestMemo::kBudgetBytes - (64u << 10)) << "the fill reached the budget";
#if !defined(SANITIZER)
        EXPECT_GT(allocation, 0) << "the thread tracker saw the inserts";
        EXPECT_LE(static_cast<double>(allocation), 1.25 * static_cast<double>(charged));
#else
        (void)allocation;
#endif
    }
#if defined(SANITIZER)
    /// The sanitizer runtime provides `operator new`, so the thread tracker is never fed; only the charge bound is checked.
    GTEST_SKIP() << "allocation is not measurable: the thread memory tracker is not fed in sanitizer builds";
#endif
}

namespace
{

/// Charge of a fresh default-budget memo holding copies of `folds`, inserted in order under `ns`,
/// without the bucket arrays, which depend on how many manifests the memo held before.
size_t chargeOf(const String & ns, std::initializer_list<const ManifestFold *> folds)
{
    GcManifestMemo probe;
    uint64_t seq = 0;
    for (const ManifestFold * fold : folds)
        EXPECT_TRUE(probe.insert(idOf(ns, ++seq), *fold));
    return probe.charged() - probe.bucketBytes();
}

}

/// Many small manifests grow the hash tables, then a manifest sized from the live memo to fit the
/// budget only alone arrives, then a small one that cannot fit beside it. A hash table keeps its
/// bucket array after eviction, so the charge must keep covering it whatever the memo decides to store.
TEST(CASGCManifestMemo, EvictionKeepsChargingRetainedCapacity)
{
    DB::MainThreadStatus::getInstance();
    InMemoryBackend backend;
    constexpr size_t kBudget = 1u << 20;
    const String ns = "00/aa@cas@";
    const ManifestFold small = foldOf(backend, idOf(ns, 1), 0, 0);
    constexpr size_t kPathBytes = 1000;
    ManifestFold large = foldOf(backend, idOf(ns, 2), kBudget * 3 / 4 / (sizeof(ManifestFoldEntry) + kPathBytes), kPathBytes);

    /// Every manifest is charged at least 256 B, so this fixed count charges at least twice the budget.
    const uint64_t small_inserts = 2 * kBudget / 256;
    const auto fillSmall = [&](GcManifestMemo & memo)
    {
        for (uint64_t seq = 1; seq <= small_inserts; ++seq)
        {
            /// A copy allocates what a decode hands the memo.
            ManifestFold copy = small;
            ASSERT_TRUE(memo.insert(idOf(ns, seq), std::move(copy)));
            ASSERT_LE(memo.charged(), kBudget) << "after insert " << seq;
        }
    };

    /// The bucket arrays a filled memo holds, read from a scratch one so that the large manifest
    /// exists before the allocation is measured. The measured memo must end up with the same arrays.
    size_t live_buckets = 0;
    {
        GcManifestMemo scratch(kBudget);
        fillSmall(scratch);
        ASSERT_GT(scratch.evictions(), 0u) << "the small manifests filled the budget";
        live_buckets = scratch.bucketBytes();
    }
    ASSERT_GT(live_buckets, 0u);

    /// Grow one path so that the large manifest, its namespace and the live bucket arrays leave half
    /// a small manifest of the budget free: it fits alone, and a small one beside it does not.
    const size_t small_charge = chargeOf(ns, {&small, &small}) - chargeOf(ns, {&small});
    const size_t grow = kBudget - live_buckets - chargeOf(ns, {&large}) - small_charge / 2;
    large.entries.back().path.append(grow, 'q');
    const size_t large_alone = chargeOf(ns, {&large}) + live_buckets;
    ASSERT_LE(large_alone, kBudget);
    ASSERT_GT(large_alone + small_charge, kBudget);

    auto & tracker = DB::CurrentThread::get().memory_tracker;
    DB::CurrentThread::flushUntrackedMemory();
    const Int64 before = tracker.get();
    {
        GcManifestMemo memo(kBudget);
        fillSmall(memo);
        ASSERT_EQ(memo.bucketBytes(), live_buckets);

        const ManifestId large_id = idOf(ns, small_inserts + 1);
        ManifestFold copy = large;
        ASSERT_TRUE(memo.insert(large_id, std::move(copy))) << "the large manifest fits beside the bucket arrays";
        EXPECT_LE(memo.charged(), kBudget);
        EXPECT_TRUE(memo.contains(large_id));
        for (uint64_t seq = 1; seq <= small_inserts; ++seq)
            ASSERT_FALSE(memo.contains(idOf(ns, seq))) << "small manifest " << seq << " survived the large insert";

        const ManifestId last_small_id = idOf(ns, small_inserts + 2);
        ManifestFold last_small = small;
        ASSERT_TRUE(memo.insert(last_small_id, std::move(last_small)));
        EXPECT_LE(memo.charged(), kBudget);
        EXPECT_FALSE(memo.contains(large_id)) << "the small manifest does not fit beside the large one";
        EXPECT_TRUE(memo.contains(last_small_id));

        DB::CurrentThread::flushUntrackedMemory();
        const Int64 allocation = tracker.get() - before;
        std::cerr << "retained allocation " << allocation << " B, charge " << memo.charged() << " B\n";
#if !defined(SANITIZER)
        EXPECT_GT(allocation, 0) << "the thread tracker saw the inserts";
        EXPECT_LE(static_cast<double>(allocation), 1.25 * static_cast<double>(memo.charged()));
#else
        (void)allocation;
#endif
    }
#if defined(SANITIZER)
    /// The sanitizer runtime provides `operator new`, so the thread tracker is never fed; only the charge bound is checked.
    GTEST_SKIP() << "allocation is not measurable: the thread memory tracker is not fed in sanitizer builds";
#endif
}
