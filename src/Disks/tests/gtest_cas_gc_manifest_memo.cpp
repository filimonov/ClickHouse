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
    size_t inserted = 0;
    for (uint64_t seq = 1; memo.evictions() == 0; ++seq)
    {
        const ManifestId id = idOf("00/aa@cas@", seq);
        const size_t before = memo.charged();
        ASSERT_TRUE(memo.insert(id, ManifestFold{.etag = etagOf(backend, id), .entries = {}}));
        if (memo.evictions() == 0)
        {
            EXPECT_GE(memo.charged() - before, 256u) << "an empty manifest still costs its node, key and etag";
            ++inserted;
        }
        ASSERT_LT(seq, 10000u) << "empty manifests never filled the budget";
    }
    EXPECT_LE(memo.charged(), budget);
    EXPECT_LE(inserted * 256, budget);
}

TEST(CASGCManifestMemo, SharedNamespaceIsChargedOnce)
{
    InMemoryBackend backend;
    const String ns(kMaxNamespaceBytes, 'n');
    GcManifestMemo memo;

    const ManifestId first = idOf(ns, 1);
    ASSERT_TRUE(memo.insert(first, ManifestFold{.etag = etagOf(backend, first), .entries = {}}));
    const size_t after_first = memo.charged();
    const ManifestId second = idOf(ns, 2);
    ASSERT_TRUE(memo.insert(second, ManifestFold{.etag = etagOf(backend, second), .entries = {}}));
    const size_t per_manifest = memo.charged() - after_first;
    EXPECT_GE(after_first - per_manifest, kMaxNamespaceBytes) << "the first insert paid for the namespace";

    for (uint64_t seq = 3; seq <= 1000; ++seq)
    {
        const ManifestId id = idOf(ns, seq);
        ASSERT_TRUE(memo.insert(id, ManifestFold{.etag = etagOf(backend, id), .entries = {}}));
    }
    EXPECT_EQ(memo.charged(), after_first + 999 * per_manifest)
        << "each further manifest pays only its own node and etag, never the namespace again";
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
    /// The charge cap stops the fill when eviction does not.
    for (uint64_t seq = 1; memo.evictions() < 1000 && memo.charged() <= 2 * GcManifestMemo::kBudgetBytes; ++seq)
    {
        const size_t n = seq % kNamespaces;
        /// A copy allocates what a decode hands the memo: a fresh etag key, entry vector and paths.
        ManifestFold fold = templates[n];
        EXPECT_TRUE(memo.insert(idOf(String(namespaces[n]), seq), std::move(fold)));
    }
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
        EXPECT_GT(allocation, 0) << "the thread tracker saw the inserts";
        EXPECT_LE(static_cast<double>(allocation), 1.25 * static_cast<double>(charged));
    }
}
