#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>

#include <atomic>
#include <functional>
#include <memory>

using namespace DB::Cas;

namespace
{

/// A writable pool on a hand-driven boot clock. Budget: envelope 100 ms, margin 100 ms, lease 30 s, so
/// conditional changes pause at 29.7 s after the confirmed renewal and the lease ends at 30 s.
struct ClockedPool
{
    std::shared_ptr<DB::Cas::tests::OrderedFaultBackend> backend = std::make_shared<DB::Cas::tests::OrderedFaultBackend>();
    std::shared_ptr<std::atomic<uint64_t>> boot = std::make_shared<std::atomic<uint64_t>>(100);
    PoolPtr store;

    /// A non-zero `sleep_advance_ms` makes every retry pause move the clock by that much.
    explicit ClockedPool(uint64_t sleep_advance_ms = 0)
    {
        PoolConfig config;
        config.pool_prefix = "p";
        config.server_root_id = "test";
        config.mount_lease_ttl_ms = std::chrono::milliseconds(30'000);
        config.cas_request_budget.attempt_timeout_ms = 100;
        config.cas_request_budget.lease_safety_margin_ms = 100;
        config.cas_request_budget.connect_timeout_cap_ms = 0;
        backend->setAttemptTimeoutMs(100);
        config.boot_ms_fn = [clock = boot] { return clock->load(); };
        if (sleep_advance_ms)
            config.retry_sleep_fn = [clock = boot, sleep_advance_ms](uint64_t) { clock->fetch_add(sleep_advance_ms); };
        store = Pool::open(backend, config);
    }

    uint64_t pausedAt() const { return 100 + store->leaseThresholds().conditionalPauseAfterMs(); }
    uint64_t expiredAt() const { return 100 + store->leaseThresholds().lease_ms; }
};

RefTxnId publishRef(const PoolPtr & store, const RootNamespace & ns, const String & ref, uint64_t ordinal)
{
    return store->appendRefOps(ns, MutationScope::ref(ref),
        [&](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(DB::Cas::tests::namespaceBirthOp());
            for (const RefOp & op : DB::Cas::tests::publishCommittedOps(ref, ManifestRef{1, ordinal, 1}))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);
}

template <typename F>
String refusalOf(F && fn)
{
    try
    {
        fn();
    }
    catch (const DB::Exception & e)
    {
        return e.message();
    }
    return {};
}

}

/// Paused window: the append is let in, the write loop's gate refuses before anything is sent, and the
/// client reads why.
TEST(CASLeaseRefusalPaths, APausedWindowAppendSaysItWasRefusedBeforeAnyRequestAndWhy)
{
    ClockedPool pool;
    ASSERT_NO_THROW(publishRef(pool.store, RootNamespace{"srv/paused"}, "r1", 1));
    pool.boot->store(pool.pausedAt());
    ASSERT_TRUE(pool.store->mayMutate());
    const String text = refusalOf([&] { publishRef(pool.store, RootNamespace{"srv/paused"}, "r2", 2); });
    EXPECT_NE(text.find("refused BEFORE any request was sent"), String::npos) << text;
    EXPECT_NE(text.find("this change is not admitted"), String::npos) << text;
}

/// An append whose attempt was sent and then lost the lease wedges the lane and still explains.
TEST(CASLeaseRefusalPaths, AWedgedAppendNamesTheLeaseRefusal)
{
    ClockedPool wedged(/*sleep_advance_ms=*/10'000);
    ASSERT_NO_THROW(publishRef(wedged.store, RootNamespace{"srv/wedged"}, "r1", 1));
    wedged.backend->armLatchedWriteFailure("_log/");
    wedged.boot->store(wedged.expiredAt() - 400);
    const String text = refusalOf([&] { publishRef(wedged.store, RootNamespace{"srv/wedged"}, "r2", 2); });
    EXPECT_NE(text.find("is UNCERTAIN"), String::npos) << text;
    EXPECT_NE(text.find("not admitted"), String::npos) << text;
}

/// A part manifest refused by the paused lease says so, not just "retry budget exhausted".
TEST(CASLeaseRefusalPaths, AStageManifestRefusedInThePausedWindowNamesTheLeaseRefusal)
{
    ClockedPool pool;
    PartWriteInfo info;
    info.intended_ref = "srv/stage/r1";
    auto build = pool.store->beginPartWrite(info);
    pool.boot->store(pool.pausedAt());
    ManifestEntry entry;
    entry.path = "data.bin";
    entry.placement = EntryPlacement::Inline;
    entry.inline_bytes = "x";
    const String text = refusalOf([&] { (void)build->stageManifest({entry}); });
    EXPECT_NE(text.find("UNCERTAIN (retry budget exhausted)"), String::npos) << text;
    EXPECT_NE(text.find("this change is not admitted"), String::npos) << text;
}
