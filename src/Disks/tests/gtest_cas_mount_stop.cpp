#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_process_stop_log.h>
#include <Disks/tests/cas_test_helpers.h>

#include <functional>
#include <limits>
#include <tuple>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using DB::Cas::tests::OperationForTest;

namespace
{

void rewriteSlot(Backend & backend, const String & key, const std::function<void(MountLease &)> & edit)
{
    OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    edit(m);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(key, encodeMountLease(m), got->etag, Retry::once())));
}

void putSlotBytes(Backend & backend, const String & key, const String & bytes)
{
    OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    ASSERT_TRUE(got.has_value());
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(key, bytes, got->etag, Retry::once())));
}

void removeSlot(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    ASSERT_TRUE(got.has_value());
    ASSERT_EQ((*op).remove(key, got->etag, Retry::once()), Removal::Removed);
}

/// The transform `computeHeartbeatFloor` writes when it fences a mount out.
void fenceOutMount(Backend & backend, const String & key)
{
    rewriteSlot(backend, key, [](MountLease & m) { m.gc_fenced = true; ++m.seq; });
}

void makeSlotForeign(Backend & backend, const String & key)
{
    rewriteSlot(backend, key, [](MountLease & m) { m.server_uuid = DB::UInt128{0xF0F0}; });
}

String stateWord(const Pool & store)
{
    return String(store.mayMutate() ? "open/" : "closed/") + String(mountEndWord(store.mountEnd()));
}

PoolPtr openPool(const std::shared_ptr<InMemoryBackend> & backend, const String & srid,
                 MountLossReaction reaction = MountLossReaction::StopServer)
{
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = srid, .mount_loss_reaction = reaction});
}

}

TEST(CASMountStop, EachSlotLossLatchesThenStopsThenLogs)
{
    using Damage = std::function<void(Backend &, const String &)>;
    const auto edit = [](std::function<void(MountLease &)> f) -> Damage
    {
        return [f](Backend & b, const String & k) { rewriteSlot(b, k, f); };
    };
    const std::vector<std::tuple<const char *, Damage, const char *>> cases = {
        {"foreign uuid", edit([](MountLease & m) { m.server_uuid = DB::UInt128{0xF0F0}; }), "slot_taken"},
        {"newer epoch of ours", edit([](MountLease & m) { ++m.writer_epoch; m.write_attempt_id = DB::UInt128{77}; }), "slot_taken"},
        {"our pair, a body we never wrote", edit([](MountLease & m) { m.hostname = "elsewhere"; }), "slot_corrupt"},
        {"undecodable", [](Backend & b, const String & k) { putSlotBytes(b, k, "not a mount lease"); }, "slot_corrupt"},
        {"absent", [](Backend & b, const String & k) { removeSlot(b, k); }, "slot_absent"},
    };
    for (const auto & [name, damage, word] : cases)
    {
        SCOPED_TRACE(name);
        auto backend = std::make_shared<InMemoryBackend>();
        auto store = DB::Cas::tests::openPoolForTest(backend);
        auto & log = DB::Cas::tests::processStopLog();
        log.reset([&] { return stateWord(*store); });
        DB::Cas::tests::ScopedStopLineCapture lines;
        const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
        damage(*backend, store->layout().mountKey("test"));

        EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);

        const auto events = log.events();
        ASSERT_EQ(events.size(), 2u);
        EXPECT_EQ(events[0], "graceful:closed/" + String(word)) << "latched before the stop request";
        EXPECT_TRUE(events[1].starts_with("error-line:CAS disk ''") && events[1].contains("server stop requested")) << events[1];
        EXPECT_EQ(store->lifecycleSnapshot().lifecycle, PoolLifecycle::Stopping);
        EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before) << "a loss never remounts";
    }
}

TEST(CASMountStop, OurOwnFenceStillRemountsAndNothingMore)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    fenceOutMount(*backend, store->layout().mountKey("test"));

    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);

    EXPECT_EQ(store->mountEnd(), MountEnd::None);
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_TRUE(log.events().empty());
    EXPECT_TRUE(store->tryRemountOnce());
}

TEST(CASMountStop, ALatchedMountNeverOpensAgain)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    DB::Cas::tests::processStopLog().reset();

    ASSERT_TRUE(store->latchMountEndForTest(MountEnd::SlotTaken));
    store->armMountFence(DB::UInt128{}, store->liveWriterEpoch(), std::numeric_limits<uint64_t>::max());

    EXPECT_FALSE(store->mayMutate());
    EXPECT_FALSE(store->latchMountEndForTest(MountEnd::SlotAbsent));
    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
}

TEST(CASMountStop, TwoDisksAskForOneStop)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto a = openPool(backend, "a");
    auto b = openPool(backend, "b");
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    makeSlotForeign(*backend, a->layout().mountKey("a"));
    makeSlotForeign(*backend, b->layout().mountKey("b"));

    EXPECT_THROW(a->renewWatermarkOnce(), DB::Exception);
    EXPECT_THROW(b->renewWatermarkOnce(), DB::Exception);

    EXPECT_EQ(log.graceful(), 1u);
    EXPECT_EQ(a->lifecycle(), PoolLifecycle::Stopping);
    EXPECT_EQ(b->lifecycle(), PoolLifecycle::Stopping);
}

TEST(CASMountStop, ALossBetweenForgetIntentAndClosedStops)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    store->publishVanishedIntentForTest();
    makeSlotForeign(*backend, store->layout().mountKey("test"));

    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    EXPECT_EQ(log.graceful(), 1u);

    store->forgetDisk({}, "r");
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Stopping);
    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
}

TEST(CASMountStop, ForgetClosesWithoutAStop)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();

    store->forgetDisk({}, "r");

    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_TRUE(log.events().empty());
    EXPECT_FALSE(store->latchMountEndForTest(MountEnd::SlotTaken));
    EXPECT_EQ(store->mountEnd(), MountEnd::Forgotten);
}

TEST(CASMountStop, AStoppingDiskRefusesWritesWithoutARequest)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    DB::Cas::tests::processStopLog().reset();
    makeSlotForeign(*backend, store->layout().mountKey("test"));
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Stopping);

    const auto requests = [&]
    {
        return backend->getTotal() + backend->headTotal() + backend->listTotal() + backend->writeTotal()
            + backend->deleteTotal() + backend->publishTotal();
    };
    const uint64_t before = requests();
    try
    {
        store->dropRef(RootNamespace{"srv1/tbl"}, "r");
        ADD_FAILURE() << "a stopping disk must refuse a write";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR) << e.message();
    }
    EXPECT_EQ(requests(), before);
}

TEST(CASMountStop, ACloseOnlyMountClosesWithoutAStop)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend, "test", MountLossReaction::CloseMount);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    {
        DB::Cas::tests::ScopedStopLineCapture lines;
        makeSlotForeign(*backend, store->layout().mountKey("test"));
        EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    }

    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Stopping);
    EXPECT_EQ(log.graceful(), 0u);
    const auto events = log.events();
    ASSERT_EQ(events.size(), 1u);
    EXPECT_TRUE(events[0].starts_with("error-line:") && events[0].ends_with("this disk stays closed until restart.")) << events[0];
}
