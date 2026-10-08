#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_process_stop_log.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Poco/Exception.h>

#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <tuple>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
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

/// The renewer is still `Active` after a ledger fault, and FORGET drains for real: only the loss end
/// keeps the teardown from writing a farewell under an identity another process may hold.
TEST(CASMountStop, ForgetAfterALossWritesNoFarewell)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    DB::Cas::tests::processStopLog().reset();
    const String key = store->layout().mountKey("test");
    ASSERT_TRUE(store->latchMountEndForTest(MountEnd::LedgerFault));
    const uint64_t slot_writes_before = backend->putOverwriteCount(key);

    store->forgetDisk({}, "r");

    EXPECT_EQ(backend->putOverwriteCount(key), slot_writes_before);
    EXPECT_EQ(store->mountEnd(), MountEnd::LedgerFault);
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

namespace
{

/// While armed, a guarded write of a mount slot fails as a defect of this machine would.
class LocalDefectBackend : public InMemoryBackend
{
public:
    std::atomic<bool> armed{false};

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        if (armed.load() && expected_value && key.ends_with("/mount"))
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "injected local defect in the mount write");
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

}

/// The line is written before the end: the process does not outlive `terminateProcess`.
TEST(CASMountStop, AnInjectedLocalFailureEndsTheProcess)
{
    auto backend = std::make_shared<LocalDefectBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset([&] { return stateWord(*store); });
    DB::Cas::tests::ScopedStopLineCapture lines;
    backend->armed = true;

    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);

    const auto events = log.events();
    ASSERT_EQ(events.size(), 2u);
    EXPECT_TRUE(events[0].starts_with("fatal-line:CAS disk ''") && events[0].contains("without a shutdown")) << events[0];
    EXPECT_EQ(events[1], "fatal:closed/local_defect");
    EXPECT_EQ(log.graceful(), 0u);
}

namespace
{

/// R8: the GC fenced our own body, the renewal ends, and the self-remount is due.
void fenceOutAndFailTheRenewal(Pool & store, Backend & backend)
{
    fenceOutMount(backend, store.layout().mountKey("test"));
    EXPECT_THROW(store.renewWatermarkOnce(), DB::Exception);
    ASSERT_EQ(store.mountEnd(), MountEnd::None);
}

/// While armed, every read of a mount slot times out.
class MountReadOutageBackend : public InMemoryBackend
{
public:
    std::atomic<bool> outage{false};

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (outage.load() && key.ends_with("/mount"))
            throw Poco::TimeoutException("injected mount slot read timeout");
        return InMemoryBackend::read(key, access);
    }
};

}

TEST(CASMountStop, ARemountThatFindsATwinStops)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    fenceOutAndFailTheRenewal(*store, *backend);
    rewriteSlot(*backend, store->layout().mountKey("test"), [](MountLease & m)
    {
        m.writer_epoch += 5;
        m.gc_fenced = false;
        m.write_attempt_id = DB::UInt128{55};
    });

    EXPECT_FALSE(store->tryRemountOnce());

    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Stopping);
    EXPECT_EQ(log.graceful(), 1u);
}

TEST(CASMountStop, ARemountThatFindsAForeignServerStops)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    fenceOutAndFailTheRenewal(*store, *backend);
    rewriteSlot(*backend, store->layout().mountKey("test"), [](MountLease & m)
    {
        m.server_uuid = DB::UInt128{0xF0F0};
        m.gc_fenced = false;
    });

    EXPECT_FALSE(store->tryRemountOnce());

    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
    EXPECT_EQ(log.graceful(), 1u);
}

/// Another process keeps renewing a body of the pair the GC fenced: the remount's observation never
/// settles, and what it watched is not ours.
TEST(CASMountStop, ARemountThatFindsAnUnknownBodyOfTheFencedPairStops)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    fenceOutAndFailTheRenewal(*store, *backend);
    const String key = store->layout().mountKey("test");
    rewriteSlot(*backend, key, [](MountLease & m)
    {
        m.gc_fenced = false;
        m.hostname = "elsewhere";
    });
    store->setWaitSleepForTest([&](uint64_t)
    {
        rewriteSlot(*backend, key, [](MountLease & m)
        {
            ++m.seq;
            m.write_attempt_id = DB::UInt128{m.seq};
        });
    });

    EXPECT_FALSE(store->tryRemountOnce());

    EXPECT_EQ(store->mountEnd(), MountEnd::SlotCorrupt);
    EXPECT_EQ(log.graceful(), 1u);
}

TEST(CASMountStop, ARemountOverAStoreOutageStillRetries)
{
    auto backend = std::make_shared<MountReadOutageBackend>();
    auto boot_ms = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    const auto advance = [boot_ms](uint64_t ms) { boot_ms->fetch_add(ms); };
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_root_id = "test",
        .boot_ms_fn = [boot_ms] { return boot_ms->load(); },
        .retry_sleep_fn = advance,
        .wait_sleep_fn = advance,
    });
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    fenceOutAndFailTheRenewal(*store, *backend);
    backend->outage = true;

    EXPECT_FALSE(store->tryRemountOnce());

    EXPECT_EQ(store->mountEnd(), MountEnd::None);
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_TRUE(log.events().empty());
}
