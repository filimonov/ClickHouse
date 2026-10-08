#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_process_stop_log.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Common/logger_useful.h>
#include <Poco/StreamChannel.h>

#include <atomic>
#include <limits>
#include <memory>
#include <sstream>

using namespace DB::Cas;

namespace
{
constexpr uint64_t kTerminated = std::numeric_limits<uint64_t>::max();

/// The decoded `min_active_build_sequence` of the slot at `key`; 0 when the slot is absent.
uint64_t farewellSentinel(Backend & backend, const String & key)
{
    tests::OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    if (!got)
        return 0;
    return decodeMountLease(got->bytes).min_active_build_sequence;
}

/// Captures what `CasPool` logs, so a test can read the unclean-shutdown line.
class ScopedPoolLogCapture
{
public:
    ScopedPoolLogCapture()
        : logger(getLogger("CasPool"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("information");
    }

    ~ScopedPoolLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel;
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

PoolPtr openPool(const std::shared_ptr<tests::CountingBackend> & backend, uint64_t attempt_ms, bool publish_every_mutation)
{
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.cas_request_budget.attempt_timeout_ms = attempt_ms;
    config.cas_request_budget.lease_safety_margin_ms = attempt_ms;
    config.cas_request_budget.connect_timeout_cap_ms = 0;
    backend->setAttemptTimeoutMs(attempt_ms);
    if (publish_every_mutation)
    {
        config.snapshot_log_count_threshold = 0;
        config.snapshot_log_bytes_threshold = 1ULL << 40;
    }
    return Pool::open(backend, config);
}

PartWriteTxnPtr stageEmptyManifest(const PoolPtr & store, const RootNamespace & ns, const String & ref_name, ManifestId & id)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref_name;
    auto build = store->beginPartWrite(std::move(info));
    id = build->stageManifest({});
    return build;
}

void publishEmptyRef(const PoolPtr & store, const RootNamespace & ns, const String & ref_name)
{
    ManifestId id;
    auto build = stageEmptyManifest(store, ns, ref_name, id);
    build->precommitAdd(ns, ref_name, id);
    build->promote(ns, ref_name, build->buildId(), id);
}
}

/// A stopping disk's teardown must not write a farewell: the loss end says our slot is not ours to retire.
TEST(CASFarewellRules, AStoppingPoolWritesNoFarewellAtTeardown)
{
    auto backend = std::make_shared<tests::CountingBackend>();
    auto store = tests::openPoolForTest(backend);
    const String key = store->layout().mountKey("test");

    tests::processStopLog().reset();
    ASSERT_TRUE(store->latchMountEndForTest(MountEnd::SlotCorrupt));
    const uint64_t before = backend->putOverwriteCount(key);
    store.reset();

    EXPECT_EQ(backend->putOverwriteCount(key), before);
}

/// FORGET writes the one farewell before `closed`; the later destructor must add none.
TEST(CASFarewellRules, AClosedPoolWritesNoSecondFarewell)
{
    auto backend = std::make_shared<tests::CountingBackend>();
    auto store = tests::openPoolForTest(backend);
    const String key = store->layout().mountKey("test");

    const uint64_t before = backend->putOverwriteCount(key);
    store->forgetDisk({}, "r");
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_EQ(backend->putOverwriteCount(key), before + 1);
    EXPECT_EQ(farewellSentinel(*backend, key), kTerminated);

    store.reset();
    EXPECT_EQ(backend->putOverwriteCount(key), before + 1);
}

TEST(CASFarewellRules, ATeardownAfterTheLeaseExpiredStillWritesTheFarewell)
{
    auto backend = std::make_shared<tests::CountingBackend>();
    auto boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.boot_ms_fn = [boot] { return boot->load(); };
    auto store = Pool::open(backend, config);
    const String key = store->layout().mountKey("test");

    *boot += 31'000;
    store.reset();

    EXPECT_EQ(farewellSentinel(*backend, key), kTerminated);
}

TEST(CASFarewellRules, TheUncleanLineNamesAnUndrainedCleanupDuty)
{
    auto backend = std::make_shared<tests::CountingBackend>();
    auto store = openPool(backend, 100, false);
    const RootNamespace ns{"srv1/cleanup_duty"};
    publishEmptyRef(store, ns, "target");
    ManifestId id;
    auto build = stageEmptyManifest(store, ns, "durable", id);
    build->precommitAdd(ns, "durable", id);
    build.reset();
    ASSERT_TRUE(store->writerCleanupDutiesPendingForTest());

    ScopedPoolLogCapture log;
    store->forgetDisk({}, "r");

    const String text = log.captured();
    EXPECT_NE(text.find("CAS store shutdown without the clean-release marker: an undrained writer cleanup duty;"), String::npos) << text;
    EXPECT_EQ(text.find("ref-log"), String::npos) << text;
}

TEST(CASFarewellRules, TheUncleanLineNamesAWedgedLane)
{
    auto backend = std::make_shared<tests::CountingBackend>();
    auto store = openPool(backend, 100, false);
    const RootNamespace ns{"srv1/wedge"};
    store->forceWedgeForTest(ns, 1, 1, "bogus/_log/key", "bogus-bytes");

    ScopedPoolLogCapture log;
    store->forgetDisk({}, "r");

    const String text = log.captured();
    EXPECT_NE(text.find("marker: a ref-log lane still writing or wedged;"), String::npos) << text;
    EXPECT_EQ(text.find("cleanup duty"), String::npos) << text;
}
