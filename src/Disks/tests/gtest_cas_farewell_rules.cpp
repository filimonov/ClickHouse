#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_process_stop_log.h>
#include <Disks/tests/cas_test_helpers.h>

#include <limits>

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
