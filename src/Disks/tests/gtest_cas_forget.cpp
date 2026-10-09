#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

/// Task 10 (rev.7 spec §5): `SYSTEM CAS FORGET` — the operator force-Vanish. FORGET drives a
/// content-addressed pool to `Vanished(forgotten)` with the fence-first protocol: (1) publish terminal
/// intent, (2) trip the local fence, (3+4) stop the GC scheduler, (5) join renewer/remount, drain, retire
/// the renewer WITHOUT an unearned clean farewell, (6) publish `Vanished(forgotten)` with the [D5] message
/// carrying the decommission timestamp. These tests exercise the Pool-level protocol body (`Pool::forgetDisk`)
/// and the end-to-end verb through a real `ContentAddressedMetadataStorage` (the six-class gate wired to the
/// new state). Harness patterns follow gtest_cas_operation_gate.cpp.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB;
using DB::Cas::PoolLifecycle;
using DB::Cas::tests::CountingBackend;

namespace
{

const String kSrid = "test";

/// A test-authored [D5] reason with a RECOGNIZABLE timestamp — the Pool-level tests assert this exact
/// string flows through `enterVanished` into the `throwIfLifecycleTerminal` message (the timestamp
/// threading the metadata storage does in production). It keeps the two [D5] substrings the gate relies on.
const String kForgetReason =
    "decommissioned by SYSTEM CAS FORGET at 2099-01-02 03:04:05 UTC — erasure was NOT "
    "verified; if this was a mistake the data may be intact (restart re-registers the name)";

/// The message thrown by `fn`, or a failure if it did not throw a `DB::Exception`.
std::string messageOf(const std::function<void()> & fn)
{
    try
    {
        fn();
    }
    catch (const Exception & e)
    {
        return std::string(e.message());
    }
    ADD_FAILURE() << "expected a DB::Exception";
    return {};
}

/// A live table dir + committed part reused by the end-to-end gate test (the shape
/// gtest_cas_operation_gate.cpp uses).
const std::string kTableDir = "gg0/gg0gg0g0-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

std::shared_ptr<ContentAddressedMetadataStorage> openForgetStorage()
{
    auto settings = Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_forget_scratch");
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

/// Deterministically interleave a real FORGET into the admission->lock window of a manual GC verb (the
/// I-1/I-2 admission TOCTOU), with BOUNDED condition-variable waits and never a sleep. The sequence pinned:
///   M (this thread, running `gc_verb`): passes the verb's pre-lock admission gate while `Live`, then the
///     installed seam signals `admitted` and blocks until `forget_done`, then M resumes to acquire
///     `gc_scheduler_mutex` and hit the under-lock re-check.
///   F (the FORGET thread): waits for `admitted`, runs the REAL `forgetDisk` (acquiring lifecycle +
///     gc_scheduler mutexes while M holds NEITHER -- M is parked in the seam BEFORE the lock), settling the
///     pool `Vanished(forgotten)`, then signals `forget_done`.
/// Returns the exception message `gc_verb` threw (via `messageOf`), so the caller asserts the typed [D5]
/// refusal. The 30s bounds trip ONLY on a genuine deadlock regression, never in the happy path.
std::string raceForgetIntoGcVerbWindow(ContentAddressedMetadataStorage & storage,
                                       const std::function<void()> & gc_verb)
{
    std::mutex m;
    std::condition_variable cv;
    bool admitted = false;
    bool forget_done = false;

    storage.setGcVerbAdmitWindowHookForTest([&]
    {
        {
            std::lock_guard lk(m);
            admitted = true;
        }
        cv.notify_all();
        std::unique_lock lk(m);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return forget_done; }))
            << "the concurrent FORGET must complete within the bound (else the interleave deadlocked)";
    });

    std::thread forgetter([&]
    {
        {
            std::unique_lock lk(m);
            EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return admitted; }))
                << "the GC verb must reach the admission->lock window before FORGET runs";
        }
        storage.forgetDisk();
        {
            std::lock_guard lk(m);
            forget_done = true;
        }
        cv.notify_all();
    });

    const std::string msg = messageOf(gc_verb);
    forgetter.join();
    storage.setGcVerbAdmitWindowHookForTest({});   /// clear the seam (references this frame's locals)
    return msg;
}

}

/// (a) FORGET on a LIVE pool: the local fence is tripped, the injected GC-stop step runs, the pool settles
/// `Vanished(forgotten)`, and store-class access fails loud with the timestamped [D5] message.
TEST(CASForget, ForgetOnLivePoolTripsFenceAndVanishes)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);
    ASSERT_TRUE(store->mayMutate());

    bool gc_stopped = false;
    store->forgetDisk([&] { gc_stopped = true; }, kForgetReason);

    /// Step 3/4 ran (the GC-stop callback was invoked from inside the protocol).
    EXPECT_TRUE(gc_stopped);
    /// Terminal truth, fence tripped.
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_TRUE(store->isVanished());
    EXPECT_FALSE(store->mayMutate());

    /// The [D5] message carries the operator's FORGET timestamp (threaded through the reason) and still
    /// names the sub-state ("erasure was NOT verified").
    const std::string msg = messageOf([&] { store->throwIfLifecycleTerminal(); });
    EXPECT_NE(msg.find("SYSTEM CAS FORGET at "), std::string::npos) << msg;
    EXPECT_NE(msg.find("2099-01-02 03:04:05 UTC"), std::string::npos) << msg;
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
}

/// (a') FORGET stops AND joins a real `CasGcScheduler`'s worker + heartbeat threads (the injected GC-stop
/// step). A long interval keeps any round from firing during the test window, so this isolates the
/// thread-lifecycle: `start()` spawns the two workers, FORGET's callback `stop()`s + joins them, and the
/// test completing (no hang) plus a clean `isQuiescent()` proves the join.
TEST(CASForget, ForgetStopsAndJoinsRealGcScheduler)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    Cas::CasGcScheduler sched(store, std::chrono::seconds(3600), "CasForgetTest", "forget-disk");
    sched.start();

    bool gc_joined = false;
    store->forgetDisk([&] { sched.stop(); gc_joined = true; }, kForgetReason);

    EXPECT_TRUE(gc_joined);
    /// What this proves is the JOIN: `stop()` returned, so the worker + heartbeat threads are joined and
    /// the test could not have hung; `isQuiescent()` confirms no round is in flight. NOTE: the callback is
    /// only `sched.stop()`, which does NOT itself clear the in-process `i_am_leader` hint — the
    /// metadata-storage handler clears leadership by DESTROYING the scheduler (see
    /// `ContentAddressedMetadataStorage::forgetDisk`), so asserting `is_leader == false` here would be
    /// vacuous (this 3600s scheduler never led) or, after a real round, wrong.
    EXPECT_TRUE(sched.isQuiescent()) << "no GC round may be in flight after FORGET joined the scheduler";
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
}

/// (c) Double FORGET is idempotent: the second call is a no-op (the pool is already `Vanished(forgotten)`),
/// so it never re-runs the protocol — the GC-stop callback is NOT invoked again, and the first reason wins.
TEST(CASForget, DoubleForgetIsIdempotent)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    int gc_stops = 0;
    store->forgetDisk([&] { ++gc_stops; }, kForgetReason);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    ASSERT_EQ(gc_stops, 1);

    /// A second FORGET with a DIFFERENT reason must change nothing (first terminal transition wins) and
    /// must NOT re-enter the teardown (idempotent short-circuit on `isVanished()`).
    store->forgetDisk([&] { ++gc_stops; }, "a different reason that must be ignored");
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_EQ(gc_stops, 1) << "the idempotent second FORGET must not re-run the protocol";

    const std::string msg = messageOf([&] { store->throwIfLifecycleTerminal(); });
    EXPECT_NE(msg.find("2099-01-02 03:04:05 UTC"), std::string::npos)
        << "the first FORGET's reason must win: " << msg;
}

/// (a'') The clean-farewell is EARNED, never unconditional: on a drained pool FORGET stamps the mount lease
/// with the terminated sentinel (`min_active_build_sequence == UINT64_MAX`) so a same-server restart reclaims immediately,
/// but with an UNSETTLED (wedged) ref lane it must NOT — the lease is left to expire by observation.
TEST(CASForget, ForgetCleanFarewellGatedOnDrain)
{
    using DB::Cas::decodeMountLease;
    constexpr uint64_t kTerminated = std::numeric_limits<uint64_t>::max();

    /// Drained pool → clean farewell written (lease stamped terminated).
    {
        auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
        auto store = DB::Cas::tests::openPoolForTest(backend);
        const String mount_key = store->layout().mountKey(kSrid);
        DB::Cas::tests::OperationForTest op(*backend);
        ASSERT_NE(decodeMountLease((*op).read(mount_key, DB::Cas::Retry::once())->bytes).min_active_build_sequence, kTerminated);   /// baseline

        store->forgetDisk([] {}, kForgetReason);
        ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);

        const auto got = (*op).read(mount_key, DB::Cas::Retry::once());
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(decodeMountLease(got->bytes).min_active_build_sequence, kTerminated)
            << "a drained FORGET earns the clean-release farewell";
    }

    /// Unsettled (wedged) ref lane → NO clean farewell (the drain cannot certify a clean death).
    {
        auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
        auto store = DB::Cas::tests::openPoolForTest(backend);
        const String mount_key = store->layout().mountKey(kSrid);

        const DB::Cas::RootNamespace ns{"test/forget_wedge"};
        store->forceWedgeForTest(ns, /*writer_epoch*/ 1, /*ref_sequence*/ 1, "bogus/_log/key", "bogus-bytes");
        ASSERT_TRUE(store->refLaneWedgedForTest(ns));

        store->forgetDisk([] {}, kForgetReason);
        ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);

        DB::Cas::tests::OperationForTest op(*backend);
        const auto got = (*op).read(mount_key, DB::Cas::Retry::once());
        ASSERT_TRUE(got.has_value()) << "the lease object must still be present (expiry by observation)";
        EXPECT_NE(decodeMountLease(got->bytes).min_active_build_sequence, kTerminated)
            << "an unearned clean farewell must NOT be written when the ref lanes did not drain";
    }
}

/// (e) End-to-end through the verb entry `ContentAddressedMetadataStorage::forgetDisk` and the six-class
/// gate: after FORGET, a Probe answers truth-absent, a Remove no-ops, and a content read throws the [D5]
/// message with the REAL decommission timestamp produced by the handler.
TEST(CASForget, ForgetEndToEndGatesTruthWithTimestampedMessage)
{
    auto storage = openForgetStorage();
    commitOnePart(*storage);
    ASSERT_TRUE(storage->existsFile(kPartFile));   /// Live baseline

    storage->forgetDisk();

    /// Probe → truth-absent (no throw): the committed part reads absent on a forgotten disk.
    EXPECT_FALSE(storage->existsFile(kPartFile));
    EXPECT_FALSE(storage->existsDirectory(kPartDir));

    /// Remove → no-op success (this is what lets a forgotten-disk table's DROP complete).
    EXPECT_NO_THROW({
        auto tx = storage->createTransaction();
        tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr);
        tx->commit(NoCommitOptions{});
    });

    /// Content read → the typed [D5] message, with the handler's real UTC timestamp.
    const std::string msg = messageOf([&] { storage->getFileSize(kPartFile); });
    EXPECT_NE(msg.find("SYSTEM CAS FORGET at "), std::string::npos) << msg;
    EXPECT_NE(msg.find(" UTC"), std::string::npos) << msg;
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
}

/// (I-1 regression) A manual `SYSTEM CAS GC RUN` admitted while `Live` but that acquires
/// `gc_scheduler_mutex` strictly AFTER a concurrent FORGET completes must NOT recreate a `CasGcScheduler`
/// on the now-`Vanished` pool: the under-lock admission re-check refuses with the typed [D5] message. The
/// interleave is deterministic (bounded cv waits, no sleep) — the GC-verb seam parks the RUN in the
/// admission→lock window while the FORGET thread drives the real teardown. The lasting-damage observable is
/// `gcHealth()` staying empty: a resurrected scheduler (the pre-fix behavior) would make it non-empty.
/// Verified RED against the pre-fix ordering (see task-17-report.md).
TEST(CASForget, GcRunAdmittedWhileLiveRefusesAfterConcurrentForget)
{
    auto storage = openForgetStorage();
    /// Capture the pool while Live (store() is fail-closed once Vanished) to assert its terminal state after.
    auto pool = storage->store();
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::Live);
    ASSERT_FALSE(storage->gcHealth().has_value())
        << "no scheduler exists before the first GC round (unit-test null context creates none at startup)";

    const std::string msg = raceForgetIntoGcVerbWindow(
        *storage, [&] { storage->runGarbageCollectionRoundNow(); });

    /// The refusal is the typed FORGET [D5] message (an under-lock admission throw), not a round-internal
    /// error and not a silently-run round.
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
    /// The I-1 lasting-damage observable: NO scheduler was created on the decommissioned pool.
    EXPECT_FALSE(storage->gcHealth().has_value())
        << "a GC RUN refused post-FORGET must NOT recreate the scheduler on a Vanished pool";
    EXPECT_EQ(pool->lifecycle(), PoolLifecycle::VanishedForgotten);
}

/// (I-2 regression) A `SYSTEM CAS GC REBUILD` holds `gc_scheduler_mutex` for its whole
/// duration, so a concurrent FORGET must SERIALIZE behind it — FORGET cannot report the disk decommissioned
/// while the rebuild is still issuing durable `gc/`-plane writes. Deterministic (bounded cv waits + a bounded
/// negative future poll anchored by a positive control, never a sleep-to-fix-a-race): the in-lock seam parks
/// the rebuild WHILE it holds the mutex; a FORGET launched in that window must NOT complete until the rebuild
/// releases the lock. The pre-fix `runGcRebuildNow` took NO lock, so an in-flight rebuild was invisible to
/// FORGET and FORGET would complete immediately. Verified RED against the pre-fix code (see task-17-report.md).
TEST(CASForget, GcRebuildInFlightSerializesForget)
{
    auto storage = openForgetStorage();
    auto pool = storage->store();   /// captured while Live
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::Live);

    std::mutex m;
    std::condition_variable cv;
    bool rebuild_holds_lock = false;
    bool may_release = false;

    /// In-lock seam: fires WHILE the rebuild holds `gc_scheduler_mutex`. It parks there (bounded) until the
    /// coordinator has verified FORGET is blocked, then lets the rebuild finish and release the lock.
    storage->setGcVerbAdmitWindowHookForTest([&]
    {
        {
            std::lock_guard lk(m);
            rebuild_holds_lock = true;
        }
        cv.notify_all();
        std::unique_lock lk(m);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return may_release; }))
            << "the coordinator must release the in-flight rebuild within the bound";
    });

    /// The rebuild runs on its own thread; it holds the lock through the seam above.
    std::promise<void> rebuild_done_p;
    auto rebuild_done = rebuild_done_p.get_future();
    std::thread rebuilder([&]
    {
        /// On release the pool may already be Vanished (RED path: FORGET ran unserialized) — `store()` then
        /// throws; swallow it, the assertions below carry the verdict.
        try { storage->runGcRebuildNow(/*force=*/false); } catch (...) {} // NOLINT(bugprone-empty-catch)
        rebuild_done_p.set_value();
    });

    /// Wait until the rebuild is genuinely in flight (holding the lock).
    {
        std::unique_lock lk(m);
        ASSERT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return rebuild_holds_lock; }))
            << "the rebuild must reach its in-lock seam";
    }

    /// Launch FORGET while the rebuild holds the lock. With the fix it BLOCKS on `gc_scheduler_mutex`;
    /// without the fix (pre-fix rebuild took no lock) it runs straight through.
    std::promise<void> forget_done_p;
    auto forget_done = forget_done_p.get_future();
    std::thread forgetter([&] { storage->forgetDisk(); forget_done_p.set_value(); });

    /// The discriminator: FORGET must NOT complete while the rebuild holds the lock. This bounded negative
    /// observation is anchored by the positive control below (FORGET DOES complete once the lock releases),
    /// so the window's meaning is real, not a race hidden behind a sleep.
    EXPECT_EQ(forget_done.wait_for(std::chrono::seconds(2)), std::future_status::timeout)
        << "FORGET must serialize behind an in-flight GC rebuild (the rebuild holds gc_scheduler_mutex)";

    /// Release the in-flight rebuild; it finishes and drops the lock, and FORGET can now proceed.
    {
        std::lock_guard lk(m);
        may_release = true;
    }
    cv.notify_all();

    ASSERT_EQ(rebuild_done.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "the rebuild must complete after release";
    ASSERT_EQ(forget_done.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "once the rebuild releases the lock, the serialized FORGET completes (positive control)";

    rebuilder.join();
    forgetter.join();
    storage->setGcVerbAdmitWindowHookForTest({});

    EXPECT_EQ(pool->lifecycle(), PoolLifecycle::VanishedForgotten)
        << "FORGET settles the pool Vanished(forgotten) once it is no longer serialized behind the rebuild";
}
