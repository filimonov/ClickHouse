# CAS lease PR 3: the lease thread does not fail on the server memory limit — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** a server above `max_server_memory_usage` keeps renewing its CAS mount lease. The renewal
thread holds `LockMemoryExceptionInThread(VariableContext::Global)` for its whole life, so every
iteration (admission, request, result handling) is covered; the tracker still counts its allocations.
A conditional `PUT` runs on the calling thread with a buffer of the body size, so the guard covers the
upload too. `std::bad_alloc` still ends the renewal. Closes #2403 and CAS-292.

**Architecture:** one RAII guard at the top of `CasMountRuntime::renewalLoop`
(`CA/Pool/CasMountRuntime.cpp`). Today an over-limit throw outside the request reaches the loop's
`catch (...)`, which calls `tripMountLost` and ends the thread. In `CA/Backend/CasObjectStorageBackend.cpp`,
`conditionalWriteSettings` sets `s3_allow_parallel_part_upload = false`. Without it,
`S3ObjectStorage::writeObject` hands the upload to the `REMOTE_FS_WRITE_THREAD_POOL` scheduler, which
runs outside the caller's guard. `nativeConditionalPut` opens the buffer at `max(1, bytes.size())`
instead of `DBMS_DEFAULT_BUFFER_SIZE`.

**Why `Global`:** `LockMemoryExceptionInThread::isBlocked` blocks levels `>= level`, and
`VariableContext::Global` is 0. The default `User` level leaves the `total_memory_tracker` check (the
`max_server_memory_usage` limit) able to throw. Task 3 has a mutation step that proves it.

**Tech stack:** C++23, gtest (`unit_tests_dbms`), `ManualBarrier`, a local Poco HTTP server standing in
for S3.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`.
- Section 2 "Потеря", the paragraph that starts "Исключение по лимиту памяти не считается ошибкой нити".
- Section 7, row 3 of the PR table: "`LockMemoryExceptionInThread(Global)` на всю итерацию продления;
  условный `PUT` на вызывающей нити с буфером по размеру тела", +10, closes #2403, CAS-292.
- Section 7, row 3 of the tests table: "глобальный лимит памяти превышен на нити продления — нить жива,
  `local_defect` нет; большой условный объект".
- Section 9 names no model for PR 3 ("Сигналы, watchdog, память … проверяются процессными и транспортными
  тестами").

**Backlog task:** `docs/superpowers/cas/backlog/tasks/cas-292 - Keep-the-mount-lease-renewal-alive-when-the-server-is-over-its-memory-limit-2403.md`
(master worktree). Acceptance: (1) over the global limit renewals keep committing and the mount is not
lost; (2) an injected `MEMORY_LIMIT_EXCEEDED` on the renewal path neither ends the thread nor trips the
fence; (3) the conditional `PUT` runs on the calling thread and the memory is still accounted.

**Reference:** an earlier implementation on the local branch `fix/antalya-26.6/cas-mount-renewal-no-deadline`
of `/home/mfilimonov/workspace/ClickHouse/cas-95-1`, commits `0ec8214d15b` and `817426de55c`. This plan
reuses their production diff. The tests differ: an allocation inside the request, a thread-tracker
measure, a `bad_alloc` golden test and a large-body golden test.

**Code base:** `/home/mfilimonov/workspace/ClickHouse/cas-95-1`, base `5b7c9f503df`
(`altinity/antalya-26.6`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Global Constraints

- One topic per PR; the PR is self-contained and leaves a working product; no behaviour outside the
  spec row changes. Changelog: one sentence, category per `.github/PULL_REQUEST_TEMPLATE.md`.
- Allman braces; no `sleep` to fix races; tests use injected clocks and deterministic barriers.
- A test that expects `LOGICAL_ERROR` is a death test under `DEBUG_OR_SANITIZER_BUILD` (the error
  aborts there); prefer injecting `NOT_IMPLEMENTED`/`BAD_ARGUMENTS` where the rule allows.
- Shared code outside `CA/` (`src/IO/S3`, `base/poco`, `src/Daemon`, `src/Storages/System`) is a fork
  patch: minimal, motivated, portable; say in the plan which files outside `CA/` are touched and why.
- Comments explain why, not what; no references to plans, reviews, backlog ids or history in code.
- Prose style for PR text and comments: point first, plain words, backticks for names, `f` not `f()`.
- Build: `ninja -C build <target>` without `-j`, output to a log file in the build dir; the gate is
  `build/src/unit_tests_dbms --gtest_filter='CAS*'` (release) and the same in `build_asan`.
- Commit exact files only (`git add -- <path> …`), never a directory, never `-A`; no rebase/amend.

## Review Focus

- **The guard is thread-scoped, not iteration-scoped.** It is the first statement after
  `setThreadName` in `renewalLoop` and lives until the function returns, so it covers every iteration
  and `consumeRenewResult`. The destructor restores the thread-local level before the pooled thread
  runs another job. Not covered, on purpose: `renewRenewerForStartupOnce` on the opening thread (its
  failure fails the open) and the remount thread `remountLoop` (it catches the failure of
  `remount_attempt` and retries). See "Spec and code disagreements" item 1.
- **`std::bad_alloc` is not absorbed.** `LockMemoryExceptionInThread` affects only `MemoryTracker`
  throws. `BadAllocInsideTheRenewalStaysTerminal` fences that no catch-all was added.
- **`s3_allow_parallel_part_upload = false` applies to every Native conditional write**: the lease,
  shard manifests, `gc/state`, the registry and the ref ledger. A body large enough for a multipart
  upload now uploads its parts one after another on the caller. Generation stores already force a single `PUT`. The
  caller waits for the whole write in both cases.
- **The test lowers the process-wide hard limit for one allocation.** `allocateOverTheGlobalLimit`
  sets `total_memory_tracker`'s hard limit to 1 and restores it with `SCOPE_EXIT` around one
  allocation. An allocation by another thread in that window would also see the limit. In the test,
  the main thread is waiting on a barrier and the remount worker is idle.
- **Accounting is measured on the renewal thread's own tracker**, exactly
  `kOverLimitAllocationBytes`. A thread tracker without a group has `total_memory_tracker` as its
  parent. A delta of the global tracker could be shrunk by a concurrent free on another thread.
- No file outside `CA/`, `src/Disks/tests/` and `docs/en/antalya/cas/` is touched.

## Files

| File | Change |
|---|---|
| `CA/Pool/CasMountRuntime.cpp` | include `<Common/LockMemoryExceptionInThread.h>`; the guard in `renewalLoop` |
| `CA/Backend/CasObjectStorageBackend.cpp` | `conditionalWriteSettings`: `s3_allow_parallel_part_upload = false`; `nativeConditionalPut`: buffer `max(1, bytes.size())` |
| `CA/Backend/CasObjectStorageBackend.h` | doc comment of `conditionalWriteSettings` names the calling-thread upload |
| `src/Disks/tests/gtest_cas_pool.cpp` | two `RuntimeRenewBackend` faults; `allocateOverTheGlobalLimit`; two tests |
| `src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp` | the test server drains and counts request bodies; three tests |
| `docs/en/antalya/cas/architecture/mounts-and-leases.md` | one paragraph on memory limits |

## Spec and code disagreements (flagged, not resolved)

1. Spec section 2 says "вся итерация продления **и получения заново** идёт под
   `LockMemoryExceptionInThread(VariableContext::Global)`". The PR row says only "на всю итерацию
   продления". On the base, getting the slot back is the remount, and it runs on the separate
   `CAS_REMOUNT` thread (`remountLoop`). The plan follows the PR row and guards the renewal thread.
   PR 14 moves reacquisition onto the renewal thread, where it inherits this guard. Until then a memory
   limit on the remount thread costs one remount attempt (caught in `remountLoop`), not the thread.
2. The tests row says "`local_defect` нет". `local_defect` does not exist before PR 12. The plan asserts
   what the base has instead: `lifecycle()` stays `Live`, `CASMountLeaseLost` does not move, and the
   thread reaches its next admission.
3. Backlog acceptance 1 says "without the change the same test reports a terminal renewal". On the base,
   the red test shows the tracker throwing at three points on the renewal thread; the helper catches
   that throw, so it does not show a terminal renewal. An uncaught throw from `consumeRenewResult`
   reaches `renewalLoop`'s `catch (...)`, which runs `chassert(false)`. That aborts debug and sanitizer
   builds and would hide every later test.
4. The backlog task also names "~1 MiB for a settling read". The spec sizes only the `PUT` buffer. The
   read buffer is unchanged here. Reads run on the renewal thread, so the guard covers them.

---

## Task 1: Branch

- [ ] **Step 1: Create the branch from the base**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git status --short -- src/Disks docs/en/antalya    # expect no output
git switch -c cas-lease-pr03-memory-guard 5b7c9f503df
```

If `git status` shows edits under `src/Disks` or `docs/en/antalya`, stop and report: they belong to
another agent.

## Task 2: Golden test — `std::bad_alloc` inside the renewal stays terminal

Consumes: `RuntimeRenewBackend`, `RuntimeUnderTest`, `runtimeRenewBudget` in `gtest_cas_pool.cpp`.
Produces: `RuntimeRenewBackend::Fault::ThrowBadAlloc` and `Fault::ThrowMemoryLimitExceeded`, used again in Task 3.

This test is green on the base and stays green. On the base, the engine rethrows a non-`Poco` exception
from a write attempt. `MountLeaseRenewer::renewOn` then classifies it `DeterministicFailure`, and
`DriverLease::finish` trips the mount.

**Files:** `src/Disks/tests/gtest_cas_pool.cpp`

- [ ] **Step 1: Includes and the error code**

Add to the include block, after `#include <Common/ProfileEvents.h>`:

```cpp
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <base/scope_guard.h>
```

Add `#include <new>` after `#include <mutex>`. In `namespace DB::ErrorCodes`, after
`extern const int NETWORK_ERROR;`, add `extern const int MEMORY_LIMIT_EXCEEDED;`.

- [ ] **Step 2: Two faults on `RuntimeRenewBackend`**

In `enum class Fault`, after `BlockThenThrow,` add:

```cpp
        ThrowMemoryLimitExceeded,
        ThrowBadAlloc,
```

In `RuntimeRenewBackend::write`, right after the closing brace of the
`if (current == Fault::BlockThenDelegate || current == Fault::BlockThenThrow)` block, add:

```cpp
        if (current == Fault::ThrowMemoryLimitExceeded)
            throw DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, "injected memory limit exceeded on the renewal request");
        if (current == Fault::ThrowBadAlloc)
            throw std::bad_alloc();
```

- [ ] **Step 3: The test**

Insert after the closing brace of `TEST(CASPoolRemount, TeardownJoinsBothWorkersBeforeRelease)`:

```cpp
/// The memory guard on the lease thread does not absorb `std::bad_alloc`: the renewal ends terminally
/// and the mount lease is lost.
TEST(CASMountRuntime, BadAllocInsideTheRenewalStaysTerminal)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-bad-alloc");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);

    String outcome;
    String classification;
    DB::Cas::tests::ManualBarrier reported;
    CasEventSink sink = [&](CasEvent event)
    {
        if (event.type != CasEventType::WatermarkRenew || !outcome.empty())
            return;
        outcome = event.outcome;
        classification = event.detail["classification"];
        reported.arriveAndWait();
    };
    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                    .boot_ms_fn = [&] { return boot_ms; }},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    /// Declared after the runtime so it runs first: a failed expectation must not leave the lease thread
    /// parked on the barrier while the runtime's destructor joins it.
    SCOPE_EXIT({ reported.release(); });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    const uint64_t leases_lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();

    backend->fault = RuntimeRenewBackend::Fault::ThrowBadAlloc;
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));

    reported.waitUntilArrived();
    EXPECT_EQ(outcome, "failed");
    EXPECT_EQ(classification, "deterministic_failure");
    EXPECT_FALSE(runtime.mayMutate());
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), leases_lost_before + 1);

    reported.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}
```

- [ ] **Step 4: Run on the base — expect PASS**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task2.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASMountRuntime.BadAllocInsideTheRenewalStaysTerminal' > build/test_pr03_task2.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`, `[  PASSED  ] 1 test`. If it fails on the base, it is not a golden
test: stop and report the failure text.

- [ ] **Step 5: Commit**

```bash
git add -- src/Disks/tests/gtest_cas_pool.cpp
git commit -F - <<'EOF'
Test that a bad_alloc inside a CAS mount renewal stays terminal

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 3: The renewal thread does not throw on the global memory limit (red on base)

Consumes: Task 2's faults and includes; `MountConfig::renewal_admitted_hook_for_test`;
`RuntimeRenewBackend::after_commit`. Produces: the guard in `renewalLoop`.

**Files:** `src/Disks/tests/gtest_cas_pool.cpp`, `CA/Pool/CasMountRuntime.cpp`

- [ ] **Step 1: The failing test**

Insert after the closing brace of `BadAllocInsideTheRenewalStaysTerminal`:

```cpp
namespace
{

/// Larger than the untracked batch a thread keeps before it reports to its tracker, so the allocation
/// reaches the global tracker at once.
constexpr Int64 kOverLimitAllocationBytes = 8 * 1024 * 1024;

struct OverLimitAllocation
{
    /// What the tracker threw; empty when it did not throw.
    String failure;
    /// How much this thread's own tracker grew by the allocation.
    Int64 counted = 0;
};

/// One allocation through the enforcing tracker path while the global tracker is over its hard limit.
/// The limit is restored before this returns, whatever the allocation did.
OverLimitAllocation allocateOverTheGlobalLimit()
{
    DB::CurrentThread::flushUntrackedMemory();
    MemoryTracker & thread_tracker = *DB::CurrentThread::getMemoryTracker();
    const Int64 saved_limit = total_memory_tracker.getHardLimit();
    SCOPE_EXIT({ total_memory_tracker.setHardLimit(saved_limit); });
    total_memory_tracker.setHardLimit(1);

    OverLimitAllocation result;
    const Int64 before = thread_tracker.get();
    try
    {
        std::ignore = CurrentMemoryTracker::alloc(kOverLimitAllocationBytes);
    }
    catch (...)
    {
        result.failure = DB::getCurrentExceptionMessage(/*with_stacktrace=*/false);
        return result;
    }
    result.counted = thread_tracker.get() - before;
    std::ignore = CurrentMemoryTracker::free(kOverLimitAllocationBytes);
    return result;
}

}

/// With the global tracker over its hard limit, allocations on the lease thread before the request,
/// inside it and while its result is consumed do not throw and are still counted. The thread goes on
/// to its next renewal and the mount stays live. A `MEMORY_LIMIT_EXCEEDED` from the first attempt is
/// retried.
TEST(CASMountRuntime, GlobalMemoryLimitDoesNotEndTheLeaseThread)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-global-memory-limit");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    const UInt128 uuid{1};
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind, MountClaimResult::Claimed);
    const Int64 hard_limit_before = total_memory_tracker.getHardLimit();

    std::atomic<uint32_t> admissions{0};
    OverLimitAllocation before_request;
    DB::Cas::tests::ManualBarrier second_admission;
    std::optional<OverLimitAllocation> inside_request;
    std::optional<OverLimitAllocation> while_consumed;
    String outcome;
    String attempts_sent;
    String classification;
    DB::Cas::tests::ManualBarrier reported;
    CasEventSink sink = [&](CasEvent event)
    {
        if (event.type != CasEventType::WatermarkRenew || while_consumed)
            return;
        while_consumed = allocateOverTheGlobalLimit();
        outcome = event.outcome;
        attempts_sent = event.detail["attempts_sent"];
        classification = event.detail["classification"];
        reported.arriveAndWait();
    };

    RuntimeUnderTest runtime_holder(
        backend, layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .background_watermark = true,
            .boot_ms_fn = [&] { return boot_ms; },
            .renewal_admitted_hook_for_test = [&]
            {
                const uint32_t admission = ++admissions;
                if (admission == 1)
                    before_request = allocateOverTheGlobalLimit();
                else if (admission == 2)
                    second_admission.arriveAndWait();
            }},
        "test", sink, runtimeRenewBudget(), [] { return false; });
    /// Declared after the runtime so it runs first: a failed expectation must not leave the lease thread
    /// parked on a barrier while the runtime's destructor joins it.
    SCOPE_EXIT({
        reported.release();
        second_admission.release();
    });
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer();
    runtime.armMountFence(uuid, 1, anchor + 1000);
    const uint64_t leases_lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();

    /// Armed only now: `startRenewer` renews on this thread, and these must run on the lease thread.
    backend->after_commit = [&]
    {
        if (!inside_request)
            inside_request = allocateOverTheGlobalLimit();
    };
    backend->fault = RuntimeRenewBackend::Fault::ThrowMemoryLimitExceeded;
    runtime.startBackgroundWorkers(std::chrono::milliseconds(0));

    reported.waitUntilArrived();
    EXPECT_EQ(outcome, "recovered") << "a memory-limit exception inside the request must be retried";
    EXPECT_EQ(attempts_sent, "2");
    EXPECT_EQ(classification, "committed_after_retry");
    reported.release();

    /// The worker is admitted for its next renewal: the thread outlived the over-limit allocations.
    second_admission.waitUntilArrived();
    EXPECT_TRUE(runtime.mayMutate());
    EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::Live);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), leases_lost_before);

    EXPECT_EQ(before_request.failure, "") << "the allocation before the request threw";
    EXPECT_EQ(before_request.counted, kOverLimitAllocationBytes);
    ASSERT_TRUE(inside_request.has_value());
    EXPECT_EQ(inside_request->failure, "") << "the allocation inside the request threw";
    EXPECT_EQ(inside_request->counted, kOverLimitAllocationBytes);
    ASSERT_TRUE(while_consumed.has_value());
    EXPECT_EQ(while_consumed->failure, "") << "the allocation while the result was consumed threw";
    EXPECT_EQ(while_consumed->counted, kOverLimitAllocationBytes);
    EXPECT_EQ(total_memory_tracker.getHardLimit(), hard_limit_before);

    second_admission.release();
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown(false);
}
```

- [ ] **Step 2: Run on the base — expect FAIL**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task3_red.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASMountRuntime.GlobalMemoryLimitDoesNotEndTheLeaseThread' > build/test_pr03_task3_red.log 2>&1; echo TEST_EXIT=$?
grep -a -A4 'Failure' build/test_pr03_task3_red.log
```

Run the red step on release only: on the base it must not reach `chassert(false)`, and it does not,
because the helper catches the throw. Expect `NINJA_EXIT=0`, `TEST_EXIT=1` and six failed expectations.
- Three print `before_request.failure` / `inside_request->failure` / `while_consumed->failure` with a
  value containing `exceeded: would use` and `maximum: 1.00 B`, followed by "the allocation before the
  request threw", "the allocation inside the request threw" and "the allocation while the result was
  consumed threw".
- Three print a `counted` of `0` against `8388608`.

The `recovered`, `2`, `committed_after_retry`, `mayMutate`, `Live` and `CASMountLeaseLost` expectations
pass on the base. Paste the six failures into the task report. If `outcome` is not `recovered` on the
base, stop and report: the test setup differs from what this plan assumes.

- [ ] **Step 3: The guard**

In `CA/Pool/CasMountRuntime.cpp`, add `#include <Common/LockMemoryExceptionInThread.h>` after
`#include <Common/Exception.h>`. In `CasMountRuntime::renewalLoop`, right after
`setThreadName(ThreadName::CAS_LEASE_RENEWER);`, add:

```cpp
    /// The tracker still counts this thread's allocations but never throws on them: a renewal must not
    /// fail on the server memory limit. `Global`, because a `User`-level lock leaves that limit enforced.
    LockMemoryExceptionInThread memory_exception_lock(VariableContext::Global);
```

- [ ] **Step 4: Run again — expect PASS**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task3.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASMountRuntime.*:CASPoolRemount.*' > build/test_pr03_task3.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`.

- [ ] **Step 5: Mutation check — `User` level is not enough**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
F=src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp
sed -i 's/memory_exception_lock(VariableContext::Global);/memory_exception_lock(VariableContext::User);/' "$F"
git diff --stat -- "$F"    # one changed line against the Step 3 edit is expected
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task3_mut.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASMountRuntime.GlobalMemoryLimitDoesNotEndTheLeaseThread' > build/test_pr03_task3_mut.log 2>&1; echo TEST_EXIT=$?
sed -i 's/memory_exception_lock(VariableContext::User);/memory_exception_lock(VariableContext::Global);/' "$F"
grep -n 'memory_exception_lock(VariableContext::Global);' "$F"
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task3.log 2>&1; echo NINJA_EXIT=$?
```

Expect `TEST_EXIT=1` with the same six failures as Step 2. Then expect the `grep` to print exactly one
line and the rebuild to give `NINJA_EXIT=0`. Do not commit the mutation.

- [ ] **Step 6: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp \
    src/Disks/tests/gtest_cas_pool.cpp
git commit -F - <<'EOF'
Keep memory-limit exceptions off the CAS mount-lease thread

The tracker still counts the renewal thread's allocations but no longer
throws on them, so a server above `max_server_memory_usage` keeps its
mount lease. `std::bad_alloc` still ends the renewal.

Related: https://github.com/Altinity/ClickHouse/issues/2403

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 4: Golden test — a large conditional object is one `PUT` with the whole body

Consumes: `DelayedResponseServer`, `makeDispatchStorageForTest`, `contextForTest` in
`gtest_cas_s3_single_attempt_client.cpp`. Produces: `DelayedResponseServer::bodyBytesSeen`,
`acceptPut` and `conditionalBackendForTest`, used again in Tasks 5 and 6.

This test is green on the base and stays green. The body is larger than the 1 MiB cap that
`WriteBufferFromS3` applies to its first buffer. That buffer then doubles up to the first part size,
so the body still goes out as one `PUT`.

**Files:** `src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp`

- [ ] **Step 1: The server drains and counts request bodies**

Add `#include <Poco/NullStream.h>` and `#include <Poco/StreamCopier.h>` after
`#include <Poco/Net/ServerSocket.h>`.

In `DelayedResponseRequestHandler`, add the member `std::atomic<size_t> & body_bytes_seen;` after
`std::atomic<size_t> & requests_seen;`, and replace the constructor and `handleRequest` with:

```cpp
    DelayedResponseRequestHandler(
        std::atomic<size_t> & requests_seen_,
        std::atomic<size_t> & body_bytes_seen_,
        std::chrono::milliseconds delay_,
        std::function<void(Poco::Net::HTTPServerResponse &)> respond_)
        : requests_seen(requests_seen_), body_bytes_seen(body_bytes_seen_), delay(delay_), respond(std::move(respond_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        ++requests_seen;
        /// Only a body with a declared length: without one, the stream would read until the client hangs up.
        if (request.hasContentLength())
        {
            Poco::NullOutputStream discard;
            body_bytes_seen += static_cast<size_t>(Poco::StreamCopier::copyStream64(request.stream(), discard));
        }
        std::this_thread::sleep_for(delay);
        respond(response);
    }
```

In `DelayedResponseRequestHandlerFactory`, add the same member after `requests_seen`. Pass it through
`createRequestHandler`:

```cpp
        return new DelayedResponseRequestHandler(requests_seen, body_bytes_seen, delay, respond);
```

Add the parameter to the factory constructor:

```cpp
    DelayedResponseRequestHandlerFactory(
        std::atomic<size_t> & requests_seen_,
        std::atomic<size_t> & body_bytes_seen_,
        std::chrono::milliseconds delay_,
        std::function<void(Poco::Net::HTTPServerResponse &)> respond_)
        : requests_seen(requests_seen_), body_bytes_seen(body_bytes_seen_), delay(delay_), respond(std::move(respond_))
    {
    }
```

In `DelayedResponseServer`, add the member `std::atomic<size_t> body_bytes_seen{0};` after
`std::atomic<size_t> requests_seen{0};`. Change the factory construction to
`new DelayedResponseRequestHandlerFactory(requests_seen, body_bytes_seen, delay, std::move(respond))`.
Add the accessor after `requestsSeen`:

```cpp
    size_t bodyBytesSeen() const { return body_bytes_seen.load(); }
```

- [ ] **Step 2: Helpers and the test**

In the anonymous namespace, after `contextForTest`, add:

```cpp
/// A server that accepts every `PUT` with a fixed `ETag`.
void acceptPut(Poco::Net::HTTPServerResponse & response)
{
    response.set("ETag", "\"put-etag\"");
    response.setContentLength(0);
    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.send();
}

/// A writable Native backend as `openPoolView` builds one: conditional writes are single-attempt.
std::shared_ptr<DB::Cas::ObjectStorageBackend> conditionalBackendForTest(DB::ObjectStoragePtr storage)
{
    return std::make_shared<DB::Cas::ObjectStorageBackend>(
        std::move(storage), DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/true, /*attempt_timeout_ms_=*/5000, /*connect_timeout_cap_ms_=*/5000);
}
```

Add `#include <variant>` after `#include <utility>`. Before the final `#endif`, add:

```cpp
/// A conditional body larger than the first write buffer still goes out as one `PUT` carrying every byte.
TEST(CASBackend, LargeConditionalPutIsOneRequestWithTheWholeBody)
{
    (void)contextForTest();

    DelayedResponseServer server(std::chrono::milliseconds(0), acceptPut);
    auto backend = conditionalBackendForTest(makeDispatchStorageForTest(server.getUrl(), 10000));
    DB::Cas::CasRequests requests(DB::Cas::BackendPtr(backend), DB::Cas::Fence::open());

    const std::string body(3 * 1024 * 1024 + 1, 'L');
    auto op = requests.admit();
    EXPECT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("large-key", body, DB::Cas::Retry::once())));
    EXPECT_EQ(server.requestsSeen(), 1u);
    EXPECT_EQ(server.bodyBytesSeen(), body.size());
}
```

- [ ] **Step 3: Run on the base — expect PASS, and the file's other tests too**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task4.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASBackend.*:CASEnvelopeWiring.*:S3SingleAttemptClient.*' > build/test_pr03_task4.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`. `S3SingleAttemptClient.*` is outside the `CAS*` gate. It is
listed here because Step 1 changed the server those tests use. If the new test fails on the base, stop
and report its output.

- [ ] **Step 4: Commit**

```bash
git add -- src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp
git commit -F - <<'EOF'
Test that a large CAS conditional PUT is one request with the whole body

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 5: A conditional `PUT` runs on the calling thread (red on base)

Consumes: Task 4's helpers. Produces: `ws.s3_allow_parallel_part_upload = false` in
`ObjectStorageBackend::conditionalWriteSettings`.

**Files:** `src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp`, `CA/Backend/CasObjectStorageBackend.cpp`,
`CA/Backend/CasObjectStorageBackend.h`

- [ ] **Step 1: The failing test**

Add after `#include <Common/RemoteHostFilter.h>`:

```cpp
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
```

Add `#include <exception>` after `#include <chrono>`. Before `namespace` (the anonymous one), add:

```cpp
namespace ProfileEvents
{
extern const Event S3PutObject;
}
```

Before the final `#endif`, add:

```cpp
/// A conditional `PUT` is sent by the thread that asked for it, so a memory guard that thread holds
/// covers the upload. `S3PutObject` is counted on the thread that sends the request.
TEST(CASBackend, ConditionalPutRunsOnTheCallingThread)
{
    (void)contextForTest();

    DelayedResponseServer server(std::chrono::milliseconds(0), acceptPut);
    auto backend = conditionalBackendForTest(makeDispatchStorageForTest(server.getUrl(), 10000));
    EXPECT_FALSE(backend->conditionalWriteSettingsForTest().s3_allow_parallel_part_upload);

    DB::Cas::CasRequests requests(DB::Cas::BackendPtr(backend), DB::Cas::Fence::open());
    bool committed = false;
    uint64_t puts_on_calling_thread = 0;
    std::exception_ptr failure;
    ThreadFromGlobalPool caller([&]
    {
        try
        {
            const uint64_t before = DB::CurrentThread::getProfileEvents()[ProfileEvents::S3PutObject].load();
            auto op = requests.admit();
            committed = std::holds_alternative<DB::Cas::Committed>(op.create("put-key", "body", DB::Cas::Retry::once()));
            puts_on_calling_thread = DB::CurrentThread::getProfileEvents()[ProfileEvents::S3PutObject].load() - before;
        }
        catch (...)
        {
            failure = std::current_exception();
        }
    });
    caller.join();
    if (failure)
        std::rethrow_exception(failure);

    EXPECT_TRUE(committed);
    EXPECT_EQ(server.requestsSeen(), 1u);
    EXPECT_EQ(puts_on_calling_thread, 1u) << "the conditional PUT was sent by another thread";
}
```

- [ ] **Step 2: Run — expect FAIL**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task5_red.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASBackend.ConditionalPutRunsOnTheCallingThread' > build/test_pr03_task5_red.log 2>&1; echo TEST_EXIT=$?
grep -a -A4 'Failure' build/test_pr03_task5_red.log
```

Expect `TEST_EXIT=1` and two failures:
- `Value of: backend->conditionalWriteSettingsForTest().s3_allow_parallel_part_upload`, `Actual: true`.
- `puts_on_calling_thread`, `Which is: 0` against `1u`, with "the conditional PUT was sent by another
  thread".

`committed` and `requestsSeen` pass. Paste both failures into the report.

- [ ] **Step 3: The setting**

In `ObjectStorageBackend::conditionalWriteSettings` (`CA/Backend/CasObjectStorageBackend.cpp`), after
`ws.s3_check_objects_after_upload_override = false;`, add:

```cpp
    /// Upload on the calling thread: a memory guard the caller holds then covers the upload, and the
    /// caller waits for the write anyway.
    ws.s3_allow_parallel_part_upload = false;
```

In `CA/Backend/CasObjectStorageBackend.h`, in the doc comment of `conditionalWriteSettings`, replace
`existence/size check, and force a single PUT on generation stores because GCS does not` with
`existence/size check, upload on the calling thread, and force a single PUT on generation stores because GCS does not`.

- [ ] **Step 4: Run again — expect PASS**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task5.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASBackend.*:CASEnvelopeWiring.*' > build/test_pr03_task5.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`.

- [ ] **Step 5: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h \
    src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp
git commit -F - <<'EOF'
Run CAS conditional PUTs on the calling thread

A conditional PUT ran on the remote-FS writer pool, outside the memory
guard of the mount-lease thread.

Related: https://github.com/Altinity/ClickHouse/issues/2403

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 6: The conditional `PUT` buffer starts at the body size (red on base)

Consumes: Task 4's helpers. Produces: the `nativeConditionalPut` buffer size.

**Files:** `src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp`, `CA/Backend/CasObjectStorageBackend.cpp`

- [ ] **Step 1: The failing test**

Make `makeDispatchStorageForTest` a template over the storage type. Replace its signature line with:

```cpp
template <typename Storage = DB::S3ObjectStorage>
std::shared_ptr<Storage> makeDispatchStorageForTest(const std::string & endpoint, long base_request_timeout_ms)
```

and its `return std::make_shared<DB::S3ObjectStorage>(` with `return std::make_shared<Storage>(`.

Add `#include <vector>` after `#include <variant>`. In the anonymous namespace, before `acceptPut`, add:

```cpp
/// A genuine `S3ObjectStorage` that remembers the buffer size each `writeObject` was opened with.
class BufferSizeRecordingS3ObjectStorage final : public DB::S3ObjectStorage
{
public:
    using DB::S3ObjectStorage::S3ObjectStorage;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        opened_buffer_sizes.push_back(buf_size);
        return DB::S3ObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    std::vector<size_t> opened_buffer_sizes;
};
```

Before the final `#endif`, add:

```cpp
/// A conditional `PUT` opens its buffer at the body size, and an empty body at one byte, which is what
/// `WriteBuffer::write` needs to accept zero bytes.
TEST(CASBackend, ConditionalPutBufferStartsAtTheBodySize)
{
    (void)contextForTest();

    DelayedResponseServer server(std::chrono::milliseconds(0), acceptPut);
    auto storage = makeDispatchStorageForTest<BufferSizeRecordingS3ObjectStorage>(server.getUrl(), 10000);
    auto backend = conditionalBackendForTest(storage);
    DB::Cas::CasRequests requests(DB::Cas::BackendPtr(backend), DB::Cas::Fence::open());

    const std::string body(37, 'b');
    {
        auto op = requests.admit();
        EXPECT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("small-key", body, DB::Cas::Retry::once())));
    }
    {
        auto op = requests.admit();
        EXPECT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("empty-key", "", DB::Cas::Retry::once())));
    }
    EXPECT_EQ(storage->opened_buffer_sizes, (std::vector<size_t>{body.size(), 1}));
}
```

- [ ] **Step 2: Run — expect FAIL**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task6_red.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASBackend.ConditionalPutBufferStartsAtTheBodySize' > build/test_pr03_task6_red.log 2>&1; echo TEST_EXIT=$?
grep -a -A6 'Failure' build/test_pr03_task6_red.log
```

Expect `TEST_EXIT=1` and one failure: `storage->opened_buffer_sizes` is `{ 1048576, 1048576 }` against
`{ 37, 1 }`. Paste it into the report.

- [ ] **Step 3: The buffer size**

In `ObjectStorageBackend::nativeConditionalPut`, replace

```cpp
    auto buf = object_storage->writeObject(
        StoredObject(key), WriteMode::Rewrite, /*attributes=*/std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, ws);
```

with

```cpp
    /// Sized to the body, so a small control-plane write does not allocate a full default buffer. At
    /// least one byte, because `WriteBuffer::write` asserts a non-empty buffer.
    auto buf = object_storage->writeObject(
        StoredObject(key), WriteMode::Rewrite, /*attributes=*/std::nullopt, std::max<size_t>(1, bytes.size()), ws);
```

The file already includes `<algorithm>`.

- [ ] **Step 4: Run again — expect PASS, with the large-body golden test**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build unit_tests_dbms > build/ninja_pr03_task6.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASBackend.*:CASEnvelopeWiring.*:S3SingleAttemptClient.*' > build/test_pr03_task6.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`, `LargeConditionalPutIsOneRequestWithTheWholeBody` included.

- [ ] **Step 5: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp \
    src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp
git commit -F - <<'EOF'
Open the CAS conditional PUT buffer at the body size

A conditional PUT allocated a 1 MiB buffer for a body of a few hundred
bytes. It now starts at the body size, one byte for an empty body.

Related: https://github.com/Altinity/ClickHouse/issues/2403

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 7: User docs

**Files:** `docs/en/antalya/cas/architecture/mounts-and-leases.md`

- [ ] **Step 1: One paragraph**

After the paragraph that starts "The renewal and remount workers are separate and long-lived" and ends
"a newer request is processed before renewal resumes.", add a blank line and:

```markdown
The renewal worker does not fail on the server memory limit. Its allocations still count toward
`max_server_memory_usage`, but a server above that limit keeps renewing its mount lease. A
`std::bad_alloc` still ends the renewal. Conditional writes upload on the thread that issues them, with
a buffer of the object's size.
```

- [ ] **Step 2: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add -- docs/en/antalya/cas/architecture/mounts-and-leases.md
git commit -F - <<'EOF'
Document that the CAS mount renewal survives the server memory limit

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 8: Gates

- [ ] **Step 1: Release gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr03_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0` and `[  PASSED  ]` with no `[  FAILED  ]` line.

- [ ] **Step 2: ASan gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build_asan/.wok-build.lock ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr03.log 2>&1; echo NINJA_EXIT=$?
build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr03_gate.log 2>&1; echo TEST_EXIT=$?
build_asan/src/unit_tests_dbms --gtest_filter='S3SingleAttemptClient.*' > build_asan/test_pr03_s3client.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0` and both `TEST_EXIT=0`. The second run covers the tests outside `CAS*` whose server
Task 4 changed.

- [ ] **Step 3: Integration module, behaviour preservation**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
flock build/.wok-build.lock ninja -C build clickhouse > build/ninja_pr03_clickhouse.log 2>&1; echo NINJA_EXIT=$?
ln -sf "$(pwd)/build/programs/clickhouse" ci/tmp/clickhouse && ls -la ci/tmp/clickhouse
python3 -m ci.praktika run "integration" --test test_cas_mount_renewal_retry > build/it_pr03_renewal_retry.log 2>&1; echo IT_EXIT=$? >> build/it_pr03_renewal_retry.log
grep -a "passed\|failed\|PASSED\|FAILED\|ERROR\|IT_EXIT" build/it_pr03_renewal_retry.log | tail -20
```

The praktika exit code is not a verdict. Read the pytest summary line: every test of the module passed,
none failed or errored. This module is green on the base. PR 3 must not change it.

- [ ] **Step 4: The commit set**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git diff --stat 5b7c9f503df..HEAD
```

Expect exactly six files: `CasMountRuntime.cpp`, `CasObjectStorageBackend.cpp`,
`CasObjectStorageBackend.h`, `gtest_cas_pool.cpp`, `gtest_cas_s3_single_attempt_client.cpp`,
`mounts-and-leases.md`.

## PR text

Title: `Keep the CAS mount lease renewing above the server memory limit`

Body, per `.github/PULL_REQUEST_TEMPLATE.md`:

```
A CAS mount lost its lease when the server was above `max_server_memory_usage`. The renewal thread has
no thread group, so the global tracker threw `MEMORY_LIMIT_EXCEEDED` on it. A throw outside the
request ended the thread for good. The renewal thread now holds
`LockMemoryExceptionInThread(VariableContext::Global)` for its whole life. The tracker still counts its
allocations. A conditional `PUT` now runs on the calling thread, so the guard covers the upload. Its
buffer starts at the body size, not 1 MiB. `std::bad_alloc` still ends the renewal.

Closes: https://github.com/Altinity/ClickHouse/issues/2403

Mixed versions: no on-storage or wire change; nodes of either version interoperate.

### Changelog category (leave one):
- Bug Fix (user-visible misbehavior in an official stable release)

### Changelog entry:
A content-addressed disk no longer loses its mount lease when the server is above `max_server_memory_usage`.

### Documentation entry for user-facing changes
- [x] Documentation is written (`docs/en/antalya/cas/architecture/mounts-and-leases.md`)
```
