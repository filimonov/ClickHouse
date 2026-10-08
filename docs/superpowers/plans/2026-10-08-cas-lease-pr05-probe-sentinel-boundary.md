# CAS lease PR 5: `probeSentinel` returns the last `Indeterminate` at the deadline — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** after at least one probe has run, a retry pause that ends exactly on the policy deadline
(zero attempt reservation) makes `CasOperation::probeSentinel` return the last `Indeterminate` it
received instead of throwing through `giveUpReadDeadline`. A lost fence and a spent lease budget stay
separate errors, and a deadline reached before the first probe still throws. Closes CAS-325.

**Architecture:** the reissue loop in `CasOperation::probeSentinel` (`CA/Backend/CasRequests.cpp`)
checks `fits(pause + reservation)` before the sleep, then `fits(reservation)` at the loop top. With a
zero reservation and a pause equal to the remaining window, the first check passes (`needed <=
remaining`), the sleep ends at `now == deadline`, and the loop-top check fails and throws. The fix
keeps the last probe result and returns it from the loop-top deadline branch. The admission `gate`
stays first at the loop top, so fence loss and lease-budget refusal still throw.

**Why the loop-top form and not a strict `<` before the sleep:** the backlog task allows either. A
strict check covers only an exact landing; a sleep that overshoots by more than the reservation (a real
sleep can) reaches the same throw. The loop-top form covers both with one branch.

**Tech stack:** C++23, gtest (`unit_tests_dbms`), injected `FakeClock` and `Fence`.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`,
section 7 "Серия PR", row 5 in both tables (PR row: "`probeSentinel`: пауза повтора, упёршаяся в
дедлайн, возвращает последний `Indeterminate`", +10, closes CAS-325; tests row: "инжектированные часы,
нулевой резерв: после probe конец бюджета возвращает `Indeterminate`"). Section 9 "Модели" names no
model for PR 5.

**Backlog task:** `docs/superpowers/cas/backlog/tasks/cas-325 - ...md` (in the master worktree).
Acceptance: (1) the loop returns the last `Indeterminate` when a pause ends exactly at the deadline;
(2) a deterministic gtest where the last pause equals the remaining window; (3)
`CASSentinelProbe.NativeClassifiesUnmodeledErrorAsIndeterminate` passes 100,000 repeats.

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

- The loop-top `gate` still runs BEFORE the deadline branch: a refusal after a probe must throw, not
  return the stored outcome. `ProbeSentinelStillThrowsARefusalAfterAPauseEndsOnTheDeadline` fences it.
- The deadline branch returns a stored value only when a probe ran. Before the first probe it still
  throws `giveUpReadDeadline` with `0 attempt(s)`. `ProbeSentinelWithNoWindowThrowsBeforeAnyProbe`
  fences it.
- No request is sent after the deadline: the regression test asserts the probe count equals the
  sleep count (the landing pause is not followed by a probe).
- Out of scope, deliberately: `removeCurrent` and `readLoop` have the same check shape, but they throw
  on give-up by contract; there only the reported attempt count could differ. Not changed here.

## Files

| File | Change |
|---|---|
| `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp` | `CasOperation::probeSentinel`: keep the last probe result; the loop-top deadline branch returns it when present |
| `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h` | doc comment of `probeSentinel`: say which refusals throw |
| `src/Disks/tests/gtest_cas_requests.cpp` | fixture `PauseLandsOnDeadline`; three tests |

No file outside `CA/` and the gtest directory is touched. `CA/Backend/CasRetry.*` is read, not changed.

---

## Task 1: Branch

- [ ] **Step 1: Create the branch from the base**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git status --short -- src/Disks    # expect no output: no local edits under src/Disks
git switch -c cas-lease-pr05-probe-sentinel-boundary 5b7c9f503df
```

If `git status` shows edits under `src/Disks`, stop and report; they belong to another agent.

## Task 2: Regression test — the pause lands on the deadline (red on base)

**Files:** `src/Disks/tests/gtest_cas_requests.cpp`.

**Consumes:** `IndeterminateProbeBackend` (same file, anonymous namespace; field
`indeterminate_answers`, counter `probes`), `makeRequests(BackendPtr, FakeClock &, Fence)` (same file),
`FakeClock` (`src/Disks/tests/cas_test_helpers.h`; public `now`, `sleeps`),
`CasRequests::setAttemptReservationForTest`, `Fence` (`CA/Backend/CasFence.h`).

**Produces:** struct `PauseLandsOnDeadline` (used by Task 4) and test
`CASRequests.ProbeSentinelReturnsTheLastIndeterminateWhenAPauseEndsOnTheDeadline`.

How the test makes the landing exact without controlling the jitter: with a zero attempt reservation,
the loop-top gate asks `Fence::admit` for `needed_ms == 0` and the reissue gate asks for `needed_ms ==
pause`. The fixture's `admit` sees the first non-zero `needed_ms`, moves the clock to `deadline - pause`
and returns `Ok`. The following `fits(pause)` passes with `pause == remaining`, `FakeClock`'s sleep
advances by `pause`, and the loop top sees `now == deadline`. A zero jitter draw reserves nothing, so
the fixture waits for the next draw; the test asserts that a landing happened.

- [ ] **Step 1: Add the fixture**

In `src/Disks/tests/gtest_cas_requests.cpp`, inside the anonymous namespace, directly after
`struct IndeterminateProbeBackend { ... };`, insert:

```cpp
/// With a zero attempt reservation only a reissue gate asks the fence for a non-zero budget, and what
/// it asks for is the pause the loop is about to sleep. The first such gate moves the clock so the
/// window left equals that pause, which lands the sleep exactly on the deadline whatever the jitter
/// drew. Every later `admit` answers `after_landing`.
struct PauseLandsOnDeadline
{
    FakeClock & clock;
    uint64_t deadline_ms;
    Fence::Admit after_landing = Fence::Admit::Ok;
    std::optional<uint64_t> landed_pause_ms;

    Fence fence()
    {
        return Fence{
            []() -> uint64_t { return 0; },
            [this](uint64_t, uint64_t needed_ms)
            {
                if (landed_pause_ms)
                    return after_landing;
                if (needed_ms > 0)
                {
                    clock.now.store(deadline_ms - needed_ms);
                    landed_pause_ms = needed_ms;
                }
                return Fence::Admit::Ok;
            },
            [](uint64_t) {}};
    }
};
```

- [ ] **Step 2: Add the test**

Directly after `TEST(CASRequests, ProbeSentinelRetriesOnlyTheIndeterminateOutcome) { ... }`, insert:

```cpp
TEST(CASRequests, ProbeSentinelReturnsTheLastIndeterminateWhenAPauseEndsOnTheDeadline)
{
    FakeClock clock;
    auto backend = std::make_shared<IndeterminateProbeBackend>();
    backend->indeterminate_answers = std::numeric_limits<size_t>::max();
    PauseLandsOnDeadline landing{.clock = clock, .deadline_ms = clock.now.load() + 10'000};
    auto requests = makeRequests(backend, clock, landing.fence());
    requests.setAttemptReservationForTest(0);
    auto op = requests.admit();

    const SentinelProbeResult result = op.probeSentinel("k", Retry::within(10'000));
    EXPECT_EQ(result.outcome, ProbeOutcome::Indeterminate);
    ASSERT_TRUE(landing.landed_pause_ms.has_value());
    EXPECT_EQ(clock.now.load(), landing.deadline_ms);
    ASSERT_FALSE(clock.sleeps.empty());
    EXPECT_EQ(clock.sleeps.back(), *landing.landed_pause_ms);
    /// The last pause ended on the boundary, so no probe followed it.
    EXPECT_EQ(backend->probes, clock.sleeps.size());
}
```

`<limits>` and `<optional>` are already included by this file.

- [ ] **Step 3: Build and run; expect the failure**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr05_task2.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASRequests.ProbeSentinelReturnsTheLastIndeterminateWhenAPauseEndsOnTheDeadline' \
    > build/test_pr05_task2_red.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=1`, and in the log:

```
C++ exception with description "CAS write could not be committed (probeSentinel of 'k': gave up at the policy deadline after N attempt(s)); retrying later" thrown in the test body.
```

`N` is the number of probes made, at least 1. Paste the line into the task report. Do not commit yet:
Task 3 commits the test together with the fix, so the branch never holds a red commit.

## Task 3: The fix

**Files:** `CA/Backend/CasRequests.cpp`, `CA/Backend/CasRequests.h`.

**Consumes:** `CasOperation::gate`, `CasOperation::fits`, `CasOperation::giveUpReadDeadline`.

**Produces:** `CasOperation::probeSentinel` with the new loop-top deadline branch; the signature is
unchanged.

- [ ] **Step 1: Change `probeSentinel`**

In `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp`, function
`CasOperation::probeSentinel`, apply:

```diff
     const Retry::Bound bound = policy.bind(owner.now_ms());
     bool refresh_attempted = false;
+    std::optional<SentinelProbeResult> last_probe;
     for (uint32_t attempt = 1;; ++attempt)
     {
         const uint64_t reservation = reservedFor(0, 1);
         switch (gate(reservation))
         {
             case Gate::FenceLost: giveUpReadFenceLost("probeSentinel", key, "before the request");
             case Gate::NoBudget:  giveUpReadNoBudget("probeSentinel", key, "for one more request");
             case Gate::Ok: break;
         }
-        /// Before the first attempt nothing has been probed, so the caller is told the request never
-        /// happened rather than being handed an outcome no request produced.
         if (!fits(reservation, bound))
-            giveUpReadDeadline("probeSentinel", key, bound, attempt - 1);
+        {
+            /// A pause that fit can still end on the boundary. Once a probe has run, its outcome is the
+            /// answer; before the first one the caller is told no request happened.
+            if (last_probe)
+                return *last_probe;
+            giveUpReadDeadline("probeSentinel", key, bound, attempt - 1);
+        }
```

and further down, in the same function:

```diff
         if (!fits(needed, bound))
             return result;
+        last_probe = std::move(result);
         detail::recordReissue();
         owner.sleep_ms(pause_ms);
```

- [ ] **Step 2: Correct the header contract**

In `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h`, the doc
comment above `SentinelProbeResult probeSentinel(const String & key, const Retry & policy);`:

```diff
     /// The one primitive that reports failure as a value, so the policy reissues on the OUTCOME:
     /// `Indeterminate` is retried, the four authoritative outcomes return at once, and an
-    /// `Indeterminate` that outlives the bound is returned rather than thrown. Admission refused before
-    /// the first attempt still throws -- nothing was probed.
+    /// `Indeterminate` that outlives the bound is returned rather than thrown. A lost fence or a spent
+    /// lease budget throws at any point, and so does a deadline reached before the first probe.
```

- [ ] **Step 3: Build and run; expect green**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr05_task3.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASRequests.ProbeSentinel*:CASSentinelProbe.*' \
    > build/test_pr05_task3.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`, and `[  PASSED  ]` for
`CASRequests.ProbeSentinelReturnsTheLastIndeterminateWhenAPauseEndsOnTheDeadline` in the log.

- [ ] **Step 4: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp \
           src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h \
           src/Disks/tests/gtest_cas_requests.cpp
git commit -F - <<'EOF'
Return the last Indeterminate when a probe pause ends on the deadline

With a zero attempt reservation, a retry pause equal to the remaining
window ends at `now == deadline`, and the loop-top check of
`probeSentinel` threw `giveUpReadDeadline` although a probe had run.
Return that probe's outcome instead. Fence loss and a spent lease
budget still throw.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 4: Preservation tests — refusals and the no-probe deadline still throw (green on base)

**Files:** `src/Disks/tests/gtest_cas_requests.cpp`.

**Consumes:** `PauseLandsOnDeadline` (Task 2), `DB::Cas::tests::expectThrowsCodeWithMessage`
(`src/Disks/tests/cas_test_helpers.h`), `DB::ErrorCodes::NETWORK_ERROR` (already declared in this file).

**Produces:** tests `CASRequests.ProbeSentinelStillThrowsARefusalAfterAPauseEndsOnTheDeadline` and
`CASRequests.ProbeSentinelWithNoWindowThrowsBeforeAnyProbe`.

Both are behaviour-preservation tests: green on the base and green on the branch. They fail if the
stored outcome were returned before the `gate` (first test) or without a probe having run (second).

- [ ] **Step 1: Add the tests**

Add `using DB::Cas::tests::expectThrowsCodeWithMessage;` next to the existing
`using DB::Cas::tests::expectThrowsCode;` near the top of the file. Then, directly after the test from
Task 2, insert:

```cpp
TEST(CASRequests, ProbeSentinelStillThrowsARefusalAfterAPauseEndsOnTheDeadline)
{
    const auto expect_refusal = [](Fence::Admit refusal, const String & expected_message)
    {
        FakeClock clock;
        auto backend = std::make_shared<IndeterminateProbeBackend>();
        backend->indeterminate_answers = std::numeric_limits<size_t>::max();
        PauseLandsOnDeadline landing{.clock = clock, .deadline_ms = clock.now.load() + 10'000, .after_landing = refusal};
        auto requests = makeRequests(backend, clock, landing.fence());
        requests.setAttemptReservationForTest(0);
        auto op = requests.admit();

        expectThrowsCodeWithMessage(DB::ErrorCodes::NETWORK_ERROR, expected_message,
                                    [&] { (void)op.probeSentinel("k", Retry::within(10'000)); });
        EXPECT_TRUE(landing.landed_pause_ms.has_value());
    };
    expect_refusal(Fence::Admit::LostOrRearmed, "mount fence tripped before the request");
    expect_refusal(Fence::Admit::NoBudget, "no lease budget for one more request");
}

TEST(CASRequests, ProbeSentinelWithNoWindowThrowsBeforeAnyProbe)
{
    FakeClock clock;
    auto backend = std::make_shared<IndeterminateProbeBackend>();
    auto requests = makeRequests(backend, clock);
    requests.setAttemptReservationForTest(0);
    auto op = requests.admit();

    expectThrowsCodeWithMessage(DB::ErrorCodes::NETWORK_ERROR, "gave up at the policy deadline after 0 attempt(s)",
                                [&] { (void)op.probeSentinel("k", Retry::within(0)); });
    EXPECT_EQ(backend->probes, 0u);
}
```

- [ ] **Step 2: Show the tests green on the base code**

Revert only the production change in the working tree, build, run, restore:

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
C=src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend
git diff 5b7c9f503df -- $C/CasRequests.cpp $C/CasRequests.h > build/pr05_fix.diff
git checkout 5b7c9f503df -- $C/CasRequests.cpp $C/CasRequests.h
ninja -C build unit_tests_dbms > build/ninja_pr05_task4_base.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASRequests.ProbeSentinelStillThrows*:CASRequests.ProbeSentinelWithNoWindow*' \
    > build/test_pr05_task4_base.log 2>&1; echo TEST_EXIT=$?
git checkout HEAD -- $C/CasRequests.cpp $C/CasRequests.h
git diff --quiet HEAD -- $C/CasRequests.cpp $C/CasRequests.h && echo RESTORED
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0` (both tests pass on the base code), then `RESTORED`.

- [ ] **Step 3: Run on the branch code**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr05_task4.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASRequests.ProbeSentinel*' > build/test_pr05_task4.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`, five `CASRequests.ProbeSentinel*` tests passed.

- [ ] **Step 4: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add -- src/Disks/tests/gtest_cas_requests.cpp
git commit -F - <<'EOF'
Test that probeSentinel refusals and no-probe deadline still throw

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
```

## Task 5: Gates

- [ ] **Step 1: The flaky test, 100,000 repeats (backlog acceptance 3)**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
build/src/unit_tests_dbms --gtest_filter='CASSentinelProbe.NativeClassifiesUnmodeledErrorAsIndeterminate' \
    --gtest_repeat=100000 --gtest_brief=1 > build/test_pr05_repeat.log 2>&1; echo TEST_EXIT=$?
grep -c '\[  FAILED  \]' build/test_pr05_repeat.log
```

Expect `TEST_EXIT=0` and a count of `0`. On the base this test failed about 6 times in 10,000 repeats.

- [ ] **Step 2: Release gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr05_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0` and `[  PASSED  ]` with no `[  FAILED  ]` line.

- [ ] **Step 3: ASan gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr05.log 2>&1; echo NINJA_EXIT=$?
build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr05_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`.

- [ ] **Step 4: Confirm the commit set**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git diff --stat 5b7c9f503df..HEAD
```

Expect exactly three files: `CasRequests.cpp`, `CasRequests.h`, `gtest_cas_requests.cpp`.

## PR text

Title: `Return the last Indeterminate when a probe pause ends on the deadline`

Body, per `.github/PULL_REQUEST_TEMPLATE.md`:

```
`probeSentinel` threw `giveUpReadDeadline` when a retry pause ended exactly on the policy deadline,
although a probe had already run. It happens with a zero attempt reservation and a jitter draw equal
to the remaining window; it showed up as a rare failure of
`CASSentinelProbe.NativeClassifiesUnmodeledErrorAsIndeterminate`. The loop now returns the last
`Indeterminate`. A lost fence, a spent lease budget and a deadline before the first probe still throw.

Mixed versions: no on-storage or wire change; nodes of either version interoperate.

### Changelog category (leave one):
- Bug Fix (user-visible misbehavior in an official stable release)

### Changelog entry:
A content-addressed disk sentinel probe no longer fails with a retry-later error when its last retry
pause ends exactly on the deadline; it reports the inconclusive outcome instead.
```
