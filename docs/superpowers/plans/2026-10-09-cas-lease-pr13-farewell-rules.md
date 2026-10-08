# CAS lease PR 13: farewell rules — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** the clean farewell is written only on a silent mount (no ref lane writing or wedged, no snapshot
publish in flight, no undrained cleanup duty, no renewal ended without a confirmed lease, not `stopping`, not
after `closed`), and after the lease expired too: it is conditional on our own token. The unclean line names
its causes. **Architecture:** `drainRefLanesForShutdown` returns `RefLaneDrain` and also waits for
`pending_snapshot_publishes`; `Pool::drainForShutdown` maps it to `FarewellBlockers`; `finishTeardown` writes
the farewell or one `WARNING` with every cause; `MountLeaseRenewer::terminate` drops its lease bound.
**Tech stack:** C++23, gtest, `CountingBackend`, TLA+ (`CaDiskLifecycle`).
**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
§7 row 13 (both tables), «Shutdown», §9 «модель farewell»; backlog CAS-179 (both AC), CAS-52 AC #3 (wording only).
**Base:** PR 12's final head (`fix/antalya-26.6/cas-terminal-losses-stop`, `02ed2c6825a` at plan time).
**Branch:** `fix/antalya-26.6/cas-farewell-rules`. `$WT` = the executor's own worktree (never
`cas-janitor-budget`); `$CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`. Every commit
ends with `Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>` then
`Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
**Files outside `$CA`:** none in production; tests in `src/Disks/tests/`, the doc
`docs/en/antalya/cas/architecture/mounts-and-leases.md`, the model in the master worktree. ≈ +70 production lines.

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
- Every build, test and gate command runs with `TMPDIR=<build_dir>/tmp`.
- New source files are globbed: run `cmake .` in `$WT/build` and `$WT/build_asan` after creating one.
- TLC: `timeout 1200`, 4 workers, `-Xmx8g`, `nice -n 10`, and the disk guard of Task 1.

## Review Focus

- **Late farewell:** safe by `precondition()` (our last ETag) alone: a GC fence or a successor moves the token,
  the write gets `Conflict`. The decommission admin's farewell (`Tools/CasDecommission.cpp`) loses the bound too.
- **Publish wait:** `publish_settle_cv.wait_until` under `rt->state_mutex`, on the lane wait's deadline. No
  admission gate: FORGET trips the fence first, and `maybeScheduleSnapshotPublish`/`settleSnapshotPublish` check it.
- **Fail closed** in `~Pool`: `FarewellBlockers blockers{.drain_failed = true}` until the drain returns.

## Code vs spec (flagged; the plan follows the spec)

1. §9 puts the farewell model in `CaCasMountCore`; the dispatch says `CaDiskLifecycle` (teardown, drain,
   renewer, expiry, `Stopping` live there). Plan: `CaDiskLifecycle`; late-farewell slot safety is unit-tested only.
2. «пишется … и после истечения lease тоже» vs `terminate`'s `Retry::untilLeaseSafe(confirmed_deadline_boot_ms, …)`,
   pinned by `CASHeartbeat.FarewellIsRefusedWhenTheLeaseExpiresBeforeItsDerivedWindow`. Plan replaces that test.
3. «В `stopping` и `closed` farewell не пишется» vs FORGET writing it before `enterVanished(VanishedForgotten)`
   publishes `closed` (model `witness_forgetfarewell`). Plan: none after `closed` (golden); FORGET's stays.
4. Row says +30; typed causes and the publish wait make ≈ +70.
5. «неясный запрос» vs code: every `RenewalTerminal` withholds it, a definite `Refused` too. Kept; worded
   "the last renewal ended without a confirmed lease".
6. CAS-179 also asks to bound `quiesceRefTablesForRemount`'s publish wait: out of scope (PR 15 removes it).
7. CAS-179 names `~Pool`; `tryDispatchDetached` gives each publisher a `DetachedTaskLease` on
   `shared_from_this()`, so `~Pool` cannot start while one runs. FORGET is the reachable case; tests drive it.

## Setup

Branch from PR 12's final head, `cmake .` in both build dirs, build, run the release `CAS*` gate: the green baseline count.

## Task 1: model first (master worktree)

**Files:** `docs/superpowers/models/CaDiskLifecycle.tla`, all `CaDiskLifecycle_*.cfg`, four new cfgs (below),
`run_disklifecycle.sh`, `CaDiskLifecycle_RESULTS.md`. **Precondition:** `git status --short
docs/superpowers/models/CaDiskLifecycle*` in the master worktree is empty, else stop. **Disk guard** before
each run: `[ "$(df --output=avail -BG …/master/tmp | tail -1 | tr -dc 0-9)" -ge 20 ] || exit 1`. **Run:**
`TLC_TIMEOUT=1200 TLC_WORKERS=4 TLC_JAVA_OPTS=-Xmx8g nice -n 10 ./run_disklifecycle.sh > ../../../tmp/run_disklifecycle_pr13.log 2>&1`.
- [ ] Variables `publishing` (a snapshot publish of this incarnation in flight), history `lateLanding`,
  `farewellExpired`, `renewUnclear`, grouped as `farewellVars`; in `vars`, `TypeOK` (`BOOLEAN`), `Init`
  (`FALSE`), and the `UNCHANGED` of every action not named here (TLC's "not completely specified" error checks it).
  `PubBegin == ~publishing /\ ~lost /\ pstate = "Live" /\ publishing' = TRUE`;
  `PubSettle == publishing /\ publishing' = FALSE /\ lateLanding' = (lateLanding \/ farewell)` (in `Next`,
  not fair). `LRenewEndEarly`: `renewUnclear' = TRUE`; `LReclaim`: `renewUnclear' = FALSE`; `FTeardown`
  writing the farewell: `farewellExpired' = ~deadlineOk`. Invariants `NoLandingAfterFarewell == ~lateLanding`,
  `NoFarewellAfterUnclearRenewal == ~(farewell /\ renewUnclear)` (both into `main.cfg`); witness
  `WNoFarewellAfterExpiry == ~farewellExpired`.
- [ ] Red first: `ROWS=main` → `violation:NoLandingAfterFarewell`. Paste the trace.
- [ ] Constants `DrainWaitsPublish`, `FarewellLeaseBound`, `SabFarewellFromTerminal` (existing cfgs: `TRUE`,
  `FALSE`, `FALSE`). `FDrain`: `\E d \in {"Yes", "No"} : drained' = d /\ ((d = "Yes" /\ DrainWaitsPublish) => ~publishing)`.
  `FTeardown`'s farewell condition gains `(keeper = "Active" \/ (SabFarewellFromTerminal /\ keeper = "Terminal"))`
  in place of `keeper = "Active"`, and `(deadlineOk \/ ~FarewellLeaseBound)`.
- [ ] Rows (main's constants unless named, `SPECIFICATION Spec`), in `CONFIGS` and the script's header table:
  `sab_drainignorespublish` (`DrainWaitsPublish = FALSE`) → violation `NoLandingAfterFarewell`;
  `sab_farewellfromterminal` (`SabFarewellFromTerminal = TRUE`) → violation `NoFarewellAfterUnclearRenewal`;
  `witness_farewellafterexpiry` → violation `WNoFarewellAfterExpiry`; `old_farewellleasebound`
  (`FarewellLeaseBound = TRUE`, `INVARIANT WNoFarewellAfterExpiry`) → green.
- [ ] Full run: `ALL EXPECTATIONS MET`. Dated RESULTS section: rows, counts, sabotage traces; not checked:
  slot bodies (late-farewell safety), the `~Pool` teardown (only FORGET's is modelled), "none after `closed`"
  (one teardown, structural). Commit the exact files: `model(cas): farewell waits for publishes; written after expiry`.

## Task 2: goldens (green before and after)

**Files:** `src/Disks/tests/gtest_cas_heartbeat.cpp`, create `src/Disks/tests/gtest_cas_farewell_rules.cpp`
(then `cmake .` in both build dirs).
- [ ] `gtest_cas_heartbeat.cpp` (add `<Common/getFQDNOrHostName.h>`, `<unistd.h>` if absent):
  `CASHeartbeat.FarewellBodyGolden` — the fixture of `StopStampsExpiredAndFarewellSentinel` (ttl 100,
  margin 0, epoch 9, `now_ms = 1000`); read the slot as `before`; `now_ms = 2000; renewer.release();` then
  expect `server_uuid == uuid`, `writer_epoch == 9`, `seq == before.seq + 1`, `started_at_ms == expires_at_ms == 2000`,
  `min_active_build_sequence == UINT64_MAX`, `!gc_fenced`, `hostname == DB::getFQDNOrHostName()`,
  `pid == ::getpid()`, `write_attempt_id != before.write_attempt_id`. Comment:
  `/// Every field of the clean farewell, so a change to its rules cannot change its bytes.`
- [ ] New file `gtest_cas_farewell_rules.cpp`, `using namespace DB::Cas;`, anonymous namespace:
  `kTerminated = std::numeric_limits<uint64_t>::max()`; `uint64_t farewellSentinel(Backend & b, const String & key)`
  (`tests::OperationForTest op(b)`, `(*op).read(key, Retry::once())`; `0` when absent, else the decoded
  `min_active_build_sequence`); `ScopedPoolLogCapture` — `ScopedParkedRenewalLogCapture` of `gtest_cas_pool.cpp`
  on logger `"CasPool"`; `PoolPtr openPool(const std::shared_ptr<tests::CountingBackend> &, uint64_t attempt_ms, bool publish_every_mutation)`
  (`"p"`/`"test"`, `attempt_timeout_ms = lease_safety_margin_ms = attempt_ms`, `connect_timeout_cap_ms = 0`,
  `backend->setAttemptTimeoutMs(attempt_ms)`; publishing: `snapshot_log_count_threshold = 0`,
  `snapshot_log_bytes_threshold = 1ULL << 40`); copies of `publishRef` (`gtest_cas_detached_work.cpp`) and
  `stageEmptyManifest`/`publishEmptyRef` (`gtest_cas_writer_duties.cpp`). Includes: `CasPool.h`,
  `CasPartWriteTxn.h`, `CasServerRootFormats.h`, `cas_test_helpers.h`, `cas_process_stop_log.h`,
  `<Poco/StreamChannel.h>`, `<base/scope_guard.h>`, `<sstream>`. Tests, each on
  `tests::openPoolForTest(CountingBackend)` with `key = mountKey("test")`:
  `AStoppingPoolWritesNoFarewellAtTeardown` — `tests::processStopLog().reset()`,
  `ASSERT_TRUE(store->latchMountEndForTest(MountEnd::SlotCorrupt))`, `before = putOverwriteCount(key)`,
  `store.reset()`, expect `putOverwriteCount(key) == before`;
  `AClosedPoolWritesNoSecondFarewell` — `forgetDisk({}, "r")`, assert `VanishedForgotten` and
  `putOverwriteCount(key) == before + 1` (FORGET's one farewell), `store.reset()`, expect still `before + 1`.
- [ ] Run `--gtest_filter='CASHeartbeat.FarewellBodyGolden:CASFarewellRules.*'` → 3 pass. Mutation: delete the
  `isLossEnd(mountEnd())` return in `CasMountRuntime::finishTeardown` → `AStoppingPoolWritesNoFarewellAtTeardown`
  fails; revert. No mutation turns `AClosedPoolWritesNoSecondFarewell` red; say so in the PR.
  Commit: `Pin the CAS farewell body and the stopping and closed teardowns`.

## Task 3: the farewell after the lease expired

**Files:** `$CA/Pool/CasServerRoot.cpp`, `$CA/Backend/CasRetry.h`, `src/Disks/tests/gtest_cas_heartbeat.cpp`,
`src/Disks/tests/gtest_cas_farewell_rules.cpp`.
- [ ] In `gtest_cas_heartbeat.cpp` delete `FarewellIsRefusedWhenTheLeaseExpiresBeforeItsDerivedWindow` with
  its comment and add, in its place, two tests with the same fixture (`DefaultEnvelopeBackend`, ttl 5000,
  margin 2000, epoch 9, `now_ms = 1000`, `boot_ms = 100`):
  `FarewellAfterTheLeaseExpiredIsWrittenUnderOurToken` — `boot_ms = 100 + 5000 + 1000; now_ms = 7000;`
  `EXPECT_NO_THROW(renewer.release())`, then `min_active_build_sequence == UINT64_MAX`, `expires_at_ms <= now_ms`;
  `AFarewellAfterExpiryLeavesAGcFenceUntouched` — before advancing, read the slot, set `gc_fenced = true` on
  its decoded body and `mustCommit(ops.op.replace(key, encodeMountLease(fenced), observed->etag, Retry::standard()), "GC fence")`;
  then the same clock advance, `EXPECT_NO_THROW(renewer.release())`, and the slot still decodes with
  `gc_fenced` and `min_active_build_sequence != UINT64_MAX`. Comment above the pair:
  `/// The farewell is conditional on our own token, so an expired lease does not forbid it: a moved slot refuses it.`
- [ ] `CASFarewellRules.ATeardownAfterTheLeaseExpiredStillWritesTheFarewell`: `CountingBackend`, `PoolConfig`
  `"p"`/`"test"` with `boot_ms_fn` reading a `shared_ptr<std::atomic<uint64_t>>` at `1'000'000`; `Pool::open`;
  `*boot += 31'000` (past the default 30 s lease, nothing renewed it); `store.reset()`; expect
  `farewellSentinel(*backend, key) == kTerminated`.

- [ ] Red: the three fail — heartbeat ones with `gave up at the lease deadline after zero attempt(s)`, the
  pool one with `0` vs `18446744073709551615`. Paste.
- [ ] In `MountLeaseRenewer::terminate` delete the paragraph from `/// The derived window alone is not enough:`
  through `` `start` alone establishes).`` and replace the write with:
```cpp
    /// No lease bound: the precondition is our own token, so a farewell sent after the lease ran out
    /// lands only if nobody moved the slot, and it spares the next start the observation window.
    WriteResult written = op.replace(key, body, precondition(), Retry::within(farewell_window_ms));
```
- [ ] In `CasRetry.h`, `untilLeaseSafe`'s comment: replace the sentence from `` `window_ms` defaults `` to
  `bounds is smaller.` with `` `window_ms` defaults to the standard 90 s write budget; `bind` takes whichever of the two bounds is smaller. ``
- [ ] Green; then `CASHeartbeat*:CASDecommission*:CASFarewellRules*:CASPoolShutdown*` green. Mutation: restore
  `Retry::untilLeaseSafe(…)` → the three red; revert. Commit: `Write the CAS mount farewell after the lease expired`.

## Task 4: typed teardown facts (behaviour-preserving)

**Files:** `$CA/Pool/CasRefLedger.h`, `$CA/Pool/CasRefLedger.cpp`, `$CA/Pool/CasMountRuntime.h`,
`$CA/Pool/CasMountRuntime.cpp`, `$CA/Pool/CasPool.h`, `$CA/Pool/CasPool.cpp`, `src/Disks/tests/gtest_cas_pool.cpp`.
**Produces:** `RefLaneDrain`, `FarewellBlockers`, `Pool::drainForShutdown`.
- [ ] `CasRefLedger.h`, before the class, `/// What `drainRefLanesForShutdown` could not settle by its deadline.`
  `struct RefLaneDrain { bool lane_unsettled = false; /* a queue not idle, or a lane writing or wedged */ bool snapshot_publish_in_flight = false; };`
  (Allman, one field per line, the comment as `///`); `RefLaneDrain drainRefLanesForShutdown(uint64_t wait_budget_ms);` and its body ends
  `return {.lane_unsettled = timed_out || any_wedge};`.
- [ ] `CasMountRuntime.h`, before `CasMountRuntime`, `struct FarewellBlockers` (Allman) with `bool`
  fields `drain_failed`, `ref_lane_unsettled`, `snapshot_publish_in_flight`, `cleanup_duty_pending`, all
  `= false`, and the comment `/// What the teardown found unsettled. The farewell certifies that no write of
  this incarnation lands after it, so any field set withholds it.` `void finishTeardown(const FarewellBlockers & blockers);`; in the body only
  `const bool drained = !(blockers.drain_failed || blockers.ref_lane_unsettled || blockers.snapshot_publish_in_flight || blockers.cleanup_duty_pending);`
  is new — the rest and the old `WARNING` text stay.
- [ ] `CasPool.h` private: `/// Drains the ref lanes and reports what still blocks the farewell.` `FarewellBlockers drainForShutdown();`.
  `CasPool.cpp`: it calls `ref_ledger.drainRefLanesForShutdown(attemptEnvelopeMs() + lease_safety_margin_ms)`
  (the budget both sites pass today) and returns `{.ref_lane_unsettled = drain.lane_unsettled,
  .snapshot_publish_in_flight = drain.snapshot_publish_in_flight, .cleanup_duty_pending = writerCleanupDutiesPending()}`.
  `~Pool` phase 2: replace `bool drained = false;` and its "False by default…" comment with
  `/// Fail closed: a drain that threw must never let the farewell through.` `FarewellBlockers blockers{.drain_failed = true};`,
  the lambda body becomes the test hook plus `blockers = drainForShutdown();`, phase 3 passes `blockers`.
  `forgetDisk` (5b)/(3+5c): `mount_runtime.finishTeardown(drainForShutdown());` (keep both comments).
- [ ] Tests: in `gtest_cas_pool.cpp` replace `finishTeardown(true)` → `finishTeardown({})` and
  `finishTeardown(false)` → `finishTeardown({.ref_lane_unsettled = true})`; re-grep
  `finishTeardown(\(true\|false\))` over `src/` → empty. The compiler is the gate for other callers.
- [ ] Build both dirs; `CAS*` release green, same count as Setup. Commit: `Type the CAS teardown facts that withhold the farewell`.

## Task 5: the unclean line names its causes

**Files:** `$CA/Pool/CasMountRuntime.cpp`, `src/Disks/tests/gtest_cas_farewell_rules.cpp`, `src/Disks/tests/gtest_cas_pool.cpp`.
- [ ] Tests (`ScopedPoolLogCapture log;` before the teardown; `text = log.captured()`):
  `CASFarewellRules.TheUncleanLineNamesAnUndrainedCleanupDuty` — `openPool(backend, 100, false)`, ns
  `srv1/cleanup_duty`, `publishEmptyRef(store, ns, "target")`, stage `durable`, `precommitAdd`, `reset`,
  `ASSERT_TRUE(store->writerCleanupDutiesPendingForTest())`, `forgetDisk({}, "r")`; expect `text` contains
  `CAS store shutdown without the clean-release marker: an undrained writer cleanup duty;` and not `ref-log`.
  `CASFarewellRules.TheUncleanLineNamesAWedgedLane` — same pool, ns `srv1/wedge`, `forceWedgeForTest(ns, 1, 1, "bogus/_log/key", "bogus-bytes")`,
  `forgetDisk`; contains `…marker: a ref-log lane still writing or wedged;`, not `cleanup duty`.
  In `gtest_cas_pool.cpp` give `ScopedParkedRenewalLogCapture` `explicit ScopedParkedRenewalLogCapture(const String & logger_name = "CasMountLeaseRenewer")`
  and add `CASPoolShutdown.AnAmbiguousRenewalAtStopIsNamedInTheUncleanLine`: the `ambiguous == true` branch
  of `PreSendCancellationAllowsFarewellButAmbiguityDoesNot` (layout `"shutdown-ambiguous-line"`), then
  `ScopedParkedRenewalLogCapture capture("CasPool"); runtime.finishTeardown({});`; expect no farewell and
  `…marker: the last renewal ended without a confirmed lease;` in `capture.captured()`.
- [ ] Red: the first two find `CAS store shutdown with an unresolved ref-log PUT`; the third finds no line. Paste.
- [ ] `finishTeardown` after the `mount_renewer` check (add `#include <fmt/ranges.h>`):
```cpp
    std::vector<std::string_view> causes;
    if (blockers.drain_failed)
        causes.emplace_back("the shutdown drain failed");
    if (blockers.ref_lane_unsettled)
        causes.emplace_back("a ref-log lane still writing or wedged");
    if (blockers.snapshot_publish_in_flight)
        causes.emplace_back("a snapshot publish in flight");
    if (blockers.cleanup_duty_pending)
        causes.emplace_back("an undrained writer cleanup duty");
    if (mount_renewer->state() == MountLeaseRenewerState::RenewalTerminal)
        causes.emplace_back("the last renewal ended without a confirmed lease");
    if (!causes.empty())
    {
        LOG_WARNING(getLogger("CasPool"),
            "CAS store shutdown without the clean-release marker: {}; the next mount will treat this end as unclean",
            fmt::join(causes, ", "));
        return;
    }
    if (mount_renewer->state() != MountLeaseRenewerState::Active)
        return;
    /// the existing `try { mount_renewer->release(); } catch (...) { tryLogCurrentException(…); }`, unchanged
```
- [ ] Green, plus `CASPoolShutdown*:CASForget*:CASMountStop*`. Mutation: drop the `RenewalTerminal` cause →
  the third red; revert. Commit: `Name the causes of an unclean CAS store shutdown`.

## Task 6: wait for snapshot publishes before the farewell

**Files:** `$CA/Pool/CasRefLedger.h`, `$CA/Pool/CasRefLedger.cpp`, `$CA/Pool/CasPool.h`, `src/Disks/tests/gtest_cas_farewell_rules.cpp`.
- [ ] Seam only: `CasRefLedger` member `std::function<void()> shutdown_publish_wait_hook_for_test;` and
  `void setShutdownPublishWaitHookForTest(std::function<void()> hook) { shutdown_publish_wait_hook_for_test = std::move(hook); }`;
  `Pool` delegate `setShutdownPublishWaitHookForTest` next to `setSnapshotBeforeCkptCasHookForTest`. Not called yet.
- [ ] Tests:
```cpp
/// A publish of this incarnation can land after the farewell, so FORGET waits for it.
TEST(CASFarewellRules, ForgetWaitsForAPublishInFlightThenWritesTheFarewell)
{
    auto backend = std::make_shared<tests::CountingBackend>();
    auto store = openPool(backend, /*attempt_ms=*/5000, /*publish_every_mutation=*/true);
    const RootNamespace ns{"srv1/publish_settles"};
    auto parked = std::make_shared<tests::ManualBarrier>();
    SCOPE_EXIT({ parked->release(); });
    store->setSnapshotBeforeCkptCasHookForTest([parked] { parked->arriveAndWait(); });
    auto waits = std::make_shared<std::atomic<int>>(0);
    store->setShutdownPublishWaitHookForTest([parked, waits] { waits->fetch_add(1); parked->release(); });
    publishRef(store, ns, "ref_1", 1);
    parked->waitUntilArrived();
    store->forgetDisk({}, "r");
    EXPECT_EQ(waits->load(), 1) << "FORGET did not wait for the publish in flight";
    EXPECT_EQ(store->pendingSnapshotPublishesForTest(ns), 0);
    EXPECT_EQ(farewellSentinel(*backend, store->layout().mountKey("test")), kTerminated);
}
```
  `ForgetWritesNoFarewellWhileAPublishOutlivesTheDrain`: the same without the wait hook, `attempt_ms = 100`
  (a 200 ms real drain budget), ns `srv1/publish_outlives`, `ScopedPoolLogCapture log;` before `forgetDisk`;
  expect `farewellSentinel(…) != kTerminated` and `marker: a snapshot publish in flight;` in the log; then
  `parked->release(); store->waitForSnapshotPublishSettleForTest(ns);`.
- [ ] Red: the first fails `waits` 0 vs 1 and pending 1 vs 0; the second finds the farewell written. Paste.
- [ ] `drainRefLanesForShutdown`, after the wedge loop:
```cpp
    /// A snapshot publish holds no lane, yet its snapshot and `_ckpt` writes can land after the farewell.
    bool publish_in_flight = false;
    for (const auto & rt : runtimes)
    {
        std::unique_lock<std::mutex> lock(rt->state_mutex);
        const auto settled = [&] { return rt->pending_snapshot_publishes.load(std::memory_order_relaxed) == 0; };
        if (!settled() && shutdown_publish_wait_hook_for_test)
            shutdown_publish_wait_hook_for_test();
        if (!rt->publish_settle_cv.wait_until(lock, deadline, settled))
            publish_in_flight = true;
    }
    return {.lane_unsettled = timed_out || any_wedge, .snapshot_publish_in_flight = publish_in_flight};
```
  The header comment of `drainRefLanesForShutdown` gains `Then waits, on the same deadline, for every snapshot publish in flight.`
- [ ] Green. Mutations: delete the loop → both red; map `.snapshot_publish_in_flight = false` in
  `Pool::drainForShutdown` → the second red; revert. Commit: `Withhold the CAS farewell while a snapshot publish is in flight`.

## Task 7: docs, gates, PR

**Files:** `docs/en/antalya/cas/architecture/mounts-and-leases.md`.
- [ ] Replace the «**Clean unmount:**» paragraph: stop and join both workers, drain the ref lanes and wait for
  snapshot publishes, then `release` on an `Active` renewer writes the farewell (`expires_at_ms` expired,
  `min_active_build_sequence = UINT64_MAX`), so a successor reclaims at once. It is conditional on this
  mount's token, so it is written after the lease expired too; a moved slot refuses it. None while a ref lane
  is writing or wedged, a publish is in flight, a cleanup duty is undrained, or the last renewal ended without
  a confirmed lease — the line `CAS store shutdown without the clean-release marker: <causes>` says which. A
  `stopping` disk writes none; `SYSTEM CAS FORGET` writes it under these rules before `closed`.
  Commit: `Document when a CAS disk writes its farewell`.
- [ ] Gates on the final head: `CAS*` release and ASan to `$WT/build*/test_pr13_gate.log`; count = Setup + 11 − 1.
- [ ] PR (template): title `Write the CAS farewell after lease expiry, not during a publish`; Bug Fix;
  changelog: `A CAS disk now leaves its clean shutdown marker after its mount lease expired, and withholds it while a snapshot publish is still in flight.`;
  body: the rules, the unclean line, `Closes:` nothing (CAS-179 is internal), `Related:` PR 12.
