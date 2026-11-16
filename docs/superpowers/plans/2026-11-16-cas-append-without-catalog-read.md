# CAS ref append without the catalog read, promote without the re-read: implementation plan {#cas-append-without-catalog-read-plan}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A positive CAS ref-log append sends no catalog `GET`, `PartWriteTxn::promote` sends no manifest `GET`,
and a table runtime acquired for a catalog row in state `Removing` refuses writes from the moment it exists.

**Architecture:** Four commits in the order of spec §8, then one test-only commit. Commit 1 closes and latches
the lane where the runtime is acquired, while the per-append read still runs. Commit 2 deletes that read.
Commit 3 moves the manifest decode from `promote` to `stageManifest` and keeps the blob leaves. Commit 4 is docs.

**Tech Stack:** C++ (`CA/` metadata storage of the ClickHouse fork), GoogleTest (`unit_tests_dbms`), stateless
tests (shell), Markdown docs.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-11-15-cas-append-without-catalog-read-design.md`,
revision 3 (binding: a conflict between this plan and the spec resolves to the spec, except the items marked
"plan decision"). "§N" below is a section of that spec. No format change, no setting, no model change.

**Plan decision, global:** §8 asks for the CAS stateless tests on each of commits 1 to 3. No task runs them (a
task has no server). The controller runs them on each of the three code commits of the final history; see
[After Task 5](#after-task-5).

## Global Constraints {#global-constraints}

### Paths {#paths}

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1`, branch `work/antalya-26.8/cas-append-no-catalog-read`, base `97123265e03` |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master` (spec, this plan) |
| `R` | `$M/tmp/partb` (ledger, `reports/task<N>.md`, saved red logs `logs/`) |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| `T/` | `src/Disks/tests/` |
| `D/` | `docs/en/antalya/cas/` |
| `Q/` | `tests/queries/0_stateless/` |
| `L:` / `W:` | `CA/Pool/CasRefLedger.cpp` / `CA/Pool/CasPartWriteTxn.cpp` |

Every `path:line` was read in `WT` at `97123265e03`. Lines move as tasks land: locate by the named symbol, use
the line as a hint. In commands `CA`, `T`, `D`, `Q` are shell variables with these values, without the slash.

### Binding rules {#binding-rules}

- **Worktree.** Modify files only under `WT`. Never touch another worktree. No push, no pull request. The only
  writes outside `WT`: the task report and saved logs under `$R`. Before the first step `git -C $WT status --short`
  prints nothing or only `?? config_file_for_test.xml`.
- **No destructive commands.** No `git clean`, `git reset --hard`, `git checkout -- <file>`, `git stash`,
  `git rebase`, `git commit --amend`. Never delete or re-create a build directory. A hung test binary is
  stopped by its exact PID.
- **C++.** Allman braces. No `sleep` to fix a race: tests use hooks and latches, every wait is bounded, a scope
  guard releases each parked thread before a join. No fallback paths: let errors propagate. Say "exception",
  not "crash", for logical errors. Function names in prose and comments as `f`, not `f()`. Comments explain
  why, in one or two lines, and carry no task ids, no history, no references to plans or reviews.
- **`LOGICAL_ERROR` tests.** A test that expects `LOGICAL_ERROR` is a death test in a `*DeathTest` suite: debug
  and sanitizer builds abort on it. Form: `#ifndef DEBUG_OR_SANITIZER_BUILD` around the catching test,
  `#if defined(DEBUG_OR_SANITIZER_BUILD)` around the `EXPECT_DEATH` twin (as
  `T/gtest_cas_promote_republish.cpp:353-402`). Death suites must not construct `DiskObjectStorage` in the
  parent process; a death-test child exits with `std::_Exit`.
- **Builds.** Build directories in `WT`: `build` (release), `build_asan` (ASan + UBSan), `build_fast` (no S3).
  No `-j`. One build at a time, in the foreground. Never build the `clickhouse` target in a task (the
  controller does). The unit test binary is `<build dir>/src/unit_tests_dbms`. Logs are read with `grep -a`.

  ```bash
  cd $WT/build && ninja unit_tests_dbms > build_<tag>.log 2>&1; \
    echo "NINJA_EXIT=$? HEAD=$(git -C $WT rev-parse --short HEAD)" >> build_<tag>.log
  cd $WT/build && ./src/unit_tests_dbms --gtest_filter='<filter>' > test_<tag>.log 2>&1; \
    echo "GTEST_EXIT=$?" >> test_<tag>.log
  grep -a 'NINJA_EXIT' $WT/build/build_<tag>.log; grep -a -c ' error: ' $WT/build/build_<tag>.log
  grep -a '\[  PASSED  \]\|\[  FAILED  \]\|GTEST_EXIT' $WT/build/test_<tag>.log | tail -20
  ```

  Tags: `t<N>_red`, `t<N>_green`, `t<N>_pre`, `t<N>_gate`, `t<N>_m<k>` (a mutant); the same in `build_asan` for
  the ASan gate. A test log is evidence only if the build log of the same tag ends with `NINJA_EXIT=0`. A test
  log without a `[  PASSED  ]` line means the binary aborted: the last `[ RUN      ]` line names the test.
- **Tests.** During a task a narrower `--gtest_filter` is fine. Before the commit run `CAS*` on `build`
  (tag `t<N>_pre`). The gate at the end of every task is EXACTLY `--gtest_filter='CAS*'` on `build` and on
  `build_asan`, on the committed tree, with the `HEAD` marker in both build logs. Gate on the base: 3137
  passed (`build`), 3141 passed (`build_asan`). A red gate after the commit: fix it in a further commit (same
  staging and trailer rules, no amend) and rerun both gates; history is rewritten at the end. It is still a
  stop when the red test is an existing test no task names and this plan does not explain its failure.
- **Red first.** Write the test before the product change and run it. The red run fails on an assertion, not
  on a compile error: a new hook or signature is added first with today's behaviour. Copy the red log to
  `$R/logs/red_t<N>.log`. A test marked "pin" is green before and after.
- **Existing tests.** Each task names every existing test it rewrites; the candidates were read and are
  listed as "kept" with the reason. Any other existing test that fails after a product change is not
  rewritten and its fixture is not changed: stop and report that test, with the failing assertion and the log.
- **Predicted numbers.** The unit-level request-count pins are authoritative. A stateless reference is edited
  to the predicted value and listed in the report as "predicted, not run". If a number differs from the
  prediction, stop and report; never adjust the product to a number.
- **Stateless and integration tests** are edited but not run in a task (no docker, no server); the controller
  runs them.
- **Mutants.** A mutant or any temporary edit exists in the tree only while it is being built in the
  foreground. Before it: `cp <file> $R/logs/t<N>_<name>.keep`. After it: copy back and `cmp` the two.
- **Docs** under `docs/`: every heading has an explicit `{#anchor}`; no invariant tags in user docs; state the
  present behaviour, no history phrasing.
- **Scope.** Change only the files the task lists. No change to
  non-CAS (upstream) code unless a task names it and says why it cannot be avoided; no task here does. A
  defect found elsewhere goes into the report, not the diff.
- **Commit.** One commit per task, by exact file list, without `-s`; a further commit only to fix a red gate.
  Never `git add -A` or `git add .`.

  ```bash
  cd $WT && git add <files> && git diff --cached --name-only   # must equal the task's list
  git commit -F - <<'EOF'
  <subject from the task, at most 72 characters>

  <body from the task: the why, literal names in backticks>

  Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
  Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>
  EOF
  ```

  No other trailer, no session link, no backlog id, none of the words "spec", "plan", "task", "review".
- **Report.** `$R/reports/task<N>.md`: what changed, red evidence, pass counts (`build`, `build_asan`),
  every existing test that failed with its assertion, numbers that are predicted and not run, deviations, doubts.

### Test vocabulary {#test-vocabulary}

All in `T/gtest_cas_ref_writer.cpp` unless a path is given.

- `openPool`, `openPoolWithConfig(backend, PoolConfig)` (`:154`, `:171`); `publishEmptyPart(store, ns, ref)`
  (`:199`: `stageManifest({})`, then two appends, the precommit and the promote); `startBuildFor` (`:190`).
- `RefWriterTestBackend` (`:465`, a `CountingBackend`): `requestJournal` returns `"GET <key>"`, `"PUT <key>"`,
  `"CAS <key>"` in order; `clearRequestJournal`; `getCount(key)`; `totalRequests(*backend)` (`:920`). Fault of
  the n-th catalog read: `catalog_fault_key`, `catalog_gets_before_fault`, `catalog_get_fault_count` (`:5222`).
- Catalog: `catalogEntryOrThrow` (`:293`), `lifeIfCatalogedForTest` (`:253`), `readOf(backend, key)` (`:279`),
  `rewriteCatalogRow` (`:1524`), `setCatalogRowRemovingForTest(backend, layout, life)` (`:1569`). The last two
  are "from outside": a catalog write no runtime of the pool made, the stand-in for the late request of §4.
  Plan decision: §6 names `CasRefCatalog::casUpdate` (as `:4936`); these raw helpers leave the same durable row.
- Remount: `fenceOutRefMount(*backend, layout.mountKey("test")); ASSERT_TRUE(store->tryRemountOnce());` (`:5997`).
- Logs of a namespace: `listLogIdsForTest(*backend, layout, ns)` (`:4336`); the log key prefix is
  `layout.namespaceStreamPrefix(life) + "_log/"`.
- Parked thread: the pre-carve park of `:4899-4931` (mutex, condition variable, `wait_for` 10 s, one-shot);
  `ParkedRecovery` (`:1711`) with `ManualBarrier` (`T/cas_test_helpers.h:215`; both waits end after 20 s).
- `updatePublishedAt(store, ns)` (`:5253`): a positive append on ref `owned`.
- A refused write is `ErrorCodes::NETWORK_ERROR` from `throwCasWriteRetryLater`. Through `namespaceLife` (every
  `appendRefOps`): "… is Removing: creation waits …", after one catalog `GET` (`L:5308-5321`). For a caller
  that holds the runtime: "… is Removing: positive ref mutation admission is closed …" (`L:2129`, `L:2168`).
  Tests assert the substring `is Removing` with `expectThrowsCodeWithMessage` (`T/cas_test_helpers.h:2935`).
- Declaration order in a test with a parked thread: (1) the scope guard that clears the test hooks, (2) every
  `std::future`, (3) the scope guard that releases the parked thread. Destructors run in reverse: release,
  join, then clear, so no thread is inside a hook when it is destroyed. No `ASSERT_*` in a non-void helper.

## Review Focus {#review-focus}

1. **An insert races a resumed `DROP`: an append lands after the terminal record, or is admitted while the drop
   recovers.** Owner: Task 1, `.ResumedDropOverARemovingRowAdmitsNoWriter` (the writer is refused while the
   drop is parked, and sends no log `PUT`); Task 2, `.QueuedAppendDrainsBeforeTheTerminalOfADropThatObservedRemoving`
   (the terminal record names the ref of the append that was queued before the close).
2. **A `Removing` observation undone by the reopen path.** A failed drop reopens the lane from a `Live` read it
   took before another caller read `Removing`. Owner: Task 1, `.RemovingObservationWinsOverAnEarlierLiveReopenRead`,
   plus the mutant "the rule skips a resident runtime".
3. **A manifest that stages but does not decode reaches a commit.** Owner: Task 3,
   `.StageManifestRefusesAnEntryPathWithDotDot`, `.StageManifestRefusesARecordLineOverTheLineCap`, and the
   rewritten cap test, whose old body was such a manifest.
4. **A foreign manifest id in `promote` is refused by the dependency guard, not by the new check.** Owner:
   Task 3, `.PromoteOfAManifestStagedElsewhereIsRefused`: the body has no `Blob` entry and the test asserts the
   message text, so "no dependency proof" cannot satisfy it.
5. **A request-count pin that passes without the change.** Owner: Task 2 (the journal of
   `.PositiveAppendIsOneCreate` is compared entry by entry; its red log must show two entries) and Task 3
   (`.PromoteSendsNoManifestGet` also asserts one write of the same key, so a count of 0 at a key nobody
   touched fails). In `05485` a misspelled counter prints 0 too: the new query also selects `CASManifestPut`,
   predicted 1.

---

### Task 1: Commit 1, a runtime acquired for a `Removing` row is closed and latched {#task-1}

Implementer `ca-impl`. Reviewer `ca-review`. Read §3 and the first three rows of the §6 table. After this task
the per-append catalog read still runs.

**Files:**
- Modify: `CA/Pool/CasRefLedger.h:363` (setter), `:918-921` (comment and the new field), `:1081` (hook member),
  `:1097-1106` (contract and declaration)
- Modify: `CA/Pool/CasRefLedger.cpp:686-762` (`acquireRefTableRuntime`), `:826`, `:841`, `:5397`, `:5504`, `:5602`
  (call sites), `:5712-5737` (hook, latch term, comment)
- Modify: `CA/Pool/CasPool.h:1089-1092` (forwarder)
- Test: `T/gtest_cas_ref_writer.cpp`, three tests after `RemountReopensTheLaneAfterAnAmbiguousBeginRemoving` (`:5986`)

**Interfaces:**
- Consumes (existing): `enum class NsState { Creating = 1, Live = 2, Removing = 3 }` (`CA/Formats/CasRefCatalogFormat.h:31`);
  `RefTableRuntime::removal_admission_closed`, guarded by `ref_queue_mutex` (`CA/Pool/CasRefLedger.h:921`).
- Produces:

  ```cpp
  /// CA/Pool/CasRefLedger.h, RefTableRuntime, after removal_admission_closed. Guarded by ref_queue_mutex.
  bool removing_observed = false;        /// set only by acquireRefTableRuntime; nothing clears it
  /// CA/Pool/CasRefLedger.h, private
  std::shared_ptr<RefTableRuntime> acquireRefTableRuntime(
      const NamespaceLifeId & life, NsState observed_state, uint64_t admitted_generation,
      ColdAdmission & admission, RuntimeAdmission kind);
  /// test seams: the member `removal_reopen_read_hook_for_test`, and
  void CasRefLedger::setRemovalReopenReadHookForTest(std::function<void()> hook);
  void Pool::setRemovalReopenReadHookForTest(std::function<void()> hook);      /// forwards to the ledger
  ```

- Rules the signatures do not state:
  - `observed_state` is the state of the catalog row the caller read for exactly `life`. Call sites: `L:826`
    `NsState::Live`; `L:841` `NsState::Live` (it reads no row: the life came from `namespaceLife`); `L:5397`
    `NsState::Live`; `L:5504` `entry->state`; `L:5602` `initial_it->state`.
  - `chassert(observed_state != NsState::Creating)` next to the `chassert` at `L:690`. A release build handles
    `Creating` as `Live`.
  - Inside the `ref_queue_mutex` section (`L:699-736`), after `result` is chosen and before
    `releaseColdAdmissionLocked`: when `result` is set and `observed_state == NsState::Removing`, set
    `result->removal_admission_closed` and `result->removing_observed`. The same for a new and for a resident
    runtime. `Live` changes neither field. No `result` (identity conflict, retirement, moved fence): nothing.
  - The reopen guard at `L:5733` gets one more term: `&& !rt->removing_observed`.
  - The hook is called right after the catch-side read at `L:5719` returns, with no lock held. It sits inside
    the inner `try`, whose `catch` (`L:5740`) swallows every exception: a test records what it saw and asserts
    after the drop returned.
  - Comments to bring up to date: the field comment at `CA/Pool/CasRefLedger.h:918-920` (the flag is set by a
    removal before its catalog write, and at acquisition for a row read as `Removing`), the contract at
    `:1097-1103`, the comment at `L:5712-5714` (a reopen also needs no `Removing` observation of this runtime).

- [ ] **Step 1: Write the failing tests**

  ```cpp
  /// T/gtest_cas_ref_writer.cpp, suite CASRefWriterNamespaceRemoval
  /// File-local setup `removingRowAfterRemount(backend, store, ns)`, returns the life: publishEmptyPart(store, ns,
  /// "owned"); life = *lifeIfCatalogedForTest(...); setCatalogRowRemovingForTest(backend, layout, life);
  /// fenceOutRefMount(...); a failed tryRemountOnce throws std::runtime_error (no ASSERT_* here: it returns a value,
  /// as rewriteCatalogRow at :1535). The caller asserts !store->refTableCachedForTest(ns). `log_prefix`: vocabulary.

  TEST(..., RuntimeAdmittedForARemovingRowRefusesWrites)
  //  setup; EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns));
  //  EXPECT_FALSE(store->resolveRef(ns, "owned"));       // plan decision: BEFORE the write, not after it as in §6.
  //                             // Today the probe's runtime serves it, and today a refused write closes the lane too
  //  backend->clearRequestJournal();
  //  expectThrowsCodeWithMessage(NETWORK_ERROR, "is Removing", [&] { updatePublishedAt(store, ns); });
  //  no entry of backend->requestJournal() starts with "PUT " + log_prefix
  //  EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns));
  //  EXPECT_NO_THROW(store->dropNamespace(ns)); EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns));

  TEST(..., ResumedDropOverARemovingRowAdmitsNoWriter)
  //  park = std::make_shared<ParkedRecovery>(); park->armed = false;       // the setup's recoveries pass
  //  store = openPoolWithConfig(backend, ParkedRecovery::config(park)); setup; park->armed = true;
  //  std::future<void> drop; std::future<String> writer; SCOPE_EXIT({ park->barrier.release(); });   // order (2), (3)
  //  drop = std::async(std::launch::async, [&] { store->dropNamespace(ns); });
  //  park->barrier.waitUntilArrived();                   // the drop is inside its recovery: its runtime exists
  //  backend->clearRequestJournal();
  //  writer = async: updatePublishedAt(store, ns) -> "committed", or e.message() of a DB::Exception
  //  ASSERT_EQ(writer.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  //  EXPECT_NE(writer.get().find("is Removing"), String::npos);
  //  no journal entry starts with "PUT " + log_prefix    // the drop is parked: the journal is the writer's
  //  park->barrier.release(); ASSERT_EQ(drop.wait_for(std::chrono::seconds(30)), std::future_status::ready);
  //  EXPECT_NO_THROW(drop.get()); EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns));

  TEST(..., RemovingObservationWinsOverAnEarlierLiveReopenRead)
  //  publishEmptyPart(store, ns, "owned"); life = *lifeIfCatalogedForTest(...); std::optional<bool> probe_answer;
  //  SCOPE_EXIT({ store->setRemovalReopenReadHookForTest({}); });
  //  store->setRemovalReopenReadHookForTest([&] {
  //      setCatalogRowRemovingForTest(backend, layout, life);
  //      probe_answer = store->namespaceStillLogicallyPresent(ns); });
  //  backend->catalog_fault_key = layout.refCatalogKey();
  //  backend->catalog_gets_before_fault = 1; backend->catalog_get_fault_count = 1;      // as :5222-5224
  //  EXPECT_THROW(store->dropNamespace(ns), std::runtime_error);
  //  ASSERT_TRUE(probe_answer.has_value()); EXPECT_TRUE(*probe_answer);
  //  expectThrowsCodeWithMessage(NETWORK_ERROR, "is Removing", [&] { updatePublishedAt(store, ns); });
  //  EXPECT_NO_THROW(store->dropNamespace(ns)); EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns));
  ```

- [ ] **Step 2: Run red.** Add only the hook: its member, the two setters, the call after `L:5719`. No rule, no
  latch, no signature change. Filter `CASRefWriterNamespaceRemoval.*`. Expected: the first test fails at
  `resolveRef` (it returns `owned`) and on the text ("exact catalog life is no longer Live"); the second at the
  10 s wait (the writer waits on the drop's recovery); the third on the text. Everything else passes.
- [ ] **Step 3: Implement** the Produces block and its rules.
- [ ] **Step 4: Run green.** Filter `CASRefWriter*`, then `CAS*` on `build` (tag `t1_pre`). No existing test
  is rewritten. Kept, read: `CASRefWriterRecovery.PresenceProbeOverAnErasedLifeDoesNotThrow` (`:1647`) and
  `.PresenceProbeWaitingOnARecoveryThatFindsTheLifeErasedDoesNotThrow` (`:1782`) acquire a runtime for a
  `Removing` row, and their answers do not depend on the lane; in
  `.PresenceProbeStaysTrueThroughRemovingUntilTerminalRetrySucceeds` (`:6076`),
  `.PresenceProbeFacadeConsistencyAcrossRemovalLifecycle` (`:6310`) and `.CreateAgainstRemovingRetriesWithoutMutation`
  (`:6528`) the runtime was closed by its own drop, or cold write admission refuses.
- [ ] **Step 5: Two mutants, one at a time,** on `build`, tags `t1_m1` and `t1_m2`, filter
  `CASRefWriterNamespaceRemoval.*`; then restore, compare (`cmp`) and copy both test logs to `$R/logs/`.
  (m1) `L:5602` passes `NsState::Live`: `.ResumedDropOver…` must fail. (m2) the rule applies only to a newly
  constructed runtime: `.RemovingObservationWins…` must fail. A mutant that stays green: stop and report.
- [ ] **Step 6: Commit,** then the gate on `build` and `build_asan` (tag `t1_gate`). Expected: 3140 and 3144.
  A red gate: the Tests rule.
  `git add $CA/Pool/CasRefLedger.h $CA/Pool/CasRefLedger.cpp $CA/Pool/CasPool.h $T/gtest_cas_ref_writer.cpp`
  Subject: ``Close the CAS ref lane of a runtime acquired for a `Removing` row``. Body: a presence probe or a
  resumed `DROP` built an open runtime for a row that was already `Removing`, and only the catalog read before
  each append refused its writers. The lane is now closed where the runtime is acquired, and a failed removal
  cannot reopen it from an earlier `Live` read.

### Task 2: Commit 2, delete the catalog read of a positive append {#task-2}

Implementer `ca-impl`. Reviewer `ca-review`. Read §2, §4 and the read-removal rows of the §6 table.

**Files:**
- Modify: `CA/Pool/CasRefLedger.cpp:3772-3776` (the `positive_append` flag: unused once the block is gone),
  `:3824-3858` (the block and its comment), after `:5602` (the barrier hook)
- Modify: `CA/Pool/CasRefLedger.h` (hook member next to `:1081`, setter next to `:363`), `CA/Pool/CasPool.h`
  (forwarder next to `:1089`)
- Test: `T/gtest_cas_ref_writer.cpp:6` (add the include of `Gc/CasNamespaceJanitor.h` next to `CasGc.h`),
  `:50-62` (`extern const Event CASGCStuckRemovals`), `:1946-1970`, `:1992-2025`, `:4880-4961` (the test and its
  comment), `:5277-5278` (comment), two new tests after Task 1's three (they call `updatePublishedAt`, defined
  at `:5253`); `Q/05485_cas_ref_append_requests.reference`

**Interfaces:**
- Consumes (Task 1): `acquireRefTableRuntime` closes the lane of the runtime it returns when its caller read
  `Removing`; so in `dropNamespaceImpl` the lane is closed when the call at `L:5602` returns for such a row.
- Consumes (existing): `Gc gc(store, UInt128{n}); gc.runRegularRound()` (as `:2909`);
  `NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; })`
  (`CA/Gc/CasNamespaceJanitor.h:33-56`, as `T/gtest_cas_namespace_janitor.cpp:289`);
  `PoolConfig::gc_stuck_removal_rounds` (`CA/Pool/CasPool.h:166`); `DropNamespaceStats{committed_refs, precommits}`;
  `Pool::setAppendAfterRuntimeCaptureHookForTest`, `Pool::setRefPreCarveHookForTest`.
- Produces:

  ```cpp
  /// test seams: the member `removal_runtime_acquired_hook_for_test`, and
  void CasRefLedger::setRemovalRuntimeAcquiredHookForTest(std::function<void()> hook);
  void Pool::setRemovalRuntimeAcquiredHookForTest(std::function<void()> hook);     /// forwards to the ledger
  ```

  The hook is called in `dropNamespaceImpl` right after `acquireRefTableRuntime` returns (`L:5602`), before
  `ensureRefTableRecovered`, with no lock held. A test uses it only to signal.
- Product change: delete `L:3824-3858` and `L:3772-3776`. The terminal branch (`L:3783-3822`), `removal_append`
  and `runtime_live` stay as they are.

- [ ] **Step 1: Write the failing tests**

  ```cpp
  TEST(CASRefWriterAppendLane, PositiveAppendIsOneCreate)       /// replaces PositiveAppendIsOneCatalogReadAndOneCreate
  //  as today; catalog_gets = backend->getCount(layout.refCatalogKey()) taken before the update
  //  EXPECT_EQ(totalRequests(*backend) - total, 1u);
  //  ASSERT_EQ(journal.size(), 1u) << ::testing::PrintToString(journal);     // the red log shows both entries
  //  EXPECT_EQ(journal[0].find("PUT " + log_prefix), 0u);
  //  EXPECT_EQ(backend->getCount(layout.refCatalogKey()), catalog_gets); the two _ckpt assertions unchanged

  TEST(CASRefWriterAppendLane, PartPublishCostsTwoLogCreatesAndNoCatalogOrCkptRequest)
  //  replaces PartPublishCostsOneCatalogReadPerLogCreateAndNoCkptRequest (plan decision: the old name is false)
  //  EXPECT_EQ(new_logs, 2u); EXPECT_EQ(backend->getCount(layout.refCatalogKey()) - catalog_gets, 0u);
  //  the two _ckpt assertions unchanged

  TEST(CASRefWriterNamespaceRemoval, RowTurnedRemovingUnderAnOpenLaneKeepsItsAppends)
  //  replaces CachedPositiveWriterCannotAppendAfterRemovingIsPublished
  //  store = openPoolWithConfig(backend, PoolConfig{.gc_stuck_removal_rounds = 1});
  //  publishEmptyPart(store, ns, "existing"); life; the pre-carve park of :4899-4931, in the vocabulary's order
  //  writer = async: returns the ManifestId of publishEmptyPart(store, ns, "late")
  //  when parked: rewriteCatalogRow(backend, layout, ns, CatalogEntry{.ns = ns, .state = NsState::Removing,
  //      .incarnation = life.incarnation, .removal_started_round = 0});
  //  release; ASSERT ready within 10 s; late_id = writer.get(); ASSERT_TRUE(store->resolveRef(ns, "late"));
  //  logs = listLogIdsForTest(...); stuck = ProfileEvents::global_counters[ProfileEvents::CASGCStuckRemovals];
  //  Gc gc(store, UInt128{1}); 12 times: EXPECT_TRUE(gc.runRegularRound().acquired_lease);
  //  CasRequests requests(backend, Fence::open(), {}, {}, nullptr, kDefaultRequestWindowMs);
  //  NamespaceJanitor(requests, layout, 100).runOnePage(false, [] { return true; });
  //  EXPECT_EQ(catalogEntryOrThrow(backend, layout, ns).state, NsState::Removing);
  //  EXPECT_TRUE(readOf(backend, layout.refCkptKey(life))); EXPECT_EQ(listLogIdsForTest(...), logs);
  //  EXPECT_TRUE(readOf(backend, layout.manifestKey(late_id)));
  //  EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::CASGCStuckRemovals], stuck);
  //  EXPECT_TRUE(store->resolveRef(ns, "late"));                  // still served: nothing observed the row
  //  remount; expectThrowsCodeWithMessage(NETWORK_ERROR, "is Removing",
  //      [&] { store->updateRefPublishedAt(ns, "late", [](RefPublishedAtUpdate & u) { u.published_at_ms = 17; }); });
  //  EXPECT_FALSE(store->resolveRef(ns, "late")); EXPECT_TRUE(readOf(backend, layout.manifestKey(late_id)));

  TEST(CASRefWriterNamespaceRemoval, QueuedAppendDrainsBeforeTheTerminalOfADropThatObservedRemoving)
  //  publishEmptyPart(store, ns, "owned"); life;
  //  build = startBuildFor(store, ns, "late"); id = build->stageManifest({}); build->precommitAdd(ns, "late", id);
  //  (1) SCOPE_EXIT that clears the pre-carve hook and the barrier hook; (2) std::future<String> promote_result;
  //  std::future<DropNamespaceStats> drop; (3) SCOPE_EXIT that releases the pre-carve park
  //  arm the one-shot pre-carve park AFTER the precommit; promote_result = async:
  //      build->promote(ns, "late", build->buildId(), id) -> "committed", or e.message()
  //  wait parked (10 s); setCatalogRowRemovingForTest(backend, layout, life); logs = listLogIdsForTest(...);
  //  store->setRemovalRuntimeAcquiredHookForTest(one-shot: set `drop_acquired` under the mutex, notify);
  //  drop = async: return store->dropNamespace(ns);
  //  wait drop_acquired (10 s)                                    // the lane is closed; the drop waits for the drain
  //  expectThrowsCodeWithMessage(NETWORK_ERROR, "is Removing", [&] { updatePublishedAt(store, ns); });
  //  EXPECT_EQ(listLogIdsForTest(...), logs);                      // no terminal record, nothing appended
  //  release; ASSERT promote_result ready (10 s); EXPECT_EQ(promote_result.get(), "committed");
  //  ASSERT drop ready (30 s); stats = drop.get();
  //  EXPECT_EQ(stats.committed_refs, 2u); EXPECT_EQ(stats.precommits, 0u);   // the terminal follows the promote
  //  EXPECT_FALSE(store->resolveRef(ns, "late")); EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns));

  TEST(CASRefWriterNamespaceRemoval, OwnDropRefusesAWriterThatHoldsTheRuntimeWithoutACatalogRead)   /// pin
  //  publishEmptyPart(store, ns, "owned"); one-shot park in store->setAppendAfterRuntimeCaptureHookForTest,
  //  in the vocabulary's order
  //  writer = async: updatePublishedAt(store, ns) -> "committed", or e.message(); wait parked (10 s)
  //  EXPECT_NO_THROW(store->dropNamespace(ns));                    // complete: the terminal record is durable
  //  catalog_gets = backend->getCount(layout.refCatalogKey()); backend->clearRequestJournal();
  //  release; ASSERT writer ready (10 s); EXPECT_NE(writer.get().find("is Removing"), String::npos);
  //  EXPECT_EQ(backend->getCount(layout.refCatalogKey()), catalog_gets); no journal entry starts with "PUT " + log_prefix
  ```

  Plan decision for the third test: the threshold is 1 and `removal_started_round` is 0, not "more than 10
  rounds" as §6 says. A round that defers keeps the round number (`CA/Gc/CasGc.h:153-157`), so eleven calls on
  a quiet pool are not eleven rounds; the stuck report runs in every round (`CA/Gc/CasGc.cpp:924`). If the
  counter has not grown after the 12 calls, stop and report.

- [ ] **Step 2: Run red.** Add the barrier hook first. Filter
  `CASRefWriterAppendLane.*:CASRefWriterNamespaceRemoval.*`. Expected: the first two fail on the counts (2
  requests, 2 catalog reads); the third and the fourth fail because the released append is refused ("exact
  catalog life is no longer Live"); the pin passes. Copy the journal that `PositiveAppendIsOneCreate` prints
  into the report: two entries, the catalog `GET` and the log `PUT`.
- [ ] **Step 3: Implement** the product change. Update the comment at `T/gtest_cas_ref_writer.cpp:5277-5278`:
  a write that lands late no longer stops writes at the next append; it turns the row `Removing` under an open
  lane.
- [ ] **Step 4: Edit** `Q/05485_cas_ref_append_requests.reference`: both lines become `0	0	3	0` (tabs).
  Predicted, not run: the fourth column is `CASOtherGet`, which counted the three catalog reads of the three
  positive appends of each query.
- [ ] **Step 5: Run green.** Filter `CASRefWriter*`, then `CAS*` on `build` (tag `t2_pre`). No existing test
  beyond the three of step 1 is rewritten. A catalog `GET` count or ordinal moves only when a positive append
  through a pool runs inside the counted window. Kept, each window read, none has one:
  - Counted after `resetCounts`, around one GC round, sweep page, janitor page, rebuild, reconciler run or
    recovery: `T/gtest_cas_orphan_sweep_requests.cpp:143-289`, `T/gtest_cas_gc_round_defer.cpp:336`, `:363`,
    `:443`, `T/gtest_cas_gc_rebuild.cpp:255`, `T/gtest_cas_rebuild_condemn_nothing.cpp:408`,
    `T/gtest_cas_gc_frontier_gate.cpp:2727`, `:2781`, `:2815`, `:2855`, `:2889`, `:3205-3464`,
    `T/gtest_cas_namespace_janitor.cpp:298-665`, `T/gtest_cas_orphan_manifest_sweep.cpp:587-656`,
    `T/gtest_cas_ref_recovery_cas_walk.cpp:2376`, `:2889`, `:2915` (its journals are filtered by key),
    `T/gtest_cas_ref_catalog_birth_wiring.cpp:272`, `:290`, `T/gtest_cas_ref_read_contract.cpp:195-208`.
  - No ref-ledger append in the file, or none after the hook is armed: `T/gtest_cas_ref_catalog.cpp:2348-3253`,
    `T/gtest_cas_ref_gc.cpp:224-433`, `T/gtest_cas_ns_creation_lifecycle.cpp:417`, `:473`,
    `T/gtest_cas_namespace_file_request_profile.cpp:470-500`, `T/gtest_cas_decommission.cpp:652-721`, `:2277`,
    `T/gtest_cas_ns_file_incarnation.cpp:324-1208` (`fire_on_read` is armed after the setup's publish;
    `park_on_read` counts the parked thread only).
  - In `T/gtest_cas_ref_writer.cpp`: the ordinals and observers of `:5223`, `:5315`, `:5381`, `:5616`, `:5664`,
    `:5765`, `:5830`, `:6279`, `:6772`, `:6799` fire inside `dropNamespace`, a probe or a GC round; `:1162` is a
    warm read; `.RefRemovalIsOneRequest` (`:1972`), `.CommittedChunkInstallsBeforeAck` (`:2027`) and `:1883`
    append with `dropRef`, which never read the catalog; `:3267` is a snapshot publication;
    `CASRefWriterRecovery.ConfirmationCostsOneCatalogReadPerInstall` (`:1929`) stays 3.
  - Total-request windows without an append: `T/gtest_cas_confirm_exact_ref.cpp:460-2288`,
    `T/gtest_cas_detached_work.cpp:914`, `T/gtest_cas_lifecycle_snapshot.cpp:186`,
    `T/gtest_cas_recovery_grounding.cpp:434-458`, `T/gtest_cas_slot_occupy.cpp:211`, `:235`,
    `T/gtest_cas_gc_teardown_stop.cpp:234-314`, `T/gtest_cas_pool.cpp:1219`.
  - No other test relies on the removed refusal: its text ("no longer Live") appears nowhere in `T/` or
    `tests/`, and the tests that change the catalog under a held runtime (`:1844`, `:3140`,
    `T/gtest_cas_ref_wedge_every_attempt.cpp:600`) append removals, which never took that read.
- [ ] **Step 6: Commit,** then the gate on `build` and `build_asan` (tag `t2_gate`). Expected: 3142 and 3146.
  A red gate: the Tests rule.
  `git add $CA/Pool/CasRefLedger.h $CA/Pool/CasRefLedger.cpp $CA/Pool/CasPool.h $T/gtest_cas_ref_writer.cpp $Q/05485_cas_ref_append_requests.reference`
  Subject: `Drop the catalog read from a positive CAS ref-log append`. Body: no positive append is in flight
  when a row leaves `Live`: a removal closes and drains the lane before its catalog write, and a runtime
  acquired for a `Removing` row starts closed. The read was not atomic with the create, so it was never the
  fence. A row turned `Removing` by a catalog write that lands after a remount now stops writers at the next
  admission that reads the row; until then GC does not erase the row and deletes none of the table's live data.

### Task 3: Commit 3, validate at staging, promote without the manifest read {#task-3}

Implementer `ca-impl`. Reviewer `ca-review`. Read §5 and the promote rows of the §6 table.

**Files:**
- Modify: `CA/Pool/CasPartWriteTxn.h:205-212`, `:217-219`, `:230-243` (contracts), `:390-395` (the member)
- Modify: `CA/Pool/CasPartWriteTxn.cpp:684-696` (validation and leaves), `:742-743`, `:760` (comment), `:766`,
  `:769` (comment),
  `:869-881` (delete the read), `:899-913` (lookup, capture list, its comment), after `:925` (the new check),
  `:965-1006` (the leaf loop)
- Modify (texts only): `CA/ContentAddressedMetadataStorage.cpp:2355-2366`, `:2472-2473`, `:2484`;
  `CA/ContentAddressedExchange.h:94`, `:223`; `T/gtest_cas_protocol_scenarios.cpp:657`
- Test: `T/gtest_cas_part_write.cpp`; `Q/05485_cas_ref_append_requests.sh`, `.reference`

**Interfaces:**
- Consumes (existing): `PartManifest decodePartManifest(...)` and `String openObject(FormatId id, std::string_view stored)`
  (`CA/Formats/CasTextFormat.h:331`), the readers' expression (`CA/Pool/CasManifestReader.cpp:75`);
  `CountingBackend::getCount`, `getStreamCount`, `writeCount` (`T/cas_test_helpers.h:1862`).
- Produces (the signatures of `stageManifest`, `precommitAdd` and `promote` do not change):

  ```cpp
  /// CA/Pool/CasPartWriteTxn.h, replaces `std::set<ManifestId> staged_manifest_ids`
  /// The `BlobRef` of every `Blob` entry of each manifest this transaction staged, in the body's canonical order.
  std::map<ManifestId, std::vector<BlobRef>> staged_manifest_leaves;
  ```

- Rules:
  - `stageManifest`, after `sealObject` (`W:684`) and before `stagingPutIfAbsent` (`W:696`): in a nested scope,
    `const PartManifest validated = decodePartManifest(openObject(FormatId::PartManifest, encoded));` and collect
    `e.ref` of each `Blob` entry of `validated.entries` into a local vector. No `try`: the decoder's
    `CORRUPTED_DATA` propagates, nothing is written, no id is returned. At `W:743` the id enters the map with
    that vector. Plan decision: the leaves come from the decoded body, not from the caller's `entries`: that is
    the durable body, in the order `promote` iterates today, so the order of its events does not change.
  - `precommitAdd` (`W:766`): membership is `staged_manifest_leaves.contains(id)`; the comment at `W:760` names
    the new member.
  - `promote`: delete `W:869-881`. Before `commit_state = CommitState::Uncertain` (`W:904`) look the id up:
    `const std::vector<BlobRef> * staged_leaves`, null when the map has no entry. The capture list drops `body`
    and takes `staged_leaves` by value. The closure, in order: `created` (`W:916`); the committed-match no-op
    (`W:923-925`); NEW: `if (!staged_leaves) throw Exception(ErrorCodes::LOGICAL_ERROR, "promote: manifest '{}'
    was not staged by this transaction and is not the current committed manifest of ref '{}'",
    manifestRefDebugString(id.ref), final_ref_name);`; `policy.expected` (`W:927-934`); the live precommit
    (`W:948-953`); the leaf loop over `*staged_leaves` with the same body per leaf; the rest unchanged.
  - Nothing is added after `appendRefOps` returns, and nothing between `W:904` and the append can throw.
- Texts to bring up to date (state what the code does; no history):
  - `CA/Pool/CasPartWriteTxn.h:205-212`: `stageManifest` decodes the sealed bytes before the `PUT`
    (`CORRUPTED_DATA`, nothing written) and records the blob leaves; the body is still not retained.
  - `:217-219` and `W:769`: GC's fold fails closed on a missing precommit body; `promote` does not read the body.
  - `:230-243`: step 2 becomes "no manifest request: the leaves `stageManifest` recorded"; step 4: lost owner
    liveness is retry-later (`NETWORK_ERROR`); a missing dependency proof, or a manifest this transaction did
    not stage and the ref does not commit, is `LOGICAL_ERROR`; drop the missing-body sentences of steps 4 and 6.
  - `W:906-913`: the closure captures no body; `staged_leaves` points into this transaction's map.
  - The six relink and test texts listed under Files: remove "body-absent precommit" from the causes they name.
  - Not changed: `src/Storages/MergeTree/DataPartsExchange.cpp:1408` (upstream file; the report names it).

- [ ] **Step 1: Write the failing tests**

  ```cpp
  /// T/gtest_cas_part_write.cpp, suite CASPartWriteTxn unless named; the stage tests go after :2376
  TEST(..., PromoteSendsNoManifestGet)
  //  b = std::make_shared<DB::Cas::tests::CountingBackend>(); s = openPool(b);
  //  build = startBuildFor(s, ns, "part_1"); id = build->stageManifest({blobManifestEntry("data.bin", "payload")});
  //  build->precommitAdd(ns, "part_1", id); build->putBlob(idOf("payload"), BlobSource::fromString("payload"));
  //  EXPECT_TRUE(build->promote(ns, "part_1", build->buildId(), id)); key = s->layout().manifestKey(id);
  //  EXPECT_EQ(b->getCount(key), 0u); EXPECT_EQ(b->getStreamCount(key), 0u); EXPECT_EQ(b->writeCount(key), 1u);
  //  EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());        // after the counts: a reader may GET the body

  TEST(..., StageManifestRefusesAnEntryPathWithDotDot)
  //  keys_before as :2357; expectThrowsCode(CORRUPTED_DATA,
  //      [&] { build->stageManifest({blobManifestEntry("a/../b.bin", "x")}); }); keys_after == keys_before
  TEST(..., StageManifestRefusesARecordLineOverTheLineCap)
  //  the same with {wideBlobManifestEntry(70'000)}: one record line over 64 KiB, far under the size caps

  TEST(..., ManifestCapEncodedBytesJustUnderStagesSuccessfully)    /// rewritten; pin
  //  std::vector<ManifestEntry> entries; ASSERT_NO_FATAL_FAILURE(fillEntriesToTheEncodedCap(ns, entries));
  //  the pool with the frozen clock, as today (:2326)
  //  id = build->stageManifest(entries); EXPECT_EQ(id.root_namespace, ns); head(manifestKey(id)) has a value

  #ifndef DEBUG_OR_SANITIZER_BUILD
  TEST(..., PromoteOfAManifestStagedElsewhereIsRefused)
  //  a = startBuildFor(s, ns, "part_1"); id = a->stageManifest({}); a->precommitAdd(ns, "part_1", id);
  //  other = startBuildFor(s, ns, "part_1");                       // stages nothing
  //  tail = s->tailSinceSnapshotCountForTest(ns);
  //  try { other->promote(ns, "part_1", other->buildId(), id); FAIL(); } catch (const DB::Exception & e)
  //  { EXPECT_EQ(e.code(), LOGICAL_ERROR);
  //    EXPECT_NE(e.message().find("was not staged by this transaction"), String::npos); }
  //  EXPECT_EQ(s->tailSinceSnapshotCountForTest(ns), tail); EXPECT_FALSE(s->resolveRef(ns, "part_1").has_value());
  //  EXPECT_TRUE(a->promote(ns, "part_1", a->buildId(), id));      // the owner still promotes
  #endif
  #if defined(DEBUG_OR_SANITIZER_BUILD)
  TEST(CASPartWriteTxnDeathTest, PromoteOfAManifestStagedElsewhereAborts)
  //  the same setup; EXPECT_DEATH({ other->promote(ns, "part_1", other->buildId(), id); },
  //      "was not staged by this transaction");
  #endif

  #if USE_AWS_S3
  TEST(CASPartWriteTxnStageManifestRetry, RefusedPutReturnsNoId)   /// pin
  //  b->refuse_count = 1; expectThrowsCodeWithMessage(NETWORK_ERROR, "definitively failed",
  //      [&] { build->stageManifest({blobManifestEntry("a.bin", "a")}); });
  //  EXPECT_EQ(b->put_attempts, 1); list(s->layout().manifestNamespacePrefix(ns)) is empty, as :2605
  #endif
  ```

  The refusal knob of `ManifestPutFaultBackend` (`:2442`). The member is unconditional; the branch that names
  an S3 type is guarded, as `T/cas_test_helpers.h:2594-2599` does, because the class is compiled without S3 too:

  ```cpp
  int refuse_count = 0;                /// remaining definite refusals of matching body PUTs
  /// in `write`, after the `attempt_cap` check and before `maybeFault(key, bytes, access)`:
  #if USE_AWS_S3
          if (refuse_count > 0)
          {
              --refuse_count;
              throw DB::S3Exception("ManifestPutFaultBackend: simulated malformed request",
                                    Aws::S3::S3Errors::UNKNOWN, "MalformedXML");
          }
  #endif
  ```

  `USE_AWS_S3` comes from `config.h`; include it if the file does not see the macro. No local build checks the
  no-S3 side: `build_fast` has `ENABLE_TESTS=0` and does not compile the unit tests.

  The helper of the rewritten cap test, next to `wideBlobManifestEntry` (`:2298`). It returns `void` because it
  holds a fatal assertion:

  ```cpp
  constexpr size_t kWideEntryPathLen = 60'000;
  /// Fills `out` with `Blob` entries whose manifest under `ns` encodes to exactly kExpectedManifestEncodedCap
  /// bytes. Every path is at most kWideEntryPathLen + 4096 bytes, so every record line is under 64 KiB.
  void fillEntriesToTheEncodedCap(const RootNamespace & ns, std::vector<ManifestEntry> & out);
  ```

  Algorithm, exactly. `entry(i, len)` is `wideBlobManifestEntry(len)` with the first 8 path bytes replaced by
  the zero-padded decimal `i`. `size(v)` is `encodePartManifest` of a `PartManifest` with `root_namespace_id = ns`
  and entries `v`, as `manifestEncodedSizeForPathLen` (`:2257`) builds it.
  1. `s1 = size({entry(0, P)})`, `s2 = size({entry(0, P), entry(1, P)})`, `P = kWideEntryPathLen`;
     `c = s2 - s1` is the cost of one full entry, `fixed = s1 - c`.
  2. `k = (cap - fixed) / c`, `r = (cap - fixed) - k * c`. `out` = `entry(0, P)` … `entry(k - 1, P)`.
  3. Spend `r`: lengthen the paths of entries 0, 1, … by `min(4096, what is left)` each until nothing is left.
  4. `d = size(out) - cap` (not 0: the trailer's count has more digits than in the probes). Change the path
     length of entry `k - 1` by `-d`.
  5. `ASSERT_EQ(size(out), kExpectedManifestEncodedCap)`.

  `findManifestEncodedCapBoundaryPathLen` and `.ManifestCapEncodedBytesOverThrowsBeforeBodyWrite` (`:2345`) stay
  unchanged: the size cap fires first.

- [ ] **Step 2: Run red.** Filter `CASPartWriteTxn*:CASPromoteRepublish*` on `build`. Expected:
  `PromoteSendsNoManifestGet` fails with a count of 1; the two `StageManifestRefuses…` tests fail (no
  exception); `PromoteOfAManifestStagedElsewhereIsRefused` fails (the promote commits). The two pins pass. The
  death twin is not run red.
- [ ] **Step 3: Implement** the rules, then the texts.
- [ ] **Step 4: Edit `Q/05485_cas_ref_append_requests.sh`:** after the loop, one query for `$Q1` only, with
  the loop's `WHERE`, that selects `ProfileEvents['CASManifestGet'] + ProfileEvents['CASManifestGetStream']` and
  the control `ProfileEvents['CASManifestPut']`; one line in the header comment for it. The reference gains a
  third line `0	1` (tab): no manifest read, one manifest create. Predicted, not run. No value for `OPTIMIZE`:
  a merge reads its source manifests, and that count is not derived.
- [ ] **Step 5: Run green.** Filter `CASPartWriteTxn*:CASPromoteRepublish*:CASPartFolderAccess*`, then `CAS*`
  on `build` (tag `t3_pre`), then the first filter on `build_asan` (tag `t3_green`). Note the wall time of the
  rewritten cap test on both builds in the report. No existing test beyond the cap test is rewritten. Kept, read:
  - `CASPartWriteTxn.MissingDependencyProofFailsClosed` (`T/gtest_cas_part_write.cpp:1332`),
    `.PromoteRevalidatesBlobPresenceFailClosed` (`:1386`), `.WDepSetCrossAlgoSatisfactionFailsClosed` (`:2386`):
    each dies on "no dependency proof". They are the tests that fail if `stageManifest` records no leaves.
  - `CASPromoteRepublish.PromoteSameManifestIsIdempotent` (`T/gtest_cas_promote_republish.cpp:121`): the only
    `promote` in `T/` of an id another transaction staged (105 four-argument calls compared with the object
    that staged their id); the ref commits it, so the closure returns before the new check.
  - `CASPartWriteTxnStageManifestRetry.BudgetExhaustionMapsToNetworkError` (`T/gtest_cas_part_write.cpp:2584`),
    `CASPartWriteTxn.StageManifestSentThenGaveUpSaysUncertain` (`:3228`).
  - Every manifest `GET` count is taken after `resetCounts` in a window without a `promote`:
    `T/gtest_cas_part_folder_access.cpp:205`, `:235`, `:926`, `:967`, `:995`, `:1005`, `:1032`, `:1131`, `:1155`,
    `:1187`, `T/gtest_cas_pool.cpp:1092`, `:3466`, `:3547`, `T/gtest_cas_gc_bounded_walk.cpp:465`,
    `T/gtest_cas_manifest_reader.cpp:48`, `T/gtest_cas_orphan_nomination.cpp:271`,
    `T/gtest_cas_orphan_sweep_requests.cpp:338`, `T/gtest_cas_orphan_manifest_sweep.cpp:644`, `:662`.
  - No test deletes or replaces a manifest body between `stageManifest` and `promote`, and no `ManifestEntry` in
    `T/` is built without a path; `wideBlobManifestEntry` is used by the two cap tests only.
- [ ] **Step 6: Commit,** then the gate on `build` and `build_asan` (tag `t3_gate`). Expected: 3147 and 3151.
  A red gate: the Tests rule.
  `git add $CA/Pool/CasPartWriteTxn.h $CA/Pool/CasPartWriteTxn.cpp $CA/ContentAddressedMetadataStorage.cpp $CA/ContentAddressedExchange.h $T/gtest_cas_part_write.cpp $T/gtest_cas_protocol_scenarios.cpp $Q/05485_cas_ref_append_requests.sh $Q/05485_cas_ref_append_requests.reference`
  Subject: `Validate a staged CAS manifest and promote without reading it back`. Body: `promote` read its own
  manifest back only to decode it and to list its blob leaves. `stageManifest` now decodes the bytes it is about
  to write, so a body no reader accepts (a `..` path, a record line over 64 KiB) fails before any object
  exists, and it keeps the leaves. A manifest another transaction staged is refused unless the ref already
  commits it.

### Task 4: Commit 4, docs {#task-4}

Implementer `ca-impl` (sonnet). Reviewer `ca-review-lite`. Plain words, present tense, no invariant tags.

**Files and the facts each must state:**

| File | Change |
|---|---|
| `D/architecture/manifests-and-refs.md:274-288` | The paragraph: no ordinary append reads the catalog; only the terminal `RemoveNamespace` append reads its row first. Table: `Positive append` is `1: log PUT`; `New part without blobs` is `4: manifest PUT 1, precommit 1, promote 1, Move 1`; the terminal row stays 2. The closing line keeps "Warm namespace, one writer." and loses the catalog clause |
| `D/operations/migration.md:194` | "A positive append costs 1 request instead of 4" |
| `D/architecture/part-lifecycle.md:56`, `:72`, `:98` | Remove the arrow `GET and validate the precommit manifest body`. Step 4 (`:98`) sends no manifest request. Step 1, `stageManifest` (`:72`): it decodes the bytes it is about to write, fails with `CORRUPTED_DATA` before any object exists, and records the blob leaves that `promote` checks |
| `D/architecture/namespaces.md:118-151` | In "Removal": a runtime acquired for a row that is already `Removing` (a presence probe, a retried `DROP`) refuses writes with "is Removing" and serves no reads, and a failed removal does not reopen it. At `:139-141`: if the write lands after a remount, the row becomes `Removing` while the table keeps accepting writes and serving reads; the stop comes when an admission reads the row (a restart, a remount, a `DROP TABLE`); until then GC does not erase the row and deletes none of the table's live data (it still cleans what the table itself dropped, as for a live table) |
| `D/operations/troubleshooting.md` (the symptom table) | New row. Symptom: the log repeats `CAS GC namespace removal is stuck … cleanup evidence is absent because terminal has not folded` and `CASGCStuckRemovals` grows, while the table may still answer. Diagnosis: its catalog row is `Removing` and no terminal record exists: a `DROP TABLE` failed and was not retried. Action: copy the data out while the table answers, then run `DROP TABLE` again; no tool returns the row to `Live`; after a restart or a remount writes fail with "is Removing", reads see no files, and the data stays in the store |
| `D/architecture/replication.md:72`, `:113` | Remove "body-absent precommit" from the causes of a relink fallback |

**Interfaces:** consumes the final behaviour only. The numbers come from unit tests:
`CASRefWriterAppendLane.PositiveAppendIsOneCreate`, `.PartPublishCostsTwoLogCreatesAndNoCatalogOrCkptRequest`,
`CASPartWriteTxn.PromoteSendsNoManifestGet`. `Move` 1 follows from the first (a `Move` is a positive append);
no test publishes with a `Move`. The warning text is `stuckRemovalWarning` (`CA/Gc/CasGc.cpp:217-242`); the
threshold is `gc_stuck_removal_rounds`, 10 rounds.

- [ ] **Step 1:** Write the changes.
- [ ] **Step 2:** `grep -n '^#' <each changed file> | grep -v '{#'` prints nothing.
  `grep -rn 'body-absent\|GET and validate the precommit\|2 requests instead of 4\|manifest .GET. 1' $WT/$D`
  prints nothing. `grep -rn 'catalog .GET., log' $WT/$D` prints only the terminal row.
- [ ] **Step 3: Commit,** then the gate on both builds (tag `t4_gate`; `ninja` has no work, the run records
  the head). Expected: 3147 and 3151. A red gate: the Tests rule.
  `git add $D/architecture/manifests-and-refs.md $D/operations/migration.md $D/architecture/part-lifecycle.md $D/architecture/namespaces.md $D/operations/troubleshooting.md $D/architecture/replication.md`
  Subject: `Document the CAS append without the catalog read`. No body.

### Task 5: Test only, `05387` without Keeper fault injection {#task-5}

Implementer `ca-fix`. Reviewer `ca-review-lite`.

`Q/05387_cas_publish_counts_replicated` fails at about 1 % per run: `tests/config/users.d/insert_keeper_retries.xml:5`
sets `insert_keeper_fault_injection_probability` to 0.01 for every stateless run, a retried replicated insert
moves the part three times (publish, rollback, publish), and the test pins `ref_move` = 1. No other stateless
test pins event or request counts of a replicated insert on a CAS disk. Checked: every `Q/*_cas_*` test that
mentions `Replicated`. `05002` and `05218` already set the setting; `04283`, `05025` and `05301` count rows
and parts, not CAS events; `05054` is a plain `MergeTree`.

**Files:**
- Modify: `Q/05387_cas_publish_counts_replicated.sh:29`, `:38` (the two `INSERT` lines)

- [ ] **Step 1:** Both `INSERT` commands become
  `${CLICKHOUSE_CLIENT} --insert_keeper_fault_injection_probability=0 -q "INSERT INTO r1 …"` (the form of
  `Q/00652_replicated_mutations_zookeeper.sh:23`). One comment line above the first says why: a retried insert
  moves the part three times and the test pins one move. Plan decision: both inserts, although only the first
  one's counts are pinned row by row. The reference does not change.
- [ ] **Step 2:** `grep -c -- '--insert_keeper_fault_injection_probability=0' $WT/$Q/05387_cas_publish_counts_replicated.sh`
  prints 2; `bash -n` on the file prints nothing. Not run. The report states the residual: a real Keeper
  session loss still retries the insert (jobs with `ZOOKEEPER_FAULT_INJECTION=1` install
  `tests/config/config.d/zookeeper_fault_injection.xml`, `tests/config/install.sh:362-366`); the test is not
  changed for it.
- [ ] **Step 3: Commit,** then the gate on both builds (tag `t5_gate`). Expected: 3147 and 3151. A red gate:
  the Tests rule.
  `git add $Q/05387_cas_publish_counts_replicated.sh`
  Subject: ``Pin Keeper fault injection off in `05387_cas_publish_counts_replicated` ``. Body:
  `tests/config/users.d/insert_keeper_retries.xml` injects Keeper faults into 1 % of inserts. A retried
  replicated insert moves the part three times, and the test pins `ref_move` = 1. A real Keeper error still
  retries the insert.

### After Task 5 (controller, no commit by an implementer) {#after-task-5}

- [ ] Whole-branch review, then the history rewrite to five commits (a gate-fix commit folds into its task's
  commit). The `CAS*` gate on `build` and `build_asan` on the final head; the `build_fast` compile.
- [ ] **CAS stateless tests on each code commit (plan decision, replaces the per-commit gate of §8).** Who: the
  controller. On which commits: the first three commits of the final history (the closed runtime, the read
  removal, `promote`), each checked out and built with its own `ninja clickhouse`. Which tests: every
  `Q/*_cas_*` test, with `05485_cas_ref_append_requests` among them; `05387_cas_publish_counts_replicated` on
  the final head too. A failure is fixed in the branch, the history is rewritten again, and the three runs are
  repeated; nothing is called done before the three are green. One exception: `05387` with `ref_move 3` on
  commits 1 to 3 is the flake that the last commit fixes; rerun that test.
- [ ] `05485` carries predicted values: `0 0 3 0` twice after commit 2, plus `0 1` after commit 3. A different
  number stops the run and goes to the owner; the product is not adjusted to it.
- [ ] Measure the `INSERT` again: expected 19 requests with new blobs and 14 with existing blobs (§1).
