# CAS improvements, wave 1: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land stories A to I of the wave as 21 commits on `work/antalya-26.8/cas-improvements-wave1`, each with
its tests and docs, then rebuild them as a clean series over the head of PR 2831.

**Architecture:** One commit per row of the spec's commit series, in the spec's order. Story A is a port of an
existing reviewed series; stories B to H are new code written test-first; story I is CI configuration. All work
happens in one worktree with one build directory, so tasks run one after another.

**Tech Stack:** C++ (ClickHouse `CA/` metadata storage), GoogleTest (`unit_tests_dbms`), stateless and integration
tests, praktika CI definitions, Markdown docs.

**Spec:** `docs/superpowers/specs/2026-11-12-cas-improvements-wave1-design.md` (binding; a conflict between this
plan and the spec resolves to the spec).

## Global Constraints {#global-constraints}

### Paths {#paths}

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1` (branch `work/antalya-26.8/cas-improvements-wave1`, base `fa1b89d1529`) |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master` |
| `P` | `$M/tmp/prbundle` |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| `T/` | `src/Disks/tests/` |
| `D/` | `docs/en/antalya/cas/` |
| `S/` | `docs/en/operations/system-tables/` |
| Facts | `$P/wave1/facts/{A-2570,B-gc,C-gcsmall,D-settings,E-ci}.md` |
| Story A sources | series `2a034cdb374..dca5e45ce32`; merged tree `76bd251bb19d09e02a5f8e088c46c5279424d8b5` |
| Logs | `$P/wave1/logs/` |
| Briefs, reports | `$P/wave1/briefs/task<N>.md`, `$P/wave1/reports/task<N>.md` |
| Helpers | `$P/wave1/bin/bt.sh`, `$P/wave1/bin/mut.sh`, `$P/wave1/bin/st.sh` |
| Gate scripts | `$P/gates/` (`release.sh`, `asan.sh`, `fast.sh`, `stateless.sh`, `stateless_all.sh`, `style.sh`) |

### Binding rules {#binding-rules}

- **Worktree.** Modify files only under `WT`. Never touch another worktree, a docker container that existed
  before the run, or anything on ports 9010, 9181, 11121, 18123, 18124, 18474, 19000, 19681-19684. Prefix every
  command with `cd $WT &&`. Exceptions, and only these:
  - the helpers and gate scripts write their locks, logs and markers under `$M/tmp` and `$P`;
  - briefs, reports and saved logs go to `$P/wave1/`;
  - the integration gate of Task 13 creates and removes its own containers;
  - the controller commits the measurement report of Task 15 in `M` by exact path.
- **No destructive commands.** Never run `git clean`, `git reset --hard`, `git checkout -- <file>`, `git stash`,
  `git rebase`, `git commit --amend`, `git push`, `git apply --3way`, `git apply --index`. Never delete a build
  directory. Never `kill` or `pkill` by pattern; a hung test binary is stopped by its exact PID.
- **Build and test only through the helpers.** They take the two build locks, run `ninja` with `nice -n 19` and
  without `-j`, and write logs to files.

  ```bash
  cd $WT && nohup $P/wave1/bin/bt.sh <tag> '<gtest_filter>' > /dev/null 2>&1 &
  cd $WT && n=0; until grep -aq 'BT_DONE=' build/bt_<tag>.marker 2>/dev/null || [ $n -ge 54 ]; do sleep 10; n=$((n+1)); done; \
    cat build/bt_<tag>.marker; tail -1 build/build_<tag>.log; tail -4 build/test_<tag>.log
  ```

  Run the wait in the foreground with a 600000 ms timeout and repeat it until the marker exists. Use a new `<tag>`
  per run: `t<task>_<commit>_red`, `_green`, `_gate`. A test log counts only if `build/build_<tag>.log` ends with
  `NINJA_EXIT=0` and names the current head. Every helper and gate script writes a `BUILD_MARKER` line as the
  first line of its test log: head, binary path and mtime, and the ninja exit code where the script built the
  binary (`stateless.sh` builds nothing and prints the code `st.sh` hands it, or `not-built-here`). A log
  without that line, or with a non-zero `NINJA_EXIT` in it, is not evidence; a saved copy keeps the line.
  `bt.sh` takes an optional third argument with extra gtest flags and honours `BT_TIMEOUT=<seconds>`.
- **Gate filter** is exactly `CAS*`. The pass count is the `PASSED` number of the gtest summary.
- **Red first.** Write the test before the product change and run it. The red run must fail on an assertion, not
  on a compile error: when a test needs a new signature or accessor, add it first with the old behaviour. Copy the
  red log to `$P/wave1/logs/red_<commit>_<test id>.log`. A test the plan marks "pin" is green before and after;
  its log is saved as `pin_<commit>_<test id>.log`.
- **Mutation check**, in every task that has a mutation step. After green, one run with the semantic change
  disabled must turn the commit's tests red. Commits D1, D2, E2, F1, G1, H2 and H3 have no mutation step: each
  changes a default, a constant, a log field or a text, and its red run on the tree without the change is that
  experiment.

  ```bash
  cd $WT && nohup $P/wave1/bin/mut.sh <tag> <file relative to WT> $WT/tmp/mut_<tag>.<basename> '<gtest_filter>' > /dev/null 2>&1 &
  cd $WT && n=0; until grep -aq 'MUT_DONE=' build/mut_<tag>.marker 2>/dev/null || [ $n -ge 54 ]; do sleep 10; n=$((n+1)); done; \
    grep -c 'RESTORED=yes' $P/wave1/logs/mut_<tag>.log; grep -a 'FAILED\|PASSED\|GTEST_EXIT' $P/wave1/logs/mut_<tag>.log | tail -8
  ```

  The `RESTORED=yes` count must be 1 and `git diff --stat` must show only intended changes afterwards. A mutation
  that does not compile is rewritten. A mutation is never committed. For story A the mutation run is also the red
  evidence (the series tests do not compile on the base).
- **`LOGICAL_ERROR` tests** are death tests under `DEBUG_OR_SANITIZER_BUILD` and exception tests otherwise.
- **Commit by exact file list.** `git add <files>`, check `git diff --cached --name-only`, `git restore --staged`
  anything that is not yours, then `git commit -s`. Never `git add -A` or `git add .`.
- **Commit message.** Subject verbatim from the task. Body: 1 to 3 sentences, lines of at most 100 columns, saying
  what changes and why. No backlog ids and none of the words "spec", "plan", "story", "draft", "audit". Then a
  blank line, the line `` `CAS*` gtest gate (release): <number> tests passed. ``, a blank line, and the trailers:
  `Signed-off-by` (from `-s`) and `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. No session links
  and no "Generated with" line.
- **Code style.** Allman braces. Comments say why, in one or two lines, with no task ids and no history. Literal
  names in inline code; a function is `f`, not `f()`. Say "exception", not "crash", for a logical error.
- **Scope.** Change only the files the task lists. A defect found elsewhere goes into the report, not the diff.
  Code outside `CA/`, `T/`, `D/`, `S/`, `tests/`, `ci/` changes only where the task names the file.
- **Docs.** They ride with the commit that changes the behaviour. Plain words, no invariant tags, no task
  numbers. Every changed default is stated with its old value and the key that restores it. A new heading under
  `docs/` carries an explicit `{#anchor}`.
- **Report.** `$P/wave1/reports/task<N>.md`: what changed, red evidence with log paths, mutation result, pass
  count per commit, deviations, doubts. The final message repeats status, commit SHAs, pass counts and doubts.
- **Implementers do not dispatch subagents** and do not review their own work with one.

### Pass counts {#pass-counts}

Release `CAS*`, base 3005 at `fa1b89d1529`. A different number is a deviation the report explains.

| Commit | New `CAS*` tests | Expected |
|---|---|---|
| A1 | A-1 to A-4 | 3009 |
| A2 | A-5 to A-7 | 3012 |
| A3 | A-8 | 3013 |
| B1 | B-1 to B-5 | 3018 |
| C1 | C-1, C-2 | 3020 |
| C2 | C-3 | 3021 |
| D1 | D-1 | 3022 |
| D2 | D-2 | 3023 |
| E1 | E-1 | 3024 |
| E2 | E-2, E-3 | 3026 |
| F1 | F-1, F-2 | 3028 |
| G1 | G-1 | 3029 |
| G2 | none (G-3, G-4 are in `ObjectStorageIteratorAsync*`) | 3029 |
| H1 | H-1, H-2 | 3031 |
| H2 | none (H-3 extends a test, H-4 is stateless) | 3031 |
| H3 | H-5 | 3032 |
| H4 | H-6, H-7 | 3034 |
| I1 to I4 | none | 3034 |

## Review Focus {#review-focus}

1. **The admission guard and the locks.** A `ColdAdmission` destroyed while `ref_queue_mutex` or a runtime
   `state_mutex` is held self-deadlocks, and a guard from before a remount must not release a slot created after
   it. Owner: Task 2; tests B-4 (every early exit) and case 2 of B-3 (a second admission of the same name
   started after the remount keeps its slot when the older guard is released).
2. **The erase give-up path.** After C1 a perpetual conflict must still end at the window deadline with
   `NETWORK_ERROR` and a message naming the deadline, in fewer than 100 attempts, and a pause that crosses the
   deadline must not be followed by another erase write. Owner: Task 3; pin
   `PerpetualConflictGivesUpWithinOneWindowNotAtTheAttemptCap` plus the deadline case in C-1.
3. **A request bound to no window.** After E1 every request must get its window from its plane. A site that still
   binds with a literal, or a test that expects 90 s by number, hides a wrong budget. Owner: Task 6; the
   implementer lists every `bind` caller, and E-1 covers `standard`, `once`, `within` and `untilLeaseSafe`.
4. **Other consumers of the async iterator.** With the default flag `nextBatch` must behave as before for every
   existing caller, including an empty page that has a continuation token and an iterator destroyed after one
   batch. Owner: Task 8; G-3, G-4 and a default-path case in G-3.
5. **Tests and alerts that depend on a changed default.** An integration test that waits out the lease must keep
   the 30 s lease in a config the killed node really loads; an alert on the success age must not read "no value"
   as healthy. Owner: Task 5 (the reviewer checks each of the four configs is attached to the node) and Task 9
   (the docs carry an `absent` rule next to the age rule).

---

### Task 0: Helpers and baseline (controller) {#task-0}

- [ ] **Step 1:** `$P/wave1/bin/bt.sh` and `mut.sh` exist, pass `bash -n`, take `$M/tmp/ninja.lock` then
  `$P/build.lock`, and write mutation logs to `$P/wave1/logs/`. Add `st.sh <tag> '<test names>'`: builds
  `clickhouse` under both locks, then runs `ONLY='<test names>' $P/gates/stateless.sh <tag>`; marker
  `build/st_<tag>.marker`, last line `ST_DONE=<rc>`. `bt.sh`, `mut.sh`, `$P/gates/stateless.sh`,
  `$P/gates/release.sh` and `$P/gates/asan.sh` write the `BUILD_MARKER` line first in their test log. `bt.sh`
  accepts extra gtest flags as its third argument and `BT_TIMEOUT`.
- [ ] **Step 2:** `cd $WT && git status --short` is empty and `git rev-parse --short HEAD` is `fa1b89d1529`.
- [ ] **Step 3:** Record the base and the baseline 3005 in the ledger, with the pre-flight conflict table.

### Task 1: Commits A1, A2, A3, story A: port the retirement series {#task-1}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite` (port fidelity). Read spec §3 and
`$P/wave1/facts/A-2570.md`.

**Files** (exactly these ten):
- `CA/Gc/CasGc.cpp`, `CA/Gc/CatalogLifecycleReconciler.{cpp,h}`, `CA/Pool/CasPool.h`,
  `CA/Pool/CasRefCatalog.{cpp,h}`, `CA/Pool/CasRefLedger.{cpp,h}`
- Test: `T/gtest_cas_gc_frontier_gate.cpp`, `T/gtest_cas_ns_file_incarnation.cpp`

**Interfaces:**
- Consumes: the merged tree. `git show 76bd251bb19d09e02a5f8e088c46c5279424d8b5:<path> > <path>` writes the final
  content of one file without touching the index. The series commits are readable with `git show <sha>`.
- Produces (final state, used by Task 2):

  ```cpp
  struct RefNameSlot { std::shared_ptr<RefTableRuntime> current; uint64_t retirements = 0; };   /// map ref_name_slots
  uint64_t refNameRetirements(const RootNamespace & ns) const;                                   /// under ref_queue_mutex
  acquireRefTableRuntime(life, admitted_generation, uint64_t retirements_seen, RuntimeAdmission admission);
  void refuseTerminalLife(...);
  uint64_t refNameRetirementsForTest(const RootNamespace & ns) const;                            /// ledger and pool
  deleteCompletedRemovingAtSnapshot(..., const std::function<void(const NamespaceLifeId &)> & before_erase = {});
  CatalogLifecycleReconciler::reconcile(refresh_authority, before_erase);                        /// second arg required
  ```

- Split of the final content into three commits:

  | Commit | Series commits | Content |
  |---|---|---|
  | A1 | `06ac915bf49`, `e8177d91398`, `19b5df85a66`, `d7135ea0330` | `RefNameSlot`, the five capture sites, the refusal in `acquireRefTableRuntime`, `refuseTerminalLife`, the slot erased only when `retirements == 0`, the four null-safe test accessors, two reader refusals moved to `throwCasTransientUnavailable`; tests A-1 to A-4 |
  | A2 | `76730045054`, `2b01a1d77e1`, `f95440d12f1` | `before_erase`, the required second argument of `reconcile`, the callback in `Gc::drainCompletedRemoving`, removal of the two post-drain loops in `CA/Gc/CasGc.cpp`, the five existing `reconcile` callers (four no-op, the fifth counts and asserts one announcement); tests A-5 to A-7 |
  | A3 | `30f75bb5d22`, comments of `dca5e45ce32` | eviction of a runtime with a closed or applied removal increments `retirements`; test A-8 |

  The test fixes of `bb7b861b95b` and `dca5e45ce32` go into the commit that owns the fixed test.

- [ ] **Step 1:** Write all ten files from the merged tree. Save copies under `$WT/tmp/A_final/`. Run
  `bt.sh t1_final 'CAS*'`; expected 3013. This proves the port before it is split.
- [ ] **Step 2:** Reduce the working tree to A1: take out the A3 and A2 content of the table. Files changed
  against `HEAD`: `CA/Pool/CasRefLedger.{cpp,h}`, `CA/Pool/CasPool.h`, `T/gtest_cas_ns_file_incarnation.cpp` only.
- [ ] **Step 3:** Run `bt.sh t1_a1_gate 'CAS*'`; expected 3009.
- [ ] **Step 4:** Red evidence for A1, one `mut.sh` run (`t1_a1_red`, filter `CASNsFileIncarnation*`) on a copy of
  `CA/Pool/CasRefLedger.cpp` with three changes disabled: the retirement comparison in `acquireRefTableRuntime`,
  the throw in `refuseTerminalLife`, the `retirements == 0` condition of the slot erase. Expected red: A-1, A-2,
  A-4, A-3. The log header names what was disabled. Copy to `$P/wave1/logs/red_A1.log`.
- [ ] **Step 5:** Commit A1. Subject: `Refuse a cold admission of a namespace life retired after its read`.
- [ ] **Step 6:** Put the A2 content back. Run `bt.sh t1_a2_gate 'CAS*'`; expected 3012.
- [ ] **Step 7:** Red evidence for A2 (`t1_a2_red`, filter `CASCatalogLifecycleReconciler*:CASNsFileIncarnation*`):
  `CA/Pool/CasRefCatalog.cpp` with the `before_erase` call removed. Expected red: A-5, A-6, A-7. Copy to
  `$P/wave1/logs/red_A2.log`.
- [ ] **Step 8:** Commit A2. Subject: `Announce a namespace-life retirement before its catalog erase`.
- [ ] **Step 9:** Restore the ten files from `$WT/tmp/A_final/`. `git diff 76bd251bb19d09e02a5f8e088c46c5279424d8b5
  -- <the ten files>` prints nothing. Run `bt.sh t1_a3_gate 'CAS*'`; expected 3013.
- [ ] **Step 10:** Red evidence for A3 (`t1_a3_red`, filter `CASNsFileIncarnation*`): `CA/Pool/CasRefLedger.cpp`
  with the eviction increment removed. Expected red: A-8. Copy to `$P/wave1/logs/red_A3.log`.
- [ ] **Step 11:** Commit A3. Subject: `Record a retirement when a closed runtime is evicted`.

Test names (spec §3.3): A-1 `CASNsFileIncarnation.ColdReaderDoesNotRepublishALifeRetiredAfterItsCatalogRead`,
A-2 `…ColdWriterDoesNotRepublishALifeRetiredAfterItsCatalogRead`, A-3
`…RetirementWithASuccessorResidentIsRecordedAndSurvivesEviction`, A-4 `…NamespaceLifeRefusesARecoveredTerminalLife`,
A-5 `CASCatalogLifecycleReconciler.AnnouncesEachLifeBeforeItsRowIsErased`, A-6
`CASCatalogLifecycleReconciler.AFailingAnnouncementSendsNoErase`, A-7
`CASNsFileIncarnation.ColdReaderIsRefusedWhenReleasedRightAfterTheDrainErasesItsRow`, A-8
`…ColdAdmissionIsRefusedWhenARemoteLeaderRetiredTheLifeAndTheRuntimeWasEvicted`.

### Task 2: Commit B1, story B: bound the retirement records {#task-2}

Implementer `ca-surgeon` (opus, medium). Reviewer `ca-review` (opus, high), who reads the diffs of Task 1 and
Task 2 together. Read spec §4 and `$P/wave1/facts/A-2570.md` lines 85-127.

**Files:**
- Modify: `CA/Pool/CasRefLedger.{h,cpp}`, `CA/Pool/CasPool.h`
- Test: `T/gtest_cas_ns_file_incarnation.cpp`

**Interfaces:**
- Consumes: the final state of Task 1.
- Produces:

  ```cpp
  struct RefNameSlot { std::shared_ptr<RefTableRuntime> current; uint64_t retirements = 0; uint32_t admissions_in_flight = 0; };
  uint64_t ref_name_slots_epoch = 0;                                  /// ledger member, under ref_queue_mutex
  class ColdAdmission;                                                /// move-only; holds name, retirements, epoch
  ColdAdmission beginColdAdmission(const RootNamespace & ns);         /// replaces refNameRetirements(ns) at five sites
  acquireRefTableRuntime(life, admitted_generation, ColdAdmission && admission, RuntimeAdmission kind);
  size_t refNameSlotCountForTest() const;                             /// ledger and pool; ref_name_slots.size()
  ```

- Invariant, stated in the comment on `RefNameSlot::retirements`: a slot exists if and only if it has a `current`
  runtime or `admissions_in_flight > 0`.
- Rules (spec §4.2):
  - `beginColdAdmission` creates the slot if absent, increments `admissions_in_flight`, stores the name, the
    `retirements` value and the epoch.
  - The guard's destructor takes `ref_queue_mutex`, decrements, and erases the slot when the invariant no longer
    holds. A guard of an older epoch releases nothing.
  - A guard is declared before any lock of its function. It is never destroyed while `ref_queue_mutex` or a
    runtime `state_mutex` is held.
  - `acquireRefTableRuntime` consumes the guard inside its critical section. A guard of an older epoch is refused
    with the generation-moved error.
  - `invalidateRemovedCatalogLife` uses `find`: an absent slot gets no record; a present one gets
    `++retirements` and is erased after `current` is reset when `admissions_in_flight == 0`.
  - Eviction increments as in A3 and erases the slot when `admissions_in_flight == 0`.
  - `quiesceRefTablesForRemount` clears the map and increments the epoch.
  - Two captures by one call chain count as two.
  - `refNameRetirements` stays as the body of `refNameRetirementsForTest`.

- [ ] **Step 1:** Add `refNameSlotCountForTest` on the ledger and the pool. Write the five tests before any
  other product change:
  - B-1 `CASNsFileIncarnation.RetirementRecordsDoNotAccumulateAcrossCreateDropCycles` (`openPoolForTest`,
    `gc_fold_max_defer_rounds = 0`, `runRegularRoundReclaiming`, 50 create-drop cycles; expects slot count 0).
  - B-2 `…RemoteLeaderRetirementsLeaveNoRecordWithoutAnAdmission` (two pools, the second leads GC; expects 0 on
    the first).
  - B-3 `…ColdAdmissionInFlightKeepsTheRecordAndReleasesIt` (`ParkedAdmissionBackend`). Case 1: while parked,
    slot count 1 and the retirement is recorded; after release the admission is refused and the count is 0.
    Case 2 (remount): park an admission, call `quiesceRefTablesForRemount`, start a second admission of the same
    name and park it, release the first; the first is refused and the slot count is still 1 (the slot of the
    second admission); after the second is released the count is 0.
  - B-4 `…AbortedColdAdmissionsReleaseTheirInFlightCount` (pin; cases: reader on an absent row; reader on a
    `Removing` row; writer whose runtime is resident; `namespaceStillLogicallyPresent` on an absent row;
    `dropNamespaceImpl` on a missing row and on an `expected_incarnation` mismatch; an admission whose catalog
    read throws; each ends with the slot count it started with).
  - B-5 `…ColdAdmissionOfAPredecessorIsRefusedAfterItsSuccessorIsEvicted` (pin of the refusal;
    `ParkedAdmissionBackend`, `ref_table_cache_bytes = 1`). Its slot-count line comes in step 5.
- [ ] **Step 2:** Run `bt.sh t2_red 'CASNsFileIncarnation*'`. Expected red: B-1 with 50, B-2 with 1, B-3 with a
  slot count of 1 after the refused admission. B-4 and B-5 pass. Copy to `$P/wave1/logs/red_B1.log` and to
  `pin_B1_B-4_B-5.log`.
- [ ] **Step 3:** Implement the guard, the epoch and the rules above.
- [ ] **Step 4:** If case 2 of B-3 cannot park two admissions through the pool, drive the second one through a
  ledger-level test seam and name the seam in the report. The assertion stays: a guard of an older epoch
  releases nothing of a slot created after the remount.
- [ ] **Step 5:** Add to B-5 the final slot count 1 (the slot of the other namespace). Update A-3: rename to
  `RetirementWithASuccessorResidentIsRecorded`, slot count 1 after the eviction (the slot of `other`). Update
  A-8: slot count 1 on the `owner` pool after the join.
- [ ] **Step 6:** Run `bt.sh t2_green 'CASNsFileIncarnation*:CASCatalogLifecycleReconciler*'`; all pass.
- [ ] **Step 7:** Mutation (`t2_mut`): the guard's destructor does not erase the slot. B-4 must fail on its
  absent-row and throwing-read cases. (B-1 and case 1 of B-3 release their slot inside
  `acquireRefTableRuntime` or keep a runtime, so they do not see this mutation.)
- [ ] **Step 8:** List in the report every place a `ColdAdmission` is destroyed and which locks are held there.
- [ ] **Step 9:** Run `bt.sh t2_gate 'CAS*'`; expected 3018.
- [ ] **Step 10:** Commit. Subject: `Keep a retirement record only while a cold admission is in flight`.

### Task 3: Commit C1, story C: pause before the resolution read {#task-3}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review` (opus, high). Read spec §5.1, §5.2 (C1), §5.3 and
`$P/wave1/facts/B-gc.md`.

**Files:**
- Modify: `CA/Pool/CasRefCatalog.cpp` (loop of `deleteCompletedRemovingAtSnapshot`),
  `D/architecture/garbage-collection.md` (the drain section)
- Test: `T/gtest_cas_ref_catalog.cpp`

**Interfaces:**
- Produces: loop order after an erase write that returns `Conflict`: `op.pause(Retry::backoff(attempt + 1))`,
  then the admission check, the resolution read, the outcome evaluation. No pause at the loop end. The pause
  schedule and the outcome rules do not change. The two comments that describe the old order are rewritten.

- [ ] **Step 1:** Write C-1 `CASRefCatalogRemoval.AWriterLandingDuringEveryBackoffCannotStarveTheErase`
  (`WriteCountingBackend`; `FakeClock` whose `sleepFn` commits one external catalog write per pause; expects
  `Deleted` on the second erase and exactly one pause) and C-2
  `CASRefCatalogRemoval.AConflictedEraseSendsTheNextWriteFromAReadTakenAfterThePause` (`journalFor`,
  `refuseNextWrite`; asserts the pause precedes the explicit catalog read; no exact event count).
- [ ] **Step 2:** Run `bt.sh t3_red 'CASRefCatalogRemoval*'`. Expected red: C-1 runs to the 90 s fake deadline and
  throws code 210; C-2 sees the read before the pause. Copy to `$P/wave1/logs/red_C1.log`.
- [ ] **Step 3:** Reorder the loop. Update the docs section.
- [ ] **Step 4:** Add to C-1 a case where the pause itself crosses the deadline: the call throws `NETWORK_ERROR`
  with "deadline" in the message and sends no further erase write.
- [ ] **Step 5:** Run `bt.sh t3_green 'CASRefCatalog*:CASCatalogLifecycleReconciler*:CASGCFrontierGate*'`; all
  pass, including the pins `AConflictingEraseBacksOffBeforeItsRetry`,
  `PerpetualConflictGivesUpWithinOneWindowNotAtTheAttemptCap` and
  `CASCatalogLifecycleReconciler.RetriesFromTheMandatoryConflictResolutionCut` (four GETs). A pin that fails is
  a stop: report it, do not edit the pin.
- [ ] **Step 6:** Mutation (`t3_mut`): restore the old order. C-1 and C-2 must fail.
- [ ] **Step 7:** Run `bt.sh t3_gate 'CAS*'`; expected 3020.
- [ ] **Step 8:** Commit. Subject: `Pause a conflicted catalog erase before its resolution read`.

### Task 4: Commit C2, story C: report the erases of a drain that throws {#task-4}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §5.2 (C2), §5.3, §21.

**Files:**
- Modify: `CA/Gc/CasGc.cpp` (`Gc::runRegularRound`, phase `pre_fold_ref_drain`),
  `CA/Gc/CatalogLifecycleReconciler.{h,cpp}`, `S/cas_gc_log.md`
- Test: `T/gtest_cas_gc_frontier_gate.cpp`

**Interfaces:**
- Produces: the reconciler keeps a count of completed erases that is readable after `reconcile` throws.
  `Gc::runRegularRound` writes it to `deleted` of phase `pre_fold_ref_drain` on every exit of the phase scope,
  before the `GcPhaseTimer` destructor runs. The mechanism (out parameter, member, or scope guard) is the
  implementer's choice. No new exception type; the original exception propagates unchanged; the record of a
  successful drain is unchanged; `Gc::rebuildBaseline` is unchanged.

- [ ] **Step 1:** Write C-3 `CASGCFrontierGate.AnAbortedDrainReportsTheRowsItErased` (`DrainRaceBackend`, two
  eligible rows, the second erase refused on every attempt; expects the round to throw and the phase record to
  carry `deleted = 1`).
- [ ] **Step 2:** Run `bt.sh t4_red 'CASGCFrontierGate*'`. Expected red: the phase record has no `deleted` key.
  Copy to
  `$P/wave1/logs/red_C2.log`.
- [ ] **Step 3:** Implement. Update `S/cas_gc_log.md`.
- [ ] **Step 4:** Run `bt.sh t4_green 'CASGCFrontierGate*:CASGCLog*:CASCatalogLifecycleReconciler*'`; all pass.
- [ ] **Step 5:** Mutation (`t4_mut`): set the metric only on the success path. C-3 must fail.
- [ ] **Step 6:** Run `bt.sh t4_gate 'CAS*'`; expected 3021.
- [ ] **Step 7:** Commit. Subject: `Report the erases of a pre-fold drain that throws`.

### Task 5: Commits D1, D2, story D: 120 s mount lease {#task-5}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §6 and `$P/wave1/facts/D-settings.md`.

**Files:**
- D1 modify: `CA/ContentAddressedSettings.cpp` (`mount_lease_ttl_ms`), `CA/Pool/CasPool.h` and
  `CA/Pool/CasMountRuntime.h` (comments only), `tests/integration/{test_cas_shared_pool,test_cas_drop_pool_member,
  test_cas_gc_sharded,test_cas_early_commit_restart}/configs/storage_conf.xml`, `D/configuration.md`,
  `D/architecture/mounts-and-leases.md`, `D/architecture/garbage-collection.md`, `D/operations/migration.md`,
  `D/operations/troubleshooting.md`
- D1 test: `T/gtest_cas_settings.cpp`
- D2 modify: `CA/Backend/CasRequestBudget.cpp`, `D/architecture/mounts-and-leases.md`
- D2 test: `T/gtest_cas_requests.cpp`

**Interfaces:**
- Produces (D1): setting default `mount_lease_ttl_ms = 120000`; the renew period stays 10000; `PoolConfig` and
  `MountConfig` keep 30000/10000. Their comments say: these are the values of a pool opened without disk
  settings, which is how unit tests open one; a disk defaults to 120000/10000. The phrase "ttl/3 by default" goes.
- Produces (D1): each of the four `storage_conf.xml` files sets a lease of 30000 and a renew period of 10000 on
  its CAS disk. Copy the key spelling from `tests/integration/test_cas_mount_renewal_retry/configs/storage_conf.xml`.
  All nodes of each module load `configs/storage_conf.xml` through `main_configs`.
- Produces (D2): the `LOG_INFO` at the end of `validateCasRequestBudget` carries `write_reserve_ms`
  (`2 × envelope + margin`) and, only when background renewal is on, `renewal_slack_ms`
  (`TTL - period - write reserve`).
- Docs of D1 state: the new default, the old value 30000, the key that restores it, and the costs.

  | Effect | 30 s lease | 120 s lease |
  |---|---|---|
  | Restart wait of a server that died without releasing its mount | 43.0 s | 138.1 s |
  | GC fence of a dead mount | 102.6 s | 223.9 s |
  | `system.cas_mounts` shows `expired` after | 45 s | 180 s |
  | A server whose renewals stop pauses its own writes after the last renewal | 14 s | 104 s |
  | `mayMutate` turns false after | 30 s | 120 s |

  The first three rows are measured; the last two follow from the code and the docs say so. The docs also say
  that `cas_unsafe_remount_no_delay` still skips the restart wait.

- [ ] **Step 1:** Write D-1 `CASContentAddressedSettings.MountLeaseDefaultsSatisfyRequestBudget`: the default
  settings object has TTL 120000 and period 10000; `validateCasRequestBudget` accepts a default budget with
  these values and renewal on; `TTL - period - (2 × envelope + margin)` computed from the default budget is 94000.
- [ ] **Step 2:** Run `bt.sh t5_d1_red 'CASContentAddressedSettings*'`. Expected red: TTL is 30000. Copy to
  `$P/wave1/logs/red_D1.log`.
- [ ] **Step 3:** Change the default, the two comments, the four configs and the docs (spec §6.2 lists the lines).
- [ ] **Step 4:** Run `bt.sh t5_d1_gate 'CAS*'`; expected 3022.
- [ ] **Step 5:** Commit D1. Subject: `Default the CAS mount lease to 120 s`.
- [ ] **Step 6:** Write D-2 `CASRequestBudget.BudgetLogLineNamesReserveAndSlack` next to
  `EnvelopeIsValidatedNotTheBareAttempt`, with the `Poco::StreamChannel` capture of `T/gtest_cas_pool.cpp:2378-2427`:
  with renewal on the line has `write_reserve_ms=16000` and `renewal_slack_ms=94000` for 120000/10000; with
  renewal off it has `write_reserve_ms` and no `renewal_slack_ms`.
- [ ] **Step 7:** Run `bt.sh t5_d2_red 'CASRequestBudget*'`. Expected red: the keys are absent. Copy to
  `$P/wave1/logs/red_D2.log`.
- [ ] **Step 8:** Extend the log line. Name the two fields and their relation in `D/architecture/mounts-and-leases.md`.
- [ ] **Step 9:** Run `bt.sh t5_d2_gate 'CAS*'`; expected 3023.
- [ ] **Step 10:** Commit D2. Subject: `Print the write reserve and the renewal slack at mount`.

### Task 6: Commits E1, E2, story E: configurable request window {#task-6}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §7.

**Files:**
- E1 modify: `CA/Backend/CasRetry.{h,cpp}`, `CA/Backend/CasRequests.{h,cpp}`, `CA/Backend/CasHotKeys.cpp`,
  `CA/Backend/CasRequestBudget.h`, `CA/Pool/CasPool.cpp`, `CA/Tools/CasDecommission.cpp`
- E1 test: `T/gtest_cas_requests.cpp`, `T/gtest_cas_ref_catalog.cpp`, `T/gtest_cas_ref_writer.cpp`
- E2 modify: `CA/ContentAddressedSettings.cpp`, `CA/ContentAddressedMetadataStorage.cpp`,
  `CA/Backend/CasRequestBudget.{h,cpp}`, `D/configuration.md`, `D/architecture/blob-protocol.md`,
  `D/architecture/garbage-collection.md`, `D/operations/troubleshooting.md`, `D/operations/debugging.md`
- E2 test: `T/gtest_cas_settings.cpp`, `T/gtest_cas_part_write.cpp`

**Interfaces:**
- Produces (E1):

  ```cpp
  std::optional<uint64_t> Retry::window_ms;                 /// empty = the default of the request plane
  Retry::bind(now_ms, uint64_t default_window_ms);          /// required; no overload without it
  inline constexpr uint64_t kDefaultRequestWindowMs = 90000;   /// CasRequestBudget.h; E2 makes it 120000
  uint64_t CasRequestBudget::request_window_ms = kDefaultRequestWindowMs;
  CasRequests::CasRequests(..., uint64_t default_window_ms = kDefaultRequestWindowMs);   /// appended last
  ```

  `CasRequests` also gets a private `bindPolicy(const Retry &)`. The constructor parameter has a default because
  more than 50 test call sites build a plane on a bare backend; they keep compiling and run on the default window.
  The seven production sites listed below pass the value of their pool's budget explicitly. The reviewer checks
  by `grep` that no file under `CA/` builds a `CasRequests` without the argument.

  `standard`, `once` and the default argument of `untilLeaseSafe` leave the window empty. `within(ms)` and
  `untilDefinitive` keep explicit values. `bindPolicy` replaces the 14 `policy.bind(owner.now_ms())` calls in
  `CA/Backend/CasRequests.cpp`, the one in `CasOperation::freeze` and the one in `CA/Backend/CasHotKeys.cpp`.
  Every plane takes the value from the budget of its pool: `mount_requests`, `lease_requests`, `gc_requests`,
  the two bootstrap planes in `CA/Pool/CasPool.cpp`, the two decommission planes. The frozen policy deadline
  stays independent of the lease bound. E1 changes no value.
- Produces (E2): `DECLARE(UInt64, request_window_ms, 120000, ...)`, user key `cas_request_window_ms`, copied into
  the budget next to the other budget fields; `kDefaultRequestWindowMs = 120000`; `validateCasRequestBudget`
  throws `BAD_ARGUMENTS` when the window is not larger than one attempt envelope; `recovery_retry_budget_ms` is
  set to `2 × request_window_ms` at the same copy site and its struct default becomes 240000, with no validation.

- [ ] **Step 1:** Add the E1 signatures with the old behaviour: the budget field `request_window_ms`, `bind` and
  `CasRequests` taking the default window, and the seven production planes passing the budget value; an empty
  policy window still resolves to the literal 90000. Update the callers and the tests that no
  longer compile: `T/gtest_cas_requests.cpp` (`makeRequests` and every direct `bind` call pass 90000),
  `T/gtest_cas_ref_catalog.cpp` and `T/gtest_cas_ref_writer.cpp` (arithmetic on `Retry::standard().window_ms`
  switches to `kDefaultRequestWindowMs`).
- [ ] **Step 2:** Write E-1 `CASRequests.StandardPolicyUsesThePlaneRequestWindow` (plane window 7000,
  `FakeClock`): `standard` gives up at 7000 ms; `once` sends one attempt; `within(1000)` keeps 1000;
  `untilLeaseSafe` uses the plane window when it is tighter than the lease bound.
  Write E-3 `CASPartWriteTxn.AGiveUpHonoursTheConfiguredRequestWindow` now as well (pool budget window 8000;
  shape of `AGiveUpReportsHowManyAttemptsItSent`; the give-up comes at the first deadline check past 8 s of fake
  time). It lands in commit E2, but only this tree can show it red.
- [ ] **Step 3:** Run `bt.sh t6_e1_red 'CASRequests*:CASRetry*:CASPartWriteTxn*'`. Expected red: E-1 gives up at
  90000; E-3 gives up after 90 s of fake time. Copy to `$P/wave1/logs/red_E1.log` and `red_E2_E-3.log`. Then
  take E-3 out of the tree until step 9:
  `git diff -- src/Disks/tests/gtest_cas_part_write.cpp > $P/wave1/logs/e3_test.patch`, then
  `git apply -R $P/wave1/logs/e3_test.patch`; `git diff --stat` no longer lists that file.
- [ ] **Step 4:** Make an empty window resolve to the plane default.
- [ ] **Step 5:** List in the report every caller of `Retry::bind` and of the `CasRequests` constructor
  (`grep -rn` output, untruncated) with the window each one passes.
- [ ] **Step 6:** Mutation (`t6_e1_mut`): `bindPolicy` passes `kDefaultRequestWindowMs` instead of the plane
  value. E-1 must fail.
- [ ] **Step 7:** Run `bt.sh t6_e1_gate 'CAS*'`; expected 3024.
- [ ] **Step 8:** Commit E1. Subject: `Make the CAS request window a default of the request plane`.
- [ ] **Step 9:** Add the setting with default 90000 and the copy into the budget, without validation. Write E-2
  `CASContentAddressedSettings.RequestWindowDefaultAndValidation` (default 120000; a window equal to the
  envelope is refused with `BAD_ARGUMENTS`; a default `CasRequestBudget` has a recovery budget of 240000).
  Put E-3 back: `git apply $P/wave1/logs/e3_test.patch`.
- [ ] **Step 10:** Run `bt.sh t6_e2_red 'CASContentAddressedSettings*:CASPartWriteTxn*'`. Expected red: E-2 on all
  three assertions. E-3 passes here, because E1 made the plane honour the pool window; its red evidence is
  `red_E2_E-3.log` of step 3. Copy to `$P/wave1/logs/red_E2.log`.
- [ ] **Step 11:** Set both defaults to 120000, add the validation and the recovery-budget rule, update the docs
  (spec §7.2 lists the lines; every "ninety seconds" becomes "the configured request window
  (`cas_request_window_ms`, default 120 s)"; the docs state the old value 90 s and the recovery-budget relation).
- [ ] **Step 12:** Run `bt.sh t6_e2_gate 'CAS*'`; expected 3026. A test that fails for a hard-coded 90 s on a
  default plane is rewritten in terms of `kDefaultRequestWindowMs` and named in the report.
- [ ] **Step 13:** Commit E2. Subject: ``Add `cas_request_window_ms`, default 120 s``.

### Task 7: Commit F1, story F: unbounded GC round budgets {#task-7}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §8.

**Files:**
- Modify: `CA/ContentAddressedSettings.cpp`, `CA/Pool/CasPool.h`, `D/configuration.md`,
  `D/architecture/garbage-collection.md`
- Test: `T/gtest_cas_settings.cpp`, `T/gtest_cas_ref_gc.cpp`

**Interfaces:**
- Produces: defaults 5000 → 0 for `gc_round_graduation_budget`, `gc_round_redelete_budget`,
  `gc_round_ref_cleanup_budget`, `gc_round_handoff_prefix_wholesale_budget`, `gc_round_outcome_entry_budget`;
  `PoolConfig::gc_round_ref_cleanup_budget` 5000 → 0. Unchanged: `gc_round_sweep_namespace_budget` (20),
  `sweep_recovery_op_budget` (5000), `prefix_wholesale_budget` (20000). The keys stay.

- [ ] **Step 1:** Write F-1 `CASContentAddressedSettings.GcRoundBudgetsDefaultToUnbounded` (the five settings
  and the five `PoolConfig` fields are 0; the three kept values are 20, 5000, 20000) and F-2
  `CASRefGc.ZeroCleanupBudgetProcessesEveryEntryInOneRound` (pin; ten entries; one round with budget 3 leaves
  seven, one round with budget 0 leaves none).
- [ ] **Step 2:** Run `bt.sh t7_red 'CASContentAddressedSettings*:CASRefGc*'`. Expected: F-1 red with 5000; F-2
  green. Copy to `$P/wave1/logs/red_F1.log` and `pin_F1_F-2.log`.
- [ ] **Step 3:** Change the six defaults and the docs: new defaults, old value 5000, the meaning of 0, how to
  restore a cap, and that the first round on a backlogged pool drains the whole backlog.
- [ ] **Step 4:** Run `bt.sh t7_gate 'CAS*'`; expected 3028. A test that relied on a default cap of 5000 sets
  the cap explicitly and is named in the report.
- [ ] **Step 5:** Commit. Subject: `Default five GC round budgets to unbounded`.

### Task 8: Commits G1, G2, story G: one list request per page {#task-8}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review` (opus, high): G2 changes shared iterator code. Read
spec §9 and §13, and `$P/wave1/facts/B-gc.md`.

**Files:**
- G1 modify: `CA/Backend/CasRequests.h`, `CA/Gc/CasGc.cpp`
- G1 test: `T/gtest_cas_s3_single_attempt_client.cpp` (the fixture may move to `T/gtest_cas_backend_listing.cpp`)
- G2 modify: `src/IO/ObjectStorageRequestProfile.h`,
  `src/Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.{h,cpp}`,
  `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp`, `CA/Backend/CasObjectStorageBackend.cpp`
- G2 test: `src/Disks/DiskObjectStorage/ObjectStorages/tests/gtest_object_storage_iterator_async.cpp`, the G-1 file

**Interfaces:**
- Produces (G1): `kCasListPageKeys = 999` next to `forEachListedKey`, with a comment giving the reason (the
  store returns at most 1000 keys per request and the page needs one key more than its size to prove the prefix
  continues). It replaces the default `page_limit = 1000` and both `kListPageLimit = 1000` in `CA/Gc/CasGc.cpp`.
  `listUnder` and the sentinel probe (page of 32) do not change.
- Produces (G2):

  ```cpp
  bool ObjectStorageControlRequest::prefetch_next_page = true;      /// src/IO/ObjectStorageRequestProfile.h
  bool IObjectStorageIteratorAsync::prefetch_next_batch = true;     /// protected

  /// nextBatch
  while (true)
  {
      if (!outcome_future.valid())            /// was: chassert(outcome_future.valid());
          outcome_future = scheduleBatch();
      BatchAndHasNext result = outcome_future.get();
      ...
      if (has_next_batch && prefetch_next_batch)   /// was: if (has_next_batch)
          outcome_future = scheduleBatch();
  ```

  `S3IteratorAsync` takes the flag from the request in the request overload of `S3ObjectStorage::iterate`.
  `ObjectStorageBackend::listUnder` sets `prefetch_next_page = false` on its request. The catch block, the
  `has_next_batch` handling and `deactivate` do not change. No new parameter on any `iterate` overload. Azure
  ignores the flag.

- [ ] **Step 1:** Extend the local HTTP server of the G-1 file: it pages a synthetic sorted key list by
  `continuation-token` and `start-after`, caps `MaxKeys` at 1000, answers `IsTruncated = true` exactly when
  keys remain, and counts requests. Write G-1 `CASBackend.AListWalkOfTenThousandKeysStaysWithinItsRequestBound`:
  a `forEachListedKey` walk of 10000 keys visits each key once, in order, in at most 21 requests.
- [ ] **Step 2:** Run `bt.sh t8_g1_red 'CASBackend.*'`. Expected red: 27 requests. Record the measured number.
  Copy to `$P/wave1/logs/red_G1.log`.
- [ ] **Step 3:** Add the constant and use it at the three sites.
- [ ] **Step 4:** Run `bt.sh t8_g1_gate 'CAS*'`; expected 3029, including the unchanged pins
  `CASObjectStorageBackend.EmulatedListTokenMatchesHeadToken`, `CASBackendListing.ForEachWalksEveryPageOnce`,
  `CASBackendListing.ForEachEmptyPrefixVisitsNothing` and the listing contract test.
- [ ] **Step 5:** Commit G1. Subject: `List CAS pages of 999 keys so the sentinel fits the batch`.
- [ ] **Step 6:** Add the two flags with no effect on `nextBatch`. Tighten the G-1 bound to 11 (this is G-2). Write
  G-3 `ObjectStorageIteratorAsync.WithoutPrefetchTheNextBatchIsFetchedOnDemand` (`ScriptedIteratorAsync`; flag
  off: after the first batch is read the source has served one batch, and serves the second only when the
  consumer steps past the first; an iterator destroyed after one batch has served one; flag on: it has served
  two) and G-4 `ObjectStorageIteratorAsync.AnEmptyPageWithoutPrefetchStillFollowsTheToken` (flag off; a page
  with no keys and a continuation token is followed to the next page).
- [ ] **Step 7:** Run `bt.sh t8_g2_red 'ObjectStorageIteratorAsync*:CASBackend.*'`. Expected red: G-2 with 21
  requests, G-3 on the served count. G-4 may pass; then it is a pin. Copy to `$P/wave1/logs/red_G2.log`.
- [ ] **Step 8:** Implement lazy scheduling and pass the flag from `listUnder` to the S3 iterator.
- [ ] **Step 9:** Run `bt.sh t8_g2_green 'ObjectStorageIteratorAsync*:CASBackend*:CASObjectStorageBackend*'`;
  all pass, including every existing test of the iterator file.
- [ ] **Step 10:** Mutation (`t8_g2_mut`, filter of step 7): `nextBatch` ignores the flag. G-2 and G-3 must fail.
- [ ] **Step 11:** Run `bt.sh t8_g2_gate 'CAS*'`; expected 3029.
- [ ] **Step 12:** Commit G2. Subject: `Let a list caller turn off the next-page prefetch`. The body states
  that the default is unchanged and names the one caller that turns the prefetch off.

### Task 9: Commits H1, H2, story H: GC round line and success age {#task-9}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §10.1, §10.2, §10.5.

**Files:**
- H1 modify: `CA/Gc/CasGcScheduler.cpp`, `D/operations/monitoring.md`
- H1 test: `T/gtest_cas_gc_log.cpp`
- H2 modify: `src/Storages/System/StorageSystemContentAddressedMounts.cpp`,
  `src/Interpreters/ServerAsynchronousMetrics.cpp`, `S/cas_mounts.md`, `D/operations/monitoring.md`,
  `D/operations/debugging.md`, `D/architecture/mounts-and-leases.md`, `D/operations/troubleshooting.md`
- H2 test: `T/gtest_cas_gc_log.cpp`, a new stateless test created with
  `./tests/queries/0_stateless/add-test cas_mounts_gc_success_age_null.sh`

**Interfaces:**
- Produces (H1): `CasGcScheduler::runRoundLogged` writes one `LOG_INFO` before `emit(fin)` on the success path
  and on the exception path, for the outcomes `Success`, `Aborted`, `Failed`, `Stopped` only. Fields by name:
  `round`, `outcome`, `trigger`, `duration_ms`, `deleted` (`objects_deleted`), `condemned`
  (`entries_condemned`), `error_code`, `keys_listed`. The first seven come from the `GcRoundLogRecord` being
  emitted. `keys_listed` is the metric `ref_keys_listed` of phase `defer_decision`, kept in a local of the
  phase sink; 0 when the round ends before that phase. The same metric of phase `fold_ref_group` is not added.
- Produces (H2): `system.cas_mounts.last_success_age_seconds` is `NULL` and the metric
  `CASGCLastSuccessAgeSeconds_<disk>` is absent until `health->ever_succeeded`. Both descriptions drop the
  "0 if it never led" clause. The docs give an alert rule with `absent` next to the age threshold.

- [ ] **Step 1:** Write H-1 `CASGCLog.FinishLogLineMatchesCasGcLogRow` (fixture of `EmitsStartFinishWithCounts`,
  `Poco::StreamChannel` capture as in `T/gtest_cas_pool.cpp:2378-2427`): exactly one line per Finish row with
  outcome `Success`, carrying the same `round`, `outcome`, `duration_ms` and `deleted` as the row; the fixture of
  `NotALeaderRoundEmitsOnlyTheLeasePhase` with the same capture gives a Finish row and no line. Write H-2
  `CASGCLog.AnAbortedRoundLogsExactlyOneFinishLine` (fixture of `TransientThrowIsClassifiedAborted`): one
  line with outcome `Aborted` and a non-zero `error_code`.
- [ ] **Step 2:** Run `bt.sh t9_h1_red 'CASGCLog*'`. Expected red: no such line. Copy to
  `$P/wave1/logs/red_H1.log`.
- [ ] **Step 3:** Implement the line. Add it, its fields and the matching `system.cas_gc_log` columns to
  `D/operations/monitoring.md`.
- [ ] **Step 4:** Mutation (`t9_h1_mut`): the line is written for every outcome. H-1 must fail on its
  `NotALeader` case.
- [ ] **Step 5:** Run `bt.sh t9_h1_gate 'CAS*'`; expected 3031.
- [ ] **Step 6:** Commit H1. Subject: ``Log one `Information` line per finished GC round``.
- [ ] **Step 7:** Extend `CASGCHealth.ReflectsLeadershipAndPendingReclaim` (H-3, pin): `ever_succeeded` is
  false before the first round and true after it. Create the stateless test (H-4) in the shape of
  `05010_cas_mounts_gc_health.sh`, with an inline disk and a long `cas_gc_interval_sec`: the column is `NULL`
  before any round and below 60 after `SYSTEM CAS GC RUN`; after `SYSTEM RELOAD ASYNCHRONOUS METRICS` the
  metric is absent before the round and present after it. Take only the tags `05010` needs for the same reason.
- [ ] **Step 8:** Red run of H-4, with the stateless helper:

  ```bash
  cd $WT && nohup $P/wave1/bin/st.sh t9_h4_red '<test name>' > /dev/null 2>&1 &
  cd $WT && n=0; until grep -aq 'ST_DONE=' build/st_t9_h4_red.marker 2>/dev/null || [ $n -ge 54 ]; do sleep 10; n=$((n+1)); done; \
    cat build/st_t9_h4_red.marker; tail -1 build/build_st_t9_h4_red.log; tail -15 tmp/gate_stateless_t9_h4_red/clickhouse-test.log
  ```

  Expected red: the column is 0. Copy the test log to `$P/wave1/logs/red_H2_H-4.log`.
- [ ] **Step 9:** Implement the two conditions and the description changes. Update the five docs files.
- [ ] **Step 10:** Run `st.sh t9_h4_green '<test name>'`; the test passes. If the metric half cannot be made
  stable, drop it from the test and say so in the report (the spec leaves it to the builder).
- [ ] **Step 11:** Run `bt.sh t9_h2_gate 'CAS*'`; expected 3031.
- [ ] **Step 12:** Commit H2. Subject: `Report no GC success age before the first success`.

### Task 10: Commits H3, H4, story H: lane-state name and give-up text {#task-10}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §10.3, §10.4, §10.5.

**Files:**
- H3 modify: `CA/Pool/CasRefLedger.{h,cpp}`; test: `T/gtest_cas_ref_snapshot_publish_ordering.cpp`
- H4 modify: `CA/Pool/CasPartWriteTxn.cpp`; test: `T/gtest_cas_part_write.cpp`

**Interfaces:**
- Produces (H3): `std::string_view refLaneStateName(RefLaneState)` next to the enum, a `switch` without
  `default`. The refused-publication message prints the name. State `Writing` logs at `Debug`, every other
  state at `Warning`. The counter `CASRefSnapshotPublishBackoff` stays.
- Produces (H4): the `GaveUp` arm of the part-manifest `PUT` picks its text with a `switch` over `why` that has
  no `default`. Text after "part-manifest PUT at '{key}'":

  | `why` | `sent_any` | `deadline_source` | Text |
  |---|---|---|---|
  | `FenceLost` | any | any | was refused: this server no longer holds the mount it was admitted under |
  | `Deadline` | no | `Lease` | was not sent: the mount lease has too little time left, so writes are paused |
  | `Deadline` | no | `Policy` | was not sent: the request window ran out before the first attempt |
  | `Deadline` | yes | either | is UNCERTAIN: {attempts_sent} attempt(s) sent, none conclusive by the {lease, policy} deadline; the body may have landed |
  | `Unresolved` | no | any | was not sent: a read of the current object did not complete ({last_seen}) |
  | `Unresolved` | yes | any | is UNCERTAIN: {attempts_sent} attempt(s) sent and the resolving read did not complete ({last_seen}); the body may have landed |

  Every text ends with "the caller re-stages with a fresh ManifestId". The exception stays
  `throwCasWriteRetryLater`. `orThrow` is not touched. No pause age in the text.

- [ ] **Step 1:** Declare `refLaneStateName` returning an empty view. Write H-5
  `CASRefSnapshotPublishOrdering.LaneStateNamesCoverEveryState`: six states, six distinct non-empty names.
- [ ] **Step 2:** Run `bt.sh t10_h3_red 'CASRefSnapshotPublishOrdering*'`. Expected red: empty names. Copy to
  `$P/wave1/logs/red_H3.log`.
- [ ] **Step 3:** Implement the names, the message and the level split.
- [ ] **Step 4:** Run `bt.sh t10_h3_gate 'CAS*'`; expected 3032.
- [ ] **Step 5:** Commit H3. Subject: `Name the append-lane state in the snapshot publication refusal`.
- [ ] **Step 6:** Write H-6 `CASPartWriteTxn.StageManifestNotSentUnderDrainedLeaseSaysNotSent`: a pool opened
  with `PoolConfig::boot_ms_fn` on a test clock (as at `T/gtest_cas_part_write.cpp:2299`), moved until the
  lease has at most its 2000 ms safety margin left and has not expired; `stageManifest` throws
  `NETWORK_ERROR`; the text has "was not sent" and no "UNCERTAIN". Write H-7
  `CASPartWriteTxn.StageManifestSentThenGaveUpSaysUncertain`: a backend double shaped like
  `AmbiguousMetaWriteBackend` that loses the answer of every write to `layout().manifestKey(id)`, with
  `useVirtualMountRequestClock`; the text has "UNCERTAIN" and the attempt count; code `NETWORK_ERROR`.
- [ ] **Step 7:** Run `bt.sh t10_h4_red 'CASPartWriteTxn*'`. Expected red: H-6 sees "UNCERTAIN", H-7 sees no
  attempt count. Copy to `$P/wave1/logs/red_H4.log`.
- [ ] **Step 8:** Implement the six texts.
- [ ] **Step 9:** Mutation (`t10_h4_mut`): the `Deadline` arm ignores `sent_any`. H-6 must fail.
- [ ] **Step 10:** Run `bt.sh t10_h4_gate 'CAS*'`; expected 3034. A test that matched the old text is updated
  and named in the report.
- [ ] **Step 11:** Commit H4. Subject: `Build the manifest give-up text from the give-up facts`.

### Task 11: Commits I1 to I4, story I: CI fixes {#task-11}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §11 and `$P/wave1/facts/E-ci.md`.
No C++ changes: no build, and the commit messages carry the check that was run instead of the `CAS*` line.

**Files:**
- I1: `tests/integration/test_backward_compatibility/configs/skip_check_for_incorrect_settings.xml` (new),
  `tests/integration/test_backward_compatibility/test_custom_s3_disk_remote_url_allow_hosts.py`
- I2: `ci/defs/altinity_jobs.py`, the regenerated files under `.github/workflows/`
- I3: `tests/broken_tests.yaml`
- I4: `ci/jobs/scripts/clickhouse_proc.py`

- [ ] **Step 1 (I1):** `git cherry-pick ad04f95e441`. It keeps the author and the message. Check:
  `xmllint --noout` on the new file, `python3 -m py_compile` on the test, `git show --stat HEAD` lists two files.
  The subject is `Skip the unknown-config check on the upstream image in
  test_custom_s3_disk_remote_url_allow_hosts` (one line). A conflict is resolved toward the tree of `WT`.
- [ ] **Step 2 (I2):** `git cherry-pick -n 2a4b5e72aa2`, then `python3 -m ci.praktika yaml` in `WT` (never in a
  copy). `git diff -- .github/workflows` must hold six `runs-on` changes in three workflows and nothing else;
  anything else is a stop. Save the original message with `git log -1 --format=%B 2a4b5e72aa2`, add one
  sentence about the regenerated workflows before the trailers, stage the exact files and commit with
  `--author="$(git log -1 --format='%an <%ae>' 2a4b5e72aa2)" -F <message file>`. Subject:
  `Run the ASan cas-s3 stateless shards on the LARGE runner`.
- [ ] **Step 3 (I3):** In the rule of `03572_export_merge_tree_part_limits_and_table_functions` set
  `check_types` to `tsan`, `msan`, `asan`, remove the `message` filter, and write a `reason` that names the
  lane load and the measured times (ASan 2 of 2 runs `BROKEN` at 600 s, MSan 2 of 3 runs `FAIL` at 482 s).
  Check: `python3 -c "import yaml; yaml.safe_load(open('tests/broken_tests.yaml'))"`. Do not add a
  `no-cas-storage` tag to the test. Commit. Subject:
  ``Mark `03572_export_merge_tree_part_limits_and_table_functions` broken under every sanitizer``.
- [ ] **Step 4 (I4):** In `dump_system_tables` add `cas_gc_log` and `cas_log` to the dumped tables only when a
  server config holds `<metadata_type>cas</metadata_type>`. Non-CAS lanes keep today's list. Check:
  `python3 -m py_compile ci/jobs/scripts/clickhouse_proc.py`, and a short Python call of the new marker
  function on a temporary config directory with and without the marker (output in the report). Commit.
  Subject: ``Dump `cas_gc_log` and `cas_log` from CI lanes that mount a CAS disk``.
- [ ] **Step 5:** Run the style functions of `ci/jobs/check_style.py` that cover Python and YAML on the changed
  files; no hits.

### Task 12: Sibling check of the snapshot-threshold tests (no commit expected) {#task-12}

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`, who checks the report against the seven
tests and the repeat log, and the diff when a commit is made. Read spec §11 (CAS-466).

- [ ] **Step 1:** Read `git show 50dbaee3a69` and `git show f1bdb36d374` to learn the wait that hung.
- [ ] **Step 2:** Read the seven tests that set `snapshot_log_count_threshold = 0`: two in
  `T/gtest_cas_detached_work.cpp`, two in `T/gtest_cas_ref_snapshot_publish_ordering.cpp`, one in
  `T/gtest_cas_ref_chunked_flush.cpp`, two in `T/gtest_cas_ref_writer.cpp`. For each, say whether it can wait
  on the same condition and why.
- [ ] **Step 3:** Run the seven tests 30 times through the helper:
  `BT_TIMEOUT=1500 $P/wave1/bin/bt.sh t12_repeat '<seven names>' '--gtest_repeat=30'`. Copy
  `build/test_t12_repeat.log` to `$P/wave1/logs/i5_repeat.log`. A timeout (`GTEST_EXIT=124`) or a failure is a
  finding.
- [ ] **Step 4:** Write the result per test to `$P/wave1/reports/task12.md`. Only if a sibling has the defect:
  fix the test, run `bt.sh t12_gate 'CAS*'` (expected 3034) and commit with a subject that names the test.

### Task 13: Gates on the work head (controller) {#task-13}

Run from spec §14 on the head of the work branch, with a clean tracked tree. Every gate script runs under
`flock $M/tmp/ninja.lock`. `release.sh` and `fast.sh` take `$P/build.lock` themselves around ninja; the
stateless scripts build nothing; `asan.sh` builds without that lock, so it runs as
`flock $M/tmp/ninja.lock flock $P/build.lock $P/gates/asan.sh w1`. Never wrap a script that takes
`$P/build.lock` itself in a second `flock` of that file. Logs go to the paths the scripts print.

- [ ] **Step 1:** Release `CAS*`: `$P/gates/release.sh w1`; expected 3034 passed, 0 failed.
- [ ] **Step 2:** `bt.sh w1_iter 'ObjectStorageIteratorAsync*'`; all pass.
- [ ] **Step 3:** ASan `CAS*`: `$P/gates/asan.sh w1`; expected the release count plus the four death-test
  variants of the baseline (3009 at the base, so 3038), 0 failed. A different number is explained.
- [ ] **Step 4:** Fast-test compile without S3: `$P/gates/fast.sh w1`; exit 0.
- [ ] **Step 5:** Stateless `_cas_` tests: `$P/gates/stateless_all.sh w1_all`; expected 59 ok (58 at the base
  plus H-4), 0 failed.
- [ ] **Step 6:** Integration, one module per run, log `$WT/build/integration_w1_<module>.log`:
  `python3 -m ci.praktika run "integration" --test "<module>"` for `test_cas_shared_pool`,
  `test_cas_drop_pool_member`, `test_cas_gc_sharded`, `test_cas_early_commit_restart`,
  `test_backward_compatibility`. All pass. Each log starts with a `BUILD_MARKER` line written before the run:
  head, the resolved path of the binary the job uses (`readlink -f ci/tmp/clickhouse`) and its mtime; that
  binary is the one `release.sh w1` built. The runs create their own containers and touch no other.
- [ ] **Step 7:** Style: `$P/gates/style.sh w1 fa1b89d1529`; `STYLE_DONE hits=0`.
- [ ] **Step 8:** Record every result with its log path in the ledger. A red gate is root-caused and fixed by a
  fresh implementer; the gates that the fix can affect run again.

### Task 14: Whole-branch reviews (controller) {#task-14}

Range `fa1b89d1529..HEAD` in `WT`.

- [ ] **Step 1:** Umbrella review (`ubrella-clickhose-review`), subagents on sonnet; report under
  `$P/wave1/review/umbrella/`.
- [ ] **Step 2:** Two final reviews in parallel: an Opus code reviewer, and
  `codex exec -m gpt-6-astra -c model_reasoning_effort=high -s read-only --cd $WT - < prompt` (fallback
  `gpt-6.1-sol`, high). Both write full reports under `$P/wave1/review/final-*`. At most two opus agents at once.
- [ ] **Step 3:** Merge the three finding lists, drop duplicates, rule on each in the ledger. Every accepted
  finding, including every Minor, goes into one fix dispatch. One scoped re-review of the fix diff.
- [ ] **Step 4:** Run again the gates of Task 13 that the fixes can affect.

### Task 15: Measurements (controller) {#task-15}

Spec §15. Report: `docs/superpowers/reports/2026-11-12-cas-improvements-wave1-measurements.md`.

- [ ] **Step 1:** Write the predictions into the report before any run: M-1 insert and merge throughput of the
  churn workload within 10 % of the base; M-2 no new `CA GC round failed` lines; M-3 exactly one
  `Information` round line per Finish row of `system.cas_gc_log` with outcome `Success`, `Aborted`, `Failed`
  or `Stopped`, and none for other outcomes; M-4 reclaim backlog 0 at the end of the idle phase on base and
  head; M-5 `system.cas_mounts.last_success_age_seconds` is `NULL` before the first round and a number after.
- [ ] **Step 2:** Pin the head binary: copy `$WT/build/programs/clickhouse` (built by `release.sh w1`) to
  `$P/bin/clickhouse-w1-<work head>`. The base binary is `$P/bin/clickhouse-pr2-fa1b89d1529`. Run, one server
  at a time, each under `flock $P/build.lock`, from `$P/measure`:
  `./run.sh W1_base_churn <base binary> 1 900 120 19781`, `./run.sh W1_head_churn <head binary> 1 900 120 19781`,
  then the same pair with churn `0` and labels `W1_base_nochurn`, `W1_head_nochurn`. The labels are new;
  `run.sh` refuses an existing output directory. Ports 19781-19785 are outside the reserved set.
  Collect per run, into `runs/<label>/w1_predictions.tsv`, with a script `$P/measure/w1_collect.sh <label>` that
  the controller writes in this step:
  - M-1: rows of `system.parts` and merges finished (`parts_final.txt`, `events_final.tsv`), churn tables;
  - M-2: `grep -a -c 'CA GC round failed' state/server.log`;
  - M-3: the count of `Information` round lines in `state/server.log` against the Finish rows of
    `system.cas_gc_log` by outcome (`postq.sh` restarts the server on the saved state for the query);
  - M-4: the reclaim backlog of the last idle sample;
  - M-5: `last_success_age_seconds` of `system.cas_mounts`, queried once right after the server start and once
    before the stop. `run.sh` gets these two queries behind `W1=1`, so older labels stay reproducible.
- [ ] **Step 3:** Fill the report: one table per prediction, base against head, with the run logs' paths. A
  failed prediction is a finding: root cause, then a fix or a task.
- [ ] **Step 4:** Commit the report on `cas-gc-rebuild` in `M` by exact path.

### Task 16: CI of PR 2831, clean history, push (controller) {#task-16}

- [ ] **Step 1:** Wait for CI run 43248330323 of PR 2831 on `d8cbdfae3ec`. Every red gets a root cause or a
  tracked task. A fix that belongs to this wave lands on the work branch through a fresh implementer and a
  review; then the affected gates of Task 13 run again.
- [ ] **Step 2:** Freeze the mapping first, in `$P/wave1/clean-map.md`: one row per source commit of
  `be74be9fa7b..<work head>` with the clean commit it goes into. Rules:
  - the late-request audit bundle (`be74be9fa7b..fa1b89d1529`) comes first, one clean commit per story of its
    spec; its ancillary test fixes (`50dbaee3a69`, `f1bdb36d374`) fold into the clean commit of the story whose
    test they fix;
  - then the commits of this plan in the order of the pass-count table, each one commit with its subject
    verbatim;
  - a fix round folds into the clean commit of the story its finding names; a fix that spans two stories is
    split by hunk, and a hunk in a shared file follows the story that introduced the lines it changes
    (`git blame` on the work head).
- [ ] **Step 3:** Build the series as a new branch over `d8cbdfae3ec`, one clean commit at a time, never
  amending. For each: apply its source commits with `git cherry-pick -n`, then compare `git write-tree` with
  the trees gated during execution. An equal tree takes the recorded pass count. Any other tree is built and
  passes `CAS*` through `bt.sh` on the uncommitted tree before the commit is created, and that count goes into
  the message. I1 and I2 keep their author. Every commit has `Signed-off-by` and
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- [ ] **Step 4:** Tree identity: `git rev-parse <clean head>^{tree}` equals `git rev-parse <work head>^{tree}`.
  Record the per-commit tree hashes and counts in the ledger.
- [ ] **Step 5:** The gates of Task 13 hold for the clean head because its tree is the gated tree; record the
  tree hash next to each gate result in the ledger.
- [ ] **Step 6:** Fast-forward `fix/antalya-26.8/cas-improvements-bundle` to the clean head and push it to
  `altinity` once. No force push.
- [ ] **Step 7:** Update the body of PR 2831 from `.github/PULL_REQUEST_TEMPLATE.md`: the stories of this wave,
  every changed default with its old value and restoring key, the CAS regression checkbox ticked.
- [ ] **Step 8:** Append to each backlog task of spec §18 the commit that closes its criteria and the criteria
  left open (spec "Not closed" subsections), with the task that owns each. Note the I4 artifact check in its
  task if no CAS lane of PR 2831 failed. Delete the status cron.
