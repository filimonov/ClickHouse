# CAS ref cleanup bundle (PR1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One branch on top of `2f89c5db182` with ten commits that fix four ref-ledger defects: an erased
incarnation re-inserted by namespace create, a `_ckpt` frontier published past a later epoch, a false
chain-broken verdict in `fsck`, and ref cleanup refused by any catalog change.

**Architecture:** Four stories, A to D, land in that order. Each story is one or more commits; a reviewer can
read a story by reading its commits. Stories A and B port two existing draft diffs; C and D are written new.
Every commit builds and keeps the `CAS*` gtests green.

**Tech Stack:** C++23, clang-21, ninja, gtest (`unit_tests_dbms`), ClickHouse 26.8 CAS code under
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` (below: `CA/`).

**Spec:** `master/docs/superpowers/specs/2026-11-11-cas-pr1-ref-cleanup-bundle-design.md` (below: bundle
spec). Story D details: `master/docs/superpowers/specs/2026-11-11-cas-ref-cleanup-per-life-licence-design.md`
(below: licence spec). Executors read the sections named in their task.

## Global Constraints

Paths:

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1` (branch `fix/antalya-26.8/cas-ref-cleanup-per-life-licence`, base `2f89c5db182`) |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master` (plans, specs, logs, reports; never built) |
| `DA` | `$M/tmp/cas298/bundle/d42febbcc9bf.diff` (story A draft) |
| `DB` | `$M/tmp/cas298/bundle/2d2182b3b19c.diff` (story B draft) |
| logs | `$M/tmp/prbundle/logs/` |
| reports | `$M/tmp/prbundle/reports/task<N>.md` |
| scratch | `$WT/tmp/` (create it; never `/tmp`) |

Rules, each binding for every task:

- **Only `WT` is modified.** No other worktree, no docker container, no server process is touched. Never
  `kill`/`pkill` by pattern. Never `git clean`, `git reset --hard`, `git stash`, or delete a build directory.
- **Shell cwd is not stable.** Prefix every command with `cd <absolute path> &&`.
- **Build** (one at a time, serialized by a lock; no `-j`):
  ```bash
  cd $WT && nohup bash -c "flock $M/tmp/prbundle/build.lock nice -n 19 ninja -C build clickhouse unit_tests_dbms > build/build_<tag>.log 2>&1; echo \"NINJA_EXIT=\$? \$(date -u +%FT%TZ) head=\$(git rev-parse --short HEAD)\" >> build/build_<tag>.log" > /dev/null 2>&1 &
  cd $WT && until grep -aq "NINJA_EXIT=" build/build_<tag>.log; do sleep 20; done; tail -1 build/build_<tag>.log
  ```
  Run the wait in the foreground with a 600000 ms timeout and repeat it on timeout. Never wait for a
  notification. A red or mutation run may build only `unit_tests_dbms`.
- **Test** only after `NINJA_EXIT=0` in the log of the same tag:
  ```bash
  cd $WT && build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_<tag>.log 2>&1; echo "GTEST_EXIT=$? $(date -u +%FT%TZ) head=$(git rev-parse --short HEAD)" >> build/test_<tag>.log
  ```
  The gate filter is exactly `CAS*`. A red or mutation run may use a narrower filter. The pass count is the
  number in `[  PASSED  ] N tests`. A build failure is never read as a test result.
- **Red first.** Tests enter the tree before the product change, are built and run, and the log is copied to
  `$M/tmp/prbundle/logs/red_<commit#>_<story>.log`. A red run compiles: where a test names a new symbol, the
  red tree carries its bare declaration (an enum value; for a `ProfileEvent` its `M(...)` line in
  `src/Common/ProfileEvents.cpp` with no increment). A compile failure is not red evidence.
- **Mutation checks** run before the commit of their task. Copy the unmutated file to `$WT/tmp/` first.
  Mutate, build `unit_tests_dbms`, run, log to `$M/tmp/prbundle/logs/mut_<commit#>_<name>.log`, copy the
  file back, and confirm with `git diff --stat` that the tree equals the pre-mutation tree. Never use
  `git checkout --` on uncommitted work. A mutation is never committed.
- **Commit:** `git add -- <exact files>` (never `-A`, never `.`), then `git commit -s`. Subject from the task.
  Body: one to three sentences on why; no backlog ids, no words "spec", "plan", "story", "draft"; last body
  line `` `CAS*` gtest gate (release): N tests passed. ``; trailer
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. No session link, no other trailer. No amend,
  no rebase, no push.
- **Code style:** Allman braces. Comments say why, carry no backlog ids and no history. Literal names in
  docs and messages go in backticks; functions are written `f`, not `f()`. A logical error is an
  "exception", not a "crash". `LOGICAL_ERROR` expectations are death tests under `DEBUG_OR_SANITIZER_BUILD`.
  No sleep to fix a race.
- **Scope:** a task changes only the files it lists. A needed change outside the list stops the task and goes
  into the report.
- **Report:** each implementer writes `$M/tmp/prbundle/reports/task<N>.md` and repeats its content in the
  final message: commit SHAs, pass counts, red and mutation log paths with the observed red set, deviations.

Baseline: `CAS*` pass count at `2f89c5db182` is `N0` = 2905 (`GTEST_EXIT=0`, `$WT/build/test_baseline.log`). Commits 1, 4,
5 and 10 keep the count at the previous value. Each other commit raises it by the number of tests it adds.

## Review Focus

Conditions the specs imply that the listed tests do not exercise directly. Each line names its owner.

1. **Other `casUpdate` callers after commit 1.** Every caller that does not set `resend_unresolved = false`
   must behave as before, including the credential-refresh reissue (`CA/Backend/CasRequests.cpp:998-1011`).
   Pinned by the unchanged pass count of Task 1; the Task 1 reviewer reads every new branch for a default
   that differs from the old path.
2. **`Superseded` on a wedge retry keeps the wedge.** `resolveWedgeOnce` must still return `StillWedged`,
   check `same_wedge_under_lock`, and call `requireRecovery` only under that check
   (`CA/Pool/CasRefLedger.cpp:2453-2464`). Pinned by `WedgeRetryLandingPastALaterEpochSealNeedsRecovery`;
   the Task 4 reviewer compares the new arm with the `FencedOut` arm line by line.
3. **`fsck` re-read of `_ckpt` fails or runs out of deadline.** The tail walk must record `Unchecked`, never
   throw out of `checkRefStream` and never record chain-broken. No fixture injects a read failure there; the
   Task 5 reviewer reads the `try`/`catch` and the `checkDeadline` call against `CasFsck.cpp:340-368`.
4. **Work budget ends inside a namespace.** `!work_budget.refCleanupAvailable()` ends the pass without
   setting a stop flag or `namespaces_skipped`: it is not a licence refusal. The Task 7 reviewer checks the
   early return; Task 7 step 2 adds, to the existing budget test (`gc_round_ref_cleanup_budget = 1`,
   `gtest_cas_ref_gc.cpp:603-663`), the assertions that `CASRefCleanupStoppedLeaseMoved`,
   `CASRefCleanupStoppedAuthorityUnverified` and `CASRefCleanupNamespacesSkipped` do not move.
5. **Suppressed or trim-disabled round.** With `suppress_destructive` or `trim_enabled == false` the phase
   row still carries the six new metrics, all 0, and no licence read happens. Task 7 step 2 extends the
   existing phase-row test at `gtest_cas_ref_gc.cpp:907-915` with `objects_deleted == 0` and
   `namespaces_skipped == 0`.

---

### Task 0: Baseline (controller)

- [x] Release build of `2f89c5db182` in `$WT/build` (`NINJA_EXIT=0 2026-11-11T12:13:51Z`).
- [x] Baseline binary saved: `$M/tmp/prbundle/bin/clickhouse-baseline-2f89c5db182`.
- [x] `CAS*` run on the baseline: 2905 tests passed, `GTEST_EXIT=0 2026-11-11T12:23:30Z`.

### Task 1: Commit 1, story A: unresolved write attempt is returned, not resent

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read bundle spec §3.2.

**Files:**
- Modify: `CA/Backend/CasRetry.h`, `CA/Backend/CasRequests.h`, `CA/Backend/CasRequests.cpp`

**Interfaces:**
- Produces: `Retry::attempts_already_sent` (`uint32_t`, default 0), `Retry::resend_unresolved` (`bool`,
  default true), `CasOperation::pauseBeforeRedecide(uint64_t ms, const GaveUp & unresolved, const Retry & policy)`.
  With `resend_unresolved == false`, `writeLoop` returns the unresolved attempt to the caller instead of
  resending the same bytes.

- [ ] **Step 1: Apply the three draft hunks.**
  ```bash
  cd $WT && git apply --numstat --include='*/Backend/CasRetry.h' --include='*/Backend/CasRequests.h' --include='*/Backend/CasRequests.cpp' $DA
  cd $WT && git apply --include='*/Backend/CasRetry.h' --include='*/Backend/CasRequests.h' --include='*/Backend/CasRequests.cpp' $DA
  ```
  Expected `--numstat`: three files, 23 added and 4 removed lines in total. Fewer files means the include
  pattern did not match: stop.
- [ ] **Step 2: Build** (tag `c1`). Expected `NINJA_EXIT=0`.
- [ ] **Step 3: Test** (tag `c1`). Expected `GTEST_EXIT=0`, pass count `N0`.
- [ ] **Step 4: Commit.** Subject: `Let a CAS write return an unresolved attempt instead of resending it`.

### Task 2: Commits 2 and 3, story A: fresh incarnation per step-1 decision

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read bundle spec §3 (all of it).

**Files:**
- Modify: `CA/Pool/CasRefCatalog.cpp`
- Test: `src/Disks/tests/gtest_cas_ref_catalog.cpp`

**Interfaces:**
- Consumes: Task 1's `Retry` fields and `pauseBeforeRedecide`.
- Produces: `createNamespaceStep1(op, layout, gc_shards, ns, creator, policy)` mints the incarnation inside
  the `mutate` callback and returns `*findEntry(landed, ns)`. Test fixtures `CatalogWriteRecorder`
  (with `after_write`), `Step1Fixture`, `lostResponse`.

- [ ] **Step 1: Apply the draft test hunk** (tests S1 to S7 and fixtures).
  ```bash
  cd $WT && git apply --numstat --include='src/Disks/tests/gtest_cas_ref_catalog.cpp' $DA && git apply --include='src/Disks/tests/gtest_cas_ref_catalog.cpp' $DA
  ```
- [ ] **Step 2: Add `after_write` and test S8.** In `CatalogWriteRecorder` add
  `std::function<void()> after_write;`. `write` runs it exactly once, after the base `write` returns or
  throws, and clears it before running it (nested catalog writes must not re-enter it).
  Test `TEST(CASRefCatalog, CreateNamespaceStep1NeverReinsertsAnErasedIncarnation)`:
  - arm `injectAmbiguousLandedWrite(catalog_key)` for the step-1 write;
  - `after_write`, acting as `other`: read the catalog, capture `I1`, `completeCreation`, then with the
    resulting **`Live`** row:
    ```cpp
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, observed, 9), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    CasFoldSeal ready_parent;
    ready_parent.ref_lives.emplace(observed.incarnation, RefLifeFoldState{
        .coverage = RefCoverage{.classification = CoverageClass::Folded, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});
    const CatalogEntry removing{.ns = RootNamespace{"a"}, .state = NsState::Removing,
                                .incarnation = observed.incarnation, .removal_started_round = 9};
    ASSERT_EQ(CasRefCatalog::deleteCompletedRemoving(other, layout, removing, ready_parent, noAuthorityRefresh).outcome,
              CasRefCatalog::CompletedRemovingDeleteOutcome::Deleted);
    ```
  - assertions: `createA` succeeds; the final row is `Live` with incarnation `!= I1`;
    `step1Incarnations()` are pairwise distinct; `_ckpt` exists under the key of the second incarnation and
    not under `I1`.
- [ ] **Step 3: Red run.** Build `unit_tests_dbms` (tag `red2`), run with
  `--gtest_filter='CASRefCatalog.*:CASPool.ConcurrentNamespaceCreations*'`, copy the log to
  `$M/tmp/prbundle/logs/red_2_A.log` and the S8 part of it to `red_3_A.log`. Expected red: S1, S2, S4, S8.
  Expected green guards: S3, S5, S6, S7. Record the observed set; a difference from the expected set is
  reported with the failing assertion, not fixed by editing the test to pass.
- [ ] **Step 4: Apply the product hunk.**
  ```bash
  cd $WT && git apply --numstat --include='*/Pool/CasRefCatalog.cpp' $DA && git apply --include='*/Pool/CasRefCatalog.cpp' $DA
  ```
- [ ] **Step 5: Build and run** (tag `green2`, same filter). All eight tests green, and
  `CASPool.ConcurrentNamespaceCreationsNeverRaceEachOtherOnTheCatalog` and
  `CASRefCatalog.ATrueRefusalCreateNamespaceStep1SeesASuperseder` green.
- [ ] **Step 6: Commit 2 without S8.** Copy the test file to `$WT/tmp/gtest_cas_ref_catalog.with_s8.cpp`.
  `git checkout HEAD -- src/Disks/tests/gtest_cas_ref_catalog.cpp`, re-apply the draft test hunk (step 1).
  Build (tag `c2`), full `CAS*` (tag `c2`), expected `N0 + 7`. Commit `CasRefCatalog.cpp` and the test file.
  Subject: `Mint a fresh incarnation on every namespace-create step-1 decision`.
- [ ] **Step 7: Commit 3.** Copy the saved file back over the test file. `git diff --stat` must show only
  the `after_write` member and S8. Build (tag `c3`), full `CAS*` (tag `c3`), expected `N0 + 8`. Commit.
  Subject: `Test that namespace create never re-inserts an erased incarnation`.

### Task 3: Commits 4 and 5, story B: two preparatory commits without behaviour change

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read bundle spec §4.2.

**Files:**
- Modify: `CA/Pool/CasRefLedger.cpp` (commits 4 and 5), `CA/Pool/CasRefCkpt.h`, `CA/Pool/CasRefCatalog.cpp`
  (commit 5)

**Interfaces:**
- Produces (commit 4): a local lambda in `commitRefChunk`, declared before its first use, that takes the
  message tail or nothing and performs the body of the existing `Occupant::SuccessorSeal` arm
  (`CasRefLedger.cpp` near `:3895`): increment `CASRefAppendSealRejected`; under `rt->state_mutex` set
  `last_epoch_seal = id`, reset `append_attempt`, set `lane_state = RefLaneState::Closed`;
  `complete_error(chunk_survivors, INVALID_STATE ...)`. The existing arm calls it and returns `false`.
  The exception text of the existing arm does not change by one character.
- Produces (commit 5), in `CasRefCkpt.h` after the `CkptPublishOutcome` enum:
  ```cpp
  /// True when the contribution is durable in `_ckpt`: it was written, or an identical value was already there.
  inline bool ckptContributionRecorded(CkptPublishOutcome outcome)
  {
      return outcome == CkptPublishOutcome::Published || outcome == CkptPublishOutcome::IdenticalSkip;
  }
  ```

- [ ] **Step 1 (commit 4): Extract the lambda.** No other line of `commitRefChunk` changes.
- [ ] **Step 2: Build and full `CAS*`** (tag `c4`). Expected `N0 + 8`. Commit.
  Subject: ``Extract the successor-seal lane close in `commitRefChunk` ``.
- [ ] **Step 3 (commit 5): Add the helper and use it at five sites.** Each site today compares the outcome
  of `publishCkpt` or `publishCkptContribution` with `CkptPublishOutcome::FencedOut`:

  | Site (base line) | Function | New condition |
  |---|---|---|
  | `CasRefLedger.cpp:929` | `runRecoveryWalkOnce`, recovered frontier | `!ckptContributionRecorded(...)` |
  | `CasRefLedger.cpp:1256` | `runRecoveryWalkOnce`, recovered seal | `!ckptContributionRecorded(...)` |
  | `CasRefLedger.cpp:3678` | `commitRefChunk`, birth contribution | `!ckptContributionRecorded(...)` |
  | `CasRefLedger.cpp:4661-4666` | `tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl` | `ckpt_advanced = ckptContributionRecorded(...)` |
  | `CasRefCatalog.cpp:616-618` | `completeCreation` step 2 | `!ckptContributionRecorded(...)` returns `NamespaceCreationOutcome::FencedOut` |

  The two frontier sites (`:2438-2453` wedge, `:4051-4063` append) and the wrapper `:4485` stay untouched:
  commit 6 owns them. Messages do not change in this commit.
- [ ] **Step 4: Build and full `CAS*`** (tag `c5`). Expected `N0 + 8`. Commit.
  Subject: ``Ask `ckptContributionRecorded` where a caller only needs durability``.

### Task 4: Commit 6, story B: refuse a frontier that a later epoch moved past

Implementer `ca-impl`. Reviewer `ca-review` (opus): ref-ledger safety. Read bundle spec §4 (all of it).

**Files:**
- Modify: `CA/Pool/CasRefCkpt.h`, `CA/Pool/CasRefCkpt.cpp`, `CA/Pool/CasRefLedger.cpp`,
  `CA/Pool/CasRefLedger.h`, `CA/Formats/CasRefCkptFormat.h`, `src/Common/ProfileEvents.cpp`,
  `docs/en/antalya/metrics_and_events.md`
- Test: `src/Disks/tests/gtest_cas_ref_ckpt.cpp`, `gtest_cas_ref_ckpt_join.cpp`,
  `gtest_cas_ref_wedge_every_attempt.cpp`

**Interfaces:**
- Consumes: Task 3's lambda and `ckptContributionRecorded`.
- Produces: `CkptPublishOutcome::SuccessorSeal`, `CkptPublishOutcome::Superseded`;
  `std::optional<CkptPublishOutcome> refuseFrontierAgainstDurable(const RefCkpt & durable, const RefCkpt & contribution)`
  in `CasRefCkpt.cpp`; `ProfileEvents::CASRefCheckpointFrontierRefused`.

- [ ] **Step 1: Apply the three draft test hunks and adapt them.**
  ```bash
  cd $WT && for f in gtest_cas_ref_ckpt.cpp gtest_cas_ref_ckpt_join.cpp gtest_cas_ref_wedge_every_attempt.cpp; do git apply --numstat --include="src/Disks/tests/$f" $DB && git apply --include="src/Disks/tests/$f" $DB; done
  cd $WT && grep -n 'global_counters\[.*\]\.load()' src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp
  ```
  Remove `.load()` from every line the `grep` prints inside the new tests (eight calls): on this base
  `ProfileEvents::global_counters[event]` returns `UInt64`.
- [ ] **Step 2: Red-tree declarations, uncommitted.** Add the two enum values to `CkptPublishOutcome`
  (comments come in step 4) and register the event in `ProfileEvents.cpp` right after
  `CASRefCheckpointNotAdvanced` (near `:877`), with no increment anywhere:
  ```cpp
  M(CASRefCheckpointFrontierRefused, "Number of CAS ref `_ckpt` frontier contributions refused before the merge: a successor's epoch seal sits at the frontier, or a later epoch moved past it. Only a deposed writer gets here; non-zero means a writer kept writing after it lost its mount, and the refusal kept that write unacknowledged.", ValueType::Number) \
  ```
- [ ] **Step 3: Red run.** Build `unit_tests_dbms` (tag `red6`), run
  `--gtest_filter='CASRefCheckpoint*:CASRefWedgeEveryAttempt*'`, copy to
  `$M/tmp/prbundle/logs/red_6_B.log`. Expected red: the eleven tests marked "yes" in bundle spec §4.3 (two of
  them through a `CORRUPTED_DATA` exception from `CasRefCkpt.cpp:49-55`). Expected green: the two guards
  `PublishStillSkipsAdoptedStraggler` and `PublishStillSkipsARepublishedSeal`. An existing test that turns
  red here is reported, not edited.
  Record the observed set and count.
- [ ] **Step 4: Apply the product hunks that apply.**
  ```bash
  cd $WT && for p in '*/Pool/CasRefCkpt.cpp' '*/Formats/CasRefCkptFormat.h' '*/Pool/CasRefLedger.h' 'docs/en/antalya/metrics_and_events.md'; do git apply --numstat --include="$p" $DB && git apply --include="$p" $DB; done
  ```
  `CasRefCkpt.h`, `ProfileEvents.cpp`: apply by hand against the draft text (`DB` lines 139-211 and 15-28),
  because steps 2 and Task 3 already changed those regions. In the `SuccessorSeal` enum comment write
  "no further write follows the refusal" where the draft says "No write was sent". The
  `CASRefCheckpointNotAdvanced` description gains "refused," as in the draft.
- [ ] **Step 5: Hand-port `CasRefLedger.cpp`** from `DB` lines 212-384. The draft hunk does not apply.
  - `extern const Event CASRefCheckpointFrontierRefused;` beside the other CAS events.
  - `publishCkptContribution` (near `:4485`): increment `CASRefCheckpointFrontierRefused` when the outcome is
    `SuccessorSeal` or `Superseded`.
  - Recovery sites (`:929`, `:1256`): message "... the mount incarnation moved, or a later epoch sealed or
    passed it, before the checkpoint could record recovered txn {}-{}; nothing is installed" as in the draft.
  - `resolveWedgeOnce` (`:2438-2464`): `bool closed_by_successor_seal = occupant == Occupant::SuccessorSeal;`
    a frontier outcome `SuccessorSeal` sets it; a later `else if (closed_by_successor_seal)` replaces the
    `occupant` test, with the draft's comment. `Superseded` shares the `FencedOut` arm: `StillWedged`,
    `same_wedge_under_lock`, conditional `requireRecovery`, early return, all preserved. Its message:
    "... wedged txn {}-{} is durable but was not acknowledged (a later epoch moved past it before
    checkpoint-frontier publication; the lane needs recovery): {`outcome_unknown_tail`}".
  - `commitRefChunk` after frontier publication (`:4051-4071`): `SuccessorSeal` calls Task 3's lambda and
    returns `false`. `Superseded`: `requireRecovery(*rt, ns, "committed-frontier publication superseded")`
    under `state_mutex`, then
    `complete_error(chunk_survivors, outcome_unknown(ErrorCodes::NETWORK_ERROR, "a later epoch moved past it before the frontier was published")); return false;`.
    This differs from the draft, which used a retry-later exception.
  - No `durable_unconfirmed` helper is used on a `Superseded` path.
- [ ] **Step 6: Build and run** (tag `green6`, the red filter). All thirteen new tests green.
- [ ] **Step 7: Full gate.** Build `clickhouse unit_tests_dbms` (tag `c6`), full `CAS*` (tag `c6`). Expected
  `N0 + 21` (thirteen new tests).
- [ ] **Step 8: Commit.** Subject: ``Refuse a `_ckpt` frontier that a later writer epoch moved past``.

### Task 5: Commit 7, story C: `fsck` revalidates `_ckpt` on an absent tail log

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read bundle spec §5 and licence spec §7 and §8.1.

**Files:**
- Modify: `CA/Tools/CasFsck.cpp` (`checkRefStream`, tail walk near `:375-385`)
- Test: `src/Disks/tests/gtest_cas_fsck.cpp` (suite `CASFsckAuthority`; model test
  `CheckpointBaseVanishingAfterAuthorityAdvanceIsUnchecked` near `:1004`)

- [ ] **Step 1: Write the fixture and tests F1, F2.** Fixture: `Live` namespace; valid checkpoint triple at
  `{1,1}`; `_ckpt.committed_through = {1,3}`; logs `{1,2}` and `{1,3}` present. Build it from the existing
  helpers (`openPoolForTest`, `fixture::admitLive`, `writeFsckCheckpointWithBase`,
  `CasRefCatalog::lifeIfCataloged`).
  - F1 `CoveredTailLogVanishingAfterAuthorityAdvanceIsUnchecked`: `MutateOnFirstGetBackend::armOnFirstGet`
    on `layout.refLogKey(life, {1,2})`; the mutation replaces `_ckpt` (new etag) and removes that log.
    Expect class `FsckClass::Unchecked` for that key and no chain-broken verdict.
  - F2 `MissingTailLogUnderUnchangedCheckpointIsChainBroken`: log `{1,2}` absent from the start, `_ckpt`
    untouched. Expect chain-broken for that key.
  - Both assert the reported key or `ref_records_walked`, so a verdict of the base path cannot satisfy them.
- [ ] **Step 2: Red run** (tag `red7`, filter `CASFsck*`), copy to `red_7_C.log`. Expected: F1 red with a
  chain-broken verdict, F2 green.
- [ ] **Step 3: Product change.** On an absent tail log, before `recordChainBroken`: `checkDeadline`, re-read
  `_ckpt` with `readCkpt(op, layout, life)`; if it is absent, or `checkpoint_sample` is absent, or the etag
  differs from `checkpoint_sample->etag`, call `verdicts.recordUnchecked(...)` and stop the tail walk of this
  stream; an exception from the re-read also records `Unchecked`; otherwise `recordChainBroken` as today.
  Mirror the structure of the base revalidation at `:340-368`; reuse its message style.
- [ ] **Step 4: Build and run** (tag `green7`, filter `CASFsck*`). F1 and F2 green.
- [ ] **Step 5: Mutation for F2.** Make the new branch always record `Unchecked`. Build `unit_tests_dbms`,
  run `CASFsck*`, log to `mut_7_always_unchecked.log`. F2 must be red. Copy the saved file back.
- [ ] **Step 6: Full gate** (tag `c7`), expected `N0 + 23`. Commit `CasFsck.cpp` and the test file.
  Subject: ``Revalidate `_ckpt` before `fsck` reports an absent tail log as a broken chain``.

### Task 6: Commit 8, story D: per-life licence

Implementer `ca-impl`. Reviewer `ca-review` (opus): GC safety. Read licence spec §5, §8 (intro, 8.2, 8.3,
8.4) and bundle spec §6.

**Files:**
- Modify: `CA/Gc/CasGc.cpp` (`Gc::cleanupRefObjects` near `:3904`; comment near `:2786`)
- Test: `src/Disks/tests/gtest_cas_ref_gc.cpp` (suite `CASRefGcCleanupAuthority`, `:733-855`; race backend
  `RefCleanupAuthorityRaceBackend` at `:123`)

**Interfaces:**
- Produces, in the anonymous namespace of `CasGc.cpp`:
  ```cpp
  enum class RefCleanupLicence : uint8_t { Delete, SkipNamespace, StopLeaseMoved, StopAuthorityUnverified };
  ```
  The lambda `authorityHolds` becomes a lambda returning `RefCleanupLicence`. Order of reads is unchanged:
  catalog first, then `gc/state`, back to back (the seam `armGcFenceMoveOnAuthorityHoldsRevalidationForTest`
  depends on it).

  | Observation | Result |
  |---|---|
  | catalog read throws | `StopAuthorityUnverified` |
  | catalog incarnation index is ambiguous | `StopAuthorityUnverified` |
  | row absent, or `row != observed_entry`, or the incarnation resolves to another life | `SkipNamespace` |
  | `gc/state` read throws, or `gc/state` is absent | `StopAuthorityUnverified` |
  | lease owner or sequence differs from `adopted_lease` | `StopLeaseMoved` |
  | otherwise | `Delete` |

  The catalog etag no longer decides anything. Chunk loop: `Delete` deletes the chunk; `SkipNamespace` logs
  one line that names the namespace and says it is skipped, then `break`s to the next namespace; a stop value
  logs and `return`s.
- Test fixture: `Authority` gains `OwnRowRemoving`, `OwnRowErased`, `AliasingIncarnation`, each with its
  mutation in the race backend (the base backend implements only `GcFence` and `CatalogRebirth`; any other
  value rewrites unchanged catalog bytes, `gtest_cas_ref_gc.cpp:267-295`).

- [ ] **Step 1: Write the tests, key assertions only** (object presence and `deleteCount`; no counter
  assertions, they belong to Task 7). Tables in licence spec §8.2 to §8.4:
  - new: A1, A2, A3, A6, B1, B2, G1, G2;
  - renamed with flipped expectations: A4 (from `CatalogTokenMoveBeforeFirstChunkRefusesEveryRefObjectDelete`),
    A5 (from `CatalogTokenMoveBetweenChunksAllowsFirstAndRefusesSecondDelete`);
  - unchanged: G3 (three tests), G4, G5.
  Namespace names sort `A` before `B`. `gc_bulk_delete_chunk_keys = 1` in every chunk-timed test. G2 injects
  the alias from the race backend at `Timing::DuringChunk`, not from the post-hot-scan hook.
- [ ] **Step 2: Red run** (tag `red8`, filter `CASRefGc*`), copy to `red_8_D.log`. Expected red: A1, A2, A3,
  A4, A5, A6, B1, B2. Expected green: G1, G2, G3, G4 (`RebirthDuringAChunkDeletesOnlyTheOldLifesKeys`,
  `:855`), G5 (six `CASRefGc.RefObjectCleanup*` tests). Record the observed set.
- [ ] **Step 3: Product change** per the Interfaces block. Fix the comment near `CasGc.cpp:2786`: at this
  base an absent checkpoint makes the namespace leak-only (`CasGc.h:880-883`); the comment must not say that
  it "WIDENS" what `cleanupRefObjects` may delete.
- [ ] **Step 4: Build and run** (tag `green8`, filter `CASRefGc*`). All green.
- [ ] **Step 5: Mutations**, one at a time, each from a clean copy of the step 3 file kept in `$WT/tmp/`:

  | Mutation | Must turn red | Log |
  |---|---|---|
  | remove the `row != observed_entry` comparison | B1 | `mut_8_row_comparison.log` |
  | a lease move returns `SkipNamespace` | G1 | `mut_8_lease_scope.log` |
  | remove both the ambiguity check and the life resolution | G2 | `mut_8_ambiguity.log` |
  | remove the lease comparison | the three G3 tests | `mut_8_lease_comparison.log` |

  A mutation that leaves its test green is a finding: report it, do not weaken the claim. Restore the file
  and confirm `git diff --stat` matches step 3.
- [ ] **Step 6: Full gate** (tag `c8`), expected `N0 + 31`. Commit `CasGc.cpp` and the test file.
  Subject: `License ref cleanup per life instead of by the whole catalog`.

### Task 7: Commit 9, story D: counters and phase metrics

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read licence spec §6 and §8.5.

**Files:**
- Modify: `CA/Gc/CasGc.cpp`, `CA/Gc/CasGc.h`, `src/Common/ProfileEvents.cpp`,
  `docs/en/antalya/metrics_and_events.md`
- Test: `src/Disks/tests/gtest_cas_ref_gc.cpp`

**Interfaces:**
- Produces, in `CasGc.h`:
  ```cpp
  struct RefCleanupPassStats
  {
      uint64_t objects_planned = 0;
      uint64_t objects_deleted = 0;
      uint64_t namespaces_skipped = 0;
      bool stopped_lease_moved = false;
      bool stopped_authority_unverified = false;
      uint64_t chunks_under_catalog_change = 0;
  };
  void cleanupRefObjects(const FoldResult & folded, const GcLease & adopted_lease, bool suppress_destructive,
                         GcRoundWorkBudget & work_budget, RefCleanupPassStats & stats);
  ```
- Events, registered after `CASRefCleanupObjectsDeleted` (`ProfileEvents.cpp` near `:868`), with one row
  each in `metrics_and_events.md` (near `:240`):

  | Event | Incremented |
  |---|---|
  | `CASRefCleanupObjectsPlanned` | by the key count of a namespace's plan when the plan is built |
  | `CASRefCleanupNamespacesSkipped` | once per `SkipNamespace` |
  | `CASRefCleanupStoppedLeaseMoved` | once per `StopLeaseMoved` |
  | `CASRefCleanupStoppedAuthorityUnverified` | once per `StopAuthorityUnverified` |
  | `CASRefCleanupChunksUnderCatalogChange` | once per chunk deleted while the catalog etag differs from the cut, after the delete returns |

  Descriptions say what the operator should conclude, as licence spec §6 words them.
- Phase row `ref_object_cleanup` (`CasGc.cpp` near `:1491-1500`) gains `objects_planned`, `objects_deleted`,
  `namespaces_skipped`, `stopped_lease_moved` (0 or 1), `stopped_authority_unverified` (0 or 1),
  `chunks_under_catalog_change`, written in every round, including suppressed and trim-disabled ones.

- [ ] **Step 1: Red-tree declarations, uncommitted:** the five `M(...)` registrations, no increments.
- [ ] **Step 2: Add the assertions** of licence spec §8.5 (C1 to C6) to A1, B1, B2, G1, G2, as global
  counter deltas (`ProfileEvents::global_counters[event]`, no `.load()`) and, for C6, phase-row metrics
  captured with `gc.setPhaseSink` as at `gtest_cas_ref_gc.cpp:907-915`. Add the two Review Focus assertions
  (items 4 and 5).
- [ ] **Step 3: Red run** (tag `red9`, filter `CASRefGc*`), copy to `red_9_D.log`. Expected red: A1, B1, B2,
  G1, G2 on the new assertions only.
- [ ] **Step 4: Product change:** the stats struct, the out-parameter, the increments, the phase metrics, the
  docs rows. The licence lambda also reports whether the catalog etag differs from `folded.catalog_cut->etag`.
- [ ] **Step 5: Build and run** (tag `green9`, filter `CASRefGc*`). All green.
- [ ] **Step 6: Full gate** (tag `c9`), expected `N0 + 31` (no new tests). Commit the five files. Subject: `Count why a ref cleanup pass stopped`.

### Task 8: Commit 10, story D: documentation

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read licence spec §10 and bundle spec §12.

**Files:**
- Modify: `docs/en/antalya/cas/architecture/garbage-collection.md` (phase table row near `:61`; phase 17
  Observability near `:580-581`)

- [ ] **Step 1: Edit** the two places: what licenses a chunk (own row, own life, GC lease), what a refusal
  costs (one namespace or the pass), and the new counters and phase metrics. Plain words: no invariant tags,
  no backlog ids. Every header keeps its `{#anchor}`.
- [ ] **Step 2: Check:** `cd $WT && git diff --stat` shows one file; every counter name in the text exists in
  `src/Common/ProfileEvents.cpp` (`grep -c` each one).
- [ ] **Step 3: Commit** (no build needed; the pass count line repeats Task 7's count and says "unchanged,
  docs only"). Subject: `Document the per-life ref cleanup licence`.

The note for `docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md` (licence spec §10) is a
separate commit on `cas-gc-rebuild` in `M`, made by the controller.

### Task 9: Gates on the final tree (controller, logs summarised by subagents)

- [ ] Release: full `CAS*` on the head of the branch (tag `final`).
- [ ] ASan: configure `$WT/build_asan` like `antalya-26.8-pr-2159/build_asan` (`CMAKE_BUILD_TYPE=None`,
  clang-21, `SANITIZE=address,undefined`, `ENABLE_TESTS=1`, `ENABLE_UTILS=0`), build `unit_tests_dbms` under
  the same lock, run `CAS*`. Delete `build_asan` after the last review fix passed it.
- [ ] Fast-test compile: `$WT/build_fast` (`-DENABLE_LIBRARIES=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0
  -DENABLE_EMBEDDED_COMPILER=OFF`), build the objects of every product TU the branch touches. Delete after.
- [ ] Stateless: the 58 `*_cas_*` tests against a standalone server of the final binary (free ports checked
  with `ss -ltn` first; `clickhouse-test ... < /dev/null`).
- [ ] Style: the C++ and docs functions of `ci/jobs/check_style.py` on the touched files.
- [ ] Every red: root cause or tracked task, recorded in the ledger.

### Task 10: Reviews

- [ ] Umbrella review of `2f89c5db182..HEAD` (`ubrella-clickhose-review`).
- [ ] Final whole-branch review twice: `ca-review` (opus) and codex (`gpt-6-astra`, read-only, prompt from a
  file). Each writes its full report to `$M/tmp/prbundle/review/`.
- [ ] Fix every finding, minors included, as new commits placed after the commit they correct (no amend).
  Re-run the gates of Task 9 on the new head.

### Task 11: Measurements

- [ ] Runs R1 to R4 of bundle spec §10 with `$M/tmp/prbundle/measure/run.sh`, one server at a time: R1 and R3
  on the baseline binary, R2 and R4 on the final binary.
- [ ] Report: `$M/docs/superpowers/reports/2026-11-11-cas-pr1-ref-cleanup-bundle-measurements.md`. Figures are
  re-read from the run outputs when the report is written.
