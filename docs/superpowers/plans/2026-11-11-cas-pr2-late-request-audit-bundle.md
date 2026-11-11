# CAS late-request audit bundle (PR2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land five late-request audit fixes (stories A-E) as nine bisectable commits on branch
`fix/antalya-26.8/cas-late-request-audit-bundle`, verified by gates, reviews and before/after measurements.

**Architecture:** Each commit carries one product change and its tests. Stories A, B and C port audit drafts with
`git apply --3way` and then tighten them to the spec. Stories D and the three parts of E are written or ported by hand.
The last commit changes one upstream interface and is droppable alone.

**Tech Stack:** C++ (clang-21), gtest (`unit_tests_dbms`), ninja, Python integration tests, TLA+ (TLC) for the model gate.

**Spec:** `docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md` (binding; section numbers
below refer to it).

## Global Constraints

### Paths

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1` (the only tree a task modifies) |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master` (read-only for implementers) |
| `P` | `$M/tmp/prbundle` |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| Tests | `src/Disks/tests/` |
| Drafts | `$M/tmp/cas298/bundle/{0c57ca5a337e,cfc79da94a9a,c7a242d05bf8,0f285d4e01d5}.diff` |
| Draft digest | `$P/pr2/drafts-digest.md`, `$P/pr2/doubts-resolved.md` |
| Logs | `$P/pr2/logs/` |
| Reports | `$P/pr2/reports/task<N>.md` |
| Helpers | `$P/pr2/bin/bt.sh`, `$P/pr2/bin/mut.sh` |

Branch: `fix/antalya-26.8/cas-late-request-audit-bundle`, created in `WT` from the final PR1 head `be74be9fa7b`.
The PR1 branch `fix/antalya-26.8/cas-ref-cleanup-per-life-licence` stays at that head.

### Binding rules

- Modify files only under `WT`. Never touch `antalya-26.8-pr-2159`, `mt-committing-blocks`, `cas-95-1`, the
  `cas297-*` containers or anything on ports 9010, 9181, 11121, 18123, 18124, 18474, 19000, 19681-19684.
- The shell cwd is not stable. Start every command with `cd <absolute path> &&`.
- Never run `git clean`, `git reset --hard`, `git checkout -- <file>`, `git stash`, `git rebase`, `git commit --amend`,
  `git push`. Never delete a build directory. Never `kill`/`pkill` by pattern.
- Build and test only through the helper, which takes the build lock (one ninja at a time):

  ```bash
  cd $WT && nohup $P/pr2/bin/bt.sh <tag> '<gtest_filter>' > /dev/null 2>&1 &
  cd $WT && n=0; until grep -aq 'BT_DONE=' build/bt_<tag>.marker 2>/dev/null || [ $n -ge 54 ]; do sleep 10; n=$((n+1)); done; cat build/bt_<tag>.marker; tail -1 build/build_<tag>.log; tail -4 build/test_<tag>.log
  ```

  Run the wait in the foreground with a 600000 ms timeout. Repeat the wait if the marker is not there yet. Use a new
  `<tag>` for every run (`t<task>_red`, `t<task>_green`, `t<task>_gate`). A test log counts only if
  `build/build_<tag>.log` ends with `NINJA_EXIT=0` and names the current head or tree.
- The gate filter is exactly `CAS*`. Do not widen or narrow it for the gate run.
- Red first. Before the product change, the new tests run against a tree that compiles (a bare declaration or a stub
  where the test needs a new symbol). Copy the red test log to `$P/pr2/logs/red_<commit#>_<story>.log`. Tests the
  spec marks "not red" are listed as such in the report.
- Mutation check: at least the one the task names. Write the mutated copy of the product file to
  `$WT/tmp/mut_<tag>.<basename>` and run

  ```bash
  cd $WT && nohup $P/pr2/bin/mut.sh <tag> <file relative to WT> $WT/tmp/mut_<tag>.<basename> '<gtest_filter>' > /dev/null 2>&1 &
  cd $WT && n=0; until grep -aq 'MUT_DONE=' build/mut_<tag>.marker 2>/dev/null || [ $n -ge 54 ]; do sleep 10; n=$((n+1)); done; grep -c 'RESTORED=yes' $P/pr2/logs/mut_<tag>.log; grep -a 'FAILED\|PASSED\|GTEST_EXIT' $P/pr2/logs/mut_<tag>.log | tail -8
  ```

  The helper restores the file and rebuilds. `grep -c 'RESTORED=yes'` must print 1, and `git diff --stat` must show
  only your intended changes afterwards. A mutation that cannot compile (`if (true || ...)` trips
  `-Wunreachable-code`) is rewritten, not skipped. Mutations are never committed.
- `LOGICAL_ERROR` expectations are death tests under `DEBUG_OR_SANITIZER_BUILD` and `EXPECT_THROW`-style otherwise.
  Follow the existing pattern in the same test file.
- Commit by exact file list: `git add <files>` then `git commit -s`. Never `git add -A` or `git add .`. Check
  `git status --short` first and `git restore --staged` anything that is not yours.
- Commit message: the subject is given by the task, verbatim. Body: 1-3 sentences on why, lines at most 100 columns.
  No backlog ids, no "spec", "plan", "story", "draft", "audit". Then one blank line. Then the line
  `` `CAS*` gtest gate (release): <number> tests passed. `` with the number read from the gate log (never the
  letter N). Then one blank line and the trailers. `-s` adds `Signed-off-by`; add
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` as the last line. No session links, no other trailers.
- Code style: Allman braces; names of functions in prose written as `f`, not `f()`; literal names in inline code;
  comments say why, carry no task ids and no history ("previously", "now"); "exception", not "crash", for
  `LOGICAL_ERROR`. No sleeps to order threads; use hooks and latches.
- Scope: only the files the task lists. A defect found elsewhere goes into the report, not into the commit.
- Docs under `docs/`: plain words, no task ids, no invariant tags; new headers carry `{#anchor}`.
- Report: write `$P/pr2/reports/task<N>.md` (what changed, red evidence with log paths, mutation result, pass
  count, deviations from the brief and why, doubts). The final message repeats status, commit SHAs, the pass
  count and the doubts.
- Implementers do not dispatch subagents.

### Pass counts

`CAS*` release at the branch base: **2958** (`$P/gates/pr1_final/test_final.log`).

| After commit | Expected `CAS*` passed |
|---|---|
| 1 | 2959 |
| 2 | 2965 |
| 3 | 2979 |
| 4 | 2979 |
| 5 | 2987 |
| 6 | 2990 |
| 7 | 2993 |
| 8 | 2994 |
| 9 | 2996 |

A different number is reported with its reason (a parameterised test, a renamed test). Zero failures always.

## Review Focus

1. **`landed` against the moved-from ETag.** In `MountLeaseRenewer::renewOn`, `landed` must receive the committed
   ETag after `last_etag` is assigned, never a moved-from value, and `attemptEnded` must not run before `landed` in
   the `Committed` branch. Owner: Task 2; test B6 asserts `holds` on the committed body straight after a renewal.
2. **`driver_mutex` on the confirm path.** `CasMountRuntime::slotOwnership` takes `driver_mutex`. No holder of that
   mutex may do object-store I/O for the length of a remount step while a confirm waits. Owner: Task 3; the
   implementer lists every `driver_mutex` holder with what it does under the lock, and test B11 covers a remount
   between snapshot and `GET`.
3. **Eviction with pinned runtimes.** `enforceRefTableCacheBudget` must terminate and must still admit another
   namespace when a runtime is pinned by `removal_write_may_land`. Owner: Task 5; C3 loads a second namespace after
   the pinned one and asserts it works.
4. **Default-policy callers.** `cancelStalledCreating` and `beginRemoving` gain a trailing defaulted `policy`.
   Callers other than `dropNamespaceImpl` must behave as before. Owner: Task 6; the implementer greps the callers and
   lists them, and the pass count of every other suite stays unchanged.
5. **Overrides of `isBucketVersioningEnabled`.** A missed override silently stops overriding. Every override keeps
   `override`; `grep -rn isBucketVersioningEnabled src/` lists one interface declaration, one implementation, one
   production caller and the test overrides. Owner: Task 8.

---

### Task 0: Branch and baseline (controller)

- [ ] **Step 1:** After the PR1 gate chain ends, copy its logs and markers to `$P/gates/pr1_final/`.
- [ ] **Step 2:** `cd $WT && git status --short` is empty; `git switch -c fix/antalya-26.8/cas-late-request-audit-bundle be74be9fa7b`.
- [ ] **Step 3:** Record base SHA and the baseline count 2958 in the ledger.

### Task 1: Commit 1, story A: compare writer epochs

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §3.

**Files:**
- Modify: `CA/Pool/CasServerRoot.cpp` (`classifyFenceCertificate`), `CA/Pool/CasServerRoot.h` (doc comment of
  `isCreatorFenceTerminal`, about line 571)
- Test: `src/Disks/tests/gtest_cas_mount.cpp` (suite `CASFenceTerminal`)

**Interfaces:**
- Consumes: draft `$M/tmp/cas298/bundle/0c57ca5a337e.diff` (equals the spec; applies with `git apply --3way`,
  which also stages, so run `git restore --staged .` after applying).
- Produces: `classifyFenceCertificate` ordering: slot epoch greater than the asked epoch gives `SupersededEpoch`;
  lower gives `None`; equal and gc-fenced gives `GcFenced`; equal and clean release gives `CleanFarewell`; equal
  and live gives `None`.

- [ ] **Step 1:** Apply only the test hunk. Tests: A1 `ALowerWriterEpochIsNotTerminalForTheHigherOne` (loops live,
  gc-fenced and farewelled bodies at epoch 7; asserts not terminal for epoch 8). A2: rename
  `ADifferentLiveWriterEpochIsTerminalForTheOldOne` to `AGreaterLiveWriterEpochIsTerminalForTheOldOne`.
- [ ] **Step 2:** Run `bt.sh t1_red 'CASFenceTerminal*'`. Expected: A1 fails. Copy to `$P/pr2/logs/red_1_A.log`.
- [ ] **Step 3:** Apply the product hunks. The doc comment says "strictly greater".
- [ ] **Step 4:** Run `bt.sh t1_green 'CASFenceTerminal*'`, all pass.
- [ ] **Step 5:** Mutation: drop the epoch comparison (restore the base order). A1 must fail.
- [ ] **Step 6:** Run `bt.sh t1_gate 'CAS*'`. Expected 2959 passed.
- [ ] **Step 7:** Commit. Subject: `Compare writer epochs before trusting a mount slot's death certificate`.

### Task 2: Commit 2, story B part 1: slot ownership tracker

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review` (opus, high). Read spec §4.2 up to the confirm path, and
§4.3 tests B1-B6.

**Files:**
- Modify: `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp` (class `MountSlotOwnership`; hooks in
  `MountLeaseRenewer`), `CA/Pool/CasMountRuntime.h`, `CA/Pool/CasMountRuntime.cpp` (`slotOwnership`)
- Test: `src/Disks/tests/gtest_cas_mount.cpp` (new suite `CASMountSlotOwnership`)

**Interfaces:**
- Consumes: the `CasServerRoot.{h,cpp}` and `CasMountRuntime.{h,cpp}` hunks of `cfc79da94a9a.diff` as a starting
  point. The draft's `holds` checks only gc-fenced; the spec also rejects a clean release. The draft reads
  `*last_etag` after `std::move` in the `Committed` branch; fix it (Review Focus 1).
- Produces:

  ```cpp
  class MountSlotOwnership
  {
  public:
      void landed(const Etag & etag);
      void attempting(UInt128 write_attempt_id);
      void attemptEnded();
      bool holds(const Object & slot) const;   /// no I/O; own mutex
  };
  std::shared_ptr<const MountSlotOwnership> CasMountRuntime::slotOwnership() const;  /// null when there is no renewer
  ```

  Use the exact slot and ETag types the neighbouring code uses. `holds` is true only if the body is neither
  gc-fenced nor a clean release, carries this mount's `(server_uuid, writer_epoch)`, and either its ETag equals the
  last landed ETag or its `write_attempt_id` equals the in-flight one. Hooks: `MountLeaseRenewer::start` calls
  `landed` after `last_etag` is set. `renewOn` calls `attempting` right after the attempt id is minted and before
  admission, `attemptEnded` from a scope guard (one `attempting` covers every resend), `landed` in the `Committed`
  branch after `last_etag` holds the committed ETag. `terminate` records nothing. `slotOwnership` takes
  `driver_mutex` only to copy the pointer.
- No behaviour change in this commit: nothing reads `holds` yet.

- [ ] **Step 1:** Add the class declaration with a stub `holds` that returns `false`, the hooks and
  `slotOwnership`. Write B1 `HoldsTheLandedBody`, B2 `HoldsTheInFlightAttemptBody`, B3
  `DoesNotHoldAForeignUuidOrEpoch`, B4 `DoesNotHoldAGcFencedOrFarewelledBody`, B5
  `DoesNotHoldTheAttemptBodyAfterTheAttemptEnded`, B6 `RenewerRecordsItsStartAndEveryCommittedRenewal` (a real
  `MountLeaseRenewer` over the in-memory backend; asserts `holds` after `start` and after each of two renewals,
  and that the pre-renewal body is no longer held).
- [ ] **Step 2:** Run `bt.sh t2_red 'CASMountSlotOwnership*'`. Expected red: B1, B2, B6. B3-B5 pass against the
  stub. Copy to `$P/pr2/logs/red_2_B.log`.
- [ ] **Step 3:** Implement `holds`.
- [ ] **Step 4:** Run `bt.sh t2_green 'CASMountSlotOwnership*:CASMount*'`, all pass.
- [ ] **Step 5:** Mutation: make `holds` ignore the clean-release flag. B4 must fail.
- [ ] **Step 6:** Run `bt.sh t2_gate 'CAS*'`. Expected 2965 passed.
- [ ] **Step 7:** Commit. Subject: `Track which mount slot body the lease renewer owns`.

### Task 3: Commit 3, story B part 2: the confirm reads the mount slot

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review` (opus, high). Read spec §4.2 (confirm path), §4.3
tests B7-B20, §4.4.

**Files:**
- Modify: `CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp` (`Pool::confirmExactRef` moves out of line; test seam),
  `CA/Pool/CasMountRuntime.{h,cpp}` (`slotHeldBy`), `CA/Pool/CasRefLedger.{h,cpp}` (comments only),
  `CA/ContentAddressedExchange.h`, `CA/ContentAddressedMetadataStorage.{h,cpp}` (comments only),
  `src/Common/ProfileEvents.cpp`, `docs/en/antalya/metrics_and_events.md`
- Test: `src/Disks/tests/gtest_cas_confirm_exact_ref.cpp` (suite `CASConfirmExactRef`),
  `tests/integration/test_cas_gcs_relink_liveness/test.py`

**Interfaces:**
- Consumes: `MountSlotOwnership::holds`, `CasMountRuntime::slotOwnership` (Task 2); the remaining hunks of
  `cfc79da94a9a.diff` except `replication.md` and `manifests-and-refs.md` (Task 4).
- Produces: events `CASRelinkConfirmRefusedSlotMoved`, `CASRelinkConfirmRefusedSlotUnreadable` next to the existing
  `CASRelinkConfirmRefused*` block; `Pool::setConfirmAfterLedgerSnapshotHookForTest(std::function<void()>)`;
  `CasMountRuntime::slotHeldBy(const MountSlotOwnership &)` returning `bool` or a small enum (your choice; the two
  counters stay distinct).
- `Pool::confirmExactRef` order: (1) copy the ownership pointer before the ledger snapshot; (2) ask
  `CasRefLedger::confirmExactRef`, any answer other than `Yes` returns with zero I/O; (3) run the test hook;
  (4) `slotHeldBy`: admit on `mount_requests`, read `Layout::mountKey(server_root_id)` under `Retry::once`, answer
  `slot && ownership.holds(*slot)`; (5) a throwing read increments `CASRelinkConfirmRefusedSlotUnreadable` and
  rethrows; (6) not held increments `CASRelinkConfirmRefusedSlotMoved` and answers `Unknown`; (7) `Yes`. A null
  ownership pointer answers `Unknown` without a read. At most one `GET` per ledger `Yes`. No ledger mutex during
  the `GET`. No cache, no cap, not through the hot-key lane.
- Comments that say the confirm does zero I/O are corrected in this commit: `CasRefLedger::confirmExactRef`
  declaration and body, near `confirmLogger`, `CasPool.h`, `ContentAddressedExchange.h`,
  `ContentAddressedMetadataStorage.{h,cpp}`. Grep `zero I/O\|no I/O\|without I/O\|zero object-store` under `CA/` and
  list every hit with its fate in the report.

- [ ] **Step 1:** Port the draft tests B7-B15 and write B16-B20 (names and schedules in spec §4.3). B7, B9 and B15
  assert the counter they name. B17 needs a mutable `PoolConfig::boot_ms_fn`. B19's fixture is a warmed read-only
  pool over refs written by a writable mount. B20 uses a test-local after-read callback around
  `CountingBackend::read` that runs one renewal. Existing tests that assert zero requests on a `Yes` change to one
  mount-key `GET`. Add the events, the seam and a `confirmExactRef` that still answers from the ledger alone.
- [ ] **Step 2:** Run `bt.sh t3_red 'CASConfirmExactRef*'`. Expected red: B7-B12 and B14-B20. B13 passes. Copy to
  `$P/pr2/logs/red_3_B.log`.
- [ ] **Step 3:** Implement the confirm path and `slotHeldBy`. Correct the comments. Add the two rows to
  `metrics_and_events.md`.
- [ ] **Step 4:** `test.py`: add only `CASRelinkConfirmRefusedSlotUnreadable` to the must-be-zero list (the draft
  adds both; `SlotMoved` stays out). `python3 -m py_compile` the file.
- [ ] **Step 5:** Review Focus 2: list every holder of `driver_mutex` in `CasMountRuntime.cpp` with what runs under
  it. If one does object-store I/O under the lock, stop and report `DONE_WITH_CONCERNS` with the path.
- [ ] **Step 6:** Run `bt.sh t3_green 'CASConfirmExactRef*:CASMount*:CASRelink*'`, all pass.
- [ ] **Step 7:** Mutation: skip the `holds` check (treat any readable slot as held). B7 and B8 must fail.
- [ ] **Step 8:** Run `bt.sh t3_gate 'CAS*'`. Expected 2979 passed.
- [ ] **Step 9:** Commit. Subject: `Read the mount slot before a relink confirm answers yes`.

### Task 4: Commit 4, story B docs

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §4.2 and §14.

**Files:**
- Modify: `docs/en/antalya/cas/architecture/replication.md` (sequence diagram and its prose),
  `docs/en/antalya/cas/architecture/manifests-and-refs.md` (the "zero object-store I/O" sentence)

**Interfaces:**
- Consumes: the docs hunks of `cfc79da94a9a.diff`. The `replication.md` hunk conflicts on row 5: port by hand.
- Produces: text that states the confirm does one mount-slot read per ledger `Yes`, answers `Unknown` when the
  slot moved, and propagates a failed read as `Unknown` at the storage layer. Counters named in inline code.

- [ ] **Step 1:** Edit both files. No mermaid line contains `;`.
- [ ] **Step 2:** Check every claim against the code at head (`CasPool.cpp`, `ContentAddressedMetadataStorage.cpp`)
  and list claim, file and symbol in the report.
- [ ] **Step 3:** Commit (no gate line needed in a docs-only body; write
  `` `CAS*` gtest gate (release): 2979 tests passed. `` only if you ran it). Subject:
  `Document the mount slot read of the relink confirm`.

### Task 5: Commit 5, story C: lane stays closed after an unresolved removal write

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review` (opus, high). Read spec §5.

**Files:**
- Modify: `CA/Pool/CasRefLedger.h`, `CA/Pool/CasRefLedger.cpp` (`dropNamespaceImpl`, about line 5207;
  `enforceRefTableCacheBudget`), `CA/Pool/CasPool.h` (`setRemovalAttemptHookForTest`),
  `docs/en/antalya/cas/architecture/namespaces.md`
- Test: `src/Disks/tests/gtest_cas_ref_writer.cpp` (suite `CASRefWriterNamespaceRemoval`)

**Interfaces:**
- Consumes: `c7a242d05bf8.diff` (applies with `--3way`; unstage after).
- Produces: runtime flags under `ref_queue_mutex`: `removal_attempt_active` (set after
  `ensureRefTableRecovered`, cleared by a scope guard), `removal_write_may_land` (set in the `catch` when the
  outcome is unknown; never cleared; blocks the lane reopen and blocks eviction by `enforceRefTableCacheBudget`).
  Local `removal_write_outcome_unknown` is set right before `beginRemoving`; only `Transitioned` and
  `AlreadyRemoving` clear it (the draft also clears it on `EntryChanged`; remove that). Hook
  `setRemovalAttemptHookForTest` on `Pool` and `CasRefLedger`, member `removal_attempt_hook_for_test`.
- Warning text, verbatim: ``CAS namespace '{}': the outcome of its Live -> Removing catalog write is unknown; writes
  stay refused until a retried removal completes or the disk is remounted``. Refusals on a pinned lane say
  "a removal write may still land", not "is Removing".

- [ ] **Step 1:** Port the draft tests C1-C4 and write C5-C8 (names and schedules in spec §5.3). C1 needs a
  backend that holds the conditional write and applies it on release; the draft's throwing backend is replaced.
  C3 additionally loads a second namespace after the pinned one and asserts a write to it succeeds (Review
  Focus 3). C7 and C8 are not red.
- [ ] **Step 2:** Add bare declarations for the hook so the tests compile. Run
  `bt.sh t5_red 'CASRefWriterNamespaceRemoval*'`. Copy to `$P/pr2/logs/red_5_C.log`.
- [ ] **Step 3:** Apply the product change, tightened as above.
- [ ] **Step 4:** `namespaces.md`: what an operator sees (the warning, refused writes) and the two ways out.
- [ ] **Step 5:** Run `bt.sh t5_green 'CASRefWriter*:CASRefLedger*'`, all pass.
- [ ] **Step 6:** Mutation: clear `removal_write_outcome_unknown` on the `FencedOut` outcome of `beginRemoving`. C5 must
  fail.
- [ ] **Step 7:** Run `bt.sh t5_gate 'CAS*'`. Expected 2987 passed.
- [ ] **Step 8:** Commit. Subject: `Keep a namespace's write lane closed after an unresolved removal write`.

### Task 6: Commit 6, story D: one retry policy for the drop

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §6.

**Files:**
- Modify: `CA/Pool/CasRefLedger.cpp` (`dropNamespaceImpl`), `CA/Pool/CasRefCatalog.h` (about lines 121 and 186),
  `CA/Pool/CasRefCatalog.cpp`
- Test: `src/Disks/tests/gtest_cas_ref_writer.cpp` (suite `CASRefWriterNamespaceRemoval`)

**Interfaces:**
- Consumes: flags and hook from Task 5 (D3 uses `setRemovalAttemptHookForTest`); `VirtualRetryClock`
  (`src/Disks/tests/cas_test_helpers.h`, about line 2819); the pattern test
  `CASRefCatalogBirthWiring.APerpetuallyChurningCatalogEntryIsPacedAndEndsWithinOneWindow`.
- Produces: `dropNamespaceImpl` starts with `const Retry policy = initial_op.freeze(Retry::standard());` and passes
  it to the first `CasRefCatalog::read`, `cancelStalledCreating`, the creator-fence read in its predicate
  (`isCreatorFenceTerminal`), the second catalog read, the `gc/state` read and `beginRemoving` including its
  mismatch-branch read. `CasRefCatalog::cancelStalledCreating` and `CasRefCatalog::beginRemoving` gain a trailing
  `const Retry & policy = Retry::standard()` forwarded to `casUpdateImpl`. The resolution read in the `catch`
  keeps a fresh standard window. Local waits count against the window and are not cut short.

- [ ] **Step 1:** Write D1 `DropWithASlowCreatorFenceReadEndsAtTheEntryDeadline`, D2
  `DropSpendsOneWindowAcrossItsReads`, D3 `AWaitLongerThanTheWindowEndsTheDropAndLeavesTheLaneOpen` (schedules in
  spec §6.3). D3 is not red.
- [ ] **Step 2:** Run `bt.sh t6_red 'CASRefWriterNamespaceRemoval*'`. Expected red: D1, D2. Copy to
  `$P/pr2/logs/red_6_D.log`.
- [ ] **Step 3:** Implement. Review Focus 4: `grep -rn 'cancelStalledCreating\|beginRemoving' src/` and list every
  caller in the report with the policy it passes.
- [ ] **Step 4:** Run `bt.sh t6_green 'CASRefWriter*:CASRefCatalog*'`, all pass.
- [ ] **Step 5:** Mutation: pass no policy to the creator-fence read. D1 must fail.
- [ ] **Step 6:** Run `bt.sh t6_gate 'CAS*'`. Expected 2990 passed.
- [ ] **Step 7:** Commit. Subject: `Freeze one retry policy for the whole namespace drop`.

### Task 7: Commits 7 and 8, story E: checkpoint merge rejection and vanished pool meta

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review-lite`. Read spec §7.1 and §7.2. Two commits.

**Files (commit 7):** `CA/Pool/CasRefCkpt.h`, `CA/Pool/CasRefCkpt.cpp`; test
`src/Disks/tests/gtest_cas_ref_ckpt_join.cpp` (suite `CASRefCheckpointJoin`).
**Files (commit 8):** `CA/Pool/CasPoolMeta.cpp`; test `src/Disks/tests/gtest_cas_pool_meta.cpp`.

**Interfaces:**
- Consumes: `0f285d4e01d5.diff` as reference only. It conflicts in `CasPoolMeta.cpp`, `CasRefCkpt.h` and
  `gtest_cas_ref_ckpt_join.cpp`: port by hand. Its `CA/Gc/CasGc.cpp` hunk is dropped.
  `refuseFrontierAgainstDurable` already exists in `CasRefCkpt.cpp`; the test bodies must pass it so that the
  rejection comes from `mergeCkpt`.
- Produces (7): `publishCkpt`'s `decide` catches `CORRUPTED_DATA` from `mergeCkpt`; if `!op.admitted()` it declines
  with `Decline::Fenced` and `publishCkpt` returns `FencedOut`; an admitted writer still gets `CORRUPTED_DATA`. The
  header comment in `CasRefCkpt.h` says so.
- Produces (8): `rmwPoolMeta` throws `INVALID_STATE` instead of `LOGICAL_ERROR`, same text
  "CAS pool meta '{}' vanished during {}"; the "MUST be present" comment is corrected.

- [ ] **Step 1:** Write E1 `NonAdjacentFrontierFromAFencedOutWriterIsReportedFencedOutNotCorruption` (durable
  `committed_through=(10,1)`, `last_epoch_seal=(9,5)`; contribution `committed_through=(7,9)`,
  `last_epoch_seal=(7,9)`), E2 `SealInvariantBreakFromAFencedOutWriterIsReportedFencedOutNotCorruption` (durable
  `(5,10)`/`(5,10)`; contribution `committed_through=(5,12)`), E3
  `NonAdjacentFrontierFromAnAdmittedWriterIsCorruption` (as E1, admitted; not red).
- [ ] **Step 2:** Run `bt.sh t7_red 'CASRefCheckpointJoin*'`. Expected red: E1, E2. Copy to
  `$P/pr2/logs/red_7_E.log`. If E1 or E2 is refused by `refuseFrontierAgainstDurable` before `mergeCkpt`, adjust
  the bodies within the spec's intent and say so in the report.
- [ ] **Step 3:** Implement commit 7. Run `bt.sh t7_green 'CASRefCheckpoint*:CASRefCkpt*'`.
- [ ] **Step 4:** Mutation: drop the `!op.admitted()` condition. E3 must fail.
- [ ] **Step 5:** Run `bt.sh t7_gate 'CAS*'`. Expected 2993 passed. Commit. Subject:
  `Decline a checkpoint merge rejection as fenced for a writer that lost admission`.
- [ ] **Step 6:** Write `CASPoolMeta.PoolMetaVanishingMidAdmissionIsInvalidStateNotLogicalError`. At the base it is
  red (an abort in a debug build, a wrong code in release). Run `bt.sh t8_red 'CASPoolMeta*'`; copy to
  `$P/pr2/logs/red_8_E.log`.
- [ ] **Step 7:** Implement commit 8. Run `bt.sh t8_gate 'CAS*'`. Expected 2994 passed. Commit. Subject:
  `Report a vanished pool metadata object as an invalid state`.

### Task 8: Commit 9, story E: bounded bucket versioning probe (upstream surface)

Implementer `ca-impl` (sonnet, medium). Reviewer `ca-review` (opus, high) plus a codex consult on the diff before
the commit. Read spec §7.3 and §10.

**Files:**
- Modify: `src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h` (line 555),
  `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h` (line 198), `S3ObjectStorage.cpp` (line 831),
  `CA/Backend/CasObjectStorageBackend.cpp` (line 155), `docs/en/antalya/cas/bucket-requirements.md`
- Test: `src/Disks/tests/gtest_cas_backend_generation.cpp` (override at line 141; new E4),
  `src/Disks/tests/gtest_cas_s3_single_attempt_client.cpp` (new E5),
  `src/Disks/tests/gtest_cas_s3_staging.cpp` (override at line 1252)

**Interfaces:**
- Consumes: the matching hunks of `0f285d4e01d5.diff`.
- Produces: `virtual std::optional<bool> isBucketVersioningEnabled(const ObjectStorageControlRequest & request) const`
  (default still returns `std::nullopt`); `S3ObjectStorage` uses
  `getSingleAttemptClient(request.attempt_timeout_ms, request.connect_timeout_cap_ms, /*disable_adaptive_timeouts=*/ true)`;
  `CasObjectStorageBackend` passes `controlRequest(0)`.
- The upstream hunks stay minimal: one parameter, no reformatting, no renames, no new includes unless required.

- [ ] **Step 1:** Write E4 `CASBackendGeneration.CheckPoolPreconditionsProbesVersioningSingleAttemptAndBounded` and
  E5 `CASEnvelopeWiring.BucketVersioningProbeSingleAttemptOutlastsTheFirstAttemptTimeouts`; update the two test
  overrides. Red run: `bt.sh t9_red 'CASBackendGeneration*:CASEnvelopeWiring*'` against a tree where the interface
  has the new parameter but `S3ObjectStorage` and the backend ignore it. Copy to `$P/pr2/logs/red_9_E.log`.
- [ ] **Step 2:** Implement. Update `bucket-requirements.md`.
- [ ] **Step 3:** Review Focus 5: `grep -rn isBucketVersioningEnabled src/` and paste the output into the report.
- [ ] **Step 4:** Run `bt.sh t9_gate 'CAS*'`. Expected 2996 passed.
- [ ] **Step 5:** **Do not commit.** Write `git diff` to `$P/pr2/commit9.diff`, a hunk-by-hunk motivation table for
  the three upstream files to `$P/pr2/reports/task8.md`, and return `READY_FOR_CONSULT`.
- [ ] **Step 6 (controller):** codex consult on `$P/pr2/commit9.diff` (read-only). Findings go back to the same
  implementer.
- [ ] **Step 7:** Apply accepted findings, rerun `bt.sh t9_gate2 'CAS*'` if anything changed, commit. Subject:
  `Probe bucket versioning with one attempt at the full attempt timeout`.

### Task 9: Gates on the final head (controller)

Tag `pr2final` for every marker and log.

- [ ] Release build + `CAS*`; ASan and UBSan `CAS*`; Fast-test compile (`$P/gates/chain.sh pr2final`).
- [ ] Stateless: the 58 `*_cas_*` tests on a standalone server (tcp 19791, keeper 19795).
- [ ] Integration: `test_cas_replicated_relink`, `test_cas_gcs_relink_liveness` (own docker network,
  `DOCKER_BASE_TAG=<n>-amd64`), on the PR1 and PR2 binaries. Wall time and counters feed Task 11.
- [ ] Model: `docs/superpowers/models/run_relinkconfirm.sh CaRelinkConfirmCore_cur_staleyes` red and
  `CaRelinkConfirmCore_fix_a` green. Not next to a ninja run.
- [ ] Style: the functions of `ci/jobs/check_style.py`, output filtered to the touched files.
- [ ] A sonnet subagent summarises the logs. Every red is root-caused or gets a backlog task.

### Task 10: Whole-branch reviews (controller)

- [ ] One Opus review (`ca-review`) and one codex review, both on `be74be9fa7b..HEAD`, reports under
  `$P/pr2/review/`.
- [ ] One umbrella review.
- [ ] Findings split into code and prose. Code fixes by implementers in parallel on disjoint files, prose fixes in
  one pass. Scoped re-review. All minors closed.
- [ ] Gates rerun on the fixed head.

### Task 11: Measurements (controller)

- [ ] S2 (churn) and S4 (no churn) with the PR2 binary, same script and settings as R2 and R4 (spec §12).
- [ ] Table R2 against S2 and R4 against S4, predictions P1-P7 each marked held or not.
- [ ] Report `docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`.
