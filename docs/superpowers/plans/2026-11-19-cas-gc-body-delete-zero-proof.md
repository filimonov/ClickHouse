# CAS GC body-delete zero proof Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** GC deletes a blob body only after proving the blob had no owner at the round's catalog read; four reproduced or code-confirmed loss paths (D1 to D5 of the spec, and two copy paths) are closed.

**Architecture:** One condition in the pure rule function (`decideIntentStep`): a `pending` row whose key the round touched is carried, not deleted. One uncached `gc/state` read in `Gc::fold` feeds the round-wide destructive gate. `Gc::rebuildBaseline` refuses a baseline it cannot count. `createHardLink` and `republishRef` confirm their source after the destination precommit, as relink does. No on-store format change.

**Tech Stack:** C++ (ClickHouse, `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` = `CA/`), gtest (`src/Disks/tests/gtest_cas_*.cpp`, helpers in `cas_test_helpers.h`), integration tests (`tests/integration/test_cas_*`).

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-11-19-cas-gc-body-delete-zero-proof.md`

## Global Constraints

- Build, test, commit commands and rules: `/home/mfilimonov/workspace/ClickHouse/master/.superpowers/sdd/2026-11-17-cas-blob-deletion-intents/global-constraints.md` (binding; read first).
- Worktree `/home/mfilimonov/workspace/ClickHouse/cas-pr1`, branch `work/antalya-26.8/cas-blob-intents`, base of this plan `1fbdd9555c4`. One builder and committer at a time. No push, no amend, no rebase, no branch switch.
- Unit gate filter EXACTLY `CAS*`, on `build` and `build_asan`. Counts at the base: 3511 / 3514.
- A `LOGICAL_ERROR` expectation is a death test in debug / sanitizer builds and a catching test in release (the split already used in `gtest_cas_intent_rules.cpp`).
- No sleeps to order a race: hooks, latches, parked requests. A new parameterized suite needs an instantiation prefix that starts with `CAS`.
- No on-store format change. No new setting. No new time assumption.
- Comments say why in one or two lines; no rule ids of the spec, no review, plan or task references.
- Reuse of a body after `HEAD` with no upload stays. The writer sends `HEAD` before `PUT`.
- Reproducing tests exist as patches: `/home/mfilimonov/workspace/ClickHouse/master/tmp/intents/reports/crossns-repro.patch` (D1), `coverage-audit-tests.patch` (D2, D3), `source-liveness-tests.patch` (copies). They are the red tests of Tasks 1 and 4; each asserts the correct behaviour or is turned to do so.
- Evidence to read where a task names it: `tmp/intents/reports/crossns-loss.md`, `coverage-audit.md`, `review-fix-design.md` (F1 to F8), `model-zeroproof.md` (sections "For an implementer", "What the veto does not guard"), `source-liveness.md`, all under `/home/mfilimonov/workspace/ClickHouse/master/`.

## Review Focus

1. A blob whose key the round touched only by a removal (an unmatched `-1`) while its row is `pending`: the row is carried, never deleted. Test in Task 1.
2. A vetoed row in the same round as an unrelated reclaimable blob of the same shard: the unrelated blob is deleted in that round. Test in Task 1.
3. The parent validation read fails or is unanswered: the round takes no destructive step and does not commit a deletion; it does not throw on a 404 of `gc/state` either, it treats it as a mismatch. Test in Task 2.
4. A rebuild refused for a bodiless live precommit succeeds on a retry once the manifest is readable. Test in Task 3.
5. An identical concurrent rename that finished before this call published anything, and one that removed the source after this call's promote: both stay success. Existing tests `CASPartFolderAccess.RepublishRefCompletesWhenIdenticalRenameTakesDestinationDuringPublish` and `...WhenSourceAlreadyTakenByIdenticalMove` must stay green. Task 4.

---

### Task 1: Addition veto in the rule function

**Files:**
- Modify: `CA/Gc/CasIntentRules.cpp` (the `delete_pending` branch, about `:140-145`), `CA/Gc/CasIntentRules.h` (the comment of `decideIntentStep`)
- Test: `src/Disks/tests/gtest_cas_intent_rules.cpp`, `src/Disks/tests/gtest_cas_gc_intents.cpp` (the patches' tests land here), `src/Disks/tests/gtest_cas_gc_shard_plan.cpp` or the `CASShards...` suite for one sharded case

**Interfaces:**
- Consumes: `IntentRuleInput::key_touched` (already set by `foldDeltasIntoGeneration` from `cur_touched`: every delta of the blob before netting, an orphan-source retirement, a surviving prior edge).
- Produces: `decideIntentStep` returns `IntentStep::Carry` for a `pending` row at zero whose decision `HEAD` shows the row's identity when `key_touched` is true; `IntentStep::DeleteBody` only when it is false. Nothing else changes.

- [ ] **Step 1: Land the reproducing tests, red.** Apply `crossns-repro.patch` and the D2 and D3 tests of `coverage-audit-tests.patch`. Each must assert: body present after the round, no body `DELETE` in the request journal, `cas-fsck` reports no dangling reference. Run them on `build`: D1 (both walk orders), D2 and D3 fail; the control passes. Paste the failures.
- [ ] **Step 2: Rule unit tests, red.** In `gtest_cas_intent_rules.cpp`: `pending`, zero, `HEAD` same identity, `key_touched` true → `Carry`, no `intent_to_delete`, no body delete; `key_touched` false → `DeleteBody` as today. Extend the independent expected-result function of `EveryReachableCellFollowsTheSpec` with the same condition and re-derive the reachable-cell count by hand in the test's comment.
- [ ] **Step 3: Implement.** In the `delete_pending` branch: `seen == HeadSeen::Same` → `input.key_touched ? IntentStep::Carry : IntentStep::DeleteBody`. Comment: a record of the blob in this round's window may be a reference that another namespace's cut missed; only an untouched zero proves no reference at the adopted commit.
- [ ] **Step 4: Verify D2 with the rule alone.** If the D2 test stays red after Step 3, STOP and report the trace: the spec argues the source's activation is in a walked life; a red D2 means that argument fails in the code.
- [ ] **Step 5: More fold tests** in `gtest_cas_gc_intents.cpp`: (a) relay over three namespaces and a two-namespace relay that copies back: body kept in every round; (b) `+1` then `-1` of one blob in one intake with the row `pending`: `Carry`; (c) Review Focus 1 and 2; (d) churn: add and drop of one content in each of three rounds: no body delete; after the last drop the second complete, unsuppressed round with delete budget deletes; (e) after D1 the destination part exists: a quiet round spares the blob and drops the row; after the destination is dropped the body is reclaimed; (f) one D1 case in a non-zero shard with `gc_shards = 4`.
- [ ] **Step 6: Gates.** `CAS*` on `build` and `build_asan`; `test_cas_gc_s3/test.py` (it pins exactly 3 logged requests per reclaimed blob on the plain drop path: must hold).
- [ ] **Step 7: Commit** (two commits: tests that reproduce, then the rule with its tests): `Reproduce the CAS body delete under a cross-namespace copy` and `Carry a pending CAS row whose blob the round touched`.

### Task 2: Validate the parent state before any destructive step

**Files:**
- Modify: `CA/Gc/CasGc.cpp` (`Gc::fold`: after the `finishNamespace` loop, before the destructive gate; the unused parameter `state_etag`), `CA/Gc/CasGc.h` if a signature changes
- Test: `src/Disks/tests/gtest_cas_gc_intents.cpp` or `gtest_cas_gc_round.cpp`

**Interfaces:**
- Consumes: the leased `gc/state` version that `Gc::fold` receives as `state_etag`.
- Produces: one uncached read of `gc/state` per fold that would otherwise take a destructive step; on any answer other than "the exact leased version" (changed, absent, unanswered) the fold sets the round-wide suppression (`suppress_destructive` or the name the code uses) before the reduce. It guards body deletes and every intent delete. A new suppression reason is counted and logged like the existing ones.

- [ ] **Step 1: Read** `review-fix-design.md` F1 and the code it cites (`planRefCleanup` `CA/Pool/CasRefProtocol.cpp:863`, `readCheckpointWitnesses` `CA/Gc/CasGc.cpp:1739`, the warm `ref_fold_memo` path).
- [ ] **Step 2: Failing test** `CASGcIntents.PausedLeaderWhoseLogWasCleanedDeletesNothing`: leader L1 commits `pending(t)` with a writer's `+1` above its cursor; L1's next round is parked after its catalog read (hook on a store answer); L2 steals, folds, commits, and `cleanupRefObjects` removes the records above L1's cursor; L1 resumes. Assert: no body `DELETE` from L1, the body present, fsck clean. Second test, same setup with a `standing(t)` row: no intent `DELETE` from L1. Both red on the base.
- [ ] **Step 3: Implement** the validation read and feed the gate. The read happens only when the fold has a destructive candidate (no extra request in an idle or pure-carry round: assert that in a test with the request journal).
- [ ] **Step 4: Tests for Review Focus 3**: the validation read answers absent; answers unanswered: no destructive step, no exception from a 404.
- [ ] **Step 5: Gates** as Task 1, plus the existing idle-round request tests (an idle round must still send zero store requests).
- [ ] **Step 6: Commit**: `Validate the leased CAS GC state before a round destroys anything`.

### Task 3: Refuse a rebuild that cannot count a live precommit

**Files:**
- Modify: `CA/Gc/CasGc.cpp` (`Gc::rebuildBaseline`, the live-precommit loop about `:5055-5082`)
- Test: `src/Disks/tests/gtest_cas_gc_rebuild.cpp`
- Docs: `docs/en/antalya/cas/operations/` (the page that documents `SYSTEM CAS GC REBUILD` / `cas-gc-rebuild`)

**Interfaces:**
- Produces: `rebuildBaseline` throws (the code the neighbouring refusals of the function use) when a live precommit's manifest body is unreadable; no baseline is installed; the message names the namespace and the precommit and says to retry when the manifest is readable or the precommit is removed. No exemption for a build below the mount floor.

- [ ] **Step 1: Failing test** `CASGcRebuild.RefusesABaselineThatCannotCountALivePrecommit` after `review-fix-design.md` F2: writer X precommits a manifest whose body is not yet uploaded and reuses body `t`; rebuild: today it installs a baseline with a hold. Assert the refusal and that `gc/state` is unchanged. Then the loss sequence as a second test that must not lose the body: manifest lands, X promotes, another part adds and drops the blob, two rounds: body present.
- [ ] **Step 2: Implement** the refusal; remove the hold-at-`cursor + 1` path for this case.
- [ ] **Step 3:** Review Focus 4 test (retry succeeds once the manifest is readable). Update the existing test that asserts `clamped_shards` for this case.
- [ ] **Step 4: Docs:** one paragraph on the refusal and the retry; the repair order for a pool that migrates to generation 4 (migrate, rebuild with this build, start the servers) in `operations/migration.md`, with the reason in one sentence. Keep every `{#anchor}`.
- [ ] **Step 5: Gates** as Task 1 plus `test_cas_blob_intent_migration/test.py`.
- [ ] **Step 6: Commit**: `Refuse a CAS GC rebuild that cannot count a live precommit`.

### Task 4: Confirm the source of a no-bytes copy after the destination precommit

**Files:**
- Modify: `CA/ContentAddressedTransaction.cpp` (`createHardLink`, the publish path that calls `precommitAdd` then promote), `CA/Parts/PartFolderAccess.cpp` (`republishRef`), `CA/Pool/CasPartWriteTxn.{h,cpp}` if the adopted entry must carry its source
- Test: `src/Disks/tests/gtest_cas_part_folder_access.cpp` and the transaction tests the patch touches

**Interfaces:**
- Consumes: `Pool::confirmExactRef` (what relink uses: it checks the exact binding, in-flight mutations and mount-slot ownership).
- Produces: every adopted entry made by `createHardLink` carries its original committed source `(namespace, ref, manifest ref)`, also through staged copies and renames inside the transaction; after `precommitAdd` and before promote each distinct source is confirmed; an unconfirmed source abandons the build with a retryable error (the error code the relink path uses for the same condition). `republishRef`: destination already committed with equal content → success with no confirmation; this call precommits a new destination and the source is missing or unconfirmed before the promote → the call fails and abandons its precommit; source removed by an identical concurrent move after this call's promote → success.

- [ ] **Step 1: Read** `source-liveness.md` and apply `source-liveness-tests.patch`; turn its three tests to assert the correct behaviour (the copy is NOT promoted; `republishRef` does not report success; fsck has no dangling reference). Red.
- [ ] **Step 2: Implement** `createHardLink` source carrying and the confirmation. An entry whose bytes the destination uploads itself carries no source.
- [ ] **Step 3: Implement** the three `republishRef` cases.
- [ ] **Step 4: Tests:** a hardlink of an entry staged earlier in the same transaction confirms the original source; Review Focus 5 (both existing identical-move tests green); a mutation-style hardlink of many entries from one source confirms that source once (assert the number of confirmations, not requests).
- [ ] **Step 5: Gates** as Task 1, plus integration `test_cas_shared_pool/test.py` and the stateless CAS tests that move or attach partitions (`ls tests/queries/0_stateless/*cas* | xargs grep -l -i "MOVE PARTITION\|ATTACH PARTITION\|REPLACE PARTITION\|RENAME TABLE"`), run as `tmp/intents/reports/testrun.md` describes.
- [ ] **Step 6: Commit** (two commits): `Confirm the source of a CAS hardlink after its destination precommit`, `Confirm the source of a CAS republish before its promote`.

### Task 5: Intake requirement tests and the docs of the proof

**Files:**
- Test: `src/Disks/tests/gtest_cas_gc_intents.cpp` (or the file that holds the catalog / walk tests)
- Docs: `docs/en/antalya/cas/architecture/garbage-collection.md`, `docs/en/antalya/cas/architecture/blob-protocol.md` (the section that argues copies without reading the body)

**Interfaces:**
- Consumes: Tasks 1 to 4.
- Produces: no product change expected. If a test of this task fails, STOP and report: it is a new defect.

- [ ] **Step 1: Tests of the requirement:** (a) a life with no parent row is walked from its first record in the round that first sees it (a `+1` in it vetoes a delete of that blob in that round); (b) a life the stream listing omits but the catalog lists is walked; (c) a `Creating` life cannot hold a precommit (the append is refused); (d) a namespace removed and re-created under the same name between two rounds: the new life starts from its own first record; (e) the catalog read of a round follows its lease acquisition (request journal order).
- [ ] **Step 2: Docs:** in `garbage-collection.md` the body-delete rule gains its condition in plain words (the round folded no record of the blob) and the reason; in `blob-protocol.md` replace the sentence that a live source edge is counted by the cut with the zero-owner argument in four sentences; a line in the monitoring page for the new suppression reason of Task 2. Follow `/home/mfilimonov/.claude/skills/tech-prose/STYLE.md`; no rule ids; keep anchors.
- [ ] **Step 3: Gates:** `CAS*` on both builds; `ninja clickhouse` in `build`.
- [ ] **Step 4: Commit**: `Test the CAS GC intake requirements of the body-delete proof`, `Document why a CAS body delete needs an untouched zero`.

### Task 6: A hook inside the round and the copy matrix

**Files:**
- Modify: `src/Disks/tests/cas_test_helpers.h` (the hook facility)
- Create: `src/Disks/tests/gtest_cas_gc_round_interleave.cpp`
- Modify: `src/Disks/tests/gtest_cas_gc_intents.cpp` (move the hand-made hooks of Tasks 1 and 2 onto the facility)

**Interfaces:**
- Consumes: Tasks 1, 2 and 4 (the cells assert the fixed behaviour).
- Produces: `RoundInterleavePoint` with the values `AfterCatalogRead`, `BetweenNamespaces`, `AfterIntake`, `AfterRequestsBeforeCommit`, and a helper `runRoundWithCallback(gc, point, selector, callback)` where `selector` names the two namespaces and the pass for `BetweenNamespaces`. Built on the store-answer hooks of the existing fakes; no product seam. If one of the four points cannot be reached without a product seam, STOP on that point and report which answer is missing.

- [ ] **Step 1: The facility with its own tests:** each point fires exactly once, at the stated place in the request journal, in both walk orders and in pass 2.
- [ ] **Step 2: The matrix** as a parameterized suite with instantiation prefix `CASInterleave`: {copy path: `createHardLink` across namespaces, `republishRef`, relink} × {source dropped: before the round, inside the intake, after the round} × {row: none, `candidate`, `pending`, `standing`} × {one hop, a relay of two hops, a destination namespace created inside the round}. Assertions per cell: the body is present while a committed part names it; no body `DELETE` was sent for it; `cas-fsck` reports no dangling reference. A cell that cannot occur is skipped by an explicit list with a one-line reason in the test, and the test prints the count of run and skipped cells; the report gives both numbers.
- [ ] **Step 3: Show the matrix can fail:** revert the Task 1 rule line in the working tree, run the suite, record which cells go red (expected: the `pending` cells with the source dropped inside the intake), restore the line and verify with `git diff` that the product file is unchanged. Paste the red list in the report.
- [ ] **Step 4: Other events inside a round, one test each:** a namespace removed and re-created between the reads; a ref-log snapshot that covers an unread add and drop (the round takes no destructive step for that blob); an intake slice cut by the time budget so that one namespace is read over two rounds; a leader change between the intake and the commit; a state rebuild between the two rounds of a relay.
- [ ] **Step 5: Gates:** `CAS*` on `build` and `build_asan`; the new suite 30 times on both.
- [ ] **Step 6: Commit** (two commits): `Add a CAS GC test hook for events inside a round`, `Test CAS copies between tables against every row state inside a round`.

### Task 7: Statements under a running GC, the soak endings and a scenario card

**Files:**
- Create: `tests/integration/test_cas_cross_table_copy_gc/` (`test.py`, `configs/`), in `cas-pr1`
- Modify (checkout `/home/mfilimonov/workspace/ClickHouse/master`, harness only, unstaged): `tmp/intents/soak/driver.py`, `utils/ca-soak/scenarios/cards/` (one new card), `utils/ca-soak/scenarios/README.md`

**Interfaces:**
- Consumes: the fixed binary of Tasks 1 to 4.
- Produces: an integration suite and harness changes; no product change. A failing oracle is a product-defect candidate: keep the evidence and report first.

- [ ] **Step 1: Integration suite.** One test per statement: `MOVE PARTITION TO TABLE`, `ATTACH PARTITION FROM`, `REPLACE PARTITION`, `CREATE TABLE ... CLONE AS` followed by `DROP` of the source, a `RENAME TABLE` that changes the namespace, a fetch by relink on a second replica followed by a drop on the first. Each test: insert content, drop it, re-insert the same content (so the blobs carry GC rows), run the statement while GC rounds run in a loop with a short period, repeat 50 times with different timing offsets; oracle after each iteration: a full read with a checksum equals the recorded one; at the end `SYSTEM CAS FSCK` shows no dangling reference and the server logs hold no `LOGICAL_ERROR`. Model the suite on `tests/integration/test_cas_shared_pool`.
- [ ] **Step 2: Run** the suite with `tmp/intents/testrun/run_integ.sh test_cas_cross_table_copy_gc/test.py zp7_xcopy 3600` on the fixed binary; then once on the binary of the plan base (`tmp/intents/bench/clickhouse.intents-5da79206a72`, by pointing the runner at it) and record whether any iteration shows a dangling reference there (the window is milliseconds: a clean run on the old binary is expected and is not evidence of safety; say so).
- [ ] **Step 3: Soak endings.** In `tmp/intents/soak/driver.py` add the operation endings `ATTACH PARTITION FROM`, `MOVE PARTITION TO TABLE` and `CLONE AS` + drop of the source to the writer loop, with their own counters in `run.log`, and a pause of one second after a failed statement (the driver retried without a pause before). Run `soak.sh 1800 <fixed binary>` on a quiet machine; read `run.done` and the log tail before any statement about the result.
- [ ] **Step 4: Scenario card** `S48`: cross-table copies of re-inserted content under GC at the suite's dev and ci scales, verdicts: data oracle, fsck hard findings, no body left dangling, counts of each statement executed. Register it; run it at dev scale; add its line to the README.
- [ ] **Step 5: Commit** in `cas-pr1`: `Test CAS copies between tables under a running GC`. The harness edits in `master` stay unstaged and are listed in the report.
