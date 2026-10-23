---
id: CAS-411
title: >-
  Strengthen the owner-Move tests: crash after `Move` before the Keeper multi,
  old-node GC in the compat test, 05387/05389 oracles
status: To Do
assignee: []
created_date: '2026-10-22 07:59'
updated_date: '2026-10-23 02:30'
labels:
  - 'area:testing'
  - 'area:replication'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-8
dependencies:
  - CAS-346
references:
  - tests/integration/test_cas_ref_move_compat/test.py
  - tests/integration/test_cas_early_commit_restart/test.py
priority: medium
type: task
ordinal: 514000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Gaps left by the owner-Move series on `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8). (1) No test kills the server after `renameParts` landed the `Move` and before the Keeper multi on a Replicated CAS table; `test_cas_early_commit_restart` pauses before the rename. Expected: 'unexpected part' at load, no ack, the ref dropped at the final name, clean FSCK, a retry lands. (2) `test_cas_ref_move_compat` asserts only that 26.6.4 refuses to mount a generation-2 pool; it does not wait several GC intervals on the still-mounted 26.6.4 node over a Move-bearing log and assert its GC log/state, nor that it keeps serving reads of a namespace it does not own. (3) 05389 checks only the two `detached/` moves (DETACH publish counts and 'ATTACH adds no publish' are unasserted); 05387 does not check the `r2` row count; 05388 uses a lower bound on the part count.

Provenance: umbrella review of the owner-Move series, 2026-10-22 (tests perspective #11, deep audit #15); `tmp/clean/umbrella_review.md` sections 'Needs verification' and 'Tests to add or strengthen'.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Integration test: kill after the Move, before the Keeper multi; after restart count() is 0, the final-name ref is gone, FSCK is clean, the retried INSERT lands
- [ ] #2 `test_cas_ref_move_compat` asserts the 26.6.4 node's GC over a Move-bearing log and its reads of a foreign namespace after the raise
- [ ] #3 05389 asserts DETACH publish counts and zero publishes on ATTACH; 05387 asserts the `r2` row count
- [ ] #4 gtest: `moveRefIfMatches` onto a destination with a precommit leaked by an interrupted publish in the same epoch (`CAS_OWNERSHIP_CONFLICT` until the stale-precommit sweep); the test states the sweep horizon it relies on
- [ ] #5 05387: replicated `OPTIMIZE` oracle stays `published_under_final_name 1, moves 0` (a replicated merge renames inside the open storage transaction and commits once under the final name)
- [ ] #6 One stateless oracle per partition-command family newly admitted on CAS without a whitelist: REPLACE PARTITION FROM, MOVE PARTITION TO DISK/VOLUME, FETCH PARTITION, FREEZE/UNFREEZE, DROP DETACHED, ATTACH PART — each with the `SYSTEM CAS FSCK` epilogue of 05388
- [ ] #7 05386 stops merges on `t_src` before its INSERT and asserts the part count before `ATTACH PARTITION FROM`
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-22: 05390 no longer masks an ALTER failure (`grep -o | head -1` removed), 05386–05392 carry `no-random-settings, no-random-merge-tree-settings`, and 05387 gained the replicated-merge oracle (branch commit after 721d7dea775). AC #2 (old-node GC in the compat test) is deprioritized by the owner: 26.6/26.8 coexistence is not a supported mode, upgrade = stop all 26.6.x first.

2026-10-23 (third umbrella review): no test can reach the new `LOGICAL_ERROR` in `MergeTreeData::Transaction::commit` (it needs a producer that skips the early commit; a gtest would need a `MergeTreeData`); accepted as untested. The compat-test extensions (readonly mount keeps floor 1; old node GC over a Move) stay deprioritized by the owner.

Review 2026-10-23 (tmp/review2/REPORT.md): add a test with a throwing event sink for the moveRefExact and dropRefExact audit sites (emitAuditBestEffort must not fail the durable mutation); the LockMemoryExceptionInThread in dropRefIfMatches/dropRefBestEffort has no test. Staged-source conflict paths now have RenameOfStagedEditConflictsWhenBaseWasDropped/Republished and RenameCommittedRefConflictsWhenSourceNamesAnotherManifest.

Audit-sink test done on the draft branch: CASPartFolderAccess.AuditSinkFailureDoesNotFailTheDurableMoveOrDrop (a throwing sink; Move and drop still land; both events reach the sink). Note: Pool::emitEvent already swallows sink exceptions ('Content-addressed audit event sink threw; the event was dropped'); emitAuditBestEffort is the second layer that also covers event construction.
<!-- SECTION:NOTES:END -->
