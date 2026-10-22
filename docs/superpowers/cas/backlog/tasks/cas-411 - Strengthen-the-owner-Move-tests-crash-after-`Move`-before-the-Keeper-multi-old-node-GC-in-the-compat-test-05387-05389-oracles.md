---
id: CAS-411
title: >-
  Strengthen the owner-Move tests: crash after `Move` before the Keeper multi,
  old-node GC in the compat test, 05387/05389 oracles
status: To Do
assignee: []
created_date: '2026-10-22 07:59'
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
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
