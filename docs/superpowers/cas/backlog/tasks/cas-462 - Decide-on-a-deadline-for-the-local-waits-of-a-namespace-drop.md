---
id: CAS-462
title: Decide on a deadline for the local waits of a namespace drop
status: To Do
assignee: []
created_date: '2026-11-11 17:47'
updated_date: '2026-11-12 01:08'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
  - 'complexity:small'
dependencies:
  - CAS-69
priority: low
type: feature
ordinal: 566000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A namespace drop has waits with no deadline. `dropNamespaceImpl` waits for the write lane to drain, for another removal attempt to end (`removal_attempt_active`, added by CAS-367) and, after the terminal append, for pending snapshot publishers to settle (`publish_settle_cv`, `CasRefLedger.cpp`). CAS-69 freezes one retry policy for the object-store requests that lead to the removal decision; it charges these waits against the window and does not cut them short. Decide whether a wait needs its own deadline, and what a waiter that gives up may conclude about the lane.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each of the three waits either has a deadline with a test, or a written reason why it needs none
- [ ] #2 A waiter that gives up never reopens a lane another attempt still owns
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
2026-11-12: input from the final review of the late-request bundle. The drop freezes one 90 s policy at entry, and `ensureRefTableRecovered` plus the lane-drain wait run inside that window. The first `DROP` of a cold table with a long unfolded ref log can therefore end with retry-later on elapsed time alone, with nothing sent; the lane reopens and the retry succeeds because recovery is then resident. `AWaitLongerThanTheWindowEndsTheDropAndLeavesTheLaneOpen` pins the wait case; no test pins the recovery case. The review proposed starting the window after recovery. Declined in the bundle: the behaviour self-heals and the choice belongs to this decision. Measure real recovery time on a long log before deciding.
<!-- SECTION:NOTES:END -->
