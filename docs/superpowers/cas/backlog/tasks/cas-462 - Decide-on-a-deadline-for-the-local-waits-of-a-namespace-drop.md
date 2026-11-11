---
id: CAS-462
title: Decide on a deadline for the local waits of a namespace drop
status: To Do
assignee: []
created_date: '2026-11-11 17:47'
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
