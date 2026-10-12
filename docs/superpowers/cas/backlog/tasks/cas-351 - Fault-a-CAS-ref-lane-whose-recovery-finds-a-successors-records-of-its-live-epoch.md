---
id: CAS-351
title: >-
  Fault a CAS ref lane whose recovery finds a successor's records of its live
  epoch
status: To Do
assignee: []
created_date: '2026-10-08 16:49'
updated_date: '2026-10-12 11:51'
labels:
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
priority: medium
type: bug
ordinal: 445000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`runRecoveryWalkOnce` can replay a successor's seal of this mount's live epoch (or records above it) and install a Ready lane without a latch. Path (i) then hits the deposed-lane gate without a latch; path (ii) writes one object past the seal and fails a round later in `checkRefCkptInvariants`. Fix exists as commit 1556359b7ac on branch `fix/antalya-26.6/cas-ref-lane-recovery-seal-fault` (stacked on PR 10 v2): latch, report and `CORRUPTED_DATA` when the final seal's epoch is >= live or the last applied epoch is > live, scoped to writing mounts (`live_epoch != 0`). Still owed: a test for the "records above the live epoch without a seal" condition alone, three comment fixes, a wider title. Opens after PR 10 v2 merges.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The two paths latch and request the remount before any error
- [ ] #2 A read-only mount is never faulted
- [ ] #3 Each condition has its own red-first test
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
2026-10-12 noclock audit (docs/superpowers/cas/2026-10-12-late-request-audit/objects/reflog.md R10–R12): recovery reading a successor's history is harmless by itself (R10; codex agrees, U12 dropped). The harmful branch is R11: a mount whose own epoch is already sealed by a higher mount derives `(E,k+1)` above its seal; `checkRefCkptInvariants` refuses, the table wedges with a false `CORRUPTED_DATA`. Writer gate: reject when `last_epoch_seal.writer_epoch >= live_epoch` (~15 lines). Trigger (a) is the lower-epoch reclaim (CAS-178).
<!-- SECTION:NOTES:END -->
