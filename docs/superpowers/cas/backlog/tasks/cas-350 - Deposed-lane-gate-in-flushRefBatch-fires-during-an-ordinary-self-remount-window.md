---
id: CAS-350
title: >-
  Deposed-lane gate in flushRefBatch fires during an ordinary self-remount
  window
status: To Do
assignee: []
created_date: '2026-10-08 16:33'
updated_date: '2026-10-09 05:44'
labels:
  - 'area:ref-ledger'
  - 'area:remount'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
dependencies: []
priority: medium
type: bug
ordinal: 444000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
During `tryRemountOnce`, step 1 (`setLiveWriterEpoch(E')`) lands before step 3 marks the runtime superseded, and step 2 can block on recovery quiescence in between. A leader's `flushRefBatch` that passed `may_mutate()` and the `superseded_by_remount` check before the fence tripped then reads `live_epoch_fn()` = E', computes next id {E', 1}, `chainLinkFor` returns nullopt, and the deposed-lane gate answers a permanent `INVALID_STATE` ("resumes only under a later epoch") with a false cause text and a `CASRefAppendSealRejected` increment — on a routine remount, where the caller should get the retry-later supersession error. No data loss, nothing is sent. Found by the PR 10 v2 re-review (B-X1); pre-existing on antalya-26.6 @5b7c9f503df. Fix sketch: read `live_epoch_fn()` into a local first, then re-check `may_mutate()` and `superseded_by_remount`, then evaluate the gate — the remount runs with the fence latched lost from before step 1 until `armMountFence` and sets the superseded flag before re-arming, so an epoch read returning E' is always followed by a refused `may_mutate()` or a set flag. Test: a quiesce-hook seam running a flush between steps 1 and 3; expect the retry-later class. Check whether the window survives the reacquire path (closeOldEpoch → claimNewEpoch) that replaces remount in the lease series; if it does, fix there too.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A flush between remount steps 1 and 3 gets the retry-later supersession error, not INVALID_STATE
- [ ] #2 CASRefAppendSealRejected does not move on a routine remount
- [ ] #3 The same window is checked on the reacquire path
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
AC #3 checked against PR 15 (fix/antalya-26.6/cas-remove-remount-machine): the window survives, narrower. `flushRefBatch` re-checks `superseded_by_reacquire` and two statements later reads `live_epoch_fn()` with no lock held. The reacquire sets the flag in `CloseOldEpoch` and publishes E' in `StartRenewer`, several store round trips later, so the leader must be preempted between those two statements for the whole sequence. The fix sketch (read the epoch, then re-check the flag) closes it: the flag store happens-before the epoch store on the renewal thread. Not fixed in PR 15.
<!-- SECTION:NOTES:END -->
