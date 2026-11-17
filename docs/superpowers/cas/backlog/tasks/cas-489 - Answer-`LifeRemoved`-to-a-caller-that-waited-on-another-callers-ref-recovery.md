---
id: CAS-489
title: Answer `LifeRemoved` to a caller that waited on another caller's ref recovery
status: Done
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-17 20:02'
labels: []
dependencies:
  - CAS-487
ordinal: 594000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A caller parked on another caller's recovery of a removed life throws retry-later, where the probe contract says it never throws. The waiter cannot read `catalog_life_invalidated`: `Gc::drainCompletedRemoving` sets it in `before_erase`, while the row is still `Removing` and the erase may not follow. Fix sketch: the recoverer records its own "ended removed" outcome in a separate field, and the waiter reads that. Follow-up of CAS-487.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-15: implemented in the CAS-487 branch (work/antalya-26.8/cas-reflog-one-request-clean, commit 14 of 14). Any caller that reaches a ref runtime whose recovery found the life erased answers LifeRemoved; no recovery starts on such a runtime. Close this task when CAS-487 merges.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Delivered in PR 2831 by `97123265e03`.
<!-- SECTION:NOTES:END -->
