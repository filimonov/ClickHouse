---
id: CAS-41
title: Rewrite the three ack-floor soak cards for the current floor and run them
status: To Do
assignee: []
created_date: '2026-09-26 07:03'
labels:
  - 'area:soak'
  - 'area:testing'
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:soak'
milestone: m-0
dependencies: []
references:
  - utils/ca-soak/scenarios/BACKLOG.md
  - utils/ca-soak/scenarios/RUN_HISTORY.md
  - CA/Formats/CasServerRootFormats.h
priority: medium
type: task
ordinal: 51000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Three proposed cards (SIGSTOP a writer holds the floor then releases it; hard-KILL mid-burst, fence-out, fsck clean;
O(delta)+O(servers) request-count guard) are still only proposals in `utils/ca-soak/scenarios/BACKLOG.md:674-705`, listed
there as release-gate items. They reference removed symbols (`observed_gc_round`, `CasGcFloorHeldByStaleAck`); the floor is
now `min_active_build_sequence` plus `gc_fenced` in the mount lease (`CA/Formats/CasServerRootFormats.h:20-57`).
Unit- and TLA-covered, never soak-validated on either branch (`RUN_HISTORY.md` has no entry).

Provenance: BACKLOG/gc.md [ack-floor soak validation]; verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The three cards exist as runnable scenarios against the current floor fields and events
- [ ] #2 Each card has one recorded run in `utils/ca-soak/scenarios/RUN_HISTORY.md` with its verdict
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
