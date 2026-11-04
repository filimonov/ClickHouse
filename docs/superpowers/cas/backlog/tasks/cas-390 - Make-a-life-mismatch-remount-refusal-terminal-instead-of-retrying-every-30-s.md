---
id: CAS-390
title: Make a life-mismatch remount refusal terminal instead of retrying every 30 s
status: In Progress
assignee: []
created_date: '2026-10-14 17:03'
updated_date: '2026-11-04 00:09'
labels:
  - 'area:mounts'
dependencies: []
priority: low
ordinal: 488000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The remount loop retries a CORRUPTED_DATA owner-life refusal forever, allocating an epoch and writing the slot each time. Fail-closed but unbounded; the refusal should end the loop.
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
2026-11-04: delivered by PR B https://github.com/Altinity/ClickHouse/pull/2567 commit bffad2662df: the owner read before the claim makes a foreign, retired or rewritten owner terminal before any epoch allocation; tests CASMount.RemountStopsForGood*, CASPoolRemount.RemountLoopEndsOverARetiredOwner. Done when #2567 merges.
<!-- SECTION:NOTES:END -->
