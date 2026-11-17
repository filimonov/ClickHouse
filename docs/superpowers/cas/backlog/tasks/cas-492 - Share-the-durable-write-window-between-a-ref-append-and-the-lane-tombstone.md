---
id: CAS-492
title: Share the durable-write window between a ref append and the lane tombstone
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
labels: []
dependencies:
  - CAS-487
ordinal: 597000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ensureEpochOpenForAppend` re-implements the arming, wedge, conflict adjudication and install steps of `commitRefChunk`. `resolveWedgeOnce` holds a third copy of the occupant verdicts. The copies agree today. Merge them into one durable-write window, with no behaviour change. Follow-up of CAS-487.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
