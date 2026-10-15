---
id: CAS-398
title: >-
  Decide on adaptive first-attempt timeouts for control-plane single-attempt
  clones
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
labels:
  - 'area:backend'
  - 'area:upstream'
  - 'needs:decision'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
dependencies: []
priority: low
ordinal: 496000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Control-plane single-attempt clones still run under short adaptive first-attempt timeouts, the same class #2555 fixes for blobs. Two overlapping knobs result: the `SingleAttempt` retry profile and the new creation flag, with different verb scope and a second clone when both are set. Either disable adaptive timeouts for every clone (drops `configured_timeouts`, the clone-key tuple and the `hasSingleAttemptClientForTest` edit) or document why control-plane keeps them.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded; code or docs match it
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
