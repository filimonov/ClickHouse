---
id: CAS-402
title: Check whether stress and integration harnesses fail on server Error log lines
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
labels:
  - 'area:testing'
  - 'area:observability'
  - 'needs:repro'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
dependencies: []
priority: low
ordinal: 500000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The #2555 review found a stateless test failing on a `<Error>` log line for a retried condition. Check whether the stress and integration harnesses also fail on server `<Error>` lines. If so, any CAS Error-level log of a retried condition is a systemic CI red. Related: CAS-354, CAS-168.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each harness classified; findings recorded, with a follow-up if any fails on Error lines
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
