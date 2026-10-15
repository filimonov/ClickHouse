---
id: CAS-399
title: >-
  Make copyObjectToAnotherObjectStorage and non-S3 storages honour or reject the
  single-attempt flag
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
labels:
  - 'area:backend'
  - 'area:upstream'
  - 'touches:upstream-code'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
dependencies: []
priority: low
ordinal: 497000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`copyObjectToAnotherObjectStorage` ignores the creation flag and the unexpected-write retry override. Non-S3 object storages ignore the flag silently, and the mount check covers only the retry profile. This is a trap for future callers. Honour the flag or fail loudly with `BAD_ARGUMENTS` or `NOT_IMPLEMENTED`.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A caller passing the flag to an unsupported path gets an error, with a test
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
