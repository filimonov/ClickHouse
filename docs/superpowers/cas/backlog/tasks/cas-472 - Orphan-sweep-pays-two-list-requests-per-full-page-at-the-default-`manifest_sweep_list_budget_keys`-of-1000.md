---
id: CAS-472
title: >-
  Orphan sweep pays two list requests per full page at the default
  `manifest_sweep_list_budget_keys` of 1000
status: To Do
assignee: []
created_date: '2026-11-13 01:20'
labels:
  - 'area:gc'
  - 'origin:review'
dependencies: []
priority: low
ordinal: 577000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The orphan sweep passes the user setting manifest_sweep_list_budget_keys (default 1000) as the page size of one list. A full page of 1000 keys plus its sentinel does not fit one store response of 1000 keys, so it costs two requests; every other CAS list walk uses CasOperation::kCasListPageKeys (999) since PR https://github.com/Altinity/ClickHouse/pull/2831 (wave 1, story G). Options: default 999; or keep the budget and cap the page at 999 inside the sweep. Needs a decision because it changes a user setting's default or its meaning. Found by the Task 8 review of wave 1 (tmp/prbundle/wave1/review/task8-review.md).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
