---
id: CAS-441
title: >-
  Regression `lightweight_delete3` on the cas disk: acceptance delete query
  takes 5.9 s against the 2 s limit
status: To Do
assignee: []
created_date: '2026-11-06 22:01'
labels:
  - cas
  - ci
  - regression
dependencies: []
priority: low
type: bug
ordinal: 544000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2577 @74562b9f124, job `RegressionTestsRelease / CASLightweightDelete (3)`, scenario `/lightweight delete/acceptance/delete_query_0/acceptance delete execution time` (RQ.SRS-023 ... DeleteQueryExecutionTime): the first reference DELETE on the acceptance table took 5 s 873 ms, the scenario requires under 2 s. Green on the previous run of the PR (02a2511744c) and on PR 2805 / PR 2812 the same day. Report: https://altinity-build-artifacts.s3.amazonaws.com/REFs/2577/merge/74562b9f1244f2b7b585c255106316f4ddabb2d6/regression/x86_64/with_analyzer/zookeeper/without_thread_fuzzer/lightweight_delete3/cas/nice-new-fails.log.txt
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Measure the DELETE latency distribution on a cas disk in the regression environment and decide whether the 2 s acceptance limit holds for CAS or the scenario needs a CAS-specific budget
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
