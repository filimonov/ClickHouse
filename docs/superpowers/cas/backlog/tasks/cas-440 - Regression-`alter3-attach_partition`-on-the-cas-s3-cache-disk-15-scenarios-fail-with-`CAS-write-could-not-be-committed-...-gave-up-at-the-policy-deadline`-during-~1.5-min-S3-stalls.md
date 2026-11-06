---
id: CAS-440
title: >-
  Regression `alter3/attach_partition` on the cas-s3-cache disk: 15 scenarios
  fail with `CAS write could not be committed ... gave up at the policy
  deadline` during ~1.5 min S3 stalls
status: To Do
assignee: []
created_date: '2026-11-06 22:01'
labels:
  - cas
  - ci
  - regression
dependencies: []
priority: medium
type: bug
ordinal: 543000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2577 @74562b9f124, job `RegressionTestsRelease / CASS3CacheAlter (attach, 3)`: 15 of 3427 scenarios failed over 2 h 23 m, each after 1 m 22 s - 1 m 48 s of waiting, with `Code: 210 ... CAS write could not be committed (read of 'data/cas/ref_catalog': gave up at the policy deadline after 13 attempt(s)); retrying later`, `CAS _ckpt for namespace ... gave up at the policy deadline after zero attempt(s)` and `CAS ref-log txn 1-9 ... is durable but was not acknowledged (publishing its checkpoint frontier was not confirmed ...)`; one mutation failed with the same cause (Code 341). The same job was green on the previous run of the PR (02a2511744c) and on PR 2805 and PR 2812 the same day; the PR changes only error texts. Report: https://altinity-build-artifacts.s3.amazonaws.com/REFs/2577/merge/74562b9f1244f2b7b585c255106316f4ddabb2d6/regression/x86_64/with_analyzer/zookeeper/without_thread_fuzzer/alter3/attach_partition/cas-s3-cache/nice-new-fails.log.txt
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Find what stalls the object store of the regression environment for about 90 s (MinIO logs, host metrics) or name the CAS request that burns the whole policy budget
- [ ] #2 Record how often the job fails this way on the 26.6 and 26.8 lanes
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
