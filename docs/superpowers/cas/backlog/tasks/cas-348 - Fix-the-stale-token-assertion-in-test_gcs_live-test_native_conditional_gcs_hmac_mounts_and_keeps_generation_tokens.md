---
id: CAS-348
title: >-
  Fix the stale token assertion in test_gcs_live
  test_native_conditional_gcs_hmac_mounts_and_keeps_generation_tokens
status: To Do
assignee: []
created_date: '2026-10-08 06:17'
labels:
  - 'area:tests'
  - 'area:gcs'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:ci'
dependencies: []
priority: low
type: task
ordinal: 442000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The live-GCS test asserts the old `cas_log.token` shape; since 2026-09-05 the column holds `generation:<n>`. The test mounts, inserts, merges and drops correctly and fails only on its last assertion. Found on 2026-10-08 while running the acceptance of the lease series PR 6 (absence per verb); an A/B with the base classifier fails the same way, so it is pre-existing. One-line test fix.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The test passes against real GCS with ci/local.env credentials
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
