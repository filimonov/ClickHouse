---
id: CAS-473
title: >-
  Measure the cold-admission guard on the warm ref-append path and drop the
  outer guard of `acquireMutableRefTableRuntime` if it costs
status: To Do
assignee: []
created_date: '2026-11-13 03:02'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
dependencies: []
priority: medium
ordinal: 578000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A warm append now pays two more ref_queue_mutex holds, two more state_mutex holds, one heap string and two map lookups (ColdAdmission guard taken before namespaceLife). The outer guard can also refuse its own call when the fresh runtime is evicted between namespaceLife and the lookup (retryable). Proposed shape: no outer guard; after namespaceLife, return the resident runtime of the same life, else call namespaceLife again at most twice, then retry-later. First measure: many-writer append benchmark before and after commit B1 (ref_queue_mutex acquisitions or throughput). Change only with a measured cost. Source: whole-branch review of wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831, tmp/prbundle/wave1/final/umbrella-aggregate.md (U-09, V-03).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
