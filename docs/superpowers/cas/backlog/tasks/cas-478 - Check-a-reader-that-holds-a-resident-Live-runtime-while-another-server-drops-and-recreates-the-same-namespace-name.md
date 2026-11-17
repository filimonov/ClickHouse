---
id: CAS-478
title: >-
  Check a reader that holds a resident Live runtime while another server drops
  and recreates the same namespace name
status: To Do
assignee: []
created_date: '2026-11-13 03:02'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
dependencies: []
priority: medium
ordinal: 583000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The hot reader path does no catalog read. If a namespace name can be dropped and reborn on another server while this server keeps a resident runtime of the old life (ref_table_cache_bytes = 0 means no eviction), the reader keeps answering from the old life until a remount. Decide by the test_cas_replicated_relink flows whether a table UUID can be reborn on another server; if yes, pin the behaviour with an integration test and document it. Source: whole-branch review of wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831, tmp/prbundle/wave1/final/umbrella-aggregate.md (V-08).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
