---
id: CAS-474
title: >-
  Ref ledger tidy-up: one helper for the terminal-removal predicate, shared
  constants for the lease and window defaults
status: To Do
assignee: []
created_date: '2026-11-13 03:02'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
dependencies: []
priority: low
ordinal: 579000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The predicate 'lifecycle is not Live and remove_txn_id is set' is written seven times in CasRefLedger.cpp. The defaults of the mount lease and the request window are independent literals in the settings table, PoolConfig and tests; the gtest fixtures run on a 30 s lease while the shipped default is 120 s. Default-pinning tests mirror the implementation. Pure refactor, no behaviour. Source: whole-branch review of wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831, tmp/prbundle/wave1/final/umbrella-aggregate.md (U-19, U-21, U-25).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
