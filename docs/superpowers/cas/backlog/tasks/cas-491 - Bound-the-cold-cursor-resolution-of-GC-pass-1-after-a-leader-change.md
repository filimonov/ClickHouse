---
id: CAS-491
title: Bound the cold cursor resolution of GC pass 1 after a leader change
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
labels: []
dependencies:
  - CAS-487
ordinal: 596000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
GC pass 1 resolves each cold cursor with sequential `GET`s and is not cut by the intake time slice `kRefIntakeBudgetMs`. Every GC leader start is cold, so a pool with many namespaces pays this on each leader change. Needs a design choice (prefetch, or a slice for pass 1) and a measurement on many namespaces. Follow-up of CAS-487; see also CAS-29.10.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
