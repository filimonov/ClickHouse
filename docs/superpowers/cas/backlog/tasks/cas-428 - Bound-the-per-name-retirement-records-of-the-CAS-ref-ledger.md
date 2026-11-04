---
id: CAS-428
title: Bound the per-name retirement records of the CAS ref ledger
status: To Do
assignee: []
created_date: '2026-11-04 02:01'
labels:
  - 'area:refs'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
dependencies: []
priority: low
type: enhancement
ordinal: 531000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR https://github.com/Altinity/ClickHouse/pull/2570 keeps a ref-name slot with a retirement count for every retired namespace name: the GC leader for every pool-wide retirement, and since 30f75bb5d22 every server for each name it dropped and evicted. The slots are never released and are walked under ref_queue_mutex. Found by the #2570 audit (alsugiliazova, 2026-10-23, Low 3). Direction from the audit: release a record once no cold admission for that name is in flight.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 a retired name's slot is released when no cold admission for it is in flight, without letting a republish of the retired life through
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
