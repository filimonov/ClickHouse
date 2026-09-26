---
id: CAS-29.3
title: >-
  Persist `marker_confirmed` on a carried condemned row once the graduation gate
  confirmed it (spec A0)
status: To Do
assignee: []
created_date: '2026-09-26 07:17'
labels:
  - 'area:gc'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
milestone: m-1
dependencies: []
references:
  - CA/Gc/CasBlobInDegree.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2429'
documentation:
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#a0-persist-marker-confirmed
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f3
parent_task_id: CAS-29
priority: high
type: bug
ordinal: 109000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`settleEntry` carries a floor-passed condemned entry unchanged when `confirm_condemned_marker` succeeded but the graduation
budget is exhausted (`CA/Gc/CasBlobInDegree.cpp:493-498`, `rmr.still_retired.push_back(e)`), so the run keeps
`marker_confirmed = false` and only the in-process memo remembers the confirmation. After a restart or a leadership change
every carried row pays one synchronous `loadMeta` GET again: 200k GETs and 4 hours in round 1379 on otel.demo, 7,100 of
8,800 s of that `fold_reduce` (audit F3, conclusion 5). The `marker_confirmed` bit already exists in the run format
(`:193`, `:217`), so this is no format change. Fix: carry a copy with `marker_confirmed = true` once the gate confirmed it.
The audit orders it first among the stage A tasks. Stage C's deadline carries rows the same way, so A0 stays necessary after C.

Provenance: spec A0 / audit F3; the restart case is stated in BACKLOG/gc.md#gc-reduce-confirm-marker-read-ahead. If u03 creates an A0 task from #otel-demo-s3-budget-audit-2026-09-25, merge it into this one. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792 (identical carry line).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest: a round that confirms a marker and carries the row past the budget writes the row with `marker_confirmed = true`
- [ ] #2 After a `Gc` re-creation with N carried confirmed rows, the next round issues no meta GET for them
- [ ] #3 Graduation decisions are unchanged against the existing reduce oracle tests
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
