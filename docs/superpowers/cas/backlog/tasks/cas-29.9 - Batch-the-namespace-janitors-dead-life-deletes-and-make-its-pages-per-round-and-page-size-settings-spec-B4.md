---
id: CAS-29.9
title: >-
  Batch the namespace janitor's dead-life deletes and make its pages per round
  and page size settings (spec B4)
status: To Do
assignee: []
created_date: '2026-09-26 07:17'
updated_date: '2026-09-26 08:26'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'confidence:solid'
  - 'origin:soak'
milestone: m-1
dependencies: []
references:
  - CA/Gc/CasGc.cpp
  - CA/Gc/CasNamespaceJanitor.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2298'
documentation:
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#b4-janitor
  - >-
    docs/superpowers/specs/2026-09-15-cas-gc-dead-namespace-debris-cleanup-design.md
parent_task_id: CAS-29
priority: high
type: enhancement
ordinal: 115000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The janitor runs one page of a hard-coded 1,000 keys per round (`CA/Gc/CasGc.cpp:363`, `Gc::runNamespaceJanitorPage`) and
deletes each dead-life key by HEAD + exact remove (`CA/Gc/CasNamespaceJanitor.cpp:111,131`); the batch path is unused.
A full page costs 1.3-7.4 s (~7 ms per key); the limiter is request count. Under churn it cannot keep up: msan rig round
24 s to 597 s in 3.4 h, `CASGCPendingReclaim` 81 to 35,351, 93-95% of listed keys dead-life debris; parallel stateless lane
`defer_decision` 673 ms to 19.7 s in 40 min with 1,404 namespaces listed for 40 live tables.
Changes: (1) dead-life `_log`/`_snap` keys are write-once, so delete them through `removeChunkWriteOnceOrOneByOne`
(`CasGc.cpp:382`), a page becoming 1-2 batch requests; (2) `gc_janitor_pages_per_round` (default 1) and `gc_janitor_page_keys`
(default 1000) become settings. The phase keeps its position: moving it before `defer_decision` breaks the invariant
(`suppress_destructive` comes from the fold verdict). Taking more pages while debris dominates is bounded by the stage C deadline.

Provenance: BACKLOG/gc.md#janitor-page-hardcoded (formerly [gc-backlog-runaway]/2031-triage CAS-034; asks 1-2 and the revised order) and #gc-namespace-janitor-one-page-per-round (item 1). u01's pointer bullet merges here. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792 (1000 hard-coded on both).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A dead life with P write-once keys drains in ceil(P/1000) delete requests
- [ ] #2 The pages-per-round and page-size settings are honoured and documented
- [ ] #3 An A/B on the msan rig with pages=10 shows `CASGCPendingReclaim` and round duration no longer growing with the run
- [ ] #4 Exact-token deletes remain for every non-write-once key the janitor removes
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
