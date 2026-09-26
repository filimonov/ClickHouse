---
id: CAS-135
title: Attribute S3 requests made on GC worker pools to the phase that scheduled them
status: To Do
assignee: []
created_date: '2026-09-26 07:39'
updated_date: '2026-09-26 07:39'
labels:
  - 'area:observability'
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-7
dependencies:
  - CAS-29.2
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcPhaseTimer.h
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcReadAhead.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMetaWriter.cpp
priority: medium
type: enhancement
ordinal: 177000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`GcPhaseTimer` diffs the round thread's `ProfileEvents` snapshot
(`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcPhaseTimer.h:42-65`). Requests made by a read-ahead
worker or a `meta_pool` job land on that worker's counters. So the S3 verb counts on `fold_ref_intake` and `fold_reduce`
under-count by exactly the hinted requests, and `meta_pool_wait` has always had an empty `ProfileEvents` column (documented at `:22`).
Semantic phase metrics and `CASGCReadAheadHit`/`Miss`/`Wasted` are correct because they are counted at the take site.
The fix belongs at the worker boundary. Stage A3 replaces the per-`Gc` pools with round-scoped ones, so do it on that shape.

Provenance: BACKLOG/gc.md#gc-phase-rows-lose-worker-requests. Verified 2026-09-26 against d4be7f7045a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest with `gc_read_concurrency > 1` sees the same `S3GetObject` count on `fold_ref_intake` as with concurrency 1
- [ ] #2 `meta_pool_wait` rows carry the verb counts of the jobs they waited for, and the column description no longer calls it empty
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
