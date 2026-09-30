---
id: CAS-135
title: Attribute S3 requests made on GC worker pools to the phase that scheduled them
status: To Do
assignee: []
created_date: '2026-09-03'
updated_date: '2026-09-30 08:20'
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-09-03 (db5bb3485c8, by 'gc-phase-rows-lose-worker-requests')

More evidence (2026-09-30), and a third off-thread source the description does not name: LIST.
- `listUnder` lists through `S3IteratorAsync`, whose batches run on the list-objects pool. Integration test `test_cas_gc_list_pages` (branch `cas/gc-request-cuts`, 20,000 `_log` keys): the `defer_decision` row has `CASRefGlobalListPages` = 21 and `S3ListObjects` = 0, while `system.events` moved `DiskS3ListObjects` by 67 over the round. The `Finish` row (`ProfileEventsScope` on the round thread) is 0 as well.
- ca-soak S11 at `--scale ci` on the same branch: no `Phase` or `Finish` row carries `DiskS3ListObjects` or `DiskS3HeadObject`; the before/after measurement had to take them from `system.metric_log` windows, which include foreground traffic.
- Since https://github.com/Altinity/ClickHouse/pull/2351 the HEAD read-ahead and the parallel deletes run on the round's `io_pool`, so more of a round's requests are off the round thread than when this task was written.
The header comment of `GcPhaseRecord` (`Gc/CasGc.h`) promises that `ProfileEvents['S3ListObjects']` on a `Phase` row answers which phase burns the LIST budget; today it cannot.
Fix shape to consider: run the round under its own `ThreadGroup`, attach every pool job to it (`ThreadGroupSwitcher`; `threadPoolCallbackRunner` already carries the caller's group), and diff the group's counters instead of the thread's.
<!-- SECTION:NOTES:END -->
