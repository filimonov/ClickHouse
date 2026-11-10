---
id: CAS-447
title: >-
  GC ref cleanup deletes nothing: ledger _log objects accumulate per live
  namespace for the whole stateless run
status: To Do
assignee: []
created_date: '2026-11-10 23:26'
labels:
  - 'area:gc'
  - 'area:ref-ledger'
  - 'origin:ci'
dependencies: []
priority: high
ordinal: 550000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the cas-s3 stateless lanes `CASRefCleanupObjectsDeleted` stays 0 for the whole run (PR 2794 shard 2/2 metric_log, 3 h, ~180k `CASRefBatchFlushes` = ~180k published `cas/ns/stream/<life>/_log/` objects, ~90 GC rounds per 20 min with `cas_gc_interval_sec` 20). The leftover RustFS data dir of the 2026-11-09 local run (antalya-26.8-pr-2159/ci/tmp/rustfs_data) holds up to 1656 `_log` objects under one live namespace. `Gc::cleanupRefObjects` only deletes logs below a checkpoint that names a `checkpoint_snapshot_id` (recovery triple), and snapshots are published ~50 times per 20 min (`CASRefSnapshotPublishDispatched`) across 100+ live namespaces, so most namespaces never get a cleanup licence while the run lasts. Consequence (CAS-432 RCA 2026-11-11): every ledger open/recovery lists a growing `_log` prefix and the GC enumerates a growing `cas/ns/stream/`; RustFS LIST costs ~0.15 ms per returned key and the LIST work starves HEAD/GET for the whole server (bench2: HEAD p50 5.6 -> 13.3 ms at 14.5 LIST/s), which is the in-run slowdown of the ASan cas-s3 lane. Unknown: whether snapshot publication is rate-limited by design (the publish backoff counters are 0) or the condition for publishing a snapshot is never met for short-lived test tables; whether a long-lived production table reaches the same state (its `_log` count would grow until a snapshot lands).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The number of `_log` objects per live namespace is bounded during a 3 h cas-s3 stateless run (measure with `CASRefCleanupObjectsDeleted` > 0 and a listing of `cas/ns/stream/<life>/_log/` at the end)
- [ ] #2 Why cleanup never ran is written in the task notes with the counter or code path that gated it
- [ ] #3 A gtest under the `CAS*` filter covers the gating condition
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
