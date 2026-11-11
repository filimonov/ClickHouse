---
id: CAS-447
title: >-
  GC ref cleanup deletes nothing: ledger _log objects accumulate per live
  namespace for the whole stateless run
status: To Do
assignee: []
created_date: '2026-11-10 23:26'
updated_date: '2026-11-11 05:42'
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-11 CAUSE CONFIRMED (local 2026-11-09 lane run, antalya-26.8-pr-2159/ci/tmp/var/log/clickhouse-server/clickhouse-server.log, root-owned, read via a read-only docker mount): every GC round that had a cleanup plan logged 'CAS GC ref cleanup stopped before the chunk starting at <first _log key>: catalog observation/life moved' (46 times in the 30 min run, one per round, e.g. 00:06:25.98 in round 2 that started ~00:06:23) and no chunk was ever deleted; no 'retained namespace' warnings, so the recovery triple is valid and the plan is non-empty. The gate is the per-chunk 'authorityHolds' revalidation in Gc::cleanupRefObjects: it re-reads cas/ref_catalog and requires its etag to equal the etag of the round's catalog cut (taken in Gc::listRefPrefix at the start of the round). The catalog is ONE object rewritten by every CAS namespace create/drop, and the stateless lane does that 2.3-3.5 times per second (PR 2794 2/2 query_log: 850-1400 CREATE ... MergeTree and 3300-4900 DROP per 30 min), while the window from the cut to the cleanup is the whole round (seconds). So the licence never holds under any concurrent DDL, the pass stops before its first chunk at DEBUG level, and CASRefCleanupObjectsDeleted stays 0. Snapshots are not the problem: the snapshot_log_count_threshold 256 trigger fires (the leftover data shows 3-6 _snap per long-lived life and ~276 logs per snapshot). Fix direction: the whole-catalog etag is a stricter witness than the licence needs; the per-namespace entry (ns, incarnation, state) plus the GC lease (owner, seq) already identify the life whose write-once _log/_snap keys are being deleted, and both are checked in the same block. Keep the entry/life/lease checks, drop the catalog-etag equality (or compare only this entry), and add a gtest under CAS* where a namespace is created between the cut and the cleanup and the cleanup still deletes the covered logs of an untouched namespace. Review the lifecycle argument first: a dropped-and-recreated namespace changes the entry's incarnation, which the entry comparison catches without the etag.

2026-11-11 CORRECTION of the scope: dropped test tables ARE removed together with their ref log. The dead-life janitor (Gc::runNamespaceJanitor, 20 s budget per round, cursor in gc/maintenance_state) walks cas/ns/ every round and deletes keys of lives absent from the catalog: the local 2026-11-09 lane run executed ~2400 tests (thousands of CREATE ... MergeTree) and left 110 lives in cas/ns/stream/. The _log objects that accumulate belong to the LONG-LIVED namespaces: the system log tables, which sit on the cas_s3 default policy on purpose (tests/config/config.d/cas_s3_storage_policy_for_merge_tree_by_default.xml, merge_tree.storage_policy = cas_s3; the comment there says the CA write path must handle the system-log workload). Locally the six biggest lives held 971-1656 _log each after 30 min; in CI the whole cas/ns/stream/ prefix grew from ~2k to ~4-7k keys over 3 h (CASRefGlobalListPages 2.1 -> 4.0 pages per round), so the growth is real but bounded by the system-log flush rate, not 180k objects as the description says. The defect is still that current-life cleanup never runs (catalog-etag gate), which matters most for long-lived production tables: their _log count grows until a restart, every recovery lists all of it, and every GC round enumerates it.

LINEAGE 2026-11-11: the gate is not new. 53e2a135980 (2026-08-02, 'Revalidate CAS ref cleanup deletes') replaced the plain per-key HEAD+deleteExact with a per-key re-read of cas/ref_catalog + gc/state and added the whole-catalog token equality (then 'token', renamed 'etag' in 55a967b433d 2026-09-03); fe0b909f011 (2026-09-04) moved the check from per-key to per-chunk (authorityHolds) and batched the delete, keeping the equality. Reached antalya-26.6 in a840c03b425 (2026-08-05, PR 2159 'CAS subsystem: GC layer') and antalya-26.8 via the 2159 cherry-pick c770be91e83. Before 53e2a135980 cleanup deleted without any catalog check. The same whole-catalog-stillness class was removed from the ref writer in 684161dcc03 (2026-08-05, 01069_database_memory 193/194 retries) but the GC site was left. Already tracked: 2031-triage CAS-079 (e811b3d68f4, 2026-08-21), CAS-29.7 (measure refusal reasons) and CAS-29.8 (per-life licence, spec B3). CAS-447 is the CI confirmation of CAS-29.7's hypothesis: PR 2794 shard 2/2 and the local lane show 0 deletes over a full run with 'catalog observation/life moved' as the only stop reason. Treat CAS-447 as a duplicate of CAS-29.8 for the fix; keep it for the CI evidence.
<!-- SECTION:NOTES:END -->
