---
id: CAS-428
title: Bound the per-name retirement records of the CAS ref ledger
status: Done
assignee: []
created_date: '2026-11-04 02:01'
updated_date: '2026-11-17 20:01'
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-12 sizing, read on PR 2570 head `dca5e45ce32` (no stand run; the defaults study measured `41d4a78eeb9`, which lacks PR 2570).
- A namespace name holds the table UUID (`srv1/<table_uuid>`), so every `CREATE TABLE` makes a new name. Record count = tables dropped since the last mount, not distinct SQL names.
- Before PR 2570 the slot is erased on invalidation and on eviction (`ref_name_slots.erase` in `invalidateRemovedCatalogLife` and `enforceRefTableCacheBudget` on `d8cbdfae3ec`). The growth is new with PR 2570.
- The walk is on the cold-admission path: `ensureRefTableRecovered` ends with `enforceRefTableCacheBudget`, which walks all of `ref_name_slots` under `ref_queue_mutex` once per newly materialized table (twice when over budget). `reconcileCatalogCut` and `wedgedRefLaneCount` walk it too.
- Standalone model, `std::map<std::string, {shared_ptr, uint64_t}>` with UUID-shaped keys, clang 21 -O2 (`master/tmp/cas428/walk_bench.cpp`): one walk takes 0.16 ms at 10k records, 2.3 ms at 100k, 104 ms at 1M, 574 ms at 5M. About 150 bytes per record.
- Churn rate measured by the defaults study: 9.4 `CREATE`/`DROP` cycles/s (`ddl4`, 16926 cycles in 30 min). At that rate: 100k records in 3 h, 1M in 30 h. At 1M each `CREATE` holds `ref_queue_mutex` for ~0.1 s.
- CI stateless runs stay under ~10k records, so CI does not show it.
- Sketch: count in-flight cold admissions in the slot; bump `retirements` only on an existing slot (`find`, not `operator[]`); erase the slot when `current` is null and no cold admission is in flight. An admission that starts after the retirement reads the catalog after it, which is the case the current code already trusts (`retirements_seen` equals the current count).

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit 65c269418a6 "Keep a retirement record only while a cold admission is in flight" covers: #1 a retired name slot is erased unless a cold admission holds it (`ColdAdmission` guard, capture to publish), while the retirement count still refuses a republish of the retired life. It builds on the port of PR 2570 (27333a84b66, 31a9bd92e3d, 4cbee14c7a0). Two defects found in review of that port were fixed in the same series: 06cfc31c64e "Detach a resident removed life whose catalog row is gone" and 382444fc36b "Detach a namespace life again after its catalog erase". Still open: no criterion. Cost noted: `acquireMutableRefTableRuntime` takes `ref_queue_mutex` twice per call when the runtime is resident (one map lookup each). No stand run of the record count. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Delivered in PR 2831 by `65c269418a6` on top of the port of PR 2570. Cost: `acquireMutableRefTableRuntime` takes `ref_queue_mutex` twice on a resident runtime (CAS-473 measures it). No stand run of the record count.
<!-- SECTION:NOTES:END -->
