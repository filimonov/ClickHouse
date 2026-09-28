---
id: CAS-223
title: >-
  Add a sanitizer-only thread-pool profile for the CAS stateless stand so MSan
  and TSan shards fit the 6-hour budget
status: To Do
assignee:
  - '@filimonov'
created_date: '2026-09-16'
updated_date: '2026-09-28 12:06'
labels:
  - 'area:ci'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'origin:issue'
  - 'confidence:contested'
milestone: m-8
dependencies: []
references:
  - tests/config/install.sh
  - 'https://github.com/Altinity/ClickHouse/issues/2298'
documentation:
  - docs/superpowers/cas/2026-09-16-msan-cas-s3-shard-budget-rca.md
priority: high
type: task
ordinal: 281000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
MSan CAS runs 0.5k tests/h against 2.0k/h on the plain S3 lane (TSan <0.9k/h vs 2.0k/h), so those shards never fit the job budget (#2298, OPEN).
The thread census points at pools: ~2400 threads at 30 inline disks (BgSchPool 508, IOWriter 165, CAS per-disk GC pools 523).
`tests/config/install.sh` has an `is_sanitizer_build` branch (`:181-183`) and a `--cas-s3-storage` flag (`:33`) but no profile for both.
Proposed keys: `threadpool_writer_pool_size` 500 to 64-100, `background_schedule_pool_size` 512 to 128-256 (not below 128: renewal and cleanup run there),
`cas_gc_read_concurrency`/`cas_gc_meta_pool_size` 16 to 4 on the default CAS disk. Inline disks do not inherit disk settings; `SYSTEM CAS FORGET` stays their lever.
`max_thread_pool_free_size` is not a lever (tried, no change). Idle-thread retirement in the shared `ThreadPool` was rejected: wide blast radius; per-disk pools go away with round-scoped GC pools instead.
The same profile is candidate (c) for the ASan RSS ceiling (threads times fake stacks).

Provenance: BACKLOG/testing-and-ci.md#sanitizer-cas-thread-pool-profile, plus candidate (c) of #asan-memory-tracker-snap; verified 2026-09-26 against 8b87aa15d21. Related: CAS-29.2, CAS-85.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A `config.d` profile is linked only when the build is a sanitizer build and CAS-s3 storage is enabled, each key with a one-line reason
- [ ] #2 A `system.stack_trace` census per thread group before and after shows the reduction
- [ ] #3 MSan and TSan CAS shards finish inside the job budget, or the remaining gap is measured and resharding is decided
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
First recorded: 2026-09-16 (cbf3fe14e36, by 'asan-memory-tracker-snap')

Issue #2298 (assigned filimonov): the ASan half is resolved on antalya-26.6 (test-policy `cas_gc_interval_sec` 5 → 20 in `38661e72127` plus the GC round-cost work; ASan CAS-S3 shards finish in 3.7 h and 2.9 h on the v26.6.4 tag). MSan and TSan shards are what remains. PR #2393 (merged, #2362 workaround) also speeds the sanitizer lanes.

2026-09-28 (owner review): the thread-pool-profile proposal above is rejected as knob-turning; the standard is CAS as fast as the plain S3 disk on the same workload, with the loss located by metrics, traces and a synthetic repro. Systematic-debugging investigation started on PR #2440 run https://github.com/Altinity/ClickHouse/actions/runs/36347426173 (tsan 's3 storage' 1/2 finished in 2h05 vs 'cas s3 storage' 1/2 cancelled at the 6h timeout with 5,618 of 5,636 tests done; msan cas s3 1/3-3/3 cancelled at 6h). Phase 1 (tmp/cas223/phase1_ci_evidence.md): the loss is broad (top-1000 tests hold 67% of the extra time, median per-test 1.83x, p90 6.9x), confined to MergeTree tests (3.19x, ramping 2.0 -> 5.7 by elapsed hour; tests without tables 1.19x flat), and present only under TSan/MSan: the ASan CAS lane has the same threads and pools and runs 2.7-3.4k tests/h, as fast as TSan S3, which refutes the thread-count explanation. A constant 2.5x exists in release between CAS over RustFS and CAS over local objects (part-removal repoint ~140 ms release, ~570 ms TSan). CI confounds: CAS lane on RustFS without cache vs S3 lane on MinIO with a 5 GiB filesystem cache; TSan S3 lane runs with random query settings off. Phase 2 (tmp/cas223/phase2_hypotheses.md): H1 = O(pool catalog) CPU per CAS op: CasRefCatalog::read (GET + full decode + CatalogLifeIndex) on every part-commit append (CasRefLedger.cpp:3394/3425), twice per readable-runtime acquire and in listings (CasPool.cpp:1884/1912); Removing rows are erased only after a GC fold adopts cleanup evidence, so catalog = live tables + GC backlog, and a sanitizer-slowed GC grows it (feedback loop, predicts MSan > TSan > ASan). Overlaps CAS-176.1 (drop the per-flush catalog GET) and CAS-72. Next: phase 3 repro on the exact CI binaries and mining the completed-but-slow sanitizer runs' system-table dumps (query_log, metric_log, trace_log, cas_log).
<!-- SECTION:NOTES:END -->
