---
id: CAS-223
title: >-
  Add a sanitizer-only thread-pool profile for the CAS stateless stand so MSan
  and TSan shards fit the 6-hour budget
status: To Do
assignee: []
created_date: '2026-09-26 07:57'
labels:
  - 'area:ci'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'confidence:solid'
  - 'origin:issue'
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

Provenance: BACKLOG/testing-and-ci.md#sanitizer-cas-thread-pool-profile, plus candidate (c) of #asan-memory-tracker-snap; verified 2026-09-26 against 8b87aa15d21. Related: u02-gc-b:gc-a3-round-pool-and-pending-deletes-fan-out, CAS-85.
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
