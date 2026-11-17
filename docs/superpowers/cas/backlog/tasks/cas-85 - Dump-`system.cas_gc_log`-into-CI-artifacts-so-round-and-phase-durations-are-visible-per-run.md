---
id: CAS-85
title: >-
  Dump `system.cas_gc_log` into CI artifacts so round and phase durations are
  visible per run
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:ci'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:soak'
  - 'theme:parity'
  - 'theme:ci-stabilization'
milestone: m-7
dependencies: []
references:
  - ci/jobs/scripts/clickhouse_proc.py
  - 'https://github.com/Altinity/ClickHouse/issues/2298'
priority: medium
type: chore
ordinal: 120000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The GC backlog runaway on the msan/tsan CAS lanes (#2298) was diagnosed from a local rig because CI keeps no GC history:
`dump_system_tables` exports `query_log`, `trace_log`, `metric_log`, `part_log` and others but not `cas_gc_log`
(`ci/jobs/scripts/clickhouse_proc.py:1260-1272`, the `TABLES` list). The dump already opens CAS disks read-only.
Add `cas_gc_log` (and consider `cas_log` within the row limit) on CAS lanes only.

Provenance: BACKLOG/gc.md#janitor-page-hardcoded ask 5. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792 (no cas_gc_log in the list on either).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A CAS stateless lane run's artifacts contain `cas_gc_log` with round and phase rows
- [ ] #2 Non-CAS lanes and the master binary's config are unaffected (unknown table skipped, not a failure)
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
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `query_log` is 2026-06-05 (2c762c70d33); the finding itself has no record before this migration, so the creation date stays 2026-09-26.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit 9fddee6d03b "Dump `cas_gc_log` and `cas_log` from CI lanes that mount a CAS disk" covers: #2 `dump_system_tables` adds the two tables only when a server config holds `<metadata_type>cas</metadata_type>`, and skips a table that `EXISTS TABLE` reports absent, so non-CAS lanes keep their list. Still open: #1 only for a run that completes and fails (the dump runs then; a green run dumps no system table). `cas_gc_log` and `cas_log` were dumped in job 131765755569 of run https://github.com/Altinity/ClickHouse/actions/runs/43379266824, and no job failed on the scrape step. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831 by `9fddee6d03b`. Open: a shard killed by the job time limit uploads no dump.
<!-- SECTION:NOTES:END -->
