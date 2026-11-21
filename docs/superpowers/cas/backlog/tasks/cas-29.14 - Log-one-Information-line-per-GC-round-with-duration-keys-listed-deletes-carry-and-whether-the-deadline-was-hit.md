---
id: CAS-29.14
title: >-
  Log one Information line per GC round with duration, keys listed, deletes,
  carry and whether the deadline was hit
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:17'
labels:
  - 'area:observability'
  - 'area:gc'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - 'impact:operability'
  - groomed-20261121
  - grooming-20261121-changed
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
milestone: m-1
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.cpp
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f28
  - docs/en/antalya/cas/operations/monitoring.md
parent_task_id: CAS-29
priority: high
type: enhancement
ordinal: 175000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
At the default `Information` level a CAS node logs no GC round at all: the scheduler logs a round only when teardown
stops it or another leader blocks it, and the fold has two `LOG_INFO` sites for rare paths. Of 73 log sites in `Gc/`
and `Pool/`, 12 are Info. Everything the otel.demo audit found came from `cas_gc_log`, `cas_log`, `metric_log` and
`trace_log`, none from the text log (audit F28). The audit calls one Info line per round the cheapest observability win.
Fields: round, outcome, duration, keys listed, deleted, carried, `deadline_hit` (the last two exist once
CAS-29.10 lands; log what exists and add them with it).

Provenance: BACKLOG/gc.md#otel-demo-s3-budget-audit-2026-09-25 (F28, one of the nine findings not threaded elsewhere). Parented on the epic per the u01 cross-unit note (spec C5 observability). Verified 2026-09-26 against d4be7f7045a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every finished or aborted round emits exactly one `LOG_INFO` line from the scheduler with the fields above
- [ ] #2 A gtest or stateless test captures the line for a `GC RUN` and checks its round number matches `cas_gc_log`
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
First recorded: 2026-09-26 (83949645ce7, by 'audit F28')

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit 9614529f050 "Log one `Information` line per finished GC round" covers: #1 for rounds ending `Success`, `Aborted`, `Error` and `Stopped`, one line from `runRoundLogged` with `round`, `outcome`, `trigger`, `duration_ms`, `deleted`, `condemned`, `error_code`, `keys_listed`; #2 `CASGCLog.FinishLogLineMatchesCasGcLogRow` and `CASGCLog.AnAbortedRoundLogsExactlyOneFinishLine` match the line to the Finish row. Still open: #1 for `Deferred` and `NotALeader` rounds (no line), and the fields `carried` and `deadline_hit` (the task says to add them with CAS-29.10). Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831 by `9614529f050`. Fields `carried` and `deadline_hit` wait for CAS-29.10.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:09
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: Gc/CasGcScheduler.cpp:308 emits the finish LOG_INFO (9614529f050, merged as 0aa837e942e); the line has no carried or deadline_hit fields; task note says Deferred and NotALeader rounds still log nothing. Changes: impact:operability. Links: Remaining after PR 2831: one line for Deferred and NotALeader rounds (AC#1), fields carried and deadline_hit once CAS-29.10 lands..
---
<!-- COMMENTS:END -->
