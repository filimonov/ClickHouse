---
id: CAS-29
title: >-
  Make GC rounds finish in minutes and catch up at S3 speed (spec stages A, B,
  C)
status: To Do
assignee: []
created_date: '2026-09-25'
updated_date: '2026-09-30 13:23'
labels:
  - 'area:gc'
  - 'complexity:epic'
  - 'risk:medium'
  - 'touches:settings'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
milestone: m-1
dependencies: []
references:
  - CA/Gc/CasGc.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2429'
  - 'https://github.com/Altinity/ClickHouse/pull/2351'
documentation:
  - docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
priority: high
type: feature
ordinal: 35000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Epic for the plan of record in the 2026-09-25 spec. On otel.demo a round takes hours: the global ref-prefix LIST alone is
4.6M keys, ~25 min and ~1.2 GiB per round (audit F19), graduation pays a serial GET per carried row after restart (F3),
and `pending_deletes` is serial at 44 ms per blob (conclusion 4).
Stage A: parallelism with no change of decisions (A0 persist `marker_confirmed` on carry, A1 graduation gate through the
read-ahead, A2 pinned HEAD window, A3 `pending_deletes` fan-out from PR #2351, A4 manifest-body memo per round).
Stage B: discovery and cleanup O(new) (B1 per-life probe, B2 cleanup lists its own range, B3 per-row licence, B4 janitor).
Stage C: `cas_gc_round_deadline_sec` replaces the count budgets, carry resumes next round, back-to-back rounds.
Section 7 records the multi-node direction the stages must not block.

Provenance: BACKLOG/gc.md#gc-rounds-in-minutes; verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6 (not built on either). Importer: re-parent the stage tasks from other units onto this epic: u02 #gc-pending-deletes-fan-out (A3), #gc-condemn-head-read-ahead-pinned-window (A2), #gc-reduce-confirm-marker-read-ahead (A1), #covered-log-cleanup-aborts-on-catalog-etag (B3), #janitor-page-hardcoded (B4), #gc-round-budgets-not-backpressure (C), #gc-deferred-round-pays-full-list (likely MERGE into B1 below); u03 audit A0/A4 items and #gc-outcome-budget-skews-round-report-counters (C2).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every stage A, B and C item of the spec is done or explicitly deferred by the owner
- [ ] #2 On otel.demo with a 500k-blob backlog, round duration stays at the deadline and the backlog decreases linearly
- [ ] #3 After the drain, rounds take under a minute
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
First recorded: 2026-09-25 (04f0180dca6, by 'gc-rounds-in-minutes')

Merged from u19a-soak (Make GC rounds finish in minutes and catch up at S3 speed (spec stages A, B, C)): Facts to add to CAS-29 (soak scaling curve, 2026-07-05 campaign, pre-rebuild GC; re-measure before quoting as current):
A single fold round's time grew with pool size: 87 ms at 400 parts (S03), 92.6 s and 93.9 s at 10,000 tables (S05 full), and 398 s at ~100,000 tiny parts (S08 full), where one round deleted 24,392 manifests.
Consequences observed: with `gc_interval_sec=10` rounds ran back to back, and `settle_fsck` could not stabilise because each round bulk-mutated the pool. Correctness held (`dangling=0`).
The idle and small-delta case was fixed by the defer round (436714d80f0..3cba4f812f8, `shouldDeferRound` at `CA/Gc/CasGc.cpp:297`). The large-delta case is what stages B and C target.
Suggested second acceptance probe beside otel.demo: S05 (10,000 tables) and S08 (100,000 parts) at `--scale full`, with per-round duration recorded in `RUN_HISTORY.md`.

2026-09-30, where a GC round spends its time (ca-soak S11 ci, base binary, `docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/03-long-rounds-and-latency.md`), shares of `fold_ref_intake` / `fold_reduce` / `defer_decision` / `ref_object_cleanup` / `manifest_deletes` / `namespace_cleanup`:
- round every 10 s, local RustFS: 12 / 12 / 12 / 43 / 14 / 4 %;
- one round per 300 s, local RustFS: 8-10 / 5 / 57-58 / 12 / 13 / 2 %;
- one round per 300 s with 20 ms per request (dev scale): 73-74 / 14-15 / the rest 1-3 % each.
The first slice (spec `2026-09-29-cas-gc-request-cuts-design.md`: A0, A2, A4, batch half of B4, LIST measurement) was chosen by request counts from the otel.demo audit, not by time share. Outcome: A0 removed (blocked by CAS-30, CAS-29.3), A2 rejected by measurement (CAS-29.5), A4 and B4 implemented but not accepted (CAS-29.12, CAS-29.9), LIST measured (CAS-29.13). Nothing merged.
Owner's rule for every further stage: no code complexity without proven value; prove it on a scenario that reproduces a GC backlog with store latency before building.
Reports: `docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/README.md`.
<!-- SECTION:NOTES:END -->
