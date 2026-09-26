---
id: CAS-29.11
title: >-
  Remove the graduation, redelete and ref-cleanup round budgets, keeping the old
  keys as deprecated no-ops for one release (spec C2)
status: To Do
assignee: []
created_date: '2026-09-26 07:17'
updated_date: '2026-09-26 08:26'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
milestone: m-1
dependencies:
  - CAS-29.10
references:
  - CA/ContentAddressedSettings.cpp
  - CA/Pool/CasPool.h
documentation:
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#c2-budgets-removed
  - docs/en/antalya/cas/configuration.md
parent_task_id: CAS-29
priority: high
type: enhancement
ordinal: 117000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Once the deadline exists, `gc_round_graduation_budget`, `gc_round_redelete_budget` and `gc_round_ref_cleanup_budget`
(server defaults 5000, `CA/ContentAddressedSettings.cpp:65-69`) only reintroduce the feedback loop. The ref-cleanup cap is the
cheapest family's cap: 5,000 write-once keys are 5 batch requests, while the LIST they leave costs ~4,600 requests per round.
The `PoolConfig` struct defaults are 0 with comments saying "Default UNBOUNDED" (`CA/Pool/CasPool.h:97-120`), which contradicts
the server default; the removal also removes that trap.
Kept: sweep namespace and recovery-op budgets, prefix-wholesale and hand-off budgets (cursor-paced or one-shot),
`rebuild_edge_budget` (memory), `gc_round_outcome_entry_budget` (audit size). Old keys are accepted for one release as
deprecated no-ops with a Warning at load (AGENTS.md invariant 7), plus a docs migration note.
Round-report counters tallied from in-memory decisions (2031-triage CAS-101) ship in the same change; that item is sourced by u03.

Provenance: BACKLOG/gc.md#gc-round-budgets-not-backpressure class A; audit F14 and conclusion 3. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792 (defaults 5000 on both).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A config with any removed key loads with one Warning and the key has no effect
- [ ] #2 `objects_deleted + objects_absent + objects_replaced == entries_redeleted` past the old 5000 cap
- [ ] #3 `configuration.md` lists the removed keys with a migration note
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
