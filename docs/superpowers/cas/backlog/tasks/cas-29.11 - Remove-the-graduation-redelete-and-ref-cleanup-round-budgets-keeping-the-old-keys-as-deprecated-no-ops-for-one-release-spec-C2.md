---
id: CAS-29.11
title: >-
  Remove the graduation, redelete and ref-cleanup round budgets, keeping the old
  keys as deprecated no-ops for one release (spec C2)
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
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

Update 2026-11-21: Reduced form delivered by 632f1b04a7d and 9ece501b97e. AC #1 (remove keys, warn) is superseded: keys stay and default to 0. Remaining scope: one test that `objects_deleted + objects_absent + objects_replaced == entries_redeleted` past 5000 (AC #2). The dependency on CAS-29.10 no longer holds for this scope.
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `PoolConfig` is 2026-06-11 (99466809c92); the finding itself has no record before this migration, so the creation date stays 2026-09-26.

Merged from u19d-soak (Remove the graduation, redelete and ref-cleanup round budgets, keeping the old keys as deprecated no-ops for one release (spec C2)): Facts to add to CAS-29.11 (otel.demo, 26.6.4.20001.altinityantalya, 2 replicas, 94 live namespaces, reported by Boris, analysed 2026-09-24):
`defer_decision.ref_log_keys_listed` grew linearly: 8k (09-14), 58k (09-16), 296k (09-17), 1.15M (09-19), 2.16M (09-21), 4.28M (09-24). Over the same days `defer_decision` went from 3 s to 1424 s, `fold_ref_intake` from 54 s to 903 s, and round duration from 98 s to 2800 s.
`_log` deletes per day equal 5000 times rounds per day exactly (455k on 09-18 with 91 rounds, 95k on 09-24 with 19 rounds), while `_log` uploads stay at 380-475k per day.
Loop onset: 09-16, when writer epoch 0x12 began while the budget was still spent on the epoch 2/5/8 backlog (~2M keys deleted 09-14..09-18). 09-21 is only when the graduation side became visible: deletes pinned at 5000, ~520k retired-but-undeleted blobs.
Ruled out: the 09-21 `url()` access change, S3 errors (upload error rate ~0.03%), lease loss, and the 09-22/09-23 restarts, which only shifted the epoch.
Stand mitigation until this ships: set `cas_gc_round_ref_cleanup_budget`, `cas_gc_round_graduation_budget` and `cas_gc_round_redelete_budget` to 0 or ~100000 in the disk config and restart, since pool config is read at mount. Expect several long catch-up rounds before the LIST shrinks.
CAS-29.6 AC#4 asks for `objects_deleted` and `objects_pending` on the `ref_object_cleanup` phase row; the source also asks for `budget_exhausted`, which only `namespaces_planned` exists beside today (`CA/Gc/CasGc.cpp:1186`).
Source: utils/ca-soak/scenarios/BACKLOG.md:3510 (uncommitted). Related owners: CAS-29.10 (deadline), CAS-29.1 (per-life probe instead of the LIST), CAS-1.3 (`pending_reclaim` is process-local and resets on restart; the 520k from the log is the durable figure).

2026-11-11 defaults review: stand reproduction of a budget-bound backlog: at a 900 s interval the graduation budget passed exactly 5000 per round at a moderate load (`gcM_i900`: 10000 of 24204 condemned) and at a heavy one (`gcH_i900`: 5000 of 38276, 0 deleted, 109584 unreachable objects after 76 min); with the three budgets at 0 the same round graduated 15174 and took 1201 s instead of 1085 s (`gcH_i900_b0`). At the default 60 s interval and 9 INSERT/s the last round redeleted 4040, one fifth under the cap (`gcH_i60_long`). Recommendation: default the three budgets to 0 now, without waiting for CAS-29.10. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 defaults review: direct (no proxy) reproduction. 11 INSERT/s into one table, 300 s interval, default budgets (`gcD_M_i300`): rounds 17, 25, 38, 51, 129, 88, 117 s, graduation at exactly 5000 three rounds running, 152k ref keys listed, 25240 unreachable objects; three budgets 0 (`gcD_M_i300_b0`): rounds flat at about 45 s, 12226 unreachable. 2000 tables at 60 s: 74277 against 20372 unreachable (`mt1000_base`, `mt1000_b0`). At 60 s the `ref_cleanup` budget allows about 76 logs/s (11 INSERT/s here), the lowest ceiling. `cas_gc_round_outcome_entry_budget` 5000 makes `objects_deleted` stop at 5000 per round while fsck shows 4x more reclaimed: default it to 0 with the other three. Caveat: above GC capacity (62-81 INSERT/s here) budgets 0 do not help and a round ran 1129 s (`gcD_H_i60_b0`); that needs CAS-29.10. Report v2: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (f9ec0861aff); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 correction after an independent check of the report against run data (docs/superpowers/reports/2026-11-11-cas-defaults-review.check.md.txt, a49f8a8e45f): three caveats. The 1000-table pair (74277 against 20372 unreachable) is one run each and `mt1000_base` also ran the restart test before fsck; by share it is 82 % against 37 %. Above GC capacity budgets make no difference at equal load (`gcH_i900` against `gcH_i900_b0`: 109584 against 107309 unreachable) and do not bound a round either (1445 s with them in `gcD_H_i300`). No run had `cas_gc_round_outcome_entry_budget` at 0 on a busy round; that part of the proposal rests on the code.

2026-11-12, owner picked the reduced form for wave 1 of the 26.8 bundle (spec `docs/superpowers/specs/2026-11-12-cas-improvements-wave1-design.md` section 8): the five round budgets default to 0 (unbounded), the keys stay and keep working. AC #1 (remove the keys and warn) is superseded: nothing is removed, so there is nothing to warn about.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit 632f1b04a7d "Default five GC round budgets to unbounded" covers: #3 `configuration.md` lists the five budgets (graduation, redelete, ref-cleanup, hand-off, outcome entry) at default `0` with a migration note; #2 only the meaning of `0` on the ref-cleanup budget is pinned. Commit 9ece501b97e "Keep the outcome log of a GC round shard under the format cap" bounds the outcome object at 128 MiB (estimate) now that the entry budget is `0`; `pending_deletes` reports `outcome_rows_dropped`. Still open: #1 is superseded, the keys stay and work, so there is no warning; #2 has no test of its own for the `entries_redeleted` sum past 5000. First round after an upgrade on a backlogged pool runs long and delays the dead-member fence-out. No study run exists for the outcome-entry and hand-off budgets at `0`. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831 by `632f1b04a7d` and `9ece501b97e` in reduced form: keys stay and default to unbounded. Open: a test of `entries_redeleted` past 5000; AC1 is superseded.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: rewrite (high). Evidence: 632f1b04a7d (five budgets default to 0, configuration.md migration note) is an ancestor of the branch; task note 2026-11-12 supersedes AC #1 (keys stay). Changes: description: Update paragraph appended.
---
<!-- COMMENTS:END -->
