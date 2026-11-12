---
id: CAS-463
title: >-
  GC makes no progress while tables are created and dropped: every round aborts
  in the ref-catalog erase
status: To Do
assignee: []
created_date: '2026-11-11 19:20'
updated_date: '2026-11-12 16:55'
labels:
  - 'area:gc'
  - 'area:ref-ledger'
  - 'origin:soak'
  - 'confidence:solid'
dependencies: []
references:
  - docs/superpowers/reports/2026-11-11-cas-defaults-review.md
priority: high
type: bug
ordinal: 567000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Stand run `ddl16` (PR 2805 @41d4a78eeb9, two servers on one pool, RustFS direct, 16 threads per server looping CREATE TABLE MergeTree, INSERT, SELECT, DROP TABLE SYNC, plus 50 steady tables; 31 min; data master/tmp/cas297/runs/ddl16): 18567 create/drop cycles (about 5 per second per server), no client error. GC: the first round succeeded (9.9 s); the next 12 rounds were all `Aborted` after 76-80 s in phase `pre_fold_ref_drain` with `Code: 210 ... CAS ref catalog erase for namespace '...' gave up at the policy deadline after zero attempt(s)`. Nothing was reclaimed: fsck at the end shows 37623 unreachable objects, `janitor_pending` 9480 keys in 1580 dead lives.

Mechanism (same line as the one explained in the CAS-440 notes): `completedRemovingDelete` (`Pool/CasRefCatalog.cpp`) freezes one `Retry::standard` bound (90 s) for its whole erase/resolve loop over the dropped namespaces. Every CREATE and DROP is a compare-and-swap on the same `cas/ref_catalog` object, so the GC erase keeps losing the race; when the 90 s are spent the next erase is refused before it is sent ('zero attempt(s)') and the whole round aborts before the fold. The round then restarts from the beginning after the interval.

Why it matters: a workload that creates and drops tables steadily (temporary tables, per-job tables, CI) stops GC for as long as it lasts; garbage and catalog rows grow without bound and no setting changes it (interval and budgets are not involved; the 90 s window is a constant, CAS-405.6).

Options: route the erase through the hot-key lane (CAS-71.4); or make the erase a best-effort step that never aborts the round (erase what it can within a small budget, leave the rest for the next round, and fold anyway); or give the loop a fresh bound per namespace.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Under the `ddl16` workload GC rounds succeed and the unreachable-object count stops growing
- [ ] #2 A failed or contended catalog erase does not abort the fold of the same round
- [ ] #3 The round row shows how many erases were done and how many were left
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
2026-11-11, single-server measurement (PR 2805 @ `2f89c5db182` and the ref cleanup bundle @ `be74be9fa7b`, local object storage, 15 min of `CREATE`/`INSERT`/`DROP` churn, `cas_gc_interval_sec` 20).

Why one lost race becomes an 80 s abort. `completedRemovingDelete` (`Pool/CasRefCatalog.cpp:495-556` at `2f89c5db182`) retries a conflicted erase with a stale ETag:
- The loop sends `op.replace` with `catalog_snapshot.etag`, then re-reads the catalog (`catalog_snapshot = read(...)`, line 528).
- On `Conflict` it sleeps `Retry::backoff(attempt + 1)` (line 556): full jitter over 200 ms, 400 ms, ... capped at 5 s.
- The next iteration sends the erase with the ETag read before the sleep. Churn writes the catalog about 100 times per second, so that ETag is stale and the erase conflicts again.
- The policy is frozen once (`op.freeze(Retry::standard())`, 90 s). About 35 conflicts in a row use it up, and the next `op.replace` refuses with `gave up at the policy deadline after zero attempt(s)`.
A normal drain erases about 600 rows per round at 0.7 ms each and rarely loses the first race. The GC thread logs nothing during the stall.

Effect on foreground queries (run `R2b_after_churn`):
- Three aborted rounds (80.9 s, 81.9 s, 84.5 s), all `pre_fold_ref_drain`, all `Code: 210 ... CAS ref catalog erase`.
- The catalog keeps the `Removing` rows of the aborted round: `evidence_rows` 2961 and 4937 after two of the aborts, 527-625 in a normal round.
- The drain after an abort erases them in one pass: 4.4 s for 2961 rows and 11.0 s for 4937 rows. `CREATE` and `DROP` stall for the same 4.6 s and 11 s.
- CPU per `CREATE` rises from 6.1 ms to 14-26 ms and memory per `CREATE` from 2.6 MiB to 5-9.5 MiB while the catalog is large. Both return to 6.1 ms and 2.7 MiB after the drain.
- Churn throughput for the run is 10 % below the base run without aborts.

Frequency in this setup: 1 of 73 rounds at the base (two runs), 5 of 67 with the bundle (two runs). The aborts cluster, the sample does not separate the two builds, and the bundle does not touch this loop.

Candidate minimal fix, next to the options above: re-read the catalog after the pause, before the next `op.replace`, so the retry sends a current ETag. It does not remove the contention with foreground writes; routing the erase through the hot-key lane does.

Evidence: `tmp/prbundle/measure/runs/{R1b_before_churn,R2b_after_churn}/{gc_all_phases.tsv,cpu_by_minute.tsv,slow_queries.tsv,query_by_minute.tsv}` in the master worktree; give-up lines at `runs/R2b_after_churn/state/server.log:743513`, `:1256388`, `:1426023`.

2026-11-12 defaults review: threshold: at about one CREATE+DROP cycle per second for the pool every GC round succeeds (`ddl16_hold30`: 1920 cycles in 30 min, 20 of 20 rounds); at about nine per second (`ddl4`: 16926 cycles) 12 of 13 rounds abort, the same as at 16 threads; budgets 0 change nothing (`ddl16_b0`). CREATE/DROP throughput itself is bound by the `cas/ref_catalog` object: 4 threads per node 16926 cycles, 16 threads 18567, median CREATE 0.18 s against 1.09 s. Report v2: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (f9ec0861aff); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 correction after an independent check of the report against run data (docs/superpowers/reports/2026-11-11-cas-defaults-review.check.md.txt, a49f8a8e45f): the stall threshold lies between 1.1 and 9.4 create/drop cycles per second for the pool; no run in between. That one object (`cas/ref_catalog`) limits CREATE/DROP throughput is the likely explanation, not a proven one: no run varied catalog contention alone.

2026-11-12, wave 1 of the 26.8 improvements bundle (spec `docs/superpowers/specs/2026-11-12-cas-improvements-wave1-design.md` section 5, PR https://github.com/Altinity/ClickHouse/pull/2831). The wave takes the minimal fix: the pause of a conflicted erase moves before the admission check and the resolution read (same growing backoff), and the `deleted` phase metric is filled when the drain throws (AC #3). AC #2 is not in the wave: a fold that runs with a `Removing` row left behind contradicts `DrainBeforeDecision` of `CaRefPreFoldDrainCore.tla`; it moved to CAS-463.1 (model configuration first). AC #1 is a stand criterion and is re-measured after the wave.
<!-- SECTION:NOTES:END -->
