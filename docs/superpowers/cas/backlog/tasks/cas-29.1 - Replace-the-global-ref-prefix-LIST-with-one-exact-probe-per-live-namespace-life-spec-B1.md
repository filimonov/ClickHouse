---
id: CAS-29.1
title: >-
  Replace the global ref-prefix LIST with one exact probe per live namespace
  life (spec B1)
status: To Do
assignee: []
created_date: '2026-07-29'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:gc'
  - 'complexity:large'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:2031-triage'
  - 'theme:parity'
milestone: m-1
dependencies: []
references:
  - CA/Gc/CasGc.cpp
  - CA/Backend/CasObjectStorageBackend.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2429'
documentation:
  - docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
parent_task_id: CAS-29
priority: high
type: feature
ordinal: 36000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`listRefPrefix` is one LIST of `<prefix>/cas/ns/stream/`, O(all `_log` keys). On otel.demo that is 4.6M keys, ~1575 s and
~1.2 GiB resident per round (audit F19); earlier it was 79% of GC wall time (2026-08), the audit re-measures 30-42%.
The S3 async iterator also schedules one global-pool job per page, ~1,500 short-lived threads per round (audit F24).
B1: take `Live`/`Removing` lives from the catalog cut, issue one exact GET of `refLogKey(life, last_folded + 1)` per life
on the round's io pool (404 = quiet), and LIST only a hot life's stream from `start-after` = its covered floor.
`shouldDeferRound` is decided from the probes. LIST is not consulted for the verdict, so decision-2 is untouched.
Before reusing the LIST path, confirm `max_keys` propagation into `S3ObjectStorage::iterate` (audit F6: ~3 requests per
1000-key page).

Provenance: BACKLOG/gc.md#gc-defer-decision-list-cost and [gc-frontier-one-list]; verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6: global LIST still in place on both.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A pool with K live lives and M `_log` keys below every floor: `defer_decision` issues exactly K exact GETs and no LIST when nothing changed
- [ ] #2 A hot life costs one LIST page starting at its floor; the DEFER verdict and `ref_tables` equal the global-LIST oracle's for the same state
- [ ] #3 A `_log` key above the floor omitted by an injected LIST omission still folds; a key below the floor is never touched
- [ ] #4 On otel.demo `defer_decision` wall time no longer grows with `_log` keys below the floor of quiet lives
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
Rejected alternative (from gc.md, deferred rounds pay the full LIST): a cross-round index of dead lives does NOT help by itself; S3 still walks their keys, only the parsing is saved.

First recorded: 2026-07-29 (539584ac324, by 'GC-DEFER-DECISION-LIST-COST')

2031-triage CAS-035: the audit's 'a GC round fully lists `cas/ns/stream/` and retains every key in memory, also on rounds that then defer' is this LIST; the edge-run memory half is CAS-88.

2026-10-12: after https://github.com/Altinity/ClickHouse/pull/2542 (CAS-359) the listing has two more consumers: `hintRefLogsWithinEpoch` hints only ids in `RefScanSummary::logs_by_life`, and the intake lookahead stops at the first id absent from `RefTableListing::logs`. Step 3's per-life LIST from the covered floor returns exactly the range the hinter needs (floor to `_ckpt.committed_through`), so build `logs_by_life` from it; the cost map's 'fold_ref_intake does not use the listing' no longer holds for the hints, the exact-key walk stays independent.

2026-10-12, owner: step 3's per-life LIST is not needed for the fold either. Ids are dense from the cursor to `_ckpt.committed_through` within an epoch, and `{E,1}.prev_epoch_seal` gives each older epoch's last id by exact GET (one per epoch crossed). The hint bound of PR 2542 moves to that back-chain (CAS-359 AC #9), so the fold consumes no listing; what still reads `listing.snapshots` is the baseline guard (no cursor, no `_ckpt`) and the rebuild health check, which need their own exact-key source or B2's cleanup listing.

2026-11-12 defaults review: with 2000 tables in the pool `defer_decision` lists every ref-log key each round: 91k keys at round 7, 366k at round 16, 104 s of a 147 s round (`mt1000_base`; the same with budgets 0). A table's logs are deleted only after its snapshot, taken every `snapshot_log_count_threshold` = 256 logs, so the steady listing is up to 256 x tables. Report v2: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (f9ec0861aff); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 correction after an independent check of the report against run data (docs/superpowers/reports/2026-11-11-cas-defaults-review.check.md.txt, a49f8a8e45f): the 256-keys-per-table plateau was not reached: the listing was still growing by about 50k keys per round at 366k when the 30-minute runs ended, and where the cleanup budget also binds it went past the bound (315k keys for 200 tables in `mt100_base`).

2026-11-13: RCA on PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824 (report tmp/prbundle/wave1/ci2/sanitizer-rca.md): evidence for this task from CI: on the release arm_binary cas-s3 lane the ref keys listed per GC round grow from 3723 to 111086 in 90 minutes and rounds abort at the 120 s request window while listing; see the CAS-47 note of the same date.

CAS-487 changes the intake probe: with generation 3 a cursor can be a tombstone, so the probe `refLogKey(life, last_folded + 1)` is wrong. The intake needs the `_ckpt` read plus a probe per life (2 requests per life, 3 cold), not exactly K `GET`s (AC #1). The note "ids are dense from the cursor to `_ckpt.committed_through`" no longer holds.

2026-11-17: PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit 67f518e7b8f: the intake now reads to the first absent key with a per-round time slice.
<!-- SECTION:NOTES:END -->
