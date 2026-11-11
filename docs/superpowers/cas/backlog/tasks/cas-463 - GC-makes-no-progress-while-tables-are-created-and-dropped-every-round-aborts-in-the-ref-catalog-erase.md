---
id: CAS-463
title: >-
  GC makes no progress while tables are created and dropped: every round aborts
  in the ref-catalog erase
status: To Do
assignee: []
created_date: '2026-11-11 19:20'
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
