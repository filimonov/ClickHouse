---
id: CAS-107
title: >-
  Build a validated GC-cadence and snapshot-size model for sizing a production
  pool
status: To Do
assignee: []
created_date: '2026-07-03'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:gc'
  - 'area:docs'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:soak'
  - 'origin:otel-demo-audit'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-0
dependencies: []
references:
  - docs/en/antalya/cas/configuration.md
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
  - docs/superpowers/cas/umbrella-roadmap.md
priority: medium
type: research
ordinal: 145000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Operators have no way to predict GC round time, snapshot size or S3 request budget from their workload. The defaults review before the first deployment needs one (roadmap §7).
Data points: a live-AWS round took 30-40 s; audit F29 shows the GC snapshot run growing 11-12 MiB -> 125 MiB per round with the condemned backlog, rewritten every round.

Provenance: BACKLOG/performance.md#scale-findings [Capacity model]. Verified 2026-09-26 against dd0ed2f263a.

Update 2026-11-21: The round-time half of the model exists (notes 2026-11-11/12: T = rho*I/(1-rho), holds for rho up to about 0.5, checked on otel.demo and the stand). Remaining scope: snapshot-bytes and request-per-day terms (AC #1), a check against a soak run (AC #2), and operator sizing guidance in docs/en/antalya/cas (AC #3; none found).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A model gives round duration, snapshot bytes and S3 requests per day as functions of parts, blobs and churn, with its assumptions stated
- [ ] #2 The model is checked against at least two measured pools (otel.demo and a soak run) within a stated error
- [ ] #3 The operator docs carry the resulting sizing guidance
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
First recorded: 2026-07-03 (7a8649f9e4f, by 'Capacity model')

Merged from u19b-soak (merge into CAS-107): Cadence model shape from the soak ledger (ADAPTIVE-GC-CADENCE, 2026-07-06): request rate is roughly A/interval + B*interval, where A is the per-round cost over the whole blob universe and B is the writer amplification that grows with time since the last fold; the optimum interval grows with pool size and shrinks with the hottest key's write rate. Hard ceiling regardless of the optimum: a hot RMW object must stay under the store's inline threshold (about 128 KiB on RustFS, rustfs#3231 territory). The trigger for a round should be per-key write pressure (event count, body size, age), not the number of changed keys: one key written 10000 times counts as one change. Dead data held a few extra minutes costs almost nothing in storage. The root-shard journal knobs the entry was written against (`gc_fold_threshold`, `gc_trim_body_soft_limit`) no longer exist at HEAD, so re-derive A and B for the current ref-lane and snapshot design before using any 2026-07 number.

2026-11-11 defaults review: model: round time T = rho x I / (1 - rho), rho = c x lambda (c = cost per ref-log object, lambda = objects per second, I = interval). GC busy share is rho whatever the interval; the interval sets reclaim lag (about 2 x (I + T)) and takeover time; capacity is 1/c. Checked on otel.demo (lambda 4.6/s, rho 0.17, I 300 s, predicted T 61 s, observed p50 50-67 s over 8 days) and on the stand (same load at 60 / 300 s: busy share 0.45 / 0.43, rounds 50 / 234 s, backlog 4105 / 8723). Per-log cost on RustFS: intake 7-18 ms, reduce about 6 ms per delta, both serial. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 defaults review: recalibration without the fault proxy: 11 INSERT/s gives 6-7 s rounds at 60 s (rho 0.10) and about 45 s at 300 s with budgets 0 (model predicts 33 s); the proxy inflated the per-log cost about 8 times, so the 7-18 ms per log of the earlier note is not a property of CAS or RustFS. The model holds for rho up to about 0.5; above that RustFS LIST degrades and rounds grow faster than the model. Report v2: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (f9ec0861aff); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: Task notes 2026-11-11/12 record the model T = rho*I/(1-rho), checked on otel.demo and the stand (report docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review); grep rho|sizing|busy share in cas-pr1 docs/en/antalya/cas: no sizing guidance (only a quick-start pointer). Changes: description: Update paragraph appended. Would settle it: whether the model covers snapshot bytes and S3 requests per day (AC #1 asks for all three); a second check against a soak run (AC #2).
---
<!-- COMMENTS:END -->
