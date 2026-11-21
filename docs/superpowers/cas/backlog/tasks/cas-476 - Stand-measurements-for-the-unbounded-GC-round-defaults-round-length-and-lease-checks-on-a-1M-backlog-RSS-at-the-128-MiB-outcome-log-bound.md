---
id: CAS-476
title: >-
  Stand measurements for the unbounded GC round defaults: round length and lease
  checks on a 1M backlog, RSS at the 128 MiB outcome-log bound
status: To Do
assignee: []
created_date: '2026-11-13 03:02'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:gc'
  - 'origin:review'
  - 'area:testing'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: task
ordinal: 581000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
With all five round budgets at 0 one round processes each phase's whole eligible cohort. Measure on a stand: round duration over a 1M-entry backlog, which loops re-check the GC authority per iteration, what the heartbeat renews during a long round, peak RSS and PUT time of one outcome log filled to the 128 MiB bound. Decide whether the stale-GC alert threshold and the docs need a number. Source: whole-branch review of wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831, tmp/prbundle/wave1/final/umbrella-aggregate.md (V-06, V-14).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-13: by reading (wave 1 final fix, Part 5): per-iteration authority checks exist in the pre-fold drain (each erase attempt), the namespace janitor (each page) and ref-object cleanup (a gc/state lease read before each chunk); the pending_deletes redelete jobs have none. Measure what an unbounded redelete phase does when the GC lease is lost mid-phase.

2026-11-13: measurement report docs/superpowers/reports/2026-11-12-cas-improvements-wave1-measurements.md: prediction M-4 (reclaim backlog 0 at the end of a 120 s idle phase) failed on base and head alike (last idle pending_reclaim: base churn 152, head churn 112, base no-churn 86, head no-churn 144). pending_reclaim equals condemned minus redeleted of system.cas_gc_log; entries are redeleted two rounds after they are condemned and rounds 47 to 49 still condemn during idle. Not a regression of the head. To confirm: extend the idle phase until two consecutive rounds condemn 0, then read pending_reclaim; find what the idle-phase condemnations are (suspect: old parts after merges). Also: the base had 4 aborted rounds (NETWORK_ERROR ref-catalog erase at the 90 s window) in the churn run, the head 0; which head change avoids them was not isolated.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: no report under docs/superpowers/reports mentions a 1M-backlog or 128 MiB outcome-log measurement (grep empty); no backlog card in utils/cas-yardstick/yardstick/cards. Changes: type unset -> task: a stand measurement run. Links: needs the backlog card of CAS-321.8 for the 1M backlog. Would settle it: a measurement report; may exist outside docs/superpowers/reports (e.g. under tmp/) which this check did not search.
---
<!-- COMMENTS:END -->
