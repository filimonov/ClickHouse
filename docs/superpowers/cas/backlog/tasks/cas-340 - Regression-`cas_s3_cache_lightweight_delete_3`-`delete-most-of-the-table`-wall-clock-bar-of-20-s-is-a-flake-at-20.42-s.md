---
id: CAS-340
title: >-
  Regression `cas_s3_cache_lightweight_delete_3`: `delete most of the table`
  wall-clock bar of 20 s is a flake at 20.42 s
status: To Do
assignee: []
created_date: '2026-10-01 17:03'
updated_date: '2026-11-08 22:33'
labels:
  - ci
  - tests
dependencies: []
priority: low
ordinal: 433000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Observed 2026-10-01 on PR #2463 (release, x86): the scenario asserts execution_time < 20 and measured 20.42 s; the aarch64 run of the same shard and the same scenario on #2452 passed. A 2 % margin on a wall-clock bar on shared runners. Evidence: tmp/ci-2463/rca.md. Fix in clickhouse-regression: widen the bar or assert the ratio against a baseline measured in the same run.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The scenario no longer asserts an absolute wall-clock bound, or the bound has a margin that survives shared-runner noise
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
2026-11-08: PR 2806 @300a8c155c7 (antalya-26.6, backport of 2730+2773): lightweight_delete3/cas 'delete most of the table' took 49.9 s (MergeTree) and 52.0 s (ReplacingMergeTree) against the 20 s bar; cas-s3-cache variant green. 2.5x over the bar, not a 0.4 s flake: compare with the next 26.6 run before merging 2806.

2026-11-08 (2806 vs 2577, lightweight_delete3/cas, node logs + raw.log): the DELETE step of 'delete most of the table' runs 13-17 s on both branches (14 steps each, 7 engines). The two 2806 failures (MergeTree 28.27 s, ReplacingMergeTree 21.64 s; earlier note's 49.9/52.0 s were scenario totals) fell inside a burst of 'system.part_log' merges: 94/95/81 merges per minute at 12:24-12:26 node time versus a 19/min baseline. On 2577 the same burst (66/65/95 per minute at 03:16-03:18) started after the last DELETE finished (03:17:03), and every DELETE stayed under 17 s. The burst is produced by the scenario itself: 'part_log.flush_interval_milliseconds=500' (lightweight_delete/configs/clickhouse/config.d/logs.xml) and the per-id partitioning make each flush a part, and cas_mode.py puts part_log on the CAS disk. Mutations are throttled meanwhile ('Not enough idle threads to apply mutations'). Not a code slowdown of 2730/2773: the fix stays on the regression side (CAS-340 acceptance), optionally with part_log on a local disk in cas_mode.py. Histograms: master/tmp/lanes/ci/regr/dl_{2806,2577}_lwd3_cas/partlog_merges_per_min.txt.
<!-- SECTION:NOTES:END -->
