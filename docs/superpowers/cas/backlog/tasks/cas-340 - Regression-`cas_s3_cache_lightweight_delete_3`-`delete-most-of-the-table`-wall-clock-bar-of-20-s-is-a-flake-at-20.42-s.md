---
id: CAS-340
title: >-
  Regression `lightweight_delete3` on both CAS lanes: `delete most of the table`
  (100 parts) costs 9 to 24 s against the 20 s bar
status: To Do
assignee: []
created_date: '2026-10-01 17:03'
updated_date: '2026-11-11 10:16'
labels:
  - ci
  - tests
  - 'area:write-path'
  - 'area:ci'
  - 'theme:ci-stabilization'
dependencies: []
priority: medium
ordinal: 433000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Observed 2026-10-01 on PR #2463 (release, x86): the scenario asserts execution_time < 20 and measured 20.42 s; the aarch64 run of the same shard and the same scenario on #2452 passed. A 2 % margin on a wall-clock bar on shared runners. Evidence: tmp/ci-2463/rca.md. Fix in clickhouse-regression: widen the bar or assert the ratio against a baseline measured in the same run.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The scenario no longer asserts an absolute wall-clock bound, or the bound has a margin that survives shared-runner noise
- [ ] #2 Count the S3 requests one lightweight-delete part mutation makes on CAS (part files, `tmp_clone` probe, stream-log append, manifest, relink) and name the avoidable ones
- [ ] #3 The scenario passes with a 2x margin on the slow runner: either the per-part cost is cut or clickhouse-regression gets a CAS-specific bar with the measured cost model recorded
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

2026-11-10: PR 2801 @3963b4f40ec, both lanes red: `cas` VersionedCollapsingMergeTree 21.9 s, GraphiteMergeTree 20.5 s; `cas-s3-cache` MergeTree 24.5 s, ReplacingMergeTree 22.8 s, VersionedCollapsingMergeTree 20.1 s. Not a 2 % margin. Per-engine DELETE time (raw.log `execution_time`, min–max over the 7 engines) across five runs of the same server code, cas / cas-s3-cache: 2801 @945b634 9.6–15.1 / 7.3–11.5; @083b575 8.2–13.1 / 7.5–14.4; @3963b4f 13.2–21.9 / 15.8–24.5; 2577 @02a2511 8.1–10.0 / 8.7–12.2; @74562b9 13.0–17.2 / 13.3–19.0. Same binary, 9 to 24 s: runner noise on top of a cost that already sits at the bar.
Cost model from the cas-lane server log (clickhouse1, `mutation_110` on `table_56785b61_d9ef_11f1_8ca3_92000a6a0759`, 15:36:11–15:36:33): 100 parts (`PARTITION BY id`, 100k rows each), 16 background threads, per-part cycle on a thread p50 2.69 s / mean 3.18 / max 11.6. Inside: mutate compute 0.09 s; part files to S3 (filled checksums to rename) p50 0.80 s, p90 1.86; the rest (commit after rename) about 0.9 s p50. 100 x 2.7 s / 16 threads = 17 s against a 20 s bar. No S3 errors or conflicts in the window: the 159 AWS-SDK 'signature check failed' debug lines are the HEAD-before-PUT 404s, minio.log is empty. The acceptance bar (CAS-441) was red in the same run on both lanes. Logs: tmp/review_2801/regression/ (raw_*.log, ch1_cas_server.log, win_vcmt.log).
<!-- SECTION:NOTES:END -->
