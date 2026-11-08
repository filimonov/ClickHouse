---
id: CAS-432
title: >-
  ASan `cas s3 storage` stateless shard 2/2 times out after memory shedding
  leaves one worker
status: To Do
assignee: []
created_date: '2026-11-04 23:11'
updated_date: '2026-11-08 21:32'
labels:
  - 'area:tests'
  - 'area:ci'
  - 'origin:ci'
dependencies: []
priority: high
type: bug
ordinal: 535000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)` hit the 12600 s job timeout on https://github.com/Altinity/ClickHouse/pull/2704 (head feefeca33d8, run 41807147619) with 2860 of 6929 tests done. The lane ran for the first time on a pull request there (before feefeca33d8 the CAS stateless lanes were deferred behind the core blocking jobs and skipped). Evidence in the job log (master/tmp/pr2704/asan22/job.clean.log): between 10:00:37 and 10:10:00 UTC `clickhouse-test` printed `Server memory at 80% of limit — signalling one worker to stop after its current test` seven times, tracked workers went 8 → 1, and nothing restores them; the per-test median stayed 3 s, so the pace fell eightfold (62-97 tests per 10 min against 500-640 in shard 1/2, which never shed a worker and finished 6889 tests in 117 min). `--memory-limit 10737418240` (10 GiB).

Unknown: whether the server really held 8 GiB (CAS ledger state, caches, ASan quarantine) or a few tests in shard 2 (for example `03748_join_on_complex_condition_result_memory`, 143 s there) pushed it over the threshold once. No server log survives a timeout.

Options, cheapest first: (a) three or four batches for the ASan CAS S3 lane, as the MSan lane already has three (ci/defs/altinity_jobs.py); (b) a higher memory limit for the ASan CAS lanes; (c) a server-log capture on timeout so the next occurrence shows what held the memory. Pick after one measured rerun with memory metrics.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The ASan `cas s3 storage` shards finish inside the job timeout on two consecutive pull-request runs
- [ ] #2 The peak server memory of the lane is measured (system.asynchronous_metric_log or the runner's memory line) and recorded in the task
- [ ] #3 If a batch or limit change is made, the job config change is in the same pull request as the measurement
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
2026-11-05: third and fourth consecutive timeouts of the ASan cas-s3 lane. PR 2730 shard 2/2 (job 126955421110): no memory shedding at all, `--jobs 6`, 6175 of 6930 tests at the 12600 s cut, plus two 600 s `BROKEN` client timeouts (03800_autopr_reuse_index_analysis, 03634_autopr_input_bytes_estimation). PR 2773 shard 1/2 (job 126956051287): `--jobs 8`, shed to 1 worker between 07:53 and 08:03, timed out; shard 2/2 (job 126956051156): no shedding, `--jobs 6`, 4851 of 6930 at the cut. The lane does not fit the budget even without shedding; the batch count or the budget has to change, not only the memory limit.

2026-11-08: reproduced on PR 2805 head ad04f95e441: job 'Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)' timed out after 12600 s at test 2412/6933 (job 128213141054, run 42305105469).

2026-11-08, 26.6 vs 26.8 comparison of the ASan cas-s3 lane (result_pr.json + GH job logs, tmp/lanes/ci/cas_s3_lane.py): antalya-26.6 PRs 2806, 2812, 2577, 2802 finish both shards in 93-109 min with 5755 tests per shard, --jobs 8, no memory shedding. antalya-26.8 has 6931-6933 tests per shard (+20%) and ~10 s per test against ~8 s on 26.6; green runs (PR 2799) take 143-148 min per shard, i.e. ~70% of the 12600 s budget already. On 2805 @ad04f95e441 shard 2/2 shed 8 -> 1 workers between 23:58 and 00:02 UTC (24 min into the run, tests ~1030-1216 of 6933) while 03748_join_on_complex_condition_result_memory (133 s), 02475_bson_each_row_format (232 s), 03326_protobuf_schemas_race (189 s), 02448_clone_replica_lost_part (205 s), 02760_dictionaries_memory, 00084_external_aggregation and 00133_long_shard_memory_tracker_and_exception_safety ran concurrently; after that the shard crawled to 2412/6933 at the cut. The shedding code is present in clickhouse-test on both branches and never fires on 26.6, so the 26.8 server under ASan sits closer to the 8 GiB (80% of 10 GiB) threshold. The budget problem is 26.8-only.
<!-- SECTION:NOTES:END -->
