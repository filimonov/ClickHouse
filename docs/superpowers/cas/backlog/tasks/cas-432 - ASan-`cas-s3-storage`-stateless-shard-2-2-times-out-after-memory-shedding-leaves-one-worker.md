---
id: CAS-432
title: >-
  ASan `cas s3 storage` stateless shard 2/2 times out after memory shedding
  leaves one worker
status: To Do
assignee: []
created_date: '2026-11-04 23:11'
updated_date: '2026-11-08 23:32'
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

2026-11-09 LOCAL REPRODUCTION (antalya-26.8-pr-2159, ASan+UBSan build of 2805 @ad04f95e441, praktika job 'Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)', --workers 8 as in CI, max_server_memory_usage pinned to the CI value 27.54 GiB by a local uncommitted patch; stateful preload skipped because the web-disk copy of test.hits stalls on this host's link - the 20 FAILs are tpc_ds/tpc_h return code 81, missing tpcds/tpch databases, an artefact of that skip). Server RSS (host /proc statm, 5 s sampler): 1.6 GiB at test start, 11.5 GiB after 3 min, 16.9 after 12, 21.7 after 21; clickhouse-test shed workers 8->7->6->5->4->3 at 81-83 percent every 30 s from minute 22 (CI 2805 2/2: minute 24, same 80-83 percent, 8->1 in 3.5 min) while RSS kept rising 21.6->22.8 GiB with clients down from 9 to 3. Shedding cannot lower this RSS: at the threshold MemoryTracking was 0.4-1.5 GiB and the rest is ASan runtime - /proc/pid/smaps: ~6 GiB fake stacks (detect_stack_use_after_return, one 11.1 MiB mapping per thread, ~7 MiB touched), ~2.6 GiB 8 MiB thread stacks, ~6 GiB ASan allocator regions, ~4 GiB other anonymous, 2 GiB shadow, 1.5 GiB binary; corr(threads, RSS)=0.87 at ~15 MiB per thread, 1237 threads (max_thread_pool_free_size default 1000 keeps idle threads and their fake stacks). 26.6 vs 26.8: same compiler-rt flag defaults and same ASan default options apart from allocator_may_return_null; the 26.8 lane runs 21 percent more tests (14485 vs 11996 files in 0_stateless) and 25 percent slower per test; 26.6 shard 2/2 peaked at 20.8 GiB (MemoryWorker resident), 26.8 shard 1/2 at 17.9, shard 2/2 crosses 22. Both branches sit within 10 percent of the bar on a 30.59 GiB runner. Root cause: clickhouse-test judges memory pressure by server RSS (comment in get_server_memory_fraction says MemoryTracking under-reports under sanitizers), but under ASan RSS is dominated by per-thread runtime state that shedding clients does not release, and the shed is irreversible, so the shard drains to one worker and hits the 12600 s limit. Fix options, in order of cost: (1) run the ASan cas-s3 lane with ASAN_OPTIONS detect_stack_use_after_return=0 or max_uar_stack_size_log=16 (cuts the ~6 GiB fake stacks; upstream CI does not set it either); (2) make the shedding reversible or base it on MemoryTracking plus a hard RSS guard near the OOM line; (3) lower max_thread_pool_free_size for sanitizer runs; (4) larger runner. Data: antalya-26.8-pr-2159/tmp/cas432/{rss_lane_2_2.tsv,mem_probe_*.txt,analyze_lane.py}, build_asan/test_cas_s3_2_2.log.
<!-- SECTION:NOTES:END -->
