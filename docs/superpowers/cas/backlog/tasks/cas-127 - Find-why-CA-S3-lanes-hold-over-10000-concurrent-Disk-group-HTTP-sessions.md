---
id: CAS-127
title: 'Find why CA-S3 lanes hold over 10,000 concurrent Disk-group HTTP sessions'
status: To Do
assignee: []
created_date: '2026-07-27'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:backend'
  - 'area:ci'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:review'
  - 'theme:ci-stabilization'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
references:
  - src/Common/HTTPConnectionPool.cpp
priority: medium
type: enhancement
ordinal: 165000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the ASan CA-s3 lane (`e2d04bfe37e`), `00149_quantiles_timing_distributed` failed on a leaked warning
`ConnectionGroup: Too many active sessions in group Disk, count 10400, warning limit 8000` (`src/Common/HTTPConnectionPool.cpp:251`).
Output was correct and reruns passed, but 10k concurrent sessions is a pressure signal. Distinct from connection churn
(audit F27, a creation rate, `u05-perf-b` connection-churn task): this is a concurrent count. Check whether CAS holds sessions
across retry backoffs before raising any limit.

Provenance: BACKLOG/operability-and-introspection.md#ca-s3-disk-session-pressure; verified 2026-09-26 against dd0ed2f263a.

Update 2026-11-21: The 2026-11-13 RCA (tmp/prbundle/wave1/ci2/sanitizer-rca.md) attributes the warning to a single system.metric_log merge (2418 columns) holding 8000 to 9000 Disk-group sessions for 93 s. The open scope is no longer 'find why' but: compare with the plain-S3 lane, decide whether the merge should hold fewer sessions or the warning limit should change, and stop the warning failing stateless tests on the cas-s3 lanes (largest single cause of stateless FAILs there).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A CA-s3 stateless run records the peak Disk-group session count and compares it with the plain-S3 lane
- [ ] #2 A recorded conclusion says whether CAS holds sessions longer than needed, with the call site if so
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
First recorded: 2026-07-27 (1258b8c2a30, by 'CA-s3 Disk session pressure')

2026-11-11 PR https://github.com/Altinity/ClickHouse/pull/2805 @3fa3a01203b (run 42921139681): `Too many active sessions in group Disk` warnings: asan 2/2 2 lines, tsan 1/2 9, tsan 2/2 8 (counts 8000-13800), none on msan, arm_binary or asan 1/2. All FAIL rows on the asan 2/2 and tsan shards are `having stderror` with this warning, clustered after about 3 h of the run.

2026-11-11: PR 2805 head `2f89c5db182`, CI run 42999688750 (triage: tmp/pr2805/run_42999688750/triage.md). The `ConnectionGroup: Too many active sessions in group Disk` warning fails 16 stateless tests in this run: 1 on ASan 2/2 (`00084_external_aggregation`, count 9100), 4 on TSan 1/2, 10 on TSan 2/2 (counts up to 19000), 1 on arm_binary (`04093_lazy_final_combined_filtering`). It is the largest single cause of stateless FAILs on the cas-s3 lanes.

2026-11-12: PR https://github.com/Altinity/ClickHouse/pull/2831 @d8cbdfae3ec, run 43248330323 (triage: tmp/prbundle/wave1/ci/triage.md). Six TSan FAILs with stdout correct and stderr = "Too many active sessions in group Disk": job 131350839384 03532_json_dynamic_updates (count 8100); job 131350839414 04339_virtual_row_prefetch_filtered_out (8300), 04813_remote_database_engine_multi_shard_sharding_key (9900), 02714_read_bytes_aggregateFunction (9500), 01913_quantile_deterministic (8100 to 9400), 05054_injective_plus_minus_type_gate_plan_shape (11000). Peak count 11100. ASan and MSan shards: 0 hits. Base 43246617309 has 17 TSan FAILs (9 in job 131304876494, 8 in job 131304877124); the 9 sampled ones carry the same warning.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824, job 131775013788 (ASan cas s3 storage 2/2 on the larger runner, no shedding): 03279_pr_3_way_joins_full_first FAIL on stderr 'Too many active sessions in group Disk, count 8500'. First occurrence on an ASan shard: with all 8 workers kept, the session count now crosses the limit there too.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824 (triage tmp/prbundle/wave1/ci2/triage.md): tsan 2/2 job 131780028435: 01061_alter_codec_with_type (count 9500), 02151_hash_table_sizes_stats_joins_reordered (10800 to 11400), 04653_merging_aggregated_deserialized_thread_count (8000).

2026-11-13: RCA on PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824 (report tmp/prbundle/wave1/ci2/sanitizer-rca.md): the session warnings are caused by one system log merge (system.metric_log, 2418 columns): 8000 to 9000 sessions held for 93 s on ASan 1/2. They are not tied to the number of workers or of CAS disks.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: task note 2026-11-13: RCA (tmp/prbundle/wave1/ci2/sanitizer-rca.md) attributes the warnings to one system.metric_log merge holding 8000-9000 sessions for 93 s, independent of workers or CAS disks; question 'why' is answered, no fix or decision recorded. Changes: impact:availability; type research -> enhancement: cause is known; remaining work is reducing session pressure or handling the warning; priority low -> medium: largest single cause of stateless FAILs on the cas-s3 lanes per notes of 2026-11-11 and 2026-11-13; description: Update paragraph appended. Links: related: connection churn tasks (CAS-74, CAS-283). Would settle it: whether the metric_log-merge session holding is CAS-specific (plain-S3 lane comparison, AC1) and which fix or limit change is chosen.
---
<!-- COMMENTS:END -->
