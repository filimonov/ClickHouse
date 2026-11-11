---
id: CAS-450
title: >-
  Stabilise CI on antalya-26.8: every CAS lane green or its red tracked
  (umbrella)
status: To Do
assignee: []
created_date: '2026-11-11 10:15'
labels:
  - 'area:ci'
  - 'area:testing'
  - 'origin:ci'
  - 'complexity:epic'
  - 'theme:ci-stabilization'
dependencies:
  - CAS-432
  - CAS-223
  - CAS-127
  - CAS-447
  - CAS-47
  - CAS-446
  - CAS-434
  - CAS-439
  - CAS-354
  - CAS-341
  - CAS-353
  - CAS-426
  - CAS-445
  - CAS-440
  - CAS-252
  - CAS-431
  - CAS-340
  - CAS-441
  - CAS-165
  - CAS-443
  - CAS-430
  - CAS-335
  - CAS-322
  - CAS-355
  - CAS-332
  - CAS-85
  - CAS-321.2
  - CAS-437
  - CAS-438
  - CAS-442
  - CAS-444
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2805'
priority: high
ordinal: 553000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Umbrella for the CI reds of CAS pull requests against `antalya-26.8`. Goal: a CAS PR against `antalya-26.8` gets a run in which every red job is either fixed or a non-CAS red with a public issue link.

Baseline: PR https://github.com/Altinity/ClickHouse/pull/2805 @3fa3a01203b, run 42921139681: 22 failed jobs, 121 passed.

| group | failed jobs | cause | task |
|---|---|---|---|
| Stateless cas-s3, ASan 2/2 | 1 | 12600 s job timeout at 5921/6933 tests, no worker shedding | CAS-432 |
| Stateless cas-s3, TSan 1/2 and 2/2 | 2 | job timeout at 6847/6890 and 6932/6933; `Too many active sessions in group Disk` | CAS-432, CAS-127 |
| Stateless cas-s3, MSan 1/3 to 3/3 | 3 | `clickhouse-test` shed workers 6 to 1, timeout at 37-43% of the tests | CAS-223 |
| Stateless cas-s3, arm_binary | 1 | two `TOO_SLOW` on `test.hits`; pace fell from 5254 to 393 tests per interval | CAS-432 |
| Integration `test_database_glue` | 2 | red on the base branch since PR 2792 | not CAS, no task |
| Integration db disk 2/8 and 8/8 | 2 | docker pull over 900 s | infrastructure, no task |
| Regression Aarch64, aggregate functions | 6 | `p_value` snapshots differ in the last digit | CAS-445 |
| Regression Aarch64, Alter attach 3 | 1 | `gave up at the policy deadline` | CAS-440 |
| Regression Aarch64, LightweightDelete 4 | 1 | 1 row left after a delete of the whole table | CAS-252 |
| Regression Swarms, both architectures | 2 | the same 3 scenarios fail on the base branch | not CAS, no task |
| Grype | 1 | result parsing hung 60 min, 0 vulnerabilities reported | infrastructure, no task |

Scope: the tasks in the dependency list, grouped in the notes. Out of scope: new CI gates (CAS-77), ports to `cas-gc-rebuild` (CAS-222, CAS-302), the GCS live gate (CAS-172, CAS-348), untagging `no-cas-storage` tests (CAS-210), the floating `clickhouse-regression` ref on antalya-26.6 (CAS-339).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every stateless `cas s3 storage` shard (binary, arm, ASan, TSan, MSan) finishes inside its job timeout in three consecutive runs on antalya-26.8
- [ ] #2 `03572_export_merge_tree_part_limits_and_table_functions` reports no FAIL on any sanitizer lane (CAS-434)
- [ ] #3 The CAS regression suites are green on both architectures, or each red scenario has an open task with a root cause
- [ ] #4 The backward-compat integration tests that boot an upstream image pass on the fork (CAS-443)
- [ ] #5 The runner change for the ASan cas-s3 shards and the integration-test fix dropped from PR 2805 are merged into antalya-26.8 as their own PRs, or rejected with a reason recorded here
- [ ] #6 CAS-437, CAS-438, CAS-442 and CAS-444 are closed after PR 2805 merges, with the first green antalya-26.8 run linked
- [ ] #7 Each non-CAS red of the baseline has a public issue link recorded here
- [ ] #8 Every task in the dependency list is Done or removed from the list with a reason
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-11, state after the history rewrite of PR 2805 (head 2f89c5db182).

Task groups
- Stateless cas-s3 lanes do not fit the job timeout: CAS-432 (ASan), CAS-223 (MSan and TSan thread-pool profile), CAS-127 (over 10,000 Disk-group HTTP sessions), CAS-447 (ref cleanup deletes nothing, `_log` objects pile up for the whole run), CAS-47 (GC round duration growth), CAS-446 (18 s process stall drains the mount lease on TSan).
- Single stateless tests: CAS-434 (EXPORT PART limits test), CAS-439 and CAS-354 (Error-level `AWSClient` 404 line fails the stderr check), CAS-341 (read retry under a 1 MB query memory limit), CAS-353 (one slow renewal PUT refuses conditional writes), CAS-426 (style check for tests that land in no lane).
- Regression suites: CAS-445 (Aarch64 aggregate functions), CAS-440 (attach partition, policy deadline), CAS-252 (surplus row after concurrent delete; upstream PR ClickHouse/ClickHouse#122934), CAS-431 (`LightweightDelete (3)` hangs until the 5 h cancel), CAS-340 and CAS-441 (`lightweight_delete3` wall-clock bars), CAS-165 (`tiered_storage` background moves).
- Integration and unit tests: CAS-443 (backward-compat tests that boot an upstream 26.8 image), CAS-430 (`test_cas_gcs` token-fetch count), CAS-335 (loopback connect timeout in a `CASIOTestAwsS3Client` gtest under host load).
- Stress lanes: CAS-322 (pre-stress restart budget), CAS-355 (sequential 40 s lease reclaim per disk after a forceful shutdown), CAS-332 (hung check on an unbounded fuzzer INSERT).
- Diagnosability: CAS-85 and CAS-321.2 (dump `cas_gc_log`, `cas_log`, `metric_log` from the CAS lanes). Timed-out jobs upload no server logs today; the limits-test RCA below stops at `OPTIMIZE FINAL` for that reason.
- Fix already on the PR 2805 branch, close after the merge and the first green run: CAS-437 (086efc95363, `IDisk::checkAccess`), CAS-438 (8c26e8792eb, `no-random-settings` on 03800 and 03927), CAS-442 (4810e687c92, merged part name in 05387), CAS-444 (c840f70b133 and 2f89c5db182, port of PR 2801).
- Older tasks (CAS-47, 85, 165, 322, 332, 335, 341, 353, 354, 355) were opened on antalya-26.6 runs and are not re-verified on antalya-26.8.

Stateless cas-s3 time budget
- ASan 1/2 passed in 2 h 27 min on the `AMD_LARGE` runner; ASan 2/2 timed out on the same runner type without shedding. The lane does not fit two shards even on the large runner.
- RCA draft `tmp/lanes/ci/cas432_rca_draft.md`: runner variance up to 1.6x between two `AMD_LARGE` runners on one commit, plus a progressive in-run slowdown (test wall time against the binary cas-s3 lane grows from 3.8x to 6.6-7x after 2.5 h; the plain ASan lane grows 4.1x to 5.0x). Workers stay busy, no shedding; server-measured S3 latency grows (HEAD 2.5 to 14.5 ms, LIST 14 to 58 ms).
- RustFS LIST load in CI is about 10% of the load that slows HEAD/GET 2.4x in the local benchmark, so LIST starvation does not explain the slowdown. Open hypothesis: page and dentry cache pressure on the runner under the 20-23 GiB ASan server. Discriminator not built yet: a sidecar HEAD/LIST probe plus cached-memory sampling in the lane.

EXPORT PART limits test (CAS-434)
`03572_export_merge_tree_part_limits_and_table_functions`: two INSERTs of 4194304 rows, `OPTIMIZE TABLE ... FINAL`, four `EXPORT PART`, `wait_for_exports 60`; client receive timeout 300 s, per-test timeout 600 s.

| build | no CAS | cas-s3 |
|---|---|---|
| amd_binary | n/a | 16.1 s OK |
| arm_binary | 16.4 s OK | 19.7 s OK |
| amd_debug | 10.8 s OK | n/a |
| ASan, CI | 79.5 s OK | 600.09 s BROKEN (position 5587/6933) |
| MSan, CI | 240.5 s OK | 481.59 s FAIL (position 1609/4640) |
| TSan, CI | 359.7 s BROKEN; s3 storage 904.8 s BROKEN | 613.64 s BROKEN (position 4761/6933) |
| ASan, local, test alone | 45.58 s OK | 36.61 s OK |

cas-s3 and non-sanitizer rows: PR 2805 run 42921139681. Sanitizer no-CAS rows: MasterCI run 39839856866 (ab9c4003d72).
- CAS does not slow the test: alone under local ASan it takes 36.6 s on cas-s3 against 45.6 s on a plain disk.
- On loaded sanitizer lanes it exceeds the 60 s export wait and the 300 s receive timeout of `OPTIMIZE FINAL`. It is already `BROKEN` on TSan without CAS.
- Only MSan reports `FAIL`, because the `tests/broken_tests.yaml` rule (5aaf212c0d8) has `check_types: [tsan]`.
- The `no-cas-storage` tags were not lost in the port: antalya-26.8 removed them on purpose in 5a1ffb222a5.
- Options: extend the `broken_tests.yaml` rule to msan and asan (recommended); raise the `wait_for_exports` budget; shrink the input. No decision yet.

Commits dropped from PR 2805 (the PR is a forward port; these need their own PRs against antalya-26.8)
Kept on the local branch `backup/pr2805-before-history-rewrite-20261111` (725253ebb60) in the worktree `antalya-26.8-pr-2159`:
- 2a4b5e72aa2 `ci/defs/altinity_jobs.py`: ASan cas-s3 shards on `AMD_LARGE`; 41d4a78eeb9 regenerated `.github/workflows/master.yml`, `pull_request.yml`, `pull_request_community.yml`. Task: CAS-432.
- ad04f95e441 `skip_check_for_incorrect_settings.xml` plus two lines in `tests/integration/test_backward_compatibility/test_custom_s3_disk_remote_url_allow_hosts.py`. Task: CAS-443.
Expected on the next run of PR 2805 @2f89c5db182: the ASan cas-s3 1/2 timeout returns and `test_custom_s3_disk_remote_url_allow_hosts` is red.

Not CAS, no task yet (recorded so the next triage does not re-derive them)
- `test_database_glue`: red on antalya-26.8 since PR 2792.
- Integration db disk 2/8 and 8/8: docker pull over 900 s.
- Regression Swarms on both architectures: the same 3 scenarios fail on the base branch.
- Grype: result parsing hung for 60 min with 0 vulnerabilities reported.

Open decisions
- Separate PRs against antalya-26.8 for the runner change and for the integration-test fix.
- Three shards for the ASan cas-s3 lane.
- Fix option for CAS-434.
- Restore the PR template's default exclusions (Aarch64, TSan, MSan, Coverage) on CAS PRs; PR 2805 runs with none ticked.
<!-- SECTION:NOTES:END -->
