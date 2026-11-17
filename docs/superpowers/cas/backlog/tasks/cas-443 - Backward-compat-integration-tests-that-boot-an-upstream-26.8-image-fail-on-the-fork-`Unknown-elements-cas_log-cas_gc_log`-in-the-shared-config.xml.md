---
id: CAS-443
title: >-
  Backward-compat integration tests that boot an upstream 26.8 image fail on the
  fork: `Unknown elements 'cas_log', 'cas_gc_log'` in the shared config.xml
status: Done
assignee: []
created_date: '2026-11-06 22:01'
updated_date: '2026-11-17 20:01'
labels:
  - cas
  - ci
  - integration
  - 'theme:ci-stabilization'
dependencies: []
priority: high
type: bug
ordinal: 546000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2805 @8c26e8792eb, jobs `Integration tests (amd_asan_ubsan, targeted)`, `... old analyzer, 1/8` and `7/8`: both tests of test_backward_compatibility/test_custom_s3_disk_remote_url_allow_hosts.py time out in cluster.start(). The node runs the official image clickhouse/clickhouse-server:26.8.26.12 with with_installed_binary=True and the integration runner's config.xml, which is the fork's programs/server/config.xml with the `<cas_log>` and `<cas_gc_log>` sections. Its clickhouse-server.err.log: `Code: 137. DB::Exception: Unknown elements 'cas_log', 'cas_gc_log' found in config /etc/clickhouse-server/config.xml. ... (UNKNOWN_ELEMENT_IN_CONFIG) (version 26.8.26.12 (official build))`. Tests that pin older upstream tags (25.x, 26.5) pass: the top-level-element check is newer than those images. Every future backward-compat test that pins an upstream image with the check will fail the same way, on antalya-26.6 as well once such tests are backported. Failed on both PR 2805 runs (a2147e55697, 8c26e8792eb); not run on PR 2806. Logs: https://altinity-build-artifacts.s3.amazonaws.com/PRs/2805/8c26e8792eb4d675d6cfa59b06f8e31b38471008/integration_tests_amd_asan_ubsan_targeted/logs.tar.gz
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decide where the fork's `cas_log`/`cas_gc_log` definitions live for integration tests (config.d file that upstream images do not get, or skip_check_for_incorrect_settings for upstream-image instances)
- [ ] #2 test_custom_s3_disk_remote_url_allow_hosts passes on PR 2805
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
2026-11-06: owner decision: skip_check_for_incorrect_settings per test, not globally. Fixed on the PR 2805 branch as ad04f95e441: configs/skip_check_for_incorrect_settings.xml on the two 26.8.26.12 instances. Affected today: this one file (2 cases); the other 29 old-image tests pin tags <= 26.6 or Altinity images. Native run: both nodes now start; test_table_... passed, test_database_... failed on the local MinIO 'XMinioStorageFull' (workstation disk 100% full), not the test. CI of 2805 will confirm.

2026-11-11: `ad04f95e441` (`skip_check_for_incorrect_settings` for `test_custom_s3_disk_remote_url_allow_hosts`) is no longer in https://github.com/Altinity/ClickHouse/pull/2805: the branch history was rewritten without it, new head `2f89c5db182`. The commit is kept on the local branch `backup/pr2805-before-history-rewrite-20261111`; it needs its own pull request against antalya-26.8. The test stays red on the 2805 head until then.

2026-11-11 21:25Z: confirmed on CI run 42999688750: both `allow_hosts` tests fail on integration 7/8 and on the targeted job with the same `UNKNOWN_ELEMENT_IN_CONFIG`; the head tree has no `configs/skip_check_for_incorrect_settings.xml`.

2026-11-12: PR https://github.com/Altinity/ClickHouse/pull/2805 merged into antalya-26.8 as db0431e95cd (head 2f89c5db182, approved by mkmkme). The fix ad04f95e441 was dropped from the PR and is not on antalya-26.8; it needs its own PR. The commit is on the local branch backup/pr2805-before-history-rewrite-20261111.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit bf13b6824d7 "Skip the unknown-config check in the custom S3 disk allow-hosts test" covers: #1 decision recorded as `skip_check_for_incorrect_settings` for the two upstream-image instances of `test_custom_s3_disk_remote_url_allow_hosts` only; #2 `test_custom_s3_disk_remote_url_allow_hosts` passed in the four integration lanes that ran it on the head (run https://github.com/Altinity/ClickHouse/actions/runs/43379266824). The criterion names PR 2805, which merged without the fix. Still open: none. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Delivered in PR 2831 by `bf13b6824d7`.
<!-- SECTION:NOTES:END -->
