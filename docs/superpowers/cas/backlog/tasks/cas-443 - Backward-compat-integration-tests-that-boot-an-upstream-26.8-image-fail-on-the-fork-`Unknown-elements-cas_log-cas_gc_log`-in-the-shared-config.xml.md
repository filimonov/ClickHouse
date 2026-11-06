---
id: CAS-443
title: >-
  Backward-compat integration tests that boot an upstream 26.8 image fail on the
  fork: `Unknown elements 'cas_log', 'cas_gc_log'` in the shared config.xml
status: To Do
assignee: []
created_date: '2026-11-06 22:01'
labels:
  - cas
  - ci
  - integration
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
