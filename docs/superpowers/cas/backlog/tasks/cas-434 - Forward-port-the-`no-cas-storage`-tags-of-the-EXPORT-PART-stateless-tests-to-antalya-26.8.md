---
id: CAS-434
title: >-
  Forward-port the `no-cas-storage` tags of the EXPORT PART stateless tests to
  antalya-26.8
status: To Do
assignee: []
created_date: '2026-11-05 13:00'
labels:
  - 'area:tests'
  - 'area:ci'
  - 'origin:ci'
milestone: m-8
dependencies: []
references:
  - >-
    tests/queries/0_stateless/03572_export_merge_tree_part_limits_and_table_functions.sh
priority: medium
type: bug
ordinal: 537000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Commit cc73c9b6eea ("Skip the EXPORT PART stateless tests on CAS storage", on antalya-26.6) tags `03572_export_merge_tree_part_basic`, `03572_export_merge_tree_part_limits_and_table_functions`, `03572_export_merge_tree_part_special_columns` and `03608_export_merge_tree_part_filename_pattern` with `no-cas-storage`. antalya-26.8 has the runner gating (`tests/clickhouse-test` skips `no-cas-storage` under `--cas-s3-storage`) but not the tags, so the four tests run on the `cas s3 storage` lanes there.

Observed: `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)` on https://github.com/Altinity/ClickHouse/pull/2773 (sha 307f55348011ccfc75290a484e5ce3dff9ca761a, 2026-11-05): `03572_export_merge_tree_part_limits_and_table_functions` FAIL after 351 s, `Timeout waiting for exports to complete after 60s`, 2 of 4 exports in `part_log`, `system.exports` empty. The same test passed on https://github.com/Altinity/ClickHouse/pull/2730 shard 2/2 the same night, so the failure is timing under ASan on a CAS disk, not deterministic.

Either cherry-pick cc73c9b6eea onto antalya-26.8, or decide that EXPORT PART from a CAS part is supported and make the wait budget fit the lane; the 26.6 decision was to skip.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The four EXPORT PART tests carry `no-cas-storage` on antalya-26.8, or a recorded decision keeps them running with a wait budget that passes the ASan cas-s3 lane twice in a row
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
