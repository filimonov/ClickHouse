---
id: CAS-431
title: >-
  Find why the regression shard `LightweightDelete (3)` on the cas-s3-cache disk
  hangs until the 5 h cancel
status: To Do
assignee: []
created_date: '2026-11-04 23:11'
updated_date: '2026-11-05 13:00'
labels:
  - 'area:tests'
  - 'area:ci'
  - 'origin:ci'
dependencies: []
priority: high
type: bug
ordinal: 534000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`RegressionTestsRelease / CASS3CacheLightweightDelete (3, --skip "/lightweight delete/disk ...")` is CANCELLED after about 5 h on every recent antalya-26.8 pull request: https://github.com/Altinity/ClickHouse/pull/2647 (2026-10-27), https://github.com/Altinity/ClickHouse/pull/2704 (run 41807147619, job 126498220379, 09:31-14:36 UTC on 2026-11-04) and https://github.com/Altinity/ClickHouse/pull/2508 (2026-11-04, where `CASLightweightDelete (3)` was cancelled as well). The job dies inside the suite step, so the set-commit-status and upload-logs steps never run: no `nice-new-fails.log.txt`, no node logs, no GitHub job log. The sibling shards pass: `CASLightweightDelete (3)` 1 h 23 min, `CASS3CacheLightweightDelete (1|2|4)` 36-86 min.

Unknown: whether a scenario hangs (a stuck DELETE / mutation against the CAS disk) or the suite's wall time merely exceeds the cancel on the cas-s3-cache disk. Either way nothing is captured today.

Approach: run shard 3 locally against the cas-s3-cache disk config with the TestFlows `--log` and a per-scenario timeout; if it hangs, dump `system.processes`, `system.mutations`, `system.cas_log` and stack traces of the stuck node at the moment of the hang. Also add a watchdog step to the regression job (upload logs on cancel) so the next hang leaves evidence.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The hanging scenario (or the slow set) is named with the server-side state at the hang: stuck query, mutation or CAS request
- [ ] #2 The regression job uploads logs on cancellation, so a hang leaves node logs and the TestFlows log behind
- [ ] #3 Shard 3 finishes on cas-s3-cache in the same order of time as its siblings, or its skip list is adjusted with the reason recorded
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
2026-11-05: on https://github.com/Altinity/ClickHouse/pull/2730 (run 41932962724) both `CASLightweightDelete (3)` and `CASS3CacheLightweightDelete (3)` were cancelled, so the non-cache lane hangs as well; https://github.com/Altinity/ClickHouse/pull/2773 (run 41932902021) ran without the CAS regression lanes.
<!-- SECTION:NOTES:END -->
