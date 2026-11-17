---
id: CAS-481
title: >-
  test_cas_early_commit_restart: `_assert_fsck_clean` accepts a `SYSTEM CAS GC
  RUN` row with `acquired_lease = 0`, FSCK then reports 3 unreachable
  (arm_binary, distributed plan)
status: Done
assignee: []
created_date: '2026-11-13 12:56'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:testing'
  - 'origin:ci'
  - 'area:gc'
dependencies: []
priority: medium
ordinal: 586000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
tests/integration/test_cas_early_commit_restart/test.py::test_crash_between_early_commit_and_rename_leaves_no_part fails on Integration tests (arm_binary, distributed plan, 4/4): job 131829639338 of PR https://github.com/Altinity/ClickHouse/pull/2831 run 43379266824 (three times including reruns) and identically on the base branch (job 131295791161, run 43246617309). After the restart the GC lease of the killed incarnation is still held ('CA GC: lease held by another mounter (tick 1)'), so SYSTEM CAS GC RUN returns a non-authoritative row (acquired_lease = 0) whose pending columns are zero by construction (InterpreterSystemQuery.cpp); the test's drained check (test.py lines 52 to 56) looks only at pending_condemned == 0 and proceeds; FSCK then sees the 3 unreleased blobs. The pinned 30 s lease was in effect (36.5 s wait, ready after 40.7 s). Fix the test: require acquired_lease = 1 (and not deferred) before it trusts a GC RUN row, and poll until then. Other lanes pass by timing. Evidence: tmp/prbundle/wave1/ci2/triage.md section 3.1.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Fixed in PR 2831 by `bc2dcc52148`.
<!-- SECTION:NOTES:END -->
