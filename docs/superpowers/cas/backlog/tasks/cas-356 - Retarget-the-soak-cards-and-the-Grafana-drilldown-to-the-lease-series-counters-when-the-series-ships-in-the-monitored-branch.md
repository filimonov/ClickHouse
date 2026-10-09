---
id: CAS-356
title: >-
  Retarget the soak cards and the Grafana drilldown to the lease-series counters
  when the series ships in the monitored branch
status: To Do
assignee: []
created_date: '2026-10-09 07:51'
labels:
  - 'area:soak'
  - 'area:grafana'
  - 'complexity:small'
dependencies: []
priority: medium
type: task
ordinal: 450000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Retarget saved as an 11-file diff: `.superpowers/sdd/2026-10-09-cas-lease-pr16-remove-observability-stack/pr16-master-retarget.diff` (cards s06_s08, s15, s19, s23, s34, s36, s39, s41; `utils/ca-soak/scenarios/README.md`; `utils/cas-grafana/casgrafana/drilldown.py`; `utils/cas-grafana/dashboards/cas-drilldown.json`).

Name changes:
- `CASRootCompareSwap` → `CASRootOverwrite` (`CASRootCompareSwapConflict` stays). Also in s41 and `scenarios/README.md`.
- `CASGCCompareSwap` → `CASGCOverwrite` (`scenarios/README.md`).
- `CASRemount*` → `CASMountReacquireAttempts`/`Succeeded`/`Failed` (s39, drilldown).
- `CASMountRenewalDeadlineExceeded` removed (s39, drilldown); drilldown "Fence lost post-write" reads `CASRequestFenceLostPostWrite`.

s06's gate on `CASRootCompareSwap` never checked anything: the counter had no producer, so the s08 "CAS contention bounded" verdict was always inconclusive. After the retarget it compares `CASRootCompareSwapConflict` with `CASRootOverwrite` and can fail.

Blocker: `utils/cas-grafana/tests/test_drilldown.py::GcRows.test_build_passes_checks_with_repo_names` validates dashboard names against this repo's own `src/Common/ProfileEvents.cpp` (cas-gc-rebuild), which never receives the series. Make the check read the monitored branch's source through an env override such as `CAS_PROFILE_EVENTS_CPP` (default: the repo file), so the retarget and the test can be green together.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The saved diff and the test change are applied in one commit once the series is in the nightly image
- [ ] #2 `utils/cas-grafana` unittest and ca-soak pytest are green
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
