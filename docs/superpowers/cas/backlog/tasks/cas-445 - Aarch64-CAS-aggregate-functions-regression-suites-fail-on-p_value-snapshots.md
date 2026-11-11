---
id: CAS-445
title: Aarch64 CAS aggregate-functions regression suites fail on p_value snapshots
status: To Do
assignee: []
created_date: '2026-11-10 22:23'
updated_date: '2026-11-11 08:00'
labels:
  - 'area:tests'
  - 'area:ci'
  - 'origin:ci'
dependencies: []
priority: medium
type: bug
ordinal: 548000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
RegressionTestsAarch64 / CASAggregateFunctions (1-3) and CASS3CacheAggregateFunctions (1-3) are red on PR 2805 @41d4a78eeb9 (run 42864462634); the RegressionTestsRelease variants of the same six suites are green. Artifact cas_aggregate_functions_1-artifacts-aarch64_zookeeper (tmp/lanes/ci/pr2805_logs/arm_aggfn_1/nice-new-fails.log.txt): 496 Fail scenarios, all 'check output against snapshot' against tests/snapshots/steps.py.<function>>=26.8.aarch64.snapshot, functions studentTTest (412), welchTTest (75), analysisOfVariance (7); the difference is the last digit of p_value (0.2519310730478018 expected, 0.2519310730478017 actual). No CAS code is involved: the queries run on plain tables and the snapshot files live in the regression repo. The non-CAS RegressionTestsAarch64 / AggregateFunctions job is skipped on PRs, so the base-level status on aarch64 is unverified. Needed: a run of the plain aarch64 AggregateFunctions suite on antalya-26.8 to confirm it fails the same way, then the fix is in Altinity/clickhouse-regression (regenerate or tolerance-compare the >=26.8.aarch64 snapshots).
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
2026-11-11 PR https://github.com/Altinity/ClickHouse/pull/2805 @3fa3a01203b (run 42921139681): still red. Six jobs: `RegressionTestsAarch64 / CASAggregateFunctions (1)(2)(3)` and `CASS3CacheAggregateFunctions (1)(2)(3)`. Part 2 and part 3 fail 3 leaf scenarios each (`analysisOfVariance`, `studentTTest`, `welchTTest`, merge and finalizeAggregation variants); part 1 has 496 `Fail` lines under the same three functions. First traceback: snapshot `p_value` 0.2519310730478018, actual 0.2519310730478017. Scenario counts equal the previous PR run 42864462634. The base-branch MasterCI run 39839856866 fails the same jobs.
<!-- SECTION:NOTES:END -->
