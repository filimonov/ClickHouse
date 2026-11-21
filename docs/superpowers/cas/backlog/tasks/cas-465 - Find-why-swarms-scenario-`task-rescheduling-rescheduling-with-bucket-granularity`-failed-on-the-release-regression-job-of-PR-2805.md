---
id: CAS-465
title: >-
  Find why swarms scenario `task rescheduling/rescheduling with bucket
  granularity` failed on the release regression job of PR 2805
status: To Do
assignee: []
created_date: '2026-11-11 21:18'
updated_date: '2026-11-21 08:25'
labels:
  - ci
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
  - 'confidence:speculative'
  - groomed-20261121
  - 'groom-20261121:unclear'
  - 'groom-20261121:certainty-low'
  - grooming-20261121-changed
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2805'
  - >-
    https://github.com/Altinity/ClickHouse/actions/runs/42999688750/job/130453946496
priority: low
type: bug
ordinal: 569000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2805 head `2f89c5db182`, CI run 42999688750, job https://github.com/Altinity/ClickHouse/actions/runs/42999688750/job/130453946496 (Regression release swarms): a fourth scenario failed next to the three that also fail on the base branch: `/swarms/feature/task rescheduling/rescheduling with bucket granularity` (27 s). The Aarch64 job of the same run passed it. The GitHub log prints a traceback only for the first failure; the assertion is in the TestFlows artifact `nice-new-fails.log.txt`. Run 42864462634 (Aarch64) failed a different fourth scenario, `object storage cluster profile events with overloaded node`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The assertion and the server-side error of the failed scenario are read from the TestFlows artifacts
- [ ] #2 The failure is classified: base-branch flake (with a base-branch run that shows it) or caused by the CAS port; a fix or an upstream report exists
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
Evidence: tmp/pr2805/run_42999688750/triage.md (NEW item 2); log tmp/pr2805/run_42999688750/logs/r_130453946496.txt line 2783. Listed under the umbrella CAS-450. Altinity fork CI only; never play.clickhouse.com.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: unclear (low). Evidence: CAS-450 note 2026-11-12: CAS-465 did not occur on either architecture in run 43248330323; seen once in run 42999688750 and the Aarch64 job of the same run passed it. Changes: confidence set to speculative. Links: related: CAS-468 (three swarms scenarios failing on the base branch). Would settle it: the assertion text in the TestFlows artifact nice-new-fails.log.txt of job 130453946496 and a base-branch run showing the scenario failing.
---
<!-- COMMENTS:END -->
