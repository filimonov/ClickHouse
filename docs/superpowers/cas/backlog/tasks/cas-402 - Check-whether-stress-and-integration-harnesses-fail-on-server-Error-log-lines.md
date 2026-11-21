---
id: CAS-402
title: Check whether stress and integration harnesses fail on server Error log lines
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:testing'
  - 'area:observability'
  - 'needs:repro'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:unclear'
  - 'groom-20261121:certainty-low'
  - grooming-20261121-changed
dependencies: []
priority: low
type: research
ordinal: 500000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The #2555 review found a stateless test failing on a `<Error>` log line for a retried condition. Check whether the stress and integration harnesses also fail on server `<Error>` lines. If so, any CAS Error-level log of a retried condition is a systemic CI red. Related: CAS-354, CAS-168.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each harness classified; findings recorded, with a follow-up if any fails on Error lines
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: unclear (low). Evidence: grep for '<Error>' in ci/, tests/clickhouse-test, tests/docker_scripts and integration helpers finds only the S3 mocks and one logical-error branch in tests/integration/helpers/cluster.py:4642; no generic fail-on-Error rule located. Changes: type unset -> research: the task is an investigation with a recorded finding as its outcome. Would settle it: how the stress job and the integration cluster teardown classify server Error-level log lines (read the stress log checker and cluster.py:4642 context).
---
<!-- COMMENTS:END -->
