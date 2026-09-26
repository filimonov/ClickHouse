---
id: CAS-107
title: >-
  Build a validated GC-cadence and snapshot-size model for sizing a production
  pool
status: To Do
assignee: []
created_date: '2026-09-26 07:23'
labels:
  - 'area:gc'
  - 'area:docs'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:soak'
  - 'origin:otel-demo-audit'
milestone: m-0
dependencies: []
references:
  - docs/en/antalya/cas/configuration.md
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
  - docs/superpowers/cas/umbrella-roadmap.md
priority: medium
type: research
ordinal: 145000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Operators have no way to predict GC round time, snapshot size or S3 request budget from their workload. The defaults review before the first deployment needs one (roadmap §7).
Data points: a live-AWS round took 30-40 s; audit F29 shows the GC snapshot run growing 11-12 MiB -> 125 MiB per round with the condemned backlog, rewritten every round.

Provenance: BACKLOG/performance.md#scale-findings [Capacity model]. Verified 2026-09-26 against dd0ed2f263a.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A model gives round duration, snapshot bytes and S3 requests per day as functions of parts, blobs and churn, with its assumptions stated
- [ ] #2 The model is checked against at least two measured pools (otel.demo and a soak run) within a stated error
- [ ] #3 The operator docs carry the resulting sizing guidance
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
