---
id: CAS-146
title: >-
  Attribute the ~200k manifest PUTs per day on otel.demo that neither part
  publishes nor repoints explain (audit F8)
status: To Do
assignee: []
created_date: '2026-09-26 07:39'
updated_date: '2026-09-26 08:34'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
milestone: m-2
dependencies: []
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f8
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#verification-items
priority: high
type: research
ordinal: 188000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CASManifestPut` is 592k/day on one otel.demo replica, against 197k parts written plus 191k `delete_tmp` repoints = 388k.
The other ~200k/day are unattributed: about one extra manifest PUT per part publish. Candidates: a staged body plus a
promoted body per publish, or `tmp_` parts that never commit. F31 plans a publish with exactly one manifest PUT,
so an unexplained second one changes that plan's arithmetic.
Method from the audit: group `cas_log` `manifest_put` events by `ref_name` pattern.

Provenance: BACKLOG/gc.md#otel-demo-s3-budget-audit-2026-09-25 (F8, one of the nine findings not threaded elsewhere). Related: u11-refproto:#part-publish-zero-gets, CAS-83. Verified 2026-09-26: no unit or BACKLOG file carries F8.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A `cas_log` query on the stand splits manifest PUTs per day by cause and the causes sum to `CASManifestPut` within 5%
- [ ] #2 Each cause that is not one-per-publish or one-per-repoint gets a follow-up task or is recorded as expected
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
