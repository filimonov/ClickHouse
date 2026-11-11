---
id: CAS-454
title: >-
  Bind the `Live -> Removing` catalog write to the writer's mount generation so
  a late copy cannot land after a remount
status: To Do
assignee: []
created_date: '2026-11-11 16:16'
labels:
  - 'area:ref-ledger'
  - 'complexity:large'
  - 'risk:medium'
  - 'touches:format'
  - 'needs:decision'
  - 'origin:late-request-audit'
dependencies: []
priority: low
type: bug
ordinal: 558000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`dropNamespaceImpl` (`CA/Pool/CasRefLedger.cpp`) keeps the write lane closed when the outcome of `beginRemoving` is unknown. The in-memory flag dies with the runtime: after a remount the lane is open again, and a wire request of the old mount generation can still land and move the row to `Removing` under live writes.
A durable fix needs a format change: a catalog revision or a `removal_by` field carrying the mount generation, checked by whoever acts on a `Removing` row. The catalog format is frozen since 26.6.4, so this is a versioned change with a compatibility plan.
Decision of CAS-367 acceptance criterion 3: not done in the late-request audit bundle; the lane stays closed until a retried removal completes or the disk is remounted.
Provenance: tmp/noclock/drafts/prep/367/REVIEW.md (needs verification).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A `Live -> Removing` write sent by a mount generation that has since been replaced cannot retire a table the new generation writes to (model or gtest)
- [ ] #2 The format change has a compatibility plan for pools written by older binaries
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
