---
id: CAS-376
title: >-
  Check whether GC rebuild can adopt incompatible proof while an old catalog
  erase is still in flight
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:gc'
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'confidence:speculative'
  - 'risk:medium'
  - 'needs:repro'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-catalog.md
  - docs/superpowers/models/CaRefPreFoldDrainCore.tla
priority: medium
type: research
ordinal: 470000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Codex (inferred): an old GC keeps a catalog erase in flight; authority becomes unreadable; rebuild adopts incompatible proof; the old erase lands on an unchanged catalog token. `CaRefPreFoldDrainCore:LoseAuthority` excludes this trace by construction; allowing a late landing should fail `DeleteUsesCurrentAdoptedProof`. Healthy takeover remains token/drain protected. Confirm or refute in code and model. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-catalog.md.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 verdict with the interleaving or the excluding happens-before
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
