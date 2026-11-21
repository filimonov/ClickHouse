---
id: CAS-493
title: >-
  Use `walkRefChain` in the GC ref intake and one restart loop for read-only
  walks
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:high'
  - 'confidence:solid'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies:
  - CAS-487
priority: low
type: chore
ordinal: 598000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The GC ref intake has its own chain walk instead of `walkRefChain`. The read-only "base moved, re-read `_ckpt`, retry" loop exists in three places. Three `KeyReader` decorators repeat the same overrides. Remove the duplicates, with no behaviour change. Follow-up of CAS-487.
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
2026-11-15: treat together with CAS-492 as one refactor (shared durable-write window; walkRefChain in the GC ref intake; one restart loop for read-only walks; one recording KeyReader). No behaviour change in either.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1: walkRefChain callers are Pool/CasRefProtocol.cpp:1268 and Gc/CasOrphanManifestSweep.cpp:356 only; Gc/CasGc.cpp has no walkRefChain call, so the GC ref intake still has its own walk. Changes: area set to gc; size unset -> medium (M): refactor of the GC intake walk under safety care, plus two other dedups; risk set to high; confidence set to solid; type unset -> chore: duplicate removal, no behaviour change; priority unset -> low: no priority set; refactor. Size: M. Links: Treat with CAS-492 as one refactor.
---
<!-- COMMENTS:END -->
