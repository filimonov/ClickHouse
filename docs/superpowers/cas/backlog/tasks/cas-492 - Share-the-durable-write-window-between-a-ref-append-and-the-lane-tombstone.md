---
id: CAS-492
title: Share the durable-write window between a ref append and the lane tombstone
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:ref-ledger'
  - 'complexity:medium'
  - 'risk:high'
  - 'confidence:plausible'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies:
  - CAS-487
priority: low
type: chore
ordinal: 597000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ensureEpochOpenForAppend` re-implements the arming, wedge, conflict adjudication and install steps of `commitRefChunk`. `resolveWedgeOnce` holds a third copy of the occupant verdicts. The copies agree today. Merge them into one durable-write window, with no behaviour change. Follow-up of CAS-487.
<!-- SECTION:DESCRIPTION:END -->

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
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 Pool/CasRefLedger.h:1344 ensureEpochOpenForAppend and :1380 resolveWedgeOnce both exist beside commitRefChunk; I did not diff the three copies. Changes: area set to ref-ledger; size unset -> medium (M): refactor of the durable-write path under GC-safety care; risk set to high; confidence set to plausible; type unset -> chore: duplicate removal, no behaviour change; priority unset -> low: no priority set; refactor. Size: M. Links: Treat with CAS-493 as one refactor (note on CAS-493). Would settle it: Whether the three copies still duplicate arming, wedge and install steps.
---
<!-- COMMENTS:END -->
