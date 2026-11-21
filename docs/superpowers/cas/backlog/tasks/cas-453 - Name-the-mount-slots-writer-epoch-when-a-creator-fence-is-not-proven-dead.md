---
id: CAS-453
title: Name the mount slot's writer epoch when a creator fence is not proven dead
status: To Do
assignee: []
created_date: '2026-11-11 16:16'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:late-request-audit'
  - 'impact:operability'
  - quick-win
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: low
type: enhancement
ordinal: 557000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`isCreatorFenceTerminal` (`CA/Pool/CasServerRoot.cpp`) answers false for a mount slot whose writer epoch is below the fence epoch. `reconcileStaleCreator` and `cancelStalledCreating` then refuse until the server root restarts, GC fences the slot, or decommission writes a farewell. The refusal text does not name the slot epoch, so an operator cannot tell a regressed slot from a live creator.
Unreachable in normal operation: the slot epoch is monotone and the fence is minted after the claim lands. Reachable only after operator deletion of the slot and epoch objects.
Provenance: CAS-364 review, tmp/noclock/drafts/prep/364/REVIEW.md (needs verification, sources 1, 12).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The refusal of a reconcile or cancel names the slot's writer epoch and the fence epoch (gtest on the message)
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
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: isCreatorFenceTerminal still exists (Pool/CasServerRoot.h:582); refusal text not re-read. Changes: impact:operability; label quick-win; type bug -> enhancement: unreachable-in-normal-operation diagnosability gap, not wrong behaviour. Would settle it: the current refusal text of reconcileStaleCreator / cancelStalledCreating (does it name the slot epoch).
---
<!-- COMMENTS:END -->
