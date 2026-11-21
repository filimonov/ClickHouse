---
id: CAS-190
title: >-
  Guard `nextRefTxnId` against `ref_sequence` overflow like its sibling in the
  checkpoint walk
status: To Do
assignee: []
created_date: '2026-08-04'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:ref-ledger'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:obsolete'
  - 'groom-20261121:certainty-high'
dependencies: []
references:
  - R/Pool/CasRefProtocol.cpp
priority: low
type: chore
ordinal: 247000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`nextRefTxnId` (`R/Pool/CasRefProtocol.cpp:589-594`) returns `ref_sequence + 1` unguarded.
The checkpoint-bounded walk throws `CORRUPTED_DATA` on the same step at `UINT64_MAX` (`:940`, `:947`).
Add the guard, or document at the function why the bound is unreachable. Same finding as 2031-triage cluster C-0514.

Provenance: BACKLOG/ref-protocol.md#ref-protocol-ledger [reftxnid-wraparound-guard-missing]. Verified 2026-09-26 against b1c34d03479 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `nextRefTxnId` at `ref_sequence == UINT64_MAX` throws `CORRUPTED_DATA` (gtest), or a comment proves it unreachable
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
First recorded: 2026-08-04 (967849c785b, by 'reftxnid-wraparound-guard-missing')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: obsolete (high). Evidence: nextRefTxnId no longer exists (grep empty); its successors in Pool/CasRefProtocol.cpp:89-92 and :151-154 throw CORRUPTED_DATA at ref_sequence == UINT64_MAX before + 1. Changes: none.
---
<!-- COMMENTS:END -->
