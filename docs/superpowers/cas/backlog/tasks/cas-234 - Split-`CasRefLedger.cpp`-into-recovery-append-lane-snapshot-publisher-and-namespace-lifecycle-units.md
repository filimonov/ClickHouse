---
id: CAS-234
title: >-
  Split `CasRefLedger.cpp` into recovery, append lane, snapshot publisher and
  namespace lifecycle units
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:ref-ledger'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
  - 'complexity:epic'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-4
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
documentation:
  - docs/superpowers/cas/umbrella-roadmap.md
priority: low
type: chore
ordinal: 299000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CA/Pool/CasRefLedger.cpp` is 5353 lines, the largest CA file. Roadmap §5: split into recovery/cache, append lane and wedge
protocol, snapshot publisher, namespace lifecycle, and a thin facade, by moving code, not rewriting it.
The ledger is the ref-safety core, so equivalence fences go in first.

Provenance: BACKLOG/docs-and-cleanup.md#refactor-candidates-from-defects item 3 (CasRefLedger half) and roadmap §5; verified 2026-09-26 against 6eb16e1cc56 and 8d62c314ec1.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Equivalence tests for recovery, append and snapshot publication exist and pass before any code moves
- [ ] #2 `CasRefLedger` is a facade over the four units, and no protocol step or on-S3 format changes
- [ ] #3 The `CAS*` gtest gate and the ASan lane are green on the committed tree after the move
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
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `CasRefLedger` is 2026-07-15 (416c982b5d6); the finding itself has no record before this migration, so the creation date stays 2026-09-26.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: Pool/CasRefLedger.cpp is one file of 6002 lines (task said 5353); no CasRefLedger split units in Pool/. Changes: size large -> epic (XL): 6000-line safety-core file split under equivalence fences must be split into per-unit subtasks before work. Size: XL. Pointers stale: file is now 6002 lines.
---
<!-- COMMENTS:END -->
