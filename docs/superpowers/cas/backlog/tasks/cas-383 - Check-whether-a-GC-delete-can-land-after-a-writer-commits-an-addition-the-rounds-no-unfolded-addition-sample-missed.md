---
id: CAS-383
title: >-
  Check whether a GC delete can land after a writer commits an addition the
  round's 'no unfolded addition' sample missed
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:speculative'
  - 'risk:high'
  - 'needs:repro'
  - 'origin:noclock-audit'
  - 'impact:data-loss'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - docs/superpowers/models/CaGcRoundDeferCore.tla
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-gclease.md
priority: medium
type: research
ordinal: 477000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CaGcRoundDeferCore` forbids the schedule "sample no unfolded addition → writer commits an addition → DELETE lands" through an atomic guard the code does not have, so its green result does not validate the real protocol. Find the code protection (round floor, in-degree re-read, exact token) or the interleaving. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-gclease.md (finding 1). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 verdict with the protecting happens-before or the interleaving; model guard replaced by the real one
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
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: docs/superpowers/specs/2026-11-19-cas-gc-body-delete-zero-proof.md (bf0fe3f20f9) states a round can include a later event of one namespace and miss an earlier one, and 'three schedules remove a body that a committed part still names', reproduced by unit tests on the real fold; status: proposal, not implemented. Changes: impact:data-loss. Links: related: spec 2026-11-19-cas-gc-body-delete-zero-proof.md (addition veto is the proposed fix); CaGcRoundDeferCore.tla guard still to be replaced by the real one. Would settle it: whether the task's exact interleaving (sample no unfolded addition, writer commits, DELETE lands) is one of those three schedules; the task's research verdict is still unwritten.
---
<!-- COMMENTS:END -->
