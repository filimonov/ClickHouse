---
id: CAS-382
title: 'GC lease: heartbeat progress does not invalidate a contender''s steal'
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:low'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-gclease.md
priority: low
type: bug
ordinal: 476000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A contender reads a frozen heartbeat, the incumbent pulses, the contender's `gc/state` CAS still succeeds: `NoFalseSteal` fails. Liveness only: the state-token fence still protects the displaced leader's publication. `CaGcLeaseCore`'s `hbAdvanced` rule is stricter than the code. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-gclease.md (finding 3). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 either condition the steal on the observed heartbeat token or relax the model rule and document it
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
