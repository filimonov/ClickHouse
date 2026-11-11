---
id: CAS-458
title: 'Use one GC lease probe for the janitor, fold and ref cleanup'
status: To Do
assignee: []
created_date: '2026-11-11 17:18'
labels:
  - 'area:gc'
  - 'origin:review'
  - 'complexity:small'
  - 'risk:low'
dependencies:
  - CAS-29.8
priority: low
type: feature
ordinal: 562000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Gc::refreshAuthority` reads `gc/state`, compares owner and seq, and publishes `authority_held`, which the janitor and fold jobs pass as the fence of their `admit`/`resume` ops. Ref cleanup (`licenceForChunk` in `CA/Gc/CasGc.cpp`, added by CAS-29.8) opens `admit` with no fence and repeats the read-and-compare inline against `adopted_lease` before each chunk. Two definitions of "this GC still leads" can drift when the lease tuple gains a field. The unfenced op also means the engine cannot refuse the delete request itself after the lease is lost; only the per-chunk check does. Behaviour is fail-closed today; this is a structure follow-up.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Ref cleanup's lease check and `Gc::refreshAuthority` share one comparison of the lease tuple
- [ ] #2 A written decision on whether ref cleanup deletes run under a fenced op, with the reason
- [ ] #3 The `CASRefCleanup*` stop reasons and their tests keep their meaning
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
