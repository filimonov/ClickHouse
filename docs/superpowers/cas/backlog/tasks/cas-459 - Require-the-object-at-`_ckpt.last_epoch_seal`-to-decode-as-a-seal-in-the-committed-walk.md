---
id: CAS-459
title: >-
  Require the object at `_ckpt.last_epoch_seal` to decode as a seal in the
  committed walk
status: To Do
assignee: []
created_date: '2026-11-11 17:18'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
  - 'risk:low'
dependencies:
  - CAS-366
priority: low
type: feature
ordinal: 563000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Optional hardening after CAS-366. The frontier refusal trusts `_ckpt.last_epoch_seal` as the id of an epoch seal. The only counter-state found is a fabricated one: durable `committed_through = last_epoch_seal` with the seal object deleted and no snapshot. Ref cleanup cannot produce it. The committed walk in `CA/Pool/CasRefLedger.cpp` could require the object at `_ckpt.last_epoch_seal`, when it is above the grounding snapshot, to decode as a seal and report `CORRUPTED_DATA` otherwise. Do it only with a state that real code can reach, or measure the extra read first.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A reachable history where the check changes the outcome, or a written decision to drop the task
- [ ] #2 If implemented: a gtest with a non-seal object at `_ckpt.last_epoch_seal` sees `CORRUPTED_DATA`; no extra read on the path where the snapshot covers the seal
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
