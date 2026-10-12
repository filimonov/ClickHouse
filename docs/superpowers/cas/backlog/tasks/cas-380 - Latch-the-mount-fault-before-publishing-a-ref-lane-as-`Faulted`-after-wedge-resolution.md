---
id: CAS-380
title: >-
  Latch the mount fault before publishing a ref lane as `Faulted` after wedge
  resolution
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
labels:
  - 'area:ref-ledger'
  - 'area:mounts'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:medium'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-ref.md
priority: medium
type: bug
ordinal: 474000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the lease-series tip `3b4037ae265` wedge resolution publishes `Faulted`, releases `state_mutex`, then latches the mount fault. Another table can certify or admit in that interval. `CaRefLaneCore:FaultHasExit` is atomic and masks it. Reorder: latch first, then publish. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-ref.md (finding 3). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 model splits publish and latch and fails on the current order
- [ ] #2 code latches before publishing
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
