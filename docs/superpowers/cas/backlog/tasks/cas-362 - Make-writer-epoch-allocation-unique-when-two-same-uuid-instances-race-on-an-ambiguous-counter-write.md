---
id: CAS-362
title: >-
  Make writer-epoch allocation unique when two same-uuid instances race on an
  ambiguous counter write
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:mounts'
  - 'touches:on-s3-format'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md
priority: high
type: bug
ordinal: 456000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`allocateWriterEpoch` (`Pool/CasServerRoot.cpp`) writes `{N+1}` to `gc/server-roots/<srid>/epoch` by `If-Match`. A times out ambiguously; B writes identical bytes; A's resolve read in `writeLoop` (`Backend/CasRequests.cpp`) sees a changed ETag with bytes equal to its own and returns `Committed`. Both get epoch `N+1`. Same on the create path. No pause needed: a twin plus one timeout. On the base `claimMount` then refreshes the same pair (MT2) so both hold the slot until a renew conflict (≤ P). The model already shows it: `W_SameBytesCollision`, `_sab_refreshownpair_immalloc` violates `ClaimNeverRegressesEpoch`.

Fix: add a random allocation nonce to the `epoch` body (~15 lines). Old readers tolerate the extra JSON field (verify before landing). Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md (S1, S1c, S4). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 two allocators racing with an ambiguous first write never both return the same epoch (gtest)
- [ ] #2 26.6.4 readers parse the new body (compat test)
- [ ] #3 spec updated for the frozen format
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
