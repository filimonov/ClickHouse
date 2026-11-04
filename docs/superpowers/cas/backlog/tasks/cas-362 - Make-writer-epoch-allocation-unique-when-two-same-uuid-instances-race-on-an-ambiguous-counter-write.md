---
id: CAS-362
title: >-
  Make writer-epoch allocation unique when two same-uuid instances race on an
  ambiguous counter write
status: Done
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-11-04 15:00'
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
- [x] #1 two allocators racing with an ambiguous first write never both return the same epoch (gtest)
- [x] #2 26.6.4 readers parse the new body (compat test)
- [x] #3 spec updated for the frozen format
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [x] #2 ASan lane green for touched suites
- [x] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [x] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-12: the unmerged lease series (PR 11a, `f95950cbd54` 'Allocate the CAS writer epoch as one immutable conditional write' on branch fix/antalya-26.6/cas-ownership-primitives-5) rewrites `allocateWriterEpoch` but still resolves an unclear outcome by comparing exact bytes, so it carries the same defect. If the series resumes, 11a must be rebased onto this fix (conflict in `allocateWriterEpoch`).

2026-10-12 model (`008991cc06f`, `CaEpochAllocCore`, base 2a034cdb374): `cur_twin`/`cur_twin_gcs` violate `EpochNeverReused` (ambiguous create, twin writes identical bytes, resolve says Committed); with #2512 the twin cannot write (`cur_twin_dangle` green) but the duplicate allocation stays. `fix_nonce` green (also GCS, restart: 167 M states); `sab_nonce` (resolve compares only the value) red. Format: `decodeServerEpoch` skips unknown keys in 26.6.4 (`JsonObjectReader::skipUnknown`, Tolerant), so a plain nonce field is compatible.

2026-10-14: its two commits ship as the first commits of PR B https://github.com/Altinity/ClickHouse/pull/2567; there is no separate PR.

2026-11-04: merged into antalya-26.6 as 9b348cf0a4b (https://github.com/Altinity/ClickHouse/pull/2567). Reds on the final run were base/infra: regression cas_alter_attach_3 (issue #2343), CASS3CacheLightweightDelete, and the cas s3 stateless lanes on `GroupingAggregatedTransform pushed bucket A twice` (not CAS). Delivered by PR B; closed on its merge.
<!-- SECTION:NOTES:END -->
