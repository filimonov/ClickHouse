---
id: CAS-368
title: >-
  Mint a fresh incarnation on each retry of namespace-create step 1 so catalog
  ETag ABA cannot revive a cancelled life
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:medium'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasNamespaceJanitor.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/catalog.md
priority: medium
type: bug
ordinal: 462000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ref_catalog` has no revision field, so add-then-cancel restores identical bytes and the same content ETag. `writeLoop` resends the same bytes with the same `If-Match` (and a proxy can duplicate a PUT), so a late attempt can bring back a cancelled `Creating` life. The janitor then deletes only objects it listed before the rebirth whose ETag is unchanged — in practice the reborn life's `_ckpt`, leaving the table unrecoverable (`CORRUPTED_DATA`). No user data loss; GCS is immune. Snapshot/`_ckpt` SK9 depends on this.

Fix (no format change): mint a fresh incarnation on every retry of `createNamespaceStep1` (~40 lines). Does not cover a proxy duplicating one request; a catalog revision field is the durable fix (next format). `CaRefCatalogCore` models the token as a flag and cannot express ABA. Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/catalog.md (T5), docs/superpowers/cas/2026-10-12-late-request-audit/objects/snapckpt.md (SK9). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 retried step 1 never re-sends identical catalog bytes (gtest)
- [ ] #2 model tracks the catalog value and a late-apply action
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
