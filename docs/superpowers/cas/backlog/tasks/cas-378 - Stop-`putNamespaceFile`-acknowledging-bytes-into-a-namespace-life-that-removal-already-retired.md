---
id: CAS-378
title: >-
  Stop `putNamespaceFile` acknowledging bytes into a namespace life that removal
  already retired
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
updated_date: '2026-10-12 22:55'
labels:
  - 'area:ref-ledger'
  - 'area:write-path'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:high'
  - 'needs:repro'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasNamespaceJanitor.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-ref.md
priority: high
type: bug
ordinal: 472000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A cold reader observes the catalog row `Live`; removal and drain retire that life before the runtime is inserted; the delayed insertion republishes it. A warm `namespaceLife` then returns the retired life, and `putNamespaceFile` can acknowledge bytes that the dead-life janitor (`Gc/CasNamespaceJanitor.cpp`) later deletes. Acknowledged table-file loss. The ref models make the cold read and the runtime insertion one step. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-ref.md (finding 1). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 interleaving reproduced by a gtest with a hook between the cold read and the runtime insertion, or refuted with the excluding happens-before
- [ ] #2 fix: insertion re-validates the life against the catalog, or the retired life refuses writes
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
2026-10-13 recheck on the release line 2a034cdb374 (docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/wave2-recheck-release-line.md): EXISTS and worse. Cold `acquireReadableRefTableRuntime` sees the row Live at its second catalog GET (no lock); before `acquireRefTableRuntime` takes `ref_queue_mutex`, drop appends the terminal record, GC drains the row, `invalidateRemovedCatalogLife` erases the slot; insertion finds an empty slot with an unchanged fence generation and publishes a runtime for the dead life (same gap on the cold mutable path of `namespaceLife`). Warm `namespaceLife` then returns the dead life with no lifecycle/catalog check: `putNamespaceFile` acknowledges bytes the janitor deletes; inferred: a ref append prepends `NamespaceBirth` (allowed after `Removed`) and a part could be promoted into a swept life. Fix: keep a per-name retirement record in `RefNameSlot` instead of erasing it; cold paths capture it before the first GET and insertion refuses if it moved; warm `namespaceLife` refuses a terminal runtime. Gtest: backend subclass whose `read` callback runs drop + GC drain after the base read (like `MountSlotRaceBackend`).
<!-- SECTION:NOTES:END -->
