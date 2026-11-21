---
id: CAS-71.4
title: Route GC's completed-removal catalog erase through the hot-key lane
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:high'
  - 'touches:protocol'
  - 'confidence:plausible'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
dependencies:
  - CAS-71.1
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CatalogLifecycleReconciler.cpp
documentation:
  - docs/superpowers/specs/2026-09-04-cas-hot-key-write-lane-design.md
parent_task_id: CAS-71
priority: medium
type: feature
ordinal: 95000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CasRefCatalog::deleteCompletedRemovingAtSnapshot` writes with `op.replace(layout.refCatalogKey(), ...)` (`R/Pool/CasRefCatalog.cpp:516`), racing the lane and costing it one stale cache entry per erase.
Target: the body from "refresh authority" through the `replace` becomes one `submit` whose `decide` refreshes authority, checks `op.admitted()`, `throwIfAmbiguous` and the exact row. The resolution read after `Committed` stays authoritative.
Prerequisites: the `gc/state` refresh reads through the erase's own operation so the clamp reaches it; the `decide` keeps the absent-catalog refusal; `CompletedRemovingDeleteResult` still needs the catalog cut for `FencedOut`/`EntryChanged`. The operation carries a `Liveness` closure, so it never combines.
The authority gap across the engine's internal reissue (a TTL on the GC lease) stays a separate item.

Provenance: BACKLOG/performance.md#hot-key-lane-phase-b item 4. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The erase goes through `submit`; `FencedOut` and `EntryChanged` results still carry the catalog cut
- [ ] #2 A gtest interleaving a lane write and a GC erase shows no stale cache entry after the erase
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
First recorded: 2026-09-26 (b1c34d03479, by 'hot-key-lane-phase-b item 4')

2026-11-11 defaults review: the catalog erase outside the lane stops GC completely under steady CREATE/DROP (12 of 13 rounds aborted in run `ddl16`); see the new bug task filed today for the numbers.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: Pool/CasRefCatalog.cpp:532 deleteCompletedRemovingAtSnapshot (defined :459) still writes via op.replace(layout.refCatalogKey(), ...); the hot-key lane submit is used only at :192. Changes: none. Links: depends on CAS-71.1 (already stated); the 2026-11-11 note refers to a new bug task for GC aborts under CREATE/DROP. Pointers stale: CasRefCatalog.cpp:516 moved to :532.
---
<!-- COMMENTS:END -->
