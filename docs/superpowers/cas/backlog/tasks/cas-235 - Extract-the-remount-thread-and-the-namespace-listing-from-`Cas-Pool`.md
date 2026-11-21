---
id: CAS-235
title: 'Extract the remount thread and the namespace listing from `Cas::Pool`'
status: To Do
assignee: []
created_date: '2026-07-13'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:mounts'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - 'complexity:small'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-4
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp
documentation:
  - docs/superpowers/cas/umbrella-roadmap.md
priority: low
type: chore
ordinal: 300000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CA/Pool/CasPool.cpp` + `.h` = 3461 lines. The caches and the ref-log lane already moved to `CasManifestReader` and
`CasRefLedger`; the remount thread is still inline. Unclaimed small candidates: `listNamespaces` / `listMirroredChildren`
(`CasPool.h:618,624`, ~112 lines) and `reportImpossibleInterference` (`CasPool.h:933`; its former partner
`peekForeignRefLogHeader` no longer exists). Low value alone: the composition root is sound, the file stays dominated by
the mount protocol.

Provenance: BACKLOG/docs-and-cleanup.md#refactoring [refactor: Store de-god-classing] (formerly [source-layout-casstore-followups]); verified 2026-09-26 against 6eb16e1cc56 and 8d62c314ec1.

Update 2026-11-21: Remaining scope: the remount thread now lives in Pool/CasMountRuntime (remountLoop, CasMountRuntime.cpp:724); only listNamespaces / listMirroredChildren (CasPool.h:675) and reportImpossibleInterference still sit in CasPool. The 2026-10-09 note that the remount thread was deleted does not match this branch.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The remount thread and the namespace listing live outside `CasPool.cpp`, with no behavior change
- [ ] #2 The `CAS*` gtest gate is green on the committed tree after the move
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
First recorded: 2026-07-13 (45a6c8ee2b6, by 'refactor: Store de-god-classing')

PR 15 (fix/antalya-26.6/cas-remove-remount-machine, c64cf779b15) deleted the remount thread instead of moving it: `tryRemountOnce` and `remountLoop` are gone. Only the namespace listing half remains.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: rewrite (high). Evidence: remountLoop is in Pool/CasMountRuntime.h:491 / CasMountRuntime.cpp:724 (a separate unit, not CasPool.cpp), so the remount half is already extracted; listNamespaces is still declared in CasPool.h:675; CasPool.cpp+h are 3914 lines. Changes: size medium -> small (S): only the namespace listing (and reportImpossibleInterference) remains to move; description: Update paragraph appended. Size: S. Pointers stale: CasPool.h:618,624,933 moved (listNamespaces now :675); 3461 lines is now 3914 across .cpp+.h.
---
<!-- COMMENTS:END -->
