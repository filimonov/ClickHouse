---
id: CAS-237
title: >-
  Page `deletePrefixWholesale` through `forEachListedKey` instead of its own
  `op.list` loop
status: To Do
assignee: []
created_date: '2026-07-13'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
milestone: m-4
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h
priority: low
type: chore
ordinal: 302000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`deletePrefixWholesale` (`CA/Gc/CasGc.cpp:3725-3750`) hand-rolls `op.list(prefix, cursor, kListPageLimit, ...)` pagination.
`forEachListedKey` (`CA/Backend/CasRequests.h:278`) now takes a `ListedKeyFn` returning `bool` (`:133`), the stop-on-true
hook this loop needed to stop at its object budget. It is the last hand-rolled listing loop in GC; the page hook
(`CasGc.cpp:84`) then counts its pages like every other listing. Called for the generation hand-off reclaim (`:1080`),
whose budget CAS-29.11 may change.

Provenance: BACKLOG/docs-and-cleanup.md#minor [C2-followups] (narrowed: the hook exists, `CasRefIntake.cpp` is gone); verified 2026-09-26 against 6eb16e1cc56 and 8d62c314ec1.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `deletePrefixWholesale` lists through `forEachListedKey` and stops at its budget via the callback
- [ ] #2 The existing wholesale-reclaim gtests pass unchanged and the page counter covers this listing
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
First recorded: 2026-07-13 (45a6c8ee2b6, by 'C2-followups')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Gc/CasGc.cpp:203 deletePrefixWholesale (defined at :193) still calls op.list(prefix, cursor, kListPageLimit, ...) in its own loop; no forEachListedKey in it. Changes: none. Pointers stale: CasGc.cpp:3725-3750 (now ~:193-240), :1080 caller (now ~:1445), :84 page hook not rechecked.
---
<!-- COMMENTS:END -->
