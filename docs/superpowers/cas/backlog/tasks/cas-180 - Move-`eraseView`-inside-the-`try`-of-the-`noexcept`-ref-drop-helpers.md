---
id: CAS-180
title: Move `eraseView` inside the `try` of the `noexcept` ref-drop helpers
status: To Do
assignee: []
created_date: '2026-08-24'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:read-path'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - R/Parts/PartFolderAccess.cpp
priority: medium
type: bug
ordinal: 237000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CachedPartFolderAccess::dropRefBestEffort` (`R/Parts/PartFolderAccess.cpp:598-616`) and `dropRefIfMatches` (`:618-677`) are `noexcept` and call `eraseView` after their `try`.
`eraseView` (`:277-284`) builds `key.cacheKey()` and calls `recordDecision`, both allocating. A memory-limit exception there terminates the process.
These run on the rollback path under memory pressure, where allocation is most likely to fail.

Provenance: BACKLOG/ref-protocol.md#noexcept-ref-drop-allocates (opus review NV-5). Verified 2026-09-26 against b1c34d03479 and 0dbbd797792. Same class: CAS-113, noexcept-destructor-allocation-nits.

Update 2026-11-21: Remaining scope is `dropRefBestEffort` only (`PartFolderAccess.cpp:782-801`): `dropRefIfMatches` already runs `eraseView` under `SCOPE_EXIT_MEMORY_SAFE` via `dropRefExact`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Both helpers invalidate the view inside a `try`, and an injected allocation failure in `eraseView` does not terminate (gtest)
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
First recorded: 2026-08-24 (45efac389eb, by 'noexcept-ref-drop-allocates')

2026-10-22: CAS-408 folds `dropRefIfMatches` onto `dropRefExact`, which runs `eraseView` from `SCOPE_EXIT_MEMORY_SAFE`; `dropRefBestEffort` is untouched by it. Verified on `feature/cas-disk-transactions` @ 7a16106160c (`PartFolderAccess.cpp:836-840`).
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: Parts/PartFolderAccess.cpp:782-801 dropRefBestEffort is noexcept and still calls eraseView (:800) after its try; dropRefIfMatches (:803) now delegates to dropRefExact which runs eraseView in SCOPE_EXIT_MEMORY_SAFE (:832). Changes: description: Update paragraph appended. Pointers stale: line numbers moved; dropRefIfMatches no longer has the problem.
---
<!-- COMMENTS:END -->
