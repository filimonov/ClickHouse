---
id: CAS-416
title: >-
  Consistency chores after the owner-Move series: shared nowMs and
  SetPublishedAt builder, Move stamp from changePoints, one reader-ceiling gate,
  one scratch-manifest predicate
status: To Do
assignee: []
created_date: '2026-10-22 20:49'
labels:
  - 'area:write-path'
  - 'area:format'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-4
dependencies:
  - CAS-346
priority: low
type: chore
ordinal: 519000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Verified 2026-10-22 on `feature/cas-disk-transactions` (consistency review, findings 1, 6, 7):

- `uint64_t nowMs()` is defined three times (`Parts/PartFolderAccess.cpp`, `Pool/CasPartWriteTxn.cpp`, `Tools/CasDecommission.cpp`); upstream has `timeInMilliseconds` in `src/Common/DateLUT.h`. The four-field `SetPublishedAt` op is hand-built three times (`PartFolderAccess.cpp` moveRefIfMatches, `CasPartWriteTxn.cpp` promote, `CasRefLedger.cpp`).
- `changePoints(FormatId::RefLog) = {{1,1},{2,2}}` and `G_REF_MOVE = 2` state one fact twice; `encodeRefLogTxn` stamps the literal. The pool-meta floor gate (`Formats/CasPoolMetaFormat.cpp`, "pool requires reader generation") compares against `G_BUILD` directly, so `setReadCeilingForTest` cannot lower it and only the integration test with a real 26.6.4 binary covers the refusal; the `checkCompatibility` docstring in `CasFormat.h` still says "exceeds G_BUILD" while the code uses the ceiling.
- The nested text-index scratch manifest is detected twice with different predicates: `publishStaging` by `hasTopLevelFile`, `moveDirectory` (staged-source re-key) by an unconditional `dropRefIfPresent(src)`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 One `Cas::nowMs` (or `timeInMilliseconds`) and one `makeSetPublishedAtOp(ref_name, expected_manifest_ref, published_at_ms)` next to `RefOp`; the three sites use them (`MoveRefIfMatchesRestampsPublishedAt` stays green)
- [ ] #2 The Move stamp is derived from `changePoints(FormatId::RefLog).back()` or pinned to `G_REF_MOVE` by a `static_assert`; the pool-meta gate goes through the same ceiling function; the `checkCompatibility` docstring matches the code
- [ ] #3 One named predicate for a nested scratch manifest used by both drop sites
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
