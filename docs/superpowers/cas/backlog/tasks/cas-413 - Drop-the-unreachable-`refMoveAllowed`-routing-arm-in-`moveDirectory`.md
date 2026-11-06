---
id: CAS-413
title: Drop the unreachable `refMoveAllowed()` routing arm in `moveDirectory`
status: Done
assignee: []
created_date: '2026-10-22 07:59'
updated_date: '2026-11-06 23:12'
labels:
  - 'area:write-path'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-4
dependencies:
  - CAS-346
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp
priority: low
type: chore
ordinal: 516000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8) `ContentAddressedTransaction::moveDirectory` still branches on `pool.refMoveAllowed()` when choosing between an owner `Move` and `republishRef` for a same-namespace rename of a committed part. Since the writable mount always raises the floor to `G_REF_MOVE` and `moveRefIfMatches` keeps its own fail-closed guard ('pool floor forbids'), the republish arm is unreachable on a writable pool; it was kept only to match the spec of the series. Remove the arm and its two tests, keep the guard in `moveRefIfMatches`.

Provenance: umbrella review of the owner-Move series, 2026-10-22 (Ockham #3, architecture #2, tests #11); simplification plan item 3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 `moveDirectory` has one path for a same-namespace rename of a committed part (owner Move); `CASWiringOps.*OwnerMove*` and 05386-05392 stay green
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
2026-11-07 audit: merged in https://github.com/Altinity/ClickHouse/pull/2647 as d8d64d936e2 (antalya-26.8, 2026-10-27).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Done on `feature/cas-disk-transactions` (2026-10-23): the move-vs-republish decision now lives in `CachedPartFolderAccess::renameCommittedRef`; inside one namespace a free destination is one `Move`, a committed destination (crashed re-drive) republishes; the `refMoveAllowed()` arm is gone — a writable 26.8 mount always has floor ≥ 2 and a read-only mount never renames. Proven by the failed attempt to test it: a floor-1 pool can only be opened read-only by this build (writer_epoch 0), so no writable floor-1 rename exists. `moveRefExact` keeps `chassert(store->refMoveAllowed())`. `poolAccess()` is private again.
<!-- SECTION:FINAL_SUMMARY:END -->
