---
id: CAS-413
title: Drop the unreachable `refMoveAllowed()` routing arm in `moveDirectory`
status: To Do
assignee: []
created_date: '2026-10-22 07:59'
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
- [ ] #1 `moveDirectory` has one path for a same-namespace rename of a committed part (owner Move); `CASWiringOps.*OwnerMove*` and 05386-05392 stay green
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
