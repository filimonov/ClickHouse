---
id: CAS-415
title: >-
  A precommit leaked under a final part name blocks every Move onto that name
  until the stale-precommit sweep
status: To Do
assignee: []
created_date: '2026-10-22 20:18'
labels:
  - 'area:ref-ledger'
  - 'area:write-path'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:probable'
  - 'origin:review'
milestone: m-8
dependencies:
  - CAS-346
priority: medium
type: task
ordinal: 518000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`moveRefIfMatches` (`Parts/PartFolderAccess.cpp`) refuses a destination that has a live precommit (`CAS_OWNERSHIP_CONFLICT`, "destination has a live precommit"); `republishRef` tolerated it. A precommit can leak under a final name when a publish is interrupted in the same epoch before the catch/abandon path runs. Until the stale-precommit sweep reclaims it, every rename onto that name conflicts and the replication queue retries.

Pin the facts: what the sweep horizon is (epoch change? time? GC round?), whether a rename re-drive can meet a leaked precommit in practice, and whether `moveRefIfMatches` should distinguish a precommit from a dead epoch from a live one. Source: second umbrella review of `feature/cas-disk-transactions`, 2026-10-22 (Needs verification #1, minor 7).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The sweep horizon for a leaked precommit is stated in `Parts/PartFolderAccess.h` next to `moveRefIfMatches`
- [ ] #2 gtest: a Move onto a name with a leaked precommit conflicts, and succeeds after the sweep
- [ ] #3 Decision: whether a precommit from a dead writer epoch is treated as absent by the Move builder
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
