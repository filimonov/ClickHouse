---
id: CAS-435
title: >-
  Backport PR 2730 to antalya-26.6: orphan sweep nominates a rowless manifest
  with no floor check
status: To Do
assignee: []
created_date: '2026-11-05 13:00'
updated_date: '2026-11-05 13:28'
labels:
  - 'area:gc'
  - 'origin:ci'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
priority: high
type: bug
ordinal: 538000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
https://github.com/Altinity/ClickHouse/pull/2730 (antalya-26.8, k-morozov) closes a data-loss race in `planManifestCursorPage` (`Gc/CasOrphanManifestSweep.cpp`): a manifest whose namespace has no catalog row in the page's catalog cut was nominated at once, with no mount-floor check, on the premise that a rowless manifest belongs to a dead life. A writer stages a manifest before `precommitAdd` creates the row, so a namespace being created or recreated lost its first manifest. The fix applies the floor to rowless namespaces and re-reads the catalog (`K1`) after the floor observations, retaining any candidate whose row appeared or changed.

The same planner code is on antalya-26.6, and the race was seen there: https://github.com/Altinity/ClickHouse/pull/2573, TSan integration shard 6/6, `test_cas_replicated_relink` twice, node2's orphan sweep deleted the receiver's freshly written relink precommit manifest 100-300 ms after the write, while the `ref_catalog` write was retrying a 412; the fetch fell back to a byte copy. Logs under `tmp/pr2573/` of the master worktree.

Cherry-pick the seven non-merge commits of PR 2730 onto antalya-26.6 and run `CASOrphanManifestSweep*`, `CASOrphanSweepRequests*`, `CASSweepDeletionPremise*` plus `test_cas_replicated_relink` under TSan.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 PR 2730 is merged into antalya-26.8
- [ ] #2 A backport PR lands the same change on antalya-26.6 with the new gtests green
- [ ] #3 `test_cas_replicated_relink` passes under TSan on antalya-26.6 without the byte-copy fallback in the node2 log
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
2026-11-05: PR 2730 merged into antalya-26.8 as 856ca70a4ff (13:24 UTC). Remaining: the antalya-26.6 backport (AC#2, #3).
<!-- SECTION:NOTES:END -->
