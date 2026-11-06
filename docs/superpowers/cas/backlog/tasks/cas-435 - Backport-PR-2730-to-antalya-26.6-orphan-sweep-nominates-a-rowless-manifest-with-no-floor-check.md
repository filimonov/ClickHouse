---
id: CAS-435
title: >-
  Backport PR 2730 to antalya-26.6: orphan sweep nominates a rowless manifest
  with no floor check
status: In Progress
assignee: []
created_date: '2026-11-05 13:00'
updated_date: '2026-11-06 23:12'
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

2026-11-06: local branch port/antalya-26.6/cas-from-26.8 (worktree wt-266-lifecycle, base 52c95e21002) carries the -m 1 cherry-pick of 856ca70a4ff as e7b45306ba3 (conflicts: CasOrphanManifestSweep.cpp keeps the min_active_build_sequence == max sentinel test and the 26.6 mountRequests admission in sweepNamespace; CasDecommission.cpp comment) plus PR 2773 (encrypted CAS disk refusal) as 300a8c155c7. Release build OK; CAS* gate result in tmp/lanes/REPORT.md of the master worktree.

2026-11-06: PR https://github.com/Altinity/ClickHouse/pull/2806 (head 300a8c155c7, base antalya-26.6) opened with 2730 + 2773; CAS regression job ticked. Related port in the other direction: PR 2805.

CI of PR 2806 @300a8c155c7 (2026-11-06): builds green. Reds: GroupingAggregatedTransform (issue 2795; PR 2802 merged into 26.6 at 10:37 after the run started), python-bottle arm64 image on amd64 runners (test_cas_gcs, relink liveness, glue, sts), test_scheduler_query (docker CPU range on a 12-CPU runner), SQLLogic on an arm64 runner, test_s3_cluster::test_graceful_shutdown (26.6 base). Nothing from the ported 2730/2773. Rerun needed.

2026-11-07 audit: open PR https://github.com/Altinity/ClickHouse/pull/2806 (base antalya-26.6).
<!-- SECTION:NOTES:END -->
