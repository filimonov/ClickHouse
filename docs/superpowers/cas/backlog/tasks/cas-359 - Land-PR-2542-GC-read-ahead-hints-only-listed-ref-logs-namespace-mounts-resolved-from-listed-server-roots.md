---
id: CAS-359
title: >-
  Land PR #2542: GC read-ahead hints only listed ref logs; namespace mounts
  resolved from listed server roots
status: In Progress
assignee: []
created_date: '2026-10-12 09:50'
updated_date: '2026-10-12 09:58'
labels:
  - 'area:gc'
  - 'area:mounts'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-1
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasKeyReader.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.cpp
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
priority: high
type: enhancement
ordinal: 453000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
https://github.com/Altinity/ClickHouse/pull/2542 (Paulo Chade, Kayro; base `antalya-26.6`, 5 commits rebased onto 0f732b6b697). The GC leader issued ~2.4 M GETs per day answered 404 under `cas/ns/stream/<life>/_log/`, 58 % of its GETs: ref-log read-ahead hinted the next `4 × cas_gc_io_concurrency` ids by arithmetic and learned where an epoch ends only by decoding its seal, and the orphan sweep rebuilds each namespace's protection view on every page, so the waste was window × epochs × visits. `floorForNamespace` added 2 more 404s per page per namespace by probing every `/`-prefix for a mount.

The PR: `hintRefLogsWithinEpoch` hints only ids the round's LIST returned (`RefScanSummary::logs_by_life`, `RefTableListing::logs`); the seal-crossing discards become dead code and are removed; `floorForNamespace` / `prefixEligible` / `sweepNamespace` take the set of server roots the caller listed (`HeartbeatFloor::listed_srids` in a round, `listServerRoots` for fsck / rebuild / decommission). No new state, setting or format. Production after rollout on 2026-10-11: `CASGCReadAheadWasted` 19 → 1 → 0 → 0 per round, 404s per steady-state round 2 (was ~1,000 per hour), GC decisions unchanged.

Review 2026-10-12 (verified against the PR head be3da0cd71f): decisions are unchanged (read-ahead caches results, `take` of an unhinted key reads inline), `listing.logs` is sorted, the phase 4 LIST is unbudgeted, `listMounts` keeps fenced / terminated / corrupt mounts, decommission sweeps debris before deleting the victim's slot, and the "mount younger than the LIST → retain" direction is conservative on every path. Open items are in the acceptance criteria. The nested-server-root fall-through the PR widens is CAS-357 (see references). The PR's "separate point" (replay from genesis on every page) is CAS-358.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 CI green on antalya-26.6 for the PR head: CAS* gtest gate, ASan lane, test_cas_gc_s3 (the branch was never compiled on this base; no checks had run as of 2026-10-12)
- [ ] #2 Decision on the removed seal-crossing discards: keep them (no-op when nothing is hinted) or add a test for a stream with listed ids past a seal (CAS-351 shape) showing the page does not degrade to serial reads
- [ ] #3 test_gc_read_ahead_wastes_nothing_across_epoch_changes passes 20 consecutive local runs (a hold from a background merge after the last restart counts as wasted)
- [ ] #4 docs/en/antalya/cas/architecture/garbage-collection.md: the read-ahead paragraph states the listed-only hint rule and the `floor_reads` row says one read per listed root
- [ ] #5 CAS-358 is filed and linked from the PR as the follow-up
- [ ] #6 CAS-357 fixed in the PR before merge: at minimum option (c), the fold's dead-precommit check keeps the probe-every-prefix lookup; the comments on `floorForNamespace`, `prefixEligible` and `floor_reads` claiming an unlisted root always retains are corrected
- [ ] #7 A gtest pins the listed-but-404 arm of `floorForNamespace` with a counted read (`AbsentFloorRetainsEverythingWithOneLookup` now covers only the absent-from-LIST arm)
- [ ] #8 Integration test: `SYSTEM STOP MERGES` before the restarts or assert on the orphan-sweep page counters, and prove an epoch-crossing walk ran under read-ahead (`epoch_crossings > 0` on the `fold_ref_intake` row of `cas_gc_log` after the last restart), since first-walk hints alone satisfy `hits > 0`
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
Review 2026-10-12: own trace + independent ca-review (report tmp/pr2542/review-ca.md, local). Read-ahead half sound: caches results never decisions; listing sorted; lifetimes OK; no stale take possible (write-once keys, trimming only in phase 17). Blocking: CAS-357 (nested srids, fold dead-precommit skip). Prose to fix in the PR: CasGcReadAhead.h:28 dropped the bounded-exception clause and :33 says untaken hints are discarded (fold-intake hold and recovery-walk throw leave them to the destructor); CasKeyReader.h:47 misses the LIST-omits-a-durable-id case the PR's own test models.
<!-- SECTION:NOTES:END -->
