---
id: CAS-14
title: >-
  Publish the empty covering part of DROP/REPLACE PARTITION outside
  `DataPartsLock`
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-10-22 07:59'
labels:
  - 'area:write-path'
  - 'complexity:medium'
  - 'risk:medium'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'needs:spec'
  - 'origin:2031-triage'
dependencies:
  - CAS-13
references:
  - src/Storages/MergeTree/MergeTreeData.cpp
  - docs/superpowers/cas/2031-triage.md#cas-048
priority: low
type: design
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`MergeTreeData::removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper` (`src/Storages/MergeTree/MergeTreeData.cpp:5883`) creates the empty covering part and, on a CA disk, calls `commitTransaction` on it (`:5980`) inside the caller's `DataPartsLock`, so the whole remote commit (manifest PUT, ref-lane flush) holds the table's parts lock.
Low impact: the part is empty, the trigger is DDL-only, failures are loud (2031-triage CAS-048, P3).
Fix needs a 3-phase caller restructuring: compute the range under the lock, release, publish, re-acquire and re-validate. A design task, not a code move; share the seam with `part-upload-before-partslock` if it fits.

Provenance: BACKLOG/performance.md#covering-part-publish-under-datapartslock (2031-triage CAS-048). Verified 2026-09-26 against aefe80eba98 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The empty cover's manifest PUT and ref-lane flush run with `DataPartsLock` released, verified by `PartsLockHoldMicroseconds` on a DROP PARTITION over a CA disk
- [ ] #2 A concurrent insert into the dropped range during the unlocked window is either covered or rejected, proven by a test
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
First recorded: 2026-08-21 (b2a38f0b611, by 'covering-part-publish-under-datapartslock')

2026-10-22, on `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8): unchanged for the Replicated site: `removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(txn, drop_range, DataPartsLock & lock, create_empty_part)` (`MergeTreeData.cpp:7076`) still calls `commitTransaction` on the empty part under the caller's lock (`:7161`), and `renameParts` then adds a `Move` under the same lock. The plain-`MergeTree` sibling `StorageMergeTree::createEmptyParts` was fixed on the branch (commit right after `createEmptyPart`, `StorageMergeTree.cpp:2751`) because the series had moved its publish under `Transaction::commit(lock)`.
<!-- SECTION:NOTES:END -->
