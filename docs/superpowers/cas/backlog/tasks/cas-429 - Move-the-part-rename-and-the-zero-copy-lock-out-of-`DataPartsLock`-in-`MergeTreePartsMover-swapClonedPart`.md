---
id: CAS-429
title: >-
  Move the part rename and the zero-copy lock out of `DataPartsLock` in
  `MergeTreePartsMover::swapClonedPart`
status: To Do
assignee: []
created_date: '2026-11-04 11:34'
labels:
  - 'area:moves'
  - 'area:upstream'
  - perf
dependencies: []
ordinal: 532000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Background moves onto a CAS disk hold the table's `DataPartsLock` across `lockSharedData` (a Keeper round trip under zero-copy) and `renameClonedPart` (on CAS: one durable ref-log append to object storage for `moving/<part>` -> `<part>`). Concurrent insert commits wait on the same lock. Measured on 26.6.8 (https://github.com/Altinity/ClickHouse/issues/2772): 200M rows / 350 parts into a local-first tiered table, insert 170 s on CAS vs 87 s on a regular S3 tier, 150 s summed lock wait. 26.8 is cheaper but not fixed: `swapClonedPart` is byte-identical to 26.6, the rename is now one `moveRefExact` append instead of 26.6's `republishRef` + `dropRefIfPresent`, and it still runs under the lock.

Fix is upstream code (`src/Storages/MergeTree/MergeTreePartsMover.cpp`), not CAS: a CAS-side async durability for the rename would let the old local copy be cleaned before the ref-log write lands (fail-open on a crash). Upstream master has the same order (lock -> `lockSharedData` -> `renameClonedPart` -> `swapActivePart`), so zero-copy RMT and `plain_rewritable` disks pay the same round trips under the lock. Standalone upstream PR first, then cherry-pick to antalya-26.6 and antalya-26.8. Not part of PR 2704.

Shape: run `lockSharedData` and `renameClonedPart` before `lockParts`; under the lock only `getActiveContainingPart` + `swapActivePart`. New case: the part was merged away between the rename and the lock, so the clone now sits under its final name on the destination disk: rename it to `delete_tmp_<name>` and remove it with the `ASK_KEEPER` policy, as the existing `!active_part` branch does for a stale `moving/` clone.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `swapClonedPart` takes `DataPartsLock` only around `getActiveContainingPart` and `swapActivePart`; no disk rename and no Keeper call under it
- [ ] #2 The merged-away-between-rename-and-lock case removes the renamed clone and leaves no duplicate part after a restart (integration test with a failpoint between the rename and the lock)
- [ ] #3 Integration test: concurrent inserts during moves onto a CAS disk show no `DataPartsLock` wait attributable to the move (compare `ProfileEvents` lock wait with and without moves)
- [ ] #4 Upstream PR opened against ClickHouse/ClickHouse; cherry-picks to antalya-26.6 and antalya-26.8 reference it and https://github.com/Altinity/ClickHouse/issues/2772
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
