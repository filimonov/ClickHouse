---
id: CAS-358
title: >-
  Stop the orphan sweep from replaying a namespace without a `_snap` from
  genesis on every page
status: To Do
assignee: []
created_date: '2026-10-12 09:50'
updated_date: '2026-10-12 14:26'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:review'
milestone: m-1
dependencies:
  - CAS-36
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.cpp
priority: medium
type: research
ordinal: 452000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
**Decision (2026-10-12): the orphan sweep must not replay a namespace at all.** The fold has already consumed every ref record below its cursor, so the sweep's owner set comes from fold state, and the sweep reads at most the committed tail above the cursor. Shortening the replay (a `_snap` threshold that counts seal-only epochs) is not the fix. PR 2542 is not merged; its `prev_epoch_seal` hint bound moved to CAS-359.

### Problem

`activeManifestKeys` (`CasOrphanManifestSweep.cpp:192`) calls `recoverRefTableDetailedFromAuthority`, which walks from `_snap` if one exists, else from `{life_epoch, 1}` (`CasRefCkpt.cpp:176-180`). It does this on every page that nominates a candidate in the namespace; memoization lives only within a page (`manifest_sweep_list_budget_keys=1000`). Tables below the snapshot threshold never publish a `_snap`, and every remount adds a seal-only epoch, so the walk grows with table age.

Production numbers from https://github.com/Altinity/ClickHouse/pull/2542 (one writer, two readers, ~800 tables, 26.6.4 Antalya):

| metric | value |
|---|---|
| streams | 792 |
| streams with >= 2 epochs | 568 |
| streams with a `_snap` | 74 |
| epochs that are a single `epoch_seal` | 3,876 |
| GC leader successful GETs/day after the PR's 404 cut | ~956 k, mostly this replay |

### Shape

Durable owned-manifest set per namespace life, maintained by the fold, keyed by `ManifestId`. The fold already extracts `RefManifestEdge{manifest_id, +-1}` per transaction; the set is the manifest-level in-degree, O(live parts), sharded by namespace.

Forward compatibility with no operator action: an old version ignores the new state and never writes it. Five rules:

1. **New object family under the generation prefix**: `gc/gen/<G>/attempt/<A>/manifest_owned/...`. `pruneSupersededGenerations` LISTs the whole `gc/gen/<g>/` prefix and deletes everything listed, so an old leader reclaims the runs with the generation without knowing them.
2. **Nothing added to the fold seal.** 26.6.4's seal reader is Strict (unknown key = `CORRUPTED_DATA`; CAS-328 only helps later releases). The pointer lives in a side object at a deterministic key per `(G, attempt)`, written before the seal's CAS. 404 means "no owned set for this generation".
3. **Per-namespace `as_of` cursor** in the side object, a `RefTxnId` (same type as `RefCoverage.last_folded_ref_id`, already a total order per namespace, see CAS-305 notes). At fold time:
   - `as_of` equals the parent coverage cursor: update incrementally from this round's +1/-1 edges;
   - anything else (absent, older because an old leader folded in between, partial from a budgeted seed): re-seed once from writer authority via `recoverRefTableDetailedFromAuthority`. Same replay the sweep does today, but once per namespace per seed, not per page. A seed taken at `committed_through` is idempotent under later folded edges because the set has no counters.
4. **Retain until current.** While a namespace's set is not at the cursor, the sweep retains that namespace under a new counted retain class. No fallback that deletes.
5. **No byte changes to existing objects**, so no format version bump. A leader flip old -> new costs one seed per namespace, once.

### Rejected

- **Blob in-degree as the owner set.** Source-edge runs hold `(blob_hash, source_id)` and are sharded by blob hash, so a per-namespace answer is a scan of every shard. A manifest whose files are all `EntryPlacement::Inline` has no edge rows, is owned, and would be deleted as an orphan.
- **`_snap` threshold counting seal-only epochs.** Shortens the replay, keeps it.
- **Cheaper rebuild source from `last_folded_ref_id` plus a cached view.** In-memory state does not survive a leader change; the set must be durable.

### Related

- CAS-87: sweep walks the committed tail twice; halves the remaining tail reads.
- CAS-305 (d): one sequence per namespace turns `as_of` into one integer; format bump, so later and independent.
- CAS-328: Tolerant seal reader for releases after 26.6.4.
- CAS-359: `prev_epoch_seal` hint bound, the keepable part of PR 2542.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Baseline: the GC leader's successful GETs per day on the PR 2542 workload are attributed by key kind and namespace age, in a report under docs/superpowers/reports/
- [ ] #2 The fold writes the owned-manifest set and the side object per rules 1-3; the seal bytes are unchanged (golden test against a 26.6.4 seal)
- [ ] #3 The sweep's owner set comes from the adopted generation's set; a namespace without a current set is retained and counted, never replayed
- [ ] #4 `recoverRefTableDetailedFromAuthority` is called by GC only on a seed, at most once per namespace per generation
- [ ] #5 Compatibility test: a 26.6.4-shaped leader adopts a seal written next to a side object, prunes the generation, and a new leader re-seeds once
- [ ] #6 Measurement on the PR 2542 workload shape: GC leader GETs/day before and after, in the report from #1
- [ ] #7 The sweep samples `floorForNamespace` before the catalog cut and before reading either the fold's owned-manifest set or the committed tail above its cursor (CAS-36 A''); gtest hooks the tail read to birth, precommit, promote and end a build and the body is retained
- [ ] #8 Namespaces without a catalog row are decided by the floor alone: rule 4 (retain while the set is not current) does not apply to them, so their debris is reclaimed once the floor makes the build eligible
- [ ] #9 `CaGcRootLocalPartManifestCore`: `GSwView` replaced by the fold set as of the cursor plus a separate tail read; `fix_floorfirst` stays green and `sab_floorfirst` red
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
Decision log (times are commit times, local):

- 2026-10-12 11:50 (13d8fe980e7): opened from the review of PR 2542; contributor asked which direction we prefer.
- 2026-10-12 14:46 (18d614a00bd): blob in-degree evaluated and rejected; owned-manifest set from the fold preferred.
- 2026-10-12 14:55 (e1864a2b168): owner decision, no replay at all; `_snap` threshold is not the fix. PR 2542 later decided not merged (CAS-359).
- 2026-10-12 15:09 (4822008176b): owner constraint, forward compatibility with no operator action; rules 1-5 in Description.
- 2026-10-12 (7909f9d2bb7, CAS-305): `as_of` is the existing `RefTxnId` pair; finality still comes from seals until CAS-305 (d).

2026-10-12: overlaps CAS-36 (same function, `planManifestCursorPage`). The CAS-36 fix proves safety by observation order floor → catalog cut → owner view; here the owner view is older than the round (fold set `as_of` the cursor), so a build that ended after the cursor but before the floor read is visible only in the tail read, which must follow the floor sample. Rule 4 would retain rowless namespaces forever because the fold has no set for a namespace without a row. Three ACs added; depends on CAS-36.
<!-- SECTION:NOTES:END -->
