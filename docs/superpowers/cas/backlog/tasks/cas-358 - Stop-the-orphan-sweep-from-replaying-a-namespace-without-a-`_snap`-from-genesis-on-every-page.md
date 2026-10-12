---
id: CAS-358
title: >-
  Stop the orphan sweep from replaying a namespace without a `_snap` from
  genesis on every page
status: To Do
assignee: []
created_date: '2026-10-12 09:50'
updated_date: '2026-10-12 13:09'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:decision'
  - 'needs:measurement'
  - 'origin:review'
milestone: m-1
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
priority: medium
type: research
ordinal: 452000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The orphan sweep's protection view (`activeManifestKeys` → `recoverRefTableDetailedFromAuthority`) replays a namespace from its `_snap`, or from genesis when it has none, on every page that nominates a candidate in that namespace. A table below the snapshot threshold never publishes a `_snap`, and every remount adds a seal-only epoch, so the replay grows with age and remounts. Production numbers from https://github.com/Altinity/ClickHouse/pull/2542 (one writer, two readers, ~800 tables, 26.6.4 Antalya): 792 streams, 568 with ≥ 2 epochs, 74 with a `_snap`, 3,876 epochs that are a single `epoch_seal`; the replay explains most of the GC leader's ~956 k successful GETs per day after the PR removed the 404 class.

Candidate directions named in the PR, not decided: a cheaper rebuild source (the fold seal's `last_folded_ref_id` plus a cached view), or letting a seal-only epoch count toward the snapshot threshold so idle tables get a `_snap`. The contributor asked which direction we prefer before writing it. CAS-87 (sweep walks the committed tail twice) halves the same reads from the other side.

Research first: attribute the 956 k reads per day by key kind and by namespace age, predict the cut of each option, then pick.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The GC leader's successful GETs per day are attributed by key kind and namespace age, in a report under docs/superpowers/reports/
- [ ] #2 Each option has a predicted cut and cost; one is chosen and the decision is written to the task
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
2026-10-12, owner question 'can blob in-degree answer this?': almost. The source-edge runs hold `(blob_hash, source_id)` with `source_id` the manifest's 16-byte id, so the set of source ids present after the merge is 'manifests owned as of the cursor'; with premise rule (1) (grants do not cross epochs, the build epoch's seal is consumed) that set is as safe as today's writer-authority `active`. Two gaps: the runs are keyed and sharded by blob hash, so a per-namespace answer is a scan of every shard (cheap only as a by-product of the merge the round already streams, O(live manifests) memory at 16 B each); and a manifest whose every file is `EntryPlacement::Inline` has no edge rows, is owned, and would be deleted as an orphan. Preferred direction: the fold already extracts `RefManifestEdge{manifest_id, +-1}` per transaction; keep a durable owned-manifest set per life keyed by `ManifestId` (manifest-level in-degree, sharded by namespace), O(live parts), no inline hole. The sweep's `active` then comes from the adopted seal's set and only the committed tail above the cursor is read for removal targets; the replay from `_snap` or genesis goes away. Research step 1 (attribution of the 956k reads) still comes first; this is the structure to cost against the `_snap`-threshold option.

2026-10-12, owner decision: PR 2542 is accepted as a temporary fix. The goal of this task is that the orphan sweep does not replay a namespace at all: the fold has already processed every record below the cursor, so the sweep's owner set must come from fold state (the owned-manifest set above), and the sweep reads at most the committed tail above the cursor. The `_snap`-threshold option only shortens the replay and is not the fix.

2026-10-12, owner constraint: forward compatibility with no operator action; an old version ignores the new state and never writes it. Shape that satisfies it: (1) the owned-manifest runs are a new object family under the generation prefix (`gc/gen/<G>/attempt/<A>/manifest_owned/...`); `pruneSupersededGenerations` LISTs the whole `gc/gen/<g>/` prefix and deletes everything listed, so an old leader reclaims them with the generation without knowing them. (2) Nothing is added to the fold seal: 26.6.4's seal reader is Strict (unknown key = CORRUPTED_DATA, CAS-328 only fixes later releases), so the pointer lives in a side object at a deterministic key per (G, attempt), written before the seal's CAS; an absent side object (404) means 'no owned set for this generation'. (3) The side object carries a per-namespace `as_of` cursor. At fold time a namespace whose `as_of` equals the parent coverage cursor is updated incrementally from this round's +1/-1 edges; any other state (absent, older because an old leader folded in between, partial from a budgeted seed) is re-seeded once from writer authority (`recoverRefTableDetailedFromAuthority`, the same replay the sweep does today, but once per namespace per seed, not per page). Set semantics make a seed taken at `committed_through` idempotent under later folded edges. (4) Until a namespace's set is current the sweep retains that namespace (a new retain class, counted), never a fallback that deletes. (5) No existing object changes bytes, so no format version bump: the compatibility path is 'old ignores, new re-seeds', and a leader flip old->new costs one seed per namespace, once.
<!-- SECTION:NOTES:END -->
