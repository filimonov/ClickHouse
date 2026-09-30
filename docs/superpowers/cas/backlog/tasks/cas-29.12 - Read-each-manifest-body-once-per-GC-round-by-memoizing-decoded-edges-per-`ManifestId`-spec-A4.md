---
id: CAS-29.12
title: >-
  Read each manifest body once per GC round by memoizing decoded edges per
  `ManifestId` (spec A4)
status: In Progress
assignee: []
created_date: '2026-09-25'
updated_date: '2026-09-30 15:57'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'needs:measurement'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
milestone: m-1
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasManifestReader.h
  - 'https://github.com/Altinity/ClickHouse/issues/2429'
documentation:
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#a4-manifest-body-memo
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f4
parent_task_id: CAS-29
priority: high
type: enhancement
ordinal: 173000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Gc::foldManifestEdges` (`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp:1265-1284`) takes and
decodes one manifest body per emitted edge, with no reuse: `CASRefManifestBodyFoldGets == CASRefEmittedEdges`
(1,346,534 since restart on otel.demo). A part's lifetime reads manifest A on publish, A on repoint, B on repoint and B on drop.
With multi-hour rounds all four fall into one round. ~1.85M GET/day, 29% of all GETs, the dominant term of
`fold_ref_intake` (6000-6800 s per round) (audit F4, conclusion 6).
Bodies are immutable at write-once keys, so a per-round memo of decoded edge lists keyed by `ManifestId`, bounded by
total edge count and re-reading past the bound, changes no decision: same edges, same order.
The read path already has an LRU decode cache (`CasManifestReader::readManifestShared`, `Pool/CasManifestReader.h:52`);
reuse its cache primitive rather than add a second shape.
Measure first (audit verification item): count distinct `ManifestId` per round against edges to size the bound.

Provenance: BACKLOG/gc.md#otel-demo-s3-budget-audit-2026-09-25 (A4 = F4). The repoint elision (CAS-83) removes half of the same reads at the source; DRAFT-14 is decided after both. Verified 2026-09-26 against d4be7f7045a and 0dbbd797792: no memo in `foldManifestEdges` on either.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest folds publish, repoint and drop of one part in one round and sees one body read per manifest
- [ ] #2 `fold_ref_intake` phase rows show manifest GETs per round at most half of `CASRefEmittedEdges` on the stand
- [ ] #3 Round memory stays within the configured edge bound, proven by a gtest past the bound
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
First recorded: 2026-09-25 (2aee871df0a, by 'audit F4')

2026-09-30: implemented on the branch as `GcManifestMemo` (`Gc/CasGcManifestMemo.{h,cpp}` + hunks in `Gc/CasGc.cpp`, about 350 lines without tests). Branch `cas/gc-request-cuts` (worktree `gc-request-cuts`, base antalya-26.6 @e2dd0f5d343, HEAD 1edf28f2a9a on 2026-09-30; not pushed, no PR). NOT accepted yet.
Measured (`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/01..03`):
- Manifest body GETs per 1000 folded owner edges: 1000 -> 691 (677..705) with 10-s rounds; 611-636 with one round per 300 s. No manifest read is fetched and discarded, so the GET saving equals the memo hits.
- Hit rate 0.31 (10-s rounds), 0.34-0.38 (long rounds): below the 0.5 the audit expected.
- Time: on local RustFS the whole round does not move outside the spread. With 20 ms per request (S11 dev, n=2 per side) the round is 54.0 s -> 41.7 s (-23%); `fold_ref_intake` is 73-74% of that round.
Cost: a byte-accounted cache (interned namespaces, two hash tables, a FIFO list, two eviction loops), up to about 80 MiB per fold.
Safety (`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/05-memo-vs-deposed-leader-adjudication.md`): blob deletes stay safe; in a leader overlap a deposed leader's round is no longer suppressed by the absent-body anomaly and can reach `deleteConfirmedMeta` jobs, which widens CAS-30. Fix CAS-30 first, or re-check leadership after the fold.
Owner's rule (2026-09-30): no code complexity without proven value. Open before acceptance: (1) a reuse-distance trace to see whether an LRU of the last K bodies without byte accounting keeps >= 90% of the hits (measurement in progress); (2) proof of value on a realistic GC-backlog scenario (CAS-321 subtask for the yardstick card).

2026-09-30, estimate for the real stand. otel.demo (`chi-otel-otel-0-0-0`, 26.6.4.20001.altinityantalya), last 24 h before 2026-09-30 13:30 UTC, from `system.cas_gc_log` / `system.cas_log`: `fold_ref_intake` is 40.5% of round time and reads 4,645 manifest bodies per round (984k per day). Bucketing the writers' `build_publish` / `ref_repoint` / `ref_drop` events of 12 hours by the `gc_fold_begin` times (104 windows) gives 606k owner edges on 347k distinct (manifest, window) pairs: an estimated memo hit rate of 0.43 (a manifest is named once in 174k cases, twice in 86k, three times in 87k). That would be about 2,000 fewer GETs per round; if intake time is proportional to its requests, about 13 s of a 109-s round (DERIVED, not measured; the window assignment by event time is approximate and counts 25% more edges than `CASRefEmittedEdges`).

2026-09-30, could a simpler cache do (`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/08-janitor-batches-on-long-streams-and-memo-reuse-distance.md`): a trace of every manifest-body take in ca-soak S11 ci (validated: equals `CASRefManifestBodyMemoHits` in 25 of 25 rounds; the memo never evicted) gives a reuse distance of p50 181-230, p90 367-376 distinct manifests, max 10,911 with long rounds. A cache of the last K bodies keeps 28% of the hits at K = 8, 47% at K = 128, 73% at K = 256, 97-99% at K = 384. So a small fixed cache does not replace the memo; K of about 400-1000 does, and wide parts then need an entry cap, which is accounting again.
More important, 2026-09-30: most repeated manifest reads come from the part-removal flow (CAS-83): per removed part the rename and the unlink batch publish two manifests that are each read twice within a second. CAS-83 removes those manifests and their edges at the source. Re-measure the hit potential after CAS-83 before spending anything more here.
<!-- SECTION:NOTES:END -->
