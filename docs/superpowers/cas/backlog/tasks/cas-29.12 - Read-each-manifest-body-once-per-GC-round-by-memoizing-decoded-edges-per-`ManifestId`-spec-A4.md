---
id: CAS-29.12
title: >-
  Read each manifest body once per GC round by memoizing decoded edges per
  `ManifestId` (spec A4)
status: To Do
assignee: []
created_date: '2026-09-26 07:39'
updated_date: '2026-09-26 08:34'
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
