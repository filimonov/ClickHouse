---
id: CAS-88
title: Stop the fold holding two full in-memory copies of a shard's edge run
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:issue'
  - 'origin:canary'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
dependencies:
  - CAS-29.1
references:
  - CA/Gc/CasBlobInDegree.cpp
  - docs/superpowers/cas/2031-triage.md#cas-035
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f29
priority: high
type: enhancement
ordinal: 123000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`foldDeltasIntoGeneration` (`CA/Gc/CasBlobInDegree.cpp:358`) writes the whole shard run into a `WriteBufferFromOwnString`
(`:398`) and then copies it (`run_bytes = out.str()`, `:688`) for the checksum and `putDeterministicArtifact`. At the default
`gc_shards = 1` that is the whole pool twice; on otel.demo the run grew to 125 MiB per round with the backlog (audit F29).
Same O(pool)-per-round class as the snapshot rewrite. The old caveat "the enumeration cannot be skipped because the defer
signal comes from it" predates spec B1 and must be re-examined once B1 lands.

Provenance: BACKLOG/gc.md#fold-edge-run-memory (2031-triage CAS-035). Verified 2026-09-26 against 59494ebf366 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Peak fold memory for one shard run is at most one run plus a bounded buffer, measured by a gtest or a MemoryTracker check
- [ ] #2 The run bytes and checksum are identical to today's for the same input
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
First recorded: 2026-08-21 (e811b3d68f4, by 'CAS-035')

2031-triage CAS-035: label added; LIST half is CAS-29.1.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Gc/CasBlobInDegree.cpp:480 WriteBufferFromOwnString out and :977 `const String run_bytes = out.str();` then checksum and putDeterministicArtifact (:981-983): the shard run is still built in a string buffer and copied. Changes: none. Links: depends on CAS-29.1 (already stated). Pointers stale: CasBlobInDegree.cpp:358/398/688 moved to :480/:977. Would settle it: not confirmed that WriteBufferFromOwnString::str() copies in this tree (it does in upstream); no memory measurement.
---
<!-- COMMENTS:END -->
