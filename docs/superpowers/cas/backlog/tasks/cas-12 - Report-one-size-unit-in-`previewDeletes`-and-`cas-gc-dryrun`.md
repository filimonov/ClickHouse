---
id: CAS-12
title: Report one size unit in `previewDeletes` and `cas-gc-dryrun`
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:18'
labels:
  - 'area:gc'
  - 'area:tooling'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-7
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - programs/disks/CommandCaGcDryRun.cpp
priority: low
type: bug
ordinal: 18000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Gc::previewDeletes` (`CA/Gc/CasGc.cpp:4608`) stores the raw HEAD size, header included, for zero-in-degree candidates
(`:4645`) but the payload-only stored size for retired-in-snapshot rows (`:4671`, written through `retiredLogicalSize`,
`:286`). `clickhouse-disks cas-gc-dryrun` prints the column raw (`programs/disks/CommandCaGcDryRun.cpp:47`), so a sum
mixes units by `blob_header_len` (256 by default) per condemned row. Diagnostic-only command; one-line fix.

Provenance: BACKLOG/operability-and-introspection.md#byte-accounting-blobs-only-and-preview-size-units second half (2031-triage CAS-123); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every `PreviewEntry::size` uses the same unit, or both physical and logical sizes are carried
- [ ] #2 A test with one candidate of each kind checks the sizes agree in unit
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
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `previewDeletes` is 2026-06-13 (b9e64af811a); the finding itself has no record before this migration, so the creation date stays 2026-09-26.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:18
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Gc/CasGc.cpp:5338-5339 stores raw HEAD observed->size for zero rows; condemned rows use rec.size; condemn path strips blob_header_len (CasGc.cpp:2244). Changes: none. Pointers stale: CasGc.cpp:4608/4645/4671 and retiredLogicalSize :286 no longer exist; previewDeletes is now at ~:5305. Would settle it: whether rec.size is payload-only for every condemned row (producer not traced).
---
<!-- COMMENTS:END -->
