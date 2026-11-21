---
id: CAS-419
title: >-
  Packed parts: one Packed-to-Full gate, a shared data.packed constant,
  freezeRemote destination format
status: To Do
assignee: []
created_date: '2026-10-23 01:50'
updated_date: '2026-11-21 08:21'
labels:
  - cas
  - 'area:write-path'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: low
type: chore
ordinal: 522000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From the packed-parts review (antalya-26.8 worktree tmp/review3/REPORT.md, items 5 and 9). The Packed->Full downgrade lives in MergeTreeData::normalizePartFormatForDisk (updatePath, writeTempPartImpl) and again inline in MergeTreeDataPartBuilder::withPartStorageType; the builder copy is the only guard for createEmptyPart and for a future_part built without updatePath, and when it fires alone future_part->part_format still says Packed. Cas::partFileMustStayBlob hardcodes data.packed / data.packed.tmp while DataPartStorageOnDiskPacked::DATA_FILE_NAME owns the name; primary.idx is matched by full path, the archive by basename. DataPartStorageOnDiskPacked::freezeRemote builds the clone's storage without consulting the destination disk's allowsPackedParts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every Packed->Full decision goes through one function; the builder either calls it or throws LOGICAL_ERROR on a disagreement, after clone/fetch/mover/restore are shown to open existing parts only
- [ ] #2 data.packed is one constant visible to both the CAS metadata storage and the Packed part storage
- [ ] #3 freezeRemote onto a disk that forbids Packed either keeps the source format by documented intent or downgrades like a fresh write; a test pins the choice
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
Also from the review (quality, needs measurement): DataPartStorageOnDiskPacked::finalizeWriter reopens the whole archive once per old member, which on CAS-over-S3 may cost one GET per member; measure before changing.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1: MergeTreeData::normalizePartFormatForDisk (MergeTreeData.cpp:6440) used at MergeTreeDataWriter.cpp:923 and FutureMergedMutatedPart.cpp:108 while MergeTreeDataPartBuilder.cpp:168 still downgrades inline; ContentAddressedTransaction.cpp:79 hardcodes "data.packed"/"data.packed.tmp"; DataPartStorageOnDiskPacked.cpp has no allowsPackedParts call. Changes: area set to write-path; size unset -> medium (M): three code changes across MergeTree and CAS plus a test; risk set to medium; confidence set to solid; type unset -> chore: consolidation, no behaviour change except the pinned freezeRemote choice; priority unset -> low: no priority set; consolidation with no user-visible failure. Size: M. Pointers stale: tmp/review3/REPORT.md lives in another worktree.
---
<!-- COMMENTS:END -->
