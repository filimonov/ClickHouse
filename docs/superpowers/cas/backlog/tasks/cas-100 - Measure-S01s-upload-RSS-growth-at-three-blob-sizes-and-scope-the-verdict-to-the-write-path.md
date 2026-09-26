---
id: CAS-100
title: >-
  Measure S01's upload RSS growth at three blob sizes and scope the verdict to
  the write path
status: To Do
assignee: []
created_date: '2026-09-01'
updated_date: '2026-09-26 12:35'
labels:
  - 'area:soak'
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:soak'
dependencies: []
references:
  - utils/ca-soak/scenarios/cards/s01_s02_huge_blob.py
priority: low
type: research
ordinal: 138000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
S01 RSS growth during the upload was 0 at `ci` (512 MiB blob) and 2.228 GiB at `full` (8 GiB blob, 28%). It tracks the blob, so it is not pipeline noise.
The verdict is `growth < blob size` (`utils/ca-soak/scenarios/cards/s01_s02_huge_blob.py:150`), which only catches full materialization.
`Memory` samples (139, 43% in undecomposed pool frames): 42% in `SerializationString::deserializeBinaryBulkWithSizeStream` under `MergeTreeReaderWide::readData` and 6% in `ColumnString::shrinkToFit` inside `MergeTask`, i.e. the merge's String reads. Only 7% was in the CAS write path (`publishBlob`, `PartWriteTxn`).

Provenance: BACKLOG/performance.md#s01-rss-scales. Comparison point Altinity#2233 is closed. Verified 2026-09-26 against dd0ed2f263a.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 RSS growth is recorded for three blob sizes and classified as proportional, bounded or above the blob
- [ ] #2 The share of growth in CAS write-path frames is reported for each size
- [ ] #3 S01's verdict measures the write path, or the card documents why whole-server RSS is kept
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
First recorded: 2026-09-01 (9deed66b471, by 's01-rss-scales')
<!-- SECTION:NOTES:END -->
