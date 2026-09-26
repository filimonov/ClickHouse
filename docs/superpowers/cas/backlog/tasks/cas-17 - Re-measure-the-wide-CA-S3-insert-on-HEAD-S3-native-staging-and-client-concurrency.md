---
id: CAS-17
title: >-
  Re-measure the wide CA-S3 insert on HEAD: S3-native staging and client
  concurrency
status: To Do
assignee: []
created_date: '2026-09-26 06:55'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:review'
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp
priority: low
type: research
ordinal: 23000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Stage 1 took the 10M rows x 30 columns x 500 parts CA-S3 `INSERT` from 58.41 s to 30.26 s (1.59x plain S3), still ~87% network-bound. That figure predates `_ckpt` and the mandatory blob HEAD, so it is no longer a valid baseline.
Two unmeasured levers: (1) S3-native staging (`staging_backend = s3`, opt-in, `R/ContentAddressedSettings.cpp:86`): local staging moves every blob's bytes twice. (2) S3 client concurrency: 16-33 concurrent PUT threads may be client-capped.
Baseline and stage-1 reports were deleted in `f5c01e88d01`; the old 268.8 HEAD/part estimate predates unconditional publication.

Provenance: BACKLOG/performance.md#writepath-candidates-post-stage1 items 1-2. Verified 2026-09-26 against aefe80eba98 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A wide-insert baseline on current HEAD (CA vs plain S3) is recorded in a report
- [ ] #2 Wall time and PUT/HEAD counts are reported with `staging_backend = local` and `s3`
- [ ] #3 The PUT thread count and connection-pool limits during the insert are reported, with a verdict on whether the client caps concurrency
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
