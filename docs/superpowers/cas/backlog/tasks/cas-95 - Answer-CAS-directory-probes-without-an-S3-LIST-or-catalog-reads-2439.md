---
id: CAS-95
title: Answer CAS directory probes without an S3 LIST or catalog reads (#2439)
status: To Do
assignee: []
created_date: '2026-07-06'
updated_date: '2026-09-26 12:43'
labels:
  - 'area:read-path'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - 'origin:issue'
milestone: m-0
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2439'
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
priority: high
type: enhancement
ordinal: 130000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Generic MergeTree directory probes on a CAS disk turn into S3 requests. A restart of the otel.demo stand (26 tables, 1,672 parts) issued 77k LISTs in three minutes, got 537 `503 Slow Down`, failed 139 uploads (108 of them ref-lane writes) and took 2 min 11 s to accept connections.
Steady state costs 155 LISTs per 10 minutes plus ~150-200k catalog GETs per day from `system.detached_parts` polling.
Three call chains, one per subtask: part-file `existsDirectory` (audit F15), table-dir `listDirectory` (F16), and the `detached` probe (F21).
Open on both branches. Priority is above the roadmap's "next" because the restart burst grows with the part count and fails ref-lane writes.

Provenance: BACKLOG/performance.md#scale-findings [startup O(refs)] (deleted in the source as superseded by audit F15/F16/F21 and #2439, which no backlog item carried); roadmap §2 'No LIST on directory probes'. Verified 2026-09-26 against dd0ed2f263a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A server restart on a pool with N parts issues no S3 LIST per part file (LIST count independent of N)
- [ ] #2 `clearOldTemporaryDirectories` and `system.detached_parts` polling issue no S3 request on a warm node
- [ ] #3 Issue #2439 is closed with before/after `S3ListObjects` and `CASOtherGet` numbers
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
First recorded: 2026-07-06 (4abf5b743ed, by 'startup O')
<!-- SECTION:NOTES:END -->
