---
id: CAS-271
title: >-
  Log when `skip_access_check` or a decommission open skips the capability
  probe, and fix the probe's 'no override' text
status: Done
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-04 15:00'
labels:
  - 'area:backend'
  - 'area:observability'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
milestone: m-7
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.cpp
priority: low
type: enhancement
ordinal: 336000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
With `skip_access_check` the pool skips the capability battery (`CA/Pool/CasPool.cpp:481-519`); `openForDecommission` forces the
skip (`:917`), so `clickhouse-disks cas-drop-member` never probes. The single-attempt gate still runs (pinned by
`CASPool.SkipAccessCheckStillEnforcesSingleAttemptGate`), and a GCS backend refuses the skip (`checkSkipAccessCheckSupport`, `:514`).
Nothing tells the operator what went unverified. The probe's versioning message says the check "has no override"
(`CA/Backend/CasProbe.cpp:39`), which `skip_access_check` contradicts.

Provenance: BACKLOG/formats-and-storage.md#skip-access-check-no-signal (2031-triage CAS-030) and the skip half of #versioning-enabled-after-mount; verified 2026-09-26 against 8b87aa15d21 (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A skipped probe logs one line at the default level naming what is unverified (versioning, conditional writes, list-after-write)
- [x] #2 The versioning message no longer claims there is no override
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [x] #2 ASan lane green for touched suites
- [x] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [x] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-08-21 (00bda042d5d, by 'skip-access-check-no-signal')

2031-triage CAS-029: the `skip_access_check` half. 'Versioning queried only on the GCS dialect; ETag mounts rely on the delete-marker probe' is decided behaviour (2026-09-02), and the post-mount `LOGICAL_ERROR` half is implemented by `654ba3d0aa7` (`CAS_DELETE_MARKER`).

2026-10-14: delivered in PR B https://github.com/Altinity/ClickHouse/pull/2567 (skipped-probe log and probe text).

2026-11-04: merged into antalya-26.6 as 9b348cf0a4b (https://github.com/Altinity/ClickHouse/pull/2567). Reds on the final run were base/infra: regression cas_alter_attach_3 (issue #2343), CASS3CacheLightweightDelete, and the cas s3 stateless lanes on `GroupingAggregatedTransform pushed bucket A twice` (not CAS). Delivered by PR B; closed on its merge.
<!-- SECTION:NOTES:END -->
