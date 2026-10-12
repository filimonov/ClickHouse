---
id: CAS-369
title: >-
  Snapshot the staging sweep's candidates before the mount claim and delete
  staging on abort
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:write-path'
  - 'area:mounts'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:medium'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp
  - tmp/noclock/audit/staging.md
priority: medium
type: bug
ordinal: 463000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`sweepOwnMountStaging` (`Pool/CasServerRoot.cpp`) LISTs `staging/<srid>/` after the claim and deletes blindly through `IObjectStorage`, bypassing `CasRequests`. A starter that stalls past ~41.5 s between claim and LIST (or a partition with SDK-retried LIST) deletes the same-srid successor's staging keys; its inserts fail (S1, S3). Leaks: every aborted transaction leaves its staging object until restart (S13), interrupted multipart uploads are never aborted (S7). Only with `cas_staging_backend = s3` (default `local`). The finalize check tests `now < deadline` with no reserve, after multipart parts were sent.

Fix (no format change): LIST before `openPoolView`, delete the listed keys after the claim wins (~25 lines; the successor writes only after its own claim, which follows ours); delete staging on abort (~10 lines, check `PendingBlob` lifetime); document `AbortIncompleteMultipartUpload` lifecycle or abort incomplete uploads at startup. Details: tmp/noclock/audit/staging.md. Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `CaCasMountCore` extension with `NoLiveStagingDeleted` fails on the current order, holds with the fix
- [ ] #2 aborted transactions leave no staging object (gtest)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
