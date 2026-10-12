---
id: CAS-361
title: >-
  Publish and copy blob bodies without SDK-internal resends so a condemned
  envelope cannot reappear
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:write-path'
  - 'area:backend'
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp
  - src/IO/S3/Client.cpp
  - src/IO/S3/copyS3File.cpp
  - tmp/noclock/audit/blob.md
priority: high
type: bug
ordinal: 455000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Blob `publish` and `copyObject` run with `WriteSettings{}` (`Backend/CasObjectStorageBackend.cpp:ObjectStorageBackend::publish`), so `S3::Client::doRequest` may resend the identical envelope (500 retries, 30 s per socket op); the engine's `asSingleAttempt` bounds only the engine loop. On content-ETag stores (AWS S3, RustFS) a resend that lands after GC deleted that incarnation restores its old ETag; a late exact-token `DELETE If-Match(t_A)` (GC retry within 90 s, a carried `delete_pending`, a deposed leader paused between `HEAD` and `DELETE`) then removes a body another writer just republished. GCS generations are immune.

Related: `Client::doRequest` also resends on credential/redirect answers even in `SingleAttempt` (tmp/noclock/atomicity/w1-mount.md); `CompleteMultipartUpload` treats `NoSuchUpload` plus any object at the key as success (`src/IO/S3/Client.cpp`), which does not prove this upload's incarnation.

Fix (no format change): publish/copy under the single-attempt profile (~40 lines, 3 files); treat `NoSuchUpload` as ambiguous for CAS blob publish (~20 lines). Details: tmp/noclock/audit/blob.md (B9, S3a–S3d, S7). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `CaBlobPublishCore` gets an `SdkResend` action; TLC fails before the fix and holds after
- [ ] #2 CAS blob publish and copy issue exactly one wire request per engine attempt (test via request counting on the mock backend)
- [ ] #3 `NoSuchUpload` on completion surfaces as an ambiguous publication, not success
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
