---
id: CAS-361
title: >-
  Publish and copy blob bodies without SDK-internal resends so a condemned
  envelope cannot reappear
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-10-12 14:28'
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
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/blob.md
priority: high
type: bug
ordinal: 455000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Blob `publish` and `copyObject` run with `WriteSettings{}` (`Backend/CasObjectStorageBackend.cpp:ObjectStorageBackend::publish`), so `S3::Client::doRequest` may resend the identical envelope (500 retries, 30 s per socket op); the engine's `asSingleAttempt` bounds only the engine loop. On content-ETag stores (AWS S3, RustFS) a resend that lands after GC deleted that incarnation restores its old ETag; a late exact-token `DELETE If-Match(t_A)` (GC retry within 90 s, a carried `delete_pending`, a deposed leader paused between `HEAD` and `DELETE`) then removes a body another writer just republished. GCS generations are immune.

Related: `Client::doRequest` also resends on credential/redirect answers even in `SingleAttempt` (docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-mount.md); `CompleteMultipartUpload` treats `NoSuchUpload` plus any object at the key as success (`src/IO/S3/Client.cpp`), which does not prove this upload's incarnation.

Fix (no format change): publish/copy under the single-attempt profile (~40 lines, 3 files); treat `NoSuchUpload` as ambiguous for CAS blob publish (~20 lines). Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/blob.md (B9, S3a–S3d, S7). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-12 model result (`0acdf9c7939`, `docs/superpowers/models/CaBlobPublishCore_RESULTS.md#sdk-resend-cas-361`): new `SdkResend` action. Current protocol violates `CommittedRefHasContent` in 20 states (`cur_resend`): staged copy response lost and lands late; writer w1 loses its fence; GC condemns, graduates, deletes body and marker; SDK resends w1's copy, the same bytes return with the same ETag; w2 adopts the resurrected body; GC's re-sent `DELETE If-Match` removes it. Streaming PUT resend reaches the same end. Generation stores (GCS) are green with resends on: the token belongs to the write, not the content.

Fix A (single-attempt transport for object-creating requests) is green and sufficient; both halves are load-bearing (`sab_A_copy_resends`, `sab_A_put_resends` violate). The exact-token delete needs no extra guard. Scope: `PutObject` and `CopyObject` only. `UploadPart` resends create no object and stay in the SDK (otherwise one part failure restarts a large upload); a `CompleteMultipartUpload` resend after it applied gets `NoSuchUpload`. Retries move up to `ensureBlobPresent` (≤ 8 iterations, 90 s window): each re-observes (`HEAD` + marker `GET`) and publishes a fresh envelope; the staged copy is once-only, so a retried copy becomes a streaming upload. A late-landing failed attempt is an unreferenced incarnation that GC re-condemns (`Replaced`).

Fix B (fresh incarnation per wire attempt) cannot be built: the SDK resends the buffered body and a copy resend copies the same staging bytes (`sab_B_copy_same_bytes`). Fix E (delete also matches last-modified time) is green with resends on and also covers non-SDK duplicates (proxy, store double-apply); an alternative to A, not an addition, and store support on AWS general-purpose buckets and RustFS is unverified.

Open for the code fix: does the single-attempt write path also disable `WriteBufferFromS3`'s `max_unexpected_write_error_retries` (if it re-runs a whole multipart with the same buffers it is the same defect class); can `UploadPartCopy`'s `Complete` re-land an object. `NoSuchUpload`-as-success (S7) is not modelled: the model's `ReconcileMetaClean` still refuses `Condemned`, an older stand-in for graduation. Re-HEAD (CAS-360 option B) closes the HEAD/marker race only together with A.

2026-10-12 decision after the two-model consult: single attempt for object-creating requests only — `PutObject`, `CopyObject`, `CompleteMultipartUpload`; `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy` keep SDK retries (one throttled part must not fail a 512-part body). New `WriteSettings` flag (e.g. `object_storage_single_attempt_object_creation`); `S3ObjectStorage::writeObject` passes a single-attempt creation client to `WriteBufferFromS3` (`makeSinglepartUpload`, `completeMultipartUpload`); `S3ObjectStorage::copyObject` honours `write_settings` (today it uses `client.get()`) and the unexpected-write retry override; `Client::CompleteMultipartUpload` skips `NoSuchUpload` → HEAD → success for single-attempt clients; `ObjectStorageBackend::publish` sets the flag plus `s3_max_unexpected_write_error_retries_override = 1` (the `NO_SUCH_KEY` loops reissue the identical envelope) and refuses an empty result ETag as ambiguous. About 30 lines in upstream files the fork already patches. Credential/region/redirect reissue in `Client::doRequest` follows a definitive rejection, so it cannot duplicate an applied request; left as is (codex wanted it disabled too). Open: `PocoHTTPClient` reconnect resend on a stale keep-alive connection; store behaviour on a duplicate `Complete` (RustFS, MinIO, AWS).
<!-- SECTION:NOTES:END -->
