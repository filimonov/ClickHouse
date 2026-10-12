# CAS late-request fixes: CAS-360, CAS-361, CAS-36 {#cas-late-request-fixes}

Three data-loss defects found by the 2026-10-12 late-request audit (`docs/superpowers/cas/2026-10-12-late-request-audit/`), reproduced in TLA+ and settled by a two-model consult (codex, opus). Three independent PRs against `antalya-26.6-20261008`, each branched from `d76b59f9295`. No on-store format change.

| PR | Defect | Fix | Model |
|---|---|---|---|
| 1 | CAS-360: writer adopts a blob body GC deleted between its `HEAD` and its marker `GET` | adopt only on a `Clean` marker; a present body with an absent marker is republished | `CaGcCondemnMarkerGate` `#cas-360` (`06e331ada7c`) |
| 2 | CAS-361: SDK resend of an identical envelope restores a condemned content-ETag; a late exact-token `DELETE` removes a live body | object-creating requests of blob publication go out once | `CaBlobPublishCore` `#sdk-resend-cas-361` (`0acdf9c7939`) |
| 3 | CAS-36: orphan sweep deletes a manifest staged before its namespace's catalog row exists | read the mount floor of every namespace on the page before the catalog cut; no lease = not eligible | `CaGcRootLocalPartManifestCore` CAS-36 (`a385db56107`) |

## Global constraints {#constraints}

- On-store format frozen since 26.6.4: no new fields, no new object kinds.
- HEAD-before-PUT on the write path stays.
- No fallback paths; ambiguity propagates as a retryable error.
- Failing-first tests: each new test fails on `d76b59f9295` before the fix.
- gtest gate: filter exactly `CAS*`, release and ASan; LOGICAL_ERROR expectations are death tests.
- Upstream files (`src/IO`, `src/Disks/DiskObjectStorage/ObjectStorages/S3`) change only where PR 2 says, minimally.
- Rolling upgrade: none of the fixes protects old binaries. Old writers stay exposed to CAS-360/361 until upgraded; an old GC leader can still make the CAS-36 nomination. Release note: upgrade all nodes; do not run a mixed fleet longer than needed.

## PR 1: CAS-360 — adopt only on a `Clean` marker {#pr1}

**Change**, `Pool/CasPartWriteTxn.cpp:PartWriteTxn::ensureBlobPresent`:
- Adoption condition becomes `loaded && loaded->meta.state == MetaState::Clean`.
- Delete the `!loaded` backfill block (`putMetaIfAbsent(Clean)` + `CASMetaAdoptBackfill`).
- A present body with an absent marker falls through to publication with `reason = BlobPublicationReason::Absent`; the reconcile then creates the marker (`op.create`; a racing writer's marker resolves through the existing read-modify-write).
- Keep the body size checks; the `BlobReuseAdopt` event text drops "or absent marker".
- `CASMetaAdoptBackfill` is renamed or replaced by a counter for "republished: marker absent" (`CASMetaAbsentRepublish`); its docs entry in `ProfileEvents.cpp` says when it fires.

**Leak fix in the same PR**, `Gc/CasGc.cpp:Gc::applyRedeleteOutcome` and `Gc/CasGcMetaWriter.cpp:deleteConfirmedMeta`: pass the deleting round − 1 as the bound instead of `entry.condemn_round`, so a `Condemned` marker the graduation gate re-stamped at a later round is removed with its body. Safe under the new adoption rule: a marker delete never enables adoption.

**Why it is safe:** a `Clean` marker read after the `HEAD` means GC had not condemned the body, or a writer republished it; GC deletes a body only after its marker is `Condemned`, and deletes the marker only after the body. Absence no longer means anything to the writer. Cost measured on otel.demo (16.8 days, one node): 8 absent-marker adoptions vs 2.82 M publications.

**Tests** (`src/Disks/tests/gtest_cas_part_write.cpp`, failing first):
- `CASPartWriteTxnMarkerRace.AbsentMarkerAfterHeadRepublishes`: interpose the first read of `blobMetaKey`; before it returns, delete the body and the marker; then replay GC's `remove(key, old_token)`. Expect action `Published`, body present with a fresh token, marker `Clean`. Base: `Observed`, body absent.
- `CASPartWriteTxnMarkerRace.PresentBodyWithoutMarkerRepublishesOnce`: a crash leftover gets one upload; the next writer adopts.
- `CASGcMarker.RedeleteRemovesMarkerRestampedByGate` (`gtest_cas_gc*`): lose the condemn-marker write, gate refuses and re-stamps at R+1, graduates at R+2, redelete at R+3; the marker is gone. Base: marker stays.
- Update tests that expect backfill adoption on an absent marker (grep `CASMetaAdoptBackfill`, `putMetaIfAbsent` in `src/Disks/tests`).

## PR 2: CAS-361 — one wire attempt for object-creating publication requests {#pr2}

Code checks (`tmp` research, summarized): nothing below the S3 client resends an applied request (Poco rewrites only when the first socket write fails); `SingleAttemptRetryStrategy` stops both SDK and `doRequestWithRetryNetworkErrors` retries; today blob publication does not use it.

**Change:**
1. `src/IO/WriteSettings.h`: `bool object_storage_single_attempt_object_creation = false;` — the request that creates the object (`PutObject`, `CopyObject`, `CompleteMultipartUpload`) gets one wire attempt; part uploads keep their retries.
2. `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp`:
   - `writeObject`: when the flag is set, give `WriteBufferFromS3` a second, single-attempt client for object creation (`getSingleAttemptClient`, the storage's own request timeout, the usual connect cap).
   - `copyObject`: honour `write_settings` (today it always uses `client.get()` and ignores the retry override): the same creation client for `CopyObject` and the multipart-copy `Complete`; apply `s3_max_unexpected_write_error_retries_override` to a local copy of the request settings, as `writeObject` does.
3. `src/IO/WriteBufferFromS3.{h,cpp}`: `makeSinglepartUpload` and `completeMultipartUpload` use the creation client when set; `CreateMultipartUpload`/`UploadPart` keep the retrying client. `src/IO/S3/copyS3File.cpp`: the same split for `CopyObject` / `UploadPartCopy` / `Complete`; under the flag, a failed `CopyObject` is not converted to a multipart copy (GCS "use Rewrite" `504`), it propagates.
4. `src/IO/S3/Client.cpp:Client::CompleteMultipartUpload`: when `usesSingleAttemptRetryStrategy()`, do not turn `NoSuchUpload` + successful `HEAD` into success; return the original error. Do not run the optional GCS self-compose for this client.
5. `Backend/CasObjectStorageBackend.cpp:ObjectStorageBackend::publish`: streaming and staged copy set the flag and `s3_max_unexpected_write_error_retries_override = 1` (the `NO_SUCH_KEY` loops reissue the identical envelope); keep unconditional publication and the body-upload timeout.

Retries move up to `ensureBlobPresent` (≤ 8 iterations, one standard window): each re-observes (`HEAD` + marker `GET`) and publishes a fresh envelope; the staged copy is once-only, so a retried copy becomes a streaming upload.

**Tests** (failing first; mock S3 HTTP server `src/IO/S3/tests/TestPocoHTTPServer.h`, request counting per verb; extend `gtest_cas_aws_s3_client.cpp` / `src/IO/tests/gtest_cas_writebuffer_s3.cpp`):
- `CASBlobPublishTransport.PutIsOneWireRequestOnTimeout`: server applies the `PUT` and drops the response; exactly one `PUT`.
- `CASBlobPublishTransport.CopyIsOneWireRequestOnTimeout`: same for `CopyObject`.
- `CASBlobPublishTransport.UploadPartStillRetried`: one `UploadPart` 500 then success; two `UploadPart`, one `Complete`.
- `CASBlobPublishTransport.CompleteNoSuchUploadIsAmbiguous`: `Complete` returns `NoSuchUpload` while an object exists at the key; the write fails, no synthetic success.
- `CASBlobPublishTransport.NoSuchKeyLoopDoesNotResend`: the `NO_SUCH_KEY` loop runs once under the override.
- Default-profile writers keep today's behaviour (one existing test per path stays green).

## PR 3: CAS-36 — floor before the catalog cut {#pr3}

**Change**, `Gc/CasOrphanManifestSweep.cpp:planManifestCursorPage`:
1. After `op.list` and parsing, call `floorForNamespace` for every distinct namespace the page may nominate, before `readAdoptedFoldSeal` / `CasRefCatalog::read` and before any owner-view read. Never fetch a missing floor after the cut; a namespace left unsampled (budget) is retained.
2. Apply `prefixEligibleUnder` to every key, catalog row or not; no lease = not eligible.
3. Rewrite the comment claiming "creation publishes its row before any life-owned object": a manifest body is staged before `precommitAdd` creates the row (table move: `moveDirectory` → `republishRef` → … → `stageManifest`; first INSERT on a CAS disk that is not the policy's first writable disk).
4. Keep the catalog-authority, `Creating`, owner-view and deletion-premise checks.

`gc_fenced` eligibility (CAS-36 AC #6) is a separate change on top; the residual — a late request of a fenced or superseded epoch into a rowless namespace — is excluded only by lease admission and is documented in the model, not fixed here.

**Tests** (`src/Disks/tests/gtest_cas_orphan_manifest_sweep.cpp`, failing first):
- `CASOrphanManifestSweep.RowlessBodyUnderLiveLeaseRetained`.
- `CASOrphanManifestSweep.RowlessBodyFloorReadBeforeCatalogCut`: interpose the catalog read; before it returns, birth the row, precommit, promote and end the build (advance the lease floor); the body is retained. Base: deleted.
- `CASOrphanManifestSweep.RowlessBodyFarewellOrLowerEpochDeleted`: debris is still reclaimed.
- Flip `CursorPageDeletesObservedBodyWhenCatalogOmitsNamespace` to `CursorPageRetainsRowlessBodyWithoutLease`.
- `CASOrphanManifestSweep.IneligibleRowlessKeyRetiresNoSourceEdges`: no `orphan_source_retirements` for a retained key.

## Out of scope {#out-of-scope}

Mixed-version protection (needs GC-side marker semantics, rejected: CAS-360 A); fix E for non-SDK duplicates (store support unverified); `gc_fenced` eligibility; CAS-358 (depends on PR 3); the cancelled `abandon` branch retiring without checking `precommit_state` (`CasPartWriteTxn.cpp:1080-1083`, to verify separately).
