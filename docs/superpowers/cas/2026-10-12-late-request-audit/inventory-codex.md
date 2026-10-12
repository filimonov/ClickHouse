The sandbox forbids filesystem writes, including the requested output file. Report provided below.

Static audit of `d76b59f9295`; untruncated call-site greps completed. The inventory contains **101 rows**, but “unmapped call sites: 0” is too strong. Executions below are inferred from code; no tests or TLC runs were performed.

`CA/` means `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Model references are under `docs/superpowers/models/`.

| Row | Verdict | Evidence: file:function | Consequence |
|---|---|---|---|
| NEW | MISSED | `src/IO/WriteBufferFromS3.cpp`: `createMultipartUpload`, `uploadPart`, `abortMultipartUpload`; `src/IO/S3/copyS3File.cpp`: `UploadHelper` | Inventory collapses multipart into blob PUT/copy. Upload sessions and parts persist separately, addressed by key, `UploadId`, and part number. Late abort targets that upload; abandoned or late-created uploads can leak parts. |
| NEW | MISSED | `src/IO/S3/Client.cpp:597–608`: `CompleteMultipartUpload` → `ComposeObject`; `Requests.cpp:81`: `SerializePayload` | Optional GCS self-compose is another unconditional target-key write. Neither source generation nor destination precondition is supplied. Generation tokens prevent U1’s content-token ABA, but this operation needs inventory coverage. |
| NEW | MISSED | `src/IO/S3/Client.cpp:583–594`: `CompleteMultipartUpload`; `WriteBufferFromS3.cpp:228`: `finalizeImpl`; `copyS3File.cpp:259`: `checkObjectAfterUpload` | Extra decision-making `HEAD`s are missing. Particularly, `NoSuchUpload` plus **any present destination** becomes synthetic success; it does not prove this upload’s incarnation exists. Subsequent metadata cleaning feeds U2. |
| NEW | MISSED | `CA/Backend/CasObjectStorageBackend.cpp:154`: `checkPoolPreconditions` → `S3ObjectStorage::isBucketVersioningEnabled` | `GetBucketVersioning` influences admission and generation-delete reclamation assumptions. Missing decision read, though not another CAS object kind. |
| NEW | MISSED | `src/Disks/DiskObjectStorage/DiskObjectStorage.cpp:817`: `prepareRead` → `src/IO/ReadPipeline.cpp`: `buildSource` and reader callbacks | Normal CAS reads also reach `IObjectStorage` through the standard cache/gather/prefetch pipeline. Extend B7/X2’s actor mapping; existing direct `readObject` sites are not exhaustive. |
| C2, C8 | MISCLASSIFIED | `CA/Formats/CasRefCatalogFormat.cpp:84`: `encodeRefCatalog`; `Pool/CasRefCatalog.cpp:303`: `createNamespaceStep1`; `Backend/CasRequests.cpp:1080`: `writeLoop` | Catalog has no revision/nonce. Adding and deleting entries can restore identical bytes and ETag. Two identical pending insertion attempts can straddle cancellation: one creates a life, cancellation restores the original catalog, the other resurrects that same life. “Every write changes ETag” is false. |
| L6, K3, P4 | MISCLASSIFIED | `CA/Gc/CasNamespaceJanitor.cpp:52–131`: `runOnePage` | Dead-life deletion is structurally safe **only if retirement is irreversible**. Catalog replay above breaks that premise; delayed deletion can hit unchanged objects belonging to the recataloged life. Random incarnation generation does not prevent replaying an already-generated incarnation. |
| E1 | MISCLASSIFIED | `CA/Pool/CasServerRoot.cpp:612–698`: `allocateWriterEpoch`; `Backend/CasRequests.cpp:1068–1075`: `writeLoop` | Monotonic counter storage does not imply unique allocation. A times-out ambiguously; B writes identical `N+1`; A’s resolution read adopts B’s bytes. Both receive epoch `N`. This applies to generation tokens too. |
| MT2 | CONFIRMED | `CA/Pool/CasServerRoot.cpp:824–843`: `claimMount` | Same-pair refresh accepts the duplicate allocation above. U8 has a reachable production cause, not merely an assumed duplicate identity. |
| MT3 | MISCLASSIFIED | `CA/Pool/CasServerRoot.cpp:846–873`: `claimMount` | Reclaim checks different epoch, but never requires a **greater** epoch. A delayed starter can replace a terminal newer slot with an older epoch. Add epoch-regression analysis separately from decommission reset. |
| C4, C5, MT9 | MISCLASSIFIED | `CA/Pool/CasServerRoot.cpp:1245–1252`: `classifyFenceCertificate` | Terminal certificates also include `SupersededEpoch`, implemented as **any inequality**, including a lower epoch. Their dependency is broader than T3; regressing claims undermine the supersession inference. |
| MT7 | MISCLASSIFIED | `CA/Pool/CasPool.cpp:1026–1028`: destructor drain; `Pool/CasMountRuntime.cpp:1133`: `finishTeardown` | Farewell’s own target is fenced. However, draining ref lanes and cleanup duties does not establish that every timed-out plain-object, catalog, or blob request is dead at the store. “No unclear requests” overstates the drain guarantee. |
| B9 | CONFIRMED | `CA/Backend/CasObjectStorageBackend.cpp`: `publish`; `src/IO/S3/Client.cpp:840–866`: retry loop | Repeated single PUT or single-operation copy can restore an old envelope/content token. A queued exact-token GC delete can then remove a currently referenced blob. Freshness per outer publication does not fence SDK resends. |
| B9 | OVERCLAIM | `src/IO/S3/Client.cpp:583–594`: `CompleteMultipartUpload` | An already-completed AWS upload’s consumed `UploadId` does not recreate the object on completion retry; it yields `NoSuchUpload`. Remove that particular replay witness, retaining PUT/copy hazards and the synthetic-success issue. [AWS API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) |
| M3 | CONFIRMED | `CA/Pool/CasPartWriteTxn.cpp:370–386`: `reconcileMetaClean` | Fallback cleans any non-`Clean` marker without proving that the currently condemned body is the publication it displaced. Target `.meta` CAS alone does not bind body incarnation. |
| P1, P2, R1, R2 | CONFIRMED | `CA/Pool/CasPlainObjects.cpp`: `casPutObject`, `casRemoveObject` | Takeover changes `mount`, not these target keys. Fixed-content RMW and current-object removal remain unfenced intentions. However, a queued exact `If-Match` cannot overwrite different successor bytes without ABA; correct the inventory’s simplified traces. |
| C6, ST4, X1, X3 | CONFIRMED | `CasRefCatalog.cpp`: `beginRemoving`; `CasServerRoot.cpp`: `sweepOwnMountStaging`; `CasRefLedger.cpp`: `confirmExactRef`; `CasRequests.cpp`: `writeLoop` | Unchanged catalog row, shared staging prefix, local confirmation authority, and admission/application separation are genuine gaps. |
| MT10, E2, E3, O3 | CONFIRMED | `CA/Tools/CasDecommission.cpp:429–478`: retirement tail | Deleting durable epoch history permits remint; a separate cross-object window permits tombstoning an owner used by a live successor. |
| E2 | OVERCLAIM | `CA/Pool/CasServerRoot.cpp:649–674`: `allocateWriterEpoch` | The wall-clock check alone does not authorize takeover: this branch allocates above the surviving epoch, then `claimMount` applies its stronger gate. Keep the absent-counter/reset hazard; remove clock skew here as an independent takeover witness. |
| L8 | OVERCLAIM | `CA/Pool/CasRefLedger.cpp:1050–1059`: `runRecoveryWalkOnce`; `:2907–2929`: append rejection | Reading valid successor history is not itself an unsafe mutation. Appends retain the local writer epoch and reject illegal/sealed progression. U12 lacks a separate harmful execution. |
| MF1; L1 | TLA-WRONG | `CaGcRootLocalPartManifestCore:Land`; `CaRefTableSnapshotLogCore:FrontierOldWriterLosesToSeal` | `Land` delivers a **blob delete**, not manifest PUT. The frontier operator records a witness; it does not issue/deliver the old writer’s request. Other cited fragments may cover broader properties. |
| K1 | TLA-WRONG | `CaRefTableSnapshotLogCore:FrontierStaleAdvance` | Sabotage directly advances the frontier without a durable log; it does not model a delayed checkpoint `If-Match` request. |
| C4, C5 | TLA-WRONG | `CaRefCatalogCore:ReconcileCreating`, `EntryDelete` | Reconciliation deletes the modeled entry, while code transfers its creator. `EntryDelete` requires `removing`, not cancellation of `Creating`. |
| X1 | TLA-WRONG | `CaRelinkConfirmCore:FenceLoss`, `ForeignRemove`, `Gate1Answer`; `CaRelinkLaneComposition:ConfirmSource` | First model makes foreign mutation follow an already-false sender fence. Composition requires `source_exists`. Neither reproduces takeover while the zombie’s local authority still says yes. |
| X3, MT3–MT4, MT8 | TLA-WRONG | `CaMountObserveCore:Write`, `ObsFenceWrite` | Observer fence delivery can be delayed; holder `Write` is atomic with admission. It omits the queued data request landing afterward. `CaErasureProof:ZombieLand` covers abandonment delivery, not every admission/send pause. |
| X4 | OVERCLAIM | Inventory itself: “not investigated” | U15 is an unverified external dependency, not an established CAS unsafe topic. |

Proposed final unsafe-topic list:

1. **Body content-token replay** — narrowed U1.
2. **Unbound body/marker reconciliation** — U2 plus synthetic multipart success.
3. **Shared mutable path intentions** — merge U3/U4; same target-key fencing remedy.
4. **Stale lifecycle removal authority** — U5.
5. **Catalog ETag ABA and retired-life replay** — new; requires catalog revision/irreversible retirement.
6. **Ambiguous epoch attribution** — split from U8; unique allocation evidence.
7. **Same-pair mount acceptance** — remaining U8; reject occupied identical pairs.
8. **Nonmonotonic reclaim/supersession** — new.
9. **Shared staging sweep** — U9.
10. **Stale relink confirmation** — U10.
11. **Stale read authority/availability** — U11.
12. **Epoch erasure and identity reuse** — U13a.
13. **Owner retirement versus live successor** — U13b.
14. **Timing and unresolved-request assumptions** — merge U6/U7/U14 as a cross-cutting dependency, including farewell drainage; associate concrete consequences with topics above.

Remove U12 as a standalone hazard. Keep U15 outside the proven CAS list.