Audited against `d76b59f9295`; checked relevant lease boundaries at `3b4037ae265`. The latter preserves the admission-to-request gap and the blob/manifest paths identified below. No files changed; no TLC runs performed.

Code abbreviations, relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`:

`T` = `Pool/CasPartWriteTxn.cpp`; `L` = `Pool/CasRefLedger.cpp`; `C` = `Pool/CasRefCkpt.cpp`; `G` = `Gc/CasGc.cpp`; `O` = `Gc/CasOrphanManifestSweep.cpp`; `M` = `Gc/CasGcMetaWriter.cpp`; `R` = `Backend/CasRequests.cpp`.

SOUND applies to the indicated fusion, not to omitted request outcomes. Related actions with identical boundaries are grouped.

**`CaRefTableSnapshotLogCore`**

| action | code steps fused (file:function) | verdict | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WAppendStart` | `L:flushRefBatch`, `prepareRefChunk`: sample state, allocate, encode, arm attempt | SOUND | Queue leadership excludes another local append; unresolved attempts block allocation. Separate instances require epoch/slot fencing. | Local allocation is justified. | Add per-instance state when composing mount models. |
| `WResolveDurable`, `LatePredecessorPut`, `WOrphanLands` | `L:commitRefChunk`: log CREATE, checkpoint GET/CAS, admission, install, acknowledgement | UNSOUND | Legacy actions equate physical log durability with applied history. GC can sample the old checkpoint between requests. | Frontier/install ordering is untested. | Use the frontier slice in the combined model; code already separates and guards these stages. |
| `WResolveConclusiveReject`, `WResolveSealRejected` | `L:resolveWedgeOnce`: conditional CREATE, settling GET, occupant classification | SOUND | Occupying an immutable slot conclusively rejects another conditional create there; absence alone does not. | Correct rejection boundary. | Preserve ambiguous attempts and exact bytes; existing code does. |
| `WriterPublishSnapshot`, `WriterCkptAdvance` | `L:tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl`; `C:publishCkpt`: capture, CREATE, GET/merge/CAS | SOUND | Captured prefix is immutable; intervening checkpoint writes invalidate stale CAS tokens; semantic maximum prevents regression. | Publication ordering survives cleanup. | Retain separate body/pointer actions. |
| `GcCleanupLog`, `GcCleanupSnap` | `G:cleanupRefObjects`: checkpoint/base validation, authority GETs, later batch DELETE | SOUND | Adopted cursor and checkpoint bounds advance monotonically; stale bounds under-clean. | Atomic gate is safe, but coverage differs. | Model code’s strict log boundary: `< checkpoint`, retaining the same-id log and predecessor seal. |
| `RReadCkpt`, `RFetchBase`, `RWalkStep`, `RWalkVanish` | `L:runRecoveryWalkOnce`: checkpoint GET; base-log, predecessor-seal, snapshot GETs; missing-read revalidation | SOUND | Successful immutable reads remain usable after deletion. Legal cleanup cannot remove the current base; token movement explains vanished older bases. | Model omits the recovery triple and uses base movement instead of token movement. | Store sampled token and copied bodies; represent each GET. |
| `RSealSlot`, `RAdoptStraggler` | `L:runRecoveryWalkOnce`: GET absence, conditional seal CREATE, occupant GET, checkpoint CAS, revalidation/install | AMBIGUOUS-AS-DEFINITE | Slot occupancy fences stragglers, but checkpoint publication can fail after the seal lands. | Legacy “done” skips an interrupted publication state. | Separate occupancy, checkpoint publication, and install; retain unresolved requests. Code already refuses incomplete recovery. |
| `FrontierIssueCheckpoint`, `FrontierIssueRecoverySuccessor`, `FrontierIssueSuccessorSeal`, `FrontierCheckpointLinearizes` | `C:publishCkpt`; `R:writeLoop`: GET/merge, admission, conditional PUT | SOUND | Model explicitly permits issued CAS landing after fence movement; checkpoint-token movement rejects stale bytes. | Good store-linearization boundary. | Keep tokens bound to individual requests, including resends. |
| Remaining frontier reads, validation, seal/body creation, snapshot publication, installation | `L:runRecoveryWalkOnce`, `commitRefChunk`, snapshot publisher | AMBIGUOUS-AS-DEFINITE | Lost-after-landing checkpoint responses exist; timeout-before-landing requests cannot survive recovery in this slice. Snapshot/seal/log creates lack equivalent outstanding channels. | Late effects and false resolution of absence are excluded. | Add outstanding request records, separate landing/response/give-up, and per-actor recovery state. |

`RScanStep`/`RScanInstall` are historical LIST sabotage actions, not the pinned recovery implementation. Their per-key enumeration is SPLIT-FINER than a page.

**`CaBuildRootPrecommit`**

| action | code steps fused (file:function) | verdict | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WriteBlob` | `T:ensureBlobPresent`: admission, HEAD, body publication, marker reconciliation, proof return | AMBIGUOUS-AS-DEFINITE | Body publication can time out, land later, or resend identical envelope bytes below the engine. | `present`/`uploaded` assume a settled publication. | Add request/outcome state and incarnation tokens; control transport resends. |
| `AdoptBlob` | `T:ensureBlobPresent`: body HEAD, marker GET, optional marker CREATE, admission checks | UNSOUND | GC deletes body and marker between HEAD and GET. | `INV_NO_DANGLE_COMMITTED` can fail after publication. | Store separate observations; re-HEAD after a missing marker, preserving pending-delete protection. |
| `Precommit`, `GcFold` | `T:stageManifest`, `precommitAdd`; `G:foldManifestEdges`: body CREATE, owner append, later body GET/fold/adoption | UNSOUND | Model immediately installs inline closure edges; pinned owner records name a manifest whose body GC reads separately. | `INV_NO_LEAK` and protection rely on nonexistent instantaneous accounting. | Separate body, owner history, candidate edges, adopted edges; protect active staged manifests from sweep. |
| `Commit`, `PublishTableRef`, `CommitAbort` | `T:promote`: manifest GET, cached dependency proofs, owner check, log CREATE, checkpoint CAS | UNSOUND | GC can delete after the manifest GET. Blob presence is not re-read at commit. | Presence-at-publication is stronger than code; committed dangle is hidden. | Add sampled body/proofs and pending publication; prevent stale sweep nominations. A reread alone leaves another gap. |
| `DropTableRef`, `RemovePrecommit`, `GcReclaimPrecommit` | `L:appendRefOps`, `sweepStalePrecommitsNow`: select current owner, append transition | SOUND | Selection is repeated inside the serialized lane; immutable transaction bytes carry the whole owner transition. | No local ownership gap. | Model durable watermark observation separately from `BuildDie`/`BuildFreeze`. |
| `GcDelete` | `G:fold`, `redeleteBlobs`, `performRedeleteIo`: in-degree decision, scheduling, HEAD, conditional DELETE | UNSOUND | A writer publishes between decision and landing; a deposed leader retains its earlier decision. | `INV_NO_DANGLE_COMMITTED` can fail. | Add retired token and outstanding delete; enforce adoption/rematerialization protection rather than instantaneous reachability. |

**`CaRefDeltaIntakeCore`**

| action | code steps fused (file:function) | verdict | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WAppendDurable`, `EpochSeal` | `T:ensureBlobPresent`; `L:commitRefChunk`, `runRecoveryWalkOnce`: rematerialize, marker write, log/seal CREATE, checkpoint CAS | UNSOUND | GC acts after materialization/adoption and before append. The model clears condemnation/deletion exactly when the ref becomes durable. | `NoAckedLoss` is protected by an artificial atomic rematerialization. | Separate blob observations/publication, log durability, frontier, acknowledgement; repair adoption gate. |
| `WAppendAbandon` | `L:commitRefChunk`: classify rejection or unknown outcome | AMBIGUOUS-AS-DEFINITE | No late landing survives abandonment here. | Reusable slot is assumed conclusively empty. | Import every-attempt/outstanding-request machinery; code already wedges sent ambiguity. |
| `BeginRound`, `HintReturn`, `WalkStep` | `G:fold`: per-life checkpoint GETs, LIST pages, pooled exact-log GETs, decode | SOUND | Each frozen CTE bounds an immutable prefix; namespaces need no simultaneous checkpoint sample. LIST supplies no authority. | Sequential reads are sufficient. | Represent cached read results when adding concurrent leaders. |
| `ProbeAbsent` | `G:fold`: absent GET, candidate hold, seal write, later state CAS | UNSOUND | Hold becomes globally durable before its candidate wins; a losing leader artificially suppresses other destruction. | `HoldSuppresses` gets stronger protection than code. | Separate candidate/adopted holds and leader views; code publishes holds through adoption. |
| `FoldCommitWin`, `FoldCommitLose` | `G:runRegularRound`: artifact writes, `gc/state` CAS, response/local adoption | AMBIGUOUS-AS-DEFINITE | CAS may land although the caller cannot establish success. “Lose” always leaves durable state unchanged. | Exactly-once/retry states are omitted. | Separate durable pointer, response, and local cursor; reread authoritative state after ambiguity. |
| `Condemn`, `Delete` | `G:fold`, `performRedeleteIo`; `M:scheduleCondemnMarkerWrite`, `scheduleConfirmedMetaDelete` | UNSOUND | Destruction occurs before this round’s CAS; pooled deletes can land after another leader wins. Marker changes are separate requests. | `NoAckedLoss`/`HoldSuppresses` may fail in composition. | Add candidate decisions, marker jobs, pending deletes and landings independent of CAS outcome. |
| `WRaiseSnap`, `Cleanup` | Snapshot publisher; `G:cleanupRefObjects` | SOUND | Ordered body publication plus max-CAS; cleanup requires adopted coverage. | Safe monotone bounds; same boundary mismatch as above. | Retain base-log/seal witnesses and model ambiguous requests. |

`BuildRefWalkPlans`/`AdapterEnrich` operate on an immutable catalog cut; they are local construction. `HideLog`/`RevealLog` and sabotaged `Rebuild` are synthetic fault controls.

**`CaGcRootLocalPartManifestCore`**

| action | code steps fused (file:function) | verdict | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WStageManifest`, `WManifestBodyArrives`, `WUploadBlob` | `T:stageManifest`, `ensureBlobPresent`: admission, CREATE/PUT, response, registration | AMBIGUOUS-AS-DEFINITE | Late staged bodies may arrive while unowned; `WManifestBodyArrives` permits only build-owned arrival. Blob resends need not mint new tokens. | Debris and incarnation-ABA schedules omitted. | Outstanding bytes/token records; allow unowned late arrival; control blob resends. |
| `WPrecommitAdd`, `WDropRef`, `WAbandonPrecommit`, `WMutableUpdate` | `T:precommitAdd`; `L:appendRefOps`, `dropRef`, `updateRefPublishedAt` | SOUND | Current-state selection is lane-serialized; transaction bytes are indivisible. | Owner-operation atomicity is justified. | Split log/frontier/response. Remove omniscient `mfDeleted` admission guard: code cannot observe that set. |
| `WPromote`, `WPublishCommitted`, `WRepoint` | `T:promote`: manifest GET, proof/owner checks, later append | UNSOUND | Sweep nomination predates ownership; promotion does not change the manifest token. Model checks current body/blob presence atomically. | `INV_NO_DANGLE` fails. | Sampled proofs plus publication actions; unconditional build-floor gate for sweep. Owner move/repoint operations themselves remain atomic. |
| `GFoldTransition`, `GScatterDelta` | `G:foldManifestEdges`, `fold`: pooled log/body GETs, reducers, artifacts, state CAS | UNSOUND | Candidate edges/cursors immediately become shared adopted state; losing attempts cannot remain private. | `INV_ONLY_ADOPTED_VIEWABLE` is disconnected from actual accounting. | Per-attempt candidate state; adopt through one pointer CAS. Per-event folding is otherwise SPLIT-FINER than transaction/batch processing. |
| `WRefreshView`, `GFenceRegistry`, `GFenceShard`, `GCoordFence`, `GCoordSeal` | `T:promote`; `G:fold`, `runRegularRound` | UNSOUND | Pinned code has no corresponding writer retire-view refresh or all-shard publication-fence protocol. | Safety depends on historical guards. | Replace with exact CTE cuts, actual marker reads and adoption boundaries; recheck `INV_NO_DANGLE`. |
| `GRetireBlob`, `GRecheckDelete` | `G:fold`; `CasBlobInDegree.cpp:foldDeltasIntoGeneration`: cached HEAD/marker reads, in-degree decision, later scheduling | UNSOUND | Another leader changes accounting after the cached decision; token equality does not bind reference liveness. | `INV_NO_LOSS` can fail with stale adoption. | Separate observations/decision/request; preserve condemnation until outstanding deletes are safe. |
| `Land` | `G:performRedeleteIo`, `applyRedeleteOutcome`: HEAD, token DELETE, response, metadata job | SOUND / AMBIGUOUS-AS-DEFINITE | Conditional DELETE protects replacement tokens; landing does not imply delivered confirmation or adopted retirement removal. | Pending retention/marker windows omitted. | Separate landing, response, marker cleanup, state adoption. |
| `Trim`, `GDeleteManifest` | `G:cleanupRefObjects`, post-CAS manifest deletion | SOUND | Adopted decrements precede deletion; normal removed identities cannot be re-owned. | Ordering is justified. | Model ambiguous batch outcomes; retain recovery witnesses. |
| `GMarkSweepEligible`, `GOrphanSweep` | `O:planManifestCursorPage`: LIST, catalog/floor/recovery GETs, nomination; `G:runRegularRound`: CAS, HEAD, DELETE | UNSOUND | Rowless candidates bypass the floor; ownership can change before DELETE without changing body token. | `INV_NO_DANGLE` fails. | Add sampled catalog/owners and nominations; apply floor to every candidate. |
| `GMintAttempt`, `GAdopt`, `GDeposedFinalWrite` | Artifact creation and `G:runRegularRound` pointer CAS | SOUND / AMBIGUOUS-AS-DEFINITE | Attempt-scoped artifacts remain invisible until adoption; separate writes and lost CAS responses are omitted. | Detached bookkeeping proves no accounting visibility property. | Connect attempts to candidate edges/cursors and durable pointer. |

Retired `GDiscoverSkip`/`GDiscoverRead`/`GReclaimDeadPrecommit` have no active production counterpart. Sabotage-only owner/reuse actions intentionally describe rejected behavior.

Configs/results: 34 snapshot, 9 build, 15 intake, 44 manifest configs inspected. Manifest results explicitly leave sharding unproven through model errors and lazy-trim unproven through timeout.

**Top findings**

1. **VERIFIED-IN-CODE — stale orphan nomination can delete a committed manifest.**  
   Writer stages before catalog creation; GC samples no row and nominates without a floor check (`O:761–775`).  
   Writer precommits and promotes; GC later deletes the unchanged body token (`G:1374–1379`). `INV_NO_DANGLE` fails.

2. **VERIFIED-IN-CODE — stale body HEAD survives marker disappearance.**  
   Writer HEAD succeeds (`T:402`); GC deletes body and subsequently marker.  
   Writer GET finds no marker (`T:424–460`), returns `Materialized`; promotion trusts that proof. Committed blob loss follows.

3. **INFERRED — identical upload resend can defeat fresh-incarnation reasoning on S3.**  
   An old envelope lands, is condemned/deleted, then an SDK resend restores identical bytes after a fresh writer’s replacement.  
   A retained old-token DELETE can match again. Default publication settings and `src/IO/S3/Client.cpp:doRequestWithRetryNetworkErrors` permit resends; the models always mint fresh tokens.

4. **VERIFIED-IN-CODE — losing GC rounds can already have deleted data.**  
   GC computes/schedules deletion; another leader advances `gc/state`; the old conditional body DELETE lands.  
   Its later state CAS loses (`G:954` before `G:1164`). The intake model forbids this entire schedule; data-loss exposure depends on the adoption/token gaps above.