The atomicity does **not** consistently match the code. The adoption gap appears in multiple models, and publication retries introduce another omitted landing path.

Audited `d76b59f9295`; checked lease interactions at `3b4037ae265`. The latter preserves the relevant admission→landing and body→marker gaps. Inspected all 15/29/27/5 assigned configurations and both available results files. No files changed; no new TLC runs.

Paths below are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`:

`P` = `Pool/CasPartWriteTxn.cpp`; `L` = `Pool/CasRefLedger.cpp`; `G` = `Gc/CasGc.cpp`; `I` = `Gc/CasBlobInDegree.cpp`; `M` = `Gc/CasGcMetaWriter.cpp`; `R` = `Backend/CasRequests.cpp`; `O` = `Backend/CasObjectStorageBackend.cpp`; `C` = `Pool/CasRefCatalog.cpp`; `S` = `Pool/CasServerRoot.cpp`; `V` = `Parts/PartFolderAccess.cpp`.

Grouped rows cover actions with the same boundary. Initial seeds and model-local bookkeeping are omitted. “SOUND” conditional operations refer to one store landing; outstanding requests are assessed separately.

**`CaBlobPublishCore`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `DurablePrecommit`, `Commit` | Admission, owner validation, log create, resolution, checkpoint publication (`P:precommitAdd/promote`; `L:commitRefChunk`) | AMBIGUOUS-AS-DEFINITE | Log can land before timeout or fence loss is reported. | Durable ownership and caller knowledge differ; physical commit can follow admission loss. | Separate log landing, acknowledgement, wedge and seal. Code already records uncertainty and seals successor epochs. |
| `ObserveBody` | One `HEAD` (`P:ensureBlobPresent`) | SOUND | Returned presence/size/token are one observation; later changes need not alter the cached result. | None from atomicity. | Keep cached observation. |
| `ObserveMeta`, `ObservationInvalidated`, `AdoptCondemned`, `MetaCleanNeedsRecovery` | Earlier body `HEAD`, later marker `GET`, decision (`P:ensureBlobPresent`) | UNSOUND | Actions inspect **current** body presence; code does not recheck it. GC can remove body and marker between reads. | `CommittedRefHasContent` can fail. | Cache `HEAD`; read marker independently without `body.present` guard. Code: reread body after marker/backfill; abort or republish on absence. |
| `BeginPublication`, `BackendLand`, `LateBackendLand`, `LoseFence` | Selection/admission, transport, repeated physical writes (`P:ensureBlobPresent`; `R:publish`; `O:publish`) | UNSOUND | `O:publish` uses ordinary retry settings. Identical envelopes can land repeatedly. Fence loss is disabled during `Publishing`. | Token recurrence; fresh serial deleted by old authorization. | Request multiset, repeat landings, fence loss in every phase; enlarge write bound. Code: single physical publication attempt through every transport layer. |
| `ReconcileMetaClean` | Metadata create/replace, conflict `GET`, RMW loop, readiness (`P:ensureBlobPresent`) | UNSOUND | GC changes marker after observation; code’s RMW can overwrite `Condemned`. Model requires non-condemned marker and present body. | Conceals stale `Clean` and dangling adoption. | Model captured precondition, conflicts, fallback reads and late writes. Code: preserve conflicting condemnation; eliminate marker ABA. |
| `GCCondemn` | Body observation, queued marker write, ledger publication (`G:fold`; `M:scheduleCondemnMarkerWrite`) | UNSOUND | Writer becomes ready while previously scheduled condemnation remains runnable. Model disables condemnation whenever a writer is ready. | Hidden marker race; presence invariant can fail in composition. | Captured candidate/job plus separate ledger adoption. Code already separates these steps. |
| `GCDelete` | Body conditional delete and marker deletion (`G:performRedeleteIo/applyRedeleteOutcome`; `M:deleteConfirmedMeta`) | UNSOUND | Writer runs between body deletion and asynchronous marker cleanup. | Adoption gap and late deletion paths disappear. | Separate body landing, response, marker job. Body token condition is sound **only without token recurrence**. |

The results’ green proof retains the fictitious body recheck. Every publication configuration also limits `MaxWrites` to two, excluding important seed/replay/replacement combinations.

**`CaGcCondemnMarkerGate`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WPrecommit`, `WCommit`, `WDrop` | Validation, log create, resolution (`P:precommitAdd/promote/abandon`; `L:commitRefChunk`) | AMBIGUOUS-AS-DEFINITE | Timeout does not undo landed ownership; namespace removal can overlap uploads. | Incorrect durable-versus-known edge state. | Pending append/landing/acknowledgement and cancellation; retain existing wedges/seals. |
| `WAdopt` | Body `HEAD`, marker `GET`, optional `Clean` create, proof return (`P:ensureBlobPresent`) | UNSOUND | `GSettleRedelete` and `GMetaDeleteJob` run between the reads. Backfill conflict is ignored in code. | `NoDangle`. | `WHead` with cached token, `WMetaRead`, `WBackfill`, `WReady`; body reread addresses the demonstrated gap. |
| `WPublish` | Observation, transport selection, landing; sometimes reconciliation (`P:ensureBlobPresent`) | UNSOUND | `SplitPublish` separates reconciliation only; racing observation and identical SDK resends remain absent. | Hidden replacement/replay schedules. | Cached observation plus independently landable requests; single-attempt transport. |
| `WReconcile`, `WLateLand` | Conditional write and RMW retries (`P:ensureBlobPresent`; `R:readModifyWrite`) | SOUND per conditional landing; AMBIGUOUS-AS-DEFINITE otherwise | Conditions bind marker bytes, not body incarnation. `OutstandingReconcile="none"` omits surviving attempts; special configs expose some ETag ABA. | `MarkerPreservedPending`/`NoDangle`. | Track every issued request after success or failure; prevent ABA and preserve conflicting condemnation. |
| `GCut` | Catalog/checkpoint/list/log reads, fold (`G:collectHotRoundInput/fold`) | UNSOUND as global snapshot | Destination prefix read before add; source prefix read after removal: code can observe zero despite continuous ownership. | Extra condemnation; safety needs later-cut and adoption protection. | Per-life cuts and consumed frontiers; retain code’s exact coverage barriers. |
| `GObserve`, `GObserveMeta` | Individual prefetched `HEAD`/`GET` (`G:fold`, read-ahead) | SOUND | Each caches one request result. | None; stale observations are explicitly possible. | Preserve separate observations. |
| `GSettleCondemn`, `GSettleSupersede`, `GSettleCarryRetry`, `GStaleAttempt` | Token capture, marker scheduling/writing, ledger publication (`I:closeBlob/settleEntry`; `G:fold`; `M:writeCondemnedMeta`) | UNSOUND when fused | Writer changes body/marker while job waits. `AsyncMarker` handles scheduling partly; obsolete registry confirmations lack a current counterpart. | Hidden marker/ledger disagreement. | Separate captured token, job, request and adoption. Capturing an old body token alone is safe: replacement makes exact deletion miss. |
| `GMarkerJob`, `GMetaDeleteJob`, `GStaleMarkerWrite` | Marker `GET`, conditional mutation, retries (`M:writeCondemnedMeta/deleteConfirmedMeta`) | SOUND per landing; AMBIGUOUS-AS-DEFINITE on job completion | Changed marker token rejects mutation; newer condemnation round blocks old cleanup. A timed-out request can nevertheless outlive the job/drain. | Missing late marker effects. | Independent in-flight requests beyond queue completion; conditional guards already exist. |
| `GSettleGraduate/GraduateDeferred`, `GSettleIdle/Spare/SpareLoud/Suppressed` | Cached settlement, artifact writes, round CAS (`I:settleEntry`; `G:runRound`) | SOUND for successful publication; AMBIGUOUS-AS-DEFINITE otherwise | `gc/state` token excludes competing adoption; immutable attempt artifacts stay invisible until adoption. | Timeout can publish an unacknowledged round. | Separate candidate, state-CAS landing and response; retain token guard. |
| `GSettleRedelete` | Selection, pool dispatch, body `HEAD`, exact delete, outcome, marker job, round CAS (`G:redeleteBlobs/performRedeleteIo/applyRedeleteOutcome`) | UNSOUND | Deposed leader’s captured delete survives another leader sparing/dropping the entry; delete may land after failure. | `NoDangle` with false adoption/token recurrence. | Independent delete requests carrying captured tokens; split responses and ledger adoption. No local reread fences an already issued delete. |

**`CaIncarnationCore`**

This is substantially historical: embedded CAS manifests, registry fences, writer retire views and tree cascades have no direct counterpart at the pinned commit. Its header delegates publication refinement to the unsound focused model above.

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WCreate`, `WResurrect`, `WOverwrite` | Observation, envelope selection, publication/retries (`P:ensureBlobPresent`; `O:publish`) | UNSOUND | Competing writers and repeated identical-byte landings are excluded by atomic creation/fresh-token allocation. | `INV_NO_RETURN`, presence invariants. | Import corrected publication request model; enforce physical single attempts. |
| `WReuse`, `WResolveEvidence` | Presence plus adoption gate (`P:ensureBlobPresent`) | UNSOUND | Missing separate marker read; same adoption gap. | `INV_NO_DANGLE`. | Cached body/marker observations; reread body. |
| `WReobserve`, `WEvObserve` | One abstract `HEAD` | SOUND as observations | Snapshot token may become stale afterwards. Current promotion does not perform these probes. | They cannot justify current promotion. | Retain snapshots; remove fictitious publish-time oracle. |
| `WEvidence`, `WPublish`, `WDrop` | Source resolve, manifest read, proof recording, later owner append (`V:getView`; `P:adoptEvidence/promote`; `L:commitRefChunk`) | UNSOUND; AMBIGUOUS-AS-DEFINITE append | Model checks live reachability/current dependencies atomically with publish. Code trusts cached evidence without those checks. | `INV_NO_DANGLE`/`INV_NO_LOSS`. | Source-observation and destination-precommit states; confirm exact source binding after precommit, or hold durable source ownership. |
| `WRegister`, `GFenceRegistry`, `GFenceShard`, `WRefreshView` | No direct counterpart; replaced by catalog lifecycle, epochs and log seals (`C:createNamespace`; `L:runRecoveryWalkOnce`) | UNSOUND as current mapping | Old registry-floor/fence argument cannot bind current requests. | Green configurations do not prove current protocol. | Replace variables/actions with namespace lives, epochs, seal slots and request landings. |
| `GStartRound`, `GFold`, `GRetire`, `GForget`, `GEndRound` | Private fold, reads, immutable artifacts, state CAS (`G:fold/runRound`; `I:closeBlob`) | UNSOUND durable-state placement | Model exposes evolving `gcRound`, edges, cursor and retirement before their adoption. | Incorrect authority seen by concurrent leaders/writers. | Separate private candidate from adopted state; token-bound adoption. |
| `GRecheckDelete`, `Land`, `ApplyPendCascade` | Pool issue, exact deletion, response, subsequent bookkeeping (`G:redeleteBlobs`) | UNSOUND fusion; SOUND token condition | Delete landing is not confirmed response or durable ledger removal. Current code has no tree cascade. | Missing stale requests; historical cascade proof. | Split landing/acknowledgement/adoption; remove obsolete tree machinery. |
| `Trim` | Historical journal watermark | SPLIT-FINER ordering abstraction | It advances one unit and never deletes log bytes. | Cannot detect physical coverage loss. | Model immutable log deletion under durable coverage. |
| `WHbStart`, `WHbRenew`, `WCrash`, `GObserveHb`, `GDebrisRetire` | Slot writes/reads (`S:claimMount/renew/computeHeartbeatFloor`); no current blob-debris retirement counterpart | AMBIGUOUS-AS-DEFINITE; UNSOUND mapping | Process death leaves slot durable; renewal outcome can be unknown. Single slot observation is sound. | Missing lease/epoch interactions. | Separate process, slot, admission and requests; replace historical debris classification. |
| `FGRead`, `FGCommit` | Recovery reads, artifacts, state CAS (`G:rebuild`) | SOUND exact-cut adoption; AMBIGUOUS-AS-DEFINITE response | Saved frontier matches replayed immutable prefix; competing adoption changes state token. | Unknown commit outcome omitted. | Pending state request and separate acknowledgement; existing guards suffice. |

**`CaEdgeBeforeObserve`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `Precommit` | Manifest staging then owner append (`P:stageManifest/precommitAdd`) | SOUND successful closure; AMBIGUOUS-AS-DEFINITE response | Code records attempted ownership before uncertain append and protects manifest cleanup. | Unknown durable precommit omitted. | Separate staging, append landing and acknowledgement. |
| `ViewAdvance` | Installed view plus advertised acknowledgement | UNSOUND current mapping | Current writer has no modeled retire-view installation; graduation uses rounds and marker reads. | Historical ordering premise. | Replace with actual marker observations/round publication. |
| `Adopt`, `AdoptGone` | `HEAD`, marker check, possible publication/reconciliation (`P:ensureBlobPresent`) | UNSOUND | GC deletes between reads; displacement also cannot instantly revoke queued requests. | `INV_NO_DANGLE`. | Cached reads, publication landing, independent exact-delete requests; reread body. |
| `Promote` | Tokenless probe/displacement plus commit (`P:promote`) | UNSOUND | Code performs **none** of the modeled tokenless probes; trusts `TrustedManifest`. | Dangling trusted adoption. | Model source lifetime; confirm source after destination precommit. |
| `GcSettle`, `GcExecute` | Prefix fold, settlement, queued deletion, outcomes/adoption (`G:fold/redeleteBlobs`; `I:settleEntry`) | UNSOUND | Live `Deg` replaces cached fold degree; deletion, entry removal and round advancement collapse separate effects. | Stale-cut/deposed-delete schedules hidden. | Cached degrees, pending requests, separate adoption. Exact-token guard already exists. |

**Top findings**

1. **VERIFIED-IN-CODE — dangling adoption across three models.**  
   Writer `HEAD` sees body; GC deletes body and marker; writer `GET` sees absent marker and returns `Materialized`.  
   Promotion skips body revalidation. `WAdopt` and `ObserveMeta` suppress this schedule; `Adopt` abstracts it away.

2. **VERIFIED-IN-CODE — identical-envelope replay below the engine.**  
   `Retry::asSingleAttempt` bounds the engine, while `O:publish` selects ordinary transport retries.  
   After owner release permits condemnation, a resend can reproduce the condemned ETag; a stale exact delete can remove the replayed body. Fresh-token allocation conceals this.

3. **INFERRED — trusted-source lifetime gap.**  
   Source resolve/manifest snapshot precedes destination precommit; another actor drops the source and GC deletes its blobs.  
   Current promotion trusts the retained entries. The inspected view holds decoded memory, not durable ownership; caller-level exclusion would need proving.

4. **VERIFIED-IN-CODE — job completion and fence checks do not settle issued requests.**  
   Admission precedes transport; timeout/drain can precede actual landing. Deposed GC work remains independently relevant.  
   Epoch seals protect successor log slots, but do not fence blob or marker requests; models need landing and response states separately.