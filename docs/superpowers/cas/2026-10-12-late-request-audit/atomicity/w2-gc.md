**No: several action boundaries are too coarse, and two models describe earlier protocols.** The strongest new loss scenario is a delayed writer clearing a newer condemn marker after its namespace was removed.

Static audit against `d76b59f9295`; lease actors considered at `3b4037ae265`. The relevant publication behavior is unchanged there. All assigned configurations and existing results were read, including B140’s `m_*.cfg`; TLC was not rerun. No files were edited. Wave 1 defects are referenced as actors, without repeating their findings.

Paths below are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`:

`P` = `Pool/CasPartWriteTxn.cpp`; `L` = `Pool/CasRefLedger.cpp`; `G` = `Gc/CasGc.cpp`; `B` = `Gc/CasBlobInDegree.cpp`; `M` = `Gc/CasGcMetaWriter.cpp`; `C` = `Pool/CasRefCatalog.cpp`; `J` = `Gc/CasNamespaceJanitor.cpp`.

**CaGcShardIncarnationCore**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WCreateBlob`, `WPublish` | `P:ensureBlobPresent`: body PUT, marker reconciliation, readiness; `C:createNamespace`, `L:commitRefChunk`: catalog/checkpoint/log publication | UNSOUND | Namespace removal and GC can run before delayed marker reconciliation; finding 1. Birth and publication also span objects. | `INV_NO_DANGLING` can fail. | Separate body landing, marker operations, precommit and promotion; code fix in finding 1. |
| `WRefreshView`, `GStartRound`, `GFenceShard`, `GFenceDone` | Historical round-view, newborn-floor and mutable-shard fencing protocol | UNSOUND — mapping | Pinned code has no corresponding birth-round read or shard fence append. `G:runRegularRound` publishes the round at the final CAS, not at round start. | The historical floor/completeness proof does not certify this code. | Replace oracle views and mutable shards with catalog cuts, immutable logs and adopted seals. No corresponding floor guard exists to patch. |
| `WDrop`, `WTombstone` | `L:dropRef`, `dropNamespaceImpl`, `commitRefChunk`: construct operations → log create → install | SOUND | Owner removal and terminal operations share one immutable envelope. Queue leadership serializes local writers; successor epoch seals arbitrate outstanding appends. | Atomic logical removal is justified. | Preserve envelope boundary; model catalog `Removing` separately. |
| `GFold` | `G:fold`: log/manifest GETs, merge, run/seal publication | SPLIT-FINER | Per-record processing exposes more intermediate states than batch publication. Inputs use immutable identities; missing required inputs fail closed. | Conservative processing granularity. | Keep durable adoption distinct from temporary fold state. |
| `GRetire` | `G:fold`: body HEAD → marker job → condemned run row | SOUND | The row retains the observed token. Replacement after HEAD makes its later exact delete mismatch; marker effects are conservative. | Stale observation alone cannot delete a replacement. | Retain captured token; separately represent marker jobs and failures. |
| `GRecheckDelete` | `B:settleEntry` → pool scheduling → `G:performRedeleteIo` | UNSOUND | Finding 1 permits same-token reuse after the zero-degree cut and before deletion. | `INV_NO_DANGLING`. | Capture cut/token; split scheduling and landing; repair delayed `Clean`. |
| `Land` | `G:performRedeleteIo`, `applyRedeleteOutcome`: conditional DELETE → response/bookkeeping | AMBIGUOUS-AS-DEFINITE | Conditional landing is safe against replacement, but landing does not imply a received response. | Retire-entry removal can be modeled prematurely. | Separate landing, response loss and confirmed settlement; code already retains rows on failures. |
| `GReclaim` | `C:deleteCompletedRemovingAtSnapshot`; `J:runOnePage`: proof → catalog CAS → LIST/HEAD/DELETE | SOUND | Rebirth uses another physical life identity. Catalog CAS protects replacement rows; janitor deletes old-life keys with observed tokens. | Interleaving rebirth cannot redirect reclamation. | Represent catalog retirement and individual physical deletions separately. |

**CaRetiredInRun**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WriterAdd`, `WriterResurrect` | `P:precommitAdd`, `ensureBlobPresent`: journal → observations → PUT → `Clean` | UNSOUND | Cancellation can remove the protecting source while an upload task remains active; finding 1. | `INV_NO_LOSS`, once the replacement writer’s edge enters coverage. | Add upload phase, captured marker, pending reconciliation and cancellation actions; fix reconciliation. |
| `WriterStaleReuse` | `P:ensureBlobPresent`: journal, body HEAD, marker GET, decision | UNSOUND | Atomic reuse excludes the supplied wave 1 interleaving actors. Its round predicate also substitutes for an explicit outstanding read. | Reuse safety is conditional on omitted request boundaries. | Capture HEAD/GET results and read phase independently; compose with wave 1 corrections. |
| `WriterRemove` | `L:dropRef` → immutable log create | SOUND | Durable ownership and its removal event are the same log transaction. | No separate durable ref/journal race. | Add unresolved-source cancellation as a separate actor. |
| `FoldRound` | `G:fold`, `runRegularRound`: cut, HEADs, marker jobs, runs/seal, deletes, drain, state CAS | UNSOUND | Writers and competing leaders interleave throughout; pre-CAS effects survive a losing CAS. Finding 1 crosses several rounds. | Atomic round prevents the loss witness. It also incorrectly writes `Clean` on spares. | Start with the witness’s phases, then split requests. Match code’s add-only GC metadata and graduation confirmation. |

**CaRetiredInRunFoldAbortWitness**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `EdgeAdd`, `BodyResolve` | `P:precommitAdd`, `ensureBlobPresent`: fresh/resurrection PUT → marker reconciliation → resolution | UNSOUND | Fresh uploads remain atomic; cancellation of unresolved uploads is absent. Finding 1 therefore remains impossible. | `INV_NO_LOSS`. | Preserve pending upload tasks independently of live ownership; add cancellation and delayed `Clean` actions. |
| `WriterRemove` | `L:dropRef`: one removal envelope | SOUND | Envelope is atomic; its `~unresolved` restriction covers less than namespace removal permits. | Missing cancellation actor, rather than a split durable removal. | Add cancellation while unresolved; code need not wait for uploads. |
| `LeaderCapture` | `G:acquireOrRenewLease`, parent-seal/log reads | SOUND | Lease mutation binds `gc/state`’s ETag; parent artifacts are immutable. Actual adoption checks are stronger than the model’s generation-only check. | Stable captured baseline. | Model lease ETag and explicit read cut for composition. |
| `LeaderExecute` | `G:redeleteBlobs`; `M`: marker GET/CAS/DELETE jobs; `B:putDeterministicArtifact` | UNSOUND | No lock spans the batch. Individual body/marker conditional pairs bind their own tokens, but aggregate execution excludes intervening writer tasks and wave 1 actors. | Split witness still cannot establish request-level no-loss. | Separate per-blob observations, scheduling, landing, marker jobs and outcomes; fix finding 1. |
| `LeaderAdopt` | Artifact PUTs → `G:runRegularRound` state CAS | SOUND | Unique attempt keys, byte-equality checks and the state ETag bind adoption to verified artifacts. Losing attempts leave harmless unadopted artifacts. | Publication fusion is sound for reachability safety. | Record artifacts before adoption, including losing attempts; add unknown CAS outcomes. |

**CaB140DangleMerge**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `WUploadBlob` | `P:ensureBlobPresent`: physical PUT | SOUND | As a store-landing action, one PUT is a valid boundary. It supplies no marker/readiness protocol. | Limited publication abstraction. | Add those phases when composing with writers/GC. |
| `WPublishTree` | `P:stageManifest`, `precommitAdd`, `promote`: manifest PUT → ownership → readiness → committed binding | UNSOUND | Separate publication and readiness requests admit sibling actors, including finding 1. | `INV_NO_LOSS`. | Separate staging, precommit, body resolution and promotion. |
| `WDropTree` | `L:dropRef`: immutable owner-removal log | SOUND | Binding removal and journal event share bytes. | Logical removal is atomic. | Preserve boundary. |
| `GAcquireLease`, `GFold`, `GCascadeStrip` | `G:acquireOrRenewLease`, `foldManifestEdges`, `fold`: conditional acquisition, immutable reads, local merge | SOUND | Acquisition binds state token; immutable manifest identities prevent unrelated replacement of captured children. Losing leaders cannot adopt through the old state token. | Stable inputs and guarded adoption. | Keep lease token and temporary fold state explicit. |
| `GPersist`, `GCommit` | `B:putDeterministicArtifact` → `G:runRegularRound` state CAS | SOUND | Immutable seal owns coverage and run references; one conditional pointer publication selects both. | Cursor/edges commit remains coherent. | Model ambiguous publication separately. |
| `GTrim` | `G:cleanupRefObjects`: sealed cursor + recovery triple → chunk deletes | SOUND | Covered extent is durable and monotone; life-qualified immutable keys cannot become successor logs. | Delayed deletion still removes covered records. | Keep adopted coverage and retained recovery triple explicit. |
| `GRetire`, `GDelete` | HEAD/condemnation → cut-based decision → conditional DELETE/outcome | UNSOUND | Hash-only state lacks incarnation tokens and outstanding requests; finding 1 allows live reuse of the selected incarnation. | `INV_NO_LOSS`, `INV_NO_GC_LOSS`. | Add captured tokens, marker state and delayed request effects. |
| `GCommitEdges`, `GCommitCursor`, `GProbeUpward`, `GEndRound` | Historical sabotage/termination abstractions | SPLIT-FINER | Current code publishes one seal pointer, rejects divergent artifact bytes, and retains the durable lease after stopping. | Useful controls, but not current implementation steps. | Document historical scope; retain current conditional commit model. |

**CaGcDestructiveGateCore**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `ComputeGate` | `G:fold`: finalized local coverage/anomaly/hold tally | SOUND | Inputs belong to the captured fold; other actors cannot mutate that local tally. | Computation is atomic enough, but its formula differs from code. | Add token-proved empty catalog authority; production default is now authoritative. |
| `CondemnPhysicalCandidate` | `G:head_blob`, `M:scheduleCondemnMarkerWrite`, condemned run publication | UNSOUND — mapping | Suppressed code still records fresh condemnations. The model forbids them whenever its gate closes. | Its condemnation invariant is not a code theorem. | Distinguish fresh condemnation from gated graduation/deletion. |
| `PhysicallyDeleteCandidate` | `B:settleEntry` → queued HEAD/conditional DELETE → outcome | UNSOUND | Frozen booleans omit token, cut, live references and pending tasks; finding 1. | Current invariants cannot express dangling references. | Add request phases and no-loss invariant; fix reconciliation. |
| `EraseProvedRemoval` — proof | `C:deleteCompletedRemovingAtSnapshot`: parent evidence + exact row → catalog CAS | SOUND | Same-life terminal evidence persists; successor row/catalog mutation changes the CAS token. | A stale request cannot erase a replacement life. | Preserve exact row/token and resolution reads. |
| `EraseProvedRemoval` — authority/outcome | `G:refreshAuthority` → admission → catalog CAS → resolution GET | AMBIGUOUS-AS-DEFINITE | Lease takeover changes `gc/state`, not necessarily the catalog token; an already-sent erase may land after deposition or timeout. | Strict “current leader at landing” is unsupported; replacement-life safety survives. | Import pre-fold-drain request/outcome phases. Strict cross-key fencing requires shared transactional fencing, not another read. |

Across the models, protocol progress coupled to durable `journal/log`, upload readiness, `meta`, artifacts, adopted pointers and deletion outcomes omits unknown outcomes. Engine retries and SDK resends need separate sends/landings; conditional immutable writes are safe to repeat, but timeouts can leave durable effects after abort. Code wedges unresolved appends, carries failed deletes and resolves catalog erasures; model those behaviors.

**Top findings**

1. **Delayed writer `Clean` can reopen adoption of a condemned incarnation — INFERRED loss schedule, verified code steps.**  
   PUT lands; namespace removal removes the precommit and cancels the task; GC condemns/graduates that body.  
   The task resumes: `P:ensureBlobPresent`’s reconciliation retry clears the newer marker without another PUT. Another writer commits after the delete cut; exact deletion removes its body.  
   Minimal guard: never clear a newly observed `Condemned` marker on conflict without republishing afterward; propagate cancellation into task admission.

2. **Gate results certify different behavior — VERIFIED-IN-CODE.**  
   An anomaly closes the physical gate; a fresh zero transition nevertheless HEADs, queues its condemn marker and publishes its condemned row.  
   Code gates graduation/deletion, while the model gates condemnation too. Token-proved empty authority and the production default also differ.

3. **Abort and timeout erase consequential intermediate states — VERIFIED-IN-CODE.**  
   A pre-CAS delete or artifact write lands; adoption loses or gives up; another leader proceeds while a request remains outstanding.  
   Whole-round and batch actions suppress these states. Existing conditional guards protect replacements, but the models cannot establish that protection across the omitted boundaries.