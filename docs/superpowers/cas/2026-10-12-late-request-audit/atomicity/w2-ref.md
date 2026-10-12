Atomic owner transitions and token-bound catalog erases are sound. The main mismatches are runtime publication after retirement, build checks separated from runtime capture, and requests that can land after an unresolved return.

Audited against `d76b59f9295`; lease-related comparisons also use `3b4037ae265`, identified below. Read all 54 configurations and both available results files. No files changed; no TLC runs.

Paths below are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Abbreviations: **L** = `Pool/CasRefLedger.cpp`, **P** = `Pool/CasPartWriteTxn.cpp`, **C** = `Pool/CasRefCatalog.cpp`, **G** = `Gc/CasGc.cpp`. Scoped wrappers inherit their underlying action’s verdict. Model-local bookkeeping and deliberate sabotage transitions are omitted.

**`CaRefLaneCore`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `StartWrite` | L:`commitRefChunk`: prepare candidate → lock/base check → arm attempt | SOUND | One queue leader; `state_mutex` binds arming to the candidate base. No request precedes arming. | Exact attempt retained. | None. |
| `WriteLands`, `WriteUnresolved` | L:`commitRefChunk`; `Backend/CasRequests.cpp:writeLoop`: conditional PUT, engine/SDK resends, response | UNSOUND | `WriteLands` requires `Writing`. An original request can land after `WriteUnresolved` has entered `Wedged`, while successor recovery competes for its slot. | Late effects and seal races excluded. | Add pending requests, late landing in `Wedged`, and per-key occupants/frontiers. Code already retains the wedge and uses conditional creates. |
| `ObserveDurable`, `ObserveUnknown`, `ObserveOccupied` | L:`resolveWedgeOnce`: conditional retry → settling GET → byte classification | SOUND | Conditional create arbitrates the exact slot; adoption requires identical bytes. Unobserved occupants retain uncertainty. | Read/retry fusion preserves occupant identity, subject to the missing pending-request state above. | Preserve conditional slot and exact-byte checks. |
| `InstallCommitted`, `ApplyResolution` | L:`commitRefChunk`, `resolveWedgeOnce`: checkpoint frontier publication → admission/base/attempt checks → cache swap | SOUND | Local mutations serialize; post-I/O checks validate the captured runtime and exact attempt under `state_mutex`. | No installation into a successor runtime. | For composition, expose separate log and checkpoint durability; existing code orders them correctly. |
| `Recover` | L:`runRecoveryWalkOnce`, `ensureRefTableRecovered`: checkpoint GET → snapshot/log GETs → seal PUTs/frontier CAS → final checkpoint GET → installation | SOUND | Changed checkpoint tokens restart recovery; uncertain seals prevent installation; runtime invalidation is checked again under the installation lock. | Successful recovery can be abstracted as one installation. | Retain restart, token, and admission checks. |
| `FreshCatalogLookup` | L:`acquireReadableRefTableRuntime`: two catalog GETs → `acquireRefTableRuntime` insertion | UNSOUND | After the second GET, removal/drain can invalidate and erase the old runtime. Delayed insertion then sees an empty slot and republishes the retired life. | Stale runtime becomes resident; later namespace-file writes can target dead-life storage. | Split observation/publication; add retirement revision and `NoRetiredRuntimePublication`. Code: revision-check insertion or revalidate after publication; reject terminal lives in `namespaceLife`. |
| `ApplyResolution` with `Occupied` | L:`resolveWedgeOnce`: set `Faulted` under lock → unlock → latch mount fault | UNSOUND | At `3b4037ae265`, lines 2530 and 2625 remain separate. Another table can certify/admit between them. | `FaultHasExit` fails for the concrete intermediate state. | Split fault publication/latching; move the nonthrowing latch before publishing `Faulted`. |
| `WriteConflict`, `RecoveryFindsOccupied` | L:`commitRefChunk`, `runRecoveryWalkOnce`: classify foreign bytes → latch → terminal handling | SOUND at `3b4037ae265` | These paths latch before subsequent terminal handling. At `d76b59f9295`, successor seals instead produce `Closed`, and foreign faults lack this immediate latch ordering. | Current results do not establish correspondence to the older base. | Version the model claims explicitly. |
| `FenceMove`, `AcceptRearm` | `Pool/CasPool.cpp:tryRemountOnce`: lease requests → recovery cancellation → runtime detachment → generation change → gate opening | SOUND | Accepted generation and immutable captured identities prevent old handles from becoming successor handles. | Safe identity abstraction; intermediate unavailable states omitted. | None for this projection. |
| `RemoveCatalogLife`, `RebirthCatalogLife` | C:`deleteCompletedRemovingAtSnapshot`, `createNamespace`: catalog CAS; `Creating` CAS → checkpoint publication → `Live` CAS | SOUND | Exact catalog token/row checks protect deletion; fresh physical IDs isolate rebirth. | Partial creation cannot redirect old-life cleanup. | None. |
| `Certify`, `ConfirmMissingName`, `LateExactInvalidation` | L:`confirmExactRef`, `invalidateRemovedCatalogLife`: slot/state/queue checks; flag invalidation → exact-pointer erase | SOUND | Certification holds both lane locks. Delayed erasure compares the captured pointer and cannot erase its successor. | Local snapshot and successor preservation are sound. | Does not repair the pre-publication retirement gap above. |

**`CaRefFoldClampRecoveryCore`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `DiscoverNeutralNomination` | `Gc/CasOrphanManifestSweep.cpp:planManifestCursorPage`: exact GET → decode → candidate construction | SOUND | Decoding consumes captured immutable bytes; candidate construction writes no durable state. Eligibility defects from wave 1 excluded here. | Neutral accounting remains local until adoption. | None for this boundary. |
| `FoldEdge1` | G:`foldManifestEdges`: pool-scheduled GET → consume result → stage deltas/token | SOUND | Manifest bytes are immutable; a competing GC advancement invalidates the final `gc/state` CAS. | Prefetch timing cannot authorize an otherwise losing round. | Optionally expose fetched observations for timing coverage. |
| `FoldEdge2` | G:fold intake, lines 2903–2918: merge buffers → advance local cursor | UNSOUND | `cursorPastL` is described as durable but changes before seal publication and round CAS. Deposition/failed CAS leaves that cursor undurable in code. | Durable-adoption claims are premature. | Separate staged and durable cursors; check `DurableCursorRequiresAdoption`. Code already stages correctly. |
| `CommitRound` | G:fold seal PUT → `gc/state` conditional replacement, lines 3716, 1164 | AMBIGUOUS-AS-DEFINITE | Deposition changes the CAS token, safely rejecting adoption. A timeout can nevertheless leave adoption durable without a successful local return. | Unacknowledged adoption and skipped cleanup absent. | Separate CAS landing, response, and abandonment; preserve the existing token guard. |
| `PostCasDelete`, `DeleteNominatedManifest` | G:`runRegularRound`: batch deletes/resends; nomination HEAD → token comparison → conditional DELETE | AMBIGUOUS-AS-DEFINITE | Immutable keys/exact tokens protect replacements, but deletes may land before an exception or after timeout. | Physical deletion and client completion are conflated. | Add per-request landing/unknown outcomes. Existing immutable-key/token guards are sound. |
| `HealB` | P:`stageManifest`, or cessation of a transient 404 | SOUND | Environmental restoration is separate from folding. | `bPresent` mixes physical presence with successful observability. | Separate store presence from GET observations if testing read-ahead timing. |

**`CaRefWriterCleanupCore`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `StartBuild` | P:`precommitAdd`: `requireAlive` → record intent → generic append captures runtime → owner grant | UNSOUND | Removal/cancellation and rebirth can complete before runtime capture; the closure retains staged-ID permission without rechecking cancellation. | Cancelled build can grant ownership in a successor life. | Add checked/pending grant plus captured life/generation; assert `NoStaleBuildPublish`. Bind code append to those identities and recheck cancellation. |
| `PromoteBuild` | P:`promote`: epoch check → manifest GET → queued owner check → append | UNSOUND | Self-remount can occur before runtime capture. A concurrent stale sweep clears its scheduling flag before finishing; another caller can bypass it while the old precommit remains. | Old build can promote through the new epoch, contrary to the atomic epoch guard. | Split admission/GET/queued validation/landing; bind append to original generation. |
| `PromoteBuild` owner move | P:`promote`; L:`commitRefChunk`: exact precommit removal and committed addition in one log envelope | SOUND | Queue serialization and conditional log-slot occupation protect the complete transaction. | No ownerless interval within the move itself. | Preserve single-envelope transition. |
| `ResolveGrantAdopted`, `ResolveGrantRejected`, `RemoveFailedPrecommit` | `Pool/CasPool.cpp:drainWriterCleanupDuties`: resolve lane → inspect exact binding → append removal → retire | SOUND | Unresolved lanes block progress; exact bindings are rebuilt against serialized current state. | Duty retirement follows settlement/removal. | Add late-request reality transitions inherited from the lane model. |
| `SuccessorCleanupStep` | L:`sweepStalePrecommitsNow`: capture old bindings → queued exact-binding recheck → append | SOUND | Epochs increase; same-runtime mutations serialize. Successor sealing arbitrates outstanding predecessor slots. | Cleanup cannot remove a replacement binding. | Keep epoch and exact-binding checks. |
| `RemoveNamespace` | L:`dropNamespaceImpl`: close positive admission → await queue → catalog transition → enumerate owners → terminal log/frontier publication | SOUND | Closed admission and the single leader protect enumeration; all removals share one transaction. Cancellation occurs afterwards. | Complete successful logical teardown. | Expose catalog/log/frontier phases for ambiguity coverage. |
| `PromoteBuild`, `RemoveFailedPrecommit`, `RemoveNamespace` durability | L:`commitRefChunk`: log PUT → checkpoint CAS → installation/return | AMBIGUOUS-AS-DEFINITE | Ownership may become durable although the caller receives failure. Only initial grants have explicit uncertainty here. | Later unresolved mutations are omitted. | Add pending transaction/outcome states; code already uses `Wedged`/`NeedsRecovery`. |

**`CaRefNsCleanupStaleLeaderCore`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `PreFoldDelete` | C:`deleteCompletedRemovingAtSnapshot`: proof checks → exact catalog CAS | SOUND | Whole-object token and exact row protect concurrent changes; proof protocol belongs to sibling models. | Only the proved old life disappears. | None. |
| `ListCandidate`, `PostPageCutAndNominate` | `Gc/CasNamespaceJanitor.cpp:runOnePage`: LIST → later catalog GET → classify captured ID | SOUND | Catalog cut is one object; fresh IDs cannot later become the captured dead ID. | Delayed nomination remains identity-safe. | None. |
| `Recreate`, `WriteObject` | C:`createNamespace`, `completeCreation`; `Pool/CasPlainObjects.cpp:putNamespaceFile` | SOUND | Intermediate `Creating` already names the fresh ID and protects it from nomination. | Creation fusion preserves this janitor property. | Add stale-runtime writes to cover the lane publication defect. |
| `StaleJanitorDelete` | `runOnePage`: optional HEAD → admission check → conditional DELETE/retries | AMBIGUOUS-AS-DEFINITE | Captured physical ID and exact token remain safe even for a deposed leader; timeout does not determine completion. | New-life safety holds; deletion/completion uncertainty omitted. | Separate landing/result; retain captured key/token. |

**`CaRefPreFoldDrainAllRowsCore`**

| action | code steps fused (file:function) | verdict SOUND/UNSOUND/SPLIT-FINER/AMBIGUOUS-AS-DEFINITE | interleaving or argument | consequence | minimal fix (model + code) |
|---|---|---|---|---|---|
| `Select` | `Gc/CatalogLifecycleReconciler.cpp:reconcile`, `selectEligible`: complete GET → selection | SOUND | Captured catalog token binds later mutation; adopted-parent evidence is immutable. | Concurrent changes cause CAS rejection. | None. |
| `DeleteSelected`, external deletes | C:`deleteCompletedRemovingAtSnapshot`: exact-row/token replacement | SOUND | Every competing catalog mutation changes the whole-object token. | No stale snapshot deletion. | None. |
| `ResolveExternalOutcome`, `RejectSelected`, `UnknownNotLanded` | Conditional-write result → admission check → mandatory resolution GET | AMBIGUOUS-AS-DEFINITE | Unknown can land after the resolution read; code throws if the selected row remains. Model resumes scanning without retaining that request. | Abort/late-land paths omitted, although token binding protects other rows. | Add pending CAS and invocation-abort states; preserve mandatory resolution reads. |
| `Decide` | `reconcile`: no eligible row in captured cut → refresh authority → return | SOUND | Eligibility uses the fixed adopted parent; retired exact lives cannot reappear. Authority is refreshed before completion. | Drain verdict is scoped to that parent/cut. | Preserve that scope. |

The namespace cleanup configuration `_sab_promoteunderhold.cfg` is obsolete: it references absent `SabotagePromoteUnderHold` and `NoHeldRowRetired`.

**Top findings**

1. **VERIFIED-IN-CODE — acknowledged namespace-file loss path.**  
   Cold reader observes `Live`; removal/drain retires its life before runtime insertion. Delayed insertion republishes it.  
   Warm `namespaceLife` returns that retired life; `putNamespaceFile` can acknowledge bytes which the dead-life janitor then deletes.

2. **VERIFIED-IN-CODE — stale build admission.**  
   `precommitAdd` passes `requireAlive`; cancellation/rebirth occurs before generic append captures its runtime.  
   The old closure can grant ownership in the successor life. Promotion has a corresponding epoch-check gap; committed-data loss from it remains **INFERRED**.

3. **VERIFIED-IN-CODE — fault publication precedes fencing.**  
   Lease-series wedge resolution publishes `Faulted`, releases `state_mutex`, then latches the mount fault.  
   Another table can certify/admit in that interval. The model’s atomic `FaultHasExit` masks it.

4. **INFERRED — missing late-request composition coverage.**  
   PUT/DELETE times out; successor or GC progresses; the original request later lands.  
   Current models suppress several such states. Existing slot/catalog tokens provide substantial protection, but green runs do not test the complete interleaving.