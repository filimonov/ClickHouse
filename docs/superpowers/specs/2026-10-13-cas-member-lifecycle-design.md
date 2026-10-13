# CAS pool member lifecycle: decommission deletes data only {#cas-member-lifecycle}

Decommission keeps a member's control objects `gc/server-roots/<srid>/{owner,epoch,mount}` forever and deletes only its data. A new `cas-readd-member` operation returns a retired `srid` as an empty member, with the same or a new `server_uuid`, by one conditional write of the owner. The epoch counter is never deleted, so epochs never repeat across lives, and the decommission tail loses its race handling.

Closes: CAS-363, CAS-158, CAS-156, CAS-156.1, CAS-157, CAS-369. Partly: CAS-156.2, CAS-156.3, CAS-357, CAS-371, CAS-264, CAS-159. Acceptance: CAS-213. Residuals go to CAS-385.

Revision 3. r2 (`afa2e9e5d43`) reviews: `tmp/noclock/lifecycle/rereview-{opus,codex}.md`. Model: `docs/superpowers/models/CaEpochAllocCore_RESULTS.md` rounds 3–4 (`c4e0dd307f8`); round 5 checks the r3 changes (#model).

## Why {#why}

- **The owner object is already permanent.** Decommission tombstones it in place (`9707a61ba2c`, `docs/superpowers/specs/2026-07-18-t5-owner-tombstone-design.md`): its bytes are identical across lives, so a delete by token cannot be fenced.
- **Deleting `epoch` and `mount` causes the open races** (history: `tmp/noclock/srid-revival-history.md`; audit: `docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md`):
  - S6 (CAS-363): a restarting victim passes the owner check before the tombstone, finds no counter, and gets epoch 1 again; stale GC floors then delete its manifests (S7).
  - S11/S16 (CAS-158): the tombstone, written last, lands over a same-uuid successor.
  - R4: a claimer paused before its mint lands a lower epoch into the deleted slot.
  - S4, S8, S9: ETag ABA and key reuse, possible only because epochs repeat.
- **Keeping them closes these in the model** (round 4, `lc_*` rows; each row checks a subset of the faults, listed in RESULTS). Guards with a model sabotage are named in #decisions.
- **Cost:** three small objects per retired `srid` instead of one; they are reused on re-add.

## Decisions {#decisions}

| # | Decision | Evidence |
|---|---|---|
| D1 | Decommission never deletes `owner`, `epoch` or `mount`. | `sab_lc_delepoch` red |
| D2 | Every decommission run writes its own tombstone (fresh nonce) right after its claim, then confirms it still holds the slot; it un-tombstones only its own tombstone. | `sab_lc_tomblast`, `sab_lc_noconfirm` red; own-tombstone rule: round 5 |
| D3 | Every claim (start and fence recovery) re-reads the owner before installing the renewer; a retired owner makes the claimer fence its own body and fail. | `sab_lc_norecheck`, `sab_lc_refusedclean` red |
| D4 | Every owner write carries a fresh random `owner_nonce`. | `sab_lc_nononce` red |
| D5 | The admin runs its owner writes and drains on its fenced mount plane and never self-remounts; a fenced admin fails and the operator reruns. | `sab_lc_retryrefuses` red, `RetryCompletes` |
| D6 | Re-add is one `If-Match` owner rewrite. `claimMount` reclaims a clean farewell of any uuid, because the owner check before it already proved the owner is the claimer. | round 5 (two re-add uuids) |
| D7 | A new `srid` may not contain `/`. | review finding |
| D8 | S3 staging keys carry the writer epoch; aborted transactions delete their staging. | CAS-369 |
| D9 | No repair code for states left by 26.6.4; they are documented. The admin never creates an absent counter. | owner decision 2026-10-13 |

## Design {#design}

Paths: `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Line numbers at `2a034cdb374`.

### Owner nonce {#owner-nonce}

`OwnerObject` (`CA/Formats/CasServerRootFormats.cpp`) gains an optional plain field `owner_nonce`, 32 lowercase hex characters, drawn fresh for every owner write: first claim, tombstone, un-tombstone, re-add. Two writes never produce the same bytes, so a delayed `If-Match` against an old owner ETag cannot land after a later write. 26.6.4 `decodeOwner` reads with `Tolerant` + `skipUnknown`, so old binaries accept it. Same generator as `allocation_nonce` (CAS-362). Record the field in `Formats/README.md`.

### Claim recheck {#claim-recheck}

In `Pool::mountWritable` (`CA/Pool/CasPool.cpp`) and on every `Claimed` in the fence-recovery path (`Pool::tryRemountOnce`, `:1233`), before `installRenewer`: `GET owner`. If it is retired and this is not the decommission admin of that `srid`:

1. Read-modify-write the `mount` slot (`claimMount` returns no ETag on `Claimed`): if it still holds this claimer's `(uuid, writer_epoch)`, replace it with the same body marked `gc_fenced`. A clean farewell would let re-add proceed while a fenced admin still drains (`sab_lc_refusedclean`).
2. Throw the retired-owner refusal.

Cost: one GET per claim. Why it closes CAS-158 for new binaries: every reclaim path (farewell, `gc_fenced`, `proven_dead`, `cas_unsafe_remount_no_delay`) completes before the recheck, and the admin tombstones before it can farewell or be fenced into a reclaimable state that matters (D2's confirm covers the fence before the tombstone).

### Decommission {#decommission}

`CA/Tools/CasDecommission.cpp:decommissionPoolMember`:

1. **Admin claim**: `Pool::openForDecommission` (`CA/Pool/CasPool.cpp:904`). The existing `MountClaimPolicy::NoWait`, used only by decommission, also lets `claimOwnerOrThrow` (refusals at `CA/Pool/CasServerRoot.cpp:539`, `:589`) and the claim recheck accept a retired owner of the victim's uuid; no new mode reaches `Pool::open`. The admin pool starts without background self-remount (`background_watermark = false`); a fence fails the run (D5). The admin never creates an absent `epoch` counter: absent means a state left by 26.6.4; refuse (D9).
2. **Tombstone**: `GET owner`, then replace by `If-Match` with `{uuid, retired_at_ms, owner_nonce}`, also when it is already retired. A paused earlier run can then never remove this run's tombstone: its un-tombstone uses an older ETag.
3. **Confirm the claim**: `GET mount`. The body must carry the admin's `server_uuid` and `writer_epoch` and be neither `gc_fenced` nor clean (renewals change the ETag and `write_attempt_id`, so they are not compared). Otherwise un-tombstone by `If-Match` on step 2's ETag and fail (`sab_lc_noconfirm`).
4. **Drain data**: victim namespaces through `dropNamespace`, manifest debris, `staging/<srid>/`, `roots/<srid>/`, all on `admin->mountRequests()` (today `CasRequests(..., Fence::open())` and `gc_requests`). The lease-deadline fence stops a fenced admin locally; only requests already in flight can land late.
5. **Farewell**: if every drain request resolved, `admin.reset()` writes the clean terminal body (`MountLeaseRenewer::terminate`, `CA/Pool/CasServerRoot.cpp:1739`; it reports no outcome). If any drain request ended unresolved, the run replaces its body with `gc_fenced` instead and fails: re-add accepts only a clean farewell, and a rerun resolves the outcome.
6. **Verify**: `GET mount` (clean farewell of the admin's `writer_epoch`) and `GET owner` (retired, nonce of step 2). Report `member_retired = true` only if both hold and no catalog rows of the victim remain (warning at `CasDecommission.cpp:309-324`). Otherwise warn; the operator reruns.

Deleted from the tail (`:326-506`) and the helper `deleteSlotObject` (`:99-116`): the `claimed_epoch` and `farewell_mount` captures, `captures_match`, the two slot deletes, the "ACCEPTED RESIDUAL WINDOW", the liveness recheck, and the tombstone at the end. About 195 lines go; about 50 come back for steps 2, 3 and 6.

`EpochMintPolicy::DecommissionRecovery` (`CA/Pool/CasServerRoot.h:158`) and the owner-absent fallback in `openForDecommission` (`:938`) serve only 26.6.4 leftovers; with step 1's rule they become unreachable from decommission. Remove them in PR B if no other caller remains; otherwise keep unchanged.

### Re-add {#readd}

`clickhouse-disks cas-readd-member --srid <srid> [--server-uuid <uuid>]` and `SYSTEM CAS READD POOL MEMBER '<srid>' [UUID '<uuid>'] FROM DISK '<disk>'`, next to the drop verbs (`programs/disks/CommandCaDropMember.cpp`, `src/Interpreters/InterpreterSystemQuery.cpp:1114`).

1. `GET owner`: must be retired. Otherwise refuse.
2. `GET epoch`: must exist. `GET mount`: must be a clean farewell. A `gc_fenced` or live-looking body refuses: rerun the decommission. Missing objects (retired by 26.6.4): refuse; use a new `server_root_id` (D9).
3. `serverRootSubtreeEmpty` (`CA/Pool/CasServerRoot.cpp:500`; reads the catalog, `cas/manifests/<srid>/` and `roots/<srid>/`) must hold. The access-probe key under `roots/<srid>/` (CAS-371) is deleted first by `If-Match`; anything else refuses and lists the keys.
4. Replace `owner` by `If-Match` on step 1's ETag with `{target uuid, owner_nonce}`, no tombstone.

The operation is one conditional write after checks, so concurrent re-adds serialize on it; the loser fails and changes nothing. The `mount` body is not touched: the slot is never absent (a delayed mint of any earlier life fails its `If-None-Match`), and its `writer_epoch` stays, so `claimMount`'s `newer_epoch` refusal (`:822`) still stops a delayed lower-epoch claimer.

**`claimMount` change** (`:810-817`): a clean farewell of another `server_uuid` is reclaimable. The owner check that runs first (`claimOwnerOrThrow`) already proved the owner is the claimer, so a foreign clean farewell can only be the previous life of a re-added `srid`. Live, fenced or non-terminal bodies of another uuid stay `ForeignOwner`.

### New srids without `/` {#srid-slash}

`validateServerRootId` (`CA/Pool/CasServerRoot.h:86`) keeps accepting `/` for existing members. `claimOwnerOrThrow`, owner-absent branch (create at `:564`), refuses an id with `/`. Decommission and staging select by path prefix, so nested ids made `a`'s decommission remove `a/b` (CAS-157). Not covered: existing nested pools, and a new `a` claimed over an existing but empty `a/b` (`serverRootSubtreeEmpty` sees no data); both documented. The fsck part of CAS-357 stays open.

### Staging {#staging}

- Key `staging/<srid>/<writer_epoch>/<random32>.tmp` (`ContentAddressedTransaction.cpp:901`). The epoch and the admission generation used at `:916` and in `finalize` come from one capture of the pool incarnation, so a self-remount between them cannot stage new-generation bytes under an old epoch.
- The startup sweep (`ContentAddressedMetadataStorage.cpp:892`, `sweepOwnMountStaging`) fixes its cutoff to the live epoch at the start and deletes keys below it. Keys without an epoch segment count as epoch 0.
- Abort deletes staging, best-effort, like commit (`cleanupPendingTempFiles`, `:167`). The comment calling an aborted transaction's object a "re-readable publication source" is false and goes.
- The decommission staging drain stays on logical keys: `ObjectStorageBackend::listUnder` applies the local prefix itself, unlike the direct `IObjectStorage` calls of the writer and sweeper. A test pins that the drain removes the objects the writer created.

### Operator surface {#operator}

- `system.cas_mounts`: rows in state `terminated` or `fenced` read the owner and show `retired` when it is tombstoned (one GET per such row per query).
- `SYSTEM CAS DROP POOL MEMBER` result column `slot_removed` becomes `member_retired`.
- Refusal texts (CAS-156.2, CAS-156.3): retired owner → `cas-readd-member`; foreign owner → decommission, then `cas-readd-member --server-uuid`; `NoWait` on a hard-killed member → wait for `state = 'fenced'`.

## Residuals {#residuals}

Tracked in CAS-385 unless marked.

| Residual | Effect | Handling |
|---|---|---|
| A drain request already sent lands after a re-add, even in one uncontended run whose delete lost its response (`res_r5_inflight_norerun`) | deletes a new member's object | fencing and process exit do not bound when the store applies a sent request. Narrowed in code: the drain deletes by exact incarnation (`deleteListedPrefix`, `If-Match`), and the farewell is clean only if every drain request resolved; otherwise the run writes `gc_fenced`, re-add refuses, and a rerun resolves the outcome. What remains is a late delete of a key the new life rewrote with identical bytes and S3 ETag: the next row |
| A late conditional DELETE under `roots/<srid>/` meets a new life's object with identical bytes and S3 ETag (codex r2 finding 3) | deletes the new member's mountpoint object or probe | same class as CAS-371; no epoch in those keys |
| The tool dies between the tombstone and its un-tombstone (`res_lc_tombover`) | a live successor keeps running under a tombstone and refuses its next start | availability only; rerun decommission or re-add |
| States left by 26.6.4 (no counter, no tombstone, or a deleted `mount`) | decommission and re-add refuse | documented (D9) |
| Old binaries during a rolling upgrade | keep their old races (S6, S11, recursive staging sweep, old decommission tool) | release note |
| Retired `srid`s accumulate | +1 GET each per GC round | no purge verb: a purge would reopen epoch reuse |

## Costs {#costs}

| Where | Change |
|---|---|
| Every claim | +1 GET `owner` |
| Every owner write | 32-byte nonce |
| Decommission | −2 DELETE, −4 GET; +4 GET (tombstone read, confirm, verify ×2); the tombstone PUT moves |
| Re-add | 3 GET, the emptiness check (1 catalog read, 2 LIST), ≤1 DELETE, 1 PUT |
| GC heartbeat floor | +1 GET per retired `srid` per round (`computeHeartbeatFloor`, `CA/Pool/CasServerRoot.cpp:1037`); LIST returns 3 keys per retired `srid` |
| `system.cas_mounts` | +1 GET per terminated or fenced row per query |
| Storage | 3 objects per retired `srid`, under 1 KiB |

## Compatibility {#compat}

| Case | Behaviour | Worse than either version? |
|---|---|---|
| Upgrade: `srid`s retired by 26.6.4 | not repaired; re-add refuses, use a new id | no |
| Upgrade: a 26.6.4 decommission stopped midway | the new tool refuses an absent counter; finish nothing, use a new id | no |
| Upgrade: flat 26.6.4 staging keys | swept as epoch 0 | no |
| Upgrade: existing ids with `/` | keep working; only new ids are refused | no |
| Downgrade: a retired `srid` kept by the new tool | 26.6.4 refuses to start it (`throwIfOwnerRetired`); its GC treats the farewell as terminal; its decommission refuses the tombstone | no |
| Downgrade: a re-added `srid` with a new uuid | after its first start on the new version the slot holds the new uuid and 26.6.4 starts it normally; before that, 26.6.4 `claimMount` returns `ForeignOwner` on the old life's farewell | no: 26.6.4 cannot re-add at all; release note says start a re-added member once before downgrading |
| Downgrade: a re-added `srid` with the same uuid | 26.6.4 starts it and continues the counter | no |
| Downgrade: `owner_nonce`, epoch staging keys | skipped; swept recursively | no |
| Mixed: 26.6.4 decommission tool | old behaviour and old races | same as 26.6.4 |
| Mixed: 26.6.4 victim restarts during a new decommission | refused if its owner check is after the tombstone; otherwise no recheck (CAS-158 stays for it) | same as 26.6.4 |
| Mixed: paused 26.6.4 process sweeps staging | deletes a new member's staging (CAS-369 for old nodes) | same as 26.6.4 |

## Delivery {#delivery}

Independent PRs into `antalya-26.6`; each commit reviewable alone (model, code with its tests, docs).

| PR | Content | Depends on |
|---|---|---|
| A | D7; abort deletes staging; drain-prefix test; refusal texts | — |
| B | D1–D5, D8 epoch keys, D9 admin rule, `member_retired`, release note | CAS-362 merged |
| C | D6 re-add (CLI and SQL) and the `claimMount` change, `retired` in `system.cas_mounts` | PR B merged |

The `fix/antalya-26.6/cas-369-staging-sweep-snapshot` draft is superseded; its abort-delete commit moves to PR A.

## Tests {#tests}

Failing first on `2a034cdb374` unless marked guard.

**PR A**
- `CASServerRootId.NewIdWithSlashIsRefused`; guard `CASServerRootId.ExistingIdWithSlashStillMounts`.
- `ContentAddressedTransaction.AbortDeletesS3Staging`.
- Guard `CASDecommission.StagingDrainRemovesWhatTheWriterStaged` (local and native prefixes).

**PR B**
- `CASDecommission.KeepsEpochAndFarewellMount`.
- `CASDecommission.VictimClaimingAfterTheTombstoneFencesItselfAndFails`: the victim waits on the admin's lease, the admin farewells, the victim's recheck sees the tombstone, writes `gc_fenced`, fails with the retired-owner error.
- `CASDecommission.FenceBeforeTombstoneUntombstonesAndFails`; `CASDecommission.ConfirmAcceptsARenewalBetweenClaimAndConfirm`.
- `CASDecommission.PausedRunCannotUntombstoneARerunsTombstone` (two admins, rewrite with a fresh nonce).
- `CASDecommission.ResumesOverItsOwnTombstone`: death after each step, GC fences the dead admin, rerun completes.
- `CASDecommission.FencedAdminFailsInsteadOfRemounting`, `CASDecommission.DrainStopsOnTheAdminLeaseFence`.
- `CASDecommission.UnconfirmedFarewellIsNotRetired`, `CASDecommission.AbsentCounterIsRefused`.
- `CASOwner.DelayedTombstoneAfterReaddFailsOnNonce` on a backend with content-derived ETags for `owner` keys (like `ContentEtagBackend` for markers); in-memory backends mint increasing ETags and would pass vacuously.
- `CASMount.NormalMountRefusesARetiredOwnerAfterClaim`, `CASMount.FenceRecoveryReclaimRechecksTheOwner`; guard `CASMount.PoolOpenNeverAcceptsARetiredOwner`.
- `CASStagingSweeper.OldEpochSweepKeepsSuccessorStaging`, `ContentAddressedTransaction.StagingEpochMatchesAdmissionGeneration`; guard `CASStagingSweeper.FlatLegacyKeysAreSwept`.
- Rewritten: `gtest_cas_decommission.cpp` tests asserting slot deletion or a refused rerun (`RemovesMutableSlotAndRefusesTombstonedRerun`, `SuccessfulDecommissionLeavesTombstonedOwnerAnchor`, `FencedSlotRetirementTailRetiresUncontendedSlot`, `RunsOnAnOpenFence`, `FailedDrainKeepsSlotThenResumes`, `ManifestDebrisFailureKeepsSlotThenResumes`, `MidRetirementCrashResumesViaMountLeaseFallback`, `ErasesAllVictimNamespaces`, `RetainsDebrisWhoseEpochSealIsUnconsumed`, `DuplicateLifeIdRefusesBeforeAnyNamespaceOrSlotMutation`, `CatalogCutIsValidatedBeforeImpersonationAndReusedForSelection`, `DrainClockUnifiesWithTheFarewellBootClockRegardlessOfHostUptime`, `OpeningRetriesPaceOnTheSameFakeClockAndSleepAsTheDrain`) and the `slot_removed` assertions in `gtest_cas_decommission_catalog_duties.cpp`. `OwnerTombstoneAmbiguousSuccessResolvesToCommitted` moves to step 2. Deleted: `SuccessorReclaimFencesSlotRetirementTail`, `SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor`, `SuccessorOwnerRewriteWinsBeforeTombstone`.
- Integration `test_cas_drop_pool_member`: case (8) scoped to one disk view expects a `terminated` row; case (10) rerun succeeds.

**PR C**
- `CASReadd.SameUuidContinuesTheCounter`, `CASReadd.NewUuidReclaimsTheOldLifesFarewell`, `CASReadd.RefusesFencedMount`, `CASReadd.RefusesNonEmptySubtree`, `CASReadd.RefusesNotRetired`, `CASReadd.RefusesSlotRetiredBy2664`, `CASReadd.ConcurrentReaddsWithDifferentUuidsOneWins`, `CASReadd.DelayedLowerEpochClaimAfterReaddIsRefused`.
- `CASMount.ForeignCleanFarewellIsReclaimable`; guard `CASMount.ForeignLiveOrFencedBodyStaysForeignOwner`.
- Integration: drop, re-add with a new uuid, the member starts on an empty disk; `system.cas_mounts` shows `retired` before the re-add (case (8) updated).
- Soak (CAS-213): decommission under load through the SQL verb, kill the tool at each step and rerun, then re-add and restart.

## Model {#model}

Rounds 3–4 (`3b7b6606ec4`, `647c0ffac72`, `c4e0dd307f8`) prove r2. Round 5 must cover the r3 changes before code:
- every run rewrites its tombstone; un-tombstone only of its own; invariant "a finished decommission leaves the owner retired";
- `ZombieRun` combined with `GcFences`;
- re-add as one owner write; `claimMount` reclaims a foreign clean farewell; two revival uuids;
- the admin drains on its fenced plane (drain steps gated by the admin's fence; in-flight requests modelled as late);
- the admin refuses an absent counter.
The model has no self-remount; D5 for remount is argued, not modelled.

Round 5 result (`5b3224c52a0`, `03d506c91aa`): all 17 rows met expectations. The own-tombstone rewrite closes the paused-run un-tombstone (`sab_r5_keeptomb` red); fenced drains close the paused-run drain (`sab_r5_drainopen` red); the foreign clean reclaim is needed for a new-uuid re-add (`sab_r5_noforeign` wedges). Two guards are not shown load-bearing, because the model has no renewals, local lease deadline or self-remount: re-add's clean-farewell precondition (`sab_r5_skipclean`) and `ForeignOwner` for a foreign `gc_fenced` body (`sab_r5_foreignfenced`). Both stay as cheap checks, argued in #readd and #claim-recheck. The in-flight drain residual is in #residuals; the unresolved-request farewell rule above is not modelled.

## Implementation checks {#checks}

- A self-written `gc_fenced` body must behave as a GC fence everywhere: `isCreatorFenceTerminal`, `computeHeartbeatFloor`, `claimMount` reclaim, `listMounts`.
- `claimOwnerOrThrow` returns no owner ETag; step 2 reads the owner itself.
- `background_watermark = false` for the admin pool must not disable anything the drain needs (farewell, `sweepNamespace` on the mount plane).
- `DecommissionRecovery` callers after step 1's rule.

## Rejected {#rejected}

- **Floor in the owner tombstone, counter deleted** (round 3): green only without zombies; a delayed genesis mint still regresses the slot (`res_ownerfloor_zmint`).
- **Floor in `_pool_meta`** (V2): same residual, a pool-wide RMW, and 26.6.4 `admitOrValidate` drops the field.
- **Document only**: leaves S6 and CAS-158 open and CAS-156 unsupported.
- **Re-add deletes `mount`** (r1): a delayed mint lands below the old epoch (`keep_main`).
- **Re-add replaces `mount`** (r2): concurrent re-adds with different uuids split owner and mount (codex and opus r2 finding 1/2).
- **Confirm by the claim ETag** (r2): renewals change it.
- **`physicalKey` in the decommission staging drain** (r2): the backend applies the prefix; adding it doubles it.
- **Staging LIST before claim** (CAS-369 draft): works, but needs a prefix-resolution refactor that epoch keys make unnecessary.
- **Recheck nested ids on every start**: +1 LIST per start and refusals of existing pools at upgrade.
