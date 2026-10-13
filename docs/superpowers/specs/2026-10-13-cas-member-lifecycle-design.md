# CAS pool member lifecycle: decommission deletes data only {#cas-member-lifecycle}

Decommission keeps a member's control objects `gc/server-roots/<srid>/{owner,epoch,mount}` forever and deletes only its data. A new `cas-readd-member` operation returns a retired `srid` as an empty member, with the same or a new `server_uuid`. The epoch counter is never deleted, so epochs never repeat across lives, and the decommission tail loses its race handling.

Closes: CAS-363, CAS-158, CAS-156, CAS-156.1, CAS-157, CAS-369. Partly: CAS-156.2, CAS-156.3, CAS-357, CAS-371, CAS-264, CAS-159. Acceptance: CAS-213. Residuals go to CAS-385.

Revision 2. Changes from r1 (`5e4b5f24639`) follow the reviews in `tmp/noclock/lifecycle/review-{opus,codex}.md` and the model rounds 3–4 (`docs/superpowers/models/CaEpochAllocCore_RESULTS.md`, `c4e0dd307f8`).

## Why {#why}

- **The owner object is already permanent.** Decommission tombstones it in place (`9707a61ba2c`, `docs/superpowers/specs/2026-07-18-t5-owner-tombstone-design.md`): its bytes are identical across lives, so a delete by token cannot be fenced.
- **Deleting `epoch` and `mount` causes the open races** (history: `tmp/noclock/srid-revival-history.md`; audit: `docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md`):
  - S6 (CAS-363): a restarting victim passes the owner check before the tombstone, finds no counter, and gets epoch 1 again; stale GC floors then delete its manifests (S7).
  - S11/S16 (CAS-158): the tombstone, written last, lands over a same-uuid successor.
  - R4: a claimer paused before its mint lands a lower epoch into the deleted slot.
  - S4, S8, S9: ETag ABA and key reuse, possible only because epochs repeat.
- **Keeping them closes these in the model.** Round 4 (`lc_*`) is green with zombie creates and mints, victim restarts, a GC fence of the admin, decommission reruns after a death at any step, and re-adds with the same or a new uuid. Each guard below has a red sabotage.
- **Cost:** three small objects per retired `srid` instead of one; they are reused on re-add.

## Decisions {#decisions}

| # | Decision | Guard (sabotage that goes red without it) |
|---|---|---|
| D1 | Decommission never deletes `owner`, `epoch` or `mount`. | `sab_lc_delepoch`, `keep_main` |
| D2 | The owner tombstone is written right after the admin claim, then the admin confirms it still holds the slot. | `sab_lc_tomblast`, `sab_lc_noconfirm` |
| D3 | Every claim (start and self-remount) re-reads the owner after it succeeds; a retired owner makes the claimer fence its own body and fail. | `sab_lc_norecheck`, `sab_lc_refusedclean` |
| D4 | Every owner write carries a fresh random `owner_nonce`. | `sab_lc_nononce` |
| D5 | Decommission is resumable over its own tombstone; a fenced admin stops, it does not remount. | `sab_lc_retryrefuses`, `RetryCompletes` |
| D6 | Re-add accepts only a clean farewell and replaces the `mount` body; it never deletes it. | `sab_lc_fenced`, `sab_lc_deletemount` |
| D7 | A new `srid` may not contain `/`. | review finding (no model) |
| D8 | S3 staging keys carry the writer epoch; aborted transactions delete their staging. | CAS-369 |
| D9 | No repair code for states left by 26.6.4; they are documented. | owner decision 2026-10-13 |

## Design {#design}

Paths: `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Line numbers at `2a034cdb374`.

### Owner nonce {#owner-nonce}

`OwnerObject` (`CA/Formats/CasServerRootFormats.cpp`) gains an optional plain field `owner_nonce`, 32 lowercase hex characters, drawn fresh for every owner write: first claim, tombstone, un-tombstone, re-add. Two writes never produce the same bytes, so a delayed `If-Match` against an old owner ETag cannot land after a re-add restored the same uuid. 26.6.4 decoders skip unknown plain keys (verify against the tag in the PR). Record the field in `Formats/README.md`.

### Claim recheck {#claim-recheck}

`Pool::mountWritable` (`CA/Pool/CasPool.cpp`) and `Pool::tryRemountOnce` (`:1436`): after `claimMount` returns `Claimed`, `GET owner`. If it is retired (and this is not the decommission admin of that `srid`):

1. Replace the just-claimed `mount` body by `If-Match` with the same body marked `gc_fenced`. A clean farewell would let re-add proceed while a fenced admin still drains (`sab_lc_refusedclean`).
2. Throw the retired-owner refusal.

Cost: one GET per claim. Why it closes CAS-158: the admin tombstones before it can farewell, and a claimer can only win the slot after a farewell or a fence, so its re-read sees the tombstone.

### Decommission {#decommission}

`CA/Tools/CasDecommission.cpp:decommissionPoolMember`:

1. **Admin claim**: `Pool::openForDecommission` (`CA/Pool/CasPool.cpp:904`), `NoWait`, with a new argument `OwnerClaim::DecommissionAdmin` that lets `claimOwnerOrThrow` (refusals at `CA/Pool/CasServerRoot.cpp:539`, `:589`) and the claim recheck accept a retired owner of the victim's uuid. It is an argument of this call, not a `PoolConfig` field, so it cannot reach a normal mount. The admin pool does not self-remount: a fence makes the run fail and the operator reruns (D5).
2. **Tombstone**: `GET owner`; if not retired, replace by `If-Match` with `{uuid, retired_at_ms, owner_nonce}`. Already retired: keep it.
3. **Confirm the claim**: `GET mount`. If the body is not the admin's claim (GC fenced the admin and the victim reclaimed between steps 1 and 2), un-tombstone by `If-Match` on the tombstone's ETag and stop with an error (`sab_lc_noconfirm`).
4. **Drain data**, unchanged: victim namespaces through `dropNamespace`, manifest debris, `staging/<srid>/`, `roots/<srid>/`.
5. **Farewell**: `admin.reset()` writes the clean terminal body (`MountLeaseRenewer::terminate`, `CA/Pool/CasServerRoot.cpp:1739`).
6. **Report** `member_retired = true` only if the farewell landed and the catalog holds no rows the victim owns (the existing warning at `:332-340` stays). Otherwise warn; the operator reruns.

Deleted from the tail (`:326-506`): `deleteSlotObject`, the `claimed_epoch` and `farewell_mount` captures, `captures_match`, the two slot deletes, the "ACCEPTED RESIDUAL WINDOW", the liveness recheck, and the tombstone at the end. About 175 of 535 lines go; about 40 come back for steps 2–3.

`EpochMintPolicy::DecommissionRecovery` and the owner-absent fallback in `openForDecommission` (`:938`) only serve states left by 26.6.4 decommissions. Kept unchanged (D9); no new legacy paths.

### Re-add {#readd}

`clickhouse-disks cas-readd-member --srid <srid> [--server-uuid <uuid>]` and `SYSTEM CAS READD POOL MEMBER '<srid>' [UUID '<uuid>'] FROM DISK '<disk>'`, next to the drop verbs (`programs/disks/CommandCaDropMember.cpp`, `src/Interpreters/InterpreterSystemQuery.cpp:1114`).

1. `GET owner`: must be retired. Otherwise refuse.
2. `serverRootSubtreeEmpty` (`CA/Pool/CasServerRoot.cpp:500`) must hold. The access-probe key under `roots/<srid>/` (CAS-371) is deleted first by `If-Match`; anything else refuses and lists the keys.
3. `GET mount`: must be a clean farewell. A `gc_fenced` or live-looking body refuses: rerun the decommission. No `mount` or no `epoch` (retired by 26.6.4): refuse; use a new `server_root_id` (D9).
4. Replace `mount` by `If-Match` with a clean farewell body of the target uuid and a fresh `write_attempt_id`. The slot is never absent, so a delayed mint of any earlier life fails its `If-None-Match` (`sab_lc_deletemount`).
5. Replace `owner` by `If-Match` with `{target uuid, owner_nonce}`, no tombstone.

The next start continues the kept counter and reclaims the clean farewell. Two re-adds serialize on the `If-Match` writes; the loser stops (`lc_race`). A crash between steps 4 and 5 leaves a retired owner over a clean farewell: rerun.

### New srids without `/` {#srid-slash}

`validateServerRootId` (`CA/Pool/CasServerRoot.h:86`) keeps accepting `/` for existing members. `claimOwnerOrThrow`, owner-absent branch (create at `:564`), refuses an id with `/`. Decommission and staging select by path prefix, so nested ids made `a`'s decommission remove `a/b` (CAS-157); existing nested pools keep that hazard (documented; fsck part of CAS-357 stays open).

### Staging {#staging}

- Key `staging/<srid>/<writer_epoch>/<random32>.tmp` (`ContentAddressedTransaction.cpp:901`). The epoch and the admission generation used at `:916` and in `finalize` come from one capture of the pool incarnation, so a self-remount between them cannot stage new-generation bytes under an old epoch.
- The startup sweep (`ContentAddressedMetadataStorage.cpp:892`, `sweepOwnMountStaging`) fixes its cutoff to the live epoch at the start and deletes keys below it. Keys without an epoch segment count as epoch 0.
- Abort deletes staging, best-effort, like commit (`cleanupPendingTempFiles`, `:167`). The comment calling an aborted transaction's object a "re-readable publication source" is false and goes.
- Decommission deletes the whole `staging/<srid>/` prefix without `physicalKey()`, while the writer and sweeper apply it (`CasDecommission.cpp:295` vs `ContentAddressedMetadataStorage.cpp:1357`). Check and fix in PR A.

### Operator surface {#operator}

- `system.cas_mounts`: rows in state `terminated` or `fenced` read the owner and show `retired` when it is tombstoned (one GET per such row per query).
- `SYSTEM CAS DROP POOL MEMBER` result column `slot_removed` becomes `member_retired`.
- Refusal texts (CAS-156.2, CAS-156.3): retired owner → `cas-readd-member`; foreign owner → decommission, then `cas-readd-member --server-uuid`; `NoWait` on a hard-killed member → wait for `state = 'fenced'`.

## Residuals {#residuals}

| Residual | Effect | Handling |
|---|---|---|
| A paused admin's drain lands after a rerun and a re-add (`res_lc_pauseddrain`) | deletes the new member's objects; drain deletes cannot be fenced | operator rule: re-add only after every earlier decommission process exited; tracked in CAS-385 |
| The tool dies between the tombstone and its un-tombstone (`res_lc_tombover`) | a live successor keeps running under a tombstone and refuses its next start | availability only; rerun the decommission or re-add |
| States left by 26.6.4 (no counter, no tombstone, or a deleted `mount`) | a new admin can reissue epoch 1; re-add refuses | documented (D9) |
| Old binaries during a rolling upgrade | keep their old races (S6, S11, recursive staging sweep, old decommission tool) | release note |

## Costs {#costs}

| Where | Change |
|---|---|
| Every claim | +1 GET `owner` |
| Every owner write | 32-byte nonce |
| Decommission | −2 DELETE, −4 GET; +1 GET (confirm), +1 PUT (tombstone moved, not new) |
| Re-add | 6 requests, once |
| GC heartbeat floor | +1 GET per retired `srid` per round (`computeHeartbeatFloor`, `CA/Pool/CasServerRoot.cpp:1037`); LIST returns 3 keys per retired `srid` |
| `system.cas_mounts` | +1 GET per terminated or fenced row per query |
| Storage | 3 objects per retired `srid`, under 1 KiB |

Retired `srid`s accumulate with no purge verb. A purge (delete all three objects of a retired, re-add-free `srid`) would reopen epoch reuse for that id; not offered.

## Compatibility {#compat}

| Case | Behaviour | Worse than either version? |
|---|---|---|
| Upgrade: `srid`s retired by 26.6.4 | not repaired; re-add refuses, use a new id | no |
| Upgrade: flat 26.6.4 staging keys | swept as epoch 0 | no |
| Upgrade: existing ids with `/` | keep working; only new ids are refused | no |
| Downgrade: a retired `srid` kept by the new tool | 26.6.4 refuses to start it (`throwIfOwnerRetired`); its GC treats the farewell as terminal; its decommission refuses the tombstone | no |
| Downgrade: a re-added `srid` | 26.6.4 starts it and continues the counter | no |
| Downgrade: `owner_nonce` | 26.6.4 skips it (verify) | no |
| Downgrade: epoch staging keys | 26.6.4 sweeps them recursively | no |
| Mixed: 26.6.4 decommission tool | old behaviour and old races | same as 26.6.4 |
| Mixed: 26.6.4 victim restarts during a new decommission | refused if its owner check is after the tombstone; otherwise no recheck (CAS-158 stays for it) | same as 26.6.4 |
| Mixed: paused 26.6.4 process sweeps staging | deletes a new member's staging (CAS-369 for old nodes) | same as 26.6.4 |

## Delivery {#delivery}

Independent PRs into `antalya-26.6`; each commit reviewable alone (model, code with its tests, docs).

| PR | Content | Depends on |
|---|---|---|
| A | D7; abort deletes staging; `physicalKey` in the decommission staging drain; refusal texts | — |
| B | D1–D5, D8 (epoch keys), owner nonce, `member_retired`, release note | CAS-362 merged |
| C | D6 re-add (CLI and SQL), `retired` in `system.cas_mounts` | PR B merged |

The `fix/antalya-26.6/cas-369-staging-sweep-snapshot` draft is superseded; its abort-delete commit moves to PR A.

## Tests {#tests}

Failing first on `2a034cdb374` unless marked guard.

**PR A**
- `CASServerRootId.NewIdWithSlashIsRefused`; guard `CASServerRootId.ExistingIdWithSlashStillMounts`.
- `ContentAddressedTransaction.AbortDeletesS3Staging`.
- `CASDecommission.StagingDrainUsesThePhysicalPrefix` (if the mismatch is real).

**PR B**
- `CASDecommission.KeepsEpochAndFarewellMount`.
- `CASDecommission.VictimClaimingAfterTheTombstoneFencesItselfAndFails`: the victim waits on the admin's lease, the admin farewells, the victim's claim recheck sees the tombstone, writes `gc_fenced`, fails with the retired-owner error.
- `CASDecommission.FenceBeforeTombstoneUntombstonesAndStops` (confirm step).
- `CASDecommission.ResumesOverItsOwnTombstone`: death after each step, GC fences the dead admin, rerun completes.
- `CASDecommission.FencedAdminStopsInsteadOfRemounting`.
- `CASDecommission.UnconfirmedFarewellIsNotRetired`.
- `CASOwner.DelayedTombstoneAfterReaddFailsOnNonce`.
- `CASMount.NormalMountRefusesARetiredOwnerAfterClaim`; guard `CASMount.AdminModeIsNotReachableFromPoolOpen`.
- `CASStagingSweeper.OldEpochSweepKeepsSuccessorStaging`, `CASStagingSweeper.FlatLegacyKeysAreSwept`, `ContentAddressedTransaction.StagingEpochMatchesAdmissionGeneration`.
- Rewritten: `gtest_cas_decommission.cpp` tests asserting slot deletion or a refused rerun (`RemovesMutableSlotAndRefusesTombstonedRerun`, `SuccessfulDecommissionLeavesTombstonedOwnerAnchor`, `FencedSlotRetirementTailRetiresUncontendedSlot`, `RunsOnAnOpenFence`, `FailedDrainKeepsSlotThenResumes`, `ManifestDebrisFailureKeepsSlotThenResumes`, `MidRetirementCrashResumesViaMountLeaseFallback`, `ErasesAllVictimNamespaces`, `RetainsDebrisWhoseEpochSealIsUnconsumed`, `DuplicateLifeIdRefusesBeforeAnyNamespaceOrSlotMutation`, `CatalogCutIsValidatedBeforeImpersonationAndReusedForSelection`, `DrainClockUnifiesWithTheFarewellBootClockRegardlessOfHostUptime`, `OpeningRetriesPaceOnTheSameFakeClockAndSleepAsTheDrain`) and the `slot_removed` assertions in `gtest_cas_decommission_catalog_duties.cpp`. `OwnerTombstoneAmbiguousSuccessResolvesToCommitted` moves to the new tombstone step. Deleted: `SuccessorReclaimFencesSlotRetirementTail`, `SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor`, `SuccessorOwnerRewriteWinsBeforeTombstone`.
- Integration `test_cas_drop_pool_member`: case (8) scoped to one disk view expects a `terminated` row; case (10) rerun succeeds.

**PR C**
- `CASReadd.SameUuidContinuesTheCounter`, `CASReadd.NewUuidStartsAboveTheOldLife`, `CASReadd.RefusesFencedMount`, `CASReadd.RefusesNonEmptySubtree`, `CASReadd.RefusesNotRetired`, `CASReadd.RefusesSlotRetiredBy2664`, `CASReadd.ConcurrentReaddsOneWins`, `CASReadd.DelayedGenesisMintFailsOverTheReplacedMount`, `CASReadd.CrashBetweenMountAndOwnerReruns`.
- Integration: drop, re-add with a new uuid, the member starts on an empty disk; `system.cas_mounts` shows `retired` before the re-add.
- Soak (CAS-213): decommission under load through the SQL verb, kill the tool at each step and rerun, then re-add and restart.

**Model**: `CaEpochAllocCore` rounds 3–4 (`3b7b6606ec4`, `647c0ffac72`, `c4e0dd307f8`). The model has no remount; the self-remount rule (D5) is argued, not modelled.

## Implementation checks {#checks}

- A self-written `gc_fenced` body must behave as a GC fence everywhere: `isCreatorFenceTerminal`, `computeHeartbeatFloor`, `claimMount` reclaim, `listMounts`.
- The confirm step needs the admin's claim ETag in the tail; `openForDecommission` must expose it.
- `claimOwnerOrThrow` returns no owner ETag today; the tombstone step reads the owner itself.
- `owner_nonce` and `allocation_nonce` (CAS-362) use the same generator.

## Rejected {#rejected}

- **Floor in the owner tombstone, counter deleted** (round 3): green only without zombies; a delayed genesis mint still regresses the slot (`res_ownerfloor_zmint`).
- **Floor in `_pool_meta`** (V2): same residual, a pool-wide RMW, and 26.6.4 `admitOrValidate` drops the field.
- **Document only**: leaves S6 and CAS-158 open and CAS-156 unsupported.
- **Re-add deletes `mount`** (r1): a delayed mint lands below the old epoch (`keep_main`).
- **Staging LIST before claim** (CAS-369 draft): works, but needs a prefix-resolution refactor that epoch keys make unnecessary.
- **Recheck nested ids on every start**: +1 LIST per start and refusals of existing pools at upgrade; forbidding `/` in new ids is simpler.
