# CAS pool member lifecycle: decommission deletes data only {#cas-member-lifecycle}

Decommission keeps a member's three control objects, `gc/server-roots/<srid>/{owner,epoch,mount}`, forever and deletes only its data. A new `cas-readd-member` operation returns the `srid` as an empty member, with the same or a new `server_uuid`. The epoch counter is never deleted, so epochs never repeat across lives. Most of the decommission tail's race handling is deleted.

Closes: CAS-363, CAS-158, CAS-156, CAS-156.1, CAS-157, CAS-369. Partly: CAS-156.2, CAS-156.3, CAS-357, CAS-371, CAS-264. Test coverage: CAS-213.

## Why {#why}

- **The owner object is already permanent.** Decommission tombstones it in place (`9707a61ba2c`, `docs/superpowers/specs/2026-07-18-t5-owner-tombstone-design.md`) because its bytes are identical across lives and a delete by token cannot be fenced. A retired `srid` already leaves one object behind.
- **Deleting `mount` and `epoch` is the source of the open races.** A history of them is in `tmp/noclock/srid-revival-history.md`; the audit scenarios are in `docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md`.
  - S6 (CAS-363): a restarting victim passes the owner check before the tombstone, finds `epoch` absent, and is issued epoch 1 again. Stale GC floors then delete live manifests (S7).
  - S11/S16 (CAS-158): the tombstone, written last, can land over a same-uuid successor that started after the liveness recheck.
  - R4 (`CaEpochAllocCore` `cur_decom_slot`): a claimer paused before its mint lands a lower epoch into the deleted slot; GC with the farewell floor deletes its manifest.
  - S4, S8, S9: ETag ABA and key reuse by zombies, possible only because epochs repeat.
- **Keeping them closes all of these in the model.** `CaEpochAllocCore` `fix_retire` and `fix_retire_gcs` are green on all invariants, zombies included; `sab_retire_delmount` (retire, then delete `mount`) is red. Nothing is deleted, so there is no ABA, no reissued epoch, no regressed slot.
- **Cost:** three small objects per retired `srid` instead of one, reused on re-add.

## Decisions {#decisions}

| # | Decision | Reason |
|---|---|---|
| D1 | Decommission never deletes `owner`, `epoch` or `mount`. | Closes S4–S11 and R4 by construction. |
| D2 | The owner tombstone is written right after the admin claim, before any data is drained. | While the admin holds the slot, no same-uuid successor can be live, so the tombstone cannot land on one (CAS-158). A victim restarting during the drain is refused at the owner check. |
| D3 | Decommission is idempotent and resumable over its own tombstone. | A crash at any step is repaired by running it again. Today a rerun after the tombstone is refused. |
| D4 | `cas-readd-member` returns a retired `srid` as an empty member, same or new uuid. | CAS-156 field report; replacing the uuid is safe because the old owner is retired (CAS-156.1). |
| D5 | Nested and reserved-segment `srid`s are refused at the first owner claim. | Decommission, staging, roots and `floorForNamespace` select by path prefix (CAS-157, CAS-357). |
| D6 | S3 staging keys carry the writer epoch; the startup sweep deletes only older epochs. Aborted transactions delete their staging. | Closes CAS-369 without the pre-claim LIST refactor. Needs monotone epochs, which D1 and CAS-362 give. |
| D7 | No new on-store fields. | Format frozen since 26.6.4; `retired_at_ms` already exists. |

## Design {#design}

Paths: `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Line numbers are at `2a034cdb374`.

### Decommission {#decommission}

`CA/Tools/CasDecommission.cpp:decommissionPoolMember`, new order:

1. **Admin claim**, as today: `Pool::openForDecommission` (`CA/Pool/CasPool.cpp:904`) with `NoWait`. It allocates an admin epoch from the kept counter.
2. **Tombstone the owner** by `If-Match` on the etag read in step 1. Already tombstoned: skip. The replace-and-resolve logic moves here from `:465-503`. A conflict means a concurrent re-add or decommission: stop with an error, no data touched.
3. **Drain data**, unchanged: victim namespaces through `dropNamespace`, manifest debris (needs the mount body for `floorForNamespace`, `:271-276`), `staging/<srid>/`, `roots/<srid>/`.
4. **Farewell**: `admin.reset()`, which writes the terminal `mount` body (`MountLeaseRenewer::terminate`, `CA/Pool/CasServerRoot.cpp:1739`).
5. **Confirm**: one `GET mount`. The run completes (`report.member_retired = true`) only if the body is terminal (clean farewell or `gc_fenced`) for the admin epoch. Otherwise it reports a warning and the operator reruns. A farewell that did not land is the only remaining reason to rerun after the drain.

Deleted from the tail (`:326-506`, about 175 of 535 lines): `deleteSlotObject`, the `claimed_epoch` and `farewell_mount` captures, `captures_match`, the mount and epoch deletes, the "ACCEPTED RESIDUAL WINDOW", the liveness recheck, and the tombstone at the end.

**Claiming over a tombstone.** `claimOwnerOrThrow` (`CA/Pool/CasServerRoot.cpp:525`) refuses a retired owner at `:541` and `:573`. It gets an `OwnerClaim::{Normal, DecommissionAdmin}` argument; `DecommissionAdmin` accepts a retired owner of the same uuid. The pool stores the mode in `PoolConfig` so the admin's self-remount (`Pool::tryRemountOnce`, `CA/Pool/CasPool.cpp:1436`) also accepts it. Without that, a GC fence of the admin during a long drain makes the remount loop throw forever.

### Re-add {#readd}

New verb `clickhouse-disks cas-readd-member --srid <srid> [--server-uuid <uuid>]` and `SYSTEM CAS READD POOL MEMBER '<srid>' [UUID '<uuid>'] FROM DISK '<disk>'`, next to the drop verbs (`programs/disks/CommandCaDropMember.cpp`, `src/Interpreters/InterpreterSystemQuery.cpp:1114`). Steps, each a conditional request:

1. `GET owner`. It must be tombstoned; otherwise refuse ("not retired").
2. `serverRootSubtreeEmpty` (`CA/Pool/CasServerRoot.cpp:500`) must hold. The known access-probe key under `roots/<srid>/` (CAS-371) is deleted first by `If-Match`; anything else refuses with the listed keys.
3. `GET mount`. It must be terminal (clean or `gc_fenced`); a live-looking body refuses ("decommission did not complete").
4. New uuid only: `DELETE mount` by `If-Match`. `claimMount` never takes over a body of another uuid (`ForeignOwner`, `CA/Pool/CasServerRoot.cpp:807`). Same uuid: keep it; the next start reclaims over the clean or fenced body (`:871`).
5. Counter absent (a slot retired by a 26.6.4 decommission, which deleted it): create it at `2^32` by `If-None-Match`. Epochs of the old life are restart counts, far below `2^32`. Check during implementation that no key, codec or metric narrows the epoch below 64 bits.
6. Rewrite `owner` by `If-Match` to `{server_uuid, no tombstone}`.

The next normal start continues the counter. Order matters: until step 6 lands, every start of this `srid` is refused at the owner, so no claimer can mint into the slot step 4 emptied. A zombie mint of the old life fails closed: decided while the slot existed, its `If-None-Match` fails; decided at the old life's first start, it lands the old uuid and an epoch below the counter, and the next start refuses (`ForeignOwner` for a new uuid, the epoch-regression abort at `:829` for the same uuid). Two concurrent re-adds serialize on the owner `If-Match`; the loser stops, and a deleted mount without a rewritten owner is harmless (rerun).

### Nested srids {#nested}

`validateServerRootId` (`CA/Pool/CasServerRoot.h:86`) checks syntax only. Added:
- Reserved segments `owner`, `epoch`, `mount` are refused (`a/mount` collides with `a`'s key).
- At the first owner claim (`claimOwnerOrThrow`, owner-absent branch, before the create at `:559`): `GET owner` of every ancestor (k−1 requests for k segments) and one `LIST` of `serverRootPrefix(srid)` for descendants. Either found: refuse. After the create, run the check again; on a hit, refuse to start (the create is not undone; an operator fixes the config).

Cost: once per `srid` lifetime. Existing nested pools are not detected; fsck reports them (CAS-357).

### Staging {#staging}

- Key: `staging/<srid>/<writer_epoch>/<random32>.tmp`, built in `ContentAddressedTransaction.cpp:901` from `store()->liveWriterEpoch()`.
- Startup sweep (`ContentAddressedMetadataStorage.cpp:892`, `sweepOwnMountStaging`): delete keys whose epoch segment is below the live epoch. Keys with no epoch segment (26.6.4 layout) count as epoch 0. An old process paused before its sweep deletes only epochs below its own, never its successor's.
- Abort deletes staging, best-effort, as commit does (`cleanupPendingTempFiles`, `:167`). The comment claiming an aborted transaction's object is a "re-readable publication source" is false and goes.
- Decommission keeps deleting the whole `staging/<srid>/` prefix. Check: it does not apply `physicalKey()` while the writer and sweeper do (`CasDecommission.cpp:295` vs `ContentAddressedMetadataStorage.cpp:1357`).

The `fix/antalya-26.6/cas-369-staging-sweep-snapshot` draft (LIST before claim) is superseded except its abort-delete commit.

### Operator surface {#operator}

- `system.cas_mounts`: a retired slot stays as a row with `state = 'terminated'`. Add `state = 'retired'` by reading the owner for terminated rows (one GET per terminated row per query).
- `SYSTEM CAS DROP POOL MEMBER` result: column `slot_removed` becomes `member_retired`.
- Refusal texts name the supported verbs (CAS-156.2): tombstoned owner → `cas-readd-member`; foreign owner → `cas-drop-member` then `cas-readd-member --server-uuid`; `NoWait` on a hard-killed member → wait for `state = 'fenced'` (CAS-156.3).

### Kept code {#kept}

- `serverRootSubtreeEmpty`, the absent-counter path of `allocateWriterEpoch` and its epoch-1 guard (`6094c1473ea`): still needed for a brand-new `srid` and for a hand-deleted counter.
- `EpochMintPolicy::DecommissionRecovery` (`CA/Pool/CasServerRoot.h:157`) and the owner-absent fallback in `openForDecommission` (`CA/Pool/CasPool.cpp:938`): legacy-only (states left by 26.6.4 decommissions). Kept; marked legacy in comments.

## Costs {#costs}

| Where | Change |
|---|---|
| GC heartbeat floor | +1 GET per retired `srid` per round (`computeHeartbeatFloor` reads every mount, `CA/Pool/CasServerRoot.cpp:1049`); LIST returns 3 keys per retired `srid`. Not gated on. |
| `system.cas_mounts` | +1 GET per retired row per query. |
| Decommission | −3 DELETE, −2 GET (recheck); +1 GET (confirm). |
| First start of a nested-capable `srid` | +(k−1) GET +1 LIST, once. |
| Storage | 3 objects per retired `srid`, under 1 KiB in total. |

## Compatibility {#compat}

- **26.6.4 servers** honour the owner tombstone (`throwIfOwnerRetired` exists there), so a victim restarting on an old binary is refused too. After re-add, an old binary starts normally and continues the counter.
- **26.6.4 decommission tool** deletes `mount` and `epoch` and tombstones last. Run decommission and re-add only with the new tool; the old tool refuses a slot the new one tombstoned (strict owner claim).
- **Slots retired by 26.6.4** (owner tombstoned, counter deleted): re-add creates the counter at `2^32` (step 5).
- **Staging keys**: 26.6.4 sweeps `staging/<srid>/` recursively, so it also removes the new nested keys; the new sweep treats flat keys as epoch 0.

## Tests {#tests}

Failing first: each test fails on `2a034cdb374` for the stated reason.

**gtest (`src/Disks/tests/`)**
1. `CASDecommission.KeepsEpochAndFarewellMount`: after a run, `epoch` and a terminal `mount` exist; the owner is tombstoned. Red: both deleted.
2. `CASDecommission.VictimRestartDuringDrainIsRefusedAtTheOwner`: a hook restarts the victim between the claim and the drain; its start fails on the tombstone and the counter does not reach a reused value. Red: the start passes the owner check.
3. `CASDecommission.ResumesOverItsOwnTombstone`: crash after each step (claim, tombstone, each drain, farewell); a rerun completes. Red: rerun refused.
4. `CASDecommission.AdminRemountAcceptsItsTombstone`: GC fences the admin mid-drain; the self-remount succeeds and the drain finishes. Red: remount throws.
5. `CASDecommission.UnconfirmedFarewellIsNotRetired`: farewell write fails; `member_retired = false`; a rerun completes.
6. `CASReadd.SameUuidContinuesTheCounter`, `CASReadd.NewUuidStartsAboveTheOldLife`, `CASReadd.LegacySlotWithoutCounterStartsAt2Pow32`, `CASReadd.RefusesNonEmptySubtree`, `CASReadd.RefusesNonTerminalMount`, `CASReadd.RefusesNotRetired`, `CASReadd.ConcurrentReaddsOneWins`, `CASReadd.ZombieMintAfterMountDeleteFails`.
7. `CASServerRootId.RefusesNestedBothDirections`, `CASServerRootId.RefusesReservedSegments`.
8. `CASStagingSweeper.OldProcessSweepKeepsSuccessorStaging` (red: whole prefix deleted), `CASStagingSweeper.FlatLegacyKeysAreSwept`, `ContentAddressedTransaction.AbortDeletesS3Staging` (red: left in place).

Rewritten (they assert deletion or a refused rerun): in `gtest_cas_decommission.cpp`, `RemovesMutableSlotAndRefusesTombstonedRerun`, `SuccessfulDecommissionLeavesTombstonedOwnerAnchor`, `FencedSlotRetirementTailRetiresUncontendedSlot`, `RunsOnAnOpenFence`, `FailedDrainKeepsSlotThenResumes`, `ManifestDebrisFailureKeepsSlotThenResumes`, `MidRetirementCrashResumesViaMountLeaseFallback`, `ErasesAllVictimNamespaces`, `RetainsDebrisWhoseEpochSealIsUnconsumed`, `DuplicateLifeIdRefusesBeforeAnyNamespaceOrSlotMutation`, `CatalogCutIsValidatedBeforeImpersonationAndReusedForSelection`; `slot_removed` assertions in `gtest_cas_decommission_catalog_duties.cpp`. Deleted (they test the removed race handling): `SuccessorReclaimFencesSlotRetirementTail`, `SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor`, `SuccessorOwnerRewriteWinsBeforeTombstone`, `OwnerTombstoneAmbiguousSuccessResolvesToCommitted`.

**Integration** `tests/integration/test_cas_drop_pool_member/test.py`: (8) expects one `retired` row instead of none; (10) a rerun succeeds; new case: drop, re-add with a new uuid, the member starts and serves an empty disk. Stateless `05013_system_cas_drop_pool_member.sql`: column rename.

**Soak** (CAS-213): S45 extended: decommission under load through the SQL verb, kill the tool at each step and rerun, then re-add and restart the member.

**Model**: `CaEpochAllocCore` gets the full lifecycle: tombstone first, resume after a crash at each step, re-add with same and new uuid, two re-adds racing, victim restarts and zombies at every point. Invariants: `EpochNeverReused` across lives, `SlotNeverRegresses`, `RetiredStaysRetired`, no tombstone over a live successor, no dangle through the GC floor. Sabotages: tombstone last; re-add rewrites the owner before deleting the mount; decommission deletes `epoch`. Results go to `CaEpochAllocCore_RESULTS.md`.

## Rollout {#rollout}

One PR into `antalya-26.6` (new branch from the release line), after CAS-362 (nonce) lands. Commits: model; decommission keeps the slot (D1–D3); re-add (D4); nested ids (D5); staging (D6); operator surface. Release note: decommission and re-add only with this version's tool; re-add of a slot retired by 26.6.4 is supported.

## Rejected {#rejected}

- **Floor in the owner tombstone, counter deleted** (`epoch_floor`): needs tombstone-first plus a fresh owner read plus a pre-mint check, and stays red for zombies. Modelled as the alternative (`fix_ownerfloor_*`).
- **Floor in `_pool_meta`** (V2, `fix_poolhwm_fresh_*`): same residual, a pool-wide RMW, and 26.6.4 `admitOrValidate` drops the field.
- **Document only** (option C): leaves S6 and CAS-158 open and CAS-156 unsupported.
- **Staging LIST before claim** (CAS-369 draft): works, but adds a prefix-resolution refactor that monotone epochs make unnecessary.
