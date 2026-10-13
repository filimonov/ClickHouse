# CAS pool member lifecycle: decommission deletes data only {#cas-member-lifecycle}

Decommission keeps a member's control objects `gc/server-roots/<srid>/{owner,epoch,mount}` forever and deletes only its data. A server started with a retired `srid` rejoins as an empty member, with the same or a new `server_uuid`, by one conditional write of the owner; there is no separate command. The epoch counter is never deleted, so epochs never repeat across lives, and the decommission tail loses its race handling.

Closes: CAS-363, CAS-158, CAS-156, CAS-156.1, CAS-157, CAS-369 (multipart abort split to CAS-369.1), CAS-271. Partly: CAS-156.2, CAS-156.3, CAS-357, CAS-371, CAS-264, CAS-159. Acceptance: CAS-213. Residuals go to CAS-385.

Revision 9: rejoin again requires a verified retirement, a verified run reports success (GC-pending rows are information), the admin's recheck uses its starting life, prose fixes (final reviews r8 `tmp/noclock/lifecycle/final2-{opus,codex}.md`). Revision 8: one monotone `owner_life` replaces the nonce, "remount never rejoins" and "rejoin only over verified"; every exit after the tombstone runs the confirm; a foreign reclaim reads the owner (model round 7, `302e8fb42ac`). Revision 7: rejoin only from `Pool::open` and only over a verified retirement; new-uuid rejoin replaces the mount body first; the verified mark is kept on refresh and does not wait for GC (final reviews `tmp/noclock/lifecycle/final-{opus,codex}.md`). Revision 6: verified retirement mark from model round 6 (`c94dca50bf6`). Revision 5.1: #removed lists code that goes beyond the tail (owner decision 2026-10-13). Revision 5: rejoin at start replaces the re-add command (owner decision 2026-10-13: less code; a returning server, same uuid or not, comes back empty). Revision 4 notes: Reviews of r1–r3: `tmp/noclock/lifecycle/{review,rereview,rereview3}-{opus,codex}.md`. Model: `docs/superpowers/models/CaEpochAllocCore_RESULTS.md` rounds 3–5; round 6 checks the r4 changes (#model). Every closure below holds for upgraded servers and tools with intact control objects; #compat lists what old participants keep.

## Why {#why}

- **The owner object is already permanent.** Decommission tombstones it in place (`9707a61ba2c`, `docs/superpowers/specs/2026-07-18-t5-owner-tombstone-design.md`): its bytes are identical across lives, so a delete by token cannot be fenced.
- **Deleting `epoch` and `mount` causes the open races** (history: `tmp/noclock/srid-revival-history.md`; audit: `docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md`):
  - S6 (CAS-363): a restarting victim passes the owner check before the tombstone, finds no counter, and gets epoch 1 again; stale GC floors then delete its manifests (S7).
  - S11/S16 (CAS-158): the tombstone, written last, lands over a same-uuid successor.
  - R4: a claimer paused before its mint lands a lower epoch into the deleted slot.
  - S4, S8, S9: ETag ABA and key reuse, possible only because epochs repeat.
- **Keeping them closes these in the model** (round 4, `lc_*` rows; each row checks a subset of the faults, listed in RESULTS). Guards with a model sabotage are named in #decisions.
- **Cost:** three small objects per retired `srid` instead of one; they are reused on rejoin.

## Decisions {#decisions}

| # | Decision | Evidence |
|---|---|---|
| D1 | Decommission never deletes `owner`, `epoch` or `mount`. | `sab_lc_delepoch` red |
| D2 | Every decommission run writes its own tombstone (new owner life) right after its claim, then confirms it still holds the slot. Every exit after the tombstone (including a failed remount) runs the confirm; on a lost claim it rolls back by `If-Match` on its own tombstone, unless the tombstone it replaced was verified, through the unfenced control plane. A completed run marks its tombstone verified; refreshes keep the mark. | `sab_lc_tomblast`, `sab_lc_noconfirm`, `sab_r5_keeptomb`, `sab_r6_rollbackall`, `sab_r6_rollbackfenced`, `sab_r6_noverify` red |
| D3 | A process records the owner life it started under. Every claim (start and remount) re-reads the owner before installing the renewer and requires exactly `(our uuid, recorded life, not retired)`; anything else makes the claimer fence its own body and fail. A remount never rejoins: a rejoin changes the life. | `sab_lc_norecheck`, `sab_lc_refusedclean`, `sab_r6_recheckretired` red; life check: round 8 |
| D4 | The owner carries a monotone `owner_life`; every owner write sets it to the read value + 1 by `If-Match`. Owner bytes never repeat, and every cached "the owner is X" names the life it saw. | `sab_lc_nononce` red; round 8 |
| D5 | The admin drains on its fenced mount plane and keeps renewing; once it has tombstoned, its remount fails the life check (D3), the run runs its confirm (D2) and fails, and the operator reruns. | `sab_r5_drainopen`, `sab_lc_retryrefuses` red, `RetryCompletes` |
| D6 | A start over a retired owner whose data is gone rejoins inside `claimOwnerOrThrow`, any uuid: for a new uuid it first replaces the terminal `mount` body with its own, then writes the owner (new life). `claimMount` reclaims a terminal body of another uuid only after a fresh `GET owner` shows `(our uuid, recorded life, not retired)`. | `sab_r5_noforeign` wedges; round 6 (a rejoined member can start) |
| D7 | A new `srid` may not contain `/`. | review finding |
| D8 | S3 staging keys carry the writer epoch; aborted transactions delete their staging. | CAS-369 |
| D9 | No repair code for states left by 26.6.4; they are documented. The admin never creates an absent counter (also reached when a new binary died between the owner create and the counter create; the refusal says so, and deleting the owner by hand is safe because the subtree is empty). | owner decision 2026-10-13 |

## Design {#design}

Paths: `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Line numbers at `2a034cdb374`.

### Owner life {#owner-life}

`OwnerObject` (`CA/Formats/CasServerRootFormats.cpp`) gains two optional plain fields:
- `owner_life` (uint64). Every owner write — first claim, tombstone, rollback, verified mark, rejoin — reads the owner and writes `owner_life + 1` by `If-Match` on the read ETag. An owner written by 26.6.4 has no field and reads as 0.
- `retirement_verified` (bool). Set only by decommission step 6; kept by a later tombstone refresh; absent after a rollback or a rejoin.

One counter replaces three r7 rules:
- **Unique bytes.** The life only grows and the owner is never deleted, so two owner writes never produce the same bytes and a delayed `If-Match` against an old ETag cannot land after a later write (the r3 `owner_nonce`).
- **Semantic ABA.** A process records the life it started under (`Pool::open`, after any rejoin). The claim recheck, the remount owner check and a foreign reclaim compare it. A same-uuid rejoin, a rollback or a tombstone in between changes the life, so a partitioned process cannot resume in place under a later life (model round 7 defect 3), and "remount never rejoins" needs no rule of its own.
- **Own writes.** A decommission rolls back by `If-Match` on its own tombstone: any later owner write, including a later run's refresh, changes the life and makes the rollback fail.

26.6.4 decodes the owner with `Tolerant` + `skipUnknown`. A 26.6.4 decommission tool re-encodes the owner without the field (life back to 0); new processes then fail their life check, closed. Record both fields in `Formats/README.md`.

### Claim recheck {#claim-recheck}

In `Pool::mountWritable` (`CA/Pool/CasPool.cpp`) and on every `Claimed` in the fence-recovery path (`Pool::tryRemountOnce`, `:1233`), before `installRenewer`: `GET owner`. It must be exactly `(our uuid, the life this process recorded, not retired)`; the decommission admin (`NoWait`) requires its victim's uuid and the life it read at its start, and accepts that owner retired; after step 2 its remount requires the life of its own tombstone. Otherwise:

1. Read-modify-write the `mount` slot (`claimMount` returns no ETag on `Claimed`): if it still holds this claimer's `(uuid, writer_epoch)`, replace it with the same body marked `gc_fenced`, so the slot is terminal at once and nobody waits out the refused claimer's lease. r3 needed `gc_fenced` rather than a clean farewell because re-add then accepted only clean bodies (`sab_lc_refusedclean`); with fenced drains (D5) either terminal body is safe, and `gc_fenced` keeps the slot visibly not a completed retirement.
2. Throw the matching refusal (retired, foreign or absent owner).

The foreign-owner case matters after a rejoin: a victim that passed `claimOwnerOrThrow` before the decommission can wake after a rejoin gave the `srid` to another uuid and reclaim its own clean farewell; only this recheck stops it from running under the new owner.

Cost: one GET per claim. Why it closes CAS-158 for new binaries: every reclaim path (farewell, `gc_fenced`, `proven_dead`, `cas_unsafe_remount_no_delay`) completes before the recheck, and the admin tombstones before its farewell; a fence before the tombstone is caught by D2's confirm. Remaining: a tombstone the admin sent before it lost the slot can land after a successor's recheck (`res_lc_tombover`); D2's rollback clears the tombstone when the admin is alive to see the lost claim; the successor recorded the older life, so it keeps running until its next remount or restart, which then starts a fresh life.

### Decommission {#decommission}

`CA/Tools/CasDecommission.cpp:decommissionPoolMember`:

1. **Admin claim**: `Pool::openForDecommission` (`CA/Pool/CasPool.cpp:904`). The existing `MountClaimPolicy::NoWait`, used only by decommission, also lets `claimOwnerOrThrow` (refusals at `CA/Pool/CasServerRoot.cpp:539`, `:589`) and the claim recheck accept a retired owner of the victim's uuid; no new mode reaches `Pool::open`. The admin keeps its background workers (`background_watermark = true`, as today: renewal is what lets a long drain outlive one TTL, pinned by `CASDecommission.AlwaysRenewsAdminClaimEvenWhenHostDiskIsObserveOnly`). After step 2 a self-remount fails in `claimOwnerOrThrow` (it does not pass `NoWait`, and the remount path never rejoins, #rejoin), so a fenced admin fails the run (D5); a remount before step 2 changes the admin's epoch and step 3 fails. Record the claim's `writer_epoch` here. The admin never creates an absent `epoch` counter: absent means a state left by 26.6.4; refuse (D9).
2. **Tombstone**: `GET owner`. Its `server_uuid` must be the victim's; another uuid means a rejoin happened, so stop. Remember its life and whether it carried a verified retirement. Replace it by `If-Match` with `{uuid, owner_life + 1, retired_at_ms}`, keeping `retirement_verified` if it was set, also when it was already retired: a paused earlier run can then never remove this run's tombstone, because its rollback uses an older ETag.
3. **Confirm the claim**: `GET mount`. The body must carry the victim's `server_uuid` and the `writer_epoch` recorded in step 1, and be neither `gc_fenced` nor clean (renewals change the ETag and `write_attempt_id`, so they are not compared). Otherwise roll back and fail (`sab_lc_noconfirm`). **Rollback**: only if step 2 found no verified retirement, replace the owner by `If-Match` on step 2's ETag with `{uuid, owner_life + 1}`, no tombstone. A previous retirement is never undone. Steps 2, 3 and the rollback use the always-admitted control-plane engine (`op` in `decommissionPoolMember`), not the fenced mount plane: a fenced admin must still be able to roll back. Every exit after step 2 and before step 5 — an exception, a failed remount (D5), a drain error — runs this confirm before it returns (model round 7 defect 2). After step 5 the slot holds the admin's own clean farewell and the run does not roll back.
4. **Drain data**: victim namespaces through `dropNamespace`, manifest debris, `staging/<srid>/`, `roots/<srid>/`, all on `admin->mountRequests()` (today `CasRequests(..., Fence::open())` and `gc_requests`). The lease-deadline fence stops a fenced admin locally; only attempts already sent can land late (the engine reissues attempts, so a resolved delete can still have one in flight).
5. **Farewell**: `admin.reset()` writes the clean terminal body (`MountLeaseRenewer::terminate`, `CA/Pool/CasServerRoot.cpp:1739`); it reports no outcome.
6. **Verify**: `GET mount` (clean farewell of the admin's `writer_epoch`) and `GET owner` (retired, the life of step 2). If both hold, rewrite the owner by `If-Match` with the tombstone plus `retirement_verified = true` (life + 1). Report `member_retired = true` once the mark is written. Catalog rows of the victim still `Removing` (warning at `CasDecommission.cpp:309-324`) are reported as GC-pending information, not as a failure: GC deletes them, and the operator does not rerun for them. The operator reruns only when the run failed before the mark. The verified mark does not wait for GC: it records that this run's claim, tombstone and farewell all held.

   **A rerun never decommissions a rejoined member.** A rerun is needed only after a run that stopped before its verified mark, and an unverified tombstone blocks rejoin (#rejoin), so the owner a rerun finds is still the old member's. A decommission over a live, non-retired owner is a new decommission of whoever owns the `srid` now; the verb says so in its report (the uuid and life it retired).

   Why the verified mark: a rerun must tell a completed retirement from a dead earlier run's tombstone that landed late. Without it, run 1's delayed tombstone can land over a victim that reclaimed while the rerun was fenced; the rerun reads it as a previous retirement and does not roll back, and run 1's rollback fails on the rewritten token (`sab_r6_noverify`). Step 2 therefore treats only a **verified** tombstone as "already retired" for the rollback decision.

Deleted from the tail (`:326-506`) and the helper `deleteSlotObject` (`:99-116`): the stale-cut recheck (`retirement_catalog_cut` against `fresh_retirement_catalog`), the `claimed_epoch` and `farewell_mount` captures, `captures_match`, the two slot deletes, the "ACCEPTED RESIDUAL WINDOW", the liveness recheck, and the tombstone at the end. About 195 lines go; about 50 come back for steps 2, 3 and 6.

`EpochMintPolicy::DecommissionRecovery` (`CA/Pool/CasServerRoot.h:158`) serves only 26.6.4 leftovers and becomes unreachable with step 1's rule; remove it in PR B. The owner-absent fallback in `openForDecommission` (`:938`) stays: an owner deleted by hand with the counter present still reaches it, and the foreign-owner refusal text tells operators to delete the owner.

### Other code removed {#removed}

Beyond the decommission tail. All in PR B unless marked.

| Code | Why it goes |
|---|---|
| `EpochMintPolicy`, its `DecommissionRecovery` branch in `allocateWriterEpoch` (`CA/Pool/CasServerRoot.cpp:646-672`, `CasServerRoot.h:157-182`), the `now_ms` parameter and its four callers (`CA/Pool/CasPool.cpp:630`, `:747`, `:794`, `:1439`), the `epoch_policy` plumbing and the forward declaration of `describeMountHolder` (`:429`) | decommission never mints over an absent counter (D9) |
| The slot-vanished branch of `claimMountAwaitingExpiry` (`CasServerRoot.cpp:~993-1006`) with its pacing and restart budget, and `CASMountAwaitExpiry.PersistentSlotVanishPacesAndBoundsRestartsInsteadOfSpinning` | only decommission deleted slots; a vanished slot now returns the last result and the next start claims the empty slot |
| The vanished-slot prune of `computeHeartbeatFloor` (`seen_srids`, final prune loop) | slots no longer vanish; terminal slots already leave `obs` |
| `CASMount.DecommissionRemintOverTerminalMountMintsDistinctEpoch`, `CASMount.DecommissionRemintOverLiveMountRefuses` | test the removed policy |
| `slot_removed` everywhere (report, `InterpreterSystemQuery.cpp:1114,1131`, `CommandCaDropMember.cpp`, event detail `"slot_removed"` at `CasDecommission.cpp:529`, docs) | renamed `member_retired` |
| Docs describing slot deletion: `docs/en/antalya/cas/architecture/mounts-and-leases.md` (owner claim, epoch-recovery precheck, the `Live --> Absent` decommission edge, the debris-ordering rationale), `configuration.md:137`, `debugging.md:145`, `backend.md:127`, `CasPool.h:420-433` | describe removed behaviour |

Kept, with comments corrected: the `same_epoch_occupied` arm of `claimMount` (#2512; a 26.6.4 node can still share an epoch), `ForeignOwner` for a live foreign body (two servers configured with one `srid`), the absent-counter branch and the epoch-1 guard of `allocateWriterEpoch` (first start of a new `srid`), `serverRootSubtreeEmpty`, `floorForNamespace`'s absent-mount path, `reconcileStaleCreator` (now also resolves a stalled `Creating` of a retired `srid`, through the kept terminal slot), `isCreatorFenceTerminal`'s fail-closed absence.

### Rejoin at start {#rejoin}

No new command. A server whose `srid` is retired rejoins as an empty member on its normal start, with the same or a new `server_uuid`. The tombstone now means "this member's data is being or has been removed"; once it is removed the slot is free.

In `claimOwnerOrThrow` (`CA/Pool/CasServerRoot.cpp:525`), when called from `Pool::open` (a process start) and the owner is retired (any uuid), instead of `throwIfOwnerRetired`. A remount does not get here: it fails the life check (D3) first. The retirement must be verified (`retirement_verified`): a decommission that stopped before its mark can be rerun, and a rejoin in between would let that rerun drop the new member (final review r8, Opus P1-A). An unverified tombstone refuses, retryable: rerun the decommission.

1. `GET epoch`: must exist. `GET mount`: must be terminal (clean or `gc_fenced`). A live-looking body: refuse, retryable. Missing objects (retired by 26.6.4): refuse; use a new `server_root_id` (D9).
2. `serverRootSubtreeEmpty` (`:500`; the catalog, `cas/manifests/<srid>/`, `roots/<srid>/`) must hold. The previous life's access-probe key under `roots/<srid>/` (CAS-371) is deleted first by `If-Match`. Anything else: refuse, retryable (the drain has not finished).
3. New uuid only: replace the terminal `mount` body by `If-Match` with a terminal body of our uuid (same `writer_epoch`, clean). A 26.6.4 starter of the old uuid that passed its owner check before the decommission then fails its own `If-Match` or gets `ForeignOwner`, instead of reclaiming its old farewell under our owner (26.6.4 has no claim recheck). Two concurrent rejoins may leave owner and mount of different uuids; that is harmless, because `claimMount` reclaims a foreign terminal body and the claim recheck decides.
4. Replace `owner` by `If-Match` on the ETag read above with `{our uuid, owner_life + 1}`, no tombstone, no verified mark; record that life. A conflict: re-read and decide again (another starter may have won).

Then the normal path continues: the kept counter gives an epoch above every earlier life, `claimMount` reclaims the terminal body, and the claim recheck (D3) proves the owner is ours.

Concurrent starters serialize on the owner `If-Match`; a loser with another uuid gets "owned by a different server". The `mount` body is not touched before the claim: the slot is never absent (a delayed mint of any earlier life fails its `If-None-Match`), and its `writer_epoch` stays, so `claimMount`'s `newer_epoch` refusal (`:822`) still stops a delayed lower-epoch claimer.

A victim that restarts during its own decommission waits on the admin's live lease or is refused (data not empty); if it reclaims right after the farewell, its recheck sees the tombstone, it self-fences and fails, and its next start rejoins once the data is gone. A returning old host comes back empty: its table definitions in the local server metadata still exist, and their paths on the CAS disk now have no data. Rejoin requires the victim's catalog rows to be gone, so the next write to such a path starts a fresh namespace (#checks).

Cost: on the rejoin path only, 2 GET, the emptiness check (1 catalog read, 2 LIST), at most 1 DELETE, 1 PUT. The normal start pays nothing extra here.

**`claimMount` change** (`:810-817`): a terminal (clean or `gc_fenced`) body of another `server_uuid` is reclaimable, after the `newer_epoch` check. Before the reclaim, `claimMount` reads the owner and requires `(our uuid, recorded life, not retired)`; the claim recheck after it is too late, because a reclaim that then fails leaves an old-uuid body a 26.6.4 starter could take (`sab_r8_foreignnoowner`). One extra GET, only on the foreign-reclaim path. A live-looking body of another uuid stays `ForeignOwner`. Two contracts change with it: the `claimOwnerOrThrow` header ("never reassigned to a different UUID") and the comment on the `ForeignOwner` arm; rejoin is now the supported reassignment.

Without the `gc_fenced` case a rejoin can wedge the slot: a victim that reclaimed and self-fenced (D3) leaves `(old uuid, gc_fenced)`, and every start of the new uuid would get `ForeignOwner` with no supported repair.

### New srids without `/` {#srid-slash}

`validateServerRootId` (`CA/Pool/CasServerRoot.h:86`) keeps accepting `/` for existing members. `claimOwnerOrThrow`, owner-absent branch (create at `:564`), refuses an id with `/`. Decommission and staging select by path prefix, so nested ids made `a`'s decommission remove `a/b` (CAS-157). Not covered: existing nested pools, and a new `a` claimed over an existing but empty `a/b` (`serverRootSubtreeEmpty` sees no data); both documented. The fsck part of CAS-357 stays open.

### Staging {#staging}

- Key `staging/<srid>/<writer_epoch>/<random32>.tmp` (`ContentAddressedTransaction.cpp:901`). The epoch and the admission generation used at `:916` and in `finalize` come from one capture of the pool incarnation, so a self-remount between them cannot stage new-generation bytes under an old epoch.
- The startup sweep (`ContentAddressedMetadataStorage.cpp:892`, `sweepOwnMountStaging`) fixes its cutoff to the live epoch at the start and deletes keys below it. Flat keys `staging/<srid>/<random32>.tmp` (26.6.4 layout) count as epoch 0; deeper paths that do not parse as an epoch are left alone, so a nested `staging/a/b/...` of another member is never swept by `a`.
- Abort deletes staging, best-effort, like commit (`cleanupPendingTempFiles`, `:167`). The comment calling an aborted transaction's object a "re-readable publication source" is false and goes.
- Incomplete multipart uploads under `staging/<srid>/` (a process killed mid-upload) are invisible to object LIST and stay forever. The docs state the bucket lifecycle rule `AbortIncompleteMultipartUpload` for S3 staging; aborting them at mount start is CAS-369.1.
- The decommission staging drain stays on logical keys: `ObjectStorageBackend::listUnder` applies the local prefix itself, unlike the direct `IObjectStorage` calls of the writer and sweeper. A test pins that the drain removes the objects the writer created.

### Operator surface {#operator}

- `system.cas_mounts`: rows in state `terminated` or `fenced` read the owner and show `retired` when it is tombstoned (one GET per such row per query).
- `SYSTEM CAS DROP POOL MEMBER` result column `slot_removed` becomes `member_retired`.
- Refusal texts (CAS-156.2, CAS-156.3): retired owner with data left → "decommission not finished; rerun `SYSTEM CAS DROP POOL MEMBER`"; foreign live owner → decommission that member first, then start this server; `NoWait` on a hard-killed member → wait for `state = 'fenced'`.

## Residuals {#residuals}

Tracked in CAS-385 unless marked.

| Residual | Effect | Handling |
|---|---|---|
| A drain attempt already sent lands after a rejoin, even in one uncontended run whose delete lost its response (`res_r5_inflight_norerun`); fencing and process exit do not bound when the store applies it | deletes a new member's object only where the key and the bytes repeat: drain deletes go by exact incarnation, namespace and manifest keys carry the life or epoch, staging keys are random. What remains is `roots/<srid>/` (mountpoint objects and the access probe), whose identical bytes repeat the S3 ETag | same class as CAS-371; no epoch in those keys |
| The admin's tombstone lands after a successor's recheck, and the admin dies before its rollback (`res_lc_tombover`) | a live successor keeps running under an unverified tombstone and refuses its next start | availability only. Repair that keeps the successor's data: clear the tombstone by hand (the refusal names this case: unverified tombstone with data present). Rerunning decommission would delete that data |
| States left by 26.6.4 (no counter, no tombstone, or a deleted `mount`) | decommission and rejoin refuse | documented (D9) |
| A paused 26.6.4 starter of uuid X wakes while the slot holds a terminal uuid-X body and the owner has another uuid. Reached by uuid reuse across two retirements (final review r8, codex 1) and, shorter, by two rejoins of different uuids racing: the losing rejoiner's mount write succeeds after the winner's, its owner write fails (`res_r9_uuidreuse`) | the 26.6.4 starter runs under another uuid's owner | mixed-version only: new binaries recheck the owner. Needs a 26.6.4 process paused before its claim through a whole decommission plus a rejoin race. Documented, CAS-385. Closing it needs a rejoin that claims the slot with a live body before its owner write |
| Old binaries during a rolling upgrade | keep their old races (S6, S11, recursive staging sweep, old decommission tool) | release note |
| Retired `srid`s accumulate | +1 GET each per GC round | no purge verb: a purge would reopen epoch reuse |

## Costs {#costs}

| Where | Change |
|---|---|
| Every claim | +1 GET `owner` |
| Every owner write | one integer (`owner_life`) |
| Decommission | today's tail: 5 GET, 2 DELETE, 1 PUT; r6: 4 GET (tombstone read, confirm, verify ×2), 2 PUT (tombstone, verified mark), +1 PUT on rollback |
| Rejoin (rare path) | 2 GET, the emptiness check (1 catalog read, 2 LIST), ≤1 DELETE, 1 PUT |
| GC heartbeat floor | +1 GET per retired `srid` per round (`computeHeartbeatFloor`, `CA/Pool/CasServerRoot.cpp:1037`); LIST returns 3 keys per retired `srid` |
| `system.cas_mounts` | +1 GET per terminated or fenced row per query |
| Storage | 3 objects per retired `srid`, under 1 KiB |

## Compatibility {#compat}

| Case | Behaviour | Worse than either version? |
|---|---|---|
| Upgrade: `srid`s retired by 26.6.4 | not repaired; rejoin refuses, use a new id | no |
| Upgrade: a 26.6.4 decommission stopped midway | the new tool refuses an absent counter; finish nothing, use a new id | no |
| Upgrade: flat 26.6.4 staging keys | swept as epoch 0 | no |
| Upgrade: existing ids with `/` | keep working; only new ids are refused | no |
| Downgrade: a retired `srid` kept by the new tool | 26.6.4 refuses to start it (`throwIfOwnerRetired`); its GC treats the farewell as terminal; its decommission refuses the tombstone | no |
| Downgrade: a rejoined `srid` with a new uuid | after its first start on the new version the slot holds the new uuid and 26.6.4 starts it normally; before that, 26.6.4 `claimMount` returns `ForeignOwner` on the old life's farewell | no: 26.6.4 cannot rejoin at all; release note says start a rejoined member once before downgrading |
| Downgrade: a rejoined `srid` with the same uuid | 26.6.4 starts it and continues the counter | no |
| Downgrade: `owner_life`, `retirement_verified`, epoch staging keys | skipped; swept recursively | no |
| Mixed: 26.6.4 decommission tool rewrites an owner | the life drops to 0; new processes fail their life check and stop | no: fail-closed |
| Mixed: 26.6.4 decommission tool | old behaviour and old races | same as 26.6.4 |
| Mixed: 26.6.4 victim restarts during a new decommission | refused if its owner check is after the tombstone; otherwise no recheck (CAS-158 stays for it) | same as 26.6.4 |
| Mixed: 26.6.4 starter of the old uuid, prechecked before the decommission, wakes after a new-uuid rejoin | its claim of the old farewell fails: the rejoin replaced the mount body with its own uuid first (#rejoin step 3) | no |
| Mixed: paused 26.6.4 process sweeps staging | deletes a new member's staging (CAS-369 for old nodes) | same as 26.6.4 |

## Delivery {#delivery}

Independent PRs into `antalya-26.6`; each commit reviewable alone (model, code with its tests, docs).

| PR | Content | Depends on |
|---|---|---|
| A | D7; abort deletes staging; the `AbortIncompleteMultipartUpload` doc rule; drain-prefix test; refusal texts | — |
| B | D1–D5, D8 epoch keys, D9 admin rule, `member_retired`, release note; CAS-271 (log that `openForDecommission` skips the capability probe, fix the probe's "no override" text in `CA/Backend/CasProbe.cpp`) | CAS-362 merged |
| C | D6 rejoin in `claimOwnerOrThrow` and the `claimMount` change, `retired` in `system.cas_mounts` | PR B merged |

The `fix/antalya-26.6/cas-369-staging-sweep-snapshot` draft is superseded; its abort-delete commit moves to PR A. Not in these PRs: local scratch sweep at mount (CAS-125/101, its own PR), the ownership-decidability table (CAS-187).

## Tests {#tests}

Failing first on `2a034cdb374` unless marked guard.

**PR A**
- `CASServerRootId.NewIdWithSlashIsRefused`; guard `CASServerRootId.ExistingIdWithSlashStillMounts`.
- `ContentAddressedTransaction.AbortDeletesS3Staging`.
- Guard `CASDecommission.StagingDrainRemovesWhatTheWriterStaged` (local and native prefixes).

**PR B**
- `CASDecommission.KeepsEpochAndFarewellMount`.
- `CASDecommission.LongDrainOutlivesOneTtl`; keep `CASDecommission.AlwaysRenewsAdminClaimEvenWhenHostDiskIsObserveOnly`.
- `CASDecommission.RollbackKeepsAPreviousRetirement`: a rerun over a verified retirement loses its claim; the owner stays retired. `CASDecommission.RefreshKeepsTheVerifiedMark` (three runs: verify, refresh and die, rerun fenced before confirm). `CASDecommission.VerifiedMarkDoesNotWaitForCatalogGc`. `CASDecommission.FailedRemountStillRunsTheConfirm`.
- `CASDecommission.RollbackRunsOnTheControlPlaneAfterAFence`.
- `CASDecommission.StopsWhenTheOwnerUuidChanged` (rejoin between runs).
- `CASDecommission.InFlightDeleteAfterRejoinHitsOnlyIdenticalRootsBytes`: a delete attempt delivered after a rerun and a rejoin removes nothing under namespace, manifest or staging keys; pins the `roots/` residual.
- `CASDecommission.VictimClaimingAfterTheTombstoneFencesItselfAndFails`: the victim waits on the admin's lease, the admin farewells, the victim's recheck sees the tombstone, writes `gc_fenced`, fails with the retired-owner error.
- `CASDecommission.FenceBeforeTombstoneUntombstonesAndFails`; guard `CASDecommission.ConfirmAcceptsARenewalBetweenClaimAndConfirm` (base has no confirm).
- `CASDecommission.PausedRunCannotUntombstoneARerunsTombstone` (two admins, the rerun's refresh bumps the life).
- `CASDecommission.ResumesOverItsOwnTombstone`: death after each step, GC fences the dead admin, rerun completes.
- `CASDecommission.FencedAdminFailsInsteadOfRemounting`, `CASDecommission.DrainStopsOnTheAdminLeaseFence`.
- `CASDecommission.UnconfirmedFarewellIsNotRetired`, `CASDecommission.AbsentCounterIsRefused`.
- `CASOwner.DelayedTombstoneAfterRejoinFailsOnLife` on a backend with content-derived ETags for `owner` keys (like `ContentEtagBackend` for markers); in-memory backends mint increasing ETags and would pass vacuously.
- `CASMount.NormalMountRefusesARetiredOwnerAfterClaim`, `CASMount.ClaimRecheckRefusesAForeignOwner` (a victim prechecked before decommission wakes after a new-uuid rejoin), `CASMount.FenceRecoveryReclaimRechecksTheOwner`, `CASMount.RemountRefusesAfterASameUuidRejoin` (life changed), `CASMount.ForeignReclaimReadsTheOwnerFirst`; guard `CASMount.PoolOpenNeverAcceptsARetiredOwner`.
- `CASStagingSweeper.OldEpochSweepKeepsSuccessorStaging`; `ContentAddressedTransaction.StagingEpochMatchesAdmissionGeneration` with a test hook that self-remounts between the staging-key build and the admission capture; guards `CASStagingSweeper.FlatLegacyKeysAreSwept`, `CASStagingSweeper.NestedOtherMemberKeysAreKept`.
- Rewritten: `gtest_cas_decommission.cpp` tests asserting slot deletion or a refused rerun (`RemovesMutableSlotAndRefusesTombstonedRerun`, `SuccessfulDecommissionLeavesTombstonedOwnerAnchor`, `FencedSlotRetirementTailRetiresUncontendedSlot`, `RunsOnAnOpenFence`, `FailedDrainKeepsSlotThenResumes`, `ManifestDebrisFailureKeepsSlotThenResumes`, `MidRetirementCrashResumesViaMountLeaseFallback`, `ErasesAllVictimNamespaces`, `RetainsDebrisWhoseEpochSealIsUnconsumed`, `DuplicateLifeIdRefusesBeforeAnyNamespaceOrSlotMutation`, `CatalogCutIsValidatedBeforeImpersonationAndReusedForSelection`, `DrainClockUnifiesWithTheFarewellBootClockRegardlessOfHostUptime`, `OpeningRetriesPaceOnTheSameFakeClockAndSleepAsTheDrain`) and the `slot_removed` assertions in `gtest_cas_decommission_catalog_duties.cpp`. `OwnerTombstoneAmbiguousSuccessResolvesToCommitted` moves to step 2. Deleted: `SuccessorReclaimFencesSlotRetirementTail`, `SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor`, `SuccessorOwnerRewriteWinsBeforeTombstone`.
- Integration `test_cas_drop_pool_member`: case (8) scoped to one disk view expects a `terminated` row; case (10) rerun succeeds.

**PR C**
- `CASRejoin.SameUuidStartsEmptyAndContinuesTheCounter`, `CASRejoin.NewUuidReclaimsTheOldLifesFarewell`, `CASRejoin.NewUuidReclaimsASelfFencedOldVictimBody`, `CASRejoin.RefusesWhileTheDecommissionRuns` (live-looking mount), `CASRejoin.RefusesWhileDataRemains`, `CASRejoin.RefusesSlotRetiredBy2664`, `CASRejoin.ConcurrentStartersWithDifferentUuidsOneWins`, `CASRejoin.DelayedLowerEpochClaimAfterRejoinIsRefused` (over a foreign farewell), `CASRejoin.VictimRefusedAfterFarewellRejoinsOnItsNextStart`, `CASRejoin.RefusesWhileAnUnfinishedDecommissionLeftData`, `CASRejoin.NewUuidReplacesTheMountBeforeTheOwner` (an old-uuid claim prechecked earlier loses its `If-Match`).
- `CASMount.ForeignTerminalBodyIsReclaimable` (clean and `gc_fenced`); guard `CASMount.ForeignLiveBodyStaysForeignOwner`.
- Integration: drop a member, then start it again with the same uuid and, separately, with a new uuid; both start on an empty disk; `system.cas_mounts` shows `retired` in between (case (8) updated).
- Soak (CAS-213): decommission under load through the SQL verb, kill the tool at each step and rerun, then restart the member and check it rejoins empty.

## Model {#model}

Bounded checks of an abstraction, not proofs. Rounds 3–4 (`3b7b6606ec4`, `647c0ffac72`, `c4e0dd307f8`) checked r2; round 5 checked r3:
- every run rewrites its tombstone; un-tombstone only of its own; invariant "a finished decommission leaves the owner retired";
- `ZombieRun` combined with `GcFences`;
- re-add as one owner write (r3; r5 moves it into the starter); `claimMount` reclaims a foreign clean farewell; two revival uuids;
- the admin drains on its fenced plane (drain steps gated by the admin's fence; in-flight requests modelled as late);
- the admin refuses an absent counter.
Self-remount is modelled since round 7 (`RemountStart`, `RemountOwner`; `sab_r8_remountstops`).

Round 5 result (`5b3224c52a0`, `03d506c91aa`): all 17 rows met expectations. The own-tombstone rewrite closes the paused-run un-tombstone (`sab_r5_keeptomb` red); fenced drains close the paused-run drain (`sab_r5_drainopen` red); the foreign clean reclaim is needed for a new-uuid re-add (`sab_r5_noforeign` wedges). Two guards are not shown load-bearing, because the model has no renewals, local lease deadline or self-remount: re-add's clean-farewell precondition (`sab_r5_skipclean`) and `ForeignOwner` for a foreign `gc_fenced` body (`sab_r5_foreignfenced`). r4 drops both (#rejected, #readd). The in-flight drain residual is in #residuals.

Round 7 (`ff90179ebb3`..`31984db4332`, RESULTS "Round 7") checked r7: green after three guards it proposed (foreign reclaim reads the owner, a failed admin remount still confirms, a life check on remount); `sab_r7_dropmark`, `sab_r7_ownerfirst`, `sab_r7_foreignnoowner`, `sab_r7_remountstops` red; `sab_r7_rejoinunverified` and `sab_r7_remountrejoin` green (not load-bearing under the life check). r8 folds these into `owner_life`.

Round 9 (`6bdcf7f2d56`..`cbb2427dee5`, RESULTS "Round 9") checked r9 with the tool reading its victim from the owner, as `readOwnerUuid` does: all seven rows green (`r9_core_z` 11.0M, `r9_core_g` 15.0M, `r9_newuuid` 2.9M, `r9_old` 4.3M, `r9_threeruns` 4.8M, `r9_remount` 56.3M, `r9_newdecom` 1.5M); new invariant `NoDrainOfLaterLife`. Opus P1-A is reproduced by two routes and closed: `sab_r9_rejoinunverified` red, `res_r9_rerunafterverified` (the witness that a verb over a live owner is a new decommission) red. `res_r9_uuidreuse` is the mixed-version residual in #residuals.

Round 8 (`bf091c5f81f`..`0c82c9b06b4`, RESULTS "Round 8") checked r8: all rows green (`r8_core_z` 12.6M, `r8_core_g` 25.6M, `r8_newuuid` 23.2M, `r8_old` 41.7M, `r8_threeruns` 8.2M, `r8_remount` 56.7M states), every sabotage red (`sab_r8_rechecknolife`, `sab_r8_rollbacknolife`, `sab_r8_foreignnoowner`, `sab_r8_remountstops`, `sab_r8_dropmark`, `sab_r8_ownerfirst`). The life check subsumes the r7 remount generation, "remount may rejoin" and "rejoin only over verified"; those switches are gone from the model. Its checklist was: `owner_life` as the only owner identity token (no nonce, no separate generation); rejoin without the verified condition; remount refused by the life check alone; sabotages: rollback ignoring the life, recheck ignoring the life, foreign reclaim without the owner read, a failed remount skipping the confirm, refresh dropping the mark.

Round 6 (`f702733167a`..`c94dca50bf6`, RESULTS "Round 6") checked r4 with r5's rejoin: green with the verified mark (`r6_core_z` 15.4M, `r6_core_g` 38.2M, `r6_newuuid_z` 23.7M, `r6_newuuid_g` 49.6M states; `NoWedge` and `DoneMeansRetired` hold); each guard's sabotage is red (`sab_r6_keepforeignfenced`, `sab_r6_recheckretired`, `sab_r6_rollbackall`, `sab_r6_rollbackfenced`, `sab_r6_nouuidcheck`, `sab_r6_rejoinany`, `sab_r6_noverify`). GC fencing dead leases is needed for liveness (`res_r6_nogcdead`). Its checklist was:
- the claim recheck refuses an absent, foreign or retired owner (the model already does; the spec now matches);
- `claimMount` reclaims a foreign `gc_fenced` body; property: a rejoined member can start (no permanent `ForeignOwner` wedge); race a prechecked victim's claim against the rejoin's reads and owner write;
- rollback only of a retirement this run introduced, on the control plane; `DoneMeansRetired` keyed on reported completions, not on the program counter, so a rerun does not reset it;
- decommission step 2 stops when the owner's uuid changed.

## Implementation checks {#checks}

- A self-written `gc_fenced` body must behave as a GC fence everywhere: `isCreatorFenceTerminal`, `computeHeartbeatFloor`, `claimMount` reclaim, `listMounts`.
- `claimOwnerOrThrow` returns no owner ETag; decommission step 2 reads the owner itself.
- A server returning with its old local table definitions after its `srid` was retired attaches those tables empty; confirm no path still resolves to a terminal namespace life (rejoin waits until GC removed the catalog rows).
- `throwIfOwnerRetired` remains only for the cases rejoin refuses.
- The admin keeps `background_watermark = true`; its self-remount must fail on the owner check after step 2 (no `NoWait` on that path).
- `DecommissionRecovery` callers after step 1's rule.

## Rejected {#rejected}

- **Floor in the owner tombstone, counter deleted** (round 3): green only without zombies; a delayed genesis mint still regresses the slot (`res_ownerfloor_zmint`).
- **Floor in `_pool_meta`** (V2): same residual, a pool-wide RMW, and 26.6.4 `admitOrValidate` drops the field.
- **Document only**: leaves S6 and CAS-158 open and CAS-156 unsupported.
- **Re-add deletes `mount`** (r1): a delayed mint lands below the old epoch (`keep_main`).
- **Re-add replaces `mount`** (r2): concurrent re-adds with different uuids split owner and mount (codex and opus r2 finding 1/2).
- **Admin without background workers** (r3): also stops renewal; a drain longer than one TTL fences itself.
- **`gc_fenced` farewell when a drain request is unresolved** (r3.1): the engine reissues attempts, so a resolved delete can still have one in flight; the rule narrows nothing and has no mechanism across `dropNamespace`, `sweepNamespace` and `deleteListedPrefix`.
- **Re-add only over a clean farewell** (r3): not load-bearing once drains are fenced (`sab_r5_skipclean`), and it refuses the self-fenced victim body.
- **Confirm by the claim ETag** (r2): renewals change it.
- **`physicalKey` in the decommission staging drain** (r2): the backend applies the prefix; adding it doubles it.
- **Staging LIST before claim** (CAS-369 draft): works, but needs a prefix-resolution refactor that epoch keys make unnecessary.
- **Recheck nested ids on every start**: +1 LIST per start and refusals of existing pools at upgrade.
