# `CaEpochAllocCore` results {#ca-epoch-alloc-core-results}

Model of writer-epoch allocation, the mount slot and the decommission tail for one server root, at release line `2a034cdb374` (`antalya-26.6-20261008` = `d76b59f9295` + PR #2512). Backlog: CAS-362 (twin allocation), CAS-363 (decommission re-mints epoch 1), CAS-158 (tombstone over a successor).
Runner: `run_epochalloc.sh` (`SLOW=1` adds the rows marked SLOW). Paths below are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Verdict {#verdict}

- **CAS-362 reproduced.** Two same-uuid instances get the same epoch. One of them sends an ambiguous create of `{2}`; the other creates the same bytes. The resolve read in `CasOperation::writeLoop` sees a moved precondition with equal bytes and returns `Committed`. Same on S3 and GCS: the rule compares bytes, not tokens.
- **PR #2512 already contains the twin's harm.** With the strict claim (`same_epoch_occupied`), `cur_twin_dangle` is green: the second twin never writes. The `d76b59f9295` refresh rule (`base_twin_dangle`) dangles a reference.
- **CAS-363 reproduced.** After the tail deletes `mount` and `epoch`, a victim restart mints epoch 1 again (`cur_decom`, S3 and GCS). This reaches a dangling reference through the orphan sweep with a floor read from the farewell (`cur_decom_reusedangle`).
- **New finding: deleting the mount slot is a hazard of its own.** A claimer that read the slot absent before the decommission and paused sends its `If-None-Match` after `DDelMount`. It mints a lower epoch into the slot (`cur_decom_slot`, `SlotNeverRegresses`) and the sweep deletes its in-flight manifest (`cur_decom_dangle`). This needs no epoch reuse, so a retired counter alone does not close it. `sab_retire_delmount` shows it stays red when the retiring tail still deletes `mount`.
- **Round 1 recommendation (retire + keep `mount`, `fix_both`) was rejected by the owner:** it leaves permanent per-srid objects and blocks srid reuse. Round 2 below keeps today's tail (delete `mount`, delete `epoch`, tombstone `owner`) and makes a re-created counter start above every earlier epoch.
- **Round 2 recommendation: V2, a pool high-water mark in `_pool_meta`, read again after the absent read, plus a mint check in `claimMount` and the nonce.** Green for all five invariants without any time assumption (`fix_poolhwm_fresh_nz_mc_*`, `fix_poolhwm_twins`). V1 (store time) reaches the same result only if the store clock never steps back and a counter is never deleted in the second it was created (`fix_storetime_fresh_nz_slow_mc_*` vs `sab_storetime_back`, `fix_storetime_fresh_nz_*`).
- **The zombie class stays red under both variants.** A counter create or a mount mint decided before the tail's DELETE and landing after it reuses or regresses an epoch. Neither V1 nor V2 changes it (`fix_poolhwm_fresh_mc_*` all red); only not deleting closes it. Every round-2 green needs `NoZombie`.
- **Round 3 (floor in the owner tombstone, counter deleted): rejected.** Green only under `NoZombie` (`fix_ownerfloor`). A delayed mint regresses the slot even with the pre-mint check (`res_ownerfloor_zmint`). The S11 step needs an un-tombstone on any delete mismatch, which makes the step itself redundant (`sab_ownerfloor_nodelfail`, `fix_ownerfloor_nomove`).
- **Round 4 (decommission keeps `owner`, `epoch` and the farewell `mount`; re-add): recommended, with two additions found here.** Green with zombie creates and mints, victim restarts, twins, a re-run after a death at any step, a GC fence of the admin, same- and new-uuid re-add and racing re-adds (`lc_*`). Every guard has a red sabotage; the pre-mint check is not needed (`lc_nomc`). Additions: the admin confirms its claim after the tombstone (`sab_lc_noconfirm`), and a claimer refused after its claim leaves a `gc_fenced` body, not a clean farewell (`sab_lc_refusedclean`). Re-add replaces the farewell; deleting it is red (`keep_main`).
- **Round 5 (spec r3): green** (`r5_*`). Every run rewrites its own tombstone (`sab_r5_keeptomb` red), drains run on the admin's lease (`sab_r5_drainopen` red), re-add is one owner write and a new uuid reclaims the old life's clean farewell. Not shown load-bearing: the re-add clean-farewell check and `ForeignOwner` for a foreign `gc_fenced` body. Residual: a drain request in flight lands after a re-add, even without a fence or a re-run (`res_r5_inflight_norerun`).
- **Round 6 (spec r4/r5, rejoin): green with a verified mark found here** (`r6_*`). Every r4 guard has a red sabotage: foreign `gc_fenced` reclaim, recheck of a foreign owner, rollback only of an introduced retirement on the control plane, the owner uuid check, the terminal-mount check of rejoin. The r3 rollback undoes a completed retirement; without the verified mark, a re-run cannot tell a dead run's late tombstone from a completed retirement (`sab_r6_noverify`).
- **Round 7 (spec r7): green with three guards found here** (`r7_*`): a failed admin remount still runs step 3 (`sab_r7_remountstops`), a foreign reclaim needs an owner read of our uuid (`sab_r7_foreignnoowner`), and an owner rejoin generation checked on remount (`r7_remount` red without it). The kept verified mark closes codex final 3 (`sab_r7_dropmark`).
- **Round 8 (spec r8): green with one owner-life rule in place of three round-7 rules** (`r8_*`); every r8 sabotage is red. Switches `RemountChecksLife`, `RemountRejoins` and `RejoinVerifiedOnly` are removed.
- **Round 4 residuals:** a paused admin's drain landing after a re-run and a re-add (`res_lc_pauseddrain`), and a tombstone left over a live successor when the tool dies between the tombstone and its un-tombstone (`res_lc_tombover`, availability only).

## Configs {#configs}

Wall time on 4 workers; the final run shared the host with other jobs (load up to 39), so seconds are upper bounds. "States" = distinct states; for violations the count varies with worker scheduling. Final battery (`SLOW=1`, 91 rows) on the committed model.

| Config | What it is | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `cur_single` | one instance, 3 starts, 1 ambiguous write, sweep | all three non-decom | green | green | 107,473 | 5 |
| `cur_twin` | 2 same-uuid instances, 1 ambiguous write, S3 | `EpochNeverReused` | violation | violation | 1,069 | 2 |
| `cur_twin_gcs` | same, GCS generations | `EpochNeverReused` | violation | violation | 1,039 | 2 |
| `cur_twin_dangle` | twins + sweep, strict claim (#2512) | `NoDangle`, `SlotNeverRegresses` | green | green | 33,364 | 3 |
| `base_twin_dangle` | twins + sweep, `d76b59f9295` refresh of own pair | `NoDangle` | violation | violation | 35,870 | 3 |
| `cur_decom` | victim restarting around the current tail | `EpochNeverReused` | violation | violation | 3,590 | 2 |
| `cur_decom_gcs` | same, GCS | `EpochNeverReused` | violation | violation | 3,080 | 2 |
| `cur_decom_dangle` | current tail + sweep | `NoDangle` | violation | violation | 43,358 | 4 |
| `cur_decom_reusedangle` | current tail + sweep, dangle after a re-mint | `NoDangleAfterReuse` | violation | violation | 64,846 | 4 |
| `cur_decom_slot` | current tail | `SlotNeverRegresses` | violation | violation | 2,354 | 2 |
| `cur_decom_retired` | current tail | `RetiredStaysRetired` | violation | violation | 5,576 | 2 |
| `fix_nonce` | twins, nonce compared in resolve | 3 non-decom | green | green | 96,460 | 5 |
| `fix_nonce_gcs` | same, GCS | 3 non-decom | green | green | 92,774 | 5 |
| `fix_nonce_restart` (SLOW) | twins, 2 starts each, `EMax = 5` | 3 non-decom | green | green | 173,650,846 | 689 |
| `sab_nonce` | nonce in body, resolve compares only the value | `EpochNeverReused` | violation | violation | 869 | 2 |
| `fix_retire` | retire `epoch` by `If-Match`, keep `mount`, 3 victim starts | all four | green | green | 1,122,982 | 22 |
| `fix_retire_gcs` | same, GCS | all four | green | green | 1,122,982 | 22 |
| `sab_retire` | retired body written, allocation ignores it (26.6.4 binary) | `RetiredStaysRetired` | violation | violation | 32,022 | 3 |
| `sab_retire_epoch` | same sabotage | `EpochNeverReused`, `SlotNeverRegresses` | green | green | 1,367,509 | 29 |
| `sab_retire_delmount` | retire, then delete the farewell `mount` | `SlotNeverRegresses` | violation | violation | 39,933 | 3 |
| `fix_retire_twin` | retire only, twins + ambiguous write | `EpochNeverReused` | violation | violation | 2,941 | 3 |
| `fix_both` | nonce + retire, twins, decommission, sweep | all four | green | green | 66,573,697 | 1011 |

`sab_retire_epoch` is green by design: a binary that ignores the retired marker still continues the counter, so epochs stay unique. The marker guards against resumption of a retired root (`sab_retire`), not uniqueness. Uniqueness after decommission comes from not deleting the counter.

## Round 2: re-created counter above every earlier epoch {#round-2}

Today's tail is unchanged. All rows have the nonce on, one victim instance `a` with two starts, the decommission tool, and one late sweep round that may read its floor from the farewell and delete after the re-creation.

### Variants and switches {#round-2-switches}

| Switch | Meaning | Code |
|---|---|---|
| V1 `StoreTime` | an absent counter restarts at `store_time * Scale + 1` (`Scale` stands for `2^20`) | `allocateWriterEpoch` absent branch |
| V1 `TimeAfterAbsent` | sample from a probe written after the absent read; FALSE = the open-time `runCapabilityProbe` | `Pool::open` |
| V1 `TailTakesASecond` | assumption: `epoch` is deleted at a later store second than it was created | — |
| V1 `ClockBack` | sabotage: the store clock steps back once | — |
| V2 `PoolHwm` | the tail raises `_pool_meta.hwm` to the captured `next_writer_epoch` by `If-Match` RMW before deleting `epoch`; an absent counter restarts at `max(hwm + 1, 1)` | `decommissionPoolMember`, `allocateWriterEpoch` |
| V2 `HwmFresh` | GET `_pool_meta` after the absent read; FALSE = the value `PoolMeta::createOrValidate` read at open | `allocateWriterEpoch` |
| V2 `HwmAfterDelete` | sabotage: the raise runs after the epoch delete | — |
| V2 `OldAdmitDropsHwm` | a 26.6.4 node's `admitOrValidate` rewrites `_pool_meta` without the field | `Pool/CasPoolMeta.cpp:admitOrValidate` |
| `MintChecksCounter` | proposed: before minting over an absent slot, `claimMount` re-reads `epoch` and mints only if its own allocation bytes (nonce) are still there | `claimMount` |
| `NoZombie` | assumption: no counter create (mount mint) decided before the tail's DELETE of `epoch` (`mount`) lands after it | — |

### Matrix {#round-2-matrix}

Suffix: `e` `EpochNeverReused`, `d` `NoDangle`, `s` `SlotNeverRegresses`, `r` `NoDangleAfterReuse`, `c` `CounterNeverRegresses`. One TLC run per cell; G = green, R = violated.

| Row | Variant | e | d | s | r | c | states of a green cell (s) |
|---|---|---|---|---|---|---|---|
| `cur_nz` | today, `NoZombie` | R | R | R | R | R | — |
| `fix_storetime` | V1, open-time sample | R | R | R | R | R | — |
| `fix_storetime_fresh` | V1, fresh sample | R | R | R | R | R | — |
| `fix_storetime_fresh_nz` | V1, fresh, `NoZombie` | R | R | R | R | R | — |
| `fix_storetime_nz_slow` | V1, open-time, `NoZombie`, `TailTakesASecond` | R | R | R | R | R | — |
| `fix_storetime_fresh_nz_slow` | V1, fresh, `NoZombie`, `TailTakesASecond` | G | R | R | G | G | 17,274,273 (58) |
| `fix_storetime_fresh_nz_slow_mc` | V1, fresh, `NoZombie`, `TailTakesASecond`, mint check | G | G | G | G | G | 15,835,915 (53) |
| `fix_poolhwm` | V2, open-time read | R | R | R | R | R | — |
| `fix_poolhwm_fresh` | V2, fresh read | R | R | R | R | R | — |
| `fix_poolhwm_nz` | V2, open-time read, `NoZombie` | R | R | R | R | R | — |
| `fix_poolhwm_fresh_nz` | V2, fresh read, `NoZombie` | G | R | R | G | G | 2,544,206 (9) |
| `fix_poolhwm_fresh_mc` | V2, fresh read, mint check (zombies allowed) | R | R | R | R | R | — |
| `fix_poolhwm_fresh_nz_mc` | V2, fresh read, `NoZombie`, mint check | G | G | G | G | G | 2,116,265 (9) |

Sabotages and the combined row:

| Row | What | Invariant | Result | States | s |
|---|---|---|---|---|---|
| `sab_storetime_back` | V1 best row + one backward clock step | CounterNeverRegresses | violation | 2,131,428 | 8 |
| `sab_poolhwm_afterdelete` | V2 best row, raise after the epoch delete | CounterNeverRegresses | violation | 42,593 | 1 |
| `sab_poolhwm_oldadmit` | V2 best row, a 26.6.4 admission drops `hwm` | CounterNeverRegresses | violation | 110,767 | 1 |
| `fix_poolhwm_twins` | V2 best row with twins `a`, `b` and one ambiguous write (SLOW) | e, s, c | green | 18,051,105 | 83 |

### What breaks each row {#round-2-traces}

- **Zombie create (`fix_storetime_*`, `fix_poolhwm_*`, `fix_poolhwm_fresh_mc_*` without `NoZombie`).** a and the tool both read `epoch` absent; the tool's create lands, a's is decided but not sent. The tool runs the whole tail (with V2 it raises `hwm` first) and deletes `epoch`. a's `If-None-Match` lands with the value it computed before: a gets epoch 1 again. Neither variant can see a request decided before the raise.
- **Open-time sample (`fix_storetime_nz_slow_*`, `fix_poolhwm_nz_*`).** a opens (probe time 0, `hwm = 0`), reads `epoch` absent, then stalls. The tool creates, runs the tail, raises `hwm` to 2 (or the clock ticks), deletes. a decides from its open-time sample: epoch 1 again. No request is in flight across a DELETE here; the sample is just old. A fresh GET of `_pool_meta` (or a fresh probe) after the absent read closes it.
- **Same store second (`fix_storetime_fresh_nz_*`).** No clock tick between the old counter's creation and the new sample: both start at `0 * Scale + 1`. V1 needs strict store-time progress between the old creation and the deletion (`TailTakesASecond`). Real decommissions take minutes, but nothing in the store enforces it.
- **Clock step back (`sab_storetime_back`).** With every other assumption on, one backward step of the store clock re-mints at or below an old value. S3 frontends and GCS give no cross-request monotonicity guarantee for `Last-Modified`.
- **Slow claimer (`*_nz_s`, `*_nz_d` without `MintChecksCounter`).** a allocates epoch 1 and stalls before `claimMount`'s GET. The tool allocates 2, mints, runs the tail, deletes `mount`. a reads the slot absent and mints `(uuid, 1)`: the slot regresses, and the late sweep (floor `(2, …)`) deletes a's manifest. This is not a pending request across a DELETE: a decides after the delete, on an old allocation. `MintChecksCounter` closes it: the counter no longer holds a's bytes, so a refuses and restarts.
- **Raise after delete (`sab_poolhwm_afterdelete`).** A restart between the delete and the raise reads the old `hwm` and re-mints low.
- **26.6.4 admission (`sab_poolhwm_oldadmit`).** A 26.6.4 node admitting a new hash algorithm re-encodes `_pool_meta` from its struct and drops `hwm`; the next re-creation starts at 1.

### Round 2 candidates {#round-2-candidates}

| Candidate | Holds (under `NoZombie`) | Needs | Cost |
|---|---|---|---|
| V1 store time, fresh sample, mint check | all five | store clock monotone across requests; counter deleted ≥ 1 s after creation | absent-counter path: PUT probe + HEAD (`Last-Modified`); mint over absent slot: +1 GET `epoch`; `Backend::RawMeta` gains `last_modified` |
| V2 pool hwm, fresh read, mint check | all five, also with twins + ambiguous write (`fix_poolhwm_twins`) | no 26.6.4 node admits a hash algorithm after a decommission (`sab_poolhwm_oldadmit`) | tail: +1 GET + 1 `If-Match` PUT of `_pool_meta` before `deleteSlotObject(epoch_key)`; absent-counter path: +1 GET `_pool_meta`; mint over absent slot: +1 GET `epoch` |
| Either, open-time sample | none | — | rejected |
| Either, without mint check | e, r, c only | — | the slot can regress |

**Recommendation: V2 with the fresh read and the mint check, plus the nonce.** It needs no clock. The residual is the zombie class, which both variants share.

## Round 3: floor in the owner tombstone (rejected) {#round-3}

The counter is still deleted. The tail tombstones `owner` first, with `epoch_floor` = the captured `next_writer_epoch`, then deletes `mount` and `epoch`. An absent counter restarts at `max(floor + 1, 1)` from a fresh `GET owner`, which also refuses a tombstone. Revival clears `retired_at_ms` and keeps the floor. Nonce on, pre-mint check on.

### Switches {#round-3-switches}

| Switch | Meaning |
|---|---|
| `OwnerFloor` | the order and the floor above |
| `TombAfterDelete` | sabotage: the floor tombstone after the deletes (today's order) |
| `FloorStale` | sabotage: the base uses the floor `claimOwnerOrThrow` read |
| `FloorIgnoresTomb` | sabotage: the fresh `GET owner` does not refuse a tombstone |
| `UntombOnMove` | S11 step: `GET epoch` after the tombstone; moved → un-tombstone by `If-Match` on our tombstone, stop |
| `UntombOnDelFail` | also un-tombstone when a delete after the tombstone finds its token moved |
| `Revive`, `ReviveDeletes`, `LateInst` | operator clears the tombstone (or deletes `owner`); `LateInst` start only after it, as fresh members |
| `DecomRetry` | the tool dies at any step and is run again once; it accepts its own tombstone |
| `TombAmbig` | the tombstone write stays unresolved; the tool stops and the PUT lands later |
| `KeepSlot` | variant: stop after the tombstone, delete nothing |
| `NoZombieCounter` | only the counter half of `NoZombie` |

### Results {#round-3-results}

"All eight": `EpochNeverReused`, `NoDangle`, `SlotNeverRegresses`, `NoDangleAfterReuse`, `CounterNeverRegresses`, `RetiredOwnerHasNoLiveMount`, `RetiredAtRest`, `RevivalAboveEarlierLives`. "Data six": the same without the two `Retired*`.

| Config | What | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `cur_revive` | today's tail, hand-cleared tombstone, fresh member `n` only, no GC | RevivalAboveEarlierLives | violation | violation `RevivalAboveEarlierLives` | 30 | 1 |
| `fix_ownerfloor` | owner floor, both un-tombstone steps, revival + fresh member, `NoZombie` (SLOW) | all eight | green | green | 59,653,115 | 397 |
| `fix_ownerfloor_nomove` | same without the post-tombstone `GET epoch` (SLOW) | all eight | green | green | 51,678,784 | 337 |
| `fix_ownerfloor_retry` | one instance, tool re-run after a death at any step (SLOW) | data six | green | green | 69,084,519 | 362 |
| `fix_ownerfloor_ambig` | one instance, unresolved tombstone landing late | data six | green | green | 1,930,965 | 11 |
| `sab_ownerfloor_nodelfail` | no un-tombstone on a delete mismatch | RetiredOwnerHasNoLiveMount | violation | violation `RetiredOwnerHasNoLiveMount` | 42,452 | 2 |
| `sab_ownerfloor_tombafter` | floor tombstone after the deletes | EpochNeverReused | violation | violation `EpochNeverReused` | 27,560 | 1 |
| `sab_ownerfloor_stale` | base from the owner-check floor | EpochNeverReused | violation | violation `EpochNeverReused` | 58,080 | 1 |
| `sab_ownerfloor_ignore` | fresh owner read ignores the tombstone | RetiredOwnerHasNoLiveMount | violation | violation `RetiredOwnerHasNoLiveMount` | 216,336 | 3 |
| `sab_ownerfloor_nomc` | no pre-mint check | SlotNeverRegresses | violation | violation `SlotNeverRegresses` | 34,025 | 1 |
| `res_ownerfloor_zombie` | zombie creates and mints allowed | SlotNeverRegresses | violation | violation `SlotNeverRegresses` | 48,592 | 1 |
| `res_ownerfloor_zmint` | only zombie mints allowed | SlotNeverRegresses | violation | violation `SlotNeverRegresses` | 48,046 | 1 |
| `res_ownerfloor_hostile` | revival deletes `owner` | EpochNeverReused | violation | violation `EpochNeverReused` | 227,780 | 3 |
| `res_ownerfloor_keepslot` | `KeepSlot`: stop after the tombstone | RetiredOwnerHasNoLiveMount | violation | violation `RetiredOwnerHasNoLiveMount` | 30,210 | 1 |
| `res_ownerfloor_tooldeath` | tool re-run, tool dies after the tombstone | RetiredOwnerHasNoLiveMount | violation | violation `RetiredOwnerHasNoLiveMount` | 441,444 | 4 |

### Findings {#round-3-findings}

- **Green only without zombies.** The floor closes the restart re-mint (R3) and revival (`RevivalAboveEarlierLives`, with a fresh member `n` after the revival), but a create or mint decided before a DELETE and landing after it stays red, as in round 2.
- **The pre-mint check does not cover the mint half.** `res_ownerfloor_zmint` allows only delayed mints: a reads `mount` absent at genesis, passes its counter check, and pauses before its `If-None-Match`. The tool allocates, mints, tombstones, deletes `mount`. a's mint lands `(uuid, 1)` below the tool's epoch. The check ran before the tool allocated, so it saw a's own bytes.
- **The S11 step as specified is not enough.** In `sab_ownerfloor_nodelfail`, a passed the owner check before the tombstone and has its counter write in flight. The tool tombstones, re-reads `epoch` (unchanged), and then a's write lands. a reclaims the farewell. The tool's `DELETE mount` fails on the token and the tool stops: owner tombstoned, a live. Un-tombstoning on any delete mismatch closes it, and makes the `GET epoch` step redundant (`fix_ownerfloor_nomove` is green).
- **A tool death between the tombstone and its checks** leaves the same state (`res_ownerfloor_tooldeath`): a rerun refuses on a's newer epoch. This is R2 with a crash in place of a race.
- **`KeepSlot` loses the detector.** Without deletes there is no token mismatch, so a successor allocating after the post-tombstone `GET epoch` runs under a tombstone (`res_ownerfloor_keepslot`).
- **Hostile revival** (`DELETE owner`) loses the floor: the next start creates the owner without one and re-mints epoch 1 (`res_ownerfloor_hostile`). Documented residual.
- **Today's protocol with a hand-cleared tombstone** re-mints below the old life (`cur_revive`).

## Round 4: decommission keeps every control object (member lifecycle) {#round-4}

Design of `docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md`, with the changes its review asked for and two more found here. Decommission never deletes `owner`, `epoch` or `mount`. `cas-readd-member` reopens a retired `srid`.

### What is modelled {#round-4-design}

| Step | Model | Switch (sabotage) |
|---|---|---|
| Admin claim, then `GET owner` and tombstone by `If-Match` (skip if already tombstoned) | `DReadOwner`, `DTombstone` | `TombLast` |
| Proposed here: `GET mount` after the tombstone; not the tool's own claim token → un-tombstone by `If-Match` on its tombstone, stop | `DConfirmClaim`, `DUntomb` | `AdminConfirmsClaim` = FALSE |
| Drain (abstract), farewell, stop. No deletes | `DDrain`, `Farewell` | `DecomDeletesEpoch` |
| Re-run after a death at any step accepts its own tombstone | `DRestart`, `AcceptsTomb` | `RetryRefusesTomb` |
| A paused earlier run: its decided tombstone, confirm and drain land after the re-run | `DRestart` with `ZombieRun`, `OwnerLateLand`, `ZombieConfirm`, `ZombieUntomb`, `ZombieDrain` | — |
| GC fences the admin's lease; the admin keeps running | `GcFence` | — |
| `GET owner` after a landed claim (`mountWritable`, `tryRemountOnce`); tombstoned or foreign → terminal body, throw | `ClaimPostOwner`, `ClaimPostFarewell` | `ClaimRecheckOwner` = FALSE |
| Proposed here: that terminal body is `gc_fenced`, not a clean farewell | `ClaimPostFarewell` | `RefusedFarewellFences` = FALSE |
| Every owner write carries a nonce (S3 tokens otherwise repeat for equal bodies) | `OTok` | `OwnerNonce` = FALSE |
| Re-add: `GET owner` tombstoned; subtree empty; `GET mount` clean only; new uuid → replace `mount` by `If-Match` with a clean farewell of the new uuid (same uuid: keep it); `PUT owner` by `If-Match` | `RvStart`, `RvEmpty`, `RvReadMount`, `RvDelMount`, `RvReadOwner`, `RvWriteOwner` | `ReaddAcceptsFenced`, `ReviveRewritesMount` = FALSE (delete), `ReviveOwnerFirst` |

Base of every `lc_*` row: nonce on, pre-mint check on, `NoZombie` off (zombie creates and mints allowed), `EMax = 6`, one start of victim `a`, a fresh member `n` that starts only after a re-add (`LateInst`), one re-add run, one decommission re-run.

### Results {#round-4-results}

"LC": `EpochNeverReused`, `NoDangle`, `SlotNeverRegresses`, `CounterNeverRegresses`, `LiveMatchesOwnerAtRest`, `RevivalAboveEarlierLives`, `RetryAcceptsTomb`, `NoStaleTombstone`. Earlier attempts at larger bounds (`lc_core` and `lc_fence` with two victim starts, `lc_race` with the victim and the re-run) hit the 600 s limit without a violation and were shrunk.

| Config | What | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `keep_main` | spec v1: re-add deletes `mount`, pre-write owner check | SlotNeverRegresses | violation | violation `SlotNeverRegresses` | 218,719 | 2 |
| `lc_core` | lifecycle, no GC fence, no paused run | LC + NoStaleDrain + strict LiveMatchesOwner, NoTombOverLive | green | green | 14,283,637 | 94 |
| `lc_fence` | GC may fence the admin (SLOW) | LC + NoStaleDrainWithoutRerun | green | green | 33,469,377 | 221 |
| `lc_zombie` | paused earlier run (SLOW) | LC + NoStaleDrainWithoutRerun | green | green | 23,256,349 | 154 |
| `lc_newuuid` | re-add with a new uuid, `n` carries it | LC + NoStaleDrain | green | green | 2,339,098 | 18 |
| `lc_race` | two re-adds, no re-run, no GC | LC + NoStaleDrain | green | green | 119,326 | 2 |
| `lc_twins` | twins `a`, `b`, one ambiguous write, no re-add | e, s, c, LiveMatchesOwner | green | green | 3,294,847 | 23 |
| `lc_nomc` | pre-mint check off | LC + NoStaleDrain | green | green | 14,151,699 | 93 |
| `lc_retry_live` | tool alone, fair, one death anywhere | RetryCompletes | green | green | 99 | 0 |
| `lc_retry_witness` | same | RetryNeverCompletes (witness) | violation | violation `RetryNeverCompletes` | 94 | 1 |
| `sab_lc_norecheck` | no post-claim owner re-read | LiveMatchesOwnerAtRest | violation | violation `LiveMatchesOwnerAtRest` | 17,970 | 0 |
| `sab_lc_noconfirm` | no claim confirm after the tombstone | NoStaleDrainWithoutRerun | violation | violation `NoStaleDrainWithoutRerun` | 42,366 | 1 |
| `sab_lc_refusedclean` | refused claimer writes a clean farewell | NoStaleDrainWithoutRerun | violation | violation `NoStaleDrainWithoutRerun` | 1,657,914 | 11 |
| `sab_lc_fenced` | re-add accepts `gc_fenced` | NoStaleDrainWithoutRerun | violation | violation `NoStaleDrainWithoutRerun` | 289,672 | 3 |
| `sab_lc_nononce` | no owner nonce, paused run | NoStaleTombstone | violation | violation `NoStaleTombstone` | 268,143 | 3 |
| `sab_lc_deletemount` | re-add deletes `mount` | SlotNeverRegresses | violation | violation `SlotNeverRegresses` | 56,303 | 1 |
| `sab_lc_tomblast` | tombstone after the farewell | LiveMatchesOwnerAtRest | violation | violation `LiveMatchesOwnerAtRest` | 15,400 | 0 |
| `sab_lc_delepoch` | tail deletes `epoch` | EpochNeverReused | violation | violation `EpochNeverReused` | 28,057 | 1 |
| `sab_lc_ownerfirst` | re-add writes `owner` first, new uuid (SLOW) | LC + NoStaleDrain | green | green | 17,122,472 | 133 |
| `sab_lc_retryrefuses` | re-run refuses its tombstone | RetryAcceptsTomb | violation | violation `RetryAcceptsTomb` | 2,362 | 1 |
| `res_lc_pauseddrain` | paused run, fresh member only | NoStaleDrain | violation | violation `NoStaleDrain` | 2,215 | 0 |
| `res_lc_tombover` | GC fence, strict invariant | LiveMatchesOwner | violation | violation `LiveMatchesOwner` | 15,224 | 1 |

### Findings {#round-4-findings}

- **Spec v1 re-add (delete `mount`) is red for zombies** (`keep_main`, `sab_lc_deletemount`). a reads `mount` absent at genesis and pauses before its mint. The tool claims, tombstones, drains and writes its farewell; re-add deletes the farewell; a's mint lands `(uuid, 1)` below the farewell's epoch. Replacing the farewell instead keeps the slot present, and every late `If-None-Match` fails.
- **Post-claim owner re-read is needed** (`sab_lc_norecheck`). a passes the owner check before the tombstone, waits on the admin's lease, and reclaims the clean farewell after the tail. Without the re-read a runs live under a tombstone.
- **New: a GC fence before the tombstone defeats D2** (`sab_lc_noconfirm`). GC fences the admin between its owner read and its tombstone. a reclaims through the `gc_fenced` arm, re-reads the owner (not yet tombstoned) and goes live. The still-running admin tombstones, then drains a's namespace. The confirm step sees the fence's token, un-tombstones and stops before the drain. The un-tombstone leaves a short window with a live a under a tombstone (`res_lc_tombover`).
- **New: the refused claimer's clean farewell lets re-add run under a draining admin** (`sab_lc_refusedclean`). GC fences the admin after its confirm; the admin keeps draining. a reclaims, sees the tombstone, writes a clean farewell and throws. Re-add accepts that farewell, the fresh member n starts live and publishes, and the admin's drain deletes n's reference. Writing a `gc_fenced` body instead means re-add needs a decommission re-run's clean farewell.
- **Re-add over `gc_fenced`** (review 3): same harm in fewer steps (`sab_lc_fenced`).
- **Owner ETag ABA** (review 2, `sab_lc_nononce`): a run pauses with its tombstone `If-Match e0` decided; a re-run completes; a same-uuid re-add writes the original bytes back; the paused tombstone lands. With the nonce the re-add's bytes differ and the write fails.
- **Legacy 26.6.4 partial state** (review 4) is not modelled: the owner accepted it as a documented risk, and the `2^32` seed is dropped.
- **Decommission deleting `epoch`** is red (`sab_lc_delepoch`): a's genesis create, decided before the tool's, lands after the delete and is issued the tool's epoch again.
- **Tombstone last** (`sab_lc_tomblast`): a reclaims the farewell before the tombstone, its re-read passes, then the tombstone lands over it.
- **`ReviveOwnerFirst` is not load-bearing** under replace (`sab_lc_ownerfirst`, green). The order mattered only for the delete variant.
- **The pre-mint check is not needed** once nothing deletes `mount` (`lc_nomc`).
- **Retry**: `lc_retry_live` (fair tool, no other actors) proves every re-run completes; `lc_retry_witness` shows completion is reachable; `sab_lc_retryrefuses` shows today's strict owner claim refuses it.

### Residuals {#round-4-residuals}

1. **A paused admin's drain** (`res_lc_pauseddrain`). The first run confirms its claim and pauses before its drain. A re-run reclaims (the paused lease looks dead), completes, and re-add opens a new life; the fresh member publishes; the paused drain lands and deletes it. Drain deletes carry no fence the store checks, so no ordering of client steps closes this. Mitigations: re-add only after the earlier tool process has exited (an operator rule), or re-check the claim before each drain batch (narrows the window to one request).
2. **Tombstone over a live successor until the confirm un-tombstones it** (`res_lc_tombover`, strict `LiveMatchesOwner`). If the tool dies inside that window, the tombstone stays: a keeps running and is refused at its next restart. Availability only: the drain does not run.
3. **26.6.4 binaries** do not re-read the owner after a claim, and slots left half-retired by a 26.6.4 tail are accepted risk. Neither is modelled.

### Race history coverage {#round-4-history}

From `tmp/noclock/srid-revival-history.md` and the spec review.

| Race | Rows |
|---|---|
| R1 (tail deletes a successor's objects) | gone: no deletes; `sab_lc_delepoch` |
| R2 / S11 (tombstone over a successor) | `sab_lc_tomblast`, `sab_lc_norecheck`, `sab_lc_noconfirm`; residual 2 |
| S16 (ambiguous tombstone landing late) | `lc_zombie` (a paused run's tombstone lands late; confirm un-tombstones), `sab_lc_nononce` |
| Partial tail, retry over the tool's own tombstone | `lc_retry_live`, `lc_retry_witness`, `sab_lc_retryrefuses`, `DRestart` from every step |
| R3, R3a (re-mint after delete, stale floor) | no delete: `lc_core` (`EpochNeverReused`, `NoDangle`); `sab_lc_delepoch` |
| R4 (slow claimer into an emptied slot) | `keep_main`, `sab_lc_deletemount` |
| Zombie creates and mints | allowed in every `lc_*` row |
| R5 (twins) | `lc_twins` (nonce, one ambiguous write) |
| Review 1, 2, 3, 5 | `sab_lc_norecheck`, `sab_lc_nononce`, `sab_lc_fenced`, `sab_lc_deletemount` |
| Review 4 (26.6.4 partial tail) | not modelled: accepted risk |
| Review 6 (no admin self-remount) | out of scope: the model has no remount; a fenced admin either stops at the confirm or keeps draining (`GcFence`) |
| R3b, R3c (S8 zombie key reuse, S4/S9 ETag ABA on manifests and counter) | out of scope: one build per process; the counter carries the nonce |
| R6 (late probe under `roots/<srid>/`) | out of scope: re-add's emptiness check is a guard here; a non-empty subtree only refuses |
| R7 (operator deletes `owner`) | round 3 `res_ownerfloor_hostile`; not in round 4 |

### Cost in the code {#round-4-cost}

| Function | Change | Requests |
|---|---|---|
| `Tools/CasDecommission.cpp:decommissionPoolMember` | tombstone right after the claim; `GET mount` confirm; un-tombstone on mismatch; deletes, captures and recheck removed | +1 GET, +1 PUT only on a mismatch; −2 DELETE, −4 GET (two captures, two recheck reads) |
| `Pool/CasServerRoot.cpp:claimOwnerOrThrow` | `DecommissionAdmin` mode accepts the tool's tombstone | — |
| `Pool/CasPool.cpp`: `Pool::mountWritable`, `Pool::tryRemountOnce` | `GET owner` after a landed claim; refused → `gc_fenced` body by `If-Match`, throw. `openForDecommission` also calls `mountWritable`: the admin accepts its own tombstone (the model skips the re-read for the tool) | +1 GET per claim |
| `Formats`: `encodeOwner` | random nonce on every owner write | — |
| `cas-readd-member` (new) | `GET owner`, subtree LIST, `GET mount` (clean only), `PUT mount` (new uuid only), `GET owner`, `PUT owner` | 6 requests once |

## Round 5: lifecycle spec r3 {#round-5}

Changes of `docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md` r3 (section "Model"), on top of round 4. Base of every `r5_*` row: the round-4 lifecycle with `ZombieRun` and `GcFences` both on, victim `a` with one start, fresh member `n` after a re-add, one decommission re-run, one GC round.

### What changed {#round-5-design}

| Step | Model | Switch (sabotage) |
|---|---|---|
| Every run rewrites the tombstone by `If-Match` with a fresh nonce, also over an existing one; un-tombstone only by `If-Match` on its own tombstone | `DReadOwner` → `DTombstone` always | `TombAlways` = FALSE (`sab_r5_keeptomb`) |
| Verify after the farewell: `GET mount` (own clean farewell), `GET owner` (its own tombstone) → done | `DVerifyMount`, `DVerifyOwner` | `VerifyRetired` |
| Re-add: `GET owner` (retired, keeps the ETag), `GET epoch` (exists), `GET mount` (clean farewell), emptiness, one `If-Match` owner write; `mount` untouched | `RvStart`, `RvReadEpoch`, `RvReadMount`, `RvEmpty`, `RvWriteOwner` | `ReaddOneWrite`; `ReaddSkipsClean` |
| `claimMount` reclaims a clean farewell of another uuid; live or `gc_fenced` foreign bodies stay `ForeignOwner` | `ClaimRead` | `ClaimForeignClean`; `ClaimForeignFenced` |
| Two re-adds with different target uuids | `RvUuid` | `RaceUuids` |
| Drains run on the admin's lease: no drain step once its claim is gone or fenced; a request sent before can land late | `DDrain`, `ZombieDrain`, `LateDelete` | `DrainFenced` = FALSE (`sab_r5_drainopen`); `DrainInFlight` |
| The admin refuses an absent counter | `AllocRead` | `AdminRefusesAbsent` |

New invariant `DoneMeansRetired`: once the tool verified its end, the owner stays retired until a re-add opens a new life. Witness `SecondUuidNeverLive`: expected violated when a new-uuid member can start.

### Results {#round-5-results}

"I": `EpochNeverReused`, `NoDangle`, `SlotNeverRegresses`, `CounterNeverRegresses`, `LiveMatchesOwnerAtRest`, `RevivalAboveEarlierLives`, `RetryAcceptsTomb`, `NoStaleTombstone`, `DoneMeansRetired`. `r5_core` with two victim starts hit the 600 s limit at 80,091,528 states without a violation.

| Config | What | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `r5_core` | round-5 base | I + NoStaleDrain | green | green | 1,540,470 | 11 |
| `r5_newuuid` | re-add with a new uuid, `n` carries it | I + NoStaleDrain | green | green | 1,456,862 | 12 |
| `r5_newuuid_witness` | same | SecondUuidNeverLive (witness) | violation | violation `SecondUuidNeverLive` | 157,209 | 2 |
| `sab_r5_noforeign` | same, no foreign-clean reclaim | SecondUuidNeverLive (witness) | green | green | 1,087,953 | 8 |
| `r5_race` | two re-adds, different uuids (SLOW) | I + NoStaleDrain | green | green | 16,949,909 | 134 |
| `r5_twins` | twins `a`, `b`, one ambiguous write, no re-add, no fence, no paused run | e, s, c, LiveMatchesOwner | green | green | 1,160,296 | 9 |
| `r5_inflight` | drain requests may land late | I | green | green | 4,168,309 | 32 |
| `r5_pauseddrain` | `res_lc_pauseddrain` bounds, drains on the lease, counter rule off | NoStaleDrain | green | green | 4,514 | 1 |
| `r5_retry_live` | tool alone, fair, one death anywhere, counter rule off | RetryCompletes | green | green | 115 | 0 |
| `r5_retry_witness` | same | RetryNeverCompletes (witness) | violation | violation `RetryNeverCompletes` | 107 | 1 |
| `sab_r5_keeptomb` | re-run keeps an existing tombstone | DoneMeansRetired | violation | violation `DoneMeansRetired` | 118,285 | 2 |
| `sab_r5_drainopen` | drains not gated by the lease | NoStaleDrain | violation | violation `NoStaleDrain` | 1,090,870 | 8 |
| `sab_r5_foreignfenced` | `claimMount` reclaims a foreign `gc_fenced` body, new uuid | I + NoStaleDrain | green | green | 1,519,458 | 12 |
| `sab_r5_skipclean` | re-add skips the clean-farewell check | I + NoStaleDrain | green | green | 9,658,825 | 77 |
| `res_r5_inflight` | drain requests may land late | NoStaleDrain | violation | violation `NoStaleDrain` | 524,145 | 4 |
| `res_r5_inflight_norerun` | same | NoStaleDrainWithoutRerun | violation | violation `NoStaleDrainWithoutRerun` | 520,832 | 4 |
| `res_r5_pauseddrain_inflight` | `r5_pauseddrain` with late drain requests | NoStaleDrain | violation | violation `NoStaleDrain` | 3,835 | 1 |

### Findings {#round-5-findings}

- **Own tombstone per run closes the paused un-tombstone** (`sab_r5_keeptomb`). Run 1 tombstones and pauses before its confirm. The re-run finds the owner retired and keeps that tombstone, claims, drains, writes its farewell and verifies done. Run 1 wakes, its confirm sees another claim, and its un-tombstone by `If-Match` on its own tombstone lands: the owner is live after a verified decommission. With `TombAlways` the re-run's rewrite changes the token, and the late un-tombstone fails (`r5_core`).
- **Fenced drains close the paused-run drain.** `res_lc_pauseddrain` (round 4) is red; the same bounds with drains on the lease are green (`r5_pauseddrain`). With drains open, the round-5 base is red too (`sab_r5_drainopen`, same trace).
- **A drain request already sent stays a residual, and needs neither a fence nor a re-run** (`res_r5_inflight_norerun`). The admin sends a drain delete, the response is lost and the tool goes on: it writes its farewell, and the response-less request is still in flight. Re-add sees a clean farewell, the fresh member starts, publishes, and the late delete removes its reference. An operator rule that waits for the tool process to exit does not bound when the store applies a request already sent.
- **Re-add with a new uuid needs the foreign-clean reclaim.** With it, `n` (uuid 2) goes live (`r5_newuuid_witness` red). Without it, `n` never does (`sab_r5_noforeign` green): the slot keeps the old life's farewell and every claim is `ForeignOwner`.
- **Not load-bearing in the model:** the clean-farewell precondition of re-add (`sab_r5_skipclean`) and `ForeignOwner` for a foreign `gc_fenced` body (`sab_r5_foreignfenced`) are green with all of "I" and `NoStaleDrain`. The model has no renewals, no local lease deadline and no self-remount, so it cannot show what these two guards protect.
- **Two racing re-adds with different uuids** serialize on the owner `If-Match` (`r5_race`). The slot is never rewritten, so the wedge the spec r2 review found (owner of one uuid, mount of the other) cannot happen.
- **Retry still completes** with the always-rewritten tombstone and the verify step (`r5_retry_live`; `AdminRefusesAbsent` off there, since the tool alone never finds a counter).

### Residuals {#round-5-residuals}

1. **A drain request in flight when the run ends or loses its lease** (`res_r5_inflight`, `res_r5_inflight_norerun`, `res_r5_pauseddrain_inflight`). Lands after a re-add and deletes a new member's object. Not closed by fencing or by waiting for the process to exit.
2. **Tombstone over a live successor until the confirm un-tombstones it** (round 4 `res_lc_tombover`), unchanged.

## Round 6: lifecycle spec r4/r5 (rejoin) {#round-6}

Changes of `docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md` r4, with r5's rejoin in place of the re-add command. Base of every `r6_*` row: round 5 plus the switches below, victim `a` (uuid 1) and a second starter `n` (uuid 1, or uuid 2 in `*_newuuid*` rows) that may start at any time, a decommission re-run, `GcFences`, `GcFencesDead`, and the admin allowed to allocate first (`AdminRefusesAbsent` off: the counter of an earlier life). Rows that drop a dimension say so.

### What changed {#round-6-design}

| Step | Model | Switch (sabotage) |
|---|---|---|
| A start over a retired owner (any uuid) rejoins in `claimOwnerOrThrow`: `GET epoch` (exists), `GET mount` (terminal: clean or `gc_fenced`), emptiness, `If-Match` owner rewrite `{own uuid, no tombstone}`; a conflict re-reads | `JoinEpoch`, `JoinMount`, `JoinEmpty`, `JoinWrite` | `Rejoin`; `RejoinAnyMount` |
| `claimMount` reclaims a foreign terminal body, clean or `gc_fenced` | `ClaimRead` | `ClaimForeignFenced` = FALSE |
| The claim recheck refuses an absent, foreign or retired owner | `ClaimPostOwner` | `RecheckRetiredOnly` |
| GC fences the expired lease of a holder that is gone, any uuid | `GcFenceDead` | `GcFencesDead` = FALSE (`res_r6_nogcdead`) |
| Roll back only a retirement this run introduced (its owner read found no retirement); the rollback runs on the control plane | `DConfirmClaim`, `ZombieConfirm`, `DUntomb` | `RollbackOnlyIfIntroduced` = FALSE; `RollbackFenced` |
| Step 2 stops on an owner of another uuid | `DReadOwner` | `TombChecksUuid` = FALSE |
| Proposed here: step 6 marks the tombstone verified (`If-Match`, fresh nonce); a re-run counts only a verified tombstone as a previous retirement | `DMarkVerified`, `DReadOwner` | `VerifiedMark` = FALSE |

`DoneMeansRetired` is now keyed on the set of lives with a verified completion (`doneLives`), not on the program counter, so a re-run cannot reset it. New invariants: `NoWedge` and `NoForeignTombstone`; witness `NoLiveAfterSelfFence`.

**Why `NoWedge` is a state invariant, not a temporal property.** It flags a state in which the owner is live with uuid U and the slot holds another uuid's body that nothing will change: no process of that uuid can still write it (each is down or before its owner check), and no claim, fence or rejoin rule applies to the body. From such a state no action changes the slot or the owner: rejoin needs a retired owner, and the tool's claim and every start of U end `ForeignOwner`. So the state is permanent, which is what "every U start ends `ForeignOwner` forever" means, and a safety check finds it without fairness assumptions.

### Results {#round-6-results}

"I6": `EpochNeverReused`, `NoDangle`, `SlotNeverRegresses`, `CounterNeverRegresses`, `LiveMatchesOwnerAtRest`, `RevivalAboveEarlierLives`, `RetryAcceptsTomb`, `NoStaleTombstone`, `DoneMeansRetired`, `NoStaleDrain`, `NoWedge`, `NoForeignTombstone`. The full base (both starters, paused run and a GC round together) hit the 600 s limit at about 78,000,000 states without a violation; the rows split it.

| Config | What | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `r6_core_z` | same uuid; `a` starts twice, no `n`; no GC round | I6 | green | green | 15,431,554 | 91 |
| `r6_core_g` | same uuid; `a` starts twice, no `n`; no paused run (SLOW) | I6 | green | green | 38,222,100 | 229 |
| `r6_newuuid_z` | `n` with uuid 2; no GC round (SLOW) | I6 | green | green | 23,717,003 | 189 |
| `r6_newuuid_g` | `n` with uuid 2; no paused run (SLOW) | I6 | green | green | 49,604,404 | 399 |
| `r6_counterrule` | `n` with uuid 2, the admin refuses an absent counter | I6 | green | green | 5,500,358 | 47 |
| `r6_newuuid_witness` | `n` with uuid 2 | `SecondUuidNeverLive` (witness) | violation | violation | 421,434 | 4 |
| `r6_selffence` | `a` starts twice, no paused run, no GC round | I6 | green | green | 2,171,183 | 13 |
| `r6_selffence_witness` | same | `NoLiveAfterSelfFence` (witness) | violation | violation | 245,947 | 2 |
| `r6_twins` | twins with one ambiguous write, no rejoin | e, s, c, LiveMatchesOwner | green | green | 5,071,012 | 38 |
| `r6_retry_live` | tool alone, fair, one death anywhere, GC quiet | RetryCompletes | green | green | 139 | 1 |
| `sab_r6_keepforeignfenced` | `claimMount` keeps `ForeignOwner` for a foreign `gc_fenced` body | NoWedge | violation | violation | 64,277 | 1 |
| `sab_r6_recheckretired` | recheck refuses only a retired owner | LiveMatchesOwnerAtRest | violation | violation | 1,703,245 | 13 |
| `sab_r6_rollbackall` | unconditional rollback | DoneMeansRetired | violation | violation | 11,573,571 | 76 |
| `sab_r6_rollbackfenced` | rollback on the tool's lease plane | LiveMatchesOwnerAtRest | violation | violation | 302,299 | 3 |
| `sab_r6_nouuidcheck` | step 2 ignores the owner's uuid | NoForeignTombstone | violation | violation | 1,442,560 | 10 |
| `sab_r6_rejoinany` | rejoin accepts a live-looking mount | LiveMatchesOwnerAtRest | violation | violation | 1,854,294 | 14 |
| `sab_r6_noverify` | no verified mark (SLOW) | LiveMatchesOwnerAtRest | violation | violation | 24,078,347 | 162 |
| `res_r6_nogcdead` | GC does not fence dead holders | NoWedge | violation | violation | 1,011,377 | 8 |

Round-5 rows re-run under the round-6 `DoneMeansRetired`: `r5_core`, `r5_newuuid`, `r5_inflight`, `r5_race`, `sab_r5_foreignfenced` and `sab_r5_skipclean` now violate it (the r3 rollback rule, see below); the other round-5 rows keep their verdicts. State counts of round-5 rows moved because the history ghosts `doneLives` and `tombForeign` split states; earlier rounds keep their counts.

### Findings {#round-6-findings}

- **The r3 rollback undoes a completed retirement** (codex r3 finding 4; `sab_r6_rollbackall`, and every round-5 row that checks `DoneMeansRetired`). A run completes and verifies. A re-run claims, rewrites the tombstone, is fenced before its confirm, and un-tombstones by `If-Match` on its own tombstone: the owner is live again without a rejoin. Rolling back only a retirement this run introduced closes it.
- **New: a re-run cannot tell a completed retirement from a dead run's late tombstone** (`sab_r6_noverify`). Run 1 sends its tombstone and pauses. A re-run claims, and GC fences the re-run's lease. The victim, which allocated above it, reclaims; its recheck passes because the tombstone has not landed yet. Run 1's tombstone then lands over the live victim. The re-run reads a retired owner and treats it as a previous retirement: it rewrites it, its confirm fails on the fence, and it correctly does not roll back. Run 1's own rollback then fails on the rewritten token. The victim runs under a tombstone and both tools stop. The proposed verified mark (step 6 rewrites the tombstone as verified) lets a re-run treat an unverified tombstone as not retired, so its rollback restores the owner; a verified retirement is still never undone (`r6_*` green). Cost: one owner PUT per completed run.
  - Rounds 5 and early round 6 missed it: with the admin refusing an absent counter and one victim start, the victim always allocated first, below the admin, so it could not wait on the admin's lease.
- **Recheck must refuse a foreign owner** (opus r3 F2 interleaving A, `sab_r6_recheckretired`). `n` (uuid 2) rejoins over the admin's fenced lease while `a` (uuid 1, owner checked before the tombstone) reclaims; `a`'s recheck sees a live owner and passes: `a` runs under uuid 2.
- **Foreign `gc_fenced` bodies must be reclaimable** (F2 interleaving B, `sab_r6_keepforeignfenced`). The dead admin's lease is GC-fenced, `n` rejoins over it, and every start of `n` ends `ForeignOwner`; the tool, uuid 1, is refused on the foreign owner.
- **Rejoin must refuse a live-looking mount** (`sab_r6_rejoinany`): `n` rejoins over `a`'s live lease, and `a` keeps running under uuid 2.
- **Rollback must run on the control plane** (codex r3 finding 3, `sab_r6_rollbackfenced`). Fenced after its claim and before its tombstone, the tool cannot un-tombstone on its lease plane, and the live successor stays under the tombstone.
- **Step 2 must check the owner's uuid** (`sab_r6_nouuidcheck`). A re-run whose owner check preceded `n`'s rejoin claims the terminal uuid-1 body and tombstones uuid 2's owner.
- **A refused victim rejoins once the data is gone** (`r6_selffence` green, its witness red). The victim reclaims right after the farewell, its recheck sees the tombstone, it self-fences and stops. On its next start it rejoins over its own `gc_fenced` body and runs.
- **GC fencing dead holders is load-bearing for liveness** (`res_r6_nogcdead`). A re-run that claims after `n` rejoined stops at the uuid check without a farewell in the model; its live-looking lease wedges `n` until GC fences it. In code the pool teardown writes a farewell on that exit, so the model is conservative here.
- **Retry with a concurrent GC fence of the dead run** can lose its reclaim race (`racedDoubleStart`) and need another re-run; `r6_retry_live` therefore runs with GC quiet.

### Residuals {#round-6-residuals}

Unchanged from round 5: a drain request in flight lands after a later life starts (`res_r5_*`); a tombstone stays over a live successor if the tool dies between its tombstone and its rollback (`res_lc_tombover`).

## Round 7: lifecycle spec r7 {#round-7}

Changes of `docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md` r7 on top of round 6, and three guards found here. Base of every `r7_*` row: round 6 plus the switches below with the proposed fixes on; rows that drop a dimension say so.

### What changed {#round-7-design}

| Step | Model | Switch (sabotage) |
|---|---|---|
| Rejoin only over a verified tombstone; an unverified one refuses (retryable) | `OwnerCheck` | `RejoinVerifiedOnly` |
| Self-remount of a process whose lease is lost (`Pool::tryRemountOnce`): owner check without rejoin, new epoch, new claim; the in-flight build fails (admission generation) | `RemountStart`, `RemountOwner` | `RemountOn`; `RemountRejoins` |
| GC fences the lease of a partitioned live instance | `GcFenceLive` | `GcFencesLive` |
| Remount rows: writes need the process's own unfenced lease; a process counts as live only while it holds one | `WriteManifest`, `Publish`, `LiveP` | — |
| Step 2 keeps `retirement_verified` when it refreshes a verified tombstone; up to three runs | `DTombstone` | `KeepVerified` = FALSE (`sab_r7_dropmark`); `DRestarts` |
| The verified mark ignores catalog GC; rejoin waits for it | `CatalogGc`, `JoinEmpty` | `DrainLeavesRows` |
| New-uuid rejoin replaces the terminal mount body (`If-Match`, same `writer_epoch`, clean) before the owner write | `JoinRepl` | `JoinReplaceMount`; `JoinOwnerFirst` |
| A 26.6.4 starter: no rejoin, no foreign reclaim, no claim recheck, claims any time | `OldInst` | — |
| Proposed here: a failed admin remount after this run's tombstone still runs the step-3 confirm and its rollback | `RemountOwner` | `AdminRemountConfirms` = FALSE (`sab_r7_remountstops`) |
| Proposed here: `claimMount` reclaims a foreign terminal body only after `GET owner` shows its own uuid, not retired | `ClaimForeignOwner` | `ForeignNeedsOwner` = FALSE (`sab_r7_foreignnoowner`) |
| Proposed here: the owner carries a rejoin generation; the remount owner check and the remounting claim's recheck refuse a changed one | `RemountOwner`, `ClaimPostOwner` | `RemountChecksLife` = FALSE (`r7_remount`) |

New invariants: `NoResumeAfterRetire` (a self-remount never makes live again a process that started before a later rejoin) and `OldNotUnderNewOwner` (a 26.6.4 starter never runs under an owner of another uuid).

### Results {#round-7-results}

"I7": "I6" plus `NoResumeAfterRetire`. `r7_old` checks "I7" with `OldNotUnderNewOwner` in place of `LiveMatchesOwnerAtRest`: a 26.6.4 same-uuid victim prechecked before the tombstone still runs under it (no recheck), which the compatibility table documents. Larger bounds than those below hit the 600 s limit without a violation.

| Config | What | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `r7_core_z` | `a` starts twice, paused run, admin fence, no GC round, no live-instance fence (SLOW) | I7 | green | green | 63,769,819 | 416 |
| `r7_core_g` | `a` starts twice, GC round, no paused run, no fences of live processes (SLOW) | I7 | green | green | 20,281,948 | 139 |
| `r7_old` | 26.6.4 starter `o` (uuid 1), new-uuid starter `n` | I7 − LiveMatchesOwnerAtRest + OldNotUnderNewOwner | green | green | 8,161,475 | 73 |
| `r7_threeruns` | up to three decommission runs, verified mark kept | I7 | green | green | 6,141,297 | 41 |
| `r7_remount_life` | partitioned `a` remounts; `n` (uuid 1) rejoins; generation check on (SLOW) | I7 | green | green | 54,965,978 | 501 |
| `r7_remount` | same, spec r7 as written (no generation check) | I7 | violation | violation `NoResumeAfterRetire` | 4,872,306 | 38 |
| `sab_r7_dropmark` | step 2 drops the verified mark, three runs | DoneMeansRetired | violation | violation `DoneMeansRetired` | 1,367,184 | 9 |
| `sab_r7_ownerfirst` | owner written before the mount replacement | OldNotUnderNewOwner | violation | violation `OldNotUnderNewOwner` | 446,436 | 4 |
| `sab_r7_foreignnoowner` | foreign reclaim without the owner read | OldNotUnderNewOwner | violation | violation `OldNotUnderNewOwner` | 1,827,480 | 15 |
| `sab_r7_remountstops` | a failed admin remount ends the run before step 3 | LiveMatchesOwnerAtRest | violation | violation `LiveMatchesOwnerAtRest` | 31,999 | 1 |
| `sab_r7_remountrejoin` | the remount may rejoin (SLOW) | NoResumeAfterRetire | — | green | 55,388,740 | 474 |
| `sab_r7_rejoinunverified` | rejoin over an unverified tombstone (SLOW) | I7 | — | green | 55,352,312 | 499 |

### Findings {#round-7-findings}

- **Kept mark closes codex final finding 3** (`sab_r7_dropmark`). D0 verifies; D1 refreshes the tombstone without the mark and dies; D2 reads an unverified tombstone, is fenced before its confirm and rolls back D0's completed retirement. Keeping the mark on refresh closes it (`r7_threeruns` green).
- **Mount replacement before the owner write protects 26.6.4 starters** (`sab_r7_ownerfirst`). Owner first: the old starter reclaims its uuid-1 farewell under owner uuid 2 and runs without a recheck.
- **New: the foreign reclaim needs an owner read** (`sab_r7_foreignnoowner`). Between the rejoin's mount replacement and its owner write, the owner is still uuid 1, retired. A new-binary uuid-1 claimer that passed its owner check before (the re-run admin, or a prechecked victim) reclaims the uuid-2 body as a foreign terminal body. It then stops (admin uuid check, victim recheck, self-fence), leaving a uuid-1 body. The 26.6.4 starter reclaims that after the owner write and runs under uuid 2. The fix: `claimMount` reclaims a foreign body only after `GET owner` shows its own uuid and no tombstone.
- **New: D5's failed remount must not skip the rollback** (`sab_r7_remountstops`). GC fences the admin between its claim and its tombstone; the victim reclaims and passes its recheck; the tombstone lands. The admin's self-remount then fails on the retired owner, and if that ends the run, step 3 never rolls back: the victim runs under a tombstone with the tool alive. The fix: a failed remount after this run's tombstone still runs step 3 on the control plane.
- **New: "no rejoin on remount" does not stop a resume in place** (`r7_remount`). A partitioned victim's remount owner check passes before the decommission; its claim lands after the farewell, and its recheck runs after a same-uuid rejoin and sees a live owner of its own uuid. It resumes with its old state. The fix: the owner carries a rejoin generation (incremented by each rejoin owner write); a process remembers it at start, and the remount owner check and the remounting claim's recheck refuse a changed one (`r7_remount_life` green). With that check, letting the remount rejoin is not harmful either (`sab_r7_remountrejoin` green), so "remount never rejoins" becomes redundant.
- **Not shown load-bearing:** rejoin only over a verified tombstone (`sab_r7_rejoinunverified` green at these bounds).
- **Model fixes found on the way:** `DRestart` kept the previous run's tombstone token, and a remounted build published under the new epoch; both reachable only with remount and corrected for remount rows.

### Residuals {#round-7-residuals}

Unchanged: the in-flight drain request (`res_r5_*`) and a tombstone left over a live successor when the tool dies before its rollback (`res_lc_tombover`). Documented compatibility case: a 26.6.4 same-uuid victim prechecked before the tombstone can run under it (no recheck).

## Round 8: one monotone owner life (spec r8) {#round-8}

Spec r8 replaces the owner nonce and the round-7 rejoin generation with one monotone `owner_life`. In the model it is `ownerGen`: every owner write (first claim, tombstone, rollback, verified mark, rejoin) adds one, and with `OwnerNonce` on the owner token is the life itself. Switch `OwnerLife`: a process records the life it started under (`pLife`, taken after its rejoin if any); the claim recheck (now also the admin's, which still accepts its victim's tombstone), the remount owner check and the foreign reclaim require exactly (own uuid, recorded life, not retired). Rejoin no longer needs a verified tombstone, and the remount has no rule of its own against rejoin: a rejoin, rollback or tombstone changes the life and the check refuses.

Base of every `r8_*` row: round 7 with `OwnerLife` on. Kept from r7: the verified mark (kept on refresh), every exit after the tombstone runs the confirm (`AdminRemountConfirms`), rollback by `If-Match` on its own tombstone unless the replaced one was verified, mount before owner for a new-uuid rejoin, and the 26.6.4 starter.

### Switches {#round-8-switches}

| Removed (dead under r8) | Was | Now |
|---|---|---|
| `RemountChecksLife` | round-7 rejoin generation, checked on remount and in the remounting recheck | the life check |
| `RemountRejoins` | sabotage: a remount may rejoin (green under the generation check) | the life check refuses a remount over a changed life; no rule needed |
| `RejoinVerifiedOnly` | rejoin only over a verified tombstone (not shown load-bearing in round 7) | dropped by r8 |

Added: `OwnerLife` (the r8 rule) and the sabotages `RecheckIgnoresLife` and `RollbackIgnoresLife`; the other r8 sabotages reuse `ForeignNeedsOwner`, `AdminRemountConfirms` and `KeepVerified`. The design rows set one rule switch (`OwnerLife`) where round 7 set two (`RemountChecksLife`, `RejoinVerifiedOnly`). The round-7 rows are removed (`ec635033c10`); their results stay in "Round 7" above, and round 8 covers each of their scenarios.

### Results {#round-8-results}

"I8": `EpochNeverReused`, `NoDangle`, `SlotNeverRegresses`, `CounterNeverRegresses`, `LiveMatchesOwnerAtRest`, `RevivalAboveEarlierLives`, `RetryAcceptsTomb`, `NoStaleTombstone`, `DoneMeansRetired`, `NoStaleDrain`, `NoWedge`, `NoForeignTombstone`, `NoResumeAfterRetire`. `r8_old` checks `OldNotUnderNewOwner` in place of `LiveMatchesOwnerAtRest` (the documented 26.6.4 same-uuid case). Larger bounds than those below hit the 600 s limit without a violation.

| Config | What | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `r8_core_z` | `a` starts twice, paused run, admin fence, no GC round, counter rule on | I8 | green | green | 12,597,853 | 83 |
| `r8_core_g` | `a` starts twice, GC round, no paused run, no fences (SLOW) | I8 | green | green | 25,629,084 | 180 |
| `r8_newuuid` | `a` and `n` (uuid 2), admin fence, no paused run, no GC round (SLOW) | I8 | green | green | 23,231,567 | 200 |
| `r8_old` | 26.6.4 starter `o`, new-uuid starter `n` (SLOW) | I8 − LiveMatchesOwnerAtRest + OldNotUnderNewOwner | green | green | 41,652,502 | 390 |
| `r8_threeruns` | up to three decommission runs | I8 | green | green | 8,168,773 | 55 |
| `r8_remount` | partitioned `a` remounts, `n` (uuid 1) rejoins (SLOW) | I8 | green | green | 56,688,908 | 509 |
| `sab_r8_rechecknolife` | recheck ignores the life | NoResumeAfterRetire | violation | violation | 2,939,908 | 22 |
| `sab_r8_rollbacknolife` | rollback clears any retired owner of the victim uuid | DoneMeansRetired | violation | violation | 179,977 | 2 |
| `sab_r8_foreignnoowner` | foreign reclaim without the owner read | OldNotUnderNewOwner | violation | violation | 1,032,264 | 9 |
| `sab_r8_remountstops` | a failed admin remount skips the confirm | LiveMatchesOwnerAtRest | violation | violation | 37,650 | 2 |
| `sab_r8_dropmark` | refresh drops the verified mark | DoneMeansRetired | violation | violation | 1,698,022 | 11 |
| `sab_r8_ownerfirst` | owner written before the mount replacement | OldNotUnderNewOwner | violation | violation | 117,232 | 2 |

### Findings {#round-8-findings}

- **The life check subsumes three round-7 rules.** It replaces the rejoin generation, the remount's "never rejoin" rule and, for the foreign reclaim, the separate "not retired" condition (any tombstone changes the life). Rejoin without a verified tombstone stays green (`r8_*`).
- **Each r8 sabotage is red.** A recheck without the life lets a remounted victim resume after a same-uuid rejoin. A rollback without its own tombstone's life lets a paused run clear a later run's completed retirement: run 1 tombstones and pauses, the re-run completes and verifies, and run 1's rollback lands.
- **No new defect** within these bounds.

## Code checks {#code-checks}

Read-only, at `2a034cdb374` and tag `v26.6.4.20001.altinityantalya` (same code at both unless noted).

- **(a) A namespace life never spans a counter re-creation.** The tail runs only when the catalog holds no victim-owned namespace (`Tools/CasDecommission.cpp:decommissionPoolMember`, `victim_owned_count`, and the fresh catalog cut must match `retirement_catalog_cut`). The absent-counter branch of `Pool/CasServerRoot.cpp:allocateWriterEpoch` refuses unless `serverRootSubtreeEmpty` holds: no catalog entry under the srid, no manifest or loose-root key. A re-created namespace gets a fresh life (`Pool/CasRefCatalog.cpp:mintFreshIncarnation`). So the one-by-one seal walk in `Pool/CasRefLedger.cpp` writer recovery (`++epoch` per dead epoch up to `live_epoch`, also for a Removed life) never crosses the jump. A jump of `2^50` inside one life would mean `2^50` GETs plus seals; nothing allows it. A brand-new srid starts its first life at the jumped value, which is harmless.
- **(b) No small-epoch assumption found.** `Primitives/CasTypes.h:renderRefTxnId`/`parseRefTxnId` use a fixed 16-hex-digit `u64` (manifest keys `Formats/CasLayout.h:manifestKey` and ref-log keys reuse it). `Formats/CasServerRootFormats.cpp` writes `next_writer_epoch` and the lease `writer_epoch` as decimal strings (`writeU64StringField`). `system.cas_mounts.writer_epoch` is `UInt64` (`Storages/System/StorageSystemContentAddressedMounts.cpp`). The only epoch sentinels are `UINT64_MAX` overflow guards (`Gc/CasOrphanManifestSweep.cpp`, `Pool/CasRefProtocol.cpp`). `min_active_build_sequence = UINT64_MAX` is a build-sequence sentinel, not an epoch. No array or loop is sized by an epoch value. `store_seconds << 20` is about `2^50.7` today, below `2^53` until about year 2255.
- **(c) Store time in one request.** `Backend/CasBackend.h:Backend::head` returns `RawMeta{size, value}` and drops the time. Below it, `Backend/CasObjectStorageBackend.cpp:nativeHead` already receives `ObjectMetadata::last_modified` (`IObjectStorage.h`) from `tryGetObjectMetadataWithNativeToken`: S3 `HeadObject` `Last-Modified`, and the same header from GCS's XML API. Resolution is one second. So: one HEAD of an object written after the absent read, with `RawMeta` extended by `last_modified`. The open-time `_probe/<uid>` is written before the absent read (`Pool/CasPool.cpp:Pool::open`, `runCapabilityProbe` precedes `allocateWriterEpoch`) and is stale by construction (`fix_storetime_nz_slow_*`). A fresh sample costs a PUT plus that HEAD. The PUT response's `Date` header would save the HEAD but is not exposed by the write path.
- **(d) `_pool_meta`.** The 26.6.4 `Formats/CasPoolMetaFormat.cpp:decodePoolMeta` builds `JsonObjectReader` with `KeyStrictness::Tolerant` and skips unknown plain keys; `2a034cdb374` skips them too. The only writer after creation is `Pool/CasPoolMeta.cpp:admitOrValidate`, a `CasOperation::readModifyWrite` `If-Match` RMW, and it writes only when admitting a new hash algorithm (`cas_blob_hash_allow_new`). A new field rides it without a race: a concurrent open and the tail serialize on the token (`PoolMetaAdmit` with a new binary is in every V2 row). But `admitOrValidate` re-encodes the decoded struct (`encodePoolMeta` writes only known fields), so a 26.6.4 node's admission **drops** the field (`sab_poolhwm_oldadmit`). The new build must carry `hwm` in `PoolMeta` so its own admissions keep it. A `!`-key or a `min_reader_generation` bump would lock 26.6.4 out of the whole pool; better to document "no hash-algorithm admission from 26.6.4 nodes after a decommission".

## Invariants {#invariants}

| Name | Meaning |
|---|---|
| `EpochNeverReused` | `allocateWriterEpoch` never returns one value twice for the uuid, across decommission (ghost `allocLog`). |
| `NoDangle` | every committed reference names a present manifest key `(epoch, seq 1)`. |
| `NoDangleAfterReuse` | `NoDangle` in runs where an epoch was re-minted; isolates the CAS-363 harm from the slot-deletion path. |
| `SlotNeverRegresses` | the mount slot never holds a `writer_epoch` below one it held before. |
| `CounterNeverRegresses` | every counter write lands above every counter value ever written (a retire keeps its value): a re-created counter starts above every earlier epoch. |
| `RetiredStaysRetired` | no instance holds the slot while `owner` is tombstoned or `epoch` is retired (S11 class, CAS-158). |
| `RetiredOwnerHasNoLiveMount` | the tool has stopped and `owner` is tombstoned ⇒ the slot holds no unclean lease. |
| `RetiredAtRest` | `RetiredStaysRetired` once the tool has stopped. |
| `RevivalAboveEarlierLives` | every allocation after a revival or re-add is above every allocation before it (ghost `allocLife`, tagged with the life at allocation time). |
| `LiveMatchesOwner` | every live instance has the owner's uuid and an untombstoned owner. |
| `LiveMatchesOwnerAtRest` | `LiveMatchesOwner` once the tool has stopped and no step of a paused earlier run is pending. |
| `NoTombOverLive` | no tombstone lands while an instance holds the slot live (CAS-158). |
| `NoStaleTombstone` | no tombstone lands in a later life than the owner read it was decided on. |
| `NoStaleDrain` | no drain removes a reference of an epoch newer than the draining run's own. |
| `NoStaleDrainWithoutRerun` | `NoStaleDrain` broken only after a decommission re-run. |
| `RetryAcceptsTomb` | a decommission re-run never refuses on the tombstone. |
| `RetryCompletes` (temporal, `FairSpec`) | a re-run after a death at any step reaches the end. |
| `RetryNeverCompletes` | witness, expected violated: a re-run can reach the end. |
| `DoneMeansRetired` | in every life with a verified decommission end (`doneLives`), the owner is retired; a rejoin or re-add opens a new life. |
| `SecondUuidNeverLive` | witness, expected violated: a member with the second uuid runs live after a re-add. |
| `NoWedge` | no state where the owner is live with uuid U and the slot holds another uuid's body that no action can change (permanent `ForeignOwner` for U). |
| `NoForeignTombstone` | no decommission tombstone lands on an owner of another uuid. |
| `NoResumeAfterRetire` | a self-remount never makes live again a process that started before a later rejoin. |
| `OldNotUnderNewOwner` | a 26.6.4 starter never runs under an owner of another uuid. |
| `NoLiveAfterSelfFence` | witness, expected violated: after a claimer fenced itself on the recheck, an instance runs live again. |

## Counterexamples {#counterexamples}

**`cur_twin` (14 states).** a reads `epoch` absent, passes the empty check and the mount probe, and sends `create {2}`; the request stays in flight and the response is lost. b does the same and its `create {2}` lands. a's resolve read sees `{2}` under a moved precondition (the key exists), bytes equal → `Committed`. Both return epoch 1. a's own request is still in flight and is refused later. On GCS the trace is the same: the generation moved, the bytes matched.

**`cur_decom` (victim restart).** a holds epoch 1 and stops. The tool allocates 2, reclaims the slot, drains, captures `epoch = {3}`, writes the farewell, reads it, deletes `mount`, deletes `epoch`. a restarts: owner is not tombstoned, `epoch` is absent, the subtree is empty, the mount probe is `KeyAbsent` → `create {2}` → epoch 1 again.

**`cur_decom_reusedangle` (27 states).** a and the tool both find `epoch` absent and decide `create {2}`. The tool's lands (it gets 1); a's is not sent yet (paused process, no ambiguity). The tool mints the slot at 1, drains, captures `{2}`, writes the farewell `(1, clean)`, deletes `mount`. GC reads the floor `(1, clean)`. The tool deletes `epoch`. a's `If-None-Match` now lands: a gets epoch 1 too, mints the slot `(uuid, 1)`, stages `(1, 1)`. GC reads the view, a publishes, GC deletes `(1, 1)` (same epoch, clean floor). The reference dangles.

**`cur_decom_dangle` (24 states, no reuse needed).** a allocates 1, reads the slot absent, and pauses before its `If-None-Match` mint. The tool allocates 2, mints the slot, writes the farewell `(2, clean)`. GC reads the floor `(2, clean)`. The tool deletes `mount`. a's mint lands: slot `(uuid, 1)`. a stages manifest `(1, 1)`, GC reads the view (no refs), a publishes, GC deletes `(1, 1)` because `1 < 2`. The reference dangles.

**`fix_retire` before `DDelMount` was dropped.** The same paused mint landed after the retiring tail deleted `mount`: a ran live with a retired owner. Keeping the farewell `mount` makes the late `If-None-Match` fail (`racedDoubleStart`).

**`base_twin_dangle`.** Twins share epoch E; both claim (the second refreshes its own pair). a stages `(E, 1)`; b writes its farewell `(E, clean)`; GC reads that floor, the view, deletes `(E, 1)`; a publishes.

## Candidates {#candidates}

| Candidate | Closes | Result | Code cost |
|---|---|---|---|
| Nonce (`fix_nonce`) | CAS-362; S4 ETag ABA | green; `sab_nonce` red | `encodeServerEpoch`/`decodeServerEpoch` + one random field drawn in `allocateWriterEpoch`'s `decide`. The resolve compares whole bytes already: nothing changes in `writeLoop`. No extra requests. |
| Retire (`fix_retire`) | CAS-363, S11 (with the existing `captures_match` check) | green; `sab_retire` red | `decommissionPoolMember`: replace both deletes and the recheck with one `op.replace(epoch_key, retired, claimed_epoch->etag)`, then the owner tombstone; do not delete `mount`. `allocateWriterEpoch`: refuse a retired body. One request fewer per decommission (3 → 1 writes before the tombstone, recheck reads gone). |
| Retire + delete `mount` | — | `sab_retire_delmount` red | do not ship. |
| Both (`fix_both`) | all modelled scenarios | green | sum of the two. |

The two fixes are independent: `fix_retire_twin` stays red without the nonce, `fix_nonce` does not touch decommission.

## Decoder compatibility (26.6.4) {#decoder-compatibility}

Both fixes are format-compatible with 26.6.4 readers if the header `v` is unchanged.
- `Formats/CasServerRootFormats.cpp:decodeServerEpoch` (tag `v26.6.4.20001.altinityantalya` and `2a034cdb374`) loops `nextKey`; unknown keys go to `JsonObjectReader::skipUnknown`. In 26.6.4 the reader is built with `KeyStrictness::Tolerant`; at `2a034cdb374` the reader has no strict mode. A plain extra field (`allocation_nonce`, `retired_at_ms`) is skipped.
- `Formats/CasTextFormat.cpp:JsonObjectReader::skipUnknown` throws `UNKNOWN_FORMAT_VERSION` for a key starting with `!` (same in 26.6.4).
- Consequence for the nonce: a 26.6.4 twin ignores the nonce and writes a body without one. A new allocator never claims such bytes; a 26.6.4 allocator against a new twin keeps the bug until upgrade.
- Consequence for the retired marker: a plain `retired_at_ms` is ignored by 26.6.4 `allocateWriterEpoch`, which then allocates (this is `sab_retire`: resumption in the window before the owner tombstone; uniqueness still holds, `sab_retire_epoch`). Spelling the marker as a critical key (`!retired_at_ms`) makes 26.6.4 throw `UNKNOWN_FORMAT_VERSION` from `decodeServerEpoch`, which `allocateWriterEpoch` propagates: the old binary fails closed. The 26.6.4 decommission tool would also throw on a retired body (`Tools/CasDecommission.cpp:decommissionPoolMember` decodes the capture), which is the wanted outcome for a re-run.

## Atomicity {#atomicity}

One action = one store request, or one in-memory step. Code at `2a034cdb374`.

| Action | Code | Request / step | Fused steps and why sound |
|---|---|---|---|
| `Start` | process start | in-memory | — |
| `OpenProbe` | `Pool/CasPool.cpp:Pool::open`: `runCapabilityProbe` (V1 time) and `PoolMeta::createOrValidate` (V2 read) | one PUT+HEAD of `_probe/<uid>` or one GET `_pool_meta` | the two samples are fused, sound because a config uses at most one of them (`ASSUME ~(StoreTime /\ PoolHwm)`) |
| `ABase` | proposed fresh sample in `allocateWriterEpoch` + `decide` | V1: PUT `_probe/<uid>/time` + HEAD; V2: GET `_pool_meta` | V1 fuses PUT and HEAD: the key is private to the process, so only the clock moves between them, and the model reads the clock at the step |
| `ClaimCheckCounter` | proposed GET `epoch` in `claimMount` before `op.create` | GET `epoch` | comparison is local |
| `DHwmRead`, `DHwmWrite` | proposed `_pool_meta` RMW in `decommissionPoolMember` (`readModifyWrite` as `admitOrValidate`) | GET; PUT `If-Match` | a conflict returns to the GET |
| `PoolMetaAdmit` | `Pool/CasPoolMeta.cpp:admitOrValidate` by a concurrent open | PUT `If-Match` `_pool_meta` | its own GET is folded: the write is unconditional on the hwm value, only the token matters to the tail |
| `Tick`, `ClockStepBack` | store clock | environment | — |
| `OwnerCheck` | `Pool/CasServerRoot.cpp:claimOwnerOrThrow` | GET `owner` | owner always present: the create branch is out of scope |
| `AllocRead` | `Backend/CasRequests.cpp:CasOperation::readModifyWrite` (observe) + `allocateWriterEpoch` `decide` | GET `epoch` | `decide` is local computation on that GET's result |
| `AllocEmpty` | `Pool/CasServerRoot.cpp:serverRootSubtreeEmpty` | LIST manifests prefix | the catalog and roots LISTs are folded: the model has one artifact; each is a read, and the result is used only as a refusal guard |
| `AllocProbe` | `allocateWriterEpoch` → `op.probeSentinel(mountKey)` + `decide` tail | HEAD `mount` | DecommissionRecovery's liveness test uses a ghost (holder process state) in place of `expires_at_ms`; `claimMount`'s own gate follows |
| `AllocWrite` | `CasOperation::writeLoop` → `backend->write` | one PUT with `If-Match`/`If-None-Match` | disjunct 1 = landing + response seen; disjunct 3 = landing with response lost; disjunct 4 = request in flight, landing is `LateLand`. The response of an ambiguous request is never a step: the client's next step is the resolve read |
| `LateLand` | store side of a sent PUT | landing of one PUT | — |
| `AllocResolve` | `writeLoop` resolve read (`observe`) + byte comparison | GET `epoch` | the comparison is local |
| `ClaimRead` | `Pool/CasServerRoot.cpp:claimMount` (`op.read`) + branch; wait loop of `claimMountAwaitingExpiry` | GET `mount` | the observation wait is a guard (ghost `ProvenDead`), not a step |
| `ClaimMint` | `claimMount` → `op.create` | PUT `If-None-Match` | not ambiguous: the body carries a random `write_attempt_id`, so resolve-by-bytes cannot claim another writer's body |
| `ClaimReclaim` | `claimMount` → `op.replace` | PUT `If-Match` | same as `ClaimMint` |
| `WriteManifest` | `Pool/CasPartWriteTxn.cpp:PartWriteTxn::stageManifest` | PUT `If-None-Match` manifest | blobs out of scope |
| `Publish` | `PartWriteTxn::promote` (ref commit) | one ref write | precommit omitted: the window is staging → commit, which the sweep's view does not protect |
| `Farewell` | `MountLeaseRenewer::terminate`; `admin.reset()` for the tool | PUT `If-Match` `mount` | — |
| `Crash` | process death | none | — |
| `DDrain` | `decommissionPoolMember` namespace drain | abstraction of the drain under the admin claim | refs dropped in one step; the drain runs under the tool's live lease and is not the subject |
| `DCapture` | `decommissionPoolMember`: `op.read(epoch_key)` | GET `epoch` | — |
| `DReadMount` | `op.read(mount_key)` + `captures_match` | GET `mount` | check is local |
| `DDelMount` | `deleteSlotObject(mount_key, farewell etag)` | DELETE `If-Match` | — |
| `DDelEpoch` | `deleteSlotObject(epoch_key, claimed etag)` | DELETE `If-Match` | — |
| `DRecheckMount`, `DRecheckEpoch` | liveness recheck reads | GET each | — |
| `DRetire` (fix) | proposed `op.replace(epoch_key, retired, claimed etag)` | PUT `If-Match` | not ambiguous in the model: `retired_at_ms` makes the bytes unique, so resolve-by-bytes is exact |
| `DReadOwner` | `op.read(owner_key)` | GET `owner` | — |
| `DTombstone` | `op.replace(owner_key, tombstone, owner etag)` | PUT `If-Match` | — |
| `GcFloor` | `Gc/CasOrphanManifestSweep.cpp:floorForNamespace` | GET `mount` | — |
| `GcView` | protection view in the sweep loop | reads of committed refs | one snapshot of `refs`; the view is built once per namespace before deletions |
| `GcDelete` | sweep loop delete after `prefixEligibleUnder` | DELETE manifest | `prefixEligibleUnder` is local on the cached floor |
| `GcEnd` | end of round | in-memory | — |
| `OwnerEmpty`, `OwnerCreate` | `claimOwnerOrThrow` owner-absent branch: `serverRootSubtreeEmpty`, `op.create(owner)` | LIST; PUT `If-None-Match` | a create conflict fails closed in the model; the code re-checks emptiness and accepts a same-uuid owner (an under-approximation of that path) |
| `ABase` (owner floor) | proposed `GET owner` in `allocateWriterEpoch` after the absent read | GET | — |
| `DTombstone` (owner floor, lifecycle) | `op.replace(owner_key, tombstone, etag)` | PUT `If-Match` | disjunct 2 = response lost and resolve given up; the landing is `OwnerLateLand` |
| `DPostTombCheck` | proposed `GET epoch` after the tombstone | GET | comparison is local |
| `DUntomb` | proposed `op.replace(owner_key, untombstoned, our tombstone etag)` | PUT `If-Match` | — |
| `DConfirmClaim` | proposed `GET mount` after the tombstone | GET | comparison with the tool's own claim token is local |
| `DRestart` | the tool dies (or pauses, with `ZombieRun`) and is run again | none | the paused run keeps only its decided writes: `OwnerLateLand`, `ZombieConfirm` (GET), `ZombieUntomb` (PUT), `ZombieDrain` |
| `ZombieDrain`, `DDrain` (lifecycle) | drain under the admin claim | abstraction of the drain | refs dropped in one step, as `DDrain` |
| `ClaimCheckOwner` | variant: `GET owner` after the mount GET, before the write | GET | — |
| `ClaimPostOwner` | proposed `GET owner` after a landed claim (`mountWritable`, `tryRemountOnce`) | GET | — |
| `ClaimPostFarewell` | refused claimer's terminal write | PUT `If-Match` own token | — |
| `GcFence` | GC's fence write on the admin's lease | PUT `If-Match` | — |
| `RRead`, `RWrite`, `RDelete` | operator revival (round 3) | GET; PUT `If-Match`; DELETE | — |
| `RvStart`, `RvEmpty`, `RvReadMount`, `RvDelMount`, `RvReadOwner`, `RvWriteOwner` | proposed `cas-readd-member` | GET owner; LIST; GET mount; PUT or DELETE `If-Match` mount; GET owner; PUT `If-Match` owner | — |
| `RvReadEpoch` (round 5) | re-add `GET epoch` | GET | — |
| `DVerifyMount`, `DVerifyOwner` | proposed verify after `admin.reset()` | GET mount; GET owner | comparisons are local |
| `DDrain` with `DrainFenced` | drain on `admin->mountRequests()` | one drain request | the lease-deadline check is local, modelled as "own claim still in the slot, unfenced"; disjunct 2 = request sent, response lost |
| `LateDelete` | store side of a drain request sent before the lease was lost | landing of one DELETE | refs dropped in one step, as `DDrain` |
| `JoinEpoch`, `JoinMount`, `JoinEmpty`, `JoinWrite` | proposed rejoin in `claimOwnerOrThrow` | GET epoch; GET mount; LIST; PUT `If-Match` owner | the owner token is the one `OwnerCheck` read (`jTok`); a conflict returns to the owner GET |
| `DMarkVerified` | proposed step-6 owner rewrite marking the tombstone verified | PUT `If-Match` owner | — |
| `GcFenceDead` | GC's fence write on an expired lease of a dead holder | PUT `If-Match` mount | death is the `ProvenDead` ghost |
| `RemountStart`, `RemountOwner` | `Pool::tryRemountOnce`: lease lost, then `claimOwnerOrThrow` | in-memory step; GET owner | the remount then allocates and claims through the normal actions; with `OwnerLife` the check compares the recorded life |
| `JoinRepl` | proposed new-uuid mount replacement in rejoin | PUT `If-Match` mount | — |
| `ClaimForeignOwner` | proposed GET owner before a foreign reclaim in `claimMount` | GET owner | the mount write that follows is `If-Match` on the token `ClaimRead` saw, so a mount change in between fails it |
| `GcFenceLive` | GC's fence write on the lease of a partitioned instance | PUT `If-Match` mount | — |
| `CatalogGc` | GC deletes the victim's `Removing` catalog rows | abstraction of the GC round | one step |

## Scope and limits {#scope}

- One uuid in rounds 1–3; round 4 adds a second uuid for re-add. Foreign-uuid owner paths (S14, S18) are out of scope.
- Renewals and lease expiry are not modelled, and mount fencing only as GC fencing the admin in round 4 (`GcFence`); a live holder is never reclaimed because `ProvenDead` reads its process state. A zombie writer (S8) is therefore out of scope; the paused claimer in `cur_decom_dangle` is the one late request modelled on the mount.
- One build per process (`seq = 1`); the floor's `min_active_build_sequence` is 1 while live, `UINT64_MAX` after the farewell.
- S9 (MD5 ABA on byte-identical manifests) is not modelled.
- Bounds: `EMax` 4–6 (round 2: 8, or 16 with `Scale = 4`, `MaxClock = 2`), 1–3 starts, at most 1 ambiguous write, at most 1 sweep round, at most one concurrent `_pool_meta` admission.
- `Scale` bounds allocations per store second; with at most four allocations per run, `Scale = 4` stands for `2^20`.
- Owner tokens: `OTok` is the body on S3 without the owner nonce (uuid, retired, floor, `retired_at_ms`); a generation otherwise. Every earlier row runs with `OwnerNonce = TRUE`, which keeps its generation semantics.
- The admin's self-remount is not modelled (spec review 6); a fenced admin either stops at its confirm or keeps running (`GcFence`).
- Round 4 bounds: victim `a` with one start, fresh member `n` after a re-add, one re-add run (two in `lc_race`), one decommission re-run, `EMax = 6`. `lc_race` drops the re-run and the GC round (larger bounds exceeded 600 s); `res_lc_pauseddrain` drops `a` and the GC round to shorten its trace.
- Drain harm is approximated by references: a drain is harmful when it removes a reference of an epoch newer than the draining run's own.
- `tombOverLive` is tracked only when `KeepAll` holds, so earlier rows keep their recorded state counts.
