# `CaEpochAllocCore` results {#ca-epoch-alloc-core-results}

Model of writer-epoch allocation, the mount slot and the decommission tail for one server root, at release line `2a034cdb374` (`antalya-26.6-20261008` = `d76b59f9295` + PR #2512). Backlog: CAS-362 (twin allocation), CAS-363 (decommission re-mints epoch 1).
Runner: `run_epochalloc.sh` (`SLOW=1` adds `fix_nonce_restart` and `fix_poolhwm_twins`). Paths below are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Verdict {#verdict}

- **CAS-362 reproduced.** Two same-uuid instances get the same epoch. One of them sends an ambiguous create of `{2}`; the other creates the same bytes. The resolve read in `CasOperation::writeLoop` sees a moved precondition with equal bytes and returns `Committed`. Same on S3 and GCS: the rule compares bytes, not tokens.
- **PR #2512 already contains the twin's harm.** With the strict claim (`same_epoch_occupied`), `cur_twin_dangle` is green: the second twin never writes. The `d76b59f9295` refresh rule (`base_twin_dangle`) dangles a reference.
- **CAS-363 reproduced.** After the tail deletes `mount` and `epoch`, a victim restart mints epoch 1 again (`cur_decom`, S3 and GCS). This reaches a dangling reference through the orphan sweep with a floor read from the farewell (`cur_decom_reusedangle`).
- **New finding: deleting the mount slot is a hazard of its own.** A claimer that read the slot absent before the decommission and paused sends its `If-None-Match` after `DDelMount`. It mints a lower epoch into the slot (`cur_decom_slot`, `SlotNeverRegresses`) and the sweep deletes its in-flight manifest (`cur_decom_dangle`). This needs no epoch reuse, so a retired counter alone does not close it. `sab_retire_delmount` shows it stays red when the retiring tail still deletes `mount`.
- **Round 1 recommendation (retire + keep `mount`, `fix_both`) was rejected by the owner:** it leaves permanent per-srid objects and blocks srid reuse. Round 2 below keeps today's tail (delete `mount`, delete `epoch`, tombstone `owner`) and makes a re-created counter start above every earlier epoch.
- **Round 2 recommendation: V2, a pool high-water mark in `_pool_meta`, read again after the absent read, plus a mint check in `claimMount` and the nonce.** Green for all five invariants without any time assumption (`fix_poolhwm_fresh_nz_mc_*`, `fix_poolhwm_twins`). V1 (store time) reaches the same result only if the store clock never steps back and a counter is never deleted in the second it was created (`fix_storetime_fresh_nz_slow_mc_*` vs `sab_storetime_back`, `fix_storetime_fresh_nz_*`).
- **The zombie class stays red under both variants.** A counter create or a mount mint decided before the tail's DELETE and landing after it reuses or regresses an epoch. Neither V1 nor V2 changes it (`fix_poolhwm_fresh_mc_*` all red); only not deleting closes it. Every round-2 green needs `NoZombie`.

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
- `Formats/CasServerRootFormats.cpp:decodeServerEpoch` (tag `v26.6.4.20001.altinityantalya` and `2a034cdb374`) loops `nextKey`; unknown keys go to `JsonObjectReader::skipUnknown`. In 26.6.4 the reader is built with `KeyStrictness::Tolerant`; at `2a034cdb374` the reader has no strict mode. A plain extra field (`alloc_nonce`, `retired_at_ms`) is skipped.
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

## Scope and limits {#scope}

- One uuid; foreign-uuid owner paths (S14, S18) are out of scope.
- Mount fencing (`gc_fenced`), renewals and lease expiry are not modelled; a live holder is never reclaimed because `ProvenDead` reads its process state. A zombie writer (S8) is therefore out of scope; the paused claimer in `cur_decom_dangle` is the one late request modelled on the mount.
- One build per process (`seq = 1`); the floor's `min_active_build_sequence` is 1 while live, `UINT64_MAX` after the farewell.
- S9 (MD5 ABA on byte-identical manifests) is not modelled.
- Bounds: `EMax` 4–6 (round 2: 8, or 16 with `Scale = 4`, `MaxClock = 2`), 1–3 starts, at most 1 ambiguous write, at most 1 sweep round, at most one concurrent `_pool_meta` admission.
- `Scale` bounds allocations per store second; with at most four allocations per run, `Scale = 4` stands for `2^20`.
