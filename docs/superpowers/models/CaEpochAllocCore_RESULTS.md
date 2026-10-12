# `CaEpochAllocCore` results {#ca-epoch-alloc-core-results}

Model of writer-epoch allocation, the mount slot and the decommission tail for one server root, at release line `2a034cdb374` (`antalya-26.6-20261008` = `d76b59f9295` + PR #2512). Backlog: CAS-362 (twin allocation), CAS-363 (decommission re-mints epoch 1).
Runner: `run_epochalloc.sh` (`SLOW=1` adds `fix_nonce_restart`). Paths below are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Verdict {#verdict}

- **CAS-362 reproduced.** Two same-uuid instances get the same epoch. One of them sends an ambiguous create of `{2}`; the other creates the same bytes. The resolve read in `CasOperation::writeLoop` sees a moved precondition with equal bytes and returns `Committed`. Same on S3 and GCS: the rule compares bytes, not tokens.
- **PR #2512 already contains the twin's harm.** With the strict claim (`same_epoch_occupied`), `cur_twin_dangle` is green: the second twin never writes. The `d76b59f9295` refresh rule (`base_twin_dangle`) dangles a reference.
- **CAS-363 reproduced.** After the tail deletes `mount` and `epoch`, a victim restart mints epoch 1 again (`cur_decom`, S3 and GCS). This reaches a dangling reference through the orphan sweep with a floor read from the farewell (`cur_decom_reusedangle`).
- **New finding: deleting the mount slot is a hazard of its own.** A claimer that read the slot absent before the decommission and paused sends its `If-None-Match` after `DDelMount`. It mints a lower epoch into the slot (`cur_decom_slot`, `SlotNeverRegresses`) and the sweep deletes its in-flight manifest (`cur_decom_dangle`). This needs no epoch reuse, so a retired counter alone does not close it. `sab_retire_delmount` shows it stays red when the retiring tail still deletes `mount`.
- **Recommendation: `fix_both` = nonce + retire, and keep the farewell `mount`.** Green with every invariant, two instances, one ambiguous write, decommission and one sweep round.

## Configs {#configs}

Wall time on 4 workers. "States" = distinct states; for violations the count varies run to run with worker scheduling. Final battery run on the committed model.

| Config | What it is | Invariants | Expected | Result | States | s |
|---|---|---|---|---|---|---|
| `cur_single` | one instance, 3 starts, 1 ambiguous write, sweep | all three non-decom | green | green | 102,763 | 1 |
| `cur_twin` | 2 same-uuid instances, 1 ambiguous write, S3 | `EpochNeverReused` | violation | violation | 911 | 0 |
| `cur_twin_gcs` | same, GCS generations | `EpochNeverReused` | violation | violation | 879 | 1 |
| `cur_twin_dangle` | twins + sweep, strict claim (#2512) | `NoDangle`, `SlotNeverRegresses` | green | green | 32,660 | 0 |
| `base_twin_dangle` | twins + sweep, `d76b59f9295` refresh of own pair | `NoDangle` | violation | violation | 34,966 | 1 |
| `cur_decom` | victim restarting around the current tail | `EpochNeverReused` | violation | violation | 2,937 | 1 |
| `cur_decom_gcs` | same, GCS | `EpochNeverReused` | violation | violation | 3,331 | 0 |
| `cur_decom_dangle` | current tail + sweep | `NoDangle` | violation | violation | 40,834 | 1 |
| `cur_decom_reusedangle` | current tail + sweep, dangle after a re-mint | `NoDangleAfterReuse` | violation | violation | 71,065 | 1 |
| `cur_decom_slot` | current tail | `SlotNeverRegresses` | violation | violation | 2,471 | 0 |
| `cur_decom_retired` | current tail | `RetiredStaysRetired` | violation | violation | 5,396 | 1 |
| `fix_nonce` | twins, nonce compared in resolve | 3 non-decom | green | green | 95,756 | 1 |
| `fix_nonce_gcs` | same, GCS | 3 non-decom | green | green | 92,070 | 1 |
| `fix_nonce_restart` (SLOW) | twins, 2 starts each, `EMax = 5` | 3 non-decom | green | green | 167,232,390 | 563 |
| `sab_nonce` | nonce in body, resolve compares only the value | `EpochNeverReused` | violation | violation | 776 | 1 |
| `fix_retire` | retire `epoch` by `If-Match`, keep `mount`, 3 victim starts | all four | green | green | 1,055,233 | 4 |
| `fix_retire_gcs` | same, GCS | all four | green | green | 1,055,233 | 3 |
| `sab_retire` | retired body written, allocation ignores it (26.6.4 binary) | `RetiredStaysRetired` | violation | violation | 43,568 | 0 |
| `sab_retire_epoch` | same sabotage | `EpochNeverReused`, `SlotNeverRegresses` | green | green | 1,290,109 | 5 |
| `sab_retire_delmount` | retire, then delete the farewell `mount` | `SlotNeverRegresses` | violation | violation | 40,237 | 1 |
| `fix_retire_twin` | retire only, twins + ambiguous write | `EpochNeverReused` | violation | violation | 2,693 | 1 |
| `fix_both` | nonce + retire, twins, decommission, sweep | all four | green | green | 66,298,700 | 241 |

`sab_retire_epoch` is green by design: a binary that ignores the retired marker still continues the counter, so epochs stay unique. The marker guards against resumption of a retired root (`sab_retire`), not uniqueness. Uniqueness after decommission comes from not deleting the counter.

## Invariants {#invariants}

| Name | Meaning |
|---|---|
| `EpochNeverReused` | `allocateWriterEpoch` never returns one value twice for the uuid, across decommission (ghost `allocLog`). |
| `NoDangle` | every committed reference names a present manifest key `(epoch, seq 1)`. |
| `NoDangleAfterReuse` | `NoDangle` in runs where an epoch was re-minted; isolates the CAS-363 harm from the slot-deletion path. |
| `SlotNeverRegresses` | the mount slot never holds a `writer_epoch` below one it held before. |
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
- Bounds: `EMax` 4–6, 1–3 starts, at most 1 ambiguous write, at most 1 sweep round.
