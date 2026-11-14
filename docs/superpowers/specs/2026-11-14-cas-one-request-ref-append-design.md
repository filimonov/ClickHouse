---
description: 'Design for a CAS ref-log append that writes one object and never touches `_ckpt`. An ordinary append becomes one create-if-absent of `_log/{epoch,seq}`; a durable log on the chain is committed history; `_ckpt` is written only at namespace creation and at snapshot publication. An epoch change is recorded lazily by a tombstone (`EpochSeal`) that carries a forward link to the successor epoch, so an epoch that does not write a namespace leaves no object. Readers find the end of history by exact reads to the first absent key. Ships behind format generation 3. A positive append keeps its catalog read.'
sidebar_label: 'CAS: one-request ref-log append'
sidebar_position: 116
slug: /superpowers/specs/cas-one-request-ref-append-design
title: 'CAS: one-request ref-log append with a lazy, forward-linked epoch tombstone'
doc_type: 'reference'
---

# CAS: one-request ref-log append — design {#cas-one-request-ref-append}

- Date: 2026-11-14. Status: design; nothing is implemented, built or run. Task: CAS-487; other tasks: [§13](#backlog).
- Fixed inputs: the change ships with 26.8 as generation 3; the acknowledgement fence is the mount lease; no seal is
  retained for fencing; no snapshot is forced at an epoch change; external deletion of CAS objects and an untrusted
  stream `LIST` stay as today; objects of generations 1 and 2 stay readable.
- Source: worktree `cas-pr1-clean` at `802077e5c1a`; every `path:line` was read there, `26.6:` paths by
  `git show altinity/antalya-26.6:<path>`. A bare `:line` repeats the last file. "Inferred": see [§12](#limits).
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`. `M/` =
  `docs/superpowers/models/` in the `master` worktree.
- Terms. **Tombstone**: a log whose only op is `EpochSeal`. **Chain**: the logs reachable from the snapshot base by
  [§3.1](#chain-rule). **Chain epoch**: the epoch of its first absent key. **Live epoch**: the lease holder's epoch.

## 1. Decision {#decision}

1. **Commit rule.** An ordinary append is one `PUT _log/{epoch,seq}`, create-if-absent. A durable log on the chain is
   committed history. No append touches `_ckpt`. A positive append keeps its catalog `GET` ([§7](#deferred)).
2. **Chain.** The `_ckpt` snapshot, then logs in successor order to the first absent key or a `RemoveNamespace`.
3. **Lazy tombstone.** A writer of epoch `W` whose chain epoch is `X < W` creates, before its first append, a tombstone
   in the next slot of `X` with the critical field `!next_epoch = W`. `{W,1}` names exactly that tombstone in
   `!prev_epoch`/`!prev_seq`. Numeric epochs between `X` and `W` get no object.
4. **Selective eager tombstone.** Recovery writes the same tombstone before it installs the table iff
   `X <= last_unclean_epoch`, a new field of the mount lease body ([§4](#eager)).
5. **Fence.** The mount lease, checked after the response and in the install hold; a tombstone is cleaned like any log.
6. **`_ckpt`.** Generation 3 holds `life_epoch`, set at creation ([§2.7](#genesis)), and `checkpoint_snapshot_id` only.
7. **Readers.** One shared walk finds the end by exact reads ([§3](#readers)); GC cuts at its first absent key. A reader
   whose catalog cut can predate an erase confirms the life after its walk ([§3.1](#chain-rule)).
8. **Gate.** Generation 3: pool floor, `_ckpt` header, and the header of every log that carries a chain link. The stamp
   of a namespace's `_ckpt` precedes its first log written under the new rule.

Store requests, warm namespace, one writer; counted from the code, not measured (CAS-167: `_ckpt` is no hot key):

| Operation | Today | After | Today's requests |
|---|---|---|---|
| Positive append (new binding, `Move`, `SetPublishedAt`) | 4 | 2 | catalog `GET` `CA/Pool/CasRefLedger.cpp:3612` (stays), log `PUT` `:3982`, `_ckpt` `GET` + `PUT` by `readModifyWrite` `:4217` |
| Ref removal (`OwnerTransition` without a new binding) | 3 | 1 | not positive, `:3550-3554` |
| Terminal `RemoveNamespace` append | 4 | 2 | catalog `GET` `:3581` (stays) |
| First append of an epoch in a namespace, extra | 0 | +1 | the tombstone `PUT` |
| New part without blobs | 14 | 8 | manifest `PUT` `CA/Pool/CasPartWriteTxn.cpp:696`, precommit `:842`, manifest `GET` `:873`, promote `:1061`, `Move` |
| Writer recovery, quiet namespace, one dead epoch, snapshot base | 12, 2 writes | 6, 0 writes | `CA/Pool/CasRefLedger.cpp:954-1386`; one of the 6 is the catalog confirmation; 7 with 1 write under the eager rule |
| GC intake, quiet namespace, per round that is not deferred | 1 | 2 | `_ckpt` `GET`; new: plus the successor probe; 3 on the cold path ([§3.2](#cursor)) |
| Snapshot publication | 3 | 3 | `_snap` `PUT`, `_ckpt` `GET` + `PUT`, `:4821`, `:4872` |

- **Given up.** The store-side refusal of a superseded writer's append in a reclaimed slot ([§8](#risks), row 1).
- The exact frontier. A cold reader pays one absent probe, GC one more read per quiet namespace per round.
- The missing-log diagnostic above the base (`CA/Gc/CasGc.cpp:2909-2914`). One `PUT` per namespace per writing epoch.
- 26.6 builds and 26.8 builds without this change cannot mount or fold a pool written under the new rule.

## 2. Writer {#writer}

### 2.1 Append order {#append-order}

| Step | Action | Where today |
|---|---|---|
| 1 | Lane is `Ready`. The chain epoch equals the live epoch, else the tombstone step ([§2.3](#tombstone)) runs first. | `flushRefBatch`, `CA/Pool/CasRefLedger.cpp:3091` |
| 2 | Read state, id, admitted generation and the open transition in one hold. Build candidate and exact attempt. | `:3741-3791` |
| 3 | Arm the attempt: `Ready -> Writing`. | `:3884-3894` |
| 4 | `PUT` create-if-absent with `Retry::standard`. The engine rechecks the lease after the response. | `CA/Backend/CasRequests.cpp:868`, `CA/Pool/CasRefLedger.cpp:3982` |
| 5 | Take `state_mutex`. Recheck admission. Swap the candidate in: `Writing -> Ready`. | `:4261-4321` |
| 6 | Wake the waiters with the committed id. Schedule the snapshot check. | `:4363-4376` |

- A log that is durable when a recheck fails is history and is never acknowledged: `Wedged` (step 4) or `NeedsRecovery`
  (step 5), today's texts (`:3927-3956`). No id is allocated while an attempt is held.

### 2.2 Lane states under the new rule {#lane}

| Outcome of the create | Lane | Note |
|---|---|---|
| Committed, admission holds | `Ready`, acknowledged | |
| Durable, admission lost inside the install hold | `NeedsRecovery` | history, not acknowledged; recovery replays the chain, `:4266-4276` |
| Sent and outcome unknown, or durable with the lease lost at the engine's recheck | `Wedged` | `:4159-4162`; `resolveWedgeOnce` re-creates the same key and bytes, `:2493`; its `_ckpt` publish at `:2553-2620` is removed |
| Refused, or nothing sent | `Ready` | id not consumed, `:4120-4191` |
| Append conflict, occupant is a tombstone | `Closed` | conclusive rejection, `:3961-3977`; the writer is deposed |
| Conflict, any other occupant | `Faulted` for an append; `NeedsRecovery` for a tombstone attempt | [§2.3](#tombstone) |
| Conflict, occupant unreadable | `Wedged` | `:4013-4039` |

### 2.3 The tombstone step {#tombstone}

- **Where.** `flushRefBatch`, at today's deposed-lane check (`:3091-3093`), before any preview (`:3360`, `:3388`) and id
  allocation (`:3744`): no candidate is built from a table recovery changes. It reads the live epoch once, re-checks
  `may_mutate` and the superseded flags, then compares. A self-remount publishes the new epoch before it detaches the
  old runtimes (`CA/Pool/CasPool.cpp:1632-1646`; CAS-350).
- **Rule.** `X` is the chain epoch, `W` the live one. `X = W`: proceed. `X > W`: this writer is deposed; the lane goes
  `Closed` with today's rejection. `X < W`: create a tombstone at the chain's next slot with `!next_epoch = W`; if that
  slot is sequence 1 (the previous record is itself a tombstone naming `X`), it carries the backlink to that record.
- **Mechanics.** A lane attempt: armed, sent (`Retry::standard`), installed after the lease recheck. It sets the open
  transition `{seal id, next epoch}`, replacing `RefTableRuntime::last_epoch_seal`; `chainLinkFor`
  (`CA/Pool/CasRefLedger.cpp:146-151`) names it iff the epochs match. Unresolved, it wedges the lane: `{W,1}` waits.
- **Conflict on the lane.** Own bytes: install (the encoding is deterministic). A later epoch's tombstone: `Closed`. Any
  other readable occupant: the lane clears the attempt, so recovery's retained-attempt check (`:1098-1104`) does not see
  it, and goes `NeedsRecovery`, whose walk adopts the occupant: a recovery tombstone or numeric seal that a build of
  commits 4 to 5 left in flight across a clean farewell ([§4](#eager)).
- **Conflict in recovery.** The walk applies the occupant and loops:

| Occupant | Recovery then |
|---|---|
| Ordinary record of `X` (a predecessor's late append) | adopted as history; chain epoch stays `X`; the tombstone goes one slot later |
| Tombstone naming `V > W` | chain epoch `V`; lane `Closed` |
| Tombstone naming `U < W` (a writer that stopped after its tombstone) | chain epoch `U`; the next tombstone goes to `{U,1}` with the backlink |
| `RemoveNamespace` | terminal; positive lane closed; no tombstone |

- The loop is bounded by `kRefRecoveryMaxSlotAttemptsPerEpoch` (`:1246`): one writer has one append in flight. A
  terminal stream gets no tombstone; a never-born one gets one like any other ([§2.7](#genesis)).

### 2.4 Writer recovery {#writer-recovery}

1. Capture the pair (live epoch, `last_unclean_epoch`) once. Read `_ckpt` once and validate the base as today
   (`CA/Pool/CasRefProtocol.cpp:944-1030`). An absent or undecodable `_ckpt` of a life the catalog still names stays
   `CORRUPTED_DATA` (`CA/Pool/CasRefCkpt.cpp:180-182`), so no tombstone or append enters that life (CAS-479).
2. Walk the chain from the base's successor, or from `{life_epoch,1}`, to the first absent key ([§3.1](#chain-rule)). A
   retained attempt whose slot holds foreign bytes stays `CORRUPTED_DATA` (`CA/Pool/CasRefLedger.cpp:1098-1104`).
3. Stamp: if the `_ckpt` header is below 3 and a write follows, publish the generation 3 body, conditional on the step 1
   token; a mismatch restarts. A recovery that writes nothing leaves the stamp to the lane.
4. If the stream is not terminal and the chain epoch `X < W` is at or below `last_unclean_epoch`: create the tombstone
   in the walk, with today's code at `:1244-1350` minus its `_ckpt` publishes. A conflict adopts the occupant and loops.
   An unresolved create fails retry-later; nothing is installed (`:1318-1337`).
5. Re-read `_ckpt` after the last read or write of steps 2 to 4; a token other than step 1's, or the stamp's, restarts
   the walk. That is today's order: the seal create (`:1269`), then the final compare (`:1377-1386`). A tombstone is a
   create into another epoch's slot: its success, or an adopted occupant, proves "this was the chain's end" only if the
   base did not move since the walk; otherwise cleanup may have emptied the slot ([§5](#crashes), row 21). A tombstone
   left by a restarted walk is on the chain, or below the new base, where no reader looks.
6. Confirm the life in the catalog ([§3.1](#chain-rule)), also before reporting an error of steps 1 to 4. A removed life
   detaches the runtime (`invalidateRemovedCatalogLife`, `:796-840`): retry-later for a write, absent for a read.
7. Install. Chain epoch above `W`: lane `Closed`. Terminal: positive lane closed. Otherwise `Ready`.

- Recovery writes nothing after a clean predecessor; read paths call it (`:315`, `:390`, `:413`). The stale-precommit
  sweep uses the lane (`:4942-4966`): a reading epoch with stale precommits writes a tombstone and a removal.
- Snapshot publication is admitted only when the chain epoch equals the live epoch (one more term at `:4381-4388`). A
  reading epoch publishes nothing, as today (`T/gtest_cas_ref_writer.cpp:3252`). Accepted.

### 2.5 What `_ckpt` carries {#ckpt}

| Field | Generation 3 | Writer |
|---|---|---|
| `life_epoch` | kept; set once at creation, before the row turns `Live` | `completeCreation` `CA/Pool/CasRefCatalog.cpp:631`; the birth chunk no longer writes it |
| `checkpoint_snapshot_id` | kept; raised at snapshot publication | `CA/Pool/CasRefLedger.cpp:4872-4876`, after the `_snap` `PUT` |
| `committed_through`, `last_epoch_seal` | gone | were written per append and per recovered record, and by the sealer |

- **Creation.** Only `completeCreation` creates a generation 3 `_ckpt`. The snapshot publish and the stamp are
  conditional replaces of a present body and refuse an absent one. Today any publisher creates it
  (`CA/Pool/CasRefCkpt.cpp:286-290`). So `life_epoch` is set once, and no publisher re-creates a dead life's `_ckpt`.
- The chain evidence of a snapshot is not in `_ckpt`: it is the same-id log (`CA/Pool/CasRefProtocol.cpp:831`) and, for
  a base at sequence 1, the tombstone its backlink names (`:833`). Cleanup keeps both. The base check adds one test, a
  forward-linked predecessor names the base's epoch, and drops the one at `:980-991`.
- In a body of generation 1 or 2 `committed_through` stays the exact frontier: a chain that ends below it is
  `CORRUPTED_DATA`. `IdenticalSkip` (`CA/Pool/CasRefCkpt.cpp:313-317`) compares the generation too.
- A publish whose `checkpoint_snapshot_id` is not above the durable one in id order writes nothing (`:163`). That keeps
  out a superseded writer's snapshot: a stray exists only below a durable `S` of a later epoch ([§3.2](#cursor)).
- A generation 3 `_ckpt` is not derivable from the logs: deleting a damaged one is no repair (CAS-32, CAS-56).

### 2.6 Removed mechanisms {#removed}

| Mechanism | Where | Fate |
|---|---|---|
| Per-append, per-wedge and recovery `_ckpt` publishes with the `SuccessorSeal` / `Superseded` outcomes; the one-unfrontiered-successor rule; the seal of every numeric epoch | `CA/Pool/CasRefLedger.cpp:4217` with its outcome arms `:4229-4253`, `:2589`, `:1026-1061`, `:1356-1375`, `:1114-1140`, `++epoch` at `:1151`, `:1173`, `:1238`, `:1301`, `:1347`; `CA/Pool/CasRefCkpt.cpp:125-148` | removed; nothing replaces them |
| Absent-below-frontier and same-epoch-seal hole verdicts | `CA/Pool/CasRefLedger.cpp:1183-1219` | removed; kept only against a generation 1 or 2 `_ckpt` |
| `mergeCommittedThrough`, adjacency invariants of the codec | `CA/Pool/CasRefCkpt.cpp:39-58`, `CA/Formats/CasRefCkptFormat.cpp:76-91` | decode-only for old bodies |
| `nextRefLogIdWithinCommittedFrontier`, frontier-bounded hints | `CA/Pool/CasRefProtocol.cpp:911-942`, `CA/Pool/CasKeyReader.cpp:9-23` | replaced by the successor rule and a window |
| GC ceiling and the `committed_through` witness | `CA/Gc/CasGc.cpp:2872-2883`, `:2736-2737` | ceiling removed; the witness stays only for a generation 1 or 2 `_ckpt` |
| Birth-time `_ckpt` publish, `life_epoch` decrease refusal | `CA/Pool/CasRefLedger.cpp:3510-3515`, `:3834-3853`, `CA/Pool/CasRefCkpt.cpp:94-118` | removed ([§2.7](#genesis)) |

### 2.7 Genesis and the terminal record {#genesis}

A never-born stream follows the rule of any first write of an epoch; `life_epoch`, set at creation, names its first key.

- Created in `C`, first written by `W > C`: tombstone `{C,1} -> W`, then the birth at `{W,1}` with the backlink. A birth
  in `C` itself goes to `{C,1}` with no backlink.
- Grammar: a tombstone is legal on a never-born stream and refused only after a terminal record
  (`CA/Pool/CasRefProtocol.cpp:393-412` today refuses both). A sequence-1 record carries a backlink iff the record
  before it is a tombstone. The lifecycle test at `:555-557` is replaced by that rule.
- A terminal record is final: the walk reads no key after it, so replay must refuse a birth after it; today recovery
  walks on and replay accepts one (`CA/Pool/CasRefProtocol.cpp:359-367`). No stream holds one. The precommit builder
  would emit it (`CA/Pool/CasPartWriteTxn.cpp:827-835`), but a positive append needs the exact row `Live` at flush time
  (`CA/Pool/CasRefLedger.cpp:3606-3636`; 26.6 too, `26.6:CA/Pool/CasRefLedger.cpp:3442-3463`), and `DROP` first shuts
  the positive lane (`CA/Pool/CasRefLedger.cpp:5521`, `:2243`) and builds no birth on a terminal state (`:5666-5675`).
- Nothing else needs the birth epoch: the contextual grammar (`CA/Formats/CasRefLogFormat.cpp:262-278`), the walk start
  (`CA/Pool/CasRefCkpt.cpp:211`) and the GC first fold (`CA/Gc/CasGc.cpp:2799`) need the first key.
- Old-format streams: the first key is `{life_epoch,1}` with the value they have, raised or not: the birth without a
  backlink, or the first tombstone of a never-born one. Nothing raises it.
- Precondition, met in the base (`CA/Pool/CasRefCatalog.cpp:302-307`, `:328-329`): every create decision mints a fresh
  incarnation, else a new life would share `_ckpt` and its set-once `life_epoch` with an erased one.

## 3. Readers {#readers}

### 3.1 One rule for the end of history {#chain-rule}

`walkRefChain` alone decides where history ends. Every consumer of [§3.3](#consumers) calls it or its successor rule.

| Record at `{e,s}` | Successor |
|---|---|
| Ordinary | `{e,s+1}` |
| Tombstone with `!next_epoch = n` | `{n,1}` |
| Tombstone without the field (generation 1 or 2) | `{e+1,1}` |
| Contains `RemoveNamespace` | none: the stream ends here and no key after it is read |

- **Reads.** Exact `GET` by key, in successor order. Read-ahead inside one epoch is allowed, but only the result taken
  in order counts: an absent key that is not the next key on the chain proves nothing.
- **Validation.** Replay rejects a record after a tombstone that is not at its successor, and a head whose backlink is
  not that tombstone's id (`RefTableState::applyTxnInPlace`, next to the density check,
  `CA/Pool/CasRefProtocol.cpp:515`). GC, which does not replay, runs the same test on the two bodies.
- **End.** The first absent key on the validated chain, including an absent successor of a tombstone. Forward links
  strictly increase, so the walk terminates ([§3.4](#budget)).
- **Why an absent read can be trusted against cleanup.** Cleanup deletes only logs strictly below the snapshot id it
  read from `_ckpt` (`:824-836`), and that id only grows. A walk from a base re-reads `_ckpt` after its last read,
  whether it ended at an absent key or at a terminal record: an unchanged token means every cleanup so far used an id
  at or below the walk's base, a changed one restarts the walk. The terminal end needs it too: a predecessor's late
  `RemoveNamespace` can land in a slot that cleanup emptied, and a read-only walker would end "terminal" on a live
  stream. A walk from the GC round's own cursor needs no re-read: cleanup deletes only at or below it (`:829`).
- **A failed validation is not yet corruption.** The walk re-reads `_ckpt` first: a changed token restarts it, only an
  unchanged one is `CORRUPTED_DATA` (CAS-315). It frees its read-ahead window on a throw (CAS-86).
- **A life that dies under the walk.** After GC erases a row, the janitor deletes that life's objects page by page
  (`CA/Gc/CasNamespaceJanitor.cpp:143-190`). A walk that spans this reads a deleted key as the end, or a late create in
  a deleted slot as a record. `_ckpt` does not detect it: a delayed `completeCreation` re-creates it
  (`CA/Pool/CasRefCatalog.cpp:631-637` precedes the row check at `:650-652`). The janitor is not changed.
- **Rule.** A reader whose catalog cut can predate an erase re-reads the catalog after its last read of the life and
  confirms that a `Live` or `Removing` row still carries this exact incarnation. A missing, replaced or `Creating` row
  is a removed life: nothing is installed, and no corruption is reported about that life without this re-read.
- **Why it is enough.** The janitor deletes only objects of a life that a catalog read taken after its listing does not
  name (`CA/Gc/CasNamespaceJanitor.cpp:75`, `:86`, `:140`), and an erased row never returns ([§2.7](#genesis);
  exception: [§8](#risks), row 9). So a row present after the walk proves that no janitor delete preceded its reads.

| Reader | Can its cut predate an erase | Confirms today | Change |
|---|---|---|---|
| Ledger recovery: write admission (`namespaceLife`, `CA/Pool/CasRefLedger.cpp:5272-5292`) and read admission (`acquireReadableRefTableRuntime`), which a read-only open shares (inferred) | yes: the catalog reads (`:5275`; `:743`, `:763`) precede the walk, and another server's GC does not signal here (`:5242-5244`). For the lease holder the window is narrow: its own `DROP` goes through the name slot (`:662`, `:1926-1930`), and a superseded mount fails its lease check | no; only the resident write path re-reads, after a recovery error or a terminal (`:5245-5267`). The positive append's read (`:3612`) guards positive appends only | step 6 of [§2.4](#writer-recovery) |
| fsck | yes: one cut for the scan (`CA/Tools/CasFsck.cpp:491`), `_ckpt` sampled later (`:628`), replay at `:644` | no; it re-reads `_ckpt` only, and keeps that (`:323-335`, CAS-448) | re-read the catalog after the walks; one read confirms every life walked before it. A late recheck walks a life again (`recoverLateRefTable`, `:127-155`; manifests `:665`, blobs `:778`, `:1074`): its `Dangling` verdict is reported only after one more batched re-read confirms the life. A removed life is janitor-pending (`:588-598`), not a finding (CAS-457) |
| Orphan sweep, `sweepNamespace` (decommission) | yes: cut at `CA/Gc/CasOrphanManifestSweep.cpp:626`, deletes after the walk | no | the exclusion of the paged sweep, before its deletes |
| Presence probe; orphan sweep, paged | yes | yes: a catalog read after the terminal proof (`CA/Pool/CasRefLedger.cpp:5415`); candidates whose row changed are dropped (`CA/Gc/CasOrphanManifestSweep.cpp:169-190`, `:964`) | none for the sweep. The probe recovers a `Removing` row through writer recovery (`CA/Pool/CasRefLedger.cpp:5397-5403`): a removed life found by step 6 goes to the probe's own catalog decision (`:5415-5424`), row gone `false`, any other row of the name `true`. It never throws, as a throw fails `DatabaseOnDisk::dropTable` |
| GC intake, `REBUILD`; inspect | no: the leader alone erases, and its drain precedes its cut (`CA/Gc/CasGc.cpp:842`, `:922`, `:4657-4662`); a deposed leader's round ends at the `gc/state` write (inferred). Inspect reads no catalog and walks no chain | — | none |

### 3.2 Where GC resumes {#cursor}

**Stray objects.** Cleanup deletes logs at or below the cursor and strictly below the snapshot id `S`
(`CA/Pool/CasRefProtocol.cpp:827-836`). A late create-if-absent can land in such a slot, with correct clocks too; a
writer whose lease check passes wrongly can go on to later slots. **A key at or below the cursor and below `S`, or after
a deleted tombstone in its epoch, may hold a stray. No reader reads it.**

The fold cursor `c` is the last folded id, without its kind (`CA/Formats/CasFoldSealFormat.h:136`). Its successor:

1. Warm path: the GC process remembers `(cursor, successor key)` from its own fold. If the durable cursor equals the
   remembered one, it probes the remembered key. Nothing is persisted.
2. `c >= S`, or no snapshot: cleanup cannot have deleted `c`, so nothing replaced it. Read `c` for its kind.
3. `c < S`: neither read `c` nor probe `{c.e,c.s+1}` first. Back-chain from the epoch of `S` by sequence-1 backlinks
   (`crossEpochFromSeal`, `CA/Pool/CasRefProtocol.cpp:847-909`). A head names `c`: `c` was a tombstone, the head its
   successor. Reaching the epoch of `c` without naming it: `c` was ordinary, its successor `{c.e,c.s+1}`. Else: hold.

- Step 3 reads only keys above the cursor that the chain leads to. They were never deleted, so their only occupant is
  the chain's record. A cursor below `S` has a successor: cleanup keeps the snapshot's same-id log (`:831`) and the
  tombstone a sequence-1 base names (`:833`), and a tombstone is never a snapshot id
  (`CA/Pool/CasRefLedger.cpp:4381-4388`, `CA/Pool/CasRefProtocol.cpp:972-978`). Id order is chain order.
- Steps 2 and 3 hold for the round that commits the cursor: cleanup runs only after its `gc/state` write
  (`CA/Gc/CasGc.cpp:806`, `:1532`). A consumer that samples the adopted seal itself can lag (`readAdoptedFoldSeal`,
  `CA/Gc/CasOrphanManifestSweep.cpp:124-134`, calls at `:628`, `:752`). After its walk it re-samples the seal through
  `gc/state` and compares the namespace's coverage cursor (`coverageOf`, `:139-157`); re-reading the write-once seal key
  proves nothing. A moved cursor restarts the walk once; after that it is unfinished and keeps every candidate.

### 3.3 Consumers {#consumers}

Consumers, by an untruncated `grep` over `src/`: 26 files in `CA/`, 46 under `T/`, `src/Common/ProfileEvents.cpp`.

| Consumer | Reads today | Change | Quiet namespace |
|---|---|---|---|
| Writer recovery | `_ckpt`, base, tail to the frontier, one successor, seals and `_ckpt` per dead epoch (`CA/Pool/CasRefLedger.cpp:911-1410`) | [§2.4](#writer-recovery) | 12 -> 6 |
| Read-only recovery | base, tail to `committed_through`, no probe (`CA/Pool/CasRefProtocol.cpp:1032-1105`) | `walkRefChain`; returns the last tombstone it saw, not `_ckpt`'s. The catalog confirmation is its caller's ([§3.1](#chain-rule)) | +1 probe, +1 `_ckpt` |
| GC intake and round cut | `_ckpt` sampled at round start bounds the fold (`CA/Gc/CasGc.cpp:2449-2455`, `:2872-2883`) | cut = first absent key read in this round; nothing above it is folded even if it appears later; [§3.2](#cursor) for the start. Witnesses stay the listing, the snapshot id and the carried hold (`:2731-2739`), and `committed_through` only in an old body; a log above that frontier is folded, as old recovery adopts it too | 1 -> 2, cold 3 |
| Destructive gate | `frontier_proven` per namespace, all must hold (`:3541-3547`) | gate unchanged; a namespace is proven only by an absent key or a terminal record read in this round | none |
| GC cursor on a tombstone with an absent successor | cursor `+1` in the same epoch (`:2794-2800`), kind unknown (`:2851-2854`) | [§3.2](#cursor): resumes at `{n,1}`, never at `{e,s+2}`, and never trusts a key cleanup may have deleted | cold path only |
| Orphan manifest sweep | owner recovery (`CA/Gc/CasOrphanManifestSweep.cpp:256`), removal walk to `committed_through` (`:271`), exact `{E+1,1}` for a missing cursor (`:277-292`) | owner recovery uses `walkRefChain`; the removal walk resumes by [§3.2](#cursor) and no longer reads the cursor object below `S`; an incomplete walk keeps every candidate; `:546` unchanged | +1 probe |
| `REBUILD` | read-only recovery, cursor = greatest applied (`CA/Gc/CasGc.cpp:4823-4829`) | complete walk required; a cut-off fails the rebuild | +1 probe |
| fsck | stream audit to the frontier (`CA/Tools/CasFsck.cpp:312-420`), owner replay (`:628-645`), late recheck (`:133-140`) | both legs share one walk result from the base and read no key below it; the listing only attributes keys to lives (`:537-547`); "hole" is reported only below a generation 1 or 2 frontier or against a listed witness | +1 probe; catalog re-reads, batched |
| Inspect | prints the four `_ckpt` fields and the backlink (`CA/Tools/CasInspect.cpp:184-188`) | prints generation and `next_epoch`; old fields labelled as old-format; a log named by key is printed as stored (`:519-520`) and labelled "may be a stray" when it is below the base | none |
| Snapshot base validation | same-id log, predecessor seal, snapshot (`CA/Pool/CasRefProtocol.cpp:944-1030`) | [§2.5](#ckpt) | none |
| Relink confirmation | resident table, zero I/O (`CA/Pool/CasRefLedger.cpp:425-574`), slot check (`CA/Pool/CasPool.cpp:2154-2180`) | none; its premise, a complete recovered table, is kept by [§4](#eager) | none |
| Unchanged | per-epoch accounting of contiguous runs (`CA/Gc/CasGc.cpp:3296-3312`): skipped epochs have no run. Cleanup planning (`CA/Pool/CasRefProtocol.cpp:814-845`, `CA/Gc/CasGc.cpp:4050-4081`): a tombstone and a stray are deleted like any covered log. Namespace janitor (`CA/Gc/CasNamespaceJanitor.cpp:86`, `:118-156`) | none | none |

### 3.4 Bound of the walk {#budget}

- A lane appends one record per round trip, the walker a window (`CA/Gc/CasGc.cpp:2895`); that is no bound. The bound is
  a fixed time slice for the ref intake of one round, in the janitor's pattern (`:430-447`):
  `kRefIntakeBudgetMs = 60'000`, one default `gc_interval_sec` (`CA/ContentAddressedSettings.cpp:62`).
- Pass 1 is never cut: each walk target gets its `_ckpt` read, successor probe and one read window. That proves every
  quiet namespace and keeps CAS-29.10's rule "never cut the intake of every live life". Pass 2 continues the unproven
  ones from `round mod n` and starts no read after the slice ends (the round deadline once CAS-29.10 lands).
- A cut-off namespace keeps its cursor and is unproven for that round, which then suppresses every destructive step for
  the pool (`CA/Gc/CasGc.cpp:3541-3547`) but still folds and seals. `gc_frontier_probe_budget` bounds no walk: it caps
  probes of namespaces without a usable `_ckpt` (`:2493-2517`; CAS-92). CAS-29.1's probe is replaced by [§3.2](#cursor).

## 4. The selective eager tombstone {#eager}

**Why.** A request sent under a valid lease can land after the successor read its slot as absent. If it is a removal, GC
folds it and deletes a blob the successor still serves (`res_readwindow_gcdelete`). **Marker.** `last_unclean_epoch`, an
optional field of the mount lease body (`CA/Formats/CasServerRootFormats.h:66-93`). The epoch object cannot carry it: it
is written (`CA/Pool/CasPool.cpp:689`) before the predecessor's end is classified (`CA/Pool/CasServerRoot.cpp:1097`).
The claim (`:1089-1100`) writes the first body with the classification in hand:

| Predecessor body | New `last_unclean_epoch` |
|---|---|
| Clean farewell (`isCleanRelease`, `CA/Formats/CasServerRootFormats.h:83`) | the predecessor's value |
| `Fenced`, `UncleanObserved`, `UncleanUnsafe` | the predecessor's `writer_epoch` |
| Body without the field (older build) | the predecessor's `writer_epoch` |
| No slot | the claimer's epoch minus 1 |

- Body writers: the claim (`CA/Pool/CasServerRoot.cpp:1004`, `:1089`); the renewer's `encodeBody` (`:1689`: `start`
  `:1799`, the adoption write `:1718`, every renewal), fed by a member set at `installRenewer`; the farewell
  (`terminate`, `:2171`); `fenceOwnClaim` (`:1126`) and the GC fence (`:1403`), which copy the body; the rejoin rewrite
  (`:654-667`), which makes a fenced body clean but only on a root without catalog rows or manifests (`:629-650`), so no
  chain depends on its value. Forgetting the field is safe: such a body reads as the predecessor's `writer_epoch`.
- Publish point: with the live epoch (`setLiveWriterEpoch`, `CA/Pool/CasPool.cpp:965`, `:1632`), before recoveries are
  cancelled and the fence re-armed. Recovery captures both once (`CA/Pool/CasRefLedger.cpp:1602`). A self-remount makes
  the same claim (`CA/Pool/CasPool.cpp:1574`) over a body that is never clean, so a fresh runtime seals the old epoch.
- The value never decreases: a claim only replaces a lower epoch (`CA/Pool/CasServerRoot.cpp:1037-1063`; CAS-364). A
  forced exit writes no farewell and must not (CAS-355 option a): the successor would skip the tombstone.
- **Rule.** Recovery of a non-terminal stream (never-born included) seals before install iff `X <= last_unclean_epoch`.
- **Why it is enough.** Only the mount of epoch `X` creates ordinary records in epoch `X`. If `X > last_unclean_epoch`,
  epoch `X` ended with a clean farewell: any other end is recorded by the next claim. A farewell is written only after
  `drainRefLanesForShutdown` returned true (`CA/Pool/CasPool.cpp:1126-1137`, `:1280-1288`,
  `CA/Pool/CasMountRuntime.cpp:1230`): every resident lane idle, none `Writing` or `Wedged`
  (`CA/Pool/CasRefLedger.cpp:2103-2161`; eviction keeps non-`Ready` lanes, `:1918-1920`). So no append and no lane
  tombstone of `X` is in flight. None starts later: the drain first latches `shutting_down` (`:2109`), which nothing
  clears; the lane's only enqueue checks it under the same lock (`:2239`, `:2251`); a flush runs only for an enqueued
  item (`:2334-2341`). An epoch between `X` and `W` is drained too or raises the value above `X`. The rule compares the
  chain epoch, not the predecessor, so an epoch that never read the namespace passes the value on.
- **The lane tombstone needs no `_ckpt` re-read.** It exists only when `X > last_unclean_epoch`: no mount of `X` or of a
  later epoch below `W` can append there, and cleanup deletes only what exists. A recovery tombstone or numeric seal of
  a build of commits 4 to 5 can still land in the slot; nothing follows it, so it stays for `W`'s create to meet
  ([§2.3](#tombstone)). A successor's tombstone is the same conflict or, once cleaned, row 1 of [§8](#risks).
- **What a clean farewell does not exclude.** An unresolved recovery tombstone (`CA/Pool/CasRefLedger.cpp:1318`) is not
  a lane attempt and can land later; the farewell is not withheld. It carries no table content: the next lane step or
  recovery adopts it on conflict, GC folds a record without edges and deletes nothing for it ([§5](#crashes), row 20).
  The drain also ignores snapshot publications and catalog requests (CAS-179): a `_snap`, its `_ckpt` publish (the base
  only grows; a walk restarts by its re-read) and a catalog write (CAS-454) can land later.

## 5. Crash and race table {#crashes}

`E` wrote last; `W` and `V` are later epochs; `n` is the last sequence of `E`. Model results: [§9](#tla).

| # | Point | Durable | Readers see | Next writer | Model |
|---|---|---|---|---|---|
| 1 | Stop after the tombstone, before `{W,1}` | `{E,n+1} -> W` | end at absent `{W,1}` | `V` creates `{W,1} -> V` with the backlink, then `{V,1}` | `witness_sealonlyepoch` |
| 2 | Tombstone `PUT` ambiguous | unknown | end before or after it | same mount: `Wedged`, same key and bytes; no `{W,1}` until resolved | `safe_lateland` |
| 3 | Tombstone in flight at an unclean takeover | unknown | as row 2 | `V` seals eagerly; first create wins, the other adopts or is `Closed` | `witness_lateseal`, `safe_lateland_selective` |
| 4 | Predecessor `PUT` lands before recovery reads the slot | `{E,n+1}` ordinary | it is history | recovery replays it; tombstone at `{E,n+2}` | `witness_adopt` |
| 5 | Predecessor `PUT` lands just before the tombstone create | `{E,n+1}` ordinary | it is history | create conflicts, adopt, tombstone one slot later | `witness_sealconflict` |
| 6 | Predecessor `PUT` arrives after the tombstone | tombstone | tombstone | predecessor gets a conflict: `Closed`, not acknowledged | `safe_lateland` |
| 7 | Predecessor `PUT` lands after cleanup deleted the tombstone | orphan log below the base | not on the chain | acknowledged only if its lease check passes wrongly ([§8](#risks), row 1); then it may write on in that epoch; GC folds none of it and cleanup deletes it | `res_clocklies`, `safe_clocklies_nocleanup`, `safe_clocklies_gc`, `safe_clocklies_gc_4txn`, `sab_gccursor_probefirst_zombie`, `sab_gccursor_anyhead_zombie` |
| 8 | Predecessor `PUT`, sent under a valid lease, lands in a slot that cleanup reclaimed; correct clocks | a stray log at or below the GC cursor and below the snapshot id | not on the chain; no reader interprets that key ([§3.2](#cursor)) | nothing is acknowledged; cleanup deletes the stray as a covered log | `safe_lateland_selective`, `witness_cursorreused`; the unsafe rule is `sab_gccursor_probefirst_lateland` |
| 9 | Predecessor `PUT` lands after a lazy recovery | `{E,n+1}` ordinary | history that the live writer lacks | excluded by [§4](#eager); without the rule this is the read window | `res_readwindow_gcdelete`, `sab_selective_immediate` |
| 10 | `E` writes, `R` only reads, `W` writes | `{E,n+1} -> W`, `{W,1}` | chain skips `R` | `R` left nothing | `witness_skipepoch`, `witness_lazyafterclean` |
| 11 | `E` writes, `R` only reads and ends uncleanly, `S` only reads, `W` writes | `{E,n+1} -> S` from the recovery of `S`, then `{S,1} -> W`, `{W,1}` | chain passes through `S`, which holds only a tombstone | `R` left nothing; `S` left one tombstone | `safe_lateland_selective` |
| 12 | Stop between `_snap` `PUT` and its `_ckpt` publish | `_snap` above the checkpoint | old base, longer tail | republish meets identical bytes; cleanup deletes the orphan once a later base passes it | `safe_lateland_selective` (the publication travels and may land after a takeover) |
| 13 | Created in `C`; `W > C` stops after its genesis tombstone, or its birth log is still in flight | `{C,1} -> W`, birth unknown | never-born, or born once the birth lands | as rows 1, 5 and 6 at slot `{W,1}`: the birth is adopted or refused; `life_epoch` stays `C` | safe rows (first record is a tombstone); creation step to add |
| 14 | `DROP` terminal in flight at an unclean takeover | unknown | terminal or tombstone | terminal first: stream ends, `W` writes nothing. Tombstone first: `DROP` did not happen | `safe_selective_terminal`, `witness_terminal` |
| 15 | Re-creation of a dropped namespace | new life with a fresh incarnation ([§2.7](#genesis)), new prefix | independent chain from its own `life_epoch` | late requests of the old life are janitor debris | to add |
| 16 | Deposed writer meets a successor tombstone in its own next slot | tombstone | tombstone | conflict, lane `Closed`, permanent rejection | `safe_lateland` |
| 17 | Recovery walk races a base move and cleanup | newer base, cleaned prefix | `_ckpt` token changed | restart from the new base | `sab_walk_norecheck`, `witness_basemovedunderwalk` |
| 18 | Chain starts with an old-format seal | seal without a link | successor `{E+1,1}` | tombstone at `{E+1,1} -> W` with the backlink to the old seal | `safe_selective_legacy`, `witness_mixedchain` |
| 19 | GC folded a tombstone whose successor was absent, then it lands; cleanup may delete the tombstone | cursor on the tombstone | — | GC resumes at `{W,1}` by [§3.2](#cursor) | `sab_gccursor_naive`, `witness_cursordeleted`, `INV_GC_NO_HOLD` |
| 20 | Recovery tombstone of `W` unresolved, clean farewell, it lands later | `{X,k} -> W`, or the successor's own tombstone | chain grows by one tombstone; no table changes | adopted on conflict by the next lane step or recovery; in the final protocol `V` seals eagerly too | `safe_lateland_selective`, `witness_sealafterfarewell` |
| 21 | Recovery tombstone of `W` in flight while a superseded writer of `X` with a lying clock appends twice and publishes a snapshot, and cleanup deletes the slot | the tombstone in a reclaimed slot; a newer base | chain through the superseded writer's records | the `_ckpt` re-read after the create sees a new token and restarts ([§2.4](#writer-recovery), step 5); without it GC deletes a blob `W` serves, and `W`'s later appends are unreachable | `sab_seal_norecheck_zombie`, `sab_seal_norecheck_zombie_acked`, `safe_clocklies_gc` |
| 22 | A reader walks a life whose row GC erases and whose objects the janitor deletes; a delayed `completeCreation` re-creates `_ckpt`, a late create lands in a deleted slot | row gone, objects going, strays | a short chain or a stray record | the catalog re-read after the walk misses the incarnation: removed life, nothing installed, no corruption reported ([§3.1](#chain-rule)) | to add |

## 6. Format and the reader gate {#format}

| Item | Change |
|---|---|
| Generation constants (`CA/Formats/CasFormat.h:16`) | new `G_REF_CHAIN = 3`. `G_BUILD = 3` and the change points `{3,3}` for `RefLog` and `RefCkpt` arrive with the first generation 3 write ([§11](#staging)) |
| `RefLogTxn` and its header | optional `next_epoch`, wire key `!next_epoch` on the meta line, only on a tombstone. Through commit 4a every log a build writes keeps today's header, 2 with a `Move`, else 1 (`CA/Formats/CasRefLogFormat.cpp:294`): the numeric seal and the sequence-1 head with `!prev_epoch` after it included. Only a transaction with `!next_epoch` gets header 3, and only tests build one. From commit 5: header 3 iff the transaction carries `!next_epoch` or `!prev_epoch`, so every tombstone and every head after one; any other log as today |
| Structural grammar (`:230-260`) | header 3: backlink epoch strictly lower, adjacency dropped; a tombstone must carry `!next_epoch` above its own epoch. Header 1 or 2: as today, and `!next_epoch` is rejected |
| `_ckpt` (`CA/Formats/CasRefCkptFormat.cpp:95-127`) | the decoder accepts 1 to 3 and rejects the old fields under header 3. The encoder keeps the old body and header through commit 4a; from commit 5 it always writes header 3 and the two fields of [§2.5](#ckpt) |
| Pool floor | every writable mount raises `min_reader_generation` to 3. The raise moves from `Pool::open` (`CA/Pool/CasPool.cpp:598-610`) into `mountWritable`: after the claim and before the renewal-window check samples the clock (`:931`, `renewal_window_fits` `:944-963`), so that check counts the raise's time, and before `armMountFence` (`:966`) and the workers (`:979-980`). `openForDecommission` (`:1057-1075`) skips it today and writes generation 3 objects under this design: the tombstone before its terminal append. A self-remount (`:1574`) follows a mount that raised it. This change keeps the WARNING line of the raise, now naming floor 3. A `min_reader_generation` column in `system.cas_mounts` and a `cas_log` event are CAS-410's deliverable for both raises and are not part of this change. It ships without a switch: one from CAS-410 needs its own design, because commit 7 removes the old append rule |
| Mount lease | additive optional `last_unclean_epoch`; unknown keys are skipped by older decoders |
| Stamp | below header 3, the runtime publishes the generation 3 body (one `PUT`, once per namespace) before its first tombstone or append. Recovery stamps by [§2.4](#writer-recovery), step 3. The lane stamps conditional on the `_ckpt` token its installed recovery accepted, which the runtime keeps from install; a mismatch or an unresolved write sends the lane to `NeedsRecovery`. A new namespace is created stamped |
| Later fields | a future log field (the block id of CAS-393) needs its own gate, an unknown `!` key or a generation, and cannot go into `_ckpt`, which holds two fields. An unknown `!` key is `UNKNOWN_FORMAT_VERSION` (`CA/Formats/CasTextFormat.cpp:315-324`); the header is read first |

| Older reader | What stops it |
|---|---|
| 26.6 or old 26.8, new mount or read-only open | pool floor 3 (`26.6:CA/Formats/CasPoolMetaFormat.cpp:155`) |
| 26.6 GC leader, already mounted | `_ckpt` header 3 fails `checkCompatibility` (`26.6:CA/Formats/CasTextFormat.cpp:416`), the namespace is recorded undecodable (`26.6:CA/Gc/CasGc.cpp:1751-1763`) and the pass suppresses every destructive step (`26.6:CA/Gc/CasGc.cpp:3503-3509`) |
| Old 26.8 GC leader, already mounted | same path: `CheckpointUndecodable` hold, nothing folded for the namespace (`CA/Gc/CasGc.cpp:2820-2836`), destruction suppressed (`:3541-3547`) |
| Old writer of the same server root | cannot coexist: one lease per root, and it cannot remount |
| Relink between versions | off in both directions already (`docs/en/antalya/cas/configuration.md:350`) |
| Not stopped, accepted: mixed versions on one pool are unsupported (`:348`) | an old GC round that sampled `_ckpt` before the stamp finishes with that cut and its planned cleanup; an old read-only view keeps serving the table it recovered |

## 7. Deferred: the catalog read of a positive append {#deferred}

Positive and terminal appends keep their catalog `GET` (`CA/Pool/CasRefLedger.cpp:3612`, `:3581`). Removing it requires:

- **Order.** The terminal record comes first, not `Removing` as today; then a late `beginRemoving` is benign. A positive
  item built after the terminal emits a birth ([§2.7](#genesis)); the flush must refuse it without the read.
- **Completer.** GC's erase (`CA/Pool/CasRefCatalog.cpp:466-475`) accepts a `Live` row with `cleanup_evidence` and no
  hold, without a `Live -> Removing` write. The writer's `beginRemoving` stays the fast path for re-creation.
- **Dead life.** `_ckpt` is re-creatable: its deletion proves nothing. Guard: the catalog re-read ([§3.1](#chain-rule)).
- **Decommission.** Its cut recheck (`CA/Tools/CasDecommission.cpp:370-378`) and accounting (`:455-464`) take a selected
  life that GC moved or erased, and a `Live` row whose terminal drop succeeded in this run, as pending GC.
- **Older builds** cannot finish a terminal stream under `Live`: `DROP` returns early (`CA/Pool/CasRefLedger.cpp:5511`),
  `refuseTerminalLife` blocks creation, GC selects only `Removing` rows (`CA/Gc/CatalogLifecycleReconciler.cpp:33`).

## 8. Residual risks {#risks}

| # | Risk | Precondition |
|---|---|---|
| 1 | A superseded writer acknowledges an append that no reader finds | its `CLOCK_BOOTTIME` undercounts the pause, and cleanup already deleted the tombstone of its slot (`M/CaRefLogOnlyAppendCore_res_clocklies.cfg`) |
| 2 | A lost log above the base is read as the end of history | an internal defect deletes a log above the base; external deletion is out of scope |
| 3 | No blob is reclaimed in the whole pool while one namespace cannot be walked to its end (CAS-31) | a writer that outruns the walker until the intake slice ends, round after round |
| 4 | A reading epoch leaves an object | three cases: the eager tombstone of [§4](#eager), once per namespace; a tombstone and one removal, when it inherits stale precommits ([§2.4](#writer-recovery)); the `_ckpt` stamp and an eager tombstone at the first touch after the upgrade, as old bodies lack the field |
| 5 | GC reads grow with the number of quiet namespaces | 1 extra `GET` per quiet namespace per round that is not deferred; 2 on the cold path (leader change, restart, `REBUILD`). The `_ckpt` read cannot go: it gives `S`, which moves without a new log |
| 6 | An old read-only view serves a stale table (accepted) | it recovered the namespace before the stamp and is never restarted |
| 7 | A stray log sits below the base | any late create that lands in a slot cleanup reclaimed, with correct clocks too. It is never folded ([§3.2](#cursor)), cleanup deletes it, and it is never a witness: witnesses are ids above the probed key (`CA/Gc/CasGc.cpp:2731`) and a stray lies below it |
| 8 | Every recovery reads the catalog once more | the confirmation of [§3.1](#chain-rule): one whole-catalog `GET` per walk that installs, on top of the one or two of a cold admission |
| 9 | The row of an erased life returns as `Creating` (accepted; no catalog stamp) | all of: a 26.6 writer, which mints one incarnation per call and resends an ambiguous catalog write (`26.6:CA/Pool/CasRefCatalog.cpp:687-698`, `26.6:CA/Backend/CasRequests.cpp:1077-1081`, `:1119-1135`); one copy of that request undelivered across the namespace's creation, the stop of that writer, the upgrade, the namespace's `DROP`, the GC erase and a janitor pass; a pool catalog then byte-identical to its state before the namespace was created, under a content-derived ETag (the erase restores the bytes, `CA/Pool/CasRefCatalog.cpp:530-533`). Then a reader that walked the life sees `Creating` and reports it removed ([§3.1](#chain-rule)). That test costs no reader, as none walks a `Creating` life: write admission resolves only a `Live` row (`CA/Pool/CasRefLedger.cpp:1461`), `DROP` cancels a stalled creation without a walk (`:5456-5483`), read admission (`:748`) and the probe (`:5383`) do not recover it, fsck (`CA/Tools/CasFsck.cpp:503`), the sweep (`CA/Gc/CasOrphanManifestSweep.cpp:630`, `:856`) and GC (`CA/Gc/CasGc.cpp:253`) skip the row. Nothing in GC cancels it either, so the row and what the janitor left of the life stay until a `DROP` of that name cancels the creation. A writer that opens the same name instead completes it (`CA/Pool/CasRefLedger.cpp:1497-1525`): the life is `Live` again, and its recovery and GC's fold read what the janitor left, a stray included |

## 9. TLA+ obligations {#tla}

`M/CaRefLogOnlyAppendCore.tla` (39 rows in `M/run_reflogonlyappend.sh`) models the chain, both tombstone rules with the
marker, a walk that re-reads the base after its absent read and after a tombstone create, a snapshot publication across
a takeover or from a superseded writer, a late recovery tombstone, and a GC cursor back-chained from the snapshot's
epoch. Results: `M/CaRefLogOnlyAppendCore_RESULTS.md`, regenerated from the battery on the current model. Expected:

- Green: the `safe_` rows. `safe_clocklies_gc` (lying clock, cleanup, selective rule, no late landings) is a long
  exhaustive run in progress; `safe_clocklies_gc_4txn` and `safe_clocklies_gc_lateland` are bounded searches.
- Red on the invariant the runner names: the `sab_` and `res_` rows. `sab_seal_norecheck_zombie`
  (`INV_NO_DELETE_SERVABLE`) and `sab_seal_norecheck_zombie_acked` (`INV_LIVE_ACKED_REACHABLE`) drop step 5 of
  [§2.4](#writer-recovery) and need a lying clock. The `witness_` rows show that a schedule is reachable.
- Not in the model: the catalog, a second physical life, the janitor, the marker as a lease field, GC rounds. To add:

| # | Addition | Expected rows |
|---|---|---|
| 1 | Genesis follows the uniform rule and the safe rows already check it: the empty chain at `<<1,1>>` is sealed by the first later epoch that writes. Remaining: creation as a step that sets `life_epoch` once, the birth as a record kind, an old-format stream whose `life_epoch` was raised | safe with `PROP_MONOTONE`; sabotage "birth raises `life_epoch`" violates it |
| 2 | The marker as a value copied through lease bodies, starting from a body without it, published with the live epoch at a self-remount | safe; `sab_selective_immediate` stays red; sabotage "marker published after the epoch" |
| 3 | GC rounds with the destructive gate, the intake slice, the remembered successor and a consumer whose cursor lags | sabotage "cut-off counts as proven" violates `INV_GC_CUT_SOUND` |
| 4 | An old reader bounded by a frozen frontier; the stamp | safe with stamp-first; sabotage "append before the stamp" lets the old GC delete |
| 5 | The catalog, a second physical life, the janitor, a delayed `completeCreation`, an older build's resent catalog admission, late requests of the first life; the catalog confirmation after a walk ([§5](#crashes) rows 15 and 22, [§8](#risks) row 9) | safe; sabotage "no confirmation" lets a reader interpret a stray |
| 6 | The `_snap` create as a request separate from its `_ckpt` publish; more than one travelling copy of a request | safe |

Every new or rerun model carries a table "action -> store requests it stands for" (CAS-375).

## 10. Test plan {#tests}

Tests are written first. A `LOGICAL_ERROR` expectation is a death test in a `*DeathTest` suite.

| Suite | Test | Pins |
|---|---|---|
| `T/gtest_cas_ref_writer.cpp` | `CASRefWriterAppendLane.PositiveAppendIsOneCatalogReadAndOneCreate`, `.RefRemovalIsOneRequest`, `.LostAdmissionAfterDurablePutNeedsRecoveryAndIsNotAcknowledged`, `.DurableUnacknowledgedAppendIsReplayedByRecovery` | request counts 2 and 1, no `_ckpt` request; commit rule; replaces `CommittedChunkPublishesFrontierBeforeInstallAndAck` (`:1450`), `:1598` and `:1693` |
| | `CASRefWriterTombstone.FirstAppendOfAnEpochWritesTombstoneThenHead`, `.BacklinkSkipsEpochsThatOnlyRead`, `.AmbiguousTombstoneWedgesAndBlocksTheHead`, `.TombstoneOnlyEpochIsClosedAtItsFirstSlot`, `.LatePredecessorAppendIsAdoptedAndTheTombstoneMovesOneSlot`, `.NewerTombstoneClosesTheLane`, `.TerminalOccupantEndsTheStream`, `.DetachedRuntimeInARemountWindowWritesNoTombstone`, `.LateRecoveryTombstoneOfAnEarlierBuildIsAdopted`, `.LaneStampMismatchNeedsRecovery` | keys, `!next_epoch`, backlink, order of checks; [§5](#crashes) rows 10, 2, 1, 5, 16, 14 and 20; [§6](#format) |
| | `CASRefWriterRecovery.CleanPredecessorCostsSixReadsAndNoWrite`, `.UncleanPredecessorSealsBeforeInstall`, `.EagerRuleSurvivesACleanEpochThatNeverReadTheNamespace`, `.StampsCkptBeforeTheFirstNewWrite` | request count and order, no object for a reading epoch; [§4](#eager), transitivity |
| | `CASRefWriterRecovery.RestartsWhenTheBaseMovesUnderTheWalk`, `.RestartsWhenTheBaseMovesUnderTheTombstoneCreate`, `.FollowsOldSealsByPlusOneAndLinksAfterThem`, `.LifeErasedUnderTheWalkIsRemovedNotInstalled`, `.AbsentCkptOfAnErasedLifeIsNotCorruption`, `.CreatingRowIsNotAConfirmation`, `.PresenceProbeOverAnErasedLifeDoesNotThrow` | rows 17, 21, 18 and 22; [§3.1](#chain-rule) |
| | `CASRefWriterGenesis.BirthInALaterEpochSealsTheCreationSlotAndWritesNoCkpt`, `.LateBirthOfAPredecessorIsAdoptedOrRefused`, `.EagerRuleSealsANeverBornStream`; `CASRefWriterSnapshotPublish.GenerationThreeBodyHasTwoFields`, `.ReadingEpochDoesNotPublish` | [§2.7](#genesis), row 13, request count; [§2.5](#ckpt), [§2.4](#writer-recovery) |
| `T/gtest_cas_ref_epoch_seal_format.cpp`, `T/gtest_cas_format.cpp` | `NextEpochMustExceedOwnEpoch`, `NextEpochOnlyOnATombstone`, `HeaderThreeTombstoneRequiresNextEpoch`, `SparseBacklinkNeedsHeaderThree`, `ReadCeilingTwoRejectsHeaderThree`, `BacklinkHeadKeepsItsHeaderBeforeTheWriterCommit` (commits 1 to 4a; replaced at commit 5 by `BacklinkHeadIsHeaderThree`); `RefLogAndRefCkptHistoriesRecordGenerationThree` (commit 5, reworks `T/gtest_cas_format.cpp:47-79`) | grammar and gate (`setReadCeilingForTest`); [§6](#format), [§11](#staging) |
| `T/gtest_cas_ref_statemachine.cpp` | `ReplayRejectsHeadThatDoesNotNameItsTombstone`, `ReplayRejectsRecordAfterTombstoneInTheSameEpoch`, `TombstoneIsLegalBeforeBirthAndRefusedAfterTerminal`, `BirthAfterTerminalIsRefused` | reciprocity, genesis grammar |
| `T/gtest_cas_ref_ckpt.cpp` | `DecodesGenerationsOneToThree`, `HeaderThreeRejectsOldFields`, `IdenticalSkipComparesGeneration`, `DurableLifeEpochIsNeverReplaced`, `OnlyCreationCreatesAnAbsentCkpt` | [§6](#format), [§2.7](#genesis), [§2.5](#ckpt) |
| `T/gtest_cas_gc_bounded_walk.cpp` | `QuietNamespaceCostsCkptAndOneProbeOnceTheSuccessorIsRemembered`, `ColdCursorCostsOneMoreRead`, `ARoundStopsAtItsFirstAbsentKey`, `SliceCutLeavesTheNamespaceUnprovenAndKeepsItsCursor`, `PassOneIsNeverCutAndPassTwoRotatesByRound`, `TerminalRecordProvesTheFrontierWithoutAProbe` | request counts 2 and 3; cut, [§3.4](#budget); replaces `:231`, `:280` and `:388` |
| | `CursorOnATombstoneResumesAtTheLinkedEpoch`, `CursorOnAnOldSealResumesAtTheNextEpoch`, `CursorBelowTheSnapshotIsNeverRead`, `StrayAtAReclaimedCursorKeyIsNotFolded`, `StrayAfterAReclaimedTombstoneIsNotFolded` | rows 7, 8 and 19, [§3.2](#cursor) |
| `T/gtest_cas_orphan_manifest_sweep.cpp`, `T/gtest_cas_fsck.cpp`, `T/gtest_cas_gc_rebuild.cpp`, `T/gtest_cas_inspect.cpp` | `RemovalAboveTheCursorAcrossASkippedEpochStaysProtected`, `RemovalWalkDoesNotTrustAReclaimedCursorKey`, `LaggingSweepRestartsOnceWhenItsCursorMoved`, `SweepNamespaceSkipsALifeErasedUnderItsWalk`; fsck `LifeErasedDuringTheScanIsJanitorPending`, `LateRecheckOfAnErasedLifeIsNotDangling`; one test each for a chain with a skipped epoch | [§3.3](#consumers), [§3.1](#chain-rule), [§3.2](#cursor) |
| `T/gtest_cas_pool.cpp` | `ClaimSetsLastUncleanEpoch` (one case per row of [§4](#eager)), `EveryBodyWriterKeepsOrResetsIt`, `SelfRemountRecoveryUsesTheRemountClaimsMarker`, `DecommissionMountRaisesThePoolFloor` | marker; [§6](#format) |

- Integration, as in `test_cas_ref_move_compat`: older servers refuse to mount after the stamp and an old GC holds; a
  kill between tombstone and head in `test_cas_insert_fault_recovery`. Stateless (`add-test`): store requests of one
  `INSERT` and one `OPTIMIZE`. Measurement: the A/B of CAS-487 again (`INSERT`: 25 requests today, 19 by arithmetic).

## 11. Staging {#staging}

Each commit builds and passes alone; readers precede writers. **Generation 3 is advertised by the commit that writes
it.** `G_BUILD` is the default read ceiling (`CA/Formats/CasFormat.cpp:88-92`) and thereby pool admission
(`CA/Formats/CasPoolMetaFormat.cpp:155`). A build of commit 1 or 2 with ceiling 3 would open a generation 3 pool, and
its GC would take a `_ckpt` without `committed_through` for a proven empty frontier (`CA/Gc/CasGc.cpp:2838-2841`).

- Commits 1 to 4a keep `G_BUILD = 2`: production refuses header 3 (`CA/Formats/CasFormat.cpp:94-100`) and a floor-3 pool
  at mount, like any older build; tests reach the new codecs and readers through `setReadCeilingForTest(3)` (`:83-92`).
- The change point `{3,3}` waits for commit 5: `:36` asserts the last entry's generation `<= G_BUILD`, and `:35` ties it
  to `G_REF_MOVE`. Nothing reads the history at decode time (`T/gtest_cas_format.cpp:44`). Commit 5 sets `G_BUILD = 3`,
  appends `{3,3}` to `REF_LOG_HISTORY`, gives `RefCkpt` its own history and retargets the assertion.

| # | Commit | State after it | Product / test lines, estimated |
|---|---|---|---|
| 1 | Format: `G_REF_CHAIN`, `!next_epoch` codec, grammar by header, `_ckpt` decode of 1 to 3. The encoders keep today's headers for every log and `_ckpt` a build writes ([§6](#format)) | nothing writes or admits generation 3 | 200 / 350 |
| 2 | Replay reciprocity, genesis and terminal grammar, successor function, `walkRefChain`; read-only recovery, fsck, inspect, `REBUILD`, orphan sweep use it; fsck and `sweepNamespace` confirm the catalog after their walks | readers handle both formats under the test ceiling; writers unchanged | 480 / 750 |
| 3 | GC intake: cut at the first absent key, cursor resolution with the remembered successor, intake slice; a `_ckpt` without `committed_through` no longer proves a frontier (`CA/Gc/CasGc.cpp:2838-2848`) | GC no longer needs the frontier | 380 / 650 |
| 4 | `last_unclean_epoch` in the lease body, every body writer, published with the live epoch | marker recorded, no consumer | 150 / 300 |
| 4a | Refactoring only: writer recovery reads through `walkRefChain`; frontier publishes, numeric seals and the one-unfrontiered-successor check (`CA/Pool/CasRefLedger.cpp:1114-1140`) stay on top of the walk | behaviour unchanged | 150 / 100 |
| 5 | The complete protocol with a tombstone at every recovery: `G_BUILD = 3` and the change points, floor 3 raised in `mountWritable`, header 3 for `!prev_epoch`, generation 3 `_ckpt` created only at creation, stamp-first, one-request append and wedge, writer recovery by [§2.4](#writer-recovery) with no `_ckpt` publish, the forward-linked tombstone written by recovery (it names the live epoch; sparse backlink), uniform genesis, no one-unfrontiered-successor rule | the eager protocol (`safe_lateland_eager`, with correct clocks; no config runs the always-eager rule under a lying clock); the series can stop here | 650 / 1250 |
| 6 | The lazy lane step with its stamp, the selective rule, the consumer of the marker | protocol complete | 250 / 500 |
| 7 | Delete what [§2.6](#removed) lists as removed | no dead code | -350 / -300 |
| 8 | Docs: `docs/en/antalya/cas/` upgrade and architecture pages, `CA/Formats/README.md` | — | 150 |

## 12. Non-goals and limits {#limits}

Non-goals: [§7](#deferred); fewer appends per part (CAS-485); a combine window (CAS-287); the promote's manifest `GET`.

### 12.1 Inferred, not traced {#inferred}

- A snapshot or a tombstone re-encoded from the same state has identical bytes ([§2.3](#tombstone)). No old read-only
  view caches ref tables; a read-only open uses the ledger's read admission. No server root that wrote namespaces lacks
  a mount slot. No consumer assumes that a stream with logs is born. No log exists before the row is `Live`.
- On a pool of old writers every durable on-chain log above `committed_through` is history.
- A deposed GC leader's round fails the `gc/state` write. A `DROP` in the same process waits for the recovery of its
  runtime ([§3.1](#chain-rule)). Nothing follows a seal or recovery tombstone that lands after its epoch's clean
  farewell ([§4](#eager)). Not measured: how often a namespace's coverage cursor moves under one sweep walk
  ([§3.2](#cursor)), any request count or line estimate. No model other than `CaRefLogOnlyAppendCore` was read.

## 13. Backlog interactions {#backlog}

Checked against the code at `802077e5c1a` unless marked. Tasks not listed are untouched or only re-measured.

| Task | What the change does to it |
|---|---|
| CAS-366 Story B, CAS-451, CAS-459 | removed with their mechanism: `SuccessorSeal` / `Superseded` are in the base (`CA/Pool/CasRefCkpt.cpp:125-148`) and go at commit 7; `last_epoch_seal` leaves `_ckpt` |
| CAS-176.2, DRAFT-31, DRAFT-26, the `_ckpt` half of CAS-71.5 | removed or moot: no `_ckpt` request per flush remains |
| CAS-305, CAS-394 | CAS-305 closed: no seal per skipped epoch; its "epochs ended clean" record is `last_unclean_epoch`. CAS-394: its trigger (a failed `_ckpt` publish after a durable promote) goes; the durable-not-acknowledged window stays under CAS-395 |
| CAS-167 | delivered; its gates become measurements: ten-minute GCS soak with zero 429 on `_ckpt`, AWS with zero 409 |
| CAS-176.1, CAS-454, CAS-367, CAS-455 | not delivered by this change; [§7](#deferred) records what their change must solve. Of the reads of CAS-176 only the `_ckpt` read goes; the catalog read and the manifest re-read stay |
| CAS-387 (epoch out of the key) | rejected: a second format generation for a cost this design removes, and the orphan sweep compares a build's epoch with the cursor's (`CA/Gc/CasOrphanManifestSweep.cpp:546`) |
| Constrained, answered in the body | CAS-350, CAS-379, CAS-391 ([§2.3](#tombstone)); CAS-32, CAS-56, CAS-479 ([§2.4](#writer-recovery), [§2.5](#ckpt)); CAS-368, CAS-449 ([§2.7](#genesis)); CAS-315, CAS-448, CAS-457, CAS-86 ([§3.1](#chain-rule)); CAS-29.1 ([§3.2](#cursor)); CAS-29.10, CAS-92, CAS-31 ([§3.4](#budget)); CAS-179, CAS-355, CAS-364 ([§4](#eager)); CAS-410, CAS-393 ([§6](#format)); CAS-375, CAS-29.8 ([§9](#tla)). CAS-147: every unclean operator restart costs one tombstone per namespace touched |
| Texts, counters and docs (commits 7 and 8) | reword "publishing its checkpoint frontier was not confirmed" (`CA/Pool/CasRefLedger.cpp:2596`, `:4226`); retire `CASRefCheckpointFrontierRefused`, re-describe `CASRefCheckpoint{Published,IdenticalSkip,NotAdvanced}`, `CASRefRecoveryEpochSeal{ed,Adopted}`, `CASRefAppendSealRejected` (`src/Common/ProfileEvents.cpp:851`, `:882-885`, `:1014-1015`); rewrite the comments that justify the removed materialization wait by "recovery closes every dead epoch": `CA/Pool/CasPool.cpp:889-895`, `:1600-1608`, `CA/Pool/CasPool.h:276-283`, `CA/Pool/CasRefLedger.cpp:1964-1970`, `CA/Pool/CasServerRoot.cpp:1033-1036`; the reason is now [§4](#eager). Docs: `docs/en/antalya/cas/bucket-requirements.md:92` (`_ckpt` write rate), `docs/en/antalya/cas/architecture/manifests-and-refs.md:277-308` (recovery). Invariant 5 of `docs/superpowers/cas/AGENTS.md` forbids dropping a protocol step as an optimization: record there that this step goes by an owner decision. Amend the CAS-321 note "the acknowledgement follows the checkpoint" |
| Tests to re-target | `tests/integration/test_cas_gcs_relink_liveness` delays every `_ckpt` `PUT` and asserts the match (`test.py:30-32`, `:191`, `:224`): move the lever to `_log`. `test_cas_ref_snaplog`, `test_cas_gcs`, `test_cas_drop_pool_member`, and a sweep of the 46 gtest files of [§3.3](#consumers) |
