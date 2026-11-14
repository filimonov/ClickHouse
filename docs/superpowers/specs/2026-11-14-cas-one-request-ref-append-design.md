---
description: 'Design for a CAS ref-log append that is one object-store request. An ordinary append becomes one create-if-absent of `_log/{epoch,seq}`; a durable log on the chain is committed history; `_ckpt` is written only at namespace birth and at snapshot publication. An epoch change is recorded lazily by a tombstone (`EpochSeal`) that carries a forward link to the successor epoch, so an epoch that does not write a namespace leaves no object. Readers find the end of history by exact reads to the first absent key. Ships behind format generation 3. Part B removes the catalog read of a positive append.'
sidebar_label: 'CAS: one-request ref-log append'
sidebar_position: 116
slug: /superpowers/specs/cas-one-request-ref-append-design
title: 'CAS: one-request ref-log append with a lazy, forward-linked epoch tombstone'
doc_type: 'reference'
---

# CAS: one-request ref-log append — design {#cas-one-request-ref-append}

- Date: 2026-11-14. Status: design; nothing is implemented, built or run. Task: CAS-487; other tasks: [§13](#backlog).
  Part A is [§2](#writer) to [§6](#format). Part B ([§7](#part-b)) ships in this change, after it, in its own commits.
- Fixed inputs: the change ships with 26.8 as generation 3; the acknowledgement fence is the mount lease;
  no seal is retained for fencing; no snapshot is forced at an epoch change; external deletion of CAS
  objects and an untrusted stream `LIST` stay as today; objects of generations 1 and 2 stay readable.
- Source: worktree `cas-pr1-clean` at `802077e5c1a`; every `path:line` was read there. `26.6:` marks a path
  read with `git show altinity/antalya-26.6:<path>`. A bare `:line` repeats the last file named.
  "Inferred" marks a statement that was not traced in the code; [§12](#limits) lists all of them.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `M/` = `docs/superpowers/models/` in the `master` worktree.
- Terms. **Tombstone**: a ref-log transaction whose only op is `EpochSeal`. **Chain**: the logs reachable
  from the snapshot base by the successor rule of [§3.1](#chain-rule). **Chain epoch**: the epoch of the
  first absent key of the chain. **Live epoch**: the writer epoch of the mount that holds the lease.

## 1. Decision {#decision}

1. **Commit rule.** An ordinary append is one `PUT _log/{epoch,seq}`, create-if-absent. A durable log on
   the chain is committed history. The append path reads nothing and does not touch `_ckpt`.
2. **Chain.** History is the snapshot named by `_ckpt`, then the logs in successor order. The first absent
   key is the end. A `RemoveNamespace` record is an absorbing end.
3. **Lazy tombstone.** A writer of epoch `W` whose chain epoch is `X < W` creates, before its first append,
   a tombstone in the next slot of `X` with the critical field `!next_epoch = W`. `{W,1}` names exactly
   that tombstone in `!prev_epoch`/`!prev_seq`. Numeric epochs between `X` and `W` get no object.
4. **Selective eager tombstone.** Recovery writes the same tombstone before it installs the table iff
   `X <= last_unclean_epoch`, a new field of the mount lease body ([§4](#eager)).
5. **Fence.** The acknowledgement fence is the mount lease, checked after the response and again inside
   the install hold. Cleanup deletes a tombstone like any covered log.
6. **`_ckpt`.** Written at creation (`life_epoch`, set once, [§2.7](#genesis)) and at snapshot publication
   (`checkpoint_snapshot_id`). A generation 3 body has these two fields only.
7. **Readers.** One shared walk finds the end by exact reads ([§3](#readers)); GC cuts at its first absent key.
8. **Gate.** Generation 3: pool floor, `_ckpt` header, and the header of every log that carries a chain
   link. The stamp of a namespace's `_ckpt` precedes its first log written under the new rule.
9. **Part B.** `DROP` commits the terminal record first, GC completes the catalog row, appends read no catalog.

Store requests, warm namespace, one writer, counted from the code, not measured. The ref lane holds one
request per flush instead of four, and `_ckpt` stops being a hot key (CAS-167):

| Operation | Today | Part A | A + B | Today's requests |
|---|---|---|---|---|
| Positive append (new binding, `Move`, `SetPublishedAt`) | 4 | 2 | 1 | catalog `GET` `CA/Pool/CasRefLedger.cpp:3612`, log `PUT` `:3982`, `_ckpt` `GET` + `PUT` by `readModifyWrite` `:4217` |
| Ref removal (`OwnerTransition` without a new binding) | 3 | 1 | 1 | not positive, `:3550-3554` |
| Terminal `RemoveNamespace` append | 4 | 2 | 1 | catalog `GET` `:3581` |
| First append of an epoch in a namespace, extra | 0 | +1 | +1 | the tombstone `PUT` |
| New part without blobs | 14 | 8 | 5 | manifest `PUT` `CA/Pool/CasPartWriteTxn.cpp:696`, precommit `:842`, manifest `GET` `:873`, promote `:1061`, `Move` |
| Writer recovery, quiet namespace, one dead epoch, snapshot base | 12, 2 writes | 5, 0 writes | 5 | `CA/Pool/CasRefLedger.cpp:954-1386`; 6 with 1 write under the eager rule |
| GC intake, quiet namespace, per round that is not deferred | 1 | 2 | 2 | `_ckpt` `GET`; new: plus the successor probe; 3 on the cold path ([§3.2](#cursor)) |
| Snapshot publication | 3 | 3 | 3 | `_snap` `PUT`, `_ckpt` `GET` + `PUT`, `:4821`, `:4872` |

What is given up:

- The store-side refusal of a superseded writer's append in a reclaimed slot ([§8](#risks), row 1).
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

- `publishCkptContribution` (`:4217`) and its outcome arms (`:4229-4253`) are removed. Nothing replaces them.
- A log that is durable when a recheck fails is history and is never acknowledged: `Wedged` (step 4) or
  `NeedsRecovery` (step 5), today's texts (`:3927-3956`). No id is allocated while an attempt is held.

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

- **Who decides the epoch.** `flushRefBatch`, at the place of today's deposed-lane check (`:3091-3093`), before
  any preview (`:3360`, `:3388`) and id allocation (`:3744`): no candidate is built from a table recovery changes.
- **Order of checks.** Read the live epoch once; re-check `may_mutate` and the superseded flags; compare. A
  self-remount publishes the new epoch before it detaches the old runtimes (`CA/Pool/CasPool.cpp:1632-1646`; CAS-350).
- **Rule.** Let `X` be the chain epoch and `W` the live epoch.
  - `X = W`: proceed. `X > W`: this writer is deposed; the lane goes `Closed` with today's rejection.
  - `X < W`: create a tombstone at the chain's next slot with `!next_epoch = W`. If that slot is sequence 1
    (the previous record is itself a tombstone naming `X`), the new tombstone carries the backlink to it.
- **Mechanics.** A lane attempt like an append: armed, sent with `Retry::standard`, installed after the
  recheck. Installing it sets the open transition `{seal id, next epoch}`, which replaces
  `RefTableRuntime::last_epoch_seal`; `chainLinkFor` (`CA/Pool/CasRefLedger.cpp:146-151`) names it iff the epochs match.
- **Ambiguity.** An unresolved tombstone leaves the lane `Wedged`: no id is allocated, so `{W,1}` waits.
- **Conflict on the lane.** Own bytes: install (the encoding is deterministic, so an earlier tombstone of
  this writer is own bytes). A tombstone naming a later epoch: `Closed`. Any other readable occupant: the
  lane clears the attempt, so recovery's retained-attempt check (`:1098-1104`) does not see it, and goes
  `NeedsRecovery`. Under [§4](#eager) the lane seals only a drained epoch, so that case is unreachable there.
- **Conflict in recovery.** The walk applies the occupant and loops:

| Occupant | Recovery then |
|---|---|
| Ordinary record of `X` (a predecessor's late append) | adopted as history; chain epoch stays `X`; the tombstone goes one slot later |
| Tombstone naming `V > W` | chain epoch `V`; lane `Closed` |
| Tombstone naming `U < W` (a writer that stopped after its tombstone) | chain epoch `U`; the next tombstone goes to `{U,1}` with the backlink |
| `RemoveNamespace` | terminal; positive lane closed; no tombstone |

- The loop is bounded by `kRefRecoveryMaxSlotAttemptsPerEpoch` (`:1246`): one writer has one append in
  flight. The codec requires `!next_epoch` above the tombstone's own epoch. A terminal stream gets no
  tombstone; a never-born one gets one like any other ([§2.7](#genesis)).

### 2.4 Writer recovery {#writer-recovery}

1. Capture the pair (live epoch, `last_unclean_epoch`) once. Read `_ckpt` once and validate the base as
   today (`CA/Pool/CasRefProtocol.cpp:944-1030`). An absent or undecodable `_ckpt` of a `Live` life stays
   `CORRUPTED_DATA` (`CA/Pool/CasRefCkpt.cpp:180-182`; a hold in GC, `CA/Gc/CasGc.cpp:2820-2836`): nothing is
   installed, so no tombstone or append enters that life (CAS-479).
2. Walk the chain from the base's successor, or from `{life_epoch,1}`, to the first absent key ([§3.1](#chain-rule)).
   A retained attempt whose slot holds foreign bytes stays `CORRUPTED_DATA` (`CA/Pool/CasRefLedger.cpp:1098-1104`).
3. Re-read `_ckpt`. If its token differs from step 1, restart. This replaces the final equality check at
   `:1382-1386` and is what makes an absent read trustworthy against cleanup ([§3.1](#chain-rule)).
3a. Stamp: if the `_ckpt` header is below 3 and a write follows, publish the generation 3 body, conditional
   on the step 3 token; a mismatch restarts. A recovery that writes nothing leaves the stamp to the lane.
4. If the stream is not terminal and the chain epoch `X < W` is at or below `last_unclean_epoch`: create
   the tombstone in the walk, with today's code at `:1244-1350` minus its `_ckpt` publishes. A conflict
   adopts the occupant and loops. An unresolved create fails retry-later; nothing is installed (`:1318-1337`).
5. Install. Chain epoch above `W`: lane `Closed`. Terminal: positive lane closed. Otherwise `Ready`.

Recovery writes nothing after a clean predecessor. The read paths keep calling it (`:315`, `:390`, `:413`).

- The stale-precommit sweep appends through the lane (`:4942-4966`), so it takes the tombstone step: a
  reading epoch that inherits stale precommits writes a tombstone and one removal.
- Snapshot publication is admitted only when the chain epoch equals the live epoch (one more term at
  `:4381-4388`). A reading epoch publishes nothing, as today (`T/gtest_cas_ref_writer.cpp:3252`). Accepted.

### 2.5 What `_ckpt` carries {#ckpt}

| Field | Generation 3 | Writer |
|---|---|---|
| `life_epoch` | kept; set once at creation, before the row turns `Live` | `completeCreation` `CA/Pool/CasRefCatalog.cpp:631`; the birth chunk no longer writes it |
| `checkpoint_snapshot_id` | kept; raised at snapshot publication | `CA/Pool/CasRefLedger.cpp:4872-4876`, after the `_snap` `PUT` |
| `committed_through` | gone | was written per append and per recovered record |
| `last_epoch_seal` | gone | was written by the sealer |

- The chain evidence of a snapshot is not in `_ckpt`: it is the same-id log (`CA/Pool/CasRefProtocol.cpp:831`)
  and, for a base at sequence 1, the tombstone its backlink names (`:833`). Cleanup keeps both. The base
  check adds one test, a forward-linked predecessor names the base's epoch, and drops the one at `:980-991`.
- In a body of generation 1 or 2 `committed_through` stays the exact frontier: a chain that ends below it
  is `CORRUPTED_DATA`. `IdenticalSkip` (`CA/Pool/CasRefCkpt.cpp:313-317`) compares the generation too.
- A publish whose `checkpoint_snapshot_id` is not above the durable one in id order writes nothing
  (`CA/Pool/CasRefCkpt.cpp:163`). That keeps out a superseded writer's snapshot of a table with strays: a
  stray exists only below a durable `S` of a later epoch ([§3.2](#cursor)).
- A generation 3 `_ckpt` is an authority: after cleanup the logs give back neither the base id nor
  `life_epoch`, so "delete the damaged object" is no repair (CAS-32, CAS-56).

### 2.6 Removed by Part A {#removed}

| Mechanism | Where | Fate |
|---|---|---|
| Per-append and per-wedge `_ckpt` publish, `SuccessorSeal` / `Superseded` outcomes | `CA/Pool/CasRefLedger.cpp:4217`, `:2589`, `CA/Pool/CasRefCkpt.cpp:125-148` | removed |
| Recovery `_ckpt` publishes, the one-unfrontiered-successor rule, the seal of every numeric epoch | `CA/Pool/CasRefLedger.cpp:1026-1061`, `:1356-1375`, `:1114-1140`, `++epoch` at `:1151`, `:1173`, `:1238`, `:1301`, `:1347` | removed |
| Absent-below-frontier and same-epoch-seal hole verdicts | `:1183-1219` | removed; kept only against a generation 1 or 2 `_ckpt` |
| `mergeCommittedThrough`, adjacency invariants of the codec | `CA/Pool/CasRefCkpt.cpp:39-58`, `CA/Formats/CasRefCkptFormat.cpp:76-91` | decode-only for old bodies |
| `nextRefLogIdWithinCommittedFrontier`, frontier-bounded hints | `CA/Pool/CasRefProtocol.cpp:911-942`, `CA/Pool/CasKeyReader.cpp:9-23` | replaced by the successor rule and a window |
| GC ceiling and the `committed_through` witness | `CA/Gc/CasGc.cpp:2872-2883`, `:2736-2737` | ceiling removed; the witness stays only for a generation 1 or 2 `_ckpt` |
| Birth-time `_ckpt` publish, `life_epoch` decrease refusal | `CA/Pool/CasRefLedger.cpp:3510-3515`, `:3834-3853`, `CA/Pool/CasRefCkpt.cpp:94-118` | removed ([§2.7](#genesis)) |

Call sites and the one-unfrontiered-successor rule go at commit 5 of [§11](#staging), dead code at commit 7.

### 2.7 Genesis {#genesis}

**Decision.** A never-born stream follows the rule of every other first write of an epoch. `life_epoch` is
set once, at creation, and names the first key of the stream: `publishCkpt` never replaces a durable value.

- Created in `C`, first written by `W > C`: tombstone `{C,1} -> W`, then the birth at `{W,1}` with the
  backlink. A birth in `C` itself goes to `{C,1}` with no backlink.
- Grammar: a tombstone is legal on a never-born stream and refused only after a terminal record
  (`CA/Pool/CasRefProtocol.cpp:393-412` today refuses both). A sequence-1 record carries a backlink iff the
  record before it is a tombstone. The lifecycle test at `:555-557` is replaced by that rule.
- Nothing else needs the birth epoch: the contextual grammar (`CA/Formats/CasRefLogFormat.cpp:262-278`), the
  walk start (`CA/Pool/CasRefCkpt.cpp:211`) and the GC first fold (`CA/Gc/CasGc.cpp:2799`) need the first key.
- Old-format streams: the first key is `{life_epoch,1}` with the value they have, raised or not. A born one
  holds its birth there without a backlink. A never-born one gets its first tombstone there. Nothing raises it.
- Precondition, met in the base (`CA/Pool/CasRefCatalog.cpp:302-307`, `:328-329`): every create decision mints
  a fresh incarnation, else a new life would share `_ckpt` and its set-once `life_epoch` with an erased one.

## 3. Readers {#readers}

### 3.1 One rule for the end of history {#chain-rule}

`walkRefChain` is the only code that decides where history ends. Every consumer in [§3.3](#consumers)
calls it or its successor function.

| Record at `{e,s}` | Successor |
|---|---|
| Ordinary | `{e,s+1}` |
| Tombstone with `!next_epoch = n` | `{n,1}` |
| Tombstone without the field (generation 1 or 2) | `{e+1,1}` |
| Contains `RemoveNamespace` | none: the stream ends here and no key after it is read |

- **Reads.** Exact `GET` by key, in successor order. Read-ahead inside one epoch is allowed, but only the
  result taken in order counts: an absent key that is not the next key on the chain proves nothing.
- **Validation.** Replay rejects a record after a tombstone that is not at its successor, and a head whose
  backlink is not that tombstone's id, in `RefTableState::applyTxnInPlace` next to the density check
  (`CA/Pool/CasRefProtocol.cpp:515`). GC, which does not replay, runs the same test on the two bodies.
- **End.** The first absent key on the validated chain, including an absent successor of a tombstone.
  Forward links strictly increase, so the walk terminates; its budget is in [§3.4](#budget).
- **Why an absent read can be trusted.** Cleanup deletes only logs strictly below the snapshot id it read
  from `_ckpt` (`CA/Pool/CasRefProtocol.cpp:824-836`), and that id only grows. A walk that starts at a
  base re-reads `_ckpt` after its absent read. An unchanged token means every cleanup so far used an id at
  or below the walk's base; a changed token restarts the walk. A walk from the GC round's own cursor needs
  no re-read: cleanup deletes only at or below it (`:829`).
- **The dead-life janitor** deletes objects of a life that a fresh catalog read does not name, page by page
  in listing order (`CA/Gc/CasNamespaceJanitor.cpp:75-86`, `:118-190`). New rule: it deletes that life's
  `_ckpt` by exact key before the first stream object. The `_ckpt` re-read then detects the janitor too, and
  a life without `_ckpt` fails closed (CAS-457). fsck keeps its re-read (`CA/Tools/CasFsck.cpp:323-335`, CAS-448).
- **A failed validation is not yet corruption.** The walk re-reads `_ckpt` first: a changed token restarts
  it, only an unchanged one is `CORRUPTED_DATA` (CAS-315). It frees its read-ahead window on a throw (CAS-86).

### 3.2 Where GC resumes {#cursor}

The GC fold cursor is the last folded id (`CA/Formats/CasFoldSealFormat.h:136`). It stores no record kind.

**Stray objects.** Cleanup deletes logs at or below the cursor and strictly below the snapshot id `S`
(`CA/Pool/CasRefProtocol.cpp:827-836`). A late create-if-absent can land in such a slot, with correct clocks
too; a writer whose lease check passes wrongly can go on to later slots of that epoch. **A key at or below
the cursor and below `S`, or after a deleted tombstone in its epoch, may hold a stray. No reader reads it.**

Successor of an inherited cursor `c`:

1. Warm path: the GC process remembers `(cursor, successor key)` from its own fold. If the durable cursor
   equals the remembered one, it probes the remembered key. Nothing is persisted.
2. `c >= S`, or no snapshot: cleanup cannot have deleted `c` and no create can have replaced it. Read `c`;
   its kind gives the successor.
3. `c < S`: do not read `c` and do not probe `{c.e,c.s+1}` first. Back-chain from the epoch of `S` through
   sequence-1 backlinks (`crossEpochFromSeal`, `:847-909`). A head names `c`: `c` was a tombstone and that
   head is the successor. The back-chain reaches the epoch of `c` without naming it: `c` was ordinary and
   the successor is `{c.e,c.s+1}`. Anything else is a hold.

- Step 3 reads only keys above the cursor that the chain leads to. They were never deleted, so their only
  occupant is the chain's record. A cursor below `S` has a successor: cleanup keeps the snapshot's same-id
  log (`:831`) and the tombstone a sequence-1 base names (`:833`), and a tombstone is never a snapshot id
  (`CA/Pool/CasRefLedger.cpp:4381-4388`, `CA/Pool/CasRefProtocol.cpp:972-978`). Id order is chain order.
- Steps 2 and 3 hold for the round that commits the cursor: cleanup runs only after its `gc/state` write
  (`CA/Gc/CasGc.cpp:806`, `:1532`). A consumer that reads the adopted seal itself, as decommission's sweep
  does (`CA/Gc/CasOrphanManifestSweep.cpp:626-627`), can lag: it re-reads the seal after its walk and restarts
  if the seal moved. An unfinished walk keeps every candidate.
- A quiet namespace costs `_ckpt` and one probe when warm, one more read in step 2 (leader change, restart,
  `REBUILD`). The `_ckpt` read cannot go: it gives `S`, which moves without a new log.

### 3.3 Consumers {#consumers}

Search: `grep -rEc` over `CA/` and `src/`, no output cut, for `committed_through`, `last_epoch_seal`,
`prev_epoch_seal`, `life_epoch`, `EpochSeal`, `crossEpochFromSeal`, `chainLinkFor`, `publishCkpt`, `readCkpt`
and the recovery entry points. Matches: 26 files in `CA/`, 46 under `T/`, `src/Common/ProfileEvents.cpp`.

| Consumer | Reads today | Change | Quiet namespace |
|---|---|---|---|
| Writer recovery | `_ckpt`, base, tail to the frontier, one successor, seals and `_ckpt` per dead epoch (`CA/Pool/CasRefLedger.cpp:911-1410`) | [§2.4](#writer-recovery) | 12 -> 5 |
| Read-only recovery | base, tail to `committed_through`, no probe (`CA/Pool/CasRefProtocol.cpp:1032-1105`) | `walkRefChain`; returns the last tombstone it saw, not `_ckpt`'s | +1 probe, +1 `_ckpt` |
| GC intake and round cut | `_ckpt` sampled at round start bounds the fold (`CA/Gc/CasGc.cpp:2449-2455`, `:2872-2883`) | cut = first absent key read in this round; nothing above it is folded even if it appears later; [§3.2](#cursor) for the start | 1 -> 2, cold 3 |
| Destructive gate | `frontier_proven` per namespace, all must hold (`:3541-3547`) | gate unchanged; a namespace is proven only by an absent key or a terminal record read in this round | none |
| GC cursor on a tombstone with an absent successor | cursor `+1` in the same epoch (`:2794-2800`), kind unknown (`:2851-2854`) | [§3.2](#cursor): resumes at `{n,1}`, never at `{e,s+2}`, and never trusts a key cleanup may have deleted | cold path only |
| Per-epoch accounting | contiguous runs (`:3296-3312`) | none: skipped epochs have no run | none |
| Cleanup planning | logs at or below the cursor and below the validated snapshot, minus the base's predecessor (`CA/Pool/CasRefProtocol.cpp:814-845`, `CA/Gc/CasGc.cpp:4050-4081`) | none; a tombstone and a stray are deleted like any covered log | none |
| Orphan manifest sweep | owner recovery (`CA/Gc/CasOrphanManifestSweep.cpp:256`), removal walk to `committed_through` (`:271`), exact `{E+1,1}` for a missing cursor (`:277-292`) | owner recovery uses `walkRefChain`; the removal walk resumes by [§3.2](#cursor) and no longer reads the cursor object below `S`; an incomplete walk keeps every candidate; `:546` unchanged | +1 probe |
| `REBUILD` | read-only recovery, cursor = greatest applied (`CA/Gc/CasGc.cpp:4823-4829`) | complete walk required; a cut-off fails the rebuild | +1 probe |
| fsck | stream audit to the frontier (`CA/Tools/CasFsck.cpp:312-420`), owner replay (`:628-645`), late recheck (`:133-140`) | both legs share one walk result from the base and read no key below it; the listing only attributes keys to lives (`:537-547`); "hole" is reported only below a generation 1 or 2 frontier or against a listed witness | +1 probe |
| Inspect | prints the four `_ckpt` fields and the backlink (`CA/Tools/CasInspect.cpp:184-188`) | prints generation and `next_epoch`; old fields labelled as old-format; a log named by key is printed as stored (`:519-520`) and labelled "may be a stray" when it is below the base | none |
| Namespace janitor | catalog membership of a physical life (`CA/Gc/CasNamespaceJanitor.cpp:86`, `:118-156`) | none | none |
| Snapshot base validation | same-id log, predecessor seal, snapshot (`CA/Pool/CasRefProtocol.cpp:944-1030`) | [§2.5](#ckpt) | none |
| Relink confirmation | resident table, zero I/O (`CA/Pool/CasRefLedger.cpp:425-574`), slot check (`CA/Pool/CasPool.cpp:2154-2180`) | none; its premise, a complete recovered table, is kept by [§4](#eager) | none |

GC keeps the listing, the snapshot id and the carried hold as witnesses (`CA/Gc/CasGc.cpp:2731-2739`), and
`committed_through` only in an old body; it folds a log above that frontier, as old recovery adopts it too.

### 3.4 Bound of the walk {#budget}

- Expected, not guaranteed: a lane appends one record per round trip, the walker a window (`CA/Gc/CasGc.cpp:2895`).
- Hard bound: a fixed time slice for the whole ref intake of one round, in the janitor's pattern (`mono_ms_fn`
  plus a constant, `:430-447`; `kJanitorBudgetMs`, `:94`). `kRefIntakeBudgetMs = 60'000`, one default
  `gc_interval_sec` (`CA/ContentAddressedSettings.cpp:62`): a longer intake gains nothing over the next round.
- The slice starts at the first read of the intake. Pass 1 is never cut: every walk target gets its
  `_ckpt` read, its successor probe and one read window, in catalog order. That proves every quiet
  namespace. Pass 2 continues the unproven ones from index `round mod n`, wrapping, and starts no read
  after the slice ends, so a slice that always runs out does not starve the same namespaces.
- A namespace that is cut off keeps its cursor and is unproven for that round, which then suppresses
  every destructive step for the whole pool (`CA/Gc/CasGc.cpp:3541-3547`) and still folds and seals.
- CAS-29.10's rule "never cut the intake of every live life" is kept by pass 1; only pass 2 is cut, and it
  stops at the round deadline instead of the constant once CAS-29.10 lands.
- `gc_frontier_probe_budget` does not bound the walk: it caps probes of namespaces without a usable `_ckpt`
  (`:2493-2517`; default unbounded, `CA/Pool/CasPool.h:155`; CAS-92). CAS-29.1's probe is replaced by [§3.2](#cursor).

## 4. The selective eager tombstone {#eager}

**Why.** A request sent under a valid lease can land after the successor read its slot as absent. If it is
a removal, GC folds it and deletes a blob the successor still resolves from memory
(`M/CaRefLogOnlyAppendCore_res_readwindow_gcdelete.cfg`). The lease guards the acknowledgement, not the landing.

**Marker.** `last_unclean_epoch`, a new optional field of the mount lease body
(`CA/Formats/CasServerRootFormats.h:66-93`). The epoch object cannot carry it: it is written
(`CA/Pool/CasPool.cpp:689`) before the predecessor's end is classified (`CA/Pool/CasServerRoot.cpp:1097`).
The claim writes the first lease body with the classification in hand (`:1089-1100`):

| Predecessor body | New `last_unclean_epoch` |
|---|---|
| Clean farewell (`isCleanRelease`, `CA/Formats/CasServerRootFormats.h:83`) | the predecessor's value |
| `Fenced`, `UncleanObserved`, `UncleanUnsafe` | the predecessor's `writer_epoch` |
| Body without the field (older build) | the predecessor's `writer_epoch` |
| No slot | the claimer's epoch minus 1 |

- Writers of the body: the claim (`claimMount`, `CA/Pool/CasServerRoot.cpp:1004`, `:1089`); the renewer's
  `encodeBody` (`:1689`), used by `start` (`:1799`), the adoption write (`claim`, `:1718`) and every renewal;
  the farewell (`terminate`, `:2171`); `fenceOwnClaim` (`:1126`) and the GC fence (`:1403`), which copy the
  decoded body; the rejoin rewrite (`rejoinRetiredOwner`, `:602-667`), which sets the slot's `writer_epoch`.
  The renewer carries the value as a member set at `installRenewer`. A writer that forgets the field is
  safe: a body without it reads as the predecessor's `writer_epoch`, at least any inherited value.
- Publish point: the claim's value is published in the same step as the live epoch (`setLiveWriterEpoch`,
  `CA/Pool/CasPool.cpp:965`, `:1632`), before recoveries are cancelled and the fence is re-armed. Recovery
  captures the pair once (`CA/Pool/CasRefLedger.cpp:1602`). A self-remount uses the same claim
  (`CA/Pool/CasPool.cpp:1574`) over a body that is never clean, so a fresh runtime seals the old epoch's slot.
- The value never decreases: a claim only replaces a lower epoch (`CA/Pool/CasServerRoot.cpp:1037-1063`; CAS-364).
  A forced exit writes no farewell and must not (CAS-355 option a): the successor would skip the tombstone.

**Rule.** Recovery of a non-terminal stream with chain epoch `X < W` seals before install iff
`X <= last_unclean_epoch`. A never-born stream is included.

**Why it is enough.** Only the mount of epoch `X` creates ordinary records in epoch `X`. If
`X > last_unclean_epoch`, epoch `X` ended with a clean farewell: any other end is recorded by the next
claim, and the value never decreases. A farewell is written only after `drainRefLanesForShutdown` returned
true (`CA/Pool/CasPool.cpp:1126-1137`, `:1280-1288`, `CA/Pool/CasMountRuntime.cpp:1230`), which requires
every resident lane idle and none `Writing` or `Wedged` (`CA/Pool/CasRefLedger.cpp:2103-2161`; eviction
keeps non-`Ready` lanes, `:1918-1920`). So no append and no lane tombstone of `X` is in flight; one of an
epoch between `X` and `W` is drained too or raises the value above `X`. Unmounted epochs wrote nothing.
The rule is transitive because it compares the chain epoch, not the predecessor: an epoch that never read
the namespace leaves it at `X` and passes the value on (sabotage: `sab_selective_immediate`).

**What a clean farewell does not exclude.** A recovery tombstone whose create was unresolved
(`CA/Pool/CasRefLedger.cpp:1318`) is not a lane attempt and can land later. The farewell is not withheld.
A tombstone carries no table content, so a late one changes only chain geometry: the next lane step or
recovery adopts it on conflict, GC folds a record without edges, and no blob is deleted because of it
([§5](#crashes), row 20). This also covers commit 5, which seals at every recovery.

The drain ignores snapshot publications and plain and catalog requests (CAS-179). After a clean farewell a
`_snap` and its `_ckpt` publish can land (the base only grows; a walk restarts by its re-read), a catalog
write ([§7](#part-b)), and objects outside the ref chain. No append or lane tombstone can.

**When an epoch that only reads a namespace writes into it.** Three cases: (1) the eager tombstone, once per
namespace, when the chain epoch is at or below `last_unclean_epoch`; (2) a tombstone and one removal, when
it inherits stale precommits ([§2.4](#writer-recovery)); (3) the `_ckpt` stamp and an eager tombstone at
the first touch after the upgrade, as old bodies lack the field.

## 5. Crash and race table {#crashes}

`E` wrote last; `W`, `V` are later epochs; `n` is the last sequence of `E`. Configs: `M/run_reflogonlyappend.sh`.

| # | Point | Durable | Readers see | Next writer | Model |
|---|---|---|---|---|---|
| 1 | Stop after the tombstone, before `{W,1}` | `{E,n+1} -> W` | end at absent `{W,1}` | `V` creates `{W,1} -> V` with the backlink, then `{V,1}` | `witness_sealonlyepoch` |
| 2 | Tombstone `PUT` ambiguous | unknown | end before or after it | same mount: `Wedged`, same key and bytes; no `{W,1}` until resolved | `safe_lateland` |
| 3 | Tombstone in flight at an unclean takeover | unknown | as row 2 | `V` seals eagerly; first create wins, the other adopts or is `Closed` | `witness_lateseal`, `safe_lateland_selective` |
| 4 | Predecessor `PUT` lands before recovery reads the slot | `{E,n+1}` ordinary | it is history | recovery replays it; tombstone at `{E,n+2}` | `witness_adopt` |
| 5 | Predecessor `PUT` lands just before the tombstone create | `{E,n+1}` ordinary | it is history | create conflicts, adopt, tombstone one slot later | `witness_sealconflict` |
| 6 | Predecessor `PUT` arrives after the tombstone | tombstone | tombstone | predecessor gets a conflict: `Closed`, not acknowledged | `safe_lateland` |
| 7 | Predecessor `PUT` lands after cleanup deleted the tombstone | orphan log below the base | not on the chain | acknowledged only if its lease check passes wrongly ([§8](#risks), row 1); then it may write on in that epoch; GC folds none of it and cleanup deletes it | `res_clocklies`, `safe_clocklies_nocleanup`, `safe_clocklies_gc_4txn`, `sab_gccursor_probefirst_zombie`, `sab_gccursor_anyhead_zombie`; `safe_clocklies_gc` pending |
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
| 21 | Part B: stop after the terminal record, before `beginRemoving` | terminal, row `Live` | stream ended | owner: lane closed, creation waits; GC folds the terminal, its next drain writes `Removing` and erases | to add |
| 22 | Part B: terminal `PUT` of a `DROP` ambiguous | unknown, row `Live` | ended or not | lane `Wedged` and closed; no catalog write is sent; a retried `DROP` resolves the attempt | `safe_selective_terminal` |
| 23 | Part B: `beginRemoving` lands after a takeover, or races the GC write | row `Removing` | terminal stream, as before | no effect: the terminal preceded it; the exact-row replace lets one writer win | to add |
| 24 | Part B: an older build's `Live -> Removing` in today's order lands after the upgrade takeover | row `Removing`, no terminal | live stream | appends go on until the next cold admission refuses (`CA/Pool/CasRefLedger.cpp:5279`); a retried `DROP` completes; CAS-454 stays for that takeover | none |
| 25 | Part B: a cold admission recovers a life whose row GC erases and whose objects the janitor deletes during the walk | terminal, row gone, objects going | — | the walk ends before the first delete and finds the terminal, or its `_ckpt` re-read finds `_ckpt` gone: removed life, nothing installed ([§7](#part-b)) | to add |

## 6. Format and the reader gate {#format}

| Item | Change |
|---|---|
| `G_BUILD` (`CA/Formats/CasFormat.h:16`) | 3. New constant `G_REF_CHAIN = 3`. Change points `{3,3}` for `RefLog` and `RefCkpt` |
| `RefLogTxn` | optional `next_epoch`, wire key `!next_epoch` on the meta line, only on a tombstone |
| Log header | 3 iff the transaction carries `!next_epoch` or `!prev_epoch`; else as today (`CA/Formats/CasRefLogFormat.cpp:294`) |
| Structural grammar (`:230-260`) | header 3: backlink epoch strictly lower, adjacency dropped; a tombstone must carry `!next_epoch` above its own epoch. Header 1 or 2: as today, and `!next_epoch` is rejected |
| `_ckpt` (`CA/Formats/CasRefCkptFormat.cpp:95-127`) | the encoder always writes header 3 and the two fields of [§2.5](#ckpt); the decoder accepts 1 to 3 and rejects the old fields under header 3 |
| Pool floor (`CA/Pool/CasPool.cpp:598-610`) | the first writable mount raises `min_reader_generation` to 3. The raise from 2 to 3 takes what CAS-410 decides for the raise from 1 to 2: a `min_reader_generation` column in `system.cas_mounts`, a `cas_log` event, and the switch, if any. A switch must also keep the old append rule: the floor precedes the first generation 3 write |
| Mount lease | additive optional `last_unclean_epoch`; unknown keys are skipped by older decoders |
| Later fields | a future log field (the block id of CAS-393) needs its own gate, an unknown `!` key or a generation, and cannot go into `_ckpt`, which holds two fields. An unknown `!` key is `UNKNOWN_FORMAT_VERSION` (`CA/Formats/CasTextFormat.cpp:315-324`); the header is read first |

**Stamp before the first new write.** Below header 3, the runtime publishes the generation 3 body (one
`PUT`, once per namespace) before its first tombstone or append, conditional on the token recovery
validated ([§2.4](#writer-recovery), step 3a). A new namespace is created stamped. The floor is raised at mount.

| Older reader | What stops it |
|---|---|
| 26.6 or old 26.8, new mount or read-only open | pool floor 3 (`26.6:CA/Formats/CasPoolMetaFormat.cpp:155`) |
| 26.6 GC leader, already mounted | `_ckpt` header 3 fails `checkCompatibility` (`26.6:CA/Formats/CasTextFormat.cpp:416`), the namespace is recorded undecodable (`26.6:CA/Gc/CasGc.cpp:1751-1763`) and the pass suppresses every destructive step (`26.6:CA/Gc/CasGc.cpp:3503-3509`) |
| Old 26.8 GC leader, already mounted | same path: `CheckpointUndecodable` hold, nothing folded for the namespace (`CA/Gc/CasGc.cpp:2820-2836`), destruction suppressed (`:3541-3547`) |
| Old writer of the same server root | cannot coexist: one lease per root, and it cannot remount |
| Relink between versions | off in both directions already (`docs/en/antalya/cas/configuration.md:350`) |

A warm old reader can still do two things, both accepted, as mixed versions on one pool are unsupported
(`docs/en/antalya/cas/configuration.md:348`): an old GC round that sampled `_ckpt` before the stamp finishes
with that cut and its planned cleanup; an old read-only view keeps serving the table it recovered.

## 7. Part B: the catalog `GET` {#part-b}

**Decision.** `DROP` commits the terminal record before any catalog write, and GC completes the catalog
transition. A `Removing` row then implies a terminal record, and the positive append reads no catalog.
Rejected: a change of the catalog row at every takeover, which writes the pool-wide object each time.

- The only writer of `Live -> Removing` is `beginRemoving` (`CA/Pool/CasRefCatalog.cpp:433`), called only
  from `dropNamespaceImpl` (`CA/Pool/CasRefLedger.cpp:5572`). The `GET` at `:3612` is for a predecessor's late
  write and does not close that case: the write can land after it.
- New order in `dropNamespaceImpl`: close and drain the lane (`:5515-5526`); append the terminal record;
  then `beginRemoving`, best effort. A terminal that provably did not land reopens the lane; an ambiguous
  one wedges it. After a durable terminal nothing reopens, so the catalog write's outcome tracking
  (`:5565-5647`) goes. A `DROP` over a terminal stream under a `Live` row no longer returns early (`:5511`).
- A terminal record is final: replay refuses `NamespaceBirth` after it (allowed today,
  `CA/Pool/CasRefProtocol.cpp:359-367`), and recovery of a terminal stream closes the positive lane. Creation
  over such a life already waits (`refuseTerminalLife`, `CA/Pool/CasRefLedger.cpp:5295-5304`).
- Completer: GC, only when the writer did not. The fold of a terminal record earns `cleanup_evidence`
  whatever the row's state (`CA/Gc/CasGc.cpp:3399-3411`). The pre-fold drain of the next round (`:5308-5320`)
  then also selects a `Live` row. Precondition, all of: the row read in this drain is `Live` without a
  `creator`; the adopted parent seal holds `ref_lives[row.incarnation]` with `cleanup_evidence` and no
  `coverage.hold` (`CA/Gc/CatalogLifecycleReconciler.cpp:36-39`); the write replaces exactly that row
  (`CA/Pool/CasRefCatalog.cpp:427-429`) under the leader's admission, refreshed per attempt. It sets
  `Removing` and `removal_started_round`. The existing erase (`:459-476`) follows in the same drain.
- The GC write uses the catalog's hot-key path like `beginRemoving` (`casUpdateImpl`, `:192`, `:440`), batched
  with the erases once they move there (a direct replace today, `:531`; CAS-463, CAS-321.3, CAS-71.4). A
  refused write leaves the row for the next round and never aborts the fold (today the drain throws,
  `CA/Gc/CatalogLifecycleReconciler.cpp:121-124`).
- Decommission drops the victim's namespaces through the same `dropNamespace`
  (`CA/Tools/CasDecommission.cpp:380-397`). Its writer mounts through `mountWritable` as the victim's next
  epoch (`CA/Pool/CasPool.cpp:627-640`, `:983`), so the tombstone rule and [§4](#eager) apply unchanged. Its
  final accounting counts only `Removing` rows as pending GC; any other row warns and blocks the retirement
  (`CA/Tools/CasDecommission.cpp:455-466`, `:570-573`). It must also count a `Live` row of an exact life whose
  terminal drop succeeded in this run. Any other `Live` row still warns.
- A writer can recover a life while GC erases its row and the janitor sweeps it. With the janitor order of
  [§3.1](#chain-rule) the walk completes before any delete, or its re-read finds `_ckpt` gone: admission
  then re-reads the catalog and reports a removed life, not corruption. Without that order a walk could
  end at a deleted log, miss the terminal record and admit an append into a dead life.
- Old-order rows (`Removing`, no terminal record) exist after the upgrade: the resume path stays and appends it.
- Removed: both catalog reads of `commitRefChunk` (`CA/Pool/CasRefLedger.cpp:3579-3593`, `:3606-3636`); the
  local authorization (`:3565-3577`) stays. The cold presence probe treats a `Live` row as `Removing` (`:5383`, `:5407`).

## 8. Residual risks {#risks}

| # | Risk | Precondition |
|---|---|---|
| 1 | A superseded writer acknowledges an append that no reader finds | its `CLOCK_BOOTTIME` undercounts the pause, and cleanup already deleted the tombstone of its slot (`M/CaRefLogOnlyAppendCore_res_clocklies.cfg`) |
| 2 | A lost log above the base is read as the end of history | an internal defect deletes a log above the base; external deletion is out of scope |
| 3 | No blob is reclaimed in the whole pool while one namespace cannot be walked to its end (CAS-31) | a writer that outruns the walker until the intake slice ends, round after round |
| 4 | A reading epoch leaves an object | the three cases of [§4](#eager) |
| 5 | GC reads grow with the number of quiet namespaces | 1 extra `GET` per quiet namespace per round that is not deferred; 2 on the cold path |
| 6 | An old read-only view serves a stale table (accepted) | it recovered the namespace before the stamp and is never restarted |
| 7 | A stray log sits below the base | any late create that lands in a slot cleanup reclaimed, with correct clocks too. It is never folded ([§3.2](#cursor)), cleanup deletes it, and it is never a witness: witnesses are ids above the probed key (`CA/Gc/CasGc.cpp:2731`) and a stray lies below it |
| 8 | A table turns `Removing` and refuses writes at its next cold admission | an older build's catalog write of a `DROP` lands after the upgrade takeover (CAS-454) |

## 9. TLA+ obligations {#tla}

`M/CaRefLogOnlyAppendCore.tla` (36 rows in `M/run_reflogonlyappend.sh`) checks the chain, both tombstone
rules with the marker, old-format seals, a terminal record, reciprocal links, a walk with the base re-read,
a snapshot publication in flight across a clean or unclean takeover, a late recovery tombstone, and a GC
cursor that back-chains from the snapshot's epoch with a hold arm (`INV_GC_NO_HOLD`, `sab_gccursor_anyhead_zombie`,
`safe_clocklies_gc_4txn`). `safe_clocklies_gc` is pending. Crash rows 15, 21, 23 to 25 have no config. To add:

| # | Addition | Expected rows |
|---|---|---|
| 1 | Genesis follows the uniform rule and the safe rows already check it: the empty chain at `<<1,1>>` is sealed by the first later epoch that writes. Remaining: creation as a step that sets `life_epoch` once, the birth as a record kind, an old-format stream whose `life_epoch` was raised | safe with `PROP_MONOTONE`; sabotage "birth raises `life_epoch`" violates it |
| 2 | A snapshot publication by a writer that is not live but passes admission; ids compared at landing ([§2.5](#ckpt)) | safe; sabotage "length instead of id" |
| 3 | The marker as a value copied through lease bodies, starting from a body without it, published with the live epoch at a self-remount | safe; `sab_selective_immediate` stays red; sabotage "marker published after the epoch" |
| 4 | GC rounds with the destructive gate, the intake slice, the remembered successor and a consumer whose cursor lags | sabotage "cut-off counts as proven" violates `INV_GC_CUT_SOUND` |
| 5 | An old reader bounded by a frozen frontier; the stamp | safe with stamp-first; sabotage "append before the stamp" lets the old GC delete |
| 6 | Part B and a second physical life: catalog row, terminal before `Removing`, GC as completer, a late catalog write, late requests of the first life, the janitor's `_ckpt`-first order | safe; sabotages: today's order; a birth after a terminal record; logs before `_ckpt` |

The new model and every rerun model carry a table "action -> store requests it stands for" (CAS-375).
Existing models, inferred from result files and config names (only `CaRefLogOnlyAppendCore` was read):

- Retire for generation 3: `CaWriterEpochBackfillCore_*`; `CaRefTableSnapshotLogCore_frontier_*`,
  `_cur_lateappend*`, `_fix_a*`, `_fix_b*`.
- Rerun unchanged: `CaRefTableSnapshotLogCore_v9_safe*`, `_v9_flip_latepred*`; `CaGcDestructiveGateCore_*`.
- Change and rerun: `CaRefDeltaIntakeCore_v9_*` and its `sab_*`; `CaRefLaneCore_*`;
  `CaCasMountCore_v9_recoverygen`; `CaGcCondemnMarkerGate_fix_*`; `CaRefCleanupLicenceCore`.

## 10. Test plan {#tests}

Tests are written first. A `LOGICAL_ERROR` expectation is a death test in a `*DeathTest` suite.

| Suite | Test | Pins |
|---|---|---|
| `T/gtest_cas_ref_writer.cpp` | `CASRefWriterAppendLane.OrdinaryAppendIsOneCreateAndNoCkptRequest` | request count 2 (1 after Part B); replaces `CommittedChunkPublishesFrontierBeforeInstallAndAck` (`:1450`) |
| | `CASRefWriterAppendLane.RefRemovalIsOneRequest` | request count 1 |
| | `CASRefWriterAppendLane.LostAdmissionAfterDurablePutNeedsRecoveryAndIsNotAcknowledged` | replaces `:1598` and `:1693` |
| | `CASRefWriterAppendLane.DurableUnacknowledgedAppendIsReplayedByRecovery` | commit rule |
| | `CASRefWriterTombstone.FirstAppendOfAnEpochWritesTombstoneThenHead`, `.BacklinkSkipsEpochsThatOnlyRead` | keys, `!next_epoch`, backlink; [§5](#crashes) row 10 |
| | `CASRefWriterTombstone.AmbiguousTombstoneWedgesAndBlocksTheHead`, `.TombstoneOnlyEpochIsClosedAtItsFirstSlot` | rows 2 and 1 |
| | `CASRefWriterTombstone.LatePredecessorAppendIsAdoptedAndTheTombstoneMovesOneSlot` | row 5 |
| | `CASRefWriterTombstone.NewerTombstoneClosesTheLane`, `.TerminalOccupantEndsTheStream`, `.DetachedRuntimeInARemountWindowWritesNoTombstone` | rows 16 and 14; order of checks |
| | `CASRefWriterRecovery.CleanPredecessorCostsFiveReadsAndNoWrite` | request count, no object for a reading epoch |
| | `CASRefWriterRecovery.UncleanPredecessorSealsBeforeInstall`, `.EagerRuleSurvivesACleanEpochThatNeverReadTheNamespace` | [§4](#eager), transitivity |
| | `CASRefWriterRecovery.RestartsWhenTheBaseMovesUnderTheWalk`, `.FollowsOldSealsByPlusOneAndLinksAfterThem` | rows 17 and 18 |
| | `CASRefWriterGenesis.BirthInALaterEpochSealsTheCreationSlotAndWritesNoCkpt`, `.LateBirthOfAPredecessorIsAdoptedOrRefused`, `.EagerRuleSealsANeverBornStream` | [§2.7](#genesis), row 13, request count |
| | `CASRefWriterRecovery.StampsCkptBeforeTheFirstNewWrite` | request order |
| | `CASRefWriterSnapshotPublish.GenerationThreeBodyHasTwoFields`, `.ReadingEpochDoesNotPublish` | [§2.5](#ckpt), [§2.4](#writer-recovery) |
| `T/gtest_cas_ref_epoch_seal_format.cpp` | `NextEpochMustExceedOwnEpoch`, `NextEpochOnlyOnATombstone`, `HeaderThreeTombstoneRequiresNextEpoch`, `SparseBacklinkNeedsHeaderThree`, `ReadCeilingTwoRejectsHeaderThree` | grammar and gate (`setReadCeilingForTest`) |
| `T/gtest_cas_ref_statemachine.cpp` | `ReplayRejectsHeadThatDoesNotNameItsTombstone`, `ReplayRejectsRecordAfterTombstoneInTheSameEpoch`, `TombstoneIsLegalBeforeBirthAndRefusedAfterTerminal` | reciprocity, genesis grammar |
| `T/gtest_cas_ref_ckpt.cpp` | `DecodesGenerationsOneToThree`, `HeaderThreeRejectsOldFields`, `IdenticalSkipComparesGeneration`, `DurableLifeEpochIsNeverReplaced` | [§6](#format), [§2.7](#genesis) |
| `T/gtest_cas_gc_bounded_walk.cpp` | `QuietNamespaceCostsCkptAndOneProbeOnceTheSuccessorIsRemembered`, `ColdCursorCostsOneMoreRead` | request counts 2 and 3; replaces `:280` |
| | `ARoundStopsAtItsFirstAbsentKey`, `SliceCutLeavesTheNamespaceUnprovenAndKeepsItsCursor`, `PassOneIsNeverCutAndPassTwoRotatesByRound`, `TerminalRecordProvesTheFrontierWithoutAProbe` | cut, [§3.4](#budget); replaces `:231` and `:388` |
| | `CursorOnATombstoneResumesAtTheLinkedEpoch`, `CursorOnAnOldSealResumesAtTheNextEpoch`, `CursorBelowTheSnapshotIsNeverRead`, `StrayAtAReclaimedCursorKeyIsNotFolded`, `StrayAfterAReclaimedTombstoneIsNotFolded` | rows 7, 8 and 19, [§3.2](#cursor) |
| `T/gtest_cas_orphan_manifest_sweep.cpp`, `T/gtest_cas_fsck.cpp`, `T/gtest_cas_gc_rebuild.cpp`, `T/gtest_cas_inspect.cpp` | `RemovalAboveTheCursorAcrossASkippedEpochStaysProtected`, `RemovalWalkDoesNotTrustAReclaimedCursorKey`, `LaggingSweepRestartsWhenTheSealMoved`; one test each for a chain with a skipped epoch | [§3.3](#consumers) |
| `T/gtest_cas_pool.cpp` | `ClaimSetsLastUncleanEpoch` (one case per row of [§4](#eager)), `EveryBodyWriterKeepsOrResetsIt`, `SelfRemountRecoveryUsesTheRemountClaimsMarker` | marker |
| Part B, `T/gtest_cas_ref_writer.cpp`, `T/gtest_cas_gc_round.cpp` | `CASRefWriterNamespaceRemoval.TerminalRecordPrecedesRemoving`, `.LateRemovingAfterTakeoverFindsATerminalStream`, `.PositiveAppendReadsNoCatalog`, `.BirthAfterTerminalIsRefused`; `GcMovesALiveRowWithTerminalEvidenceToRemovingAndErasesIt`; `JanitorDeletesCkptBeforeStreamObjects`; `DecommissionCountsItsOwnTerminalLifeAsPendingGc` | rows 21 to 23; replaces `T/gtest_cas_ref_writer.cpp:4342`; reworks the `beginRemoving` outcome tests at `:4462-5235` |

- Crash rows use the seams `carve_hook_for_test`, `wedge_before_slot_occupy_hook_for_test`,
  `snapshot_before_ckpt_cas_hook_for_test` and the fault backends of `T/cas_test_helpers.h`. Integration, as
  `tests/integration/test_cas_ref_move_compat`: older servers refuse to mount after the stamp and an old GC
  holds; a kill between tombstone and head in `test_cas_insert_fault_recovery`.
- Stateless (`add-test`): store requests of one `INSERT` and one `OPTIMIZE`. Measurement: the A/B of
  CAS-487 again (`INSERT` requests 25 today, 19 and 16 by arithmetic; queue wait).

## 11. Staging {#staging}

Each commit builds and passes alone; readers precede writers; the gate comes with the first new write. No
commit writes an object an earlier one cannot read: commit 4 adds a lease key that decoders skip; commit 5
writes only what commit 1 decodes, commit 2 replays and commit 3 folds. `G_BUILD = 3` sits in commit 1
(the history assertion needs it, `CA/Formats/CasFormat.cpp:36`); the floor is raised by name at commit 5
(`CA/Pool/CasPool.cpp:598`). Lines: estimates.

| # | Commit | State after it | Product / test lines |
|---|---|---|---|
| 1 | Format: `G_BUILD = 3`, `!next_epoch` codec, grammar by header, `_ckpt` decode of 1 to 3 | nothing writes generation 3 | 200 / 350 |
| 2 | Replay reciprocity and genesis grammar, successor function, `walkRefChain`; read-only recovery, fsck, inspect, `REBUILD`, orphan sweep use it | readers accept both formats; writers unchanged | 450 / 700 |
| 3 | GC intake: cut at the first absent key, cursor resolution with the remembered successor, intake slice; a `_ckpt` without `committed_through` no longer proves a frontier (`CA/Gc/CasGc.cpp:2838-2848`) | GC no longer needs the frontier | 380 / 650 |
| 4 | `last_unclean_epoch` in the lease body, every body writer, published with the live epoch | marker recorded, no consumer | 150 / 300 |
| 4a | Refactoring only: writer recovery reads through `walkRefChain`; frontier publishes and numeric seals stay | behaviour unchanged | 150 / 100 |
| 5 | The complete protocol with a tombstone at every recovery: floor 3, generation 3 `_ckpt`, stamp-first, one-request append and wedge, writer recovery with the `_ckpt` re-read and no `_ckpt` publish, the forward-linked tombstone written by recovery (it names the live epoch; sparse backlink), uniform genesis, no one-unfrontiered-successor rule | the model's `safe_lateland_eager`; the series can stop here | 600 / 1200 |
| 6 | The lazy lane step and the selective rule, the consumer of the marker | Part A complete | 250 / 500 |
| 7 | Delete what [§2.6](#removed) lists as removed | no dead code | -350 / -300 |
| 8 | Docs: `docs/en/antalya/cas/` upgrade and architecture pages, `CA/Formats/README.md` | — | 150 |
| 9 | Part B.1: terminal before the catalog write, with the terminal branch's catalog read removed; a terminal record is final; GC completes `Live -> Removing`; presence probe; janitor order; decommission accounting | `Removing` implies a terminal record; the series can stop here | 350 / 650 |
| 10 | Part B.2: remove the positive-append catalog read | positive append is one request | -60 / 100 |

## 12. Non-goals and limits {#limits}

### 12.1 Non-goals {#non-goals}

- Fewer appends per part (CAS-485); the promote's manifest re-read; a combine window (CAS-287); counting the
  fence marker of the short mount wait design as a clean farewell; detecting external deletion.

### 12.2 Inferred, not traced {#inferred}

- A snapshot re-encoded later from the same state has identical bytes. No old read-only view caches ref tables.
- No mount slot is absent for a server root that has written namespaces. No caller needs a birth after a
  terminal record of the same life.
- No consumer assumes that a stream that holds logs is born. No log exists before the row is `Live`.
- The claims about models other than `CaRefLogOnlyAppendCore`. No request count or line estimate was measured.

## 13. Backlog interactions {#backlog}

Checked against the code at `802077e5c1a` unless marked. Tasks not listed are untouched or only re-measured.

| Task | What the change does to it |
|---|---|
| CAS-366 Story B, CAS-451, CAS-459 | removed with their mechanism: `SuccessorSeal` / `Superseded` are in the base (`CA/Pool/CasRefCkpt.cpp:125-148`) and go at commit 7; `last_epoch_seal` leaves `_ckpt` |
| CAS-176.2, DRAFT-31, DRAFT-26, the `_ckpt` half of CAS-71.5, CAS-455 | removed or moot: no `_ckpt` request per flush remains; after a durable terminal record nothing reopens the lane |
| CAS-305 | closed: no seal per skipped epoch; its "epochs ended clean" record is `last_unclean_epoch` |
| CAS-394 | its trigger (a failed `_ckpt` publish after a durable promote) goes; the durable-not-acknowledged window stays under CAS-395 |
| CAS-167 | delivered; its gates become measurements: ten-minute GCS soak with zero 429 on `_ckpt`, AWS with zero 409 |
| CAS-176.1, CAS-176 | delivered by Part B; two of the four reads of CAS-176 go (catalog, `_ckpt`); the manifest re-read stays |
| CAS-454, CAS-367 | delivered by the `DROP` order of [§7](#part-b), minus the upgrade takeover ([§5](#crashes) row 24) |
| CAS-387 (epoch out of the key) | rejected: a second format generation for a cost this design removes, and the orphan sweep compares a build's epoch with the cursor's (`CA/Gc/CasOrphanManifestSweep.cpp:546`) |
| Constrained, answered in the body | CAS-350, CAS-379, CAS-391 ([§2.3](#tombstone)); CAS-32, CAS-56, CAS-479 ([§2.4](#writer-recovery), [§2.5](#ckpt)); CAS-368, CAS-449 ([§2.7](#genesis)); CAS-315, CAS-448, CAS-457, CAS-86 ([§3.1](#chain-rule)); CAS-29.1 ([§3.2](#cursor)); CAS-29.10, CAS-92, CAS-31 ([§3.4](#budget)); CAS-179, CAS-355, CAS-364 ([§4](#eager)); CAS-410, CAS-393 ([§6](#format)); CAS-463, CAS-321.3, CAS-71.4 ([§7](#part-b)); CAS-375, CAS-29.8 ([§9](#tla)). CAS-147: every unclean operator restart costs one tombstone per namespace touched |
| Texts and counters (commits 7 and 8) | reword "publishing its checkpoint frontier was not confirmed" (`CA/Pool/CasRefLedger.cpp:2596`, `:4226`); retire `CASRefCheckpointFrontierRefused`, re-describe `CASRefCheckpoint{Published,IdenticalSkip,NotAdvanced}`, `CASRefRecoveryEpochSeal{ed,Adopted}`, `CASRefAppendSealRejected` (`src/Common/ProfileEvents.cpp:851`, `:882-885`, `:1014-1015`); rewrite the comments that justify the removed materialization wait by "recovery closes every dead epoch": `CA/Pool/CasPool.cpp:889-895`, `:1600-1608`, `CA/Pool/CasPool.h:276-283`, `CA/Pool/CasRefLedger.cpp:1964-1970`, `CA/Pool/CasServerRoot.cpp:1033-1036`. The reason is now [§4](#eager) |
| Docs (commit 8) | `docs/en/antalya/cas/bucket-requirements.md:92` (`_ckpt` write rate), `docs/en/antalya/cas/architecture/manifests-and-refs.md:277-308` (recovery). Invariant 5 of `docs/superpowers/cas/AGENTS.md` forbids dropping a protocol step as an optimization: record there that this step goes by an owner decision. Amend the CAS-321 note "the acknowledgement follows the checkpoint" |
| Tests to re-target | `tests/integration/test_cas_gcs_relink_liveness` delays every `_ckpt` `PUT` and asserts the match (`test.py:30-32`, `:191`, `:224`): move the lever to `_log`. `test_cas_ref_snaplog`, `test_cas_gcs`, `test_cas_drop_pool_member`, and a sweep of the 46 gtest files of [§3.3](#consumers) |
