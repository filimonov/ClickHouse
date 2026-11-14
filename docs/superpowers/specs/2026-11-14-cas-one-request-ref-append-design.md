---
description: 'Design for a CAS ref-log append that is one object-store request. An ordinary append becomes one create-if-absent of `_log/{epoch,seq}`; a durable log on the chain is committed history; `_ckpt` is written only at namespace birth and at snapshot publication. An epoch change is recorded lazily by a tombstone (`EpochSeal`) that carries a forward link to the successor epoch, so an epoch that does not write a namespace leaves no object. Readers find the end of history by exact reads to the first absent key. Ships behind format generation 3. Part B removes the catalog read of a positive append.'
sidebar_label: 'CAS: one-request ref-log append'
sidebar_position: 116
slug: /superpowers/specs/cas-one-request-ref-append-design
title: 'CAS: one-request ref-log append with a lazy, forward-linked epoch tombstone'
doc_type: 'reference'
---

# CAS: one-request ref-log append — design {#cas-one-request-ref-append}

- Date: 2026-11-14. Status: design. Nothing here is implemented, built or run. Task: CAS-487. What the
  change does to other open tasks is in [§13](#backlog).
- Fixed inputs: the change ships with 26.8 as generation 3; the acknowledgement fence is the mount lease;
  no seal is retained for fencing; no snapshot is forced at an epoch change; external deletion of CAS
  objects and an untrusted stream `LIST` stay as today; objects of generations 1 and 2 stay readable.
- Part A is [§2](#writer) to [§6](#format). Part B ([§7](#part-b)) ships in this change, after it, in its own commits.
- Source tree: worktree `cas-pr1-clean` at `802077e5c1a`. Every `path:line` was read at that commit.
  `26.6:` marks a path read with `git show altinity/antalya-26.6:<path>`.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `M/` = `docs/superpowers/models/` in the `master` worktree. A bare `:line` repeats the last file named.
- Terms. **Tombstone**: a ref-log transaction whose only op is `EpochSeal`. **Chain**: the logs reachable
  from the snapshot base by the successor rule of [§3.1](#chain-rule). **Chain epoch**: the epoch of the
  first absent key of the chain. **Live epoch**: the writer epoch of the mount that holds the lease.
- "Inferred" marks a statement that was not traced in the code. [§12](#limits) lists all of them.

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

Store requests, warm namespace, one writer, counted from the code, not measured:

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

The ref lane holds one request per flush instead of four, and `_ckpt` stops being a hot key (CAS-167).

What is given up:

- The store-side refusal of a superseded writer's append in a reclaimed slot ([§8](#risks), row 1).
- The exact frontier. A cold reader pays one absent probe, GC one more read per quiet namespace per round.
- The missing-log diagnostic above the snapshot base (`CA/Gc/CasGc.cpp:2909-2914`).
- One small `PUT` per namespace per epoch that writes it.
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

- `publishCkptContribution` at `:4217` and its outcome arms at `:4229-4253` are removed. Nothing replaces them.
- Both rechecks precede the install. A log that is durable when a recheck fails is history and is never
  acknowledged: the lane goes `Wedged` (step 4) or `NeedsRecovery` (step 5), with today's texts (`:3927-3956`).
- Exact-attempt discipline is unchanged: no id is allocated while an attempt (key, bytes, generation) is held.

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

- **Who decides the epoch.** `flushRefBatch`, at the place of today's deposed-lane check (`:3091-3093`),
  before any preview (`:3360`, `:3388`) and before id allocation (`:3744`). Candidates are built only after
  the chain epoch equals the live epoch, so nothing is derived from a table that an adopted record changes.
- **Order of checks.** Read the live epoch once; re-check `may_mutate` and the superseded flags; then
  compare. A self-remount publishes the new epoch before it detaches the old runtimes
  (`CA/Pool/CasPool.cpp:1632-1646`), so a detached runtime could otherwise name the new epoch (CAS-350).
- **Rule.** Let `X` be the chain epoch and `W` the live epoch.
  - `X = W`: proceed. `X > W`: this writer is deposed; the lane goes `Closed` with today's rejection.
  - `X < W`: create a tombstone at the chain's next slot with `!next_epoch = W`. If that slot is sequence 1
    (the previous record is itself a tombstone naming `X`), the new tombstone carries the backlink to it.
- **Mechanics.** The tombstone is a lane attempt like an append: armed, sent with `Retry::standard`,
  installed after the recheck. Installing it sets the state's open transition `{seal id, next epoch}`,
  which replaces `RefTableRuntime::last_epoch_seal`. `chainLinkFor` (`:146-151`) returns the seal id iff
  its next epoch equals the id's epoch.
- **Ambiguity.** An unresolved tombstone leaves the lane `Wedged`: no id is allocated, so `{W,1}` waits.
- **Conflict.** Own bytes: install. Any other occupant: `NeedsRecovery`. Recovery re-reads the chain and
  applies whatever is there, then the step runs again:

| Occupant | After the re-read |
|---|---|
| Ordinary record of `X` (a predecessor's late append) | adopted as history; chain epoch stays `X`; the tombstone goes one slot later |
| Tombstone naming `V > W` | chain epoch `V`; lane `Closed` |
| Tombstone naming `U < W` (a writer that stopped after its tombstone) | chain epoch `U`; the next tombstone goes to `{U,1}` with the backlink |
| `RemoveNamespace` | terminal; positive lane closed; no tombstone |

- The loop is bounded by `kRefRecoveryMaxSlotAttemptsPerEpoch` (`:1246`): one writer has one append in
  flight, so repeated adoption in one epoch means a store that keeps creating objects.
- The codec requires `!next_epoch` above the tombstone's own epoch. A terminal stream gets no tombstone;
  a never-born one gets one like any other ([§2.7](#genesis)).

### 2.4 Writer recovery {#writer-recovery}

1. Read `_ckpt` once. Validate the base as today (`CA/Pool/CasRefProtocol.cpp:944-1030`). An absent or
   undecodable `_ckpt` of a `Live` life stays `CORRUPTED_DATA` (`CA/Pool/CasRefCkpt.cpp:180-182`; a hold in GC,
   `CA/Gc/CasGc.cpp:2820-2836`): nothing is installed, so no tombstone or append enters that life (CAS-479).
2. Walk the chain from the base's successor, or from `{life_epoch,1}`, to the first absent key ([§3.1](#chain-rule)).
   A retained attempt whose slot holds foreign bytes stays `CORRUPTED_DATA` (`CA/Pool/CasRefLedger.cpp:1098-1104`).
3. Re-read `_ckpt`. If its token differs from step 1, restart. This replaces the final equality check at
   `:1382-1386` and is what makes an absent read trustworthy against cleanup ([§3.1](#chain-rule)).
4. If the stream is not terminal and the chain epoch `X < W` is at or below `last_unclean_epoch`: create
   the tombstone in the walk, with today's code at `:1244-1350` minus its `_ckpt` publishes. A conflict
   adopts the occupant and loops. An unresolved create fails retry-later; nothing is installed (`:1318-1337`).
5. Install. Chain epoch above `W`: lane `Closed`. Terminal: positive lane closed. Otherwise `Ready`.

Recovery no longer writes `_ckpt` or seals every numeric epoch, and it writes nothing after a clean
predecessor. The read paths keep calling it (`:315`, `:390`, `:413`). Maintenance triggered by a read:

- The stale-precommit sweep appends through the lane (`:4942-4966`), so it takes the tombstone step. A
  reading epoch that inherits stale precommits writes a tombstone and one removal.
- Snapshot publication is admitted only when the chain epoch equals the live epoch: one more term in
  `hasStateBearingSnapshotCandidateUnderStateLock` (`:4381-4388`). A reading epoch publishes nothing, as
  today, where a recovered seal blocks publication (`T/gtest_cas_ref_writer.cpp:3252`). Accepted.

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
- A generation 3 `_ckpt` is an authority, not a derived object: after cleanup neither the base id nor
  `life_epoch` can be read back from the logs. "Delete the damaged object" is no repair (CAS-32, CAS-56).

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

### 2.7 Genesis {#genesis}

**Decision.** A never-born stream follows the rule of every other first write of an epoch. `life_epoch` is
set once, at creation, and names the first key of the stream: `publishCkpt` never replaces a durable value.

- Created in `C`, first written by `W > C`: tombstone at `{C,1}` with `!next_epoch = W`, then the birth at
  `{W,1}` with the backlink. A birth in `C` itself goes to `{C,1}` with no backlink.
- Grammar: a tombstone is legal on a never-born stream and refused only after a terminal record
  (`CA/Pool/CasRefProtocol.cpp:393-412` today refuses both). A sequence-1 record carries a backlink iff the
  record before it is a tombstone. The lifecycle test at `:555-557` is replaced by that rule.
- Nothing else needs `life_epoch` to be the birth epoch. The contextual grammar
  (`CA/Formats/CasRefLogFormat.cpp:262-278`), the base check (`CA/Pool/CasRefProtocol.cpp:979`), the walk
  start (`CA/Pool/CasRefCkpt.cpp:211`) and the GC first fold (`CA/Gc/CasGc.cpp:2799`) need only the first key.
- Old-format streams: the first key is `{life_epoch,1}` with the value they have, raised or not. A born one
  holds its birth there without a backlink. A never-born one gets its first tombstone there. Nothing raises it.
- Precondition: every create decision mints a fresh incarnation, else a new life shares `_ckpt` and its
  set-once `life_epoch` with an erased one. The base has it (`CA/Pool/CasRefCatalog.cpp:302-307`, `:328-329`).

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
- **Validation.** Replay rejects (`CORRUPTED_DATA`) a record after a tombstone that is not at the
  tombstone's successor and a head whose backlink is not that tombstone's id. This lives in
  `RefTableState::applyTxnInPlace` next to the density check (`CA/Pool/CasRefProtocol.cpp:515`), so every
  apply runs it. GC, which does not replay, runs the same test on the two decoded bodies.
- **End.** The first absent key on the validated chain, including an absent successor of a tombstone.
- **Why an absent read can be trusted.** Cleanup deletes only logs strictly below the snapshot id it read
  from `_ckpt` (`CA/Pool/CasRefProtocol.cpp:824-836`), and that id only grows. A walk that starts at a
  base re-reads `_ckpt` after its absent read. An unchanged token means every cleanup planned so far used
  an id at or below the walk's base, so it deleted nothing the walk read as absent; a changed token restarts
  the walk. A walk from the GC cursor needs no re-read: cleanup deletes only at or below it (`:829`).
- **The dead-life janitor** also deletes logs, snapshots and `_ckpt`, in listing order, but only of a life
  that a fresh catalog read does not name (`CA/Gc/CasNamespaceJanitor.cpp:86`, `:118-190`). So a reader that is
  not the life's writer confirms the catalog row after its absent read (CAS-457). fsck keeps its re-read
  against a generation 1 or 2 frontier (`CA/Tools/CasFsck.cpp:323-335`, CAS-448).
- **A failed validation is not yet corruption.** The walk re-reads `_ckpt` first: a changed token restarts
  it, only an unchanged one is `CORRUPTED_DATA` (CAS-315). It frees its read-ahead window on a throw (CAS-86).
- **Termination.** Forward links strictly increase. The walk's budget is in [§3.4](#budget).

### 3.2 Where GC resumes {#cursor}

The GC fold cursor is the last folded id (`CA/Formats/CasFoldSealFormat.h:136`). It stores no record kind.

**Stray objects.** Cleanup deletes logs at or below the cursor and strictly below the snapshot id `S`
(`CA/Pool/CasRefProtocol.cpp:827-836`). A late create-if-absent can land in such a slot, with correct
clocks too: nothing is acknowledged, but the object exists. A writer whose lease check passes wrongly can
go on to later slots of that epoch. **A key at or below the cursor and below `S`, or after a deleted
tombstone in its epoch, may hold a stray. No reader interprets such a key.**

Successor of an inherited cursor `c`:

1. Warm path: the GC process remembers `(cursor, successor key)` from its own fold. If the durable cursor
   equals the remembered one, it probes the remembered key. Nothing is persisted.
2. `c >= S`, or no snapshot: cleanup cannot have deleted `c` and no create can have replaced it. Read `c`;
   its kind gives the successor.
3. `c < S`: do not read `c` and do not probe `{c.e,c.s+1}` first. Back-chain from the epoch of `S` through
   sequence-1 backlinks (`crossEpochFromSeal`, `:847-909`). A head names `c`: `c` was a tombstone and that
   head is the successor. The back-chain reaches the epoch of `c` without naming it: `c` was ordinary and
   the successor is `{c.e,c.s+1}`. Anything else is a hold.

- Step 3 reads only keys above the cursor that the chain leads to. They were never deleted, so their
  first and only occupant is the chain's record.
- A cursor below `S` has a successor: cleanup keeps the snapshot's same-id log (`:831`) and, for a base at
  sequence 1, the tombstone its backlink names (`:833`), and a tombstone is never a snapshot id
  (`CA/Pool/CasRefLedger.cpp:4381-4388`, `CA/Pool/CasRefProtocol.cpp:972-978`). Id order is chain order.
- A quiet namespace costs `_ckpt` and one probe on the warm path, one more read in step 2 (GC leader
  change, restart, `REBUILD`). The `_ckpt` read cannot go: it gives `S`, which moves without a new log.

### 3.3 Consumers {#consumers}

Search: `grep -rEc` over `CA/` and `src/`, no output cut, for `committed_through`, `last_epoch_seal`,
`prev_epoch_seal`, `life_epoch`, `EpochSeal`, `nextRefLogIdWithinCommittedFrontier`, `crossEpochFromSeal`,
`chainLinkFor`, `publishCkpt`, `readCkpt`, `recoverRefTableDetailedFromAuthority`, `chooseRecoveryGrounding`,
`readCheckpointSnapshotBase`. Matches: 26 files in `CA/`, 46 under `T/`, `src/Common/ProfileEvents.cpp`.

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
`committed_through` only in a body of generation 1 or 2. It folds a durable log above such a frontier: the
old recovery adopts that log too (`CA/Pool/CasRefLedger.cpp:1142-1145`).

### 3.4 Bound of the walk {#budget}

- Expected, not guaranteed: a lane appends one record per round trip, the walker a window (`CA/Gc/CasGc.cpp:2895`).
- Hard bound: a fixed time slice for the ref intake of one round as a whole, in the janitor's pattern
  (`mono_ms_fn` plus a constant, `:430-447`; `kJanitorBudgetMs`, `:94`). `kRefIntakeBudgetMs = 60'000`, one
  default `gc_interval_sec` (`CA/ContentAddressedSettings.cpp:62`): an intake longer than the pause between
  rounds gains nothing over the next round, which starts from the kept cursors. No new setting.
- The slice starts at the first read of the intake. Pass 1 is never cut: every walk target gets its
  `_ckpt` read, its successor probe and one read window, in catalog order. That proves every quiet
  namespace. Pass 2 continues the unproven ones from index `round mod n`, wrapping, and starts no read
  after the slice ends. The rotation keeps a slice that always runs out from starving the same namespaces.
- A namespace that is cut off keeps the cursor it earned and is unproven for that round. The round then
  suppresses every destructive step for the whole pool (`CA/Gc/CasGc.cpp:3541-3547`) and still folds and seals.
- CAS-29.10 says a deadline never cuts "the intake of every live life". Pass 1 keeps that; only pass 2 is
  cut. When CAS-29.10 lands, pass 2 stops at its round deadline instead of the constant.
- `gc_frontier_probe_budget` keeps its job and does not bound the walk: it caps probes of unhinted
  namespaces without a usable `_ckpt` (`:2493-2517`), default unbounded (`CA/Pool/CasPool.h:155`; CAS-92).
  The probe `last_folded + 1` and "exactly K `GET`" of CAS-29.1 are replaced by [§3.2](#cursor).

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

- Every later writer of the body copies the value: renewal (`CA/Pool/CasServerRoot.cpp:1696`), farewell
  (`:2174`), the GC fence (`fencedCopy`, `:1138`, `:1403`). The self-remount goes through the same claim
  (`CA/Pool/CasPool.cpp:1559-1574`), so a lease lost inside one process counts as unclean. The ledger
  reads the value once per mount, like the live epoch (`CA/Pool/CasRefLedger.cpp:1602`).
- The value never decreases: a claim only replaces a body of a lower epoch (`CA/Pool/CasServerRoot.cpp:1037-1063`;
  CAS-364). A forced exit writes no farewell and must not: without the drain the successor would skip the
  eager tombstone. Option (a) of CAS-355 is excluded.

**Rule.** Recovery of a non-terminal stream with chain epoch `X < W` seals before install iff
`X <= last_unclean_epoch`. A never-born stream is included.

**Why it is enough.** Only the mount of epoch `X` creates ordinary records in epoch `X`. If
`X > last_unclean_epoch`, epoch `X` ended with a clean farewell: any other end is recorded by the next
claim, and the value never decreases. A farewell is written only after `drainRefLanesForShutdown` returned
true (`CA/Pool/CasPool.cpp:1126-1137`, `:1280-1288`, `CA/Pool/CasMountRuntime.cpp:1230`), which requires
every resident lane idle and none `Writing` or `Wedged` (`CA/Pool/CasRefLedger.cpp:2103-2161`). So no
append and no lane tombstone of `X` is in flight. A lane tombstone of an epoch between `X` and `W` is
either drained the same way or raises the value above `X`. Epochs that never held the lease wrote nothing.
The rule is transitive because it compares the chain epoch, not the predecessor: an epoch that never read
the namespace leaves it at `X` and passes the value on (sabotage: `sab_selective_immediate`).

**What a clean farewell does not exclude.** A recovery tombstone whose create was unresolved
(`CA/Pool/CasRefLedger.cpp:1318`) is not a lane attempt and can land later. The farewell is not withheld: it
leaves `last_unclean_epoch` unchanged, so the successor finds the same chain epoch at or below it and seals
eagerly too. One create wins, the other adopts it or loses ([§5](#crashes), row 20). The owner at rest
holds the whole chain, so no invariant is weakened to table content.

The drain ignores snapshot publications and plain and catalog requests (CAS-179). After a clean farewell
these can still land: a `_snap` object and its `_ckpt` publish (the base only grows, a walk in progress
restarts by its re-read, an installed table is unaffected); a catalog write of a creation or a `DROP`
([§7](#part-b)); namespace files, manifests and blobs, outside the ref chain. No append or lane tombstone can.

**When an epoch that only reads a namespace writes into it.** It leaves no object, with three exceptions:

1. the eager tombstone, once per namespace, when the chain epoch is at or below `last_unclean_epoch`;
2. a tombstone and one removal, when it inherits stale precommits ([§2.4](#writer-recovery));
3. the `_ckpt` stamp and an eager tombstone at the first touch after the upgrade: old bodies lack the field.

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
| 7 | Predecessor `PUT` lands after cleanup deleted the tombstone | orphan log below the base | not on the chain | acknowledged only if its lease check passes wrongly ([§8](#risks), row 1); then it may write on in that epoch; GC folds none of it and cleanup deletes it | `res_clocklies`, `safe_clocklies_nocleanup`, `safe_clocklies_gc`, `sab_gccursor_probefirst_zombie` |
| 8 | Predecessor `PUT`, sent under a valid lease, lands in a slot that cleanup reclaimed; correct clocks | a stray log at or below the GC cursor and below the snapshot id | not on the chain; no reader interprets that key ([§3.2](#cursor)) | nothing is acknowledged; cleanup deletes the stray as a covered log | `safe_lateland_selective`, `witness_cursorreused`; the unsafe rule is `ctl_gccursor_probefirst_honest` |
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
| 19 | GC folded a tombstone whose successor was absent, then it lands; cleanup may delete the tombstone | cursor on the tombstone | — | GC resumes at `{W,1}` by [§3.2](#cursor) | `sab_gccursor_naive`, `witness_cursordeleted`, `safe_clocklies_gc` |
| 20 | Eager tombstone of `W` unresolved at recovery, clean farewell, it lands later | `{X,k} -> W`, or the successor's own tombstone | chain grows by one tombstone | `V` sees the same chain epoch `X <= last_unclean_epoch` and seals eagerly too: one create wins, the other adopts it or loses | `safe_lateland_selective`, `witness_sealafterfarewell` |
| 21 | Part B: stop after the terminal record, before `beginRemoving` | terminal, row `Live` | stream ended | owner: lane closed, creation waits; GC folds the terminal, its next drain writes `Removing` and erases | to add |
| 22 | Part B: terminal `PUT` of a `DROP` ambiguous | unknown, row `Live` | ended or not | lane `Wedged` and closed; no catalog write is sent; a retried `DROP` resolves the attempt | `safe_selective_terminal` |
| 23 | Part B: `beginRemoving` lands after a takeover, or races the GC write | row `Removing` | terminal stream, as before | no effect: the terminal preceded it; the exact-row replace lets one writer win | to add |
| 24 | Part B: an older build's `Live -> Removing` in today's order lands after the upgrade takeover | row `Removing`, no terminal | live stream | appends go on until the next cold admission refuses (`CA/Pool/CasRefLedger.cpp:5279`); a retried `DROP` completes; CAS-454 stays for that takeover | none |

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
| Later fields | a future log field (the block id of CAS-393) needs its own gate, an unknown `!` key or a generation, and cannot go into `_ckpt`, which holds two fields |

An unknown `!` key is `UNKNOWN_FORMAT_VERSION` (`CA/Formats/CasTextFormat.cpp:315-324`); the header is read first.

**Stamp before the first new write.** A runtime learns the generation of its `_ckpt` from the header at
recovery. Below 3, it publishes the generation 3 body (one `PUT`, once per namespace) before its first
tombstone or append, conditional on the token recovery validated; a mismatch re-runs recovery. A new
namespace is created stamped. The floor is raised at mount.

| Older reader | What stops it |
|---|---|
| 26.6 or old 26.8, new mount or read-only open | pool floor 3 (`26.6:CA/Formats/CasPoolMetaFormat.cpp:155`) |
| 26.6 GC leader, already mounted | `_ckpt` header 3 fails `checkCompatibility` (`26.6:CA/Formats/CasTextFormat.cpp:416`), the namespace is recorded undecodable (`26.6:CA/Gc/CasGc.cpp:1751-1763`) and the pass suppresses every destructive step (`26.6:CA/Gc/CasGc.cpp:3503-3509`) |
| Old 26.8 GC leader, already mounted | same path: `CheckpointUndecodable` hold, nothing folded for the namespace (`CA/Gc/CasGc.cpp:2820-2836`), destruction suppressed (`:3541-3547`) |
| Old writer of the same server root | cannot coexist: one lease per root, and it cannot remount |
| Relink between versions | off in both directions already (`docs/en/antalya/cas/configuration.md:350`) |

A warm old reader can still do two things. Both are accepted: mixed versions on one pool are unsupported
(`docs/en/antalya/cas/configuration.md:348`). An old GC round that sampled `_ckpt` before the stamp finishes
with that cut, a prefix of history, and its planned cleanup deletes only logs nothing new needs; its next
`_ckpt` read fails closed. An old read-only view keeps serving the table it recovered before the stamp.

## 7. Part B: the catalog `GET` {#part-b}

**Decision.** `DROP` commits the terminal record before any catalog write, and GC completes the catalog
transition. A `Removing` row then implies a terminal record, and the positive append reads no catalog.
Rejected: a change of the catalog row at every takeover, which writes the pool-wide object each time.

- The only writer of `Live -> Removing` is `beginRemoving` (`CA/Pool/CasRefCatalog.cpp:433`), called only
  from `dropNamespaceImpl` (`CA/Pool/CasRefLedger.cpp:5572`), which closes the local lane first. The `GET` at
  `:3612` is for a predecessor's late write, and it does not close that case: the write can land after it.
- New order in `dropNamespaceImpl`: close and drain the lane (`:5515-5526`); append the terminal record;
  then `beginRemoving`, best effort. A terminal that provably did not land reopens the lane; an ambiguous
  one wedges it. After a durable terminal nothing reopens, so the outcome tracking of the catalog write
  (`:5565-5647`) goes. A `DROP` over a terminal stream under a `Live` row no longer returns early (`:5511`).
- A terminal record is final: replay refuses `NamespaceBirth` after it (`CA/Pool/CasRefProtocol.cpp:359-367`
  allows it today), and recovery of a terminal stream closes the positive lane. Creation over such a life
  already waits (`refuseTerminalLife`, `CA/Pool/CasRefLedger.cpp:5295-5304`).
- Completer: GC. The fold of a terminal record earns `cleanup_evidence` whatever the row's state
  (`CA/Gc/CasGc.cpp:3399-3411`). The pre-fold drain of the next round (`:5308-5320`) then also selects a
  `Live` row. Precondition, all of: the row read in this drain is `Live` and has no `creator`; the adopted
  parent seal holds `ref_lives[row.incarnation]` with `cleanup_evidence` and no `coverage.hold`
  (the test of `CA/Gc/CatalogLifecycleReconciler.cpp:36-39`); the write replaces exactly that row
  (`CA/Pool/CasRefCatalog.cpp:427-429`) under the leader's admission, refreshed per attempt. It sets
  `Removing` and `removal_started_round`. The existing erase (`:459-476`) follows in the same drain.
- The GC write uses the catalog's hot-key path like `beginRemoving` (`casUpdateImpl`, `:192`, `:440`), batched
  with the drain's erases once they move there (a direct replace today, `:531`; CAS-463, CAS-321.3,
  CAS-71.4). A refused or contended write leaves the row for the next round and never aborts the fold
  (today a drain that leaves a row throws, `CA/Gc/CatalogLifecycleReconciler.cpp:121-124`).
- Decommission drops the victim's namespaces through the same `dropNamespace`
  (`CA/Tools/CasDecommission.cpp:380-397`). Its writer mounts through `mountWritable` as the victim's next
  epoch (`CA/Pool/CasPool.cpp:627-640`, `:983`), so the tombstone rule and [§4](#eager) apply unchanged.
- Old-order rows, `Removing` without a terminal record, exist after the upgrade. The resume path stays: a
  `DROP` over such a row appends the terminal record, after the tombstone step.
- Removed: both catalog reads of `commitRefChunk` (`CA/Pool/CasRefLedger.cpp:3579-3593`, `:3606-3636`).
  The local authorization at `:3565-3577` stays. The cold presence probe no longer answers "present" for a
  `Live` row unread (`:5383`): it takes the path of a `Removing` row (`:5407`).

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

`M/CaRefLogOnlyAppendCore.tla` version 2 (34 rows in `M/run_reflogonlyappend.sh`) checks the chain, both
tombstone rules with the marker, old-format seals, a terminal record, reciprocal links, a step-wise walk
with the base re-read, a travelling snapshot publication, a GC cursor that is the last folded id, and a late
recovery tombstone. Crash rows 15, 21, 23, 24 of [§5](#crashes) have no config; row 13 has one in part. To add:

| # | Addition | Expected rows |
|---|---|---|
| 1 | Genesis follows the uniform rule and the safe rows already check it: the empty chain at `<<1,1>>` is sealed by the first later epoch that writes. Remaining: creation as a step that sets `life_epoch` once, the birth as a record kind, an old-format stream whose `life_epoch` was raised | safe with `PROP_MONOTONE`; sabotage "birth raises `life_epoch`" violates it |
| 2 | Two heads that name the cursor id: the chain's, and one a superseded writer wrote after its own tombstone landed in the reclaimed slot. Needs four appends; the model's bound is three and it picks any head | safe with the back-chain from `S`; sabotage "any head" violates `INV_GC_PREFIX` |
| 3 | Second physical life after `DROP`; late requests of the first life | safe; witness |
| 4 | The marker as a value copied through lease bodies, starting from a body without it | safe; `sab_selective_immediate` stays red |
| 5 | GC rounds with the destructive gate, a time cut-off and the remembered successor | sabotage "cut-off counts as proven" violates `INV_GC_CUT_SOUND` |
| 6 | An old reader bounded by a frozen frontier; the stamp | safe with stamp-first; sabotage "append before the stamp" lets the old GC delete |
| 7 | Part B: catalog row, terminal before `Removing`, GC as completer, a late catalog write | safe; sabotages: today's order; a birth after a terminal record |

The new model and every rerun model carry a table "action -> store requests it stands for" (CAS-375).
Existing models, inferred from result files and config names; only `CaRefLogOnlyAppendCore` was read:

| Family | Action |
|---|---|
| `CaWriterEpochBackfillCore_{safe,sab_direct_skip,sab_frontier_after_terminal,sab_snapshot_base_is_seal,witness_authorizations}` | retire for generation 3: its contract is one seal per numeric epoch. Keep as the record of the old format |
| `CaRefTableSnapshotLogCore_frontier_*`; `_cur_lateappend*`, `_fix_a*`, `_fix_b*` | retire: they assert the `_ckpt` publish before the acknowledgement, and the store-side refusal that [§8](#risks) row 1 gives up |
| `CaRefTableSnapshotLogCore_v9_safe*`, `_v9_flip_latepred*` | rerun unchanged: reading of old-format history |
| `CaRefDeltaIntakeCore_{v9_safe,v9_hintomission,v9_hold,ctl_holdsuppresses,sab_cleanupignorescursor,sab_clearholdonabsent}` | change and rerun: the cut becomes an absent observation instead of an atomic frontier |
| `CaRefLaneCore_safe` and its `sab_*`; `CaCasMountCore_v9_recoverygen`, `_witness_sealrejected`, `_sab_wedgeretryoldgen*` | rerun; add the tombstone as a lane attempt and the marker to the claim |
| `CaRefCleanupLicenceCore` | rerun: it shares the premise that cleanup deletes only below the snapshot id it read |
| `CaGcDestructiveGateCore_*` (13 configs); `CaGcCondemnMarkerGate_{fix_prefetch,fix_C_bound,fix_C_recur}` | rerun: the gate takes the frontier proof as an input; the marker gate with the cut taken during the walk |

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
| `T/gtest_cas_orphan_manifest_sweep.cpp`, `T/gtest_cas_fsck.cpp`, `T/gtest_cas_gc_rebuild.cpp`, `T/gtest_cas_inspect.cpp` | `RemovalAboveTheCursorAcrossASkippedEpochStaysProtected`, `RemovalWalkDoesNotTrustAReclaimedCursorKey`; one test each for a chain with a skipped epoch | [§3.3](#consumers) |
| `T/gtest_cas_pool.cpp` | `ClaimSetsLastUncleanEpoch` (one case per row of [§4](#eager)), `RenewalFarewellAndFenceKeepIt` | marker |
| Part B, `T/gtest_cas_ref_writer.cpp`, `T/gtest_cas_gc_round.cpp` | `CASRefWriterNamespaceRemoval.TerminalRecordPrecedesRemoving`, `.LateRemovingAfterTakeoverFindsATerminalStream`, `.PositiveAppendReadsNoCatalog`, `.BirthAfterTerminalIsRefused`; `GcMovesALiveRowWithTerminalEvidenceToRemovingAndErasesIt` | rows 21 to 23; replaces `:4342`; reworks the `beginRemoving` outcome tests at `:4462-5235` |

- Crash rows use the existing seams (`carve_hook_for_test`, `wedge_before_slot_occupy_hook_for_test`,
  `snapshot_before_ckpt_cas_hook_for_test`) and the fault backends of `T/cas_test_helpers.h`.
- Integration, modelled on `tests/integration/test_cas_ref_move_compat`: older servers refuse to mount after
  the stamp and an old GC holds; a kill between tombstone and head in `test_cas_insert_fault_recovery`.
- Stateless: a new test (`add-test`) asserts the store requests of one `INSERT` and one `OPTIMIZE`.
- Measurement: repeat the A/B of CAS-487: `INSERT` requests (25 today, 19 and 16 by arithmetic), queue wait.

## 11. Staging {#staging}

Each commit builds and passes alone; readers precede writers; the gate comes with the first new write. Lines: estimates.

| # | Commit | State after it | Product / test lines |
|---|---|---|---|
| 1 | Format: `G_BUILD = 3`, `!next_epoch` codec, grammar by header, `_ckpt` decode of 1 to 3 | nothing writes generation 3 | 200 / 350 |
| 2 | Replay reciprocity and genesis grammar, successor function, `walkRefChain`; read-only recovery, fsck, inspect, `REBUILD`, orphan sweep use it | readers accept both formats; writers unchanged | 450 / 700 |
| 3 | GC intake: cut at the first absent key, cursor resolution with the remembered successor, time cut-off | GC no longer needs the frontier | 380 / 650 |
| 4 | `last_unclean_epoch` in the lease body and the claim | marker recorded, no consumer | 120 / 250 |
| 5 | Gate and one-request append: floor 3, generation 3 `_ckpt`, stamp-first, append and wedge without `_ckpt`; recovery still seals at every start | sound without the lazy step (`safe_lateland_eager`) | 350 / 700 |
| 6 | Lazy forward-linked tombstone: lane step, selective rule, sparse backlink, genesis tombstone, recovery without numeric sealing | Part A complete | 480 / 950 |
| 7 | Delete what [§2.6](#removed) lists as removed | no dead code | -350 / -300 |
| 8 | Docs: `docs/en/antalya/cas/` upgrade and architecture pages, `CA/Formats/README.md` | — | 150 |
| 9 | Part B.1: terminal before the catalog write; a terminal record is final; GC completes `Live -> Removing`; presence probe | `Removing` implies a terminal record | 300 / 550 |
| 10 | Part B.2: remove both catalog reads of `commitRefChunk` and the catalog-write outcome tracking | positive append is one request | -150 / 100 |

## 12. Non-goals and limits {#limits}

### 12.1 Non-goals {#non-goals}

- Fewer appends per part (CAS-485); the promote's manifest re-read; a combine window (CAS-287); counting the
  fence marker of the short mount wait design as a clean farewell; detecting external deletion.

### 12.2 Inferred, not traced {#inferred}

- A GC round reads the condemn and graduation markers after every namespace's cut.
- An evicted ref-table runtime is never `Writing` or `Wedged`, so the shutdown drain sees every attempt.
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
| Constrained, answered in the body | CAS-350, CAS-379, CAS-391 ([§2.3](#tombstone)); CAS-32, CAS-56, CAS-479 ([§2.4](#writer-recovery), [§2.5](#ckpt)); CAS-368, CAS-449 ([§2.7](#genesis)); CAS-315, CAS-448, CAS-457, CAS-86 ([§3.1](#chain-rule)); CAS-29.1 ([§3.2](#cursor)); CAS-29.10, CAS-92, CAS-31 ([§3.4](#budget)) |
| Constrained, answered in the body | CAS-179, CAS-355, CAS-364 ([§4](#eager)); CAS-410, CAS-393 ([§6](#format)); CAS-463, CAS-321.3, CAS-71.4 ([§7](#part-b)); CAS-375, CAS-29.8 ([§9](#tla)). CAS-147: every unclean operator restart costs one tombstone per namespace touched |
| Texts and counters (commits 7 and 8) | reword "publishing its checkpoint frontier was not confirmed" (`CA/Pool/CasRefLedger.cpp:2596`, `:4226`); retire `CASRefCheckpointFrontierRefused`, re-describe `CASRefCheckpoint{Published,IdenticalSkip,NotAdvanced}`, `CASRefRecoveryEpochSeal{ed,Adopted}`, `CASRefAppendSealRejected` (`src/Common/ProfileEvents.cpp:851`, `:882-885`, `:1014-1015`) |
| Docs (commit 8) | `docs/en/antalya/cas/bucket-requirements.md:92` (`_ckpt` write rate), `docs/en/antalya/cas/architecture/manifests-and-refs.md:277-308` (recovery). Invariant 5 of `docs/superpowers/cas/AGENTS.md` forbids dropping a protocol step as an optimization: record there that this step goes by an owner decision. Amend the CAS-321 note "the acknowledgement follows the checkpoint" |
| Tests to re-target | `tests/integration/test_cas_gcs_relink_liveness` delays every `_ckpt` `PUT` and asserts the match (`test.py:30-32`, `:191`, `:224`): move the lever to `_log`. `test_cas_ref_snaplog`, `test_cas_gcs`, `test_cas_drop_pool_member`, and a sweep of the 46 gtest files of [§3.3](#consumers) |
