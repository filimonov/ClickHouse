---
description: 'TLC results for CaRefLogOnlyAppendCore: a ref-log append that is one create-if-absent, with a lazy epoch tombstone that carries a forward link, a selective tombstone at recovery, and a GC cursor that never trusts a key cleanup may have emptied.'
sidebar_label: 'CaRefLogOnlyAppendCore results'
sidebar_position: 120
slug: /superpowers/models/ca-ref-log-only-append-core-results
title: 'CaRefLogOnlyAppendCore — TLC results'
doc_type: 'reference'
---

# `CaRefLogOnlyAppendCore` — TLC results {#ca-ref-log-only-append-core-results}

The model checks the protocol of `docs/superpowers/specs/2026-11-14-cas-one-request-ref-append-design.md` (CAS-487),
not an implementation.

- Module: `CaRefLogOnlyAppendCore.tla`. Runner: `run_reflogonlyappend.sh` (40 rows).
- Run: 2026-11-14, pinned TLC jar. Bounds: 3 epochs, 3 slots per epoch, 3 appends, unless a row says otherwise.
- 37 rows ran to completion and gave the expected result. Three rows are bounded searches ([below](#bounded)).

## Protocol under test {#protocol}

- An ordinary append is one create-if-absent of `<<epoch, seq>>`. A durable object on the chain is committed history.
- Recovery reads the base, then exact keys one at a time to the first absent key or a terminal record, following
  tombstones. It re-reads the base after its last read and restarts when the base moved.
- The first append of an epoch `W` whose chain ends in another epoch is preceded by a create-if-absent of a tombstone
  in the chain's next slot. The tombstone names `W`; the head `<<W, 1>>` names the tombstone. A never-born stream
  follows the same rule.
- Recovery writes that tombstone before the table is trusted iff the chain ends in an epoch at or below
  `lastUnclean`, and re-reads the base after the create.
- The acknowledgement fence is the mount lease only.
- GC keeps the id of the last folded record. At or above the snapshot id it reads that object. Below it, it walks
  back from the head of the snapshot's epoch through the backlinks.
- Cleanup deletes logs strictly below the snapshot id that GC folded, except the tombstone the snapshot's own log names.

## Green rows {#green}

| Config | Switches | Expectation | Result | Distinct states |
|---|---|---|---|---|
| `safe_lease` | none | green | green | 1038119 |
| `safe_lateland` | `LateLand` | green | green | 10373571 |
| `safe_lateland_eager` | `LateLand`, `EagerSeal` | green | green | 23626814 |
| `safe_lateland_selective` | `LateLand`, `SelectiveSeal` | green | green | 21622721 |
| `safe_selective_legacy` | `LateLand`, `SelectiveSeal`, `Legacy` | green | green | 550377 |
| `safe_selective_terminal` | `LateLand`, `SelectiveSeal`, `Terminal` | green | green | 31959696 |
| `safe_clocklies_nocleanup` | `LateLand`, `ClockLies`, `NoCleanup` (2 slots, 3 appends) | green | green | 65024449 |
| `safe_clocklies_gc` | `ClockLies`, `SelectiveSeal` (2 slots, 3 appends) | green | green | 314526969 |

Invariants of the green rows: `INV_ACKED_REACHABLE` (or `INV_LIVE_ACKED_REACHABLE` with `ClockLies`),
`INV_GC_PREFIX`, `INV_GC_CUT_SOUND`, `INV_GC_NO_HOLD`, `INV_WRITER_PREFIX`, `INV_RECIPROCAL`, `INV_NO_TRACE` or
`INV_NO_TRACE_SELECTIVE`, `INV_OWNER_EXACT` and `INV_NO_DELETE_SERVABLE` where the tombstone precedes trust, and the
action property `PROP_MONOTONE`. Each `.cfg` lists its own set.

## Accepted residuals {#residuals}

| Config | Switches | Expectation | Result | Distinct states |
|---|---|---|---|---|
| `res_readwindow` | `LateLand` | `INV_OWNER_EXACT` | violated | 569 |
| `res_readwindow_gcdelete` | `LateLand` | `INV_NO_DELETE_SERVABLE` | violated | 1439 |
| `res_clocklies` | `LateLand`, `ClockLies` (2 slots, 3 appends) | `INV_ACKED_REACHABLE` | violated | 606710 |

- `res_readwindow*`: the read window without the selective rule. Closed by `SelectiveSeal`.
- `res_clocklies`: a superseded writer whose lease check passes wrongly acknowledges an append into a slot that
  cleanup emptied. It loses its own acknowledgement. Accepted.

## Sabotages {#sabotages}

Each removes one rule and must produce a counterexample.

| Config | Switches | Expectation | Result | Distinct states |
|---|---|---|---|---|
| `sab_sealblind` | `LateLand`, `SealBlind` | `INV_GC_PREFIX` | violated | 2266 |
| `sab_sealblind_monotone` | `LateLand`, `SealBlind` | `PROP_MONOTONE` | violated | 887 |
| `sab_sealskip` | `SealSkip` | `INV_ACKED_REACHABLE` | violated | 486 |
| `sab_selective_immediate` | `LateLand`, `SelectiveSeal`, `SelectiveImmediate` | `INV_NO_DELETE_SERVABLE` | violated | 2200 |
| `sab_gccursor_naive` | `GcCursorNaive` | `INV_GC_CUT_SOUND` | violated | 18096 |
| `sab_gccursor_naive_zombie` | `LateLand`, `ClockLies`, `GcCursorNaive` (2 slots, 3 appends) | `INV_GC_PREFIX` | violated | 5813872 |
| `sab_gccursor_probefirst_zombie` | `LateLand`, `ClockLies`, `GcCursorProbeFirst` (2 slots, 3 appends) | `INV_GC_PREFIX` | violated | 5092509 |
| `sab_gccursor_probefirst_lateland` | `LateLand`, `SelectiveSeal`, `GcCursorProbeFirst` | `INV_GC_CUT_SOUND` | violated | 291666 |
| `sab_gccursor_anyhead_zombie` | `LateLand`, `ClockLies`, `SelectiveSeal`, `GcCursorAnyHead` (2 slots, 3 appends) | `INV_GC_PREFIX` | violated | 12644665 |
| `sab_walk_norecheck` | `LateLand`, `SelectiveSeal`, `WalkNoRecheck` | `INV_OWNER_EXACT` | violated | 31757 |
| `sab_seal_norecheck_zombie` | `ClockLies`, `SelectiveSeal`, `SealNoRecheck` (2 slots, 3 appends) | `INV_NO_DELETE_SERVABLE` | violated | 267622 |
| `sab_seal_norecheck_zombie_acked` | `ClockLies`, `SelectiveSeal`, `SealNoRecheck` (2 slots, 3 appends) | `INV_LIVE_ACKED_REACHABLE` | violated | 871958 |

| Sabotage | Rule removed |
|---|---|
| `SealBlind` | the tombstone is a create-if-absent |
| `SealSkip` | a new epoch closes the old one before it appends |
| `SelectiveImmediate` | the selective rule compares the chain's epoch, not the immediate predecessor's stop |
| `GcCursorNaive`, `GcCursorProbeFirst` | GC does not interpret a key at or below its cursor and below the snapshot id |
| `GcCursorAnyHead` | below the snapshot GC walks back from the snapshot's epoch, it does not take any head naming the cursor |
| `WalkNoRecheck` | recovery re-reads the base after its last read |
| `SealNoRecheck` | recovery re-reads the base after its tombstone create |

## Witnesses {#witnesses}

Negated reachability: a "violation" means the state is reachable.

| Config | Switches | Expectation | Result | Distinct states |
|---|---|---|---|---|
| `witness_seal` | none | reachable | reachable | 355 |
| `witness_skipepoch` | none | reachable | reachable | 337 |
| `witness_adopt` | `LateLand` | reachable | reachable | 638 |
| `witness_lazyafterclean` | `SelectiveSeal` | reachable | reachable | 1038 |
| `witness_sealonlyepoch` | none | reachable | reachable | 5046 |
| `witness_sealconflict` | `LateLand` | reachable | reachable | 1022 |
| `witness_lateseal` | `LateLand` | reachable | reachable | 482 |
| `witness_sealafterfarewell` | `LateLand`, `SelectiveSeal` | reachable | reachable | 492 |
| `witness_terminal` | `Terminal` | reachable | reachable | 217 |
| `witness_mixedchain` | `Legacy` | reachable | reachable | 234 |
| `witness_cleanupseal` | none | reachable | reachable | 16821 |
| `witness_basemovedunderwalk` | `LateLand` | reachable | reachable | 1104 |
| `witness_cursordeleted` | none | reachable | reachable | 3161 |
| `witness_cursorreused` | `LateLand`, `ClockLies` (2 slots, 3 appends) | reachable | reachable | 110642 |

## Bounded searches {#bounded}

The state space of these rows is too large for an exhaustive run at these bounds. Each was run for 30 minutes with 6 workers; a
violation would be a finding, the absence of one is not a proof.

| Config | What it adds to `safe_clocklies_gc` | Result |
|---|---|---|
| `safe_clocklies_eager` | a tombstone at every recovery instead of the selective rule (the state after commit 5 of the series) | no violation in 63341418 distinct states, depth 37 |
| `safe_clocklies_gc_4txn` | four appends | no violation in 56987186 distinct states, depth 33 |
| `safe_clocklies_gc_lateland` | late landings together with the lying clock | no violation in 67717105 distinct states, depth 30 |

## What the model does not contain {#limits}

- The catalog, a second physical life, the dead-life janitor.
- The marker as a field copied through lease bodies; it is one durable value set at an unclean takeover.
- GC rounds, the destructive gate, the intake slice; GC is one cursor and deletion is one step.
- The snapshot object as a request separate from its `_ckpt` publication.
- More than one travelling copy of a request.
- A lagging second consumer of the fold cursor.

## Action to store requests {#requests}

| Action | Store requests it stands for |
|---|---|
| `WalkStart` | `GET _ckpt` (and the snapshot object) |
| `WalkStep` | one exact `GET _log/<id>` that is present |
| `WalkEnd` | the absent `GET`, then the `_ckpt` re-read |
| `StartSeal`, `StartAppend`, `Resend` | sending one create-if-absent `PUT _log/<id>` |
| `Land` | the store applying that `PUT` |
| `Resolve` | the response, or an exact `GET` of the slot; for a recovery tombstone also the `_ckpt` re-read |
| `SnapshotSend`, `SnapshotLand` | `PUT _snap` and the conditional `_ckpt` publish, as one request |
| `Cleanup` | one `DELETE _log/<id>` |
| `GcStep` | resolving the cursor's successor (0 to 2 `GET`) and one `GET` of it |
| `TakeoverClean`, `TakeoverUnclean` | the claim's conditional write of the lease body |
