---
description: 'TLC results for the simplified CAS reference lane and its relink composition.'
sidebar_label: 'CAS ref lane TLA+ results'
sidebar_position: 20260730
slug: /superpowers/models/cas-ref-lane-core-results
title: 'CAS reference lane TLA+ results'
doc_type: 'reference'
---

# CAS reference lane TLA+ results {#cas-reference-lane-tla-results}

**Date:** 2026-08-01, updated 2026-10-08. **Verdict:** pass.

## Lane model {#lane-model}

`CaRefLaneCore.tla` models the six ownership states `Ready`, `Writing`, `Wedged`,
`NeedsRecovery`, `Closed`, and `Faulted`. It composes that single lane with a bounded cache model
containing two concrete runtime ids and two durable life ids. The predecessor runtime remains owned
by its captured handle while the non-authoritative logical-name slot may publish a distinct
successor after removal/rebirth or same-life self-remount. `StartWrite` captures `lane_runtime` in
`attempt_runtime`; `BeginResolve` carries it into `resolver_runtime`; every resolver observation
records `observation_runtime`; and the real write, install, observation, resolution, and recovery
transitions update or validate per-runtime projections of `cache_id` and `durable_id`.

The battery ran with:

```bash
docs/superpowers/models/run_reflane.sh
```

All 36 expectations passed:

- one honest exhaustive configuration was green;
- sixteen single-rule sabotages violated their named invariant or property;
- nineteen reachability witnesses violated their negated witness invariant.

The honest run (re-run 2026-10-08 with occupant outcomes and the fault latch) generated 4,086,679
states, found 1,226,710 distinct states, reached depth 28, and exhausted the queue. The run summary
is `tmp/lease-series/pr10-reflane.log`; detailed TLC output is under
`build/tlc-runs/reflane/20261008T125949-1309896`.

The sabotage controls cover arming before send, retaining uncertain attempts, blocking later
appends, complete recovery, exact attempt identity, and mount fencing. Two independent controls
attack the touch-scoped certification guard (`Ready`, or `Writing` while the outstanding mutation
does not touch the certified identity) rather than one, because TLC stops at the first violation it
finds and a single flag spanning both arms would always be caught by the broken-state arm first:
`sab_certifyblocked` certifies from a state that may certify nothing at all (`Wedged` at depth 4, via
a non-touching write that never lands); `sab_certifytouching` certifies in `Writing` while the
outstanding mutation touches the identity (a touching write lands at depth 4, so `cache_binding`
disagrees with `durable_binding`), proving the touch check itself, not merely the guard's presence,
is load-bearing. The new controls independently violate `NoOldHandleRetarget`,
`ExactPredecessorInvalidationPreservesSuccessor`, `PublishedRuntimeHasAcceptedIdentity`, and
`MissingNameConfirmationAllocatesNothing`. Every focused sabotage config checks all non-target
safety invariants before its expected RED. The new witnesses reach predecessor/successor
coexistence with a real predecessor-scoped `WriteLands`, same-life self-remount at accepted generation
3 after fence-loss generation 2, exact delayed invalidation that preserves the successor, and an
allocation-free missing-name confirmation. The old-handle sabotage drives that same real transition
to `r2`, while the honest trace updates only `r1`. Missing-confirm sabotage only reattaches already
valid live `r1` to an armed empty slot, so published identity remains valid. The earlier witnesses continue to reach ordinary commit,
unresolved write, recovery, inert unrelated result, `Closed`, and `Faulted`.

Resolver provenance is checked independently of application targeting. In the focused sabotage,
`resolver_runtime = r1` survives successor publication, but `ObserveSuccessorSealScoped` records
`observation_runtime = r2` from the current slot and violates only `NoOldHandleRetarget` after 2,689
generated / 1,003 distinct states at depth 8. The paired honest witness follows the same unresolved,
removal, rebirth, lookup, and successor-seal sequence, records the observation against `r1`, applies
it to `r1`, reaches `Closed`, and violates `W_RebirthResolverScope` after 9,071 generated / 3,941
distinct states at depth 9.

With only `NoOldHandleRetarget` omitted, the observation-retarget sabotage exhaustively preserves
every non-target invariant: 223,716 generated / 62,061 distinct states, empty queue, depth 21.

`witness_certifyoutstanding` reaches a certification in `Writing` with a same-binding attempt
outstanding and already durable, so the honest run's green `CertifiedViewIsCurrent` covers the
relaxed guard in the post-`PUT`, pre-install window and not only `Ready`. It violates
`W_CertifiedWhileOutstanding` after 62 generated / 39 distinct states at depth 4.

## Occupant outcomes and the fault latch {#occupant-outcomes-fault-latch}

The resolver and the append site's conflict both classify an occupant that is not the attempt's
bytes as `Foreign`, `Unreadable`, `ClassifyMemory` or `ClassifyDefect`; recovery can meet a foreign
object at its retained attempt. Every terminal observation latches the fence in the same step
(`FaultHasExit`). A terminal observation never returns the lane to `Ready`
(`TerminalObservationIsTerminal`), and a `Faulted` lane never leaves `Faulted` (`FaultedAbsorbing`).
A memory-limit classification returns the append to `Ready` and leaves a wedge wedged.
`sab_unlatchedfault` (fault without latch), `sab_recoverafterfault` (recovery ignoring the latch)
and `sab_leavefaulted` (`Faulted -> NeedsRecovery`, which no invariant forbids) are red on their
target. The latch is never reset, so after a latch the model explores no further writes, the
remounted runtime included. Not modelled: a non-`Ready` lane at new-id allocation (`StartWrite`
requires `Ready`) and remount liveness; `W_FaultThenRemount` only shows the remount is reachable
after a latch.

## Relink composition {#relink-composition}

`CaRelinkLaneComposition.tla` consumes the lane as a six-state component. It checks three seam
properties:

1. confirmation requires `Ready`, or `Writing` with an outstanding mutation that does not touch
   the identity (`ConfirmationRequiresUntouchedIdentity`);
2. promotion uses the exact confirmed identity;
3. source deletion requires receiver ownership.

The battery ran with:

```bash
docs/superpowers/models/run_relinklane.sh
```

All ten expectations passed: one honest configuration, three named sabotage violations, and six
reachability witnesses. The honest run (re-run 2026-09-02) generated 797 states, found 196 distinct
states, reached depth 11, and exhausted the queue. The retained run summary is
`build/tlc_relinklane_task2_fix1_final.log`; detailed TLC output is under
`build/tlc-runs/relinklane/20260902T181521-3415520`.

`witness_confirmedoutsideready` reaches a confirmation in `Writing` while the outstanding mutation
does not touch the identity, violating `W_ConfirmedOutsideReady` after 8 generated / 7 distinct
states at depth 3.

## Conclusion {#conclusion}

The reference lane is now modeled from its semantic obligations rather than reconstructed from the
old implementation's nested decisions. The C++ implementation follows that model, and the relink
seam depends only on the small certification contract: `Ready`, or `Writing` without an outstanding
mutation of the certified identity.
