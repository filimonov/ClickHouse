---
description: 'Design for CAS-298: the background mount-lease renewal retries until the store gives a definitive answer instead of giving up against the lease deadline, so a transient object-store outage no longer fences the mount and forces a fresh-epoch remount.'
sidebar_label: 'Mount renewal without a lease deadline'
sidebar_position: 103
slug: /superpowers/specs/cas-mount-renewal-without-lease-deadline
title: 'CAS mount renewal without a lease deadline'
doc_type: 'guide'
---

# CAS mount renewal without a lease deadline {#cas-mount-renewal-without-lease-deadline}

## Decision {#decision}

- **The background renewal has no deadline.** It retries one immutable body until the store gives a
  definitive answer, or until it is stopped or parked.
- **A transient failure is never terminal.** Terminal stays for definitive answers only: a foreign or
  successor body, `gc_fenced`, a vanished slot, a same-pair twin, a deterministic refusal.
- **Write authority is the clock alone.** `CasMountRuntime::admit` and `mayMutate` already refuse
  writes once the confirmed deadline is too close or past. A lapse trips nothing: no `tripMountLost`,
  no generation bump, no remount, no new epoch.
- **Writes resume when a renewal commits**, under the same `writer_epoch` and fence generation.
- **Prerequisite: GC timestamps a mount observation after the read that produced it.** Today
  `computeHeartbeatFloor` credits an observation with a clock sample taken before its `LIST` and
  `GET`, which can fence a holder that still has authority ([timing](#timing)). The fix lands first.

Closes [issue #2431](https://github.com/Altinity/ClickHouse/issues/2431) (CAS-298) and CAS-152.

## Context {#context}

A renewal is one conditional `PUT` of `gc/server-roots/<server_root_id>/mount`. Today it runs through
`CasOperation::writeLoop` under `Retry::untilLeaseSafe`. Before each attempt the engine reserves the
pause plus two attempt envelopes against the lease deadline.

With the defaults (TTL 30 s, period 10 s, margin 2 s, envelope 7 s) that leaves
`TTL − period − margin − 2 × envelope = 4 s` for retries, less the next jittered pause of up to 5 s.
Issue #2431 shows five renewals: four gave up after 1.8 to 2.6 s with about 17 s of confirmed budget
left, one committed after 3.6 s.

Giving up is terminal. The renewer enters `RenewalTerminal`, the runtime trips the fence and
schedules a whole-chain remount. The remount allocates a fresh epoch and
`claimMountAwaitingExpiry` watches the server's own unchanged token for
`TTL + TTL/20 + period/2 = 36.5 s` before reclaiming. An 8 s burst of `503` on `PUT` therefore costs
at least 36.5 s of refused writes, while the lease in the store is valid and no other party touched
the slot.

## Rules {#rules}

A review checks the design against this list.

1. **Scope.** The rules apply to the renewal started by the background renewal worker. The startup
   renewal, the renewal inside a remount and the direct renewal keep `Retry::untilLeaseSafe` and their
   current terminal behaviour.
2. **One logical renewal, one tuple.** A logical renewal fixes `(key, bytes, expected token,
   write_attempt_id, anchor)` before I/O. Every physical attempt repeats it byte for byte. Unchanged.
3. **Anchor.** The anchor is the `BOOTTIME` instant sampled before the first physical attempt. A
   commit sets the confirmed deadline to `anchor + TTL`. A response never moves it. Unchanged.
4. **No deadline.** The logical renewal is not bounded by the lease deadline or by a policy window and
   reserves nothing against either. It ends only on: commit, a definitive answer, stop, or park. The
   policy carries no lease bound and the window `UINT64_MAX`; `Retry::bind` saturates, so no
   reservation can fail against it.
5. **One engine call.** A logical renewal is one call of the request engine, so the resolve read that
   adopts a landed attempt always belongs to the call that sent it. No second logical renewal starts
   while one is unresolved.
6. **Definitive answers stay terminal.** Unchanged classification in `throwRenewConflict` and for
   `Refused`: foreign or successor occupant, own slot `gc_fenced`, own `(uuid, epoch)` body that is not
   this renewal's bytes, vanished slot, deterministic store refusal, deterministic local failure.
7. **Stop and park.** Today's classification is kept as it is. `NotAttempted` only when a stop was
   already requested when the renewal began and no request was sent. Every other stop or park that
   ends the renewal is terminal and forbids the farewell, whether or not a request was sent.
8. **Pacing.** Every pause taken under the worker policy is a flat 1 s: the reissue of a failed
   `PUT` of any failure kind, and the retry of a failed resolve read. The first-attempt fuse keeps its
   one immediate reissue per verb. A stop wakes the pause. A park is noticed within one pause plus one
   attempt in flight.
9. **Authority.** Write admission is unchanged. A lapse changes neither `mount_fence.lost`, nor
   `fence_generation`, nor the lifecycle enum, nor the epoch.
10. **Resume.** After a commit whose `anchor + TTL` is already past, writes stay refused and the
    existing catch-up cadence starts the next renewal at once. A lapse ends with the first commit whose
    anchor is fresh.
11. **Observer timestamps.** An observer records the first sighting of a token with a clock sample
    taken after the read returned, and tests stability with a sample taken before the confirming
    read. `claimMountAwaitingExpiry` does this today; `computeHeartbeatFloor` must.

## Why this is safe {#safety}

**Lemma.** A third party acts on the holder's presumed death only after it changed the slot token.
A renewal that commits against its expected token therefore proves that nobody acted.

Every writer of the mount slot is conditional on the token it observed:

| Writer | Where |
|---|---|
| GC fence-out | `readModifyWrite` in `computeHeartbeatFloor` |
| Claim: mint, refresh, reclaim, `cas_unsafe_remount_no_delay` | `create` / `replace` in `claimMount` |
| Renewal and adoption | `MountLeaseRenewer::renewOn`, `MountLeaseRenewer::claim` |
| Farewell | `MountLeaseRenewer::terminate` |
| Decommission | reclaims only a fenced or farewelled slot; token-exact deletes in `Tools/CasDecommission.cpp` |

Holder bodies carry a random `write_attempt_id`, so a token never repeats. `expires_at_ms` is read
for display and for a non-authoritative precheck only.

### Timing {#timing}

Authority from a commit ends at `anchor + TTL` on the holder's clock. An observer sees the new token
no earlier than its landing, which is no earlier than the anchor, and acts only after the token
stayed unchanged for `mountObservationThresholdMs` on its own clock. The holder side depends on
rule 3 only: a late retry does not move the anchor, so retrying past the deadline cannot extend
authority.

The observer side depends on rule 11, and GC breaks it today. `Gc::runRegularRound` passes
`mono_ms_fn()` to `computeHeartbeatFloor` as an argument, sampled before the `LIST` and the per-slot
reads, and the function stores that value as `first_seen_mono_ms`. A sighting made `d` after the
sample is credited `d` too early:

1. The GC leader samples its clock at `t0`, then spends `d` in the walk before it reads holder `H`'s
   slot and sees token `T`, committed with anchor `a <= t0 + d`. It records `first_seen = t0`.
2. A later round samples `t1 >= t0 + threshold`, reads `T` unchanged and fences.
3. `H` has authority until `a + TTL`, which is after the fence when `d > threshold − TTL`, 11.5 s
   with the defaults.

The token stays unchanged across both reads when `H` is healthy and the reads are less than one
period apart, or when `H`'s renewals are failing. Today the second case is masked, because a failing
renewal fences `H` itself about 4 s in. Under this design `H` keeps authority until the deadline, so
the defect must be fixed first: record `first_seen` from a sample taken inside the decision, after
the read, including the re-decision that follows a refused fence-out. The stability test keeps the
round-start sample, which is conservative.

### Late landings and writes across a lapse {#across-a-lapse}

**Late landings.** An attempt that lands after the holder stopped is conditional on the old token.
Either it lands and the next reader sees an unfenced, non-farewell body, or a successor moved the
token and it is refused. Both cases exist today (`WitnessLateBeforeReclaim`,
`WitnessLateAfterSuccessor` in `CaMountRenewRetryCore`).

**Writes across a lapse.** An operation admitted before a lapse may continue after the resume under
the same generation: a thread can stall between its gate and its send, or across a pause, and the
reservation is not an execution deadline. That is allowed. By the lemma nobody else held authority in
between, so such a write equals the same write inside one uninterrupted lease, and its safety rests
on the object protocols: every such write is a conditional create, a token-conditional update, or
content-addressed. The same holds for a request sent before the lapse that lands after the resume.
Callers that see a refusal during the lapse (`resolveWedgeOnce`, `commitRefChunk`, `publishCkpt`,
the catalog mutators, `ensureBlobPresent`) refuse or mark the table for recovery; none acts on it as
proof that the incarnation ended.

**Models.** `CaCasMountCore.Renew` has no expiry guard, and `Write` is guarded by `clock <
fenceUntil` only, so that model already contains lapse, late same-epoch `Renew`, then `Write`.
`NoRequestAfterSafeDeadline` is defined and used only inside `CaMountRenewRetryCore`.

## Behaviour by outage length {#behaviour}

Defaults; "outage" means `PUT` fails with a transient error.

| Outage | Lease | Writes |
|---|---|---|
| Under about 19 s | Never lapses. The renewal commits when the store returns. | Fail only while the store refuses them. Ref appends are refused at admission while less than `2 × envelope + margin = 16 s` remain, as today. |
| Longer, store-wide | Lapses at `anchor + 30 s`. | Refused until the store returns and two renewals commit: one with the stale anchor, one catch-up. Same epoch. |
| Longer, this node only | A GC leader elsewhere fences the slot once it saw one token on two rounds at least 41.5 s apart. | The renewal reads `gc_fenced`, which is terminal. The remount reclaims at once on the `Fenced` certificate with a fresh epoch. |

## Code shape {#code-shape}

- `computeHeartbeatFloor`: takes the clock as a function and samples it when it records a sighting
  (rule 11). Both call sites in `Gc/CasGc.cpp` change with it.
- `MountLeaseRenewer::renewOn`: the worker path uses the unbounded policy of rule 4 with the flat
  pause. Startup and direct renewals share `renew` with the worker today, so the policy is chosen by
  the caller's kind, not by the method. The remount path keeps `renewForRemount`.
- Request plane for the worker renewal: not gated by the mount fence, takes the worker's liveness
  predicate, and sleeps through the runtime's interruptible sleep. The existing open plane sleeps
  with a plain sleep, so the worker needs its own plane or that plane's sleep changes.
- `Retry`: one optional flat-pause field. When set, it replaces the pause in `gatedPause` and in the
  read loop that serves the resolve read.
- `CasMountRuntime::consumeRenewResult`: on a commit, detect that the previous deadline had passed
  and report the lapse ([observability](#observability)). Terminal handling is unchanged.
- `deliverMountRenewObservability`: drop the loop that replays one `DEBUG` line per attempt at
  completion. The summary line already carries the count.
- Not reachable any more for the worker path: `GaveUp{Deadline}` and `GaveUp{Unresolved}`. If the
  engine returns either, the result stays terminal as today.

## What does not change {#unchanged}

- The mount body format and the token discipline.
- Admission of data writes, and the envelope reservation of every write other than the worker
  renewal.
- The remount chain, `claimMountAwaitingExpiry`, the GC fence and its thresholds.
- `validateCasRequestBudget`. Its cadence inequality no longer bounds a retry window; relaxing it is
  a separate change.

## Observability {#observability}

A lapse must not look healthy.

- `system.cas_mounts`: while the lifecycle is `Live` and the confirmed deadline has passed, the row
  shows `lifecycle = 'not_live'`, `lifecycle_reason = 'lease_lapsed'`, `lifecycle_since` = the
  deadline. Derived when the row is built; the enum is untouched.
- Refusals during a lapse say that the lease renewal is failing and that writes resume when a
  renewal commits. They do not say that the incarnation was lost.
- On the commit that ends a lapse: `CASMountLeaseLapsed` +1, a `WARNING` with the lapse duration, and
  `lapsed_ms` in the `watermark_renew` event.
- `CASMountRenewalAttempts` and `CASMountRenewalRetries` advance as attempts are sent, not at
  completion, so a long outage is visible while it lasts.
- `CASMountLeaseLost` keeps its meaning: a terminal renewal.

## Wedged ref lanes after an outage {#wedged-lanes}

An append made ambiguous by the outage leaves its lane `Wedged`. `resolveWedgeOnce` runs only from
`flushRefBatch`, so the next mutation of that namespace settles it. Today a remount also clears it,
because `quiesceRefTablesForRemount` drops the runtime and the new epoch's seal settles the write.

Under this design an outage no longer remounts, so a wedge in a namespace that gets no further
mutation stays. It has no correctness consequence: the attempted tuple is retained and cannot be
bypassed. Its costs:

- a relink confirm for that table answers `Unknown`;
- `drainRefLanesForShutdown` reports a wedge, so the shutdown writes no farewell and the next start
  pays the observation wait;
- the runtime and its attempt bytes stay in memory, and cache eviction skips a runtime that is not
  `Ready`, so the cache can exceed its budget;
- snapshot publication for that table is refused.

This is not a new state: a blip shorter than today's 4 s window already leaves the same wedge, and
the code documents it as accepted. The worst case equals what every outage costs today, deferred to
the next restart and limited to servers with a quiet wedged table. Retrying wedged lanes in the
background is a separate change to the ref ledger and is out of scope here.

## Models {#models}

`CaMountRenewRetryCore` must express the following. The edits are the plan's business.

- A renewal can be sent and retried without write authority and past the confirmed deadline
  (`CanStartRequest` and the `SafeOperationDeadline` guards go).
- Write authority is derived: it holds only while `clock < confirmedDeadline`.
- No exhaustion transition: the `Exhausted` arm of `LocalTerminalize` goes. `MaxRetries` stays as a
  bound on the state space only.
- Two logical renewals in one behaviour: after a commit the model returns to a state from which
  `SendRenewal` fires again.
- Bounds large enough for a first commit whose `anchor + LeaseDuration` is strictly in the past.
- Invariants: drop `NoRequestAfterSafeDeadline` and `WitnessExhaustionFences`, keep the other ten,
  add `AuthorityEndsAtConfirmedDeadline`.
- Sabotage: the deadline is anchored at a later attempt. It must violate an invariant that compares
  against an independently recorded first-send instant.
- Witness: stale-anchor commit, then a second logical renewal commits and authority is restored.
- Sabotage for rule 11, in the model that owns the timing of `GCFence`: an observer credits a
  sighting with a sample taken before its read and fences a holder inside its authority.

`CaCasMountCore`: add a negated-reachability witness for `Renew` at `clock >= fenceUntil` followed by
`WedgeRetryCreate`, with every invariant of `CaCasMountCore_v9_recoverygen.cfg` enabled.

## Tests, failing first {#tests}

Unit tests use `RenewalScriptBackend` and `Ops` from `src/Disks/tests/gtest_cas_heartbeat.cpp` with
an injected boot clock, unless noted. The rule each test guards is in brackets.

1. `CASHeartbeat.GcCreditsASightingAfterItsRead` [11]: the clock function advances by more than
   `threshold − TTL` between the call of `computeHeartbeatFloor` and its read of a slot. A second
   call one threshold after the first call's start reads the same token and must not fence. A call
   one threshold after the read fences.
2. `CASHeartbeat.WorkerRenewalOutlivesTheReservationCutoff` [4]: every `PUT` fails transiently for
   8 s of injected time, then succeeds. The result is `Committed`, the renewer is `Active`, and the
   attempt count equals one per elapsed pause. Replaces
   `RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope`.
3. `CASHeartbeat.WorkerRenewalSendsOneTupleOnEveryAttempt` [2, 5]: across the same outage every
   recorded attempt carries identical bytes, expected token and `write_attempt_id`, and the backend
   sees no second body before the first is resolved.
4. `CASHeartbeat.WorkerRenewalCommitsPastTheDeadlineWithItsFirstAnchor` [3]: failures last longer
   than the TTL. The commit reports the first attempt's anchor; `anchor + TTL` is in the past.
5. `CASHeartbeat.LandedAttemptIsAdoptedAfterALongOutage` [5]: the first attempt lands and its
   response is lost; later attempts and resolve reads fail past the deadline; then a read succeeds.
   The result is `Committed` with `resolved_by_read`, not `same_epoch_state_uncertain`.
6. `CASHeartbeat.DefinitiveAnswersStayTerminalPastTheDeadline` [6]: after the deadline the slot
   turns `gc_fenced`, foreign, a newer own epoch, an own-epoch body with other bytes, or absent, or
   the store refuses deterministically. Each is terminal with today's classification.
7. `CASHeartbeat.WorkerRenewalPacesEveryPauseFlat` [8]: with a connect-failure text on every `PUT`
   and a failing resolve read, consecutive requests on the injected clock are exactly 1000 ms apart,
   apart from the one fuse reissue per verb, and the renewal is still running after 30 s.
8. `CASHeartbeat.StopDuringARetryPauseEndsTheRenewal` [7, 8]: a stop during the pause returns
   without waiting it out; the result is terminal and `release` is forbidden.
   `CancellationBeforeSendIsNotAttemptedAndAllowsRelease` and
   `CancellationAfterSendIsTerminalAndForbidsRelease` stay green.
9. `CASMountRuntime.ParkEndsAnUnboundedRenewalWithinOnePause`
   (`src/Disks/tests/gtest_cas_mount_runtime.cpp`) [7, 8]: a remount request while the worker
   renewal is retrying reaches `Parked` within one pause plus one attempt, and the renewal is
   terminal.
10. `CASHeartbeat.BoundedPathsStillStopAtTheLeaseDeadline` [1]: the startup renewal, the direct
    renewal and `renewForRemount` give up against the lease deadline with a typed terminal failure,
    as `DeadlineBeforeSendTerminalizesWithTypedFailure` asserts today.
11. `CASMountRuntime.LapseRefusesWritesAndResumesUnderTheSameEpoch` [9, 10]: driven by real worker
    renewals over a scripted backend, not by setting the deadline. Past the deadline `admit` returns
    `NoBudget` and `lost` is false. After the stale-anchor commit `admit` still returns `NoBudget`
    and the next renewal starts with no cadence wait. After that commit `admit` returns `Ok` with the
    same epoch and generation.
12. `CASMountRuntime.LapseIsReported`: during the lapse the lifecycle snapshot reads `not_live` /
    `lease_lapsed`; the ending commit increments `CASMountLeaseLapsed` and the event carries
    `lapsed_ms`.
13. Integration, `tests/integration/test_cas_mount_renewal_retry` with `s3_fault_proxy.py`:
    `test_short_put_outage_does_not_fence` holds `503` on `PUT` for 8 s across a renewal; no
    `fenced after` line on any node, `CASMountLeaseLost` unchanged, the next `INSERT` succeeds.
14. Same file: `test_long_put_outage_resumes_under_the_same_epoch` holds the fault for 45 s;
    afterwards `writer_epoch` in `system.cas_mounts` is unchanged, `CASMountLeaseLapsed` is 1 and an
    `INSERT` succeeds without a remount line in the log.
15. Soak card S39 queries every replica in its short-fault check.

## Boundaries checked {#boundaries-checked}

The lemma was checked beyond the mount, request-engine and ref-ledger code:

- `Gc/`: no unconditional writer of the mount slot and no destructive decision from elapsed time
  alone. Creator retirement uses the fenced, farewell or superseded-epoch certificate; orphan
  eligibility uses the durable epoch and build floor; recovery seals only older epochs; GC
  leadership rests on `gc/state`, not on this node's mount deadline. The one defect is the
  observation timestamp of rule 11.
- Code above `ContentAddressed/`: nothing parses the lease error text or consumes
  `system.cas_mounts` as authority. The refusal class stays `NETWORK_ERROR`, which
  `checkDataPart` and `ReplicatedMergeTreePartCheckThread` treat as retryable.
- `Backend/CasProbe.cpp`: the capability probe fails a store that accepts a stale-token `PUT` or
  `DELETE`.

Owed before code: both model changes run green in TLC.

## Documentation {#documentation}

- `docs/en/antalya/cas/architecture/mounts-and-leases.md`: the "Absolute deadline" and "Losing the
  lease" paragraphs, the lifecycle diagram, the 4 s scheduling-lateness sentence.
- `docs/en/antalya/cas/configuration.md` and `operations/troubleshooting.md`: the lapse state, the
  new event and the changed meaning of a renewal failure.
- `docs/superpowers/models/CaMountRenewRetryCore_RESULTS.md` and `README.md`.

## Options considered {#options}

- **Tune the numbers** (period 5 s, clamp the pause to what fits): the window grows to 9 s; the rule
  and the 36.5 s cost stay.
- **Bound the retries by the real deadline, keep terminal**: about 18 s of tolerance, then the same
  remount.
- **Fresh epoch after every lapse**, written against the holder's own confirmed token without the
  observation wait: keeps "any authority gap changes the epoch", but fails every in-flight build and
  re-recovers every ref table on each lapse, and is not needed for safety.
- **Fresh body and anchor per attempt**: breaks `OneLogicalBodyPerExpectedToken` and needs a set of
  pending attempt ids. The catch-up renewal gives the same result for one extra `PUT`.
- **Immediate reclaim of a self-authored slot** (CAS-153): makes the remount cheaper instead of
  avoiding it. Still useful after an unclean restart; no longer needed for a store outage.

## Out of scope {#out-of-scope}

- Background retry of wedged ref lanes.
- The envelope reservation and gates of data writes in `CasOperation`.
- Collapsing `RenewalDriverState` and the remount parking protocol.
- Relaxing `validateCasRequestBudget`.
