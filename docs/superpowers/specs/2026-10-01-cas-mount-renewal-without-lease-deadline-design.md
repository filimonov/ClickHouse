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
   reserves nothing against either. It ends only on: commit, a definitive answer, stop, or park.
5. **One engine call.** A logical renewal is one call of the request engine, so the resolve read that
   adopts a landed attempt always belongs to the call that sent it. No second logical renewal starts
   while one is unresolved.
6. **Definitive answers stay terminal.** Unchanged classification in `throwRenewConflict` and for
   `Refused`: foreign or successor occupant, own slot `gc_fenced`, own `(uuid, epoch)` body that is not
   this renewal's bytes, vanished slot, deterministic store refusal, deterministic local failure.
7. **Stop and park.** A stop or park before any request was sent is `NotAttempted`. After a request
   was sent it is terminal and forbids the farewell. Unchanged.
8. **Pacing.** Failed attempts are spaced by a flat 1 s pause whatever the failure kind. The
   first-attempt fuse may reissue once at once. A stop wakes the pause. A park is noticed within one
   pause plus one attempt in flight.
9. **Authority.** Write admission is unchanged. A lapse changes neither `mount_fence.lost`, nor
   `fence_generation`, nor the lifecycle enum, nor the epoch.
10. **Resume.** After a commit whose `anchor + TTL` is already past, writes stay refused and the
    existing catch-up cadence starts the next renewal at once. A lapse ends with the first commit whose
    anchor is fresh.

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

**Timing.** Authority from a commit ends at `anchor + TTL` on the holder's clock. An observer sees
the new token no earlier than its landing, which is no earlier than the anchor, and then waits
`mountObservationThresholdMs` on its own clock. That argument is today's and depends on rule 3 only.
A late retry does not move the anchor, so retrying past the deadline cannot extend authority.

**Late landings.** An attempt that lands after the holder stopped is conditional on the old token.
Either it lands and the next reader sees an unfenced, non-farewell body, or a successor moved the
token and it is refused. Both cases exist today (`WitnessLateBeforeReclaim`,
`WitnessLateAfterSuccessor` in `CaMountRenewRetryCore`).

**Writes across a lapse.** Operations in the engine cannot sleep across a lapse: each attempt and
pause reserves its cost and gives up before the deadline. Callers that see a refusal
(`resolveWedgeOnce`, `commitRefChunk`, `publishCkpt`, the catalog mutators, `ensureBlobPresent`)
refuse or mark the table for recovery; none acts on it as proof that the incarnation ended. A request
sent before the lapse that lands after the resume is the same straggler as inside one uninterrupted
lease: every such write is a conditional create, a token-conditional update, or content-addressed.

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

- `MountLeaseRenewer::renewOn`: the worker path uses a policy with no lease bound, no window and the
  flat pause, on a request plane that is not gated by the mount fence and takes the worker's liveness
  predicate. The other three callers are unchanged.
- `Retry`: one optional flat-pause field. When set, the reissue pauses in `writeLoop` use it.
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
mutation stays. Its costs: a relink confirm for that table answers `Unknown`, and
`drainRefLanesForShutdown` reports a wedge, so the shutdown writes no farewell and the next start
pays the observation wait.

This is not a new state: a blip shorter than today's 4 s window already leaves the same wedge, and
the code documents it as accepted. The worst case equals what every outage costs today, deferred to
the next restart and limited to servers with a quiet wedged table. Retrying wedged lanes in the
background is a separate change to the ref ledger and is out of scope here.

## Models {#models}

`CaMountRenewRetryCore`:

- Remove `CanStartRequest` from `SendRenewal` and `RetrySend`, and the `SafeOperationDeadline` guard
  from `AcknowledgeRenewal` and `AcceptResolved`.
- Remove the `Exhausted` arm of `LocalTerminalize`.
- Make `localAuthority` depend on the clock: authority holds only while `clock < requestDeadline` of
  the confirmed body.
- Drop `NoRequestAfterSafeDeadline` and `WitnessExhaustionFences`. Keep the other ten invariants.
- Add the invariant `AuthorityEndsAtConfirmedDeadline`, a sabotage that anchors the deadline at a
  later attempt and must violate it, and a witness for commit with a stale anchor followed by a
  catch-up renewal.

`CaCasMountCore`: add a negated-reachability witness for `Renew` at `clock >= fenceUntil` followed by
`WedgeRetryCreate`, with every invariant of `CaCasMountCore_v9_recoverygen.cfg` enabled.

## Tests, failing first {#tests}

Unit tests use `RenewalScriptBackend` and `Ops` from `src/Disks/tests/gtest_cas_heartbeat.cpp` with
an injected boot clock, unless noted.

1. `CASHeartbeat.WorkerRenewalOutlivesTheReservationCutoff`: every `PUT` fails transiently for 8 s of
   injected time, then succeeds. The result is `Committed`, the renewer is `Active`. Replaces
   `RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope`.
2. `CASHeartbeat.WorkerRenewalCommitsPastTheDeadlineWithItsFirstAnchor`: failures last longer than
   the TTL. The commit reports the first attempt's anchor; `anchor + TTL` is in the past.
3. `CASHeartbeat.LandedAttemptIsAdoptedAfterALongOutage`: the first attempt lands and its response is
   lost; later attempts and resolve reads fail past the deadline; then a read succeeds. The result is
   `Committed` with `resolved_by_read`, not `same_epoch_state_uncertain`.
4. `CASHeartbeat.DefinitiveAnswersStayTerminalPastTheDeadline`: after the deadline the slot turns
   `gc_fenced`, foreign, or absent. Each is terminal with today's classification.
5. `CASHeartbeat.WorkerRenewalPacesAttemptsFlat`: with a connect-failure text on every attempt, 10 s
   of injected time sends at most 12 attempts.
6. `CASHeartbeat.StopDuringARetryPauseEndsTheRenewal`: a stop during the pause returns without
   waiting it out; the result is terminal and `release` is forbidden.
   `CancellationAfterSendIsTerminalAndForbidsRelease` stays green.
7. `CASHeartbeat.StartupRenewalStillStopsAtTheLeaseDeadline`: the bounded path keeps
   `DeadlineBeforeSendTerminalizesWithTypedFailure`'s behaviour.
8. `CASMountRuntime.LapseRefusesWritesAndResumesUnderTheSameEpoch`
   (`src/Disks/tests/gtest_cas_mount_runtime.cpp`): past the deadline `admit` returns `NoBudget` and
   `lost` is false; after a stale-anchor commit writes stay refused; after the catch-up commit
   `admit` returns `Ok` with the same epoch and generation.
9. `CASMountRuntime.LapseIsReported`: during the lapse the lifecycle snapshot reads `not_live` /
   `lease_lapsed`; the ending commit increments `CASMountLeaseLapsed` and the event carries
   `lapsed_ms`.
10. Integration, `tests/integration/test_cas_mount_renewal_retry` with `s3_fault_proxy.py`:
    `test_short_put_outage_does_not_fence` holds `503` on `PUT` for 8 s across a renewal; no
    `fenced after` line on any node, `CASMountLeaseLost` unchanged, the next `INSERT` succeeds.
11. Same file: `test_long_put_outage_resumes_under_the_same_epoch` holds the fault for 45 s;
    afterwards `writer_epoch` in `system.cas_mounts` is unchanged, `CASMountLeaseLapsed` is 1 and an
    `INSERT` succeeds without a remount line in the log.
12. Soak card S39 queries every replica in its short-fault check.

## Checks before the plan {#checks-before-plan}

The safety argument was verified by reading the mount, request-engine and ref-ledger code. Three
areas were read only at their boundary. Each check either confirms the lemma or blocks the plan.

- `Gc/`: no decision about a writer's liveness is derived from time or from `expires_at_ms` without
  a write to the mount slot.
- Code above `ContentAddressed/`: nothing reacts to the lease error text or to `system.cas_mounts`
  in a way that treats a refusal as loss of the incarnation.
- `Backend/CasProbe.cpp`: the capability probe refuses a store that does not honour `If-Match` on
  `PUT` and `DELETE`.
- TLC: both model changes above run green before code is written.

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
