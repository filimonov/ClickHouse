---
description: 'Design for CAS-298: the background mount-lease renewal keeps retrying until the store gives a definitive answer, and the lease TTL alone decides whether writes are accepted, so a transient object-store outage no longer fences the mount and forces a fresh-epoch remount.'
sidebar_label: 'Mount renewal without a lease deadline'
sidebar_position: 103
slug: /superpowers/specs/cas-mount-renewal-without-lease-deadline
title: 'CAS mount renewal without a lease deadline'
doc_type: 'guide'
---

# CAS mount renewal without a lease deadline {#cas-mount-renewal-without-lease-deadline}

The background renewal of the mount lease keeps trying until the store answers. Whether writes are
accepted depends on one thing: has the lease TTL passed since the last successful renewal.

Closes [issue #2431](https://github.com/Altinity/ClickHouse/issues/2431) (CAS-298) and CAS-152.
Needs CAS-342 first.

## How it works {#how-it-works}

1. A renewal succeeded. The lease is valid for `cas_mount_lease_ttl_ms` (30 s), counted from the
   moment that renewal started.
2. Wait `cas_mount_renew_period_ms` (10 s), counted from the same moment. Start the next renewal.
3. A renewal is one conditional `PUT` of the mount object. One request may take up to
   `cas_attempt_timeout_ms` (5 s) plus the connect timeout.
4. It succeeded: go to 1.
5. It failed for a transient reason (timeout, `503`, connection error): try again.
   - The request used a second or more: retry at once.
   - The request failed faster: wait out the rest of that second first, so retries never spin.
   - The outcome is unclear: read the object first to see whether the `PUT` landed.
6. Keep trying for as long as it takes. Meanwhile the clock decides about writes. Once the lease has
   expired, writes are refused. When a renewal succeeds, writes are accepted again, under the same
   `writer_epoch`, with no remount.
7. Stop trying only when the store says the slot is no longer ours: another writer's body,
   `gc_fenced`, or the object is gone. Then remount, as today.

Two details:

- Writes stop about 16 s before the expiry, because a write reserves time for its own request and
  for the read that checks it. This is today's admission rule.
- After a long outage the first successful renewal counts from when it started, which may already
  be more than a TTL ago. The next renewal then follows at once, and that one restores the lease.

## What changes {#what-changes}

| | Today | With this design |
|---|---|---|
| Retries of a failed renewal | about 4 s | until a definitive answer |
| When retries end | fence, remount, fresh epoch, 36.5 s wait | they do not end |
| 8 s of failing `PUT` | at least 36.5 s without writes | nothing happens to the lease |
| 60 s of failing `PUT`, store-wide | fence, then remount after the store returns | writes refused from the expiry until the store returns |
| This node cut off, GC elsewhere healthy | fence and remount | GC fences the slot after 60 to 120 s; the renewal sees it; remount reclaims at once |

Today's 4 s is `TTL − period − margin − 2 × envelope`: before each request the engine reserves two
7 s envelopes against the lease deadline. The 36.5 s is the token-stability observation in
`claimMountAwaitingExpiry`.

The startup renewal, the renewal inside a remount and the direct renewal do not change.

## Settings {#settings}

No new setting.

| Setting | Default | Role |
|---|---|---|
| `cas_mount_lease_ttl_ms` | 30000 | How long writes are accepted after the start of the last successful renewal |
| `cas_mount_renew_period_ms` | 10000 | How often a renewal starts |
| `cas_attempt_timeout_ms` | 5000 | Send and receive timeout of one request |
| `connect_timeout_ms` of the disk | 1000 | Connect timeout of one request |
| `cas_lease_safety_margin_ms` | 2000 | Margin for data writes before the expiry; not used by the background renewal |

The one-second spacing of retries is a constant in code.

## What you see {#observability}

| Signal | Where | Meaning |
|---|---|---|
| `lifecycle = 'not_live'`, `lifecycle_reason = 'lease_expired'`, `lifecycle_since` | `system.cas_mounts` | The lease has expired and no renewal has succeeded yet |
| `CASMountLeaseExpired` (new) | `system.events` | +1 when a renewal restores a lease that had expired |
| `expired_ms` in the `watermark_renew` event; a `WARNING` | `system.cas_log`, server log | How long the lease was expired |
| `CASMountRenewalAttempts`, `CASMountRenewalRetries` | `system.events` | Advance as requests are sent, not at the end, so an outage shows while it lasts |
| `CASMountLeaseLost` | `system.events` | Unchanged: the slot was really lost and a remount follows |
| Text of a refused write | exception | Says the lease expired and that writes resume when a renewal succeeds |

The `system.cas_mounts` state is derived when the row is built. The lifecycle enum is not touched.

## Why it is safe {#safety}

**Nobody acts on a holder's death without first changing the mount object.** GC fences by a
conditional write (`computeHeartbeatFloor`). A claim, a reclaim, `cas_unsafe_remount_no_delay`, the
farewell and decommission are all conditional on the token they read. `expires_at_ms` is display
only. A renewal that succeeds against its expected token therefore proves the slot was not taken.

**The holder never has authority longer than an observer waits.** Authority ends at `start + TTL` on
the holder's clock. An observer sees the new token no earlier than that start and acts only after
the token stayed unchanged for `TTL + TTL/20 + cadence` on its own clock. Retrying does not move the
start. The observer side needs CAS-342: GC today credits a sighting with a clock sample taken before
its read.

**A write that spans the expiry is the same write as inside one lease.** A thread can stall between
admission and send and continue after the lease was restored. Nobody else held authority in between,
and every write on the mount plane is a conditional create, a token-conditional update or delete, or
content-addressed. Callers that see a refusal (`resolveWedgeOnce`, `commitRefChunk`, `publishCkpt`,
the catalog mutators, `ensureBlobPresent`) refuse or mark the table for recovery; none treats it as
the end of the incarnation.

**The models agree.** `CaCasMountCore.Renew` has no expiry guard and `Write` is guarded by the clock
only. `NoRequestAfterSafeDeadline` is defined and used only inside `CaMountRenewRetryCore`.

Checked beyond the mount code: `Gc/` takes no destructive decision from elapsed time alone; code
above `ContentAddressed/` does not parse the lease error and treats `NETWORK_ERROR` as retryable;
`Backend/CasProbe.cpp` fails a store that ignores `If-Match`.

## Rules {#rules}

The exact contract. A review checks the design against this list.

1. **Scope.** The background renewal worker only. The startup renewal, the renewal inside a remount
   and the direct renewal keep `Retry::untilLeaseSafe` and their terminal behaviour.
2. **One tuple.** A renewal fixes `(key, bytes, expected token, write_attempt_id, start)` before I/O.
   Every request repeats it byte for byte. Unchanged.
3. **Start.** The start is the `BOOTTIME` instant sampled before the first request. Success sets the
   deadline to `start + TTL`. A response never moves it. Unchanged.
4. **No deadline.** The renewal is not bounded by the lease or by a policy window and reserves
   nothing against either. It ends on success, a definitive answer, stop or park. The policy has no
   lease bound and the window `UINT64_MAX`; `Retry::bind` saturates, so the only horizon is the range
   of the clock.
5. **One engine call.** A renewal is one call of the request engine, so the read that adopts a
   landed request belongs to the call that sent it. No second renewal starts while one is unresolved.
6. **Definitive answers are terminal.** Unchanged classification: foreign or successor occupant,
   own slot `gc_fenced`, own `(uuid, epoch)` body with other bytes, vanished slot, deterministic
   store refusal, deterministic local failure.
7. **Stop and park.** Unchanged. `NotAttempted` only when a stop was already requested when the
   renewal began and nothing was sent. Any other stop or park that ends the renewal is terminal and
   forbids the farewell.
8. **Spacing.** Before a retry, of the `PUT` or of the read that checks it, the renewal waits until
   one second has passed since the failed request started, randomised by up to 20 percent. A request
   that already took that long is retried at once. The first-attempt fuse keeps its immediate
   reissue. A stop wakes the wait. A park is seen at the next admission check, after a request or a
   credential refresh already under way returns.
9. **Authority.** Write admission is unchanged. An expiry changes neither `mount_fence.lost`, nor
   `fence_generation`, nor the lifecycle enum, nor the epoch.
10. **Resume.** After a success whose `start + TTL` is already past, writes stay refused and the
    existing catch-up cadence starts the next renewal at once.
11. **Observer timestamps.** An observer records the first sighting of a token with a clock sample
    taken after the read, and tests stability with a sample taken before the confirming read
    (CAS-342).

## Code shape {#code-shape}

- `computeHeartbeatFloor` and its two callers in `Gc/CasGc.cpp`: CAS-342.
- `MountLeaseRenewer::renewOn`: the worker path uses the policy of rules 4 and 8. Startup and direct
  renewals share `renew` with the worker today, so the policy follows the caller's kind.
- Request plane of the worker renewal: not gated by the mount fence, takes the worker's liveness
  predicate, sleeps through the runtime's interruptible sleep. The existing open plane uses a plain
  sleep.
- `Retry`: one optional spacing field, used by `gatedPause` and by the read loop of the resolve read.
- `CasMountRuntime::consumeRenewResult`: on success, report an expiry that just ended.
- `deliverMountRenewObservability`: drop the loop that replays one `DEBUG` line per request.
- `GaveUp{Deadline}` and `GaveUp{Unresolved}` are not reachable for the worker path. If the engine
  returns either, the result is terminal as today.

Unchanged: the mount body format, admission and reservation of data writes, the remount chain,
`claimMountAwaitingExpiry`, the GC thresholds, `validateCasRequestBudget`.

## Wedged ref lanes after an outage {#wedged-lanes}

An append made ambiguous by the outage leaves its lane `Wedged` until the next mutation of that
namespace runs `resolveWedgeOnce`. Today a remount also clears it. Without the remount, a wedge in a
namespace that gets no further mutation stays. It has no correctness consequence: the attempted
tuple is retained and cannot be bypassed. Costs:

- a relink confirm for that table answers `Unknown`;
- the shutdown writes no farewell, so the next start pays the 36.5 s observation wait;
- the runtime stays in memory and cache eviction skips it;
- snapshot publication for that table is refused.

The state is not new: a blip shorter than today's 4 s window leaves the same wedge. The worst case
equals what every outage costs today, deferred to the next restart. Background retry of wedged lanes
is a separate change.

## Models {#models}

`CaMountRenewRetryCore` must express:

- a renewal sent and retried without write authority and past the deadline (`CanStartRequest` and
  the `SafeOperationDeadline` guards go);
- write authority derived from the clock: `clock < confirmedDeadline`;
- no exhaustion transition; `MaxRetries` bounds the state space only;
- two renewals in one behaviour, with bounds that allow a first success whose `start +
  LeaseDuration` is strictly past;
- invariants: drop `NoRequestAfterSafeDeadline` and `WitnessExhaustionFences`; keep the other ten,
  restated per renewal where they name one request; add `AuthorityEndsAtConfirmedDeadline`;
- sabotage: the deadline anchored at a later request, caught against an independently recorded
  first-send instant;
- witness: a stale success, then a second renewal succeeds and authority returns.

`CaCasMountCore`: a witness for `Renew` at `clock >= fenceUntil` followed by `WedgeRetryCreate`, with
the invariants of `CaCasMountCore_v9_recoverygen.cfg`. Both run green in TLC before code is written.

## Tests, failing first {#tests}

Unit tests use `RenewalScriptBackend` and `Ops` from `src/Disks/tests/gtest_cas_heartbeat.cpp` with
an injected boot clock; `CASMountRuntime` tests live in `gtest_cas_mount_runtime.cpp`. The rule each
test guards is in brackets. Tests for rule 11 belong to CAS-342.

1. `CASHeartbeat.WorkerRenewalOutlivesTheReservationCutoff` [4]: `PUT` fails transiently for 8 s,
   then succeeds. `Committed`, renewer `Active`. Replaces
   `RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope`.
2. `CASHeartbeat.WorkerRenewalSendsOneTupleOnEveryAttempt` [2, 5]: across that outage every request
   carries the same bytes, expected token and `write_attempt_id`; no second body appears before the
   first is resolved; the renewal succeeds when the outage ends.
3. `CASHeartbeat.WorkerRenewalSucceedsPastTheDeadlineWithItsFirstStart` [3]: failures last longer
   than the TTL. The result reports the first request's start; `start + TTL` is past.
4. `CASHeartbeat.LandedAttemptIsAdoptedAfterALongOutage` [5]: the first request lands, its response
   is lost, later requests and reads fail past the deadline, then a read succeeds. `Committed` with
   `resolved_by_read`, not `same_epoch_state_uncertain`.
5. `CASHeartbeat.DefinitiveAnswersStayTerminalPastTheDeadline` [6]: past the deadline the slot turns
   `gc_fenced`, foreign, a newer own epoch, an own-epoch body with other bytes, or absent; or the
   store refuses deterministically; or the request raises a deterministic local failure. Each is
   terminal with today's classification, asserted by name.
6. `CASHeartbeat.WorkerRenewalSpacesRetries` [8], three cases. Fast connect failures: `PUT`s start
   0.8 to 1.2 s apart. Unclear `PUT` with a failing read: the read follows its `PUT` at once; retries
   of the read and of the `PUT` keep the spacing. A request that takes 5 s on the injected clock is
   retried with no wait. In all three the renewal is still running after 30 s.
7. `CASHeartbeat.StopDuringARetryWaitEndsTheRenewal` [7, 8]: a stop during the wait returns without
   waiting it out; terminal; `release` forbidden. A stop after the renewal began but before its first
   send is terminal too. `CancellationBeforeSendIsNotAttemptedAndAllowsRelease` and
   `CancellationAfterSendIsTerminalAndForbidsRelease` stay green.
8. `CASMountRuntime.ParkEndsAnUnboundedRenewal` [7, 8]: a remount request while the worker renewal
   retries reaches `Parked` within one wait plus one request; the renewal is terminal.
9. `CASHeartbeat.BoundedPathsStillStopAtTheLeaseDeadline` [1]: startup, direct and `renewForRemount`
   give up against the lease deadline, as `DeadlineBeforeSendTerminalizesWithTypedFailure` asserts.
10. `CASMountRuntime.ExpiryRefusesWritesAndResumesUnderTheSameEpoch` [9, 10]: driven by real worker
    renewals over a scripted backend. Past the deadline `admit` returns `NoBudget`, `lost` is false,
    the lifecycle enum is `Live`. After the stale success `admit` still returns `NoBudget` and the
    next renewal starts with no cadence wait. After it `admit` returns `Ok`, same epoch and
    generation.
11. `CASMountRuntime.ExpiryIsReported`: during the expiry the lifecycle snapshot reads `not_live` /
    `lease_expired`; the restoring renewal increments `CASMountLeaseExpired` and the event carries
    `expired_ms`.
12. Integration, `tests/integration/test_cas_mount_renewal_retry` with `s3_fault_proxy.py`:
    `test_short_put_outage_does_not_fence` holds `503` on `PUT` for 8 s across a renewal. No
    `fenced after` line on any node, `CASMountLeaseLost` unchanged, the next `INSERT` succeeds.
13. Same file, `test_long_put_outage_resumes_under_the_same_epoch`: the fault lasts 45 s. Afterwards
    `writer_epoch` in `system.cas_mounts` is unchanged, `CASMountLeaseExpired` is 1, an `INSERT`
    succeeds and the log has no remount line.
14. Soak card S39 queries every replica in its short-fault check.

## Documentation {#documentation}

- `docs/en/antalya/cas/architecture/mounts-and-leases.md`: the "Absolute deadline" and "Losing the
  lease" paragraphs, the lifecycle diagram, the 4 s scheduling-lateness sentence.
- `docs/en/antalya/cas/configuration.md`, `operations/troubleshooting.md`: the expired state and the
  new counter.
- `docs/superpowers/models/CaMountRenewRetryCore_RESULTS.md` and `README.md`.

## Options considered {#options}

- **Tune the numbers** (period 5 s): 9 s of retries; the rule and the 36.5 s cost stay.
- **Retry until the TTL, then fence and remount**: 30 s of tolerance, then today's cost.
- **Fresh epoch after every expiry**, written against the holder's own token: fails every build in
  flight and re-recovers every ref table each time; not needed for safety.
- **Fresh body per request**: needs a set of pending attempt ids; the catch-up renewal gives the same
  result for one extra `PUT`.
- **Standard S3 retry delays** (`retry_initial_delay_ms`, `retry_max_delay_ms`) for the spacing: up
  to 5 s of extra wait after the store returns.
- **Immediate reclaim of a self-authored slot** (CAS-153): makes the remount cheaper instead of
  avoiding it. Still useful after an unclean restart.

## Out of scope {#out-of-scope}

- Background retry of wedged ref lanes.
- Reservation and gates of data writes in `CasOperation`.
- Collapsing `RenewalDriverState` and the remount parking protocol.
- Renaming or splitting `cas_attempt_timeout_ms`; relaxing `validateCasRequestBudget`.
