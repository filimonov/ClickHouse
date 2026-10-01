---
description: 'Design for CAS-298 and CAS-292: the background mount-lease renewal keeps retrying until the store gives a definitive answer, the lease TTL decides when writes stop, and the renewal thread is not failed by the memory tracker, so a transient object-store outage or a memory-limit event no longer fences the mount and forces a fresh-epoch remount.'
sidebar_label: 'Mount renewal without a lease deadline'
sidebar_position: 103
slug: /superpowers/specs/cas-mount-renewal-without-lease-deadline
title: 'CAS mount renewal without a lease deadline'
doc_type: 'guide'
---

# CAS mount renewal without a lease deadline {#cas-mount-renewal-without-lease-deadline}

The background renewal of the mount lease keeps trying until the store answers. The lease TTL, not
the renewal's retry budget, decides when writes stop.

Closes [issue #2431](https://github.com/Altinity/ClickHouse/issues/2431) (CAS-298), CAS-152 and
[issue #2403](https://github.com/Altinity/ClickHouse/issues/2403) (CAS-292), and CAS-342, which is
the first step.

## Step one: GC counts a sighting from after its read {#gc-sighting}

GC treats a server as dead when its mount token stays unchanged for `TTL + TTL/20 + period`
(41.5 s) on the GC leader's own clock, and then writes `gc_fenced` into the slot. The wait must
start when GC saw the token. Today it starts earlier.

`Gc::runRegularRound` and the rebuild pass call `computeHeartbeatFloor` with a clock sample taken
before the `LIST` and the per-slot reads. The function stores that sample as the first sighting.
A sighting made `d` after the sample is credited `d` too early:

| Time | Event |
|---|---|
| 0 s | The round starts and samples the clock. The store is slow. |
| 18 s | The holder commits a renewal: token `T`, authority until 48 s. |
| 20 s | GC reaches the slot, sees `T`, records "first seen at 0 s". |
| 42 s | A second round sees `T` unchanged: 42 − 0 ≥ 41.5, so it fences. It really watched for 22 s. |
| 42 to 48 s | The slot is fenced and the holder still considers itself entitled to write. |

Both conditions are needed: the walk spends more than `threshold − TTL` (11.5 s; 10 s with a
5 percent faster observer clock) before the read, and a second round follows soon. The regular
timer waits a full `gc_interval_sec` after a round ends, so only a manual round, an early requested
round or the rebuild pass can be that second round.

Today a holder whose renewals fail fences itself about 14 s after its last success, which hides
most of this. The rest of this design keeps the lease until the TTL, so the fix comes first.

**Fix.** `computeHeartbeatFloor` takes the clock as a function and records a sighting with a sample
taken inside the decision, after the read, on the first decision and on the re-decision that
follows a refused fence-out. The stability test keeps the round-start sample, which is earlier than
the real instant and so errs on the safe side. `claimMountAwaitingExpiry` already samples this way.
No format change.

## How it works {#how-it-works}

1. A renewal succeeded. The lease is valid for `cas_mount_lease_ttl_ms` (30 s), counted from the
   moment that renewal started.
2. Wait `cas_mount_renew_period_ms` (10 s), counted from the same moment. Start the next renewal.
3. A renewal is one conditional `PUT` of the mount object. Its timeouts are the disk's connect
   timeout (1 s) and `cas_attempt_timeout_ms` (5 s) for each send and receive.
4. It succeeded: go to 1.
5. It failed for a transient reason (timeout, `503`, connection error): try again.
   - The request used a second or more: retry at once.
   - The request failed faster: wait out the rest of that second first, so retries never spin.
   - The outcome is unclear: read the object first to see whether the `PUT` landed.
6. Keep trying for as long as it takes. Meanwhile the clock decides about writes. Once the lease has
   expired, writes are refused. When a renewal restores the lease, writes are accepted again, under
   the same `writer_epoch`, with no remount.
7. Stop trying only on a definitive answer: the slot is no longer ours (another writer's body,
   `gc_fenced`, the object is gone), the store refuses the request for good, or the server is
   stopping or remounting. The exact list is rules 6 and 7. Then remount, as today.

Two details:

- New conditional writes stop being admitted about 16 s before the expiry, because a write reserves
  time for its own request and for the read that checks it. This is today's admission rule.
- After a long outage the first successful renewal counts from when it started, which may already
  be more than a TTL ago. The next renewal then follows at once, and that one restores the lease.

## What changes {#what-changes}

| | Today | With this design |
|---|---|---|
| Retries of a failed renewal | about 4 s | until a definitive answer |
| When retries end | fence, remount, fresh epoch, an observation of at least 36.5 s | they do not end |
| `PUT` fails for under about 19 s | at least 36.5 s without writes | the lease never expires |
| `PUT` fails for 60 s, store-wide | fence, then remount after the store returns | writes refused until the store returns and a renewal restores the lease |
| This node cut off, GC elsewhere healthy | fence and remount | a later GC round that saw the token unchanged for 41.5 s fences the slot; the renewal reads `gc_fenced`; the remount reclaims at once |
| Memory limit exceeded on the renewal thread | terminal renewal, or the thread ends | the tracker does not throw on that thread |

Today's 4 s is `TTL − period − margin − 2 × envelope`: before each request the engine reserves two
7 s envelopes against the lease deadline. The 36.5 s is the token-stability threshold in
`claimMountAwaitingExpiry`.

The startup renewal, the renewal inside a remount and the direct renewal do not change.

## Settings {#settings}

No new setting.

| Setting | Default | Role |
|---|---|---|
| `cas_mount_lease_ttl_ms` | 30000 | Lease length from the start of the last successful renewal. Writes are never admitted past it |
| `cas_mount_renew_period_ms` | 10000 | How often a renewal starts, counted from the start of the last successful one |
| `cas_attempt_timeout_ms` | 5000 | Send and receive timeout of one request; also the size of the time a data write reserves |
| `connect_timeout_ms` of the disk | 1000 | Connect timeout, applied to TCP and again to TLS |
| `cas_lease_safety_margin_ms` | 2000 | Margin for data writes and for the bounded renewals; not used by the background renewal |

The one-second spacing of retries is a constant in code.

## What you see {#observability}

| Signal | Where | Meaning |
|---|---|---|
| `lifecycle = 'not_live'`, `lifecycle_reason = 'lease_expired'`, `lifecycle_since`, `lifecycle_detail` | `system.cas_mounts` | The confirmed deadline has passed and no renewal has restored the lease yet. `lifecycle_since` is that deadline; `lifecycle_detail` carries the text of the last failed request |
| `CASMountLeaseExpired` (new) | `system.events` | +1 when a renewal restores a lease that had expired |
| `expired_ms` in the `watermark_renew` event; a `WARNING` with the last failure | `system.cas_log`, server log | How long the lease was expired and why |
| `CASMountRenewalAttempts`, `CASMountRenewalRetries` | `system.events` | Advance as requests are sent, not at the end, so an outage shows while it lasts |
| `CASMountLeaseLost` | `system.events` | Unchanged: the mount was fenced locally and a remount follows |
| Text of a write refused only because the lease expired | exception | Says the lease expired and that writes resume when a renewal succeeds. Other refusals keep their text |

The expired row is shown only while the lifecycle enum is `Live`; the enum itself is not touched.
`lifecycle_reason = 'lease_expired'` is about this server's own confirmed deadline. The existing
`state = 'expired'` column is a display value from the body's wall-clock `expires_at_ms` plus a skew
allowance. The two are independent signals and need not agree.

## Renewal under memory pressure {#memory-pressure}

A memory-limit exception inside a request is a transient failure and is retried like any other. Two
gaps remain, from issue #2403: an exception outside the request (`encodeBody`, `plane.admit`,
`consumeRenewResult`) ends the renewal thread for good, and the `PUT` runs on a pool thread with a
1 MiB buffer.

- `renewalLoop` holds `LockMemoryExceptionInThread(VariableContext::Global)` across each iteration.
  Allocations stay accounted; the tracker does not throw on this thread. Keeper and the transaction
  log use the same guard.
- `ObjectStorageBackend::conditionalWriteSettings` sets `s3_allow_parallel_part_upload = false`, so
  a conditional `PUT` runs on the calling thread and the guard covers it. This applies to every
  conditional control-plane write, not only the renewal. The caller waits for the `PUT` either way;
  a large conditional write uploads its parts one after another, which can take longer and uses
  less memory.
- A conditional `PUT` starts with a buffer of the body size, at least one byte, instead of 1 MiB;
  `WriteBufferFromS3` keeps its own growth and caps. Reads keep their current sizing.
- An unwrapped `std::bad_alloc` that reaches the request engine is still terminal. Classification
  of failures is unchanged.

## Why it is safe {#safety}

**Nobody acts on a holder's death without first changing the mount object.** GC fences by a
conditional write (`computeHeartbeatFloor`). A claim, a reclaim, `cas_unsafe_remount_no_delay` and
the farewell are conditional on the token they read. Decommission reclaims only a fenced or
farewelled slot and deletes by exact token. Holder bodies carry a random `write_attempt_id`, so a
token never repeats. `expires_at_ms` is used for display and for one non-authoritative precheck in
`allocateWriterEpoch`. A renewal that succeeds against its expected token therefore proves the slot
was not taken.

**The holder never has authority longer than an observer waits.** Authority ends at `start + TTL` on
the holder's clock. An observer sees the new token no earlier than that start and acts only after
the token stayed unchanged for `TTL + TTL/20 + cadence` on its own clock. Retrying does not move the
start. The observer side needs [step one](#gc-sighting).

**A request that lands late is harmless.** A renewal request that lands after the holder stopped is
conditional on the old token: it lands on an unfenced, non-farewell body, or a successor moved the
token and it is refused (`WitnessLateBeforeReclaim`, `WitnessLateAfterSuccessor`).

**A write that spans the expiry is the same write as inside one lease.** A thread can stall between
admission and send, or across a retry pause, and continue after the lease was restored; a reservation
is not an execution deadline. A request sent before the expiry can land after the restore. Nobody
else held authority in between, and every write on the mount plane is a conditional create, a
token-conditional update or delete, or content-addressed. Callers that see a refusal
(`resolveWedgeOnce`, `commitRefChunk`, `publishCkpt`, the catalog mutators, `ensureBlobPresent`)
refuse or mark the table for recovery; none treats it as the end of the incarnation.

**The models agree.** `CaCasMountCore.Renew` has no expiry guard and `Write` is guarded by the clock
only. `NoRequestAfterSafeDeadline` is defined and used only inside `CaMountRenewRetryCore`.

**Checked beyond the mount code.** In `Gc/`, creator retirement needs a fenced, farewell or
superseded-epoch certificate, orphan eligibility uses the durable epoch and build floor, recovery
seals only older epochs, and GC leadership rests on `gc/state`. Code above `ContentAddressed/` does
not parse the lease error and does not use `system.cas_mounts` as authority; the refusal class stays
`NETWORK_ERROR`, which the part-check paths retry. `Backend/CasProbe.cpp` fails a store that ignores
`If-Match` on `PUT` or `DELETE`.

## Rules {#rules}

The exact contract. A review checks the design against this list.

1. **Scope.** Rules 2 to 10 apply to the background renewal worker only. The startup renewal, the
   renewal inside a remount and the direct renewal keep `Retry::untilLeaseSafe` and their terminal
   behaviour.
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
8. **Spacing.** Each retry draws an interval of 800 to 1200 ms and waits until that long has passed
   since the failed request started; a request that already took that long is retried at once. This
   covers the ordinary, the connect-failure and the credential-refresh retry of the `PUT` and the
   retry of the read that checks it. The read that follows an unclear `PUT` is not a retry and is
   sent at once. The first-attempt fuse keeps its immediate reissue. Worst case is about five
   requests per second. A stop wakes the wait. A park is seen at the next admission check, after a
   request or a credential refresh already under way returns.
9. **Authority.** Write admission is unchanged: budget, `lost` and generation as today. An expiry
   changes neither `mount_fence.lost`, nor `fence_generation`, nor the lifecycle enum, nor the
   epoch.
10. **Resume.** After a success whose `start + TTL` is already past, writes stay refused and the
    existing catch-up cadence starts the next renewal at once. Writes return when a success leaves
    enough budget for admission.
11. **Observer timestamps.** An observer records the first sighting of a token with a clock sample
    taken after the read, and tests stability with a sample taken before the confirming read
    ([step one](#gc-sighting)).
12. **Memory guard.** The tracker does not throw on the renewal thread, and a conditional `PUT` runs
    on the thread that issues it ([memory pressure](#memory-pressure)).

## Code shape {#code-shape}

- `computeHeartbeatFloor` and its two callers in `Gc/CasGc.cpp`: [step one](#gc-sighting).
- `MountLeaseRenewer::renewOn`: the worker path uses the policy of rules 4 and 8. Startup and direct
  renewals share `renew` with the worker today, so the policy follows the caller's kind.
- Request plane of the worker renewal: not gated by the mount fence, takes the worker's liveness
  predicate, sleeps through the runtime's interruptible sleep. The existing open plane uses a plain
  sleep.
- `Retry`: one optional spacing field. `writeLoop` and the read loop record when each request
  started and pass it to the pause; `gatedPause` takes neither today.
- One callback from the worker plane to the runtime per sent request and per failure text. It feeds
  the two attempt counters and the last-failure text. The text is owned by the runtime under a
  mutex, outside the trivially copyable observability context; failing to record it is diagnostic
  only.
- `CasMountRuntime::consumeRenewResult`: on success, report an expiry that just ended.
- `deliverMountRenewObservability`: drop the loop that replays one `DEBUG` line per request.
- `renewalLoop`, `conditionalWriteSettings`, `nativeConditionalPut`:
  [memory pressure](#memory-pressure).
- `GaveUp{Deadline}` and `GaveUp{Unresolved}` are not reachable for the worker path. If the engine
  returns either, the result is terminal as today.

Unchanged: the mount body format and token discipline; admission and reservation of every write
other than the worker renewal; the remount chain; `claimMountAwaitingExpiry`; the GC thresholds;
`validateCasRequestBudget`, whose cadence inequality no longer bounds a retry window.

## Wedged ref lanes after an outage {#wedged-lanes}

An append made ambiguous by the outage leaves its lane `Wedged` until the next mutation of that
namespace runs `resolveWedgeOnce`. Today a remount also clears it. Without the remount, a wedge in a
namespace that gets no further mutation stays. It has no correctness consequence: the attempted
tuple is retained and cannot be bypassed. Costs:

- a relink confirm for that table answers `Unknown`;
- the shutdown writes no farewell, so the next start pays the observation wait;
- the runtime and its attempt bytes stay in memory, cache eviction skips it, and the cache can
  exceed its budget;
- snapshot publication for that table is refused.

The state is not new: a blip shorter than today's 4 s window leaves the same wedge. The worst case
equals what every outage costs today, deferred to the next restart and limited to servers with a
quiet wedged table. Background retry of wedged lanes is a separate change.

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
For rule 11, the model that owns the timing of `GCFence` gets a sabotage in which an observer credits
a sighting with a sample taken before its read and fences a holder inside its authority.

## Tests, failing first {#tests}

Unit tests use `RenewalScriptBackend` and `Ops` from `src/Disks/tests/gtest_cas_heartbeat.cpp` with
an injected boot clock; `CASMountRuntime` tests live in `gtest_cas_mount_runtime.cpp`. The rule each
test guards is in brackets.

0. `CASHeartbeat.GcCountsASightingFromAfterItsRead` [11], three cases over an injected clock function.
   The clock advances by more than `threshold − TTL` between the call of `computeHeartbeatFloor` and
   its read of a slot: a second call one threshold after the first call's start does not fence; a
   call one threshold after the read does. A sighting recorded in the re-decision after a refused
   fence-out is counted from after that read. A call whose own clock sample precedes the threshold
   does not fence, however long its walk takes.
1. `CASHeartbeat.WorkerRenewalOutlivesTheReservationCutoff` [4]: `PUT` fails transiently for 8 s,
   then succeeds. `Committed`, renewer `Active`, and the number of `PUT`s matches one per spacing
   interval. Replaces `RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope`.
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
   800 to 1200 ms apart. Unclear `PUT` with a failing read: the read follows its `PUT` at once;
   retries of the read and of the `PUT` keep the spacing. A request that takes 5 s on the injected
   clock is retried with no wait. In each case the first-attempt fuse reissues at once for its verb,
   and the renewal is still running after 30 s.
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
    `lease_expired`, `lifecycle_since` is the deadline and `lifecycle_detail` holds the last failure
    text; the two attempt counters advance during the outage; the restoring renewal increments
    `CASMountLeaseExpired` and the event carries `expired_ms`.
12. `CASMountRuntime.MemoryLimitDoesNotEndTheRenewalThread` [12]: with the global tracker over its
    limit, an allocation before the request and another while the result is consumed do not throw,
    the tracker still counts them, and the renewal over the in-memory backend succeeds. A memory-limit exception injected into a request
    is retried, does not trip the fence and does not end the thread.
13. `CASBackend.ConditionalPutRunsOnTheCallingThread` [12]: `conditionalWriteSettings` has parallel
    part upload off, and a conditional `PUT` over the S3 object storage runs on the thread that
    issued it. A small conditional `PUT` starts with a buffer of its body size, and an empty body with
    one byte.
14. Integration, `tests/integration/test_cas_mount_renewal_retry` with `s3_fault_proxy.py`:
    `test_short_put_outage_does_not_fence` holds `503` on `PUT` for 8 s across a renewal. No
    `fenced after` line on any node, `CASMountLeaseLost` unchanged, the next `INSERT` succeeds.
15. Same file, `test_long_put_outage_resumes_under_the_same_epoch`: the fault lasts 45 s. Afterwards
    `writer_epoch` in `system.cas_mounts` is unchanged, `CASMountLeaseExpired` is 1, an `INSERT`
    succeeds and the log has no remount line.
16. Soak card S39 queries every replica in its short-fault check.

## Documentation {#documentation}

- `docs/en/antalya/cas/architecture/mounts-and-leases.md`: the "Absolute deadline" and "Losing the
  lease" paragraphs, the lifecycle diagram, the 4 s scheduling-lateness sentence.
- `docs/en/antalya/cas/configuration.md`, `operations/troubleshooting.md`: the expired state, the
  new counter and event field, and what a failing renewal now means.
- `docs/superpowers/models/CaMountRenewRetryCore_RESULTS.md` and `README.md`.

## Options considered {#options}

- **Tune the numbers** (period 5 s, pause clamped to what fits): 9 s of retries; the rule and the
  36.5 s cost stay.
- **Retry until the real deadline, then fence and remount**: about 18 s of tolerance, then today's
  cost.
- **Fresh epoch after every expiry**, written against the holder's own token with no observation
  wait: fails every build in flight and re-recovers every ref table each time; not needed for safety.
- **Fresh body and start per request**: breaks `OneLogicalBodyPerExpectedToken` and needs a set of
  pending attempt ids; the catch-up renewal gives the same result for one extra `PUT`.
- **Standard S3 retry delays** (`retry_initial_delay_ms`, `retry_max_delay_ms`) for the spacing: up
  to 5 s of extra wait after the store returns.
- **Immediate reclaim of a self-authored slot** (CAS-153): makes the remount cheaper instead of
  avoiding it. Still useful after an unclean restart.
- **A preallocated buffer per renewer** for memory pressure: the object-storage API has no
  read-into-caller-buffer entry point and the SDK allocates per request anyway.

## Out of scope {#out-of-scope}

- Background retry of wedged ref lanes.
- Reservation and gates of data writes in `CasOperation`.
- Collapsing `RenewalDriverState` and the remount parking protocol.
- Renaming or splitting `cas_attempt_timeout_ms`; relaxing `validateCasRequestBudget`.
- The policy that an exception escaping the renewal loop's own state machine ends the thread.
