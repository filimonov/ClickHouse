---
description: 'Design for CAS-298, CAS-292 and CAS-342: the mount lease is renewed by one loop that keeps retrying until the store gives a definitive answer, the lease TTL decides when writes stop, GC counts a token sighting from after its read, the renewal thread is not failed by the memory tracker, and the code that existed only to keep renewals inside a lease budget is removed.'
sidebar_label: 'Mount renewal without a lease deadline'
sidebar_position: 103
slug: /superpowers/specs/cas-mount-renewal-without-lease-deadline
title: 'CAS mount renewal without a lease deadline'
doc_type: 'guide'
---

# CAS mount renewal without a lease deadline {#cas-mount-renewal-without-lease-deadline}

One loop renews the mount lease and keeps trying until the store answers. The lease TTL, not the
renewal's retry budget, decides when writes stop. The code that existed to keep a renewal inside a
lease budget goes away.

Closes [issue #2431](https://github.com/Altinity/ClickHouse/issues/2431) (CAS-298), CAS-152,
[issue #2403](https://github.com/Altinity/ClickHouse/issues/2403) (CAS-292) and CAS-342.

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

**Fix.** A sighting is recorded with a clock sample taken after the read that produced it, on the
first decision and on the re-decision that follows a refused fence-out. The stability test uses a
sample taken before the confirming read; the round-start sample qualifies. `claimMountAwaitingExpiry`
already samples this way. Both observers use one helper, [`TokenWatch`](#what-goes-away). No format
change.

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
   stopping or was told to remount. Then remount, as today.

Three details:

- New conditional writes stop being admitted about 16 s before the expiry, because a write reserves
  time for its own request and for the read that checks it. This is today's admission rule.
- After a long outage the first successful renewal counts from when it started, which may already
  be more than a TTL ago. The next renewal then follows at once, and that one restores the lease.
- The same holds at startup and after a remount: the lease counts from the claim. If the claim is
  already old, the loop renews at once. Nothing else renews.

## What changes {#what-changes}

| | Today | With this design |
|---|---|---|
| Retries of a failed renewal | about 4 s | until a definitive answer |
| When retries end | fence, remount, fresh epoch, an observation of at least 36.5 s | they do not end |
| `PUT` fails for under about 19 s | at least 36.5 s without writes | the lease never expires |
| `PUT` fails for 60 s, store-wide | fence, then remount after the store returns | writes refused until the store returns and a renewal restores the lease |
| This node cut off, GC elsewhere healthy | fence and remount | a later GC round that saw the token unchanged for 41.5 s fences the slot; the renewal reads `gc_fenced`; the remount reclaims at once |
| Memory limit exceeded on the renewal thread | terminal renewal, or the thread ends | the tracker does not throw on that thread |
| Ways to renew | four: worker, startup, remount, direct | one: the loop |
| Threads per mount for the lease | two, with a parking protocol | one |

Today's 4 s is `TTL − period − margin − 2 × envelope`: before each request the engine reserves two
7 s envelopes against the lease deadline. The 36.5 s is the token-stability threshold in
`claimMountAwaitingExpiry`.

## Settings {#settings}

No new setting.

| Setting | Default | Role |
|---|---|---|
| `cas_mount_lease_ttl_ms` | 30000 | Lease length from the start of the last successful renewal. Writes are never admitted past it |
| `cas_mount_renew_period_ms` | 10000 | How often a renewal starts, counted from the start of the last successful one |
| `cas_attempt_timeout_ms` | 5000 | Send and receive timeout of one request; also the size of the time a data write reserves |
| `connect_timeout_ms` of the disk | 1000 | Connect timeout, applied to TCP and again to TLS |
| `cas_lease_safety_margin_ms` | 2000 | Margin that write admission and the farewell keep before the expiry; not used by renewals |

The one-second spacing of retries is a constant in code. `validateCasRequestBudget` keeps both
inequalities. `period + 2 × envelope + margin < TTL` now means: between two renewals there is always
room to admit a write.

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

## Simpler code {#simpler-code}

Most of the machinery around the lease exists because a renewal used to need lease budget. Without
that need it has no job. Nothing is built in its place.

### One loop {#one-loop}

One thread per writable mount owns the lease:

```text
until stopped:
    if the fence is lost:              # terminal renewal, interference report from the ref ledger
        reclaim, or wait and retry     # today's remount chain: probe, fresh epoch, claim, quiesce, arm
    wait until one period after the last successful start
    renew                              # retries inside; ends on success, a definitive answer,
                                       # stop, or a remount request
    success:  deadline = start + TTL
    terminal: trip the fence
```

The loop exits on stop and on a terminal lifecycle (`IdentityLost`, `VanishedReplaced`,
`VanishedForgotten`), as both workers do today.

### What goes away {#what-goes-away}

| Removed | What it guaranteed | What guarantees it now |
|---|---|---|
| The startup and remount re-anchor renewals (`renewRenewerForStartupOnce`, `renewRenewerForRemountOnce`, `renewForRemount`) and the copied `period + 2 × envelope` window test in `mountWritable` and `tryRemountOnce` | The first worker renewal still had lease budget after a slow claim or a long quiesce | A renewal needs no budget. The fence is armed from the claim; if that deadline is stale, the loop renews at once |
| The direct renewal (`renewWatermarkOnce`, `DirectCall`), used by tests only | Tests could drive one renewal | Tests call the loop's renewal step |
| The remount worker thread, `ParkRequested` / `Parked`, `DriverLease`, `admitRenewerCall`, all nine `RenewalDriverState` values | The renewer is never replaced while a renewal is in flight; one driver at a time | One thread renews and reclaims in sequence |
| The thread-local observability stack, its deferred delivery and nesting (`MountRenewObservabilityStack`, `configureMountRenewObservability`, `deliverDeferredMountRenewObservability`) | The sink reached `renewOn` without a parameter; a sink that renews was tolerated; logging waited until `remount_mutex` was released | `renew` returns its result and the loop reports it. No renewal runs under `remount_mutex`. An event sink must not renew; production sinks only append to a system log |
| `Retry::untilLeaseSafe`, `Retry::lease_deadline_ms`, `Retry::Bound::lease_bound`; classifications `external_lease_deadline` and `request_deadline` | The renewal and the farewell ended before the lease did | The renewal has no bound. The farewell passes `Retry::within(min(window, deadline − margin − now))`, the same instant computed by the caller |
| `MountLeaseRenewer::confirmed_deadline_boot_ms`; renewals on the fence-gated mount plane | A copy of the fence deadline inside the renewer; a renewal that the fence could refuse | The runtime's fence deadline is the only copy and the farewell receives it as an argument. Renewals run on an ungated plane whose sleep a stop wakes. Claim and farewell keep their plane and its plain sleep, because the farewell runs after the stop |
| The token-stability code written twice | See [step one](#gc-sighting) | `TokenWatch`: `sighted(token, sample_after_read)` and `stableFor(sample_before_read)`, used by `claimMountAwaitingExpiry` and `computeHeartbeatFloor` |
| `2 × envelope` computed in `refAppendFenceOk`, `validateCasRequestBudget` and `terminate`; the literals `2000` (four places) and `90'000` (three); ignored parameters of `consumeRenewResult`; comments about a stack-local observer and an unclean-boundary marker that do not exist; "Startup-only" in the text of `cas_lease_safety_margin_ms` | Nothing | One definition each; the stale text is corrected |

Estimated effect on `CasMountRuntime.cpp`, `CasServerRoot.cpp` and `CasPool.cpp`: about 900 of
3,000 lines removed. The estimate comes from the line counts of the removed areas, not from a
patch.

Tests that pin the removed machinery go with it: the parking and driver-state tests in
`gtest_cas_pool.cpp` and the nested-renewal tests in `gtest_cas_event_log.cpp`. The behaviours they
protected are covered by the tests [below](#tests).

### What stays {#what-stays}

- The claim protocol, the observation wait, the epoch and owner objects, the remount chain's steps
  and their order.
- The renewer's states (`New`, `Active`, `RenewalTerminal`, `Released`) and the tuple and start
  rules.
- The fence, `admit`, `mayMutate`, the fence generation, lifecycle terminal publication.
- The request engine: reservations, gates and pauses of every data write.
- The GC leader lease. It looks similar and is a different thing: it counts loop ticks, has no TTL
  and no clock, keeps its heartbeat in a separate object, and safety does not rest on it (GC deletes
  by exact token and commits by a conditional write of `gc/state`). The only place GC meets the
  mount lease is the fence-out of dead mounts, which `TokenWatch` now serves.

### One behaviour change {#behaviour-change}

Today an open or a remount whose claim is more than 4 s old renews once before arming the fence.
With one renewal path the fence is armed from the claim and the loop renews next. If the claim is
more than 14 s old at that moment, new conditional writes are refused until that renewal lands. The
refusal is the transient class that callers already retry.

## Renewal under memory pressure {#memory-pressure}

A memory-limit exception inside a request is a transient failure and is retried like any other. Two
gaps remain, from issue #2403: an exception outside the request ends the renewal thread for good,
and the `PUT` runs on a pool thread with a 1 MiB buffer.

- The loop holds `LockMemoryExceptionInThread(VariableContext::Global)` across each iteration.
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

1. **One path.** Only the loop renews. Open and remount arm the fence from the claim and do not
   renew.
2. **One tuple.** A renewal fixes `(key, bytes, expected token, write_attempt_id, start)` before I/O.
   Every request repeats it byte for byte. Unchanged.
3. **Start.** The start is the `BOOTTIME` instant sampled before the first request. Success sets the
   deadline to `start + TTL`. A response never moves it. Unchanged.
4. **No deadline.** The renewal is not bounded by the lease or by a policy window and reserves
   nothing against either. It ends on success, a definitive answer, stop or a remount request. The
   policy window is `UINT64_MAX`; `Retry::bind` saturates, so the only horizon is the range of the
   clock.
5. **One engine call.** A renewal is one call of the request engine, so the read that adopts a
   landed request belongs to the call that sent it. No second renewal starts while one is unresolved.
6. **Definitive answers are terminal.** Unchanged classification: foreign or successor occupant,
   own slot `gc_fenced`, own `(uuid, epoch)` body with other bytes, vanished slot, a refused
   precondition with an unreadable slot, deterministic store refusal, deterministic local failure.
7. **Stop and remount request.** `NotAttempted` only when a stop was already requested when the
   renewal began and nothing was sent. Any other stop or remount request that ends the renewal is
   terminal and forbids the farewell. Unchanged classification.
8. **Spacing.** Each retry draws an interval of 800 to 1200 ms and waits until that long has passed
   since the failed request started; a request that already took that long is retried at once. This
   covers the ordinary, the connect-failure and the credential-refresh retry of the `PUT` and the
   retry of the read that checks it. The read that follows an unclear `PUT` is not a retry and is
   sent at once. The first-attempt fuse keeps its immediate reissue. Worst case is about five
   requests per second. A stop wakes the wait. A remount request is seen at the next admission
   check, after a request or a credential refresh already under way returns.
9. **Authority.** Write admission is unchanged: budget, `lost` and generation as today. An expiry
   changes neither `mount_fence.lost`, nor `fence_generation`, nor the lifecycle enum, nor the
   epoch.
10. **Resume.** After a success whose `start + TTL` is already past, writes stay refused and the
    cadence starts the next renewal at once. Writes return when a success leaves enough budget for
    admission.
11. **Observer timestamps.** An observer records the first sighting of a token with a clock sample
    taken after the read, and tests stability with a sample taken before the confirming read.
12. **Memory guard.** The tracker does not throw on the lease thread, and a conditional `PUT` runs
    on the thread that issues it.
13. **One owner of the renewer.** Only the lease thread renews, replaces or reclaims while it runs.
    Open installs the renewer before the thread starts; teardown writes the farewell after it is
    joined.

## Order of work {#order-of-work}

Each step leaves the tree working. Steps 4 to 7 only remove code and can be dropped or deferred.

1. `TokenWatch` and the GC sighting fix (CAS-342).
2. Memory guard, inline `PUT`, buffer size (CAS-292).
3. The loop's renewal without a deadline, spacing, expiry reporting (CAS-298).
4. One renewal path: remove the re-anchors, the direct renewal and the lease-bound policy.
5. One thread: move the reclaim into the loop; remove the remount worker and the driver states.
6. The renewal returns its result; remove the thread-local stack.
7. One definition for each duplicated quantity; stale text.

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
the invariants of `CaCasMountCore_v9_recoverygen.cfg`. For rule 11, the model that owns the timing
of `GCFence` gets a sabotage in which an observer credits a sighting with a sample taken before its
read and fences a holder inside its authority. All run green in TLC before code is written.

## Tests, failing first {#tests}

Unit tests use `RenewalScriptBackend` and `Ops` from `src/Disks/tests/gtest_cas_heartbeat.cpp` with
an injected boot clock; `CASMountRuntime` tests live in `gtest_cas_mount_runtime.cpp`. The rule each
test guards is in brackets. Order follows the [order of work](#order-of-work).

1. `CASTokenWatch.CountsFromAfterTheRead` [11]: a sighting is dated by the sample passed after the
   read; `stableFor` compares against the sample passed before the confirming read; a changed token
   restarts the watch.
2. `CASHeartbeat.GcCountsASightingFromAfterItsRead` [11], three cases over an injected clock
   function. The clock advances by more than `threshold − TTL` between the call of
   `computeHeartbeatFloor` and its read of a slot: a second call one threshold after the first
   call's start does not fence; a call one threshold after the read does. A sighting recorded in the
   re-decision after a refused fence-out is counted from after that read. A call whose own clock
   sample precedes the threshold does not fence, however long its walk takes.
3. `CASMountRuntime.MemoryLimitDoesNotEndTheLeaseThread` [12]: with the global tracker over its
   limit, an allocation before the request and another while the result is consumed do not throw,
   the tracker still counts them, and the renewal over the in-memory backend succeeds. A memory-limit
   exception injected into a request is retried, does not trip the fence and does not end the thread.
4. `CASBackend.ConditionalPutRunsOnTheCallingThread` [12]: `conditionalWriteSettings` has parallel
   part upload off, and a conditional `PUT` over the S3 object storage runs on the thread that
   issued it. A small conditional `PUT` starts with a buffer of its body size, and an empty body with
   one byte.
5. `CASHeartbeat.RenewalOutlivesTheReservationCutoff` [4]: `PUT` fails transiently for 8 s, then
   succeeds. `Committed`, renewer `Active`, and the number of `PUT`s matches one per spacing
   interval. Replaces `RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope`.
6. `CASHeartbeat.RenewalSendsOneTupleOnEveryAttempt` [2, 5]: across that outage every request
   carries the same bytes, expected token and `write_attempt_id`; no second body appears before the
   first is resolved; the renewal succeeds when the outage ends.
7. `CASHeartbeat.RenewalSucceedsPastTheDeadlineWithItsFirstStart` [3]: failures last longer than the
   TTL. The result reports the first request's start; `start + TTL` is past.
8. `CASHeartbeat.LandedAttemptIsAdoptedAfterALongOutage` [5]: the first request lands, its response
   is lost, later requests and reads fail past the deadline, then a read succeeds. `Committed` with
   `resolved_by_read`, not `same_epoch_state_uncertain`.
9. `CASHeartbeat.DefinitiveAnswersStayTerminalPastTheDeadline` [6]: past the deadline the slot turns
   `gc_fenced`, foreign, a newer own epoch, an own-epoch body with other bytes, or absent; or the
   store refuses deterministically; or the request raises a deterministic local failure. Each is
   terminal with today's classification, asserted by name.
10. `CASHeartbeat.RenewalSpacesRetries` [8], three cases. Fast connect failures: `PUT`s start 800 to
    1200 ms apart. Unclear `PUT` with a failing read: the read follows its `PUT` at once; retries of
    the read and of the `PUT` keep the spacing. A request that takes 5 s on the injected clock is
    retried with no wait. In each case the first-attempt fuse reissues at once for its verb, and the
    renewal is still running after 30 s.
11. `CASHeartbeat.StopDuringARetryWaitEndsTheRenewal` [7, 8]: a stop during the wait returns without
    waiting it out; terminal; `release` forbidden. A stop after the renewal began but before its
    first send is terminal too. `CancellationBeforeSendIsNotAttemptedAndAllowsRelease` and
    `CancellationAfterSendIsTerminalAndForbidsRelease` stay green.
12. `CASMountRuntime.ExpiryRefusesWritesAndResumesUnderTheSameEpoch` [9, 10]: driven by the real
    loop over a scripted backend. Past the deadline `admit` returns `NoBudget`, `lost` is false, the
    lifecycle enum is `Live`. After the stale success `admit` still returns `NoBudget` and the next
    renewal starts with no cadence wait. After it `admit` returns `Ok`, same epoch and generation.
13. `CASMountRuntime.ExpiryIsReported`: during the expiry the lifecycle snapshot reads `not_live` /
    `lease_expired`, `lifecycle_since` is the deadline and `lifecycle_detail` holds the last failure
    text; the two attempt counters advance during the outage; the restoring renewal increments
    `CASMountLeaseExpired` and the event carries `expired_ms`.
14. `CASMountRuntime.OpenArmsFromTheClaimAndTheLoopRenews` [1]: an open whose claim is 20 s old arms
    the fence with the claim's deadline and sends no renewal itself; a write is refused with the
    transient class; the loop renews with no cadence wait; the write is then admitted. The same after
    a remount whose quiesce took 20 s.
15. `CASMountRuntime.ARemountRequestEndsTheRenewalAndTheSameThreadReclaims` [7, 13]: an interference
    report while the renewal retries ends it within one wait plus one request; the lease thread runs
    the reclaim and then renews under the new epoch; a second request raised during the reclaim is
    handled before the next renewal.
16. `CASMountRuntime.StopDuringAReclaimJoinsTheThread` [13]: a stop during the observation wait of a
    reclaim joins the thread without waiting the observation out, and no farewell is written for the
    unclaimed slot.
17. `CASHeartbeat.FarewellKeepsItsBound` [1]: with `Retry::within` the farewell is admitted, refused
    and bounded exactly as `FarewellIsAdmittedUnderTheDefaultBudget`,
    `FarewellIsAdmittedUnderADifferentEnvelope` and
    `FarewellIsRefusedWhenTheLeaseExpiresBeforeItsDerivedWindow` assert today; those three stay green.
18. Integration, `tests/integration/test_cas_mount_renewal_retry` with `s3_fault_proxy.py`:
    `test_short_put_outage_does_not_fence` holds `503` on `PUT` for 8 s across a renewal. No
    `fenced after` line on any node, `CASMountLeaseLost` unchanged, the next `INSERT` succeeds.
19. Same file, `test_long_put_outage_resumes_under_the_same_epoch`: the fault lasts 45 s. Afterwards
    `writer_epoch` in `system.cas_mounts` is unchanged, `CASMountLeaseExpired` is 1, an `INSERT`
    succeeds and the log has no remount line.
20. Soak card S39 queries every replica in its short-fault check.

## Documentation {#documentation}

- `docs/en/antalya/cas/architecture/mounts-and-leases.md`: the "Absolute deadline" and "Losing the
  lease" paragraphs, the lifecycle diagram, the 4 s scheduling-lateness sentence, the two-worker and
  parking description under "Mount, unmount, crash".
- `docs/en/antalya/cas/configuration.md`, `operations/troubleshooting.md`: the expired state, the
  new counter and event field, what a failing renewal now means, the text of
  `cas_lease_safety_margin_ms`.
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
- **One lease class for the mount and for GC leadership**: the two share an idea, not a mechanism
  ([what stays](#what-stays)); a common class would have to carry both a clock and a tick counter.
- **Keep the re-anchor at open** so that an open always returns with an admissible lease: needs a
  second, bounded renewal path for a case that already retries.

## Out of scope {#out-of-scope}

- Background retry of wedged ref lanes.
- Reservation, gates and pauses of data writes in `CasOperation`, including the sites that check
  both `gate` and `fits` and the loops that pause without a reservation.
- Renaming or splitting `cas_attempt_timeout_ms`.
- The policy that an exception escaping the loop's own state machine ends the thread.
- The GC leader lease.
