---
description: 'TLC evidence for observer timing of the CAS mount lease: a GC round or a re-mounting server fences a mount only after the holder authority ended. Covers the CAS-342 sabotage, the measured fence margin and the witnesses.'
sidebar_label: 'CAS mount observer timing TLC results'
sidebar_position: 6
slug: /superpowers/models/CaMountObserveCore-results
title: 'CAS mount observer timing TLC results'
doc_type: 'reference'
---

# `CaMountObserveCore` results {#camountobservecore-results}

## Verdict {#verdict}

The battery passed: `run_mountobserve.sh` printed `EXECUTED ROWS: 26/26`, `SWEPT POINTS: 12/12`
and `ALL EXPECTATIONS MET`.

- **The fixed rule is safe.** A sighting dated after its read, and a stability test that uses a
  sample taken before the confirming read, never fence a holder inside its authority. This holds
  for a holder that keeps its lease for the whole `HolderLease`, with clock drift up to the allowance.
- **CAS-342 is reproduced.** Dating a sighting with the round-start sample fences a holder inside
  its authority, on the first decision and on the re-decision after a refused fence-out.
- **Mixed leases are safe by construction when the body declares.** The observer waits the longer
  of its own lease and the one in the body, so it never fences inside the holder's authority,
  whichever lease is longer. An old body declares nothing: the observer falls back to its own
  lease, and a holder with a longer lease is fenced inside its authority
  ([mixed leases](#mixed-leases)).
- **The margin is the cadence term.** The fence lands `Cadence / 1.05` after authority ends at
  full drift: 9.5 s for GC and 4.8 s for a re-mount with the default settings.

## What the model contains {#model}

One holder, one observer, one mount slot.

- **Holder.** `RenewStart` samples the start on `hclock` before the request. `RenewLand` installs
  a fresh token at any later time, because the holder retries until the store answers.
  `RenewAck` sets `lastStart` to that start. `Write` is admitted while
  `hclock < lastStart + AuthoritySpan`. `RenewLearnFenced` is the definitive answer.
- **Observer.** `ObsSample` reads `oclock` into `roundSample`. `ObsRead` reads the slot any number
  of ticks later. `ObsDecide` dates a new token with `oclock` and tests stability with
  `roundSample >= watchSince + Threshold`. `ObsFenceWrite` is conditional on the token the decision
  saw. A refused fence is followed by `ObsReRead` and a second `ObsDecide`.
- **Threshold.** `max(ObserverLease, DeclaredLease) + Allowance + Cadence`, as
  `mountObservationThresholdMs`. `DeclaredLease` is `HolderLease` when `Declares` and 0 for an old
  body. `Allowance` is sized for the longer lease. `Cadence` is
  `Period` for GC and `Period / 2` for `claimMountAwaitingExpiry`.

The observer is more adversarial than the code: a read may reuse a sample of any age, and the fence
write may land at any later time.

## Timing assumption {#timing-assumption}

`hclock` and `oclock` are separate variables. No action and no invariant compares them.

Their only coupling is a rate bound. `TickBoth` advances both. `TickObserverOnly` and
`TickHolderOnly` advance one, and each needs `GainEvery` joint ticks since the last solo tick. One
solo tick can be banked, so over `X` holder ticks the observer gains at most
`floor(X / GainEvery) + 1`. With `GainEvery` the longer lease over `Allowance` this is the code's assumption (the
`lease / 20` term) plus one tick of jitter.

`CaMountObserveCore_env_drift_exceeds_allowance.cfg` breaks the bound and is red: the rule depends
on it.

## Invariants {#invariants}

| Invariant | Says |
|---|---|
| `NoFenceInsideAuthority` | When the fence lands, `hclock >= lastStart + AuthoritySpan`, and no late `RenewAck` changes that |
| `FenceMarginAtLeast` | The fence lands at least `MinMargin` holder ticks after authority ended |
| `NoWriteAfterFence` | `Write` is never admitted after the fence landed |
| `FencingWaitCoversDeclaredLease` | A decision to fence follows an observation at least `DeclaredLease` long |
| `HealthyNeverFenced` | A holder whose renewals land on time is never fenced |
| `HealthyKeepsAuthority` | That holder is never without authority |
| `TypeOK` | Variable domains |

## Sabotages and traces {#sabotages}

All at `HolderLease = ObserverLease = 4`, `Period = 2`, `Allowance = 1`, `Cadence = 2` (`Threshold = 7`), `GainEvery = 4`,
`MaxClock = 14`. Each violates `NoFenceInsideAuthority`.

| Config | Defect | Trace |
|---|---|---|
| `_sab_sighting_before_read` | CAS-342: a first sighting is dated with `roundSample` | `ObsSample` at 0, the holder renews at 7, `ObsRead` sees that token, `ObsDecide` dates it 0, `ObsSample` at 7, `ObsFenceWrite` at 7 with authority until 11 |
| `_sab_redecision_stale_sample` | A sighting after a refused fence is dated with `roundSample` | Token 1 watched from 0, `ObsSample` at 7, `ObsRead` at 14, the holder renews at 14, `ObsFenceWrite` is refused, `ObsReRead` sees token 2, `ObsDecide` dates it 7, `ObsSample` at 14, `ObsFenceWrite` at 14 with authority until 18 |
| `_sab_wrapping_stability` | The stability test wraps when the sample precedes the sighting | `ObsSample` at 0, `ObsRead` and `ObsDecide` at 1 date the token 1, a second `ObsRead` under the sample 0 is "stable", `ObsFenceWrite` at 1 with authority until 4 |
| `_env_drift_exceeds_allowance` | No defect: `GainEvery = 1`, outside the assumption | Token watched from 0, four `TickObserverOnly` among three `TickBoth`, `ObsSample` at observer tick 7, `ObsFenceWrite` at holder tick 3 with authority until 4 |

The third sabotage has no route through `computeHeartbeatFloor`: `readModifyWrite` returns as
soon as the decision declines, so a slot is not decided twice under one sample after a sighting.
It guards the shared helper: rule 11 allows the two samples in either order, and an unsigned
subtraction of a later sighting from an earlier sample is a huge number.

### How long a walk the defect needs {#walk-bound}

`MaxWalk` bounds the ticks between a sample and the reads decided with it. With both stale stamps
on, which is `computeHeartbeatFloor` taking one `mono_now_ms` argument for every stamp:

| Clocks | Green up to | Red from | Configs |
|---|---|---|---|
| One rate (`GainEvery = 0`) | `MaxWalk = 3` | `MaxWalk = 4` | `_stale_stamps_nodrift_short_walk`, `_stale_stamps_nodrift_long_walk` |
| One tick of drift per lease (`GainEvery = 4`) | `MaxWalk = 2` | `MaxWalk = 3` | `_stale_stamps_short_walk`, `_stale_stamps_long_walk` |

3 is `Threshold - HolderLease`. The defect needs a walk longer than that, less what the observer's clock
gains in one lease. In seconds: 11.5 s, and 10 s at 5 percent drift.

## Mixed leases {#mixed-leases}

`HolderLease` is what the holder writes into the body as `expires - started`. It bounds the holder's
authority on its own clock. `ObserverLease` is the observer's own setting. All rows use
`Period = 2`, `Allowance = 1`, `Cadence = 2`, `GainEvery = 6` and a horizon of 12 ticks, so
`Allowance` covers the drift over the longer lease.

| Config | `HolderLease` | `ObserverLease` | Body | `Threshold` | Result |
|---|---:|---:|---|---:|---|
| `_mixed_longer_holder` | 6 | 2 | declares | 9 | green, margin at least 1 |
| `_mixed_shorter_holder` | 4 | 6 | declares | 9 | green, margin at least 3: the observer waits longer than needed |
| `_old_body_longer_holder` | 6 | 2 | declares nothing | 5 | `NoFenceInsideAuthority` violated |
| `_sab_own_lease_only` | 6 | 2 | declares | 5 (sabotage) | `NoFenceInsideAuthority` violated |
| `_sab_own_lease_only_wait` | 6 | 2 | declares | 5 (sabotage) | `FencingWaitCoversDeclaredLease` violated |
| `_witness_declared_wait_covers_holder_lease` | 6 | 2 | declares | 9 | `W_FencingWaitCoversHolderLease` reached |

- **The old body is a red row on purpose.** It is the rollout caveat: a holder that wrote its body
  before stamps existed declares nothing, so the observer uses only its own lease. If the holder's
  lease is longer, the observer fences inside the holder's authority. Trace: token 1 is watched
  from tick 0, the observer's clock gains one tick, and `ObsFenceWrite` lands at holder tick 4
  while authority runs until 6. It is the same violation as `_sab_own_lease_only`, which ignores a
  declared lease.
- **`FencingWaitCoversDeclaredLease`** says a decision to fence follows an observation at least as
  long as the declared lease. `_sab_own_lease_only_wait` checks it alone and shows it fails when the
  declared lease is ignored. The witness shows the premise of that invariant is reachable with a wait of
  `HolderLease` or more.
- **Not covered.** The two green rows use `MinMargin` 1 and 3, which holds; the exact margin was
  not measured for them. The model has one holder lease per run, so a holder that changes its lease
  between renewals is not modelled.

## Fence margin {#fence-margin}

The margin is `hclock` at the fence minus the end of authority. Each value below is exact: green
with `MinMargin` at that value, `FenceMarginAtLeast` violated one above.

| Config pair | Observer | `AuthoritySpan` | Margin |
|---|---|---:|---:|
| `_gc`, `_margin_gc_tight` | GC, `Threshold = 7` | 4 (`HolderLease`) | 1 |
| `_remount`, `_margin_remount_tight` | re-mount, `Threshold = 6` | 4 (`HolderLease`) | 0 |
| `_earlyfence`, `_margin_earlyfence_tight` | GC | 3 | 2 |

The margin survives a holder that uses its whole lease. A holder that gives up earlier gets a
margin larger by exactly the ticks it gave up.

Every measured point fits one formula. Let `X` be the least holder-tick count with
`X + G(X) >= Threshold`, where `G(X) = 0` for `GainEvery = 0` and `floor(X / GainEvery) + 1`
otherwise. Then the margin is `X - AuthoritySpan`. The formula was written before the sweep ran,
and all 12 points matched.

| Point | Lease | `Period` | `Allowance` | `Cadence` | `AuthoritySpan` | `GainEvery` | Margin |
|---|---:|---:|---:|---:|---:|---:|---:|
| `nodrift_gc` | 4 | 2 | 1 | 2 | 4 | 0 | 3 |
| `nodrift_remount` | 4 | 2 | 1 | 1 | 4 | 0 | 2 |
| `noallowance` | 4 | 2 | 0 | 2 | 4 | 0 | 2 |
| `drift1_gc` | 4 | 2 | 1 | 2 | 4 | 4 | 1 |
| `drift1_remount` | 4 | 2 | 1 | 1 | 4 | 4 | 0 |
| `drift2_gc` | 4 | 2 | 1 | 2 | 4 | 2 | 0 |
| `allowance2` | 4 | 2 | 2 | 2 | 4 | 4 | 2 |
| `span2` | 4 | 2 | 1 | 2 | 2 | 4 | 3 |
| `ttl6_gc` | 6 | 2 | 1 | 2 | 6 | 6 | 1 |
| `ttl6_cadence3` | 6 | 3 | 1 | 3 | 6 | 6 | 2 |
| `ttl6_drift2` | 6 | 3 | 1 | 3 | 6 | 3 | 1 |
| `ttl10_gc` | 10 | 3 | 1 | 3 | 10 | 10 | 2 |

Read as a function:

- No drift: the margin is `Allowance + Cadence`.
- Drift equal to the allowance: the margin is `Cadence` minus the one banked tick.
- Drift above the allowance eats the cadence term (`drift2_gc`: margin 0), then authority.

The continuous form is `(lease + lease/20 + Cadence) / (1 + drift) - AuthoritySpan`. With the default
settings (lease 30 s, `Period` 10 s):

| Observer | Threshold | Margin, no drift | Margin, 5 percent drift |
|---|---:|---:|---:|
| GC | 41.5 s | 11.5 s | 9.5 s |
| Re-mount | 36.5 s | 6.5 s | 4.8 s |

These four numbers come from the formula. TLC did not run at that scale.

## Witnesses {#witnesses}

Negated reachability, with every safety invariant enabled.

| Config | Shows |
|---|---|
| `_witness_observer_fences` | The observer fences a holder that stopped: token watched from 0, fence at 7 |
| `_witness_failing_holder_keeps_lease` | `RenewStart` at 3 never lands, `Write` at 3 (the last tick, authority ends at 4), fence at holder tick 6 |
| `_witness_refused_fence_redecided` | The holder's renewal starts at 7 and lands between `ObsRead` and `ObsFenceWrite`, the fence is refused, `ObsDecide` dates the new token 7 (after `ObsReRead`), and a fence lands at 14; that renewal gives authority until 11 at most |
| `_witness_healthy_outlives_threshold` | The observer waits 7 ticks and finds that a healthy holder moved the token |
| `_witness_declared_wait_covers_holder_lease` | A fence decision after a wait of at least `HolderLease` (6) with `ObserverLease` 2 and a declaring body |

`_healthy_gc` and `_healthy_remount` have equal state counts: the stability test never passes for a
healthy holder, so the cadence term is never consulted.

## Recorded run {#recorded-run}

```bash
SWEEP=1 TLC_WORKERS=4 TLC_GREEN_WORKERS=4 TLC_JAVA_OPTS=-Xmx8g nice -n 10 docs/superpowers/models/run_mountobserve.sh
```

- TLC `2026.07.18.145032`, revision `30cc360`; `tla2tools.jar` SHA-256
  `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`.
- Violation and witness rows: 4 workers, so their counts and depths are not those of a single
  worker. Green rows: 4 workers; the depth of a green row can vary between runs, its state counts
  cannot. The state counts of the 7 green rows that existed before the lease split are unchanged.
- Every green row ends with `0 states left on queue`.

| Configuration | Expected | Observed | Generated | Distinct | Depth |
|---|---|---|---:|---:|---:|
| `_sab_sighting_before_read` | violation | `NoFenceInsideAuthority` | 4,295,573 | 1,801,548 | 20 |
| `_sab_redecision_stale_sample` | violation | `NoFenceInsideAuthority` | 178,554,938 | 55,440,948 | 33 |
| `_sab_wrapping_stability` | violation | `NoFenceInsideAuthority` | 7,085 | 4,144 | 10 |
| `_stale_stamps_long_walk` | violation | `NoFenceInsideAuthority` | 3,425,184 | 1,497,368 | 19 |
| `_stale_stamps_nodrift_long_walk` | violation | `NoFenceInsideAuthority` | 190,520 | 85,704 | 20 |
| `_env_drift_exceeds_allowance` | violation | `NoFenceInsideAuthority` | 1,202,892 | 455,293 | 17 |
| `_sab_own_lease_only` | violation | `NoFenceInsideAuthority` | 510,167 | 238,623 | 15 |
| `_sab_own_lease_only_wait` | violation | `FencingWaitCoversDeclaredLease` | 187,226 | 91,290 | 14 |
| `_old_body_longer_holder` | violation | `NoFenceInsideAuthority` | 425,728 | 201,555 | 15 |
| `_margin_gc_tight` | violation | `FenceMarginAtLeast` | 1,520,586 | 687,616 | 17 |
| `_margin_remount_tight` | violation | `FenceMarginAtLeast` | 849,610 | 392,142 | 16 |
| `_margin_earlyfence_tight` | violation | `FenceMarginAtLeast` | 1,353,312 | 623,857 | 17 |
| `_gc` | green | green | 219,879,569 | 65,946,998 | 48 |
| `_remount` | green | green | 234,922,803 | 70,541,891 | 51 |
| `_earlyfence` | green | green | 194,883,584 | 60,240,243 | 48 |
| `_stale_stamps_short_walk` | green | green | 110,925,407 | 35,294,799 | 52 |
| `_stale_stamps_nodrift_short_walk` | green | green | 4,700,725 | 1,585,241 | 50 |
| `_healthy_gc` | green | green | 5,943,858 | 1,691,748 | 43 |
| `_healthy_remount` | green | green | 5,943,858 | 1,691,748 | 43 |
| `_mixed_longer_holder` | green | green | 74,616,789 | 21,707,527 | 39 |
| `_mixed_shorter_holder` | green | green | 70,631,334 | 21,905,489 | 39 |
| `_witness_observer_fences` | witness | `W_ObserverFences` | 1,577,598 | 712,461 | 17 |
| `_witness_failing_holder_keeps_lease` | witness | `W_FailingHolderKeepsLeaseToItsEnd` | 5,152,829 | 2,205,840 | 19 |
| `_witness_refused_fence_redecided` | witness | `W_RefusedFenceIsRedecided` | 169,335,516 | 53,135,403 | 32 |
| `_witness_healthy_outlives_threshold` | witness | `W_HealthyHolderOutlivesTheThreshold` | 114,191 | 39,088 | 19 |
| `_witness_declared_wait_covers_holder_lease` | witness | `W_FencingWaitCoversHolderLease` | 2,877,840 | 1,230,906 | 18 |

Sweep rows, horizon `Threshold + 3`. The green half of each point is exhaustive:

| Point | Green: generated | Green: distinct | Depth | Red: generated | Red: distinct | Depth |
|---|---:|---:|---:|---:|---:|---:|
| `nodrift_gc` | 1,078,765 | 346,243 | 33 | 89,135 | 40,084 | 17 |
| `nodrift_remount` | 594,172 | 190,511 | 32 | 45,625 | 20,919 | 15 |
| `noallowance` | 594,172 | 190,511 | 32 | 55,436 | 25,142 | 16 |
| `drift1_gc` | 19,595,890 | 5,944,540 | 37 | 1,448,940 | 630,068 | 17 |
| `drift1_remount` | 9,621,705 | 2,944,387 | 36 | 557,113 | 251,377 | 15 |
| `drift2_gc` | 17,201,713 | 4,907,747 | 38 | 1,371,154 | 549,487 | 17 |
| `allowance2` | 37,693,533 | 11,314,792 | 38 | 2,608,056 | 1,120,991 | 18 |
| `span2` | 12,933,411 | 4,323,887 | 37 | 1,166,113 | 526,954 | 17 |
| `ttl6_gc` | 74,616,789 | 21,707,527 | 39 | 4,927,760 | 2,041,627 | 19 |
| `ttl6_cadence3` | 48,880,334 | 14,613,928 | 37 | 5,907,934 | 2,419,901 | 20 |
| `ttl6_drift2` | 43,169,331 | 12,180,045 | 38 | 5,650,391 | 2,165,654 | 20 |
| `ttl10_gc` | 351,042,621 | 96,837,486 | 41 | 39,957,730 | 14,821,742 | 24 |

## Findings against the design {#findings}

Design: `docs/superpowers/specs/2026-10-01-cas-mount-renewal-without-lease-deadline-design.md`.

- **Rule 11 holds.** Nothing in the design has to change for safety.
- **The walk bound of step one is confirmed.** The defect needs a walk longer than
  `Threshold - HolderLease`, less the drift gain ([walk bound](#walk-bound)).
- **Rule 11 should name the order of the two samples.** The test `sample - sighting >= threshold`
  must be false when the sample precedes the sighting. The design says so only in test 1.
- **The early self-fence does not hide CAS-342 on every path.** It ends authority early only for a
  holder whose renewals fail. `CasMountRuntime::admit` refuses on `lost`, on a changed generation
  or on the clock reaching the deadline, and a renewal thread that is stalled sets none of them
  before `start + HolderLease`. So the code before this design has that exposure too. This is read from
  the code, not reproduced. `_sab_sighting_before_read` runs with `AuthoritySpan = HolderLease` and
  describes that path as well.

## Limits {#limits}

- **Scale.** Exhaustive at a lease of 4 with a horizon of 14 ticks, and for the margin at a lease of 4, 6
  and 10 with a horizon of `Threshold + 3`. `Allowance = 1` tick is 25, 17 and 10 percent of the
  lease there, not 5.
- **A config with exactly 5 percent did not complete.** `HolderLease = ObserverLease = 20`, `Period = 6`, `Allowance = 1`,
  `GainEvery = 20`, `MaxClock = 28` was stopped at 184,038,696 distinct states, depth 35, with
  47 million states on the queue and growing. It is not in the runner.
- **The rate bound is an assumption.** A holder whose clock stops while the observer's runs (a
  paused virtual machine whose `BOOTTIME` does not advance) is outside it.
- **`Write` is admission.** A request admitted inside authority that lands after the fence is not
  modelled here. The successor's seal and generation check in `CaCasMountCore` cover it.
- **The start of a renewal precedes its first request.** The model takes this from rule 3;
  `CaMountRenewRetryCore` owns it.
- **One observer.** A second GC leader has its own sightings and its own conditional write. Their
  interplay is not modelled.
- **No remount.** The model stops at the fence. Reclaim and the next epoch are `CaCasMountCore`.
