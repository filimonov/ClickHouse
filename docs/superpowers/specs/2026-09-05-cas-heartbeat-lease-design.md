---
description: 'Design for expressing the CAS mount lease as heartbeats: one bounded attempt per interval, TTL as a count of heartbeats, a small explicit clock-skew margin, and a configurable observation length for reclaiming a slot after a hard restart. Replaces the TTL/period/safety-margin/envelope arithmetic.'
sidebar_label: 'CAS heartbeat lease'
sidebar_position: 10
slug: /superpowers/specs/cas-heartbeat-lease-design
title: 'CAS mount lease as heartbeats'
doc_type: 'design'
---

# CAS mount lease as heartbeats {#cas-heartbeat-lease-design}

**Status:** DRAFT rev.1 (2026-09-05), awaiting review. Spec 2 of the R2 stability series. Lands on
top of the merge of PR #2307 (configurable `cas_mount_lease_ttl_ms` / `cas_mount_renew_period_ms`)
and carries the review points made there (same values on every member; lowering the TTL only with
the pool stopped; shorter timings mean easier lease loss; a no-wait reclaim knob; a test that the
config reaches `PoolConfig` and that a kill/restart logs the expected wait).

## Decision {#decision}

The writer's side of the mount lease is restated in one vocabulary. Nothing on the wire changes:
the mount object still carries `expires_at_ms`, and every other server still judges liveness by
`now <= expires_at_ms + skew` as today.

| Setting | Default | Meaning |
|---|---|---|
| `cas_heartbeat_interval_ms` | `8000` | One heartbeat write per interval. |
| `cas_lease_heartbeats` | `5` | The lease lasts this many heartbeats: `TTL = interval × heartbeats` = 40 s. |
| `cas_lease_skew_margin_ms` | `2000` | This server stops admitting writes this long before its own `expires_at`, so a peer with a slightly different clock never sees it writing past expiry. |
| `cas_mount_observation_heartbeats` | `= cas_lease_heartbeats` | How many missed heartbeats this server must observe on its own clock before reclaiming a slot that carries its own `server_uuid` after a hard restart. `0` reclaims at once. |

Removed: `cas_mount_lease_ttl_ms`, `cas_mount_renew_period_ms` (superseded), `cas_lease_safety_margin_ms`
(its startup validation role disappears; the skew margin above is the only margin left). Kept:
`cas_attempt_timeout_ms` for every other control-plane request.

## The heartbeat {#heartbeat}

- Every `interval` the renewer issues **one** `replace(mount, body{expires_at = now + TTL, seq+1},
  last_committed_etag)` bounded by `min(interval, remaining_until_expiry − skew_margin)` as a single
  physical attempt (`Retry::within(bound).asSingleAttempt()`), with the HTTP attempt timeout equal to
  that bound. No settle read inside a heartbeat.
- An ambiguous heartbeat is settled by the next one: its precondition either commits (the previous
  did not land) or meets `412` (it landed), and then the engine's existing single exact read adopts
  the body by `write_attempt_id`. This is the loop the engine already has; the heartbeat just does not
  reserve two envelopes for it.
- Spec 1 applies inside the bound: a pre-send failure is reissued immediately within the same
  interval; anything else waits for the next beat.
- `N − 1` consecutive missed heartbeats are survived by the lease and by write admission alike; the
  `N`-th miss is expiry. With the defaults: 32 s of a store that cannot be reached, versus ~8 s today.

## Admission and fence {#admission}

- Writes are admitted while `now < last_committed_anchor + TTL − skew_margin` (boot clock, as today).
- When the last heartbeat that could commit before expiry fails, the mount is fenced exactly as it is
  today and the remount path takes over. No behaviour of the fence, the remount or GC changes.

## Reclaim after a hard restart {#reclaim}

Today a same-`server_uuid`, different-epoch slot without a certificate (`gc_fenced` or a clean
farewell) is reclaimed only after the claimant observed the slot's token stable for
`TTL + TTL/20 + poll` on its own clock. In the heartbeat vocabulary that threshold becomes
`cas_mount_observation_heartbeats × interval + skew_margin + poll`; the default keeps the full TTL.

`cas_mount_observation_heartbeats = 0` reclaims a same-uuid slot without observing it. This is the
knob the hard-restart regression needs (100 kills, ~42 s of observation each at the defaults). It is
unsafe whenever two processes can hold the same `server_uuid`: a stalled predecessor may still be
writing for up to one interval after the reclaim, until its next heartbeat meets the token guard and
fences it. The setting description and `configuration.md` say so in those words. Certificates keep
their instant reclaim regardless of the knob.

## Validation at writable open {#validation}

`validateWritableMountTiming` becomes: `interval ≥ 1000 ms`, `heartbeats ≥ 3`, `skew_margin < interval`,
`observation_heartbeats ≤ heartbeats`. The old inequality over period, attempt timeout and safety
margin is gone with the arithmetic it guarded.

## Documentation rules {#docs}

`configuration.md` and `mounts-and-leases.md` state, next to the settings:

1. All servers sharing a pool run the same values. Observation thresholds are computed from local
   config only, so a member with a shorter lease treats a healthy peer with a longer one as dead.
2. Lowering `interval` or `heartbeats` is safe only with every member of the pool stopped first, or
   restarted gracefully: a restart with a shorter lease observes for less than the old lease of the
   previous process.
3. Shorter timings make lease loss easier: any store delay longer than the tolerated misses costs a
   remount.

## Tests {#tests}

- gtests on cadence (`gtest_cas_pool.cpp`, `gtest_cas_event_log.cpp`, the renewer twins): rewritten
  to the interval/heartbeats vocabulary with a virtual clock; a store unreachable for `N − 1`
  intervals keeps the lease, unreachable for `N` loses it; a heartbeat that returns ambiguous is
  adopted by the next beat with one read.
- gtest: the observation threshold equals `observation_heartbeats × interval + skew + poll`; `0`
  reclaims a same-uuid slot immediately; certificates reclaim regardless.
- Integration (`test_cas_mount_renewal_retry` extended or a sibling): a disk config with the four
  settings reaches `PoolConfig` (visible in `system.content_addressed_mounts`); a kill/restart logs
  `waiting ~N ms (token-stability observation)` with the expected N; with
  `cas_mount_observation_heartbeats = 0` the restart mounts without the wait.
- The regression hard-restart suite is pointed at the `0` knob instead of shortened lease timings.

## Out of scope {#out-of-scope}

Renewal isolation from data-plane connection churn (spike 3) and the adaptive first-attempt timeout
cooperation (spec 4) are separate documents.
