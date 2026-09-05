---
description: 'Design for the small path on the CAS mount lease after PR #2307: keep the configurable TTL and renewal period, add an explicitly unsafe no-delay reclaim of a slot that carries this server's own uuid after a hard restart, document the cross-server rules, and test that the settings reach PoolConfig and that a kill/restart waits the expected time.'
sidebar_label: 'CAS mount lease: unsafe no-delay reclaim'
sidebar_position: 10
slug: /superpowers/specs/cas-heartbeat-lease-design
title: 'CAS mount lease: configurable timing plus an unsafe no-delay reclaim'
doc_type: 'design'
---

# CAS mount lease: configurable timing plus an unsafe no-delay reclaim {#cas-heartbeat-lease-design}

**Status:** DRAFT rev.2 (2026-09-05). rev.1 proposed a heartbeat re-vocabulary of the lease; `codex`
(`gpt-5.6-sol`, xhigh; record in `tmp/pr2300-cicd-watch/review/codex_spec2.final.md`) found 6 MAJOR —
the renewer has no pending/missed state machine, the engine has no one-envelope primitive, admission
must stay duration-aware, the proportional clock-rate allowance must stay, GC must keep its full
threshold — and recommended the small path. The user chose the small path (2026-09-05). The heartbeat
re-vocabulary is recorded in the backlog as a redesign. Spec 2 of the R2 series; lands on the merge
of PR #2307 (`ee6b0fd7826`).

## Decision {#decision}

1. `cas_mount_lease_ttl_ms` (30000) and `cas_mount_renew_period_ms` (10000) from PR #2307 stay as
   they are. No renaming, no aliases.
2. One new boolean disk setting, `cas_unsafe_remount_no_delay` (default `0`): when set, a writable
   open or remount that finds the mount slot held by this server's OWN `server_uuid` with a different
   `writer_epoch` reclaims it at once instead of observing the slot's token stable for
   `TTL + TTL/20 + poll`. Certificates (`gc_fenced`, clean farewell) reclaim instantly regardless, as
   today. The knob applies ONLY to the two same-uuid claim sites (`Pool::open` and `tryRemountOnce`,
   `CasPool.cpp` ~716 / ~1428); GC's fence-out keeps the full shared threshold, and a foreign uuid is
   never reclaimed.
3. The setting's description and `configuration.md` say, in these words: unsafe whenever two processes
   can hold the same `server_uuid` (a copied uuid file, a stalled predecessor); after such a reclaim the
   predecessor may keep writing until its own local cutoff or until its next renewal meets the token
   guard — up to its remaining `TTL − margin`, not one period. Intended for test stands and for
   deployments that guarantee one process per uuid.

## Documentation rules {#docs}

Next to the settings in `configuration.md`, and in `mounts-and-leases.md`:

1. All servers sharing a pool run the same `cas_mount_lease_ttl_ms` and `cas_mount_renew_period_ms`.
   Reclaim (startup, remount) and GC fence-out judge liveness by token stability on the observer's
   OWN clock with the observer's OWN threshold; nothing about the writer's timing is on the wire. A
   member or GC leader with a shorter TTL treats a healthy peer with a longer one as dead. Rolling
   timing changes are therefore unsafe: change the values with every member stopped, or restart
   members gracefully (a graceful farewell leaves a certificate that needs no observation).
2. Shorter TTL or period makes lease loss easier: any store delay longer than the margin costs a
   remount (`TTL − margin − period − 2 × attempt_timeout` is the effective renewal window; 8 s at
   the defaults).
3. `expires_at_ms` in the mount object is used for local fencing and for `system.cas_mounts`
   diagnostics only; the operator-facing double-start message still says liveness is judged from the
   wall clock and is corrected in the same change (`CasServerRoot.cpp:909`).

## Tests {#tests}

- gtest (`gtest_cas_bootstrap_ordering.cpp` / pool tests with a virtual clock): a same-uuid,
  different-epoch, uncertified slot is reclaimed without a wait when `unsafe_remount_no_delay` is
  set and only after the full threshold otherwise; a foreign-uuid slot is refused in both cases; GC's
  fence-out threshold is unchanged by the knob.
- gtest: the predecessor interleaving — reclaim with the knob, then the predecessor's renewal meets
  the token guard and is fenced (`gc_fenced`/terminal), proving the bound stated in the docs.
- Integration (`test_cas_mount_renewal_retry` or a sibling with `with_rustfs`): the disk config sets
  the two timing settings and the knob; they reach `PoolConfig` (verified through the observation
  log line `waiting ~N ms (token-stability observation)` on a kill/restart without the knob, N =
  `TTL + TTL/20 + period/2`, and through the absence of that wait with the knob).
- The regression hard-restart suite (`lightweight_delete/tests/hard_restart.py`) is pointed at the
  knob instead of shortened lease timings (clickhouse-regression change, out of this repo).

## Out of scope {#out-of-scope}

The heartbeat re-vocabulary (backlog), in-period renewal retries (issue #2244 direction 1; partly
covered by spec 1 for the pre-send class), spec 3 and spec 4.
