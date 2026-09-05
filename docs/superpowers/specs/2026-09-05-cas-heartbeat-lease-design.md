---
description: 'Design for the small path on the CAS mount lease after PR #2307: keep the configurable TTL and renewal period, add an explicitly unsafe no-delay reclaim of a slot that carries this server's own uuid after a hard restart, document the cross-server rules, and test that the settings reach PoolConfig and that a kill/restart waits the expected time.'
sidebar_label: 'CAS mount lease: unsafe no-delay reclaim'
sidebar_position: 10
slug: /superpowers/specs/cas-heartbeat-lease-design
title: 'CAS mount lease: configurable timing plus an unsafe no-delay reclaim'
doc_type: 'design'
---

# CAS mount lease: configurable timing plus an unsafe no-delay reclaim {#cas-heartbeat-lease-design}

**Status:** DRAFT rev.3 (2026-09-05). rev.1 (heartbeat re-vocabulary) and rev.2 (small path) were
reviewed by `codex` (`gpt-5.6-sol`, xhigh; records `codex_spec2.final.md`, `codex_spec2r2.final.md`).
rev.2's two MAJORs — automatic remount would let duplicate-uuid processes steal the slot back and
forth, and a graceful rolling timing change is not safe — are folded in below. The user chose the
small path (2026-09-05). The heartbeat re-vocabulary is a backlog redesign. Spec 2 of the R2 series;
lands on the merge of PR #2307 (`ee6b0fd7826`).

## Decision {#decision}

1. `cas_mount_lease_ttl_ms` (30000) and `cas_mount_renew_period_ms` (10000) from PR #2307 stay as
   they are. No renaming, no aliases, no `MountConfig` or GC threading.
2. One new boolean disk setting, `cas_unsafe_remount_no_delay` (default `0`), consulted at exactly
   ONE site: the writable `Pool::open` claim (`Pool::mountWritable`, `CasPool.cpp` ~716). When set and
   the slot is held by this server's OWN `server_uuid` with a different `writer_epoch` and no
   certificate, the open reclaims at once instead of observing the slot's token stable for
   `TTL + floor(TTL/20) + max(1, floor(period/2))` on its own clock. The reclaim is routed through
   `claimMount` with an explicit authorization carrying the exact token it read (a new argument next
   to `proven_dead_incarnation`, never reusing it), so the foreign-uuid refusal at `CasServerRoot.cpp:811`
   still precedes it, the prior state is a new `MountPriorState::UncleanUnsafe`, and the audit event
   reason names `cas_unsafe_remount_no_delay`. Certificates (`gc_fenced`, clean farewell) reclaim
   instantly regardless, as today.
3. `tryRemountOnce` (`CasPool.cpp` ~1428) does NOT consult the knob: an incarnation superseded by a
   duplicate-uuid process must still observe before it may reclaim, or two such processes would
   alternate authority indefinitely. GC's fence-out (`CasGc.cpp` ~473, ~4520) keeps the shared full
   threshold.
4. The setting's description and `configuration.md` say, in these words: unsafe whenever two
   processes can hold the same `server_uuid` (a copied uuid file, a stalled predecessor). After such a
   reclaim the predecessor can still START conditional writes until its own cutoff
   (`confirmed deadline − margin − 2 × attempt_timeout`) or until its next renewal meets the token
   guard, and a request already sent may materialize later; ref-log keys carry `(writer_epoch,
   sequence)` and creates are conditional, so two writers cannot commit different bodies to one key
   and recovery's `EpochSeal` settles stragglers — the exposure is availability (recovery fails closed
   after 64 consecutive old-epoch stragglers), not data. Intended for test stands and deployments that
   guarantee one process per uuid.

## Documentation rules {#docs}

Next to the settings in `configuration.md`, and in `mounts-and-leases.md`:

1. All servers sharing a pool run the same `cas_mount_lease_ttl_ms` and `cas_mount_renew_period_ms`.
   Startup reclaim and GC fence-out judge liveness by token stability on the observer's OWN
   `CLOCK_BOOTTIME` with the observer's OWN threshold; nothing about the writer's timing is on the
   wire. A member or GC leader with a shorter threshold treats a healthy peer with a longer one as
   dead. Change the values only with EVERY member of the pool stopped; a graceful restart removes
   only that member's own startup observation and does not make mixed thresholds safe.
2. A shorter TTL reduces the tolerance for store delays; a shorter period increases it (renewal
   starts earlier) at the cost of traffic. With the defaults, `TTL − margin − period − 2 ×
   attempt_timeout = 8 s` is the scheduling-lateness budget before the first renewal attempt of a
   period can begin; the renewal then retries until `confirmed deadline − margin`.
3. `expires_at_ms` in the mount object is a writer-stamped diagnostic used by `system.cas_mounts`
   and by the non-authoritative decommission epoch-recovery precheck; it never authorizes a reclaim
   or a GC fence-out, and local fencing is derived from the confirmed request's pre-I/O BOOTTIME
   anchor plus the TTL. The operator-facing double-start message (`CasServerRoot.cpp:909`) and the
   stale comment in `CasMountRuntime` are corrected in the same change; the observation log line says
   "token-stability observation".

## Tests {#tests}

- Startup, virtual clock (`CASMountOpenWaits.UncleanOpenPaysOnlyTheObservationWindow` fixture in
  `gtest_cas_pool.cpp` ~2299): an uncertified same-uuid slot is reclaimed without a wait when the knob
  is set, with prior state `UncleanUnsafe` and the audit reason naming the knob; after the full
  threshold otherwise. A foreign-uuid slot is refused in both cases.
- Helper level (`CASMountAwaitExpiry` / `CASMountObservation` tests): unchanged behaviour of
  `claimMountAwaitingExpiry` — the knob never reaches it.
- Remount: `tryRemountOnce` uses raw `sleep_for` (`CasPool.cpp:1425`); route it through
  `mount_runtime.waitSleep` first, then a two-runtime test: after A is superseded by B (B started with
  the knob), A's renewal becomes `RenewalTerminal` with its local fence tripped, and A's automatic
  remount does NOT reclaim B's epoch. Separately advance A's BOOTTIME past its cutoff without renewing
  to prove cutoff-only fencing.
- GC: `mountObservationThresholdMs` callers unchanged; a GC fence-out test with the knob set behaves
  identically.
- Integration (`test_cas_mount_renewal_retry`, `with_rustfs`), short valid values (TTL 1000 ms,
  period 200 ms, attempt timeout 50 ms, margin 50 ms → observation exactly 1150 ms): the settings
  reach `PoolConfig` (observed through behaviour); record the log line count before each restart and
  inspect only appended lines: the safe restart appends exactly one
  `waiting ~1150 ms (token-stability observation)` line; the unsafe restart appends none, mounts
  writable, and advances the epoch.
- The regression hard-restart suite (`lightweight_delete/tests/hard_restart.py`) is pointed at the
  knob instead of shortened lease timings (clickhouse-regression change, out of this repo).

## Out of scope {#out-of-scope}

The heartbeat re-vocabulary (backlog), in-period renewal retries (issue #2244 direction 1; partly
covered by spec 1 for the connect-failure class), spec 3 and spec 4.
