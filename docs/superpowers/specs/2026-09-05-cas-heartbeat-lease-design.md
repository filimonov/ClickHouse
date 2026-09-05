---
description: 'Design for the small path on the CAS mount lease after PR #2307: keep the configurable TTL and renewal period, add an explicitly unsafe no-delay reclaim of a slot that carries this server's own uuid after a hard restart, document the cross-server rules, and test that the settings reach PoolConfig and that a kill/restart waits the expected time.'
sidebar_label: 'CAS mount lease: unsafe no-delay reclaim'
sidebar_position: 10
slug: /superpowers/specs/cas-heartbeat-lease-design
title: 'CAS mount lease: configurable timing plus an unsafe no-delay reclaim'
doc_type: 'design'
---

# CAS mount lease: configurable timing plus an unsafe no-delay reclaim {#cas-heartbeat-lease-design}

**Status:** DRAFT rev.5 (2026-09-05; rev.5 folds the combined four-spec review `codex_cross.final.md`: the
envelope arithmetic from spec 4 and the user-doc reconciliation list). rev.1 (heartbeat re-vocabulary), rev.2 and rev.3 (small path)
were reviewed by `codex` (`gpt-5.6-sol`, xhigh; records `codex_spec2.final.md`, `codex_spec2r2.final.md`,
`codex_spec2r3.final.md`; rev.3 = NO MAJOR, its MINORs folded here).
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
   (`confirmed deadline − margin − 2 × envelope`, spec 4) or until its next renewal meets the token
   guard, and a request already sent may materialize later; ref-log keys carry `(writer_epoch,
   sequence)` and creates are conditional, so two writers cannot commit different bodies to one key
   and recovery's `EpochSeal` settles stragglers — the exposure is availability (recovery fails closed
   after 64 successive seal-create attempts displaced by newly materializing old-epoch transactions),
   not data. Intended for test stands and deployments that
   guarantee one process per uuid.

## Documentation rules {#docs}

Next to the settings in `configuration.md`, and in `mounts-and-leases.md`:

1. All servers sharing a pool run the same `cas_mount_lease_ttl_ms` and `cas_mount_renew_period_ms`.
   Startup reclaim and GC fence-out judge liveness by token stability on the observer's OWN
   `CLOCK_BOOTTIME` with the observer's OWN threshold; nothing about the writer's timing is on the
   wire (startup observes `TTL + floor(TTL/20) + max(1, floor(period/2))`, GC observes
   `TTL + floor(TTL/20) + period`, both on the observer's values). A member or GC leader with a
   shorter threshold CAN fence a healthy peer whose token-update gap exceeds it — a peer renewing
   frequently stays live, one that missed a renewal does not. Change the values only with EVERY
   member of the pool stopped; a graceful restart removes
   only that member's own startup observation and does not make mixed thresholds safe.
2. A shorter TTL reduces the tolerance for store delays; a shorter period increases it (renewal
   starts earlier) at the cost of traffic. With the defaults, `TTL − margin − period − 2 ×
   envelope = 6 s` is the scheduling-lateness budget before the first renewal attempt of a
   period can begin, where `envelope = attempt_timeout + min(connect_timeout_ms, attempt_timeout)`
   (spec 4; 6 s with defaults); the renewal then retries until `confirmed deadline − margin`.
3. `expires_at_ms` in the mount object is a writer-stamped diagnostic used by `system.cas_mounts`
   and by the non-authoritative decommission epoch-recovery precheck; it never authorizes a reclaim
   or a GC fence-out, and local fencing is derived from the confirmed request's pre-I/O BOOTTIME
   anchor plus the TTL. The operator-facing double-start message (`CasServerRoot.cpp:909`) and the
   stale comment in `CasMountRuntime` are corrected in the same change; the observation log line says
   "token-stability observation".
4. `mounts-and-leases.md` is reconciled in the same change: the "identical formula" sentence (~116)
   becomes the two formulas of rule 1 (startup `max(1, floor(period/2))`, GC `period`); the claim
   outcomes table and state diagram (~140) gain `UncleanUnsafe`; the writable-open order (~203)
   drops "materialization grace if the predecessor was unclean (default 30 s)", which the code no
   longer pays (`CasPool.cpp` ~802, "THIS NO LONGER WAITS").

## Tests, in the order they are written and made to pass {#tests}

1. `CASMountClaim.UnsafeAuthorizationIsTokenExact` (direct `claimMount` test, `CASMountAwaitExpiry`
   family): a same-uuid, different-epoch, uncertified slot is reclaimed when the authorization carries
   the slot's exact token → prior state `UncleanUnsafe`, audit reason names `cas_unsafe_remount_no_delay`;
   a stale token is refused; a foreign uuid is refused before the authorization is consulted. Fails
   until the argument and the prior state exist.
2. `CASMountOpenWaits.UnsafeNoDelayOpensWithoutTheObservationWindow` (`gtest_cas_pool.cpp` ~2299
   fixture, virtual clock): with the setting, `Pool::open` over such a slot pays no observation wait
   and emits the `UncleanUnsafe` audit event; without it, the full `TTL + floor(TTL/20) +
   max(1, floor(period/2))` — the existing `UncleanOpenPaysOnlyTheObservationWindow` stays green.
3. `CASMountAwaitExpiry.*` unchanged — the helper never sees the knob (no new test; the existing
   nine keep passing).
4. `Pool::tryRemountOnce` routed through `mount_runtime.waitSleep` (refactor with the existing remount
   tests green), then `CASMountRemount.SupersededIncarnationDoesNotReclaimALiveSuccessor`: two
   runtimes, B opened with the knob over A's slot; A's renewal becomes `RenewalTerminal` with its local
   fence tripped; while A's remount observes, B keeps renewing (driven from A's wait callback) and
   A does not reclaim; separately, advancing A's BOOTTIME past its cutoff without renewals proves
   cutoff-only fencing.
5. `CASGcFenceOut.ThresholdUnchangedByUnsafeKnob`: GC's fence-out test with the knob set behaves
   identically to today.
6. Integration (`test_cas_mount_renewal_retry`, `with_rustfs`), values TTL 1000 ms / period 200 ms /
   attempt timeout 50 ms / margin 50 ms (observation exactly 1150 ms): both restarts are hard kills
   (`stop_clickhouse(kill=True)`); record the log line count before each; the safe restart appends
   exactly one `waiting ~1150 ms (token-stability observation)` line; the knob is enabled while the
   server is stopped; the unsafe restart appends none, mounts writable, and advances the epoch.
7. The regression hard-restart suite (`lightweight_delete/tests/hard_restart.py`) is pointed at the
   knob instead of shortened lease timings (clickhouse-regression change, out of this repo).

## Out of scope {#out-of-scope}

The heartbeat re-vocabulary (backlog), in-period renewal retries (issue #2244 direction 1; partly
covered by spec 1 for the connect-failure class), spec 3 and spec 4.
