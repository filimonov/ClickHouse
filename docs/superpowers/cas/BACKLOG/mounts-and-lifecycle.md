---
description: 'Live backlog — mount-lease/fence recovery and CA disk lifecycle: startup, decommission, and pool bootstrap.'
sidebar_label: 'Mounts & lifecycle'
sidebar_position: 3
slug: /superpowers/cas/backlog/mounts-and-lifecycle
title: 'CAS Backlog — Mounts and disk lifecycle'
doc_type: 'guide'
---

# CAS Backlog — Mounts and disk lifecycle {#mounts-and-lifecycle}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for mount-lease/fence recovery
and CA disk lifecycle: startup, decommission, and pool bootstrap.

## Mount-lease / fence recovery {#mount-fence}

- **KEEP: [P3.1 Task 6 / S13] live validation of fence-recovery.** TLA+ gate passed and self-remount
  on GC fence-out landed. **Task 5** (decouple renewal from the retired-view sync beat) is confirmed
  MOOT: freshness-v3 deleted `RetireView`/the syncer/`observed_gc_round` (absent from `src/` on both
  `cas-gc-rebuild` and `altinity/antalya-26.6`, only test names still say "no `RetireView`"). The
  **S13 3×-green live gate is now satisfied**: three consecutive `pass` rows in
  `utils/ca-soak/scenarios/RUN_HISTORY.md` (2026-08-31T01:16, 2026-08-31T19:42, 2026-09-01T05:53).
  Still open: which gtest suite constitutes "the sweep" is unspecified in this backlog and unverified.
- **KEEP: [A7-residual] `gc_scheduler` lifetime vs manual rounds.** `89845c2a544` (shutdown
  serializes `gc_scheduler` teardown with `gcHealth` reads) explicitly leaves both residuals open per
  its own commit message: the manual-round raw-pointer path "keeps the same latent race", and "the
  lazy-creation call sites can still resurrect a scheduler after shutdown". Confirmed still true at
  HEAD.
- **KEEP: [STID-3982-3b48 part 2] mount-lease self-race Gate 3 re-run still owed.** {#stid-3982-3b48-part-2}
  A third variant (an ambiguous client-side timeout on the renewal `PUT` misdiagnosed as a
  foreign-writer collision, SIGABRT under ASan) is fixed and landed 2026-07-24 (fence-not-rescue
  rev.4, TLA+-gated, full `Cas*:CA*` green). No record found of the CAS-s3 stateless-lane re-run that
  originally caught the crash; it still rides the next dedicated CI push.
- **KEEP (partially closed): [fence-window observability] mount-lease keeper logging.** Found
  2026-07-28 (fence-cascade RCA): during a msan CA-s3 fence window (run `07f8398acddff2c`) the log
  held ZERO `CasMountLease*` lines for the whole ~7-minute episode; closes gap #3 of
  `reference_cas_ci_observability_gaps`. Renewal failure/recovery logging landed since
  (`deliverMountRenewObservability`, `CasServerRoot.cpp`, commit `d27ce8c6c01`): `LOG_INFO` on
  recovery, `LOG_WARNING` "fenced after N attempts" on terminal failure. Still missing: fence-armed,
  remount-begin, remount-phase and remount-complete-with-duration log lines; none found under
  `Pool/CasPool.cpp` or `Pool/CasMountRuntime.cpp`.
- **KEEP: [fence-window blast radius] durable writes fail instantly for the whole fence→remount
  window.** During fence→remount (~2 min core window plus straggler tails on the msan lane) every
  durable write still returns `668`/`210` immediately (`throwCasWriteRetryLater`,
  `Backend/CasRequests.cpp`, confirmed unchanged); no bounded wait-for-remount exists on the write
  path. Design question, unresolved: should query writes block up to N seconds during a self-remount
  instead of failing immediately (the RMT-Keeper-reconnect contract)? Measure remount-time breakdown
  once the logging above lands, before tuning.
- **KEEP: [B208] CA startup mount-probe is fail-closed against a transient S3 outage.** Still exits
  (243) with no retry when the mount-probe times out during startup metadata load. No degraded-start
  code found (only a renumbering commit, `70b360471b4`). Not a gate for the durability fix (S40's
  contract only requires acked data to survive on the live replica); when fixed, add an informational
  recovery verdict to S40 so a regression here produces signal. Design question: bounded startup retry
  / degraded-start vs. today's fail-closed abort.
- **KEEP: [POOL-REFUSAL-NODE-FATAL] a pool bootstrap refusal takes the whole node down.**
  {#pool-refusal-node-fatal} `CasPool.cpp`'s residual-data guard (`missing _pool_meta over a
  non-empty pool prefix`, `Code 668`, container exit 156) still propagates out of metadata load and
  exits the whole server, confirmed unchanged at HEAD (evidence for the current framing: S43's W3
  answer, refusal plus causation control). Direction: mark the one disk broken/read-refused and keep
  the node up, per the disk-lifecycle redesign goals below.
- **KEEP: [self-authored-mount-reclaim] design not started.** {#self-authored-mount-reclaim}
  `docs/superpowers/specs/2026-09-01-cas-self-authored-mount-reclaim-design.md` (rev.5) proposes
  instant mount-lease reclaim when the slot body is byte-identical to this runtime's last write,
  replacing the full observation wait (36.5 s at defaults). Not implemented on either branch: no
  `MountPriorState::SelfAuthored` exists, and the spec still cites `CasRequestController`/
  `diagnostics.resolved_by_get`, both deleted by `37c9bd4356b` ("migrate every CAS subsystem onto
  `CasRequests`/`CasOperation`"). Needs re-grounding against the current `CasRequests`/`CasOperation`
  API before implementation.

### `[resolved-by-get-unbounds-clone-overlap]` Exact-byte resolution lets lockstep clones both stay authoritative indefinitely {#resolved-by-get-clone-overlap}

Found 2026-09-01, same design work. Not a fix this release needs; recorded because two successive
revisions of that design asserted the opposite baseline and were wrong.

A VM snapshot restored twice gives two runtimes with the same `server_uuid`, the same
`MountLeaseKeeper` state and the same `thread_local_rng` state, so they mint the same
`write_attempt_id` (`UUID.cpp:11`). Where their other body inputs also stay in lockstep — wall time,
hostname, PID, `seq`, watermark — they build **byte-identical** renewal bodies. One lands; the other
loses its `If-Match`, and `putOverwriteControlled`'s resolve-by-get sees identical bytes at a changed
token and reports `Committed` to it too (`CasRequestControl.cpp:678-687`). Both extend local authority,
and the cycle can repeat.

So the intuition that the token guard fences whichever clone loses, bounding the overlap to about one
renew period, is false: `resolved_by_get` is what removes the bound. Cloning a running server that holds
a CAS mount is outside supported operation, so this is a documentation and threat-model item rather than
a fix — but the mount documentation should say it, and any future argument that reasons from "the store
serialises us" should not assume a bound that is not there.

## CAS disk lifecycle rev.8 round (FORGET-only) — residuals {#disk-lifecycle-rev8-closure}

Round rev.8 (FORGET-only) resolved goals G1-G5 (isolation fix, throw-not-abort, GC self-exit on
Vanished/IdentityLost, generic-code correctness, FSCK-on-running advisory).

**KEEP: not resolved (deliberately deferred): the disk-lifecycle-leak proper.** A CA disk is still
cached forever in `Context::getOrCreateDisk` (confirmed, `src/Interpreters/Context.cpp:6771`) with no
teardown/eject on `DROP TABLE`, and there is no runtime re-use of the same disk after a stop (G6 is met
only node-locally via `FORGET`; G7 abandoned). The Dormant/UNMOUNT/MOUNT reuse machinery that pursued
this was rolled back (spec rev.8 §9). Full eject-on-`DROP` is future work: the disk-lifecycle
redesign, a live user goal (see [operator-replica-readd-uuid-trap](#operator-replica-readd-uuid-trap)).

Accepted residuals / watch items from that round:
- **KEEP: `search_orphaned_parts_disks=ANY` × a transient CA disk strands an unrelated table's
  load.** Accepted (spec §4 blast radius); guidance stands: keep `search_orphaned_parts_disks=LOCAL`
  when a CA disk may be transiently unreachable. Cure: `ATTACH`/restart.
- **KEEP (informational): teardown/shutdown-window is now fail-loud, not silently empty.**
  Null-pool access is `INVALID_STATE` (plan Task 15); the old T8a null-pool wedge is structurally
  gone. Watch for benign-but-noisy shutdown-window throws in generic all-disks sweeps; not a defect.
- **KEEP: GC `start()` has no partial-start desync guard.** Confirmed unchanged (T11 review, M4):
  `Cas::CasGcScheduler::start()` (`Gc/CasGcScheduler.cpp`) still spawns `thread` then `hb_thread` with
  no guard against one construction succeeding and the other failing, which would leave
  `thread.joinable()==true` while `hb_thread` never started. Pre-existing, low priority, carried for a
  future GC-scheduler hardening pass.
- **KEEP (watched): `RefWriter` DeathTest fork-under-load flake.** One `fork()` failure (~1 ms) under
  full parallel gate load (fix1 review, `1fe585ea078`), 3/3 green isolated and on clean re-run; class
  is fork-under-load, not a product red. No recurrence found since. If it recurs: serialize the CAS
  DeathTests or lower gate parallelism around them.

## Operator recovery: mounting a pool whose owner uuid differs — not decided, not started {#operator-uuid-recovery}

A server whose local uuid file was regenerated (wiped data dir, a pod recreated without a persistent
volume) cannot mount its own pool. `CasServerRoot.cpp`'s refusal (around line 547) names three manual
recoveries: restore the uuid file, configure a fresh `server_root_id`, or delete the owner object by
hand; a supported command would automate the third. **KEEP: open design choice**. Overwrite the owner
uuid with a new one (locks the original server out permanently, must cover both owner and mount
objects to keep the epoch-1 re-mint guard armed), or adopt the pool's existing owner uuid and mount as
it (`Pool::openForDecommission`, `Pool/CasPool.cpp`, already does exactly this, the reading that looks
strictly better). Not the read-only-mount task, which is separate and unimplemented. Confirmed still
open: no `force_owner_claim` mechanism exists anywhere in `src/`. Three folded 2026-08-04-triage
findings (a concrete `force_owner_claim` proposal, an audit-record ask, and a post-mortem-vs-live-
misuse distinction) add detail but do not change the open status.

## Removing and re-adding a replica under a k8s operator walks into an unrecoverable identity refusal (field report 2026-09-04) {#operator-replica-readd-uuid-trap}

**KEEP: HARD.** Field evidence: a `clickhouse-operator` cluster (`chi-cas-demo`) had replica
`cas-demo-0-1` removed one day and re-added the next; the pod's local `uuid` file
(`ServerUUID::load`, `programs/server/Server.cpp:1753`) was regenerated because the data dir did not
survive. `claimOwnerOrThrow` still refuses on a `server_uuid` mismatch (`CasServerRoot.cpp:546-547`), and
even a matching uuid is refused once retired (`throwIfOwnerRetired`, `CasServerRoot.cpp:539`,
confirmed unchanged), because decommission's retirement tail tombstones the owner in place without
changing `server_uuid` (`tombstoned.retired_at_ms = nowMs()`, `CasDecommission.cpp:468`, confirmed
unchanged). A hand-delete of the owner anchor over a non-empty subtree instead hits a second refusal
with no recovery advice at all (`CasServerRoot.cpp:560`, "identity lost over existing data"). So
`SYSTEM DROP REPLICA` + `SYSTEM CAS DROP POOL MEMBER` followed by re-adding the same pod does not
work today; only an accidental hand-edit path leaves a claimable slot, the opposite of what the
refusal messages recommend. This is the concrete instance of the [open design
choice](#operator-uuid-recovery) above; the same disk-lifecycle redesign goal (UNMOUNT ejects, MOUNT
sans table, GC stop/start, disks never torn down on `DROP TABLE`) is a live user goal and applies here.

**The ask (user, 2026-09-04):** "Оператор уже делает `SYSTEM DROP REPLICA`, должно быть что-то
похожее для CAS." A supported replica-removal verb such that, after it runs, re-adding the same
`server_root_id` with a new `server_uuid` succeeds without hand-editing the object store. Also owed:
qualify the refusal messages (name `SYSTEM CAS DROP POOL MEMBER` / `clickhouse-disks cas-drop-member`
as the supported verb, give the identity-lost refusal its own recovery sentence) and document the
interim recovery that does work today: with the victim's mount object still present, run
`SYSTEM CAS DROP POOL MEMBER '<srid>' FROM DISK '<disk>'` (or `cas-drop-member` offline) from a
surviving member while the victim is down; it adopts the uuid from the mount lease, empties the
subtree, and reports a harmless `slot tombstone failed: ... object absent before tombstone write`
warning, clearing the slot for re-add. Until the verb exists, the only advice that holds for k8s is
"keep the `uuid` file on a persistent volume", undocumented today
(`operations/migration.md#decommission` covers permanent removal only).

## `life_epoch` monotonicity holds PER SERVER ROOT — decommission must not break it {#life-epoch-monotone-per-server-root}

`writer_epoch` is durable-monotone **per server root** (`allocateWriterEpoch` CAS-bumps
`<prefix>/gc/server-roots/<srid>/epoch`), which is what makes "a decrease means a fenced-out writer,
not a race" true. **KEEP: the argument has a stated limit**: if a namespace's `_ckpt` could ever be
created under one server root and later contributed to by another, the two counters are independent
and an honest low contribution would be misread as corruption. Nothing does that today: confirmed,
decommission (`Tools/CasDecommission.cpp`) tombstones and empties a slot, it does not move or adopt a
namespace across roots. Whoever builds cross-root namespace transfer must keep a namespace within one
root for its life, or replace this argument; the limit is also stated at `joinLifeEpoch` in the code.

## Decommission race findings {#decommission-toctou-stamps-successor}

**KEEP: a live successor can be stamped retired.** Formerly filed separately as
`[decommission-successor-mount-race]` (2026-08-04 triage, "review finding №9"); that framing,
"the sweep deletes what the successor needs", turned out inexact (the destructive phases run under
the victim's own mount lease). The real, still-open defect is a cross-key TOCTOU between the liveness
recheck (the `mount`/`epoch` GETs) and the owner-anchor CAS: a LIVE same-uuid successor recreated in
that narrow gap gets its owner anchor tombstoned by the decommission run, so its next restart is
wrongly refused with `CORRUPTED_DATA`. Availability, not data loss. Confirmed still open and explicitly
accepted at `Tools/CasDecommission.cpp:419-428`, code comment: "ACCEPTED RESIDUAL WINDOW (final
review, not closed by this recheck) ... T5's owner-tombstone design (finding #9) intentionally stopped
short of making concurrent decommission-vs-recreate airtight". P2. Reproducing it needs a chaos test
that restarts the victim between the recheck and the CAS. Loosely adjacent: 2031-triage CAS-063 and
CAS-007 below.

### `[decommission-waits-on-the-wrong-predicate]` `cas_mounts` liveness and `NoWait` decommission disagree about what "dead" means {#decommission-wrong-predicate}

`SYSTEM CAS DROP POOL MEMBER` under the `NoWait` policy refused a genuinely dead node in CI
(`test_cas_drop_pool_member::test_drop_dead_pool_member_heals_the_pool`, PR 2073, integration
amd_tsan 4/6), 15.5 seconds AFTER the target's lease wall-clock expiry:

```
CAS decommission 'node2': pool member is alive or contended -- mount lease held by
uuid=... epoch=1 pid=10 hostname=node2 (expires_at_ms=1785811895007). Refusing ...
```

This is not a stuck lease. The two sides use different definitions of dead, and each is right on its
own terms:

- **The observable one** is wall-clock: `CasServerRoot.cpp:236` computes `live = !gc_fenced &&
  expires_at_ms > now_ms`, and that is what a `cas_mounts` reader sees. The same file's own operator
  text carries a `CLOCK SKEW CAVEAT` about precisely this comparison.
- **The one reclaim requires** refuses that comparison outright. `claimMount`'s comment
  (`CasServerRoot.cpp:410-424`) says a same-uuid/different-epoch lease is reclaimed "ONLY on a
  certificate of death that needs no fresh wall-clock trust -- never by comparing `expires_at_ms`
  against `now_ms`": `gc_fenced`, the clean marker, or a `proven_dead_token`. A `kill=True` stop
  leaves none of the three, and `NoWait` passes an empty `proven_dead_token` (`CasPool.cpp:668`),
  skipping the observation wait that would mint one.

So the only route to `NoWait` success for a hard-killed node is a GC round fencing the dead mount
first. In the failing run GC rounds were executing on their ~1s cadence but reporting
`deferred`/`candidates=0` — the fence had not happened yet. The test's precondition polls
`cas_mounts.state != 'live'` for up to 90s, which the wall-clock definition satisfies on its own, so
passing that gate does not establish what the call it guards actually needs.

**Not yet decided, and the decision is the work here:** whether this is a test that waits on the wrong
predicate (fix: wait for the fence, or use the waiting policy), or a product gap (fix: `NoWait`
decommission should accept a hard-killed member without requiring GC to get there first, or say in
its refusal what the operator must wait for). Do not "fix" it by weakening the certificate-of-death
rule — that rule is what keeps a live twin from being decommissioned across two clocks.

Falsification: if a rerun passes on unchanged code, it is a cadence race rather than a deterministic
gap, which changes the fix but not the mismatch.

## `createNamespaceStep1` is the one writer-plane durable write without a fence check {#create-namespace-step1-unfenced}

**KEEP**, confirmed unchanged: `createNamespaceStep1` (`Pool/CasRefCatalog.cpp`) is still the only
caller of `casUpdateImpl` among the mutating `Cas::Backend` methods that skips the fence obligation its
own header documents: its signature carries no `admitted_generation`/`check_fence_or_throw`, unlike
sibling callers and unlike `casUpdate`'s `CatalogFenceMovedMarker` path. Consequence: a fenced-out
mount can durably insert a `Creating` row under an already-dead `CreatorFence`; self-heals on the next
opener, so durable garbage, not data loss, P3. Fix is one line, symmetric with the sibling callers; no
test exists for it today. Lower-confidence secondary: the abort-path `deleteExact`
(`Pool/CasPartWriteTxn.cpp`) is unfenced and sits inside a swallowing `catch(...)`.

## Small hygiene residuals (P3) {#hygiene-residuals}

- **KEEP: `CasGcScheduler::stop` joins worker threads outside their mutex.**
  {#gc-scheduler-stop-join-race} Confirmed unchanged at `Gc/CasGcScheduler.cpp`: `stop()` takes `mutex`
  only to set `stopping`, then joins `thread`/`hb_thread` unlocked, while `start()` and
  `requestRoundSoon()` still touch the same members under `mutex`, a real data race
  (`ThreadFromGlobalPool::join`'s `state.reset()` overlaps `joinable()`'s read). Reachable pair:
  `requestRoundSoon` (only caller: `SYSTEM CAS DROP POOL MEMBER`) racing `SYSTEM CAS GC STOP`. No data
  loss, P2. Fix: hoist the thread objects into locals under `mutex`, join outside the lock.
- **KEEP: [fence-costs-epoch-distinct-mint] mint `epochCeiling + 1` instead of literal `1` in
  `RemintEpoch`'s honest branch.** Not implemented on either branch (no `RemintEpoch`/`epochCeiling`
  symbol found anywhere in `src/`).
  Its own source, `docs/superpowers/models/CaCasMountCore_RESULTS.md`, still flags this as a candidate
  refinement for a later round; closes the documented `FenceCostsEpoch` gap. Desirable, not urgent.
- **KEEP: `type=encrypted` over a CAS disk is accepted with no capability gate.**
  {#encrypted-over-cas-missing-gate} Confirmed unchanged: `DiskEncrypted` (`Disks/DiskEncrypted.h:332`)
  forwards `isPlain` but not `isContentAddressed`/`supportsAtomicFileWrites`, so both fall back to
  `false`. CAS+encryption is out of scope for this release (`[B17]` in
  `BACKLOG/operability-and-introspection.md` covers the real per-key-dedup feature); the residual is
  only the missing fail-fast, which fails loud at the first INSERT today (`NOT_IMPLEMENTED`), not
  silently. Fix (small): refuse at disk construction/config validation when the delegate answers
  `isContentAddressed()`.
- **KEEP: `system.content_addressed_mounts` can't tell why a slot has no live row.**
  {#owner-only-slot-invisible-in-mounts} (formerly also filed as `[mounts-list-failure-
  indistinguishable]`, merged here: same view, complementary causes.) Both confirmed still open at
  `StorageSystemContentAddressedMounts.cpp`: (1) when `Cas::listMounts` throws, the synthesized
  fallback row sets `state` via `insertDefault()`, so a throttled LIST reads exactly like a genuinely
  empty pool; (2) a decommission crash can leave a member with `mount` and `epoch` deleted but the
  owner anchor not yet tombstoned, so that slot has no row at all, even though it is NOT unrepairable
  (the owner anchor is the resume anchor, `Pool::openForDecommission` and
  `EpochMintPolicy::DecommissionRecovery` handle it; pinned by `gtest_cas_decommission.cpp`
  `SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor` and
  `MidRetirementCrashResumesViaMountLeaseFallback`). Both P3, observability-only. Owed (small): a
  `state` word distinguishing `unknown`/`retiring` from a genuinely empty listing, or a nullable
  `mount_list_error` column. Related:
  [`[decommission-waits-on-the-wrong-predicate]`](#decommission-wrong-predicate)
  (`cas_mounts` liveness vs `NoWait` decommission disagreeing about "dead").
- **KEEP: `decodeServerEpoch` accepts `nwe = 0`, and `MountFence` carries two never-read identity
  fields.** {#server-epoch-zero-and-dead-fence-identity} Both confirmed unchanged, neither a live
  defect (both fail loud or are inert): (1) `Formats/CasServerRootFormats.cpp`'s `decodeServerEpoch`
  still has no zero-clamp on the object-present path (the absent path is clamped); no writer produces
  such a body today, and the first ref/manifest encode would throw `CORRUPTED_DATA` if one did. Owed:
  reject zero at decode time, matching `CasDecommission`'s own precondition. (2)
  `MountFence::server_uuid`/`writer_epoch` (`Pool/CasMountRuntime.h`) are still assigned by
  `armMountFence` and read by nothing in the tree; durable identity is enforced elsewhere
  (`liveWriterEpoch` in `Pool/CasPartWriteTxn.cpp`). Owed: drop the two fields, or comment that they
  are diagnostic-only.
- **KEEP: the mount-fence clock uses `CLOCK_BOOTTIME` with no portability shim.**
  {#boottime-not-portable} Confirmed unchanged: two unconditional `clock_gettime(CLOCK_BOOTTIME, ...)`
  reads (`Pool/CasMountRuntime.cpp`, `Pool/CasServerRoot.cpp`) compile on every platform; `dbms`
  sources are not `OS_LINUX`-guarded (`src/CMakeLists.txt`). `CLOCK_BOOTTIME` is Linux-only; Darwin has
  no drop-in (`CLOCK_UPTIME_RAW` excludes sleep, unlike the fence's stated requirement), so only Darwin
  builds are expected to break — FreeBSD already aliases `CLOCK_BOOTTIME` to `CLOCK_UPTIME` under
  `__BSD_VISIBLE`. Compile error on Darwin builds, zero runtime exposure. Fix: extend
  `base/base/time.h`'s existing
  `CLOCK_MONOTONIC_COARSE` per-platform shim with a `CLOCK_BOOTTIME` mapping, noting Darwin's
  substitute weakens (does not break) the suspend argument.

## Issue #2243 CONFIRMED: local port exhaustion fences out the mount lease (2026-08-20) {#issue-2243-port-exhaustion-lease}

https://github.com/Altinity/ClickHouse/issues/2243 — read-heavy concurrent `SELECT FINAL` on a CAS
default-policy disk exhausts the container's ephemeral ports (errno 99 `EADDRNOTAVAIL`), which kills
the mount-lease renewal → fence → `TransientNotLive` → ~42 s full-disk refusal → self-remount discards
in-flight `PartWriteTxn`s. CONFIRMED REAL (adjudicated from code; mechanism matches our own S07 finding
from 2026-07-06). The rev.8 fail-close chain itself worked exactly as designed; the defects are in the
trigger and its classification. Issue closed 2026-08-20.

Mechanism (verified at file:line): the S3 client keep-alive default is 5 s (`S3Defines.h:12`); pooled
DISK connections expire at 0.8×5 s idle (0.1×5 s once the group crosses `disk_connections_soft_limit`),
`mustReconnect` resets any connection whose request STARTED > 4.5 s ago, and `http_keep_alive_max_requests`
rotates every 100 requests. With P pooled connections and R req/s, reuse survives only while P/R < 4 s —
concurrent FINAL prefetch puts P in the dead band, so nearly every request connects fresh and the CLIENT
closes on return → TIME_WAIT at request rate (reporter: ~430 GET/s × 60 s ≈ 26k of 28,232 ports). The
store honoring keep-alive is irrelevant. Retry amplification: `EADDRNOTAVAIL` is classified as remote
transient at EVERY layer (Poco → `CoreErrors::NETWORK_CONNECTION` → retryable; CAS controller →
`Unresolved`); reads ride `s3_retry_attempts=500`. Lease renewal = ONE bare single-attempt PUT per 10 s
through the same DISK pool (no reserve, no controller budget — WEAKER than any data write); outage length
is constructive: `mountObservationThresholdMs = ttl + 5% + cadence = 36.5 s` (field answer to
`[fence-window blast radius]`'s "measure where remount time goes").

Fix directions, in value order:
- (1) **Classify local socket-resource errors as local** (`EADDRNOTAVAIL`, `EMFILE`, `ENFILE`): hard
  backoff instead of retry-at-rate. Template = the existing DNS sub-classification branch
  (`PocoHTTPClient.cpp:840`); CAS side maps it out of `Unresolved`'s full budget. Retry loops are the
  positive-feedback term — this cuts the amplifier.
- (2) **Mount-lease resilience**: in-period fast retry (today a failed renewal waits the FULL next
  period — `CasServerRoot.cpp:1437` `continue` re-enters `wait_for(period)`), and/or reserved
  connection / dedicated small budget for the renewal PUT. Note the margin arithmetic: ttl=30s /
  period=10s / margin=2s allows at most TWO ride-out warnings per lease generation, not three. Fix (2)
  landed with the request-engine migration (`37c9bd4356b`, 2026-09-05), but
  `[renewal-gives-up-with-budget-left]` below shows a live gap.
- (3) **Keep-alive defaults for CAS-over-S3 profiles**: 5 s TTL + 100-request rotation is the churn
  engine under sustained read concurrency; evaluate raising `http_keep_alive_timeout` (60 s) and
  `http_keep_alive_max_requests` in the CAS disk profile / docs. Caveat: the server-advertised
  `Keep-Alive: timeout=N` header mins the client value (`HTTPClientSession.cpp:377`) — capture what
  RustFS advertises first. Diagnostic BEFORE any change: `DiskConnectionsCreated/Reused/Expired/Reset/
  Preserved` + `DiskConnectionsTotal/Stored` (prediction: Created≈request rate, Expired>>Reset, Stored≈0;
  if Reset>>Expired the cause is the non-drained-body branch and the fix belongs in the read path). DONE:
  keep-alive defaults are now documented (`docs/en/antalya/cas/configuration.md:34-35,143,158-159`, 30 s /
  10000), unblocked by the settings-namespace fix (`[cas-disk-s3-key-whitelist-gap]`).
- (4) NOT a fix: capping pool limits below the ephemeral range (reporter's #1) — limits gate KEEPING,
  not CREATING; TIME_WAIT is invisible to them (hence errno 99, never `HTTP_CONNECTION_LIMIT_REACHED`);
  lower caps flip the pool into the 0.5 s TTL regime sooner and worsen churn.

Housekeeping folded in:
- **S07 wide-part port-exhaustion finding re-rated**: was closed 2026-07-06 as "cost/latency only, not a
  data bug" (`BACKLOG/performance.md`, DESIRABLE) — #2243 refutes that scope: the same condition takes the
  LEASE down and discards in-flight write txns. Availability class, not just cost.
- **[B196] is stale as written**: `s3_max_connections` is dead code for the disk path (an AWS-SDK
  `ClientConfiguration` field; `PocoHTTPClient` never reads `maxConnections`) — the disk path is governed
  by `disk_connections_*` + keep-alive, and NONE of those caps concurrent socket creation either.
- **Soak-harness blind spot**: `soak/cluster.py` classifies "mount lease not held" as retryable
  `NETWORK_ERROR` — our own soaks would ride through a #2243 event and score it recoverable; add a
  lease-loss detector (count `TransientNotLive` windows / `CasMountLeaseKeeper` errors) to checkpoints.
  Still open at `utils/ca-soak/soak/cluster.py:24-28,201-218`.

## `[renewal-gives-up-with-budget-left]` A renewal stopped with 1,969 ms of confirmed budget unspent, and one attempt burned 23.7 s {#renewal-gives-up-with-budget-left}

**Supersedes an earlier framing of mine that reported known, fixed behaviour as a new defect.** The
first version of this entry said "a single unresolved heartbeat write costs the mount lease, with no
retry" — which is exactly what
[Issue #2244](https://github.com/Altinity/ClickHouse/issues/2244) diagnosed on 2026-08-20 and whose
minimum fix landed **2026-08-24** (`docs/superpowers/specs/2026-08-23-cas-mount-renewal-retry-design.md`),
with focused and full TLA+, Release/Debug, proxy-integration and 15-minute S39 gates. Filing it again
as novel was a duplicate. Issue #2244 and its per-step-remount follow-up are both DONE, closed
2026-09-14 (`7f932d31352`/`37c9bd4356b`), and carry no topic-file anchor.

**Observed 2026-09-01, S03 at `--scale full`**, which then failed with `Code: 210 ... mount lease not
held`. Read off `system.ca_event_log`:

| field | ch1 | ch2 |
|---|---|---|
| `attempts_sent` | 1 | **1** |
| `elapsed_ms` | **23,755** | 18,031 |
| `remaining_confirmed_budget_ms` | 0 | **1,969** |
| `unresolved_reason` | `deadline_mid_way` | `deadline_mid_way` |
| `classification` | `external_lease_deadline` | `external_lease_deadline` |
| `stop_cause` | `continue` | `continue` |

**What is NOT a defect, on the current design.** The implemented protocol retries "within the time
still justified by its last confirmed lease". ch1 had **zero** budget left, so sending no second
attempt is the design working, not failing.

**What is worth investigating, and both are separate from #2244's original diagnosis.**

**ch2 stopped with 1,969 ms of confirmed budget remaining** and `stop_cause = continue`, meaning
nothing asked it to stop — precisely the window in which the 2026-08-24 fix is supposed to retry.
Either the budget arithmetic, the margin term (`now + margin < confirmed_deadline`), or the loop's
exit condition keeps a retry from being issued when one is still justified.

**A single attempt consumed 23.7 s against a 30 s TTL.** #2244 describes renewal as "one
5-second-timeout `PUT` per 10-second period". An attempt running 23.7 s is nearly five times that
bound, so either the per-attempt timeout is not being applied on this path or the attempt is not the
`PUT` alone. Whatever the answer, an attempt that can eat 79% of the TTL leaves no room for the retry
protocol to help — the budget is gone before the second attempt could be considered.

The request-engine migration (`37c9bd4356b`, 2026-09-05) rewrote the retry machinery this sits on
(`CasRequestBudget.cpp`), but no commit or test specifically targets either gap. `validateCasRequestBudget`
only validates a budget shape at mount time; it does not bound a live attempt. Needs an S03
`--scale full` rerun to check whether the migration incidentally closed this. Does not reproduce at
`dev`/`ci`.

**Why the write did not resolve** is the storage saturation measured the same night: RustFS reporting
`permits_in_use: 256/256`, 100% queue utilization, answering `503` after ~5 s with its CPU at 0.13%.
A heartbeat is an ordinary write on that path and gets no privilege.

**Downstream, and correct.** The fenced epoch is terminal, so the self-remount re-claims with a fresh
`writer_epoch`, finds the previous epoch's slot un-fenced and not proven dead, and refuses under "no
wall-clock trust" — three `mount_conflict` events with `outcome = live_double_start`, five seconds
apart. That refusal prevents taking a lease from a possibly-live writer on a clock guess. Do not read
those conflicts as the fault. Related: issue #2244's still-open per-step remount work (DONE, closed
2026-09-14, carries no topic-file anchor).

**Reproduction:** S03 at `--scale full`; does not reproduce at `dev` or `ci`. The ten-second span of
the conflicts is an artifact of when `predown_dump` ran, not the duration of the event.

## Nested `server_root_id` + prefix-based decommission victim selection (2031-triage CAS-007) {#nested-srid-decommission}

`validateServerRootId` accepts slashes (`Pool/CasServerRoot.h:199-229`; `gtest_cas_mount.cpp:97` asserts
`shard-01/replica-a` valid, and multi-segment srid support was deliberately fixed in `b97847d32f9`),
while `CasDecommission` picks victims by path prefix (`Tools/CasDecommission.cpp:146,150` +
prefix-LIST drains of `cas/manifests/<srid>/`, `staging/`, `roots/`). So
`SYSTEM CAS DROP POOL MEMBER 'a'` destroys the namespaces and control objects of a LIVE member
`a/b`. Existing test covers only the sibling case (`gtest_cas_decommission.cpp:699-716`). Partial
mitigation exists in one direction only: mounting `a` when `a/b` already exists fails closed
(`CasServerRoot.cpp:145-149`); the reverse order is unguarded. Note the asymmetry — relink routing
compares srid for EXACT equality (`ContentAddressedMetadataStorage.cpp:2021`), i.e. the prefix rule
is decommission-local.

Fix options (decide at fix time): (1) make decommission victim selection exact-srid + an explicit
refusal when any other member's srid is prefixed by the victim's, or (2) forbid nesting at validation
(reject a srid that is a prefix of, or prefixed by, an existing member's) — cheaper but removes the
multi-segment layouts that were deliberately enabled. Either way add the nesting case to
`gtest_cas_decommission.cpp`. P2: destructive but operator-initiated and requires a nested-srid
layout.

## CAS mount protocols run serially on the startup thread (found 2026-09-05, analysed 2026-09-06) {#mount-protocols-serial-startup}

One disk's token-stability observation (~TTL + TTL/20 + poll) blocks the startup thread while
earlier-mounted disks' leases age; renewers are per-disk and start eagerly, so the exposure is the
serial `DiskSelector::initialize` → `disk->startup()` → `Pool::open` chain. Trigger is gone on ordinary
restarts since the farewell-window fix; a genuine hard kill of a multi-disk server still opens it. Fix
direction: run `Pool::open` per disk on a background task and collect futures after the disk loop
(touches `DiskSelector`, consult-first). Needs a hard-kill integration step (`stop_clickhouse(kill=True)`
with two CAS disks) and a spec.

## Mount-lease budget: fewer knobs, derived attempt timeout (2026-09-07, from the PR #2300 run-3 triage) {#mount-lease-budget-derived-timeout}

Today the operator sets TTL, renew period, `cas_attempt_timeout_ms` and margin, and the connect cap is
`min(connect_timeout_ms, attempt)`; the mount refuses when `period + 2×(attempt + 2×cap) + margin ≥ TTL`
(a disk with `connect_timeout_ms ≥ 2 s` under the old defaults). Proposed: (1) `cas_attempt_timeout_ms = 0`
= auto = `(TTL − period − margin)/5 − 2×cap`, i.e. "five full attempts fit in the renewal window" as a
design constant; the validation then becomes tautological; (2) clamp the connect cap to the lease budget
instead of refusing the mount, WARN once at mount and show the effective cap in `system.cas_mounts` and
the "budget in effect" log line; (3) document the fencing-latency trade-off (observation =
TTL + TTL/20 + period/2; GC fence-out = TTL + TTL/20 + period). Interim fix applied: the `test_cas_s3`
config keeps `connect_timeout_ms` at 1000 (user chose the smallest change; the default TTL stays 30 s).

## Closed this round {#closed-this-round}

Verified DONE at HEAD (`cas-gc-rebuild`), removed from the live backlog:

- **`~Pool`'s network teardown running under `pointer_mutex`.** Fixed:
  `ContentAddressedMetadataStorage::stopAndDrainForTeardown` now moves `part_access`/`cas_store` into
  locals under `pointer_mutex`, releases the lock, then resets/drains them unlocked. Commits
  `4b04b2c2cae`, `205af29c7f2`.
- **Detached CAS work outliving `Context`; `~Pool`'s farewell logging through a null shared
  context.** Both halves fixed by the same refactor: shutdown now drains detached dispatches with a
  bounded timeout (`stopAndDrainForTeardown`, `ProfileEvents::CASDetachedWorkDrainTimeouts` on
  timeout, proceeds rather than hanging), and the event-emit path tolerates an absent sink/context
  (`emitMountEvent`'s `if (!sink) return;`, `Context::getContentAddressedLog`'s `if (!shared) return
  {};`). Commits `e69b4d3c26f`, `4b04b2c2cae`, `205af29c7f2`.
- **Recovery-thread spawn could permanently disable self-remount.** Fixed by the runtime-ownership
  work, `ecf3d5d7c76f`, confirmed present on `cas-gc-rebuild`.
- **`CasGcScheduler` lazy-construction race under concurrent `SYSTEM GC`.** Fixed: lazy creation now
  happens under `pointer_mutex` inside the caller's `gc_scheduler_mutex`/`lifecycle_mutex`
  (`ContentAddressedMetadataStorage::gcStart`). Commits `452d17af42f`, `e79a109b142`.
