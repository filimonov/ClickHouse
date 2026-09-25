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
