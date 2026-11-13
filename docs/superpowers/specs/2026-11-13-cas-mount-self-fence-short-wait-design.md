---
description: 'Design in three separable parts for a shorter mount wait of a CAS disk after a restart on the same node, with or without a node reboot. Part A: on a controlled stop that cannot write the clean farewell, the holder raises its mount fence and writes a new optional field into its own mount slot; a successor of the same server uuid waits a bound declared by the holder (about 22 s) instead of the 131 s lease observation threshold. Part B: a local record of slot write attempts for a restart after a kill on the same running kernel. Part C: the successor credits the age of the slot body, measured by the object store, against the threshold.'
sidebar_label: 'CAS: short mount wait after a self-fence'
sidebar_position: 115
slug: /superpowers/specs/cas-mount-self-fence-short-wait-design
title: 'CAS: shorter mount wait after a restart: holder fence marker, local attempt record, store-clock age'
doc_type: 'reference'
---

# CAS: shorter mount wait after a restart — design {#cas-mount-self-fence-short-wait}

- Date: 2026-11-13. Status: Part A is decided in principle, this document fixes its details. Parts B and C
  are proposals; this document gives a verdict on each. Nothing here is implemented, built or run.
- Target case: a restart of the server on the same node, possibly together with a reboot of the node.
- The three parts are separable. Each can be reviewed, built and dropped alone ([§9](#composition)).
- Tasks: CAS-355 (forced shutdown leaves the lease), CAS-480 (restart after a kill waits 131 s),
  CAS-482 (local witness, shelved), CAS-483 (slot age on the store's clock: Part C),
  CAS-484 (wrong claim in the text of `cas_unsafe_remount_no_delay`, not fixed here).
- Source tree: worktree `cas-pr1-clean` at `a3967637843`. Every `path:line` below was read at that commit.
  The head `802077e5c1a` adds test configs only.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `D/` = `docs/en/antalya/cas/`. `M/` = `docs/superpowers/models/` in the `master` worktree.
- Input: `tmp/prbundle/wave2/lease-inventory.md` in the `master` worktree, called "the inventory". It lists
  what relies on the mount lease and the write kinds R1 to R4.
- Old format reference: tag `v26.6.4.20001.altinityantalya`, called "26.6.4".
- "Inferred" marks a statement that was not traced in the code. [§12](#limits) lists all of them.

## 1. Decision {#decision}

**Part A. A holder that stops under its own control tells the successor that its fence is up.**

- On a controlled stop that cannot write the clean farewell, the holder trips its mount fence, stops its
  renewal workers, and writes one more body into its own mount slot. The body carries a new optional JSON
  field `holder_fenced_settle_ms`.
- The field says two things. The holder's mount fence was up before this body was sent. Every request the
  holder had admitted on the mount plane ends within that many milliseconds.
- A successor with the same `server_uuid` that observes such a body uses the threshold
  `settle + settle / 20 + poll` instead of `L + L / 20 + poll`. The claim stays conditional on the observed
  token. Defaults: 21800 ms against 131000 ms.
- Two paths write it: the server's forced exit (`Will shutdown forcefully`), and a pool teardown whose ref
  lanes did not drain.
- The field is not `gc_fenced`. A reader that does not know the field skips it and waits the full lease.
- The clean farewell stays an instant reclaim. The lease stays 120 s. GC fencing, decommission, creator
  takeover and the relink confirm do not change.
- One generic file changes: `programs/server/Server.cpp`, one include and one call.

**Part B. A local record of slot write attempts: sound only under deployment rules the code cannot check.**

- Rule: the server stores the `write_attempt_id` of every slot body it sends, and the kernel boot id, in a
  local file. A restart that finds the slot body's `write_attempt_id` in the file, under the same boot id,
  waits about 25 s instead of 131 s. No on-store change.
- What a match proves: the last writer of the slot used this local file during this kernel boot. That it is
  dead follows only from the `status` file lock, and only while four deployment rules hold
  ([§7.5](#b-holes)).
- Two of those rules cover cases the lease protects today: a `status` file removed under a live server, and
  a copy of the data directory started on the same kernel.
- Recommendation: build Part A first. Decide Part B separately, on the production need for a fast restart
  after an OOM kill. The CI lanes no longer need it: their configs set `cas_unsafe_remount_no_delay`
  (commit `802077e5c1a`).

**Part C. The successor credits the slot's age on the store's clock: sound under conditions on the store's
clock.**

- Rule: `wait = threshold - age`, never below one poll step. `age` is the `Last-Modified` of the epoch
  object this mount just wrote minus the `Last-Modified` of the slot body, less a 4 s margin. Both stamps
  come from the store. No on-store change, no local state, no generic file, one extra read per mount.
- Why it is safe: a holder counts its lease from the start of the renewal attempt, and the store stamps the
  body later. The age is at most what the holder itself has counted ([§8.4](#c-safety)).
- It is the only part that helps after a node reboot, a power loss, or a move to another node without a
  controlled stop. It does nothing for a restart seconds after a kill.
- Conditions: the store's stamps are accurate to seconds across its nodes and are never set back
  ([§8.5](#c-assumptions)). Not in doubt on AWS S3; not measured on self-hosted clusters.

Restart wait before the reclaim, default settings, S3 disk, computed, not measured. `d` is the store time
from the last slot write to the successor's start. The full table is in [§9](#composition).

| Stop of the previous process | Today | A | B | C | Best |
|---|---|---|---|---|---|
| Clean stop, farewell written | 0 s | 0 s | 0 s | 0 s | 0 s |
| Forced exit (`Will shutdown forcefully`), restart in place | 135 s | 25 s | 25 s | 130 s | A: 25 s |
| Forced exit, then a node reboot | 135 s | 25 s | 135 s | `135 s - d` | A+C: 5 s |
| `SIGKILL` or OOM kill, restart on the same running kernel | 135 s | 135 s | 25 s | `135 s - d` | B+C: `29 s - d` |
| Power loss, another node, any downtime above 135 s | 135 s | 135 s | 135 s | 5 s | C |
| Successor runs a build without these parts | 135 s | 135 s | 135 s | 135 s | 135 s |
| Slot fenced by GC before the restart | 0 s | 0 s | 0 s | 0 s | 0 s |

The loop looks at the slot every `poll` = 5 s and sleeps once before its second look. So every wait ends at
the first multiple of 5 s at or above the remaining time, and is at least 5 s: 21.8 s becomes 25 s, and a
full credit under Part C still costs 5 s. No part changes that.

### 1.1 Failure asymmetry {#asymmetry}

- A wrong short wait puts two writers under one `server_root_id`. The damage is R1 to R4 of the inventory:
  a committed ref over a deleted manifest or deleted blobs, a lost table-level update, a table stuck in
  `Removing`. Recovery is manual.
- A wrong full wait costs about 110 s of start-up.
- So every case without positive evidence keeps the full wait, and a failed or unclear marker write is never
  repaired by a second write.

### 1.2 Verdicts {#verdicts}

| Part | Verdict | Rests on |
|---|---|---|
| A | Sound | The fence check before every mount-plane request, the join of both workers before the marker, and a fence that nothing re-arms ([§3.5](#bound), [§3.7](#no-rearm)). Residuals are the ones the full wait has today: a request the store applies after the client gave up, and a thread stalled between the fence check and the wire |
| B | Sound with conditions | Four deployment rules outside CAS code ([§7.5](#b-holes)). Without them a live but stalled predecessor is reclaimed after 25 s |
| C | Sound with conditions | Five properties of the store's clock and stamps ([§8.5](#c-assumptions)). A stamp error beyond the margin and the threshold's slack (about 13 s at defaults) reclaims a live but stalled predecessor early |

## 2. Context {#context}

### 2.1 Problem {#problem}

- A server restarted after an unclean stop waits `L + L / 20 + poll` before it listens: 131 s at the 120 s
  default, 135 s on the 5 s poll grid (`CA/Pool/CasServerRoot.cpp:1209-1300`).
- The forced exit of the server is an unclean stop for the mount: it calls `safeExit` before the disks are
  shut down (`programs/server/Server.cpp:4006-4018`). CI stress run 43379266824 hit it: two CAS disks waited
  131 s against a 110 s start budget (CAS-355).
- A teardown with a wedged ref lane is also unclean: `finishTeardown` logs "skipping the clean-release
  marker; the next mount will treat this end as unclean" (`CA/Pool/CasMountRuntime.cpp:1254-1260`).
- In both cases the holder runs code before it stops. It can stop writing and say so.

### 2.2 Rejected {#rejected}

| Option | Why not |
|---|---|
| A shorter lease | The lease sets the write window during a store outage: 94 s at 120 s, 4 s at 30 s (commit `b528b234dca`) |
| A separate, shorter write window inside the lease | It restores the 4 s figure. The restart wait cannot be shorter than the time the old holder may keep writing without a renewal |
| A local lock or socket witness | Shelved (CAS-482): too much mechanism for one narrow case |
| Marking the slot `gc_fenced` on exit | Every reader reclaims a `gc_fenced` slot at once (`CasServerRoot.cpp:1087`), builds without this feature included. That is a wait of 0 with requests still in flight |

### 2.3 Code facts {#code-facts}

| # | Fact | Where |
|---|---|---|
| F1 | The wait loop sets its threshold once per observed token, from the observed body: `mountObservationThresholdMs(ttl_ms, declaredMountLeaseMs(*r.body), poll)`. It sleeps a whole `poll` between looks | `CasServerRoot.cpp:1233-1235`, `:1289`, `:1298` |
| F2 | `poll = max(1, cas_mount_renew_period_ms / 2)`: 5 s by default | `CA/Pool/CasPool.cpp:701-702` |
| F3 | A same-uuid older-epoch body is reclaimed when it is `gc_fenced`, a farewell, proven dead by the loop, or authorized by the unsafe knob. Anything else is `LiveDoubleStart` with no write. The same four conditions are in 26.6.4 | `CasServerRoot.cpp:1085-1119`; 26.6.4 `CasServerRoot.cpp:867-869` |
| F4 | A body of another uuid is reclaimed only when it is `gc_fenced` or a farewell | `CasServerRoot.cpp:1022-1032` |
| F5 | The mount fence refuses when `lost` is set or the generation changed, before it looks at the deadline | `CasMountRuntime.cpp:156-170` |
| F6 | `tripFenceWithoutOperationalLoss` sets `lost` and bumps the generation. It changes no lifecycle and schedules no remount | `CasMountRuntime.cpp:131-135` |
| F7 | `lost` is cleared in one place, `armMountFence`. It has two callers: the first arm of a new `Pool`, and `Pool::tryRemountOnce` | `CasMountRuntime.cpp:236`; `CasPool.cpp:966`, `:1679` |
| F8 | `Pool::tryRemountOnce` is reachable only as the `remount_attempt` callback of the remount worker | `CasPool.cpp:271`; `CasMountRuntime.cpp:895` |
| F9 | `stopBackgroundWorkers` sets `workers_stop_requested`, joins both workers, and never clears the flag. Only `startBackgroundWorkers` clears it, and only a new mount calls that | `CasMountRuntime.cpp:936-965`, `:701`; `CasPool.cpp:979-980` |
| F10 | `scheduleRemount` does nothing once `workers_stop_requested` is set | `CasMountRuntime.cpp:1173-1185` |
| F11 | A committed renewal publishes a new deadline. It does not touch `lost` | `CasMountRuntime.cpp:362-363`, `:603-612` |
| F12 | A renewal that ends terminal while the driver is `Stopping` trips the fence without a remount | `CasMountRuntime.cpp:387-391` |
| F13 | Every engine verb asks the fence before each request, each reissue and each pause. A write asks once more after a proven commit, with no request | `CA/Backend/CasRequests.cpp:436-449`, `:954-966`, `:886-907`, `:868-884`; `CA/Backend/CasRequests.h:526-596` |
| F14 | The read that settles an unclear write attempt is a gated read. After a trip it is not sent and the write ends as `GaveUp` with `FenceLost` | `CasRequests.cpp:1105-1113`, `:784-811`, `:843-856` |
| F15 | The three planes: mount (the mount fence), lease (open), GC (refuses only after `beginTeardown`) | `CasPool.cpp:191-220`, `:1187-1199` |
| F16 | Attempt envelope = `attempt_timeout_ms + 2 × connect cap`: 7000 ms for S3, 5000 ms without a connect cap. Write reserve = `2 × envelope + lease_safety_margin_ms`: 16000 ms | `CA/Backend/CasRequestBudget.cpp:19-29`, `:105`; `CA/Backend/CasRequestBudget.h:22`, `:30`, `:36` |
| F17 | The farewell: join both workers, drain the ref lanes, then `release` only for an `Active` renewer. `release` sets `Released` first and writes on the lease plane | `CasPool.cpp:1103-1138`; `CasMountRuntime.cpp:1230-1261`; `CasServerRoot.cpp:2245-2255` |
| F18 | A renewal stopped before any request was sent leaves the renewer `Active`. One stopped after a request was sent leaves it `RenewalTerminal` | `CasServerRoot.cpp:2124-2134`, `:2136-2161`, `:1895-1919` |
| F19 | `decodeMountLease` skips unknown keys. A key that starts with `!` is refused. 26.6.4 opens the mount lease with `KeyStrictness::Tolerant`. Both trees have the test `CASMountLeaseFormat.UnknownFieldsRemainTolerated` | `CA/Formats/CasServerRootFormats.cpp:218-219`; `CA/Formats/CasTextFormat.cpp:315-324`; 26.6.4 `CasServerRootFormats.cpp:165`, `:204`; `T/gtest_cas_server_root_format.cpp:181` |
| F20 | A GC fence decodes the body, sets `gc_fenced`, adds one to `seq` and encodes it again. A field the decoder does not know is lost in that rewrite | `CA/Formats/CasServerRootFormats.h:85-91`; `CasServerRoot.cpp:1365`, `:1403` |
| F21 | The GC leader fences a slot after it watched one token for `L + L / 20 + period` | `CasServerRoot.cpp:1380-1403` |
| F22 | The forced exit: `safeExit` runs when connections, refresh tasks or background queries remain after `shutdown_wait_unfinished` (default 120 s, 60 s in the CI config). `safeExit` calls `_exit` | `programs/server/Server.cpp:3985-4018`; `src/Core/ServerSettings.cpp:126`; `tests/config/config.d/wait_remaining_connections.xml:2`; `base/base/safeExit.cpp:13-38` |
| F23 | Generic code already reaches CAS storages through `Context::getDisksMap` and `ContentAddressedMetadataStorage::tryFromDisk`. `Server.cpp` already includes a CAS header and calls `DB::Cas` functions | `src/Interpreters/Context.cpp:7696-7705`; `CA/ContentAddressedMetadataStorage.cpp:349-364`; `src/Interpreters/ServerAsynchronousMetrics.cpp:396-405`; `Server.cpp:116`, `:1722`, `:2013` |
| F24 | A normal shutdown arms the pool with `beginTeardown` before it stops the GC scheduler and drains | `ContentAddressedMetadataStorage.cpp:954-970`, `:1014-1034` |
| F25 | `forgetDisk` trips the fence, joins the workers, then trips again, because a remount in flight can re-arm the fence before the join | `CasPool.cpp:1254-1277` |
| F26 | The decommission admin withholds its farewell when the owner tombstone is unresolved, so that the victim's restart waits the full threshold | `CA/Tools/CasDecommission.cpp:260`; `CasPool.cpp:1128`; `D/architecture/mounts-and-leases.md` section `mount-lifecycle`, step 2 |
| F27 | After a reclaim nothing waits. The prior state selects a log line through an exhaustive `switch` | `CasPool.cpp:898-920` |

## 3. Part A: mechanism {#part-a}

### 3.1 The marker {#marker}

One new optional field in the body of the mount object (`FormatId::MountLease`, header line unchanged):

```text
"holder_fenced_settle_ms": <positive JSON number>
```

- Struct: `std::optional<uint64_t> MountLease::holder_fenced_settle_ms`.
- Encoder: written only when set, after `write_attempt_id`. A body without it keeps today's bytes.
- Decoder: absent or `0` decodes as unset. Unset means "no marker" everywhere.
- The key does not start with `!`, so no reader treats it as critical (F19).
- Name: `gc_fenced` is a fence by GC, terminal, reclaimed at once. `holder_fenced_settle_ms` is a fence by
  the holder, reclaimed after the settle time.

The rest of a marker body:

| Field | Value | Why |
|---|---|---|
| `server_uuid`, `writer_epoch` | the holder's own pair | The slot still belongs to this incarnation |
| `seq` | last `seq + 1` | As every holder write |
| `write_attempt_id` | fresh | As every holder body (`CasServerRoot.cpp:1981`) |
| `started_at_ms`, `expires_at_ms` | now, now + lease | A reader without the feature computes the declared lease as before and waits it in full |
| `min_active_build_sequence` | what a renewal would stamp now | The GC floor reads it. A build with an attempt in flight stays covered. Never `UINT64_MAX`: that is the farewell |
| `gc_fenced` | `false` | See [§2.2](#rejected) |
| `holder_fenced_settle_ms` | `2 × attemptEnvelopeMs + lease_safety_margin_ms` of the holder, saturating | The holder knows its own attempt timeouts; the successor's may differ |

### 3.2 Holder: order of steps {#holder-steps}

For one pool, on either path:

1. Arm the GC plane: `Pool::beginTeardown`. Forced path only; a teardown has done it or has no users left.
2. Trip the mount fence: `tripFenceWithoutOperationalLoss`.
3. Stop and join both workers: `stopBackgroundWorkers`.
4. Trip the mount fence again. A remount that was past its last request at step 2 can have re-armed it
   (F25).
5. If the renewer exists and is `Active`: set it `Released`, then send the marker body once on the lease
   plane, conditional on the renewer's last token, with `Retry::once`. Otherwise write nothing.
6. Forced path: return; the server calls `safeExit`.

Why each step:

- Step 1 bounds step 3. A remount in flight sends its requests on the GC plane and may sit in the 131 s
  observation loop (`CasPool.cpp:1574-1586`). After `beginTeardown` its next request is refused and the
  attempt returns. Step 1 also answers what the forced path does with the GC plane: it is armed, new GC
  requests are refused, the GC scheduler is not joined. GC writes are guarded by their own tokens and the GC
  lease, not by the mount lease.
- Step 3 is the farewell's own answer to a renewal in flight (F17, F18). After the join the renewal is
  committed, never sent, or unclear. Only the unclear one leaves the renewer not `Active`, and then nothing
  is written.
- Step 5 sets `Released` before the request, as `release` does. No later renewal or farewell can go out
  through this renewer, whatever the marker's outcome: `renewOn` and `release` both require `Active`
  (`CasServerRoot.cpp:1970-1975`, `:2247-2248`).
- The marker write needs no lease room. It is conditional on the token and its statement is about the local
  fence. So it uses `Retry::once`, not the farewell's lease-bounded policy.

Time added to the stop, per pool:

| State | Join | Marker |
|---|---|---|
| Store healthy, renewal idle | immediate | one `PUT` |
| Renewal attempt in flight | at most one envelope | one `PUT` |
| Store unreachable | at most one envelope | at most two envelopes (attempt and its settling read) |
| Remount in flight | at most one poll sleep plus one envelope | as above, through the renewer the remount installed, if any |

### 3.3 Who writes it {#writers}

**Pool teardown.** `CasMountRuntime::finishTeardown` gets a third outcome. Today it has two: farewell, or
nothing.

| State at teardown | Today | New |
|---|---|---|
| Lanes drained, renewer `Active`, lease has room | farewell | farewell, unchanged |
| Lanes not drained (wedged or still writing), or writer cleanup duties pending; renewer `Active` | nothing | steps 2, 4, 5: marker |
| Lanes drained, renewer `Active`, writes paused ("too little time left for the farewell") | nothing | nothing, unchanged. A marker would be safe there, because it needs no lease room, but the branch is left as it is |
| Renewer not `Active` (a sent renewal is unclear, or the renewer is terminal) | nothing | nothing |
| Farewell withheld by a decommission admin (F26) | nothing | nothing. The admin wants the victim's restart delayed by the full threshold |
| `Pool` opened for decommission | farewell or nothing | never a marker |

Both callers of `finishTeardown` get this: `Pool::~Pool` (`CasPool.cpp:1133-1138`) and `forgetDisk`
(`CasPool.cpp:1288`). `Pool` passes the decision; the runtime does not read `farewell_withheld`.

**Forced server exit.** The smallest additive hook:

```text
programs/server/Server.cpp, before safeExit at :4018
  -> DB::Cas::declareMountFencesBeforeForcedExit(global_context)      new free function, noexcept
     -> Context::getDisksMap                                           existing
     -> ContentAddressedMetadataStorage::tryFromDisk                   existing
     -> ContentAddressedMetadataStorage::declareMountFenceBeforeExit   new; takes the pool under pointer_mutex
        -> Pool::declareMountFenceBeforeExit                           new; steps 1 to 5
           -> CasMountRuntime::declareHolderFenced                     new
              -> MountLeaseRenewer::declareHolderFenced                new; the marker write
```

- No new `IDisk` or `IMetadataStorage` method. The chain uses the accessors that
  `ServerAsynchronousMetrics.cpp:396-405` uses on every tick.
- Generic files touched: `programs/server/Server.cpp` only, one include and one call. The free function
  lives in a new file under `CA/`.
- Read-only disks, disks still being created, and decommission admin pools are skipped. They get the full
  wait.
- Order across pools: steps 1 and 2 for every pool first, then steps 3 to 5 pool by pool. One deadline for
  the whole hook: 16 s from its start. A pool whose turn comes after the
  deadline is skipped with one `Warning`. Every skipped pool gets the full wait.
- The function catches everything. An exception from one pool is logged and does not stop the exit.
- Not covered: the second `SIGTERM` (`src/Common/SignalHandlers.cpp:501`), `SYSTEM KILL`
  (`src/Interpreters/InterpreterSystemQuery.cpp:432`), `SIGKILL`, an OOM kill, a fatal signal. No code may
  run there. The two other `safeExit` sites of `Server.cpp` (`:1894`, `:3671`) are not hooked: at `:1894`
  the storages are already shut down; `:3671` was not traced.

### 3.4 Successor: threshold {#threshold}

In `claimMountAwaitingExpiry`, where an observation starts (`CasServerRoot.cpp:1289`):

```text
full   = mountObservationThresholdMs(ttl_ms, declaredMountLeaseMs(body), poll)
marked = mountObservationThresholdMs(0, *body.holder_fenced_settle_ms, poll)     = settle + settle/20 + poll
threshold_ms = body.holder_fenced_settle_ms ? min(marked, full) : full
```

- Same function, same shape as the lease threshold: the bound, 5 % for clock rate, one poll step. The
  saturating arithmetic is reused.
- `min` with `full`: a marker never makes the wait longer than without it.
- Defaults: S3 `16000 + 800 + 5000 = 21800 ms`; local object storage `12000 + 600 + 5000 = 17600 ms`. On
  the 5 s poll grid: 25 s and 20 s.
- `claimMount` does not change its decision: the reclaim is still "this exact token was watched for the
  threshold" (`CasServerRoot.cpp:1085-1087`). It classifies the prior state as `HolderFenced` when the
  reclaimed body carries the field.
- `gc_fenced` and the farewell are tested first, as today. A body that is `gc_fenced` and carries the field
  is reclaimed at once.
- A marked body of another uuid is not reclaimable (F4).
- The token can change during the short wait: a GC fence (then the reclaim is instant), or another start of
  the same uuid. The loop restarts its observation as today and counts the restart
  (`CasServerRoot.cpp:1279-1285`).
- A successor that was already watching the previous token when the marker landed sees a new token, counts
  one restart of the allowed 3, and continues with the short threshold.

### 3.5 What the bound covers {#bound}

Order of events. `E` is the holder, `E+1` the successor.

1. `t_trip`: `lost` is set (F6).
2. `t_send > t_trip`: the marker is sent, by the same thread after the join.
3. `t_land >= t_send`: the marker is in the slot.
4. `t_seen >= t_land`: `E+1` reads the marker body for the first time and starts its clock.
5. `E+1` reclaims no earlier than `t_seen + threshold` on its own clock, which is at least `settle` on the
   holder's clock under the 5 % rate bound of `M/CaMountObserveCore_RESULTS.md`.

Claim 1: **no new request of `E` starts on the mount plane after `t_trip`.** Every verb asks the fence before
each request, reissue and pause (F13), the fence refuses on `lost` first (F5), and `lost` is never cleared
for this pool ([§3.7](#no-rearm)).

Claim 2: **one bound covers what was in flight.** A request that passed its fence check before `t_trip` is
sent `g` later and ends at the client within one envelope. `g` is the gap between the check and the wire.
With `settle = 2 × envelope + margin`, every such request has ended at the client before the reclaim when
`g <= envelope + margin` (9 s by default).

- On the forced path the process exits right after the marker. No thread survives to send anything, so `g`
  ends at the exit.
- In `Pool::~Pool` no caller thread exists: the pool has no owner left.
- A thread stalled between the check and the wire for longer than that, in a process that survives, is the
  one residual. The full wait has the same residual with a different number (inventory, section 3.3).

What is in flight at `t_trip`, and what happens to it:

| In flight | Mutates the store | After the trip |
|---|---|---|
| A write attempt: create, replace, the write of a read-modify-write | Yes | Ends within one envelope. No reissue: the pause and the next attempt are refused (F13) |
| The read that settles an unclear attempt | No | Not sent (F14). The call ends as `GaveUp`, the caller is told nothing is acknowledged |
| The recheck after a proven commit | No request | Refused: the call reports `FenceLost`, not `Committed` (`CasRequests.cpp:868-884`) |
| A token-exact delete | Yes | Ends within one envelope, no reissue (`CasRequests.h:526-536`) |
| A read, head, list, stream open | No | Ends within one envelope. The next buffer of a stream body is refused (`CasRequests.cpp:407-416`) |
| A pause before a reissue | No | Sleeps out, then the check at the top of the loop refuses |
| A multi-step operation: precommit then promote; `HEAD` then put of a namespace file; catalog read then write | Each step is its own admission | The step in flight ends as above. The next step is refused |
| A blob publication admitted before the trip | Yes, a content-keyed body | Not bounded by the envelope: the transfer runs under the disk's `request_timeout_ms` (`CasRequests.h:352-353`). Harmless: debris or a leak. It ends with the process |
| An S3 staging upload | Yes, a key private to the epoch | Not gated by the mount fence; it may even start after the trip. Debris; a mount sweep of a later epoch deletes it (`CasServerRoot.cpp:2257-2292`) |
| A lease-plane renewal | Conditional on the token | Joined before the marker (step 3). None after: the renewer is `Released` |
| A GC-plane request | Guarded by the GC lease and tokens | The next one is refused after `beginTeardown`. Independent of the mount lease |

The write kinds R1 to R4 of the inventory. Each needs new requests of `E` after `E+1` claimed:

| Kind | Needs after the claim | With the marker |
|---|---|---|
| R1: commit into a namespace with no catalog row | At least four new requests: create the row, birth and precommit, read the manifest, promote | All refused. A promote that was in flight lands before the claim and is history of `E` |
| R2: commit a part cloned by evidence from another namespace | New precommit and promote, after `E+1` dropped the source and GC deleted its blobs | Refused |
| R3: put or delete of a namespace file or mountpoint object | A `HEAD` and a conditional write | The pair cannot start. A write in flight ends before the claim, or is the late form that exists today |
| R4: catalog `Live -> Removing` | A read and a conditional write | The same as R3 |

The plane of each call site is taken from the inventory (its section 1.2), not traced again. Nothing found
there or in the engine starts a mount-plane mutation without the fence check. Had one been found, it would
block this design.

### 3.6 Failure, ambiguity, process death {#cases}

| Case | Slot afterwards | Successor waits |
|---|---|---|
| The process dies before the trip, or between the trip and the marker send | Last renewal body | Full |
| The renewer is not `Active` after the join | Last renewal body, or an unclear renewal | Full |
| The marker write is refused by the store | Unchanged | Full |
| The marker write meets a changed token: a GC fence, a successor, or a renewal that had landed | The other body | Instant for `gc_fenced`; a successor is already there; otherwise full |
| The marker write is unclear and did not land | Unchanged | Full |
| The marker write is unclear and landed, at once or later | Marker | Short. Correct: see below |
| The process dies after the marker was sent | Either | As the two rows above |
| The hook's deadline passed before this pool | Last renewal body | Full |

An unclear marker write is not repaired, and it does not need to be. The marker states a fact that was true
before the request was sent: the fence is up and stays up. A request that lands late still states it
truthfully, and the successor counts its wait from the moment it first reads that body. The holder does not
need to know whether it landed, because it writes nothing more in either case (`Released`). So the rule is
not "an unclear write must end in the full wait". It is "a short wait happens only behind a fence that was up
before the marker was sent".

A marker cannot land over a successor: it is conditional on the holder's last token, and every claim changes
the token.

### 3.7 Nothing re-arms the fence {#no-rearm}

The process may live on after the marker: `safeExit` could hang, or the teardown was a `SYSTEM CAS FORGET`
and the pool object stays registered.

| Path that could clear `lost` or write the slot | Closed by |
|---|---|
| `armMountFence` from the remount worker | The worker is joined before the marker. It cannot restart: `startBackgroundWorkers` is called only by a new mount (F8, F9) |
| A remount that re-armed between the first trip and the join | The second trip, after the join (F25) |
| `scheduleRemount` from a foreign-interference report | No-op once `workers_stop_requested` is set (F10) |
| A late renewal commit calling `publishRenewedDeadline` | It moves the deadline only; the fence refuses on `lost` first (F5, F11). The renewal worker is joined before the marker |
| A renewal or a farewell after the marker | The renewer is `Released`: both throw `LOGICAL_ERROR` on a non-`Active` renewer; `finishTeardown` tests `Active` before `release` |
| A pool without background workers (unit tests) | It has no remount worker, so nothing calls `tryRemountOnce` |
| A new `Pool` for the same slot in the same process | It is a successor like any other: a fresh epoch, the marker branch, a conditional claim. The old pool stays fenced |

### 3.8 Other consumers of the slot {#other-consumers}

No consumer treats the marker as terminal.

| Consumer | With a marked slot | Why no change |
|---|---|---|
| GC leader (`computeHeartbeatFloor`) | Counts it `live`, fences it after the full threshold | A fence makes the slot terminal for every server. The settle wait is an observation by one claimer |
| `isCreatorFenceTerminal` (`reconcileStaleCreator`, `cancelStalledCreating`) | Not terminal | It answers from one read with no clock. The marker is a certificate only after the settle time, which needs a watcher. The case ends by itself: about 25 s later the same-uuid successor's epoch is in the slot, which is a certificate |
| Decommission (`NoWait` claim) | Refused as alive | It accepts only a terminal slot |
| Pool recreation (`probeNonTerminalMountSlots`) | Non-terminal, refused | The same |
| Relink confirm (`MountSlotOwnership::holds`) | Answers `Unknown` | No code change. The holder's own slot read goes through the mount plane and is refused after the trip (`CasMountRuntime.cpp:110-116`). `holds` is false for the marker body anyway: its token is never recorded as landed and its id never as in flight (`CasServerRoot.cpp:1677-1687`) |
| GC floor (`buildPrefixEligibleUnderFloor`) | Unchanged | It reads `writer_epoch` and `min_active_build_sequence`; the marker keeps both ([§3.1](#marker)) |
| Rejoin under a new uuid | Unchanged | It needs a terminal body (F4) |

### 3.9 The clean farewell stays instant {#farewell}

Recommendation: no change.

- The farewell is written when the ref lanes drained, no cleanup duty is pending, and the renewer is
  `Active`. In `Pool::~Pool` no caller thread is left, so no mount-plane request is in flight at all.
- A uniform rule "fence, marker, short wait" would be one rule instead of two. Its cost: every clean
  restart waits 25 s (20 s on local object storage) instead of 0 s, on every rolling restart and upgrade.
- What it would buy cannot be stated as a guarantee. The farewell does not certify requests outside the ref
  lanes that ended at the client with an unknown outcome. A 25 s wait makes it less likely that the store
  applies one after the claim; it does not bound it. No wait does (`CasPool.cpp:880-897`).
- So unification trades 25 s per clean restart for a smaller chance of an event that the recovery seal and
  the conditional writes already handle. Not worth it.
- With Part C the cost falls to `25.8 s - d`. [§9.1](#uniform-farewell) evaluates that case; the
  recommendation stays the same.

### 3.10 Names, logs, tables, docs {#surface}

| Surface | Change |
|---|---|
| `MountPriorState` | New value `HolderFenced`: the body carried the field and held for the short threshold. The `switch` at `CasPool.cpp:899-910` puts it with the unclean states: a request of the predecessor may still land |
| Holder log, `CasPool`, Information | "CAS mount lease of '{}' under writer_epoch {}: the write fence is up and the slot says so (settle {} ms); the next mount of this server waits that long instead of the lease". When nothing is written, one line with the reason: renewer not active, token changed, write unclear, deadline passed |
| Successor log, `CasMountLease`, Information | "... the previous incarnation raised its write fence before it stopped; waiting ~{} ms for its requests in flight" in place of the "token-stability observation" line |
| Event `MountRelease` | New branch `holder_fenced`. The branch `self_fenced` exists and means something else: a refused claim rewritten `gc_fenced` (`CasServerRoot.cpp:1142`). On the forced path the row may not reach `system.cas_log`; the text log is the record |
| Event `MountClaim`, branch `reclaim` | New reason text for `HolderFenced` |
| `system.cas_mounts` | `state = 'holder_fenced'` for a body with the field, tested after `fenced` and `terminated`. No new column. Builds without the feature show `live`, then `expired` |
| Profile events | None |
| Settings | None. The bound comes from the holder's request budget |

Doc pages that change:

- `D/architecture/mounts-and-leases.md`: the `MountPriorState` table (`claim-outcomes`), the slot state
  diagram (`mount-state-machines`), the clean unmount and the unclean stop paragraphs (`mount-lifecycle`),
  the states of `system.cas_mounts` (`mounts-table`), the body fields.
- `D/configuration.md`: the paragraph on startup reclaim timing after the settings table; one sentence at
  `cas_attempt_timeout_ms` and `cas_lease_safety_margin_ms` that they also set the wait after a forced exit.
  The text of `cas_unsafe_remount_no_delay` at `:114` is CAS-484 and is not touched here.
- `D/operations/troubleshooting.md`: the row on `SYSTEM CAS DROP POOL MEMBER` refusing a member that is
  down (a `holder_fenced` slot is not terminal).
- `D/operations/migration.md`: one row in the upgrade and downgrade table (`decommission-compatibility`).
- `docs/en/operations/system-tables/cas_mounts.md`: the new `state` value.
- `src/Storages/System/StorageSystemContentAddressedMounts.cpp:51`: the column description.

## 4. Part A: compatibility {#compat}

The format is frozen since 26.6.4. This is one additive optional field. No version bump, no migration.

| Pair | Behaviour |
|---|---|
| Old holder, new successor | No marker is ever written. Full wait, as today |
| New holder, old successor | The old decoder skips the field (F19). The body looks like a renewal, so the old successor waits its own full threshold from the moment it first reads it. Never shorter than today |
| New holder, 26.6.4 successor | The same. 26.6.4 uses its own lease for the threshold, not the declared one (26.6.4 `CasServerRoot.cpp:958`). That is the existing mixed-lease case of `D/configuration.md`; the marker does not make it worse, because a holder that wrote the marker is fenced |
| Old GC leader reads a marked slot | Skips the field, counts the slot live, fences after its full threshold. The fence rewrite drops the field (F20). The result is an ordinary `gc_fenced` body |
| New GC leader reads a marked slot | The same decision. Its fence copy keeps the field next to `gc_fenced`; `gc_fenced` is tested first everywhere |
| Old binary, `system.cas_mounts` | `live`, then `expired` |
| `isCreatorFenceTerminal`, any version | Not terminal |
| GC floor, any version | Reads `writer_epoch` and `min_active_build_sequence`, both as a renewal would have left them |
| Downgrade after a marker was written | The old binary waits the full threshold and reclaims. Its claim body has no field |
| Old decommission tool | Refuses a marked slot as alive until GC fences it |
| A body re-encoded by any reader | No holder re-encodes what it read: renewals, the farewell and the claim build their bodies from local state. Only the fence copy re-encodes, covered above |

The marker lives in exactly one body: the one the holder's marker write produced. Any later write of the slot
by a build without the feature removes it.

## 5. Part A: model {#model}

`M/CaMountObserveCore.tla` has one holder, one observer and one slot. Its `Threshold` is
`TTL + Allowance + Cadence`.

- **Which invariant.** `NoFenceInsideAuthority` and `NoWriteAfterFence`. Both need a new reading. Today
  authority ends at `lastStart + AuthoritySpan`. With the marker it ends at the earlier of that and
  `fenceAt + Settle`. A marked reclaim lands inside the lease authority on purpose, so the invariant as
  written would go red.
- **To add.**
  - Holder variables: `hfenced`, `fenceAt`, and the set of requests in flight with their start ticks.
  - Holder actions: `HolderFence` (sets `hfenced`, records `fenceAt`); `MarkerLand` (installs a fresh marked
    token, enabled only after `HolderFence`, only on the holder's last token, at any later tick);
    `InFlightEnd` (a request ends at most `Settle` ticks after `fenceAt`).
  - `Write` and `RenewStart` are disabled once `hfenced`.
  - `ObsDecide` uses `Settle + SettleAllowance + Cadence` when the watched token is marked.
  - Invariants: `NoReclaimBeforeSettled` (when a marked reclaim lands, `hclock >= fenceAt + Settle` and no
    request is in flight); `NoWriteStartAfterHolderFence`.
- **Sabotage rows, each must go red.** Marker written before the fence. A remount re-arms after the marker.
  The successor uses its own, smaller settle. The marker is treated as `gc_fenced`.
- **Witness rows.** A marked reclaim lands while the lease authority has not ended. A marker that lands late
  restarts an observation that was counting the full threshold.
- **Margin.** `FenceMarginAtLeast` carries over: the margin is the cadence term, as for the lease.
- `M/CaMountRenewRetryCore.tla` has the sabotage `_sab_late_rearm`. It needs the marker as a terminal
  renewer action and the invariant "no slot write after the marker". Whether `M/CaCasMountCore.tla`
  enumerates prior states was not read.

The model runs before the code.

## 6. Part A: tests {#tests}

`LOGICAL_ERROR` expectations are death tests under sanitizer builds.

New gtests:

| Suite | Test | Asserts |
|---|---|---|
| `CASMountLeaseFormat` | Round trip | A body with the field encodes and decodes to the same value; a body without it has today's bytes |
| `CASMountLeaseFormat` | Zero is unset | `0` decodes as unset |
| `CASMountLeaseFormat` | Old decoder skip | A body with the field, decoded with the field's branch removed, equals the body without it. The existing `UnknownFieldsRemainTolerated` pins a boolean; this pins a number |
| `CASMountObservationThreshold` | Marked branch | `settle + settle / 20 + poll`; saturates; never above the full threshold |
| `CASMountAwaitExpiry` | Marked body is reclaimed after the short threshold | Fake clock: no reclaim one tick before, reclaim at the threshold, prior `HolderFenced` |
| `CASMountAwaitExpiry` | The holder's bound is used | Successor configured with a smaller budget still waits the declared value |
| `CASMountAwaitExpiry` | Control | `PastExpiryStillPaysTheFullObservationThreshold` stays as is: no field, full wait |
| `CASMountAwaitExpiry` | Marker lands during a full observation | One restart counted, then the short threshold |
| `CASClaimMount` | Marked body of another uuid | `ForeignOwner`, no write |
| `CASClaimMount` | Marked and `gc_fenced` | Instant, prior `Fenced` |
| `CASMountLease` | Marker write order | The fence is `lost` before the first byte of the marker request; the renewer is `Released` before it |
| `CASMountLease` | No marker when a renewal is unclear | Renewer `RenewalTerminal` after the join: no request sent |
| `CASMountLease` | Marker write refused, token changed, unclear | No second write in any of the three; an unclear write that landed is in the slot and is reclaimed after the short threshold |
| `CASMountLease` | No write after the marker | A renewal or a `release` on that renewer: death test |
| `CASPoolRemount` | Fence stays down | After the marker: `scheduleRemount` is a no-op; a renewal commit that publishes a deadline does not admit a write; a remount that re-armed before the join is tripped again |
| `CASPoolShutdown` | Forced-exit entry point | Steps 1 to 5 on a pool with a caller blocked in a request: the caller's next attempt is refused, the slot is marked |
| `CASPoolShutdown` | Deadline | A pool reached after the deadline writes nothing |
| `CASDecommission` | Admin pools | A withheld farewell and an admin teardown write no marker |
| `CASFenceTerminal`, `CASMountSlotOwnership`, `CASHeartbeatFloor` | Marked body | Not terminal; not held; counted live and fenced only after the full threshold |

Existing tests that change:

| Test | Today | New |
|---|---|---|
| `CASPoolShutdown.UnresolvedWedgeSkipsFarewell` (`T/gtest_cas_pool.cpp:2525`) | No farewell; the slot is a plain live body | No farewell; the body carries the field |
| `CASMountRuntime.ACleanShutdownDuringAPauseLeavesTheSlotForObservation` (`T/gtest_cas_pool.cpp:4770`) | Slot unchanged | Unchanged: the paused branch writes no marker |
| `CASForget.ForgetCleanFarewellGatedOnDrain` (`T/gtest_cas_forget.cpp:319`) | Wedged arm: no farewell | Wedged arm: no farewell, marker |
| `CASRefWriterStalePrecommitSweep.BoundedBatchesAndInterruptionResumeAcrossMounts` (`T/gtest_cas_ref_writer.cpp:3502`, comment at `:3594`) | Third mount is `UncleanObserved` and "pays ~36.5 s" on a fake clock | Third mount is `HolderFenced`. Stays green on the fake clock (inferred, not run); the comment changes |

Tests that do not change: every test that stops a server with `SIGKILL`
(`tests/integration/test_cas_mount_renewal_retry/test.py:631-663`, `test_cas_shared_pool/test.py:263-299`,
`test_cas_drop_pool_member`, `test_cas_gc_sharded`). No code runs on `SIGKILL`, so they keep the full wait
or their own setting.

One integration test, `test_cas_forced_exit_restart`:

- Config: default 120 s lease, no `cas_unsafe_remount_no_delay`, `shutdown_wait_unfinished = 3`.
- Keep a query running past the wait, or a client connection open (inferred from the CI evidence that open
  connections alone trigger the forced exit), send `SIGTERM`.
- Assert in the old log: `Will shutdown forcefully`, then the holder line of [§3.10](#surface).
- Start. Assert: the successor line with the short wait; no `waiting ~131000 ms`; listening within 45 s;
  `system.cas_log` has a `MountClaim` row with the `HolderFenced` reason; a table written before the stop
  reads back.
- Second half: the same with the marker write blocked by a failpoint. Assert the full wait.

Predictions that would refute the design if they fail:

| # | Prediction | Refuted if |
|---|---|---|
| P1 | A 26.6.4 binary started over a marked slot logs the token-stability wait with its own full threshold | It reclaims at once, or fails to decode the slot |
| P2 | After the trip, a thread blocked in a mount-plane write returns `FenceLost` without a second request on the wire | The backend sees a reissue or a settling read |
| P3 | With the marker write unclear and landed late, the successor's observation restarts once and reclaims after the short threshold | It reclaims without a restart, or the holder sends another slot write |

## 7. Part B: a local record of slot write attempts {#part-b}

Separable from Part A. No on-store change. Nothing of the shelved lock or socket design is used.

### 7.1 The rule {#b-rule}

- **Local file**, one per CAS disk, under the server's `path`, for example
  `<path>/disks/<disk name>/cas_mount_attempts`. Text, one checksum line:

  ```text
  boot_id <content of /proc/sys/kernel/random/boot_id>
  landed <write_attempt_id of the last body known to be in the slot>
  pending <write_attempt_id of the body being sent, or none>
  short_wait_ms <max(2 × envelope + margin, renew period + envelope + margin)>
  checksum <hash of the lines above>
  ```

- **Writer.** Only a server process: a mount created with a `Context` of application type `SERVER`, writable,
  not a decommission admin. Before each body the renewer sends (the adoption at `CasServerRoot.cpp:1806`,
  each renewal at `:1981`), it rewrites the file with that body's id as `pending`. A failed local write is
  logged and does not stop the renewal: the lease must not depend on the local disk.
- **Reader.** The mount reads the file once, before its own first write. In the wait loop, for a same-uuid
  older-epoch body without the Part A field:
  - file readable and checksum valid, and
  - `boot_id` in the file equals the boot id read now, and
  - the body's `write_attempt_id` equals `landed` or `pending`
  - then `threshold = min(full, short_wait + short_wait / 20 + poll)`; otherwise `full`.
- The claim stays conditional on the observed token. New prior state, for the log only.
- Nothing about the machine goes to the store. Hostname and IP are not used: a StatefulSet pod keeps its
  hostname on another node, and a cloned VM keeps both.
- A changed boot id is no evidence of anything. It is not read as "the same node rebooted": another node
  has another boot id too. The rule falls through to the full threshold, where Part C applies if built.
- `short_wait_ms` covers the requests in flight and gives a healthy twin time to renew once. Defaults:
  `max(16000, 19000) = 19000`, threshold 24950 ms, 25 s on the poll grid.

### 7.2 What a match proves {#b-proof}

| Step | Statement | Holds because |
|---|---|---|
| 1 | The body in the slot was sent by a process that wrote its id into this file | Ids are random UUIDv4 values, fresh per holder body (`CasServerRoot.cpp:541-546`, `:1981`). A GC fence copies the id but sets `gc_fenced`, which is reclaimed at once anyway. A claim body and a farewell carry ids the file never holds |
| 2 | That process ran on this kernel in this boot | The file's `boot_id` equals the current one |
| 3 | That process is dead | It was a server and held `flock` on `<path>/status` for its whole life (`programs/server/Server.cpp:2033`; `src/Common/StatusFile.cpp:65`, `:72-76`). The starting server holds the same lock now. The lock is taken before any disk starts |

Step 3 is the weak one. A match alone does not prove death; it proves a shared local directory. Death comes
from "one live server per `status` file per kernel", and that is a property of the deployment.

Facts checked for step 3:

- `StatusFile` opens with `O_CLOEXEC`, so child processes do not keep the lock (`StatusFile.cpp:65`).
- `clickhouse-disks` loads the same `uuid` file, takes no `status` lock, and can mount the disk writable
  (`programs/disks/DisksApp.cpp:575`, `:598-610`). It can hold the mount lease of the server's uuid. It
  writes no local file under the rule above, so a slot it wrote never matches.
- `clickhouse local` takes a `status` lock at its own path (`programs/local/LocalServer.cpp:1754`) and has
  application type `LOCAL`. It writes no local file.
- A decommission admin writes the victim's slot under the victim's uuid and its own epoch
  (`CasPool.cpp:983-1076`). It writes no local file. The victim's file never holds the admin's ids.
- CAS code does not read the application type today (`ContentAddressedMetadataStorage.cpp:787` tests only
  that a `Context` exists). The rule needs that one new check.

### 7.3 Order of the local write and the store write {#b-order}

The local write completes before the request is sent. No `fsync`: the evidence is valid only inside one
boot, and the page cache outlives the process. A power loss changes the boot id.

| Death of the process | Slot | File | Result |
|---|---|---|---|
| Before the local write | `Y` | `landed Y` | Match. Correct |
| After the local write, before the request lands | `Y` | `landed Y, pending X` | Match on `landed`. Correct |
| After the request landed, before the answer | `X` | `landed Y, pending X` | Match on `pending`. Correct. This is why the local write comes first |
| Between the claim and the renewer's adoption | claim body | the previous incarnation's ids | No match. Full wait |
| During the local write | any | torn | Checksum fails. Full wait |
| Local write failed earlier, renewals went on | a later id | stale ids | No match. Full wait |
| Machine reboot or power loss | any | old boot id or lost | Full wait |
| Boot id unreadable | any | — | No file is written, no shortcut |

Every failure of the local side ends in the full wait.

### 7.4 Scenarios {#b-scenarios}

| Scenario | Result |
|---|---|
| `SIGKILL`, OOM kill, abort; restart in the same container or pod on the same node | Short wait |
| CI hard-restart tests (kill and start inside one container) | Short wait |
| Forced server exit, restart on the same node | Short wait, without Part A |
| Node reboot | Full threshold, although the old process is certainly dead. Part C credits the downtime |
| Pod rescheduled to another node with the same volume, old node partitioned | Full threshold: other boot id. Part C credits the downtime |
| VM cloned from a disk image, started elsewhere | Full wait: other boot id |
| Predecessor alive and healthy | Its renewal changes the token inside the short wait; the observation restarts; after 3 restarts the start is refused as today |
| A tool or an admin holds the slot | No match. Full wait, or refusal while it renews |

### 7.5 What breaks it {#b-holes}

Each hole needs two things at once: the rule below is violated, and the predecessor is alive but does not
renew within the short wait, then writes again inside its lease. A healthy predecessor is always caught.

| # | Rule the deployment must keep | Violation | Today | With Part B |
|---|---|---|---|---|
| A1 | Nobody removes or replaces `<path>/status` while a server runs from that path | An operator deletes `status` to get a second start past "Cannot lock file" | The second start waits the full lease, by which time the first has stopped writing by its own clock | The second start shares the first one's file, matches, and reclaims after 25 s |
| A2 | A data directory is never copied or snapshotted onto the same kernel and started while the original runs | `cp -a`, a volume snapshot or clone mounted on the same node, started with the copied `uuid` inside one renewal period | The same as A1. `D/architecture/mounts-and-leases.md:215-227` calls the copy unsupported and says the second start is refused | The copy carries the file; it matches; reclaim after 25 s |
| A3 | The boot id is unique per running kernel instance | A live VM clone with memory; a sandbox runtime that hands every sandbox the same boot id | A live memory clone is already outside the lease's reach. A constant boot id costs nothing today | With a constant boot id, the "other node" rows of [§7.4](#b-scenarios) become a short wait |
| A4 | `flock` on `<path>/status` excludes every other process on the same kernel | A file system that does not enforce `flock` between two local processes | Not relied on for the lease | Two servers on one path; then as A1 |

- A1 and A2 can be told apart from a real restart only by an identity of the directory itself, such as the
  inode of the lock file. That is the mechanism the shelved design carried. It is not proposed here.
- A3 for gVisor and Kata and A4 for network and FUSE file systems were not verified.
- The window of damage in each hole: the predecessor must be silent for more than about 25 s and start a
  write before `L - write reserve` = 104 s after its last confirmed renewal started.

### 7.6 Verdict {#b-verdict}

- **Sound with conditions.** The conditions are A1 to A4. They hold outside CAS code and the code cannot
  check them.
- **It weakens two cases that are safe today**: A1 and A2 with a stalled predecessor. Today they end in a
  full wait or a refusal; with Part B they can end in two writers.
- **Size:** about 250 lines of product code, all under `CA/`, plus one read of the application type. No
  generic file changes. No on-store change. One file write per renewal per disk.
- **Evidence missing for a default-on decision:** the boot id in the container runtimes in use; `flock` on
  the file systems that hold `path` in production; how often a restart after an OOM kill matters in
  production. Without these the honest answer is "off by default, or not built".

## 8. Part C: credit the slot's age on the store's clock {#part-c}

Separable from Parts A and B. Task CAS-483. No on-store change, no local state, no generic file.

### 8.1 The rule {#c-rule}

The wait loop subtracts from its threshold the time the slot body has already existed, measured by the
store.

```text
LM_slot = Last-Modified of the slot body the loop is watching        (the read the loop already makes)
LM_now  = Last-Modified of the epoch object this mount just wrote     (one extra read, once per mount attempt)
credit  = LM_now - LM_slot - margin        when both stamps are known and the result is positive; else 0
credit  = 0                                when a cross-check fails (see 8.5)
remaining = threshold - min(credit, threshold)
```

- `threshold` is whatever applies to the body: the full lease threshold, the Part A marker threshold, or the
  Part B threshold. The credit does not depend on which.
- The loop starts its own clock at the first sighting, as today, and reclaims when `remaining` has passed on
  that clock with the token unchanged.
- The claim stays conditional on the token.
- The loop keeps its timing: it sleeps a whole `poll` between looks. So the wait is the first multiple of
  `poll` at or above `remaining`, and at least one `poll`: a credit of the full threshold still costs 5 s.
  The claim then reads the slot a second time and requires the same token.
- A new token restarts the observation, as today. Its stamp is later than `LM_now`, so its credit is 0.
- The log keeps the existing line with the threshold and adds one line with the credit and the remaining
  wait. A test that matches the existing line stays green
  (`tests/integration/test_cas_mount_renewal_retry/test.py:631`).
- Scope: the same-uuid wait of a starting server and of the in-process remount. The GC leader's fence
  threshold does not change.

Defaults, `margin = 4000 ms`, `d` = store time between the two stamps:

| Threshold | Wait |
|---|---|
| Full lease, 131000 ms | `135 s - d`, rounded up to 5 s, at least 5 s |
| Part A marker, 21800 ms | `25.8 s - d`, rounded up to 5 s, at least 5 s |
| Part B match, 24950 ms | `29 s - d`, rounded up to 5 s, at least 5 s |

`d` starts at the last renewal, not at the stop. A killed server's last renewal is up to one renewal period
(10 s) older than the kill.

### 8.2 Plumbing {#c-plumbing}

The generic interface already carries the value: `ObjectMetadata::last_modified` with
`is_last_modified_known` (`src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h:135-140`). The CAS
backend receives it on every read and drops it.

| Backend of a writable CAS mount | Where the stamp is today | Resolution | Clock |
|---|---|---|---|
| S3 API, native mode: AWS S3, MinIO, RustFS, GCS through its S3 endpoint | The `GET` the backend already sends returns it: `ReadBufferFromGetObjectResult` fills `last_modified` from the response; `ObjectStorageBackend::readUnder` takes `got.metadata.etag` and ignores the rest. `HEAD` fills it too | 1 s | The store's |
| `local` object storage, emulated mode (tests, single process) | `makeObjectMetadata` fills it from `st_mtime`; the emulated read already asks for the metadata to mint its token | 1 s | The node's wall clock, through the file system |
| In-memory backend (unit tests) | None. Add a settable stamp | test-defined | test-defined |
| Azure, HDFS | Not a writable CAS backend: native mode needs `supportsRetryProfile(SingleAttempt)`, which only `S3ObjectStorage` provides. Azure's small-object read returns metadata as well, should that change | — | — |

Where: `src/IO/S3/ReadBufferFromGetObjectResult.cpp:21`;
`src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:407-427`, `:972`, `S3ObjectStorage.h:200`;
`CA/Backend/CasObjectStorageBackend.cpp:626-643` (native read), `:561-565` and `:657-676` (emulated read),
`:203-218` (the capability check);
`src/Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.cpp:87-94`;
`src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h:562`;
`src/Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.cpp:389-412`.

Changes, all under `CA/`:

1. `Backend::Raw` (`CA/Backend/CasBackend.h:159`) and `Object` (`CA/Backend/CasWriteResult.h:28`) get
   `std::optional<uint64_t> store_modified_ms`. Unset when the storage does not know it or reports 0.
2. `ObjectStorageBackend::readUnder` fills it in both modes. `CasOperation::readUnder` passes it on
   (`CA/Backend/CasRequests.cpp:537-545`). The decorating backends forward `Raw` unchanged.
3. `MountClaimResult` carries the stamp of the body it reports, next to `etag`
   (`CasServerRoot.cpp:1015`, `:1119`, `:1264-1276`).
4. A helper reads the epoch object after `allocateWriterEpoch` and returns its stamp. Four call sites
   allocate an epoch: `CasPool.cpp:689`, `:810`, `:872`, `:1559`.
5. `claimMountAwaitingExpiry` takes that stamp and applies the rule at `CasServerRoot.cpp:1289`.

No request changes. One request is added: the read of the epoch object.

### 8.3 "Now" on the store's clock {#c-now}

- **The mount already writes before it looks at the slot.** Every writable open and every in-process remount
  allocates its `writer_epoch` by a conditional write of the `epoch` object, with a fresh nonce, before the
  mount claim (`CasServerRoot.cpp:817-900`; `CasPool.cpp:688-690` before `:755-788`; `:1559` before
  `:1574`).
- **A `PUT` response does not return `Last-Modified`.** So the stamp of that write costs one read of the
  `epoch` object: one `GET` of a 100-byte object per mount attempt.
- **Why this is "now".** The store stamped that object when the write reached it, which is before this
  process read it back. So `LM_now` is never later than the moment the loop first reads the slot. An earlier
  "now" only makes the credit smaller.
- **It need not be our own write.** If another process rewrote the `epoch` object in between, its stamp is
  later than ours and still earlier than our read. The argument does not change.
- **Order matters.** `LM_now` must be taken before the loop starts its own clock. Taken after, the time
  between the first sighting and the stamp would be counted twice. The epoch allocation precedes the loop on
  every path, and the stamp is taken once per mount attempt, not per observation.
- **Rejected: the `Date` response header.** `ObjectMetadata` has no field for it and the S3 read path does
  not surface it. It would need changes in generic S3 code. The mount's own write gives the same information
  from the interface that exists.

### 8.4 Safety argument {#c-safety}

1. A holder counts its authority from the start of the renewal attempt, before any I/O:
   `attempt_start_boot_ms` is sampled at the top of `renewOn` and the deadline is that plus the lease
   (`CasServerRoot.cpp:1977-1979`, `:2077-2081`; `CasMountRuntime.cpp:603-609`). The first arm does the same
   (`CasServerRoot.cpp:1805`, `:1816-1820`; `CasPool.cpp:966-971`). 26.6.4 does the same
   (26.6.4 `CasServerRoot.cpp:1447-1461`, `CasMountRuntime.cpp:485-488`).
2. A logical renewal that retries through an outage keeps one body and one start. Whenever it lands, the
   authority it gives ends one lease after that start.
3. The store stamps a body no earlier than the request reached it, which is after the holder sent it, which
   is after that start. So the store-side age of the body in the slot is at most the time the holder itself
   has counted against that body. A slot body is one small `PUT`, never a multipart upload.
4. So a wait of `threshold - age` after the first sighting ends no earlier than a wait of `threshold` that
   had started when the body reached the store. Today's protocol already allows that second wait: it is an
   observer that happened to read the slot at that moment, such as a GC leader watching continuously.
5. Only durations are compared: one store stamp against another, then the remainder on the claimer's own
   `CLOCK_BOOTTIME`. No wall clock of a ClickHouse node decides anything. The stamped `expires_at_ms` still
   never enters the decision.
6. Rate: the threshold carries `L / 20` for a 5 % difference between the holder's and the observer's clocks.
   The part of the wait measured by the store needs the same bound for the store's clock against the
   holder's.
7. A renewal in flight is handled as today. If it lands before the claim, the token changes and the claim
   fails. If the claim lands first, the renewal is refused and gives no authority.

Step 4 is why holder versions do not matter: the credit never reclaims earlier than a real observer of today
could, less the stamp error.

### 8.5 Assumptions, margin, bad stamps {#c-assumptions}

The unsafe direction is a credit that is too large: `LM_slot` too early or `LM_now` too late.

| # | Assumption about the store | If it fails | Covered by |
|---|---|---|---|
| S1 | A stamp has an error of at most 1 s (resolution, rounding) | Credit too large by up to 2 s | 2000 ms of the margin |
| S2 | Two stamps taken by different nodes of one store disagree by a bounded amount | Credit too large by the disagreement | `cas_lease_safety_margin_ms` (2000 ms) of the margin, then the slack of the threshold, then the cross-checks |
| S3 | The store's clock runs within 5 % of the holder's | The stored part of the wait is short by the excess | `L / 20`, as for two servers |
| S4 | The store's clock does not step forward between the two stamps | Credit too large by the step | Cross-check 2, when this node's wall clock is right |
| S5 | `Last-Modified` of an object version is not set to an earlier time afterwards (a restore that keeps old stamps, a copy of a `local` pool with `cp -p`) | Credit too large | Not covered. A later re-stamp (replication, healing) is the safe direction |

- `margin = 2 × 1000 ms + cas_lease_safety_margin_ms`: 4000 ms by default. No new setting.
- Slack behind the margin, at 120 s and no clock drift: the threshold exceeds the lease by
  `L / 20 + poll` = 11 s, and a holder admits its last request one envelope plus the margin (9 s) before its
  deadline. A disagreement of up to about 13 s still reclaims after the holder's authority ended. Up to
  about 22 s still reclaims after the holder could start its last request. Beyond that, a holder that is
  alive but not renewing can overlap the successor. A healthy holder is never affected: its stamp is at most
  one renewal period old.
- For the Part A marker threshold the slack is `settle / 20 + poll` = 5.8 s plus the spare envelope inside
  the declared bound.

Cross-checks. Each can only remove the credit:

1. `LM_slot + margin < started_at_ms` of the body: the store stamped the body before the holder says it
   started the attempt. One of the two clocks is wrong. No credit.
2. `LM_now > wall clock of this node after the read + margin`: the store stamped our write in our future.
   No credit.

These compare a store stamp with a wall clock of a ClickHouse node. That is new, and it is allowed only in
this direction. A node with a wrong wall clock loses the credit and waits as today. They are droppable
([§11](#open)).

Bad stamps:

| Stamps | Result |
|---|---|
| Either unknown, or 0 | No credit, full wait |
| `LM_now < LM_slot + margin`: negative or within the margin | No credit, full wait. One `Information` line when negative: the slot was written after our epoch write, or the clocks disagree |
| A cross-check fails | No credit, full wait, one `Warning` with both values |
| Very large age | Valid: a server may have been down for days. The credit is capped at the threshold |

### 8.6 Compatibility {#c-compat}

- Nothing is written differently. No field, no object, no key. Checked: the rule reads two existing objects
  and changes one local comparison.
- Any holder version: covered by step 4 of [§8.4](#c-safety). A 26.6.4 holder counts from the attempt start
  too.
- A successor without Part C waits as today.
- Mixed leases: the threshold is still the longer of the own and the declared lease. The credit is
  subtracted from that.
- `cas_unsafe_remount_no_delay`, a `gc_fenced` slot and a farewell are decided before the loop and do not
  change.

### 8.7 Model and tests {#c-model-tests}

Model, `M/CaMountObserveCore.tla`:

- A third clock `sclock` with the same rate coupling. `RenewLand` records `landStamp` from it.
- Observer: `ObsStampNow` records `nowStamp` from `sclock` before `ObsRead`; `ObsDecide` dates the sighting
  `oclock - max(0, nowStamp - landStamp - Margin)`.
- Invariant: `NoFenceInsideAuthority`, unchanged. `FenceMarginAtLeast` with the margin reduced by the stamp
  error.
- Sabotage rows, each must go red: `nowStamp` taken after the sighting (double count); no margin with
  truncated stamps; a store clock that steps beyond the margin (an environment row, like the existing drift
  row).

New gtests:

| Suite | Test | Asserts |
|---|---|---|
| `CASObjectStorageBackend` | Stamp pass-through | Native read returns the storage's `last_modified`; emulated read returns the file time; unknown and 0 are unset |
| `CASMountAwaitExpiry` | Credit shortens the wait | Fake clocks: reclaim at `threshold - credit`, not earlier |
| `CASMountAwaitExpiry` | Full credit claims at the second look, still conditionally | Exactly one `poll` sleep; a token that changes between the two reads restarts the observation with no credit |
| `CASMountAwaitExpiry` | Margin | A credit at or below the margin is 0 |
| `CASMountAwaitExpiry` | Bad stamps | Unknown, zero, negative: full wait |
| `CASMountAwaitExpiry` | Cross-checks | Each one alone removes the credit |
| `CASMountAwaitExpiry` | A renewing holder | Credit at most one period; the start is refused after 3 restarts as today |
| `CASMountAwaitExpiry` | Control | `PastExpiryStillPaysTheFullObservationThreshold` (`T/gtest_cas_mount.cpp:1326`) stays: a stamped `expires_at_ms` in the past, no store stamp, full wait |
| `CASPoolRemount` | Order | The stamp is read after the epoch allocation and before the first slot read, once per attempt |

One integration test on the S3 stand: kill the server, wait longer than the threshold, start; assert the
credit line, one poll step of wait, listening within 15 s. Second half: start at once after the kill; assert
a credit of at most one renewal period and the remaining wait.

Predictions:

| # | Prediction | Refuted if |
|---|---|---|
| P4 | On the S3 stands in use, the `Last-Modified` of a fresh slot body is within 1 s of the body's `started_at_ms` | It is earlier, or off by more than the margin: S1 or S2 does not hold there |
| P5 | With the holder alive and renewing, the credit never exceeds one renewal period plus one envelope | A larger credit is logged |
| P6 | After a kill and 140 s of downtime, the start logs a credit of the full threshold and claims on its second read, one poll step later | It waits longer than one poll step |

### 8.8 Verdict {#c-verdict}

- **Sound with conditions.** The conditions are S1 to S5: properties of the store's clock and stamps. The
  design checks S2 and S4 partly, against the wall clocks of the two ClickHouse nodes. S5 is not checked.
- **It adds a third clock to the protocol.** Today the wait trusts the rate of two `CLOCK_BOOTTIME` clocks.
  With Part C part of the wait is measured by the store's wall clock.
- **What it cannot do:** shorten a restart that follows the last renewal by less than the threshold. A
  killed server restarted after 30 s still waits about 100 s. That case is Part A or Part B.
- **Size:** about 110 lines of product code, all under `CA/`; about 250 lines of tests.
- **Evidence missing for a default-on decision:** measured stamp disagreement on the MinIO and RustFS
  clusters in use (P4); `Last-Modified` through the GCS S3 endpoint. On AWS S3 the assumptions are not in
  doubt.

## 9. How the three parts compose {#composition}

Each part stands alone and can be dropped alone. In the wait loop, for a same-uuid older-epoch body that is
neither `gc_fenced` nor a farewell:

1. Pick the threshold: the marker's (Part A), else the local match's (Part B), else the full lease
   threshold.
2. Subtract the credit (Part C).

Without A step 1 has no marker branch. Without B it has no local branch. Without C step 2 is absent. No part
reads another part's state.

Target case: a restart of the server on the same node, possibly together with a reboot of the node.

Waits at default settings on S3. `d` is the store time from the last slot write to the successor's epoch
write. "A+C" and "B+C" are the part plus the credit. Every wait is on the 5 s poll grid.

| Case | Today | A | B | C | Best combination |
|---|---|---|---|---|---|
| Clean stop, any restart | 0 s | 0 s | 0 s | 0 s | 0 s |
| Controlled stop with unfinished queries (forced exit), restart in place, `d` about 5 s | 135 s | 25 s | 25 s | 130 s | A: 25 s. The 5 s of credit is lost in the poll grid |
| The same, then a node reboot, `d` above 26 s | 135 s | 25 s | 135 s | `135 s - d` | A+C: 5 s |
| `SIGKILL` or OOM kill, restart at once on the same running kernel, `d` up to 10 s | 135 s | 135 s | 25 s | 125 s to 135 s | B+C: 20 s to 30 s |
| The same, restarted by `systemd` after `RestartSec=30` (`packages/clickhouse-server.service:19`), `d` 30 s to 40 s | 135 s | 135 s | 25 s | 95 s to 105 s | B+C: 5 s |
| Power loss or kernel failure, node back after 90 s | 135 s | 135 s | 135 s: the boot id changed | 35 s to 45 s | C |
| Any downtime above 135 s | 135 s | 135 s | 135 s | 5 s | C |
| Pod moved to another node, old node partitioned; eviction takes minutes | 135 s | 135 s | 135 s | 5 s | C |
| VM cloned from a disk image, original alive and renewing | refused | refused | refused: other boot id | refused: the stamp is at most 10 s old | — |

- **The moved pod is safe under C** because the credit is the old holder's own lease running out. It
  counted from the start of its last renewal; the store stamped that renewal later. If the old node can
  still reach the store, it renews, the stamp is fresh and the credit is small.
- **A changed boot id is not "the same node rebooted".** Another node has another boot id too. Part B treats
  a changed boot id as no evidence and falls through to the full threshold, where Part C applies. Hostname
  and IP are not identities and are used nowhere.
- **For the target case:** a clean stop is already instant. A forced exit needs A. A kill with an immediate
  restart needs B. Everything with a reboot or a long downtime needs C, and C is the only part that helps
  across nodes without a controlled stop.

### 9.1 The clean farewell under a uniform rule, with Part C {#uniform-farewell}

The uniform rule: every controlled stop trips the fence and declares the settle bound; the successor waits
it, less the credit.

- **The farewell body cannot be replaced by the marker.** It is a terminal certificate for other consumers:
  decommission, rejoin, pool recreation, creator takeover, and it releases the GC floor with
  `min_active_build_sequence = UINT64_MAX` (`CasServerRoot.cpp:1024`, `:1373-1378`, `:1472`, `:1570-1571`).
  So the uniform rule can only mean: the farewell body also carries `holder_fenced_settle_ms`, and a
  same-uuid successor waits `25.8 s - d`, on the poll grid, before it reclaims a farewell.
- **Cost.** A clean stop followed by a node reboot, a pod reschedule or an image pull: `d` exceeds 26 s,
  the wait is one poll step, 5 s. A clean restart in place (`systemctl restart`, a config change, a package
  upgrade): `d` is a few seconds, the wait is 20 s or 25 s. Both are 0 s today.
- **Gain.** Today a clean stop gives requests outside the ref lanes no time at all: one that ended at the
  client unresolved a second before the stop can land after the successor started. Under the uniform rule
  it gets the same bound as after a forced exit. It is a smaller chance, not a guarantee
  ([§3.9](#farewell)).
- **Recommendation: no.** The restart in place is the most frequent clean restart and the target case. The
  uniform rule makes exactly that case slower by 20 s to 25 s. Even when the restart itself takes longer than
  the bound, the cost is 5 s, not zero. If the late form of R3 and R4 after a clean stop is later judged worth
  it, this is the form to build: a farewell with the field, no other change.

## 10. Size {#size}

| Part | Product code | Tests | Docs | Model |
|---|---|---|---|---|
| A | about 240 lines: format field 15; renewer marker write 45; runtime and teardown decision 40; pool steps 1 to 5 40; storage entry and free function 40; `Server.cpp` 4; threshold, prior state, logs 35; `listMounts` and the `switch` 10; events 10 | about 500 lines of gtests, one integration test | 6 pages, about 80 lines | `CaMountObserveCore` and `CaMountRenewRetryCore`: about 100 lines and 8 configs |
| B | about 250 lines: file codec 90; boot id 15; renewer hook 30; config plumbing and application type 40; threshold, prior state, logs 50; file lifecycle 25 | about 400 lines | 3 pages | A new holder-side record in `CaMountObserveCore` |
| C | about 110 lines: `Raw` and `Object` field 6; backend reads 10; in-memory backend stamp 15; `MountClaimResult` and `claimMount` 8; epoch stamp helper and four call sites 25; credit, cross-checks, logs in the loop 45 | about 250 lines of gtests, one integration test | 2 pages: `D/architecture/mounts-and-leases.md`, `D/configuration.md` | A third clock in `CaMountObserveCore`: about 60 lines and 4 configs |

Estimates from reading the code, not from a prototype.

## 11. Open questions {#open}

Most important first.

1. **Part A does not reach `SIGKILL`.** CAS-480 (regression hard restart) and
   `test_shutdown_waits_for_backup` kill with `SIGKILL` before the server's own forced exit, which waits
   `shutdown_wait_unfinished` (120 s by default, the helper kills after 60 s). Part A closes CAS-355 only.
   The test configs already skip the wait with `cas_unsafe_remount_no_delay`. Is the production restart
   after an OOM kill worth Part B and its rules A1 to A4? Part C alone leaves that case at `135 s - d`:
   about 100 s after the 30 s `RestartSec` of the packaged unit.
2. **Decided: the forced-exit hook has one overall deadline of 16 s.** The hook joins two threads and
   sends one request per pool. Healthy store: tens of milliseconds. Unreachable store: up to one envelope
   for a join, then the deadline. A supervisor that sends `SIGKILL` meanwhile only loses the marker.
3. **Decided: two envelopes in the declared bound.** One mutating request in flight needs one envelope. The
   second envelope and the margin cover the gap between the fence check and the wire. One envelope would
   give `9000 + 450 + 5000 = 14450 ms`, 15 s on the grid, and a gap budget of 2 s.
4. **Decided: the `+ poll` term stays.** It is the margin of the lease threshold against clock drift. One
   function, one model shape. Dropping it would save 5 s.
5. **Decided: the poll grid stays.** The loop sleeps whole polls, so 21.8 s becomes 25 s and 131 s becomes
   135 s. No part changes the loop's timing. For Part C this means a floor of 5 s, see item 15.
6. **Decided: the paused-writes branch is unchanged.** No marker where the farewell has no lease room. The
   slot is left for observation, as the existing test pins.
7. **Decided: no marker after an unclear renewal.** With a renewal sent and unresolved, no marker is
   written, by the farewell's rule. The store's conditional write would keep a marker safe there too: both
   name the same token, so only one can land. It would need the renewer to write from a non-`Active`
   state, and that state means the store is failing.
8. **Decided: `system.cas_mounts` shows `state = 'holder_fenced'`.** A new value in a user-visible column.
9. **Decided: `Server.cpp:3671` is not hooked.** The exit of the introspection servers keeps the full wait.
10. **Part C: trust in the store's clock.** Part C makes part of the wait depend on a third clock. On by
    default for every S3-compatible store, or only after P4 of [§8.7](#c-model-tests) was measured on the
    self-hosted clusters in use?
11. **Part C: the margin.** `2 s + cas_lease_safety_margin_ms` = 4 s, with about 11 s more slack in the
    threshold. A cluster with worse clock agreement needs a setting to raise it or to turn the credit off.
    None is proposed; one disk setting would be about 10 lines.
12. **Part C: the two cross-checks.** They remove the credit when a store stamp contradicts a ClickHouse
    wall clock. They cost 15 lines and can cost a full wait on a node whose clock is wrong after boot.
    Recommended: keep them.
13. **Part C on `local` object storage.** The "store clock" there is the node's wall clock, which can be
    stepped. The emulated mode is single-process by contract, so a second live holder is already excluded.
    Allow the credit there, as proposed, or restrict it to native mode?
14. **Part C for the GC leader.** The fence threshold of `computeHeartbeatFloor` could take the same credit
    and fence a dead member sooner. Not included.
15. **Part C: the 5 s floor.** With the poll grid kept, a full credit still waits one poll step, and every
    wait rounds up to 5 s. Claiming at once when nothing remains, and sleeping only the remainder, would
    make the target case about 0 s. It changes the loop's timing for the lease case too (135 s becomes
    131 s), so it is not included.

## 12. Limits of what is established {#limits}

- **Nothing was built or run.** No test, no TLC run. P1 to P3 in [§6](#tests) are predictions.
- **Plane assignment.** That every durable mutation of a mount goes through the mount plane is taken from
  the inventory's section 1.2. This document re-read the engine's fence checks, the fence, the renewer, the
  teardown and the claim, not every call site.
- **The envelope at the client.** That one attempt ends within `attempt_timeout_ms + 2 × connect cap` rests
  on the single-attempt client's socket timeouts (`CasRequestBudget.h:19-22`). Not traced into the client.
- **Late landing.** A request the store applies after the client gave up is not bounded, with or without
  this design (`CasPool.cpp:880-897`).
- **The gap between the fence check and the wire** is not bounded by code. On the forced path the exit ends
  it. In a process that survives it is covered up to `envelope + margin`.
- **26.6.4** was read at one tag: the decoder, the tolerant mode and the claim conditions. Builds between
  that tag and this tree were not read (inferred to be the same).
- **`Server.cpp`**: the exit at `:4018` was read. `:3671` and `:1894` were located, not traced. Whether the
  log sink flushes the hook's lines before `_exit` is inferred from the existing `Will shutdown forcefully`
  line in CI logs.
- **Integration test trigger**: that an idle client connection alone forces the exit is inferred from CI
  logs.
- **Part C**: no stamp was measured on any store. That a `GET` through the GCS S3 endpoint returns
  `Last-Modified` with the semantics assumed here is inferred. That S3-compatible stores stamp a `PUT` no
  earlier than its arrival is taken from the protocol, not tested. Azure and HDFS as CAS backends were
  checked only at the capability gate.
- **Part B**: boot id behaviour of gVisor and Kata, `flock` on network and FUSE file systems, and the
  watchdog fork order were not verified. `clickhouse-disks` mounting a CAS disk writable was read at its
  start-up code only.
- **Scope.** This covers one mount slot, its holder and a same-uuid successor at this commit. It says
  nothing about GC leadership hand-over, about pools already run with `cas_unsafe_remount_no_delay`, or
  about R1 to R4 when the predecessor did not fence itself.
