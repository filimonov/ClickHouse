---
description: 'Design for a shorter mount wait of a CAS disk after a restart on the same node, with or without a node reboot. Part A, on by default: before a forced server exit the holder raises its mount fence, observes that no mutating request of the mount is in flight, and writes a new optional field into its own mount slot; a successor of the same server uuid then waits a bound declared by the holder (about 22 s) instead of the 131 s lease observation threshold. Part C, opt-in: the successor credits the age of the slot body, measured by the object store, against the threshold. Part B, a local record of slot write attempts, was considered and rejected.'
sidebar_label: 'CAS: short mount wait after a self-fence'
sidebar_position: 115
slug: /superpowers/specs/cas-mount-self-fence-short-wait-design
title: 'CAS: shorter mount wait after a restart: holder fence marker and store-clock age'
doc_type: 'reference'
---

# CAS: shorter mount wait after a restart — design {#cas-mount-self-fence-short-wait}

- Date: 2026-11-13. Status: Part A is decided. Part B is rejected. Part C is opt-in. Nothing here is
  implemented, built or run.
- Target case: a restart of the server on the same node, possibly together with a reboot of the node.
- Parts A and C are separable. Each can be reviewed, built and dropped alone ([§9](#composition)).
- Tasks: CAS-355 (forced shutdown leaves the lease), CAS-480 (restart after a kill waits 131 s),
  CAS-482 (local witness, shelved), CAS-483 (slot age on the store's clock: Part C),
  CAS-484 (wrong claim in the text of `cas_unsafe_remount_no_delay`, not fixed here),
  CAS-486 (the attempt envelope is not an end-to-end transport deadline, not fixed here).
- Source tree: worktree `cas-pr1-clean` at `a3967637843`. Every `path:line` below was read at that commit.
  The head `802077e5c1a` adds test configs only.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `D/` = `docs/en/antalya/cas/`. `M/` = `docs/superpowers/models/` in the `master` worktree.
- Input: `tmp/prbundle/wave2/lease-inventory.md` in the `master` worktree, called "the inventory". It lists
  what relies on the mount lease and the write kinds R1 to R4.
- Old format reference: tag `v26.6.4.20001.altinityantalya`, called "26.6.4".
- "Inferred" marks a statement that was not traced in the code. [§12](#limits) lists all of them.

## 1. Decision {#decision}

**What a default build does after this design: Part A only. Part C applies when the operator turns it on.**

**Part A. Before a forced exit the holder proves it has stopped writing and says so in its slot.**

- The server's forced exit (`Will shutdown forcefully`) does three things per CAS pool: it trips the mount
  fence so that it cannot be re-armed, it observes that no mutating request of the mount plane is in flight,
  and it writes one more body into its own mount slot with a new optional JSON field
  `holder_fenced_settle_ms`.
- The certificate is observed, not timed. The field is written only after the count of mutating backend
  calls in flight read zero with the fence up. No assumption about how long a request can take is used.
- A successor with the same `server_uuid` that observes such a body uses the threshold
  `settle + settle / 20 + poll` instead of `L + L / 20 + poll`. The claim stays conditional on the observed
  token. Defaults: 21800 ms against 131000 ms.
- The settle time covers only what the client cannot observe: a request whose call has returned without an
  answer and which the store may still apply.
- If the count does not reach zero, a worker does not stop, or anything else is not done within 16 s, no
  field is written and the successor waits the full lease.
- The field is not `gc_fenced`. A reader that does not know the field skips it and waits the full lease.
- The wait loop sleeps only the time still left, so a wait equals its threshold: 131 s for the lease, not
  135 s.
- The clean farewell stays an instant reclaim. The lease stays 120 s. GC fencing, decommission, creator
  takeover, the relink confirm and `system.cas_mounts` do not change.
- One generic file changes: `programs/server/Server.cpp`, one include and two calls.

**Part B. A local record of slot write attempts: rejected.** A match does not prove that the predecessor is
dead ([§7](#part-b)). A restart after `SIGKILL` or an OOM kill keeps the lease wait.

**Part C. The successor credits the slot's age on the store's clock: opt-in, off by default.**

- Rule: `wait = max(0, T - max(0, d - margin))`. `d` is the `Last-Modified` of the epoch object this mount
  just wrote minus the `Last-Modified` of the slot body. `T` is the threshold in force. Both stamps come
  from the store. No on-store change, no local state, no generic file, one extra read per mount.
- Why it is safe under its contract: a holder counts its lease from the start of the renewal attempt, and
  the store creates the stamp later. The age is at most what the holder itself has counted
  ([§8.4](#c-safety)).
- The contract is on the store's clock: the difference of two `Last-Modified` stamps may exceed the true
  elapsed time by at most 2 s. No store documents such a bound, so the operator accepts it with one disk
  setting.
- It is the only part that helps after a power loss, or a move to another node without a controlled stop.
  It does little for a restart seconds after a kill.

Restart wait before the reclaim, default settings, S3 disk, computed, not measured. `d` is the store time
from the last slot write to the successor's start. The full table is in [§9](#composition).

| Stop of the previous process | Today | A | A + C |
|---|---|---|---|
| Clean stop, farewell written | 0 s | 0 s | 0 s |
| Forced exit, marker written, restart in place (`d` about 5 s) | 135 s | 21.8 s | 20.8 s |
| Forced exit, marker written, then a node reboot | 135 s | 21.8 s | 0 s |
| `SIGKILL` or OOM kill, restart after 30 s | 135 s | 131 s | 95 s to 105 s |
| Power loss, another node, any downtime above 135 s | 135 s | 131 s | 0 s |
| Successor runs a build without these parts | 135 s | 135 s | 135 s |
| Slot fenced by GC before the restart | 0 s | 0 s | 0 s |

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
| A | Sound | A fence nothing re-arms after the last trip, an observed zero of mutating calls after that trip, a marker conditional on the last token ([§3.5](#bound), [§3.7](#no-rearm)). Residual: a request the store applies after the client's call has returned. The full wait has the same residual |
| B | Rejected | Three counterexamples in which the record matches while the predecessor lives ([§7](#part-b)) |
| C | Sound under a contract the operator accepts | One property of the store's stamps that no store documents ([§8.5](#c-assumptions)). Off by default |

## 2. Context {#context}

### 2.1 Problem {#problem}

- A server restarted after an unclean stop waits `L + L / 20 + poll` before it listens: 131 s at the 120 s
  default, 135 s on the 5 s poll grid (`CA/Pool/CasServerRoot.cpp:1209-1300`).
- The forced exit of the server is an unclean stop for the mount: it calls `safeExit` before the disks are
  shut down (`programs/server/Server.cpp:4006-4018`). CI stress run 43379266824 hit it: two CAS disks waited
  131 s against a 110 s start budget (CAS-355).
- On that path the holder runs code before it stops. It can stop writing, check that it has, and say so.

### 2.2 Rejected {#rejected}

| Option | Why not |
|---|---|
| A shorter lease | The lease sets the write window during a store outage: 94 s at 120 s, 4 s at 30 s (commit `b528b234dca`) |
| A separate, shorter write window inside the lease | It restores the 4 s figure. The restart wait cannot be shorter than the time the old holder may keep writing without a renewal |
| A local lock or socket witness | Shelved (CAS-482): too much mechanism for one narrow case |
| Marking the slot `gc_fenced` on exit | Every reader reclaims a `gc_fenced` slot at once (`CasServerRoot.cpp:1087`), builds without this feature included. That is a wait of 0 with requests still in flight |
| A marker certified by time alone ("every admitted request ends within one attempt envelope") | The envelope is not an end-to-end deadline. One conditional write on an ETag store can be a multipart upload whose parts and completion are sent long after the fence check (`CA/Backend/CasObjectStorageBackend.cpp:810-820`). The HTTP client follows redirects and the S3 client repeats requests inside one call (`src/IO/S3/PocoHTTPClient.cpp:557`, `:744`; `src/IO/S3/Client.cpp:749`). Timeouts are per socket operation. CAS-486 |

### 2.3 Code facts {#code-facts}

| # | Fact | Where |
|---|---|---|
| F1 | The wait loop sets its threshold once per observed token, from the observed body: `mountObservationThresholdMs(ttl_ms, declaredMountLeaseMs(*r.body), poll)`. It sleeps a whole `poll` between looks | `CasServerRoot.cpp:1233-1235`, `:1289`, `:1298` |
| F2 | `poll = max(1, cas_mount_renew_period_ms / 2)`: 5 s by default | `CA/Pool/CasPool.cpp:701-702` |
| F3 | A same-uuid older-epoch body is reclaimed when it is `gc_fenced`, a farewell, proven dead by the loop, or authorized by the unsafe knob. Anything else is `LiveDoubleStart` with no write. The same four conditions are in 26.6.4 | `CasServerRoot.cpp:1085-1119`; 26.6.4 `CasServerRoot.cpp:867-869` |
| F4 | A body of another uuid is reclaimed only when it is `gc_fenced` or a farewell | `CasServerRoot.cpp:1022-1032` |
| F5 | The mount fence refuses when `lost` is set or the generation changed, before it looks at the deadline. `lost` is loaded with `memory_order_acquire` | `CA/Pool/CasMountRuntime.cpp:156-170` |
| F6 | `tripFenceWithoutOperationalLoss` sets `lost` with `memory_order_release` and bumps the generation. It changes no lifecycle and schedules no remount | `CasMountRuntime.cpp:131-135` |
| F7 | `lost` is cleared in one place, `armMountFence`. It has two callers: the first arm of a new `Pool`, and `Pool::tryRemountOnce` | `CasMountRuntime.cpp:236`; `CasPool.cpp:966`, `:1679` |
| F8 | `Pool::tryRemountOnce` is reachable only as the `remount_attempt` callback of the remount worker | `CasPool.cpp:271`; `CasMountRuntime.cpp:895` |
| F9 | `stopBackgroundWorkers` sets `workers_stop_requested`, joins both workers with untimed joins, and never clears the flag. Only `startBackgroundWorkers` clears it, and only a new mount calls that | `CasMountRuntime.cpp:936-965`, `:701`; `CasPool.cpp:979-980` |
| F10 | `scheduleRemount` does nothing once `workers_stop_requested` is set | `CasMountRuntime.cpp:1173-1185` |
| F11 | A committed renewal publishes a new deadline. It does not touch `lost` | `CasMountRuntime.cpp:362-363`, `:603-612` |
| F12 | A renewal that ends terminal while the driver is `Stopping` trips the fence without a remount. One that ends terminal while the driver is `WorkerIdle` calls `tripMountLost` and requests a remount | `CasMountRuntime.cpp:371-391` |
| F13 | Every engine verb asks the fence before each request, each reissue and each pause. A write asks once more after a proven commit, with no request | `CA/Backend/CasRequests.cpp:436-449`, `:954-966`, `:886-907`, `:868-884`; `CA/Backend/CasRequests.h:526-596` |
| F14 | The read that settles an unclear write attempt is a gated read. After a trip it is not sent and the write ends as `GaveUp` with `FenceLost` | `CasRequests.cpp:1105-1113`, `:784-811`, `:843-856` |
| F15 | The three planes: mount (the mount fence), lease (open), GC (refuses only after `beginTeardown`) | `CasPool.cpp:191-220`, `:1187-1199` |
| F16 | Attempt envelope = `attempt_timeout_ms + 2 × connect cap`: 7000 ms for S3, 5000 ms without a connect cap. Write reserve = `2 × envelope + lease_safety_margin_ms`: 16000 ms | `CA/Backend/CasRequestBudget.cpp:19-29`, `:105`; `CA/Backend/CasRequestBudget.h:22`, `:30`, `:36` |
| F17 | The farewell: join both workers, drain the ref lanes, then `release` only for an `Active` renewer. `release` sets `Released` first and writes on the lease plane | `CasPool.cpp:1103-1138`; `CasMountRuntime.cpp:1230-1261`; `CasServerRoot.cpp:2245-2255` |
| F18 | A renewal that gives up before any request was sent leaves the renewer `Active` only if `workers_stop_requested` was already set (`cancelled`). The same give-up under a lost fence without a stop request is terminal: the renewer becomes `RenewalTerminal`. A give-up after a request was sent is always terminal | `CasServerRoot.cpp:2123-2161`, `:1895-1919`; `CasMountRuntime.cpp:529-545` |
| F19 | `decodeMountLease` skips unknown keys. A key that starts with `!` is refused. 26.6.4 opens the mount lease with `KeyStrictness::Tolerant`. Both trees have the test `CASMountLeaseFormat.UnknownFieldsRemainTolerated` | `CA/Formats/CasServerRootFormats.cpp:218-219`; `CA/Formats/CasTextFormat.cpp:315-324`; 26.6.4 `CasServerRootFormats.cpp:165`, `:204`; `T/gtest_cas_server_root_format.cpp:181` |
| F20 | Three places copy a decoded slot body and encode it again: the GC fence, the fence of a refused own claim, and the rejoin of a retired owner. A field the decoder does not know is lost there; a field the struct holds is kept | `CA/Formats/CasServerRootFormats.h:85-91`; `CasServerRoot.cpp:1365`, `:1403`; `:1135-1138`; `:658-667` |
| F21 | The GC leader fences a slot after it watched one token for `L + L / 20 + period` | `CasServerRoot.cpp:1380-1403` |
| F22 | Two forced exits run before the disks are shut down: after `shutdown_wait_unfinished` with connections, refresh tasks or background queries left, and from the introspection servers' stop inside `ContextSharedPart::shutdown`. `shutdown_wait_unfinished` is 120 s by default, 60 s in the CI config. Without `shutdown_wait_unfinished_queries` the queries are killed before the wait. `safeExit` calls `_exit` | `programs/server/Server.cpp:3981-4018`, `:3636-3672`; `src/Interpreters/Context.cpp:1006-1019`, `:1097`, `:1172`; `src/Core/ServerSettings.cpp:125-126`; `tests/config/config.d/wait_remaining_connections.xml:2-3`; `base/base/safeExit.cpp:13-38` |
| F23 | Generic code already reaches CAS storages through `Context::getDisksMap` and `ContentAddressedMetadataStorage::tryFromDisk`. `Server.cpp` already includes a CAS header and calls `DB::Cas` functions | `src/Interpreters/Context.cpp:7696-7705`; `CA/ContentAddressedMetadataStorage.cpp:349-364`; `src/Interpreters/ServerAsynchronousMetrics.cpp:396-405`; `Server.cpp:116`, `:1722`, `:2013` |
| F24 | A normal shutdown arms the pool with `beginTeardown` before it stops the GC scheduler and drains | `ContentAddressedMetadataStorage.cpp:954-970`, `:1014-1034` |
| F25 | `forgetDisk` trips the fence, joins the workers, then trips again, because a remount in flight can re-arm the fence before the join. It runs under the storage's `lifecycle_mutex`, as FSCK and GC stop and start do | `CasPool.cpp:1254-1277`; `ContentAddressedMetadataStorage.cpp:1060`, `:1109`; `CA/ContentAddressedMetadataStorage.h:713-724` |
| F26 | Every backend entry point takes a `TransportAccess`, which only `CasRequests::withTransportAccess` can construct. The mutating entry points are called from four engine functions: `writeLoop` (`write`), `removeUnder` (`remove`), `removeManyWriteOnce`, `publish`. No count of calls in flight exists | `CA/Backend/CasTransportAccess.h:7-22`; `CasRequests.h:246-253`; `CasRequests.cpp:941`, `:996`, `:580-585`, `:634-646`, `:775-780` |
| F27 | After a reclaim nothing waits. The prior state selects a log line through an exhaustive `switch` | `CasPool.cpp:898-920` |
| F28 | A blob publication runs its whole transfer inside the backend call: a streamed upload or a same-store copy | `CasObjectStorageBackend.cpp:889-962` |

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
| `min_active_build_sequence` | what a renewal would stamp now | The GC floor reads it. Never `UINT64_MAX`: that is the farewell |
| `gc_fenced` | `false` | See [§2.2](#rejected) |
| `holder_fenced_settle_ms` | `2 × attemptEnvelopeMs + lease_safety_margin_ms` of the holder, saturating | The holder knows its own attempt timeouts; the successor's may differ |

What the settle time is for. The certificate of [§3.5](#bound) already says that no mutating call of the
holder is running and none can start. The settle time covers what the holder cannot see: a request whose
call returned without an answer may still be applied by the store. Twice the attempt envelope plus the
margin is the time such a request is given. It is not a proof that the store has finished: no wait is
([§12](#limits)).

### 3.2 Holder: order of steps {#holder-steps}

For one pool:

0. Take the storage's `lifecycle_mutex` without blocking. If it is held, skip the pool.
1. Arm the GC plane: `Pool::beginTeardown`.
2. Request the workers' stop: set `workers_stop_requested` and the `Stopping` driver state, without a join.
3. Trip the mount fence: `tripFenceWithoutOperationalLoss`.
4. Join both workers: `stopBackgroundWorkers`.
5. Trip the mount fence again.
6. Wait until the count of mutating mount-plane calls in flight is zero ([§3.5](#bound)).
7. If the renewer exists and is `Active`: set it `Released`, then send the marker body once on the lease
   plane, conditional on the renewer's last token, with `Retry::once`. Otherwise write nothing.

Steps 4, 6 and 7 can block. None of them is timed. The caller bounds them from outside
([§3.3](#writers)): after 16 s the server exits whatever step a pool is in.

Why each step:

- Step 0: a `SYSTEM CAS FORGET`, an FSCK or a GC stop or start in progress holds that mutex and may be
  joining the same worker threads (F25). Two joins of one thread object are undefined. A skipped pool gets
  the full wait.
- Step 1 lets step 4 end. A remount in flight sends its requests on the GC plane and may sit in the 131 s
  observation loop (`CasPool.cpp:1574-1586`). After `beginTeardown` its next request is refused. It also
  answers what the forced path does with the GC plane: it is armed, new GC requests are refused, the GC
  scheduler is not joined. GC writes are guarded by their own tokens and the GC lease.
- Step 2 before step 3: a renewal that comes due after the trip gives up under the lost fence. With the stop
  already requested it stays `Active` and step 7 can write. Without it the renewer becomes
  `RenewalTerminal`, a remount is requested, and no marker is written (F18, F12). With several pools the
  window between one pool's trip and its join is long enough for that.
- Step 4 is the farewell's own answer to a renewal in flight (F17). After the join the renewal is committed,
  never sent, or unclear. Only the unclear one leaves the renewer not `Active`, and then nothing is written.
- Step 5: a remount that was past its last GC-plane request at step 3 can have re-armed the fence, under a
  new epoch and a new renewer (F25). After the join it cannot run again. The marker then goes through the
  renewer the remount installed.
- Step 6 comes after the last trip. Only then does a zero mean something.
- Step 7 sets `Released` before the request, as `release` does. No later renewal or farewell can go out
  through this renewer, whatever the marker's outcome: `renewOn` and `release` both require `Active`
  (`CasServerRoot.cpp:1970-1975`, `:2247-2248`).
- The marker write needs no lease room. It is conditional on the token and its statement is about the local
  fence. So it uses `Retry::once`, not the farewell's lease-bounded policy.

`request the workers' stop` is the first, locked half of `stopBackgroundWorkers`. The code has it as
`beginShutdownForTest` (`CasMountRuntime.cpp:1194-1200`); it becomes a production method.

No helper is shared with `forgetDisk`. The common part is three calls, and the two paths differ in each of
them: FORGET trips with `tripMountLost`, does not arm the GC plane and joins without a bound.

### 3.3 Who writes it {#writers}

Only the server's forced exit. The hook:

```text
programs/server/Server.cpp, before safeExit at :4018 and at :3671
  -> DB::Cas::declareMountFencesBeforeForcedExit(global_context)      new free function, noexcept
     -> Context::getDisksMap                                           existing
     -> ContentAddressedMetadataStorage::tryFromDisk                   existing
     one thread per writable CAS pool:
     -> ContentAddressedMetadataStorage::declareMountFenceBeforeExit   new; step 0, then the pool pointer
        -> Pool::declareMountFenceBeforeExit                           new; steps 1 to 7
           -> CasMountRuntime::declareHolderFenced                     new
              -> MountLeaseRenewer::declareHolderFenced                new; the marker write
     the calling thread waits until every pool thread has finished, at most 16 s, then returns
```

- **Hard cap.** The calling thread waits on a completion count with a 16 s timeout and then returns to
  `safeExit`, which ends the process with `_exit`. A pool thread still in a join, in the wait for zero or
  in the marker write is cut off there. Cut off before the marker was sent: no marker, full wait. Cut off
  during the marker write: an unclear marker, which [§3.6](#cases) allows because steps 5 and 6 came first.
- **One thread per pool**, so a pool that blocks does not take the others' markers with it. A pool whose
  thread cannot be started is skipped.
- No new `IDisk` or `IMetadataStorage` method. The chain uses the accessors that
  `ServerAsynchronousMetrics.cpp:396-405` uses on every tick.
- **Generic files touched:** `programs/server/Server.cpp` only: one include and two call lines. The free
  function lives in a new file under `CA/`.
- Both exits are the same case: at `:3671` and at `:4018` every CAS pool is still mounted and renewing
  (F22). At `:3671` the callback runs with no mutex of `ContextSharedPart` held.
- Read-only disks, disks still being created, and decommission admin pools are not reached. They get the
  full wait.
- The function catches everything. An exception from one pool is logged and does not stop the exit.
- Not covered: the second `SIGTERM` (`src/Common/SignalHandlers.cpp:501`), `SYSTEM KILL`
  (`src/Interpreters/InterpreterSystemQuery.cpp:432`), `SIGKILL`, an OOM kill, a fatal signal. No code may
  run there. The exit at `Server.cpp:1894` is not hooked: the storages are already shut down there.
- Not included, possible follow-up: a marker from a pool teardown whose ref lanes did not drain, and from
  `SYSTEM CAS FORGET`. Both would add a third outcome to `finishTeardown` and a decision passed from `Pool`.
  An undrained lane mostly means a failing store, where the marker write fails too. The branch "too little
  time left for the farewell" and an unclear renewal at stop also stay as they are: no marker.

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
- Defaults: S3 `16000 + 800 + 5000 = 21800 ms`; local object storage `12000 + 600 + 5000 = 17600 ms`.
- **The loop sleeps `min(poll, time still left)`**, where the time still left is the threshold minus the
  time already observed. When nothing is left it claims at once. So a wait equals its threshold: 21.8 s
  for a marker, 131 s for the lease, where the whole-poll sleep gives 25 s and 135 s today. The claim is
  unchanged: it reads the slot again and requires the same token.
- `claimMount` does not change. The reclaim is still "this exact token was watched for the threshold"
  (`CasServerRoot.cpp:1085-1087`), and the prior state is the existing `UncleanObserved`.
- `gc_fenced` and the farewell are tested first, as today. A body that is `gc_fenced` and carries the field
  is reclaimed at once.
- A marked body of another uuid is not reclaimable (F4).
- The token can change during the short wait: a GC fence (then the reclaim is instant), or another start of
  the same uuid. The loop restarts its observation as today and counts the restart
  (`CasServerRoot.cpp:1279-1285`).
- A successor that was already watching the previous token when the marker landed sees a new token, counts
  one restart of the allowed 3, and continues with the short threshold.

### 3.5 The certificate: fence up, nothing in flight {#bound}

The marker is sent only when both hold:

- **(a)** the mount fence is `lost` and nothing can clear it: both workers are joined and the fence was
  tripped after the join (steps 4 and 5, [§3.7](#no-rearm));
- **(b)** after (a), the count of mutating mount-plane calls in flight was read as zero (step 6).

**The count.** `CasRequests` gets one atomic counter, `mutations_in_flight`. No such count exists today
(F26). A scope guard increments it at entry and decrements it at exit of three engine functions:

| Function | Backend call inside | Counted |
|---|---|---|
| `CasOperation::writeLoop` (`CasRequests.cpp:941`) | `write`: every create, replace and read-modify-write | Yes |
| `CasOperation::removeUnder` (`CasRequests.cpp:580`) | `remove`: every token-exact delete | Yes |
| `CasOperation::removeManyWriteOnce` (`CasRequests.cpp:634`) | the batch delete; no mount sends one (inventory, section 1.2) | Yes, so the claim does not rest on that list |
| `CasOperation::publish` (`CasRequests.cpp:775`) | `publish`: a blob body | No, see below |
| reads, heads, lists, streams, probes | — | No: they change nothing |

- **Incremented before the fence check.** The guard is the first statement of the function; the first fence
  check comes after it (`CasRequests.cpp:958-959` for a write, `CasRequests.h:528-529` for a delete). No
  thread can be between its fence check and the wire without being counted.
- **Decremented when the function returns or throws.** Everything one backend call does is inside: all
  parts and the completion of a multipart upload, HTTP redirects, the client's own repeats. So are the
  function's own reissues, settling reads and pauses. A pause is at most 5 s (`CA/Backend/CasRetry.cpp:11-17`)
  and the fence check before it refuses after a trip.
- **Ordering.** Thread: increment, then load `lost`. Hook: store `lost`, then load the count. For one of the
  two to always see the other, all four operations must be sequentially consistent. Today `lost` is stored
  with `release` and loaded with `acquire` (F5, F6), which allows both to miss. The store in the two trip
  functions and the load in `admit` change to `memory_order_seq_cst`.
- **The wait for zero** uses the counter's own `wait` and `notify_all`. It is untimed: the hard cap of
  [§3.3](#writers) bounds it. There is no polling sleep.
- **Which plane.** The mount plane's counter is the one read. The lease plane has one user, the renewer,
  which is joined. The GC plane's writes are licensed by the GC lease and their tokens, not by the mount.

**Proof.** Let the last trip be at `t` and the zero be read at `o > t`. Take any counted call.

- Its guard was taken before `o` and released before `o`: the call has returned.
- Its guard was taken before `o` and not released: the count at `o` is not zero. Contradiction.
- Its guard was taken after `o`: its fence check is later still, so after `t`, and `lost` is never cleared
  after `t`. The check refuses and no request is sent.

So at `o` no mutating call of the mount plane is running, and none starts later. A count that briefly rises
again after `o` is a guard on its way to a refusal.

**Writers outside the count**, and why each needs no counting:

| Writer | Path | Why not counted |
|---|---|---|
| Blob publication | `publish`, through the engine; the transfer runs inside the backend call (F28) | The key is the content hash and the write is unconditional. Any pool member may publish any blob at any time; GC decides by in-degree, marker and exact token, not by mount epochs (inventory, part 2, section c). A blob that lands late is debris or a leak. Counting it would hold the marker back for the length of an upload, which is the normal state of a server stopped under load |
| S3 staging upload and its delete | `IObjectStorage`, not the engine (`CA/ContentAddressedTransaction.cpp:949`, `:218`) | The key is private to the epoch (`CA/Formats/CasLayout.h:418-423`). Only the same epoch's publication reads it. A later mount sweeps it |
| The renewal | Lease plane | Joined before the marker. The marker itself is the renewer's last write |
| GC writes of this process | GC plane | Independent of the mount lease. Refused at their next request after `beginTeardown` |

**What the successor's wait covers.** Order of events, `E` the holder, `E+1` the successor:

1. `t`: last trip. `o`: zero observed.
2. `t_send > o`: the marker is sent. `t_land >= t_send`: it is in the slot.
3. `t_seen >= t_land`: `E+1` reads the marker body for the first time and starts its clock.
4. `E+1` reclaims no earlier than `t_seen + threshold` on its own clock, which is at least `settle` on the
   holder's clock under the 5 % rate bound of `M/CaMountObserveCore_RESULTS.md`.

So every mutating call of `E` had returned before the marker was sent, and at least `settle` passes after
that before `E+1` writes. What can still happen in that time, and after it:

| Left over at `o` | After `o` |
|---|---|
| A request whose call returned with a timeout or a transport error | The store may still apply it. The settle time is for this. Not bounded by any wait |
| A multi-step operation: precommit then promote; `HEAD` then put of a namespace file; catalog read then write | The step that was running has returned. The next step is refused (F13) |
| The read that settles an unclear attempt | Not sent (F14). The caller is told nothing is acknowledged |
| A blob publication, a staging upload | May still run. Harmless, see above. Ends with the process |

The write kinds R1 to R4 of the inventory. Each needs new requests of `E` after `E+1` claimed:

| Kind | Needs after the claim | With the marker |
|---|---|---|
| R1: commit into a namespace with no catalog row | At least four new requests: create the row, birth and precommit, read the manifest, promote | All refused |
| R2: commit a part cloned by evidence from another namespace | New precommit and promote, after `E+1` dropped the source and GC deleted its blobs | Refused |
| R3: put or delete of a namespace file or mountpoint object | A `HEAD` and a conditional write | The pair cannot start. A write whose call returned unanswered is the late form that exists today |
| R4: catalog `Live -> Removing` | A read and a conditional write | The same as R3 |

That every durable mutation of a mount goes through the mount plane was traced twice: in the inventory
(section 1.2) and again from the construction sites of `CasRequests` (`CasPool.cpp:191-220`, `:427`,
`:1019`; `CA/Tools/CasDecommission.cpp:496`, `:522`). A mutation that reached the store without the engine
and without a private key would block this design. None was found.

### 3.6 Failure, ambiguity, process death {#cases}

| Case | Slot afterwards | Successor waits |
|---|---|---|
| The process dies, or the 16 s cap ends it, before the marker is sent | Last renewal body | Full |
| The pool's lifecycle mutex is held | Last renewal body | Full |
| A worker does not stop, or the count does not reach zero, before the cap | Last renewal body | Full |
| The renewer is not `Active` after the join | Last renewal body, or an unclear renewal | Full |
| The marker write is refused by the store | Unchanged | Full |
| The marker write meets a changed token: a GC fence, a successor, or a renewal that had landed | The other body | Instant for `gc_fenced`; a successor is already there; otherwise full |
| The marker write is unclear and did not land | Unchanged | Full |
| The marker write is unclear and landed, at once or later | Marker | Short. Correct: see below |
| The process dies, or the cap ends it, after the marker was sent | Either | As the two rows above |

An unclear marker write is not repaired, and it does not need to be. The marker states a fact that was true
before the request was sent: the fence is up, stays up, and nothing was in flight. A request that lands late
still states it truthfully, and the successor counts its wait from the moment it first reads that body. The
holder does not need to know whether it landed, because it writes nothing more in either case (`Released`).
So the rule is not "an unclear write must end in the full wait". It is "a short wait happens only behind a
certificate that held before the marker was sent".

A marker cannot land over a successor: it is conditional on the holder's last token, and every claim changes
the token.

### 3.7 Nothing re-arms the fence {#no-rearm}

Between the marker and `_exit` the process lives on: other pools' threads run for up to the 16 s cap, and
the query threads that forced the exit are still there.

| Path that could clear `lost` or write the slot | Closed by |
|---|---|
| `armMountFence` from the remount worker | The worker is joined before the marker. It cannot restart: `startBackgroundWorkers` is called only by a new mount (F8, F9) |
| A remount that re-armed between the first trip and the join | The second trip, after the join (F25). The count is read after that trip |
| `scheduleRemount` from a foreign-interference report | No-op once `workers_stop_requested` is set (F10) |
| A late renewal commit calling `publishRenewedDeadline` | It moves the deadline only; the fence refuses on `lost` first (F5, F11). The renewal worker is joined before the marker |
| A renewal or a farewell after the marker | The renewer is `Released`: both throw `LOGICAL_ERROR` on a non-`Active` renewer |
| A lifecycle command on the same pool | Excluded by the lifecycle mutex the pool's thread holds until the process ends |
| A new `Pool` for the same slot in the same process | It is a successor like any other: a fresh epoch, the marker branch, a conditional claim. The old pool stays fenced |

### 3.8 Other consumers of the slot {#other-consumers}

No consumer treats the marker as terminal.

| Consumer | With a marked slot | Why no change |
|---|---|---|
| GC leader (`computeHeartbeatFloor`) | Counts it `live`, fences it after the full threshold | A fence makes the slot terminal for every server. The settle wait is an observation by one claimer |
| `isCreatorFenceTerminal` (`reconcileStaleCreator`, `cancelStalledCreating`) | Not terminal | It answers from one read with no clock. The marker is a certificate only after the settle time, which needs a watcher. The case ends by itself: about 22 s later the same-uuid successor's epoch is in the slot, which is a certificate |
| Decommission (`NoWait` claim) | Refused as alive | It accepts only a terminal slot |
| Pool recreation (`probeNonTerminalMountSlots`) | Non-terminal, refused | The same |
| Relink confirm (`MountSlotOwnership::holds`) | Answers `Unknown` | No code change. The holder's own slot read goes through the mount plane and is refused after the trip (`CasMountRuntime.cpp:110-116`). `holds` is false for the marker body anyway: its token is never recorded as landed and its id never as in flight (`CasServerRoot.cpp:1677-1687`) |
| GC floor (`buildPrefixEligibleUnderFloor`) | Unchanged | It reads `writer_epoch` and `min_active_build_sequence`; the marker keeps both ([§3.1](#marker)) |
| Rejoin under a new uuid | Unchanged | It needs a terminal body (F4) |
| `system.cas_mounts` | `live`, then `expired` | The table has no new state. The text log names the marker |

### 3.9 The clean farewell stays instant {#farewell}

Recommendation: no change.

- The farewell is written when the ref lanes drained, no cleanup duty is pending, and the renewer is
  `Active`. In `Pool::~Pool` no caller thread is left, so no mount-plane request is in flight at all.
- The farewell cannot be replaced by the marker. It is a terminal certificate for other consumers:
  decommission, rejoin, pool recreation, creator takeover, and it releases the GC floor with
  `min_active_build_sequence = UINT64_MAX` (`CasServerRoot.cpp:1024`, `:1373-1378`, `:1472`, `:1570-1571`).
- A uniform rule "fence, marker, short wait" can therefore only mean a farewell that also carries the
  field, with a same-uuid successor waiting the marker threshold before it reclaims it. Its cost: a clean
  restart in place waits 21.8 s instead of 0 s. With Part C on: `21.8 s` less the credit, which is about
  21 s for a restart in place and 0 s only when the restart itself takes longer than 25.8 s.
- What it would buy is not a guarantee. A request outside the ref lanes whose call returned unanswered a
  second before a clean stop can land after the successor started. A 22 s wait makes that less likely; it
  does not bound it.
- The restart in place is the most frequent clean restart and the target case. The uniform rule makes
  exactly that case slower. Not worth it.

### 3.10 Names, logs, docs {#surface}

| Surface | Change |
|---|---|
| `MountPriorState`, mount events, `system.cas_mounts`, profile events, settings | None |
| Holder log, `CasPool`, Information | "CAS mount lease of '{}' under writer_epoch {}: the write fence is up, no write is in flight, and the slot says so (settle {} ms); the next mount of this server waits that long instead of the lease". When nothing is written, one line with the reason: lifecycle work in progress, renewer not active, token changed, write unclear |
| Successor log, `CasMountLease`, Information | For a marked body: "... the previous incarnation raised its write fence before it stopped; waiting ~{} ms for requests the store may still apply", in place of the "token-stability observation" line. The existing line after the reclaim stays (`CasPool.cpp:913-916`) |

On the forced path a pool thread can be cut off before its log line. Then the slot itself is the record.

Doc pages that change:

- `D/architecture/mounts-and-leases.md`: the unclean stop paragraph (`mount-lifecycle`), the body fields,
  one row in the slot state diagram (`mount-state-machines`), the wait formula.
- `D/configuration.md`: the paragraph on startup reclaim timing after the settings table; one sentence at
  `cas_attempt_timeout_ms` and `cas_lease_safety_margin_ms` that they also set the wait after a forced exit.
  The text of `cas_unsafe_remount_no_delay` at `:114` is CAS-484 and is not touched here.
- `D/operations/migration.md`: one row in the upgrade and downgrade table (`decommission-compatibility`).

## 4. Part A: compatibility {#compat}

The format is frozen since 26.6.4. This is one additive optional field. No version bump, no migration.

| Pair | Behaviour |
|---|---|
| Old holder, new successor | No marker is ever written. Full wait, as today |
| New holder, old successor | The old decoder skips the field (F19). The body looks like a renewal, so the old successor waits its own full threshold from the moment it first reads it. Never shorter than today |
| New holder, 26.6.4 successor | The same. 26.6.4 uses its own lease for the threshold, not the declared one (26.6.4 `CasServerRoot.cpp:958`). That is the existing mixed-lease case of `D/configuration.md`; the marker does not make it worse, because a holder that wrote the marker is fenced |
| Old GC leader reads a marked slot | Skips the field, counts the slot live, fences after its full threshold. The fence rewrite drops the field (F20). The result is an ordinary `gc_fenced` body |
| New GC leader reads a marked slot | The same decision. Its fence copy keeps the field next to `gc_fenced`; `gc_fenced` is tested first everywhere |
| Old binary, `system.cas_mounts` | `live`, then `expired`. A new binary shows the same |
| `isCreatorFenceTerminal`, any version | Not terminal |
| GC floor, any version | Reads `writer_epoch` and `min_active_build_sequence`, both as a renewal would have left them |
| Downgrade after a marker was written | The old binary waits the full threshold and reclaims. Its claim body has no field |
| Old decommission tool | Refuses a marked slot as alive until GC fences it |
| A body that copies a marked body | Renewals, the farewell and the claim build their bodies from local state. Three writes copy the body they read (F20). In a new build each keeps the field: a GC fence gives `gc_fenced` plus the field; a refused own claim never meets a marked body of its own fresh epoch; a rejoin of a retired owner copies a terminal body into a farewell. All three results are terminal forms, which are tested before the field |

So the field can outlive the holder's own marker body, but only inside a body that is `gc_fenced` or a
farewell, where it has no effect.

## 5. Part A: model {#model}

`M/CaMountObserveCore.tla` has one holder, one observer and one slot. Its `Threshold` is
`TTL + Allowance + Cadence`.

- **Which invariant.** `NoFenceInsideAuthority` and `NoWriteAfterFence`. Both need a new reading. Today
  authority ends at `lastStart + AuthoritySpan`. With the marker it ends when the certificate holds. A
  marked reclaim lands inside the lease authority on purpose, so the invariant as written would go red.
- **To add.**
  - A `Write` is a call of one or more requests. The fence is checked once, before the first; the later
    requests of the call are sent without a check. This is what the abstract single-step `Write` hides.
  - Holder variables: `hfenced`, the set of calls in flight, the set of requests sent and not answered.
  - Holder actions: `HolderFence` (sets `hfenced`); `CallEnter` (joins the set, then checks `hfenced`);
    `CallExit`; `MarkerSend` (enabled only when `hfenced` and the set of calls is empty, on the holder's
    last token); `MarkerLand` at any later tick; `LateApply` (the store applies an unanswered request).
  - `ObsDecide` uses `Settle + SettleAllowance + Cadence` when the watched token is marked.
  - Invariant `NoRequestSentAfterMarker`: no request of the holder is sent after `MarkerSend`.
- **Sabotage rows, each must go red.** The marker is sent with a call in flight. The call joins the set
  after its fence check. A remount re-arms after the marker. The successor uses its own, smaller settle. The
  marker is treated as `gc_fenced`.
- **Witness rows.** A marked reclaim lands while the lease authority has not ended. A marker that lands late
  restarts an observation that was counting the full threshold. `LateApply` after a marked reclaim is
  reachable: the model must show the residual, not hide it.
- **Margin.** `FenceMarginAtLeast` carries over: the margin is the cadence term, as for the lease.
- `M/CaMountRenewRetryCore.tla` has the sabotage `_sab_late_rearm`. It needs the marker as a terminal
  renewer action and the invariant "no slot write after the marker". Whether `M/CaCasMountCore.tla`
  enumerates prior states was not read; this design adds none.

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
| `CASMountAwaitExpiry` | Marked body is reclaimed after the short threshold | Fake clock: no reclaim one tick before, reclaim at the threshold |
| `CASMountAwaitExpiry` | The holder's bound is used | Successor configured with a smaller budget still waits the declared value |
| `CASMountAwaitExpiry` | The loop sleeps the time left | The sleeps sum to the threshold exactly; the last sleep is the remainder; nothing left means no sleep |
| `CASMountAwaitExpiry` | Control | `PastExpiryStillPaysTheFullObservationThreshold` stays as is: no field, full wait |
| `CASMountAwaitExpiry` | Marker lands during a full observation | One restart counted, then the short threshold |
| `CASClaimMount` | Marked body of another uuid | `ForeignOwner`, no write |
| `CASClaimMount` | Marked and `gc_fenced` | Instant, prior `Fenced` |
| `CASRequests` | Count before the check | A write blocked by a test hook between its guard and its fence check: the count is 1, and after a trip the write sends nothing |
| `CASRequests` | Count spans the backend call | A backend `write` that blocks mid-call, as a second part of a multipart upload would: the count stays 1 until the call returns |
| `CASRequests` | What is not counted | A read, a list and a `publish` in flight leave the count at 0 |
| `CASPoolShutdown` | No marker with a call in flight | A caller blocked inside a backend `write`: steps 1 to 5 complete, step 6 does not return, the slot is unchanged. Released: the marker is written |
| `CASPoolShutdown` | A renewal due between the trip and the join | With `renewal_admitted_hook_for_test` and a fake clock: the renewer stays `Active` and the marker is written. With the stop request moved after the trip the test fails |
| `CASPoolShutdown` | Marker write order | The fence is `lost`, the count is 0 and the renewer is `Released` before the first byte of the marker request |
| `CASPoolShutdown` | No marker when a renewal is unclear | Renewer `RenewalTerminal` after the join: no request sent |
| `CASPoolShutdown` | Marker write refused, token changed, unclear | No second write in any of the three; an unclear write that landed is in the slot and is reclaimed after the short threshold |
| `CASPoolShutdown` | No write after the marker | A renewal or a `release` on that renewer: death test |
| `CASPoolShutdown` | Lifecycle work in progress | With the lifecycle mutex held the pool is skipped and nothing is joined |
| `CASPoolShutdown` | Hard cap | A pool whose worker never stops: the entry function returns at the cap and the other pool's marker is in its slot |
| `CASPoolRemount` | Fence stays down | After the marker: `scheduleRemount` is a no-op; a renewal commit that publishes a deadline does not admit a write; a remount that re-armed before the join is tripped again and the count is read after that |
| `CASFenceTerminal`, `CASMountSlotOwnership`, `CASHeartbeatFloor` | Marked body | Not terminal; not held; counted live and fenced only after the full threshold |

Existing tests:

- None asserts the 135 s grid value. The tests that drive the loop with a fake sleep assert `>=` the
  threshold or use a threshold that is a multiple of the poll:
  `CASMountObservation.IncarnationStableForThresholdThenReclaimed` (`T/gtest_cas_mount.cpp:2991`),
  `CASMountAwaitExpiry.PastExpiryStillPaysTheFullObservationThreshold` (`:1326`),
  `HolderDeclaringALongerLeaseIsWaitedOutInFull` (`:1487`, 64000 ms with a 1000 ms poll). They stay green
  with the new sleep (inferred, not run). `T/gtest_cas_pool.cpp` and `T/gtest_cas_heartbeat.cpp` were
  searched for the number, not read in full.
- Integration tests that kill with `SIGKILL` do not change
  (`tests/integration/test_cas_mount_renewal_retry/test.py:631-663`, `test_cas_shared_pool/test.py:263-299`,
  `test_cas_drop_pool_member`, `test_cas_gc_sharded`). Their start budgets assume 135 s or more; 131 s fits.
- No teardown or FORGET test changes: that path writes no marker.

One integration test, `test_cas_forced_exit_restart`:

- Config: default 120 s lease, no `cas_unsafe_remount_no_delay`, `shutdown_wait_unfinished = 3`,
  `shutdown_wait_unfinished_queries = true`. Without the second setting the queries are killed before the
  wait and the exit is not forced (F22).
- Run `SELECT sleepEachRow(1) FROM numbers(60) SETTINGS max_block_size = 1`, send `SIGTERM`.
- Assert in the old log: `Will shutdown forcefully`, and the holder line of [§3.10](#surface).
- Start. Assert: the successor line with the short wait; no `waiting ~131000 ms`; listening within 45 s; a
  table written before the stop reads back.
- Second variant: the same with the marker write blocked by a failpoint. Assert the full wait.
- Third variant: an `INSERT` held inside a ref-log write by a failpoint during the exit. Assert no marker and
  the full wait.

Predictions that would refute the design if they fail:

| # | Prediction | Refuted if |
|---|---|---|
| P1 | A 26.6.4 binary started over a marked slot logs the token-stability wait with its own full threshold | It reclaims at once, or fails to decode the slot |
| P2 | After the trip, a thread blocked in a mount-plane write returns `FenceLost` without a second request on the wire | The backend sees a reissue or a settling read |
| P3 | With the marker write unclear and landed late, the successor's observation restarts once and reclaims after the short threshold | It reclaims without a restart, or the holder sends another slot write |
| P4 | On a healthy store the count reaches zero within one attempt envelope of the trip in the CI stress lane | Markers are regularly missing there: the count is held by something this design did not expect |

## 7. Part B: a local record of slot write attempts, considered and rejected {#part-b}

**The idea.** The server stores, in a local file under its `path`, the kernel boot id and the
`write_attempt_id` of every slot body it sends. A restart that finds the slot body's id in the file, under
the same boot id, concludes that the last writer of the slot was a server on this path in this boot. It
holds the `status` file lock, so that writer must be dead, and it waits about 25 s instead of the lease.

**Why it is rejected.** A match proves a shared local file. It does not prove that the writer is dead. Three
counterexamples, each with the predecessor alive and its renewal stalled inside a valid lease:

| # | Counterexample | Where |
|---|---|---|
| 1 | A tool reads the record without the lock. `clickhouse-disks` loads the same `uuid`, takes no `status` lock and mounts the disk writable with background renewal. It finds the live server's id in the file and reclaims after 25 s. Restricting who writes the file does not stop who reads it | `programs/disks/DisksApp.cpp:575`, `:598-610`; `CA/ContentAddressedMetadataStorage.cpp:787` |
| 2 | The record is not bound to the lock object. Two containers on one kernel have private `status` files and share the `uuid` and `disks` subtrees by bind mounts. Each lock works, both boot ids are genuine, and the second server matches the first one's record | `programs/server/Server.cpp:2033`; `src/Common/StatusFile.cpp:65` |
| 3 | The server gives the lock up before its disks stop. `StatusFile` is declared after the scope exit that runs `global_context->shutdown`, so it is destroyed first: closed and unlinked while merges finish, system logs flush and the mount still renews. A second server started then takes a fresh lock and matches | `programs/server/Server.cpp:1823`, `:2033`; `src/Common/StatusFile.cpp:108-115`; `src/Interpreters/Context.cpp:1172` |

A removed `status` file, a same-kernel copy of the data directory, and a file system whose `flock` is local
to each mount give further cases.

**What would make it sound.** A lock held by the pool itself for its whole life, on the very file the
successor reads. That is the witness design shelved as CAS-482. It is not revived here.

**Consequence.** A restart after `SIGKILL`, an OOM kill or a fatal signal keeps the lease wait: 131 s. With
Part C on, it is 131 s less the credit, for example 95 s to 105 s after the 30 s `RestartSec` of the
packaged unit (`packages/clickhouse-server.service:19`). The test lanes skip the wait with
`cas_unsafe_remount_no_delay` (commit `802077e5c1a`).

## 8. Part C: credit the slot's age on the store's clock, opt-in {#part-c}

Separable from Part A. Task CAS-483. No on-store change, no local state, no generic file.

### 8.1 The rule {#c-rule}

One Boolean disk setting, `cas_mount_credit_store_age`, default `0`. With `0` nothing below happens.

```text
LM_slot = Last-Modified of the slot body the loop is watching        (the read the loop already makes)
LM_now  = Last-Modified of this server root's epoch object            (one extra read at the entry of the loop)
d       = LM_now - LM_slot
credit  = max(0, d - margin)       0 when either stamp is unknown or zero, or d is negative
wait    = max(0, T - credit)       T is the threshold of the body being watched
```

- The loop starts its own clock at the first sighting, as today, and reclaims when `wait` has passed on
  that clock with the token unchanged. With the sleep of [§3.4](#threshold) the wait is exact; with
  `wait = 0` the loop claims at once, by a second read that requires the same token.
- A new token restarts the observation, as today. Its stamp is later than `LM_now`, so its credit is 0.
- The log keeps the existing line with the threshold and adds one line with `d`, the credit and the wait.
  A test that matches the existing line stays green
  (`tests/integration/test_cas_mount_renewal_retry/test.py:631`).
- Scope: the same-uuid wait of a starting server and of the in-process remount. The GC leader's fence
  threshold does not change.

The reference point per case. The credit is counted from the stamp of the body in the slot, so what matters
is what that stamp follows:

| Body being watched | `T` | The stamp follows | Credit |
|---|---|---|---|
| A renewal or an adoption body | Full lease threshold | The start of the attempt from which the holder counts its authority | Yes |
| A claim body whose adoption never happened | Full lease threshold | Nothing: the holder never armed its fence | Yes |
| A Part A marker body | Marker threshold | The last fence trip and the observed zero: the marker is sent after both | Yes |
| The renewal body that preceded a marker | — | Not used: once the marker is in the slot, only the marker body's stamp counts | — |
| `gc_fenced`, a farewell | none, instant | — | Not applicable |

Defaults, `margin = 4000 ms`:

| `T` | Wait |
|---|---|
| Full lease, 131000 ms | 131 s for `d <= 4 s`; `135 s - d` above; 0 from `d = 135 s` |
| Part A marker, 21800 ms | 21.8 s for `d <= 4 s`; `25.8 s - d` above; 0 from `d = 25.8 s` |

`d` starts at the last slot write, not at the stop. A killed server's last renewal is up to one renewal
period (10 s) older than the kill.

### 8.2 Plumbing {#c-plumbing}

The generic interface already carries the value: `ObjectMetadata::last_modified` with
`is_last_modified_known` (`src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h:135-140`). The CAS
backend receives it on every read and drops it.

| Backend of a writable CAS mount | Where the stamp is today | Resolution | Clock |
|---|---|---|---|
| S3 API, native mode: AWS S3, MinIO, RustFS, GCS through its S3 endpoint | The `GET` the backend already sends returns it: `ReadBufferFromGetObjectResult` fills `last_modified` from the response; `ObjectStorageBackend::readUnder` takes `got.metadata.etag` and ignores the rest. `HEAD` fills it too | 1 s | The store's |
| `local` object storage, emulated mode (tests, single process) | `makeObjectMetadata` fills it from `st_mtime`; the emulated read already asks for the metadata to mint its token | 1 s | The node's wall clock, through the file system |
| In-memory backend (unit tests) | None. Add a settable stamp | test-defined | test-defined |
| Azure, HDFS | Not a writable CAS backend: native mode needs `supportsRetryProfile(SingleAttempt)`, which only `S3ObjectStorage` provides | — | — |

Where: `src/IO/S3/ReadBufferFromGetObjectResult.cpp:21`;
`src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:407-427`, `:972`, `S3ObjectStorage.h:200`;
`CA/Backend/CasObjectStorageBackend.cpp:626-643` (native read), `:561-565` and `:657-676` (emulated read),
`:203-218` (the capability check);
`src/Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.cpp:87-94`;
`src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h:562`.

Changes, all under `CA/`:

1. `Backend::Raw` (`CA/Backend/CasBackend.h:159`) and `Object` (`CA/Backend/CasWriteResult.h:28`) get
   `std::optional<uint64_t> store_modified_ms`. Unset when the storage does not know it or reports 0.
2. `ObjectStorageBackend::readUnder` fills it in both modes. `CasOperation::readUnder` passes it on
   (`CA/Backend/CasRequests.cpp:537-545`). The decorating backends forward `Raw` unchanged.
3. `MountClaimResult` carries the stamp of the body it reports, next to `etag`
   (`CasServerRoot.cpp:1015`, `:1119`, `:1264-1276`).
4. `claimMountAwaitingExpiry` gets one flag. With it set, it reads the epoch object once at its entry,
   before the loop, and applies the rule at `CasServerRoot.cpp:1289`. Its callers do not change otherwise.
5. The disk setting, passed through `PoolConfig`.

No request changes. One request is added: the read of the epoch object.

### 8.3 "Now" on the store's clock {#c-now}

- **The mount already writes before it looks at the slot.** Every writable open and every in-process remount
  allocates its `writer_epoch` by a conditional write of the `epoch` object, with a fresh nonce, before the
  mount claim (`CasServerRoot.cpp:817-900`; `CasPool.cpp:688-690` before `:755-788`; `:810`, `:872`;
  `:1559` before `:1574`).
- **A `PUT` response does not return `Last-Modified`.** So the stamp of that write costs one read of the
  `epoch` object: one `GET` of a 100-byte object per mount attempt.
- **Why this is "now".** The store created that stamp before this process read it back. So `LM_now` is
  never later than the moment the loop first reads the slot. An earlier "now" only makes the credit smaller.
- **It need not be our own write.** If another process rewrote the `epoch` object in between, its stamp is
  later than ours and still earlier than our read. The argument does not change.
- **Order.** `LM_now` is read at the entry of the loop, before the first slot read and before the loop
  starts its own clock. Read after, the time between the first sighting and the stamp would be counted
  twice.
- **Rejected: the `Date` response header.** `ObjectMetadata` has no field for it and the S3 read path does
  not surface it. It would need changes in generic S3 code.

### 8.4 Safety argument {#c-safety}

1. A holder counts its authority from the start of the renewal attempt, before any I/O:
   `attempt_start_boot_ms` is sampled at the top of `renewOn` and the deadline is that plus the lease
   (`CasServerRoot.cpp:1977-1979`, `:2077-2081`; `CasMountRuntime.cpp:603-609`). The first arm does the same
   (`CasServerRoot.cpp:1805`, `:1816-1820`; `CasPool.cpp:966-971`). 26.6.4 does the same
   (26.6.4 `CasServerRoot.cpp:1447-1461`, `CasMountRuntime.cpp:485-488`).
2. A logical renewal that retries through an outage keeps one body and one start. Whenever it lands, the
   authority it gives ends one lease after that start.
3. **The stamp is created after the authority anchor.** The store cannot stamp a body before the first
   request of its write reached it, and the holder sends that request after it sampled the start. This holds
   for a single `PUT` and for a multipart upload, whose stamp on AWS is the time of its initiation. A slot
   write can be multipart on an ETag store when `max_single_part_upload_size` is set below the body size
   (`CasObjectStorageBackend.cpp:810-820`; `src/IO/WriteBufferFromS3.cpp:182`). For a marker body the same
   holds with the last fence trip as the anchor.
4. So the store-side age of the body in the slot is at most the time the holder itself has counted against
   that body, and a wait of `T - age` after the first sighting ends no earlier than a wait of `T` that had
   started when the stamp was created. Today's protocol already allows that second wait: it is an observer
   that happened to read the slot at that moment, such as a GC leader watching continuously.
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

The credit shortens the wait for the slot. It does not shorten what the wait stands for in each case: for a
renewal body, the rest of the holder's lease; for a marker body, the settle time after a certificate that
already held when the stamp was created.

### 8.5 The contract, the margin, bad stamps {#c-assumptions}

The unsafe direction is a credit that is too large: `LM_slot` too early or `LM_now` too late.

**The contract the operator accepts by turning the setting on:** for any two objects of the pool, the
difference of their `Last-Modified` stamps exceeds the true time between the two writes by at most
`cas_lease_safety_margin_ms` (2 s by default). This must hold across all nodes of the store and through
clock steps.

- `margin = 2 × 1000 ms + cas_lease_safety_margin_ms`: 4000 ms by default. The first 2 s cover the 1 s
  resolution of each stamp. The rest is the contract.
- No store documents such a bound. A MinIO node stamps with its own wall clock, and its nodes tolerate far
  more disagreement than 2 s between themselves. AWS and GCS document strong consistency and the header,
  not the error of its difference. This is why the part is off by default.
- What a violation costs. With stamps that disagree by `e` beyond the contract, a holder that is alive but
  not renewing is reclaimed `e` early. At 120 s and no clock drift the threshold exceeds the lease by 11 s
  and a holder admits its last request 9 s before its deadline. So up to about 11 s of excess the reclaim
  still follows the end of the holder's authority, and up to about 20 s it still follows the holder's last
  possible request start. These are slack figures, not guarantees. A healthy holder is never affected: its
  stamp is at most one renewal period old.
- Further properties the contract implies: the store's clock runs within 5 % of the holder's; a stamp is
  not set back afterwards (a restore that keeps old stamps, a copy of a `local` pool with `cp -p`). A later
  re-stamp is the safe direction.
- On `local` object storage the store's clock is the node's wall clock. The contract then says that clock
  does not step forward by more than 2 s between the two writes.

Bad stamps:

| Stamps | Result |
|---|---|
| Either unknown, or 0 | No credit, full wait |
| `d` negative | No credit, full wait. One `Information` line: the slot was written after the epoch object, or the stamps disagree |
| `d` within the margin | No credit, full wait |
| Very large `d` | Valid: a server may have been down for days. The wait is 0 |

No check against a ClickHouse wall clock is made. Such a check cannot establish the contract: a store node
and the successor's clock can be wrong together.

### 8.6 Compatibility {#c-compat}

- Nothing is written differently. No field, no object, no key. Checked: the rule reads two existing objects
  and changes one local comparison.
- Any holder version: covered by step 4 of [§8.4](#c-safety). A 26.6.4 holder counts from the attempt start
  too.
- A successor without Part C, or with the setting off, waits as today.
- Mixed leases: the threshold is still the longer of the own and the declared lease. The credit is
  subtracted from that.
- `cas_unsafe_remount_no_delay`, a `gc_fenced` slot and a farewell are decided before the loop and do not
  change.

### 8.7 Model and tests {#c-model-tests}

Model, `M/CaMountObserveCore.tla`:

- A third clock `sclock` with the same rate coupling and a bounded error per stamp. `RenewLand` records
  `landStamp` from it, at or after the attempt's first request.
- Observer: `ObsStampNow` records `nowStamp` from `sclock` before `ObsRead`; `ObsDecide` dates the sighting
  `oclock - max(0, nowStamp - landStamp - Margin)`.
- Invariant: `NoFenceInsideAuthority`, unchanged. `FenceMarginAtLeast` with the margin reduced by the stamp
  error.
- Sabotage rows, each must go red: `nowStamp` taken after the sighting (double count); no margin with
  truncated stamps; a stamp error beyond the contract (an environment row, like the existing drift row); for
  a marked slot, the credit counted from the renewal body that preceded the marker.

New gtests:

| Suite | Test | Asserts |
|---|---|---|
| `CASObjectStorageBackend` | Stamp pass-through | Native read returns the storage's `last_modified`; emulated read returns the file time; unknown and 0 are unset |
| `CASMountAwaitExpiry` | Off by default | With the setting off no epoch read is made and the wait is the threshold |
| `CASMountAwaitExpiry` | Credit shortens the wait | Fake clocks: reclaim at `T - credit`, not earlier |
| `CASMountAwaitExpiry` | Full credit claims at once, still conditionally | No sleep; a token that changes between the two reads restarts the observation with no credit |
| `CASMountAwaitExpiry` | Margin | `d` at or below the margin gives the full threshold exactly |
| `CASMountAwaitExpiry` | Bad stamps | Unknown, zero, negative: full wait |
| `CASMountAwaitExpiry` | Marker body | The credit is counted from the marker body's stamp; a marker that lands during the observation restarts it with credit 0 |
| `CASMountAwaitExpiry` | A renewing holder | Credit at most one period; the start is refused after 3 restarts as today |
| `CASMountAwaitExpiry` | Order | The epoch object is read once, before the first slot read |

One integration test on the S3 stand, with the setting on: kill the server, wait longer than the threshold,
start; assert the credit line and listening within 15 s. Second variant: start at once after the kill; assert a
credit of at most one renewal period and the remaining wait.

Predictions:

| # | Prediction | Refuted if |
|---|---|---|
| P5 | With the holder alive and renewing, the credit never exceeds one renewal period plus one envelope | A larger credit is logged |
| P6 | After a kill and 140 s of downtime, the start logs a credit of the full threshold and claims on its second read | It sleeps, or waits the threshold |

Neither prediction tests the contract of [§8.5](#c-assumptions). A stamp compared with a body's
`started_at_ms` mixes request latency, the holder's wall clock and the store's, so it cannot serve as an
acceptance test for it.

### 8.8 Verdict {#c-verdict}

- **Sound under its contract; opt-in.** The code cannot check the contract and no store documents it.
- **It adds a third clock to the protocol.** Today the wait trusts the rate of two `CLOCK_BOOTTIME` clocks.
  With Part C part of the wait is measured by the store's wall clock.
- **What it cannot do:** shorten a restart that follows the last renewal by less than the threshold. A
  killed server restarted after 30 s still waits about 100 s.
- **Size:** about 80 lines of product code, all under `CA/`; about 200 lines of tests.

## 9. How the parts compose {#composition}

Each part stands alone and can be dropped alone. In the wait loop, for a same-uuid older-epoch body that is
neither `gc_fenced` nor a farewell:

1. Pick the threshold: the marker's (Part A), else the full lease threshold.
2. If the setting is on, subtract the credit counted from that body's stamp (Part C).

Without A step 1 has no marker branch. Without C step 2 is absent. Neither part reads the other's state.
The exact sleep of [§3.4](#threshold) is described with Part A and serves both.

Target case: a restart of the server on the same node, possibly together with a reboot of the node.

Waits at default settings on S3. `d` is the store time from the last slot write to the successor's epoch
write. "Today" is on the 5 s poll grid; the other columns are exact.

| Case | Today | A (default) | A + C (opt-in) |
|---|---|---|---|
| Clean stop, any restart | 0 s | 0 s | 0 s |
| Controlled stop with unfinished queries (forced exit), marker written, restart in place, `d` about 5 s | 135 s | 21.8 s | 20.8 s |
| The same, then a node reboot, `d` above 25.8 s | 135 s | 21.8 s | 0 s |
| Forced exit, no marker: a write still in flight at the 16 s cap, lifecycle work in progress, an unclear renewal | 135 s | 131 s | 131 s for `d <= 4 s`, else `135 s - d` |
| `SIGKILL` or OOM kill, restart at once on the same running kernel, `d` up to 10 s | 135 s | 131 s | 125 s to 131 s |
| The same, restarted by `systemd` after `RestartSec=30`, `d` 30 s to 40 s | 135 s | 131 s | 95 s to 105 s |
| Power loss or kernel failure, node back after 90 s, `d` 90 s to 100 s | 135 s | 131 s | 35 s to 45 s |
| Any downtime above 135 s | 135 s | 131 s | 0 s |
| Pod moved to another node, old node partitioned; eviction takes minutes | 135 s | 131 s | 0 s |
| VM cloned from a disk image, original alive and renewing | refused | refused | refused: the stamp is at most 10 s old |

- **The moved pod is safe under C** because the credit is the old holder's own lease running out. It
  counted from the start of its last renewal; the store stamped that renewal later. If the old node can
  still reach the store, it renews, the stamp is fresh and the credit is small.
- **Nothing identifies a node.** Hostname, IP and the kernel boot id are used nowhere. A changed boot id
  would not mean "the same node rebooted": another node has another boot id too.
- **For the target case:** a clean stop is already instant. A forced exit needs A. A kill with an immediate
  restart is not helped by either part. Everything with a reboot or a long downtime needs C, and C is the
  only part that helps across nodes without a controlled stop.

## 10. Size {#size}

| Part | Product code | Tests | Docs | Model |
|---|---|---|---|---|
| A | about 220 lines: format field 15; renewer marker write 40; stop request and runtime entry 25; counter, guard, three guard sites, the two ordering changes, the wait 35; pool steps 1 to 7 30; storage entry with the lifecycle lock 15; free function with pool threads and the cap 40; `Server.cpp` 5; threshold branch, exact sleep, logs 20 | about 450 lines of gtests, one integration test | 3 pages, about 50 lines | `CaMountObserveCore` and `CaMountRenewRetryCore`: about 120 lines and 9 configs |
| B | none | none | none | none |
| C | about 80 lines: `Raw` and `Object` field 6; backend reads 10; in-memory backend stamp 15; `MountClaimResult` and `claimMount` 8; epoch read and credit in the loop, log 25; setting and its plumbing 15 | about 200 lines of gtests, one integration test | 2 pages: `D/architecture/mounts-and-leases.md`, `D/configuration.md` with the contract | A third clock in `CaMountObserveCore`: about 60 lines and 5 configs |

Estimates from reading the code, not from a prototype.

## 11. Open questions {#open}

Decided, and where the design reflects it:

| Question | Decision |
|---|---|
| How the marker is certified | By an observed zero of mutating calls after the last trip, not by a time bound ([§3.5](#bound)) |
| The declared settle value | `2 × envelope + margin`, kept; its purpose is restated ([§3.1](#marker)) |
| The `+ poll` term of the threshold | Kept |
| The poll grid | Dropped: the loop sleeps the time left; the lease wait is 131 s ([§3.4](#threshold)) |
| A marker from a pool teardown, from FORGET, in the paused-writes branch, after an unclear renewal | No. The first two are a possible follow-up ([§3.3](#writers)) |
| A new `system.cas_mounts` state, a new prior state, new event branches | No. Two log lines ([§3.10](#surface)) |
| `Server.cpp:3671` | Hooked, with the same call |
| The forced-exit hook's time | A hard cap of 16 s, enforced by the calling thread ([§3.3](#writers)) |
| Part B | Rejected ([§7](#part-b)) |
| Part C by default | Off. One disk setting ([§8.1](#c-rule)) |
| Part C cross-checks against a ClickHouse wall clock | Dropped ([§8.5](#c-assumptions)) |

Still open, most important first:

1. **A restart after `SIGKILL` or an OOM kill keeps the lease wait.** Part A needs code to run at the stop;
   Part B is rejected; Part C gives `135 s - d`. The test lanes use `cas_unsafe_remount_no_delay`. Nothing
   in this design makes that restart fast in production.
2. **Blob publication is not counted.** The argument is in [§3.5](#bound): a blob write is content-keyed and
   outside the mount's authority. If that is not accepted, a server stopped while it uploads blobs writes
   no marker until the uploads end, and usually none within 16 s.
3. **Late application by the store.** A request whose call returned unanswered can be applied after the
   successor started. The settle time makes it unlikely; nothing bounds it. The lease wait has the same
   residual, with 131 s in place of 22 s. CAS-486 is the place to close it.
4. **Part C on `local` object storage.** The store's clock there is the node's wall clock. Allowed under
   the same opt-in, as written, or native mode only?
5. **Part C for the GC leader.** The fence threshold of `computeHeartbeatFloor` could take the same credit
   and fence a dead member sooner. Not included.

## 12. Limits of what is established {#limits}

- **Nothing was built or run.** No test, no TLC run. P1 to P6 are predictions.
- **Plane assignment.** That every durable mutation of a mount goes through the mount plane was traced from
  the engine's entry points and the construction sites of `CasRequests`, and in the inventory. The callers
  of `CasPlainObjects` and of the ref ledger were not each read line by line.
- **The count covers engine calls.** A write to the store that used `IObjectStorage` directly with a key
  that is not private to the epoch would escape it. The two direct writers found are the staging upload and
  its delete.
- **Sequential consistency.** The proof of [§3.5](#bound) needs the four atomic operations to be
  sequentially consistent. That is a property of the implementation to be checked in review and under TSan,
  not something a unit test shows.
- **Late application.** A request the store applies after the client's call returned is not bounded, with
  or without this design (`CasPool.cpp:880-897`).
- **26.6.4** was read at one tag: the decoder, the tolerant mode, the claim conditions and the deadline
  anchor. Builds between that tag and this tree were not read (inferred to be the same).
- **`Server.cpp`**: the exits at `:4018` and `:3671` were read, and the state of the disks at each. Whether a
  thread can still be started from the global pool at `:3671` is inferred. Whether the log sink flushes the
  hook's lines before `_exit` is inferred from the existing `Will shutdown forcefully` line in CI logs.
- **Part B's counterexamples** were checked at the cited code locations. The bind-mount case was not run.
- **Part C**: no stamp was measured on any store. That S3-compatible stores stamp a write no earlier than
  its first request is taken from the protocol, not tested. What MinIO and RustFS do with clocks between
  their nodes was not read in their code. Azure and HDFS as CAS backends were checked only at the
  capability gate.
- **Scope.** This covers one mount slot, its holder and a same-uuid successor at this commit. It says
  nothing about GC leadership hand-over, about pools already run with `cas_unsafe_remount_no_delay`, or
  about R1 to R4 when the predecessor did not fence itself.
