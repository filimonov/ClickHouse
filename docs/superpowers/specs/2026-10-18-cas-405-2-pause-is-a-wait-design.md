# CAS-405.2: a write pause is a wait, not an error {#cas-405-2-pause-is-a-wait}

**Proposal.** A query that finds the mount lease spent waits for the renewal, up to a new disk setting
`cas_operation_timeout_ms` (default 180000), instead of failing with `NETWORK_ERROR` (210). The same
setting replaces the fixed 90 s `Retry` window. Background work keeps failing fast. No new state, no
format change. Status: approved 2026-10-18. Task: CAS-405.2. Evidence: 20 fault-proxy stand runs (CAS-405 notes).
Code refs are at `d410298e2c4` (PR #2517); `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## 1. Today {#today}

Server error logs: 772 of 824 insert errors in `pr_def_hang120` (lease 30/10 s), 210 of 250 in `pr_l120_hang120` (120/40 s)
are one in-memory check at the start of the insert:

- `MergeTreeDataWriter.cpp:881` → part ctor → `VersionMetadataOnDisk.cpp:50` → `DataPartStorageOnDiskFull.cpp:86`
- → `CA/ContentAddressedMetadataStorage.cpp:1517` `existsFile`. Its entry gate `checkOpAdmitted` (:1243) passes,
  because #2517 keeps the pool `Live` during the pause.
- → `CA/Parts/PartFolderAccess.cpp:289` → `CA/Pool/CasRefLedger.cpp:294` `resolveRef`
  → `:625` `acquireReadableRefTableRuntime` → `:258` `refuseUnlessAdmitted` → `op.admitted()`
  (`CA/Backend/CasRequests.h:271`, `gate(0)`).
- → `CA/Pool/CasMountRuntime.cpp:141` `admit` returns `NoBudget` once `now ≥ deadline − margin`.
  → `throwCasTransientUnavailable` → 210 at once. The client retries and is refused again at once.

The rest are operations already running when the lease ran short. `gatedPause` (`CasRequests.cpp:873-880, 949`)
and `readLoop` (`CasRequests.h:492-550`) turn `Gate::NoBudget` into `GaveUp(Deadline, Lease)` or
`giveUpReadNoBudget`, for example `stageManifest ... UNCERTAIN` (`CasPartWriteTxn.cpp:701`), 52 and 24 errors in the two runs.
Every operation also stops at the fixed 90 s window (`CA/Backend/CasRetry.h:48, 55, 62`), which is shorter than a 120 s outage.

## 2. Proposal {#proposal}

**One waiting function.** `CasMountRuntime::awaitBudget(generation, needed_ms, until_boot_ms, cancelled)`. It
loops on `admit(generation, needed_ms)` and blocks on a new `admission_cv` while the answer is `NoBudget`.
It returns the first answer that is not `NoBudget`, or `NoBudget` at `until_boot_ms`, or when `cancelled()` is true.
It is exposed as a fourth `Fence` closure. `Fence::open()` and test fences leave it empty, so they never wait.

**What wakes it.** Every store that can end a pause locks `admission_mutex`, then notifies:

- `setMountDeadline` (`:202`): a committed renewal (`publishRenewedDeadline`, `:348`/`:588`) and the remount re-anchor.
- `tripMountLost` / `tripFenceWithoutOperationalLoss` (`:103`, `:116`): lease lost. The waiter gets `LostOrRearmed`.
- `armMountFence` (`:207`): new generation. The waiter gets `LostOrRearmed`.
- Pool stop and lifecycle change (`stopBackgroundWorkers`, `:921`): the predicate also ends on `lifecycle() != Live`.
- Query cancel: there is no callback, so the wait polls `cancelled()` every 100 ms. The deadline is BOOTTIME and the
  cv uses the steady clock, so each slice re-reads the clock.

The waiters use their own mutex, not `driver_mutex`, so the renewal driver is unchanged.

**Who waits.** Only a thread that runs a query: `CurrentThread::getQueryContext()->getProcessListElementSafe()`
is non-null. `cancelled()` is `isKilled() || !checkTimeLimitSoft()`, so `KILL QUERY` and `max_execution_time` end the wait.
Other threads (merges, fetches, cleanup, GC, renewal on `open_requests`) keep failing fast, because:
- a disk-level wait has no way to see a table shutdown, `DROP` or merge cancel, so `DROP TABLE` would hang for the whole timeout;
- background pools are shared with non-CAS tables.

**Where it is called.** Two existing admission points. No per-call-site edits.
1. `checkOpAdmitted` (`ContentAddressedMetadataStorage.cpp:1243`): the documented entry gate of every public method
   of the metadata storage and its transaction (inventory at `:130-180`). No CAS lock is held there. If the pool is `Live`
   and `admit(gen, 0) == NoBudget`, it waits until now + `cas_operation_timeout_ms`.
   This fixes the 770 refusals above.
2. `CasOperation::gate` (`CasRequests.cpp:435`), when it is called before a request or a pause: `readLoop`, `writeLoop`/`gatedPause`,
   `postCommit` (`:864`) and `CasHotKeys` (`:117`, outside the lane mutex). The bound is the operation's own `Retry::Bound`.
   `gate` gets the bound as an argument. `admitted()` keeps its non-waiting verdict: it is called under `rt->state_mutex`
   (`CasRefLedger.cpp:1566`, `:4022`), where waiting would block the remount barrier.

**One window, not two.** New disk setting `cas_operation_timeout_ms`, default **180000**, carried in
`CasRequestBudget` and `CasRequests` like `attempt_reservation_ms`.
- `Retry::standard()`, `once()` and the default window of `untilLeaseSafe` no longer hard-code 90 s. They take the plane's value at `bind`/`freeze`.
- A running operation spends the same budget on request retries and on waiting. An insert that started at the outage start
  retries requests for the whole outage before any pause begins, so a separate wait bound would still be cut by a
  shorter window. With two knobs, users would have to keep them in order.
- Default: a 120 s outage, plus about 10 s for the renewal (column "Back" in `tmp/cas405/REPORT.md`), plus the work left
  after the renewal. That is about 150 s; 180 s leaves 30 s of slack.
- It also clears the 90-100 s read stalls (#2343).
- Validation: `attemptEnvelopeMs() < cas_operation_timeout_ms`.

**Relation to the lease.** The lease decides when a wait starts:
- writes stop at `2 × envelope + margin` (16 s) before the deadline;
- in-memory checks stop at `margin` (2 s) before the deadline.

The timeout decides when the wait ends. The two are independent. Admission after a wait reserves the same
`needed + margin` as today. With a lease of 120/40 s, a query waits at most about 45 s in a 120 s outage. With 30/10 s, it waits up to about 105 s.

**When the bound is hit.** The client gets the same `NETWORK_ERROR` (210). The text adds the wait, for example
`... writes paused 183 s ago; waited 180000 ms (cas_operation_timeout_ms) for the mount lease renewal`.
`KILL QUERY` and `max_execution_time` give their usual errors at the entry gate; inside the engine they end the wait as a 210.
New ProfileEvents `CASLeaseWaits` and `CASLeaseWaitMicroseconds` show the wait per query in `system.query_log`.
`system.cas_mounts` does not change: `lifecycle = 'not_live'`, `lifecycle_reason = 'lease_expired'`,
`lifecycle_since` = when writes paused. The waiting queries appear in `system.processes`.

**Safety (fence argument).** The wait only re-evaluates the unchanged admission predicate.
- **No new admission rule.** A request is sent only if `!lost`, the generation is unchanged, and `needed + margin < deadline − now`.
- **The deadline moves forward only on proof.** It moves only after our own conditional renewal commits (`LocalAuthority`,
  anchored at the attempt start). A GC fence rewrites the slot, so that renewal gets 412 → `Terminal` → `tripMountLost`
  → the waiter gets `LostOrRearmed`. A remount bumps the generation, which refuses every waiter admitted before it.
- **Long operations depend on no timer.** GC acts on a member as dead only on what the slot says
  (`GcSweep`: fence, farewell, successor). A paused member keeps its build-sequence floor until then.
  The successor's handover wait relies on per-attempt admission, not on the 90 s window (`CasRequestBudget.h`).

**TLA+.** No model changes. A waiter is a writer that has not been admitted yet: the model just stutters.
- `CaMountRenewRetryCore`: `LateSuccessNeverOpensWrites`, `ResumedEpochNeverWritesAfterGcActed` and
  `NoAuthorityAfterEntitledTakeover` cover it unchanged.
- `CaDiskLifecycle`: `OpAdmit`/`OpDurable` already allow any gap, and `OldEpochOpRefused` covers an operation that resumes after a remount.
- `CaRenewalDriver`: untouched, because waiters never take `driver_mutex`.

**Background, CAS-256 and CAS-405.1.** Both loops stay as they are and stay in their own tasks:
- CAS-256: merges still get 210 at the pause and rerun 127-397 times. Fix it by postponing the merge entry
  while `writesPausedSinceBootMs()` is set.
- CAS-405.1: the fetch ⇄ cleanup loop is in upstream fetch code. Waiting would not stop it: `fetchPart` returns
  false whether cleanup fails or blocks.

This design removes only the client-visible half.

## 3. Alternatives rejected {#alternatives}

- Longer lease only (CAS-405.3): still 4-250 errors per outage; crash restart grows to 163-243 s.
- Wait in `admitted()` / `refuseUnlessAdmitted`: both are called under `rt->state_mutex`.
- Skip the lease check for resident reads: removes 770 errors without a wait, but serves reads after the deadline,
  when GC may already have fenced the node. This weakens fail-closed reads.
- Retry 210 in `ReplicatedMergeTreeSink`: an upstream change for a CAS condition.
- Background writers wait too: no cancel source for shutdown or `DROP`, and the pools are shared.
- Separate wait bound and policy window: two knobs that must stay ordered, and the shorter one decides anyway.
- A `Paused` lifecycle state: banned; `writesPausedSinceBootMs` already reports the pause.

## 4. Test plan {#test-plan}

Failing first. The new gtests go under the `CAS*` filter and use an injected clock; today each of these throws 210 or never waits.
- `CASRequests.QueryReadWaitsForRenewal`: the fence returns `NoBudget` until a second thread publishes a deadline.
  The read returns the object after exactly 1 attempt. Today it throws "no lease budget for one more request".
- `CASRequests.QueryWriteReissueWaitsForRenewal`: an ambiguous PUT, then `NoBudget`, then a renewal. The result is `Committed`, not `GaveUp(Lease)`.
- `CASMountRuntime.AwaitBudgetWakes`:
  - `publishRenewedDeadline` returns `Ok`;
  - `tripMountLost` and `armMountFence` return `LostOrRearmed` at once, not at the bound;
  - the bound returns `NoBudget`;
  - cancel returns within one slice.
- `CASRequests.BackgroundNoBudgetStillFailsFast`: without a process-list element the read throws at once. This guards CAS-256 behavior.
- `CASRetry.StandardWindowIsThePlaneTimeout`: with 150000 configured, the deadline is now + 150000.

Fault-proxy stand (`tmp/cas405/stand/exp.py`), with `cas_operation_timeout_ms` left at its default:
- **Acceptance:** `hang 120` on all nodes and `iso ch2` with `gc 10`, lease 120/40. Expected:
  - insert errors 0 during and after the outage;
  - `CASMountLeaseLost` 0 and `CASRemountAttempts` 0;
  - oracle exact, fsck `dangling=0`;
  - max insert latency at most outage + 15 s; `CASLeaseWaitMicroseconds` > 0.
- **Also:** 30/10 `hang 120` on all nodes gives 0 errors (GC cannot fence anyone in a full outage). A single-node outage
  with 30/10 is still fenced at ~49 s, so CAS-405.3 (lease 120/40 s) stays required. `503 120` and `readstall 100` give 0 errors.
- **Negative cases:**
  - `hang 200`: the first errors come about 180 s into a wait, with the new text;
  - `KILL QUERY` on a waiting insert returns in under 1 s;
  - server stop during a pause finishes without waiting out the timeout.

## 5. Decisions {#decisions}

1. **Who waits:** only threads with a process-list element. Merges, fetches, cleanup, GC and renewal fail fast;
   CAS-256 and CAS-405.1 pace them.
2. **Default `cas_operation_timeout_ms` = 180000.** A load balancer with a shorter idle timeout (AWS ALB: 60 s)
   drops a waiting HTTP connection at either 150 s or 180 s; the docs say so.
3. **SELECTs wait too**, through the same entry gate.
4. **Pile-up:** accepted. `max_concurrent_queries` is the existing bound; the docs say waiting queries count toward it.
5. **Residual race** (in-memory verdict after the deadline with no request since the entry gate): accepted for the first
   version. If the stand acceptance shows errors on this path, add the wait at the ledger's runtime-acquisition entries
   (`CasRefLedger.cpp:625, 642, 4925, 4955, 5022`, none under a lock).

## 6. Delivery {#delivery}

- Branch `fix/antalya-26.6/cas-405-2-pause-is-a-wait` on top of #2517 (`d410298e2c4`); the PR opens after #2517
  merges, with the base merged in.
- Docs: `docs/en/antalya/cas/` pause description, `cas_operation_timeout_ms` in the configuration reference.
