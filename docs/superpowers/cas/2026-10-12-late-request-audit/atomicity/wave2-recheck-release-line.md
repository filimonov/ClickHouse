# Wave-2 findings rechecked on the release line `2a034cdb374` {#wave2-recheck}

Paths relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`; lines at `2a034cdb374`.

| Task | Verdict | Harm | Time assumption |
|---|---|---|---|
| CAS-378 | EXISTS, worse than reported | Acknowledged namespace-file loss. Inferred: committed-ref loss | Yes. Only scheduling closes the window |
| CAS-379 | EXISTS (cancel and remount forms) | Orphan namespace life or stray precommit (leak). No loss shown | Yes, plus the sweep backoff timer |
| CAS-380 | EXISTS, same order, 3 sites | None demonstrated | No |
| CAS-381 | DIFFERENT SHAPE: the mid-FORGET re-arm | Writes admitted after FORGET began. No loss shown | No |

## CAS-378: a delayed insertion republishes a retired life

**Interleaving** (`Pool/CasRefLedger.cpp`):
1. Reader R runs `acquireReadableRefTableRuntime` (614). It finds no resident runtime and takes the cold path. The first and second catalog GETs both see the row `Live` for life L. No lock is held across the GETs.
2. R is descheduled after the second GET (650-668) and before `acquireRefTableRuntime` (566) takes `ref_queue_mutex`.
3. Meanwhile:
   - `dropNamespaceImpl` (5094) installs its own runtime for L, sets `Removing`, and appends the terminal.
   - GC drains the row and calls `invalidateRemovedCatalogLife` (`Gc/CasGc.cpp:691/4435`). That erases the slot by pointer (721).
4. R takes `ref_queue_mutex`. The fence generation is unchanged and the slot is empty, so R emplaces a fresh runtime for L (590-596).
   - `reconcileCatalogCut` looks only at closed runtimes, so it never revisits this one.

The cold mutable path has the same gap. `namespaceLife` (4919) does one catalog GET at 4956, then calls `acquireRefTableRuntime` at 4967.

**Harm**
- `namespaceLife`'s warm path (4921-4946) returns `rt->life` with no lifecycle or catalog check. Recovery (1429) checks only `catalog_life_invalidated`.
- A table re-created with the same UUID therefore gets the dead life L.
- `ContentAddressedTransaction.cpp:842-851` writes `putNamespaceFile(L, ...)` and acknowledges it. `Gc/CasNamespaceJanitor.cpp:103` deletes every `_files/`, stream or `_ckpt` key whose life is absent from the catalog cut. The acknowledged bytes are later deleted.
- Worse, a ref append on that runtime sees a non-`Live` lifecycle and prepends `NamespaceBirth`. The state machine allows this after `Removed` (`Pool/CasRefProtocol.cpp:375`). A part could be promoted into a life the janitor sweeps, which would be committed-ref loss. This is inferred: the full chain has not been run.

**Gtest.** No production hook sits after the second GET. `readable_catalog_after_observation_hook_for_test` (643) sits between the two GETs, so the second GET catches anything injected there. Use a test backend instead:
- Subclass `InMemoryBackend` with a `read` override that fires after the base read returns, on the second catalog-key GET. Pattern: `MountSlotRaceBackend` (`src/Disks/tests/cas_test_helpers.h:2671`).
- In that callback, run `dropNamespace` and the GC drain (or `invalidateRemovedCatalogLife`).
- Then assert that `namespaceLife` returns the retired life and that the janitor deletes a `putNamespaceFile` the test saw acknowledged.

**Fix sketch**
- Keep a per-name retirement record in `RefNameSlot` instead of erasing the slot, for example `retired_incarnation` or a retire sequence number bumped under `ref_queue_mutex`.
- The cold paths capture that sequence before the first GET. `acquireRefTableRuntime` refuses with retry-later if it moved or if `life` is retired.
- Also: warm `namespaceLife` refuses a runtime whose recovered state is terminal.

## CAS-379: build liveness is checked only at entry

**Cancel form** (`Pool/CasPartWriteTxn.cpp:702`, `Pool/CasRefLedger.cpp:2040`):
1. `precommitAdd` calls `requireAlive` (704), records its intent, then `appendRefOps` → `acquireMutableRefTableRuntime` → `namespaceLife`.
2. Before the runtime is captured, `dropNamespaceImpl` finishes:
   - the terminal is durable;
   - `cancel_inflight_builds` sets `cancelled` (5329);
   - GC drains the row and erases the slot.
3. `namespaceLife` goes cold. The row is absent, so `resolveNamespaceLife` births a new life L2 (1324).
4. The closure (747-797) captures only `id_staged_by_this_txn` and never re-reads `cancelled`. On the empty L2 state it emits `NamespaceBirth` plus a precommit add.
5. Result: a cancelled build births a catalog life for a dropped table and owns a precommit in it.
   - The next `promote` fails at `requireAlive`, and the writer cleanup duty removes the precommit.
   - The L2 catalog row stays `Live` with no table to drop it. This is a leak.
   - If another actor re-created the table, the stray precommit lands in that successor life until the duty settles.

No committed loss: a promote in L2 needs a precommit in L2, and the sticky `cancelled` stops the build's later `requireAlive`.

**Remount form** (`promote`, 815):
1. `requireAlive` checks `epoch == liveWriterEpoch` at entry. A manifest GET (an I/O window) follows.
2. A self-remount (`Pool/CasPool.cpp:1233`) can complete in that window. The append then captures the new-epoch runtime, and the closure checks only that `{ref, id.ref}` is still a precommit.
3. The old-epoch precommit survives replay. The stale sweep that would remove it is bypassable in two ways:
   - `maybeSweepStalePrecommits` (4690) clears `needs_stale_precommit_sweep` before the sweep finishes, so a concurrent caller skips it.
   - During the sweep's backoff window (a timer), callers skip it entirely.
4. Result: an old-mount build promotes under the new epoch. Its blobs remain protected by the precommit edge. No loss is demonstrated; the "superseded build fails closed" contract is violated.

**Gtest**
- Existing hook `setAppendAfterRuntimeCaptureHookForTest` (`Pool/CasPool.h:1027`) fires after capture. That is too late for the cancel form.
- Cancel form: use a backend `before_get` gate on the catalog key with an evicted runtime (`MountSlotRaceBackend`). In the gate, run `dropNamespace` plus the drain.
- Remount form: gate the promote's manifest GET and run `tryRemountOnce` inside it. Hold the sweep in flight with `setCarveHookForTest`, or arm the backoff with an injected `boot_ms_fn`.

**Fix sketch**
- Give the closures `cancelled` and `epoch` (pass the build's `shared_ptr` or an atomic snapshot).
- Re-check both in the closure. It runs under the leader's serialization, against the exact runtime and state that will be appended.
- Also require `rt->life` to equal the life `requireAlive` admitted, captured at build start.
- Make the stale sweep's flag clear on completion, not on start, and serialize concurrent callers behind it.

## CAS-380: `Faulted` is published before the fence trips

The same order exists at three sites on the release line. Each publishes `lane_state = Faulted` under `state_mutex`, releases it, and only then calls `on_impossible_interference`:
- `resolveWedgeOnce` foreign occupant: 2530, then 2639.
- The invalid-lane-state arm: 3486, then 3488.
- The `commitRefChunk` foreign occupant: 3868, then 3874.

The reaction is `Pool::reportImpossibleInterference` (`Pool/CasPool.cpp:1758`). It logs, emits the event, then calls `tripMountLost` and `scheduleRemount`. Release line: remount; series tip: process exit. So the window also covers the log and the event emit.

**Harm: none demonstrated.**
- In the window, table B's `confirmExactRef` (rule 6 at 541) still sees `fence_ok_fn()` true and can answer `Yes`.
- Nothing causally orders B's certification after A's observation, so this is the same as B certifying just before A's GET.
- The issue is a model-correspondence problem: `FaultHasExit` is atomic in the model but not in the code.

**Gtest.** Pass a test `CasEventSink` to the pool. The `ForeignInterference` event fires inside the window, so in the sink assert that A's lane is `Faulted` while `store->mayMutate()` is still true. Drive the foreign occupant with the existing foreign-interference tests in `src/Disks/tests/gtest_cas_ref_writer.cpp` (around 1877-2062).

**Fix sketch**
- Split the reaction. A nothrow `fence_trip_fn` (`tripMountLost`) goes before each `Faulted` store.
- In `resolveWedgeOnce`, call it inside the allocation-free region before the store; it is two atomics.
- In `commitRefChunk`, call it before taking the lock.
- Log, event and remount scheduling follow.

## CAS-381: terminal-latch admission window

`openAfterReacquire` and `latchMountEnd` do not exist on the release line, so the series' interleaving cannot occur. The analogous window is the mid-FORGET re-arm, which the code documents (`Pool/CasPool.cpp:1167-1177`, 1376-1383):
1. `forgetDisk` (1125) calls `publishVanishedIntent` and then `tripMountLost` (trip 1, at 1159).
2. A `tryRemountOnce` already past its `isVanished()` gate (1331) holds `remount_mutex`. FORGET does not take that lock until step 6.
3. That remount reaches `armMountFence` (1552). `Pool/CasMountRuntime.cpp:171` bumps the generation and stores `lost=false`; it never checks `vanished_intent`.
4. `noteRemounted` publishes `Live`.
5. Admission does not consult the terminal intent (`remountTerminal`), so durable writes are admitted again:
   - `mayMutate` (96) and `admit` (138) check only `lost`, the generation and the deadline;
   - `throwIfLifecycleTerminal` (357) checks the state, which is still non-terminal until step 6.
6. The window closes only at trip 2 (1177). That comes after `stop_and_join_gc` and the remount-worker join.

Renewal-loss trips cannot interleave here: the renewal worker is parked during `tryRemountOnce`. `reportImpossibleInterference` from another thread can, but it also schedules another remount.

**Harm.** Writes admitted after FORGET started, under a lease the reclaim really holds. FORGET then drains the lanes, and a successor recovers the log. No loss is shown; FORGET's "no durable write after step 2" contract is broken for up to one GC join plus one remount step.

**Gtest.** Extend `CASForget.ForgetReLatchesFenceAfterAReclaimReachesArmMountFence` (`src/Disks/tests/gtest_cas_forget.cpp:410`). Inside the `stop_and_join_gc` callback, after `tryRemountOnce()` returns true:
- assert `store->mayMutate()` is true while `vanishedIntentPublished()` is true;
- perform a `putNamespaceFile` or a ref append and assert that it succeeds.

The existing `setArmMountFenceInterpositionHookForTest` gives a finer point inside `armMountFence`.

**Fix sketch.** In `armMountFence`, publish `lost=false` only if `!vanished_intent`. The check and the clear must be serialized with `publishVanishedIntent`, which uses `lockTerminalPublication`. Re-check after the store and trip again if the intent appeared. Simplest: fold `vanished_intent` into `mayMutate` and `admit`. Trip 2 then becomes redundant.
