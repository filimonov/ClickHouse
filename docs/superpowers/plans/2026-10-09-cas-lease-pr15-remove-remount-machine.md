# CAS lease PR 15: remove the remount machine — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** delete the self-remount: the second thread (`remountLoop`), parking, `DriverLease`, the nine
`RenewalDriverState` values, the request generations, `Pool::tryRemountOnce` with its re-anchor and observation
window, the states only it entered (`IdentityLost`, `VanishedReplaced`), and the test seams. Every loss end is
already handled elsewhere: R8 reacquires in the lease thread (PR 14), R9–R18 stop or close (PR 12). The one end that
still reaches the remount on this base — a memory-limit failure of a background renewal — needs PR 3.
**Architecture:** one renewal thread. A renewal is one of four kinds (`Startup`, `Direct`, `Worker`, `Reacquire`). One
`bool renewer_in_use` under `driver_mutex` takes over from the driver-state machine. The worker loop renews, runs an
owed reacquire, or exits. A terminal renewal that is not a loss, not R8 and not a stop is a defect: `local_defect`. A
memory-limit failure goes to the loop's memory-limit catch.
**Tech stack:** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend` seams, praktika integration, TLA+ (`CaDiskLifecycle`).

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
— §7 row 15 and its test row («15, 16»), §5 «Что удаляется», §2 «Потеря» (R8–R18), «Получение заново» (R19–R24),
«Старт», §1 «Настройки» (`cas_unsafe_remount_no_delay`), §9. Backlog: CAS-225, CAS-235, CAS-160, CAS-350, CAS-355, CAS-179.

**Base:** `fix/antalya-26.6/cas-reacquire-in-lease-thread` (PR 14) at execution time (`55fb37682f5` at plan time; it
contains PRs 1, 4, 7, 8, 9a, 10 v2, 11a, 11b, 12, 13, 14). **Branch:** `fix/antalya-26.6/cas-remove-remount-machine`.
`$WT` = the executor's own worktree (never `cas-janitor-budget`, which is in use; read it only with `git show <head>:<path>`).
`$CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`. `$T` = `src/Disks/tests`.
`$M` = `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/models`. Every commit message ends with:

```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

**Files outside `$CA`:** `src/Common/ProfileEvents.cpp` (the four `CASRemount*` and `CASIdentityLost`: nothing increments
them), `src/Common/setThreadName.h` (`CAS_REMOUNT`), `src/Storages/System/StorageSystemContentAddressedMounts.cpp` (two
column descriptions); tests, two integration modules, `docs/en/`, the model in the master worktree.

## Global Constraints

- One topic per PR; the PR is self-contained and leaves a working product; no behaviour outside the
  spec row changes. Changelog: one sentence, category per `.github/PULL_REQUEST_TEMPLATE.md`.
- Allman braces; no `sleep` to fix races; tests use injected clocks and deterministic barriers.
- A test that expects `LOGICAL_ERROR` is a death test under `DEBUG_OR_SANITIZER_BUILD` (the error
  aborts there); prefer injecting `NOT_IMPLEMENTED`/`BAD_ARGUMENTS` where the rule allows.
- Shared code outside `CA/` (`src/IO/S3`, `base/poco`, `src/Daemon`, `src/Storages/System`) is a fork
  patch: minimal, motivated, portable; say in the plan which files outside `CA/` are touched and why.
- Comments explain why, not what; no references to plans, reviews, backlog ids or history in code.
- Prose style for PR text and comments: point first, plain words, backticks for names, `f` not `f()`.
- Build: `ninja -C build <target>` without `-j`, output to a log file in the build dir; the gate is
  `build/src/unit_tests_dbms --gtest_filter='CAS*'` (release) and the same in `build_asan`.
- Commit exact files only (`git add -- <path> …`), never a directory, never `-A`; no rebase/amend.
- Every build, test and gate command runs with `TMPDIR=<build_dir>/tmp`.
- After every task: release and ASan `CAS*` green on the committed tree. A deletion task never leaves a red.
- A test is deleted only when the inventory below names it; a retarget that cannot express the original property
  stops the task and is reported. It is never silently deleted.

## Review Focus

- **Failure asymmetry.** Deleting the only exit from a reachable state wedges a disk silently (closed, shown as
  `paused`, never recovers); keeping dead code costs reading time. So every deletion names its replacement exit and
  has a behaviour test that runs on the base and after («Proof» below).
- **The residual end.** On the base, a background renewal that ends terminal, with no loss and no R8, still requests a
  remount (`DriverLease::finish`, the `WorkerIdle` arm). Producers: a liveness refusal (stop, FORGET intent, an end
  latched elsewhere), for which the base never ran the remount, and `MEMORY_LIMIT_EXCEEDED`, for which it was the only
  exit (Code vs spec 1).
- **No false `local_defect`.** It ends the process without a shutdown. A FORGET or a stop racing a background renewal
  must not reach it; Task 2 pins both races before anything is deleted.

## Code vs spec (flagged; the plan follows the spec, the owner rulings, and the dispatch)

1. **PR 3 dependency.** Spec row 15 depends on 14 only. Code: `renewOn` turns `MEMORY_LIMIT_EXCEEDED` into a terminal
   result without a slot; `DriverLease::finish` requests a remount, the only exit. PR 3 = Altinity #2511
   (`949578b2d73`, `LockMemoryExceptionInThread(Global)` in `renewalLoop`), merged into `antalya-26.6-20261008`, absent
   from `antalya-26.6` and this base. Plan: Task 1 stops without it. A residual memory-limit end goes to the loop's
   memory-limit catch (fence tripped, `chassert(false)`, thread ends), as PR 14 ruling 5 did for the reacquire; not to
   `local_defect`, which spec R17 excludes it from.
2. **Spec §5 «`CASRemount*` → `CASMountReacquire*` (PR 16)».** After this PR nothing increments `CASRemount*`. Plan:
   delete the four events, the `mount_remount` event type and `CAS_REMOUNT`; PR 16 adds `CASMountReacquire*` if wanted.
   Always-zero counters rejected: they would say "0 remounts" while reacquires happen.
3. **States without a producer** (not in spec §5): `PoolLifecycle::IdentityLost`, `VanishedReplaced`, `MountEnd::Replaced`,
   `CASIdentityLost`, `system.cas_mounts` `identity_lost` and `vanished/replaced` — entered only by `tryRemountOnce`;
   the spec state table already lacks them. Plan: delete in Task 6, which can become its own PR.
4. **Spec §5 «re-anchor … общим кодом».** Two renewals stay: startup's bounded `renew` (spec «Старт») and the
   reacquire's `renewUntilDefinitive` (PR 14 deviation 1). Plan: share the window predicate; delete the remount re-anchor.
5. **CAS-160 #2** (note: "not touched, format frozen"). `MountFence` is in memory; no encoder reads it, so the on-store
   bytes are identical by construction (Task 7 greps). Its one reader, `fencedIdentity`, serves only `tryRemountOnce`.
   Plan: delete both fields; `armMountFence` takes the deadline only.
6. **CAS-179** (PR 13 ruling: «PR 15 removes it»). `quiesceRefTablesForRemount` stays: the reacquire's `CloseOldEpoch`
   calls it and spec R24 leaves quiescence unbounded. The bound stays open.
7. **CAS-350.** The remount window goes. The reacquire supersedes before it publishes `E'`, but a leader past both
   checks before `CloseOldEpoch` that reads `live_epoch_fn` after `StartRenewer` may still hit the gate. Not
   established; AC #3 stays open.
8. **CAS-355** unchanged: startup's per-disk observation is untouched. **CAS-225** obsolete: the seam is deleted.
9. **`cas_unsafe_remount_no_delay`** stays a startup setting: take a slot with our uuid and another epoch without the
   observation window. The remount never passed it; the reacquire ignores it (R20). Nothing changes.
10. **`cas_mounts.md`** still lists `remounting` (PR 14 changed only the column description): fixed in Task 10.
11. **Model:** spec §9 has no `CaDiskLifecycle` step 3; added per the dispatch (Task 9).
12. **Size:** spec −600; inventory ≈ −1000 net production (`tryRemountOnce` alone ≈ 360 lines).
13. **FORGET's second trip** guards a remount that reclaimed inside the join window. `openAfterReacquire` re-checks the
    intent under `driver_mutex`, which `publishVanishedIntent` takes. Plan: delete it only after Task 9's prediction
    row is green (Task 9 runs first, in the master worktree).

## Inventory (the content Task 1 verifies)

**Production symbols (52).** Exit column: the state that symbol left, and what leaves it now.

| Symbol (`$CA/Pool/…` unless named) | Left which state | Now left by |
|---|---|---|
| `CasMountRuntime::remountLoop`, `remount_worker`, `remount_attempt` (ctor param), `Pool::tryRemountOnce`, `remount_attempt_sequence` | `TransientNotLive` with a request pending | R8: `reacquireUntilSettled` on the renewal thread (PR 14); others: see the residual row |
| `scheduleRemount`, `scheduleRemountForTest` (both classes), `scheduleRemountCallCountForTest`, `schedule_remount_calls_for_test`, `remount_requested_generation`, `remount_handled_generation`, `remountRequestedGenerationForTest` | request bookkeeping | no requests exist |
| `RenewalDriverState` (9 values), `DriverLease`, `AdmittedRenewerCall`, `admitRenewerCall`, `renewalWorkerMayRenew`, `renewal_driver_state`, `renewalDriverStateForTest`, `waitForRenewalDriverStateForTest` | `Parked` / `ParkRequested` (worker excluded while the remount ran) | one thread: exclusion is structural; `renewer_in_use` guards replacement |
| `renewRenewerForRemountOnce`, `MountLeaseRenewer::renewForRemount`, the `RemountCall` arm of `renewRenewerOnce` | the remount re-anchor | reacquire: `renewForReacquireOnce` (PR 14); startup: `renewRenewerForStartupOnce` |
| `Pool::stopOnRemountObservation` | remount found twin/foreign/unknown body → `Stopping` | `classifyReacquireSlot` → `lose` (PR 14) |
| the `LifecycleGateVerdict::Replaced`/`IdentityLost` arms of `tryRemountOnce`, `CasMountRuntime::enterIdentityLost`, `PoolLifecycle::IdentityLost`, `PoolLifecycle::VanishedReplaced`, `MountEnd::Replaced`, `CASIdentityLost` | `TransientNotLive` → terminal identity loss | reacquire `ProbeIdentity` → `MountEnd::PoolIdentityLost` (stop) |
| `deliverDeferredMountRenewObservability`, the `deferred` field of `MountRenewObservabilityContext` and `…Configuration`, the `deferred` parameter of `configureMountRenewObservability` | delivery after `remount_mutex` released | nothing runs a parked redo |
| `CASRemountAttempts`, `CASRemountSucceeded`, `CASRemountFailed`, `CASRemountHeldTransient`, `CasEventType::MountRemount`, `ThreadName::CAS_REMOUNT` | observability of the remount | reacquire's `INFO`/`WARNING` lines (PR 14) |
| `MountConfig::remount_parked_hook_for_test`, `renewal_parked_predicate_false_hook_for_test`, `renewal_before_driver_lock_hook_for_test`, `renewal_admitted_hook_for_test`, `renewal_terminal_deposited_hook_for_test`, `terminal_publication_waiting_for_driver_lock_hook_for_test`, `terminal_publication_driver_lock_contended_hook_for_test`, `terminal_publication_driver_lock_acquired_hook_for_test` | seams of the races above | — |
| `MountFence::server_uuid`, `MountFence::writer_epoch`, `fencedIdentity` | read only by the remount | — (CAS-160 #2) |
| `forgetDisk`'s second `tripMountLost` | a reclaim armed inside the join window | `openAfterReacquire` refuses after the intent |
| the `WorkerIdle` non-R8 arm of `DriverLease::finish` (`++remount_requested_generation`) | residual terminal renewal | stop/intent/end: loop exits (as on the base); memory: loop catch; other: `local_defect` |

Stays (used by the reacquire, renamed in Task 8): `remount_mutex`, `quiesceRefTablesForRemount`,
`cancelRecoveriesAndAwaitQuiescence`, `superseded_by_remount`, `remount_quiesce_hook_for_test`,
`probePoolLifecycleGate`, `noteLeaseLost`, `noteRemounted`, `TransientNotLive`, `remountTerminal`, `installRenewer`,
`startRenewer`, `beginShutdownForTest`, `workersRunningForTest` (one worker).

**Tests (the Task 1 script prints 119 lines: 111 `TEST`s and 8 file-scope helpers).** Dispositions:

- **Delete (51)** — machine-only, or replaced by a named reacquire test: `CASPoolRemount.*` (22) except the nine named in
  the other lists (`DirectRenewCannotRaceWorkerStartOrRenewerReplacement`, `DirectRenewIsRefusedForBackgroundConfiguredRuntimeAfterStop`,
  `LiveForgetDoesNotCountOperationalLeaseLoss`, `SameRuntimeReentrantForeignConflictSinkCannotReplaceTerminalOutcome` and
  `ThrowingForeignConflictSinkCannotReplaceTerminalOutcome` stay untouched); `CASMountRuntime.{AnOldEpochRenewalDoesNotMoveTheNewEpochDeadline,
  AParkRequestEndsTheCredentialRefusalWaitAtOnce, ARemountRequestEndsARenewalRetryingPastTheLease}`;
  `CASMountRemount.SupersededIncarnationDoesNotReclaimALiveSuccessor`; `CASRemountWaits.{DrainedRemountPaysNoWait,
  UnresolvedWedgeRemountPaysNoWaitEither}`; `CASMountStop.ARemount*` (4; reacquire: `AStorm503OnTheCounterAllocatesOneNumber`,
  `AForeignBodyStops`, `ATwinThatTookAndReleasedTheSlotStops`); `CASOwnershipReacquire.RemountDoesNotRecreate*` (2;
  reacquire: `AnAbsentCounterIsPoolIdentityLost`, `AnAbsentAnchorIsPoolIdentityLostAndNotCreated`);
  `CASMountLease.RemountRenewalIsAdmittedOffTheMountFence`; `CASLosslessClaim.RemountWritesTheSlotOnce`;
  `CASLifecycleCondition.*` (6; Task 2's X7–X9); `CASForget.{ForgetIntentBlocksNaturalReplacedPromotion,
  ForgetOnIdentityLostPoolVanishesForgotten, ForgetRacingActiveRemountThreadCompletesBounded,
  ForgetReLatchesFenceAfterAReclaimReachesArmMountFence}` (reacquire: `ForgetDuringReacquireNeverOpens`);
  `CASGCStopStart.SchedulerSelfExitsOn{IdentityLost,NaturalVanished}`; `CASLifecycleSnapshot.{IdentityLostHasDetailAndSince,
  NaturalIdentityLostMatchesThrowDetail}`; `CASOperationGate.EveryClassThrows668OnIdentityLost`.
- **Retarget the epoch change to `reacquireAfterGcFence` (17):** `CASRefWriterRemount.*` (5),
  `CASRefWriterStalePrecommitSweep.VerifiedCleanSweepClearsFlagWithoutEvents`,
  `CASRefWriterAppendLane.I1AppendCorruptionSurfacesAndFencesTheMountForRemount`, `CASRefRecoveryCasWalk.{FenceBumpedAfterCkptCasBeforeInstallPublishesNoState,
  FenceBumpedMidWalkRefusesTheInstallAndTheRetrySucceeds}`, `CASRetirementSweep.AStragglerFromTheDyingEpochLosesItsCreateToTheRecoverySeal`,
  `CASPoolRemount.OldEpochBuildFailsClosedAfterRemount`, `CASRemountWaits.ALateTouchedTableClosesEveryDeadEpochInBandHoweverItsPredecessorsDied`,
  `CASGCStopStart.OperatorStopPersistsAcrossTransientRecovery`, `CASOperationGate.{GcSchedulerIsQuiescentReflectsRoundInFlight,
  RemoveThrowsDuringTransientAndDrainsAfterRecovery}`, `CASMountOpenWaits.PublicationHorizonUsesTheEnvelope` and
  `CASLosslessClaim.GoldenOpenAndRemountPaths` (drop the remount half, keep the open half). `FailedRemountPublishesNoRuntimeUnderFenceLossGeneration`
  (one of the five) becomes a reacquire that stops at `AllocateEpoch` (the counter write throws `Poco::TimeoutException` once): `runReacquireForTest` returns
  `Retry`, then the original "no runtime" assertions.
- **Drop the remount-count assertion only (23):** `CASRefWedgeEveryAttempt.*` (6), `CASRefLaneFault.*` (4),
  `CASRefLane.LivenessPredicateWithoutGenerationTermStillRefusesARetiredRuntime`, `CASAnomalyPolicy.*` (2),
  `CASPartFolderAccess.*` (2), `CASDecommission.AnAdminRenewalThatFindsATwinClosesWithoutAStop`,
  `CASRefRecoveryCasWalk.RetainedWriterAttemptMeetingASuccessorSealFaultsTheMount`,
  `CASPool.RenewWatermarkOnceRefreshesFenceAndDepositsOneFailure`, `CASMountRemount.CutoffFencesWithoutRenewals`,
  `CASMountStop.{EachSlotLossLatchesThenStopsThenLogs, OurOwnFenceIsReacquiredAndNothingMore}`,
  `CASReacquire.{TheRenewalThreadReacquiresWithoutTheRemountWorker, ALossFoundByTheReacquireStopsTheServer}`. Rule: an
  assertion that the count did **not** move is deleted with its capture; an assertion that it moved stops the task.
- **Task 5/6/8 edits (20, plus `CASPoolRemount.TeardownJoinsBothWorkersBeforeRelease`, which the script does not match):** one worker (`CASPoolRemount.{WorkerConstructionRollbackFailsOpenClosed, TeardownJoinsBothWorkersBeforeRelease}`);
  `VanishedReplaced` → `VanishedForgotten` or `IdentityLost` → `Stopping`: `CASOperationGate.{ContentReadOnVanishedThrowsTypedPerReasonMessage,
  GcEntryPointsRefuseOnNotLive, ProbesOnVanishedAnswerAbsentEmpty, RemovesOnVanishedAreNoOpSuccessBackendUntouched,
  TransientRefusalIsUpstreamRetryableTerminalIsNot, TryGetInManifestBytesPropagatesTypedError}`,
  `CASFsckRunning.FsckOnNotLiveDiskRefusesTransientRetryableAndIdentityLostTerminal`, `CASGCStopStart.StopAndDestroyCleanAfterSelfExit`,
  `CASLifecycleSnapshot.{OldLifecycleValuesMapToTheNewVocabulary, PerformsZeroBackendOps}`,
  `CASPoolRemount.VanishedReasonPreparationFailureLeavesTerminalTransitionRetryable`, `CASReacquire.AnEndWithoutALossLatchedDuringReacquireNeverOpens`
  (`Replaced` → `Forgotten`); the remaining matches are name-only (`TransientNotLive`, `PoolIdentityLost`, texts) and
  change only in Task 8 (7 tests). The 8 file-scope helpers follow the disposition of the tests that call them. Every
  `RuntimeUnderTest` construction (28) loses its remount callback.

**Docs (9 files):** `docs/en/antalya/cas/{architecture/mounts-and-leases,architecture/manifests-and-refs,operations/debugging,
operations/monitoring,operations/troubleshooting,configuration}.md`, `docs/en/antalya/metrics_and_events.md`, `docs/en/operations/system-tables/{cas_mounts,cas_log}.md`.

### Proof: no deleted path was the only exit

| Former remount entry | Reached on the base when | Exit after this PR | Test (Task 2 unless named) |
|---|---|---|---|
| E1 R8, own fenced body | renewal `Conflict` → `FencedByGc` with certificate | reacquire in the lease thread (PR 14) | `TheRenewalThreadReacquiresWithoutTheRemountWorker` (Task 3) |
| E2 R9–R15, R17 | renewal or ref lane | latch → stop (PR 12) | `EachSlotLossLatchesThenStopsThenLogs`, `CASRefLaneFault.*` |
| E3a liveness refusal | stop, FORGET intent, end latched by another thread | loop exits; the base never ran a remount for these (`remountTerminal`/stop gate the request) | X3 `ForgetDuringAWorkerRenewalClosesWithoutAStop`, X4 `AStopDuringAWorkerRenewalEndsWithoutAnEnd` |
| E3b `MEMORY_LIMIT_EXCEEDED` in a worker renewal | memory pressure, without PR 3 | none needed: under PR 3's lock the renewal does not end (`std::bad_alloc` stays `local_defect`) | PR 3's `CASMountRuntime.{GlobalMemoryLimitDoesNotEndTheLeaseThread, BadAllocInsideTheRenewalStaysTerminal}` |
| E3c any other non-loss terminal | none in production (an external `tripMountLost` with workers runs only in deleted tests) | `local_defect` | X6 `CASMountRuntime.AWorkerRenewalEndedWithoutACauseIsALocalDefect` (Task 4, red on the base) |
| E4 remount outcomes: reclaim / twin / identity lost / replaced / held transient | inside `tryRemountOnce` | reacquire `Open` / `lose` / `PoolIdentityLost` / `PoolIdentityLost` / `Retry` | PR 14 suite, X7 `AForeignPoolMetaStopsTheReacquire`, X8 `AnUnreadablePoolMetaRetriesTheReacquire`, X9 `ChangedAlgosStillReacquire` |
| E5 `DirectCall`/`StartupCall` terminal | tests (no workers) / open | trip only (no request on the base either) | `DirectAndStartupTerminalFailuresRethrowTypedExceptions` |

States: `TransientNotLive` is entered only by `CloseOldEpoch` (FORGET skips `noteLeaseLost`) and left by
`openAfterReacquire`, `lose`, FORGET or stop. `Parked`, `IdentityLost`, `VanishedReplaced` lose their only entry.
**Not covered:** interleavings inside the reacquire (PR 14, model step 2); memory-limit throws under PR 3's lock (PR 3's tests).

## Task 1: inventory and preconditions (no commit)

- [ ] `git -C $WT switch -c fix/antalya-26.6/cas-remove-remount-machine <PR 14 head>`; `cmake .` in `build`, `build_asan`.
- [ ] PR 3 check: `git -C $WT merge-base --is-ancestor 949578b2d73 HEAD || git -C $WT log --oneline -1 --grep='memory limit' -- $CA/Pool/CasMountRuntime.cpp`, and
  `git -C $WT grep -n 'LockMemoryExceptionInThread' -- $CA/Pool/CasMountRuntime.cpp` must print the `renewalLoop` line.
  `CASMountRuntime.GlobalMemoryLimitDoesNotEndTheLeaseThread` must exist in `$T/gtest_cas_pool.cpp`. If not: stop and
  report "PR 3 (#2511) is not in the base" (Code vs spec 1).
- [ ] Test inventory: in `$WT`, for every `$T/*.cpp`, print `file: Suite,Name` of each `TEST` whose body matches
  `scheduleRemount|remountRequestedGeneration|remount_attempt|RenewalDriverState|ParkRequested|noteRemounted|fencedIdentity|tryRemountOnce|IdentityLost|VanishedReplaced|CASRemount|renewForRemount|RemountOnce|remount_parked|parked_predicate|terminal_deposited|before_driver_lock|renewal_admitted_hook|workersRunningForTest|MountEnd::Replaced|mount_remount|MountRemount`
  (awk over `^TEST(_F|_P)?\(`). Expected: 119 lines, each named in exactly one disposition list above (the 8 helper
  lines have an empty test name). Any other line: stop and report.
- [ ] `git grep -n -i remount -- $CA src/Common src/Storages/System`: an unlisted non-comment hit stops the task.
  Baseline: build both, run the `CAS*` gate, record the counts (release 2805 / ASan 2809 at plan time).

## Task 2: a behaviour test per former exit, golden (≈ +190 test lines)

**Files:** `$T/gtest_cas_reacquire.cpp`. **Consumes:** `Running`, `Fenced`, `put`, `read`, `kLayout`, `kSrid`.
All five are green on the base and stay green; the commit says so.

- [ ] Add, inside the anonymous namespace after `OrderRecorder`:

```cpp
/// Holds the first mount-slot write after `armed` until `gate` is released, then fails it before it reaches
/// the store, as a `503` does: the renewal's next attempt re-checks its liveness.
class HeldRenewalBackend : public InMemoryBackend
{
public:
    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid) && armed.exchange(false))
        {
            gate.arriveAndWait();
            throw Poco::TimeoutException("injected: renewal held");
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }

    std::atomic<bool> armed{false};
    DB::Cas::tests::ManualBarrier gate;
};
```

- [ ] Tests (after `ALossFoundByTheReacquireStopsTheServer`):

```cpp
TEST(CASReacquire, ForgetDuringAWorkerRenewalClosesWithoutAStop)
{
    auto backend = std::make_shared<HeldRenewalBackend>();
    Running running(backend);
    Pool & store = *running.store;
    backend->armed = true;
    backend->gate.waitUntilArrived();

    /// FORGET publishes its intent and trips the fence before it stops the GC; the held renewal resumes only then.
    store.forgetDisk([&] { backend->gate.release(); }, "decommissioned by the test");

    EXPECT_EQ(store.lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_EQ(store.mountEnd(), MountEnd::Forgotten);
    EXPECT_EQ(DB::Cas::tests::processStopLog().graceful(), 0u);
    EXPECT_EQ(DB::Cas::tests::processStopLog().fatal(), 0u);
}

TEST(CASReacquire, AStopDuringAWorkerRenewalEndsWithoutAnEnd)
{
    auto backend = std::make_shared<HeldRenewalBackend>();
    Running running(backend);
    Pool & store = *running.store;
    const uint64_t epoch = store.writerEpoch();
    backend->armed = true;
    backend->gate.waitUntilArrived();
    const uint64_t changes = running.changes;

    store.beginShutdownForTest();
    backend->gate.release();

    ASSERT_TRUE(running.waitFor([&] { return running.changes > changes; }));
    EXPECT_EQ(store.mountEnd(), MountEnd::None);
    EXPECT_EQ(store.writerEpoch(), epoch);
    EXPECT_EQ(counterNext(*backend), epoch + 1);
    EXPECT_EQ(DB::Cas::tests::processStopLog().graceful(), 0u);
    EXPECT_EQ(DB::Cas::tests::processStopLog().fatal(), 0u);
}

TEST(CASReacquire, AForeignPoolMetaStopsTheReacquire)
{
    Fenced f;
    PoolMeta meta = decodePoolMeta(read(*f.backend, kLayout.poolMetaKey())->bytes);
    meta.pool_id = meta.pool_id + DB::UInt128(1);
    put(*f.backend, kLayout.poolMetaKey(), encodePoolMeta(meta));

    expectStoppedOn(f, reacquireUntilSettled(*f.store), MountEnd::PoolIdentityLost);
    EXPECT_EQ(counterNext(*f.backend), f.epoch + 1) << "no epoch is allocated after the identity probe fails";
}

TEST(CASReacquire, AnUnreadablePoolMetaRetriesTheReacquire)
{
    Fenced f;
    put(*f.backend, kLayout.poolMetaKey(), "not a pool meta");

    EXPECT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Retry);
    EXPECT_EQ(f.store->mountEnd(), MountEnd::None);
    EXPECT_FALSE(f.store->mayMutate());
    EXPECT_EQ(counterNext(*f.backend), f.epoch + 1);
}

TEST(CASReacquire, ChangedAlgosStillReacquire)
{
    Fenced f;
    PoolMeta meta = decodePoolMeta(read(*f.backend, kLayout.poolMetaKey())->bytes);
    const auto extra = static_cast<uint8_t>(BlobHashAlgo::XXH3_128);
    ASSERT_FALSE(std::binary_search(meta.algos_used.begin(), meta.algos_used.end(), extra));
    meta.algos_used.push_back(extra);
    std::sort(meta.algos_used.begin(), meta.algos_used.end());
    put(*f.backend, kLayout.poolMetaKey(), encodePoolMeta(meta));

    EXPECT_EQ(reacquireUntilSettled(*f.store), ReacquireOutcome::Open);
    EXPECT_EQ(f.store->writerEpoch(), f.epoch + 1);
}
```

  Includes if missing: `Formats/CasPoolMetaFormat.h`, `<algorithm>`. `Running` resets `processStopLog` itself.
- [ ] Run `TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASReacquire.*' > build/test_pr15_t2.log 2>&1`, then
  the same in `build_asan`. Expected: all pass on the base. If X3 or X4 fails on the base, the race behaves differently
  from the Proof table: stop and report (the rule in Task 4 depends on it).
- [ ] Commit `-- $T/gtest_cas_reacquire.cpp`: `Pin the CAS mount exits that replace the remount`.

## Task 3: retire the tests that drive the remount (≈ −2600 / +300 test lines)

**Files:** `$T/cas_test_helpers.h` and the inventory's test files. Production is untouched: every edit compiles on the base.

- [ ] Add to `cas_test_helpers.h` after `rearmMountFenceAfterAnomalyForTest`:

```cpp
/// Moves `store` to its next writer epoch the way production does: the GC fences our own body, the next renewal
/// reads it, and the reacquire opens the new epoch. Needs a pool without background workers.
inline void reacquireAfterGcFence(DB::Cas::Pool & store, DB::Cas::Backend & backend)
{
    using namespace DB::Cas;
    const String key = store.layout().mountKey(store.poolConfig().server_root_id);
    OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    ASSERT_TRUE(got.has_value());
    MountLease body = decodeMountLease(got->bytes);
    body.gc_fenced = true;
    ++body.seq;
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(key, encodeMountLease(body), got->etag, Retry::once())));
    EXPECT_THROW(store.renewWatermarkOnce(), DB::Exception);
    ReacquireOutcome outcome = ReacquireOutcome::Retry;
    for (size_t i = 0; i < 100 && outcome == ReacquireOutcome::Retry; ++i)
        outcome = store.runReacquireForTest();
    ASSERT_EQ(outcome, ReacquireOutcome::Open);
}
```

- [ ] Apply the inventory: delete the 51; in the 17 retargets replace `store->tryRemountOnce()` (and its `EXPECT_TRUE`)
  by `reacquireAfterGcFence(*store, *backend)` and keep every other assertion; drop the 23 count assertions; delete
  helpers left unused (`-Wunused-function` is an error).
- [ ] Rename `TheRenewalThreadReacquiresWithoutTheRemountWorker` → `TheRenewalThreadReacquiresAnOwnFence`.
- [ ] Build both; run the gate. Expected: green; count = baseline + 5 (Task 2) − 51.
- [ ] Commit the exact files: `Retire the CAS tests that drive the self-remount`.

## Task 4: delete the remount worker and `tryRemountOnce` (≈ −760 / +45 production)

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `$CA/Pool/CasPool.{h,cpp}`, `$CA/Pool/CasServerRoot.{h,cpp}`,
`$CA/Primitives/CasEvent.{h,cpp}`, `src/Common/ProfileEvents.cpp`, `src/Common/setThreadName.h`, `$T/gtest_cas_pool.cpp`
(X6, `RuntimeUnderTest` and its constructions), `$T/gtest_cas_event_log.cpp`.

- [ ] Red first (X6), in `gtest_cas_pool.cpp` after `CASMountRuntime.ALeaseExpiryIsLoggedOnceAndReported`. The test
  liveness override is the only producer of this end: production has none (Proof, E3c).

```cpp
/// A background renewal that ends with no loss, no fence of our own body and no stop has no recovery left:
/// the mount ends as a local defect instead of staying closed with nothing to reopen it.
TEST(CASMountRuntime, AWorkerRenewalEndedWithoutACauseIsALocalDefect)
{
    auto backend = std::make_shared<RuntimeRenewBackend>();
    const Layout layout("runtime-residual-end");
    uint64_t wall_ms = 1000;
    std::atomic<uint64_t> boot_ms{100};
    const UInt128 uuid{1};
    constexpr uint64_t ttl_ms = 10'000;
    const MountClaimResult claim = claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms,
        ttl_ms, {}, {}, {}, [&] { return boot_ms.load(); });
    ASSERT_EQ(claim.kind, MountClaimResult::Claimed);
    std::mutex mutex;
    std::condition_variable cv;
    bool stop_requested = false;
    DB::Cas::tests::processStopLog().reset([&]
    {
        {
            std::lock_guard lock(mutex);
            stop_requested = true;
        }
        cv.notify_all();
        return String{};
    });
    CasEventSink sink;
    RuntimeUnderTest runtime_holder(backend, layout,
        MountConfig{
            .mount_lease_ttl_ms = std::chrono::milliseconds(ttl_ms),
            .background_watermark = true,
            .boot_ms_fn = [&] { return boot_ms.load(); },
            .renewal_live_for_test = [] { return false; }},
        "test", sink, runtimeRenewBudget(), [] { return false; });   /// the remount callback; Task 4 removes it
    CasMountRuntime & runtime = *runtime_holder;
    runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
    const uint64_t anchor = runtime.startRenewer(*claim.claimed);
    runtime.armMountFence(uuid, 1, anchor + ttl_ms);
    boot_ms = anchor + 1'000;
    runtime.startBackgroundWorkers(std::chrono::milliseconds(1'000));

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(60), [&] { return stop_requested; }))
            << "the renewal end waited for a recovery";
    }
    EXPECT_EQ(runtime.mountEnd(), MountEnd::LocalDefect);
    EXPECT_EQ(DB::Cas::tests::processStopLog().fatal(), 1u);
    runtime.stopBackgroundWorkers();
    runtime.finishTeardown({.ref_lane_unsettled = true});
}
```

  Run in release and ASan: expected **red** on the base after the 60 s hang guard ("the renewal end waited for a
  recovery"): the base requests a remount, and the callback keeps failing. Record the text.
- [ ] `CasMountRuntime`: delete `remountLoop`, `remount_worker`, `remount_attempt`, both generations, `scheduleRemount*`,
  `schedule_remount_calls_for_test`, `remountRequestedGenerationForTest`, `renewRenewerForRemountOnce`, the remount arm of
  `renewRenewerOnce`, the second worker in `start/stopBackgroundWorkers`, and the generation bump in `DriverLease::finish`
  (its `tripMountLost()` stays). `workersRunningForTest` checks `renewal_worker` only.
- [ ] Residual rule in `renewalLoop`, where the worker's `renewRenewerOnce` returns (make it keep the result):

```cpp
            const MountRenewResult result = renewRenewerOnce(std::move(call), RenewalDriverState::WorkerCall,
                /*propagate_failure=*/false, /*worker_call=*/true);
            if (result.outcome == MountRenewOutcome::Terminal && mountEnd() == MountEnd::None && !reacquireOwed())
            {
                if (renewalCancelled() || vanishedIntentPublished())
                    return;
                /// Under the global memory lock this cannot be thrown; if it is, the handler below closes the disk.
                if (result.failure && getExceptionErrorCode(result.failure) == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    std::rethrow_exception(result.failure);
                if (latchMountEnd(MountEnd::LocalDefect))
                    reactToMountEnd(MountEnd::LocalDefect, std::nullopt,
                        "a background renewal ended without a loss, a fence of our own body or a stop");
                return;
            }
```

  `reacquireOwed()` = `std::lock_guard lock(driver_mutex); return owed_reacquire.has_value();` (private).
- [ ] `Pool`: delete `tryRemountOnce` with its finalizer, `remount_attempt_sequence`, `stopOnRemountObservation`, the three
  remount test seams, the constructor's remount callback and the `CASRemount*` externs. `forgetDisk`: delete the second
  `tripMountLost()`; the `chassert` names `CAS_LEASE_RENEWER` instead of `CAS_REMOUNT`; step comments say the reacquire
  abandons at its next stage. `CasServerRoot`: delete `renewForRemount`, `deliverDeferredMountRenewObservability`, the
  `deferred` fields and parameter (caller in `$T/gtest_cas_event_log.cpp` too) and the always-0 `remount_attempt_no`.
  Delete the four `CASRemount*` events, `CAS_REMOUNT`, `CasEventType::MountRemount` and `"mount_remount"`.
  `RuntimeUnderTest` stops forwarding a remount callback; every construction loses its lambda.
- [ ] Precondition for the `forgetDisk` edit: Task 9's prediction row is green. Build both; X6 green; gate green.
  Commit the exact files: `Remove the CAS self-remount and its second thread`.

## Task 5: one driver flag instead of `DriverLease` (≈ −260 / +110 production)

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `$CA/Pool/CasPool.{h,cpp}`, `$T/gtest_cas_pool.cpp`.
**Produces:** `enum class RenewalKind : uint8_t { Startup, Direct, Worker, Reacquire };` (private),
`MountLeaseRenewer * beginRenewal(RenewalKind kind)`, `void finishRenewal(RenewalKind kind, const MountRenewResult &, const MountLeaseRenewer *)`,
`bool renewalWindowFits(uint64_t deadline_boot_ms) const` (public, `CasMountRuntime`).

- [ ] Delete `RenewalDriverState`, `DriverLease`, `AdmittedRenewerCall`, `admitRenewerCall`, `renewalWorkerMayRenew`,
  `renewal_driver_state`, the two driver-state test seams, `lockTerminalPublication` (callers lock `driver_mutex`), and
  the eight `MountConfig` hooks of the inventory with their `PoolConfig` projections.
- [ ] `bool renewer_in_use` (under `driver_mutex`). `beginRenewal` throws `LOGICAL_ERROR` without an `Active` renewer,
  while `renewer_in_use`, for `Direct` when `config.background_watermark` (today's text), and for `Startup`/`Direct`
  while `workers_started`; then sets the flag. `installRenewer` and `renewerReset` throw while it is set.
- [ ] `finishRenewal` (replaces `DriverLease::finish`):

```cpp
void CasMountRuntime::finishRenewal(RenewalKind kind, const MountRenewResult & result, const MountLeaseRenewer * renewer)
{
    std::lock_guard lock(driver_mutex);
    renewer_in_use = false;
    if (result.outcome == MountRenewOutcome::Committed && renewer == mount_renewer.get())
    {
        publishRenewedDeadline(result.attempt_start_boot_ms);
        publishConfirmedBody(renewer->lastConfirmedBody());
    }
    if (result.outcome == MountRenewOutcome::Terminal && mountEnd() == MountEnd::None && kind != RenewalKind::Reacquire)
    {
        const MountSlotReading * slot = result.slot ? &*result.slot : nullptr;
        if (kind == RenewalKind::Startup || workers_stop_requested)
        {
            tripFenceWithoutOperationalLoss();
        }
        else if (slot && slot->verdict == MountSlotVerdict::FencedByGc && slot->fenced && slot->holder)
        {
            /// The fence is our own body: the reacquire replaces exactly it, by the ETag read here.
            tripMountLost();
            owed_reacquire = ReacquireCertificate{.fenced = *slot->fenced, .body = *slot->holder};
        }
        else
        {
            tripMountLost();
        }
    }
    driver_cv.notify_all();
}
```

  `renewRenewerOnce(RenewalKind)`: `beginRenewal`; progress bookkeeping; `renewUntilDefinitive` for `Worker`/`Reacquire`
  (`live = reacquireLive` for `Reacquire`; `renewal_live_for_test` still overrides), `renew` otherwise; `mountEndOf` (none for `Startup`); `latchMountEnd`;
  `finishRenewal`; the finished hook; `reactToMountEnd`; `consumeRenewResult` (propagates for `Startup`/`Direct`). A
  `SCOPE_EXIT` clears `renewer_in_use` if the call throws. `renewalLive` and `waitBeforeNextRenewalCall` lose the park
  conditions. `renewalLoop`'s cadence wait tests `mount_renewer->state() == Active`. `openAfterReacquire` and
  `runReacquireForTest` drop their driver-state lines.
- [ ] `renewalWindowFits(deadline_boot_ms)`: the predicate `mountWritable` and `reacquireOnce`'s `StartRenewer` both
  compute today (`deadline − margin`; window `period + 2 × envelope` with background renewal, else `2 × envelope`;
  strict `<`). Both call sites use it.
- [ ] Tests: `WorkerConstructionRollbackFailsOpenClosed` (one factory call); `TeardownJoinsBothWorkersBeforeRelease` →
  `TeardownJoinsTheWorkerBeforeRelease`.
- [ ] Build both, gate green, `CASReacquire.*` and X3/X4/X6 green. Commit: `Drive the CAS mount renewal from one thread without parking`.

## Task 6: delete the states only the remount entered (≈ −170 / +15 production, −450 / +40 tests)

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `$CA/Pool/CasMountLoss.{h,cpp}`, `$CA/Pool/CasPool.{h,cpp}`,
`$CA/ContentAddressedMetadataStorage.{h,cpp}`, `$CA/ContentAddressedTransaction.cpp` (comments), `$CA/Gc/CasGcScheduler.cpp`,
`src/Common/ProfileEvents.cpp`, `src/Storages/System/StorageSystemContentAddressedMounts.cpp`, the inventory's tests.

- [ ] Delete `PoolLifecycle::IdentityLost`, `PoolLifecycle::VanishedReplaced`, `enterIdentityLost`, `CASIdentityLost`,
  `MountEnd::Replaced` (with its `mountEndWord`/`isLossEnd` cases). `enterVanished(reason)` loses its `which` parameter;
  `isVanished` is `lifecycle() == VanishedForgotten`; `CasGcScheduler`'s two `IdentityLost` checks, `casLifecycleWords`'
  two rows and the `IdentityLost`/`Replaced` branches of `checkOpAdmitted`, `lifecycleReasonDetail`, `throwIfLifecycleTerminal` go.
- [ ] Column descriptions: `lifecycle` = `live, paused, stopping, closed, constructing (never started) or shutdown (torn
  down)`; `lifecycle_reason` = `renewal_overdue, lease_expired or fenced_by_gc for paused, the end word for stopping,
  forgotten for closed, empty otherwise`.
- [ ] Tests: `VanishedReplaced` cells become `VanishedForgotten` (`setLifecycleForTest`); `IdentityLost` cells are
  deleted (PR 12's tests pin the `Stopping` row); a cell that then duplicates another is deleted.
- [ ] Build both, gate green. Commit: `Remove the CAS lifecycle states only the self-remount entered`.

## Task 7: drop the unread `MountFence` identity (≈ −20 production, ±45 test call sites)

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `$CA/Pool/CasPool.{h,cpp}`, `$T/cas_test_helpers.h`, `$T/gtest_cas_pool.cpp`,
`$T/gtest_cas_mount_runtime.cpp`, `$T/gtest_cas_mount_stop.cpp`, `$T/gtest_cas_fence_generation.cpp`,
`$T/gtest_cas_ref_wedge_every_attempt.cpp`.

- [ ] `git -C $WT grep -n 'MountFence\|mount_fence' -- $CA/Formats $CA/Backend` prints nothing (no encoder sees it).
  Delete `MountFence::server_uuid`, `MountFence::writer_epoch`, `fencedIdentity`; `armMountFence(uint64_t deadline_boot_ms)`
  on both classes and all callers. `rearmMountFenceAfterAnomalyForTest`'s comment stands in for a restart, not a remount.
- [ ] Build both, gate green. Commit: `Drop the unread identity fields of the CAS mount fence`.

## Task 8: names that still say remount (net 0; the owner may drop this task)

- [ ] Rename in `src` (≈ 180 sites, 18 files): `TransientNotLive` → `Reacquiring` (the model's name), `noteRemounted` →
  `noteReacquired`, `remount_mutex` → `reacquire_mutex`, `quiesceRefTablesForRemount` → `quiesceRefTablesForReacquire`,
  `remount_quiesce_hook_for_test` → `reacquire_quiesce_hook_for_test`, `superseded_by_remount` → `superseded_by_reacquire`,
  `remountTerminal` → `workersMustExit`; suites `CASPoolRemount`/`CASMountRemount`/`CASRemountWaits` → `CASMountWorker`/
  `CASMountLeaseCutoff`/`CASReacquireWaits`. Comments that name the remount as the actor name the reacquire; the texts
  "a remount never creates one", "… mints one", "… vanished during a remount" (`CasServerRoot.cpp`) and "self-remounted"
  (`CasPartWriteTxn.cpp`) say reacquire, and `CASMountLoss.TheIdentityAndCounterLinesNameWhatWasFound` follows.
- [ ] Gate: `git grep -n -i remount -- $CA src/Common src/Storages/System` prints only `unsafe_remount_no_delay` lines;
  build both, gate green. Commit: `Name the CAS reacquire where the code still said remount`.

## Task 9: `CaDiskLifecycle` step 3 — no remount (master worktree; runs first, beside Task 2)

**Files:** `$M/CaDiskLifecycle.tla`, `$M/CaDiskLifecycle_*.cfg`, `$M/run_disklifecycle.sh`, `$M/CaDiskLifecycle_RESULTS.md`.
**Precondition:** `git -C /home/mfilimonov/workspace/ClickHouse/master status --short -- docs/superpowers/models/CaDiskLifecycle*` is empty.
TLC: `TLC_TIMEOUT=1200 TLC_WORKERS=4 TLC_JAVA_OPTS=-Xmx8g nice -n 10`, with the disk guard of PR 14's Task 1.

- [ ] Prediction before any edit: `_main_step2_notrip2.cfg` = `_main_step2.cfg` with `EnableTrip2 = FALSE`, run as a
  temporary `green` row. Predicted **green** (the arm re-check under the mutex does trip #2's job). Red: stop; Code vs
  spec 13 is wrong and Task 4 keeps the trip.
- [ ] Delete constants `LiveChecksPending`, `ReclaimChecksPending`, `AckSnapshot`, `ReclaimTrips`, `LossStops`,
  `ReacquireInThread` (always on), `EnableTrip2`; variables `reqGen`, `handledGen`, `snapGen`; `"Transient"`; actions
  `LAttemptBegin`, `LAttemptFail`, `LReclaim`, `LAttemptLoss`, `FTrip2` and the `ELSE` arms of `LRenewFenced`, `LRenewLoss`,
  `XInterfere`; `Pending` and its uses; properties `RenewNotWhilePending`, `NoArmWhileRequestPending`, `I7RequestNotLost`,
  `I4aTransientImpliesLost`, `WNoRequestDuringReclaim`; `PauseKeepsEpochAndGeneration` → `PauseKeepsEpoch`.
- [ ] Rows deleted: `sab_livependingrequest`, `sab_reclaimarmpending`, `sab_ackrequested`, `sab_reclaimnotrip`,
  `sab_reclaimnotrip_vanished`, `sab_notrip2`, `sab_lossremounts`, `witness_requestduringreclaim`, `step1_norequestrace`,
  and the step-1 `main`, `sab_armafterintent`, `sab_liveunready`, `sab_interferearmrace`, `sab_forgetlosesarm`; the
  `_step2` cfgs take the plain names. Predicted: the 19 kept rows keep their expectations; a changed verdict stops.
  Run `run_disklifecycle.sh`: `ALL EXPECTATIONS MET`.
- [ ] RESULTS: `## 2026-10-09 step 3: no remount {#2026-10-09-step-3}` — the prediction row and its count, each removed
  row with why it can no longer happen (no request, no reclaim, no trip #2; the arm re-check covers the trip), the
  kept rows' counts. Commit the exact files: `model(cas): one lease thread, no remount`.

## Task 10: docs and integration tests (≈ −160 / +60)

- [ ] `mounts-and-leases.md`: no "remount park request" or "remount re-anchor"; the "latches one self-remount" sentence
  becomes the residual rule (`local_defect`); the state diagram drops `IdentityLost`, `VanishedReplaced` and "or a
  self-remount"; one renewal thread. `manifests-and-refs.md`: a lane fault ends the process (true since PR 12).
  `cas_mounts.md`: Task 6's values, `remounting` → `fenced_by_gc`. `cas_log.md`: no `mount_remount`.
  `metrics_and_events.md`: delete `#### Remount {#cas-remount}` and `CASIdentityLost`; `CASRefRecoveryCancelled` says "a
  reacquire"; `CASMountRenewalDeadlineExceeded` names the startup re-anchor only. `monitoring.md`: drop the `CASRemount*`
  rows and query names; keep the anchor `{#mount-renewal-remount-counters}` for PR 16. `debugging.md`,
  `troubleshooting.md`: delete the `mount_remount` parts and case 6. `configuration.md`: `cas_unsafe_remount_no_delay`
  adds "Startup only; a reacquire after a GC fence never uses it."
- [ ] Integration: delete the `CASRemount*` names and assertions and the `whole-chain remount` log assertions in
  `test_cas_mount_renewal_retry/test.py` and `test_cas_mount_reacquire_matrix/test.py`. Run both modules with
  `python -m ci.praktika run "integration" --test <module>` (logs `build/test_pr15_<module>.log`): all pass.
- [ ] Commit the exact files: `Describe the CAS mount without a self-remount`.

## Task 11: final gate, line count, PR text, backlog

- [ ] Fast-test (no S3) objects of every changed `$CA` file compile; release and ASan `CAS*` green on the committed
  tree; `CASReacquire.*` with `--gtest_repeat=20` in ASan green.
- [ ] `git diff --stat <base>..HEAD` split by production / `$T` / docs / integration. Expected: production ≈ −1170 / +175,
  tests ≈ −3300 / +600. Record the actual numbers in the PR text.
- [ ] PR body per `.github/PULL_REQUEST_TEMPLATE.md`: the renewal thread is the only thread; every loss end reacquires
  or stops; the Proof table; the PR 3 dependency; removed counters and `system.cas_mounts` values (a dashboard on
  `CASRemount*` sees no rows). Changelog "Improvement": "Remove the CAS self-remount: the renewal thread reacquires
  after a GC fence, and every other loss stops the server."
- [ ] Backlog (CLI): CAS-225 Done (seam deleted); CAS-235 "remount half done by deletion"; CAS-160 AC #2 checked; notes
  on CAS-350 (Code vs spec 7), CAS-179 (6), CAS-355 (unchanged). Commit: `backlog(cas): PR 15 effects on remount tasks`.
