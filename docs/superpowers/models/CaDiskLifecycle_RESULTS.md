# `CaDiskLifecycle` results

Run record for `CaDiskLifecycle.tla`. Run the suite with `docs/superpowers/models/run_disklifecycle.sh`.
It takes no arguments and exits nonzero on any unmet expectation. All runs: TLC `2026.07.18.145032`,
jar pinned by `tlc_temporal_gate.sh`, `-workers 1`, under `nice -n 10`.

## Step 1 of the lease series: absorbing `Stopping` and `Closed` (2026-10-08) {#step-1}

Design: `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`, section 2 (loss table
R8–R18, the latch rules, "Остановка сервера") and the `CaDiskLifecycle, шаг 1` row of section 9. The
remount path for an own `gc_fenced` body (R8) is unchanged; `Reacquiring` is step 2.

### Baseline before the change {#baseline}

Model at `137d9faf2fa`, run unchanged on 2026-10-08: 17 rows, `ALL EXPECTATIONS MET`, suite wall time
9.8 s.

| Row | Expected | Actual | Generated / distinct | Depth |
|---|---|---|---|---|
| `sab_nogcselfexit` | temporal `GcExitsAfterVanished` | same | 22,603 / 5,423 | 9 |
| `sab_armafterintent` | `NoJoinWindowReclaim` | same | 15,790 / 3,818 | 13 |
| `sab_notrip2` | `I1ForgetTerminal` | same | 21,434 / 4,979 | 17 |
| `sab_unearnedfarewell` | `I2EarnedFarewell` | same | 16,118 / 3,861 | 13 |
| `sab_renewafterintent` | `RenewNotAfterIntent` | same | 418 / 169 | 5 |
| `sab_liveunready` | `LiveOnlyWhenAdmissible` | same | 86 / 44 | 4 |
| `sab_livependingrequest` | `NoArmWhileRequestPending` | same | 49 / 32 | 4 |
| `sab_reclaimarmpending` | `NoArmWhileRequestPending` | same | 288 / 130 | 5 |
| `sab_ackrequested` | `I7RequestNotLost` | same | 287 / 130 | 5 |
| `sab_reclaimnotrip` | `I4aTransientImpliesLost` | same | 754 / 295 | 6 |
| `sab_reclaimnotrip_vanished` | `I4bVanishedImpliesLost` | same | 1,676 / 585 | 7 |
| `main` | green | green | 22,883 / 5,423 | 25 |
| `witness_forgetdone` | `WForgetNeverDone` | same | 1,666 / 575 | 7 |
| `witness_racedreplaced` | `WNoRacedReplaced` | same | 1,666 / 575 | 7 |
| `witness_livebyrenewal` | `WNoLiveByRenewal` | same | 648 / 248 | 6 |
| `witness_requestduringreclaim` | `WNoRequestDuringReclaim` | same | 90 / 48 | 4 |
| `witness_forgetfarewell` | `WNoForgetFarewell` | same | 19,378 / 4,479 | 15 |

Depth: the counterexample length for a violation, the state-graph depth for a green row.

### What changed {#changes}

**States.** `PStates = {Live, Transient, Stopping, Closed}`, `TerminalSet = {Stopping, Closed}`.
`IdentityLost` and `VanishedReplaced` fold into `Stopping`; `VanishedForgotten` becomes `Closed`.

**New variables.**
- `armReady`: the reclaim's arming decision, carried to the step that opens writes.
- `epoch`: writer epoch, one per installed incarnation.
- `d1stop`: this disk's process-stop request.
- `other`: the second disk.
- `stopRequests`: requests issued.
- History: `everStopping`, `everClosed` (replace `everIL`, `everVanished`), `lossSeen`,
  `stopByInterference`, `pauseEpoch`.

**New constants** (sabotage and legacy switches; honest value `TRUE`):
- `LossStops`: `FALSE` restores the product before Stopping, where a loss trips the fence and requests a remount.
- `RenewOkChecksLatch`.
- `ArmRechecksAtOpen`.
- `StopOnce`.

**Operators.**
- `EnterStopping`: publishes `intent` (the terminal latch) in the observing step. The first terminal
  transition sets `Stopping` and `d1stop = "Pending"`.
- `IssueStop`: the callback under the once-flag.
- `LeaseTerminal == intent`: every terminal entry publishes the latch.
- `LRenewEnd` is split into three actions:
  - `LRenewEndEarly`: stop, latch or pending; the action that already existed.
  - `LRenewFenced`: R8, trip plus remount request, which is today's path.
  - `LRenewLoss`: R9–R17, latch the fence plus `EnterStopping`.
- `LEnterIL` and `LPromoteReplaced` become `LAttemptLoss` (`EnterStopping`, first terminal wins). It
  does not trip the fence; step 0 of the attempt did.
- `XInterfere`: latch the fence plus `EnterStopping`. It no longer bumps `reqGen`.
- `LReclaim` installs the incarnation (`epoch + 1`) and decides; the new `LArmOpen` opens writes and
  re-reads the arming rule and the deadline in that step.
- `LRenewOk`: arms only a `Transient` pool under `CanArm`. `RenewOkChecksLatch = FALSE` lets it arm a
  latched or `Stopping` pool.
- New actions `XRaise`, `OtherObserve`, `OtherRaise`.
- `XExpire` records `pauseEpoch`.
- `FEnter` enters `Closed`.
- `FCheckShort`, `FCheckProceed`, `GcRoundStart` and `GcSelfExit` use `TerminalSet`.

**New properties.**
- Invariants: `I9LossTerminates`, `OneProcessStop`, `StopFollowsLatch`, `ClosedRequestsNoStop`,
  `NoRequestRace`.
- Action properties: `TerminalAbsorbing`, `NoArmAfterLatch`, `ForgetWinsArm`,
  `PauseKeepsEpochAndGeneration`.
- Liveness: `StopIsRequested`.
- Witnesses: `WNoPauseResume`, `WNoInterferenceStop`.

**Renamed or redefined** (each keeps its row; only the state set changed):

| Old | New | Definition now |
|---|---|---|
| `GcExitsAfterVanished` | `GcExitsAfterTerminal` | `TerminalSet ~> gcsched # Running` |
| `I4bVanishedImpliesLost` | `I4bTerminalImpliesLost` | `pstate \in TerminalSet => lost` |
| `WNoRacedReplaced` | `WNoRacedStopping` | `~(ForgetDone /\ pstate = Stopping)` |
| `I1ForgetTerminal` | same | `lost /\ pstate \in TerminalSet` |
| `I1bForgetWinsUnlessNatural` | same | `pstate = Closed` |
| `I3OneWay` | same | over `everStopping`, `everClosed` |
| `I4aTransientImpliesLost` | same | `Transient` only |

### Rows after the change {#rows}

Final run 2026-10-08: 25 rows, `ALL EXPECTATIONS MET`, suite wall time 34.5 s.

| Row | Expected | Actual | Generated / distinct | Depth | Seconds |
|---|---|---|---|---|---|
| `sab_nogcselfexit` | temporal `GcExitsAfterTerminal` | same | 243,229 / 55,584 | 8 | 3 |
| `sab_armafterintent` | `NoJoinWindowReclaim` | same | 102,426 / 24,384 | 15 | 1 |
| `sab_notrip2` | `I1ForgetTerminal` | same | 17,577 / 4,842 | 10 | 1 |
| `sab_unearnedfarewell` | `I2EarnedFarewell` | same | 64,055 / 15,353 | 13 | 1 |
| `sab_renewafterintent` | `RenewNotAfterIntent` | same | 870 / 333 | 5 | 0 |
| `sab_liveunready` | `LiveOnlyWhenAdmissible` | same | 911 / 357 | 6 | 1 |
| `sab_livependingrequest` ¹ | `NoArmWhileRequestPending` | same | 78 / 55 | 4 | 0 |
| `sab_reclaimarmpending` ¹ | `NoArmWhileRequestPending` | same | 1,698 / 627 | 6 | 1 |
| `sab_ackrequested` ¹ | `I7RequestNotLost` | same | 577 / 258 | 5 | 0 |
| `sab_reclaimnotrip` ¹ | `I4aTransientImpliesLost` | same | 4,176 / 1,376 | 7 | 1 |
| `sab_reclaimnotrip_vanished` ¹ | `I4bTerminalImpliesLost` | same | 8,939 / 2,701 | 8 | 0 |
| `sab_latesuccessrearm` | `NoArmAfterLatch` | same | 80 / 55 | 4 | 0 |
| `sab_interferearmrace` | `TerminalAbsorbing` | same | 2,386 / 832 | 7 | 1 |
| `sab_forgetlosesarm` | `ForgetWinsArm` | same | 10,274 / 3,004 | 9 | 1 |
| `sab_secondstop` | `OneProcessStop` | same | 606 / 261 | 5 | 0 |
| `sab_lossremounts` | `I9LossTerminates` | same | 3 / 3 | 2 | 0 |
| `main` | green | green | 367,607 / 77,640 | 36 | 18 |
| `step1_norequestrace` | green | green | 367,607 / 77,640 | 36 | 2 |
| `witness_forgetdone` | `WForgetNeverDone` | same | 678 / 279 | 5 | 0 |
| `witness_racedreplaced` | `WNoRacedStopping` | same | 678 / 279 | 5 | 1 |
| `witness_livebyrenewal` | `WNoLiveByRenewal` | same | 5,114 / 1,589 | 8 | 0 |
| `witness_requestduringreclaim` ¹ | `WNoRequestDuringReclaim` | same | 138 / 80 | 4 | 0 |
| `witness_forgetfarewell` | `WNoForgetFarewell` | same | 106,315 / 24,860 | 15 | 1 |
| `witness_pauseresume` | `WNoPauseResume` | same | 101 / 66 | 4 | 1 |
| `witness_interferencestop` | `WNoInterferenceStop` | same | 606 / 261 | 5 | 0 |

`main` checks every invariant and action property above, plus four liveness properties under
`FairSpec`: `ForgetCompletes`, `GcExitsAfterTerminal`, `LeaseThreadExits`, `StopIsRequested`.

¹ These rows run with `LossStops = FALSE`. With `LossStops = TRUE` all six came out green, which
breaks their expectation: their race needs a remount request raised while the lease thread is away
from `Top`. Before Stopping, `XInterfere` was the only action that raised one. `step1_norequestrace`
checks this under the honest flags. A request never appears in `InAttempt`, `Arming` or `Renewing`,
and a reclaim never begins on an armed fence.

### Sabotage counterexamples {#counterexamples}

| Row | Trace (actions after `Init`) | What it shows |
|---|---|---|
| `sab_latesuccessrearm` | `LRenewBegin`, `XInterfere`, `LRenewOk` | an interference report latches `Stopping` while a renewal is in flight; its late success arms the fence |
| `sab_interferearmrace` | `LRenewBegin`, `LRenewFenced`, `LAttemptBegin`, `LReclaim`, `XInterfere`, `LArmOpen` | interference lands between the reclaim's decision and the open; the open moves `Stopping` to `Live` |
| `sab_forgetlosesarm` | `LRenewBegin`, `LRenewFenced`, `LAttemptBegin`, `LReclaim`, `FStart`, `FCheckProceed`, `FIntent`, `LArmOpen` | FORGET publishes its intent in the same window; the open arms after it |
| `sab_secondstop` | `XInterfere`, `XRaise`, `OtherObserve`, `OtherRaise` | both disks raise; without the once-flag the second raise issues a second request |
| `sab_lossremounts` | `XInterfere` | a loss observation trips and requests a remount; the pool is `Transient`, not terminal |

### Findings {#findings}

- **The open must re-read the deadline, not only the latch.** The first version of `LArmOpen`
  re-read only the arming rule, and `main` went red on `LiveOnlyWhenAdmissible`. Trace: `LReclaim`
  with a ready claim, then `XExpire`, then `LArmOpen` reports `Live` without a valid deadline. R23
  names only the latch: "Латч завершения, поставленный с другой нити во время получения (R14, R15,
  R18, stop), проверяется под `driver_mutex` перед открытием записи". When the decision and the open
  are separate steps, the open has to check the deadline as well.
- **A loss between FORGET's intent and `Closed` stops the process.** The model keeps
  first-terminal-wins. A loss observed after `FIntent` and before `FEnter` enters `Stopping`, and the
  process stop is requested (`witness_racedreplaced`, `I1bForgetWinsUnlessNatural`). R18 says only
  "`SYSTEM CAS FORGET` | `closed` / `forgotten`; сервер работает". The spec does not say who wins this
  race. The owner should confirm the choice.
- **A farewell is still written on a `Stopping` pool.** A temporary probe row checked
  `~(farewell /\ pstate = Stopping)` and was violated. Trace: `FStart`, `FCheckProceed`, `XInterfere`,
  `LExit`, `FIntent` … `FDrain`, `FTeardown` with an `Active` renewer. The probe row is not kept. The
  spec says "В `stopping` и `closed` farewell не пишется", which is PR 13 scope. The PR 13 farewell
  model must cover this.
- **The request generations protect nothing here.** In this model, once a loss stops the disk,
  nothing reaches the generations (`step1_norequestrace`). The model has one lease thread; the
  product keeps a separate remount thread until PR 15. This result does not show that the
  generations are unnecessary in the product.

### What this model does not check {#not-checked}

- "старый remount ограничен сертификатом" from the section 9 row: no slot body or ETag is modelled
  here.
- The late renewal success "of the old epoch". No epoch changes while a renewal is in flight, because
  reclaim runs on the same thread. So "old" here means a renewal that started before the latch.
- `paused`: only lease expiry (`XExpire`) is modelled. An overdue renewal is a long `Renewing`, which
  does not change state. `PauseKeepsEpochAndGeneration` checks the remount generations
  (`reqGen`, `handledGen`), `epoch`, `lost` and `pstate`. `fence_generation` is not modelled.
- The second disk is reduced to observe and raise over the shared once-flag. The shutdown that a
  request starts is not modelled.
- No sabotage turns these red, so they are not shown to fence anything: `StopFollowsLatch`,
  `ClosedRequestsNoStop`, `PauseKeepsEpochAndGeneration`, `StopIsRequested`, `NoRequestRace`.

## 2026-10-09 no farewell on a `Stopping` pool {#2026-10-09-no-farewell-on-stopping}

`FTeardown` writes the farewell only if the pool is not `Stopping`; the `SabFarewellOnStopping` constant
(`FALSE` in every cfg except the new sabotage) restores the old behaviour. New invariant
`NoFarewellOnStopping == ~(farewell /\ everStopping)`, checked by `main`. The fatal class (ledger fault,
local defect) is not modelled: the process ends and no action follows.

| cfg | expected | result | generated | distinct |
|---|---|---|---:|---:|
| `sab_farewellonstopping` (new) | violation | `NoFarewellOnStopping` | 94,318 | 22,353 |
| `main` | green | green, exhaustive | 366,263 | 77,064 |
| `step1_norequestrace` | green | green | - | - |
| `witness_forgetfarewell` | violation | `WNoForgetFarewell` | - | - |
| `witness_racedreplaced` | violation | `WNoRacedStopping` | - | - |
| `witness_forgetdone` | violation | `WForgetNeverDone` | - | - |
| `sab_secondstop` | violation | `OneProcessStop` | - | - |

`main` ran 367,607 generated and 77,640 distinct states on the committed model; the lower count is the
farewell no longer being written on `Stopping`. Counterexample of the sabotage (14 states): `FStart`,
`FCheckProceed`, `XInterfere`, `LExit` (the loss enters `Stopping`), `FIntent`, `FTrip1`, the GC stop and
join, `FJoinSignal`, `FJoinWait`, `FTrip2`, `FDrain`, `FTeardown` writes the farewell. This is the probe
trace recorded under "A farewell is still written on a `Stopping` pool" in the step 1 section; the row is
now kept.

## 2026-10-09 step 2: `Reacquiring` in the lease thread {#2026-10-09-step-2}

`ReacquireInThread = TRUE` changes R8 (our own body carries `gc_fenced`). `LRenewFenced` no longer raises a
remount request. The lease thread latches the fence, sets the pool to `Reacquiring` and runs the reacquire
itself: `LClose` (the old epoch ends; no stop check), `LAlloc` (one allocation per fence), `LClaimed`
(the claim lands under the next epoch, the arming decision is taken), then `LArmOpen` reads the arming rule
and the deadline again. `LClaimLoss` is a loss found by the claim and enters `Stopping`; `LReacqAbandon`
ends the reacquire on a stop or the latch; `LStepFail` is a failed step. `LRenewOk` arms a `Reacquiring`
pool as it arms a `Transient` one. `OpAdmit` and `OpDurable` model an operation admitted under one epoch
and made durable later. New properties: `I4cReacquiringImpliesLost`, `OneAllocationPerFence`,
`OldEpochOpRefused`, `LiveLeavesOnlyByFenceOrEnd`, `OneEpochPerFence`, `NoOpenOverEnd` and the
liveness property `ReacquireSettles`. Every pre-existing cfg sets the four new constants to `FALSE`: the
new actions are disabled and the new variables stay at their `Init` values.

Changes against the plan text:
- `FairSpec2` also has `WF_vars(LRenewBegin)`. With the plan's fairness `main_step2` was red on
  `ReacquireSettles`. Trace: `LRenewBegin`, `VGcStop`, `OtherObserve`, `XExpire`, `LRenewFenced`,
  `LClose`, `LAlloc`, `OtherRaise`, `LClaimed` with the lease too short to open, `LArmOpen` (does not
  open), then the thread stays at `Top` forever and never starts the renewal that would arm the pool. The
  thread renews every period, so a claim that leaves too little lease waits for that renewal.
- `LRenewFenced` under the flag requires `epoch < MaxGen` where it required `reqGen < MaxGen`, and
  `LClaimed` requires it too. The bound limits the reacquires per behavior.
- `reacqLive` is set by `LArmOpen` only. A reacquire that opens through `LRenewOk` does not set it.

| cfg | expected | result | trace states | generated | distinct |
|---|---|---|---:|---:|---:|
| `sab_reacqallocperattempt` (new) | violation | `OneAllocationPerFence` | 6 | 1,999 | 719 |
| `sab_oldopunchecked` (new) | violation | `OldEpochOpRefused` | 9 | 16,099 | 4,327 |
| `sab_lossreacquires` (new) | violation | `I9LossTerminates` | 5 | 609 | 279 |
| `sab_armafterintent_step2` (new) | violation | `NoJoinWindowReclaim` | 16 | 293,408 | 63,166 |
| `sab_liveunready_step2` (new) | violation | `LiveOnlyWhenAdmissible` | 7 | 5,171 | 1,614 |
| `sab_interferearmrace_step2` (new) | violation | `TerminalAbsorbing` | 8 | 11,685 | 3,270 |
| `sab_forgetlosesarm_step2` (new) | violation | `ForgetWinsArm` | 10 | 34,240 | 8,490 |
| `witness_reacquire` (new) | violation | `WNoReacquire` | 7 | 5,813 | 1,770 |
| `main_step2` (new) | green | green, exhaustive, depth 40 | - | 1,916,701 | 345,546 |
| `main` (control) | green | green, exhaustive, depth 35 | - | 366,263 | 77,064 |

All other pre-existing rows were re-run with their verdicts. `main` and `step1_norequestrace` keep the
counts of the previous section (366,263 generated, 77,064 distinct).

Counterexamples:
- `sab_reacqallocperattempt`: `LRenewBegin`, `LRenewFenced`, `LClose`, `LAlloc`, `LAlloc`. A second
  allocation for one fence, without a failed step in between.
- `sab_oldopunchecked`: `OpAdmit` (epoch 0), `LRenewBegin`, `LRenewFenced`, `LClose`, `LAlloc`,
  `LClaimed`, `LArmOpen`, `OpDurable`. The old operation lands after the fence is armed under epoch 1.
- `sab_lossreacquires`: `LRenewBegin`, `LRenewFenced`, `LClose`, `LClaimLoss` goes back to `Closing`.
- `sab_armafterintent_step2`: `LClaimed`, then FORGET reaches `FTrip1`, `LArmOpen` opens writes, `LExit`,
  `FJoinWait`, and the pool is `Live` with the fence open in `Trip2`.
- `sab_liveunready_step2`: `LClaimed` with the lease too short, `LArmOpen` reports `Live` at once.
- `sab_interferearmrace_step2`: `LClaimed`, `XInterfere` enters `Stopping`, `LArmOpen` opens writes over it.
- `sab_forgetlosesarm_step2`: `LClaimed`, `FIntent`, `LArmOpen` opens writes after the intent.
- `witness_reacquire`: `LRenewBegin`, `LRenewFenced`, `LClose`, `LAlloc`, `LClaimed`, `LArmOpen`.

Limits:
- `OldEpochOpRefused` is green in `main_step2` by construction: the honest `landed` predicate contains the
  epoch comparison the property restates. Its evidence is `sab_oldopunchecked`.
- `sab_reacqallocperattempt` reaches its second allocation through `LAlloc` directly. The failed-step
  path (`LStepFail` clears `allocated` under the sabotage) is a longer route to the same red and is not
  what the counterexample uses. In the honest model `LStepFail` is a stutter.
- No slot body, ETag or counter is modelled: "the claim replaces only the fenced body it read" is
  checked in `CaCasMountCore`, not here. `Closing` is one step, so the quiescence of in-flight ref work
  is not modelled.
