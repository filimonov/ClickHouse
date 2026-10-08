---
description: 'TLC evidence for CAS mount renewal without a lease deadline: retries, two renewals, exact sabotages, and non-vacuity witnesses'
sidebar_label: 'CAS mount renewal retry TLC results'
sidebar_position: 5
slug: /superpowers/models/CaMountRenewRetryCore-results
title: 'CAS mount renewal retry TLC results'
doc_type: 'reference'
---

# `CaMountRenewRetryCore` results {#camountrenewretrycore-results}

## Verdict {#verdict}

The battery passed: `EXECUTED ROWS: 19/19` and `ALL EXPECTATIONS MET`.

- Eleven single-defect configurations each violate their mapped invariant.
- The honest configuration exhausts its state graph with no error.
- Seven negated-reachability witnesses fire with all twelve safety invariants enabled.

## What the model covers {#what-the-model-covers}

The renewal is not bounded by the lease. It sends and retries past the confirmed deadline and
ends only on success, a definitive answer, or a cancel. There is no exhaustion transition:
`MaxRetries` and `MaxOutstanding` bound the state space and nothing else.

- **Authority is the holder's own timer.** `LocalAuthority` is `~fenceLost /\ clock <
  confirmedDeadline`. Only `LocalTerminalize` sets `fenceLost`. Observer actions never touch
  local state.
- **Deadline from the start.** A commit sets `confirmedDeadline` to `attemptStartedAt +
  LeaseDuration`. `firstSendAt` records each renewal's first send. Only `SendRenewal` writes it.
- **Two renewals.** After a commit, `ScheduleNextBeat` and `SendRenewal` start the second renewal
  with a new attempt ID, expected token and start. `SendRenewal` needs no authority. Copies of the
  first renewal still in flight become `staleOutstanding`. `StaleCopyDelivery` lands one only if
  the first renewal's expected token is still current.
- **Observer entitlement.** Every durable write sets `tokenStableUntil` to `clock +
  LeaseDuration`. A takeover by `GCFence`, `SuccessorClaim` or `ForeignHolderWrite` is entitled
  once `clock >= tokenStableUntil`. `SuccessorClaim` is also entitled once the old runtime is
  locally terminal. Observers still run at any time. Unentitled takeovers stay in the graph as
  adversarial perturbations.

`sab_send_after_deadline`, `NoRequestAfterSafeDeadline` and `WitnessExhaustionFences` were
removed. Sending after the deadline is the intended behaviour, and exhaustion no longer exists.

## Checker and finite scope {#checker-and-finite-scope}

- TLC: `2026.07.18.145032`, revision `30cc360`, one worker.
- Pinned `tla2tools.jar` SHA-256:
  `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`.
- Attempt IDs: `attempt1` (first renewal), `attempt2` (second), `attempt3` (twin, successor,
  foreign).
- Tokens: predecessor, two renewal tokens, same-pair twin, successor, foreign.
- Time `0..5`. Initial confirmed deadline `3`. Lease duration `4`. Cadence period `2`.
- One retry per renewal. At most two outstanding copies.
- `MaxTime = 5` admits a first success at clock `5` from a start at `0`: `start + LeaseDuration`
  is strictly past.

## Safety invariants {#safety-invariants}

The honest and witness configurations check these twelve:

- `ExactAttemptOnly`: an adopted attempt ID is the committed renewal's own.
- `ForeignOrSuccessorNeverAdopted`: twins, fenced, successor and foreign bodies are never
  adopted.
- `ConfirmedDeadlineNeverExtendedByResponse`: a commit sets the deadline from the start it used,
  never from the response time.
- `TerminalNeverRearmsAuthority`: after a local terminal, authority stays false.
- `OneLogicalBodyPerExpectedToken`: every copy of a renewal keeps its attempt, body and expected
  token. The first renewal expects the predecessor token, the second expects the first renewal's
  token.
- `AcknowledgedRenewalIsDurable`: a direct acknowledgement needs the exact body to be durable.
- `LateDeliveryCannotOverwriteSuccessor`: a post-terminal copy never replaces a successor.
- `PendingSurvivesLocalTerminal`: `LocalTerminalize` does not drop outstanding copies. This is a
  transition-history guard. It does not claim every unresolved result has pending I/O.
- `OneIncarnationPerPredecessor`: at most one copy consumes each renewal's expected token. Stale
  copies count against the first renewal.
- `CadenceAnchoredAtAttemptStart`: the next beat is the renewal's first send plus the period.
- `AuthorityEndsAtConfirmedDeadline`: the confirmed deadline is the initial one before any
  commit, then `firstSendAt` of the committed renewal plus the lease. The conjunct "authority
  implies `clock < confirmedDeadline`" holds by the definition of `LocalAuthority`.
- `NoAuthorityAfterEntitledTakeover`: once an entitled takeover happened, the holder has no
  authority.

## Recorded complete run {#recorded-complete-run}

TLC row-log run ID: `CaMountRenewRetryCore-2694715-1790889808360628074`.

| Configuration | Expected | Exact observed result | Generated | Distinct | Depth |
|---|---|---|---:|---:|---:|
| `sab_ignore_attempt_id` | violation | `ExactAttemptOnly` | 1,160 | 691 | 6 |
| `sab_refresh_deadline_from_response` | violation | `ConfirmedDeadlineNeverExtendedByResponse` | 187 | 143 | 5 |
| `sab_response_deadline_outlives_observer` | violation | `NoAuthorityAfterEntitledTakeover` | 26,627 | 10,559 | 9 |
| `sab_anchor_deadline_at_retry` | violation | `AuthorityEndsAtConfirmedDeadline` | 23,893 | 9,876 | 9 |
| `sab_retry_with_new_body` | violation | `OneLogicalBodyPerExpectedToken` | 1,074 | 642 | 6 |
| `sab_accept_after_terminal` | violation | `TerminalNeverRearmsAuthority` | 1,330 | 766 | 6 |
| `sab_accept_successor` | violation | `ForeignOrSuccessorNeverAdopted` | 401 | 260 | 5 |
| `sab_drop_pending_on_terminal` | violation | `PendingSurvivesLocalTerminal` | 59 | 53 | 4 |
| `sab_late_rearm` | violation | `TerminalNeverRearmsAuthority` | 324 | 230 | 5 |
| `sab_response_relative_cadence` | violation | `CadenceAnchoredAtAttemptStart` | 901 | 562 | 6 |
| `sab_double_conditional_landing` | violation | `OneIncarnationPerPredecessor` | 3,821 | 1,942 | 7 |
| `safe` | green | `green` | 11,942,930 | 2,775,783 | 33 |
| `witness_direct_retry` | witness | `WitnessDirectRetry` | 10,986 | 4,911 | 8 |
| `witness_read_adoption` | witness | `WitnessReadAdoption` | 1,272 | 726 | 6 |
| `witness_late_before_reclaim` | witness | `WitnessLateBeforeReclaim` | 4,626 | 2,240 | 7 |
| `witness_late_after_successor` | witness | `WitnessLateAfterSuccessor` | 1,390 | 790 | 6 |
| `witness_catchup` | witness | `WitnessImmediateCatchup` | 2,971 | 1,573 | 7 |
| `witness_retry_past_deadline` | witness | `WitnessRetryPastDeadline` | 13,661 | 5,887 | 9 |
| `witness_authority_restored` | witness | `WitnessAuthorityRestored` | 234,410 | 78,639 | 13 |

Configuration files are `CaMountRenewRetryCore_<name>.cfg`. The honest graph completed with zero
states left on the queue.

## Sabotages on the deadline {#sabotages-on-the-deadline}

- `sab_anchor_deadline_at_retry` moves `attemptStartedAt` to the retry's send. The commit then
  agrees with its own anchor, so `ConfirmedDeadlineNeverExtendedByResponse` holds. Only
  `AuthorityEndsAtConfirmedDeadline` catches it, by comparing with `firstSendAt`. Trace:
  `SendRenewal` at 0 → `ResponseLoss` → `ExactResolve` (predecessor) → `EnterRetryWait` →
  `RetrySend` at 1 → `LandCopy` → `AcknowledgeRenewal` with deadline 5 instead of 4.
- `sab_response_deadline_outlives_observer` uses the response-time defect against the observer.
  Trace: `SendRenewal` and `LandCopy` at 0 → `AcknowledgeRenewal` at 4 with deadline 8 →
  entitled `GCFence` at 4 while the holder still has authority.

## Witness audit {#witness-audit}

- `WitnessDirectRetry`: `SendRenewal` → `ResponseLoss` (not landed) → `ExactResolve`
  (predecessor) → `EnterRetryWait` → `RetrySend` → `LandCopy` → `AcknowledgeRenewal`.
- `WitnessReadAdoption`: `SendRenewal` → `LandCopy` → `ResponseLoss` → `ExactResolve` →
  `AcceptResolved` of the exact attempt ID and bytes.
- `WitnessImmediateCatchup`: success at clock 2 from a start at 0, then `ScheduleNextBeat`
  schedules a beat that is already due.
- `WitnessRetryPastDeadline`: the first renewal starts at 3, the confirmed deadline. Its read
  finds the predecessor and `RetrySend` fires at 3.
- `WitnessAuthorityRestored`: `SendRenewal` at 0, the copy lands at 5, `AcknowledgeRenewal` at 5
  confirms deadline 4, and authority stays false. `ScheduleNextBeat` gives beat 2, so
  `SendRenewal` starts the second renewal at 5 without authority. It lands and commits deadline
  9, and authority returns.
- `WitnessLateBeforeReclaim`: `SendRenewal` → `Cancel` → `LocalTerminalize` →
  `LateDeliveryLands` → `GCFence` → `Reclaim`. The late copy extends the old body while authority
  stays false.
- `WitnessLateAfterSuccessor`: `SendRenewal` → `Cancel` → `LocalTerminalize` → `SuccessorClaim` →
  `LateDeliveryRefused`. The successor token is unchanged and old authority stays false.

`WitnessRetryPastDeadline` and `WitnessAuthorityRestored` were checked against a scratch copy
with the budget guards restored (`CanStartRequest` on both sends, `clock <= 3` on both commits).
Both came back `green`, which the runner rejects for a witness row. `WitnessDirectRetry` still
fired as a control.

## Not modelled {#not-modelled}

- Clock drift: the observer waits exactly one lease on the same clock. `CaCasMountCore` owns the
  observer's timing with drift.
- Write admission margin: authority is `clock < confirmedDeadline`, without the time a write
  reserves.
- Retry spacing, the memory guard, reporting.
- A third renewal: the second commit ends the renewal sequence.

## Reproduction commands {#reproduction-commands}

Run from `docs/superpowers/models`:

```bash
TLC_JAVA_OPTS=-Xmx16g nice -n 10 ./run_mountrenewretry.sh > ../../../tmp/run_mountrenewretry.log 2>&1
```

The command exited `0`. Its final lines were:

```text
EXECUTED ROWS: 19/19
ALL EXPECTATIONS MET
```

## Abstraction boundary {#abstraction-boundary}

No formal refinement to atomic `CaCasMountCore.Renew` is claimed. The focused model owns landed but
locally unconfirmed states that the atomic action abstracts away. When the focused operation
reaches local `Committed`, its durable body/token and deadline satisfy the atomic postconditions;
terminal focused outcomes trip the fence at the existing remount boundary. This correspondence is
an audit aid, not a machine-checked refinement.

## 2026-10-08: `Refused` and `Conflict{NotObserved}` continue the same tuple {#2026-10-08-refused-and-notobserved-continue}

This section supersedes the verdict and the run table above. The model now follows rules R1–R7 of
`docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md` (section "Продление") and the
`CaMountRenewRetryCore` row of its section "Модели". The battery passed: `EXECUTED ROWS: 30/30` and
`ALL EXPECTATIONS MET`.

### What changed {#2026-10-08-what-changed}

- **Engine calls.** A logical renewal can now span several calls of `CasOperation::writeLoop`.
  `callAmbiguous` is the engine's `any_ambiguous`: an attempt of the current call has an unknown
  fate. `ResponseLoss` sets it. `NewEngineCall` clears it.
- **Current attempt vs ghosts.** `LandCopy` and `RefuseCopy` are split. `LandCurrent` and
  `RefuseCurrent` answer the attempt the holder waits on. `LandGhost` and `RefuseGhost` resolve an
  earlier copy whose answer nobody waits for (`GhostExists`). Before the split, a `412` for a
  ghost after our own copy landed moved `Landed` to `Conflict` and terminalized on our own body.
- **`Refused`.** `DenyCurrent` is a definite refusal (`AccessDenied`, `MalformedXML`, ...). It
  yields `Refused` only when `~callAmbiguous`. Otherwise the read settles the call, as in
  `writeLoop`. `NewEngineCall` resends the same attempt ID, body, expected token and start.
  `LocalTerminalize` no longer fires on `Refused`.
- **`Conflict{NotObserved}`.** A `412` with `~callAmbiguous` goes to `CleanConflict`.
  `ResolvingRead` either observes the slot and concludes `Conflict`, or fails and moves to
  `NotObserved`. From `NotObserved` only `ResolvingRead` runs again. `ExactResolve` gained a
  read-failure branch: with an ambiguous attempt, a failed read reissues the `PUT`, as
  `preconditionStillSatisfiable` does for `NotObserved`.
- **Credential refresh.** `CredentialRefresh` is the first credential answer of a call. Without
  an ambiguous attempt the engine reissues at once; with one the read settles first. It keeps the
  tuple and the start.
- **Explicit arm.** `writesOpen` starts `TRUE`. `AdvanceTime` closes it at the confirmed
  deadline. `LocalTerminalize` closes it. Only `OpenWrites` opens it: committed, not terminal, fence
  intact, `clock < confirmedDeadline`.
- New constant `MaxEngineCalls = 1`: one new engine call per logical renewal. It bounds the state
  space only.
- Runner: six sabotage rows, four witness rows and one green evidence row
  (`env_repeat_put_harmless`). Every cfg declares `MaxEngineCalls`.

### New safety invariants {#2026-10-08-new-invariants}

The honest and witness configurations check sixteen invariants: the twelve above and these four.

- `OwnLandedBodyNeverLost`: a resolving read after a clean `412` never finds our exact body.
  This is the safety argument for `NewEngineCall`: a call reported free of ambiguity left no copy
  that could land.
- `NotObservedRepeatsOnlyTheRead`: no `PUT` is sent after a clean `412` of the same logical
  renewal. Every sending action (`RetrySend`, `NewEngineCall`, `CredentialRefresh`'s reissue, the
  sabotage action) sets the history flag when `preconditionRefused` holds.
- `NotObservedNeverConcludes`: a failed resolving read never concludes `Conflict`. It is a
  history flag that only the sabotage branch sets. It checks that no honest path concludes on a
  failed read; it does not model the loss-table classification.
- `LateSuccessNeverOpensWrites`: `writesOpen => ~fenceLost /\ clock < confirmedDeadline`.
  Together with `AuthorityEndsAtConfirmedDeadline`, an open write implies the clock is before the
  committed renewal's first start plus the lease.

`AuthorityEndsAtConfirmedDeadline` is unchanged and green. It now also covers `NewEngineCall` and
`CredentialRefresh`: neither writes `firstSendAt`, and the deadline still equals
`firstSendAt[committedRound] + LeaseDuration`.

### Recorded complete run {#2026-10-08-recorded-complete-run}

TLC row-log run ID: `CaMountRenewRetryCore-217265-1791413803271456719`. One TLC worker, `-Xmx16g`, `nice -n 10`. Wall time is the runner's
`SECONDS` column.

| Configuration | Expected | Exact observed result | Generated | Distinct | Depth | Wall (s) |
|---|---|---|---:|---:|---:|---:|
| `sab_ignore_attempt_id` | violation | `ExactAttemptOnly` | 1,833 | 1,044 | 6 | 0 |
| `sab_refresh_deadline_from_response` | violation | `ConfirmedDeadlineNeverExtendedByResponse` | 254 | 186 | 5 | 1 |
| `sab_response_deadline_outlives_observer` | violation | `NoAuthorityAfterEntitledTakeover` | 59,075 | 23,247 | 9 | 1 |
| `sab_anchor_deadline_at_retry` | violation | `AuthorityEndsAtConfirmedDeadline` | 51,961 | 21,237 | 9 | 0 |
| `sab_retry_with_new_body` | violation | `OneLogicalBodyPerExpectedToken` | 1,700 | 974 | 6 | 1 |
| `sab_accept_after_terminal` | violation | `TerminalNeverRearmsAuthority` | 2,024 | 1,129 | 6 | 0 |
| `sab_accept_successor` | violation | `ForeignOrSuccessorNeverAdopted` | 1,894 | 1,064 | 6 | 1 |
| `sab_drop_pending_on_terminal` | violation | `PendingSurvivesLocalTerminal` | 84 | 74 | 4 | 0 |
| `sab_late_rearm` | violation | `TerminalNeverRearmsAuthority` | 532 | 354 | 5 | 1 |
| `sab_response_relative_cadence` | violation | `CadenceAnchoredAtAttemptStart` | 1,388 | 826 | 6 | 0 |
| `sab_double_conditional_landing` | violation | `OneIncarnationPerPredecessor` | 6,740 | 3,346 | 7 | 1 |
| `sab_refused_while_ambiguous` | violation | `OwnLandedBodyNeverLost` | 280,760 | 98,367 | 11 | 2 |
| `sab_repeat_put_after_not_observed` | violation | `NotObservedRepeatsOnlyTheRead` | 2,610 | 1,390 | 6 | 0 |
| `sab_not_observed_concludes` | violation | `NotObservedNeverConcludes` | 578 | 371 | 5 | 0 |
| `sab_open_writes_on_late_success` | violation | `LateSuccessNeverOpensWrites` | 46,958 | 19,163 | 9 | 1 |
| `sab_refresh_resets_start` | violation | `AuthorityEndsAtConfirmedDeadline` | 1,489 | 911 | 6 | 1 |
| `sab_new_call_new_attempt_id` | violation | `OneLogicalBodyPerExpectedToken` | 64 | 60 | 4 | 0 |
| `safe` | green | `green` | 441,259,980 | 95,092,214 | 39 | 2058 |
| `env_repeat_put_harmless` | green | `green` | 669,350,428 | 142,568,542 | 41 | 3169 |
| `witness_direct_retry` | witness | `WitnessDirectRetry` | 21,656 | 9,550 | 8 | 1 |
| `witness_read_adoption` | witness | `WitnessReadAdoption` | 1,956 | 1,084 | 6 | 1 |
| `witness_late_before_reclaim` | witness | `WitnessLateBeforeReclaim` | 8,959 | 4,228 | 7 | 1 |
| `witness_late_after_successor` | witness | `WitnessLateAfterSuccessor` | 2,479 | 1,343 | 6 | 0 |
| `witness_catchup` | witness | `WitnessImmediateCatchup` | 5,167 | 2,652 | 7 | 0 |
| `witness_retry_past_deadline` | witness | `WitnessRetryPastDeadline` | 28,976 | 12,199 | 9 | 0 |
| `witness_authority_restored` | witness | `WitnessAuthorityRestored` | 796,836 | 257,285 | 13 | 5 |
| `witness_refused_then_commit` | witness | `WitnessRefusedThenCommit` | 2,082 | 1,154 | 6 | 1 |
| `witness_not_observed_then_observed` | witness | `WitnessNotObservedThenObserved` | 2,608 | 1,389 | 6 | 0 |
| `witness_refresh_then_commit` | witness | `WitnessRefreshThenCommit` | 1,462 | 870 | 6 | 0 |
| `witness_writes_reopened` | witness | `WitnessWritesReopened` | 9,983 | 4,641 | 8 | 1 |

The battery took 5,248 s (87 min) end to end. Two rows exceed 20 min: `safe` (2,058 s) and
`env_repeat_put_harmless` (3,169 s). Their constants were not reduced, because each one is needed
by a row: `MaxTime = 5` by the late-success cases, `MaxOutstanding = 2` by ghosts, `MaxRetries =
1` by `sab_refused_while_ambiguous`. The honest graph grew
from 2,775,783 to 95,092,214 distinct states.

### New sabotages: counterexamples {#2026-10-08-sabotage-counterexamples}

- `sab_refused_while_ambiguous` (`OwnLandedBodyNeverLost`): `SendRenewal` → `ResponseLoss` →
  `LandGhost` (our copy lands, unseen) → `ExactResolve` (read fails) → `EnterRetryWait` →
  `RetrySend` → `DenyCurrent` returns `Refused` despite the ambiguity → `NewEngineCall` starts
  clean → `RefuseCurrent` is now a clean `412` → `ResolvingRead` finds our own exact body and
  concludes `Conflict`. The renewal terminalizes on its own landed body. Depth 11.
- `sab_repeat_put_after_not_observed` (`NotObservedRepeatsOnlyTheRead`): `SendRenewal` →
  `SamePairTwin` → `RefuseCurrent` (clean `412`) → `ResolvingRead` fails →
  `RepeatPutAfterNotObserved`. Depth 6.
- `sab_not_observed_concludes` (`NotObservedNeverConcludes`): a clean `412` and a failed read
  conclude `Conflict`.
- `sab_open_writes_on_late_success` (`LateSuccessNeverOpensWrites`): `SendRenewal` at 0 →
  `AdvanceTime` to 3 closes writes → clock 4 → `LandCurrent` → `AcknowledgeRenewal` confirms
  deadline 4 at clock 4 → `OpenWrites` opens writes. Depth 9.
- `sab_refresh_resets_start` (`AuthorityEndsAtConfirmedDeadline`): `SendRenewal` at 0 →
  `AdvanceTime` → `CredentialRefresh` at 1 re-samples the start → `LandCurrent` →
  `AcknowledgeRenewal` confirms deadline 5 instead of 4. Depth 6.
- `sab_new_call_new_attempt_id` (`OneLogicalBodyPerExpectedToken`): `SendRenewal` →
  `DenyCurrent` → `NewEngineCall` with another attempt ID. Depth 4.

The eleven earlier sabotages still violate exactly their mapped invariant.

### New witnesses {#2026-10-08-new-witnesses}

- `WitnessRefusedThenCommit`: `SendRenewal` → `DenyCurrent` (`Refused`) → `NewEngineCall` →
  `LandCurrent` → `AcknowledgeRenewal`, deadline 4 from start 0.
- `WitnessNotObservedThenObserved`: a clean `412`, a failed read, then a repeated read observes
  the slot.
- `WitnessRefreshThenCommit`: `SendRenewal` at 0 → `AdvanceTime` → `CredentialRefresh` at 1 →
  `LandCurrent` → `AcknowledgeRenewal`, deadline 4 (first start plus lease), not 5.
- `WitnessWritesReopened`: writes close at 3 → `SendRenewal` at 3 → commit with deadline 7 →
  `OpenWrites`.

### Findings against the spec {#2026-10-08-findings}

No run contradicts R1–R7. Points for the implementation and the spec text:

- **The re-`PUT` after `Conflict{NotObserved}` is not a safety defect in this model.** R2 says:
  "После `Conflict{NotObserved}` повторяется только чтение: `412` уже доказал, что precondition
  ушёл, и повторный `PUT` с тем же ETag даст `412` снова". Row `env_repeat_put_harmless` enables
  that sabotage and checks every other safety invariant: green. A consumed token never returns, so
  the repeated `PUT` cannot land. In this model the rule saves requests; it does not protect the
  slot. `NotObservedRepeatsOnlyTheRead` checks the protocol shape only.
- **With an ambiguous attempt the engine does re-`PUT` after a `412` and a failed read.**
  `preconditionStillSatisfiable` returns `true` for `NotObserved`, so `writeLoop` reissues. R2
  covers only the `Conflict{NotObserved}` the engine returns, so this is consistent. The model
  keeps the reissue in `ExactResolve`.
- **R1 needs the tuple hoisted out of the per-call code.** `MountLeaseRenewer::renewOn` today
  samples `attempt_start_boot_ms`, draws `newMountWriteAttemptId` and encodes the body on every
  call. A new engine call through `renewOn` would be `sab_new_call_new_attempt_id` plus
  `sab_refresh_resets_start`. Today `renewOn` also terminalizes `Refused` and, through
  `throwRenewConflict`, `Conflict{NotObserved}`; PR 8 of the series spec removes both.
- **The `Refused` precondition is load-bearing.** `safe` keeps `OwnLandedBodyNeverLost` green;
  `sab_refused_while_ambiguous` drops only the guard and violates it. The guard in `writeLoop` is
  `definitely_refused && !state.any_ambiguous`. `state.any_ambiguous` resets at the start of each
  `writeLoop`, so a refactor that splits one renewal into several engine calls must not carry a
  ghost across that reset.

### Scope added in this revision {#2026-10-08-scope}

- One new engine call per logical renewal, one retry, at most two outstanding copies. A second
  `Refused` in the same renewal is not explored.
- The credential refresh is one per engine call. A second credential answer in the same call is
  a `DenyCurrent`.
- The admission margin ("остатка не хватает на запись") is not modelled: `OpenWrites` needs only
  `clock < confirmedDeadline`.
- Epoch is fixed. The R5 rule that an old epoch's result does not touch the new epoch's deadline
  is out of scope here.
- R4 pacing and R6/R7 (request plane, pause leaves epoch and ref tables) are not modelled.

### Reproduction {#2026-10-08-reproduction}

Run from `docs/superpowers/models`:

```bash
TLC_JAVA_OPTS=-Xmx16g nice -n 10 ./run_mountrenewretry.sh > ../../../tmp/lease-series/renewretry-full.log 2>&1
```

The command exited `0`. Its final lines were:

```text
EXECUTED ROWS: 30/30
ALL EXPECTATIONS MET
```
