# CAS lease PR 10 v2: an occupied ref-log id is our own bytes or one fault

**Goal.** When the ref lane finds an object at an id it owns, there are two outcomes. Our exact bytes
(an earlier attempt of the same transaction landed): continue as today. Anything else is one untyped
fault: a decodable foreign transaction, a successor's `EpochSeal` (a successor exists, so this mount
is already a zombie), an object that does not decode, and a lane state the lane machine cannot
produce. Every fault latches the mount fence first, then goes through one report path that requests
the remount before anything that can throw. The ref-lane commit path (`commitRefChunk`,
`resolveWedgeOnce`) runs under `LockMemoryExceptionInThread(VariableContext::Global)`, so no
memory-limit arm is needed. Supersedes v1 (`e3a36466d06`..`b22e5f42bf9`), which typed the cause and
retried on the memory limit.

**Architecture.** `CasRefLedger` gets one new callback, `latch_mount_fault` (Pool binds
`CasMountRuntime::tripMountLost`: atomics only). The existing `on_impossible_interference` keeps its
signature and becomes the report of an already-latched fault; `Pool::reportImpossibleInterference`
calls `scheduleRemount` first and no longer trips the fence. `classifyRefLogOccupant` and the
`Occupant` enum are deleted: the verdict is byte equality, and decoding happens only to describe the
object in the report.

**Tech stack.** C++23, gtest (`unit_tests_dbms`, `WedgeTestBackend`, `VirtualRetryClock`), TLC.

**Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`, revision 7: section
"Потеря" rows R14, R15, R17 and the paragraph "Латч с нити писателя"; section 7 PR row 10 and tests
row 10; section 9 row "модель ref-полосы". This plan implements the owner's 2026-10-08 simplification,
which supersedes those rows; the deltas are listed under "Spec deltas" below and are not edited into
the spec by this plan.

**Base and branch.** `altinity/antalya-26.6` at `5b7c9f503df`; branch
`fix/antalya-26.6/cas-ref-lane-fault-latch`; worktree `/home/mfilimonov/workspace/ClickHouse/cas-95-1`
(Task 0). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. The model lives in
the master worktree (`/home/mfilimonov/workspace/ClickHouse/master`, branch `cas-gc-rebuild`) and is
committed there. No on-storage format change.

Files outside `CA/` and `src/Disks/tests/`: `src/Common/ProfileEvents.cpp` (two description strings,
no new event) and `docs/en/antalya/...` (user docs). Both are fork-only text.

Every commit message ends with exactly these two trailers, in this order:

```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

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

## Review Focus

- **Two outcomes, decided by bytes.** `commitRefChunk` (conflict arm), `resolveWedgeOnce` and
  `runRecoveryWalkOnce` (retained attempt above the frontier) compare the occupant to the attempt's
  bytes and branch on nothing else. `describeRefLogOccupant` decodes only for the message and runs
  after the latch.
- **Latch first.** Each fault arm's first statement is `latch_mount_fault()`: before the lane lock,
  `fmt::format` and the key move. In `resolveWedgeOnce` the decision is taken under `state_mutex` and
  the latch is the first statement after that lock is released (the callback never runs under the
  lane lock). The non-`Ready` arm in `commitRefChunk` latches the same way.
- **Remount before anything that can throw.** `Pool::reportImpossibleInterference` calls
  `mount_runtime.scheduleRemount()` as its first statement. `reportLatchedFault` swallows a failure of
  the message builder so the report always runs. The one `bad_alloc` that does not propagate is one
  thrown while describing an already-latched fault (see "Limits").
- **The seal is no longer a conclusive rejection at our own slot.** A successor's seal at an id this
  mount derived means a higher epoch exists. Base answered `INVALID_STATE` and a quiet `Closed` lane;
  v2 faults the mount. Failure asymmetry: if the seal were benign, the cost is one bounded remount of a
  mount that has already lost its lease; if it is a zombie and we stay quiet, every other lane of the
  zombie keeps writing until the lease thread notices. The second is the one to avoid.
- **Memory guard.** `LockMemoryExceptionInThread memory_exception_lock(VariableContext::Global)` at the
  top of `commitRefChunk` and `resolveWedgeOnce`. `Global` blocks every level; `User` leaves the
  server limit enforced (the mutation the tests catch). Allocations are still counted. `std::bad_alloc`
  is not affected. The guard also covers `prepareRefChunk`'s candidate copy, so a large ref table can
  push the server past `max_server_memory_usage` by one candidate; this is the trade the renewal guard
  (`CasMountRuntime::renewalLoop` on `fix/antalya-26.6/cas-renewal-memory-guard`) already made.
- **Untouched on purpose.** The recovery walk's dead-epoch seal slot (`op.create` of a seal that meets
  a peer's seal or a predecessor's straggler) still adopts both, and a decode failure there still
  propagates `CORRUPTED_DATA` without a latch. That is the predecessor-record distinction sealing
  needs; it is not our own slot. The deposed-lane gate in `flushRefBatch` (`INVALID_STATE`,
  `CASRefAppendSealRejected`, no latch) is unchanged; see "Code vs dispatch".
- `WedgeResolution::Rejected` is deleted (no producer remains). `RefLaneState::Closed` stays: it is
  what `laneStateForTest` reports for a name with no runtime and what `resolveWedgeOnce`'s
  invalid-state branch handles.

## Spec deltas

Rows as they are in revision 7:

> | R14 | occupant в нашем слоте ref-журнала декодирован и классифицирован как `Foreign` (не наш и не `EpochSeal` преемника) | остановка, `ledger_interference` |
> | R15 | occupant в нашем слоте ref-журнала прочитан, но не декодируется (`Unreadable`) | остановка, `ledger_corrupt` |
> | R17 | дефект нашего кода: исключение, вышедшее из машины нити продления или получения заново (кроме `MEMORY_LIMIT_EXCEEDED`, раздел ниже) и не отнесённое к строкам выше по месту возникновения; внутреннее нарушение машины ref-полосы (например, полоса не в `Ready` при выделении нового ID — сегодня это ветка `on_impossible_interference` без occupant) | остановка, `local_defect`. Нить не перезапускается |

Proposed (R15 is kept as a merged marker so that "R9–R15" ranges in later rows stay valid):

> | R14 | ID ref-журнала, который полоса выводит для своей записи (append, повтор wedge, retained-попытка `runRecoveryWalkOnce`), занят объектом, чьи байты не равны байтам попытки: чужая транзакция, `EpochSeal` преемника (преемник существует — мы уже zombie), недекодируемый объект; а также состояние полосы, которое машина не порождает (не `Ready` при выделении нового ID) | остановка, `ledger_fault`. Причина не типизируется; ключ и прочитанное (поля заголовка или текст ошибки декодирования) — только в сообщении |
> | R15 | объединена с R14 | — |
> | R17 | дефект нашего кода: исключение, вышедшее из машины нити продления или получения заново (кроме `MEMORY_LIMIT_EXCEEDED`, раздел ниже) и не отнесённое к строкам выше по месту возникновения | остановка, `local_defect`. Нить не перезапускается |

Other deltas:

- Paragraph "Латч с нити писателя": the latch covers R14 only (R15 merged, the ref-lane clause of R17
  moved into R14). The sentence on the classifier exception (`MEMORY_LIMIT_EXCEEDED` returns the
  attempt to `Ready` / keeps `Wedged`; other exceptions are R17) is replaced by: "Путь фиксации
  ref-полосы (`commitRefChunk`, `resolveWedgeOnce`) идёт под
  `LockMemoryExceptionInThread(VariableContext::Global)`; классификатора нет, вердикт — равенство
  байтов; `std::bad_alloc` пробрасывается."
- Seal semantics: a successor's `EpochSeal` at an id this mount derived is no longer "conclusive
  rejection, lane `Closed`" (INV-2's reaction in the 2026-07-28 ref-chain design); it is R14. The
  fencing half of INV-2 is unchanged: the write-once key still stops the zombie's write.
- PR row 10 becomes: "Занятый ID ref-полосы: равные байты — продолжение, всё прочее — одна
  нетипизированная ошибка (R14) с латчем первым действием во всех ветках (append, разрешение wedge,
  `runRecoveryWalkOnce`, полоса не в `Ready`); один путь отчёта (`scheduleRemount` первым); путь
  фиксации под `LockMemoryExceptionInThread(Global)`; тест сохранения выхода из `NeedsRecovery`;
  модель ref-полосы до реализации".
- Tests row 10 becomes: "полоса выходит из `NeedsRecovery` без remount (сохранение); чужая транзакция,
  `EpochSeal` преемника и недекодируемый объект в своём слоте → R14 с латчем до отчёта во всех трёх
  ветках; полоса не в `Ready` → R14; remount запрошен, даже если отчёт бросил; лимит памяти на пути
  фиксации не даёт ошибки, append завершается; `std::bad_alloc` пробрасывается".
- Section 9 row "модель ref-полосы": columns become "занятый слот: свои байты / одна ошибка" and
  "каждая ошибка с латчем; `Faulted` без выхода недостижим; терминальное наблюдение терминально".
- PR 12 maps R14 to the stop reason `ledger_fault` instead of `ledger_interference`/`ledger_corrupt`.

## Code vs dispatch

- **The rows are R14, R15 and half of R17, not R15–R17.** R16 is the identity probe and is not touched.
- **Deposed-lane gate.** `flushRefBatch` refuses before sending when the lane's only seal is of its own
  live epoch (`INVALID_STATE`, `CASRefAppendSealRejected`, no latch, no remount). It is a fourth place
  where "a successor exists" is known, but no id is occupied (nothing is read). The dispatch names
  three sites, so this plan leaves the gate unchanged and keeps the counter. Its producer shrinks: the
  two seal-collision arms that recorded `last_epoch_seal` are gone; recovery replaying a seal of the
  live epoch is what remains. Owner decision needed on whether it should latch too.
- **`CASRefAppendOccupantUnreadable` is still counted** by the arm where the settling read names no
  occupant (it wedges, it is not a fault). It is kept; the classifier-exception increment goes. No new
  fault counter: the fault is a `LOG_ERROR` plus a `ForeignInterference` event.
- **"Remount requested even when the report's logging throws".** `LOG_ERROR` swallows channel failures
  and `EventDispatcher::emit` swallows sink failures, so neither can be made to throw from a test.
  Task 3 adds the test seam `PoolConfig::ref_lane_fault_report_hook_for_test`, called right after
  `scheduleRemount` and before `LOG_ERROR`.

## Limits

- A `std::bad_alloc` while describing an already-latched fault is swallowed by `reportLatchedFault`
  so the remount request is not skipped. Everywhere else `bad_alloc` propagates.
- The recovery walk's dead-epoch seal slot is unchanged (see Review Focus).
- What the latch-order tests observe: the `ForeignInterference` event sees the fence closed and the
  remount already requested. The order against `fmt::format` inside the ledger is read from the diff.

---

## Task 0: worktree and branch

- [ ] Confirm `cas-95-1` is free: `git -C /home/mfilimonov/workspace/ClickHouse/cas-95-1 status --short`
  prints nothing outside `contrib/` and `tmp/`. If it prints any `src/` or `docs/` path, stop and ask
  the controller.
- [ ] `git -C /home/mfilimonov/workspace/ClickHouse/cas-95-1 switch -c fix/antalya-26.6/cas-ref-lane-fault-latch 5b7c9f503df`
- [ ] `cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && cmake build > build/cmake_pr10v2.log 2>&1 && cmake build_asan > build_asan/cmake_pr10v2.log 2>&1`

## Task 1: model — an occupied slot is one fault

**Files (master worktree):** `docs/superpowers/models/CaRefLaneCore.tla`,
`docs/superpowers/models/run_reflane.sh`, `docs/superpowers/models/CaRefLaneCore_RESULTS.md`,
`docs/superpowers/models/CaRefLaneCore_witness_faulted.cfg`,
`docs/superpowers/models/CaRefLaneCore_witness_rebirthresolver.cfg` (unchanged content, re-run),
rename `CaRefLaneCore_witness_recoveryforeign.cfg` → `CaRefLaneCore_witness_recoveryoccupied.cfg`,
delete `CaRefLaneCore_witness_unreadablefault.cfg`, `CaRefLaneCore_witness_defectfault.cfg`,
`CaRefLaneCore_witness_classifyretry.cfg`, `CaRefLaneCore_witness_closed.cfg`.

**Produces:** 32 expectations (1 honest, 16 sabotages, 15 witnesses), all PASS.

- [ ] **Edit `CaRefLaneCore.tla`.** Exact replacements:
  1. Definitions block:
     ```tla
     Observations == {"none", "pending", "Durable", "Unknown", "Occupied"}
     (* An occupied slot is our exact bytes ("Durable") or a fault; nothing in between. *)
     ResolvedObservations == {"Durable", "Unknown", "Occupied"}
     FaultRecords == {"none", "Occupied", "RecoveryOccupied"}
     ```
     replacing the five lines from `Observations ==` through `FaultRecords ==` (drop
     `OccupantOutcomes`, `FaultOutcomes` and their comment).
  2. Rename `ObserveSuccessorSeal` → `ObserveOccupied` and set `observation' = "Occupied"`. Delete
     `ObserveOccupant(o)`. Rename `ObserveSuccessorSealScoped` → `ObserveOccupiedScoped` (body
     unchanged, it keeps `ObservationTarget`, which `sab_observationretarget` attacks). Delete
     `ObserveOccupantScoped(o)`.
  3. In `ApplyResolution`: `[] observation \in {"Unknown", "ClassifyMemory"} ->` becomes
     `[] observation = "Unknown" ->`; delete the whole `[] observation = "SuccessorSeal" ->` arm (the
     one that sets `lane' = "Closed"`); `[] observation \in FaultOutcomes ->` becomes
     `[] observation = "Occupied" ->`.
  4. In `ApplyResolutionScoped`: delete the line `/\ UNCHANGED saw_classify_retry`.
  5. Replace `WriteConflict(o)` and `WriteConflictScoped(o)` with:
     ```tla
     (* The append's conditional create lost to an object that is not this attempt's bytes. The
        attempt did not land; the lane faults and latches in the same step. *)
     WriteConflict ==
         /\ lane = "Writing"
         /\ durable_id = cache_id
         /\ attempt' = NoAttempt
         /\ lane' = "Faulted"
         /\ saw_faulted' = TRUE
         /\ terminal_observed' = TRUE
         /\ fault_latched' = (fault_latched \/ ~SabotageUnlatchedFault)
         /\ fault_record' = "Occupied"
         /\ UNCHANGED << cache_id, durable_id, cache_binding, durable_binding,
                         runtime_generation, authority_generation, resolver_attempt,
                         resolver_generation, observation, bad_append, bad_install,
                         bad_certification, saw_commit, saw_unresolved,
                         saw_retry_created, saw_durable_adoption, saw_recovery,
                         saw_stale_result, saw_certified_while_outstanding >>

     WriteConflictScoped ==
         /\ WriteConflict
         /\ attempt_runtime' = NoRuntime
         /\ UNCHANGED << runtimeObjectVars, runtimeAuxVars, resolver_runtime,
                         resolverProvenanceVars, lane_effect_target, runtime_cache_marker,
                         runtime_durable_marker, bad_lane_retarget, saw_rebirth_old_action >>
     ```
  6. Replace `RecoveryFindsForeign` with:
     ```tla
     (* Recovery meets anything but the retained attempt's bytes at its id above the checkpoint. *)
     RecoveryFindsOccupied ==
         /\ lane = "NeedsRecovery"
         /\ CurrentRuntime
         /\ terminal_observed' = TRUE
         /\ fault_latched' = (fault_latched \/ ~SabotageUnlatchedFault)
         /\ fault_record' = "RecoveryOccupied"
         /\ UNCHANGED << laneVars, runtimeVars >>
     ```
  7. `CoreNext`: replace `\/ ObserveSuccessorSealScoped` with `\/ ObserveOccupiedScoped` and delete
     `\/ \E o \in OccupantOutcomes : ObserveOccupantScoped(o)`. `Next`: replace
     `\/ \E o \in OccupantOutcomes : WriteConflictScoped(o)` with `\/ WriteConflictScoped` and
     `\/ RecoveryFindsForeign` with `\/ RecoveryFindsOccupied`.
  8. Remove `saw_classify_retry` everywhere: the `VARIABLES` entry (and the comma after
     `fault_record` on the line above), `faultVars` (becomes
     `<< fault_latched, terminal_observed, fault_record >>`), `Init`, `TypeOK`, `LeaveFaulted` is
     unaffected (it uses `faultVars`), and the definition `W_ClassifyRetry`.
  9. Remove `saw_closed` everywhere (no action sets it any more):
     `sed -i -E '/^    saw_closed,$/d; /\/\\ saw_closed = FALSE/d; /\/\\ saw_closed \\in BOOLEAN/d; /^W_Closed ==/d; s/saw_closed, //g; s/, saw_closed//g' docs/superpowers/models/CaRefLaneCore.tla`
  10. Witness definitions: delete `W_UnreadableFault`, `W_DefectFault`, `W_ClassifyRetry`; replace
      ```tla
      W_Faulted == ~(saw_faulted /\ lane = "Faulted" /\ fault_latched /\ fault_record = "Occupied")
      W_RecoveryOccupied == ~(fault_record = "RecoveryOccupied" /\ lane = "NeedsRecovery" /\ fault_latched)
      ```
      for the old `W_Faulted` and `W_RecoveryForeign`; in `W_RebirthResolverScope` replace
      `/\ saw_closed` with `/\ saw_faulted`.
- [ ] **Check** `grep -nE 'saw_closed|saw_classify_retry|SuccessorSeal|ClassifyMemory|ClassifyDefect|Unreadable|OccupantOutcomes|FaultOutcomes|RecoveryForeign' docs/superpowers/models/CaRefLaneCore.tla`
  prints nothing.
- [ ] **Configs.** `git rm -- docs/superpowers/models/CaRefLaneCore_witness_unreadablefault.cfg docs/superpowers/models/CaRefLaneCore_witness_defectfault.cfg docs/superpowers/models/CaRefLaneCore_witness_classifyretry.cfg docs/superpowers/models/CaRefLaneCore_witness_closed.cfg`;
  `git mv -- docs/superpowers/models/CaRefLaneCore_witness_recoveryforeign.cfg docs/superpowers/models/CaRefLaneCore_witness_recoveryoccupied.cfg`
  and in it replace `W_RecoveryForeign` with `W_RecoveryOccupied`. `CaRefLaneCore_witness_faulted.cfg`
  keeps `W_Faulted` (its definition changed).
- [ ] **`run_reflane.sh`.** Delete the rows `witness_closed`, `witness_unreadablefault`,
  `witness_defectfault`, `witness_classifyretry`; replace
  `"witness_recoveryforeign violation W_RecoveryForeign"` with
  `"witness_recoveryoccupied violation W_RecoveryOccupied"`.
- [ ] **Run** from the master root: `mkdir -p tmp/lease-series && TLC_WORKERS=4 nice -n 10 docs/superpowers/models/run_reflane.sh > tmp/lease-series/pr10v2-reflane.log 2>&1; echo EXIT=$?`.
  Expected: `EXIT=0`, 32 rows, every `VERDICT` is `PASS`. A `FAIL` on `safe` or a sabotage is a model
  error to fix, not to suppress. Do not commit any `CaRefLaneCore_TTrace_*` file.
- [ ] **`CaRefLaneCore_RESULTS.md`.** Set the date line to "updated 2026-10-08 (v2)"; "All 36
  expectations" → "All 32 expectations"; "nineteen reachability witnesses" → "fifteen reachability
  witnesses"; re-read the honest run's generated/distinct/depth and the `sab_observationretarget` /
  `witness_rebirthresolver` figures from the log under `build/tlc-runs/reflane/<RUN_ID>` and replace
  the numbers in place (never reuse the old ones); in the `W_RebirthResolverScope` paragraph replace
  "reaches `Closed`" with "reaches `Faulted`"; replace the section "Occupant outcomes and the fault
  latch" body with:

  > An occupied slot has two outcomes. Our exact bytes adopt the attempt (`Durable`). Anything else —
  > the resolver's `Occupied` observation, the append's conflict (`WriteConflict`), or recovery
  > meeting another object at its retained attempt (`RecoveryFindsOccupied`) — faults and latches the
  > fence in the same step (`FaultHasExit`). A terminal observation never returns the lane to `Ready`
  > (`TerminalObservationIsTerminal`), and a `Faulted` lane never leaves `Faulted`
  > (`FaultedAbsorbing`). `Closed` is no longer reachable: a successor's seal at our slot is a fault.
  > `sab_unlatchedfault`, `sab_recoverafterfault` and `sab_leavefaulted` are red on their target.
  > `witness_faulted` reaches a latched `Faulted` lane; `witness_recoveryoccupied` reaches a latched
  > `NeedsRecovery` lane; `witness_faultremount` shows a remount is reachable after a latch. Not
  > modelled: a non-`Ready` lane at new-id allocation (`StartWrite` requires `Ready`) and remount
  > liveness.
- [ ] **Commit** (master worktree):
  `git add -- docs/superpowers/models/CaRefLaneCore.tla docs/superpowers/models/run_reflane.sh docs/superpowers/models/CaRefLaneCore_RESULTS.md docs/superpowers/models/CaRefLaneCore_witness_recoveryoccupied.cfg`
  (the `git rm`/`git mv` above are already staged); `git status --short -- docs/superpowers/models`
  shows only these paths staged. Message: `model(cas): an occupied ref-lane slot is our bytes or one fault`.

## Task 2: one fault for an occupied id, latched before its report

**Files.** Production: `CA/Pool/CasRefLedger.h`, `CA/Pool/CasRefLedger.cpp`, `CA/Pool/CasPool.h`,
`CA/Pool/CasPool.cpp`. Tests: `src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp`,
`src/Disks/tests/gtest_cas_ref_recovery_cas_walk.cpp`, `src/Disks/tests/gtest_cas_detached_work.cpp`.

**Consumes:** `CasMountRuntime::tripMountLost`, `scheduleRemount`. **Produces:** ledger ctor
parameter `std::function<void()> latch_mount_fault_` (inserted right before
`on_impossible_interference_`); private `template <typename Describe> String reportLatchedFault(const RootNamespace &, Describe &&)`.

### 2.1 Tests first

- [ ] In `gtest_cas_ref_wedge_every_attempt.cpp` **delete** the tests
  `SuccessorSealAtTheWedgedKeyRejectsConclusivelyAndSourcesPrevEpochSeal` and
  `AppendSiteMeetingASuccessorSealIsAConclusiveRejectionNotInterference` (replaced by
  `CASRefLaneFault.WedgeSuccessorSealIsAFault` / `AppendSiteSuccessorSealIsAFault` below) and the now
  unused `extern const Event CASRefAppendSealRejected;`.
- [ ] **Re-target** in the same file:
  - `CreationCkptSurvivesAConclusiveFirstRefLogRejection` → rename to
    `CreationCkptSurvivesASealAtTheFirstRefLogId`; `INVALID_STATE` → `CORRUPTED_DATA`; message
    "must survive a conclusively rejected first ref-log PUT unchanged" → "must survive a fault at the
    first ref-log id unchanged".
  - `CreationCkptSurvivesALaterConclusiveRejection` → rename to `CreationCkptSurvivesASealAtALaterRefLogId`;
    `INVALID_STATE` → `CORRUPTED_DATA`; in its comment replace
    `` `AppendSiteMeetingASuccessorSealIsAConclusiveRejectionNotInterference` `` with
    `` `CASRefLaneFault.AppendSiteSuccessorSealIsAFault` ``.
  - `ALiveEpochSealIsNeverStampedAsItsOwnPrevEpochSeal` (behaviour unchanged): comment
    "A seal of the LIVE epoch — the deposed-lane shape the wedge rejection arm can record." →
    "A seal of the LIVE epoch — what recovery records when it replays a successor's seal of this
    epoch."; "the conclusive-rejection class the successor-seal arm uses," → "the conclusive-rejection
    class,"; replace the three sentences after "NO remount is scheduled," with "this lane learned the
    deposition from recovered state, not from an occupied id, and only an occupied id is a fault."
  - File header: replace "-- our own earlier write (adopt), a successor's `EpochSeal` (the operation
    is conclusively rejected and never was acked), or a foreign object (impossible under mount-lease
    exclusivity: fail loud)." with "-- our own earlier write (adopt), or anything else, a successor's
    `EpochSeal` included (a fault: the mount is fenced and remounts)."; replace the `prev_epoch_seal`
    bullet's first sentence with "`prev_epoch_seal`: the seal recovery records for a closed epoch
    becomes the `prev_epoch_seal` the next sequence-1 append carries."
  - Section banner "A successor's `EpochSeal` is the conclusive rejection (spec INV-2)" →
    "`prev_epoch_seal` on the wire"; banner "The append site owes the SAME three-way adjudication as
    the wedge site" → "The append site".
- [ ] In `gtest_cas_ref_recovery_cas_walk.cpp` replace the test
  `RetainedOldWriterAttemptLosesConclusiveToASuccessorSeal` with:

```cpp
/// The retained attempt is this writer's own, so a successor's seal at its id means this mount was
/// deposed: a fault, not a record to replay.
TEST(CASRefRecoveryCasWalk, RetainedWriterAttemptMeetingASuccessorSealFaultsTheMount)
{
    auto backend = std::make_shared<HidingListBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/recovery_successor_seal"};
    auto store = openWalkPool(backend);
    const NamespaceLifeId life = strandOneUnfrontieredSuccessor(backend, store, layout, ns);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    const RefTxnId successor_id{1, 2};
    const String successor_key = layout.refLogKey(life, successor_id);
    const auto original = readAt(*backend, successor_key);
    ASSERT_TRUE(original);
    const RefLogTxn successor_seal = makeSealTxn(ns, successor_id);
    {
        OperationForTest op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(successor_key,
            sealObject(FormatId::RefLog, encodeRefLogTxn(successor_seal)),
            original->etag, Retry::once())));
    }
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    ASSERT_TRUE(store->mayMutate());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_FALSE(store->mayMutate()) << "the fault latches the mount fence";
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
    EXPECT_EQ(readCkptForTest(backend, layout, life)->ckpt.committed_through, (RefTxnId{1, 1}))
        << "the seal must not be published as this writer's frontier";
}
```

- [ ] In `gtest_cas_ref_wedge_every_attempt.cpp` add after the last test:

```cpp
/// ================================================================================================
/// An occupied ref-log id: our own bytes continue, anything else is one fault
/// ================================================================================================

namespace
{

constexpr std::string_view undecodable_occupant = "not a ref-log object at all";

enum class OccupantKind : uint8_t
{
    ForeignTxn,
    EpochSeal,
    Undecodable,
};

/// A decodable transaction of `ns` at `id` that is neither ours nor a seal, a successor's seal, or
/// bytes that do not decode.
String occupantBytes(OccupantKind kind, const RootNamespace & ns, const RefTxnId & id)
{
    switch (kind)
    {
        case OccupantKind::ForeignTxn:
        {
            RefOp birth;
            birth.kind = RefOpKind::NamespaceBirth;
            return sealObject(FormatId::RefLog, encodeRefLogTxn(RefLogTxn{ns.string(), id, {birth}, std::nullopt}));
        }
        case OccupantKind::EpochSeal:
            return epochSealBytes(ns, id);
        case OccupantKind::Undecodable:
            return String{undecodable_occupant};
    }
    throw std::logic_error("unknown occupant kind");
}

/// The words the fault report uses for what it read.
std::string_view occupantDescription(OccupantKind kind)
{
    switch (kind)
    {
        case OccupantKind::ForeignTxn: return "a transaction of this namespace";
        case OccupantKind::EpochSeal: return "an epoch seal of this namespace";
        case OccupantKind::Undecodable: return "do not decode";
    }
    throw std::logic_error("unknown occupant kind");
}

/// Overwrites `key` in place past every seam: how a test damages or restores an object.
void replaceObj(WedgeTestBackend & backend, const String & key, const String & bytes)
{
    const auto current = readObj(backend, key);
    ASSERT_TRUE(current);
    backend.bypass_seams.store(true, std::memory_order_release);
    DB::Cas::tests::OperationForTest op(backend);
    const WriteResult result = (*op).replace(key, bytes, current->etag, Retry::once());
    backend.bypass_seams.store(false, std::memory_order_release);
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
}

/// Leaves the lane `NeedsRecovery`: the drop's log is durable and every replace of its checkpoint
/// frontier times out.
void enterNeedsRecoveryOnDrop(WedgeTestBackend & backend, const PoolPtr & store, const RootNamespace & ns,
                              const String & ckpt_key, const String & ref)
{
    backend.fail_cas_substr = ckpt_key;
    backend.fail_cas_latched = true;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, ref); });
    backend.fail_cas_latched = false;
    backend.fail_cas_substr.clear();
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
}

/// One delivered `ForeignInterference` event, with what the mount looked like when it arrived.
struct FaultEvent
{
    String reason;
    bool fence_closed = false;
    uint64_t remounts_requested = 0;
};

class FaultEventLog
{
public:
    void push(FaultEvent event) { std::lock_guard lock(mutex); events.push_back(std::move(event)); }
    std::vector<FaultEvent> snapshot() const { std::lock_guard lock(mutex); return events; }

private:
    mutable std::mutex mutex;
    std::vector<FaultEvent> events;
};

/// The sink holds a raw `Pool *`: the pool owns its sink, so a `PoolPtr` here would be a cycle.
std::shared_ptr<FaultEventLog> recordFaultEvents(const PoolPtr & store)
{
    auto log = std::make_shared<FaultEventLog>();
    Pool * pool = store.get();
    store->setEventSink([log, pool](const CasEvent & e)
    {
        if (e.type != CasEventType::ForeignInterference)
            return;
        log->push({e.reason, !pool->mayMutate(), pool->scheduleRemountCallCountForTest()});
    });
    return log;
}

void expectOneLatchedFault(const FaultEventLog & log, uint64_t remounts_before, std::string_view description)
{
    const auto events = log.snapshot();
    ASSERT_EQ(events.size(), 1u);
    EXPECT_TRUE(events[0].fence_closed) << "the latch must precede the report";
    EXPECT_EQ(events[0].remounts_requested, remounts_before + 1) << "the remount must be requested before the event";
    EXPECT_NE(events[0].reason.find(description), String::npos) << events[0].reason;
}

void expectAppendSiteFault(const String & ns_name, OccupantKind kind)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{ns_name};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    const RefTxnId next{store->liveWriterEpoch(), 3};
    backend->conflict_substr = store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), next);
    backend->conflict_bytes = occupantBytes(kind, ns, next);
    backend->conflict_count = 1;
    const auto faults = recordFaultEvents(store);
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    ASSERT_TRUE(store->mayMutate());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::nullopt) << "a fault records no epoch seal";
    EXPECT_FALSE(store->mayMutate());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
    expectOneLatchedFault(*faults, remounts_before, occupantDescription(kind));
}

void expectWedgeFault(const String & ns_name, OccupantKind kind)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend, wedgeTestBudget());
    auto clock = VirtualRetryClock::installOn(store);
    const RootNamespace ns{ns_name};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    wedgeLaneOnUnresolvedAppend(*clock, *backend, store, ns, [&] { store->dropRef(ns, "x"); });
    const String wedged_key = store->wedgedKeyForTest(ns);
    const auto parsed = store->layout().parseRefObjectKey(wedged_key);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_TRUE(std::holds_alternative<Committed>(
        backend->putAsSuccessor(wedged_key, occupantBytes(kind, ns, parsed->txn_id))));
    const auto faults = recordFaultEvents(store);
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    ASSERT_TRUE(store->mayMutate());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->lastEpochSealForTest(ns), std::nullopt) << "a fault records no epoch seal";
    EXPECT_FALSE(store->mayMutate());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
    expectOneLatchedFault(*faults, remounts_before, occupantDescription(kind));
}

/// Recovery finds something other than the retained attempt's bytes at its id above the checkpoint.
void expectRecoveryFault(const String & ns_name, OccupantKind kind)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend, wedgeTestBudget());
    auto clock = VirtualRetryClock::installOn(store);
    const RootNamespace ns{ns_name};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);
    const String ckpt_key = store->layout().refCkptKey(life);
    const RefCkpt ckpt_before = decodeRefCkpt(readObj(*backend, ckpt_key)->bytes);
    ASSERT_TRUE(ckpt_before.committed_through);
    const RefTxnId retained{ckpt_before.committed_through->writer_epoch,
                            ckpt_before.committed_through->ref_sequence + 1};
    enterNeedsRecoveryOnDrop(*backend, store, ns, ckpt_key, "x");
    replaceObj(*backend, store->layout().refLogKey(life, retained), occupantBytes(kind, ns, retained));
    const auto faults = recordFaultEvents(store);
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    ASSERT_TRUE(store->mayMutate());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_FALSE(store->mayMutate());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
    expectOneLatchedFault(*faults, remounts_before, occupantDescription(kind));
}

}

TEST(CASRefLaneFault, AppendSiteForeignTxnLatchesBeforeTheReport)
{
    expectAppendSiteFault("srv1/fault_append_foreign", OccupantKind::ForeignTxn);
}

/// A successor exists only if this mount was already deposed, so its seal at our id is a fault.
TEST(CASRefLaneFault, AppendSiteSuccessorSealIsAFault)
{
    expectAppendSiteFault("srv1/fault_append_seal", OccupantKind::EpochSeal);
}

TEST(CASRefLaneFault, AppendSiteUndecodableOccupantIsAFault)
{
    expectAppendSiteFault("srv1/fault_append_undecodable", OccupantKind::Undecodable);
}

TEST(CASRefLaneFault, WedgeForeignTxnLatchesBeforeTheReport)
{
    expectWedgeFault("srv1/fault_wedge_foreign", OccupantKind::ForeignTxn);
}

TEST(CASRefLaneFault, WedgeSuccessorSealIsAFault)
{
    expectWedgeFault("srv1/fault_wedge_seal", OccupantKind::EpochSeal);
}

TEST(CASRefLaneFault, WedgeUndecodableOccupantIsAFault)
{
    expectWedgeFault("srv1/fault_wedge_undecodable", OccupantKind::Undecodable);
}

TEST(CASRefLaneFault, RecoveryForeignTxnAtTheRetainedIdLatchesBeforeTheReport)
{
    expectRecoveryFault("srv1/fault_recovery_foreign", OccupantKind::ForeignTxn);
}

TEST(CASRefLaneFault, RecoveryUndecodableOccupantAtTheRetainedIdIsAFault)
{
    expectRecoveryFault("srv1/fault_recovery_undecodable", OccupantKind::Undecodable);
}

TEST(CASRefLaneFault, NonReadyLaneAtNewIdLatchesBeforeTheReport)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/fault_non_ready"};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    const auto faults = recordFaultEvents(store);
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    ASSERT_TRUE(store->mayMutate());
    store->setRefPreCarveHookForTest([&]
    {
        store->forceWedgeForTest(ns, /*writer_epoch*/ 1, /*ref_sequence*/ 1, "bogus/_log/key", "bogus-bytes");
    });

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
    expectOneLatchedFault(*faults, remounts_before, "instead of Ready");
}

/// A lane in `NeedsRecovery` leaves it on the next touch once its checkpoint reads again, with no
/// remount: a damaged `_ckpt` fails that touch and leaves the lane where it was; the byte-identical
/// original lets the following touch replay the durable drop and commit its own.
TEST(CASRefLaneFault, NeedsRecoveryLaneLeavesAfterADamagedCkptIsRestoredWithoutRemount)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend, wedgeTestBudget());
    auto clock = VirtualRetryClock::installOn(store);
    const RootNamespace ns{"srv1/fault_needs_recovery_ckpt"};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    const String ckpt_key = store->layout().refCkptKey(*store->refTableLifeForTest(ns));
    enterNeedsRecoveryOnDrop(*backend, store, ns, ckpt_key, "x");
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    const uint64_t installs_before = store->recoveryInstallCountForTest();

    const String original = readObj(*backend, ckpt_key)->bytes;
    replaceObj(*backend, ckpt_key, original.substr(0, original.size() / 2));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    replaceObj(*backend, ckpt_key, original);
    EXPECT_NO_THROW(store->dropRef(ns, "y"));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_EQ(store->recoveryInstallCountForTest(), installs_before + 1);
    EXPECT_FALSE(store->resolveRef(ns, "x").has_value()) << "recovery replayed the durable drop";
    EXPECT_FALSE(store->resolveRef(ns, "y").has_value());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before);
    EXPECT_TRUE(store->mayMutate());
}
```

- [ ] **Build and run on the base code** (tests only changed so far):
  `ninja -C build unit_tests_dbms > build/ninja_pr10v2_t2_red.log 2>&1` (expect `NINJA_EXIT=0`), then
  `build/src/unit_tests_dbms --gtest_filter='CASRefLaneFault.*:CASRefWedgeEveryAttempt.CreationCkptSurvivesASeal*:CASRefRecoveryCasWalk.RetainedWriterAttemptMeetingASuccessorSealFaultsTheMount' > build/test_pr10v2_t2_red.log 2>&1`.
  Expected (have a subagent summarise the log):
  - `AppendSiteForeignTxn…`, `AppendSiteUndecodable…`, `WedgeForeignTxn…`, `WedgeUndecodable…`,
    `NonReadyLane…`: fail on "the latch must precede the report" and "the remount must be requested
    before the event" (base logs and emits, then trips the fence and schedules the remount).
  - `AppendSiteSuccessorSealIsAFault`, `WedgeSuccessorSealIsAFault`, both `CreationCkptSurvivesASeal…`:
    fail `e.code()` "Which is: 668" vs "Which is: 246" (`INVALID_STATE` vs `CORRUPTED_DATA`).
  - `Recovery…` (both): fail `events.size()` "Which is: 0" vs 1, and the remount count.
  - `RetainedWriterAttemptMeetingASuccessorSealFaultsTheMount`: fails "expected DB::Exception".
  - `NeedsRecoveryLaneLeaves…`: passes (golden).

### 2.2 Implementation

- [ ] **`CasRefLedger.h`.**
  - `RefLaneState` doc: replace "`Closed`\n/// records a successor's epoch seal, and `Faulted` records foreign or internally inconsistent durable\n/// state." with "`Closed`\n/// is what an observer reads for a name with no runtime, and `Faulted` records a fault: our slot holding\n/// anything but our own bytes, or a lane state this machine cannot produce."
  - Constructor, between `std::function<bool()> may_mutate_,` and the `on_impossible_interference_` line:
    ```cpp
            /// Closes the mount's write fence on a ref lane fault. Called before anything that can throw,
            /// so it must not allocate, block or throw.
            std::function<void()> latch_mount_fault_,
            /// Reports a fault already latched: the offending key, what was read there, the namespace.
    ```
  - Member, before `on_impossible_interference;`: `std::function<void()> latch_mount_fault;`
  - `enum class WedgeResolution`: delete `Rejected,`.
  - `commitRefChunk` doc: "(a rejected apply / DefiniteFailure / unresolved wedge / a conclusive PUT
    rejection / an encode failure)" → "(a rejected apply / DefiniteFailure / unresolved wedge / a ref
    lane fault / an encode failure)".
  - After the `requireRecovery` declaration:
    ```cpp
        /// Reports a fault the caller has already latched, with the key and detail `describe(key, detail)`
        /// fills in. The report runs even when `describe` throws, because only the report requests the
        /// remount. Returns the detail.
        template <typename Describe>
        String reportLatchedFault(const RootNamespace & ns, Describe && describe);
    ```
- [ ] **`CasRefLedger.cpp`.**
  - Includes: add `#include <Common/LockMemoryExceptionInThread.h>` after `#include <Common/Exception.h>`
    (used in Task 4; adding it now keeps Task 4 to two lines). Remove `extern const int UNKNOWN_FORMAT_VERSION;`.
  - In the anonymous namespace, replace everything from `/// What sits at a ref-log key that our own
    conditional create just lost to.` through the closing `}` of `chainLinkFor` with:
    ```cpp
    /// What an occupied ref-log id holds, for the fault report only: no outcome depends on it.
    String describeRefLogOccupant(const RootNamespace & ns, const RefTxnId & id, const String & occupant_bytes)
    {
        try
        {
            const RefLogTxn txn = decodeRefLogTxn(openObject(FormatId::RefLog, occupant_bytes), ns.string(), id);
            if (refLogTxnIsEpochSeal(txn))
                return "an epoch seal of this namespace";
            return fmt::format("a transaction of this namespace with {} ops", txn.ops.size());
        }
        catch (...)
        {
            return fmt::format("{} bytes that do not decode: {}", occupant_bytes.size(),
                               getCurrentExceptionMessage(/*with_stacktrace*/ false));
        }
    }

    <the chainLinkFor doc comment, moved verbatim from above classifyRefLogOccupant: the 11 lines from
     "/// The `prev_epoch_seal` a transaction at `id` carries" to "/// meet the seal, which is the intended conclusive rejection.">
    std::optional<RefTxnId> chainLinkFor(const RefTxnId & id, const std::optional<RefTxnId> & last_epoch_seal)
    {
        return id.ref_sequence == 1 && last_epoch_seal
            && last_epoch_seal->writer_epoch + 1 == id.writer_epoch
            ? last_epoch_seal : std::nullopt;
    }
    ```
    In the moved comment, replace the last sentence "Stamping nothing lets the attempt go out and
    meet the seal, which is the intended conclusive rejection." with "Stamping nothing lets the
    attempt go out and meet the seal, which faults the mount."
    Then delete the old copy of that comment and the whole `classifyRefLogOccupant` function (they sit
    right after `makeEpochSealTxn`). `grep -n "Occupant::\|classifyRefLogOccupant" ` must print nothing.
  - After the anonymous namespace's closing `}` and before `CasRefLedger::CasRefLedger(`:
    ```cpp
    template <typename Describe>
    String CasRefLedger::reportLatchedFault(const RootNamespace & ns, Describe && describe)
    {
        String key;
        String detail;
        std::optional<String> offending_ns;
        try
        {
            describe(key, detail);
            offending_ns = ns.string();
        }
        catch (...)   // NOLINT(bugprone-empty-catch)
        {
            /// The fence is already closed and only the report requests the remount, so a failure to
            /// describe the fault must not skip it.
        }
        on_impossible_interference(key, detail, offending_ns);
        return detail;
    }
    ```
  - Constructor: add `std::function<void()> latch_mount_fault_,` before the
    `on_impossible_interference_` parameter and `, latch_mount_fault(std::move(latch_mount_fault_))`
    before `, on_impossible_interference(...)`.
  - `runRecoveryWalkOnce`, the retained-attempt block. Replace
    ```cpp
                if (above_sampled_frontier && retained_attempt && retained_attempt->txn_id == id)
                {
                    /// `NeedsRecovery` retains ... <7 comment lines> ...
                    const Occupant occupant = classifyRefLogOccupant(ns, id, got->bytes, retained_attempt->bytes);
                    if (occupant == Occupant::Foreign)
                        throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "CAS ref-table recovery for namespace '{}': a DIFFERENT object occupies retained "
                            "writer txn {}-{} above the exact checkpoint frontier; recovery must not publish "
                            "or install it as that writer's history",
                            ns.string(), id.writer_epoch, id.ref_sequence);
                }
    ```
    with
    ```cpp
                if (above_sampled_frontier && retained_attempt && retained_attempt->txn_id == id
                    && got->bytes != retained_attempt->bytes)
                {
                    /// `NeedsRecovery` retains the writer's exact bytes for this check. The id is this
                    /// writer's own, so anything else there -- a successor's seal included, which means
                    /// this mount was deposed -- is a fault, never history to replay.
                    latch_mount_fault();
                    const String detail = reportLatchedFault(ns, [&](String & key, String & text)
                    {
                        key = layout.refLogKey(life, id);
                        text = fmt::format(
                            "ref-table recovery for namespace '{}' found retained writer txn {}-{} above the "
                            "exact checkpoint frontier holding {}",
                            ns.string(), id.writer_epoch, id.ref_sequence,
                            describeRefLogOccupant(ns, id, got->bytes));
                    });
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS {}; recovery must not publish or install it as that writer's history", detail);
                }
    ```
  - `resolveWedgeOnce`:
    - Replace the block from `/// ---- Classify the occupant OFF the lock: pure, and the decode allocates ----`
      through `= std::holds_alternative<Committed>(*attempted) || occupant == Occupant::Ours;` with:
      ```cpp
          /// ---- Name the occupant: our exact bytes, or a fault ----
          /// "Ours" means BYTE EQUALITY, never a shape or generation match: two attempts can share both
          /// and differ in bytes. Anything else at our slot, a successor's seal included, is a fault.
          const auto * conflict = std::get_if<Conflict>(&*attempted);
          const auto * occupant_object = conflict ? std::get_if<Object>(&conflict->seen) : nullptr;
          const auto * gave_up = std::get_if<GaveUp>(&*attempted);
          /// A conflict whose settling read observed nothing is as unresolved as a give-up: the key holds
          /// something this call could not name, so nothing may be adopted or unwedged from it.
          const bool unresolved = gave_up || (conflict && !occupant_object);
          const bool occupied_by_ours = occupant_object && occupant_object->bytes == wedge.bytes;
          const bool occupied_by_other = occupant_object && !occupied_by_ours;
          const bool exact_attempt_is_durable = std::holds_alternative<Committed>(*attempted) || occupied_by_ours;
      ```
    - In the frontier comment, replace "This deliberately runs only for `Created` or byte-identical
      `Ours`: a successor seal is conclusive evidence that OUR transaction did not land and retains the
      rejection path below without publishing our frontier." with "This runs only for `Created` or our
      exact bytes: any other occupant means our transaction did not land."
    - Replace the two arms `else if (occupant == Occupant::SuccessorSeal) { … }` and
      `else if (occupant == Occupant::Foreign) { … }` with:
      ```cpp
              else if (occupied_by_other)
              {
                  /// Under mount-lease exclusivity nothing else may write our slot, and a successor's seal
                  /// means this mount was already deposed. The lane is done; nothing remains to retry.
                  rt->append_attempt.reset();
                  rt->lane_state = RefLaneState::Faulted;
                  result.kind = WedgeResolution::Corrupted;
              }
      ```
    - Right after the closing `}` of the post-I/O recheck block and before
      `/// ---- Everything that allocates or reacts, now that `state_mutex` is released ----`:
      ```cpp
          if (result.kind == WedgeResolution::Corrupted)
              latch_mount_fault();
      ```
    - In the final `switch`, delete the `case WedgeResolution::Rejected:` arm and replace the
      `case WedgeResolution::Corrupted:` arm with:
      ```cpp
              case WedgeResolution::Corrupted:
              {
                  const String detail = reportLatchedFault(ns, [&](String & key, String & text)
                  {
                      key = wedge.key;
                      text = fmt::format(
                          "ref-log wedge resolution for namespace '{}' txn {}-{} found the wedged slot holding {}",
                          ns.string(), wedge.txn_id.writer_epoch, wedge.txn_id.ref_sequence,
                          describeRefLogOccupant(ns, wedge.txn_id, occupant_object->bytes));
                  });
                  result.survivor_error = std::make_exception_ptr(Exception(ErrorCodes::CORRUPTED_DATA,
                      "CAS {} — the mount is fenced closed, a remount is scheduled, and the lane is faulted until "
                      "then", detail));
                  break;
              }
      ```
  - `flushRefBatch`: delete `case WedgeResolution::Rejected:` from the resolution switch; in the
    contract comment delete the clause "a TRANSIENT failure while decoding a foreign occupant
    (deliberately not laundered into a verdict);" (keep the sentence grammatical: "…before anything is
    proven; and an allocation failure in this function's own bookkeeping…").
  - `commitRefChunk`, the non-`Ready` block: replace from `RefLaneState lane_state = RefLaneState::Faulted;`
    through `return false;` of that block with:
    ```cpp
            RefLaneState lane_state = RefLaneState::Faulted;
            {
                std::lock_guard lock(rt->state_mutex);
                lane_state = rt->lane_state;
            }
            if (lane_state != RefLaneState::Ready)
            {
                latch_mount_fault();
                String attempt_key;
                {
                    std::lock_guard lock(rt->state_mutex);
                    if (rt->append_attempt)
                        attempt_key = std::move(rt->append_attempt->key);
                    rt->append_attempt.reset();
                    rt->lane_state = RefLaneState::Faulted;
                }
                reportLatchedFault(ns, [&](String & key, String & text)
                {
                    key = attempt_key;
                    text = fmt::format(
                        "ref-log append reached new-id allocation while the lane was in state {} instead of Ready",
                        static_cast<uint8_t>(lane_state));
                });
                <keep the existing 9-line `/// \`Faulted\` is a TERMINAL lane state …` comment unchanged>
                complete_error(chunk_survivors, std::make_exception_ptr(Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS ref-log append for namespace '{}': refusing to allocate a new ref-log id while the "
                    "lane is not Ready (state {}, attempt key '{}'); the lane is faulted until remount",
                    ns.string(), static_cast<uint8_t>(lane_state), attempt_key)));
                return false;
            }
    ```
  - `commitRefChunk`, the `Conflict` arm. Replace the comment "THREE-WAY ADJUDICATION, the same one the
    wedge resolution owes. … must be exactly as loud as before." (7 lines) with "Our exact bytes mean an
    earlier attempt of this transaction landed; anything else at our id is a fault." Keep the
    "occupant arrives WITH the conflict" sentence and the `if (!occupant_object)` arm unchanged. Replace
    everything from `Occupant occupant = Occupant::Foreign;` through the end of the
    `/// \`Occupant::Ours\`: …` comment with:
    ```cpp
            if (occupant_object->bytes != active_attempt.bytes)
            {
                latch_mount_fault();
                String attempt_key;
                {
                    std::lock_guard lock(rt->state_mutex);
                    if (rt->append_attempt)
                        attempt_key = std::move(rt->append_attempt->key);
                    rt->append_attempt.reset();
                    rt->lane_state = RefLaneState::Faulted;
                }
                const String fault_detail = reportLatchedFault(ns, [&](String & key, String & text)
                {
                    key = std::move(attempt_key);
                    text = fmt::format(
                        "ref-log append for namespace '{}' txn {}-{} found the id it derived holding {} (observed {})",
                        ns.string(), id.writer_epoch, id.ref_sequence,
                        describeRefLogOccupant(ns, id, occupant_object->bytes),
                        detail::renderObservation(conflict->seen));
                });
                complete_error(chunk_survivors, std::make_exception_ptr(Exception(
                    ErrorCodes::CORRUPTED_DATA, "CAS {}", fault_detail)));
                return false;
            }
            /// Our own bytes: an earlier attempt of this exact transaction is durable. The lane machine
            /// does not produce this here (a maybe-landed attempt wedges), but it is our content, so it is
            /// installed below like a fresh commit rather than reported.
    ```
    (`active_attempt` refers to `*rt->append_attempt`; it is read before the reset, never after.)
- [ ] **`CasPool.h`.** Replace the doc of `reportImpossibleInterference` (the 12 lines from
  "/// incidental-detection reaction for a foreign-interference" to "the message can name the specific
  contract that broke.") with:
  ```cpp
      /// The reaction to a ref lane fault the ledger has already latched through `latch_mount_fault`.
      /// Requests the remount first, because nothing else would and everything after it can throw; then
      /// `LOG_ERROR`, a `ForeignInterference` CasEvent, and ONE background GET of `key` for diagnosis
      /// that never blocks or throws on the caller's thread.
  ```
  In the comment above `cancelInflightBuildsForNamespace` replace
  "(`fence_ok_fn` / `cancel_inflight_builds` /\n    /// `on_impossible_interference`)" with
  "(`fence_ok_fn` / `cancel_inflight_builds` /\n    /// `latch_mount_fault` / `on_impossible_interference`)".
- [ ] **`CasPool.cpp`.**
  - Ledger construction: before the `reportImpossibleInterference` lambda insert
    `[this] { mount_runtime.tripMountLost(); },`.
  - `Pool::reportImpossibleInterference`: insert as the first statement
    ```cpp
        /// First: the ledger already closed the fence, and only this requests the remount.
        mount_runtime.scheduleRemount();

    ```
    change the `LOG_ERROR` text to
    `"CAS ref lane fault for server_root '{}' (namespace='{}', key='{}'): {} -- this mount is fenced closed; a remount is scheduled"`
    (same arguments); delete the block "/// Incidental-only detection has the same fail-closed reaction …"
    with its `mount_runtime.tripMountLost();` and `mount_runtime.scheduleRemount();`; in the diagnostic
    comment replace "after the fence was deliberately tripped a few\n /// lines above" with "after the
    fence was deliberately tripped".
- [ ] **`gtest_cas_detached_work.cpp`.** Before `[](const String &, const String &, const std::optional<String> &) {},`
  insert `[] {},`.
- [ ] **Build and run.** `ninja -C build unit_tests_dbms > build/ninja_pr10v2_t2.log 2>&1`, then
  `build/src/unit_tests_dbms --gtest_filter='CASRefLaneFault.*:CASRefWedgeEveryAttempt.*:CASRefRecoveryCasWalk.*:CASAnomalyPolicy.*:CASRefWriterAppendLane.*:CASDetached*:CASWriterDuties.*:CASRefCheckpoint.*' > build/test_pr10v2_t2.log 2>&1`.
  Expected: all pass. Goldens that must stay green and are named here because they pin what does not
  change: `CASRefWedgeEveryAttempt.OwnLandedAttemptIsAdoptedFromOccupiedWithoutDoubleApply` (own
  bytes continue), `…ResultReleasedAfterAFenceBumpAndSuccessorSealIsInert` (a superseded runtime does
  not fault), `…AppendSiteWedgesWhenTheSettlingReadNamesNoOccupant`,
  `…ALiveEpochSealIsNeverStampedAsItsOwnPrevEpochSeal`,
  `CASRefRecoveryCasWalk.DeadEpochIsClosedByOurOwnSealAtTPlusOne`,
  `…ConcurrentRecoverersSealIsAdoptedNotContested`,
  `…StragglerAtTPlusOneIsAdoptedAndResealedAtTheNewTPlusOne`,
  `…UndecodableOccupantAtTheSealSlotFailsClosedAndLeavesRecoveryRestartable` (the recovery walk seals
  the predecessor lane), `CASWriterDuties.RejectedAttemptBodyIsEventuallyNominatedAndSwept`.
- [ ] **Mutation check (not committed).** Delete the `latch_mount_fault();` line in the `Conflict` arm;
  rebuild; `CASRefLaneFault.AppendSite*` fail on "the latch must precede the report". Restore.
- [ ] **Commit.**
  `git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp src/Disks/tests/gtest_cas_ref_recovery_cas_walk.cpp src/Disks/tests/gtest_cas_detached_work.cpp`.
  Message: `Fault the mount on any occupant of a ref-log id but our own bytes`.

## Task 3: the remount is requested even when the report throws

**Files.** `CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp`, `src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp`.
**Produces:** `PoolConfig::ref_lane_fault_report_hook_for_test` (`std::function<void()>`).

- [ ] **Test.** Add `extern const int NOT_IMPLEMENTED;` to the file's `DB::ErrorCodes` block; add to the
  anonymous namespace of the `CASRefLaneFault` section:

```cpp
template <typename BackendT>
PoolPtr openPoolWithFaultReportHook(const std::shared_ptr<BackendT> & backend, std::function<void()> hook)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    backend->setAttemptTimeoutMs(CasRequestBudget{}.attempt_timeout_ms);
    PoolConfig config{.pool_prefix = "p", .server_root_id = "test"};
    config.ref_lane_fault_report_hook_for_test = std::move(hook);
    return Pool::open(backend, std::move(config));
}
```

  and the test:

```cpp
/// Everything in the report after the remount request can throw; the request must already be made.
TEST(CASRefLaneFault, RemountIsRequestedWhenTheFaultReportThrows)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPoolWithFaultReportHook(backend, []
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "injected failure inside the fault report");
    });
    const RootNamespace ns{"srv1/fault_report_throws"};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    const RefTxnId next{store->liveWriterEpoch(), 3};
    backend->conflict_substr = store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), next);
    backend->conflict_bytes = occupantBytes(OccupantKind::ForeignTxn, ns, next);
    backend->conflict_count = 1;
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();

    expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&] { store->dropRef(ns, "x"); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
    EXPECT_FALSE(store->mayMutate());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
}
```

- [ ] `ninja -C build unit_tests_dbms > build/ninja_pr10v2_t3_red.log 2>&1`. Expected failure:
  `no member named 'ref_lane_fault_report_hook_for_test' in 'DB::Cas::PoolConfig'`.
- [ ] **`CasPool.h`**, after `diagnostic_dispatch_error_hook_for_test`:
  ```cpp

      /// TEST SEAM: invoked in the ref lane fault report right after the remount request and before
      /// logging, so a test can make the report throw. Empty in production.
      std::function<void()> ref_lane_fault_report_hook_for_test = {};
  ```
- [ ] **`CasPool.cpp`**, `Pool::reportImpossibleInterference`, right after `mount_runtime.scheduleRemount();`:
  ```cpp
      if (config.ref_lane_fault_report_hook_for_test)
          config.ref_lane_fault_report_hook_for_test();
  ```
- [ ] Build; run `--gtest_filter='CASRefLaneFault.*:CASAnomalyPolicy.*'` into
  `build/test_pr10v2_t3.log`. Expected: all pass. Mutation (not committed): move
  `mount_runtime.scheduleRemount();` below the hook call; the new test fails on the remount count.
- [ ] **Commit.** `git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp`.
  Message: `Pin that a ref lane fault requests the remount before its report can throw`.

## Task 4: the ref-lane commit path does not fail on the server memory limit

**Files.** `CA/Pool/CasRefLedger.cpp`, `src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp`.

- [ ] **Tests.** Add includes `<Common/CurrentMemoryTracker.h>`, `<Common/CurrentThread.h>`,
  `<Common/ThreadStatus.h>`, `<base/scope_guard.h>`, `<new>`. Add to the `CASRefLaneFault` anonymous
  namespace:

```cpp
/// Larger than the untracked batch a thread keeps before it reports to its tracker, so the allocation
/// reaches the trackers at once.
constexpr Int64 over_limit_allocation_bytes = 8 * 1024 * 1024;

/// One allocation through the enforcing tracker path while a `Global`-level tracker above this thread
/// is over its hard limit. The tracker is spliced in between this thread's tracker and its parent, so
/// no other thread can hit the limit. Throws `MEMORY_LIMIT_EXCEEDED` unless this thread holds a
/// `Global`-level `LockMemoryExceptionInThread`.
void allocateOverTheGlobalLimit()
{
    DB::CurrentThread::flushUntrackedMemory();
    MemoryTracker & thread_tracker = *DB::CurrentThread::getMemoryTracker();
    MemoryTracker * const parent = thread_tracker.getParent();
    MemoryTracker over_limit(parent, VariableContext::Global);
    over_limit.setHardLimit(1);
    thread_tracker.setParent(&over_limit);
    SCOPE_EXIT({ thread_tracker.setParent(parent); });
    std::ignore = CurrentMemoryTracker::alloc(over_limit_allocation_bytes);
    std::ignore = CurrentMemoryTracker::free(over_limit_allocation_bytes);
}
```

  and the tests:

```cpp
/// An allocation over the server limit between the PUT and the install neither fails nor faults.
TEST(CASRefLaneFault, AppendCompletesOverTheGlobalMemoryLimit)
{
    /// The splice needs a thread tracker; a debug build already has one on this thread.
    std::optional<DB::ThreadStatus> thread_status{DB::current_thread ? std::nullopt : std::optional<DB::ThreadStatus>(std::in_place)};
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/fault_append_memory_limit"};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    const auto faults = recordFaultEvents(store);
    bool allocated = false;
    store->setCarveHookForTest([&](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::PostDurableInstall || allocated)
            return;
        allocated = true;
        allocateOverTheGlobalLimit();
    });

    EXPECT_NO_THROW(store->dropRef(ns, "x"));
    store->setCarveHookForTest(nullptr);

    EXPECT_TRUE(allocated);
    EXPECT_FALSE(store->resolveRef(ns, "x").has_value());
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_TRUE(store->mayMutate());
    EXPECT_TRUE(faults->snapshot().empty());
}

/// The same inside the wedge resolution: the retry lands and is adopted.
TEST(CASRefLaneFault, WedgeResolvesOverTheGlobalMemoryLimit)
{
    std::optional<DB::ThreadStatus> thread_status{DB::current_thread ? std::nullopt : std::optional<DB::ThreadStatus>(std::in_place)};
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend, wedgeTestBudget());
    auto clock = VirtualRetryClock::installOn(store);
    const RootNamespace ns{"srv1/fault_wedge_memory_limit"};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    wedgeLaneOnUnresolvedAppend(*clock, *backend, store, ns, [&] { store->dropRef(ns, "x"); });
    bool allocated = false;
    store->setWedgeBeforeSlotOccupyHookForTest([&]
    {
        if (allocated)
            return;
        allocated = true;
        allocateOverTheGlobalLimit();
    });

    EXPECT_NO_THROW(store->dropRef(ns, "y"));
    store->setWedgeBeforeSlotOccupyHookForTest(nullptr);

    EXPECT_TRUE(allocated);
    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_FALSE(store->resolveRef(ns, "x").has_value()) << "the adopted wedge applied its drop";
    EXPECT_FALSE(store->resolveRef(ns, "y").has_value());
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
}

/// The guard is not a catch: an allocation failure in the commit path reaches the caller unchanged and
/// is not a fault.
TEST(CASRefLaneFault, BadAllocInsideTheAppendPropagates)
{
    auto backend = std::make_shared<WedgeTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/fault_bad_alloc"};
    admitProperlyBornEntry(backend, store->layout(), ns);
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");
    const auto faults = recordFaultEvents(store);
    const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
    auto planned = std::make_exception_ptr(std::bad_alloc());
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned, fired]
    {
        if (fired->exchange(true))
            return;
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned);
    });

    EXPECT_THROW(store->dropRef(ns, "x"), std::bad_alloc);
    store->setInstallRegionProbeForTest(nullptr);

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery)
        << "the install failed after the PUT, so replay owns the transaction";
    EXPECT_TRUE(store->mayMutate());
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before);
    EXPECT_TRUE(faults->snapshot().empty());
    EXPECT_NO_THROW(store->dropRef(ns, "y"));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
}
```

- [ ] Build (`build/ninja_pr10v2_t4_red.log`) and run
  `--gtest_filter='CASRefLaneFault.*OverTheGlobalMemoryLimit:CASRefLaneFault.BadAlloc*'` into
  `build/test_pr10v2_t4_red.log`. Expected: `AppendCompletesOverTheGlobalMemoryLimit` fails with
  `Memory limit (total) exceeded` thrown out of `dropRef`; `WedgeResolvesOverTheGlobalMemoryLimit`
  fails with `NETWORK_ERROR` out of `dropRef` ("the bounded retry of wedged txn … failed (Memory
  limit … exceeded …)"); `BadAllocInsideTheAppendPropagates` passes (golden).
- [ ] **`CasRefLedger.cpp`.** First statement of `resolveWedgeOnce` (before `WedgeResolutionResult result;`):
  ```cpp
      /// The tracker still counts the resolution's allocations but never throws on them: a memory-limit
      /// exception here would keep the wedge for no reason the store gave. `Global`, because a
      /// `User`-level lock leaves the server limit enforced.
      LockMemoryExceptionInThread memory_exception_lock(VariableContext::Global);
  ```
  First statement of `commitRefChunk` (before `auto complete_error = …`):
  ```cpp
      /// The tracker still counts the append's allocations but never throws on them: a memory-limit
      /// exception after the PUT would wedge the lane or strand a durable object over memory pressure
      /// that says nothing about the store. `Global`, because a `User`-level lock leaves the server
      /// limit enforced. `std::bad_alloc` is not affected.
      LockMemoryExceptionInThread memory_exception_lock(VariableContext::Global);
  ```
- [ ] Build; run `--gtest_filter='CASRefLaneFault.*:CASRefInstallSafety.*:CASRefWedgeEveryAttempt.*'`
  into `build/test_pr10v2_t4.log`. Expected: all pass (`CASRefInstallSafety` uses explicit
  `MEMORY_LIMIT_EXCEEDED` throws, which the lock does not block).
- [ ] **Mutations (not committed).** `VariableContext::User` in `commitRefChunk` → `AppendCompletes…`
  red; delete the `resolveWedgeOnce` guard → `WedgeResolves…` red. Restore both.
- [ ] **Commit.** `git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp`.
  Message: `Keep the CAS ref-lane commit path alive over the server memory limit`.

## Task 5: counters and user docs

**Files.** `src/Common/ProfileEvents.cpp`, `docs/en/antalya/metrics_and_events.md`,
`docs/en/antalya/cas/operations/monitoring.md`, `docs/en/antalya/cas/architecture/manifests-and-refs.md`.

- [ ] `ProfileEvents.cpp` (description strings only):
  - `CASRefAppendSealRejected`: "Number of CAS ref-log appends refused before sending because the
    lane holds the epoch seal of its own live epoch: a successor closed that epoch, so nothing legal
    can be written in it. A lane that keeps counting here is a writer that has lost its mount and does
    not yet know it."
  - `CASRefAppendOccupantUnreadable`: "Number of CAS ref-log appends whose conditional create was
    refused while the settling read named no occupant: the read failed, or found the key absent. The
    lane stays wedged and the next flush creates the same key again. Sustained growth means a fault
    may be going unreported, because only an occupant that was read can be reported as one."
- [ ] `metrics_and_events.md`: replace the description cells of the two rows with the same texts.
- [ ] `monitoring.md`: the `CASRefAppendSealRejected` row becomes
  `| \`CASRefAppendSealRejected\` | Zero | This mount's lane holds the seal of its own epoch: the mount was deposed and does not know it yet |`.
- [ ] `manifests-and-refs.md`, section "Publishing a ref mutation": in the flowchart add after the
  `WR -->|still unresolved| …` line
  `    WR -->|"other bytes at the slot"| FAULT["fault: lane Faulted, fence closed, remount requested"]`
  and after the `PUT -->|"Unresolved, otherwise"| …` line
  `    PUT -->|"Conflict, other bytes at the id"| FAULT`
  (no `;` in labels). After the wedge paragraph add:

  > **An occupied id.** Ref-log keys are write-once. When the lane's conditional `PUT`, a wedge's
  > retry, or recovery of a retained attempt finds an object at the lane's own id, only the attempt's
  > exact bytes let the lane continue: an earlier attempt of the same transaction landed. Anything
  > else is a fault: a foreign transaction, a successor's `EpochSeal` (a successor exists, so this
  > mount was already deposed), or an object that does not decode. The lane becomes `Faulted`, the
  > mount's write fence closes, a remount is requested, and the server logs an error and a
  > `foreign_interference` event naming the key and what was read.
- [ ] **Commit.** `git add -- src/Common/ProfileEvents.cpp docs/en/antalya/metrics_and_events.md docs/en/antalya/cas/operations/monitoring.md docs/en/antalya/cas/architecture/manifests-and-refs.md`.
  Message: `Describe the CAS ref lane fault in counters and docs`.

## Task 6: gates

- [ ] `ninja -C build unit_tests_dbms > build/ninja_pr10v2_gate.log 2>&1` and
  `ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr10v2_gate.log 2>&1`; both `NINJA_EXIT=0`.
- [ ] `build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr10v2_gate.log 2>&1` and
  `build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr10v2_gate.log 2>&1`;
  a subagent reports the pass/fail counts and every failure. Any red gets a root cause before the
  branch moves on.
- [ ] No-S3 compile: `ninja -C build_fast clickhouse > build_fast/ninja_pr10v2_gate.log 2>&1`;
  `NINJA_EXIT=0`.
- [ ] `git diff 5b7c9f503df --stat` lists exactly the files of Tasks 2–5.

## PR text

Title: `Fault the CAS mount on any occupant of a ref-log id but our own bytes`

Body (per `.github/PULL_REQUEST_TEMPLATE.md`):

> A ref-log id this mount derived is write-once and ours under the mount lease. When the lane finds
> an object there, it now has two outcomes: our exact bytes continue, and anything else — a foreign
> transaction, a successor's epoch seal, an undecodable object, or an impossible lane state — closes
> the mount fence first and then reports through one path that requests the remount before anything
> that can throw. A successor's seal at our own id used to close only that lane, quietly. The commit
> path runs under a `Global` memory-exception lock, so the server memory limit cannot wedge it.
>
> Changelog category: Bug Fix (user-visible misbehavior in an official stable release)
>
> Changelog entry: A `CAS` ref-log append or recovery that finds anything but its own bytes at its own
> id now fences the mount and remounts it, and the ref-lane commit path no longer fails on the server
> memory limit.
