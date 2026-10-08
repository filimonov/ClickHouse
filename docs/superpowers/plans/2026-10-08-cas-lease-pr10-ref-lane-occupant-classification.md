# CAS lease PR 10: ref-lane occupant classification with a typed fault latched first

**Goal.** The ref lane names every occupant it meets at its own slot: `Ours`, `SuccessorSeal`,
`Foreign` (decodes, not ours, not a seal) or `Unreadable` (does not decode). Every branch that
decides the lane cannot continue latches the mount first, with a typed cause (`interference`,
`corrupt`, `defect`), and only then formats, copies keys, logs and audits. Two branches that today
end in a dead lane with no remount now reach the remount: the append site's classifier exception
(CAS-184) and recovery's retained attempt with a foreign occupant. A memory-limit exception from the
classifier is retried, not faulted. The remount executor itself is unchanged. Closes CAS-184,
CAS-56.3.

**Architecture.** `CasRefLedger` gets two callbacks instead of one: `on_impossible_interference
(RefLaneFault)` latches (Pool binds it to `CasMountRuntime::tripMountLost`, atomics only), and
`report_impossible_interference(RefLaneFault, key, reason, ns)` does today's log, event,
`scheduleRemount` and diagnostic GET. `classifyRefLogOccupant` returns `Unreadable` where it returns
`Foreign` today for a decode failure, and takes a test probe that runs before the decode.

**Tech stack.** C++23, gtest (`unit_tests_dbms`, `WedgeTestBackend`, `VirtualRetryClock`), TLC.

**Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`, revision 7:
section 2 "Потеря" rows R14, R15, R17 and the paragraph "Латч с нити писателя"; section 6 (the
`NeedsRecovery` bullet); section 7 PR row 10 and tests row 10; section 9 row "модель ref-полосы".
Backlog: `docs/superpowers/cas/backlog/tasks/cas-184 - Fire-the-anomaly-policy-when-a-ref-log-append-finds-an-unreadable-occupant.md`,
`docs/superpowers/cas/backlog/tasks/cas-56.3 - Find-out-whether-a-ref-lane-can-leave-`NeedsRecovery`-after-a-damaged-`_ckpt`-is-restored.md`.

**Base and branch.** `altinity/antalya-26.6` at `5b7c9f503df`, worktree
`/home/mfilimonov/workspace/ClickHouse/cas-95-1`, branch
`fix/antalya-26.6/cas-ref-lane-occupant-classification`. `CA/` =
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. No file outside `CA/` and
`src/Disks/tests/` is touched. No on-storage format change. The model lives in the master worktree
(`/home/mfilimonov/workspace/ClickHouse/master`, branch `cas-gc-rebuild`) and is committed there.

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

- **Latch first, in four branches.** `commitRefChunk` (`Foreign`/`Unreadable`; classifier defect;
  lane not `Ready` at new-id allocation), `resolveWedgeOnce` (`Corrupted` result), and
  `runRecoveryWalkOnce` (retained attempt with a foreign or unreadable occupant). In each, the first
  statement after the decision is `on_impossible_interference(cause)`: before `fmt::format`, before
  `active_attempt.key` is copied, before `ProfileEvents::increment`. In `resolveWedgeOnce` the
  decision is taken under `state_mutex`; the latch runs as the first statement after that lock is
  released, so the callback never runs under the lane lock.
- **Memory limit in `commitRefChunk` returns `Writing -> Ready`.** Safety argument, re-derived for
  this exact branch: the classifier runs only after `occupant_bytes != expected_bytes` (the byte
  comparison precedes the decode and cannot throw), so the object at the key is not this attempt's
  bytes; the ref-log key is write-once, so this attempt did not land there and cannot land later
  while that object stays. Dropping the attempt is what the `Foreign` and `SuccessorSeal` arms already
  do on the same observation. The next flush re-derives the same id (nothing was applied), conflicts
  with the same object and classifies it again. Nothing durable is touched.
- **Memory limit in `resolveWedgeOnce` is unchanged:** the exception propagates as on the base, the
  wedge stays installed. `WedgeClassifierMemoryLimitKeepsTheWedge` is golden (green before and after).
- **What the latch-order tests prove.** The event sink records `!mayMutate()` at delivery. That
  proves the latch ran before the `ForeignInterference` event and, because both come from the one
  report call that starts with `LOG_ERROR`, before the log line. It does not observe the order
  against `fmt::format` inside the ledger; that order is read from the diff.
- **Classification boundary.** `Unreadable` = `decodeRefLogTxn` (or `openObject`) threw
  `CORRUPTED_DATA` or `UNKNOWN_FORMAT_VERSION`. That includes a well-formed transaction whose body
  names another key (body/key mismatch is `CORRUPTED_DATA`). See "Spec and code" below.
- `CASRefAppendOccupantUnreadable` keeps its meaning (the quiet arms: no occupant named, or the
  classifier threw). The new `Unreadable` occupant is loud and does not increment it.
- Event type stays `ForeignInterference` for all three causes; the cause is a new `detail` key
  `cause` and is in the `LOG_ERROR` line. Behaviour after the report is unchanged.
- No test expects `LOGICAL_ERROR`. The defect tests inject `NOT_IMPLEMENTED`.

## Spec and code: disagreements carried by this plan

1. R15 says `Unreadable` is "прочитан, но не декодируется". The code's decode also rejects a
   decodable transaction copied from another key (`decodeRefLogTxn`: "does not match the key it was
   read from", `CORRUPTED_DATA`). This plan classifies that as `Unreadable` / `corrupt`, not
   `Foreign` / `interference`. Both are terminal; only the label differs.
2. "`MEMORY_LIMIT_EXCEEDED` в `commitRefChunk` возвращает попытку в `Ready` и повторяет": there is no
   retry loop inside `commitRefChunk`. This plan returns the lane to `Ready`, fails the chunk with the
   retry-later class (`NETWORK_ERROR`), and the next flush repeats the classification.
3. The spec names one callback with a typed cause. A latch that must precede `fmt::format` cannot
   carry the formatted detail, so the plan splits it into the latch (`on_impossible_interference`,
   cause only) and the report (`report_impossible_interference`, cause plus strings).
4. `runRecoveryWalkOnce`: the spec names only `Foreign`. The plan latches `corrupt` for `Unreadable`
   there too (today both are the same `Foreign` value). Classifier exceptions in recovery propagate
   unchanged (the lane stays `NeedsRecovery`, the next touch recovers again); the spec's R17 wording
   for classifier exceptions names only `commitRefChunk` and `resolveWedgeOnce`.
5. CAS-184 calls the classifier-exception arm "unreadable occupant". Under the spec's taxonomy it is a
   classifier exception, so it latches `defect`; the decode-failure case is the new `Unreadable`.
6. In `CasRefLedger.cpp` the doc comment of `classifyRefLogOccupant` sits above `chainLinkFor` and
   vice versa (base). Task 4 edits the classifier's text where it is; it does not move either block.

## Files

| File | Change |
|---|---|
| `CA/Pool/CasRefLedger.h` | `RefLaneFault`, `refLaneFaultName`; latch + report callbacks; `setOccupantDecodeProbeForTest` |
| `CA/Pool/CasRefLedger.cpp` | `Occupant::Unreadable`; classifier probe; four branches latch first; memory → `Ready` |
| `CA/Pool/CasPool.h` | `reportImpossibleInterference` takes the cause; doc; probe forwarder |
| `CA/Pool/CasPool.cpp` | latch lambda (`tripMountLost`); report without `tripMountLost`; cause in log and event |
| `src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp` | fault-event recorder, helpers, 11 tests (`CASRefLaneOccupant`) |
| master: `docs/superpowers/models/CaRefLaneCore.tla`, its cfgs, `run_reflane.sh`, `CaRefLaneCore_RESULTS.md` | occupant outcomes, fault latch |

---

## Task 0: Branch

- [ ] **Step 1.** In `/home/mfilimonov/workspace/ClickHouse/cas-95-1`:
  ```bash
  git fetch altinity antalya-26.6
  git switch -c fix/antalya-26.6/cas-ref-lane-occupant-classification 5b7c9f503df
  ```

## Task 1: Model first — every occupant outcome has an exit

Runs in `/home/mfilimonov/workspace/ClickHouse/master`. **Consumes:** `CaRefLaneCore.tla` as
committed (`89a60b1b7f8`). **Produces:** occupant outcomes `Unreadable`, `ClassifyMemory`,
`ClassifyDefect`; the append-site conflict; recovery's foreign retained attempt; the latch
`fault_latched`; invariants `FaultHasExit`, `TerminalObservationIsTerminal`; property
`FaultedAbsorbing`. "Exit" in the model is safety-shaped: every terminal observation sets
`fault_latched` (the remount executor ran) in the same step, and `W_FaultThenRemount` shows the
remount is reachable after a latch. The model does not represent "lane not `Ready` at new-id
allocation" (`StartWrite` requires `Ready`) and does not prove remount liveness.

- [ ] **Step 1: constants, sets, variables.** In `docs/superpowers/models/CaRefLaneCore.tla`:
  - append `,\n    SabotageUnlatchedFault,\n    SabotageRecoverAfterFault` after
    `SabotageObservationRetarget` in `CONSTANTS`;
  - replace the `Observations ==` line with
    ```tla
    Observations == {"none", "pending", "Durable", "Unknown", "SuccessorSeal",
                     "Foreign", "Unreadable", "ClassifyMemory", "ClassifyDefect"}
    (* What the classifier can answer for an object that is not this attempt's bytes. *)
    OccupantOutcomes == {"Foreign", "Unreadable", "ClassifyMemory", "ClassifyDefect"}
    FaultOutcomes == {"Foreign", "Unreadable", "ClassifyDefect"}
    ResolvedObservations == {"Durable", "Unknown", "SuccessorSeal"} \cup OccupantOutcomes
    FaultRecords == {"none", "RecoveryForeign"} \cup FaultOutcomes
    ```
  - append `,\n    fault_latched,\n    terminal_observed,\n    fault_record,\n    saw_classify_retry`
    after `saw_missing_confirmation` in `VARIABLES`;
  - after `resolverProvenanceVars == …` add
    `faultVars == << fault_latched, terminal_observed, fault_record, saw_classify_retry >>` and change
    `vars == << laneVars, runtimeVars >>` to `vars == << laneVars, runtimeVars, faultVars >>`;
  - replace `CurrentRuntime == runtime_generation = authority_generation` with
    ```tla
    (* The latch trips the fence, so a latched runtime is never current again. *)
    CurrentRuntime == runtime_generation = authority_generation /\ ~fault_latched
    ```
  - in `Init` append `/\ fault_latched = FALSE`, `/\ terminal_observed = FALSE`,
    `/\ fault_record = "none"`, `/\ saw_classify_retry = FALSE`;
  - in `TypeOK` append `/\ fault_latched \in BOOLEAN`, `/\ terminal_observed \in BOOLEAN`,
    `/\ fault_record \in FaultRecords`, `/\ saw_classify_retry \in BOOLEAN`.

- [ ] **Step 2: resolver outcomes.**
  - Rename `ObserveForeign ==` to `ObserveOccupant(o) ==`, insert `/\ o \in OccupantOutcomes` as its
    first conjunct and replace `/\ observation' = "Foreign"` with `/\ observation' = o`.
  - Rename `ObserveForeignScoped ==` to `ObserveOccupantScoped(o) ==` and its first conjunct
    `/\ ObserveForeign` to `/\ ObserveOccupant(o)`.
  - In `ApplyResolution`: first conjunct becomes `/\ observation \in ResolvedObservations`;
    `[] observation = "Unknown" ->` becomes `[] observation \in {"Unknown", "ClassifyMemory"} ->`;
    `[] observation = "Foreign" ->` becomes `[] observation \in FaultOutcomes ->`.
  - In `CapturedRuntimeMatchesOwnership` replace
    `(observation \in {"Durable", "Unknown", "SuccessorSeal", "Foreign"})` with
    `(observation \in ResolvedObservations)`.
  - Append to `ApplyResolutionScoped`:
    ```tla
        /\ IF lane' = "Faulted" /\ lane # "Faulted"
              THEN /\ terminal_observed' = TRUE
                   /\ fault_latched' = (fault_latched \/ ~SabotageUnlatchedFault)
                   /\ fault_record' = observation
              ELSE UNCHANGED << fault_latched, terminal_observed, fault_record >>
        /\ UNCHANGED saw_classify_retry
    ```

- [ ] **Step 3: append-site conflict and recovery.** Add after `ApplyWithoutIdentityScoped`:
  ```tla
  (* The append's conditional create lost to an object that is not this attempt's bytes, so the
     attempt did not land. A memory-limit classification returns the lane to Ready; every other
     outcome faults the lane and latches in the same step. *)
  WriteConflict(o) ==
      /\ o \in OccupantOutcomes
      /\ lane = "Writing"
      /\ durable_id = cache_id
      /\ attempt' = NoAttempt
      /\ IF o = "ClassifyMemory"
            THEN /\ lane' = "Ready"
                 /\ saw_classify_retry' = TRUE
                 /\ UNCHANGED << saw_faulted, fault_latched, terminal_observed, fault_record >>
            ELSE /\ lane' = "Faulted"
                 /\ saw_faulted' = TRUE
                 /\ terminal_observed' = TRUE
                 /\ fault_latched' = (fault_latched \/ ~SabotageUnlatchedFault)
                 /\ fault_record' = o
                 /\ UNCHANGED saw_classify_retry
      /\ UNCHANGED << cache_id, durable_id, cache_binding, durable_binding,
                      runtime_generation, authority_generation, resolver_attempt,
                      resolver_generation, observation, bad_append, bad_install,
                      bad_certification, saw_commit, saw_unresolved,
                      saw_retry_created, saw_durable_adoption, saw_recovery,
                      saw_stale_result, saw_closed, saw_certified_while_outstanding >>

  WriteConflictScoped(o) ==
      /\ WriteConflict(o)
      /\ attempt_runtime' = NoRuntime
      /\ UNCHANGED << runtimeObjectVars, runtimeAuxVars, resolver_runtime,
                      resolverProvenanceVars, lane_effect_target, runtime_cache_marker,
                      runtime_durable_marker, bad_lane_retarget, saw_rebirth_old_action >>

  (* Recovery meets a different object at the retained attempt's id above the checkpoint. *)
  RecoveryFindsForeign ==
      /\ lane = "NeedsRecovery"
      /\ CurrentRuntime
      /\ terminal_observed' = TRUE
      /\ fault_latched' = (fault_latched \/ ~SabotageUnlatchedFault)
      /\ fault_record' = "RecoveryForeign"
      /\ UNCHANGED << laneVars, runtimeVars, saw_classify_retry >>
  ```
  In `Recover` replace `/\ CurrentRuntime` with
  `/\ (CurrentRuntime \/ (SabotageRecoverAfterFault /\ runtime_generation = authority_generation))`.

- [ ] **Step 4: `Next`.** Rename the existing `Next ==` to `CoreNext ==`; in it delete the
  `\/ ObserveForeignScoped` and `\/ ApplyResolutionScoped` lines and add
  `\/ \E o \in OccupantOutcomes : ObserveOccupantScoped(o)`. Then add:
  ```tla
  Next ==
      \/ (CoreNext /\ UNCHANGED faultVars)
      \/ ApplyResolutionScoped
      \/ \E o \in OccupantOutcomes : WriteConflictScoped(o)
      \/ RecoveryFindsForeign
  ```

- [ ] **Step 5: properties.** Before `W_Commit ==` add:
  ```tla
  FaultHasExit == (terminal_observed \/ lane = "Faulted") => fault_latched
  TerminalObservationIsTerminal == terminal_observed => lane \in {"Faulted", "NeedsRecovery"}
  FaultedAbsorbing == [][lane = "Faulted" => lane' = "Faulted"]_vars
  ```
  After `W_MissingConfirmation == …` add:
  ```tla
  W_UnreadableFault == ~(fault_record = "Unreadable" /\ lane = "Faulted" /\ fault_latched)
  W_DefectFault == ~(fault_record = "ClassifyDefect" /\ lane = "Faulted" /\ fault_latched)
  W_RecoveryForeign == ~(fault_record = "RecoveryForeign" /\ lane = "NeedsRecovery" /\ fault_latched)
  W_ClassifyRetry == ~(saw_classify_retry /\ lane = "Ready")
  W_FaultThenRemount == ~(fault_latched /\ saw_self_remount)
  ```

- [ ] **Step 6: configs.** Every existing config gets the two constants:
  ```bash
  cd docs/superpowers/models
  sed -i '/^    SabotageObservationRetarget = /a\    SabotageUnlatchedFault = FALSE\n    SabotageRecoverAfterFault = FALSE' CaRefLaneCore_*.cfg
  grep -L 'SabotageRecoverAfterFault' CaRefLaneCore_*.cfg   # prints nothing
  grep -c 'SabotageUnlatchedFault' CaRefLaneCore_*.cfg | grep -v ':1$'   # prints nothing
  ```
  In `CaRefLaneCore_safe.cfg` append `FaultHasExit` and `TerminalObservationIsTerminal` to
  `INVARIANTS` and add a line `PROPERTY FaultedAbsorbing`. Create, each as a copy of
  `CaRefLaneCore_safe.cfg` with the change named:
  - `CaRefLaneCore_sab_unlatchedfault.cfg`: `SabotageUnlatchedFault = TRUE`;
  - `CaRefLaneCore_sab_recoverafterfault.cfg`: `SabotageRecoverAfterFault = TRUE`;
  - `CaRefLaneCore_witness_<x>.cfg` for `unreadablefault`, `defectfault`, `recoveryforeign`,
    `classifyretry`, `faultremount`: all sabotages `FALSE`, no `PROPERTY`, and
    `INVARIANTS TypeOK ReadyCaughtUp OutstandingRetainsExactAttempt NoAttemptOutsideOutstanding W_<X>`
    with `W_UnreadableFault`, `W_DefectFault`, `W_RecoveryForeign`, `W_ClassifyRetry`,
    `W_FaultThenRemount` respectively.

  In `run_reflane.sh` add after the `sab_missingconfirmation` row
  `"sab_unlatchedfault     violation FaultHasExit"` and
  `"sab_recoverafterfault  violation TerminalObservationIsTerminal"`, and after the
  `witness_certifyoutstanding` row `"witness_unreadablefault violation W_UnreadableFault"`,
  `"witness_defectfault    violation W_DefectFault"`,
  `"witness_recoveryforeign violation W_RecoveryForeign"`,
  `"witness_classifyretry  violation W_ClassifyRetry"`,
  `"witness_faultremount   violation W_FaultThenRemount"`.

- [ ] **Step 7: run.**
  ```bash
  docs/superpowers/models/run_reflane.sh > tmp/lease-series/pr10-reflane.log 2>&1; echo EXIT=$?
  ```
  Expected `EXIT=0`, `ALL EXPECTATIONS MET`, every row `PASS`. `sab_unlatchedfault` is the base
  code's shape (a terminal observation without the latch) and must report
  `violation:FaultHasExit`. If any pre-existing row changes its result, stop and report the row and
  its log; do not edit an existing config's expectation.

- [ ] **Step 8: results.** In `docs/superpowers/models/CaRefLaneCore_RESULTS.md`, update the date line
  to `2026-10-08`, the expectation count and the honest-run figures to what the log prints, and add
  after the lane-model section:
  ```markdown
  ## Occupant outcomes and the fault latch {#occupant-outcomes-fault-latch}

  The resolver and the append site's conflict both classify an occupant that is not the attempt's
  bytes as `Foreign`, `Unreadable`, `ClassifyMemory` or `ClassifyDefect`; recovery can meet a foreign
  object at its retained attempt. Every terminal observation latches the fence in the same step
  (`FaultHasExit`), a latched runtime is never current again, and a terminal observation never
  returns the lane to `Ready` (`TerminalObservationIsTerminal`, `FaultedAbsorbing`). A memory-limit
  classification returns the append to `Ready` and leaves a wedge wedged. `sab_unlatchedfault`
  (fault without latch) and `sab_recoverafterfault` (recovery ignoring the latch) are red on their
  target. Not modelled: a non-`Ready` lane at new-id allocation (`StartWrite` requires `Ready`) and
  remount liveness; `W_FaultThenRemount` only shows the remount is reachable after a latch.
  ```

- [ ] **Step 9: commit (master worktree).**
  ```bash
  git add -- docs/superpowers/models/CaRefLaneCore.tla docs/superpowers/models/run_reflane.sh \
      docs/superpowers/models/CaRefLaneCore_RESULTS.md \
      $(git ls-files 'docs/superpowers/models/CaRefLaneCore_*.cfg') \
      docs/superpowers/models/CaRefLaneCore_sab_unlatchedfault.cfg \
      docs/superpowers/models/CaRefLaneCore_sab_recoverafterfault.cfg \
      docs/superpowers/models/CaRefLaneCore_witness_unreadablefault.cfg \
      docs/superpowers/models/CaRefLaneCore_witness_defectfault.cfg \
      docs/superpowers/models/CaRefLaneCore_witness_recoveryforeign.cfg \
      docs/superpowers/models/CaRefLaneCore_witness_classifyretry.cfg \
      docs/superpowers/models/CaRefLaneCore_witness_faultremount.cfg
  git diff --cached --stat   # only the files above
  git commit -m "model(cas): ref lane occupant outcomes latch before any exit" \
             -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

## Task 2: Classifier probe (behaviour-preserving) and two golden tests

**Consumes:** base. **Produces:** `CasRefLedger::setOccupantDecodeProbeForTest`,
`Pool::setOccupantDecodeProbeForTest`; the classifier's fifth parameter; tests green now and after
Task 3.

- [ ] **Step 1: seam.** In `CA/Pool/CasRefLedger.cpp` change `classifyRefLogOccupant` to:
  ```cpp
  Occupant classifyRefLogOccupant(const RootNamespace & ns, const RefTxnId & id, const String & occupant_bytes,
                                  const String & expected_bytes, const std::function<void()> & decode_probe_for_test)
  {
      if (occupant_bytes == expected_bytes)
          return Occupant::Ours;
      try
      {
          if (decode_probe_for_test)
              decode_probe_for_test();
          return refLogTxnIsEpochSeal(decodeRefLogTxn(openObject(FormatId::RefLog, occupant_bytes), ns.string(), id))
              ? Occupant::SuccessorSeal : Occupant::Foreign;
      }
      catch (const Exception & e)
      {
          if (e.code() != ErrorCodes::CORRUPTED_DATA && e.code() != ErrorCodes::UNKNOWN_FORMAT_VERSION)
              throw;
          return Occupant::Foreign;
      }
  }
  ```
  and append `, occupant_decode_probe_for_test` as the last argument at its three call sites
  (`runRecoveryWalkOnce`, `resolveWedgeOnce`, `commitRefChunk`; `grep -n 'classifyRefLogOccupant('`
  prints exactly the definition and these three). In `CA/Pool/CasRefLedger.h`, after
  `setWedgeBeforeSlotOccupyHookForTest`:
  ```cpp
      /// Runs inside the occupant classifier after the byte comparison and before the decode, so a
      /// test can make the classification throw.
      void setOccupantDecodeProbeForTest(std::function<void()> probe)
      {
          occupant_decode_probe_for_test = std::move(probe);
      }
  ```
  and after the member `wedge_before_slot_occupy_hook_for_test;` add
  `std::function<void()> occupant_decode_probe_for_test;`. In `CA/Pool/CasPool.h`, after
  `setWedgeBeforeSlotOccupyHookForTest`:
  ```cpp
      void setOccupantDecodeProbeForTest(std::function<void()> probe)
      {
          ref_ledger.setOccupantDecodeProbeForTest(std::move(probe));
      }
  ```

- [ ] **Step 2: helpers.** Append to `src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp`; add
  `extern const int NOT_IMPLEMENTED;` to its `DB::ErrorCodes` block:
  ```cpp
  /// ================================================================================================
  /// Occupant classification and the fault latch
  /// ================================================================================================

  namespace
  {

  constexpr std::string_view undecodable_occupant = "not a ref-log object at all";

  /// A well-formed transaction of `ns` at `id` that is neither an epoch seal nor this writer's.
  String foreignTxnBytes(const RootNamespace & ns, const RefTxnId & id)
  {
      RefOp birth;
      birth.kind = RefOpKind::NamespaceBirth;
      return sealObject(FormatId::RefLog, encodeRefLogTxn(RefLogTxn{ns.string(), id, {birth}, std::nullopt}));
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

  [[noreturn]] void throwInClassifier(int code)
  {
      throw DB::Exception(code, "injected failure inside the occupant classifier");
  }

  /// Wedges `ns` on the drop of "x" and lands an occupant at the wedged key; returns that key.
  String wedgeUnder(VirtualRetryClock & clock, const std::shared_ptr<WedgeTestBackend> & backend,
                    const PoolPtr & store, const RootNamespace & ns, bool decodable_occupant)
  {
      admitProperlyBornEntry(backend, store->layout(), ns);
      publishEmptyPart(store, ns, "x");
      publishEmptyPart(store, ns, "y");
      wedgeLaneOnUnresolvedAppend(clock, *backend, store, ns, [&] { store->dropRef(ns, "x"); });
      const String wedged_key = store->wedgedKeyForTest(ns);
      const auto parsed = store->layout().parseRefObjectKey(wedged_key);
      if (!parsed)
          throw std::runtime_error("wedged key does not parse");
      const String occupant = decodable_occupant ? foreignTxnBytes(ns, parsed->txn_id) : String{undecodable_occupant};
      if (!std::holds_alternative<Committed>(backend->putAsSuccessor(wedged_key, occupant)))
          throw std::runtime_error("the successor's occupant did not land");
      return wedged_key;
  }

  }
  ```
  `wedgeUnder` throws instead of using `ASSERT_*` because it returns a value.

- [ ] **Step 3: golden tests.** Append:
  ```cpp
  /// A memory limit inside the wedge's classification changes nothing: the exception reaches the
  /// caller, the wedge stays, and the next flush classifies the same occupant.
  TEST(CASRefLaneOccupant, WedgeClassifierMemoryLimitKeepsTheWedge)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend, wedgeTestBudget());
      auto clock = VirtualRetryClock::installOn(store);
      const RootNamespace ns{"srv1/occupant_wedge_memory"};
      const String wedged_key = wedgeUnder(*clock, backend, store, ns, /*decodable_occupant*/ true);
      const uint64_t remounts_before = store->scheduleRemountCallCountForTest();

      store->setOccupantDecodeProbeForTest([] { throwInClassifier(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED); });
      expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, [&] { store->dropRef(ns, "y"); });
      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Wedged);
      EXPECT_EQ(store->wedgedKeyForTest(ns), wedged_key);
      EXPECT_TRUE(store->mayMutate());
      EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before);

      store->setOccupantDecodeProbeForTest(nullptr);
      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });
      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
  }

  /// A lane in `NeedsRecovery` leaves it on the next touch once its checkpoint reads again, with no
  /// remount: a damaged `_ckpt` fails that touch and leaves the lane where it was; the byte-identical
  /// original lets the following touch replay the durable drop and commit its own.
  TEST(CASRefLaneOccupant, NeedsRecoveryLaneLeavesAfterADamagedCkptIsRestoredWithoutRemount)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend, wedgeTestBudget());
      auto clock = VirtualRetryClock::installOn(store);
      const RootNamespace ns{"srv1/occupant_needs_recovery_ckpt"};
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

- [ ] **Step 4: run.**
  ```bash
  ninja -C build unit_tests_dbms > build/ninja_pr10_t2.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CASRefLaneOccupant.*:CASRefWedgeEveryAttempt.*:CASAnomalyPolicy.*' > build/test_pr10_t2.log 2>&1; echo TEST_EXIT=$?
  ```
  Expected `NINJA_EXIT=0`, `TEST_EXIT=0`. Both new tests are golden: they describe the base and stay
  green after Task 3. If `NeedsRecoveryLane…` fails on the base, that is the CAS-56.3 answer
  "does not self-heal": stop, keep the log, report the failing assertion.

- [ ] **Step 5: commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h \
             src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp
  git commit -m "Add a ref-lane classifier probe and pin wedge and recovery exits" \
             -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

## Task 3: Typed causes, latch first, occupant outcomes (the fix)

**Consumes:** Task 2. **Produces:** `RefLaneFault`, `refLaneFaultName`, the latch/report callback
pair, `Occupant::Unreadable`, the four branches, `detail["cause"]`.

- [ ] **Step 1: failing tests.** Append to the test file (inside the anonymous namespace of Task 2,
  before its closing `}`):
  ```cpp
  /// One delivered fault event: its cause and whether the write fence was already closed. The fence
  /// is the latch, so `false` means the report ran before the latch.
  struct FaultEvent
  {
      String cause;
      bool fence_closed = false;
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
          const auto cause = e.detail.find("cause");
          log->push({cause == e.detail.end() ? String{"<none>"} : cause->second, !pool->mayMutate()});
      });
      return log;
  }

  void expectOneLatchedFault(const FaultEventLog & log, const String & cause)
  {
      const auto events = log.snapshot();
      ASSERT_EQ(events.size(), 1u);
      EXPECT_EQ(events[0].cause, cause);
      EXPECT_TRUE(events[0].fence_closed) << "the latch must precede the report";
  }

  void expectAppendOccupantLatches(const String & ns_name, bool decodable_occupant, const String & cause)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend);
      const RootNamespace ns{ns_name};
      admitProperlyBornEntry(backend, store->layout(), ns);
      publishEmptyPart(store, ns, "x");
      const RefTxnId next{store->liveWriterEpoch(), 3};
      backend->conflict_substr = store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), next);
      backend->conflict_bytes = decodable_occupant ? foreignTxnBytes(ns, next) : String{undecodable_occupant};
      backend->conflict_count = 1;
      const auto faults = recordFaultEvents(store);
      const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
      ASSERT_TRUE(store->mayMutate());

      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });

      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
      expectOneLatchedFault(*faults, cause);
  }

  void expectWedgeOccupantLatches(const String & ns_name, bool decodable_occupant, const String & cause)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend, wedgeTestBudget());
      auto clock = VirtualRetryClock::installOn(store);
      const RootNamespace ns{ns_name};
      wedgeUnder(*clock, backend, store, ns, decodable_occupant);
      const auto faults = recordFaultEvents(store);
      ASSERT_TRUE(store->mayMutate());

      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });

      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      expectOneLatchedFault(*faults, cause);
  }
  ```
  and after the namespace's closing `}`:
  ```cpp
  TEST(CASRefLaneOccupant, AppendSiteForeignOccupantLatchesInterference)
  {
      expectAppendOccupantLatches("srv1/occupant_append_foreign", /*decodable_occupant*/ true, "interference");
  }

  TEST(CASRefLaneOccupant, AppendSiteUnreadableOccupantLatchesCorrupt)
  {
      expectAppendOccupantLatches("srv1/occupant_append_unreadable", /*decodable_occupant*/ false, "corrupt");
  }

  TEST(CASRefLaneOccupant, WedgeForeignOccupantLatchesInterference)
  {
      expectWedgeOccupantLatches("srv1/occupant_wedge_foreign", /*decodable_occupant*/ true, "interference");
  }

  TEST(CASRefLaneOccupant, WedgeUnreadableOccupantLatchesCorrupt)
  {
      expectWedgeOccupantLatches("srv1/occupant_wedge_unreadable", /*decodable_occupant*/ false, "corrupt");
  }

  TEST(CASRefLaneOccupant, NonReadyLaneAtNewIdLatchesDefect)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend);
      const RootNamespace ns{"srv1/occupant_non_ready"};
      admitProperlyBornEntry(backend, store->layout(), ns);
      publishEmptyPart(store, ns, "x");
      const auto faults = recordFaultEvents(store);
      ASSERT_TRUE(store->mayMutate());
      store->setRefPreCarveHookForTest([&]
      {
          store->forceWedgeForTest(ns, /*writer_epoch*/ 1, /*ref_sequence*/ 1, "bogus/_log/key", "bogus-bytes");
      });

      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });
      store->setRefPreCarveHookForTest(nullptr);

      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      expectOneLatchedFault(*faults, "defect");
  }

  /// Recovery finds a different object at the retained attempt's id above the checkpoint: the mount
  /// latches and remounts instead of failing every later touch with nothing scheduled.
  TEST(CASRefLaneOccupant, RecoveryForeignRetainedAttemptLatchesInterference)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend, wedgeTestBudget());
      auto clock = VirtualRetryClock::installOn(store);
      const RootNamespace ns{"srv1/occupant_recovery_foreign"};
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
      replaceObj(*backend, store->layout().refLogKey(life, retained), foreignTxnBytes(ns, retained));
      const auto faults = recordFaultEvents(store);
      const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
      ASSERT_TRUE(store->mayMutate());

      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });

      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
      EXPECT_FALSE(store->mayMutate());
      EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
      expectOneLatchedFault(*faults, "interference");
  }

  /// The occupant is not this attempt's bytes, so the attempt did not land: the lane goes back to
  /// `Ready` and the next flush meets and classifies the same occupant.
  TEST(CASRefLaneOccupant, AppendSiteClassifierMemoryLimitReturnsTheLaneToReady)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend);
      const RootNamespace ns{"srv1/occupant_append_memory"};
      admitProperlyBornEntry(backend, store->layout(), ns);
      publishEmptyPart(store, ns, "x");
      const RefTxnId next{store->liveWriterEpoch(), 3};
      backend->conflict_substr = store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), next);
      backend->conflict_bytes = foreignTxnBytes(ns, next);
      backend->conflict_count = 1;
      const auto faults = recordFaultEvents(store);
      const uint64_t remounts_before = store->scheduleRemountCallCountForTest();

      store->setOccupantDecodeProbeForTest([] { throwInClassifier(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED); });
      expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
      EXPECT_TRUE(store->mayMutate());
      EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before);
      EXPECT_TRUE(faults->snapshot().empty());

      store->setOccupantDecodeProbeForTest(nullptr);
      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });
      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      expectOneLatchedFault(*faults, "interference");
  }

  TEST(CASRefLaneOccupant, AppendSiteClassifierDefectLatchesDefect)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend);
      const RootNamespace ns{"srv1/occupant_append_defect"};
      admitProperlyBornEntry(backend, store->layout(), ns);
      publishEmptyPart(store, ns, "x");
      const RefTxnId next{store->liveWriterEpoch(), 3};
      backend->conflict_substr = store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), next);
      backend->conflict_bytes = foreignTxnBytes(ns, next);
      backend->conflict_count = 1;
      const auto faults = recordFaultEvents(store);
      const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
      ASSERT_TRUE(store->mayMutate());

      store->setOccupantDecodeProbeForTest([] { throwInClassifier(DB::ErrorCodes::NOT_IMPLEMENTED); });
      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });
      store->setOccupantDecodeProbeForTest(nullptr);

      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      EXPECT_FALSE(store->mayMutate());
      EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before + 1);
      expectOneLatchedFault(*faults, "defect");
  }

  TEST(CASRefLaneOccupant, WedgeClassifierDefectLatchesDefect)
  {
      auto backend = std::make_shared<WedgeTestBackend>();
      auto store = openPool(backend, wedgeTestBudget());
      auto clock = VirtualRetryClock::installOn(store);
      const RootNamespace ns{"srv1/occupant_wedge_defect"};
      wedgeUnder(*clock, backend, store, ns, /*decodable_occupant*/ true);
      const auto faults = recordFaultEvents(store);
      ASSERT_TRUE(store->mayMutate());

      store->setOccupantDecodeProbeForTest([] { throwInClassifier(DB::ErrorCodes::NOT_IMPLEMENTED); });
      expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });
      store->setOccupantDecodeProbeForTest(nullptr);

      EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
      expectOneLatchedFault(*faults, "defect");
  }
  ```

- [ ] **Step 2: run red.**
  ```bash
  ninja -C build unit_tests_dbms > build/ninja_pr10_t3_red.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CASRefLaneOccupant.*' > build/test_pr10_t3_red.log 2>&1; echo TEST_EXIT=$?
  ```
  Expected `NINJA_EXIT=0`, `TEST_EXIT=1`, the two Task 2 tests pass, the nine new ones fail:
  - `AppendSite{Foreign,Unreadable}…`, `Wedge{Foreign,Unreadable}…`, `NonReadyLaneAtNewId…`:
    `events[0].cause` is `"<none>"` and "the latch must precede the report";
  - `RecoveryForeignRetainedAttempt…`: `mayMutate()` is true, the remount count did not move,
    `events.size()` is 0 — the base throws `CORRUPTED_DATA` with no callback;
  - `AppendSiteClassifierMemoryLimit…`: `e.code()` is 246 (`CORRUPTED_DATA`), expected 210, and the
    lane is `Faulted`, not `Ready`;
  - `AppendSiteClassifierDefect…`: `mayMutate()` true, remount count unchanged, `events.size()` 0
    (CAS-184: `Faulted` with no remount);
  - `WedgeClassifierDefect…`: `e.code()` is 48 (`NOT_IMPLEMENTED`), expected 246; the lane is
    `Wedged`.
  Keep the log. Do not commit the red tree.

- [ ] **Step 3: types and callbacks (`CA/Pool/CasRefLedger.h`).** After the `RefLaneState` enum:
  ```cpp
  /// Why a ref lane stops: `Interference` is a decodable object at our slot that is neither ours nor a
  /// successor's epoch seal; `Corrupt` is an object there that does not decode; `Defect` is our own
  /// code failing (a classifier exception, or a lane state the lane machine cannot produce).
  enum class RefLaneFault : uint8_t
  {
      Interference,
      Corrupt,
      Defect,
  };

  /// Lowercase name of `fault` for logs and events.
  std::string_view refLaneFaultName(RefLaneFault fault);
  ```
  Replace the constructor parameter
  `std::function<void(const String &, const String &, const std::optional<String> &)> on_impossible_interference_,`
  with:
  ```cpp
          /// Latches the mount's fail-closed reaction. The deciding thread calls it before it formats or
          /// copies anything, so it must not allocate, block or throw.
          std::function<void(RefLaneFault)> on_impossible_interference_,
          /// Logs, audits and reacts to a fault already latched: cause, offending key, detail, namespace.
          std::function<void(RefLaneFault, const String &, const String &, const std::optional<String> &)>
              report_impossible_interference_,
  ```
  and the member of the same name with
  `std::function<void(RefLaneFault)> on_impossible_interference;` followed by
  `std::function<void(RefLaneFault, const String &, const String &, const std::optional<String> &)> report_impossible_interference;`.

- [ ] **Step 4: ledger (`CA/Pool/CasRefLedger.cpp`).**
  - `ErrorCodes`: add `extern const int MEMORY_LIMIT_EXCEEDED;`. Includes: add `#include <base/defines.h>`
    (for `UNREACHABLE`). `getExceptionErrorCode(std::exception_ptr)` and
    `getExceptionMessage(std::exception_ptr, bool)` come from `Common/Exception.h`, already included.
  - Constructor: take `report_impossible_interference_` after `on_impossible_interference_` and
    initialise `, report_impossible_interference(std::move(report_impossible_interference_))` after
    `on_impossible_interference(…)`.
  - `enum class Occupant`: replace the `Foreign` line with
    ```cpp
        Foreign,         /// a decodable transaction that is none of the above -- impossible under mount-lease exclusivity
        Unreadable,      /// does not decode as this key's ref-log transaction
    ```
  - Classifier: `return Occupant::Foreign;` in the `catch` becomes `return Occupant::Unreadable;`. In
    its doc block (the one that starts "The `catch` is narrow ON PURPOSE", above `chainLinkFor`),
    replace "into a `Foreign` verdict, and `Foreign` fences the mount closed and raises a
    foreign-interference alarm" with "into an `Unreadable` verdict, which fences the mount closed",
    and replace "Everything else propagates, leaving the caller's lane exactly as it was." with
    "Everything else propagates; each caller decides what a failed classification costs its lane."
  - After the anonymous namespace that holds the classifier, add:
    ```cpp
    std::string_view refLaneFaultName(RefLaneFault fault)
    {
        switch (fault)
        {
            case RefLaneFault::Interference: return "interference";
            case RefLaneFault::Corrupt: return "corrupt";
            case RefLaneFault::Defect: return "defect";
        }
        UNREACHABLE();
    }
    ```
  - **`runRecoveryWalkOnce`**, the `if (occupant == Occupant::Foreign) throw …` statement becomes:
    ```cpp
                    if (occupant == Occupant::Foreign || occupant == Occupant::Unreadable)
                    {
                        const RefLaneFault fault = occupant == Occupant::Unreadable
                            ? RefLaneFault::Corrupt : RefLaneFault::Interference;
                        on_impossible_interference(fault);
                        const String detail = fmt::format(
                            "ref-table recovery for namespace '{}': a DIFFERENT object occupies retained "
                            "writer txn {}-{} above the exact checkpoint frontier",
                            ns.string(), id.writer_epoch, id.ref_sequence);
                        report_impossible_interference(fault, layout.refLogKey(life, id), detail, ns.string());
                        throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "CAS {}; recovery must not publish or install it as that writer's history", detail);
                    }
    ```
  - **`resolveWedgeOnce`**, replace `const Occupant occupant = occupant_object ? … : Occupant::NotOccupied;` with:
    ```cpp
        Occupant occupant = Occupant::NotOccupied;
        /// The memory limit propagates as it always did: the wedge stays and the next flush classifies
        /// again. Any other classifier exception is our defect and faults the lane below.
        std::exception_ptr classify_defect;
        if (occupant_object)
        {
            try
            {
                occupant = classifyRefLogOccupant(ns, wedge.txn_id, occupant_object->bytes, wedge.bytes,
                                                  occupant_decode_probe_for_test);
            }
            catch (...)
            {
                if (getCurrentExceptionCode() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    throw;
                classify_defect = std::current_exception();
            }
        }
    ```
    Declare `std::optional<RefLaneFault> fault;` next to `Reason reason = Reason::None;`. Replace the
    arm `else if (occupant == Occupant::Foreign)` with
    `else if (classify_defect || occupant == Occupant::Foreign || occupant == Occupant::Unreadable)`
    and add as its first statement (after the existing comment):
    ```cpp
                fault = classify_defect ? RefLaneFault::Defect
                    : (occupant == Occupant::Unreadable ? RefLaneFault::Corrupt : RefLaneFault::Interference);
    ```
    Between the closing `}` of the post-I/O lock block and `switch (result.kind)` add:
    ```cpp
        if (fault)
            on_impossible_interference(*fault);
    ```
    In `case WedgeResolution::Corrupted:` replace the `on_impossible_interference(wedge.key, …);`
    statement with:
    ```cpp
                report_impossible_interference(*fault, wedge.key,
                    fmt::format("ref-log wedge resolution for namespace '{}' txn {}-{} observed {} at the wedged slot",
                        ns.string(), wedge.txn_id.writer_epoch, wedge.txn_id.ref_sequence,
                        classify_defect
                            ? fmt::format("an object whose classification failed ({})",
                                          getExceptionMessage(classify_defect, /*with_stacktrace*/ false))
                            : String{*fault == RefLaneFault::Corrupt
                                ? "an object that does not decode as this slot's ref-log transaction"
                                : "a foreign object: neither this attempt's own bytes nor an epoch seal of this namespace"}),
                    ns.string());
    ```
    The survivor `CORRUPTED_DATA` exception after it is unchanged.
  - **`commitRefChunk`, lane not `Ready` at new-id allocation.** Replace the block from
    `RefLaneState lane_state = RefLaneState::Faulted;` through the `on_impossible_interference(…);`
    call with:
    ```cpp
        RefLaneState lane_state = RefLaneState::Faulted;
        {
            std::lock_guard lock(rt->state_mutex);
            lane_state = rt->lane_state;
        }
        if (lane_state != RefLaneState::Ready)
        {
            on_impossible_interference(RefLaneFault::Defect);
            std::optional<String> attempt_key;
            {
                std::lock_guard lock(rt->state_mutex);
                if (rt->append_attempt)
                    attempt_key = rt->append_attempt->key;
                rt->append_attempt.reset();
                rt->lane_state = RefLaneState::Faulted;
            }
            report_impossible_interference(RefLaneFault::Defect, attempt_key.value_or(""), fmt::format(
                "ref-log append reached new-id allocation while the lane was in state {} instead of Ready",
                static_cast<uint8_t>(lane_state)), ns.string());
    ```
    The comment block and `complete_error(…)` that follow stay as they are.
  - **`commitRefChunk`, classification.** Replace the `try`/`catch` around `classifyRefLogOccupant`
    and the whole `if (!classified)` arm with:
    ```cpp
            Occupant occupant = Occupant::Foreign;
            bool classified = false;
            std::exception_ptr classify_error;
            try
            {
                occupant = classifyRefLogOccupant(ns, id, occupant_object->bytes, active_attempt.bytes,
                                                  occupant_decode_probe_for_test);
                classified = true;
            }
            catch (...)
            {
                classify_error = std::current_exception();
            }
            if (!classified && getExceptionErrorCode(classify_error) == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            {
                /// The occupant is not this attempt's bytes and the key is write-once, so the attempt did
                /// not land: the next flush re-derives this id and classifies the same occupant.
                {
                    std::lock_guard lock(rt->state_mutex);
                    if (rt->lane_state == RefLaneState::Writing && rt->append_attempt
                        && rt->append_attempt->txn_id == id)
                    {
                        rt->append_attempt.reset();
                        rt->lane_state = RefLaneState::Ready;
                    }
                }
                complete_error(chunk_survivors, makeCasWriteRetryLaterExceptionPtr(fmt::format(
                    "CAS ref-log append for namespace '{}': a different object occupies txn {}-{} and "
                    "classifying it hit the memory limit; the next flush classifies it again",
                    ns.string(), id.writer_epoch, id.ref_sequence)));
                return false;
            }
            if (!classified)
            {
                on_impossible_interference(RefLaneFault::Defect);
                /// Counted with the quiet arms: a classifier that fails on every attempt would otherwise
                /// show up only as remounts.
                ProfileEvents::increment(ProfileEvents::CASRefAppendOccupantUnreadable);
                const String attempt_key = active_attempt.key;
                {
                    std::lock_guard lock(rt->state_mutex);
                    rt->append_attempt.reset();
                    rt->lane_state = RefLaneState::Faulted;
                }
                report_impossible_interference(RefLaneFault::Defect, attempt_key, fmt::format(
                    "ref-log append for namespace '{}' txn {}-{}: classifying the different object at the id "
                    "it derived failed ({}; observed {})",
                    ns.string(), id.writer_epoch, id.ref_sequence,
                    getExceptionMessage(classify_error, /*with_stacktrace*/ false),
                    detail::renderObservation(conflict->seen)), ns.string());
                complete_error(chunk_survivors, std::make_exception_ptr(Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "CAS ref-log append for namespace '{}': a DIFFERENT object occupies the id {}-{} this table "
                    "derived, and telling a successor's epoch seal from foreign interference did not succeed "
                    "(observed {}) — the lane is faulted until remount recovery adjudicates durable state",
                    ns.string(), id.writer_epoch, id.ref_sequence, detail::renderObservation(conflict->seen))));
                return false;
            }
    ```
  - **`commitRefChunk`, `Foreign`/`Unreadable`.** In the arm `if (occupant != Occupant::Ours)`, make
    the first statements
    ```cpp
                const RefLaneFault fault = occupant == Occupant::Unreadable
                    ? RefLaneFault::Corrupt : RefLaneFault::Interference;
                on_impossible_interference(fault);
    ```
    (before the existing comment and `const String attempt_key = active_attempt.key;`), and replace
    `on_impossible_interference(attempt_key, interference_detail, ns.string());` with
    `report_impossible_interference(fault, attempt_key, interference_detail, ns.string());`.
  - Check: `grep -n 'on_impossible_interference(' CA/Pool/CasRefLedger.cpp` prints exactly five
    calls, each with a single `RefLaneFault` argument, and `grep -n 'report_impossible_interference('`
    prints five calls.

- [ ] **Step 5: Pool (`CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp`).** In the `ref_ledger(…)`
  initialiser replace the interference lambda with:
  ```cpp
            [this] (RefLaneFault) { mount_runtime.tripMountLost(); },
            [this] (RefLaneFault cause, const String & key, const String & reason, const std::optional<String> & offending_ns)
                { reportImpossibleInterference(cause, key, reason, offending_ns); },
  ```
  `Pool::reportImpossibleInterference` takes `RefLaneFault cause` as its first parameter; its
  `LOG_ERROR` becomes
  ```cpp
      LOG_ERROR(getLogger("CasPool"),
          "CAS anomaly policy: ref lane fault '{}' for server_root '{}' (namespace='{}', key='{}'): {} -- "
          "this mount is fenced closed; scheduling a remount",
          refLaneFaultName(cause), config.server_root_id, offending_ns.value_or(String{}), key, reason);
  ```
  the event detail becomes
  `e.detail = {{"key", key}, {"server_root_id", config.server_root_id}, {"cause", String{refLaneFaultName(cause)}}};`,
  and `mount_runtime.tripMountLost();` is deleted, leaving `mount_runtime.scheduleRemount();` with the
  comment "The ledger tripped the fence before this report; the remount is the same bounded
  self-remount a foreign or superseded lease renewal drives." In `CasPool.h` the declaration becomes
  `void reportImpossibleInterference(RefLaneFault cause, const String & key, const String & reason, const std::optional<String> & offending_ns = {});`
  and in its doc replace "LOG_ERROR with full context, emit a `ForeignInterference` CasEvent, then
  fence this mount closed and arm" with "The ledger has already tripped the fence through its latch
  callback. LOG_ERROR with the cause and full context, emit a `ForeignInterference` CasEvent carrying
  the cause, then arm", and replace "every call site raises its OWN `LOGICAL_ERROR`" with "every call
  site raises its own exception".

- [ ] **Step 6: run green.**
  ```bash
  ninja -C build unit_tests_dbms > build/ninja_pr10_t3.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CASRefLaneOccupant.*:CASRefWedgeEveryAttempt.*:CASAnomalyPolicy.*:CASRefWriterAppendLane.*:CASRefInstallSafety.*:CASRefRecovery*:CASRefCheckpoint.*' > build/test_pr10_t3.log 2>&1; echo TEST_EXIT=$?
  ```
  Expected `NINJA_EXIT=0`, `TEST_EXIT=0`; the eleven `CASRefLaneOccupant` tests pass, including the
  two golden ones from Task 2.

- [ ] **Step 7: commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp \
             src/Disks/tests/gtest_cas_ref_wedge_every_attempt.cpp
  git commit -m "Latch every ref-lane fault first with a typed cause" \
             -m "Before this change the nine new CASRefLaneOccupant tests fail: no cause on the event, the report before the latch, and no remount for a classifier exception or for recovery's foreign retained attempt." \
             -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

## Task 4: Gates

- [ ] **Step 1: release gate.**
  `build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr10_gate.log 2>&1; echo TEST_EXIT=$?`
  Expected `TEST_EXIT=0`.
- [ ] **Step 2: ASan gate.**
  ```bash
  ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr10.log 2>&1; echo NINJA_EXIT=$?
  build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr10_gate.log 2>&1; echo TEST_EXIT=$?
  ```
  Expected `NINJA_EXIT=0`, `TEST_EXIT=0`. The sink in `recordFaultEvents` holds a raw `Pool *`; an
  ASan leak report naming `FaultEventLog` means a `PoolPtr` slipped into the capture.
- [ ] **Step 3: style.** `git diff 5b7c9f503df -- src | grep -n '^+.*) {$\|^+.*else {$'` prints
  nothing.

## Task 5: Pull request text

- [ ] **Step 1.** Title: `Latch every CAS ref-lane fault first, with a typed cause`. Body (template
  order, base `antalya-26.6`; do not push without the owner's go):

  ```
  The ref lane now names every object it meets at its own slot: ours, a successor's epoch seal,
  foreign (decodes, not ours) or unreadable (does not decode). Every branch that stops the lane trips
  the mount's write fence first, with a cause `interference`, `corrupt` or `defect`, and only then
  logs, audits and schedules the remount. Two branches used to stop without a remount: a classifier
  exception at the append site left the lane `Faulted` until a manual remount, and recovery that
  found a foreign object at its retained attempt failed every later touch. Both now remount. A
  memory-limit exception while classifying is retried by the next flush instead of faulting the lane.
  A lane in `NeedsRecovery` leaves it on the next touch once its checkpoint reads again; a test now
  pins that.

  The `ForeignInterference` event in `system.cas_log` gains a `cause` key in `detail`.

  Mixed versions: local behaviour only; no format change.

  ### Changelog category (leave one):
  - Bug Fix (user-visible misbehavior in an official stable release)

  ### Changelog entry:
  A CAS table whose ref log meets an unreadable or foreign object now always fences and remounts the disk instead of staying blocked until a manual remount.

  ### Documentation entry for user-facing changes
  - [ ] Documentation is written (mandatory for new features)
  ```
