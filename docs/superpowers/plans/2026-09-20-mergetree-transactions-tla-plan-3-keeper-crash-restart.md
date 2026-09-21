---
description: 'Implementation plan 3 of the TLA+ model of MergeTree transactions: the two failing outcomes of the commit request, the unknown-state list and its two-list resolution, the layered disk with fsync, the process crash, and the restart loader with its coverage tree and legacy records. Adds the scenarios Keeper, Crash, SnapshotCrash and NonTxnCrash, pays the three witness debts plan 2 sent here, closes model defects M1, M3 and M5, and records the durability finding the transaction log documents in a comment and does not fix.'
sidebar_label: 'TLA+ transactions, plan 3'
sidebar_position: 23
slug: /superpowers/plans/mergetree-transactions-tla-plan-3-keeper-crash-restart
title: 'MergeTree transactions TLA+ model, plan 3: Keeper faults, crash and restart'
doc_type: 'plan'
---

# MergeTree transactions TLA+ model, plan 3: Keeper faults, crash and restart {#tla-plan-3}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

Revision 1, 2026-09-20. Plan 1 installed the foundation and the `Base` scenario; plan 2 added `SetSnapshot`,
`Merge` and `NonTxn`, the cleanup thread, the truncation pass and the actor-generic publication and commit
bodies, and left `Server.tla` with thirty-one `== FALSE` stubs. This plan replaces fourteen of them: everything
that makes a server fail and come back.

**Goal:** Four more scenarios, each with its own `MC_<Scenario>.{tla,cfg}`, its own view argument and its own
state-space budget: `Keeper` (a commit request whose response is lost, the two-list resolution of
`unknown_state_list`, both wait modes), `Crash` (the layered disk, `fsync`, the process crash and the restart
loader, with `LEGACY_PARTS` on and off), `SnapshotCrash` (a snapshot lowered below `tail_ptr` across a restart)
and `NonTxnCrash` (the non-transactional removal batch judged on the stored record after a restart). The three
witness debts plan 2 sent here, `AckedWriteIsDurable`, `NoFalseCorruption` and `NtBatchDone`, are paid. Model
defects `M1`, `M3` and `M5` are closed.

**Architecture:** Unchanged from plan 1. `MergeTreeTransactions.tla` keeps the action groups; each new scenario
composes its own `Next` from them. This plan adds two actors to the ones that already exist: the updating thread
becomes a driver of the commit and rollback machines (it finalizes transactions whose commit response was lost),
and the restart loader is a phase machine that rebuilds the in-memory world from the durable disk and from
Keeper. Both reuse the actor-generic bodies plan 2 extracted, which is why they are cheap.

**Tech Stack:** TLA+ (TLA+2), TLC from `tmp/tla2tools.jar` (Java 21), bash.

**Spec:** `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` (revision 11). The spec is the
authority on what an action means; where a row of the spec disagrees with the C++ at the baseline commit, the
model follows the C++ and the task records the disagreement in `FINDINGS.md` section 3.

## Global Constraints {#global-constraints}

- Baseline C++ is upstream master `2c24b6b9291e`, checked out in this worktree; every code-map row is checked by
  opening the function before it is written down, and a row whose spec text disagrees with the function follows
  the function and says so.
- All model files live under `utils/tla/transactions/`; temporary files under `tmp/` (never `/tmp`).
- Every TLC run uses its own `-metadir` under `tmp/tla/<run>/states` and its log goes to `tmp/tla/<run>/tlc.log`.
  Two runs must never share a state directory.
- **One TLC run at a time, always inside `timeout`.** `run_tlc.sh` and `witness.sh` already wrap the JVM in
  `timeout`; nothing in this plan starts a run outside them, and nothing starts a second run while one is going.
  Two runs on one machine do not merely halve the workers, they change the measured numbers this plan records.
- **Never `pkill -f tlc2.TLC`.** Other agents share this machine and that pattern matches their runs too. Kill
  the run by the process group `run_tlc.sh` occupies, or let its `timeout` expire.
- **The STOP file.** A sweep (task 5) checks for `tmp/tla/STOP` between runs and stops there, so that a sweep can
  be ended without killing a JVM in the middle of a run and losing its count. Remove the file before starting a
  sweep, create it to stop one.
- A TLC run that passes 30 million distinct states, or whose queue is still growing after 15 minutes, is a defect
  of the model or of the bounds, not something to wait for: kill it, record the last `Progress` line, reduce
  under the bound contract, and say in `STATE_SPACE.md` which bound moved and why every witness of every property
  that scenario checks is still red under it.
- **Two bound sets per scenario (spec defect S7).** The exhaustive bounds are the ones at which the scenario is
  model-checked green; the witness bounds are the ones at which its witnesses are shown red, and they are at
  least the exhaustive ones. A witness run stops at the first violation, so it can afford bounds an exhaustive
  run cannot. Every scenario this plan adds declares both, as `MC_<Scenario>.{tla,cfg}` and
  `MC_<Scenario>Witness.{tla,cfg}`, and a witness that is not red at the witness bounds is a bound-contract debt
  recorded in `FINDINGS.md` section 2a, not a bound that quietly moves.
- Witness contract (spec, section "Invariants and properties"): `witness.sh Scenario Property [WitnessName]`
  checks only that property with `WITNESS_NAME` set and exits zero only when TLC reports a violation of it. A
  two-change witness declares halves `<WitnessName>_only1` and `<WitnessName>_only2` and both halves must be
  green.
- The four counterexample classes and what to do with each: (a) TLC parse or type error: fix the model; (b) a
  property red on the baseline because the property or the action misreads the code: fix the model and name in
  the report the C++ line that decided; (c) a property red because the C++ has the defect: do not change the
  model, add the trace to `FINDINGS.md` with the action sequence and the C++ call sequence; (d) a witness that
  does not fire: fix the hook or the property.
- **A blocking baseline defect gets a fix variant.** TLC stops at the first violation, so a real code defect
  found on a baseline run masks every counterexample behind it. When class (c) fires, the task additionally
  (1) writes a proposed C++ fix into the `FINDINGS.md` entry, naming file, function and what changes, realistic
  enough to be a pull request; (2) adds a model variant, a declared constant (never a `Witness`) that encodes
  that fix; and (3) runs the scenario again with the variant on, so the search continues past the defect and the
  scenario's remaining properties are checked. Both runs are recorded. Task 3 is where this plan exercises it.
- **Every debt is closed or placed.** A step that cannot finish its obligation names the task, in this plan or in
  a later one, that does, and writes the row into `FINDINGS.md` section 2 or 2a. "Later" is not a placement; a
  task name is.
- Commit after every task with `git commit -- <paths>`; never `git add -A`; do not push. Commit messages end with
  the attribution lines from the session's system reminder.
- No placeholders. Where this plan writes TLA+, the implementer transcribes it; where a step says
  **SANY-check before proceeding**, run `run_tlc.sh Schema` (which parses the whole module chain) before writing
  the next step, and expect the named error class if it was written wrong.

## What this plan replaces {#stubs-replaced}

Fourteen of the thirty-one `== FALSE` stubs in the plan-2 tree (`Server.tla:531-561`), in the order the tasks
reach them:

| Stub | Task |
|---|---|
| `CommitUnknown(k)` (`Server.tla:553`, outside the stub block) | 1 |
| `UpdReconnect` | 1 |
| `UpdSwapUnknownLists` | 1 |
| `UpdFinalizeUnknown(t)` | 1 |
| `Crash` | 2 |
| `ProcessDown(cause)` (the `Other` cause only; `StoreFault` and `RetryExhausted` are plan 5's) | 2 |
| `RestartLoadLog` | 2 |
| `RestartTableStart` | 2 |
| `RestartLoadPart(p)` | 2 |
| `RestartLoadMutation(m)` | 2 |
| `RestartTablePublished` | 2 |
| `RestartOutdatedDone` | 2 |
| `RestartDone` | 2 |

Plus new actions the spec's rows imply but plan 1 did not declare: the two failing outcomes of the commit
request (`CommitKeeperFault`, `MergeCommitKeeperFault`), the client's return from `waitStateChange`
(`CommitUnknownResolved`), the Keeper session expiry (`KeeperSessionExpire`), the end of the unknown-state pass
(`UpdFinalizeDone`), and the updater-driven commit and rollback steps (`UpdCommit*`, `UpdRollback*`). The
remaining seventeen stubs are the mutation machine (`Mut*`, `Kill*`, `CommitStoreMutation`) and the disk-fault
policy (`StoreRetry`, `KillRetry`), which are plans 4 and 5.

`NoexceptFrameDown` is not a stub but is replaced too: it becomes `ProcessDown("Other")` in task 2, because once
`Crash` exists a server that goes down must lose its memory like every other one.

## Spec rows this plan does not follow {#spec-vs-code}

Found while reading the C++ for the code map, and one found by reading the spec against itself. Each is recorded
in `FINDINGS.md` section 3 by the task that hits it, and the model row follows the code.

| Id | Spec row | The code, or the other spec row | Task |
|---|---|---|---|
| S14 | the `SnapshotCrash` row of the scenario matrix lists `NoOutdatedLookup` among its checks | its action set is "`SetSnapshot` scenario + `Restart*`", and the `SetSnapshot` scenario does not enable `Updater+Unknown`. `assertTIDIsNotOutdated` has exactly two call sites, `tryFinalizeUnknownStateTransactions` (`TransactionLog.cpp:387`) and the callerless `getCSNAndAssert` (`:645`), so the property is vacuous there for the same reason spec defect S6 records for `SetSnapshot`. The model adds `Updater+Unknown` to `SnapshotCrash`, which is what makes the row's own claim checkable | 4 |
| S15 | `RestartLoadLog`: "creates one placeholder `csn-` znode, loads `tid_to_csn`, `latest_snapshot`, `tail_ptr`" | complete, but it omits `local_tid_counter = Tx::MaxReservedLocalTID` (`TransactionLog.cpp:219`), which the model deliberately does not reproduce; see model defect M17 in task 2 step 3 for the argument | 2 |
| S16 | the `Crash` row's checks include `RolledBackEventuallyDeleted` "under fairness for the tmp-only part" | a liveness property needs the fairness conditions of the `Live` row, which the spec's own development order puts at item 10, after the disk-fault work. No scenario of this plan declares fairness, and adding it to a 20-million-state scenario would change what TLC does to every run in the plan. Placement: plan 5's liveness task, which owns `Live`, runs `RolledBackEventuallyDeleted` and `OutdatedEventuallyDeleted` in a reduced `Crash` configuration with the fairness the spec names | 5 of this plan records the placement; plan 5 executes it |
| S17 | `UpdFinalizeUnknown`: "`getCSN` then `finalizeCommittedTransaction` or `assertTIDIsNotOutdated` plus `rollbackTransaction`" | correct, and it omits the consequence that matters: the `LOGICAL_ERROR` `assertTIDIsNotOutdated` throws is caught by `runUpdatingThread`'s catch-all (`TransactionLog.cpp:262`), so the thread survives and the *remaining* entries of its local list are destroyed with it. Those transactions are in neither unknown list any more and are never finalized. `NoOutdatedLookup` firing is therefore not only an assertion, it strands every transaction behind it in the same pass | 1 |

## Interfaces this plan produces {#interfaces}

Named here because four tasks consume each other's definitions:

- `Upd == <<"Updater", 0>>` and `Rst == <<"Restart", 0>>`, the two non-actor frame owners `Parts.tla` already
  declares in `FrameOwners` (task 1, task 2).
- `DrivesA(a, t)`, the rollback machine's driver test, and the actor-parameterized rollback actions
  `Rollback*A(a, t, p)` (task 1).
- `tlog.unknown_ready`, `tlog.finalizing` (task 1); `sys.load_queue`, `sys.outdated_queue`, `sys.mut_queue`
  (task 2); `h.payload` (task 2).
- `LegacyInfo`, the corrected `StoredRecord`, `LoadedRecord(p)`, `DiskRoot(p)`, `OnDisk(p)` (task 2, `Parts.tla`
  and `Disk.tla`).
- `DurableRecord(p)`, `DurablePState(p)`, `PState(p)`, the invariant preamble every crash property is stated
  over (task 2, `Invariants.tla`).
- The properties `UnknownResolvesByLog` (task 1), `NoResurrection`, `LogEntryNeeded`,
  `Assert_IsNonTransactionalDomain`, `LegacyLoads` (task 3), `NtBatchDone` (task 4).

---

### Task 1: Keeper faults, the unknown-state list, and the `Keeper` scenario {#task-1}

First, because it is the only task of this plan that does not need the layered disk, and because it makes
`NoOutdatedLookup` non-vacuous, which retires the vacuity half of spec defect S6 before the crash scenarios
inherit it.

**Files:**
- Modify: `utils/tla/transactions/Server.tla` (`TxnPcs`, `Holders`, `TLogRecord`, `TLogInit`, the rollback
  machine's actor parameter, `CommitFinalizeEffect`, `RollbackFinalize`, the new commit-fault and unknown-state
  actions)
- Modify: `utils/tla/transactions/Invariants.tla` (`UnknownResolvesByLog`, `RollbackRestoresStep`'s second
  conjunct)
- Modify: `utils/tla/transactions/MergeTreeTransactions.tla` (`UpdaterUnknownNext`, `KeeperFaultNext`,
  `KeeperNext`, `ClientNext`, `TaskNext`)
- Create: `utils/tla/transactions/MC_Keeper.{tla,cfg}`, `MC_KeeperUnknownWait.{tla,cfg}`,
  `MC_KeeperWitness.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Interfaces:**
- Produces: `Upd`; `DrivesA(a, t)` and the six `Rollback*A` bodies, consumed by nothing else in this plan but
  owed to plan 5's task-driven rollback; `tlog.unknown_ready`, `tlog.finalizing`; `UnknownResolvesByLog`.
- Consumes: `CommitCreateEffect`, `CommitStoreEffect`, `CommitFlipEffect`, `CommitFinalizeEffect`,
  `FirstCommitPc`, `FirstCommitWork`, `KeeperAppended`, `KeeperExpired`, `KeeperRenewed`, `LookupCsn`,
  `NoOutdatedLookup` (defined in plan 2, vacuous until now).

- [ ] **Step 1: The updater becomes an actor**

In `Server.tla`, next to `Sess(k)` and `Tsk(i)`, add

```tla
\* The updating thread and the restart loader are frame owners but not holders of anything and not sessions;
\* Parts.tla already declares both in FrameOwners. Upd is an actor here for two reasons the C++ forces:
\* finalizeCommittedTransaction runs afterCommit on the updating thread, so the per-part CSN stores of a
\* transaction resolved from unknown_state_list are that thread's frames, and rollbackTransaction called from
\* the same place makes the thread the rollback driver.
Upd == <<"Updater", 0>>
Rst == <<"Restart", 0>>
```

`Holders == Actors` becomes `Holders == Actors \cup {Upd}`: `unknown_state_list` holds a
`MergeTreeTransactionPtr` (`TransactionLog.cpp:468`), a real `shared_ptr`, so a transaction in the list has a
holder even after its session or its merge task is gone. Split `rb_driver` off the same set while you are
there, because it is a different thing:

```tla
RbDrivers == Actors \cup {Upd}
TxnRecord == [... holders : SUBSET Holders, rb_driver : RbDrivers \cup {NoActor}, ...]
```

**SANY-check before proceeding.** The error class if `Holders` was widened and `Pins` was not is
`Error: The invariant TypeOK is violated`, on `part[p].pins`; `Upd` is never a pin and `Pins` must not gain it.

- [ ] **Step 2: The rollback machine takes its driver as an argument**

Every rollback action is currently `Drives(k, t)`-shaped. The updater drives one too, and the transaction would
otherwise sit in `RollbackCopyLists` with nothing able to advance it, which is the wedge
`NoTaskDrivenRollback` was written to make visible for tasks. Rename each of the six actions to an `A` form
taking the actor, and keep the session names as one-line wrappers. Mechanically, for each of
`RollbackCopyLists`, `RollbackMarkCreated`, `RollbackOutdateCreated`, `RollbackRestore`, `RollbackUnlock` and
`RollbackFinalize`:

```tla
DrivesA(a, t) == txn[t].rb_driver = a
Drives(k, t) == DrivesA(Sess(k), t)

RollbackCopyListsA(a, t) ==
  /\ DrivesA(a, t) /\ txn[t].pc = "RollbackCopyLists" /\ txn[t].mutex = NoActor
  /\ part' = [p \in Parts |-> IF p \in Range(txn[t].creating) \cup Range(txn[t].removing)
                              THEN [part[p] EXCEPT !.pins = @ \cup {<<"Rollback", t>>}] ELSE part[p]]
  /\ txn' = [txn EXCEPT ![t].pc = NextRollbackPc(t, "RollbackCopyLists"), ![t].work = NextRollbackWork(t, "RollbackCopyLists")]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
RollbackCopyLists(k, t) == RollbackCopyListsA(Sess(k), t)
UpdRollbackCopyLists(t) == RollbackCopyListsA(Upd, t)
```

The only occurrences of `Sess(k)` inside the five bodies that have one are frame owners
(`StartFrame(p, Sess(k), ...)`, `HasFrame(p, Sess(k))`, `FrameDone(p, Sess(k), ...)`); each becomes `a`. None of
the six writes `client` or `task`, so the `UNCHANGED` lists are already actor-independent and do not move.

`RollbackFinalizeA(a, t)` additionally gains one clause, which task 1 step 6 needs and which is a no-op for a
session:

```tla
  /\ tlog' = [tlog EXCEPT !.running_list = @ \ {t}, !.snapshots_in_use[t] = UnknownCSN,
                          !.finalizing = IF @ = t THEN EmptyTID ELSE @]
```

Apply the same one-line change to `CommitFinalizeEffect(a, t)`: `!.finalizing = IF @ = t THEN EmptyTID ELSE @`.
Both are how the unknown-state pass learns that the transaction it was finalizing is done, and writing it into
the existing `tlog'` assignment is what keeps an action to one assignment per variable.

In `Invariants.tla`, `RollbackRestoresStep` quantifies over `Sessions`; give it the second conjunct the same way
`FlipAfterStoresStep` has one per actor:

```tla
RollbackRestoresStep ==
  /\ \A k \in Sessions, t \in Tids : RollbackFinalize(k, t) => RollbackRestoresOn(t)
  /\ \A t \in Tids : UpdRollbackFinalize(t) => RollbackRestoresOn(t)
```

with `RollbackRestoresOn(t)` the existing body (`\A p \in h.removing[t] \ h.creating[t] : ...`) lifted out.

**SANY-check, then run `run_tlc.sh Base` and `witness.sh Base RollbackRestores`.** Both must match the numbers
`STATE_SPACE.md` records for the plan-2 tree (`Base` 26,839,136 distinct). This step is behaviour-preserving for
every existing scenario, and measuring it afterwards is not a check: record the two numbers before the step.

- [ ] **Step 3: The log's unknown-state fields**

`TLogRecord` gains two fields and `TLogInit` two values:

```tla
\*  unknown_state_list, unknown_state_list_loaded: the two lists of tryFinalizeUnknownStateTransactions
\*    (src/Interpreters/TransactionLog.cpp:374-376), already declared in plan 1.
\*  unknown_ready: the local `list` the swap leaves on the stack (:357, :374), which is what the pass actually
\*    walks. It is a field rather than a local because the pass is several model steps: each transaction it
\*    finalizes runs the whole commit or rollback machine before the next one starts.
\*  finalizing: the transaction the pass is inside. The C++ has no such variable; the loop body is one
\*    iteration, and the model needs a name for "the updater is in the middle of this transaction's
\*    afterCommit" because those stores are steps.
              unknown_ready : SUBSET Tids, finalizing : Tids \cup {EmptyTID}]
```

with `unknown_ready |-> {}`, `finalizing |-> EmptyTID` in `TLogInit`. Add `"CommitUnknown"` to `TxnPcs`.

**SANY-check before proceeding.** The error class for a missed `TLogInit` field is
`Error: The invariant TypeOK is violated by the initial state`.

- [ ] **Step 4: The two failing outcomes of the commit request**

`TransactionLog::commitTransaction` (`src/Interpreters/TransactionLog.cpp:419`) sends one `multi` whose last
request is the sequential `csn-` create (`:445`), and catches `Coordination::Exception` at `:459`. The spec's
three outcomes are `Ok` (plan 1's `CommitCreateEffect`), `FailBefore` and `LostAfter`; the catch block is the
same for both failing ones, which is why the fault and the catch are two actions rather than one: after a
`LostAfter` the znode exists, and the updating thread may load it before the catch has run.

```tla
\* The failing outcomes of the sequential create. `lost` is LostAfter: the znode is there and the response is
\* not, which is what the fail point transaction_force_unknown_state_after_commit (TransactionLog.cpp:449)
\* injects and what the two-list scheme in tryFinalizeUnknownStateTransactions exists to survive. ~lost is
\* FailBefore: nothing was appended. Both take the catch at :459, so both park the transaction at CommitUnknown.
\* An expired session is a hardware error by itself and does not consume the fault budget again; with the
\* session expired the request cannot have reached Keeper, so only FailBefore is possible.
CommitKeeperFaultEffect(t, lost) ==
  /\ (zk.session = "Expired" \/ sys.keeper_faults < KEEPER_FAULTS_MAX)
  /\ (lost => zk.session = "Alive" /\ KeeperCanAppend)
  /\ sys' = [sys EXCEPT !.keeper_faults = IF zk.session = "Expired" THEN @ ELSE @ + 1]
  /\ IF lost
     THEN /\ zk' = KeeperAppended(t)
          /\ h' = [h EXCEPT !.committed = @ \cup {t}, !.csn[t] = KeeperNextCsn,
                            !.removers = [p \in Parts |-> IF p \in h.removing[t] THEN @[p] \cup {t} ELSE @[p]]]
     ELSE /\ UNCHANGED <<zk, h>>
  /\ txn' = [txn EXCEPT ![t].pc = "CommitUnknown"]

CommitKeeperFault(k, lost) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitCreateCSN"
     /\ CommitKeeperFaultEffect(t, lost)
  /\ UNCHANGED <<disk, mdisk, part, tlog, client, stmt, mut, task>>
MergeCommitKeeperFault(i, lost) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit"
  /\ txn[task[i].txn].pc = "CommitCreateCSN"
  /\ CommitKeeperFaultEffect(task[i].txn, lost)
  /\ UNCHANGED <<disk, mdisk, part, tlog, client, stmt, mut, task>>

\* The Keeper session expiring on its own, which is the other producer of the catch block. It shares the fault
\* budget with the commit fault, so at KEEPER_FAULTS_MAX = 1 a behaviour has one or the other; what that costs
\* is a behaviour with both, and task 5 measures whether KEEPER_FAULTS_MAX = 2 fits.
KeeperSessionExpire ==
  /\ sys.keeper_faults < KEEPER_FAULTS_MAX /\ zk.session = "Alive"
  /\ zk' = KeeperExpired
  /\ sys' = [sys EXCEPT !.keeper_faults = @ + 1]
  /\ UNCHANGED <<disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>
```

- [ ] **Step 5: The catch block, and what the client is told**

```tla
\* The catch at TransactionLog.cpp:459: under running_list_mutex the transaction and its state guard go into
\* unknown_state_list (:465-469); then either UNKNOWN_STATUS_OF_TRANSACTION is thrown (:472) or, with
\* throw_on_unknown_status = false, CommittingCSN is returned (:477). The transaction keeps state Committing
\* and csn CommittingCSN, and csn_notified stays FALSE, which is what blocks the WAIT_UNKNOWN client.
\* Upd joins holders because the list holds a MergeTreeTransactionPtr; with WAIT_UNKNOWN the session keeps its
\* own holder and blocks, otherwise it is told UnknownStatus and detaches.
CommitUnknown(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitUnknown"
     /\ tlog' = [tlog EXCEPT !.unknown_state_list = @ \cup {t}]
     /\ txn' = [txn EXCEPT ![t].pc = "Idle",
                            ![t].holders = IF WAIT_MODE = "WAIT_UNKNOWN" THEN @ \cup {Upd}
                                           ELSE (@ \cup {Upd}) \ {Sess(k)}]
     /\ IF WAIT_MODE = "WAIT_UNKNOWN"
        THEN /\ client' = [client EXCEPT ![k].waiting = "ForState"]
             /\ UNCHANGED h
        ELSE /\ client' = [client EXCEPT ![k].outcome = "UnknownStatus", ![k].outcome_tid = t,
                                         ![k].current = EmptyTID, ![k].pc = "Idle", ![k].waiting = "None"]
             /\ h' = [h EXCEPT !.outcome[t] = "UnknownStatus"]
  /\ UNCHANGED <<zk, disk, mdisk, part, sys, stmt, mut, task>>

\* MergePlainMergeTreeTask.cpp:195 commits with throw_on_unknown_status = false, so the task simply continues:
\* the tagger releases the reservation and the source pins, the holder is destroyed, and the transaction stays
\* in the list with Upd as its only holder. The task does NOT run CommitFinalizeEffect: the transaction is still
\* in running_list and still holds its snapshot, which is the whole point of the unknown state.
MergeCommitUnknown(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "CommitUnknown"
  /\ LET t == task[i].txn IN
     /\ tlog' = [tlog EXCEPT !.unknown_state_list = @ \cup {t}]
     /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].holders = (@ \cup {Upd}) \ {Tsk(i)}]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {Tsk(i)}]]
     /\ task' = [task EXCEPT ![i] = IdleTask]
  /\ UNCHANGED <<zk, disk, mdisk, h, sys, client, stmt, mut>>

\* waitStateChange returned (WAIT_UNKNOWN only): the updater has finalized the transaction or rolled it back
\* and notified. executeCommit then returns normally, or the query ends with "Transaction was rolled back",
\* which is the CommitError the spec's row names for this path.
CommitUnknownResolved(k) ==
  /\ WAIT_MODE = "WAIT_UNKNOWN"
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ client[k].waiting = "ForState"
     /\ txn[t].csn_notified /\ txn[t].state \in {"Committed", "RolledBack"}
     /\ txn' = [txn EXCEPT ![t].holders = @ \ {Sess(k)}]
     /\ \/ /\ txn[t].state = "Committed"
           /\ client' = [client EXCEPT ![k].outcome = "Acked", ![k].outcome_tid = t, ![k].current = EmptyTID,
                                       ![k].waiting = "None", ![k].pc = "Idle"]
           /\ h' = [h EXCEPT !.outcome[t] = "Acked"]
        \/ /\ txn[t].state = "RolledBack"
           /\ client' = [client EXCEPT ![k].outcome = "Error", ![k].outcome_tid = t, ![k].current = EmptyTID,
                                       ![k].waiting = "None", ![k].pc = "Idle", ![k].last_error = "INVALID_TRANSACTION"]
           /\ h' = [h EXCEPT !.outcome[t] = "Error"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, stmt, mut, task>>
```

Two refinements to write as comments, because a reviewer will ask. `CommitAck`'s `waitForCSNLoaded` guard is not
applied on this path: `executeCommit` with `WAIT_UNKNOWN` returns once the state changed, and the CSN it would
wait for is the one the updater has just loaded by construction. And `CommitUnknownResolved` delivers `Acked`
only for `state = "Committed"`, which `UpdCommitFlip` sets after every per-part store, so
`AckedWriteIsDurable`'s antecedent cannot be reached before the stores.

- [ ] **Step 6: The unknown-state pass**

```tla
\* tryFinalizeUnknownStateTransactions, the two swaps (src/Interpreters/TransactionLog.cpp:373-376). The local
\* list takes the PREVIOUS iteration's unknown_state_list_loaded, and unknown_state_list_loaded takes the
\* current unknown_state_list, which is what makes a transaction wait one whole iteration before it can be
\* resolved: by then every entry that existed when it was appended has been loaded. Collapsing the two lists is
\* the witness of UnknownResolvesByLog.
UpdSwapUnknownLists ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ tlog.unknown_ready = {} /\ tlog.finalizing = EmptyTID
  /\ (tlog.unknown_state_list \cup tlog.unknown_state_list_loaded) /= {}
  /\ IF Witness("UnknownResolvesByLog")
     THEN tlog' = [tlog EXCEPT !.unknown_ready = tlog.unknown_state_list \cup tlog.unknown_state_list_loaded,
                               !.unknown_state_list_loaded = {}, !.unknown_state_list = {}]
     ELSE tlog' = [tlog EXCEPT !.unknown_ready = tlog.unknown_state_list_loaded,
                               !.unknown_state_list_loaded = tlog.unknown_state_list,
                               !.unknown_state_list = {}]
  /\ sys' = [sys EXCEPT !.updater_pc = "Finalize"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, txn, client, stmt, mut, task>>

\* One transaction of the local list (TransactionLog.cpp:379-393). getCSN decides. A CSN means
\* finalizeCommittedTransaction, which runs afterCommit ON THE UPDATING THREAD, so the per-part CSN stores that
\* follow are frames owned by Upd. No CSN means assertTIDIsNotOutdated (:387), then `state_guard = {}` (:388),
\* which CASes csn from CommittingCSN back to UnknownCSN and notifies, and then rollbackTransaction (:389).
\* The model writes RolledBackCSN with csn_notified in one step: the reset and the CAS that follows it are two
\* writes of the same atomic, and waitStateChange is gated on the notification that both carry, so no actor can
\* observe the intermediate value.
UpdFinalizeUnknown(t) ==
  /\ sys.updater_pc = "Finalize" /\ tlog.finalizing = EmptyTID
  /\ t \in tlog.unknown_ready
  /\ \/ /\ LookupCsn(t) /= UnknownCSN
        /\ tlog' = [tlog EXCEPT !.unknown_ready = @ \ {t}, !.finalizing = t]
        /\ txn' = [txn EXCEPT ![t].pc = FirstCommitPc(t), ![t].work = FirstCommitWork(t)]
        /\ h' = [h EXCEPT !.unknown[t] = "Committed"]
     \/ /\ LookupCsn(t) = UnknownCSN
        /\ tlog' = [tlog EXCEPT !.unknown_ready = @ \ {t}, !.finalizing = t]
        /\ txn' = [txn EXCEPT ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                               ![t].pc = "RollbackCopyLists", ![t].rb_driver = Upd]
        /\ h' = [h EXCEPT !.unknown[t] = "RolledBack", !.snapshot[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, sys, client, stmt, mut, task>>

\* the pass ends and the thread leaves tryFinalizeUnknownStateTransactions
UpdFinalizeDone ==
  /\ sys.updater_pc = "Finalize" /\ tlog.unknown_ready = {} /\ tlog.finalizing = EmptyTID
  /\ sys' = [sys EXCEPT !.updater_pc = "Idle"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>

\* afterCommit and the rest of finalizeCommittedTransaction, run by the updating thread on the transaction it
\* is finalizing. Each is the actor-generic body plan 2 extracted, with Upd for the actor.
UpdCommitStore(t, p, op, phase) ==
  /\ tlog.finalizing = t
  /\ CommitStoreEffect(Upd, t, p, op, phase)
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
UpdCommitStoreCreation(t, p) == UpdCommitStore(t, p, "CreationCSN", "CommitStoreCreation")
UpdCommitStoreRemoval(t, p) == UpdCommitStore(t, p, "RemovalCSN", "CommitStoreRemoval")
UpdCommitFlip(t) ==
  /\ tlog.finalizing = t /\ Effects(t) /\ txn[t].state = "Committing"
  /\ (txn[t].pc = "CommitFlip"
      \/ (Witness("FlipAfterStores") /\ txn[t].pc \in {"CommitStoreCreation", "CommitStoreRemoval"}))
  /\ CommitFlipEffect(t)
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, stmt, mut, task>>
UpdCommitFinalize(t) ==
  /\ tlog.finalizing = t /\ txn[t].pc = "CommitFinalize"
  /\ CommitFinalizeEffect(Upd, t)
  /\ UNCHANGED <<zk, disk, mdisk, h, sys, client, stmt, mut, task>>
```

`UpdCommitFinalize` needs no `sys'`: `CommitFinalizeEffect` clears `tlog.finalizing`, and `UpdFinalizeDone`
returns the thread to `Idle` once the list is empty. The `Effects(t)` branch that `CommitReadOnly` covers for a
session cannot arise here: a read-only transaction never reaches `CommitCreateCSN` and therefore never enters
the list. Write that as a comment rather than an action.

**SANY-check before proceeding.** The error class if `UpdCommitFlip` was given its own `tlog'` as well as
`CommitFlipEffect`'s is `Error: Two or more conjuncts assign to tlog'`.

- [ ] **Step 7: `UpdReconnect` and the property**

```tla
\* runUpdatingThread, the expired() branch (src/Interpreters/TransactionLog.cpp:242-252): a new session and a
\* sync. The model has one Keeper, so sync is a no-op and only the session state moves.
UpdReconnect ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ zk.session = "Expired"
  /\ zk' = KeeperRenewed
  /\ UNCHANGED <<disk, mdisk, h, part, tlog, txn, sys, client, stmt, mut, task>>
```

In `Invariants.tla`:

```tla
\* spec #invariants-durability, UnknownResolvesByLog. Stated on the step that decides: the decision the pass
\* takes for t agrees with whether t is in h.committed, which is the fact the two-list scheme buys. It is the
\* property the comment at TransactionLog.cpp:361-373 argues for in prose.
\* A red here has two possible causes and they are different findings: the swap resolved a transaction whose
\* entry was not yet loaded (the two-list scheme), or the truncation pass removed the entry of a committed one
\* before the pass read it (which NoOutdatedLookup also reports, and which spec defect S17 says additionally
\* strands the rest of the list). Read the trace for which before classifying.
UnknownResolvesByLogStep == \A t \in Tids :
  UpdFinalizeUnknown(t) => ((h'.unknown[t] = "Committed") <=> (t \in h.committed))
UnknownResolvesByLog == [][UnknownResolvesByLogStep]_vars
```

- [ ] **Step 8: The scenario and its three configurations**

In `MergeTreeTransactions.tla`:

```tla
UpdaterUnknownNext ==
  \/ UpdReconnect \/ UpdSwapUnknownLists \/ UpdFinalizeDone
  \/ (\E t \in Tids : UpdFinalizeUnknown(t) \/ UpdCommitFlip(t) \/ UpdCommitFinalize(t)
                      \/ UpdRollbackCopyLists(t) \/ UpdRollbackFinalize(t)
                      \/ (\E p \in Parts : UpdCommitStoreCreation(t, p) \/ UpdCommitStoreRemoval(t, p)
                                           \/ UpdRollbackMarkCreated(t, p) \/ UpdRollbackOutdateCreated(t, p)
                                           \/ UpdRollbackRestore(t, p) \/ UpdRollbackUnlock(t, p)))
KeeperFaultNext ==
  \/ KeeperSessionExpire
  \/ (\E k \in Sessions, b \in BOOLEAN : CommitKeeperFault(k, b))
  \/ (\E i \in Tasks, b \in BOOLEAN : MergeCommitKeeperFault(i, b))

\* the Keeper scenario (spec matrix): Base + Merge* + Updater+GC + Updater+Unknown, Keeper faults
KeeperNext == BaseNext \/ TaskNext \/ UpdaterGCNext \/ UpdaterUnknownNext \/ KeeperFaultNext
KeeperSpec == Init /\ [][KeeperNext]_vars
```

and add `CommitUnknownResolved(k)` to `ClientNext` (`CommitUnknown(k)` is already there) and
`MergeCommitUnknown(i)` to `TaskNext`.

`MC_Keeper.tla`: `CoversDef` as `MC_Merge`'s (`M12` covers `P1` and `P2`), `SymSessions`, and a `KeeperView`
built from `MergeView` by adding `tlog.unknown_state_list`, `tlog.unknown_state_list_loaded`,
`tlog.unknown_ready`, `tlog.finalizing`, `h.unknown`, `h.outcome`, `zk.session` (`zk` is kept whole in
`BaseView`, so the session comes with it) and `client[k].waiting`, and by removing from the third justification
("fields no action of this scenario writes") the unknown-state part of `tlog` and the `keeper_faults` counter,
both of which this scenario writes. Write the corrected three-part justification into the module comment.

`MC_Keeper.cfg`: `SPECIFICATION KeeperSpec`, `SYMMETRY SymSessions`, `VIEW KeeperView`, the `MC_Merge`
invariant and property lists plus `UnknownResolvesByLog` and `NoOutdatedLookup`; constants as `MC_Merge`'s plus
`KEEPER_FAULTS_MAX = 1` and `WAIT_MODE = "WAIT"`.

`MC_KeeperUnknownWait.{tla,cfg}`: identical, module renamed, `WAIT_MODE = "WAIT_UNKNOWN"`. That is the
configuration in which `CommitUnknownResolved` is live and `ErrorIsAbsent` is reachable through the rollback
branch, which is the path the spec's `ErrorIsAbsent` row names for this scenario.

`MC_KeeperWitness.{tla,cfg}`: the witness bounds, `TID_MAX = 3`, `CSN_MAX = 36`, two sessions, the same
constants otherwise. It is never run exhaustively.

- [ ] **Step 9: Run and measure**

`run_tlc.sh Keeper`, then `run_tlc.sh KeeperUnknownWait`. Expected green on both. `Merge` is 5.2 million at one
session and `TID_MAX = 3`; this scenario adds the truncation pass (which cost `SetSnapshot` roughly a factor of
two), the unknown-state fields and one fault, so the expected order of magnitude is tens of millions and a
reduction is likely. **Reduce in this order**, recording each measurement in `STATE_SPACE.md`:

1. `CSN_MAX = 35`. Cheap and it only removes commits the scenario cannot reach anyway; check the log's
   `zk.seq < CSN_MAX` guard is not what ends behaviours early.
2. `TID_MAX = 2`, after re-running every witness of step 10 at that bound. `MergeBegin` draws from the same
   counter as `Begin`, so `TID_MAX = 2` means one client transaction beside the merge; the shape
   `UnknownResolvesByLog` needs is one transaction whose commit is lost and one updater pass, so it survives,
   and the shapes that do not survive go into `FINDINGS.md` section 2a as debt `B6` with `MC_KeeperWitness` as
   where they are shown instead.
3. Drop the merge task (`Tasks = {}`, `Parts = {P1, P2}`), which is a deviation from the matrix row and is
   recorded as a bound, with `MC_KeeperWitness` keeping the merge. Take this only if 1 and 2 are not enough.

- [ ] **Step 10: Witnesses**

| Property | Witness name | Scenario | Expected |
|---|---|---|---|
| `UnknownResolvesByLog` | `UnknownResolvesByLog` | `Keeper` | RED: the swap collapses the two lists, so a `LostAfter` transaction whose entry is not yet loaded is rolled back although it is in `h.committed` |
| `NoOutdatedLookup` | `NoOutdatedLookup` | `Keeper` | RED: `UpdRemoveOldEntriesSetTail` uses `latest_snapshot` instead of `getOldestSnapshot`, so `tail_ptr` passes the start CSN of a transaction still in the unknown list |
| `AckedWriteIsDurable` | `AckedWriteIsDurable` | `Keeper`, both wait modes | expected RED only in task 3; here it is the debt's first half and must stay green, which is the check that the unknown path does not acknowledge before the commit point |

`NoOutdatedLookup` is the row that retires the vacuity half of spec defect S6: it is now reached, and
`WITNESSES.md` records it as red rather than as `GREEN (vacuous)`. If its witness is green, that is class (d)
and the hook, not the property, is wrong: check that `UpdFinalizeUnknown` is reachable at the scenario's bounds
at all by running `MC_KeeperWitness` first.

- [ ] **Step 11: Commit**

```bash
git commit -m "tla(transactions): Keeper faults, the unknown-state list, the Keeper scenario" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 2: The layered disk, the crash, and the restart loader {#task-2}

The largest task. It makes three things variable that have been constants since plan 1: the durable layer of the
disk, the server's phase, and the set of loaded parts. Every property that reads `part[p].pstate` has to be
restated over the preamble the spec's invariant section gives, and that restatement is step 7.

**Files:**
- Modify: `utils/tla/transactions/Disk.tla` (`Fsync` helpers already exist; add `DiskWithoutTmp`)
- Modify: `utils/tla/transactions/Parts.tla` (`LegacyInfo`, `StoredRecord`, `NoStoredRecord`, `OnDisk`,
  `DiskRoot`, `LoadedRecord`)
- Modify: `utils/tla/transactions/History.tla` (`h.payload`, `HistoryTypeOK`)
- Modify: `utils/tla/transactions/Server.tla` (`SysRecord`, `SysInit`, `CrashEffect`, `Crash`, `ProcessDown`,
  the seven `Restart*` actions, `FsyncDir`, the four creators' `h.payload` write, the deletion of
  `NoexceptFrameDown`)
- Modify: `utils/tla/transactions/Invariants.tla` (`DurableRecord`, `DurablePState`, `PState`,
  `AckedWriteIsDurable`, `ErrorIsAbsent`)
- Modify: `utils/tla/transactions/MergeTreeTransactions.tla` (`FaultNext`, `RestartNext`, `CrashNext`)
- Create: `utils/tla/transactions/MC_Crash.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Interfaces:**
- Produces: `sys.load_queue`, `sys.outdated_queue`, `sys.mut_queue`; `h.payload`; `LoadedRecord(p)`,
  `OnDisk(p)`, `DiskRoot(p)`, `LegacyInfo`; `DurableRecord(p)`, `DurablePState(p)`, `PState(p)`; the corrected
  `StoredRecord`. Tasks 3 and 4 consume all of them.
- Consumes: `DiskAfterCrash`, `MutDiskAfterCrash`, `DiskWithMetaSynced`, `DiskWithDirSynced`, `DiskWithInfo`
  (`Disk.tla`); `UpdateCsnIfNeeded`, `ApplyOp`, `InfoIsRemoved`, `Involved` (`Parts.tla`).

- [ ] **Step 1: `h.payload`, and why a ghost**

A part's fragment version and its tombstone flag are written by the four actions that create a part and are read
by `FragsOf`. They live in `part[p].payload`, which a crash clears, and the model has no disk field for the data
files. Add the ghost:

```tla
  payload     |-> [p \in Parts |-> [ver |-> 0, tomb |-> FALSE]],
```

to `HistoryInit`, with `/\ h.payload \in [Parts -> (0..3) \X BOOLEAN]` written as
`/\ h.payload \in [Parts -> [ver : 0..3, tomb : BOOLEAN]]` in `HistoryTypeOK`, and write it in `InsertWrite`,
`MergeWrite`, `NtInsertWrite` and `NtDropWrite` next to each one's `h.creator[p]` assignment. `RestartLoadPart`
reads it.

The comment to write: this is a ghost for convenience, not an abstraction. The data files are written and
fsynced before the directory is renamed into place, so a directory that survives a crash carries its payload;
giving the disk record a payload field of its own would double every disk value for a quantity no action ever
changes after creation.

- [ ] **Step 2: The disk's remaining helpers, and the legacy record**

In `Parts.tla`, lift the legacy record out of `LegacyPartRecord` and teach `StoredRecord` about it, which is
model defect `M1` closed:

```tla
\* The record VersionInfo::readFromMultiLineBuffer produces for the pre-storing_version format (upstream
\* aea1c111e0a8): a non-transactional creation. Its storing_version is 0 and not -1, because the FILE EXISTS;
\* -1 is the value StoredRecord returns when there is no record at all. Model defect M1 was exactly this
\* confusion: a legacy part carried mem.sv = 0 while StoredRecord fell through to EmptyInfo with sv = -1, so
\* every store on it took TOO_OLD_VERSION and ended in STALE_VERSION, and no legacy part could ever be written.
LegacyInfo == [EmptyInfo EXCEPT !.ctid = NonTransactionalTID, !.ccsn = NonTransactionalCSN, !.sv = 0]

StoredRecord(p) == IF part[p].deferred_on THEN part[p].deferred
                   ELSE IF DiskHasInfo(p) THEN DiskInfo(p)
                   ELSE IF disk[p].cached.kind = "Legacy" THEN LegacyInfo
                   ELSE EmptyInfo
\* No txn_version.txt of any format and no deferred record: readMetadata would throw CANNOT_OPEN_FILE.
NoStoredRecord(p) == ~part[p].deferred_on /\ disk[p].cached.kind = "None"
```

and `LegacyPartRecord == [AbsentPartRecord EXCEPT !.pstate = "Active", !.deferrable = FALSE, !.mem = LegacyInfo]`.

The rule this makes explicit, and which `RestartLoadPart` must keep: **after any load, `part[p].mem.sv` equals
`StoredRecord(p).sv`.** A part loaded from a directory with no metadata file has `mem.sv = -1`, not 0, or the
first store on it hits `M1` all over again.

Add to `Parts.tla`:

```tla
\* the part directories the loader can see, and the roots of the coverage tree it builds from their names
\* (MergeTreeData::loadDataParts, src/Storages/MergeTree/MergeTreeData.cpp:2857 region)
OnDisk(p) == disk[p].dir_cached
DiskRoot(p) == OnDisk(p) /\ ~\E c \in Parts : c /= p /\ OnDisk(c) /\ p \in Expand({c})

\* VersionMetadataOnDisk::loadMetadata (src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:49) and
\* its four cases: the record that is there (:63-68); the legacy format, which the model carries as its own disk
\* kind; the tmp-only directory, which becomes DummyTID with RolledBackCSN (:76-83) after the tmp file is
\* removed (:58-59); and the directory with neither, which becomes a non-transactional creation (:86-92).
LoadedRecord(p) ==
  IF DiskHasInfo(p) THEN DiskInfo(p)
  ELSE IF disk[p].cached.kind = "Legacy" THEN LegacyInfo
  ELSE IF disk[p].tmp_cached THEN [EmptyInfo EXCEPT !.ctid = DummyTID, !.ccsn = RolledBackCSN, !.sv = -1]
  ELSE [EmptyInfo EXCEPT !.ctid = NonTransactionalTID, !.ccsn = NonTransactionalCSN, !.sv = -1]
```

In `Disk.tla`, add the one helper the tmp-file removal needs:

```tla
\* loadMetadata removes the tmp file it found (VersionMetadataOnDisk.cpp:58-59, removeTmpMetadataFile at :334)
DiskWithoutTmp(p) == [disk EXCEPT ![p].tmp_cached = FALSE, ![p].tmp_durable = FALSE]
```

**SANY-check before proceeding**, then `run_tlc.sh Base` and `run_tlc.sh NonTxnInsert`: both green and within
counting noise of `STATE_SPACE.md`'s figures (26,839,136 and 15,787,889). `LEGACY_PARTS` is empty in both, so
the `StoredRecord` arm is dead there and the counts must not move at all; a moved count means the rewrite
changed a live branch.

- [ ] **Step 3: The crash**

In `Server.tla`, the loading queues and the effect:

```tla
\*  load_queue: the roots of the coverage tree still to load as Active candidates, which is what
\*    loadDataPartsFromDisk walks (src/Storages/MergeTree/MergeTreeData.cpp:2771), pushing a non-active root's
\*    children back onto it (:2828-2834).
\*  outdated_queue: the covered children left for loadOutdatedDataParts (:3539), the asynchronous pass
\*    async_loading_jobs counts.
\*  mut_queue: the mutation files StorageMergeTree::loadMutations (StorageMergeTree.cpp:1619) still has to read.
              load_queue : SUBSET Parts, outdated_queue : SUBSET Parts, mut_queue : SUBSET Mutations]
```

with `load_queue |-> {}`, `outdated_queue |-> {}`, `mut_queue |-> {}` in `SysInit`.

```tla
\* Crash discards every in-memory variable of the server and every cached disk layer. Keeper, the durable
\* layers and the history module survive (spec #failures-crash).
\* local_tid_counter is the one field that does NOT go back to its initial value, although
\* TransactionLog::loadLogFromZooKeeper sets it to Tx::MaxReservedLocalTID (src/Interpreters/TransactionLog.cpp:219).
\* The code may reuse a local number because a TID is (start_csn, local_tid, host) and loadLogFromZooKeeper
\* creates a placeholder znode first (:190), which raises latest_snapshot, so every transaction begun after a
\* restart has a start CSN above every one begun before it. The model's Tids ARE the distinct transactions, so
\* reusing one would merge two of them; keeping the counter monotone is the same set of transactions under a
\* different naming. Recorded as model defect M17, with the placement: the plan that needs a restart to produce
\* more transactions than TID_MAX allows, which is none of them.
CrashEffect ==
  /\ disk' = DiskAfterCrash
  /\ mdisk' = MutDiskAfterCrash
  /\ part' = [p \in Parts |-> AbsentPartRecord]
  /\ txn' = [t \in Tids |-> AbsentTxn]
  /\ tlog' = [TLogInit EXCEPT !.local_tid_counter = tlog.local_tid_counter]
  /\ client' = [k \in Sessions |-> IdleClient]
  /\ stmt' = [a \in Actors |-> NoStmt]
  /\ mut' = [m \in Mutations |-> AbsentMutRecord]
  /\ task' = [i \in Tasks |-> IdleTask]
SysDown == [SysInit EXCEPT !.server = "Down", !.completely_started = FALSE, !.async_loading_jobs = 0,
                           !.loaded_parts = {}, !.loaded_mutations = {},
                           !.keeper_faults = sys.keeper_faults, !.disk_faults = sys.disk_faults,
                           !.query_faults = sys.query_faults]

Crash ==
  /\ sys.server /= "Down" /\ sys.restarts < RESTARTS_MAX
  /\ CrashEffect
  /\ sys' = [SysDown EXCEPT !.restarts = sys.restarts + 1]
  /\ UNCHANGED <<zk, h>>

\* ProcessDown (spec #failures-disk): the effect of Crash plus down_cause, and it does not consume a restart.
\* Only the `Other` cause is produced in this plan: a Refuse-class exception raised inside a noexcept call site,
\* which in the model is a frame parked in Error whose owner is one. StoreFault and RetryExhausted need
\* StorePersist to fault, which is plan 5's. This action replaces NoexceptFrameDown, which took the server
\* "Down" and left every other variable standing; with Restart* live that is no longer a tolerable shortcut.
ProcessDown(cause) ==
  /\ cause = "Other" /\ sys.server /= "Down"
  /\ \E p \in Parts, o \in FrameOwners : FrameError(p, o) /\ FrameOf(p, o).noexcept_owner
  /\ CrashEffect
  /\ sys' = SysDown
  /\ h' = [h EXCEPT !.down_cause = cause]
  /\ UNCHANGED zk
```

Delete `NoexceptFrameDown` and replace its use in `BaseNext` with `ProcessDown("Other")`.

**Run `run_tlc.sh Base`.** Expected green and **expected to move**: a server that goes down now loses its
memory, so the states after that transition collapse onto one another. Record the new number in
`STATE_SPACE.md` under the `Base` history with the reason. Expected direction: down. A red is about a property
that reads a variable the crash clears while the server is `Down`, which is what step 7 fixes; if it comes
before step 7, note it and do step 7 first.

- [ ] **Step 4: The log phase of the restart**

```tla
\* TransactionLog::loadLogFromZooKeeper (src/Interpreters/TransactionLog.cpp:180): the placeholder csn- znode
\* (:190), tid_to_csn and latest_snapshot from the children list (:215), tail_ptr from its znode (:220). The
\* updating thread starts here (:72), which is why UpdLoadEntriesMap, UpdSwapUnknownLists and UpdFinalizeUnknown
\* run in LogUp while UpdRemoveOldEntries* waits for server_completely_started.
RestartLoadLog ==
  /\ sys.server = "Down"
  /\ zk.session = "Alive" /\ KeeperCanAppend
  /\ LET newzk == KeeperAppended(EmptyTID)
         ph == KeeperNextCsn IN
     /\ zk' = newzk
     /\ tlog' = [tlog EXCEPT
          !.tid_to_csn = [t \in Tids |-> IF \E c \in DOMAIN newzk.log : newzk.log[c] = t
                                         THEN CHOOSE c \in DOMAIN newzk.log : newzk.log[c] = t
                                         ELSE UnknownCSN],
          !.latest_snapshot = ph, !.last_loaded_entry = ph,
          !.tail_ptr = zk.tail, !.updated_tail_ptr = FALSE]
     /\ h' = [h EXCEPT !.loaded = [t \in Tids |-> @[t] \/ KeeperHas(t)]]
  /\ sys' = [sys EXCEPT !.server = "LogUp"]
  /\ UNCHANGED <<disk, mdisk, part, txn, client, stmt, mut, task>>

\* the storage constructor begins (StorageMergeTree.cpp:238 calls loadMutations; loadDataParts builds the
\* coverage tree from the part names on disk). No client, merge, mutation or cleanup action is enabled in
\* TableLoading, which the Up guard already gives.
RestartTableStart ==
  /\ sys.server = "LogUp"
  /\ sys' = [sys EXCEPT !.server = "TableLoading",
                        !.load_queue = { p \in Parts : DiskRoot(p) },
                        !.outdated_queue = { p \in Parts : OnDisk(p) /\ ~DiskRoot(p) },
                        !.mut_queue = { m \in Mutations : mdisk[m].file_cached }]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>

\* every root and every mutation file processed: the table is published while the covered children keep loading
RestartTablePublished ==
  /\ sys.server = "TableLoading" /\ sys.load_queue = {} /\ sys.mut_queue = {}
  /\ sys' = [sys EXCEPT !.server = "TableUp",
                        !.async_loading_jobs = IF sys.outdated_queue = {} THEN 0 ELSE 1]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>
RestartOutdatedDone ==
  /\ sys.server = "TableUp" /\ sys.outdated_queue = {} /\ sys.async_loading_jobs = 1
  /\ sys' = [sys EXCEPT !.async_loading_jobs = 0]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>
\* isServerCompletelyStarted does not wait for the outdated pass, which is the race the async_loading_jobs gate
\* of removeOldEntries (TransactionLog.cpp:296-301) exists for.
RestartDone ==
  /\ sys.server = "TableUp" /\ ~sys.completely_started
  /\ sys' = [sys EXCEPT !.completely_started = TRUE]
  /\ h' = [h EXCEPT !.down_cause = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, txn, client, stmt, mut, task>>
```

**SANY-check before proceeding.** `newzk.log` is a record field of a `LET`-bound value, which parses; the error
class if `zk'` was written there instead is
`Error: The prime operator cannot be applied to the identifier zk in this context`.

- [ ] **Step 5: Loading one part**

This is the step to transcribe slowly. `MergeTreeData::loadDataPart`
(`src/Storages/MergeTree/MergeTreeData.cpp:2591`) calls `loadAndUpdateMetadata`
(`VersionMetadata.cpp:605`: `loadMetadata`, `updateCSNIfNeeded`, `validateInfo`, and `storeInfo` when the update
changed something) and then decides the state at `:2686-2691`: a part that was involved in a transaction and
whose creation is `RolledBackCSN` or whose removal has a CSN goes to `Outdated` through `preparePartForRemoval`
(`:2516`), everything else keeps the state the caller asked for.

```tla
\* MergeTreeData::loadDataPart (src/Storages/MergeTree/MergeTreeData.cpp:2591) for one part, on either pass:
\* the active pass of loadDataPartsFromDisk (:2771) or the asynchronous outdated pass (:3539).
\* Refinement, stated here because it is the one place this action departs from the store machinery: the load's
\* own store is a single step, not the three-step frame every other store uses. The loader is the only actor
\* that can touch the part (the table is not published for an active-pass part, and an outdated-pass part is not
\* in the working set yet), so no second frame can interleave and the frame would add states without adding
\* behaviours. deferrable is recomputed from whether a metadata file exists, which is what the
\* VersionMetadataOnDisk constructor does at VersionMetadataOnDisk.cpp:44, and it is what gives task 4 the part
\* that is both involved in a transaction and carries a deferred record.
RestartLoadPart(p) ==
  /\ \/ (sys.server = "TableLoading" /\ p \in sys.load_queue)
     \/ (sys.server = "TableUp" /\ p \in sys.outdated_queue)
  /\ LET active_pass == p \in sys.load_queue
         raw == LoadedRecord(p)
         upd == UpdateCsnIfNeeded(p, raw).info
         \* loadDataPart:2686-2691, "deactivate part if creation was not committed or if removal was"
         dead == Involved(upd) /\ (upd.ccsn = RolledBackCSN \/ upd.rcsn /= UnknownCSN)
         st == IF active_pass /\ ~dead THEN "Active" ELSE "Outdated"
         \* preparePartForRemoval (:2516): an Outdated part with no removal at all gets a non-transactional one
         final == IF st = "Outdated" /\ ~InfoIsRemoved(upd)
                  THEN ApplyOp("RemovalTID", NonTransactionalTID, upd) ELSE upd
         wrote == final /= raw
         defer == wrote /\ ~DiskHasInfo(p) /\ ~Involved(final)
         sv1 == IF wrote THEN StoredRecord(p).sv + 1 ELSE raw.sv
         rec == [final EXCEPT !.sv = sv1]
         kids == IF active_pass /\ st /= "Active" THEN Covers[p] ELSE {} IN
     /\ part' = [part EXCEPT ![p] = [AbsentPartRecord EXCEPT
                   !.pstate = st, !.mem = rec, !.payload = h.payload[p],
                   !.deferrable = ~DiskHasInfo(p) /\ ~wrote,
                   !.deferred_on = defer, !.deferred = IF defer THEN rec ELSE EmptyInfo]]
     /\ disk' = IF wrote /\ ~defer THEN DiskWithInfo(p, rec)
                ELSE IF disk[p].tmp_cached THEN DiskWithoutTmp(p) ELSE disk
     /\ sys' = [sys EXCEPT !.loaded_parts = @ \cup {p},
                           !.load_queue = (@ \ {p}) \cup kids,
                           !.outdated_queue = (@ \ {p}) \ kids]
  /\ UNCHANGED <<zk, mdisk, h, tlog, txn, client, stmt, mut, task>>
```

Four points to check while transcribing, because they are where this will go wrong.

1. `UpdateCsnIfNeeded(p, raw)` can return `status = "Retry"`, the stale-removal-lock case. It cannot here:
   `part[p].lock` is `EmptyTID` for an unloaded part, and the `Retry` arm needs a non-empty lock that disagrees
   with the record. Write `UpdateCsnIfNeeded(p, raw).info` and a comment saying so, rather than a branch nothing
   can take.
2. The tmp file is removed by `loadMetadata` whether or not a record was found (`VersionMetadataOnDisk.cpp:58`),
   which is why the `disk'` expression has a third arm. If `wrote` and the record came from a tmp-only
   directory, `DiskWithInfo` already clears `tmp_cached`.
3. `validateInfo` (`VersionMetadata.cpp:613`) throws on a record that fails it, and the code then marks the part
   broken and detaches it. The model has no detach; `Assert_validateInfo` quantifies over every part whose
   `pstate` is not `Absent`, so a load that produced an invalid record is reported as an assertion violation
   instead. That is the honest treatment and it is stronger, not weaker; write it as a comment next to the
   action.
4. `preparePartForRemoval`'s own `LOGICAL_ERROR` (`:2526`) is not transcribed, for the reason the spec gives in
   the paragraph after the invariant tables: on the modelled paths every part that reaches it satisfies the
   condition. Keep that sentence as a comment.

- [ ] **Step 6: Loading one mutation, and the two remaining actions**

`RestartLoadMutation` belongs to **this** plan and not to plan 4, and the spec settles it: the `Crash` row has no
mutation in its universe, but `RestartTablePublished` waits for `loadMutations` to finish, so the phase machine
needs the action even where it is vacuous. Its properties (`MutationNotResurrected`, `MutationRecovered`,
`MutationRecoveredStrict`) are named for `MutationCrash`, which is plan 4's, and this plan does not define them.

```tla
\* StorageMergeTree::loadMutations (src/Storages/StorageMergeTree.cpp:1619): a transactional entry with no csn
\* gets the log's value written to it (:1645) or the file is removed (:1653); a non-transactional entry is
\* registered unconditionally, which is the pre-fix behaviour of upstream 2903f6d48693 that
\* NoUnattachedMutationAfterFailure targets in plan 4. Vacuous while Mutations = {}.
RestartLoadMutation(m) ==
  /\ sys.server = "TableLoading" /\ m \in sys.mut_queue
  /\ LET tid == mdisk[m].tid
         csn == IF mdisk[m].csn_cached /= UnknownCSN THEN mdisk[m].csn_cached ELSE LookupCsn(tid)
         keep == tid = NonTransactionalTID \/ csn /= UnknownCSN IN
     /\ IF keep
        THEN /\ mut' = [mut EXCEPT ![m] = [AbsentMutRecord EXCEPT !.mstate = "Registered", !.tid = tid,
                                             !.csn = csn, !.file_owner = {"Map"}]]
             /\ mdisk' = [mdisk EXCEPT ![m].csn_cached = csn]
        ELSE /\ mut' = [mut EXCEPT ![m] = AbsentMutRecord]
             /\ mdisk' = [mdisk EXCEPT ![m].file_cached = FALSE, ![m].file_durable = FALSE]
     /\ sys' = [sys EXCEPT !.mut_queue = @ \ {m},
                           !.loaded_mutations = IF keep THEN @ \cup {m} ELSE @]
  /\ UNCHANGED <<zk, disk, h, part, tlog, txn, client, stmt, mut_unused_none, task>>
```

The `UNCHANGED` list above is deliberately wrong as written: `mut` and `mdisk` are assigned, so neither belongs
in it, and there is no variable called `mut_unused_none`. **Write it as
`UNCHANGED <<zk, disk, h, part, tlog, txn, client, stmt, task>>`.** The error class if it is transcribed
literally is `Error: Unknown identifier mut_unused_none`.

The directory fsync, which the spec's disk section names and plan 2 did not need:

```tla
\* The other half of Fsync: the directory entry and the rename. storeInfoToDataPartStorage takes a directory
\* sync guard only when fsync_part_directory is on (VersionMetadataOnDisk.cpp:361-363), so without it the rename
\* reaches the durable layer only through a later sync, which is this action.
FsyncDir(p) == /\ Layered /\ disk' = DiskWithDirSynced(p)
               /\ UNCHANGED <<zk, mdisk, h, part, tlog, txn, sys, client, stmt, mut, task>>
```

- [ ] **Step 7: The invariant preamble**

Every part-state clause must read the durable layer for a part the loader has not reached. In `Invariants.tla`,
above the durability section:

```tla
\* spec, "Invariants and properties", the preamble: a part-state clause is evaluated on part[p].pstate while
\* p \in sys.loaded_parts and on the durable layer of disk[p] otherwise. A durable committed or
\* non-transactional removal counts as Outdated; a durable rolled-back creation, and a missing directory, count
\* as Absent. Before the first crash loaded_parts is the whole universe, so every scenario of plans 1 and 2 is
\* unaffected.
DurableRecord(p) ==
  IF disk[p].durable.kind = "Info" THEN disk[p].durable.info
  ELSE IF disk[p].durable.kind = "Legacy" THEN LegacyInfo
  ELSE EmptyInfo
DurablePState(p) ==
  IF ~disk[p].dir_durable THEN "Absent"
  ELSE LET r == DurableRecord(p) IN
       IF r.ccsn = RolledBackCSN THEN "Absent"
       ELSE IF r.rcsn /= UnknownCSN \/ r.rtid = NonTransactionalTID THEN "Outdated"
       ELSE "Active"
PState(p) == IF p \in sys.loaded_parts THEN part[p].pstate ELSE DurablePState(p)
```

Replace `part[p].pstate` with `PState(p)` in `AckedWriteIsDurable` and in `ErrorIsAbsent`, and nowhere else:
every other property is about what an actor of the running server does, and an actor cannot act while the server
is down. Write that sentence next to the two changes.

`AckedWriteIsDurable` keeps the `S5` relaxation plan 2 gave it (an `Outdated` created part is admitted when the
removal is committed **or** in flight); with `PState` the in-flight disjunct reads `part[p].lock`, which is
meaningless for an unloaded part, so guard it: `(p \in sys.loaded_parts /\ part[p].lock /= EmptyTID)`.

**SANY-check, then `run_tlc.sh Base`.** Green, and within noise of the number step 3 recorded.

- [ ] **Step 8: The scenario**

```tla
FaultNext == Crash \/ ProcessDown("Other")
RestartNext == RestartLoadLog \/ RestartTableStart \/ RestartTablePublished \/ RestartOutdatedDone \/ RestartDone
               \/ (\E p \in Parts : RestartLoadPart(p))
               \/ (\E m \in Mutations : RestartLoadMutation(m))
SyncNext == \E p \in Parts : Fsync(p) \/ FsyncDir(p)

\* the Crash scenario (spec matrix): Base + Merge* + Cleanup* + Updater+GC + Restart*, Layered disk
CrashNext == BaseNext \/ TaskNext \/ CleanupNext \/ UpdaterGCNext \/ RestartNext \/ FaultNext \/ SyncNext
CrashSpec == Init /\ [][CrashNext]_vars
```

`MC_Crash.tla`: `CoversDef` as `MC_Merge`'s, `SymSessions`, and a `CrashView` built from `MergeView` by
**adding** the durable disk layers (`disk[p].durable`, `disk[p].tmp_durable`, `disk[p].dir_durable`), `h.payload`,
`sys.server`, `sys.completely_started`, `sys.async_loading_jobs`, `sys.loaded_parts`, `sys.load_queue`,
`sys.outdated_queue`, `sys.restarts` and `tlog.last_loaded_entry`. `MergeView`'s second justification, "fields
that are a function of the ones kept: the durable layer, which `DISK_MODE = "Durable"` keeps equal to the cached
layer", is exactly the one that dies here; write the corrected justification and say which clause replaced it.

`MC_Crash.cfg`: `SPECIFICATION CrashSpec`, `SYMMETRY SymSessions`, `VIEW CrashView`, the `MC_Merge` invariant and
property lists (task 3 adds the crash properties); constants as `MC_Merge`'s plus `RESTARTS_MAX = 1`,
`DISK_MODE = "Layered"`, `FSYNC_PART_DIRECTORY = FALSE`.

- [ ] **Step 9: Run, and expect to reduce**

`run_tlc.sh Crash`. Expected green at this stage, before the crash properties exist; what this run is for is the
budget and the type invariant. The scenario multiplies `Merge`'s 5.2 million by the durable layer of three parts
(three boolean-ish fields each), the two free fsync actions per part, the restart phase machine and the restart
counter. **Expect to reduce, in this order**, recording each measurement:

1. `TID_MAX = 2`, `CSN_MAX = 35`. Note that a restart consumes a CSN for the placeholder znode, so `CSN_MAX`
   must leave room for `RESTARTS_MAX` of them above the commits the scenario needs; if `KeeperCanAppend` is what
   ends behaviours, the bound is wrong, not the model.
2. Drop `FsyncDir` and make the directory durable at creation (`DiskWithDir` sets both layers). What that costs
   is the crash that loses a part directory whose metadata file survived, which is a shape `NoResurrection` does
   not need and `LogEntryNeeded` does not read. Record it as a bound with that argument, not as a simplification.
3. Drop the merge task (`Tasks = {}`, `Parts = {P1, P2}`) into `MC_CrashSmall` and keep `MC_Crash` at the witness
   bounds only. Take this only if 1 and 2 are not enough, and record the coverage loss as a bound-contract debt:
   the coverage-tree shapes the spec's `Crash` paragraph names (a crash between the merge's rename and its
   `CommitCreateCSN`, and one between that and its `CommitStoreCreation`) both need `M12`, and they are then
   shown only at the witness bounds.

Record the outcome in `STATE_SPACE.md` under a new section "The `Crash` scenario".

- [ ] **Step 10: Commit**

```bash
git commit -m "tla(transactions): the layered disk, the crash and the restart loader" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 3: What a restart must not do {#task-3}

Task 2 built the machine; this task states what it owes and runs it against the two `LEGACY_PARTS` settings and
the two `FSYNC_PART_DIRECTORY` settings the matrix asks for. It is the task this plan expects to produce a
finding.

**Files:**
- Modify: `utils/tla/transactions/Parts.tla` (the `NoResurrection` hook in `TryGetCsn`)
- Modify: `utils/tla/transactions/Server.tla` (the `LegacyLoads` and `Assert_IsNonTransactionalDomain` hooks in
  `RestartLoadPart`, the `AckedWriteIsDurable` hook in `CommitAck`, the `LogEntryNeeded` hook in
  `UpdRemoveOldEntriesSetTail`, the M3 and M5 closures)
- Modify: `utils/tla/transactions/Invariants.tla` (`NoResurrection`, `LogEntryNeeded`, `LegacyLoads`,
  `Assert_IsNonTransactionalDomain`)
- Modify: `utils/tla/transactions/MC_Crash.{tla,cfg}`; create `MC_CrashLegacy.{tla,cfg}`,
  `MC_CrashSynced.{tla,cfg}`, `MC_CrashWitness.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Interfaces:**
- Produces: `NoResurrection`, `LogEntryNeeded`, `LegacyLoads`, `Assert_IsNonTransactionalDomain`, consumed by
  task 4's two scenarios.
- Consumes: everything task 2 produced.

- [ ] **Step 1: The three properties of the loader**

```tla
\* spec #invariants-cleanup, NoResurrection. Stated on the load step and over history, not over the durable
\* record, so that a wrong updateCSNIfNeeded is caught rather than assumed: a part must not come back Active if
\* anybody committed its removal, if a non-transactional removal took effect, or if its creator never committed.
NoResurrectionStep == \A p \in Parts : RestartLoadPart(p) =>
  (part'[p].pstate = "Active" =>
     /\ h.removers[p] = {}
     /\ (h.creator[p] \in Tids => h.creator[p] \in h.committed))
NoResurrection == [][NoResurrectionStep]_vars

\* spec #invariants-cleanup, LogEntryNeeded. A state invariant rather than an action property: the obligation is
\* about the world after the entry is gone, and h.truncated only grows. It is the property the TODO in
\* TransactionLog::removeOldEntries (src/Interpreters/TransactionLog.cpp:305-308) says the code does not yet
\* keep: "we write CSNs into data parts without fsync, so it's theoretically possible that we wrote CSN,
\* finished transaction, removed its entry from the log, but after that server restarts and CSN is not actually
\* saved to metadata on disk. We should store a bit more entries in ZK and keep outdated entries for a while."
LogEntryNeeded == \A t \in h.truncated : \A p \in Parts :
  LET r == DurableRecord(p) IN
  /\ ~(r.ctid = t /\ r.ccsn = UnknownCSN)
  /\ ~(r.rtid = t /\ r.rcsn = UnknownCSN)

\* spec #invariants-cleanup, LegacyLoads: a part whose stored record is the pre-storing_version format loads
\* Active with a non-transactional creation (upstream aea1c111e0a8).
LegacyLoadsStep == \A p \in Parts :
  (RestartLoadPart(p) /\ disk[p].cached.kind = "Legacy" /\ p \in sys.load_queue) =>
    (part'[p].pstate = "Active" /\ part'[p].mem.ctid = NonTransactionalTID)
LegacyLoads == [][LegacyLoadsStep]_vars

\* spec #invariants-code, Assert_IsNonTransactionalDomain: every step that evaluates isNonTransactional sees a
\* tid in the predicate's domain, which on the baseline includes the exact DummyTID (upstream c309b745aaf0).
\* The three evaluating sites are RestartLoadPart, validateInfo inside StoreRead, and NtBatchPreflight; the
\* model states it over the one the tmp-only shape reaches.
IsNonTransactionalDomain(t) ==
  \/ t = DummyTID
  \/ t = NonTransactionalTID
  \/ t \in Tids \cup {EmptyTID}
Assert_IsNonTransactionalDomainStep == \A p \in Parts : RestartLoadPart(p) =>
  (IsNonTransactionalDomain(LoadedRecord(p).ctid) /\ IsNonTransactionalDomain(LoadedRecord(p).rtid))
Assert_IsNonTransactionalDomain == [][Assert_IsNonTransactionalDomainStep]_vars
```

`Assert_IsNonTransactionalDomain` is green by construction on the baseline, because `AllTids` is the domain and
nothing else can be in a record. Its witness is the pre-`f5f4635154a0` predicate, which asserted on `DummyTID`
itself, and the witness therefore removes `t = DummyTID` from `IsNonTransactionalDomain`, guarded by
`Witness("Assert_IsNonTransactionalDomain")`. Say in `WITNESSES.md` that the property is a tautology on the
model's type and that what the witness really shows is that a tmp-only part is loaded at all, which is the
reachability the calibration row needs.

- [ ] **Step 2: The four hooks**

- `NoResurrection`: in `Parts.tla`'s `TryGetCsn`, the third arm becomes
  `ELSE IF Witness("NoResurrection") THEN UnknownCSN ELSE RolledBackCSN`. That is the spec's row exactly: a tid
  absent from the log resolves to unknown rather than rolled back, so a part whose creating transaction never
  committed loads `Active`.
- `LegacyLoads`: in `RestartLoadPart`, `raw` becomes
  `IF Witness("LegacyLoads") /\ disk[p].cached.kind = "Legacy" THEN [EmptyInfo EXCEPT !.ctid = DummyTID, !.ccsn = RolledBackCSN, !.sv = -1] ELSE LoadedRecord(p)`,
  which is "the loader treats a legacy record as a parse failure".
- `LogEntryNeeded`: in `UpdRemoveOldEntriesSetTail`, the gate conjunct
  `(tlog.updated_tail_ptr \/ sys.async_loading_jobs = 0)` becomes
  `(Witness("LogEntryNeeded") \/ tlog.updated_tail_ptr \/ sys.async_loading_jobs = 0)`, which is calibration row
  `349c46105730`.
- `AckedWriteIsDurable`: in `CommitAck`, the guard `txn[t].state = "Committed" /\ txn[t].pc = "Idle"` becomes
  `(txn[t].state = "Committed" /\ txn[t].pc = "Idle") \/ (Witness("AckedWriteIsDurable") /\ txn[t].pc = "CommitCreateCSN")`,
  which is the spec's row, "`CommitAck` moved before `CommitCreateCSN`". Its second half, "and `Fail` allowed
  after it", is not needed: `Crash` is an enabled action of this scenario and supplies the loss, so the witness
  stays a one-change witness and `witness.sh`'s minimality machinery is not involved.

- [ ] **Step 3: Close M3 and M5**

Both model defects were placed on "the task that enables `Crash`", and both are now live rather than dead.

`M5`: `TransactionLog::removeOldEntries` stores `updated_tail_ptr = true` at `:302`, before it reads the znode
and before the `new == old` early return; the model sets it inside the branch where the tail moves. With
`completely_started` and `async_loading_jobs` now variable, the difference is observable: the model keeps the
async gate armed after a pass the code would have disarmed, which makes the model's truncation *more*
conservative than the code's and could hide a `LogEntryNeeded` violation. Fix it by splitting the action:

```tla
\* removeOldEntries up to the early return (TransactionLog.cpp:288-317). updated_tail_ptr is stored at :302,
\* before the znode read, so a pass that finds the tail unchanged still disarms the async-loading gate. That is
\* the whole content of this action when the tail does not move, and it is why it is separate from SetTail.
UpdRemoveOldEntriesArm ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ sys.completely_started /\ ~tlog.updated_tail_ptr /\ sys.async_loading_jobs = 0
  /\ tlog' = [tlog EXCEPT !.updated_tail_ptr = TRUE]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, txn, sys, client, stmt, mut, task>>
```

and `UpdRemoveOldEntriesSetTail` keeps its own `!.updated_tail_ptr = TRUE` (harmless, idempotent) and its gate.
Add the new action to `UpdaterGCNext`.

`M3`: `UpdRemoveOldEntriesDelete` re-reads `tid_to_csn` and `latest_snapshot` per iteration where the code
snapshots both once, and `UpdRemoveOldEntriesDone` is unguarded. The placement said this task either narrows
them or argues the widening is sound for `LogEntryNeeded`. **Argue it, do not narrow it**, and write the
argument into `FINDINGS.md`: both departures only let the model remove *more* entries than the code would in the
same pass, `LogEntryNeeded` is violated by removing an entry that is still needed, and a property of the form
"nothing needed is removed" is monotone in the set of removals, so a model that removes a superset of the code's
removals reports every violation the code has and possibly more. If the property fires on a trace that needs the
re-read, the trace is checked against the C++ before it is called a finding, and the task says so in the entry.

- [ ] **Step 4: The four configurations**

Add `NoResurrection`, `LogEntryNeeded`, `Assert_IsNonTransactionalDomain` to `MC_Crash.cfg`'s `PROPERTIES` and
`LegacyLoads` only to the legacy one.

- `MC_Crash`: `LEGACY_PARTS = {}`, `FSYNC_PART_DIRECTORY = FALSE`. The baseline run.
- `MC_CrashSynced`: `FSYNC_PART_DIRECTORY = TRUE`. The matrix asks for both values, and this is also the variant
  step 5 uses to continue the search.
- `MC_CrashLegacy`: `LEGACY_PARTS = {P1}`, `FSYNC_PART_DIRECTORY = FALSE`, plus `LegacyLoads`. `LEGACY_PARTS` is
  constrained by the spec to roots that are `Active` at init, and `P1` is one; `Covers` is unchanged, so `M12`
  can still merge `P1` and `P2` over it.
- `MC_CrashWitness`: the witness bounds (`TID_MAX = 3`, `CSN_MAX = 36`, two sessions, `Tasks = {i1}`), never run
  exhaustively.

- [ ] **Step 5: Run, and the finding this task expects**

`run_tlc.sh Crash`. **Expected red on `LogEntryNeeded`**, and that is finding F8, a class (c) result. The shape
to look for: a transaction commits and `afterCommit` stores its creation CSN into the *cached* layer only,
because `DISK_MODE = "Layered"` with `FSYNC_PART_DIRECTORY = FALSE` makes the rename durable only through a
later `Fsync`; the transaction finalizes, leaves `running_list`, `UpdRemoveOldEntriesSetTail` moves `tail_ptr`
past its start CSN and `UpdRemoveOldEntriesDelete` removes its entry; the durable record still names the tid
with no CSN. No crash is needed to reach the violated state, only the missing `Fsync`; the crash is what makes
it matter, and the trace should be read for whether the scenario found the version with the restart.

This is a defect the code documents and does not fix: the TODO at `TransactionLog.cpp:305-308` describes exactly
this window and proposes "store a bit more entries in ZK and keep outdated entries for a while". The
`FINDINGS.md` entry gets that quote, the trace, the action sequence, the C++ call sequence, and a proposed fix
realistic enough to be a pull request: hold `tail_ptr` back to the oldest CSN that any loaded part still needs,
which `MergeTreeData` can report (the minimum over parts whose `creation_csn` or `removal_csn` is set in memory
but whose `txn_version.txt` has not been fsynced), or fsync the metadata file in `storeInfoToDataPartStorage`
rather than only the tmp file.

The model variant that continues the search is **`FSYNC_PART_DIRECTORY = TRUE`**, which is already a declared
constant and is `MC_CrashSynced`: with it, `DiskWithInfo` makes the rename durable at once and the window
closes. Run `run_tlc.sh CrashSynced` next and treat **that** run as the one that checks the scenario's remaining
properties. Record both runs. Note in the entry that the constant is a model variant of the *second* proposed
fix, and that the first (holding `tail_ptr` back) has no variant in this plan; if the reviewer wants it, its
placement is plan 5's calibration task.

**If `LogEntryNeeded` is green on the baseline**, that is a class (d) result and the task must say why before
moving on: check that `UpdRemoveOldEntriesDelete` is reachable at the scenario's bounds (it needs a transaction
that has finalized and a `tail_ptr` above its start CSN), and that `Fsync` is not forced by something in the
store path.

Then `run_tlc.sh CrashLegacy`. Expected green, and it is the run that makes model defect `M1`'s closure worth
something: if it is red on `Assert_validateInfo` or `NoSpuriousStaleVersion` with a `STALE_VERSION` on a legacy
part, the `StoredRecord` arm of step 2 of task 2 is wrong and `M1` is not closed.

- [ ] **Step 6: Witnesses**

| Property | Witness name | Scenario | Expected |
|---|---|---|---|
| `NoResurrection` | `NoResurrection` | `CrashSynced` | RED: `tryGetCSN` answers unknown instead of rolled back for a tid absent from the log, so an uncommitted creation loads `Active` |
| `LogEntryNeeded` | `LogEntryNeeded` | `CrashSynced` | RED: the `async_loading_jobs` gate removed, so `tail_ptr` advances while covered parts are still loading (calibration row `349c46105730`) |
| `LegacyLoads` | `LegacyLoads` | `CrashLegacy` | RED: the loader treats the legacy record as a parse failure and the part loads rolled back |
| `Assert_IsNonTransactionalDomain` | `Assert_IsNonTransactionalDomain` | `CrashSynced` | RED: the pre-`f5f4635154a0` predicate, which asserts on `DummyTID`, against a tmp-only directory |
| `AckedWriteIsDurable` | `AckedWriteIsDurable` | `CrashSynced` | RED: the acknowledgement moved before the commit point, and the crash then loses a write the client was told was durable |

Every witness runs in `CrashSynced` rather than `Crash`, because the baseline `Crash` stops on F8 first. Say so
in `WITNESSES.md`, with the sentence that a witness run checks only its own property and therefore would not
stop on F8 — the reason is the state space, not the property: `CrashSynced` is smaller and the shapes these
witnesses need do not involve the unsynced window. Strike `AckedWriteIsDurable` from the deferred table.

- [ ] **Step 7: Commit**

```bash
git commit -m "tla(transactions): the restart properties and the truncation durability finding" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 4: `SnapshotCrash` and `NonTxnCrash` {#task-4}

The two pairwise crash rows. Both exist because a subsystem that is correct against a restart on its own can
still be wrong against one while another subsystem acts, and both are deliberately narrow.

**Files:**
- Modify: `utils/tla/transactions/Server.tla` (the `NtBatchDone` hook in `NtBatchStore`)
- Modify: `utils/tla/transactions/Invariants.tla` (`NtBatchDone`)
- Modify: `utils/tla/transactions/MergeTreeTransactions.tla` (`SnapshotCrashNext`, `NonTxnCrashNext`)
- Create: `utils/tla/transactions/MC_SnapshotCrash.{tla,cfg}`, `MC_NonTxnCrash.{tla,cfg}`,
  `MC_NonTxnCrashWitness.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Carried into this task by controller rulings (2026-09-21):** model defect `M3` (the truncation pass captures the
candidate map and the latest CSN once, deletes per znode, erases the local map once, and reports `Done` only
after the captured traversal) and `M17` (a rolled-back transaction may still issue `SET TRANSACTION SNAPSHOT`
through the `ASTTransactionControl` exemption; under `SET_SNAPSHOT_PROTECTS` this is the erase-a-gone-iterator
hazard, so the scenario that models it is `SetSnapshotFixed`). Both rows in `FINDINGS.md` §2 carry the sizing;
close each naming the commit, or send the controller the reason it does not fit before placing it elsewhere.

**Interfaces:**
- Produces: `NtBatchDone`; the two scenario specs.
- Consumes: `PState`, `DurableRecord`, `NoResurrection`, `NoOutdatedLookup`, `StoredRecord`, `RealDisagreement`,
  `ValidateMetadataOK`.

- [ ] **Step 1: `NtBatchDone`**

```tla
\* spec #invariants-conflicts, NtBatchDone. Stated on the step that ends a batch with Done, over the STORED
\* record rather than over mem: the pre-fix defect of upstream ba2ee3239b8d stamped a covered part's removal in
\* memory only, which mem-based clause would not see. Durability of that record is Fsync's business and is what
\* NoResurrection checks after the restart, which is why this scenario runs both properties.
\* The skipped targets are excluded, as the spec's row says: the preflight found them already removed.
NtBatchDoneStep ==
  (sys.nt_batch.active /\ ~sys'.nt_batch.active /\ h'.batch_outcome = "Done") =>
    \A p \in h.batch.targets \ sys.nt_batch.skipped :
      /\ StoredRecord(p)'.rtid = NonTransactionalTID
      /\ StoredRecord(p)'.rcsn = NonTransactionalCSN
      /\ part'[p].lock = EmptyTID
      /\ NonTransactionalTID \in h'.removers[p]
NtBatchDone == [][NtBatchDoneStep]_vars
```

The hook, in `NtBatchStore`'s first disjunct: under `Witness("NtBatchDone")` the store writes `mem` directly and
starts no frame, so nothing reaches the stored record.

```tla
     /\ \/ /\ Witness("NtBatchDone")
           /\ part' = [part EXCEPT ![p].mem = ApplyOp("RemovalTID", NonTransactionalTID, part[p].mem),
                                   ![p].lock = EmptyTID]
           /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.locked = @ \ {p}, !.cursor = @ - 1]]
           /\ UNCHANGED h
        \/ /\ ~Witness("NtBatchDone") /\ ~HasFrame(p, a) /\ ...     \* the existing two disjuncts, unchanged
```

`h.removers[p]` is written by `StorePublishStep`, which this hook skips, so the fourth clause fires as well as
the first two. That is correct and is what makes the witness a one-change witness: the defect is one change and
it breaks the property in several places.

**SANY-check before proceeding.** `StoredRecord(p)'` primes the disk and part variables inside; the error class
if TLC refuses is `Error: Attempted to apply the prime operator to a non-state expression`, and the fix is to
inline the body with `disk'` and `part'` written out, as plan 2's task 4 step 5 already had to consider.

- [ ] **Step 2: `SnapshotCrash`**

```tla
\* the SnapshotCrash scenario (spec matrix): the SetSnapshot scenario + Restart*, Layered disk. Updater+Unknown
\* is added to the matrix row's action set, because the row's own check list names NoOutdatedLookup and
\* UpdFinalizeUnknown is that property's only reachable call site; spec defect S14.
SnapshotCrashNext == SetSnapshotNext \/ RestartNext \/ FaultNext \/ SyncNext \/ UpdaterUnknownNext \/ KeeperFaultNext
SnapshotCrashSpec == Init /\ [][SnapshotCrashNext]_vars
```

`MC_SnapshotCrash.{tla,cfg}`: `Parts = {P1, P2}`, `Tasks = {}`, `CoversDef == [p \in Parts |-> {}]`,
`SNAPSHOT_TARGETS = {33}`, `SET_SNAPSHOT_PROTECTS = TRUE`, `RESTARTS_MAX = 1`, `KEEPER_FAULTS_MAX = 1`,
`DISK_MODE = "Layered"`, `FSYNC_PART_DIRECTORY = TRUE`; properties: the `SetSnapshotFixed` list plus
`NoResurrection`, `NoOutdatedLookup`, `UnknownResolvesByLog`. The view is `SetSnapshotView` plus the fields
`CrashView` added over `MergeView`.

Two constants are deliberately not the baseline and each needs its sentence in the module comment.
`SET_SNAPSHOT_PROTECTS = TRUE` because the baseline value produces finding F2 (`NoPrematureDelete`) on the first
trace and this scenario is not about F2; the fix variant is what lets the restart shapes be reached at all.
`FSYNC_PART_DIRECTORY = TRUE` because the baseline value produces F8, for the same reason. Both are the
"continue past the defect" rule applied to a scenario rather than to a run, and both belong in `STATE_SPACE.md`
as bounds with their arguments.

What this scenario is for, in one sentence to put in the comment: a transaction whose snapshot is below
`tail_ptr` when the server restarts, so that the entries its parts need are gone from the log and
`updateCSNIfNeeded` has to decide their fate without them.

- [ ] **Step 3: `NonTxnCrash`**

The matrix row is `NonTxn + NtMerge* + Restart*`. Two departures, both recorded:

`NtMerge*`, a merge with a null transaction, is **not** implemented here. It is a second copy of the merge task
with the non-transactional batch in place of the per-part enrolment, and the spec's `NtBatchDone` witness names
it only as one producer of the shape; `NtDropCover` is the other and it already exists. Placement: plan 4's task
that adds the mutation executor also adds `NtMutate*` and `NtMerge*`, which share the same shape. Write the row
into `FINDINGS.md` section 2 with that placement.

The action set is trimmed below `NonTxn`'s, because model defect `M15` records that the half of `NonTxn`
carrying the removal batch has **no finishing configuration** and this scenario is that half plus a layered disk
and a restart. `M15`'s own closure paragraph names the remedy: a `Base` action set trimmed to what the batch
actually races rather than the whole client repertoire.

```tla
\* What the batch races: one transaction that inserts, publishes, drops, commits or rolls back, plus the
\* non-transactional queries, the cleanup thread and the restart. No SELECT, no KILL, no SET TRANSACTION
\* SNAPSHOT: none of them writes a part's metadata, and every property this scenario checks is about a part's
\* metadata. The trimming is a bound with that argument, recorded in STATE_SPACE.md, not a simplification.
NtCrashClientNext == \E k \in Sessions :
  \/ Begin(k) \/ CommitBefore(k) \/ CommitCreateCSN(k) \/ CommitReadOnly(k) \/ CommitFlip(k)
  \/ CommitFinalize(k) \/ CommitAck(k) \/ FrameFail(k) \/ Refuse(k)
  \/ RollbackStart(k) \/ RollbackOnException(k) \/ RollbackReturn(k) \/ PublishFlip(k) \/ StmtRollbackDrop(k)
  \/ DropStart(k) \/ DropLock(k) \/ DropOutdate(k)
  \/ (\E p \in Parts : InsertWrite(k, p) \/ InsertPreActive(k, p) \/ PublishStart(k, p) \/ PublishEnrol(k, p)
                       \/ PublishStore(k, p) \/ StmtRollbackMark(k, p)
                       \/ DropEnrol(k, p) \/ DropStore(k, p) \/ CommitStoreCreation(k, p) \/ CommitStoreRemoval(k, p))
  \/ (\E t \in Tids : RollbackCopyLists(k, t) \/ RollbackFinalize(k, t)
                      \/ (\E p \in Parts : RollbackMarkCreated(k, t, p) \/ RollbackOutdateCreated(k, t, p)
                                           \/ RollbackRestore(k, t, p) \/ RollbackUnlock(k, t, p)))

NonTxnCrashNext == NtCrashClientNext \/ StoreNext \/ UpdaterNext \/ NtNext \/ CleanupNext
                   \/ RestartNext \/ FaultNext \/ SyncNext
NonTxnCrashSpec == Init /\ [][NonTxnCrashNext]_vars
```

`MC_NonTxnCrash.{tla,cfg}`: `Sessions = {k1}`, `Parts = {P1, P2, E}`, `Tasks = {}`, `Mutations = {}`,
`CoversDef == [p \in Parts |-> IF p = E THEN {P1, P2} ELSE {}]`, `TID_MAX = 2`, `CSN_MAX = 34`,
`RESTARTS_MAX = 1`, `DISK_MODE = "Layered"`, `FSYNC_PART_DIRECTORY = FALSE`, `OBSOLETE_IS_ROLLED_BACK = TRUE`
(finding F6's fix variant, for the same "continue past the defect" reason as task 4's other constants; the
baseline value is red on `Assert_validateInfo` before the restart is reached). Properties: `NtBatchDone`,
`NoResurrection`, `NoFalseCorruption`, `NtBatchRefusedUnchanged`, `NtRefusalJustified`, `LockConsistent`,
`SingleRemover`, plus `TypeOK` and `Assert_validateInfo`.

Note the `FSYNC_PART_DIRECTORY = FALSE` here is the opposite of `SnapshotCrash`'s, deliberately: `NtBatchDone` is
stated on the stored record and `NoResurrection` on the durable one, and the gap between them is exactly what
this scenario is for. If F8 fires here first, run it a second time with `TRUE` and record both, the same way
task 3 does.

- [ ] **Step 4: Run**

`run_tlc.sh SnapshotCrash`, then `run_tlc.sh NonTxnCrash`, 20-minute cap each. Reduction ladder for
`SnapshotCrash`: `CSN_MAX = 35`, then `Sessions = {k1}` (checking the bound contract for
`Assert_getOldestSnapshot`'s witness, which plan 2 recorded as needing three transactions and the witness
bounds), then `RESTARTS_MAX` stays at 1 and the fsync actions go the way task 2 step 9 item 2 describes. For
`NonTxnCrash`: `CSN_MAX = 33` if two commits still fit, then drop `P2` — and check first whether that removes
`NtBatchRefusedUnchanged`'s subject, which plan 2's reviewer already ruled out once for the same reason.

A red on `NoFalseCorruption` here is the interesting one and must be classified before anything else: after a
restart `deferrable` is recomputed from whether a metadata file exists, so a part that is involved in a
transaction can carry a *deferred* record for the first time in the model's history. That is the shape the
`NoFalseCorruption` witness debt needs, and it is also the shape calibration row `3cc9b0936320` is **open** on:
the spec records that no modelled action produces the "memory holds a CSN the disk does not" transient, and the
restart loader may be the producing action it was waiting for. If it is, close the calibration row in
`FINDINGS.md` and say so; if it is not, say why not.

- [ ] **Step 5: Witnesses**

| Property | Witness name | Scenario | Expected |
|---|---|---|---|
| `NtBatchDone` | `NtBatchDone` | `NonTxnCrash` | RED: the store writes `mem` and skips the record, which is the pre-fix defect of `ba2ee3239b8d` |
| `NoFalseCorruption` | `NoFalseCorruption` | `NonTxnCrash` | RED: the deferred-record and `NonTransactionalCSN` exemptions removed from `ValidateMetadataOK`, on a part whose record the restart deferred |
| `NoResurrection` | `NoResurrection` | `NonTxnCrash` | RED, the same hook as task 3, re-run here because the spec's row names the covered parts of a non-transactional removal |
| `NoOutdatedLookup` | `NoOutdatedLookup` | `SnapshotCrash` | RED, which is what makes spec defect S14's correction worth making |

Strike `NoFalseCorruption` and `NtBatchDone` from `WITNESSES.md`'s deferred table. What stays deferred after this
task: `ActiveSetShape`'s reservation clause (plan 4, `MergeMutation`), `NoAvoidableTermination` (plan 5),
`KillerNotStranded` (plan 5).

- [ ] **Step 6: Commit**

```bash
git commit -m "tla(transactions): the SnapshotCrash and NonTxnCrash scenarios" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 5: Part incarnation (model defect `M37`) {#task-5}

Added during execution (controller ruling of 2026-09-21, after task 3 sized the defect). The permanent
`h.creator[p] = EmptyTID` guard on the four creating actions (`InsertWrite`, `MergeWrite`, `NtInsertWrite`,
`NtDropWrite`, plus the merge-result site) hides real part-name reuse after a restart: `StorageMergeTree`
rebuilds the block-number allocator from the parts it loaded, so a name whose part was physically removed and
that no higher part outlives can be reissued. The shape is reachable in `Crash` at `RESTARTS_MAX = 1`.

**Files:**
- Modify: `utils/tla/transactions/History.tla` (every `Parts`-keyed field of `h`: `removers`, `creator`,
  `payload`, `abandoned`, `content`, `selected`, `batch.targets`, `batch.before`; `HistoryInit`, `HistoryTypeOK`)
- Modify: `utils/tla/transactions/Server.tla` (the four guarded creators and the merge-result site; a part
  incarnation counter bumped at each creation of a name)
- Modify: `utils/tla/transactions/Invariants.tla` (`OracleVisible` and the seven properties reading it say which
  incarnation they mean)
- Modify: every `MC_*.tla` `VIEW` that projects a `Parts`-keyed field (thirty-one modules at the time of the ruling)
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md` (close `M37` naming the commit)

**Interfaces:**
- Produces: the incarnation-keyed history; `Incarnation(p)`.
- Consumes: everything task 3 left; task 6 re-measures every scenario and witness row once, after this task.

- [ ] **Step 1: State the sound key.** Histories are keyed by `(name, incarnation)`; the incarnation of a name is
  the number of creations of that name so far. The oracle's visibility of a fragment is the visibility of its
  own incarnation's creator; a later incarnation is a new fragment.
- [ ] **Step 2: Failing test first.** Drop the permanent guard in `InsertWrite` only and run `Crash` at the matrix
  bounds under `timeout 1200`: expected RED (a property confuses the two incarnations) — commit the trace as the
  `M37` evidence.
- [ ] **Step 3: Rekey `h` and the properties**, restore the creators without the guard, rewrite the `VIEW`s.
- [ ] **Step 4: Run** `Schema`, `BaseSmall`, `Base`, `Merge`, `Crash`, `CrashUnsynced` at their committed bounds;
  every one must return to green with its count recorded; the `Crash` count is expected to GROW (reuse is now
  reachable) — record the new figure, not a bound change.
- [ ] **Step 5: Commit**, close `M37` in `FINDINGS.md` §2, and hand task 6 the list of modules whose witness rows
  must be re-run.

### Task 6: Budget, witness sweep, documents and debts {#task-6}

**Files:**
- Modify: every `MC_*.cfg` whose bounds moved
- Modify: `utils/tla/transactions/README.md`, `STATE_SPACE.md`, `WITNESSES.md`, `FINDINGS.md`

- [ ] **Step 1: The full run table**

Run, one at a time, `-workers auto`, with `tmp/tla/STOP` removed first: `Schema`, `BaseSmall`, `Base`,
`SetSnapshot`, `SetSnapshotFixed`, `Merge`, `NonTxnInsert`, `NonTxnDrop`, `NonTxnFixed`, `Keeper`,
`KeeperUnknownWait`, `Crash`, `CrashSynced`, `CrashLegacy`, `SnapshotCrash`, `NonTxnCrash`. Record scenario,
bounds, states generated, distinct states, time and result. `Crash` is the one row whose result is
`RED (expected, F8)`. The nine scenarios of plans 1 and 2 are re-run because tasks 1, 2 and 3 all changed
definitions they use (`Holders`, the rollback machine's actor, `StoredRecord`, `ProcessDown`, the invariant
preamble), and a count that moved without an explanation in `STATE_SPACE.md` is a finding.

- [ ] **Step 2: The full witness sweep**

Every non-deferred row of `WITNESSES.md`, in its own scenario, including the rows plans 1 and 2 measured before
this plan changed shared definitions, and both halves of every two-change witness. Any row that is not `RED` is
a finding, not a rerun: classify it (class (b) or class (d)) and fix the hook or the property before the task
ends. Budget about 45 minutes; the sweep is dominated by `Assert_validateInfo_removal`, which plan 2 measured at
53 million distinct states.

- [ ] **Step 3: The bound-contract debts**

Close or place each of these, in `FINDINGS.md` section 2a:

- `B1` to `B5`, inherited from plan 2, are that plan's task 5's business and are not reopened here unless a run
  of step 1 changed one of their numbers.
- `B6`, the `Keeper` reductions of task 1 step 9: every witness that is green or does not finish at the
  exhaustive bounds, with `MC_KeeperWitness` as where it is shown and the count it reached.
- `B7`, the `Crash` reductions of task 2 step 9: the same, with `MC_CrashWitness`, and in particular the two
  coverage-tree shapes the spec's `Crash` paragraph names if the merge task was dropped.
- `B8`, `KEEPER_FAULTS_MAX = 1`: a behaviour with both a session expiry and a lost commit does not exist. Measure
  `MC_KeeperWitness` at `KEEPER_FAULTS_MAX = 2` once, with a 20-minute cap, and record whether it fits; if it
  does not, the row stays as a debt with plan 5's calibration task as its destination.

- [ ] **Step 4: The four documents**

`STATE_SPACE.md`: a section per new scenario, each with where the states come from, the reductions applied with
their measured effect, the bounds with their argument, and the final counts. Add to the `Base` history the two
lines tasks 1 and 2 owe (the rollback-machine extraction, expected unchanged; `ProcessDown` replacing
`NoexceptFrameDown`, expected down).

`WITNESSES.md`: the new rows, the struck deferred rows (`AckedWriteIsDurable`, `NoFalseCorruption`,
`NtBatchDone`), the still-deferred ones with their destinations, the `NoOutdatedLookup` row rewritten from
`GREEN (vacuous)` to its `Keeper` result, and a scenarios-used section naming the six new configurations and
their two bound sets each.

`FINDINGS.md`: F8 complete with its trace, the C++ TODO it quotes, its proposed fix and its model variant; spec
defects S14 to S17 in section 3; model defect M17 in section 2 with its placement; the closures of `M1`, `M3` and
`M5` marked as closed by this plan with the task that did it; the `NtMerge*` row with plan 4 as its placement;
and, if task 4 step 4 settled it, the calibration row `3cc9b0936320` moved from open to closed with the
producing action named.

`README.md`: extend the code map with one row per action this plan defined, columns
`Action | C++ file | Function | Step boundary`, each checked against the function; extend the run table with
step 1's rows; add the new refinement parameters (the single-step load store, the monotone `local_tid_counter`,
the shared Keeper fault budget, the free `Fsync` and `FsyncDir`) to the refinement-parameters section; add F8 to
the expected-red findings section; and move `Crash`, `Restart*`, `CommitUnknown` and the unknown-state rows out
of the "deferred to plan 3" table.

- [ ] **Step 5: Commit**

```bash
git commit -m "tla(transactions): state-space budget, witness sweep and documents for plan 3" -- utils/tla/transactions docs/superpowers/plans
```

---

## Self-review {#self-review}

**Spec coverage of the four matrix rows this plan owns.** `Keeper` names `Base` + `Merge*` + `Updater+GC` +
`Updater+Unknown`, Keeper faults and both wait modes, and checks `UnknownResolvesByLog`, `NoOutdatedLookup` and
"the two-list race": task 1 has all of it, in two configurations. `Crash` names `Base` + `Merge*` + `Cleanup*` +
`Updater+GC` + `Restart*` on a `Layered` disk, both `FSYNC_PART_DIRECTORY` values and `LEGACY_PARTS` on and off,
and checks `NoResurrection`, `LogEntryNeeded`, `AckedWriteIsDurable` across restart, `LegacyLoads`,
`Assert_IsNonTransactionalDomain`, `NoFalseCorruption` after restart and `RolledBackEventuallyDeleted` under
fairness: tasks 2 and 3 have every one except the liveness row, which is placed on plan 5's liveness task as
spec defect S16 with the reason. `SnapshotCrash` and `NonTxnCrash` are task 4, the first with the action-set
correction S14 records, the second without `NtMerge*`, which is placed on plan 4 with the argument that
`NtDropCover` already produces the batch shape the row's property needs.

**Expected reds, and what continues the search past each.** One is expected: `LogEntryNeeded` in `Crash`
(finding F8, the unsynced metadata record whose log entry is truncated), with the code's own TODO as the
proposed fix and `FSYNC_PART_DIRECTORY = TRUE` as the model variant that `MC_CrashSynced` runs. Two are
scenario-level applications of the same rule rather than new findings: `SnapshotCrash` runs with
`SET_SNAPSHOT_PROTECTS = TRUE` because the baseline value stops on plan 2's F2, and `NonTxnCrash` runs with
`OBSOLETE_IS_ROLLED_BACK = TRUE` because the baseline value stops on plan 2's F6. Two more are named as things
to classify rather than predicted: `NoOutdatedLookup` in `Keeper`, whose consequence spec defect S17 records
(the rest of the pass's list is stranded), and `NoFalseCorruption` in `NonTxnCrash`, which may be the producing
action calibration row `3cc9b0936320` has been open on since revision 7. `NoAvoidableTermination` is checked in
every scenario of this plan and is expected green: the only producer of `down_cause /= "None"` here is
`ProcessDown("Other")`, which needs a frame parked in `Error` with a `noexcept` owner, and the only such frame
is one whose `validateInfo` failed, which `Assert_validateInfo` reports first. If it fires, it is a finding with
the `Retry` policy of Altinity PR 2396 as its fix, and that policy is plan 5's; the minimal variant this plan
would add is named in the entry rather than built.

**Placeholder scan.** No step says "similar to task N", "add appropriate guards" or "TBD". Two steps name a
transcription hazard instead of hiding it: task 2 step 6's deliberately wrong `UNCHANGED` list, with the error
class and the correction, and task 2 step 5's four numbered points. Seven steps are marked **SANY-check before
proceeding** with the error class to expect. Every reduction ladder names its items in order with what each
costs, and every item that costs coverage names the debt row it becomes.

**Type consistency.** New fields are `tlog.unknown_ready` and `tlog.finalizing` (task 1), `sys.load_queue`,
`sys.outdated_queue`, `sys.mut_queue` and `h.payload` (task 2), each with its `TypeOK` clause and its `Init`
value named in the same step. `TxnPcs` gains `"CommitUnknown"` (task 1); `Holders` gains `Upd` and `rb_driver`
gets its own set `RbDrivers` (task 1); `Pins` deliberately gains nothing. No new constants are declared: this
plan uses `RESTARTS_MAX`, `KEEPER_FAULTS_MAX`, `DISK_MODE`, `FSYNC_PART_DIRECTORY`, `LEGACY_PARTS` and
`WAIT_MODE`, all declared in plan 1 and set to their inert values everywhere until now, which is why no existing
`.cfg` has to change. Five steps change definitions that earlier scenarios use (the rollback machine's actor and
`CommitFinalizeEffect` in task 1, `StoredRecord` and `ProcessDown` and the invariant preamble in task 2), and
each is followed by a re-run with the expected count stated: unchanged for the extractions, moved and explained
for `ProcessDown`.
