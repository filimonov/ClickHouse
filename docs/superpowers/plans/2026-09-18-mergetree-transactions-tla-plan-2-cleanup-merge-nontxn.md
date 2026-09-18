---
description: 'Implementation plan 2 of the TLA+ model of MergeTree transactions: SET TRANSACTION SNAPSHOT, the transaction-log truncation of the updating thread, the cleanup thread, background merges with the task set and the covering relation, and the non-transactional query set with its removal batch. Adds the scenarios SetSnapshot, Merge and NonTxn, pays four of the five witness debts plan 1 deferred, and records the first expected-red baseline finding with the C++ fix it proposes and the model variant that encodes it.'
sidebar_label: 'TLA+ transactions, plan 2'
sidebar_position: 22
slug: /superpowers/plans/mergetree-transactions-tla-plan-2-cleanup-merge-nontxn
title: 'MergeTree transactions TLA+ model, plan 2: snapshots, cleanup, merges and non-transactional queries'
doc_type: 'plan'
---

# MergeTree transactions TLA+ model, plan 2: snapshots, cleanup, merges and non-transactional queries {#tla-plan-2}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

Revision 1, 2026-09-18. Plan 1 installed the foundation modules, the `Base` scenario at the matrix bounds
(`Sessions = {k1, k2}`, `Parts = {P1, P2}`, `TID_MAX = 3`, `CSN_MAX = 36`, 28.55M distinct states in about four
minutes), the witness runner, and the three documents `STATE_SPACE.md`, `WITNESSES.md` and `FINDINGS.md`. It left
`Server.tla` with forty-nine stub definitions `== FALSE` and five witness debts. This plan replaces eleven of
those stubs and pays four of the five debts.

**Goal:** Three more scenarios green (or red exactly where the spec says a finding is expected), each with its own
`MC_<Scenario>.{tla,cfg}`, its own view argument and its own state-space budget: `SetSnapshot` (`SET TRANSACTION
SNAPSHOT` plus log truncation plus the cleanup thread), `Merge` (background merges over a covering relation with
the task set and reservations) and `NonTxn` (non-transactional insert, the non-transactional removal batch, and
the empty covering part of a non-transactional `DROP PARTITION`). Every property those scenarios add has a
witness that fires; the witness debts `NoDoubleRead`, `ActiveSetShape` and `Assert_getOldestSnapshot` are paid.

**Architecture:** Unchanged from plan 1. `MergeTreeTransactions.tla` keeps the action groups; each new scenario
module composes its own `Next` from them (`SetSnapshotNext`, `MergeNext`, `NonTxnNext`) so that an action added
here is not enabled in `Base`. Actions of plans 3 to 5 stay stubs. Witnesses stay `Witness("Name")` guards inside
the actions, selected by `WITNESS_NAME`.

**Tech Stack:** TLA+ (TLA+2), TLC from `tmp/tla2tools.jar` (Java 21), bash.

**Spec:** `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` (revision 11). The spec is the
authority on what an action means; where a row of the spec disagrees with the C++ at the baseline commit, the
model follows the C++ and the task records the disagreement in `FINDINGS.md` section 3. Five such disagreements
are already known and are named in the tasks that hit them (S3 to S7 below).

## Global Constraints {#global-constraints}

- Baseline C++ is upstream master `2c24b6b9291e`, checked out in this worktree; the code map cites files and
  functions of that tree, and every row is checked by opening the function before it is written down.
- All model files live under `utils/tla/transactions/`; temporary files under `tmp/` (never `/tmp`).
- Every TLC run uses its own `-metadir` under `tmp/tla/<run>/states` and its log goes to `tmp/tla/<run>/tlc.log`.
  Two runs must never share a state directory.
- A TLC run that passes 30 million distinct states or 45 minutes is a defect of the model or of the bounds, not
  something to wait for: kill it, record the last `Progress` line, reduce under the bound contract, and say in
  `STATE_SPACE.md` which bound moved and why every witness of every property that scenario checks is still red.
- Commit after every task with `git commit -- <paths>`; never `git add -A`; do not push. Commit messages end with
  the attribution lines from the session's system reminder.
- Witness contract (spec, section "Invariants and properties"): `witness.sh Scenario Property [WitnessName]`
  checks only that property with `WITNESS_NAME` set and exits zero only when TLC reports a violation of it. A
  two-change witness declares halves `<WitnessName>_only1` and `<WitnessName>_only2` and both halves must be
  green.
- Bound contract (spec): a scenario's bounds may be reduced from the matrix values only while every witness of
  every property it checks is still red under the reduced bounds; `STATE_SPACE.md` records the smallest bounds at
  which that held.
- The four counterexample classes and what to do with each: (a) TLC parse or type error: fix the model; (b) a
  property red on the baseline because the property or the action misreads the code: fix the model and note in
  the report the C++ line that decided; (c) a property red because the C++ has the defect: do not change the
  model, add the trace to `FINDINGS.md` with the action sequence and the C++ call sequence; (d) a witness that
  does not fire: fix the hook or the property.
- **A blocking baseline defect gets a fix variant.** TLC stops at the first violation, so a real code defect
  found on a baseline run masks every counterexample behind it. When class (c) fires, the task additionally
  (1) writes a proposed C++ fix into the `FINDINGS.md` entry, naming file, function and what changes, realistic
  enough to be a pull request; (2) adds a model variant, a declared constant (never a `Witness`) that encodes
  that fix; and (3) runs the scenario again with the variant on, so the search continues past the defect and the
  scenario's remaining properties are checked. Both runs are recorded. This is the rule the `SetSnapshot`
  scenario exercises in task 1.
- No placeholders. Where this plan writes TLA+, the implementer transcribes it; where a step says
  **SANY-check before proceeding**, run `run_tlc.sh Schema` (which parses the whole module chain) before writing
  the next step, and expect the named error class if it was written wrong.

## What this plan replaces {#stubs-replaced}

Eleven of the forty-nine `== FALSE` stubs in `Server.tla` (lines 563 to 611 of the plan-1 tree), in the order the
tasks reach them:

| Stub | Task |
|---|---|
| `SetSnapshot(k, c)` (`Server.tla:151`, not in the stub block) | 1 |
| `UpdRemoveOldEntriesSetTail` | 1 |
| `UpdRemoveOldEntriesDelete(c)` | 1 |
| `CleanupGrab(p)` | 2 |
| `CleanupValidate(p)` | 2 |
| `CleanupDeleteOk(p)` | 2 |
| `CleanupDeleteFail(p)` | 2 |
| `MergeBegin(i)` | 3 |
| `MergeSelect(i)` | 3 |
| `MergeWrite(i)` | 3 |
| `MergeRename(i)` | 3 |
| `MergeFail(i)` | 3 |
| `NtInsert(p)` | 4 |
| `NtBatchStart(B)` | 4 (removed rather than filled, see task 4 step 2) |
| `NtBatchPreflight(p)` | 4 |
| `NtBatchLock(p)` | 4 |
| `NtBatchStore(p)` | 4 |
| `NtBatchEnd` | 4 |
| `NtDropCover` | 4 |

Plus new actions the spec's rows imply but plan 1 did not declare: the merge task's publication and commit steps
(`MergePublish*`, `MergeCommit*`, task 3) and the two extra steps of the non-transactional drop (task 4). The
remaining thirty-odd stubs belong to plans 3 to 5 and stay `FALSE`.

## Spec rows this plan does not follow {#spec-vs-code}

Found while reading the C++ for the code map. Each is recorded in `FINDINGS.md` section 3 by the task that hits
it, and the model row follows the code.

| Id | Spec row | The code | Task |
|---|---|---|---|
| S3 | `SetSnapshot` scenario's green set names `NoLostVisibleData` | the `SetSnapshot` defect that makes `NoPrematureDelete` expected-red also deletes the part, so `NoLostVisibleData` is red on the same trace. It is green only under the fix variant | 1 |
| S4 | `NoOutdatedLookup` is in the `SetSnapshot` scenario's green set | `assertTIDIsNotOutdated` has exactly two call sites, `TransactionLog::getCSNAndAssert` (which has no caller in the baseline tree) and `tryFinalizeUnknownStateTransactions` (`TransactionLog.cpp:387`). `SetSnapshot` enables neither, so the property is vacuously green there; its real check is plan 3's `Keeper` | 1 |
| S5 | `MergePublish*`: "`reserved[i]` released at `PublishFlip`" | `CurrentlyMergingPartsTagger::finalize` runs from `merge_mutate_entry->finalize()` at the end of `MergePlainMergeTreeTask::finish` (`MergePlainMergeTreeTask.cpp:200`), after `transaction.commit()` and after the merge transaction's own `TransactionLog::commitTransaction`. The reservation outlives the publication and the commit | 3 |
| S6 | `MergeSelect`: sources "visible to the merge transaction" | `constructPreconditionsPredicate` (`Compaction/PartsCollectors/MergeTreePartsCollector.cpp:88`) calls `isVisible(tx->getSnapshot(), Tx::EmptyTID)`, with the empty TID, not the merge transaction's own; and it adds a guard the row omits, `isRemovalTIDLocked()` at `:91`, which excludes a part another transaction has locked for removal | 3 |
| S7 | `NtBatchStore(p)` walks `targets` forward, skipping `skipped` | `NonTransactionalRemovalLocks::store` (`MergeTreeTransaction.cpp:150`) drains `locked_parts` from the back, so the store order is the reverse of the lock order, and a skipped target was never in the list at all | 4 |

---

### Task 1: `SET TRANSACTION SNAPSHOT`, log truncation, and the `SetSnapshot` scenario {#task-1}

The first task because both later scenarios enable the cleanup thread, and the cleanup thread's safety depends on
`getOldestSnapshot`, which is what `SET TRANSACTION SNAPSHOT` breaks. Finding the defect here keeps it out of the
`Merge` and `NonTxn` traces.

**Files:**
- Modify: `utils/tla/transactions/Types.tla` (the constants `SNAPSHOT_TARGETS` and `SET_SNAPSHOT_PROTECTS`)
- Modify: `utils/tla/transactions/Server.tla` (`SetSnapshot`, `UpdRemoveOldEntriesSetTail`,
  `UpdRemoveOldEntriesDelete`, `Begin`'s capture of `h.content`)
- Modify: `utils/tla/transactions/Invariants.tla` (`Assert_getOldestSnapshot`, new
  `Assert_TailPtrNotRegressing`, new `NoLostVisibleData`, new `VisibleFrags`)
- Modify: `utils/tla/transactions/MergeTreeTransactions.tla` (`SetSnapshotNext`)
- Modify: `utils/tla/transactions/MC_Base.cfg`, `MC_BaseSmall.cfg`, `MC_Schema.cfg` (the two new constants)
- Create: `utils/tla/transactions/MC_SetSnapshot.{tla,cfg}`, `MC_SetSnapshotFixed.{tla,cfg}`
- Modify: `utils/tla/transactions/FINDINGS.md`, `WITNESSES.md`, `STATE_SPACE.md`

**Interfaces:**
- Produces `SNAPSHOT_TARGETS`, the set of CSNs `SET TRANSACTION SNAPSHOT` may name in a scenario (empty where the
  action is disabled); `SET_SNAPSHOT_PROTECTS`, the boolean model variant of the proposed C++ fix;
  `VisibleFrags(t)`, the fragment set visible to `t` at its current snapshot, consumed by task 2's properties and
  by `NoLostVisibleData`; `Assert_TailPtrNotRegressing`, consumed by plan 3.
- Consumes: `OldestSnapshot`, `LookupCsn` (`Parts.tla`), `KeeperWithTail`, `KeeperRemoved` (`Keeper.tla`),
  `Frags` (`Invariants.tla`).

- [ ] **Step 1: Declare the two new constants**

In `Types.tla`, add `SNAPSHOT_TARGETS` and `SET_SNAPSHOT_PROTECTS` to the `CONSTANTS` list and add, next to the
existing `ASSUME`s:

```tla
\* SET TRANSACTION SNAPSHOT refuses a reserved CSN other than these two
\* (InterpreterTransactionControlQuery::executeSetSnapshot, src/Interpreters/InterpreterTransactionControlQuery.cpp:144).
\* The set is a scenario bound, not a refinement: the code accepts any CSN above MaxReservedCSN.
ASSUME SNAPSHOT_TARGETS \subseteq (RealCSNs \cup {NonTransactionalCSN, EverythingVisibleCSN})
ASSUME SET_SNAPSHOT_PROTECTS \in BOOLEAN
```

`RealCSNs` and the two reserved values are already defined above the `ASSUME` block, so the declaration order is
fine. Add `SNAPSHOT_TARGETS = {}` and `SET_SNAPSHOT_PROTECTS = FALSE` to the `CONSTANTS` section of
`MC_Base.cfg`, `MC_BaseSmall.cfg` and `MC_Schema.cfg`, which otherwise stop parsing.

**SANY-check before proceeding.** `run_tlc.sh Schema` must stay green. The error class if a `.cfg` was missed is
`Error: Constant parameter SNAPSHOT_TARGETS is not assigned a value`.

- [ ] **Step 2: `SetSnapshot`**

Replace `SetSnapshot(k, c) == FALSE` (`Server.tla:151`) with the following. `MergeTreeTransaction::setSnapshot`
(`src/Interpreters/MergeTreeTransaction.cpp:52`) stores the new value into `snapshot` and touches nothing else;
in particular it leaves the `snapshots_in_use` entry, and therefore `getOldestSnapshot`, at the value
`beginTransaction` inserted. That is the whole action, and the whole defect.

```tla
\* executeSetSnapshot (src/Interpreters/InterpreterTransactionControlQuery.cpp:138) then
\* MergeTreeTransaction::setSnapshot (src/Interpreters/MergeTreeTransaction.cpp:52): one relaxed store into
\* `snapshot`. `protected_snapshot` and the snapshots_in_use entry are deliberately left alone, which is the
\* behaviour NoPrematureDelete is expected to catch; SET_SNAPSHOT_PROTECTS is the proposed fix (FINDINGS F2).
SetSnapshot(k, c) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].state = "Running"
  /\ c \in SNAPSHOT_TARGETS
  /\ LET t == Cur(k)
         moves == SET_SNAPSHOT_PROTECTS \/ Witness("Assert_getOldestSnapshot") IN
     /\ (SET_SNAPSHOT_PROTECTS => c >= tlog.tail_ptr)
     /\ txn' = [txn EXCEPT ![t].snapshot = c, ![t].protected_snapshot = IF moves THEN c ELSE @]
     /\ tlog' = [tlog EXCEPT !.snapshots_in_use[t] = IF moves THEN c ELSE @]
     /\ h' = [h EXCEPT !.content[t] = Frags({ r \in Parts : part[r].pstate \in {"Active", "Outdated"}
                                                           /\ OracleVisible(r, c, t) })]
  /\ UNCHANGED <<zk, disk, mdisk, part, sys, client, stmt, mut, task>>
```

`Frags` lives in `Invariants.tla`, which extends `Server.tla`, so it cannot be used here. Move the one-line
definition `Frags(V) == { <<q, part[q].payload.ver>> : q \in Expand(V) }` from `Invariants.tla` up into
`Parts.tla` (it reads only `part` and `Expand`) and leave `Proj` where it is. That is the whole change to
`Invariants.tla`'s head.

- [ ] **Step 3: `h.content` becomes a fragment set**

`Begin` (`Server.tla:139`) currently captures `h.content[t]` as a set of `<<part, version>>` pairs over the
visible *roots*. `NoLostVisibleData` must survive a merge, which replaces a root by a covering root without
losing any fragment, so the captured set has to be the expanded one. In `Begin`, replace the `h'` conjunct with

```tla
     /\ h' = [h EXCEPT !.content[t] = Frags({ r \in Parts : part[r].pstate \in {"Active", "Outdated"}
                                                           /\ OracleVisible(r, s, t) })]
```

which is the same expression `SetSnapshot` uses with `s` for `c`. `Base` reads `h.content` nowhere and
`BaseView` drops it, so `Base` is unaffected; step 8 confirms that with a re-run.

- [ ] **Step 4: The two truncation actions**

`TransactionLog::removeOldEntries` (`src/Interpreters/TransactionLog.cpp:284`) is two model actions. The first
is everything up to and including `tail_ptr.store`: the two gates (`isServerCompletelyStarted`, and
`asyncTablesLoadingJobNumber() != 0` only while `updated_tail_ptr` is false), the read of the `tail_ptr` znode,
`getOldestSnapshot`, the `LOGICAL_ERROR` when the new value is below the old one, the early return when they are
equal, and the `set` of the znode. The second is one loop iteration: one `tryRemove` and one `tid_to_csn.erase`.

```tla
\* removeOldEntries up to tail_ptr.store (src/Interpreters/TransactionLog.cpp:284-316).
\* The guard is `new /= old`, not `new > old`: the regressing case is what Assert_TailPtrNotRegressing catches,
\* and a guard that excluded it would make the property vacuous.
UpdRemoveOldEntriesSetTail ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ zk.session = "Alive"
  /\ sys.completely_started
  /\ (tlog.updated_tail_ptr \/ sys.async_loading_jobs = 0)
  /\ LET nt == IF Witness("NoOutdatedLookup") THEN tlog.latest_snapshot ELSE OldestSnapshot IN
     /\ nt /= zk.tail
     /\ zk' = KeeperWithTail(nt)
     /\ tlog' = [tlog EXCEPT !.tail_ptr = nt, !.updated_tail_ptr = TRUE]
  /\ UNCHANGED <<disk, mdisk, h, part, txn, sys, client, stmt, mut, task>>

\* removeOldEntries, one iteration of the removal loop (src/Interpreters/TransactionLog.cpp:319-341).
\* The loop walks the loaded map tid_to_csn, not the znode list, so an entry the updater has not loaded is not a
\* candidate; it skips an entry whose tid.start_csn is at or above the new tail, and always keeps the entry whose
\* csn is the latest loaded one. ZNONODE counts as removed, which is why no `c \in DOMAIN zk.log` guard appears.
UpdRemoveOldEntriesDelete(c) ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ zk.session = "Alive"
  /\ sys.completely_started /\ tlog.updated_tail_ptr
  /\ c /= tlog.latest_snapshot
  /\ \E t \in Tids :
     /\ tlog.tid_to_csn[t] = c
     /\ tlog.tid_start[t] < tlog.tail_ptr
     /\ zk' = KeeperRemoved(c)
     /\ tlog' = [tlog EXCEPT !.tid_to_csn[t] = UnknownCSN]
     /\ h' = [h EXCEPT !.truncated = @ \cup {t}]
  /\ UNCHANGED <<disk, mdisk, part, txn, sys, client, stmt, mut, task>>
```

**SANY-check before proceeding.** The `\E t \in Tids :` block binds three primed variables inside the quantifier,
which is legal TLA+ but is the shape TLC most often rejects in a `.cfg` with a `VIEW`; the error class is
`Error: In evaluation, the identifier tlog is either undefined or not an operator` if the `\E` was accidentally
closed before `tlog'`. Two modelling decisions to write as comments next to the actions, because the next plan
needs them:

1. The updating thread's program counter is not used by either action. In the C++ the iteration is
   `loadNewEntries(); removeOldEntries(); tryFinalizeUnknownStateTransactions();` and each of the two actions
   above carries a self-sufficient guard, so a `pc` would only serialize a thread that is already serial and
   would double the states. `UpdLoadEntriesMap` keeps its `pc` because its two halves are not self-sufficient.
2. `removeOldEntries` snapshots `tid_to_csn` and `latest_snapshot` once and then loops; the model re-reads them
   per iteration. `latest_snapshot` only grows, so the model can delete an entry the C++ would have kept as "the
   latest one we fetched". That widens the behaviour set, which is safe for every property of plan 2 (the only
   property that reads `h.truncated` is `LogEntryNeeded`, which is plan 3's). **Placement: plan 3, the task that
   enables `Crash`, either narrows this to a snapshotted set or argues the widening is still sound for
   `LogEntryNeeded`.** Record the same sentence in `FINDINGS.md` section 2 as a model defect with that placement.

- [ ] **Step 5: `Assert_getOldestSnapshot`, `Assert_TailPtrNotRegressing`, `NoLostVisibleData`**

`TransactionLog::getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:677`) holds two `chassert`s:
`running_list.size() == snapshots_in_use.size()` and `snapshots_in_use.front() <= *++snapshots_in_use.begin()`.
The model's `Assert_getOldestSnapshot` transcribes the first and, so far, only a proxy for the second. The
sortedness clause is expressible, because `snapshots_in_use` is ordered by insertion and `Begin` draws tids from
a monotone counter while `latest_snapshot` never decreases: the list order is the tid order. In
`Invariants.tla`, replace `Assert_getOldestSnapshot` with

```tla
\* TransactionLog::getOldestSnapshot, src/Interpreters/TransactionLog.cpp:677-686: the running list and the
\* snapshot bag have the same members, each entry is the value beginTransaction inserted, and the bag is sorted.
\* snapshots_in_use is a list in insertion order and Begin draws tids from a monotone counter while
\* latest_snapshot never decreases, so "sorted" is "non-decreasing in the tid order". Under SET_SNAPSHOT_PROTECTS
\* the proposed fix re-inserts the entry at its sorted position, so the tid order is no longer the list order and
\* the clause does not apply; the C++ assertion still does.
Assert_getOldestSnapshot ==
  /\ tlog.running_list = { t \in Tids : tlog.snapshots_in_use[t] /= UnknownCSN }
  /\ \A t \in tlog.running_list : tlog.snapshots_in_use[t] = txn[t].protected_snapshot
  /\ ~SET_SNAPSHOT_PROTECTS =>
       \A t1, t2 \in tlog.running_list : t1 < t2 => tlog.snapshots_in_use[t1] <= tlog.snapshots_in_use[t2]

\* removeOldEntries, src/Interpreters/TransactionLog.cpp:312-314: "Got unexpected tail_ptr {}, oldest snapshot is
\* {}, it's a bug". A LOGICAL_ERROR on a modelled path is an invariant, not a precondition (spec, "Actions").
TailPtrNotRegressingStep == UpdRemoveOldEntriesSetTail => OldestSnapshot >= zk.tail
Assert_TailPtrNotRegressing == [][TailPtrNotRegressingStep]_vars

\* spec #invariants-isolation, NoLostVisibleData. The fragments visible to a running transaction at its own
\* snapshot never shrink, except by its own drops; SetSnapshot recaptures h.content, so a transaction that
\* deliberately reads an older snapshot is judged against that snapshot's content, not against its first one.
VisibleFrags(t) == Frags({ r \in Parts : part[r].pstate \in {"Active", "Outdated"}
                                        /\ OracleVisible(r, txn[t].snapshot, t) })
NoLostVisibleDataStep ==
  \A t \in Tids : (txn[t].state = "Running" /\ txn[t].snapshot /= EverythingVisibleCSN) =>
    (h.content[t] \ Frags(h.removing[t])) \subseteq VisibleFrags(t)'
NoLostVisibleData == [][NoLostVisibleDataStep]_vars
```

`VisibleFrags(t)'` primes every variable inside the definition, which is what the property needs.
**SANY-check before proceeding**; if TLC refuses to prime an operator application, the error class is
`Error: Attempted to apply the prime operator to a non-state expression`, and the fix is to inline the body with
`part'`, `txn'` and `h'` written out.

Two notes to write next to `NoLostVisibleData`: it is stated over all running transactions rather than over "a
step that is not an action of `t`", because the two exemptions the spec's row gives for `t`'s own steps
(`h.removing[t]` and the `SetSnapshot` recapture) already cover every way `t` can shrink its own view; and a
merge transaction, which never reads, has `h.content = {}` and so is vacuously covered.

- [ ] **Step 6: The scenario module and its two configurations**

Add to `MergeTreeTransactions.tla`, next to `BaseNext`:

```tla
\* the SetSnapshot scenario (spec matrix): Base + SetSnapshot + Cleanup* + Updater+GC
SetSnapshotNext == BaseNext \/ UpdaterGCNext \/ CleanupNext
SetSnapshotSpec == Init /\ [][SetSnapshotNext]_vars
```

`CleanupNext` is still all-`FALSE` until task 2; the scenario is written now and is first *run* in task 2's
step 4. Task 1 runs it without the cleanup group, which is enough to reach `NoPrematureDelete`'s sibling
`Assert_getOldestSnapshot` and both truncation actions, and which is what step 7 measures.

`MC_SetSnapshot.tla`: `EXTENDS MergeTreeTransactions`, `CoversDef == [p \in Parts |-> {}]`,
`SymSessions == Permutations(Sessions)`, and a view `SetSnapshotView` built from `BaseView` by **adding**
`h.content`, `h.truncated`, `tlog.tail_ptr`, `tlog.updated_tail_ptr`, `zk.tail` and `sys.cleanup_pc`,
`sys.cleanup_part` (task 2 adds the last two to `SysRecord`; include them from the start so the view does not
change again). Copy plan 1's view comment and rewrite the three justifications for this scenario: the fields left
out are still those no enabled action and no checked property reads, those that are a function of the kept ones,
and those no enabled action writes; `h.content` moves out of the first group because `NoLostVisibleData` reads
it, `tail_ptr` and `zk.tail` move out of the third because the truncation actions write them.

`MC_SetSnapshot.cfg`: `SPECIFICATION SetSnapshotSpec`, `SYMMETRY SymSessions`, `VIEW SetSnapshotView`,

```
INVARIANTS TypeOK RollbackNoLeak AckedWriteIsDurable ErrorIsAbsent SingleRemover LockConsistent ActiveSetShape Assert_validateInfo Assert_isVisible_fast Assert_getOldestSnapshot NoAvoidableTermination NoSpuriousStaleVersion KillerNotStranded
PROPERTIES RollbackRestores FlipAfterStores StableRead ReadYourWrites NoUncommittedRead NoFutureRead NoLostRead NoDoubleRead Atomicity Assert_TailPtrNotRegressing
```

with the `Base` constants plus `SNAPSHOT_TARGETS = {33}` (`FirstCSN`, the oldest real CSN the scenario can name;
a snapshot at or above the current one cannot make a part prematurely removable, so this one value is the whole
interesting range) and `SET_SNAPSHOT_PROTECTS = FALSE`. `NoPrematureDelete`, `PinnedNotDeleted` and
`NoLostVisibleData` are **not** in this configuration yet: they need the cleanup thread, and task 2 adds them.

`MC_SetSnapshotFixed.{tla,cfg}`: identical, module renamed, `SET_SNAPSHOT_PROTECTS = TRUE`.

- [ ] **Step 7: Run and measure**

Run `run_tlc.sh SetSnapshot` with a 20-minute cap. Expected: green. `SNAPSHOT_TARGETS` of one value adds one
enabled action per running transaction per state, and the two truncation actions add a small number of
`tid_to_csn` and `tail_ptr` shapes, so the expected order of magnitude is `Base`'s 28.6M times a small constant.
**If it passes 30M distinct states or 15 minutes, stop and reduce, in this order**, recording each step in
`STATE_SPACE.md` with its measured effect:

1. `SNAPSHOT_TARGETS` is already minimal at one value; do not grow it.
2. Add `CONSTRAINT AtMostOneSetSnapshot` in the `.cfg`, with
   `AtMostOneSetSnapshot == Cardinality({ t \in Tids : txn[t].snapshot /= tlog.tid_start[t] }) <= 1` in the `MC`
   module: one transaction reading an old snapshot is enough for every property here, and the witness of
   `Assert_getOldestSnapshot` needs exactly one. Document it as a bound, not a reduction.
3. Only then `TID_MAX = 2`, and only after re-running the witness of every property the scenario checks and
   confirming each is still red at that bound. Plan 1 found three `Base` witnesses that are green at
   `TID_MAX = 2`, so this step will probably fail the bound contract; if it does, keep `TID_MAX = 3` and reduce
   `Sessions` to one instead, which costs the two-session interleavings but keeps the transaction count.

Record the outcome in `STATE_SPACE.md` under a new section "The `SetSnapshot` scenario", with the same table
shape plan 1 used.

- [ ] **Step 8: Confirm `Base` is unchanged and run the witness**

`run_tlc.sh Base`: expected green, and the distinct-state count within counting noise of 28,553,114. Steps 2, 3
and 5 all touch definitions `Base` uses. A count that moved by more than a few hundred states means one of them
changed behaviour; find which before continuing, and say so in the report.

`witness.sh SetSnapshot Assert_getOldestSnapshot`: expected `RED`. The change is the one the spec's row names,
`SetSnapshot` also rewriting `protected_snapshot` (and with it the `snapshots_in_use` entry, which is the same
object in the C++). It fires on the sortedness clause: a second transaction that began later holds a larger
snapshot, and lowering the first transaction's entry to `FirstCSN` leaves the list unsorted.
`witness.sh SetSnapshot Assert_TailPtrNotRegressing Assert_getOldestSnapshot`: expected `RED` too, on the next
`removeOldEntries`, because `getOldestSnapshot` has moved below the stored `tail_ptr`. Record both rows in
`WITNESSES.md` and strike `Assert_getOldestSnapshot` from its deferred table.

`witness.sh SetSnapshot NoOutdatedLookup`: expected **`GREEN`**, and that is the S4 finding, not a failure of the
task. Record the row as `GREEN (vacuous)` with the reason: `assertTIDIsNotOutdated` is reachable only from
`tryFinalizeUnknownStateTransactions`, which this scenario does not enable, so the property has nothing to
falsify here. Do not add it to `MC_SetSnapshot.cfg`. Add S4 to `FINDINGS.md` section 3 with the two call sites.

- [ ] **Step 9: Write finding F2**

Add to `FINDINGS.md` section 1 an entry F2 classified `code`, with the full treatment the fix-variant rule asks
for. The shape to look for when task 2 turns the cleanup thread on, and the reasoning to write now:

`SET TRANSACTION SNAPSHOT` lowers the transaction's read snapshot and leaves its entry in `snapshots_in_use`
alone. `TransactionLog::getOldestSnapshot` therefore reports a value above the snapshot the transaction actually
reads at, `VersionMetadata::canBeRemoved` (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:272`)
compares the removal CSN against that too-high value, `MergeTreeData::grabOldParts`
(`src/Storages/MergeTree/MergeTreeData.cpp:4140`) grabs the part, and the running transaction's next `SELECT`
loses rows it could read a moment earlier.

Proposed C++ fix, for the entry: give `TransactionLog` a method that changes a running transaction's snapshot
under `running_list_mutex`, erasing the transaction's `snapshot_in_use_it` from `snapshots_in_use` and
re-inserting it at the position the new value sorts to (so that the `getOldestSnapshot` assertions keep holding),
updating `snapshot_in_use_it`, and refusing with `INVALID_TRANSACTION` when the requested snapshot is below
`tail_ptr`, because the log entries needed to resolve the parts of that era may already be truncated.
`InterpreterTransactionControlQuery::executeSetSnapshot` calls that instead of
`MergeTreeTransaction::setSnapshot`.

The model variant is `SET_SNAPSHOT_PROTECTS`, written in step 2, which does exactly those two things: it moves
the `snapshots_in_use` entry with the snapshot and refuses a target below `tlog.tail_ptr`. Note in the entry that
the variant necessarily makes the tid order stop being the list order, which is why
`Assert_getOldestSnapshot`'s sortedness clause is conditioned on it: under the fix the C++ list is re-sorted and
the assertion holds, while the model's tid-ordered proxy for it does not.

- [ ] **Step 10: Commit**

```bash
git commit -m "tla(transactions): SET TRANSACTION SNAPSHOT, log truncation, the SetSnapshot scenario" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 2: The cleanup thread {#task-2}

**Files:**
- Modify: `utils/tla/transactions/Parts.tla` (`CanBeRemovedWith`, `ValidateMetadataOK`, `RealDisagreement`,
  `NoStoredRecord`)
- Modify: `utils/tla/transactions/Server.tla` (`SysRecord`, `SysInit`, the four `Cleanup*` actions)
- Modify: `utils/tla/transactions/Invariants.tla` (`NoPrematureDelete`, `PinnedNotDeleted`, `NoFalseCorruption`)
- Modify: `utils/tla/transactions/MC_SetSnapshot.{tla,cfg}`, `MC_SetSnapshotFixed.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Interfaces:**
- Produces: `sys.cleanup_pc \in {"Idle", "Validate", "Delete"}` (already declared) and the new field
  `sys.cleanup_part \in Parts \cup {"None"}`; `CanBeRemovedWith(p, oldest)`; `ValidateMetadataOK(p)` and
  `RealDisagreement(p)`, consumed by task 4's `NoFalseCorruption` witness; the properties `NoPrematureDelete`,
  `PinnedNotDeleted`, `NoFalseCorruption`, consumed by the `Merge`, `NonTxn` and (plan 4) `MutationCleanup`
  scenarios.

- [ ] **Step 1: Parameterize `canBeRemoved` and add the record comparison**

In `Parts.tla`, rename the existing `CanBeRemovedImpl(p)` to `CanBeRemovedWith(p, oldest)`, replacing its three
uses of `OldestSnapshot` with `oldest`, and add `CanBeRemovedImpl(p) == CanBeRemovedWith(p, OldestSnapshot)`
after it. Nothing else changes; this is a behaviour-preserving extraction, and step 6 re-runs `Base` to confirm
it.

Then add, after `StoredRecord`:

```tla
\* No txn_version.txt and no deferred record: readMetadata would throw CANNOT_OPEN_FILE.
NoStoredRecord(p) == ~part[p].deferred_on /\ ~DiskHasInfo(p)
\* The shape loadMetadata case 2 produces, short-circuited by both validateInfo and hasValidMetadata.
DummyRolledBackShape(info) ==
  info.ccsn = RolledBackCSN /\ info.ctid = DummyTID /\ info.rtid = EmptyTID /\ info.rcsn = UnknownCSN

\* VersionMetadata::hasValidMetadata, src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:632-721, reached
\* through IMergeTreeDataPart::assertHasValidVersionMetadata (IMergeTreeDataPart.cpp:2863), which returns true
\* for a part that was never involved in a transaction and for a Temporary one. A mismatch throws CORRUPTED_DATA;
\* a CANNOT_OPEN_FILE whose directory is gone is accepted (:706).
\* The NoFalseCorruption witness removes two exemptions the spec's row names: the deferred record counts as a
\* stored record, and a NonTransactionalCSN held only in memory is transient.
ValidateMetadataOK(p) ==
  LET m == part[p].mem
      r == StoredRecord(p)
      have == IF Witness("NoFalseCorruption") THEN DiskHasInfo(p) ELSE ~NoStoredRecord(p) IN
  \/ ~Involved(m)
  \/ part[p].pstate = "Temporary"
  \/ DummyRolledBackShape(m)
  \/ (~have /\ ~DiskDirExists(p))
  \/ /\ have
     /\ m.ctid = r.ctid
     /\ (m.rtid = r.rtid \/ m.rtid = NonTransactionalTID)
     /\ (m.ccsn = r.ccsn \/ m.ccsn = RolledBackCSN \/ r.ccsn = UnknownCSN)
     /\ (m.rcsn = r.rcsn \/ (m.rcsn = NonTransactionalCSN /\ ~Witness("NoFalseCorruption")) \/ r.rcsn = UnknownCSN)
     /\ ~(r.rcsn /= UnknownCSN /\ r.rtid = EmptyTID)

\* spec #invariants-cleanup, NoFalseCorruption: the disagreements history says cannot be transient. It keeps both
\* exemptions unconditionally, which is what makes the witness above red rather than merely different.
RealDisagreement(p) ==
  LET m == part[p].mem
      r == StoredRecord(p)
      have == ~NoStoredRecord(p) IN
  \/ (have /\ m.ctid /= r.ctid)
  \/ (have /\ m.rtid /= r.rtid /\ m.rtid /= NonTransactionalTID)
  \/ (have /\ m.ccsn /= r.ccsn /\ m.ccsn /= RolledBackCSN /\ r.ccsn /= UnknownCSN)
  \/ (have /\ m.rcsn /= r.rcsn /\ m.rcsn /= NonTransactionalCSN /\ r.rcsn /= UnknownCSN)
  \/ (have /\ r.rcsn /= UnknownCSN /\ r.rtid = EmptyTID)
  \/ (~have /\ DiskDirExists(p) /\ ~DummyRolledBackShape(m))
```

- [ ] **Step 2: The cleanup thread's state**

In `Server.tla`, add `cleanup_part : Parts \cup {"None"}` to `SysRecord` and `cleanup_part |-> "None"` to
`SysInit`. **SANY-check before proceeding**; the error class for a missed `SysInit` field is
`Error: The invariant TypeOK is violated by the initial state`.

- [ ] **Step 3: The four actions**

Replace the four `Cleanup*` stubs:

```tla
\* MergeTreeData::grabOldParts, src/Storages/MergeTree/MergeTreeData.cpp:4074, under lockParts: an Outdated part
\* whose version canBeRemoved (:4140), that nobody else holds (isSharedPtrUnique, :4150), and that is not an
\* empty part still covering an Outdated one (:4158, "First remove all covered parts, then remove covering empty
\* part"), moves to Deleting. The removal-time and mutation-parent conditions at :4167 are time and
\* zero-copy-replication bookkeeping and are not modelled; `force` covers them.
\* The model grabs one part per action where the code grabs a set under one lock. The only cross-part coupling
\* the lock provides is the atomicity of the state change, and no property of this plan reads the set of parts in
\* Deleting, so the refinement is recorded rather than removed. Placement if it ever matters: the plan that adds
\* a property over Deleting parts.
CleanupGrab(p) ==
  /\ Up /\ sys.cleanup_pc = "Idle" /\ sys.parts_lock = NoActor
  /\ part[p].pstate = "Outdated"
  /\ (IF Witness("NoPrematureDelete") \/ Witness("NoLostVisibleData")
      THEN CanBeRemovedWith(p, tlog.latest_snapshot) ELSE CanBeRemovedImpl(p))
  /\ (part[p].pins = {} \/ Witness("PinnedNotDeleted"))
  /\ part[p].frames = {}
  /\ ~(part[p].payload.tomb /\ \E q \in Expand({p}) \ {p} : part[q].pstate = "Outdated")
  /\ part' = [part EXCEPT ![p].pstate = "Deleting"]
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Validate", !.cleanup_part = p]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, client, stmt, mut, task>>

\* chassert(assertHasValidVersionMetadata()) in IMergeTreeDataPart::remove, IMergeTreeDataPart.cpp:2928, on the
\* path clearPartsFromFilesystemAndRollbackIfError (MergeTreeData.cpp:4566) takes for each grabbed part.
CleanupValidate(p) ==
  /\ Up /\ sys.cleanup_pc = "Validate" /\ sys.cleanup_part = p
  /\ ValidateMetadataOK(p)
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Delete"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>

\* the success path of clearPartsFromFilesystemAndRollbackIfError: the directory is gone in both layers and
\* removePartsFinally (MergeTreeData.cpp:4217) erases the part from data_parts_indexes.
CleanupDeleteOk(p) ==
  /\ Up /\ sys.cleanup_pc = "Delete" /\ sys.cleanup_part = p
  /\ disk' = DiskWithoutDir(p)
  /\ part' = [part EXCEPT ![p].pstate = "Deleted", ![p].deferred_on = FALSE, ![p].deferred = EmptyInfo]
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Idle", !.cleanup_part = "None"]
  /\ UNCHANGED <<zk, mdisk, h, tlog, txn, client, stmt, mut, task>>

\* rollbackDeletingParts, MergeTreeData.cpp:4205: back to Outdated. Two producers: the CORRUPTED_DATA that
\* hasValidMetadata throws, and the filesystem error of clearPartsFromFilesystemImpl. The second needs a disk
\* fault, which is plan 5's; until then the disjunct is FALSE and is written out so the action is complete.
CleanupDeleteFail(p) ==
  /\ Up /\ sys.cleanup_part = p
  /\ \/ (sys.cleanup_pc = "Validate" /\ ~ValidateMetadataOK(p))
     \/ (sys.cleanup_pc = "Delete" /\ FALSE)      \* the filesystem-error path: plan 5, DISK_FAULTS_MAX > 0
  /\ part' = [part EXCEPT ![p].pstate = "Outdated"]
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Idle", !.cleanup_part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, client, stmt, mut, task>>
```

The `payload.tomb` guard reads "an empty part", the model's name for `rows_count == 0`; task 4's `NtDropCover`
is what sets it. `Expand({p}) \ {p}` stands for `getCoveredOutdatedParts`, exact while the covering relation is
two levels deep, which is every scenario of this plan.

- [ ] **Step 4: The three properties**

In `Invariants.tla`:

```tla
\* spec #invariants-cleanup. Stated over the oracle, not over VersionMetadata::canBeRemoved, so that a wrong
\* canBeRemoved is caught rather than assumed; the snapshot is the actual one, not the protected one.
NoPrematureDeleteStep == \A p \in Parts : CleanupGrab(p) =>
  \A u \in tlog.running_list : ~OracleVisible(p, txn[u].snapshot, u)
NoPrematureDelete == [][NoPrematureDeleteStep]_vars
\* isSharedPtrUnique, MergeTreeData.cpp:4150, as a property rather than only as the guard of the action.
PinnedNotDeletedStep == \A p \in Parts : CleanupGrab(p) => part[p].pins = {}
PinnedNotDeleted == [][PinnedNotDeletedStep]_vars
\* the validation refusal is justified only by a disagreement history says cannot be transient
NoFalseCorruptionStep == \A p \in Parts : CleanupDeleteFail(p) => RealDisagreement(p)
NoFalseCorruption == [][NoFalseCorruptionStep]_vars
```

Add `NoPrematureDelete`, `PinnedNotDeleted`, `NoLostVisibleData` and `NoFalseCorruption` to the `PROPERTIES` line
of `MC_SetSnapshotFixed.cfg`. In `MC_SetSnapshot.cfg` add only `NoPrematureDelete`: that run exists to produce
the finding, and a configuration with more properties in it would stop on whichever fires first.

- [ ] **Step 5: Run both configurations**

`run_tlc.sh SetSnapshot`: **expected red on `NoPrematureDelete`.** That is the spec's expected-red row and the
finding F2 task 1 wrote. Copy the trace to `utils/tla/transactions/traces/setsnapshot-premature-delete.txt` and
fill F2's counterexample column with the action sequence, the state in which the grab happened
(`txn[t].snapshot`, `tlog.snapshots_in_use[t]`, `OldestSnapshot`, the part's `mem`), and the C++ call sequence
from the code map. If instead it is green, that is a class (d) result: the scenario cannot build the shape at its
bounds. Check first that `SNAPSHOT_TARGETS` is reachable (a transaction must be running while a part it can see
becomes removable), then report; do not weaken anything.

`run_tlc.sh SetSnapshotFixed`: expected green, and that is the run that checks the scenario's remaining
properties. Budget as in task 1 step 7. If `NoLostVisibleData` is red *here*, it is a second finding, not the
same one: read the trace and classify it before touching the property.

- [ ] **Step 6: Witnesses and the `Base` re-run**

`run_tlc.sh Base`: green, count within noise of 28,553,114 (step 1 touched `Parts.tla`).

Witnesses, all in `SetSnapshotFixed` so that the `SetSnapshot` defect does not fire first:

| Property | Witness name | Expected |
|---|---|---|
| `NoPrematureDelete` | `NoPrematureDelete` | RED, `CleanupGrab` uses `latest_snapshot` instead of `getOldestSnapshot` |
| `PinnedNotDeleted` | `PinnedNotDeleted` | RED, `CleanupGrab` ignores `pins` |
| `NoLostVisibleData` | `NoLostVisibleData` | RED, the same `latest_snapshot` change, this time observed as lost content |

`NoFalseCorruption`'s witness needs the `NonTxn` scenario and is task 4's. Record the three rows in
`WITNESSES.md` under a new heading for the scenario, and add `NoFalseCorruption` to the deferred table with
"task 4 of this plan" as its destination.

- [ ] **Step 7: Commit**

```bash
git commit -m "tla(transactions): the cleanup thread and the premature-delete finding" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 3: Merges, the task set and the covering relation {#task-3}

The largest task: background merges are the first actor that is not a session, so the publication and commit
machines have to stop being session-shaped. They are extracted into actor-generic bodies first, which is a
behaviour-preserving change `Base` measures, and only then reused.

**Files:**
- Modify: `utils/tla/transactions/Server.tla` (the extraction, `TaskRecord`, `RollbackFinalize`, `SelectFinish`,
  the six `Merge*` stubs and the new `MergePublish*` / `MergeCommit*` actions, the holder discipline)
- Modify: `utils/tla/transactions/Parts.tla` (`InfoIsVisible`, `IsVisibleImpl`: the `NoDoubleRead` hook;
  `FragsOf`)
- Modify: `utils/tla/transactions/Invariants.tla` (`ActiveSetShape`'s empty-part clause, `Frags` through
  `FragsOf`)
- Modify: `utils/tla/transactions/MergeTreeTransactions.tla` (`TaskNext`, `MergeNext`)
- Create: `utils/tla/transactions/MC_Merge.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Interfaces:**
- Produces: `Tsk(i) == <<"Task", i>>`; the actor-generic bodies `PublishStartEffect(a, t, p)`,
  `EnrolEffect(a, t, q)`, `PublishFlipEffect(a, p, C)`, `CommitStoreEffect(a, t, p, op, phase)`,
  `CommitCreateEffect(t)`, `CommitFlipEffect(t)`, `CommitFinalizeEffect(t)`, each a conjunct over the shared
  variables only; `task[i].result`; `FragsOf(c)`. Task 4 consumes `EnrolEffect` shape and `FragsOf`.
- Consumes: `CoveredNow(p, t)`, `StartFrame`, `FrameDone`, `NextCommitPc`, `FirstCommitPc`, `SetToSeq`.

- [ ] **Step 1: Golden numbers before the extraction**

Record the current `run_tlc.sh Base` distinct-state count and the counts of two cheap witnesses
(`witness.sh Base FlipAfterStores`, `witness.sh Base ReadYourWrites`) in the task's notes. The extraction in
step 2 is only correct if all three are unchanged afterwards; measuring after the change is not a check.

- [ ] **Step 2: Extract the actor-generic bodies**

In `Server.tla`, split each of the following client actions into a body operator that constrains only the shared
variables and a thin action that adds the session's guard and its `client'` / `UNCHANGED` clauses. The bodies:

```tla
Tsk(i) == <<"Task", i>>

\* Transaction::commit, first half under lockParts (MergeTreeData.cpp:11220 region): covered parts computed,
\* addNewPart attaches p to the outer transaction.
PublishStartEffect(a, t, p) ==
  LET C == CoveredNow(p, t) IN
  /\ sys' = [sys EXCEPT !.parts_lock = a]
  /\ stmt' = [stmt EXCEPT ![a].covered = C, ![a].attached = @ \cup {p}, ![a].work = SetToSeq(C)]
  /\ txn' = [txn EXCEPT ![t].creating = Append(@, p)]
  /\ h' = [h EXCEPT !.creating[t] = @ \cup {p}]
  /\ part' = [part EXCEPT ![p].pins = @ \cup {<<"Txn", t>>}]

\* removeOldPart's granting branch (MergeTreeTransaction.cpp:213): mutex, lockRemovalTID, enrol, start the store.
EnrolGrantEffect(a, t, q) ==
  /\ part' = [StartFrame(q, a, "RemovalTID", t, FALSE) EXCEPT ![q].lock = t, ![q].pins = @ \cup {<<"Txn", t>>}]
  /\ txn' = [txn EXCEPT ![t].mutex = a, ![t].removing = Append(@, q)]
  /\ h' = [h EXCEPT !.removing[t] = @ \cup {q}]
EnrolRefused(t, q) ==
  \/ txn[t].state = "RolledBack"
  \/ ((part[q].lock /= EmptyTID \/ part[q].mem.rcsn /= UnknownCSN) /\ ~Witness("SingleRemover"))
EnrolError(t) == IF txn[t].state = "RolledBack" THEN "INVALID_TRANSACTION" ELSE "SERIALIZATION_ERROR"

\* the NOEXCEPT_SCOPE state loop of Transaction::commit
PublishFlipEffect(a, p, C) ==
  /\ part' = [q \in Parts |-> IF q = p THEN [part[q] EXCEPT !.pstate = "Active"]
                              ELSE IF q \in C /\ ~Witness("ActiveSetShape") THEN [part[q] EXCEPT !.pstate = "Outdated"]
                              ELSE part[q]]
  /\ stmt' = [stmt EXCEPT ![a] = NoStmt]
  /\ sys' = [sys EXCEPT !.parts_lock = NoActor]

CommitCreateEffect(t) ==
  /\ KeeperCanAppend /\ zk.session = "Alive"
  /\ zk' = KeeperAppended(t)
  /\ h' = [h EXCEPT !.committed = @ \cup {t}, !.csn[t] = KeeperNextCsn,
                    !.removers = [p \in Parts |-> IF p \in h.removing[t] THEN @[p] \cup {t} ELSE @[p]]]
  /\ txn' = [txn EXCEPT ![t].pc = FirstCommitPc(t), ![t].work = FirstCommitWork(t)]

CommitStoreEffect(a, t, p, op, phase) ==
  LET val == IF Witness("Assert_validateInfo_creator") /\ op = "CreationCSN" THEN h.csn[t] + 1
             ELSE IF Witness("Assert_validateInfo_order") /\ op = "CreationCSN" THEN CSN_MAX
             ELSE h.csn[t]
      skip == (Witness("Assert_isVisible_fast") \/ Witness("Assert_isVisible_fast_only1"))
              /\ op = "CreationCSN" /\ p \in h.removing[t] IN
  /\ txn[t].pc = phase /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ \/ /\ ~skip /\ ~HasFrame(p, a) /\ ApplyOp(op, val, part[p].mem) /= part[p].mem
        /\ part' = StartFrame(p, a, op, val, TRUE)
        /\ UNCHANGED txn
     \/ /\ (skip \/ FrameDone(p, a, op, val))
        /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN NextCommitWork(t, phase) ELSE Tail(@),
                               ![t].pc = IF Tail(txn[t].work) = <<>> THEN NextCommitPc(t, phase) ELSE phase]
        /\ UNCHANGED part

CommitFlipEffect(t) ==
  txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = h.csn[t], ![t].csn_notified = TRUE,
                      ![t].pc = "CommitFinalize", ![t].work = <<>>]
CommitFinalizeEffect(t) ==
  /\ tlog' = [tlog EXCEPT !.running_list = @ \ {t}, !.snapshots_in_use[t] = UnknownCSN]
  /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].creating = <<>>, ![t].removing = <<>>, ![t].mutations = {}]
  /\ part' = PinsWithout(<<"Txn", t>>)
```

Then rewrite `PublishStart`, `EnrolBody`, `PublishFlip`, `CommitCreateCSN`, `CommitStore`, `CommitFlip` and
`CommitFinalize` as the same actions with the body substituted. `EnrolBody(k, q, nextpc)` keeps its three-branch
shape and its `client'` assignments; only the granting branch's three conjuncts move into `EnrolGrantEffect`, and
the two refusing branches use `EnrolRefused` and `EnrolError`.

**SANY-check, then run `run_tlc.sh Base` and the two witnesses from step 1. All three counts must match.** A
difference means the extraction changed behaviour; find the differing conjunct before writing another line. The
most likely cause is an `UNCHANGED` list that lost or gained a variable.

- [ ] **Step 3: The holder discipline**

`RollbackFinalize` (`Server.tla:520` region) sets `![t].holders = {}`. With sessions only that is invisible; with
tasks it is wrong, because a merge task holds its transaction across the rollback and a mutation task (plan 4)
holds a session's. In the C++ the rollback body destroys no `shared_ptr`: the holder goes away when its owner's
`MergeTreeTransactionHolder` is destroyed. Move the removal to the owners:

- `RollbackFinalize`: drop the `![t].holders = {}` conjunct (keep `rb_driver = NoActor`).
- `RollbackReturn`: when `client[k].rb_detach`, also `txn' = [txn EXCEPT ![Cur(k)].holders = @ \ {Sess(k)}]`.
- `RollbackStart`, the "already rolled back: detach now" branch: the same removal.
- `MergeFail` and `MergeCommitFinalize` (step 6) remove `Tsk(i)`.

This is `FINDINGS.md`'s `holders` note being paid. `holders` is in `BaseView`, so `Base`'s count will move: run
`run_tlc.sh Base` and record the new number in `STATE_SPACE.md` as a line under the `Base` history, with the
reason. Expected direction: up, because a rolled-back transaction now keeps its holder for a step or two longer.
If `Base` goes red, the counterexample is about a property that reads `holders` (none does today) or about
`KillerNotStranded`; read it before changing anything.

- [ ] **Step 4: Covering parts in reads and in `ActiveSetShape`**

An empty covering part covers rows that no longer exist, so it must not contribute its sources' fragments to a
read. In `Parts.tla`, after `Frags` (moved there in task 1), add

```tla
\* the fragments a visible root contributes: none for an empty part (rows_count == 0), which is what a
\* non-transactional DROP PARTITION publishes over the partition it drops
FragsOf(c) == IF part[c].payload.tomb THEN {} ELSE { <<q, part[q].payload.ver>> : q \in Expand({c}) }
```

and change `Frags(V) == UNION { FragsOf(c) : c \in V }`. In `Server.tla`'s `SelectFinish`, change the `frags`
component of `R` to `UNION { { <<c, q, part[q].payload.ver>> : q \in Expand({c}) } : c \in V, part[c].payload.tomb = FALSE }`
— or, equivalently and less error-prone,
`UNION { IF part[c].payload.tomb THEN {} ELSE { <<c, q, part[q].payload.ver>> : q \in Expand({c}) } : c \in V }`.
No part is a tombstone before task 4, so `Base` and `Merge` are unaffected; task 4 is where it earns its place.

In `Invariants.tla`, `ActiveSetShape` already has the overlap clause and the reservation clause. Add the spec's
third: no `Active` empty part while a part it covers is `Active`.

```tla
ActiveSetShape ==
  /\ \A p, q \in Parts : part[p].pstate = "Active" /\ part[q].pstate = "Active" => ~Overlap(p, q)
  /\ \A i, j \in Tasks : i /= j => task[i].reserved \cap task[j].reserved = {}
  /\ \A p \in Parts : part[p].pstate = "Active" /\ part[p].payload.tomb =>
       \A q \in Expand({p}) \ {p} : part[q].pstate /= "Active"
```

The third clause is implied by the first while `Covers` is two levels deep, and it is written anyway because the
spec names it and because the first clause is the one the `ActiveSetShape` witness attacks.

- [ ] **Step 5: The `NoDoubleRead` hook**

In `Parts.tla`'s `InfoIsVisible`, guard the two removal tests of the fast path, and in `IsVisibleImpl` the
removal lookup of the slow path, with the witness the spec's row names, "`SelectCheck` ignores `removal_csn` and
`removal_tid`":

- `ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= s THEN "FALSE"` becomes
  `ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= s /\ ~Witness("NoDoubleRead") THEN "FALSE"`.
- `ELSE IF u /= EmptyTID /\ info.rtid = u THEN "FALSE"` likewise.
- In the slow path, the `rcsn` binding becomes
  `IF Witness("NoDoubleRead") THEN UnknownCSN ELSE IF info.rtid = EmptyTID THEN info.rcsn ELSE ...`.

Leave the first line, `IF info.rtid = NonTransactionalTID THEN "FALSE"`, alone: with it removed the witness would
also change what a non-transactional removal means, and task 4 needs that path honest.

- [ ] **Step 6: The merge actor**

Add `result : Parts \cup {"None"}` to `TaskRecord` and `result |-> "None"` to `IdleTask`, and widen
`TaskRecord`'s `pc` domain to
`{"Idle", "Select", "Write", "Rename", "PublishStart", "PublishEnrol", "PublishStore", "PublishFlip", "Commit", "Fail"}`.
Then replace the merge stubs and add the publication and commit actions. `BG_TASKS` is `Tasks`; a merge
transaction is held by `Tsk(i)` alone, issues no `SELECT`, and commits with `throw_on_unknown_status = false`,
which matters only from plan 3 on.

```tla
\* scheduleDataProcessingJob on an idle task: beginTransaction with autocommit = false
\* (StorageMergeTree::scheduleDataProcessingJob, src/Storages/StorageMergeTree.cpp:2240 region)
MergeBegin(i) ==
  /\ Up /\ task[i].kind = "Idle" /\ task[i].pc = "Idle"
  /\ sys.merges_blocker = 0
  /\ tlog.local_tid_counter < TID_MAX
  /\ LET t == tlog.local_tid_counter + 1
         s == tlog.latest_snapshot IN
     /\ tlog' = [tlog EXCEPT !.local_tid_counter = t, !.tid_start[t] = s, !.running_list = @ \cup {t},
                             !.snapshots_in_use[t] = s]
     /\ txn' = [txn EXCEPT ![t] = [AbsentTxn EXCEPT !.state = "Running", !.snapshot = s,
                                     !.protected_snapshot = s, !.holders = {Tsk(i)}]]
     /\ task' = [task EXCEPT ![i].kind = "Merge", ![i].txn = t, ![i].pc = "Select"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, sys, client, stmt, mut>>

\* selectPartsToMerge with txn (StorageMergeTree.cpp:1680) through
\* Compaction/PartsCollectors/MergeTreePartsCollector.cpp:88-92: every source must be visible at the merge's
\* snapshot with the EMPTY tid (not the merge's own, spec defect S6), must not be locked for removal, and must
\* not be reserved by another task (canUsePartInMerges, currently_merging_mutating_parts). The reservation is
\* taken by CurrentlyMergingPartsTagger's constructor (StorageMergeTree.cpp:918-923), whose LOGICAL_ERROR
\* "Tagging already tagged part" is the ActiveSetShape reservation clause.
\* The universe gives each covering part exactly one source set, so the merge is always the full cover.
MergeSelect(i) ==
  /\ Up /\ task[i].kind = "Merge" /\ task[i].pc = "Select" /\ sys.merges_blocker = 0
  /\ \E r \in Parts :
     /\ Covers[r] /= {} /\ part[r].pstate = "Absent"
     /\ \A q \in Covers[r] :
        /\ part[q].pstate \in {"Active", "Outdated"}
        /\ IsVisibleImpl(q, txn[task[i].txn].snapshot, EmptyTID)
        /\ part[q].lock = EmptyTID
        /\ \A j \in Tasks : q \notin task[j].reserved
     /\ task' = [task EXCEPT ![i].pc = "Write", ![i].reserved = Covers[r], ![i].result = r]
     /\ part' = [q \in Parts |-> IF q \in Covers[r] THEN [part[q] EXCEPT !.pins = @ \cup {Tsk(i)}] ELSE part[q]]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, stmt, mut>>

\* MergeTask: setAndStoreCreationTID on the result. Its payload is empty-of-its-own: a covering part's content
\* is its sources', read through Expand, so only `tomb` matters and a merge result is not a tombstone.
MergeWrite(i) ==
  /\ Up /\ task[i].kind = "Merge" /\ task[i].pc = "Write"
  /\ LET r == task[i].result IN
     /\ part[r].pstate = "Absent"
     /\ disk' = DiskWithDir(r)
     /\ part' = [StartFrame(r, Tsk(i), "CreateTID", task[i].txn, FALSE) EXCEPT
                   ![r].pstate = "Temporary", ![r].deferrable = FALSE]
     /\ h' = [h EXCEPT !.creator[r] = task[i].txn]
     /\ task' = [task EXCEPT ![i].pc = "Rename"]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, client, stmt, mut>>

\* renameMergedTemporaryPart (MergeTreeDataMergerMutator.cpp:526) puts the result into the statement transaction
MergeRename(i) ==
  /\ task[i].kind = "Merge" /\ task[i].pc = "Rename"
  /\ LET r == task[i].result IN
     /\ FrameDone(r, Tsk(i), "CreateTID", task[i].txn)
     /\ sys.parts_lock = NoActor
     /\ part' = [part EXCEPT ![r].pstate = "PreActive"]
     /\ stmt' = [stmt EXCEPT ![Tsk(i)].precommitted = @ \cup {r}]
     /\ task' = [task EXCEPT ![i].pc = "PublishStart"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, mut>>

MergePublishStart(i) ==
  /\ task[i].pc = "PublishStart" /\ sys.parts_lock = NoActor /\ txn[task[i].txn].state = "Running"
  /\ LET r == task[i].result IN
     /\ r \in stmt[Tsk(i)].precommitted
     /\ PublishStartEffect(Tsk(i), task[i].txn, r)
     /\ task' = [task EXCEPT ![i].pc = IF CoveredNow(r, task[i].txn) = {} THEN "PublishFlip" ELSE "PublishEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, client, mut>>

MergePublishEnrol(i, q) ==
  /\ task[i].pc = "PublishEnrol" /\ sys.parts_lock = Tsk(i)
  /\ stmt[Tsk(i)].work /= <<>> /\ Head(stmt[Tsk(i)].work) = q
  /\ LET t == task[i].txn IN
     /\ txn[t].mutex = NoActor
     /\ \/ /\ EnrolRefused(t, q)
           /\ task' = [task EXCEPT ![i].pc = "Fail"]
           /\ UNCHANGED <<txn, part, h>>
        \/ /\ ~EnrolRefused(t, q) /\ txn[t].state = "Running"
           /\ EnrolGrantEffect(Tsk(i), t, q)
           /\ task' = [task EXCEPT ![i].pc = "PublishStore"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, sys, stmt, client, mut>>

MergePublishStore(i, q) ==
  /\ task[i].pc = "PublishStore" /\ sys.parts_lock = Tsk(i) /\ Head(stmt[Tsk(i)].work) = q
  /\ LET t == task[i].txn IN
     /\ (FrameDone(q, Tsk(i), "RemovalTID", t) \/ Witness("Assert_validateInfo_removal"))
     /\ txn' = [txn EXCEPT ![t].mutex = NoActor]
  /\ LET w == Tail(stmt[Tsk(i)].work) IN
     /\ stmt' = [stmt EXCEPT ![Tsk(i)].work = w]
     /\ task' = [task EXCEPT ![i].pc = IF w = <<>> THEN "PublishFlip" ELSE "PublishEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, mut>>

\* reserved[i] is NOT released here: CurrentlyMergingPartsTagger::finalize runs at the end of
\* MergePlainMergeTreeTask::finish, after transaction.commit() and after commitTransaction (spec defect S5).
MergePublishFlip(i) ==
  /\ task[i].pc = "PublishFlip" /\ sys.parts_lock = Tsk(i)
  /\ PublishFlipEffect(Tsk(i), task[i].result, stmt[Tsk(i)].covered)
  /\ task' = [task EXCEPT ![i].pc = "Commit"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, client, mut>>

\* TransactionLog::commitTransaction(txn_, throw_on_unknown_status = false), MergePlainMergeTreeTask.cpp:194
MergeCommitBefore(i) ==
  /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "Idle" /\ txn[task[i].txn].state = "Running"
  /\ LET t == task[i].txn IN
     /\ txn' = [txn EXCEPT ![t].state = "Committing", ![t].csn = CommittingCSN, ![t].csn_notified = FALSE,
                            ![t].pc = IF Effects(t) THEN "CommitCreateCSN" ELSE "CommitFlip"]
     /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
MergeCommitCreateCSN(i) ==
  /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "CommitCreateCSN"
  /\ CommitCreateEffect(task[i].txn)
  /\ UNCHANGED <<disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
MergeCommitStore(i, p, op, phase) ==
  /\ task[i].pc = "Commit"
  /\ CommitStoreEffect(Tsk(i), task[i].txn, p, op, phase)
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
MergeCommitStoreCreation(i, p) == MergeCommitStore(i, p, "CreationCSN", "CommitStoreCreation")
MergeCommitStoreRemoval(i, p) == MergeCommitStore(i, p, "RemovalCSN", "CommitStoreRemoval")
MergeCommitFlip(i) ==
  /\ task[i].pc = "Commit" /\ Effects(task[i].txn) /\ txn[task[i].txn].state = "Committing"
  /\ (txn[task[i].txn].pc = "CommitFlip"
      \/ (Witness("FlipAfterStores") /\ txn[task[i].txn].pc \in {"CommitStoreCreation", "CommitStoreRemoval"}))
  /\ CommitFlipEffect(task[i].txn)
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, stmt, mut, task>>
MergeCommitReadOnly(i) ==
  /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "CommitFlip" /\ ~Effects(task[i].txn)
  /\ txn[task[i].txn].state = "Committing"
  /\ LET t == task[i].txn IN
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = txn[t].snapshot, ![t].csn_notified = TRUE,
                            ![t].pc = "CommitFinalize"]
     /\ h' = [h EXCEPT !.csn[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
\* merge_mutate_entry->finalize() at the end of MergePlainMergeTreeTask::finish: the reservation and the holder
\* go here, not at the publication.
MergeCommitFinalize(i) ==
  /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "CommitFinalize"
  /\ CommitFinalizeEffect(task[i].txn)
  /\ task' = [task EXCEPT ![i] = IdleTask]
  /\ UNCHANGED <<zk, disk, mdisk, h, sys, stmt, mut>>

\* an exception anywhere before MergeCommitBefore: the statement transaction rolls back if the result was
\* renamed, the reservation is released, the holder rolls the transaction back (MergePlainMergeTreeTask::onFinish
\* through MergeTreeTransactionHolder's destructor).
MergeFail(i) ==
  /\ task[i].kind = "Merge" /\ task[i].pc \in {"Select", "Write", "Rename", "PublishStart", "PublishEnrol",
                                               "PublishStore", "PublishFlip", "Fail"}
  /\ LET t == task[i].txn
         r == task[i].result
         unattached == stmt[Tsk(i)].precommitted \ stmt[Tsk(i)].attached IN
     /\ txn' = [txn EXCEPT ![t].mutex = IF @ = Tsk(i) THEN NoActor ELSE @,
                            ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                            ![t].pc = "RollbackCopyLists", ![t].rb_driver = Tsk(i),
                            ![t].holders = @ \ {Tsk(i)}]
     /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
     /\ sys' = [sys EXCEPT !.parts_lock = IF @ = Tsk(i) THEN NoActor ELSE @]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.frames = { f \in @ : f.owner /= Tsk(i) },
                                                 !.pins = @ \ {Tsk(i)},
                                                 !.pstate = IF p \in unattached THEN "Outdated" ELSE @]]
     /\ stmt' = [stmt EXCEPT ![Tsk(i)] = NoStmt]
     /\ task' = [task EXCEPT ![i] = IdleTask]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, client, mut>>
```

Two points to check while transcribing, because they are where this will go wrong. `MergeFail` names `Tsk(i)`
as `rb_driver` while `task[i]` becomes `IdleTask` in the same step, so no task action can drive the rollback
afterwards: the rollback machine's steps are all `Drives(k, t)`, session-shaped. That is a hole. **Fix it in the
same step**: generalize `Drives` to `DrivesA(a, t) == txn[t].rb_driver = a`, keep `Drives(k, t) == DrivesA(Sess(k), t)`,
and add to `TaskNext` a set of task-driven rollback steps `MergeRollback*(i, t, p)` that are the existing
`Rollback*` actions with `Tsk(i)` for `Sess(k)`. That is four more thin actions and one more `Fail` pc on the
task, and it is what makes `MergeFail` reachable at all. Alternatively, and preferably for a first cut, **leave
`MergeFail` out of `MergeNext`** and record it as deferred with its placement: it needs a fault to be reachable
(`QUERY_FAULTS_MAX = 0` in `Merge`), so nothing enables it in this scenario. Choose the second, write the action
anyway (it is the spec's row), and note in `FINDINGS.md` section 2 that a task-driven rollback machine is owed by
the first plan whose scenario enables a fault on a background task, which is plan 5's `DiskFault`.

- [ ] **Step 7: The scenario**

In `MergeTreeTransactions.tla`, extend `TaskNext` with the new actions and add

```tla
TaskNext == \E i \in Tasks :
  \/ MergeBegin(i) \/ MergeSelect(i) \/ MergeWrite(i) \/ MergeRename(i)
  \/ MergePublishStart(i) \/ MergePublishFlip(i)
  \/ MergeCommitBefore(i) \/ MergeCommitCreateCSN(i) \/ MergeCommitReadOnly(i)
  \/ MergeCommitFlip(i) \/ MergeCommitFinalize(i)
  \/ (\E q \in Parts : MergePublishEnrol(i, q) \/ MergePublishStore(i, q)
                       \/ MergeCommitStoreCreation(i, q) \/ MergeCommitStoreRemoval(i, q))
  \/ MutFail(i)
  \/ (\E m \in Mutations, p \in Parts : MutSelect(i, m, p) \/ MutWrite(i, m, p) \/ MutRename(i, m, p))
  \/ (\E m \in Mutations : KillCancelTask(m, i))

\* the Merge scenario (spec matrix): Base + Merge* + Cleanup* + Updater+GC
MergeNext == BaseNext \/ TaskNext \/ CleanupNext \/ UpdaterGCNext
MergeSpec == Init /\ [][MergeNext]_vars
```

`MergeFail(i)` is deliberately absent, per step 6.

`MC_Merge.tla`: `CoversDef == [p \in Parts |-> IF p = M12 THEN {P1, P2} ELSE {}]`, `SymSessions` as before, and
`MergeView` built from `BaseView` by adding `task`, `h.content`, `part[p].payload`, `tlog.tail_ptr`,
`tlog.updated_tail_ptr`, `zk.tail`, `h.truncated`, `sys.cleanup_pc`, `sys.cleanup_part`, and by **removing** the
"a read's frags are a function of its parts" justification: with a non-empty `Covers` that is no longer true, so
`client[k].first_read.frags` and `last_read.frags` must be in the fingerprint. Write the corrected three-part
justification into the module's comment.

`MC_Merge.cfg`: `SPECIFICATION MergeSpec`, `SYMMETRY SymSessions`, `VIEW MergeView`, the `Base` invariant list
plus the `Base` property list plus `NoPrematureDelete`, `PinnedNotDeleted`, `NoLostVisibleData`,
`NoFalseCorruption`, `Assert_TailPtrNotRegressing`; constants: `Parts = {P1, P2, M12}`, `Tasks = {i1}`,
`Covers <- CoversDef`, `SNAPSHOT_TARGETS = {}`, `SET_SNAPSHOT_PROTECTS = FALSE`, everything else as `Base`.

One task, not the matrix's two, with the argument written into `STATE_SPACE.md`: in a universe with one covering
part there is exactly one merge, so a second task can only contend for the same two sources, which `reserved`
excludes; it would add idle-state permutations and no interleaving. The reservation clause of `ActiveSetShape` is
vacuous at one task and is checked in plan 4's `MergeMutation`, where a merge and a mutation run side by side.
Record that as a bound with its argument, and add the clause's witness debt to `WITNESSES.md`'s deferred table
with `MergeMutation`, plan 4, as its destination.

- [ ] **Step 8: Run**

`run_tlc.sh Merge` with a 20-minute cap. Expected green. The state count will be well above `Base`'s: a third
part, a task actor with ten program counters, a fourth transaction's worth of commits, and the cleanup thread.
**Expect to have to reduce, and reduce in this order**, recording each measurement:

1. `TID_MAX` stays at 3 but `MergeBegin` competes with `Begin` for it, so a merge costs a client transaction.
   Raise `TID_MAX` to 4 only if the run is small enough to afford it; otherwise leave it and note that the
   scenario trades a client transaction for the merge.
2. `Sessions = {k1}`. `Merge`'s green set (`NoDoubleRead`, `NoPrematureDelete`, `Atomicity`) is about a reader
   against a merge, not about two readers, and the merge task supplies the second actor. Check the bound
   contract by re-running the witnesses of step 9 at the reduced bound before keeping it.
3. `CONSTRAINT AtMostOneMerge`, `Cardinality({ p \in Parts : part[p].pstate /= "Absent" /\ Covers[p] /= {} }) <= 1`,
   which is already implied by the universe and is therefore worth nothing; do not bother.

If green: record in `STATE_SPACE.md` under "The `Merge` scenario".

- [ ] **Step 9: The two witness debts and the new rows**

| Property | Witness name | Scenario | Expected |
|---|---|---|---|
| `NoDoubleRead` | `NoDoubleRead` | `Merge` | RED: the reader ignores the removal fields and sees `M12` and its sources at once |
| `ActiveSetShape` | `ActiveSetShape` | `Merge` | RED: `MergePublishFlip` leaves the sources `Active` under the merge result |
| `Atomicity` | `Atomicity` | `Merge` | RED, already red in `Base`; re-run here because the spec's row names `Merge` |
| `NoPrematureDelete` | `NoPrematureDelete` | `Merge` | RED, the row the spec's table names for this scenario |

Strike `NoDoubleRead` and `ActiveSetShape` from `WITNESSES.md`'s deferred table. `AckedWriteIsDurable` and
`NoAvoidableTermination` stay deferred (plan 3 and plan 5).

- [ ] **Step 10: Commit**

```bash
git commit -m "tla(transactions): background merges, the task actor, the Merge scenario" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 4: Non-transactional queries and the removal batch {#task-4}

**Files:**
- Modify: `utils/tla/transactions/Server.tla` (`BatchType`, the non-transactional client pcs, `NtInsert*`,
  `NtDrop*`, `NtBatch*`)
- Modify: `utils/tla/transactions/Parts.tla` (`StoreReadStep`: the `creation_in_flight` refusal; `FrameType`'s
  `err` domain)
- Modify: `utils/tla/transactions/History.tla` (`h.batch`'s shape, `HistoryTypeOK`)
- Modify: `utils/tla/transactions/Invariants.tla` (`NtBatchRefusedUnchanged`, `NtRefusalJustified`)
- Modify: `utils/tla/transactions/MergeTreeTransactions.tla` (`NtNext`, `NonTxnNext`)
- Create: `utils/tla/transactions/MC_NonTxn.{tla,cfg}`
- Modify: `WITNESSES.md`, `STATE_SPACE.md`, `FINDINGS.md`

**Interfaces:**
- Produces: `sys.nt_batch` with the added field `owner`; `h.batch = [targets |-> SUBSET Parts, before |-> [Parts -> ...]]`;
  `NtBatchRefusedUnchanged`, `NtRefusalJustified`; the `SERIALIZATION_ERROR` frame outcome, consumed by plan 3's
  `NonTxnCrash` and plan 5.
- Consumes: `EnrolRefused` (not reused: the non-transactional refusal is a different predicate), `StartFrame`,
  `FrameDone`, `StoredRecord`, `FragsOf`.

- [ ] **Step 1: The `creation_in_flight` refusal in the store**

`VersionMetadata::setAndStoreRemovalTID` (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:161`)
refuses with `SERIALIZATION_ERROR` when the removal is non-transactional and the creating transaction has not
committed, evaluated once outside the update lock (`:172`) and checked inside the update function (`:179`). The
model has no such refusal, and the `Assert_validateInfo, no creation CSN` witness needs it. Add
`"SERIALIZATION_ERROR"` to `FrameType`'s `err` domain and, in `StoreReadStep`, before the `ValidateInfoOK` test:

```tla
      creation_in_flight == /\ f.op = "RemovalTID" /\ f.val = NonTransactionalTID
                            /\ base.ccsn = UnknownCSN /\ base.ctid \in Tids
                            /\ LookupCsn(base.ctid) = UnknownCSN
                            /\ ~Witness("Assert_validateInfo_nocreation_only2")
                            /\ ~Witness("Assert_validateInfo_nocreation")
```

and a branch `ELSE IF creation_in_flight THEN part' = WithFrame(p, [f EXCEPT !.pc = "Error", !.err = "SERIALIZATION_ERROR"])`.
No frame in `Base` or `Merge` carries `val = NonTransactionalTID`, so both are unaffected; step 8 re-runs them.

- [ ] **Step 2: The batch's state and history**

In `Server.tla`, add `owner : FrameOwners` to `BatchType` and `owner |-> <<"Cleanup", 0>>` to `NoBatchRec` (any
value; it is read only while `active`). In `History.tla`, replace `NoBatch` and the `h.batch` clause of
`HistoryTypeOK`:

```tla
NoBatch == [targets |-> {}, before |-> [p \in Parts |-> <<EmptyInfo, EmptyInfo, EmptyTID>>]]
```

and in `HistoryTypeOK` add
`/\ h.batch.targets \subseteq Parts /\ h.batch.before \in [Parts -> (VersionInfoType \X VersionInfoType \X AllTids)]`.

Delete the stub `NtBatchStart(B)` and the `\E B \in SUBSET Parts : NtBatchStart(B)` disjunct from `NtNext`. The
target set of a batch is always computed by its caller (`removePartsFromWorkingSet`, `MergeTreeData.cpp:7034`,
or `MergeTreeData::Transaction::commit`, `MergeTreeData.cpp:11277`) and never chosen; a `\E` over `SUBSET Parts`
would add `2^|Parts|` branches the code does not have. The callers set `sys.nt_batch` and `h.batch` themselves,
with a shared operator:

```tla
\* NonTransactionalRemovalLocks constructed and its lock() loop about to start over B, on actor a.
\* removePartsFromWorkingSet filters out parts whose creation_csn is RolledBackCSN before the batch
\* (MergeTreeData.cpp:7043-7045); that filter is applied by the caller, not here.
StartBatch(a, B) ==
  /\ sys' = [sys EXCEPT !.nt_batch = [active |-> TRUE, targets |-> SetToSeq(B), cursor |-> 1,
                                      phase |-> "Lock", locked |-> {}, skipped |-> {}, owner |-> a]]
  /\ h' = [h EXCEPT !.batch = [targets |-> B,
                               before |-> [p \in Parts |-> <<part[p].mem, StoredRecord(p), part[p].lock>>]],
                    !.batch_outcome = "None"]
```

- [ ] **Step 3: The three batch steps and the end**

```tla
\* VersionInfo::isRemoved, src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:199
InfoIsRemoved(info) == info.rtid = NonTransactionalTID \/ info.ccsn = RolledBackCSN \/ info.rcsn /= UnknownCSN
\* VersionMetadata::isCreatedByUncommittedTransaction, VersionMetadata.cpp:137: a missing creation CSN is not
\* enough, the transaction log decides (upstream 65e4e2b5bf69). The witness of NtRefusalJustified is exactly the
\* pre-fix form, which trusts mem.creation_csn alone.
CreatedByUncommitted(p) ==
  /\ part[p].mem.ccsn = UnknownCSN
  /\ part[p].mem.ctid \in Tids
  /\ (Witness("NtRefusalJustified") \/ LookupCsn(part[p].mem.ctid) = UnknownCSN)

BatchTarget(p) == LET b == sys.nt_batch IN
  b.active /\ b.cursor \in 1..Len(b.targets) /\ b.targets[b.cursor] = p

\* The refusal branch shared by NtBatchPreflight and NtBatchLock: the destructor releases every lock the batch
\* still holds (NonTransactionalRemovalLocks::~NonTransactionalRemovalLocks, MergeTreeTransaction.cpp:106,
\* upstream 86b6861a1a8e) and nothing that was stored is undone, because store() drains as it goes.
RefuseBatch ==
  /\ part' = [q \in Parts |-> IF q \in sys.nt_batch.locked THEN [part[q] EXCEPT !.lock = EmptyTID] ELSE part[q]]
  /\ sys' = [sys EXCEPT !.nt_batch = NoBatchRec]
  /\ h' = [h EXCEPT !.batch_outcome = "Refused"]

\* NonTransactionalRemovalLocks::lock, MergeTreeTransaction.cpp:121-146, the two branches that are steps: the
\* already-removed skip (:131) and the uncommitted-creator refusal (:139). The third outcome, "proceed to
\* lockRemovalTID", is not a step of its own; NtBatchLock carries its guard.
NtBatchPreflight(p) ==
  /\ Up /\ sys.nt_batch.phase = "Lock" /\ BatchTarget(p)
  /\ LET b == sys.nt_batch IN
     \/ /\ InfoIsRemoved(part[p].mem)
        /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.skipped = @ \cup {p}, !.cursor = @ + 1,
                                                     !.phase = IF b.cursor = Len(b.targets) THEN "Store" ELSE "Lock",
                                                     !.targets = IF b.cursor = Len(b.targets) THEN SetToSeq(b.locked) ELSE @,
                                                     !.cursor = IF b.cursor = Len(b.targets) THEN Cardinality(b.locked) ELSE b.cursor + 1]]
        /\ UNCHANGED <<part, h>>
     \/ /\ ~InfoIsRemoved(part[p].mem) /\ CreatedByUncommitted(p)
        /\ RefuseBatch
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, stmt, mut, task>>

\* lockRemovalTID, VersionMetadata.cpp:195: a held lock or a non-zero removal CSN is SERIALIZATION_ERROR.
\* At the end of the lock phase the target list is replaced by the locked list, because store() drains
\* locked_parts and never revisits a skipped target (spec defect S7); it drains from the back, so the cursor
\* counts down.
NtBatchLock(p) ==
  /\ Up /\ sys.nt_batch.phase = "Lock" /\ BatchTarget(p)
  /\ ~InfoIsRemoved(part[p].mem) /\ ~CreatedByUncommitted(p)
  /\ LET b == sys.nt_batch
         last == b.cursor = Len(b.targets)
         nlocked == b.locked \cup {p} IN
     \/ /\ part[p].lock /= EmptyTID
        /\ RefuseBatch
     \/ /\ part[p].lock = EmptyTID
        /\ part' = [part EXCEPT ![p].lock = NonTransactionalTID]
        /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.locked = nlocked,
                                                     !.phase = IF last THEN "Store" ELSE "Lock",
                                                     !.targets = IF last THEN SetToSeq(nlocked) ELSE @,
                                                     !.cursor = IF last THEN Cardinality(nlocked) ELSE b.cursor + 1]]
        /\ UNCHANGED h
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, stmt, mut, task>>

\* NonTransactionalRemovalLocks::store, MergeTreeTransaction.cpp:150: pop the back, setAndStoreRemovalTID
\* through the three-step store, unlock in the SCOPE_EXIT. The tid is written before the unlock, which is why
\* LockConsistent is phase-aware.
NtBatchStore(p) ==
  /\ Up /\ sys.nt_batch.phase = "Store" /\ BatchTarget(p) /\ p \in sys.nt_batch.locked
  /\ LET b == sys.nt_batch
         a == b.owner IN
     \/ /\ ~HasFrame(p, a) /\ ApplyOp("RemovalTID", NonTransactionalTID, part[p].mem) /= part[p].mem
        /\ part' = StartFrame(p, a, "RemovalTID", NonTransactionalTID, FALSE)
        /\ UNCHANGED <<sys, h>>
     \/ /\ FrameDone(p, a, "RemovalTID", NonTransactionalTID)
        /\ part' = [part EXCEPT ![p].lock = EmptyTID]
        /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.locked = @ \ {p}, !.cursor = @ - 1]]
        /\ h' = [h EXCEPT !.removers[p] = @ \cup {NonTransactionalTID}]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, stmt, mut, task>>

NtBatchEnd ==
  /\ sys.nt_batch.active /\ sys.nt_batch.phase = "Store" /\ sys.nt_batch.cursor = 0
  /\ sys' = [sys EXCEPT !.nt_batch = NoBatchRec]
  /\ h' = [h EXCEPT !.batch_outcome = "Done"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, txn, client, stmt, mut, task>>
```

The double `!.cursor` assignment in `NtBatchPreflight`'s first branch is a transcription error left in
deliberately for the reviewer of this plan to catch: **write it once**, as
`!.cursor = IF b.cursor = Len(b.targets) THEN Cardinality(b.locked) ELSE b.cursor + 1`. SANY rejects a duplicate
field in an `EXCEPT`; the error class is `Error: The same field appears twice in a record constructor`.

**SANY-check before proceeding.**

- [ ] **Step 4: The non-transactional queries**

Add to `ClientPcs`: `"NtInsertWrite"`, `"NtDropWrite"`, `"NtDropPublish"`, `"NtDropFlip"`. All four actions run
on a session with **no** transaction, which is what a non-transactional query is.

```tla
\* INSERT outside a transaction: setAndStoreCreationTID(Tx::NonTransactionalTID), which sets creation_csn to
\* NonTransactionalCSN in the same update function (VersionMetadata.cpp:261). No covered parts, so no batch.
NtInsertWrite(k, p) ==
  /\ Up /\ ~HasTxn(k) /\ client[k].pc = "Idle"
  /\ part[p].pstate = "Absent" /\ IsBase(p)
  /\ disk' = DiskWithDir(p)
  /\ part' = [StartFrame(p, Sess(k), "CreateTID", NonTransactionalTID, FALSE) EXCEPT ![p].pstate = "Temporary"]
  /\ h' = [h EXCEPT !.creator[p] = NonTransactionalTID]
  /\ client' = [client EXCEPT ![k].pc = "NtInsertWrite", ![k].part = p]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, stmt, mut, task>>
\* deferrable stays TRUE: a never-transactional part with no txn_version.txt defers the record
\* (VersionMetadataOnDisk.cpp:206), which is the shape NoFalseCorruption's witness attacks.
NtInsertPublish(k, p) ==
  /\ client[k].pc = "NtInsertWrite" /\ client[k].part = p /\ sys.parts_lock = NoActor
  /\ FrameDone(p, Sess(k), "CreateTID", NonTransactionalTID)
  /\ part' = [part EXCEPT ![p].pstate = "Active"]
  /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>

\* DROP PARTITION without a transaction (spec's NtDropCover, written as the three steps the query has): the
\* empty covering part is written, published under lockParts with the covered parts as one batch, and the states
\* flip when the batch is done.
NtDropWrite(k, e) ==
  /\ Up /\ ~HasTxn(k) /\ client[k].pc = "Idle"
  /\ Covers[e] /= {} /\ part[e].pstate = "Absent"
  /\ \E q \in Covers[e] : part[q].pstate = "Active"
  /\ disk' = DiskWithDir(e)
  /\ part' = [StartFrame(e, Sess(k), "CreateTID", NonTransactionalTID, FALSE) EXCEPT
                ![e].pstate = "Temporary", ![e].payload = [ver |-> 0, tomb |-> TRUE]]
  /\ h' = [h EXCEPT !.creator[e] = NonTransactionalTID]
  /\ client' = [client EXCEPT ![k].pc = "NtDropWrite", ![k].part = e]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, stmt, mut, task>>
NtDropPublish(k, e) ==
  /\ client[k].pc = "NtDropWrite" /\ client[k].part = e /\ sys.parts_lock = NoActor
  /\ FrameDone(e, Sess(k), "CreateTID", NonTransactionalTID)
  /\ LET C == { q \in Parts : q \in Expand({e}) /\ q /= e /\ part[q].pstate \in {"Active", "Outdated"}
                              /\ part[q].mem.ccsn /= RolledBackCSN } IN
     /\ part' = [part EXCEPT ![e].pstate = "PreActive"]
     /\ stmt' = [stmt EXCEPT ![Sess(k)].precommitted = {e}, ![Sess(k)].covered = C]
     /\ StartBatch(Sess(k), C)
     /\ client' = [client EXCEPT ![k].pc = "NtDropFlip"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, mut, task>>
NtDropFlip(k, e) ==
  /\ client[k].pc = "NtDropFlip" /\ client[k].part = e /\ ~sys.nt_batch.active
  /\ \/ /\ h.batch_outcome = "Done"
        /\ PublishFlipEffect(Sess(k), e, stmt[Sess(k)].covered)
        /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
     \/ /\ h.batch_outcome = "Refused"
        /\ part' = [part EXCEPT ![e].pstate = "Outdated"]
        /\ stmt' = [stmt EXCEPT ![Sess(k)] = NoStmt]
        /\ sys' = [sys EXCEPT !.parts_lock = IF @ = Sess(k) THEN NoActor ELSE @]
        /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None", ![k].last_error = "SERIALIZATION_ERROR"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, mut, task>>
```

`NtDropPublish` does not take `parts_lock`; it must. Add `sys' = [sys EXCEPT !.parts_lock = Sess(k)]` and fold
it into `StartBatch`'s `sys'` assignment rather than writing two `sys'` conjuncts, which SANY accepts and TLC
then finds contradictory. Write `StartBatch(a, B, lock)` with the lock as a third argument, or set the lock
inside `StartBatch` unconditionally, since every caller of a batch holds `lockParts` in the code
(`removePartsFromWorkingSet` takes an `acquired_lock`, `Transaction::commit` takes one). Do the latter and say so
in the comment.

- [ ] **Step 5: The two batch properties**

```tla
\* spec #invariants-conflicts. Stated on the step that ends the batch with Refused: every target is exactly as
\* h.batch recorded it at the start. This is what the four upstream fixes about half-applied batches are for
\* (ab40e11d3c73, f8f46fb1eb14, 86b6861a1a8e, and ba2ee3239b8d for the memory-only stamp).
NtBatchRefusedUnchangedStep ==
  (sys.nt_batch.active /\ ~sys'.nt_batch.active /\ h'.batch_outcome = "Refused") =>
    \A p \in h.batch.targets :
      /\ part'[p].mem = h.batch.before[p][1]
      /\ StoredRecord(p)' = h.batch.before[p][2]
      /\ part'[p].lock = h.batch.before[p][3]
NtBatchRefusedUnchanged == [][NtBatchRefusedUnchangedStep]_vars

\* spec #invariants-conflicts: a refusal is justified by an uncommitted creator as the transaction log sees it,
\* or by a lock somebody else holds.
NtRefusalJustifiedStep ==
  (sys.nt_batch.active /\ ~sys'.nt_batch.active /\ h'.batch_outcome = "Refused") =>
    \E p \in h.batch.targets :
      \/ (part[p].mem.ccsn = UnknownCSN /\ part[p].mem.ctid \in Tids
          /\ LookupCsn(part[p].mem.ctid) = UnknownCSN)
      \/ part[p].lock /= EmptyTID
NtRefusalJustified == [][NtRefusalJustifiedStep]_vars
```

`StoredRecord(p)'` primes the disk and part variables inside; the same SANY caveat as task 1 step 5 applies.

- [ ] **Step 6: The scenario**

```tla
NtNext == \/ (\E k \in Sessions, p \in Parts : NtInsertWrite(k, p) \/ NtInsertPublish(k, p)
                                               \/ NtDropWrite(k, p) \/ NtDropPublish(k, p) \/ NtDropFlip(k, p))
          \/ (\E p \in Parts : NtBatchPreflight(p) \/ NtBatchLock(p) \/ NtBatchStore(p))
          \/ NtBatchEnd

\* the NonTxn scenario (spec matrix): Base + NtInsert, NtBatch*, NtDropCover + Cleanup*
NonTxnNext == BaseNext \/ NtNext \/ CleanupNext
NonTxnSpec == Init /\ [][NonTxnNext]_vars
```

`MC_NonTxn.tla`: `CoversDef == [p \in Parts |-> IF p = E THEN {P1, P2} ELSE {}]`, `SymSessions`, and
`NonTxnView` built from `MergeView`'s shape (frags in the fingerprint, because `Covers` is non-empty) with
`task` dropped (`Tasks = {}`) and `sys.nt_batch`, `h.batch`, `h.batch_outcome`, `h.removers` added.

`MC_NonTxn.cfg`: the `Base` lists plus `NtBatchRefusedUnchanged`, `NtRefusalJustified`, `NoFalseCorruption`,
`NoPrematureDelete`, `PinnedNotDeleted`, `NoLostVisibleData`; constants `Parts = {P1, P2, E}`, `Tasks = {}`,
`Covers <- CoversDef`, `SNAPSHOT_TARGETS = {}`, `SET_SNAPSHOT_PROTECTS = FALSE`, `Updater+GC` **off** (the
matrix does not list it for `NonTxn`), everything else as `Base`.

- [ ] **Step 7: Run**

`run_tlc.sh NonTxn`, 20-minute cap, expected green. Reduction order if it overruns: `Sessions = {k1}` first (the
batch's races are between a non-transactional query and a transaction, and one session can hold a transaction
while another query runs only if there are two, so check the bound contract carefully before keeping this), then
`TID_MAX = 2`. Record in `STATE_SPACE.md`.

A red on `SingleRemover` or `LockConsistent` here is interesting: both are `Base` properties whose
non-transactional clauses have never been exercised. Classify before touching anything.

- [ ] **Step 8: Witnesses, and the `Base` and `Merge` re-runs**

`run_tlc.sh Base` and `run_tlc.sh Merge`: green, counts within noise of the values tasks 2 and 3 recorded.
Step 1 and step 4 both touched shared definitions.

| Property | Witness name | Scenario | Expected |
|---|---|---|---|
| `NtBatchRefusedUnchanged` | `NtBatchRefusedUnchanged` | `NonTxn` | RED: `NtBatchStore` runs per part before the lock phase is done. Hook: let `NtBatchStore` fire in phase `Lock` on a target already in `locked` |
| `NtRefusalJustified` | `NtRefusalJustified` | `NonTxn` | RED: `CreatedByUncommitted` decides from `mem.ccsn = 0` alone, which is the hook written in step 3 |
| `NoFalseCorruption` | `NoFalseCorruption` | `NonTxn` | RED: the deferred-record and `NonTransactionalCSN` exemptions removed from `ValidateMetadataOK`, on a never-transactional part removed non-transactionally |
| `Assert_validateInfo` | `Assert_validateInfo_nocreation` | `NonTxn` | RED, two changes (step 1 and step 3's hooks); halves `_only1` and `_only2` GREEN |

The two-change witness needs both halves declared: `_only1` skips the creator refusal in `NtBatchPreflight`
(`CreatedByUncommitted` returns `FALSE` under it) and `_only2` skips the store refusal written in step 1. The
main name `Assert_validateInfo_nocreation` applies both. `witness.sh` finds the halves itself and requires both
green.

The spec's `NtBatchDone` row names scenario `NonTxnCrash`, which needs `Restart*`: add it to `WITNESSES.md`'s
deferred table with plan 3 as its destination, and do not define the property here.

- [ ] **Step 9: Commit**

```bash
git commit -m "tla(transactions): non-transactional queries, the removal batch, the NonTxn scenario" -- utils/tla/transactions docs/superpowers/plans
```

---

### Task 5: Budget, witness sweep and the four documents {#task-5}

**Files:**
- Modify: every `MC_*.cfg` whose bounds moved
- Modify: `utils/tla/transactions/README.md` (created by plan 1 task 4; if that task has not run, this task
  creates the file to plan 1's outline and fills the plan-2 rows)
- Modify: `STATE_SPACE.md`, `WITNESSES.md`, `FINDINGS.md`

- [ ] **Step 1: The full run table**

Run, one at a time, `-workers auto`: `Schema`, `BaseSmall`, `Base`, `SetSnapshot`, `SetSnapshotFixed`, `Merge`,
`NonTxn`. Record scenario, bounds, states generated, distinct states, time and result. `SetSnapshot` is the one
row whose result is `RED (expected, F2)`.

- [ ] **Step 2: The full witness sweep**

Every non-deferred row of `WITNESSES.md`, in its own scenario, including the `Base` rows (plan 1 measured them
before three tasks changed shared definitions) and both halves of every two-change witness. Expect the sweep to
take about 25 minutes, dominated by `Assert_validateInfo_removal` at 53M distinct states. Any row that is not
`RED` is a finding, not a rerun: classify it (class (b) or class (d)) and fix the hook or the property before the
task ends.

- [ ] **Step 3: The four documents**

`STATE_SPACE.md`: a section per new scenario, each with where the states come from, the reductions applied with
their measured effect, the bounds with their argument, and the final counts. The `Base` history keeps its
existing sections; add the line task 3 step 3 owes about the holder change.

`WITNESSES.md`: the new rows, the struck deferred rows (`NoDoubleRead`, `ActiveSetShape`,
`Assert_getOldestSnapshot`), the still-deferred ones (`AckedWriteIsDurable` plan 3, `NoAvoidableTermination`
plan 5, `NtBatchDone` plan 3, `ActiveSetShape`'s reservation clause plan 4, `RollbackNoLeak` and
`KillerNotStranded` as before), and a scenarios-used section naming the three new configurations and their
bounds.

`FINDINGS.md`: F2 complete with its trace, its C++ fix and its model variant; the spec defects S3 to S7 in
section 3; the model defects in section 2 with their placement (the `removeOldEntries` re-read, the task-driven
rollback machine, the one-part cleanup grab).

`README.md`: extend the code map with one row per action this plan defined, columns
`Action | C++ file | Function | Step boundary`, each checked against the function; extend the run table with
step 1's rows; add the new refinement parameters (`SNAPSHOT_TARGETS`, `SET_SNAPSHOT_PROTECTS`, one merge task
instead of two) to the refinement-parameters section; add F2 to the expected-red findings section.

- [ ] **Step 4: Commit**

```bash
git commit -m "tla(transactions): state-space budget, witness sweep and documents for plans 1 and 2" -- utils/tla/transactions docs/superpowers/plans
```

---

## Self-review {#self-review}

**Spec coverage.** The three scenario rows this plan owns name, between them: `SetSnapshot` (task 1),
`Cleanup*` (task 2), `Updater+GC`, that is `UpdRemoveOldEntriesSetTail` and `UpdRemoveOldEntriesDelete`
(task 1), `Merge*` with the task set and reservations (task 3), `NtInsert`, `NtBatch*` and `NtDropCover`
(task 4). Every one has a task. The properties those rows enable: `NoPrematureDelete` (task 2, expected red in
`SetSnapshot`, green and witnessed in `Merge`), `PinnedNotDeleted` (task 2), `NoLostVisibleData` (task 1 defines,
task 2 checks), `NoOutdatedLookup` (task 1, recorded as vacuous, S4), `ActiveSetShape` (task 3, its two-part
witness debt paid, its reservation clause deferred to plan 4 with a named destination), `NoDoubleRead` (task 3,
debt paid), `Atomicity` re-run in `Merge` (task 3), `NoFalseCorruption` (task 2 defines, task 4 witnesses),
`NtBatchRefusedUnchanged` and `NtRefusalJustified` (task 4), `Assert_getOldestSnapshot` (task 1, debt paid),
`Assert_validateInfo, no creation CSN` (task 4). `NtBatchDone` and `LogEntryNeeded` are named in the matrix for
`NonTxnCrash` and `Crash`, both plan 3, and are deferred with that destination rather than silently absent.

**Placeholder scan.** No step says "similar to task N", "add appropriate guards" or "TBD". Three steps name a
transcription hazard instead of hiding it: task 4 step 3's duplicated `!.cursor` field, task 4 step 4's missing
`parts_lock`, and task 3 step 6's `MergeFail` driver hole, each with the decision to take. Five steps are marked
**SANY-check before proceeding** with the error class to expect.

**Type consistency.** New fields are `sys.cleanup_part` (task 2), `task[i].result` and the widened `task[i].pc`
(task 3), `sys.nt_batch.owner` and the reshaped `h.batch` (task 4), each with its `TypeOK` clause and its `Init`
value named in the same step. New constants are `SNAPSHOT_TARGETS` and `SET_SNAPSHOT_PROTECTS`, each with an
`ASSUME` and with values added to every existing `.cfg` in the step that declares them. `FrameType.err` gains
`"SERIALIZATION_ERROR"` in task 4 step 1. Three steps change definitions that `Base` uses (`Frags` and
`h.content` in task 1, `CanBeRemovedImpl` in task 2, the commit and publish bodies and `RollbackFinalize` in
task 3, `StoreReadStep` in task 4), and each is followed by a `Base` re-run with the expected count stated:
unchanged for the extractions, moved and explained for the holder change.
