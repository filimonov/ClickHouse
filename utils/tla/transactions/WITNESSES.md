# Witnesses of the TLA+ model of MergeTree transactions {#witnesses}

A property that no run can falsify proves nothing. Every property in `Invariants.tla` therefore has a *witness*:
a single named change to the model, guarded by `Witness("<name>")` in the action it changes, that must make that
property fail. `witness.sh` applies one witness, checks that one property and nothing else, and reports whether
the property went red.

**Every row below was re-run after the cleanup split, on 2026-09-21.** The final-review fix commit split
`CleanupGrab` into `CleanupDecide`, `CleanupGrab` and `CleanupAbandon`, which moves every count in a scenario
that enables the cleanup group by about ten per cent, and the split holds the parts lock across two steps,
which removes interleavings, so a row that was red before it was not thereby red after it. The verdicts had to
be re-derived rather than carried, and that was debt `B6` in `FINDINGS.md`. The sweep re-ran 83 rows, the
cleanup-enabled scenarios `SetSnapshot`, `SetSnapshotWitness`, `SetSnapshotFixed`, `SetSnapshotF2Fixed`,
`Merge`, `MergeWitness`, `NonTxnDrop`, `NonTxnInsert` and `NonTxnWitness`, together with the minimality halves
of the three two-change witnesses. `NonTxnDropTwo` enables no cleanup group and its four rows were re-run as
the control the split needs, unchanged in colour and within a few hundred states of their recorded counts.
**No row changed colour**, and the counts and times in every table below are from that sweep. `Base` and
`BaseSmall` enable no cleanup group either, and their rows were not re-run.

The round after it separated the cleanup horizon from the log-retention horizon, which is the second half of
finding `F2`'s fix. That change is confined to the `SetSnapshot` family: outside it the two registries hold the
same value in every reachable state, and `NonTxnDrop` re-run as the control gave 1,246,141, inside the noise.
Six rows were re-taken there and none changed colour: the three cleanup witnesses and `SnapshotEntryOnly` in
the tables below, `Assert_TailPtrNotRegressing` under `Assert_getOldestSnapshot_entry`, and the sortedness
witness at the witness bounds.

Counts vary by a few hundred states between runs of the same configuration, which is visible in the control
rows and in the exhaustive figures; it is well below the ten per cent the split costs and it is not a change of
verdict.

The five rows the tables record as **killed unfired** or as **not finishing** were not re-run: they are stopped
by budget rather than by a verdict, they keep their documented counts, and they stay at their documented
placement in plan 5, task 4 (budget and calibration). They are `Assert_validateInfo_removal` in `SetSnapshot`,
in `SetSnapshotWitness` and in `MergeWitness`, `SingleRemover` in `NonTxnDropTwo`, and `NoDoubleRead` and
`NoUncommittedRead` in `NonTxnWitness`.

The witness contract is stated in the design document, section "Invariants and properties": the witness changes
one or two named actions, the run checks only the target property, other properties are expected to fail too and
are not the subject of the run, and a two-change witness must be minimal, meaning that restoring either change
alone makes the run green again.

## Running a witness {#running-a-witness}

```bash
./witness.sh <Scenario> <Property> [<WitnessName>] [workers]
```

`<WitnessName>` defaults to `<Property>`; it differs only where one property has several witnesses, as the
`Assert_validateInfo_*` variants do. The script derives `MC_W.cfg` from `MC_<Scenario>.cfg` by dropping every
property the scenario checks, setting `WITNESS_NAME`, and adding a single `INVARIANT` or `PROPERTY` line for the
target, and derives `MC_W.tla` from `MC_<Scenario>.tla`. `SPECIFICATION`, `SYMMETRY`, `VIEW` and every constant
are kept, so a witness run has its scenario's bounds. Each run works in `tmp/tla/w_<Scenario>_<WitnessName>/` and removes
its own `states` directory when it ends. The scenario is in the directory name so that a sweep of one scenario never overwrites another's log: the same witness name is used in several scenarios, and reading the wrong one is how a `Merge` result once got reported as a `Base` regression. Output is one line:

```
RED|GREEN|ERROR|TIMEOUT <Property> states=<distinct> time=<s>
```

`RED` means the witness fired, which is the outcome a witness must have. Exit status is 0 for `RED`, 1 for
`GREEN`, 2 for an error, a timeout, or a two-change witness that turned out not to be minimal. The default
timeout is 600 seconds and `WITNESS_TIMEOUT` overrides it.

A two-change witness declares its halves in the model under the names `<WitnessName>_only1` and
`<WitnessName>_only2`, each applying one of the two changes. After a red main run `witness.sh` finds those names
in the modules, runs both, and requires both to be green. That is the minimality check the contract asks for.

## Scenarios used {#scenarios-used}

`Base` is the scenario every witness below runs in: `Sessions = {k1, k2}`, `Parts = {P1, P2}`, `TID_MAX = 3`,
`CSN_MAX = 36`, `SYMMETRY SymSessions`, `VIEW BaseView`, no faults.

It ran at `TID_MAX = 2` and `CSN_MAX = 35` until this round, and three of the witnesses below could not reach
their target there. The part universe starts empty, so a part that a transaction other than its creator can see
costs one transaction to create and commit, which leaves exactly one where a second remover, an uncommitted
removal observed by a third party, and a read taken after a commit that followed another transaction's start
each need two. Those three ran in a separate `BaseWitness` scenario, identical but for the bounds. That is the
bound contract not being met: the contract allows a reduction below the matrix's `TID_MAX = 3` only while every
witness of every property the scenario checks stays red, and for those three it did not. `Base` now carries the
matrix bounds, `BaseWitness` is deleted, and its three rows are ordinary `Base` rows.

Plan 2 adds scenarios of its own, and three of them carry two sets of bounds because no exhaustive run finishes
at the matrix's, which is spec defect `S7`. The configurations the tables below use are:

| Configuration | Bounds | Used for |
|---|---|---|
| `SetSnapshot`, `SetSnapshotFixed` | two sessions, `Parts = {P1, P2}`, `TID_MAX = 2`, `CSN_MAX = 35` | the exhaustive runs and the witnesses that fire at them |
| `SetSnapshotWitness` | the same at `TID_MAX = 3`, `CSN_MAX = 36` | witnesses only; no exhaustive run finishes |
| `SetSnapshotF2`, `SetSnapshotF2Fixed` | one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`, `SNAPSHOT_TARGETS = {34}` | finding F2 and the two cleanup witnesses that need its shape |
| `Merge` | one session, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 3`, `CSN_MAX = 36` | the exhaustive run and most of its witnesses |
| `MergeWitness` | the same at two sessions | witnesses only; debt B3's three rows |
| `NonTxnDrop` | two sessions, `Parts = {P1, P2, E}`, `TID_MAX = 1`, `CSN_MAX = 34`, cleanup group on | the drop half's exhaustive run and sweep |
| `NonTxnDropTwo` | the same at `TID_MAX = 2` with the cleanup group off | the witnesses that need a second transaction |
| `NonTxnInsert` | two sessions, `TID_MAX = 2`, `CSN_MAX = 34`, cleanup group on | the insert half |
| `NonTxnWitness` | both halves, two sessions, `TID_MAX = 2`, `CSN_MAX = 35` | witnesses only; the rows that need the cleanup group and a second transaction |
| `NonTxnF2` | one session, one part, `TID_MAX = 2`, `CSN_MAX = 35`, `SNAPSHOT_TARGETS = {33, 34}` | finding F2 with a non-transactional creator, debt B2 |

Every one of them is argued in `STATE_SPACE.md`, which carries the measurements each bound rests on.

A fourth witness was distorted by the reduced bound rather than blocked by it. `Assert_validateInfo_removal` was
built as a two-change witness because the one change the design document names, `DropStore` not waiting for the
removal-TID store, left the run green at `TID_MAX = 2`; the second change stopped `EnrolBody` from starting that
store at all. At three the first change is red on its own, so the witness is now the one-change witness the
document names, the `EnrolBody` hook is gone, and so are the two minimality halves, which a one-change witness
does not have. What made the difference is a third transaction: the target shape needs a rollback to clear the
removal TID under a later remover, which two transactions cannot produce.

That row is now by far the most expensive witness in the table. It is red only after 53.1 million distinct
states, well above the 30 million a witness is budgeted, because the shape lies deep in the full state space
rather than near the root like every other row here. It was kept rather than deferred because it still finishes
in about five and a half minutes, so the budget it exceeds costs wall clock that a witness sweep can afford. A
full sweep of this table is about 15 minutes, and that one row is a third of it.

The cost of the third transaction is a factor of 13, from 2.16 million distinct states in 19 seconds to
28.6 million in about 4 minutes, and it bought a counterexample as well as the contract: `Atomicity` went red on
the first `Base` run at the new bounds. See `FINDINGS.md`, findings F1 and section 3, and `STATE_SPACE.md` for
the bounds argument.

## Witnesses of the Base properties {#witnesses-of-the-base-properties}

`States` is distinct states, `Time` wall clock. Every figure is approximate: a witness run stops at the first
violation, and how many states it has fingerprinted by then depends on the worker scheduling, so two runs of one
witness differ by more than the few states a green run differs by. Every figure here was re-taken on the tree
this file is committed with: model defect `M13` restored a guard that had been vacuous in `Base`, so the whole
sweep had to be run again rather than carried over. Every verdict held, and the counts came down by a few per
cent, which is the direction a restored guard predicts. The `Assert_isVisible_fast_only2` row is worth a glance
for a second reason: it explores the whole of `Base`, and 26,839,098 against the scenario's own 26,839,136 is
the multi-worker counting noise, so it doubles as a check that the run table's figure is where it says it is.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `ReadYourWrites` | `ReadYourWrites` | the `creation_tid = current_tid` clause is removed from the fast path of `isVisible`, so a transaction stops seeing the parts it created | `Base` | RED | 8,513 | 1 s |
| `StableRead` | `StableRead` | `SelectCheck` reads at `tlog.latest_snapshot` instead of the transaction's own snapshot | `Base` | RED | 366,278 | 5 s |
| `NoUncommittedRead` | `NoUncommittedRead` | the slow path of `isVisible` treats an unknown creation CSN as the reader's snapshot instead of looking the creator up in `tid_to_csn` | `Base` | RED | 1,020,241 | 8 s |
| `NoFutureRead` | `NoFutureRead` | `SelectCheck` compares against `tlog.latest_snapshot` when the part's creation CSN is unknown | `Base` | RED | 34,402 | 2 s |
| `NoLostRead` | `NoLostRead` | `SelectCapture` captures only `Active` parts, skipping the `Outdated` ones a transactional `DROP` has in flight | `Base` | RED | 501,796 | 5 s |
| `Atomicity` | `Atomicity` | the slow path of `isVisible` decides from `mem` alone, skipping both `tid_to_csn` lookups | `Base` | RED | 471,091 | 5 s |
| `ErrorIsAbsent` | `ErrorIsAbsent` | `RollbackOutdateCreated` leaves a part the rolled-back transaction created `Active` | `Base` | RED | 60,192 | 2 s |
| `RollbackRestores` | `RollbackRestores` | `RollbackRestore` leaves a part the transaction had outdated `Outdated` | `Base` | RED | 697,099 | 6 s |
| `SingleRemover` | `SingleRemover` | the compare-and-set in `DropEnrol` becomes an unconditional write of `lock`, so a second transaction overwrites a remover's lock and enrols its own removal | `Base` | RED | 2,260,914 | 17 s |
| `LockConsistent` | `LockConsistent` | `RollbackUnlock` clears the lock before the removal TID is cleared | `Base` | RED | 276,590 | 3 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | `CommitStoreCreation` stores `h.csn[t] + 1` instead of the transaction's CSN | `Base` | RED | 27,447 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | `CommitStoreCreation` stores `CSN_MAX`, so a later committed removal carries a smaller CSN | `Base` | RED | 33,350 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | `StoreDoneBody` lets `removeOldPart` return without waiting for the removal-TID store it started, so a rollback can clear the removal TID under a later remover and `CommitStoreRemoval` then computes a removal CSN on a record that has none | `Base` | RED | 51,268,074 | 5 min 13 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | two changes: `CommitStoreCreation` is skipped for a part the transaction both creates and removes, and `StoreRead` skips `validateInfo`, so `CommitStoreRemoval` publishes a removal CSN over an unknown creation CSN | `Base` | RED | 170,287 | 3 s |
| | `Assert_isVisible_fast_only1` | the skipped creation store alone | `Base` | GREEN, as minimality requires | 21,097,806 | 2 min 42 s |
| | `Assert_isVisible_fast_only2` | the skipped validation alone | `Base` | GREEN, as minimality requires | 26,839,098 | 3 min 23 s |
| `FlipAfterStores` | `FlipAfterStores` | `CommitFlip` may run while the store loops are still going | `Base` | RED | 7,710 | 1 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | `StoreRead` on a retry re-reads memory instead of the stored record, so an attempt that met no interference still sees a stale version | `Base` | RED | 337,861 | 4 s |


`RollbackNoLeak` has no row of its own because it is not a property of its own: the design document's
`RollbackRestores` row states an action property on `RollbackFinalize` and, as a state invariant, that no
part of `h_creating[t]` is ever in the visible-parts set of a read by another transaction.
`RollbackRestoresStep` is the first half and `RollbackNoLeak` is the second, stated more strongly, over
every uncommitted transaction rather than only the rolled-back ones. The row's witness, `RollbackRestore`
skipped, falsifies the first half; a witness for the second is writable within `Base` and belongs with the
task that next rewrites the rollback actions, which is plan 3, task 1 (Keeper faults at commit, the unknown-state pass, updater-driven commit and rollback).
`FINDINGS.md`, section 3, carries the ruling.

## Witnesses deferred to a later plan {#witnesses-deferred-to-a-later-plan}

These properties are checked in `Base`, but the witness the design document names needs an action `Base` does not
enable, so the witness belongs to the plan that adds that action. Until then each property is checked without
ever having been shown to be falsifiable, which is what the witness contract exists to prevent. They are debts,
not results.

| Property | Witness the design document names | Action it needs | Deferred to |
|---|---|---|---|
| `AckedWriteIsDurable` | `CommitAck` moved before `CommitCreateCSN` and a `Fail` allowed after it | `Crash` | plan 3, task 2 (the layered disk, `Fsync`, `Crash`, `ProcessDown`, the restart loader), which adds `Crash` |
| `ActiveSetShape`, reservation clause | two tasks reserving the same source | a second background task; the `Merge` scenario has one covering part and therefore one merge, so the clause is vacuous there | plan 4, task 4 (merges with mutations and the covering relation in range shape), which builds the `MergeMutation` scenario, where a merge and a mutation run side by side |
| `NoAvoidableTermination` | `Fail` allowed inside `afterCommit`, taking the server down with `down_cause = Other` | `ProcessDown` | plan 5, task 2 (`ProcessDown` policies `Terminate` versus `Retry`, `NoAvoidableTermination`, `KillRetry`, `Implicit`), which adds `ProcessDown`; the design document's row names scenario `Base`, which does not enable `ProcessDown`, recorded as spec defect S2 in `FINDINGS.md` |
| `NoFalseCorruption` | `CleanupValidate` treats the deferred record as absent and a `NonTransactionalCSN` held only in memory as a disagreement, which is the `Witness("NoFalseCorruption")` hook in `ValidateMetadataOK` | a part that is BOTH involved in a transaction and carries a deferred record, which `NonTxn` cannot build: see the `NonTxn` section below | plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), which builds the `NonTxnCrash` scenario |
| `NtBatchDone` | `NtBatchStore` updates `mem` and skips the store for the covered parts of a non-transactional merge, which is the pre-fix defect of `ba2ee3239b8d` | `Restart*`, because the spec states the property on the stored record and the scenario it names is `NonTxnCrash` | plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`) |

`NoDoubleRead` and `ActiveSetShape` came off this table with the `Merge` scenario, which is the first with a covering relation; both are red there and both have rows below. What stays of `ActiveSetShape` is its second clause, which one task cannot falsify.

`Assert_getOldestSnapshot` came off this table with the `SetSnapshot` scenario: the action its witness needs
now exists, and the row is in the scenario's own table below.

One further property checked in `Base` is not in the design document at all: not as a witness row, and not as a
property either. The witness contract says a property without a passing witness is not accepted into
`Invariants.tla`, so it was admitted against it. It stays, and the ruling and the witness the next spec
revision owes it are in `FINDINGS.md`, section 3. It is listed here so the gap is visible rather than silently
absent.

| Property | Status |
|---|---|
| `KillerNotStranded` | the property appears nowhere in the design document; added with the rollback-driver change of task 3. Its witness needs a rollback step that starts and never completes, so it goes to plan 5 |

`TypeOK` is a type invariant, not a behavioural property, and has no witness by design.

Four rows of this table are in the rosters of the `NonTxn` halves as well, and are deferred there for the same
reasons rather than silently absent: `AckedWriteIsDurable` to plan 3, task 2 (the layered disk, `Fsync`, `Crash`, `ProcessDown`, the restart loader),
`NtBatchDone` and `NoFalseCorruption` to plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), and
`NoAvoidableTermination` to plan 5, task 2 (`ProcessDown` policies `Terminate` versus `Retry`, `NoAvoidableTermination`, `KillRetry`, `Implicit`),
the third with an argument of its own in the `NonTxn` section below. `NoAvoidableTermination` is worth naming twice, because it is checked in both halves and in every other
scenario of this plan and is nowhere falsifiable: its witness needs `ProcessDown`, which no scenario before
plan 5, task 2 (`ProcessDown` policies `Terminate` versus `Retry`, `NoAvoidableTermination`, `KillRetry`, `Implicit`) enables, so what the invariant does until then is state that no modelled path terminates the server.
`RollbackNoLeak` and `KillerNotStranded` are the two rows below, and both are in the halves' rosters too.

## Witnesses of the `SetSnapshot` scenario {#witnesses-setsnapshot}

This scenario has **two sets of bounds**, and the reason is in `FINDINGS.md` as spec defect S7. An exhaustive
run at the scenario matrix's `TID_MAX = 3` does not finish, so `MC_SetSnapshot` and `MC_SetSnapshotFixed` are
checked exhaustively at `TID_MAX = 2`, `CSN_MAX = 35`, two sessions, in about two minutes. A witness run stops at
the first violation and can afford bounds an exhaustive run cannot, so the witnesses that need a third
transaction are run against `MC_SetSnapshotWitness`, which is the same module at `TID_MAX = 3`, `CSN_MAX = 36`.
The gap between the two is debt B1.

`Assert_getOldestSnapshot` has three conjuncts and therefore three witness names, the way
`Assert_validateInfo` has three. Two of the three are red at the exhaustive bounds; the third is the one that
needs the third transaction.

| Conjunct | Witness name | The model change | Bounds | Result | States | Time |
|---|---|---|---|---|---|---|
| the running list and the snapshot bag have the same members | `Assert_getOldestSnapshot_size` | `Begin` joins `running_list` without writing `snapshots_in_use`, breaking at one site the lockstep `beginTransaction` keeps under one lock | exhaustive | RED | 2 | 1 s |
| each entry is the value `beginTransaction` inserted | `Assert_getOldestSnapshot_entry` | `SetSnapshot` moves the `snapshots_in_use` entry and leaves `protected_snapshot` where it was | exhaustive | RED | 12,594 | 2 s |
| the bag is sorted | `Assert_getOldestSnapshot` | `SetSnapshot` also rewrites `protected_snapshot`, and with it the entry, which is the change the design document names | witness | RED | 406,870 | 4 s |

The third row is the only one that exercises a C++ assertion end to end, and it is the one that cannot be shown
at the exhaustive bounds: breaking sortedness needs a transaction that began above `FirstCSN`, so a committed
transaction has to precede the two that run concurrently. The second row's *clause* has no C++ counterpart,
because `protected_snapshot` is a model ghost; it is what makes "the entry did not follow the snapshot"
observable, and it must not be read as evidence that the first-class clause is reachable at two transactions.
Its *hook* is a different matter and is not ghost-only: moving the `snapshots_in_use` entry down while the
transaction's own snapshot moves with it is exactly what an in-place `setSnapshot`, one that writes the list
entry and does not re-sort, would do in the C++. That is why the same hook falsifies
`Assert_TailPtrNotRegressing`, which is a `LOGICAL_ERROR` on a modelled path rather than a ghost.

| Property | Witness name | The model change | Bounds | Result | States | Time |
|---|---|---|---|---|---|---|
| `Assert_TailPtrNotRegressing` | `Assert_getOldestSnapshot_entry` | the entry moves below the stored tail, and the next `removeOldEntries` computes a `getOldestSnapshot` below the `tail_ptr` it has already stored | exhaustive | RED | 148,291 | 3 s |

It shares a witness rather than having one of its own, which the contract allows: the change is a single named
one and the property it runs against is named on the command line. There is no separate hook, because the only
way to make the stored tail regress is to lower a running transaction's entry.

### The full sweep at the exhaustive bounds {#witnesses-setsnapshot-exhaustive}

Every witness of every property `MC_SetSnapshot.cfg` checks, at `TID_MAX = 2`, `CSN_MAX = 35`. Eighteen runs.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 40,999 | 2 s |
| `SingleRemover` | `SingleRemover` | **GREEN**, debt B1 | 14,289,634 | 2 min 58 s |
| `LockConsistent` | `LockConsistent` | RED | 185,335 | 3 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | RED | 229,632 | 4 s |
| `RollbackRestores` | `RollbackRestores` | RED | 483,866 | 7 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 5,159 | 2 s |
| `StableRead` | `StableRead` | RED | 232,658 | 5 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 6,159 | 3 s |
| `NoUncommittedRead` | `NoUncommittedRead` | **GREEN**, debt B1 | 14,288,648 | 1 min 40 s |
| `NoFutureRead` | `NoFutureRead` | RED | 22,260 | 3 s |
| `NoLostRead` | `NoLostRead` | **GREEN**, debt B1 | 13,828,282 | 1 min 34 s |
| `Atomicity` | `Atomicity` | RED | 311,788 | 7 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 26,440 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 24,272 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **did not finish**, debt B1; 40,439,049 distinct after 5 min and still growing | — | killed at 301 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | RED | 120,274 | 3 s |
|  | `Assert_isVisible_fast_only1` | GREEN, as minimality requires | 10,365,309 | 1 min 24 s |
|  | `Assert_isVisible_fast_only2` | GREEN, as minimality requires | 14,289,268 | 2 min 15 s |
| `Assert_getOldestSnapshot` | `_size`, `_entry`, sortedness | two RED here, one at the witness bounds | see above | |
| `Assert_TailPtrNotRegressing` | `Assert_getOldestSnapshot_entry` | RED | 152,085 | 3 s |

`Assert_validateInfo_removal` is worth a note. It is not slow; it explores more than the scenario itself does,
because the witness removes a wait and the truncation actions then multiply the behaviours it opens. It reached
40 million distinct states in five minutes on a scenario whose own state space is 12.8 million. Plan 1 found the
same witness needs three transactions, so it is expected green here and it is in B1 with the other three.

`TypeOK`, `RollbackNoLeak`, `AckedWriteIsDurable`, `ActiveSetShape`, `NoAvoidableTermination`,
`KillerNotStranded` and `NoDoubleRead` have no witness in this scenario for the reasons the two tables above
this section already give; none of those reasons changes here.

### The witness bounds {#witnesses-setsnapshot-witness-bounds}

`MC_SetSnapshotWitness`, `TID_MAX = 3`, `CSN_MAX = 36`. Only for witnesses; an exhaustive run there does not
finish, which is model defect M4.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot` | RED | 436,697 | 5 s |
| `SingleRemover` | `SingleRemover` | RED | 8,193,903 | 53 s |
| `NoUncommittedRead` | `NoUncommittedRead` | RED | 3,170,252 | 21 s |
| `NoLostRead` | `NoLostRead` | RED | 1,310,488 | 10 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **killed unfired**, 108,439,476 distinct after 674 s | — | |

The sortedness row is the one figure in this table that is not from the budget task's sweep: it was taken
earlier on the same tree, and the module has not changed since, so it stands as it is rather than being
re-taken.

The middle three are `Base` rows, already red in `Base` at the same `TID_MAX`; they are listed because B1 names
them, and because running them here shows the scenario's extra actions do not get in their way. The three
counts are task 5's re-runs on the committed tree, and they are a few per cent below the figures the scenario's
own task measured -- 8,159,425, 3,086,497 and 1,357,615 -- which is the direction model defect `M13`'s repair
predicts.

The last row is the one of B1's five that is still not paid. `Assert_validateInfo_removal` removes a wait, and
the truncation actions this scenario enables multiply the behaviours that opens: at the exhaustive bounds it
reached 40 million distinct states without firing, and here, at the witness bounds, 108 million in eleven
minutes. It is red in `Base`, at three transactions and the same witness, so the property is falsifiable and
the hook works; what is not shown is that it is falsifiable **in this scenario**. That is what moves to
plan 5, task 4 (budget and calibration), with the count.

`NoOutdatedLookup` is defined in `Invariants.tla` and is **not** in any `SetSnapshot` cfg. It is vacuous here,
and by inspection rather than by search: `UpdFinalizeUnknown(t)` is `FALSE` in this plan, so
`NoOutdatedLookupStep` is an implication with a false antecedent in every step. `assertTIDIsNotOutdated` is
reached only from `tryFinalizeUnknownStateTransactions` (`src/Interpreters/TransactionLog.cpp:387`) and from
`getCSNAndAssert` (`:645`), which has no caller in the tree, so no scenario without the unknown-state group can
falsify it. The design document lists it among the `SetSnapshot` scenario's properties anyway, which is spec
defect S6. `NoLostVisibleData` is now in the `Fixed` configurations and has its
witness in the cleanup section below; it is deliberately not in `MC_SetSnapshot.cfg`, which exists to produce
finding F2 and would otherwise stop on whichever property fires first.

### The fix variant of finding `F9` {#witnesses-f9-fix}

`REMOVAL_REFUSES_UNCOMMITTED_CREATION` is a fix variant rather than a baseline guard, and a fix variant that
does more than it needs to is as much a defect as one that does too little. The witness below is the second
kind of check: it narrows the fix to the half the finding's first shape alone would suggest and shows that the
narrowed fix is not enough.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `Assert_validateInfo` | `F9FixInFlightOnly` | the refusal `EnrolRefused` gains under the fix drops its `RolledBackCSN` half and refuses only a creation still in flight | `SetSnapshotF9SiblingFixed` | RED | 417,821 | 4 s |

The trace is `traces/f9-rollback-after-skip-window.txt`, 29 states: the creator rolls back after `DropLock` has
read its creation CSN and before the enrolment stores the removal TID, and the remover's commit then stamps a
removal CSN under a creation CSN of `RolledBackCSN`, inside the `noexcept` frame. It is not a minimality half
of any other witness; it is the argument for the predicate the fix uses, `isCreationCommitted`, rather than
the narrower "has not committed yet".

## Witnesses of the cleanup thread {#witnesses-cleanup}

The cleanup thread adds three properties. Two of them need a shape the `SetSnapshot` bounds cannot build, so
they are shown in `SetSnapshotF2Fixed`: one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`,
`SNAPSHOT_TARGETS = {34}`, `SET_SNAPSHOT_PROTECTS = TRUE`. That is the configuration finding F2 needed, and the
reason is the same one: a part still visible to a running transaction at a lowered snapshot takes three
transactions and a target above `FirstCSN`. The gap is debt B2 in `FINDINGS.md`. All three witnesses run
against a `Fixed` configuration, so that the `SET TRANSACTION SNAPSHOT` defect does not fire first.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `NoPrematureDelete` | `NoPrematureDelete` | `CleanupDecide` asks `CanBeRemovedWith(p, tlog.latest_snapshot)` instead of `CanBeRemovedImpl`, which is `canBeRemoved` reading `getLatestSnapshot` where the code reads `getOldestSnapshot` | `SetSnapshotF2Fixed` | RED | 196,609 | 3 s |
| `NoPrematureDelete` | `SnapshotEntryOnly` | `CleanupGrab` skips the revalidation `SET_SNAPSHOT_PROTECTS` adds, which reduces finding F2's fix to its `snapshots_in_use` half, the half the entry first proposed | `SetSnapshotF2Fixed` | RED | 205,424 | 3 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | `CleanupDecide` drops the `part[p].pins = {}` guard, which is `grabOldParts` skipping the `isSharedPtrUnique` check at `MergeTreeData.cpp:4150` | `SetSnapshotFixed` | RED | 38,655 | 2 s |
| `NoLostVisibleData` | `NoLostVisibleData` | the same `latest_snapshot` change at the same site, observed as content a running transaction could read and then could not | `SetSnapshotF2Fixed` | RED | 208,181 | 3 s |

`PinnedNotDeleted` is red at the exhaustive bounds because it needs no visible part at all: a `SELECT` pin on an
`Outdated` part whose removal has committed is enough, and two transactions produce that. The other two were
run at the exhaustive bounds first and are green there, 13,664,284 and 13,664,666 distinct states in 96 and
98 seconds, which is what debt B2 recorded. Those two figures are **not re-run after model defect `M13`**: they
are from the tree before `DropLock`'s `lockParts` guard was restored, on a scenario that now measures
12,766,799 rather than 13,622,631. What they say is still the reason B2 existed, and B2 is closed by
`MC_NonTxnF2` rather than by re-taking them.

`SnapshotEntryOnly` is the second witness of `NoPrematureDelete` and is not a minimality half of the first: it
removes the second half of the proposed fix rather than a guard of the baseline, and its red is finding F2's
second shape, the trace `traces/f2-cleanup-decision-grab-window.txt`. `FINDINGS.md`, finding F2, has the
argument.

`PinnedNotDeleted` restates `CleanupDecide`'s own guard, and the guard and the state change are now two steps,
so the property says the part had no pin at the moment it moved to `Deleting`. What keeps that true is not the
parts lock. Three actions add pins and only `SelectCapture` needs the lock; `MergeSelect` and
`RollbackCopyLists` do not. The reason those two cannot pin a part between the decision and the grab is what
`canBeRemoved` accepts: a part whose removal committed at or below the oldest snapshot, or whose creation
carries `RolledBackCSN`. Such a part is invisible at every **ordinary** snapshot, and a merge always reads at
one, because `MergeBegin` registers `latest_snapshot` the way a session's `Begin` does, so no merge can select
it; and `RollbackCopyLists` pins a transaction's lists at the start of the rollback, before the stamp that
makes the part removable. The qualifier is what finding `F8` costs the argument: at `EverythingVisibleCSN` a
rolled-back creation **is** visible, and a `SELECT` there does see it. That reader is the third action, the
one that takes the parts lock, so it cannot run inside the cleanup's hold, and whatever it captured before the
hold it pinned, which is what `CleanupDecide` then refuses on. A witness that raises the snapshot the decision compares against, which is what the
`NoPrematureDelete` and `NoLostVisibleData` hooks do, breaks that argument; neither of them checks this
property. In every non-witness run it is still a tautology and its only content is the witness row above.
`Invariants.tla` carries the same argument beside the property. It is stated anyway because the guard is a refinement decision that a
later task could change without noticing that nothing was checking it.

`NoFalseCorruption` is **vacuously green** in `SetSnapshotFixed` and in `SetSnapshotF2Fixed`: no
`CleanupDeleteFail` step is reachable in either, because a validation refusal needs a disagreement between the
in-memory and the stored record that no action of this plan can produce, and the filesystem-error disjunct is
`FALSE` until plan 5. The parts-lock conjunct the review round added to `CleanupDeleteFail` narrows the
property's antecedent, since `NoFalseCorruptionStep` is an implication whose antecedent is that action; that is
fidelity to `rollbackDeletingParts` taking `lockParts` at `MergeTreeData.cpp:4207`, not a weakening, and here it
changes nothing because the antecedent was already unreachable. The green says nothing about the property, and its only content is the witness, which is
deferred to task 4 of this plan and is in the deferred table above.

The state counts in the table above are **first-violation counts and are not reproducible**: a witness run
stops at the first violation, and how many states it has fingerprinted by then depends on how the workers
raced. A reviewer's re-run of the same three gave 259,738, 235,808 and 29,910. They are recorded to show the
order of magnitude, not as figures to match.


## Witnesses of the `Merge` scenario {#witnesses-merge}

`Merge` is one session, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 3`, `CSN_MAX = 36`, `SYMMETRY
SymSessions`, `VIEW MergeView`, no faults. The scenario finishes exhaustively at them in 49
seconds, so the sweep below runs against the same configuration the green run uses. Three of its witnesses are
green there and fire only at two sessions; `MC_MergeWitness` is that configuration, for `witness.sh` only,
which is debt `B3` and the section after the sweep. Why the exhaustive bounds are one session and not the
matrix's two is in `STATE_SPACE.md`.

The four rows the scenario matrix names for `Merge`, and the two the deferred table above owed it:

| Property | Witness name | The model change | Result | States | Time |
|---|---|---|---|---|---|
| `NoDoubleRead` | `NoDoubleRead` | the two removal tests of `isVisible`'s fast path and the removal lookup of its slow path are skipped, so a reader sees `M12` and the sources it covers at once | RED | 1,409,859 | 11 s |
| `ActiveSetShape` | `ActiveSetShape` | `PublishFlip` does not outdate the covered parts, so the merge result goes `Active` over sources that are still `Active` | RED | 291,656 | 4 s |
| `Atomicity` | `Atomicity` | the slow path of `isVisible` decides from `mem` alone, skipping both `tid_to_csn` lookups | RED | 2,891,864 | 21 s |
| `NoPrematureDelete` | `NoPrematureDelete` | `CleanupDecide` asks `CanBeRemovedWith(p, tlog.latest_snapshot)` instead of `CanBeRemovedImpl` | RED | 509,341 | 5 s |

`NoDoubleRead` and `ActiveSetShape` are the two rows this scenario exists to pay. Neither is writable in `Base`,
where `Covers` is empty and no two parts are related. `NoPrematureDelete` is red here without the lowered
snapshot `SetSnapshotF2Fixed` needs: a merge gives a running reader a part that is `Outdated` and removable
while it is still visible at the reader's own snapshot, which is a shape two sessions inserting and dropping
cannot build.

`FlipAfterStores` has one conjunct per actor that can reach `CommitFlipEffect`, a session through `CommitFlip`
and a background task through `MergeCommitFlip`, because `afterCommit` stores every CSN before the state flip on
whatever thread is committing. Quantifying over `Sessions` alone would have left a merge's own flip unobserved.
That the task conjunct has teeth of its own was measured rather than assumed: a scratch copy under `tmp/` with
the session conjunct removed, so that only the `Tasks` half is checked, is red under the same witness at
1,336,942 distinct states. The witness needs no second hook, because `MergeCommitFlip` carries the same
`Witness("FlipAfterStores")` disjunct the session's action does.

### The full sweep at these bounds {#witnesses-merge-full}

Every witness of every property `MC_Merge.cfg` checks: the twenty-four runs of the table below, about eleven
minutes in all, of which one row is five.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 15,261 | 2 s |
| `SingleRemover` | `SingleRemover` | RED | 5,191,758 | 36 s |
| `LockConsistent` | `LockConsistent` | RED | 51,917 | 2 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | **GREEN**, debt B3 | 6,856,950 | 48 s |
| `RollbackRestores` | `RollbackRestores` | RED | 261,859 | 4 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 2,154 | 1 s |
| `StableRead` | `StableRead` | RED | 2,042,447 | 15 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 1,768 | 2 s |
| `NoUncommittedRead` | `NoUncommittedRead` | **GREEN**, debt B3 | 6,124,691 | 43 s |
| `NoFutureRead` | `NoFutureRead` | RED | 1,374,035 | 10 s |
| `NoLostRead` | `NoLostRead` | RED | 792,896 | 7 s |
| `NoDoubleRead` | `NoDoubleRead` | RED | 1,409,859 | 11 s |
| `Atomicity` | `Atomicity` | RED | 2,891,864 | 21 s |
| `ActiveSetShape` | `ActiveSetShape` | RED | 291,656 | 4 s |
| `NoPrematureDelete` | `NoPrematureDelete` | RED | 509,341 | 5 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | RED | 11,506 | 3 s |
| `NoLostVisibleData` | `NoLostVisibleData` | RED | 3,771,760 | 27 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 6,450 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 6,300 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **GREEN**, debt B3 | 41,978,209 | 5 min 12 s |
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot_size` | RED | 2 | 0 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | RED | 27,775 | 2 s |
|  | `Assert_isVisible_fast_only1` | GREEN, as minimality requires | 4,532,152 | 34 s |
|  | `Assert_isVisible_fast_only2` | GREEN, as minimality requires | 6,124,691 | 44 s |

### The witness bounds, at two sessions {#witnesses-merge-witness-bounds}

`MC_MergeWitness` is `MC_Merge` at two sessions, for witness runs only; an exhaustive run there does not finish,
which is why the scenario itself is one session. It exists to pay debt B3, which is the three witnesses the one
session loses.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | RED | 1,751,581 | 14 s |
| `NoUncommittedRead` | `NoUncommittedRead` | RED | 6,164,810 | 46 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **killed unfired**, 34,184,268 distinct after 279 s | — | |

`NoSpuriousStaleVersion` is the row that mattered most, because at one session the store-interference machinery
is not exercised at all rather than merely under-exercised. A second session gives the second storer, and the
witness fires in fourteen seconds. `NoUncommittedRead` needs the third client transaction the merge task takes
away at one session, and a second session gives it too. `Assert_validateInfo_removal` is the expensive row
again, killed by the same rule as everywhere else; it is red in `Base` and it is placed in plan 5's budget and
calibration task with the count above.

`NoLostRead` and `SingleRemover` are worth a note, because both are green at `SetSnapshot`'s reduced bounds and
red here at bounds that are reduced too. The merge task is why: it is a second actor that removes parts and
outdates them without being a second session, so the shapes those two witnesses need are reachable with one
session and three transactions where two sessions and two transactions could not build them.

Four properties have no witness in this scenario and are not debts of it. `NoFalseCorruption` is the deferred
task-4 row; `AckedWriteIsDurable`, `NoAvoidableTermination` and `KillerNotStranded` are the deferred plan-3 and
plan-5 rows; `Assert_getOldestSnapshot` and `Assert_TailPtrNotRegressing` both need the `SetSnapshot` action,
and `SNAPSHOT_TARGETS` is empty here, so the two `SetSnapshot`-sited witnesses are vacuous in `Merge` and are
verified in the scenario that owns them. `Assert_getOldestSnapshot`'s third witness, `_size`, is not one of
those: its hook is in `Begin`, so it is live wherever a transaction begins. It is red here in two states, and
the row is in the sweep table above; it is also red in the `SetSnapshot` family and in the drop half. `TypeOK`, `RollbackNoLeak` and `NoTaskDrivenRollback` have no witness by design: the first is a type
invariant, the second is the debt `FINDINGS.md` section 3 records, and the third is a bound guard rather than a
property of the server.

## Witnesses of the `NonTxn` scenario {#witnesses-nontxn}

`NonTxn` is two sessions, `Parts = {P1, P2, E}`, `Tasks = {}`, `SYMMETRY SymSessions`, no faults. No exhaustive
run of the undivided scenario finishes, so it is checked as three committed configurations, and the witnesses
are run in all three plus the undivided witness module. `STATE_SPACE.md` carries the bounds and the argument
for each:

- `MC_NonTxnDrop`: the drop half with the cleanup group, `TID_MAX = 1`, `CSN_MAX = 34`, green at 1,246,158
  distinct states. The full roster, including the three cleanup properties.
- `MC_NonTxnDropTwo`: the drop half without the cleanup group, `TID_MAX = 2`, `CSN_MAX = 34`, green at
  47,958,711. The roster minus the three cleanup properties.
- `MC_NonTxnInsert`: the insert half with the cleanup group, `TID_MAX = 2`, `CSN_MAX = 34`, green at
  16,969,548.
- `MC_NonTxnWitness`: the undivided scenario at `TID_MAX = 2`, `CSN_MAX = 35`, for witness runs only, which is
  the two-bound-sets rule of spec defect `S7`. It is where the rows that need the cleanup group **and** a
  second transaction are shown.

All four carry `OBSOLETE_IS_ROLLED_BACK = TRUE`, finding F6's fix, because the baseline is red on
`Assert_validateInfo` without it and every witness run would stop there instead of on its own subject.

The sweep below **replaces** the one the first commit of this scenario took against the undivided `MC_NonTxn`,
which no longer exists; none of those figures was reproducible and they are not carried.

### The drop half with the cleanup group {#witnesses-nontxn-drop}

`MC_NonTxnDrop`, every witness of every property its cfg checks: the twenty-eight runs of the table below.
Twenty-seven of them take under three minutes in all; the twenty-eighth is `Assert_validateInfo_removal`, which
explores twenty-three times the scenario's own space. The count to quote is the table's: `witness.sh` names its
output directory after the witness, so `tmp/tla/w_<Scenario>_*` also holds any probe a reviewer ran by hand and
is not a census of the sweep.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 4,501 | 1 s |
| `LockConsistent` | `LockConsistent` | RED | 14,251 | 2 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 449 | 1 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 490 | 0 s |
| `NtRefusalJustified` | `NtRefusalJustified` | RED | 2,000 | 2 s |
| `NtBatchRefusedUnchanged` | `NtBatchRefusedUnchanged` | RED | 39,292 | 2 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | RED | 2,173 | 1 s |
| `RollbackRestores` | `RollbackRestores` | RED | 469,898 | 5 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | RED | 18,513 | 1 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 7,501 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_nocreation` | RED | 1,961 | 1 s |
|  | `Assert_validateInfo_nocreation_only1` | GREEN, as minimality requires | 658,225 | 7 s |
|  | `Assert_validateInfo_nocreation_only2` | GREEN, as minimality requires | 1,246,158 | 11 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | RED | 10,856 | 2 s |
|  | `Assert_isVisible_fast_only1` | GREEN, as minimality requires | 1,023,745 | 9 s |
|  | `Assert_isVisible_fast_only2` | GREEN, as minimality requires | 1,246,141 | 12 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | GREEN, paid in `MC_NonTxnWitness` | 1,246,124 | 11 s |
| `SingleRemover` | `SingleRemover` | GREEN, paid in `MC_NonTxnWitness` | 1,246,158 | 11 s |
| `NoPrematureDelete` | `NoPrematureDelete` | GREEN, paid in `MC_NonTxnWitness` | 1,246,158 | 11 s |
| `NoUncommittedRead` | `NoUncommittedRead` | GREEN, placed in plan 5 | 1,246,158 | 11 s |
| `NoDoubleRead` | `NoDoubleRead` | GREEN, placed in plan 5 | 966,300 | 9 s |
| `Atomicity` | `Atomicity` | GREEN, paid in `MC_NonTxnDropTwo` | 1,246,107 | 11 s |
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot_size` | RED | 2 | 1 s |
| `NoNtStoreError` | `NoNtStoreError`, an alias of `Assert_validateInfo_nocreation_only1` | RED | 2,286 | 1 s |
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot_entry` | GREEN, vacuously: the hook is in `SetSnapshot` and `SNAPSHOT_TARGETS` is empty here | 1,246,158 | 11 s |
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot` | GREEN, vacuously, same reason | 1,246,141 | 12 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | GREEN over the whole witness-mutated space; placed in plan 5 | 26,978,659 | 3 min 28 s |
| `NoFalseCorruption` | `NoFalseCorruption` | GREEN, structurally; deferred to plan 3 | 1,246,141 | 11 s |

Every green here is a full exploration rather than a run that ran out of budget, so each one is a verdict: the
witness does not fire at one transaction. That is the price of the bound `STATE_SPACE.md` argues, and every row
names where it is paid instead.

`NoNtStoreError` is the row that had no witness at all until the re-review found one. It is a bound guard
rather than a property of the server, and the change that falsifies it is one the sweep already carries: with
`NtBatchPreflight`'s uncommitted-creator refusal removed, which is `Assert_validateInfo_nocreation_only1`, a
target whose creation is still in flight reaches the store phase, where `setAndStoreRemovalTID` refuses it
instead and parks the batch's frame in `Error`. One change, two properties, two routes, so the witness is an
alias on that one site rather than a hook of its own, and `witness.sh NonTxnDrop NoNtStoreError` runs it.

Three more of the rows are worth a word. `Assert_getOldestSnapshot` has three witnesses and only one of them is live
here: `_size`'s hook is in `Begin`, so it fires in two states, while the other two are inside `SetSnapshot`,
which an empty `SNAPSHOT_TARGETS` disables. Their greens are vacuities and say nothing about the property;
both are red in the scenario that owns the action. `Assert_validateInfo_removal` is the row that explores far
past the configuration it belongs to: the witness removes a wait, and the behaviours that opens take it to
26,978,659 distinct states on a scenario whose own space is 1.2 million. It finished, with nothing left on the
queue, so its green is a verdict too, and the reason is the one `Base` already found: the shape needs three
transactions. It is placed in plan 5 with that count, which the cleanup-split re-run moved from 25,686,095 without moving
the verdict.

`Assert_validateInfo_nocreation` is the two-change witness debt B5 was about. Its minimality was unverified
because both half-runs had to be cut at 24 million distinct states in the undivided module; here the module
finishes, so both halves are green by exploration and the witness is minimal. B5 is closed.

### The drop half at two transactions {#witnesses-nontxn-drop-two}

`MC_NonTxnDropTwo`, the five rows the one-transaction half lost and this configuration can take.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `Atomicity` | `Atomicity` | RED | 168,130 | 4 s |
| `NoDoubleRead` | `NoDoubleRead` | GREEN | 22,070,439 | 2 min 59 s |
| `NoUncommittedRead` | `NoUncommittedRead` | GREEN | 47,958,942 | 6 min 33 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | GREEN | 47,958,749 | 6 min 37 s |
| `SingleRemover` | `SingleRemover` | **killed unfired**, 65,525,357 distinct after 540 s, queue 4.17M and growing | — | |

`SingleRemover`'s row was first cut at 33,336,890 states, which was premature: a witness on a scenario that
finishes may legitimately explore the whole of it, and this one finishes at 47,958,711. The re-run with a
fifteen-minute allowance settles it the other way: the witness-mutated space is **larger** than the scenario's,
and at 65,525,357 the queue was still growing, so the mutated space does not finish either. The row is a
placement in plan 5 with that count. It is red in `MC_NonTxnWitness` in six seconds and in `Base`, so what is
owed is the two-transaction drop configuration, not the property.

`Atomicity` is the row this configuration pays outright. The two `Assert_validateInfo_order` and `SingleRemover`
rows need the cleanup group as well, which this half does not have, and they are red in `MC_NonTxnWitness`
below. `NoDoubleRead` and `NoUncommittedRead` are green here as fully explored runs and are what is left of
debt B4.

### The insert half {#witnesses-nontxn-insert}

`MC_NonTxnInsert` is the half with a non-transactional `INSERT` and no removal batch. Two rows were open on it
and both are now run.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `Atomicity` | `Atomicity` | RED | 481,158 | 5 s |
| `ActiveSetShape` | `ActiveSetShape` | GREEN, vacuously | 16,969,569 | 2 min 20 s |

`ActiveSetShape`'s green is a statement about the half rather than about the property. `NtInsertWrite` requires
`IsBase(p)` and the only part with a non-empty `Covers` entry is `E`, which a non-transactional `DROP PARTITION`
creates and this half has no `DROP PARTITION`: part `E` is never created here, so no two parts ever overlap and
the invariant is vacuously true whatever the witness does to the publication. The same is true of the two batch
properties `NtBatchRefusedUnchanged` and `NtRefusalJustified`: both read `sys.nt_batch` in their antecedent, and
`NonTxnInsertNext` enables no `NtDrop*` step, so the batch never becomes active and neither can fail here.
`NoNtStoreError` was in this roster for the same reason and has been **removed from `MC_NonTxnInsert.cfg`**: a
structurally vacuous invariant in an exhaustive configuration is the silent green the witness contract exists to
prevent, and unlike the two above it has a witness that fires in the sibling half, red at 2,286 distinct states
in `NonTxnDrop`. The two that stay are kept so that the halves differ only in their `Next`, and their vacuity is
recorded here. All three are kept in the roster so that the two halves differ only in their `Next`,
and all three are red in a drop-half configuration. `ActiveSetShape` is also the property finding F5 violates,
which is why `MC_NonTxnF5` exists and why the property is not in `MC_NonTxnDrop`'s roster.

### The undivided module, at witness bounds {#witnesses-nontxn-witness}

`MC_NonTxnWitness`, `TID_MAX = 2`, `CSN_MAX = 35`, both halves, the cleanup group on. An exhaustive run there
does not finish, which is the whole point of having two sets of bounds.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `SingleRemover` | `SingleRemover` | RED | 628,262 | 6 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 43,037 | 3 s |
| `NoPrematureDelete` | `NoPrematureDelete` | RED | 795,095 | 7 s |
| `NoDoubleRead` | `NoDoubleRead` | **killed unfired**, 40,810,177 distinct after 342 s | — | |
| `NoUncommittedRead` | `NoUncommittedRead` | **killed unfired**, 33,628,474 distinct after 269 s | — | |

The three red rows are the ones that need a second transaction and the cleanup thread at once, and they fire in
seconds once they have both.

`NoDoubleRead` and `NoUncommittedRead` are the two rows of this scenario that no configuration pays. Both are
red in `Base` -- `NoUncommittedRead` at three transactions, `NoDoubleRead` in `Merge`, which is the scenario
with a covering relation -- so the properties are falsifiable and the hooks work; what is unshown is that they
are falsifiable with a non-transactional writer in the behaviour. Both are placed in plan 5's budget and
calibration task with the counts above, and they are the residue of debt B4.

`NoFalseCorruption` is worth a note whatever a later round finds, because the witness the spec names is not
reachable here at all and the reason is structural. Its target is "a never-transactional part removed
non-transactionally, whose record is deferred". Such a part has `creation_tid = NonTransactionalTID` and
`removal_csn = NonTransactionalCSN`, which is exactly the shape `VersionInfo::wasInvolvedInTransaction`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:58`) answers no to, and
`IMergeTreeDataPart::assertHasValidVersionMetadata` returns true for such a part before it validates anything.
The witness needs a part that is BOTH involved in a transaction and carries a deferred record, and no action of
this plan produces one: `deferrable` survives only a store whose result is uninvolved, so the store that would
make the part involved is the same store that writes the record to disk. `Crash` and `Restart*` can separate
the two, which is why the deferred table sends the row to plan 3 and the `NonTxnCrash` scenario. The green row
in the drop half is that argument measured, not a witness.

## Baseline after the witness work {#baseline-after-the-witness-work}

Both on the tree this file is committed with, at the bounds above.

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 28,552,935 | 4 min 06 s |

After the `SetSnapshot` work, which changed `Types.tla`, `Parts.tla`, `Server.tla` and `Invariants.tla`:

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 28,553,090 | 4 min 11 s |
| `SetSnapshot` | green at `TID_MAX = 2`, `CSN_MAX = 35` | 7,420,004 | 1 min 05 s |
| `SetSnapshotFixed` | green at the same bounds | 7,291,951 | 1 min 04 s |

After the merge work, which changed `Parts.tla`, `Server.tla`, `Invariants.tla` and `MergeTreeTransactions.tla`:

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 28,547,508 | 4 min 12 s |
| `SetSnapshot` | green at `TID_MAX = 2`, `CSN_MAX = 35` | 13,622,631 | 2 min 01 s |
| `SetSnapshotFixed` | green at the same bounds | 13,092,635 | 2 min 02 s |
| `SetSnapshotF2Fixed` | green | 367,183 | 4 s |
| `Merge` | green at one session, `TID_MAX = 3`, `CSN_MAX = 36` | 5,196,830 | 49 s |

The full `Base` sweep was re-run on that tree, because the extraction moved the operators six of its witnesses
hook into. All eighteen rows keep their verdict: fifteen red, `Assert_validateInfo_removal` red at 53,619,513
distinct states in 5 min 31 s against 53,133,545 in 5 min 21 s before, and both `Assert_isVisible_fast`
minimality halves green, at 22,549,585 and 28,547,605. The second of those two explores the whole of `Base`, so
it doubles as a check that the scenario's own count is where the run table says it is.

`SetSnapshotF2` is still red on `NoPrematureDelete`, and the three cleanup witnesses are still red, which is
what had to be shown after that property's antecedent was narrowed to the transactions that can still read.
`FINDINGS.md`, finding F3, carries the narrowing and its argument.

After the non-transactional work, which restored `DropLock`'s `lockParts` guard (model defect `M13`) and moved
the visibility oracle's ghost to the publishing step (`M16`):

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 26,839,136 | 3 min 56 s |
| `SetSnapshot` | green at `TID_MAX = 2`, `CSN_MAX = 35` | 12,766,799 | 1 min 55 s |
| `SetSnapshotFixed` | green at the same bounds | 12,236,834 | 1 min 52 s |
| `SetSnapshotF2Fixed` | green | 367,183 | 4 s |
| `Merge` | green at one session | 5,196,830 | 51 s |
| `NonTxnInsert` | green at `TID_MAX = 2`, `CSN_MAX = 34`, two sessions | 15,787,889 | 2 min 33 s |
| `NonTxnFixed` | green at one session | 841,907 | 8 s |


After the budget and sweep task, which changed no model file and added `MC_MergeWitness`, `MC_NonTxnDropTwo`
and `MC_NonTxnF2`:

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `Schema` | green | 1 | 1 s |
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 26,839,128 | 3 min 54 s |
| `Merge` | green at one session | 5,196,830 | 50 s |
| `SetSnapshotF2` | **red on `NoPrematureDelete`**, as expected | a first-violation count | 1 s |
| `NonTxnInsert` | green | 15,788,049 | 2 min 35 s |
| `NonTxnDrop` | green at `TID_MAX = 1` | 1,112,076 | 11 s |
| `NonTxnDropTwo` | green at `TID_MAX = 2`, no cleanup group | 47,958,711 | 7 min 34 s |
| `NonTxnF2` | **red on `NoPrematureDelete`** and, alone, on `NoLostVisibleData` | 9,132 and 9,193 | under 1 s each |

`M13` is why the `SetSnapshot` family had to be re-measured at all, and why its witnesses had to be re-run
rather than carried over. The guard had been vacuous in every configuration with an empty `Tasks`, which is all
three `SetSnapshot` modules, so every one of their runs had explored behaviours the code forbids. A green
survives that narrowing, but a **red witness** does not: a witness that fired only through an interleaving the
guard excludes would be green on the repaired model, and that is an unverified property rather than a stale
number. Both sweeps were therefore re-run in full. Every verdict held: no witness that was red went green, and
no witness that was green went red. The counts moved by a few per cent, in the direction the narrowing
predicts, and the new ones are in the two sweep tables above.

After the cleanup split, which is the re-run this file's opening records, on the tree that closes debt `B6`:

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `SetSnapshot` | green at `TID_MAX = 2`, `CSN_MAX = 35` | 14,289,310 | 2 min 03 s |
| `SetSnapshotFixed` | green at the same bounds | 13,607,915 | 2 min 02 s |
| `SetSnapshotF2` | **red on `NoPrematureDelete`**, as expected | a first-violation count | 3 s |
| `SetSnapshotF2Fixed` | green | 411,641 | 4 s |
| `Merge` | green at one session | 6,124,691 | 56 s |
| `NonTxnDrop` | green at `TID_MAX = 1` | 1,246,158 | 12 s |
| `NonTxnDropTwo` | green at `TID_MAX = 2`, no cleanup group | 47,958,902 | 7 min 29 s |
| `NonTxnInsert` | green | 16,969,548 | 2 min 42 s |
| `NonTxnF2` | **red on `NoPrematureDelete`** | a first-violation count | 2 s |
| `NonTxnF4` | **red on `NoLostVisibleData`** | a first-violation count | 2 s |
| `NonTxnF5` | **red on `ActiveSetShape`** | a first-violation count | 2 s |
| `NonTxnF6` | **red on `Assert_validateInfo`** | a first-violation count | 3 s |

The cleanup split is the same kind of change `M13` was, and it got the same treatment: the split holds the
parts lock from `CleanupDecide` to `CleanupGrab`, which removes interleavings, so a red witness that fired only
through one of them would be green on the split model. The whole sweep of every cleanup-enabled scenario was
therefore re-run rather than carried. Every verdict held: no witness that was red went green, and no witness
that was green went red. The counts moved by about ten per cent, in the direction the extra step predicts.
