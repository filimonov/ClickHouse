# Witnesses of the TLA+ model of MergeTree transactions {#witnesses}

A property that no run can falsify proves nothing. Every property in `Invariants.tla` therefore has a *witness*:
a single named change to the model, guarded by `Witness("<name>")` in the action it changes, that must make that
property fail. `witness.sh` applies one witness, checks that one property and nothing else, and reports whether
the property went red.

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
plan that next touches the rollback actions. `FINDINGS.md`, section 3, carries the ruling.

## Witnesses deferred to a later plan {#witnesses-deferred-to-a-later-plan}

These properties are checked in `Base`, but the witness the design document names needs an action `Base` does not
enable, so the witness belongs to the plan that adds that action. Until then each property is checked without
ever having been shown to be falsifiable, which is what the witness contract exists to prevent. They are debts,
not results.

| Property | Witness the design document names | Action it needs | Deferred to |
|---|---|---|---|
| `AckedWriteIsDurable` | `CommitAck` moved before `CommitCreateCSN` and a `Fail` allowed after it | `Crash` | plan 3 |
| `ActiveSetShape`, reservation clause | two tasks reserving the same source | a second background task; the `Merge` scenario has one covering part and therefore one merge, so the clause is vacuous there | plan 4, with the `MergeMutation` scenario, where a merge and a mutation run side by side |
| `NoAvoidableTermination` | `Fail` allowed inside `afterCommit`, taking the server down with `down_cause = Other` | `ProcessDown` | plan 5; the design document's row names scenario `Base`, which does not enable `ProcessDown`, recorded as spec defect S2 in `FINDINGS.md` |
| `NoFalseCorruption` | `CleanupValidate` treats the deferred record as absent and a `NonTransactionalCSN` held only in memory as a disagreement, which is the `Witness("NoFalseCorruption")` hook in `ValidateMetadataOK` | a part that is BOTH involved in a transaction and carries a deferred record, which `NonTxn` cannot build: see the `NonTxn` section below | plan 3, with the `NonTxnCrash` scenario |
| `NtBatchDone` | `NtBatchStore` updates `mem` and skips the store for the covered parts of a non-transactional merge, which is the pre-fix defect of `ba2ee3239b8d` | `Restart*`, because the spec states the property on the stored record and the scenario it names is `NonTxnCrash` | plan 3 |

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

## Witnesses of the `SetSnapshot` scenario {#witnesses-setsnapshot}

This scenario has **two sets of bounds**, and the reason is in `FINDINGS.md` as spec defect S7. An exhaustive
run at the scenario matrix's `TID_MAX = 3` does not finish, so `MC_SetSnapshot` and `MC_SetSnapshotFixed` are
checked exhaustively at `TID_MAX = 2`, `CSN_MAX = 35`, two sessions, in about 65 seconds. A witness run stops at
the first violation and can afford bounds an exhaustive run cannot, so the witnesses that need a third
transaction are run against `MC_SetSnapshotWitness`, which is the same module at `TID_MAX = 3`, `CSN_MAX = 36`.
The gap between the two is debt B1.

`Assert_getOldestSnapshot` has three conjuncts and therefore three witness names, the way
`Assert_validateInfo` has three. Two of the three are red at the exhaustive bounds; the third is the one that
needs the third transaction.

| Conjunct | Witness name | The model change | Bounds | Result | States | Time |
|---|---|---|---|---|---|---|
| the running list and the snapshot bag have the same members | `Assert_getOldestSnapshot_size` | `Begin` joins `running_list` without writing `snapshots_in_use`, breaking at one site the lockstep `beginTransaction` keeps under one lock | exhaustive | RED | 2 | 1 s |
| each entry is the value `beginTransaction` inserted | `Assert_getOldestSnapshot_entry` | `SetSnapshot` moves the `snapshots_in_use` entry and leaves `protected_snapshot` where it was | exhaustive | RED | 14,408 | 2 s |
| the bag is sorted | `Assert_getOldestSnapshot` | `SetSnapshot` also rewrites `protected_snapshot`, and with it the entry, which is the change the design document names | witness | RED | 407,927 | 4 s |

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
| `Assert_TailPtrNotRegressing` | `Assert_getOldestSnapshot_entry` | the entry moves below the stored tail, and the next `removeOldEntries` computes a `getOldestSnapshot` below the `tail_ptr` it has already stored | exhaustive | RED | 153,609 | 3 s |

It shares a witness rather than having one of its own, which the contract allows: the change is a single named
one and the property it runs against is named on the command line. There is no separate hook, because the only
way to make the stored tail regress is to lower a running transaction's entry.

### The full sweep at the exhaustive bounds {#witnesses-setsnapshot-exhaustive}

Every witness of every property `MC_SetSnapshot.cfg` checks, at `TID_MAX = 2`, `CSN_MAX = 35`. Eighteen runs.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 39,745 | 2 s |
| `SingleRemover` | `SingleRemover` | **GREEN**, debt B1 | 12,766,764 | 1 min 26 s |
| `LockConsistent` | `LockConsistent` | RED | 181,555 | 3 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | RED | 228,776 | 3 s |
| `RollbackRestores` | `RollbackRestores` | RED | 430,129 | 5 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 6,203 | 2 s |
| `StableRead` | `StableRead` | RED | 239,629 | 3 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 5,857 | 1 s |
| `NoUncommittedRead` | `NoUncommittedRead` | **GREEN**, debt B1 | 12,766,806 | 1 min 28 s |
| `NoFutureRead` | `NoFutureRead` | RED | 25,443 | 2 s |
| `NoLostRead` | `NoLostRead` | **GREEN**, debt B1 | 12,191,660 | 1 min 24 s |
| `Atomicity` | `Atomicity` | RED | 322,895 | 4 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 23,120 | 1 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 23,472 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **did not finish**, debt B1; 40,439,049 distinct after 5 min and still growing | — | killed at 301 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | RED | 123,021 | 3 s |
| | `Assert_isVisible_fast_only1` | GREEN, as minimality requires | 9,311,054 | 1 min 05 s |
| | `Assert_isVisible_fast_only2` | GREEN, as minimality requires | 12,766,746 | 1 min 27 s |
| `Assert_getOldestSnapshot` | `_size`, `_entry`, sortedness | two RED here, one at the witness bounds | see above | |
| `Assert_TailPtrNotRegressing` | `Assert_getOldestSnapshot_entry` | RED | 147,288 | 2 s |

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
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot` | RED | 407,927 | 4 s |
| `SingleRemover` | `SingleRemover` | RED | 8,159,425 | 51 s |
| `NoUncommittedRead` | `NoUncommittedRead` | RED | 3,086,497 | 21 s |
| `NoLostRead` | `NoLostRead` | RED | 1,357,615 | 10 s |

The last three are `Base` rows, already red in `Base` at the same `TID_MAX`; they are listed because B1 names
them, and because running them here shows the scenario's extra actions do not get in their way.

`NoOutdatedLookup` is defined in `Invariants.tla` and is **not** in any `SetSnapshot` cfg. It is vacuous here,
and by inspection rather than by search: `UpdFinalizeUnknown(t)` is `FALSE` in this plan, so
`NoOutdatedLookupStep` is an implication with a false antecedent in every step. `assertTIDIsNotOutdated` is
reached only from `tryFinalizeUnknownStateTransactions` (`src/Interpreters/TransactionLog.cpp:387`) and from
`getCSNAndAssert` (`:645`), which has no caller in the tree, so no scenario without the unknown-state group can
falsify it. The design document lists it among the `SetSnapshot` scenario's properties anyway, which is spec
defect S6. `NoLostVisibleData` is now in the `Fixed` configurations and has its
witness in the cleanup section below; it is deliberately not in `MC_SetSnapshot.cfg`, which exists to produce
finding F2 and would otherwise stop on whichever property fires first.

## Witnesses of the cleanup thread {#witnesses-cleanup}

The cleanup thread adds three properties. Two of them need a shape the `SetSnapshot` bounds cannot build, so
they are shown in `SetSnapshotF2Fixed`: one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`,
`SNAPSHOT_TARGETS = {34}`, `SET_SNAPSHOT_PROTECTS = TRUE`. That is the configuration finding F2 needed, and the
reason is the same one: a part still visible to a running transaction at a lowered snapshot takes three
transactions and a target above `FirstCSN`. The gap is debt B2 in `FINDINGS.md`. All three witnesses run
against a `Fixed` configuration, so that the `SET TRANSACTION SNAPSHOT` defect does not fire first.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `NoPrematureDelete` | `NoPrematureDelete` | `CleanupGrab` asks `CanBeRemovedWith(p, tlog.latest_snapshot)` instead of `CanBeRemovedImpl`, which is `canBeRemoved` reading `getLatestSnapshot` where the code reads `getOldestSnapshot` | `SetSnapshotF2Fixed` | RED | 248,909 | 3 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | `CleanupGrab` drops the `part[p].pins = {}` guard, which is `grabOldParts` skipping the `isSharedPtrUnique` check at `MergeTreeData.cpp:4150` | `SetSnapshotFixed` | RED | 30,635 | 2 s |
| `NoLostVisibleData` | `NoLostVisibleData` | the same `latest_snapshot` change at the same site, observed as content a running transaction could read and then could not | `SetSnapshotF2Fixed` | RED | 241,736 | 3 s |

`PinnedNotDeleted` is red at the exhaustive bounds because it needs no visible part at all: a `SELECT` pin on an
`Outdated` part whose removal has committed is enough, and two transactions produce that. The other two were
run at the exhaustive bounds first and are green there, 13,664,284 and 13,664,666 distinct states in 96 and
98 seconds, which is what debt B2 records.

`PinnedNotDeleted` restates `CleanupGrab`'s own guard, so in every non-witness run it is a tautology and its
only content is the witness row above. It is stated anyway because the guard is a refinement decision that a
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
SymSessions`, `VIEW MergeView`, no faults. One set of bounds, not two: the scenario finishes exhaustively at
them in 49 seconds, so the witnesses run against the same configuration the green run uses. Why the bounds are
those and not the matrix's two sessions is in `STATE_SPACE.md`.

The four rows the scenario matrix names for `Merge`, and the two the deferred table above owed it:

| Property | Witness name | The model change | Result | States | Time |
|---|---|---|---|---|---|
| `NoDoubleRead` | `NoDoubleRead` | the two removal tests of `isVisible`'s fast path and the removal lookup of its slow path are skipped, so a reader sees `M12` and the sources it covers at once | RED | 1,270,978 | 10 s |
| `ActiveSetShape` | `ActiveSetShape` | `PublishFlip` does not outdate the covered parts, so the merge result goes `Active` over sources that are still `Active` | RED | 263,245 | 5 s |
| `Atomicity` | `Atomicity` | the slow path of `isVisible` decides from `mem` alone, skipping both `tid_to_csn` lookups | RED | 2,666,265 | 20 s |
| `NoPrematureDelete` | `NoPrematureDelete` | `CleanupGrab` asks `CanBeRemovedWith(p, tlog.latest_snapshot)` instead of `CanBeRemovedImpl` | RED | 431,931 | 5 s |

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

Every witness of every property `MC_Merge.cfg` checks. Nineteen runs, about nine minutes in all, of which one
row is five.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 13,914 | 2 s |
| `SingleRemover` | `SingleRemover` | RED | 4,568,647 | 35 s |
| `LockConsistent` | `LockConsistent` | RED | 45,237 | 2 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | **GREEN**, debt B3 | 5,921,770 | 44 s |
| `RollbackRestores` | `RollbackRestores` | RED | 254,238 | 4 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 2,119 | 1 s |
| `StableRead` | `StableRead` | RED | 1,898,738 | 15 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 2,315 | 1 s |
| `NoUncommittedRead` | `NoUncommittedRead` | **GREEN**, debt B3 | 5,196,830 | 39 s |
| `NoFutureRead` | `NoFutureRead` | RED | 1,248,697 | 10 s |
| `NoLostRead` | `NoLostRead` | RED | 794,561 | 8 s |
| `NoDoubleRead` | `NoDoubleRead` | RED | 1,270,978 | 10 s |
| `Atomicity` | `Atomicity` | RED | 2,666,265 | 20 s |
| `ActiveSetShape` | `ActiveSetShape` | RED | 263,245 | 5 s |
| `NoPrematureDelete` | `NoPrematureDelete` | RED | 431,931 | 5 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | RED | 7,088 | 2 s |
| `NoLostVisibleData` | `NoLostVisibleData` | RED | 3,173,837 | 23 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 5,643 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 6,386 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **GREEN**, debt B3 | 38,568,430 | 4 min 51 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | RED | 30,588 | 2 s |
| | `Assert_isVisible_fast_only1` | GREEN, as minimality requires | 3,892,113 | 31 s |
| | `Assert_isVisible_fast_only2` | GREEN, as minimality requires | 5,196,830 | 39 s |

`NoLostRead` and `SingleRemover` are worth a note, because both are green at `SetSnapshot`'s reduced bounds and
red here at bounds that are reduced too. The merge task is why: it is a second actor that removes parts and
outdates them without being a second session, so the shapes those two witnesses need are reachable with one
session and three transactions where two sessions and two transactions could not build them.

Three properties have no witness in this scenario and are not debts of it. `NoFalseCorruption` is the deferred
task-4 row; `AckedWriteIsDurable`, `NoAvoidableTermination` and `KillerNotStranded` are the deferred plan-3 and
plan-5 rows; `Assert_getOldestSnapshot` and `Assert_TailPtrNotRegressing` both need the `SetSnapshot` action,
and `SNAPSHOT_TARGETS` is empty here, so they are vacuous in `Merge` and are verified in the scenario that owns
them. `TypeOK`, `RollbackNoLeak` and `NoTaskDrivenRollback` have no witness by design: the first is a type
invariant, the second is the debt `FINDINGS.md` section 3 records, and the third is a bound guard rather than a
property of the server.

## Witnesses of the `NonTxn` scenario {#witnesses-nontxn}

`NonTxn` is two sessions, `Parts = {P1, P2, E}`, `Tasks = {}`, `SYMMETRY SymSessions`, no faults. No
exhaustive run of the undivided scenario finishes, so it is committed as two halves, `NonTxnDrop` and
`NonTxnInsert`, both at `TID_MAX = 2`, `CSN_MAX = 34` and both with `OBSOLETE_IS_ROLLED_BACK = TRUE`.
`STATE_SPACE.md` carries the split and the bounds argument. The drop half still does not finish at those
bounds, which is model defect `M15`, and what that costs the witnesses is debt B4.

The sweep below was run against the undivided `MC_NonTxn`, the module the first commit of this scenario
carried, and it is **not re-run after the split**. The half that every witness of this scenario needs is
`MC_NonTxnDrop`, because it is the half that has the removal batch, and that half has no finishing
configuration: a witness removes a guard, which widens the space a scenario already over budget, so the five
rows that were attempted there were killed unfired rather than turning red. Those five are named in debt B4
with the counts they reached, and B4 is closed in task 5 of this plan. Every other figure in the tables below
is the undivided module's, carried rather than re-measured, and the module no longer exists, so none of them
is reproducible as it stands; what a reader can reproduce is the verdict column, by re-running the row against
whatever configuration task 5 makes finish.

Two witnesses are the scenario's own, and one of them is a two-change witness.

| Property | Witness name | The model change | Result | States | Time |
|---|---|---|---|---|---|
| `NtBatchRefusedUnchanged` | `NtBatchRefusedUnchanged` | `NtBatchStore` fires in the `Lock` phase on a target that has just been locked, so the batch stores and unlocks as it goes instead of storing after the whole lock loop, and a conflict on a later target then refuses a batch whose earlier members are already removed | RED | 264,818 | 5 s |
| `NtRefusalJustified` | `NtRefusalJustified` | `CreatedByUncommitted` decides from `mem.creation_csn = 0` alone, without asking the transaction log, which is the pre-`65e4e2b5bf69` form | RED | 53,807 | 3 s |
| `Assert_validateInfo` | `Assert_validateInfo_nocreation` | two changes: `NtBatchPreflight` skips the uncommitted-creator refusal and `StoreRead` skips the `creation_in_flight` refusal of `setAndStoreRemovalTID`, so a non-transactional removal stores `removal_csn = NonTransactionalCSN` on a part whose `creation_csn` is zero | RED, in `MC_NonTxnWitness` | 43,851 | 2 s |
| | `Assert_validateInfo_nocreation_only1` | the skipped preflight refusal alone | does not finish, debt B5 | 24,335,282 after 240 s, no violation | |
| | `Assert_validateInfo_nocreation_only2` | the skipped store refusal alone | does not finish, debt B5 | 24,546,344 after 240 s, no violation | |

`Assert_validateInfo` is the one row that needs a second module, and the reason is now a different one from
the reason the first commit gave. It was that finding F6 violates the property on the baseline, so a witness run
would be trivially red and say nothing; both committed halves carry F6's fix, so that reason has lapsed and the
witness could run in `MC_NonTxnDrop`. What `MC_NonTxnWitness` still is, is the **undivided** scenario with the
fix applied, at `CSN_MAX = 35` rather than the halves' 34, and the witness is run there because that is the
configuration debt B5 is stated at: its two minimality halves are the runs that did not finish, and retrying
them anywhere else would not pay the debt. An exhaustive run there does not finish either, which is exactly the
exhaustive-versus-witness split of spec defect S7.

### The sweep at these bounds {#witnesses-nontxn-sweep}

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 65,913 | 4 s |
| `SingleRemover` | `SingleRemover` | RED | 611,359 | 10 s |
| `LockConsistent` | `LockConsistent` | RED | 59,879 | 4 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | RED | 462,289 | 8 s |
| `RollbackRestores` | `RollbackRestores` | RED | 243,311 | 7 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 6,530 | 2 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 6,762 | 2 s |
| `NoUncommittedRead` | `NoUncommittedRead` | **does not finish**, debt B4 | 44,213,335 after 600 s | |
| `NoPrematureDelete` | `NoPrematureDelete` | RED | 629,800 | 7 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | RED | 56,003 | 3 s |
| `NtBatchRefusedUnchanged` | `NtBatchRefusedUnchanged` | RED | 264,818 | 5 s |
| `NtRefusalJustified` | `NtRefusalJustified` | RED | 53,807 | 3 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 41,585 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 39,785 | 2 s |

`NoDoubleRead` and `NoFalseCorruption` are in the scenario's cfg and have no row, and neither absence is an
accident nobody looked at. `NoDoubleRead`'s run was killed at 69.8 million distinct states, which is a budget
verdict rather than a witness verdict. `NoFalseCorruption`'s run was re-made at the committed bounds and
reached 16.2 million distinct states in 140 seconds with no violation, and did not finish; that is consistent
with the structural-unreachability argument below, and it is not a green. Both rows belong to debt B4.

`NoFalseCorruption` is worth a note whatever that round finds, because the witness the spec names is not
reachable here at all and the reason is structural. Its target is "a never-transactional part removed
non-transactionally, whose record is deferred". Such a part has `creation_tid = NonTransactionalTID` and
`removal_csn = NonTransactionalCSN`, which is exactly the shape `VersionInfo::wasInvolvedInTransaction`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:58`) answers no to, and
`IMergeTreeDataPart::assertHasValidVersionMetadata` returns true for such a part before it validates anything.
The witness needs a part that is BOTH involved in a transaction and carries a deferred record, and no action of
this plan produces one: `deferrable` survives only a store whose result is uninvolved, so the store that would
make the part involved is the same store that writes the record to disk. `Crash` and `Restart*` can separate
the two, which is why the deferred table sends the row to plan 3 and the `NonTxnCrash` scenario.


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

`M13` is why the `SetSnapshot` family had to be re-measured at all, and why its witnesses had to be re-run
rather than carried over. The guard had been vacuous in every configuration with an empty `Tasks`, which is all
three `SetSnapshot` modules, so every one of their runs had explored behaviours the code forbids. A green
survives that narrowing, but a **red witness** does not: a witness that fired only through an interleaving the
guard excludes would be green on the repaired model, and that is an unverified property rather than a stale
number. Both sweeps were therefore re-run in full. Every verdict held: no witness that was red went green, and
no witness that was green went red. The counts moved by a few per cent, in the direction the narrowing
predicts, and the new ones are in the two sweep tables above.
