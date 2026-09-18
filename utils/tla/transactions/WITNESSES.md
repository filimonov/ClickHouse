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
are kept, so a witness run has its scenario's bounds. Each run works in `tmp/tla/w_<WitnessName>/` and removes
its own `states` directory when it ends. Output is one line:

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
witness differ by more than the few states a green run differs by. The two rows marked `(re-run)` were taken on
the tree this file is committed with, after the final-review fix; the rest were taken on the tree of the witness
sweep, before it, and the fix changes no reachable `Base` state.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `ReadYourWrites` | `ReadYourWrites` | the `creation_tid = current_tid` clause is removed from the fast path of `isVisible`, so a transaction stops seeing the parts it created | `Base` | RED | 8,216 | 1 s |
| `StableRead` | `StableRead` | `SelectCheck` reads at `tlog.latest_snapshot` instead of the transaction's own snapshot | `Base` | RED | 375,186 | 5 s |
| `NoUncommittedRead` | `NoUncommittedRead` | the slow path of `isVisible` treats an unknown creation CSN as the reader's snapshot instead of looking the creator up in `tid_to_csn` | `Base` | RED | 1,135,111 | 9 s |
| `NoFutureRead` | `NoFutureRead` | `SelectCheck` compares against `tlog.latest_snapshot` when the part's creation CSN is unknown | `Base` | RED | 32,748 | 2 s |
| `NoLostRead` | `NoLostRead` | `SelectCapture` captures only `Active` parts, skipping the `Outdated` ones a transactional `DROP` has in flight | `Base` | RED | 514,938 | 6 s |
| `Atomicity` | `Atomicity` | the slow path of `isVisible` decides from `mem` alone, skipping both `tid_to_csn` lookups | `Base` | RED | 550,880 | 6 s |
| `ErrorIsAbsent` | `ErrorIsAbsent` | `RollbackOutdateCreated` leaves a part the rolled-back transaction created `Active` | `Base` | RED (re-run) | 64,836 | 2 s |
| `RollbackRestores` | `RollbackRestores` | `RollbackRestore` leaves a part the transaction had outdated `Outdated` | `Base` | RED | 751,590 | 7 s |
| `SingleRemover` | `SingleRemover` | the compare-and-set in `DropEnrol` becomes an unconditional write of `lock`, so a second transaction overwrites a remover's lock and enrols its own removal | `Base` | RED | 2,462,558 | 18 s |
| `LockConsistent` | `LockConsistent` | `RollbackUnlock` clears the lock before the removal TID is cleared | `Base` | RED | 288,159 | 4 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | `CommitStoreCreation` stores `h.csn[t] + 1` instead of the transaction's CSN | `Base` | RED | 35,093 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | `CommitStoreCreation` stores `CSN_MAX`, so a later committed removal carries a smaller CSN | `Base` | RED | 34,843 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | `StoreDoneBody` lets `removeOldPart` return without waiting for the removal-TID store it started, so a rollback can clear the removal TID under a later remover and `CommitStoreRemoval` then computes a removal CSN on a record that has none | `Base` | RED | 53,133,545 | 5 min 21 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | two changes: `CommitStoreCreation` is skipped for a part the transaction both creates and removes, and `StoreRead` skips `validateInfo`, so `CommitStoreRemoval` publishes a removal CSN over an unknown creation CSN | `Base` | RED | 196,417 | 3 s |
| | `Assert_isVisible_fast_only1` | the skipped creation store alone | `Base` | GREEN, as minimality requires | 22,554,686 | 2 min 51 s |
| | `Assert_isVisible_fast_only2` | the skipped validation alone | `Base` | GREEN, as minimality requires | 28,553,258 | 3 min 34 s |
| `FlipAfterStores` | `FlipAfterStores` | `CommitFlip` may run while the store loops are still going | `Base` | RED (re-run) | 8,017 | 2 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | `StoreRead` on a retry re-reads memory instead of the stored record, so an attempt that met no interference still sees a stale version | `Base` | RED | 368,402 | 4 s |


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
| `NoDoubleRead` | `SelectCheck` ignores `removal_csn` and `removal_tid` | `Merge` | plan 2 |
| `ActiveSetShape` | `PublishFlip` does not outdate the covered parts | `Merge`; the `Base` universe has no covering relation, so no two parts can be related | plan 2 |
| `NoAvoidableTermination` | `Fail` allowed inside `afterCommit`, taking the server down with `down_cause = Other` | `ProcessDown` | plan 5; the design document's row names scenario `Base`, which does not enable `ProcessDown`, recorded as spec defect S2 in `FINDINGS.md` |
| `NoFalseCorruption` | `CleanupValidate` treats the deferred record as absent and a `NonTransactionalCSN` held only in memory as a disagreement, which is the `Witness("NoFalseCorruption")` hook in `ValidateMetadataOK` | `NtInsert` and the non-transactional batch, so that a part carries a record the two exemptions are about | task 4 of this plan, with the `NonTxn` scenario |

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
| each entry is the value `beginTransaction` inserted | `Assert_getOldestSnapshot_entry` | `SetSnapshot` moves the `snapshots_in_use` entry and leaves `protected_snapshot` where it was | exhaustive | RED | 14,478 | 2 s |
| the bag is sorted | `Assert_getOldestSnapshot` | `SetSnapshot` also rewrites `protected_snapshot`, and with it the entry, which is the change the design document names | witness | RED | 438,838 | 4 s |

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
| `ErrorIsAbsent` | `ErrorIsAbsent` | RED | 45,066 | 2 s |
| `SingleRemover` | `SingleRemover` | **GREEN**, debt B1 | 7,420,048 | 53 s |
| `LockConsistent` | `LockConsistent` | RED | 183,212 | 3 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | RED | 226,752 | 3 s |
| `RollbackRestores` | `RollbackRestores` | RED | 432,161 | 5 s |
| `FlipAfterStores` | `FlipAfterStores` | RED | 6,199 | 2 s |
| `StableRead` | `StableRead` | RED | 233,924 | 4 s |
| `ReadYourWrites` | `ReadYourWrites` | RED | 6,387 | 2 s |
| `NoUncommittedRead` | `NoUncommittedRead` | **GREEN**, debt B1 | 7,419,992 | 53 s |
| `NoFutureRead` | `NoFutureRead` | RED | 26,447 | 2 s |
| `NoLostRead` | `NoLostRead` | **GREEN**, debt B1 | 6,815,478 | 50 s |
| `Atomicity` | `Atomicity` | RED | 306,925 | 4 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | RED | 23,535 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | RED | 23,492 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | **did not finish**, debt B1; 41,169,992 distinct after 4 min and still growing | — | killed at 300 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | RED | 129,849 | 2 s |
| | `Assert_isVisible_fast_only1` | GREEN, as minimality requires | 5,661,854 | 43 s |
| | `Assert_isVisible_fast_only2` | GREEN, as minimality requires | 7,420,024 | 54 s |
| `Assert_getOldestSnapshot` | `_size`, `_entry`, sortedness | two RED here, one at the witness bounds | see above | |
| `Assert_TailPtrNotRegressing` | `Assert_getOldestSnapshot_entry` | RED | 153,609 | 3 s |

`Assert_validateInfo_removal` is worth a note. It is not slow; it explores more than the scenario itself does,
because the witness removes a wait and the truncation actions then multiply the behaviours it opens. It reached
41 million distinct states in four minutes on a scenario whose own state space is 7.4 million. Plan 1 found the
same witness needs three transactions, so it is expected green here and it is in B1 with the other three.

`TypeOK`, `RollbackNoLeak`, `AckedWriteIsDurable`, `ActiveSetShape`, `NoAvoidableTermination`,
`KillerNotStranded` and `NoDoubleRead` have no witness in this scenario for the reasons the two tables above
this section already give; none of those reasons changes here.

### The witness bounds {#witnesses-setsnapshot-witness-bounds}

`MC_SetSnapshotWitness`, `TID_MAX = 3`, `CSN_MAX = 36`. Only for witnesses; an exhaustive run there does not
finish, which is model defect M4.

| Property | Witness name | Result | States | Time |
|---|---|---|---|---|
| `Assert_getOldestSnapshot` | `Assert_getOldestSnapshot` | RED | 438,838 | 4 s |
| `SingleRemover` | `SingleRemover` | RED | 7,261,414 | 48 s |
| `NoUncommittedRead` | `NoUncommittedRead` | RED | 2,909,770 | 20 s |
| `NoLostRead` | `NoLostRead` | RED | 1,313,160 | 10 s |

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
| `NoPrematureDelete` | `NoPrematureDelete` | `CleanupGrab` asks `CanBeRemovedWith(p, tlog.latest_snapshot)` instead of `CanBeRemovedImpl`, which is `canBeRemoved` reading `getLatestSnapshot` where the code reads `getOldestSnapshot` | `SetSnapshotF2Fixed` | RED | 238,916 | 3 s |
| `PinnedNotDeleted` | `PinnedNotDeleted` | `CleanupGrab` drops the `part[p].pins = {}` guard, which is `grabOldParts` skipping the `isSharedPtrUnique` check at `MergeTreeData.cpp:4150` | `SetSnapshotFixed` | RED | 30,526 | 3 s |
| `NoLostVisibleData` | `NoLostVisibleData` | the same `latest_snapshot` change at the same site, observed as content a running transaction could read and then could not | `SetSnapshotF2Fixed` | RED | 243,397 | 4 s |

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
`FALSE` until plan 5. The green says nothing about the property, and its only content is the witness, which is
deferred to task 4 of this plan and is in the deferred table above.

The state counts in the table above are **first-violation counts and are not reproducible**: a witness run
stops at the first violation, and how many states it has fingerprinted by then depends on how the workers
raced. A reviewer's re-run of the same three gave 259,738, 235,808 and 29,910. They are recorded to show the
order of magnitude, not as figures to match.

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
