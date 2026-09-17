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

`Base` is the scenario the properties are checked in: `Sessions = {k1, k2}`, `Parts = {P1, P2}`, `TID_MAX = 2`,
`CSN_MAX = 35`, `SYMMETRY SymSessions`, `VIEW BaseView`, no faults.

`BaseWitness` is the same set of actions with `TID_MAX = 3` and `CSN_MAX = 38`. It is never run green; it exists
because three `Base` witnesses cannot reach their target at `TID_MAX = 2`. The part universe starts empty, so a
part that a transaction other than its creator can see costs one transaction to create and commit, and two
transactions then leave exactly one where a second remover, an uncommitted removal observed by a third party, and
a read taken after a commit that followed another transaction's start each need two. The scenario matrix names
`TID_MAX = 3` for every scenario, and the bound contract allows the reduction to 2 only while every witness of
every property `Base` checks stays red, so `Base` at `TID_MAX = 2` does not currently satisfy that contract for
those three properties. Raising `Base` itself is a bounds decision: at `TID_MAX = 2` the green run is 2.16
million distinct states in 19 seconds, and the cost at 3 has not been measured.

## Witnesses of the Base properties {#witnesses-of-the-base-properties}

`States` is distinct states, `Time` wall clock, both from the run of 2026-09-18 on the tree this file is
committed with.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `ReadYourWrites` | `ReadYourWrites` | the `creation_tid = current_tid` clause is removed from the fast path of `isVisible`, so a transaction stops seeing the parts it created | `Base` | RED | 2,917 | 2 s |
| `StableRead` | `StableRead` | `SelectCheck` reads at `tlog.latest_snapshot` instead of the transaction's own snapshot | `Base` | RED | 95,571 | 2 s |
| `NoUncommittedRead` | `NoUncommittedRead` | the slow path of `isVisible` treats an unknown creation CSN as the reader's snapshot instead of looking the creator up in `tid_to_csn` | `BaseWitness` | RED | 1,096,164 | 9 s |
| `NoFutureRead` | `NoFutureRead` | `SelectCheck` compares against `tlog.latest_snapshot` when the part's creation CSN is unknown | `Base` | RED | 10,177 | 2 s |
| `NoLostRead` | `NoLostRead` | `SelectCapture` captures only `Active` parts, skipping the `Outdated` ones a transactional `DROP` has in flight | `BaseWitness` | RED | 540,353 | 6 s |
| `Atomicity` | `Atomicity` | the slow path of `isVisible` decides from `mem` alone, skipping both `tid_to_csn` lookups | `Base` | RED | 124,757 | 3 s |
| `ErrorIsAbsent` | `ErrorIsAbsent` | `RollbackOutdateCreated` leaves a part the rolled-back transaction created `Active` | `Base` | RED | 25,502 | 2 s |
| `RollbackRestores` | `RollbackRestores` | `RollbackRestore` leaves a part the transaction had outdated `Outdated` | `Base` | RED | 154,636 | 3 s |
| `SingleRemover` | `SingleRemover` | the compare-and-set in `DropEnrol` becomes an unconditional write of `lock`, so a second transaction overwrites a remover's lock and enrols its own removal | `BaseWitness` | RED | 2,574,008 | 19 s |
| `LockConsistent` | `LockConsistent` | `RollbackUnlock` clears the lock before the removal TID is cleared | `Base` | RED | 78,105 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | `CommitStoreCreation` stores `h.csn[t] + 1` instead of the transaction's CSN | `Base` | RED | 11,525 | 1 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | `CommitStoreCreation` stores `CSN_MAX`, so a later committed removal carries a smaller CSN | `Base` | RED | 9,992 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | two changes: `DropEnrol` takes the lock without starting the removal-TID store, and `DropStore` does not wait for that store, so `CommitStoreRemoval` later computes a removal CSN on a record with no removal TID | `Base` | RED | 43,866 | 1 s |
| | `Assert_validateInfo_removal_only1` | the enrolment change alone: the drop then waits forever for a store that was never started | `Base` | GREEN, as minimality requires | 234,326 | 4 s |
| | `Assert_validateInfo_removal_only2` | the wait change alone: the store still runs and still writes the removal TID | `Base` | GREEN, as minimality requires | 37,784,778 | 255 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | two changes: `CommitStoreCreation` is skipped for a part the transaction both creates and removes, and `StoreRead` skips `validateInfo`, so `CommitStoreRemoval` publishes a removal CSN over an unknown creation CSN | `Base` | RED | 47,558 | 2 s |
| | `Assert_isVisible_fast_only1` | the skipped creation store alone | `Base` | GREEN, as minimality requires | 1,654,789 | 14 s |
| | `Assert_isVisible_fast_only2` | the skipped validation alone | `Base` | GREEN, as minimality requires | 2,163,670 | 17 s |
| `FlipAfterStores` | `FlipAfterStores` | `CommitFlip` may run while the store loops are still going | `Base` | RED | 2,758 | 1 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | `StoreRead` on a retry re-reads memory instead of the stored record, so an attempt that met no interference still sees a stale version | `Base` | RED | 88,217 | 3 s |

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
| `NoAvoidableTermination` | `Fail` allowed inside `afterCommit`, taking the server down with `down_cause = Other` | `ProcessDown` | plan 5 |
| `Assert_getOldestSnapshot` | `SetSnapshot` also rewrites `protected_snapshot` | `SetSnapshot` | plan 2 |

Two further properties checked in `Base` have no witness row in the design document at all, so none was invented
here. They are listed so the gap is visible rather than silently absent.

| Property | Status |
|---|---|
| `RollbackNoLeak` | no witness in the design document, in `Base` or in any other scenario |
| `KillerNotStranded` | no witness in the design document; the property was added with the rollback-driver change of task 3 |

`TypeOK` is a type invariant, not a behavioural property, and has no witness by design.

## Baseline after the witness work {#baseline-after-the-witness-work}

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 2,163,765 | 19 s |
