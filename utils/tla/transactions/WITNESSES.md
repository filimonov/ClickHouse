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

`States` is distinct states, `Time` wall clock, both from the run of 2026-09-18 on the tree this file is
committed with. Every figure is approximate (multi-worker): under `-workers auto` two workers can fingerprint the
same state before either has inserted it, so consecutive runs of the same witness differ by a few states.

| Property | Witness name | The model change | Scenario | Result | States | Time |
|---|---|---|---|---|---|---|
| `ReadYourWrites` | `ReadYourWrites` | the `creation_tid = current_tid` clause is removed from the fast path of `isVisible`, so a transaction stops seeing the parts it created | `Base` | RED | 8,216 | 1 s |
| `StableRead` | `StableRead` | `SelectCheck` reads at `tlog.latest_snapshot` instead of the transaction's own snapshot | `Base` | RED | 375,186 | 5 s |
| `NoUncommittedRead` | `NoUncommittedRead` | the slow path of `isVisible` treats an unknown creation CSN as the reader's snapshot instead of looking the creator up in `tid_to_csn` | `Base` | RED | 1,135,111 | 9 s |
| `NoFutureRead` | `NoFutureRead` | `SelectCheck` compares against `tlog.latest_snapshot` when the part's creation CSN is unknown | `Base` | RED | 32,748 | 2 s |
| `NoLostRead` | `NoLostRead` | `SelectCapture` captures only `Active` parts, skipping the `Outdated` ones a transactional `DROP` has in flight | `Base` | RED | 514,938 | 6 s |
| `Atomicity` | `Atomicity` | the slow path of `isVisible` decides from `mem` alone, skipping both `tid_to_csn` lookups | `Base` | RED | 550,880 | 6 s |
| `ErrorIsAbsent` | `ErrorIsAbsent` | `RollbackOutdateCreated` leaves a part the rolled-back transaction created `Active` | `Base` | RED | 64,365 | 2 s |
| `RollbackRestores` | `RollbackRestores` | `RollbackRestore` leaves a part the transaction had outdated `Outdated` | `Base` | RED | 751,590 | 7 s |
| `SingleRemover` | `SingleRemover` | the compare-and-set in `DropEnrol` becomes an unconditional write of `lock`, so a second transaction overwrites a remover's lock and enrols its own removal | `Base` | RED | 2,462,558 | 18 s |
| `LockConsistent` | `LockConsistent` | `RollbackUnlock` clears the lock before the removal TID is cleared | `Base` | RED | 288,159 | 4 s |
| `Assert_validateInfo` | `Assert_validateInfo_creator` | `CommitStoreCreation` stores `h.csn[t] + 1` instead of the transaction's CSN | `Base` | RED | 35,093 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_order` | `CommitStoreCreation` stores `CSN_MAX`, so a later committed removal carries a smaller CSN | `Base` | RED | 34,843 | 2 s |
| `Assert_validateInfo` | `Assert_validateInfo_removal` | `StoreDoneBody` lets `removeOldPart` return without waiting for the removal-TID store it started, so a rollback can clear the removal TID under a later remover and `CommitStoreRemoval` then computes a removal CSN on a record that has none | `Base` | RED | 53,133,545 | 5 min 21 s |
| `Assert_isVisible_fast` | `Assert_isVisible_fast` | two changes: `CommitStoreCreation` is skipped for a part the transaction both creates and removes, and `StoreRead` skips `validateInfo`, so `CommitStoreRemoval` publishes a removal CSN over an unknown creation CSN | `Base` | RED | 196,417 | 3 s |
| | `Assert_isVisible_fast_only1` | the skipped creation store alone | `Base` | GREEN, as minimality requires | 22,554,686 | 2 min 51 s |
| | `Assert_isVisible_fast_only2` | the skipped validation alone | `Base` | GREEN, as minimality requires | 28,553,258 | 3 min 34 s |
| `FlipAfterStores` | `FlipAfterStores` | `CommitFlip` may run while the store loops are still going | `Base` | RED | 7,541 | 1 s |
| `NoSpuriousStaleVersion` | `NoSpuriousStaleVersion` | `StoreRead` on a retry re-reads memory instead of the stored record, so an attempt that met no interference still sees a stale version | `Base` | RED | 368,402 | 4 s |

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
| `Assert_getOldestSnapshot` | `SetSnapshot` also rewrites `protected_snapshot` | `SetSnapshot` | plan 2 |

Two further properties checked in `Base` are not in the design document at all: not as a witness row, and not
as properties either. The witness contract says a property without a passing witness is not accepted into
`Invariants.tla`, so both were admitted against it. They stay, and the ruling and the witnesses the next spec
revision owes them are in `FINDINGS.md`, section 3. They are listed here so the gap is visible rather than
silently absent.

| Property | Status |
|---|---|
| `RollbackNoLeak` | the property appears nowhere in the design document; added by task 1. A witness is writable within `Base` and belongs with the plan that next touches the rollback actions |
| `KillerNotStranded` | the property appears nowhere in the design document; added with the rollback-driver change of task 3. Its witness needs a rollback step that starts and never completes, so it goes to plan 5 |

`TypeOK` is a type invariant, not a behavioural property, and has no witness by design.

## Baseline after the witness work {#baseline-after-the-witness-work}

Both on the tree this file is committed with, at the bounds above.

| Scenario | Result | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | green | 47,381 | 1 s |
| `Base` | green | 28,553,114 | 4 min 08 s |
