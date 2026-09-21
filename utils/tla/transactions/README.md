# A TLA+ model of MergeTree transactions {#mergetree-transactions-tla}

A TLA+ specification of the `MergeTree` transaction algorithm, written to be checked with TLC against the
properties a user of transactions relies on. The design document is
`docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` and it is the authority: the action
names here are its action names, and a disagreement between this directory and that document is recorded in
`FINDINGS.md`, section 3, not resolved silently.

Baseline C++ throughout is upstream `master` `2c24b6b9291e`, checked out in this worktree. Every row of the code
map below was opened in that tree.

Companion files: `STATE_SPACE.md` (where the states come from and what the bounds cost), `WITNESSES.md` (the
witness table), `FINDINGS.md` (counterexamples, model defects, spec defects), `traces/` (counterexample traces).

## 1. Goal and scope {#goal-and-scope}

The goal is a specification faithful to the current C++ at the granularity where terminations and thread
interleavings matter, checked against the properties transactions promise: an acknowledged `COMMIT` is durable,
reads inside a transaction are snapshot-isolated, parts and transaction-log entries are collected only when
nobody can need them, two transactions cannot both remove the same part, and a transient fault never takes the
server down, because committed data unreadable until a restart is a loss of availability. The outcome is a list
of counterexample traces mapped back to C++ calls, or evidence that the bounded model has none.

The model is written so that a `ReplicatedMergeTree` model can later be built on the same Keeper and disk
modules. That extension is out of scope.

Covered by the first version of the design: one server, one Keeper, one local disk, one `MergeTree` table in an
`Atomic` database; the client operations inside an explicit transaction (`BEGIN`, `SET TRANSACTION SNAPSHOT`,
`INSERT`, `SELECT`, `ALTER TABLE DROP PARTITION`, `ALTER TABLE DETACH PARTITION`, `ALTER TABLE UPDATE` and
`DELETE`, `COMMIT`, `ROLLBACK`, and the automatic rollback on any exception); `KILL TRANSACTION` and
`KILL MUTATION` from another session; the legacy `txn_version.txt` format without `storing_version`; implicit
transactions; the same writing operations without a transaction; background merges and the mutation executor;
the transaction-log updating thread including log truncation, and the outdated-parts cleanup thread; the merge
blocker, the reservation set and the parts lock; failures (a lost Keeper response, an expired session, a server
termination with loss of every unsynced write, a disk write that throws, an exception thrown by a query between
any two steps); and server restart with metadata repair from disk and Keeper.

Not covered in the first version, listed so that a reader does not look for them:

- Every other partition operation: `ATTACH`, `MOVE`, `REPLACE`, `FETCH`, `FREEZE`, `UNFREEZE`, `DROP PART`,
  `DROP DETACHED`. The `detached/` directory itself is not modelled.
- `KILL QUERY`.
- Backups and restores.
- `SYSTEM` commands: `STOP MERGES`, `START MERGES`, `SYNC TRANSACTION LOG`, `RESTART REPLICA`, `DROP ... CACHE`.
- `OPTIMIZE`, `TRUNCATE`, `ALTER ... MODIFY` and the other metadata alters, projections, lightweight deletes and
  patch parts, `ReplacingMergeTree` and the other special engines.
- A transaction that touches two tables. `afterCommit` and `rollback` iterate over a set of storages and a
  failure between the per-storage mutation-CSN writes is a real window; a narrow two-table scenario is the first
  item of version 1.1.
- Disk corruption (bit flips or partial writes inside `txn_version.txt` and `mutation_N.txt`) and memory
  corruption. The disk module is designed so that corruption can be added as one more outcome of a write.
- `ReplicatedMergeTree`. The Keeper module is designed for it.
- Weak-memory effects. TLA+ models sequentially consistent shared state; the observable race, a lookup in
  `tid_to_csn` that misses a concurrent commit, is reproduced as an explicit interleaving instead.

What plan 1 installs is narrower still: the `Base` scenario, that is `BEGIN`, `INSERT`, `SELECT`,
`DROP PARTITION`, `COMMIT`, `ROLLBACK`, `KILL TRANSACTION`, the updating thread's load and publish steps, and
the three-step metadata store. Every other action of the design document is present as a stub whose body is
`FALSE`; the code map names the plan that fills each one in.

## 2. How to run {#how-to-run}

```bash
./utils/tla/transactions/run_tlc.sh <Scenario> [workers]
./utils/tla/transactions/witness.sh <Scenario> <Property> [<WitnessName>] [workers]
```

`run_tlc.sh` runs `MC_<Scenario>` with `-workers auto` unless a second argument overrides it. The scenarios that
exist today are `Schema` (one state, a type check), `BaseSmall` (one session), `Base` (two sessions, the
matrix bounds), `SetSnapshot` (`Base` plus `SET TRANSACTION SNAPSHOT`, the cleanup group and the updater's GC
group), `SetSnapshotFixed` (the same with `SET_SNAPSHOT_PROTECTS = TRUE`, the model variant of the fix proposed
in `FINDINGS.md`, finding F2) `SetSnapshotWitness` (the same as `SetSnapshot` at the scenario matrix's
bounds, for `witness.sh` only; an exhaustive run there does not finish), and the pair `SetSnapshotF2` and
`SetSnapshotF2Fixed` (one session, one part, three transactions and the snapshot target 34, the configuration
that reaches finding F2; the first is expected red on `NoPrematureDelete` and the second is green), `Merge`
(one session, `P1`, `P2` and the covering `M12`, one background task, plus the cleanup group and the updater's
GC group) and `MergeWitness` (the same at two sessions, for `witness.sh` only), `Keeper` (`Merge` without the
cleanup group, plus the updater's unknown-state pass and the two Keeper faults, at the matrix bounds),
`KeeperUnknownWait` (the same with `WAIT_MODE = "WAIT_UNKNOWN"`), `KeeperWitness` (the same at two sessions, for `witness.sh` only) and `KeeperUnknownWaitWitness` (two sessions
under `WAIT_UNKNOWN`, one part, no task, for `witness.sh` only), and the five of the `Crash` family: `Crash`,
`CrashUnsynced`, `CrashLegacy`, `CrashF10` and `CrashWitness`.

`Crash` is `Merge` plus the restart group, the three sync actions and the two ways the server goes down, on a
`Layered` disk at `RESTARTS_MAX = 1`, at the matrix bounds. It runs with `FSYNC_AFTER_INSERT`,
`FSYNC_OUTER_RENAME` and `FSYNC_PART_DIRECTORY` all on and is green with the whole roster. `CrashF10` is the
same configuration with the `txn_version.txt` rename left unsynced and is expected red on
`AckedWriteIsDurable`: a crash then leaves a part directory under its final name holding only a
`txn_version.txt.tmp`, which the loader reads as a rolled-back creation, so an acknowledged part is
reclassified and the cleanup thread may remove it. `FINDINGS.md`, finding `F10`, and `STATE_SPACE.md` carry why
the scenario splits at those constants rather than carrying one roster at all values, and `STATE_SPACE.md`
also states the two storage assumptions the restart loader makes, a writable local disk and a coverage
relation one level deep.

`CrashUnsynced` is the same scenario with all three settings off, which is the upstream default, and it is the
configuration the restart properties are stated over. It carries the whole roster less the rows that world
falsifies by construction, and `STATE_SPACE.md` names each of them with its finding. `CrashLegacy` is `Crash`
with `LEGACY_PARTS = {P1}` and the property `LegacyLoads`, the run that checks the pre-`storing_version`
metadata format through the loader. `CrashWitness` is `Crash` at two sessions, for `witness.sh` only.

Five more modules belong to the `SetSnapshot` family and carry the two reserved snapshots
`SET TRANSACTION SNAPSHOT` accepts beside an ordinary CSN, `NonTransactionalCSN = 1` and
`EverythingVisibleCSN = 3`. `SetSnapshotF2Special` is the fix variant at 1 and is green with the whole roster;
`SetSnapshotF2SpecialEV` is the fix variant at 3 and is green with `RollbackNoLeak` left out, which is the one
row finding `F8` falsifies; `SetSnapshotF8` is that finding, expected red on `RollbackNoLeak`.
`SetSnapshotF9Sibling` is finding `F9` at two sessions, expected red on `Assert_validateInfo`, and
`SetSnapshotF9SiblingFixed` is the same configuration under that finding's fix variant, expected green.
`FINDINGS.md` has both findings.

The non-transactional scenario is four modules, because no exhaustive run of the whole of it finishes:
`NonTxnDrop` (the `DROP PARTITION` and its removal batch with the cleanup group, one transaction),
`NonTxnDropTwo` (the same without the cleanup group, two transactions), `NonTxnInsert` (a non-transactional
`INSERT` with the cleanup group, two transactions) and `NonTxnWitness` (both halves at witness bounds, for
`witness.sh` only). Five more modules each produce one finding and are expected red or are a verified fix
variant: `NonTxnF4`, `NonTxnF5`, `NonTxnF6`, `NonTxnFixed` and `NonTxnF2`, the last being finding F2 reproduced
at two transactions with a non-transactional creator. `STATE_SPACE.md` has the bounds of each and why.

Every configuration of that scenario runs with `OBSOLETE_IS_ROLLED_BACK = TRUE`, and that has to be read with
its greens. The constant is finding `F6`'s proposed fix, which is **not** in upstream `master`: without it the
scenario is red on `Assert_validateInfo`, which is what `MC_NonTxnF6` shows. `MC_NonTxnDrop` and
`MC_NonTxnDropTwo` additionally leave `ActiveSetShape` out of their `INVARIANTS`, because finding `F5`
falsifies it and `MC_NonTxnF5` is where that is shown; `MC_NonTxnInsert` keeps it. So the greens of this
scenario are greens against a patched `master` with one known violation off the roster, not against `master` as
it stands. `FINDINGS.md`, findings `F5` and `F6`, and `STATE_SPACE.md` carry the detail.

`run_tlc.sh` downloads `tla2tools.jar` into `tmp/` if it is missing, and it uses `-Xmx16g` and a 45-minute
`timeout`.

Output goes under `tmp/tla/<Scenario>/`: the full TLC log is `tlc.log`, and a counterexample is additionally
extracted to `trace.txt`. The metadir lives in `tmp/tla/<Scenario>/states` while the run lasts and is removed
afterwards, so runs of two different scenarios never share a state directory. Two concurrent runs of the same
scenario do share it, and must not be started; `witness.sh` is the one that is per-run. Nothing under `tmp/` is committed.

Exit codes:

| Code | Meaning |
|---|---|
| 0 | green; TLC reported `Model checking completed. No error has been found` |
| 1 | a violation; the trace is in `tmp/tla/<Scenario>/trace.txt` |
| 2 | a parse or configuration error, a missing `MC_<Scenario>` module, a missing tool, or a timeout |

A timeout is not something to wait out. A run that passes 45 minutes, or 30 million distinct states, is a defect
of the model or of the bounds: kill it, record what it reached, and reduce. `SetSnapshot` hit that limit and is the reason
scenarios can now carry two sets of bounds: exhaustive bounds, where the scenario is checked, and witness
bounds, where a run that stops at the first violation can afford more. See `FINDINGS.md`, `M4`, `B1` and `S7`. The `Base` scenario reached
115,663,927 distinct states without finishing when it was first written, and the three model corrections that
fixed that are in `STATE_SPACE.md`.

`witness.sh` applies one witness, checks that one property and nothing else, and prints a single line
`RED|GREEN|ERROR|TIMEOUT <Property> states=<distinct> time=<s>`. `RED` is the outcome a witness must have, and
the exit status is 0 only for `RED`. It works in `tmp/tla/w_<Scenario>_<WitnessName>/`, so sweeps of two scenarios do not overwrite each other's logs. Its default timeout is 600 seconds and `WITNESS_TIMEOUT` overrides it. See
`WITNESSES.md`.

## 3. Code map {#code-map}

One row per action of `Server.tla` and `Parts.tla`. The C++ column names the file, the function and the step
boundary the action stands for: what has happened when the action fires, and what the next action of the same
machine picks up. Line numbers are of the baseline tree.

Citations into the model's own files name the operator, never a line number: an operator name survives an
insertion above it and a line number does not. Line numbers are used for the C++, whose baseline tree does not
move under the model.

Three rows have no single C++ counterpart and say so: `Fail` is an injected exception, `Refuse` bundles the
unwinding an exception does on its way out of a query, and `Fsync` is the page cache becoming durable.
`SelectFinish` also records the read into a monitor the server does not have, but returning a read set and
dropping the pins on it is a counterpart.

Where a row disagrees with the design document's action table, the row follows the code and the disagreement is
recorded in `FINDINGS.md`, section 3. Two rows do: `UpdLoadEntriesMap` (`S3`) and `CommitError` (`S4`). Four
actions also have no row of their own in that table, because it is coarser there: `DropLock` and
`StmtRollbackDrop` are the second halves of its `DropStart` and `StmtRollback` rows, and `RollbackReturn` and
`KillReturn` are the returns of calls it treats as atomic. `FINDINGS.md`, section 3, says why the `DropStart`
and `StmtRollback` splits were made; the reason for `RollbackReturn` and `KillReturn`, that
`TransactionLog::rollbackTransaction` runs on the caller's thread, is in section 8 below.

### Client and session {#code-map-client}

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `Begin(k)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::beginTransaction` (:399) | one step for the whole body, because the code holds `running_list_mutex` throughout: the snapshot is read, `local_tid_counter` is bumped, the snapshot is inserted into `snapshots_in_use` and the transaction into `running_list` |
| `SetSnapshot(k, c)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::setSnapshot` (:52), reached from `InterpreterTransactionControlQuery::executeSetSnapshot` (`src/Interpreters/InterpreterTransactionControlQuery.cpp:138`) | one relaxed store into `snapshot`. `protected_snapshot` and the `snapshots_in_use` entry are deliberately left where they were, which is finding F2; `SET_SNAPSHOT_PROTECTS` is the proposed fix. The model guards the action on a `Running` transaction, which is narrower than the code, and `FINDINGS.md` says why |
| `InsertWrite(k, p)` | `src/Storages/MergeTree/MergedBlockOutputStream.cpp` | the `MergedBlockOutputStream` constructor (:74) | the part directory exists and `setAndStoreCreationTID(tid)` has opened its store frame; the part is `Temporary` and the frame has not finished |
| `InsertPreActive(k, p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::renameTempPartAndReplaceImpl` (:6924) through `MergeTreeData::preparePartForCommit` (:6859) | the creation-TID store has finished; under `lockParts` the part becomes `PreActive` and `Transaction::addPart` puts it in `precommitted_parts` |
| `PublishStart(k, p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::commit(DataPartsLock &)` (:11219) | the parts lock is taken and held; `getActivePartsToReplace` plus `getCoveredOutdatedParts` filtered by `filterVisibleDataParts` give the covered set; `addNewPartAndRemoveCovered` has run its `MergeTreeTransaction::addNewPart`, so the part is in `creating_parts`, and the per-covered `removeOldPart` calls have not started |
| `PublishEnrol(k, q)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::removeOldPart` (:213), reached from `addNewPartAndRemoveCovered` (:179) | `mutex` taken, `checkIsNotCancelled` passed, `lockRemovalTID` won, `q` pushed to `removing_parts`; `setAndStoreRemovalTID` has not started. A `KILL TRANSACTION` that arrived between two covered parts is noticed here |
| `PublishStore(k, q)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::removeOldPart` (:213) | `setAndStoreRemovalTID(tid)` has finished and the `mutex` scope ends; the loop moves to the next covered part or leaves |
| `PublishFlip(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::commit(DataPartsLock &)` (:11219) | the `NOEXCEPT_SCOPE` after `removal_locks.store`: the new part becomes `Active`, every covered part `Outdated`, `clear` empties the statement transaction and the parts lock is released |
| `StmtRollbackMark(k, p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::rollback` (:11122) | one `setAndStoreCreationCSN(Tx::RolledBackCSN)` of the loop over `precommitted_parts`, which runs before the parts lock is taken |
| `StmtRollbackDrop(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::rollback` (:11122) | the parts lock is taken, `removePartsFromWorkingSet` outdates every precommitted part and `clear` empties the statement transaction; the outer transaction's own rollback follows |
| `SelectCapture(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::getVisibleDataPartsVector` (:9797) | `getDataPartsVectorForInternalUsage` has copied the `Active` and `Outdated` parts under `lockParts` and the lock is released; the copied shared pointers are what the model calls the `Select(k)` pins. Nothing has been filtered yet |
| `SelectCheck(k, p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::filterVisibleDataParts` (:9819) | one `VersionMetadata::isVisible(snapshot, tid)` call for one captured part, with no lock held, which is why any other action may run between two checks |
| `SelectFinish(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::filterVisibleDataParts` (:9819) | the `std::erase_if` has run over every captured part, so the read set is fixed and the pins are dropped. Recording that set into the read monitor is the model's own step; the server keeps no such record |
| `DropStart(k)` | `src/Storages/StorageMergeTree.cpp` | `StorageMergeTree::stopMergesAndWait` (:2775), from `StorageMergeTree::dropPartition` | the merge blocker is taken; the wait on `currently_merging_mutating_parts` has not finished |
| `DropLock(k)` | `src/Storages/StorageMergeTree.cpp` | `StorageMergeTree::dropPartition`, the branch taken when the query has a transaction | the wait is over; `lockParts` is taken and `getVisibleDataPartsVectorInPartition` under that lock has produced the set of parts to remove. The action splits it in two, as the two loops of `removePartsFromWorkingSet` do: the enrolment walks the parts whose `creation_csn` is not `Tx::RolledBackCSN` (`src/Storages/MergeTree/MergeTreeData.cpp:7044-7046`), and the whole set is outdated afterwards |
| `DropEnrol(k, q)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::removeOldPart` (:213), reached from `MergeTreeData::removePartsFromWorkingSet` (:7034) | `mutex` taken, `checkIsNotCancelled` passed, `lockRemovalTID` won, `q` pushed to `removing_parts`; `setAndStoreRemovalTID` has not started |
| `DropStore(k, q)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::removeOldPart` (:213) | `setAndStoreRemovalTID(tid)` has finished and the `mutex` scope ends; the loop moves to the next part of the batch |
| `DropOutdate(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::removePartsFromWorkingSet` (:7034) | the state loop after the removal metadata of the whole batch is written: every part of the batch becomes `Outdated`, still under the acquired parts lock. The model releases the merge blocker here; in the code the blocker's `ActionLock` is released a little later, when `dropPartition`'s scope ends |
| `CommitBefore(k)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::beforeCommit` (:284) | `waitForMutation` has returned for every attached mutation and the CAS `UnknownCSN -> CommittingCSN` has succeeded; no Keeper request has been made |
| `CommitError(k)` | `src/Interpreters/InterpreterTransactionControlQuery.cpp` | `InterpreterTransactionControlQuery::executeCommit` (:55) | the guard at (:64), which unless `getState` is `RUNNING` refuses a `COMMIT` on a transaction that `KILL TRANSACTION` already rolled back, with `INVALID_TRANSACTION`, before `commitTransaction` is called. The same error comes from the failed CAS in `beforeCommit` (:308) when the kill lands after the guard |
| `CommitCreateCSN(k)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::commitTransaction` (:419) | the commit point: the `multi` whose last request is the sequential `csn-` create has returned, and the allocated CSN is deserialized inside `NOEXCEPT_SCOPE_STRICT`. The two other outcomes of that request belong to plan 3 |
| `CommitReadOnly(k)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::commitTransaction` (:419) | the `isReadOnly` branch: no Keeper request at all, and `finalizeCommittedTransaction` takes the transaction's own snapshot as the commit timestamp |
| `CommitStoreCreation(k, p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::afterCommit` (:321) | one `setAndStoreCreationCSN(assigned_csn)` of the loop over `created_parts`. The action is two steps, one that opens the store frame and one that consumes it |
| `CommitStoreRemoval(k, p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::afterCommit` (:321) | one `setAndStoreRemovalCSN(assigned_csn)` of the loop over `removed_parts`, which runs only after the creation loop has finished |
| `CommitFlip(k)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::afterCommit` (:321) | both store loops are done: `csn.exchange(assigned_csn)` and the `csn.notify_all` that releases `waitStateChange` |
| `CommitFinalize(k)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::finalizeCommittedTransaction` (:490) | `afterCommit` has returned: under `running_list_mutex` the snapshot leaves `snapshots_in_use` and the transaction leaves `running_list`, then `afterFinalize` clears the part lists |
| `CommitAck(k)` | `src/Interpreters/InterpreterTransactionControlQuery.cpp` | `InterpreterTransactionControlQuery::executeCommit` (:55) | `waitForCSNLoaded(csn)` has returned unless the wait mode is `ASYNC`, and `setCurrentTransaction(NO_TRANSACTION_PTR)` detaches the transaction from the session. This is the step the client's `Acked` outcome is recorded on |

### Failure, rollback and kill {#code-map-rollback}

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `FrameFail(k)` | `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp` | `VersionMetadata::updateInfoWithRefreshDataThenStoreAndSetMetadata` (:324) | a store the session owns has thrown, either `STALE_VERSION` after the retry loop is exhausted (:357) or the `LOGICAL_ERROR` `validateInfo` raises (:345); the exception leaves the store and reaches the query |
| `Refuse(k)` | no single function | the unwind of the failing query | everything the query holds is given back before the rollback starts: the parts lock, the merge blocker and the transaction mutex by their guards, the store frame with the exception, and `MergeTreeData::Transaction::~Transaction` (:11110) runs the statement rollback when parts are still precommitted |
| `Fail(k)` | none | none (model only) | an injected exception between two steps of a query, counted by `QUERY_FAULTS_MAX`. It stands for a disk write fault outside a `noexcept` scope and for any unrelated exception the model does not name |
| `QueryOnCancelled(k)` | `src/Interpreters/executeQuery.cpp` | `executeQueryImpl` (:2692) | the `ROLLED_BACK && !is_special_query` guard refuses any query other than a transaction-control statement with `INVALID_TRANSACTION`, before the interpreter runs; the transaction stays bound to the session |
| `RollbackStart(k)` | `src/Interpreters/InterpreterTransactionControlQuery.cpp` | `InterpreterTransactionControlQuery::executeRollback` (:118), then `TransactionLog::rollbackTransaction` (:538) and `MergeTreeTransaction::rollback` (:377) | the explicit `ROLLBACK`: a `COMMITTED` or `COMMITTING` transaction is refused with `LOGICAL_ERROR`, an already rolled back one is only detached, and a `RUNNING` one reaches the CAS `UnknownCSN -> RolledBackCSN` and the `csn.notify_all` that follows it. The rollback body has not begun |
| `RollbackOnException(k)` | `src/Interpreters/executeQuery.cpp` | the exception callback (:3372) calling `MergeTreeTransaction::onException` (:467) | the same CAS, reached from a query that failed instead of from a statement; the session keeps the transaction bound, so the client still owes an explicit `ROLLBACK` |
| `RollbackReturn(k)` | `src/Interpreters/InterpreterTransactionControlQuery.cpp` | `InterpreterTransactionControlQuery::executeRollback` (:118), or the exception callback (:3372) | `rollbackTransaction` has returned, so the body it drove is finished. The explicit `ROLLBACK` then detaches with `setCurrentTransaction(NO_TRANSACTION_PTR)`; `onException` does not detach |
| `KillTransaction(k, t)` | `src/Interpreters/InterpreterKillQueryQuery.cpp` | `InterpreterKillQueryQuery::execute`, the `ASTKillQueryQuery::Type::Transaction` branch (:382) | `tryGetRunningTransaction(tid_hash)` found the victim and `onException` has won the CAS on the killer's own thread. The victim may be the killer's own transaction, and the kill does not detach it |
| `KillReturn(k)` | `src/Interpreters/InterpreterKillQueryQuery.cpp` | the same branch (:382) | `onException` has returned, so the rollback body the killer drove is finished and the `KILL` query can report `CancelSent`. The killer could not start another statement before this point |
| `RollbackCopyLists(k, t)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::rollback` (:377) | under `mutex`: `mutations`, `creating_parts` and `removing_parts` are copied into the local work lists, then the mutex is released. This is what serialises the rollback against `DropEnrol` and `DropStore`, which hold the same mutex |
| `RollbackMarkCreated(k, t, p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::rollback` (:377) | one `setAndStoreCreationCSN(Tx::RolledBackCSN)` of the first loop over `parts_to_remove` |
| `RollbackOutdateCreated(k, t, p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::rollback` (:377) | one `removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part}, true)` of the second loop over `parts_to_remove`, which takes `lockParts` for itself |
| `RollbackRestore(k, t, p)` | `src/Interpreters/MergeTreeTransaction.cpp`, `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeTransaction::rollback` (:377) calling `MergeTreeData::restoreAndActivatePart` (:7325) | one part of `parts_to_activate` whose `creation_tid` is not this transaction's; under `lockParts` it goes back to `Active` |
| `RollbackUnlock(k, t, p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::rollback` (:377) | one part of `parts_to_activate`: `setAndStoreRemovalTID(Tx::EmptyTID)` has finished and `unlockRemovalTID` follows it, in that order |
| `RollbackFinalize(k, t)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::rollbackTransaction` (:538) | `rollback` returned `true`: under `running_list_mutex` the transaction leaves `running_list` and its snapshot leaves `snapshots_in_use`, then `afterFinalize` clears the lists and the pins are dropped |
| `NoexceptFrameDown` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::afterCommit` (:321) and `MergeTreeTransaction::rollback` (:377) | a store frame opened inside one of those two functions ended in error. Both are `noexcept`, so the exception terminates the process, which is what `NOEXCEPT_STORE_FAULT_POLICY = Terminate` means and what `NoAvoidableTermination` reports |

### Updating thread {#code-map-updater}

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `UpdLoadEntriesMap` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::loadEntries` (:135), from `loadNewEntries` (:272) | the `NOEXCEPT_SCOPE_STRICT` block (:161) has run: the batch of new entries is in `tid_to_csn` under `TransactionLog::mutex` and `last_loaded_entry` has advanced. `latest_snapshot` has not moved, which is the window a `Begin` or a `SelectCheck` can fall into |
| `UpdPublishSnapshot` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::loadEntries` (:135) | the block under `running_list_mutex` (:174): `latest_snapshot` takes the CSN of the last loaded entry and `local_tid_counter` is reset. `loadNewEntries` then calls `latest_snapshot.notify_all` (:281), which is what releases `waitForCSNLoaded` |
| `UpdRemoveOldEntriesSetTail` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::removeOldEntries` (:284) | everything up to and including `tail_ptr.store` (:321): the `isServerCompletelyStarted` gate, the `asyncTablesLoadingJobNumber` gate that applies only while `updated_tail_ptr` is false, the read of the `tail_ptr` znode, `getOldestSnapshot`, the `LOGICAL_ERROR` when the new value is below the old one, the early return when they are equal, and the `set` of the znode |
| `UpdRemoveOldEntriesDelete(c)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::removeOldEntries` (:284) | one iteration of the removal loop (:319-341): one `tryRemove` of the log znode and one `tid_to_csn` erase, for an entry whose `tid.start_csn` is below the new tail and whose CSN is not the latest loaded one. `ZNONODE` counts as removed |

### Keeper faults and the unknown-state pass {#code-map-keeper}

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `CommitKeeperFault(k, lost)`, `MergeCommitKeeperFault(i, lost)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::commitTransaction` (:419) | the `multi` (:446) failed with a hardware error, in one of the two ways the spec names: `lost` is the response lost after the `csn-` znode was created, which the fail point at :450 injects, and `~lost` is nothing appended. The catch at :459 has not run yet, which is why this is a step of its own: after a lost response the znode exists and the updating thread may load it first |
| `KeeperSessionExpire` | `src/Common/ZooKeeper` | the session | the Keeper session expires on its own. It shares `KEEPER_FAULTS_MAX` with the commit fault |
| `CommitUnknown(k)`, `MergeCommitUnknown(i)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::commitTransaction` (:459-477) | the catch block: under `running_list_mutex` the transaction and its state guard go into `unknown_state_list` (:467-468), then `UNKNOWN_STATUS_OF_TRANSACTION` is thrown (:472) or `CommittingCSN` returned (:477). The transaction keeps `Committing` and `CommittingCSN`, and stays in `running_list` |
| `CommitUnknownResolved(k)` | `src/Interpreters/InterpreterTransactionControlQuery.cpp` | `InterpreterTransactionControlQuery::executeCommit` (:84-115) | `waitStateChange` returned with a decided state. The committed branch reaches `setCurrentTransaction(NO_TRANSACTION_PTR)` at :114; the rolled-back branch throws at :103 before it, so the session stays bound |
| `UpdReconnect` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::runUpdatingThread` (:238-250) | the expired session is replaced and `sync` has run. One Keeper here, so only the session state moves |
| `UpdLoadNothing` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::loadNewEntries` (:272) | the `getChildren` happened and `loadEntries` was called over an empty range (:277-278). All it leaves behind is that the iteration's load has run |
| `UpdSwapUnknownLists` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::tryFinalizeUnknownStateTransactions` (:355) | the two swaps under `running_list_mutex` (:374-375). The local list takes the previous iteration's loaded list, which is the delay of one whole iteration the scheme exists for |
| `UpdFinalizeUnknown(t)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::tryFinalizeUnknownStateTransactions` (:378-391) | `getCSN` decided for one entry. A CSN starts `finalizeCommittedTransaction`; no CSN runs `assertTIDIsNotOutdated` (:387) and then releases the state guard (:388), which CASes the CSN back to `UnknownCSN` and notifies. The transaction is `RUNNING` again at that value |
| `UpdRollbackStart(t)`, `UpdRollbackLost(t)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::rollbackTransaction` (:538) | the CAS of `UnknownCSN` to `RolledBackCSN` (`MergeTreeTransaction.cpp:381-382`), won or lost. Lost means a `KILL TRANSACTION` got there in the window the guard release opened, and the pass returns at :544-548 and goes on |
| `UpdFinalizeDone` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::tryFinalizeUnknownStateTransactions` (:391-392) | the loop ended and the thread leaves the function |
| `UpdCommitStoreCreation(t, p)`, `UpdCommitStoreRemoval(t, p)`, `UpdCommitFlip(t)`, `UpdCommitFinalize(t)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::finalizeCommittedTransaction` (:490) | the same steps a session's commit runs, on the updating thread, because that is the thread `afterCommit` runs on here |
| `UpdRollbackCopyLists(t)`, `UpdRollbackMarkCreated(t, p)`, `UpdRollbackOutdateCreated(t, p)`, `UpdRollbackRestore(t, p)`, `UpdRollbackUnlock(t, p)`, `UpdRollbackFinalize(t)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::rollback` (:377) | the rollback machine with the updating thread as its driver. Each is the session's step with `Upd` for the actor |

### Outdated-parts cleanup thread {#code-map-cleanup}

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `CleanupDecide(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::grabOldParts` (:4074) | `lockParts` is taken and one part is accepted for removal: its version `canBeRemoved` (:4142), which reads `getOldestSnapshot` under `running_list_mutex` and releases it, nobody else holds it (`isSharedPtrUnique`, :4150), and it is not an empty part still covering an `Outdated` one (:4158). The part does not move yet. The code grabs a set under one lock and the model one part per pass, which is model defect `M23`; the removal-time and mutation-parent conditions at :4167 are time and zero-copy-replication bookkeeping, which `force` covers |
| `CleanupGrab(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::grabOldParts` (:4190-4194) | `modifyPartState(..., Deleting, parts_lock)` and the release of `lockParts` with the enclosing block. It is a step of its own because a `SET TRANSACTION SNAPSHOT` can land between the decision and it, which is finding F2's second shape. Under `SET_SNAPSHOT_PROTECTS` the removal condition is re-evaluated here, which is the model's form of the fix |
| `CleanupAbandon(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::grabOldParts` (:4142-4160) | the pass leaves the part where it is and releases the lock: the accepted part is skipped rather than moved. Reachable only under the fix, where it is the revalidation refusing |
| `CleanupValidate(p)` | `src/Storages/MergeTree/IMergeTreeDataPart.cpp` | `IMergeTreeDataPart::remove` (:2928) through `assertHasValidVersionMetadata` (:2863) and `VersionMetadata::hasValidMetadata` | the `chassert` on the grabbed part passes, on the path `clearPartsFromFilesystemAndRollbackIfError` (`MergeTreeData.cpp:4566`) takes for each grabbed part. This row and the validation half of `CleanupDeleteFail` model a `DEBUG_OR_SANITIZER_BUILD`: in release `chassert` is `(void)sizeof(!(x))` (`base/base/defines.h:84-102`) and does not evaluate its argument, so nothing validates and nothing refuses |
| `CleanupDeleteOk(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `clearPartsFromFilesystemAndRollbackIfError` (:4566) and `removePartsFinally` (:4217-4240), whose `lockParts` is at :4223 | the directory is gone in both disk layers and the part leaves `data_parts_indexes` |
| `CleanupDeleteFail(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `rollbackDeletingParts` (:4205-4215), whose `lockParts` is at :4207 | the part goes back to `Outdated`. Two producers: the `CORRUPTED_DATA` `hasValidMetadata` raises, and a filesystem error in `clearPartsFromFilesystemImpl`. The second needs a disk fault, so its disjunct is `FALSE` until plan 5 raises `DISK_FAULTS_MAX` |

### Background merge task {#code-map-merge}

`Tsk(i)` is the actor, the counterpart of `Sess(k)`. Every publication and commit step is the actor-generic body
a session's action uses, with the task's guard and its `task'` clause around it; the bodies are the `*Effect`
operators in `Server.tla`.

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `MergeBegin(i)` | `src/Storages/StorageMergeTree.cpp` | `StorageMergeTree::scheduleDataProcessingJob` (:2203) | `beginTransaction` (:2226) and the `MergeTreeTransactionHolder` with `autocommit = false` (:2227), under the `transactions_enabled` gate. It is the same `TransactionLog::beginTransaction` a session's `Begin` calls, so it registers the merge transaction in both the snapshot and the retention registry. `sys.merges_blocker` is the `merges_blocker.isCancelled` check at :2240 |
| `MergeSelect(i)` | `src/Storages/MergeTree/Compaction/PartsCollectors/MergeTreePartsCollector.cpp` | the predicate `constructPreconditionsPredicate` builds (:80), from `StorageMergeTree::selectPartsToMerge` (:1680) | each source is visible at the merge's snapshot with the **empty** tid (:88), is not locked for removal (:91), and passes `canUsePartInMerges` (:98). The reservation and the pins are `CurrentlyMergingPartsTagger`'s constructor (`StorageMergeTree.cpp:867`), whose `Tagging already tagged part` `LOGICAL_ERROR` (:918-921) is the reservation clause of `ActiveSetShape` |
| `MergeWrite(i)` | `src/Storages/MergeTree/MergePlainMergeTreeTask.cpp` | `MergePlainMergeTreeTask::prepare` (:92) through `mergePartsToTemporaryPart` (:137) | `setAndStoreCreationTID` on the result, which becomes `Temporary`. The task holds it from here, first through `merge_task` and then through `new_part` (:156) |
| `MergeRename(i)` | `src/Storages/MergeTree/MergeTreeDataMergerMutator.cpp` | `MergeTreeDataMergerMutator::renameMergedTemporaryPart` (:526), called from `MergePlainMergeTreeTask::finish` (:160) | the result is `PreActive` and is in the statement transaction's `precommitted_parts` |
| `MergePublishStart(i)`, `MergePublishEnrol(i, q)`, `MergePublishStore(i, q)`, `MergePublishFlip(i)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::commit` (:11219), called at `MergePlainMergeTreeTask.cpp:161` | the same four steps a session's `Publish*` takes, with the sources as the covered parts. `reserved[i]` is **not** released here |
| `MergeCommitBefore(i)` … `MergeCommitFinalize(i)` | `src/Interpreters/TransactionLog.cpp` | `TransactionLog::commitTransaction(txn, throw_on_unknown_status = false)`, called at `MergePlainMergeTreeTask.cpp:195` | the same `Commit*` steps. `MergeCommitFinalize` also runs `merge_mutate_entry->finalize` (:200), which releases the reservation, the source pins and the transaction holder |
| `MergeFail(i)` | `src/Storages/MergeTree/MergePlainMergeTreeTask.cpp` | `MergePlainMergeTreeTask::executeStep` rethrowing (:70-74) | what the query holds is released: the transaction mutex, the parts lock and the task's frames. `MergeFailTrigger` enumerates the three triggers that are live without an injected fault |
| `MergeStmtRollbackMark(i, p)`, `MergeStmtRollbackDrop(i)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::rollback` (:11122) | the same two halves the client's `StmtRollback*` takes: `setAndStoreCreationCSN(RolledBackCSN)` per precommitted part (:11126), then `removePartsFromWorkingSet` for the set under `lockParts` (:11181) |
| `MergeUnwind(i)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::rollback` (:377), reached from `MergeTreeTransactionHolder`'s destructor | the tagger releases the reservation and the source pins, and the holder's `rollbackTransaction` either wins the compare-and-exchange at :382 or finds that a `KILL` already did |

### Non-transactional queries and the removal batch {#code-map-nontxn}

A query outside a transaction has no commit point: the publication is the moment its work takes effect. The
batch is `NonTransactionalRemovalLocks`, whose lock loop and store loop are separate phases, which is what
`NtBatchRefusedUnchanged` is about.

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `NtInsertWrite(k, p)` | `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp` | `VersionMetadata::setAndStoreCreationTID` with `Tx::NonTransactionalTID` (:261), from the `MergedBlockOutputStream` constructor | the part directory exists, the creation record names the non-transactional TID and carries `NonTransactionalCSN` in the same update, and the part is `Temporary`. `deferrable` stays true: a never-transactional part writes no `txn_version.txt` |
| `NtInsertPublish(k, p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `renameTempPartAndReplace` and `MergeTreeData::Transaction::commit` (:11219) with a null transaction, under one `lockParts` | the part is published. It goes `Active`, or `Outdated` if a covering part appeared while it was being written (:11282, :11316), which is the branch that keeps `ActiveSetShape` from firing on an `INSERT` that lost to a `DROP PARTITION`. A base part covers nothing, so the batch is empty and there is nothing to interleave with between the two calls |
| `NtDropWrite(k, e)` | `src/Storages/StorageMergeTree.cpp` | `StorageMergeTree::dropPartition`, the branch without a transaction (:3186), through `initCoverageWithNewEmptyParts` | the empty tombstone part `e` covering the partition is written and `Temporary`, with `setAndStoreCreationTID(Tx::NonTransactionalTID)` open on it |
| `NtDropPublish(k, e)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `renameAndCommitEmptyParts` into `MergeTreeData::Transaction::commit` (:11219) | the parts lock is taken, `getActivePartsToReplace` has given the covered set, and the removal batch over that set starts. The covered `Outdated` parts a transactional publication also collects are inside the `if (txn)` at :11248 and are deliberately not collected here |
| `NtBatchPreflight(p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `NonTransactionalRemovalLocks::lock` (:121-146) | one target is classified before the lock is attempted: already removed, so skipped (:131), or created by a transaction that has not committed, so the whole batch is refused (:139). The third outcome, proceed to `lockRemovalTID`, is `NtBatchLock`'s guard rather than a step |
| `NtBatchLock(p)` | `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp` | `VersionMetadata::lockRemovalTID` (:195) | the removal lock on one target is taken, or the batch is refused with `SERIALIZATION_ERROR` because another actor holds it. At the end of the lock phase the target list becomes the locked list |
| `NtBatchStore(p)` | `src/Interpreters/MergeTreeTransaction.cpp` | `NonTransactionalRemovalLocks::store` (:150) | one target's `removal_tid` is stored and its lock released in the `SCOPE_EXIT`, draining the locked list from the back (:153-155). The store phase begins only after the whole lock loop has finished, which is the class's reason to exist |
| `NtBatchEnd` | `src/Interpreters/MergeTreeTransaction.cpp` | `NonTransactionalRemovalLocks::store` (:150), returning | the locked list is empty and the batch is done |
| `NtDropFlip(k, e)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::commit` (:11219), its `NOEXCEPT_SCOPE`, or the exception path out of it | on a done batch the states flip: the empty part goes `Active` and every covered part `Outdated`. On a refused batch the query fails with `SERIALIZATION_ERROR`, the parts lock is released, and `MergeTreeData::Transaction`'s destructor runs the statement rollback below |
| `NtDropUnwindMark(k, e)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::rollback` (:11122) | `setAndStoreCreationCSN(Tx::RolledBackCSN)` on the empty part (:11126). This is the one transition `setAndStoreCreationCSN`'s `chassert` exempts by name, and it is what makes the abandoned empty part invisible and removable |
| `NtDropUnwindDrop(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::Transaction::rollback` (:11122) | the parts lock is taken and `removePartsFromWorkingSet` (:11181) drops the empty part out of the working set |

### The metadata store {#code-map-store}

`updateInfoWithRefreshDataThenStoreAndSetMetadata` is three actions per caller, because `persisted_info_mutex`
covers only the middle one and `version_info_mutex` only the last. The `Server.tla` actions are the roots;
`Parts.tla` holds their bodies under the same names with a `Step` suffix, so each pair is one row.

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `StoreRead(p, o)`, `StoreReadStep(p, o)` | `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp` | `VersionMetadata::updateInfoWithRefreshDataThenStoreAndSetMetadata` (:324) | the head of one attempt: `getInfo` on the first attempt and `loadMetadata` on a retry, the update function applied to what was read, `updateCSNIfNeeded` (whose `std::nullopt` sends the frame back for another attempt), and `validateInfo` |
| `StorePersist(p, o)`, `StorePersistStep(p, o)` | `src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp` | `VersionMetadataOnDisk::storeInfo` (:281) calling `storeInfoUnlocked` (:187) | under `persisted_info_mutex`, one of three outcomes: the deferred branch for a part no transaction has touched, `TOO_OLD_VERSION` when `getExpectedStoringVersionUnlocked` disagrees with the frame's `storing_version`, or the write itself through `storeInfoToDataPartStorage` (:340), which creates the temporary file, syncs it and renames it over `txn_version.txt` |
| `StorePublish(p, o)`, `StorePublishStep(p, o)` | `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp` | `VersionMetadata::setInfo` (:367) | under `version_info_mutex`: the record becomes the in-memory one unless its `storing_version` is below the current one, in which case it is dropped; either way the frame leaves |
| `Fsync(p)` | no single function | the page cache | the cached copy of `txn_version.txt` becomes durable. In the code that is the directory `SyncGuard` taken under `fsync_part_directory` inside `storeInfoToDataPartStorage` (:361), or the operating system writing the page back later. Disabled in `Base`, where `DISK_MODE` is `Durable` and the two layers are one |

### Stubs {#code-map-stubs}

Defined with the body `FALSE` so that a later plan can fill them in without renaming anything. The plan number is
the one that implements the action, from the plan's "Plans that follow this one" section.

The non-transactional group this table used to carry for plan 2 is implemented and has its own section above,
and so is the unknown-state group it carried for plan 3: `CommitUnknown`, `UpdReconnect`, `UpdSwapUnknownLists`
and `UpdFinalizeUnknown` are live and are in the code map with the rest of the Keeper-fault actions.
The design document's separate `NtBatchStart(B)` action is not among them: its targets are computed by the
caller under the same parts lock, so the model folds it into `NtDropPublish` through the operator `StartBatch`,
and nothing observable happens between the two.

| Action | Plan |
|---|---|
| `Crash` | plan 3 |
| `RestartLoadLog`, `RestartTableStart`, `RestartLoadPart(p)`, `RestartTablePublished`, `RestartOutdatedDone`, `RestartDone` | plan 3 |
| `MutPrepareWrite(k, m)`, `MutPrepareAttach(k, m)`, `MutRegister(k, m)`, `MutSelect(i, m, p)`, `MutWrite(i, m, p)`, `MutRename(i, m, p)`, `MutWait(k, m)`, `MutFail(i)`, `MutDestroyOwner(m)` | plan 4 |
| `KillUnregister(m)`, `KillRollbackTxn(m)`, `KillCancelTask(m, i)`, `KillRemoveFile(m)`, `KillMutation(k, m)` | plan 4 |
| `RollbackKill(k, t, m)`, `CommitStoreMutation(k, m)` | plan 4 |
| `RestartLoadMutation(m)` | plan 4, with the `MutationCrash` scenario |
| `StoreRetry(p, o)`, `KillRetry(m)` | plan 5 |
| `ProcessDown(cause)` | plan 5 |

## 4. Run table {#run-table}

Every run on this table used `-workers auto` on a 32-core machine, one at a time, with the `-Xmx16g` that
`run_tlc.sh` sets. Counts are approximate: under `-workers auto` two workers can fingerprint the same state
before either has inserted it, so consecutive runs of one configuration differ by a few states.
`STATE_SPACE.md` has the measurement history behind these numbers, including the variants that were tried and
discarded.

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `Schema` | 2026-09-17 | `885a5c382cab` | 1 | 1 s | green |
| `BaseSmall` | 2026-09-17 | `885a5c382cab` | 24,667 | 1 s | green |
| `Base` | 2026-09-17 | `885a5c382cab` | 115,663,927 and the queue still growing | killed at 20 min | did not finish; the state-space defect Task 3 resolved |
| `Base` | 2026-09-18 | `53814c46e7e5` | 1,814,598 | 16 s | green at `TID_MAX = 2`, `CSN_MAX = 35`, after the three model corrections and the view |
| `BaseSmall` | 2026-09-18 | `ad432095717a` | 47,381 | 1 s | green, with a session allowed to kill its own transaction |
| `Base` | 2026-09-18 | `ad432095717a` | 2,163,747 | 19 s | green at `TID_MAX = 2`, `CSN_MAX = 35` |
| `Schema` | 2026-09-18 | `5aaefae31249` | 1 | 1 s | green |
| `BaseSmall` | 2026-09-18 | `5aaefae31249` | 47,381 | 1 s | green |
| `Base` | 2026-09-18 | `5aaefae31249` | 28,553,114 | 4 min 08 s | green at the matrix bounds `TID_MAX = 3`, `CSN_MAX = 36` |
| witness sweep, 18 rows | 2026-09-18 | `5aaefae31249` | 53,133,545 for the largest single row | ≈ 15 min in total, 5 min 21 s for that row | every witness red, both minimality halves green |
| `Schema` | 2026-09-18 | the final-review fix commit | 1 | 1 s | green |
| `Schema` | 2026-09-18 | the `SetSnapshot` commit | 1 | 1 s | green |
| `BaseSmall` | 2026-09-18 | the `SetSnapshot` commit | 47,381 | 1 s | green |
| `Base` | 2026-09-18 | the `SetSnapshot` commit | 28,553,090 | 4 min 11 s | green; 28,553,114 before the shared-module changes, which is the multi-worker counting noise |
| `SetSnapshot` | 2026-09-18 | the `SetSnapshot` commit | 7,420,004 | 1 min 05 s | green at the exhaustive bounds `TID_MAX = 2`, `CSN_MAX = 35` |
| `SetSnapshotFixed` | 2026-09-18 | the `SetSnapshot` commit | 7,291,951 | 1 min 04 s | green at the same bounds |
| `SetSnapshot` | 2026-09-18 | the `SetSnapshot` commit | 56,968,754 after 8 min, queue 4.47M | killed, five times | an exhaustive run at the matrix bounds does not finish; model defect `M4` |
| `SetSnapshot` witness sweep, 18 rows | 2026-09-18 | the `SetSnapshot` commit | 7,420,048 for the largest completed row | ≈ 5 min in total | red except the five of debt `B1` |
| `SetSnapshotWitness` witnesses, 4 rows | 2026-09-18 | the `SetSnapshot` commit | 7,261,414 for the largest | 1 min 22 s in total | all four red at the matrix bounds |
| `BaseSmall` | 2026-09-18 | the final-review fix commit | 47,381 | 1 s | green |
| `Base` | 2026-09-18 | the final-review fix commit | 28,552,935 | 4 min 06 s | green at the matrix bounds |
| witness `FlipAfterStores` | 2026-09-18 | the final-review fix commit | 8,017 | 2 s | red, as required |
| witness `ErrorIsAbsent` | 2026-09-18 | the final-review fix commit | 64,836 | 2 s | red, as required |
| `Schema` | 2026-09-18 | the cleanup-thread commit | 1 | 1 s | green |
| `BaseSmall` | 2026-09-18 | the cleanup-thread commit | 47,381 | 1 s | green |
| `Base` | 2026-09-18 | the cleanup-thread commit | 28,552,285 | 4 min 11 s | green at the matrix bounds; an unmodified `630d8ad28674` re-run in the same session gave 28,553,740, so the spread is run-to-run counting noise and not this change |
| `SetSnapshot` | 2026-09-18 | the cleanup-thread commit | 13,634,229 | 2 min 01 s | green at the exhaustive bounds; the cleanup group and the two view fields roughly double it |
| `SetSnapshotFixed` | 2026-09-18 | the cleanup-thread commit | 13,104,416 | 2 min 01 s | green at the same bounds |
| `SetSnapshotF2` | 2026-09-18 | the cleanup-thread commit | 170,881, a first-violation count that is not reproducible | 3 s | **red on `NoPrematureDelete`**, 45 states, which is finding F2 |
| `SetSnapshotF2Fixed` | 2026-09-18 | the cleanup-thread commit | 367,183 | 4 s | green on the four cleanup-scenario properties; `NoFalseCorruption` is vacuously green there and in `SetSnapshotFixed`, because no `CleanupDeleteFail` step is reachable |
| cleanup witnesses, 3 rows | 2026-09-18 | the cleanup-thread commit | 243,397 for the largest, all first-violation counts | 10 s in total | all three red |
| witnesses `NoPrematureDelete` and `NoLostVisibleData` in `SetSnapshotFixed` | 2026-09-18 | the cleanup-thread commit | 13,664,284 and 13,664,666 | 96 s and 98 s | green, which is debt `B2` |

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `BaseSmall` | 2026-09-18 | the merge commit | 47,381 | 1 s | green |
| `Base` | 2026-09-18 | the merge commit | 28,547,508 | 4 min 12 s | green at the matrix bounds; 28,552,913 on the tree before, the net of two changes described below |
| `SetSnapshot` | 2026-09-18 | the merge commit | 13,622,631 | 2 min 01 s | green at the exhaustive bounds |
| `SetSnapshotFixed` | 2026-09-18 | the merge commit | 13,092,635 | 2 min 02 s | green at the same bounds |
| `SetSnapshotF2` | 2026-09-18 | the merge commit | a first-violation count | 3 s | **red on `NoPrematureDelete`**, still finding F2 after the property's antecedent was narrowed |
| `SetSnapshotF2Fixed` | 2026-09-18 | the merge commit | 367,183 | 4 s | green |
| cleanup witnesses, 3 rows | 2026-09-18 | the merge commit | 259,148 for the largest | 8 s in total | all three still red |
| `Merge` | 2026-09-18 | the merge commit | 44,277,426 after 8 min, queue 4.7M and growing | killed twice | two sessions: an exhaustive run at the matrix bounds does not finish |
| `Merge` | 2026-09-18 | the merge commit | 5,196,830 | 49 s | green at one session, which is the committed configuration |
| `Merge` witness sweep, 22 rows | 2026-09-18 | the merge commit | 38,568,430 for the largest | ≈ 9 min in total, 4 min 51 s for that row | red except the three of debt `B3`; both minimality halves green |
| `Base` witness sweep, 18 rows | 2026-09-18 | the fix-round commit | 53,619,513 for the largest | ≈ 14 min in total, 5 min 31 s for that row | every row red, both minimality halves green, after the extraction moved six of the hooked operators |
| `Merge` | 2026-09-18 | the fix-round commit | 5,196,830 | 49 s | green after `FlipAfterStores` gained its task conjunct |

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `BaseSmall` | 2026-09-18 | the non-transactional commit | 47,381 | 1 s | green |
| `Base` | 2026-09-18 | the non-transactional commit | 26,839,136 | 3 min 56 s | green; it moves by 6% because model defect `M13` restored `DropLock`'s `lockParts` guard, which an empty `Tasks` had made vacuous |
| `NonTxn`, undivided | 2026-09-18 | the non-transactional commit | 27,234,570 at the matrix bounds and 59,047,617 at `TID_MAX = 2` | killed, twice | an exhaustive run does not finish at any bounds that keep the scenario's subject |
| `NonTxnF4`, `NonTxnF5`, `NonTxnF6` | 2026-09-18 | the non-transactional commit | 74,225, 106,922 and 113,093, all first-violation counts | 4 s in total | one red module per finding, traces in `traces/` |
| `NonTxnFixed` | 2026-09-18 | the non-transactional commit | 841,907 | 8 s | green, which is finding F6's fix verified; `MC_NonTxnFixed` is the undivided module with `OBSOLETE_IS_ROLLED_BACK = TRUE` |
| `NonTxn` witness sweep, 14 rows | 2026-09-18 | the non-transactional commit | 44,213,335 for the one that does not finish | ≈ 11 min in total | red except `NoUncommittedRead`, which is debt `B4` |

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `NonTxnInsert` | 2026-09-20 | the review fix-round commit | 15,787,889, and 15,787,838 on the closing re-run of the committed tree | 2 min 33 s | green under `OBSOLETE_IS_ROLLED_BACK = TRUE`, finding `F6`'s unmerged fix; the half of the split scenario that keeps the non-transactional `INSERT` |
| `NonTxnDrop` | 2026-09-20 | the review fix-round commit | 56,703,779 after 9 min 30 s, queue 3.39M | killed, three times | the half that keeps the removal batch; no finishing configuration, model defect `M15` |
| `NonTxnF7` | 2026-09-20 | the review fix-round commit | 75.3 million after 600 s | no violation | the ghost-lag fix of `M16` withdraws finding F7; the module is deleted |
| `SetSnapshot` and `SetSnapshotFixed` | 2026-09-20 | the review fix-round commit | 12,766,799 and 12,236,834 | 1 min 55 s and 1 min 52 s | green; the `M13` re-measurement, about 6% below the pre-fix figures |
| `SetSnapshot` witness sweep, the full table | 2026-09-20 | the review fix-round commit | 40,439,049 for `Assert_validateInfo_removal`, which does not finish and is debt `B1` | ≈ 11 min in total | re-run after `M13`; every verdict unchanged |
| `Base` witness sweep, the full table | 2026-09-20 | the review fix-round commit | 51,268,074 for `Assert_validateInfo_removal`, the largest | ≈ 15 min in total | re-run after `M13`; every verdict unchanged |

The commits are `885a5c382cab` (Task 1, the modules and the runner), `5260d5d44f67` and `53814c46e7e5`
(Task 3, the state-space budget and the rollback-driver correction), `bea5c15bf346` and `ad432095717a` (Task 2,
the witness runner and the witness table), `40ba930673fd` and `5aaefae31249` (Task 5, the matrix bounds, the
`Atomicity` correction and the findings file), and the final-review fix commit, which is the last of plan 1 and
names itself in its own message rather than by a hash it cannot yet know.

`Base` did not move when the statement rollback was corrected, and 28,552,935 against 28,553,114 is the
multi-worker noise, not a change. The cleanup task settled that by measurement rather than by argument: it
re-ran `Base` from an unmodified checkout of `630d8ad28674` in the same session as its own run and got
28,553,740 against 28,552,285, a wider spread than any change has ever produced here, with the generated count
moving too. The count is a property of how the workers race, not of the behaviour set. The corrected actions are unreachable in `Base`: `QUERY_FAULTS_MAX = 0`
disables `Fail`, and an empty `Covers` leaves `PublishEnrol` unreachable, so no refusal can happen while
`stmt.precommitted` is non-empty. Plan 2 gives the sibling scenario a covering relation and the path goes live
there. The two witness counts moved more than that, from 7,541 and 64,365, because a witness run stops at the
first violation and how many states it has fingerprinted by then depends on the worker scheduling.

`Base` moved by more than the counting noise for the first time, from 28,552,913 to 28,547,508, and the two
changes behind it pull in opposite directions. The `attached` component left `stmt`, which is in `BaseView`, and
that merges states which differed only in a ghost no action read. The holder discipline moved the removal of a
holder from `RollbackFinalize` to the owners that destroy it, `RollbackReturn` and the detaching branch of
`RollbackStart`, and that splits states, because a rolled-back transaction now keeps its holder for a step or
two longer. The net is 5,405 fewer, about 0.02%, which says the merge is worth slightly more than the split. The
direction was expected to be the other one; what it shows is that the two are of the same small size, not that
either is negligible.

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `NonTxnDrop`, `TID_MAX = 2` | 2026-09-21 | the budget commit | 32,818,006 after 5 min, queue 2.71M | killed | the committed configuration re-measured with its own view; still growing, so still no exhaustive run |
| `NonTxnDrop`, `TID_MAX = 2`, one non-transactional query per behaviour | 2026-09-21 | the budget commit | 32,216,335 after 5 min, queue 2.70M | killed | the `CONSTRAINT` lever measured and **rejected**: a two per cent cut. Reverted with its ghost counter |
| `NonTxnDrop`, `TID_MAX = 1`, **committed** | 2026-09-21 | the budget commit | 1,112,063 | 11 s | green under `OBSOLETE_IS_ROLLED_BACK = TRUE`, finding `F6`'s unmerged fix, and with `ActiveSetShape` off the roster, which finding `F5` falsifies; the exhaustive configuration of the drop half, with the cleanup group |
| `NonTxnDropTwo`, `TID_MAX = 2`, no cleanup group, **committed** | 2026-09-21 | the budget commit | 47,958,711 | 7 min 34 s | green under `OBSOLETE_IS_ROLLED_BACK = TRUE`, finding `F6`'s unmerged fix, and with `ActiveSetShape` off the roster, which finding `F5` falsifies; the queue peaked at 1.21M and drained, which is why a count above 30 million is accepted here |
| `NonTxnF2` | 2026-09-21 | the budget commit | 9,132 and 9,193, first-violation counts; a re-run gave 10,233 for the first | under 1 s each | **red on `NoPrematureDelete` and on `NoLostVisibleData`**: finding F2 at two transactions with a non-transactional creator, which closes debt `B2` |
| `NonTxnDrop` witness sweep, 28 rows | 2026-09-21 | the budget commit and the two fix rounds | 25,686,095 for the largest | about 7 min in total | fourteen red, including both batch witnesses, `Assert_getOldestSnapshot_size`, `NoNtStoreError` and the `Assert_validateInfo_nocreation` minimality halves, which closes `B5`; fourteen green by full exploration, each named where it is paid or placed |
| `NonTxnDropTwo` witnesses, 5 rows | 2026-09-21 | the budget commit and the two fix rounds | 65,525,357 for the largest | 30 min in total | `Atomicity` red; three green by full exploration; `SingleRemover` killed unfired at 65,525,357 with the queue growing, and placed in plan 5 |
| `NonTxnWitness` witnesses, 5 rows | 2026-09-21 | the budget commit | 672,101 for the largest red row | 11 min in total | three red, two killed unfired at 40.8M and 33.6M, which is what is left of `B4` |
| `NonTxnInsert` witnesses, 2 rows | 2026-09-21 | the budget commit | 15,788,005 | 2 min 15 s in total | `Atomicity` red; `ActiveSetShape` green because part `E` is never created in that half |
| `SetSnapshotWitness` witnesses, 4 rows | 2026-09-21 | the budget commit | 7,913,163 for the largest red row | 12 min in total | three red, `Assert_validateInfo_removal` killed unfired at 108,439,476, which is what is left of `B1` |
| `MergeWitness` witnesses, 3 rows, and the `Merge` sweep's `Assert_getOldestSnapshot_size` row | 2026-09-21 | the budget commit | 6,065,808 for the largest red row | 6 min in total | two red, including `NoSpuriousStaleVersion`, `Assert_validateInfo_removal` killed unfired at 34,184,268; that is `B3` |
| `Schema` | 2026-09-21 | the budget commit | 1 | 1 s | green |
| `BaseSmall` | 2026-09-21 | the budget commit | 47,381 | 1 s | green |
| `Merge` | 2026-09-21 | the budget commit | 5,196,830 | 50 s | green at one session |
| `SetSnapshotF2` | 2026-09-21 | the budget commit | a first-violation count | 1 s | **red on `NoPrematureDelete`**, as it is expected to be |
| `NonTxnInsert` | 2026-09-21 | the budget commit | 15,788,049 | 2 min 35 s | green under `OBSOLETE_IS_ROLLED_BACK = TRUE`, finding `F6`'s unmerged fix; 15,787,838 and 15,787,889 on the two earlier runs, which is the counting noise |
| `Base` | 2026-09-21 | the budget commit | 26,839,128 | 3 min 54 s | green at the matrix bounds; 26,839,136 on the previous commit |

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `BaseSmall` | 2026-09-21 | the Keeper-faults commit | 47,381 | 1 s | green, unchanged |
| `Base` | 2026-09-21 | the Keeper-faults commit | 26,839,116 | 4 min 01 s | green at the matrix bounds; 26,839,061 on the base commit, which is the multi-worker counting noise |
| `Merge` | 2026-09-21 | the Keeper-faults commit | 6,124,691 | 56 s | green at one session, unchanged |
| `Keeper` | 2026-09-21 | the Keeper-faults commit | 12,081, a first-violation count | 1 s | **red on `UnknownResolvesByLog` on the baseline**, which is model defect `M26`, the updating thread's unordered passes |
| `Keeper` | 2026-09-21 | the Keeper-faults commit | 30,544,101 | 5 min 15 s | green at the matrix bounds `TID_MAX = 3`, `CSN_MAX = 36`, after `M26`; the queue peaked near 0.8M and drained |
| `KeeperUnknownWait` | 2026-09-21 | the Keeper-faults commit | 60,249,422 after 6 min, queue 9.18M and growing | killed | `WAIT_MODE = "WAIT_UNKNOWN"` at the matrix bounds does not finish |
| `KeeperUnknownWait` | 2026-09-21 | the Keeper-faults commit | 71,323,386 | 12 min 28 s | green at `TID_MAX = 2`, `CSN_MAX = 35`, with the merge task kept, which is debt `B8`; dropping the merge instead buys 1.4% |
| `Keeper` and `KeeperUnknownWait` witnesses, 5 rows | 2026-09-21 | the Keeper-faults commit | 2,496,715 for the largest | 30 s in total | every row red, including both configurations of each new property and one row on `MC_KeeperWitness` |
| witness `RollbackRestores` in `Base` | 2026-09-21 | the Keeper-faults commit | 689,452 | 7 s | red, as required, after the rollback machine took its driver as an argument; 745,525 on the base commit |
| `BaseSmall` | 2026-09-21 | the fix-round commit | 47,381 | 1 s | green, unchanged |
| `Keeper` | 2026-09-21 | the fix-round commit | 30,544,101 | 5 min 07 s | green, unchanged after the updating thread's holder release and the three cleanup properties left the roster |
| `KeeperUnknownWait` | 2026-09-21 | the fix-round commit | 71,323,386 | 11 min 57 s | green, unchanged; the generated count is unchanged too, which is what a relabelling of the graph looks like |
| `BaseSmall` | 2026-09-21 | the review fix-round commit | 47,381 | 1 s | green, unchanged |
| `Keeper` | 2026-09-21 | the review fix-round commit | 29,274,410 | 4 min 53 s | green; 4.2% below 30,544,101 because the updating thread's two load actions gained the live-session guard the truncation pass already had |
| `KeeperUnknownWait` | 2026-09-21 | the review fix-round commit | 71,209,832 | 11 min 53 s | green; 0.16% below 71,323,386, the same guard against a session that is parked for most of the expiry window |
| `BaseSmall`, `Merge` | 2026-09-21 | the lifecycle commit | 47,381 and 6,124,691 | 1 s and 57 s | green, unchanged |
| `Keeper` | 2026-09-21 | the lifecycle commit | 37,785,664 | 6 min 06 s | green; 29% above 29,274,410, which is the notified `UnknownCSN` window and the `KILL TRANSACTION` that can win inside it |
| `KeeperUnknownWait` | 2026-09-21 | the lifecycle commit | 71,779,055 | 12 min 01 s | green; 0.8% above 71,209,832, at `TID_MAX = 2` |
| `BaseSmall`, `Merge` | 2026-09-21 | the pass-ownership commit | 47,381 and 6,124,691 | 1 s and 58 s | green, unchanged |
| `Keeper` | 2026-09-21 | the pass-ownership commit | 31,042,348 | 5 min 08 s | green; 18% below 37,785,664, which is the updating thread no longer losing a compare-exchange it had won |
| `KeeperUnknownWait` | 2026-09-21 | the pass-ownership commit | 11,537,098 | 1 min 55 s | green **at the matrix bounds**, where 71 million had not finished: a parked client was running the commit machine beside the updating thread. That closes debt `B8` |
| `Keeper`, `KeeperUnknownWait` and `KeeperUnknownWaitWitness` witnesses, 6 rows | 2026-09-21 | the pass-ownership commit | 4,204,536 for the largest | 65 s in total | all red; the sixth, `RollbackRestoresKillRace`, runs only on the witness-bounds module |
| `Keeper` witnesses, 3 rows | 2026-09-21 | the lifecycle commit | 4,852,299 for the largest | 42 s in total | all red, including the new `RollbackRestoresUpd` |
| `Base` | 2026-09-21 | the layered-disk commit | 26,839,086 | 4 min 03 s | green, unchanged |
| `Merge` | 2026-09-21 | the layered-disk commit | 6,124,691 | 59 s | green, unchanged to the state |
| `NonTxnInsert` | 2026-09-21 | the layered-disk commit | 16,969,409 | 2 min 48 s | green, unchanged; the figure is 7.5% above the one `STATE_SPACE.md` recorded, and the same configuration rebuilt from the previous commit measures 16,969,487, so the move predates this work |
| `Crash` | 2026-09-21 | the layered-disk commit | 47,838,278 | 13 min 27 s | green at the matrix bounds with `FSYNC_PART_DIRECTORY = TRUE`; 3,198,920 in 49 s at `TID_MAX = 2`, which is the lever that is costed and not taken |
| `Crash`, re-taken on the committed tree | 2026-09-21 | the citation commit | 47,838,278 | 13 min 13 s | green; the distinct count and the generated count, 583,693,727, are both identical to the row above, which is what a disjunct guarded by a witness name that is empty predicts |
| `Schema`, `BaseSmall` | 2026-09-21 | the fix-round commit | 1 and 47,381 | 1 s each | green, unchanged |
| `Base` | 2026-09-21 | the fix-round commit | 26,839,063 | 4 min 30 s | green, unchanged |
| `Merge` | 2026-09-21 | the fix-round commit | 6,124,691 | 1 min 00 s | green, unchanged to the state for the second round running |
| `NonTxnInsert` | 2026-09-21 | the fix-round commit | 16,969,516 | 2 min 51 s | green, unchanged |
| `Crash`, with the three sync actions unguarded | 2026-09-21 | the fix-round commit | 46,266,705, queue 2.68M and growing | killed at 15 min | did not finish; the cause was `Fsync` setting a durability bit on a part with no directory, not the new bits |
| `Crash` | 2026-09-21 | the fix-round commit | 48,142,550 | 12 min 28 s | green at the matrix bounds with all three settings on; 0.6% above 47,838,278, which is what the three new durability bits cost when every one of them is set at the moment of the operation |
| `Crash` | 2026-09-21 | the second fix-round commit | 48,142,550 | 15 min 19 s | green; the distinct count and the generated count, 495,201,353, are both identical to the row above, so neither of that round's two model fixes changes a reachable state here. The 23% of extra wall clock is `DurabilityMonotone`, an action property evaluated on every step |
| `CrashF10` | 2026-09-21 | the second fix-round commit | 97,483, a first-violation count | 5 s | **red on `AckedWriteIsDurable`**, the acknowledged shape unchanged at twenty-five steps |
| `CrashF10` with model defect `M45` restored | 2026-09-21 | the second fix-round commit | 461 | 1 s | **red on `DurabilityMonotone`**, which is that fence shown to fence something |
| `BaseSmall` | 2026-09-21 | the second fix-round commit | 47,381 | 1 s | green, unchanged |
| witness `AckedWriteIsDurable` in `Crash` | 2026-09-21 | the second fix-round commit | 3,567, a first-violation count | 2 s | red, as required |
| `CrashF10` | 2026-09-21 | the fix-round commit | 395,095, a first-violation count and therefore not reproducible | 5 s | **red on `AckedWriteIsDurable`**, the acknowledged shape of finding `F10`; 494,552 with the data files unsynced as well, where the part is absent rather than reclassified |
| witness `AckedWriteIsDurable` in `Crash` | 2026-09-21 | the fix-round commit | 5,017, a first-violation count | 1 s | red, as required; 2,820 before the disk record gained its three bits |
| `CrashF10` | 2026-09-21 | the layered-disk commit | 42,035, a first-violation count | 2 s | **red on `NoPrematureDelete`**, as it is expected to be: finding `F10` |
| witness `AckedWriteIsDurable` in `Crash` | 2026-09-21 | the layered-disk commit | 2,820 | 1 s | red, which closes the last of the deferred `Base` rows |

Every `NonTxn*` row of these tables, the witness sweeps included, was measured with
`OBSOLETE_IS_ROLLED_BACK = TRUE`, and the two drop configurations without `ActiveSetShape`. Section 2 says what
that costs; it is repeated here because a result cell is what a reader quotes.

### Per-scenario assurance {#per-scenario-assurance}

What each committed configuration verifies and what it gives up, so that a green can be read without assembling
it from three files. "Exhaustive" means TLC drained the queue at those bounds.

| Configuration | Verified exhaustively | Given up | Where the gap is recorded |
|---|---|---|---|
| `BaseSmall` | one session, `TID_MAX = 2`, `CSN_MAX = 35`, 47,381 states, the whole roster | every interleaving of two sessions | `STATE_SPACE.md`, "Where `Base` stands" |
| `Base` | two sessions at the matrix bounds `TID_MAX = 3`, `CSN_MAX = 36`, 26,839,128 states, the whole roster | nothing of its own slice; the cleanup thread, the merge task, the non-transactional queries and `SET TRANSACTION SNAPSHOT` are all stubs here | section 3, the stub table |
| `SetSnapshot`, `SetSnapshotFixed` | `TID_MAX = 2`, `CSN_MAX = 35`, 14,289,310 and 13,607,915 states, the whole roster | the matrix bounds: an exhaustive run at `TID_MAX = 3` does not finish, so three witnesses are shown at the witness bounds instead and one, `Assert_validateInfo_removal`, is not shown at all | `FINDINGS.md`, `M4` and `B1`; `STATE_SPACE.md`, the `SetSnapshot` section |
| `SetSnapshotF2` | nothing: it stops at the first violation, which is finding `F2` | everything else; it is a reproducer, not a check | `FINDINGS.md`, finding `F2` |
| `SetSnapshotF2Fixed` | one session, one part, `TID_MAX = 3`, 309,987 states, over five properties | the properties outside those four; the truncation tail, which the model publishes in one step, so the green verifies the refusal and not the publication; and any window between the revalidation and the state change, which are one action here | `FINDINGS.md`, finding `F2` and model defects `M18` and `M19` |
| `Merge` | one session, `TID_MAX = 3`, `CSN_MAX = 36`, 6,124,691 states, the whole roster | the second session: an exhaustive run at two does not finish, two witnesses fire only in `MergeWitness`, and `Assert_validateInfo_removal` fires in neither | `FINDINGS.md`, `B3`; `STATE_SPACE.md`, the `Merge` section |
| `Keeper` | one session, `TID_MAX = 3`, `CSN_MAX = 36`, `KEEPER_FAULTS_MAX = 1`, 31,042,348 states, `Merge`'s roster less the three cleanup properties, plus `UnknownResolvesByLog` and `NoOutdatedLookup` | the second session and the second fault; the roster's witness sweep, which is `Merge`'s twenty-four rows over a scenario six times its size | `STATE_SPACE.md`, the `Keeper` section; `WITNESSES.md`, the `Keeper` witnesses |
| `KeeperUnknownWait` | one session at the matrix bounds, 11,537,098 states, the same roster under `WAIT_MODE = "WAIT_UNKNOWN"` | the second session, and with it the race a `KILL` wins inside the guard-release window, which `KeeperUnknownWaitWitness` covers | `STATE_SPACE.md`, the `Keeper` section; `WITNESSES.md`, the `Keeper` witnesses |
| `NonTxnDrop` | one transaction with the cleanup group, `TID_MAX = 1`, 1,246,158 states | the second transaction; `ActiveSetShape`, which finding `F5` falsifies; the four snapshot-isolation rows of spec defect `S13`; and `F6`'s fix is assumed rather than tested | `FINDINGS.md`, `B4`, `M15`, `S13`, findings `F5` and `F6` |
| `NonTxnDropTwo` | two transactions without the cleanup group, `TID_MAX = 2`, `CSN_MAX = 34`, 47,958,711 states, unchanged by the cleanup split | the cleanup group, so no finishing configuration checks the removal batch beside two transactions and cleanup at once, which is the open bound of `M15`; `SingleRemover` unfired at 65,525,357; the same properties as the row above | `FINDINGS.md`, `B4` and `M15` |
| `NonTxnInsert` | two transactions with the cleanup group, `TID_MAX = 2`, `CSN_MAX = 34`, 16,969,548 states | the removal batch; `ActiveSetShape` is on the roster but vacuous, because part `E` is never created here; the `S13` rows; `F6`'s fix is assumed | `FINDINGS.md`, `B4` and `S13`; `WITNESSES.md`, the `NonTxn` section |
| `NonTxnFixed` | the undivided scenario at one session with `OBSOLETE_IS_ROLLED_BACK = TRUE`, 1,029,281 states | the two-session interleavings the split modules cover | `FINDINGS.md`, finding `F6` |
| `NonTxnF2`, `NonTxnF4`, `NonTxnF5`, `NonTxnF6` | nothing: each stops at the first violation it was built to produce | everything else | `FINDINGS.md`, section 1 |
| `SetSnapshotF2Special` | one session, one part, `TID_MAX = 2`, `SNAPSHOT_TARGETS = {1, 34}`, 116,020 states, the whole roster | everything above `TID_MAX = 2`; it is about the reserved target, not about the three-transaction shape | `FINDINGS.md`, finding `F2`, "the two reserved snapshots" |
| `SetSnapshotF2SpecialEV` | the same at `SNAPSHOT_TARGETS = {3, 34}`, 125,673 states | `RollbackNoLeak`, which finding `F8` falsifies at this target | `FINDINGS.md`, finding `F8` |
| `SetSnapshotF9SiblingFixed` | two sessions, one part, `TID_MAX = 2`, `SNAPSHOT_TARGETS = {3}` under `REMOVAL_REFUSES_UNCOMMITTED_CREATION`, about 1,858,350 states, the roster `SetSnapshotF2SpecialEV` carries less `ErrorIsAbsent`, which the fix's own refusal falsifies | `ErrorIsAbsent` and `RollbackNoLeak`, the latter for finding `F8`'s reason; and everything above `TID_MAX = 2` | `FINDINGS.md`, findings `F8` and `F9` |
| `SetSnapshotF8`, `SetSnapshotF9Sibling` | nothing: each stops at the first violation it was built to produce | everything else | `FINDINGS.md`, findings `F8` and `F9` |
| `SetSnapshotWitness`, `MergeWitness`, `NonTxnWitness` | nothing: `witness.sh` only, one property at a time, stopping at the first violation | exhaustive coverage at those bounds, by construction | `WITNESSES.md` |

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `Schema` | 2026-09-21 | the cleanup-split commit | 1 | 0 s | green |
| `BaseSmall` | 2026-09-21 | the cleanup-split commit | 47,381 | 1 s | green; `Base` and `BaseSmall` enable no cleanup group, so the split does not reach them |
| `SetSnapshot` | 2026-09-21 | the cleanup-split commit | 14,289,310 | 2 min 03 s | green at the exhaustive bounds; 12,766,799 before the split, which is the cost of the extra step |
| `SetSnapshotFixed` | 2026-09-21 | the cleanup-split commit | 13,607,915 | 2 min 02 s | green; 12,236,834 before the split |
| `SetSnapshotF2` | 2026-09-21 | the cleanup-split commit | a first-violation count | 1 s | **red on `NoPrematureDelete`**, finding F2's first shape, unchanged |
| `SetSnapshotF2Fixed` | 2026-09-21 | the cleanup-split commit | 411,641 | 4 s | green with both halves of the fix; 367,183 before the split |
| witness `SnapshotEntryOnly` on `SetSnapshotF2Fixed` | 2026-09-21 | the cleanup-split commit | 280,957 | 4 s | **red on `NoPrematureDelete`**: finding F2's second shape, the fix reduced to its `snapshots_in_use` half |
| the three cleanup witnesses | 2026-09-21 | the cleanup-split commit | 280,340, 39,613 and 261,531 | 9 s in total | all three still red |
| `Merge` | 2026-09-21 | the cleanup-split commit | 6,124,691 | 56 s | green at one session; 5,196,830 before the split |
| `NonTxnDrop` | 2026-09-21 | the cleanup-split commit | 1,246,158 | 12 s | green under `OBSOLETE_IS_ROLLED_BACK = TRUE` and with `ActiveSetShape` off the roster, as every `NonTxn*` row of every table here is; 1,112,076 before the split |
| `NonTxnInsert` | 2026-09-21 | the cleanup-split commit | 16,969,548 | 2 min 42 s | green under `OBSOLETE_IS_ROLLED_BACK = TRUE`, finding `F6`'s unmerged fix; 15,788,049 before the split |
| `NonTxnFixed` | 2026-09-21 | the cleanup-split commit | 1,029,281 | 9 s | green; `OBSOLETE_IS_ROLLED_BACK = TRUE` is what this module verifies; 841,907 before the split |
| `NonTxnF2`, `NonTxnF4`, `NonTxnF5`, `NonTxnF6` | 2026-09-21 | the cleanup-split commit | first-violation counts | 4 s in total | each still produces its own finding |

`NonTxnDropTwo` is not in this table: it enables no cleanup group, so the split cannot move it, and its
47,958,711 stands; re-run as the control of that claim it gave 47,958,902 in 7 min 29 s.

The figures above are the re-measurement that closed debt `B6`. That debt was the witness sweeps, which had not
been re-run at the new action set: the split holds the parts lock from `CleanupDecide` to `CleanupGrab`, which
removes interleavings, so a witness that was red before it was not thereby red after it. All 83 rows of the
cleanup-enabled scenarios were re-run, together with the minimality halves of the three two-change witnesses,
and **no row changed colour**. The five rows the tables record as killed unfired keep their counts and their
placement in plan 5, task 4 (budget and calibration). `WITNESSES.md` carries the sweep.

The last table is the two-horizon change: `SET TRANSACTION SNAPSHOT` at the two reserved snapshots, which
separates the cleanup horizon from the log-retention horizon. It moves the `SetSnapshot` family and nothing
else, because the two registries are equal in every state the other scenarios reach.

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `Schema` | 2026-09-21 | the two-horizon commit | 1 | 0 s | green |
| `BaseSmall` | 2026-09-21 | the two-horizon commit | 47,381 | 1 s | green |
| `SetSnapshot` | 2026-09-21 | the two-horizon commit | 14,289,328 | 2 min 04 s | green; 14,289,310 on the previous commit, which is the counting noise |
| `SetSnapshotFixed` | 2026-09-21 | the two-horizon commit | 13,607,839 | 2 min 05 s | green; 13,607,915 before |
| `SetSnapshotF2` | 2026-09-21 | the two-horizon commit | a first-violation count | 1 s | **red on `NoPrematureDelete`**, finding `F2`'s first shape |
| `SetSnapshotF2Fixed` | 2026-09-21 | the two-horizon commit | 309,987 | 3 s | green, and now with `Assert_TailPtrNotRegressing` on its roster; 411,641 before, and the drop is the retention entry no longer following a target above `latest_snapshot` |
| `SetSnapshotF2Special` | 2026-09-21 | the two-horizon commit | 116,020 | 1 s | green; the fix at `NonTransactionalCSN` beside an ordinary target, whole roster |
| `SetSnapshotF2SpecialEV` | 2026-09-21 | the two-horizon commit | 134,553 | 1 s | green; the fix at `EverythingVisibleCSN`, without the four rows findings `F8` and `F9` falsify |
| `SetSnapshotF8` | 2026-09-21 | the two-horizon commit | a first-violation count | 1 s | **red on `RollbackNoLeak`**, which is finding `F8` |
| `SetSnapshotF9` | 2026-09-21 | the two-horizon commit | a first-violation count | 1 s | **red on `Assert_validateInfo`**, which is finding `F9` |
| the four `SetSnapshot`-sited witnesses | 2026-09-21 | the two-horizon commit | 2, 15,371, 14,042,565 and 152,085 | 1 min 45 s in total | `_size`, `_entry` and `Assert_TailPtrNotRegressing` red; the sortedness witness green at the exhaustive bounds, as documented, and red at the witness bounds at 436,697 |
| the three cleanup witnesses and `SnapshotEntryOnly` | 2026-09-21 | the two-horizon commit | 201,730, 38,466, 205,039 and 202,222 | 11 s in total | all four red |
| `NonTxnDrop` | 2026-09-21 | the two-horizon commit | 1,246,141 | 12 s | green; the control for the claim above |

Finding `F2`'s third shape came out of these runs. `MC_SetSnapshotF2Special` was red on
`Assert_TailPtrNotRegressing` before the variant separated the two horizons, on a nine-state trace, which is
why that property is now on `MC_SetSnapshotF2Fixed`'s roster as well.

The last table is the round that re-derived finding `F9`. `DropLock` gained the enrolment skip
`removePartsFromWorkingSet` applies, `MergeBegin` gained the retention entry a session's `Begin` already wrote,
and the finding moved from a shape the code cannot reach to one it can, which needs two sessions.

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `Schema` | 2026-09-21 | the `F9` re-derivation commit | 1 | 1 s | green |
| `BaseSmall` | 2026-09-21 | the `F9` re-derivation commit | 47,381 | 1 s | green |
| `SetSnapshot` | 2026-09-21 | the `F9` re-derivation commit | 14,289,315 | 2 min 05 s | green; 14,289,328 before, the counting noise |
| `SetSnapshotFixed` | 2026-09-21 | the `F9` re-derivation commit | 13,607,899 | 2 min 04 s | green; 13,607,839 before |
| `SetSnapshotF2` | 2026-09-21 | the `F9` re-derivation commit | a first-violation count | 3 s | **red on `NoPrematureDelete`**, finding `F2`'s first shape |
| `SetSnapshotF2Fixed` | 2026-09-21 | the `F9` re-derivation commit | 309,987 | 4 s | green, unchanged |
| `SetSnapshotF2Special` | 2026-09-21 | the `F9` re-derivation commit | 116,020 | 2 s | green, unchanged; the enrolment skip cannot fire at `NonTransactionalCSN` |
| `SetSnapshotF2SpecialEV` | 2026-09-21 | the `F9` re-derivation commit | 125,673 | 2 s | green with the three termination rows back on its roster; 134,553 before, and the drop is the enrolment the skip removed |
| `SetSnapshotF8` | 2026-09-21 | the `F9` re-derivation commit | a first-violation count | 2 s | **red on `RollbackNoLeak`**, which is finding `F8` |
| `SetSnapshotF9Sibling` | 2026-09-21 | the `F9` re-derivation commit | a first-violation count | 2 s | **red on `Assert_validateInfo`**, which is finding `F9` |
| `SetSnapshotF9SiblingFixed` | 2026-09-21 | the `F9` re-derivation commit | 1,858,362 | 14 s | green under `REMOVAL_REFUSES_UNCOMMITTED_CREATION`, finding `F9`'s fix |
| `Merge` | 2026-09-21 | the `F9` re-derivation commit | 6,124,691 | 58 s | green; **red on `Assert_getOldestSnapshot` in 28 states on the previous commit**, where `MergeBegin` left the merge transaction's retention entry unset |
| `NonTxnDrop` | 2026-09-21 | the `F9` re-derivation commit | 1,246,158 | 13 s | green; the `VIEW` counting noise the previous tables document |
| the five `Merge`-sited witnesses that touch enrolment or snapshots | 2026-09-21 | the `F9` re-derivation commit | 5,191,758, 2, 509,341, 11,506 and 3,771,760 | 1 min 12 s in total | all five red; `SingleRemover` and `NoPrematureDelete` moved with the enrolment skip |
| the four `SetSnapshot`-sited witnesses | 2026-09-21 | the `F9` re-derivation commit | 2, 12,594, 148,291 and 406,870 | 12 s in total | `_size`, `_entry` and `Assert_TailPtrNotRegressing` red; the sortedness witness at the witness bounds |
| the three cleanup witnesses and `SnapshotEntryOnly` | 2026-09-21 | the `F9` re-derivation commit | 196,609, 38,655, 208,181 and 205,424 | 11 s in total | all four red |
| `SingleRemover` in `SetSnapshotWitness` | 2026-09-21 | the `F9` re-derivation commit | 8,193,903 | 53 s | red, which is the witness bounds paying debt `B1`; 8,511,843 before |
| `F9FixInFlightOnly` in `SetSnapshotF9SiblingFixed` | 2026-09-21 | the `F9` re-derivation commit | 417,821 | 4 s | red, which is what the fix variant's second half is for |

`Merge`'s row is the one that mattered. `Assert_getOldestSnapshot` is on `MC_Merge`'s roster and the scenario
was not re-run when the retention registry was added, so the previous commit carried a red it did not know
about. It was reproduced deliberately, on a scratch copy of the directory with the `MergeBegin` line reverted,
before the fix was accepted.

The closing round changed no action and no property. It rewrote finding `F9`'s entry over its two shapes and
put `MC_SetSnapshotF2SpecialEV`'s roster, less `ErrorIsAbsent`, on `MC_SetSnapshotF9SiblingFixed`, which was
the only two-session exhaustive run at `SNAPSHOT_TARGETS = {3}` and was carrying three rows. These are the
sanity runs on that tree.

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `Schema` | 2026-09-21 | the closing commit | 1 | 1 s | green |
| `BaseSmall` | 2026-09-21 | the closing commit | 47,381 | 1 s | green |
| `SetSnapshotF9Sibling` | 2026-09-21 | the closing commit | a first-violation count, 22,609 and 22,618 on two runs | 2 s | **red on `Assert_validateInfo`**, which is finding `F9` |
| `SetSnapshotF9SiblingFixed` | 2026-09-21 | the closing commit | 1,858,364 and 1,858,346 on two runs | 15 s | green on the restored roster; the spread is the counting noise every run of this module shows |
| `SetSnapshotF2` | 2026-09-21 | the closing commit | a first-violation count | 3 s | **red on `NoPrematureDelete`**, finding `F2`'s first shape |
| `Merge` | 2026-09-21 | the closing commit | 6,124,691 | 56 s | green, unchanged |

| Scenario | Date | Commit | Distinct states | Time | Result |
|---|---|---|---|---|---|
| `Schema`, `BaseSmall` | 2026-09-21 | the restart-properties commit | 1 and 47,381 | 1 s each | green, unchanged |
| `Merge` | 2026-09-21 | the restart-properties commit | 6,124,691 | 1 min 00 s | green, identical to the state and to the generated count |
| `Crash` | 2026-09-21 | the restart-properties commit | 48,142,550 | 15 min 41 s | green with the four new rows; the distinct count and the generated count, 495,201,353, are both identical to the previous figure, which is what an added property and a dead action predict |
| `CrashUnsynced` | 2026-09-21 | the restart-properties commit | 29,484, a first-violation count | 3 s | **red on `LogEntryNeeded`**, which is finding `F11` |
| `CrashUnsyncedFixed` | 2026-09-21 | the restart-properties commit | 36,755,726 | 7 min 34 s | green under `ENTRY_KEPT_UNTIL_CSN_DURABLE`, finding `F11`'s fix |
| `Crash`, re-taken after `M48` and `M49` | 2026-09-21 | the restart-properties commit | 48,142,550 | 16 min 17 s | green, identical to the state and to the generated count for the third round running |
| `CrashLegacy` | 2026-09-21 | the restart-properties commit | 20,580,014 | 7 min 23 s | green with `LegacyLoads` on the roster; it is smaller than `Crash` because `P1` exists at init and cannot be inserted |
| `CrashUnsyncedFixed`, history-free rows added | 2026-09-21 | the restart-properties commit | 36,755,726 | 8 min 48 s | green; the payment of debt `B9` |
| `CrashHarm` | 2026-09-21 | the restart-properties commit | 106,627, a first-violation count | 9 s | **red on `F11Harm`**, which is finding `F11`'s harm at `RESTARTS_MAX = 2` |

`Crash`'s row is the control that matters. The round added four properties, one action and, after the unsynced
world found them, two disk-model repairs, and the scenario did not move by a state. A property cannot move it,
`UpdRemoveOldEntriesArm` is dead there for the reason `FINDINGS.md` gives under `M5`, and the two repairs are
dead in a configuration where every durability bit is set at the moment of the operation. `BaseSmall` and
`Merge` are the same control in `Durable` mode, where `DiskTypeOK` pins the new `payload_cached` bit to its
durable counterpart.

## 5. Witnesses {#witnesses}

A property no run can falsify proves nothing, so every property in `Invariants.tla` has a witness: one named
change to the model that must make that property fail. The table of them, with the change each one makes, the
scenario, the result and the cost, is `WITNESSES.md`. That file also lists the five witnesses whose change needs
an action `Base` does not enable, deferred with the plan that adds it, and the one property that entered
`Invariants.tla` without a design-document row at all.

## 6. Refinement parameters {#refinement-parameters}

Constants where the model deliberately runs a smaller value than the server, and why the smaller one is enough.

| Parameter | Model | C++ or design | Why |
|---|---|---|---|
| `MAX_STORE_RETRIES` | 2 | 20 (`MAX_RETRIES`, a file-scope constant at `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:30`) | the second collision already exhibits every distinct interleaving of two frames on one part; further retries repeat the same shapes at a linear cost in states |
| `NOEXCEPT_RETRY_BUDGET` | 2 | a 60-second budget | the design's budget is a time, not a count. A counter is what keeps the retry loop finite, and two retries reach both ends of it, the retry that succeeds and the budget that is exhausted |
| `Tasks` | `{}` in `Base`, `{i1}` in `Merge` | up to 2 background tasks | `Base` enables neither merges nor the mutation executor, so no task can act; the set is empty rather than unused so that quantifiers over it are trivially true. `Merge` has one covering part and therefore one possible merge, so a second task could only contend for the same two sources, which the reservation excludes |
| `Mutations` | `{}` in `Base` | one or more per scenario | same reason: the mutation actions are stubs until plan 4 |
| `TID_MAX` | 3 in `Base`, 2 in `BaseSmall`, 2 in `SetSnapshot` and the `NonTxn` modules, 1 in `NonTxnDrop` | the matrix asks for 3 | `Begin` draws from a monotone counter capped at this value, so the state space is finite by construction rather than cut by a constraint. Three is what three of the witnesses need to reach their target at all, and it is what produced the model's first counterexample |
| `CSN_MAX` | 36 in `Base`, 35 in `BaseSmall` | unbounded | real CSNs start at `FirstCSN = 33` and `KeeperCanAppend` requires `zk.seq < CSN_MAX`, so each unit above 33 buys one commit. It is set to the smallest value that lets every transaction commit; raising it to 38 changed the total by 0.002%, inside the counting noise |
| `Sessions` | `{k1, k2}` in `Base` | 2 | `SYMMETRY SymSessions` halves the fingerprints. Parts cannot join the symmetry: `SetToSeq` is a `CHOOSE`, so the next-state relation is not equivariant under a permutation of `Parts` |

No `CONSTRAINT` is applied in any committed configuration. Four were built or held in reserve and all four
were dropped. The first was built and measured for the `NonTxnDrop` half, a ghost counter of the
non-transactional queries a behaviour issues with a constraint allowing one, and it was rejected and reverted
because it cut two per cent of a space that was fifty per cent over budget; what that half needed was a
transaction fewer, not a query fewer. Two more were held in reserve by the plan and both turned out to be
unnecessary or vacuous: bounding the concurrently open store frames left a two-session run growing past 8.6 million states, and
bounding the transactions that reach `CommitAck` or `RollbackFinalize` is already implied by `TID_MAX`. A third,
restricting the kill to a session that holds no transaction, was built, measured and discarded because it both
failed to bound the run and lost the case the scenario exists for. `STATE_SPACE.md`, section "Bounds", has the
numbers. `VIEW BaseView` and `SYMMETRY SymSessions` are reductions, not bounds: they change how states are
counted, not which behaviours the model has.

## 7. Expected-red findings {#expected-red-findings}

The design document names four properties that are expected red on the baseline, each in a scenario later plans
build. One of the four is built and red today: `NoPrematureDelete` in `SetSnapshotF2`, which is finding `F2`.
The other three need scenarios plans 3 to 5 add, so they are neither built nor run. Outside those rows a red is
a finding to be explained rather than a result to be accepted: every property of `Base` is green.

| Property | Scenario | Why it is expected red |
|---|---|---|
| `NoAvoidableTermination` | `DiskFault` under `Terminate` | a transient store fault inside a `noexcept` callback terminates the process, which is the first defect Altinity PR 2396 addresses |
| `NoAvoidableTermination` | `Mutation` | `KILL MUTATION` in the commit window makes `setMutationCSN` raise a `LOGICAL_ERROR` inside `noexcept`, the second defect of that PR |
| `NoPrematureDelete` | `SetSnapshotF2` | a snapshot set by `SET TRANSACTION SNAPSHOT` does not enter `snapshots_in_use`, so cleanup can delete a part the transaction can still read. Red, 45 states: `traces/f2-set-snapshot-premature-delete.txt`. `SetSnapshot` itself is green, because the shape needs three transactions and a snapshot target above `FirstCSN` and its exhaustive bounds give neither; `FINDINGS.md`, finding F2, has the argument, the trace and the proposed code fix |
| `MutationRecoveredStrict` | `MutationCrash` | the `writeCSN` append is not synced, so a truncation that follows a termination can lose a committed mutation's CSN |

What the model has actually found is in `FINDINGS.md`: counterexamples on baseline runs with their
classification and resolution, defects of the model itself with the plan that fixes each, and the rows of the
design document the model contradicts. Plan 1 produced one counterexample, `F1`, which turned out to be a
property defect rather than a code defect, and two spec defects. The cleanup task produced `F2`, which is a code
defect with a proposed upstream fix.

## 8. Model decisions {#model-decisions}

Places where the model does not read the C++ literally, each with the reason and the code that decided it. These
are deliberate, and a reviewer who finds the model disagreeing with the code here should read this section before
filing it as a defect.

**Transaction identifiers are monotone integers.** `UpdPublishSnapshot` does not reset `local_tid_counter`, as
`TransactionLog::loadEntries` (:176) does with `local_tid_counter = Tx::MaxReservedLocalTID`. A reset lets two
different transactions carry the same local identifier at different times, which in the model would make the
history variables indexed by `Tids` ambiguous. Transaction identity in the code is the pair of the local
identifier and the start CSN, so nothing observable depends on the reset, and dropping it keeps `Tids` a clean
index.

**`NoActor` and record sentinels instead of the string `"None"`.** TLC compares values of different types by
raising an error rather than returning false, so a field that is sometimes a session tuple `<<"Session", k>>`
and sometimes the string `"None"` makes every comparison on it a potential run-time failure. Fields of that kind
carry a sentinel of their own type: `NoActor` for an actor, `EmptyInfo` for a version record, `NoRead` for a
read, `AbsentTxn` for a transaction.

**Isolation properties are checked at `SelectFinish`.** `StableRead`, `ReadYourWrites`, `NoUncommittedRead`,
`NoFutureRead`, `NoLostRead`, `NoDoubleRead` and `Atomicity` are action properties over the read that the step
produces, not state invariants over `client[k].last_read`. A state invariant over a stored read would be
re-evaluated in every later state, where the world has moved on and the read is stale, and it would report
violations that describe nothing the client ever saw.

**A session waits for its own rollback.** `RollbackStart` parks the client at `RollbackWait` and `RollbackReturn`
releases it, because `TransactionLog::rollbackTransaction` (:538) runs the whole body on the caller's thread.
Until it returns, `InterpreterTransactionControlQuery::executeRollback` (:118) has not reached
`setCurrentTransaction(NO_TRANSACTION_PTR)`, so the session cannot start another statement. A query on a
transaction already rolled back is refused with `INVALID_TRANSACTION` by the guard in `executeQueryImpl` (:2692),
which the action `QueryOnCancelled` models.

**`KillTransaction` requires the killer to be idle.** A session executes one query at a time, so the action is
guarded by `client[k].pc = "Idle"`. Without that guard a session could issue `KILL TRANSACTION` in the middle of
its own insert, select or drop, which `executeQuery` never does.

**The killer blocks in `KillWait` until the rollback finishes.** `InterpreterKillQueryQuery::execute` (:382)
calls `onException`, which runs `rollbackTransaction` synchronously on the killer's own thread, so the `KILL`
query returns only once the rollback it started is done. `KillReturn` releases the killer when no transaction
still names it as the rollback driver.

**A rollback body has exactly one driver.** `MergeTreeTransaction::rollback` (:377) opens with a
`compare_exchange_strong` on `csn` and returns `false` to every caller that loses it, so the body that marks the
created parts, outdates them, restores the removed ones and unlocks their removal identifiers runs on exactly
one thread. The model keeps that winner in `txn[t].rb_driver`, separately from `holders`, which tracks the
`MergeTreeTransactionPtr` holders: a `KILL` wins the exchange and destroys no shared pointer, so it names a
driver and leaves the holders alone. This is the correction that made the two-session scenario finite in
practice, worth more than two orders of magnitude; `STATE_SPACE.md` has the measurements.

**A session may kill its own transaction.** `InterpreterKillQueryQuery` looks the victim up by transaction-id
hash and calls `onException` on whatever it finds (:404), with no check that the victim belongs to another
session, and the kill does not detach it. Allowing this is why `BaseSmall`, which has one session, can reach a
kill at all.

**`Atomicity` excludes the reader's own removals.** The set `C` of a committed writer's creations that the
reader must see leaves out the parts the reader is itself dropping. `VersionInfo::isVisible` returns `false` on
`removal_tid == current_tid` (`VersionInfo.cpp:171`) before it reaches the creation clause at `:178`, so own
removal wins over visible creation for the reader exactly as it does for the writer. This came out of
counterexample `F1`; the design document states only the writer's half, recorded as spec defect `S1`.

**`Assert_validateInfo` is stated over the record about to be stored.** `validateInfo` runs inside
`updateInfoWithRefreshDataThenStoreAndSetMetadata` (:345) on the tentative record, after `updateCSNIfNeeded` and
before `storeInfo`, not on the in-memory record at rest. The invariant is therefore evaluated on the frame's
tentative record too; an invariant over memory alone would miss every record the code rejects before publishing
it, which is the whole point of the assertion.

**`KillTransaction` is disabled while the victim is `Committing`.** The action requires
`txn[t].state = "Running"`, so the model has no step for a `KILL TRANSACTION` that lands between `beforeCommit`
and the flip. The server runs that query: `InterpreterKillQueryQuery` finds the victim and calls `onException`,
`MergeTreeTransaction::rollback` (:377) loses the `compare_exchange_strong` against the `CommittingCSN` that
`beforeCommit` (:308) installed, and the `KILL` reports `CancelCannotBeSent` while the commit proceeds. The two
are stutter-equivalent: nothing in the model's state changes and the victim's outcome is the same. Plan 4 puts
`KILL MUTATION` into exactly that window, where the equivalence has to be re-derived rather than assumed.
