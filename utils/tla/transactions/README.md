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
that reaches finding F2; the first is expected red on `NoPrematureDelete` and the second is green). It downloads `tla2tools.jar` into `tmp/` if it is missing, and it uses `-Xmx16g` and a
45-minute `timeout`.

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
the exit status is 0 only for `RED`. Its default timeout is 600 seconds and `WITNESS_TIMEOUT` overrides it. See
`WITNESSES.md`.

## 3. Code map {#code-map}

One row per action of `Server.tla` and `Parts.tla`. The C++ column names the file, the function and the step
boundary the action stands for: what has happened when the action fires, and what the next action of the same
machine picks up. Line numbers are of the baseline tree.

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
| `DropLock(k)` | `src/Storages/StorageMergeTree.cpp` | `StorageMergeTree::dropPartition`, the branch taken when the query has a transaction | the wait is over; `lockParts` is taken and `getVisibleDataPartsVectorInPartition` under that lock has produced the set of parts to remove |
| `DropEnrol(k, q)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::removeOldPart` (:213), reached from `MergeTreeData::removePartsFromWorkingSet` (:7034) | `mutex` taken, `checkIsNotCancelled` passed, `lockRemovalTID` won, `q` pushed to `removing_parts`; `setAndStoreRemovalTID` has not started |
| `DropStore(k, q)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::removeOldPart` (:213) | `setAndStoreRemovalTID(tid)` has finished and the `mutex` scope ends; the loop moves to the next part of the batch |
| `DropOutdate(k)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::removePartsFromWorkingSet` (:7034) | the state loop after the removal metadata of the whole batch is written: every part of the batch becomes `Outdated`, still under the acquired parts lock. The model releases the merge blocker here; in the code the blocker's `ActionLock` is released a little later, when `dropPartition`'s scope ends |
| `CommitBefore(k)` | `src/Interpreters/MergeTreeTransaction.cpp` | `MergeTreeTransaction::beforeCommit` (:284) | `waitForMutation` has returned for every attached mutation and the CAS `UnknownCSN -> CommittingCSN` has succeeded; no Keeper request has been made |
| `CommitError(k)` | `src/Interpreters/InterpreterTransactionControlQuery.cpp` | `InterpreterTransactionControlQuery::executeCommit` (:55) | the `getState() != RUNNING` guard (:64) refuses a `COMMIT` on a transaction that `KILL TRANSACTION` already rolled back, with `INVALID_TRANSACTION`, before `commitTransaction` is called. The same error comes from the failed CAS in `beforeCommit` (:308) when the kill lands after the guard |
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

### Outdated-parts cleanup thread {#code-map-cleanup}

| Action | C++ file | Function | Step boundary |
|---|---|---|---|
| `CleanupGrab(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `MergeTreeData::grabOldParts` (:4074) | one part moves from `Outdated` to `Deleting` under `lockParts`: its version `canBeRemoved` (:4140), nobody else holds it (`isSharedPtrUnique`, :4150), and it is not an empty part still covering an `Outdated` one (:4158). The code grabs a set under one lock and the model one part per step; the removal-time and mutation-parent conditions at :4167 are time and zero-copy-replication bookkeeping, which `force` covers |
| `CleanupValidate(p)` | `src/Storages/MergeTree/IMergeTreeDataPart.cpp` | `IMergeTreeDataPart::remove` (:2928) through `assertHasValidVersionMetadata` (:2863) and `VersionMetadata::hasValidMetadata` | the `chassert` on the grabbed part passes, on the path `clearPartsFromFilesystemAndRollbackIfError` (`MergeTreeData.cpp:4566`) takes for each grabbed part. This row and the validation half of `CleanupDeleteFail` model a `DEBUG_OR_SANITIZER_BUILD`: in release `chassert` is `(void)sizeof(!(x))` (`base/base/defines.h:84-102`) and does not evaluate its argument, so nothing validates and nothing refuses |
| `CleanupDeleteOk(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `clearPartsFromFilesystemAndRollbackIfError` (:4566) and `removePartsFinally` (:4217-4240), under `lockParts` | the directory is gone in both disk layers and the part leaves `data_parts_indexes` |
| `CleanupDeleteFail(p)` | `src/Storages/MergeTree/MergeTreeData.cpp` | `rollbackDeletingParts` (:4204-4215), under `lockParts` | the part goes back to `Outdated`. Two producers: the `CORRUPTED_DATA` `hasValidMetadata` raises, and a filesystem error in `clearPartsFromFilesystemImpl`. The second needs a disk fault, so its disjunct is `FALSE` until plan 5 raises `DISK_FAULTS_MAX` |

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

| Action | Plan |
|---|---|
| `MergeBegin(i)`, `MergeSelect(i)`, `MergeWrite(i)`, `MergeRename(i)`, `MergeFail(i)` | plan 2 |
| `NtInsert(p)`, `NtBatchStart(B)`, `NtBatchPreflight(p)`, `NtBatchLock(p)`, `NtBatchStore(p)`, `NtBatchEnd`, `NtDropCover` | plan 2 |
| `CommitUnknown(k)` | plan 3 |
| `UpdReconnect`, `UpdSwapUnknownLists`, `UpdFinalizeUnknown(t)` | plan 3 |
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
| `Tasks` | `{}` in `Base` | up to 2 background tasks | `Base` enables neither merges nor the mutation executor, so no task can act; the set is empty rather than unused so that quantifiers over it are trivially true |
| `Mutations` | `{}` in `Base` | one or more per scenario | same reason: the mutation actions are stubs until plan 4 |
| `TID_MAX` | 3 in `Base`, 2 in `BaseSmall` | the matrix asks for 3 | `Begin` draws from a monotone counter capped at this value, so the state space is finite by construction rather than cut by a constraint. Three is what three of the witnesses need to reach their target at all, and it is what produced the model's first counterexample |
| `CSN_MAX` | 36 in `Base`, 35 in `BaseSmall` | unbounded | real CSNs start at `FirstCSN = 33` and `KeeperCanAppend` requires `zk.seq < CSN_MAX`, so each unit above 33 buys one commit. It is set to the smallest value that lets every transaction commit; raising it to 38 changed the total by 0.002%, inside the counting noise |
| `Sessions` | `{k1, k2}` in `Base` | 2 | `SYMMETRY SymSessions` halves the fingerprints. Parts cannot join the symmetry: `SetToSeq` is a `CHOOSE`, so the next-state relation is not equivariant under a permutation of `Parts` |

No `CONSTRAINT` is applied. Two bounds were held in reserve by the plan and both turned out to be unnecessary or
vacuous: bounding the concurrently open store frames left a two-session run growing past 8.6 million states, and
bounding the transactions that reach `CommitAck` or `RollbackFinalize` is already implied by `TID_MAX`. A third,
restricting the kill to a session that holds no transaction, was built, measured and discarded because it both
failed to bound the run and lost the case the scenario exists for. `STATE_SPACE.md`, section "Bounds", has the
numbers. `VIEW BaseView` and `SYMMETRY SymSessions` are reductions, not bounds: they change how states are
counted, not which behaviours the model has.

## 7. Expected-red findings {#expected-red-findings}

The design document names four properties that are expected red on the baseline, each in a scenario later plans
build. None of them is in plan 1's slice, so plan 1 has no expected red at all: every property of `Base` is
green, and any red on a `Base` run is a finding to be explained rather than a result to be accepted.

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
