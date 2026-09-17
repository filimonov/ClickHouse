---
description: 'Design of a TLA+ model of MergeTree transactions as implemented in upstream ClickHouse master: which entities, transitions, failures and invariants the model covers, how it is kept faithful to the C++ code, how TLC runs are bounded so that they finish, and what is deliberately left out of the first version. The model exists to find bugs in the implementation, not to document it.'
sidebar_label: 'MergeTree transactions, TLA+ model'
sidebar_position: 20
slug: /superpowers/specs/mergetree-transactions-tla-design
title: 'A TLA+ model of MergeTree transactions for finding bugs with TLC'
doc_type: 'design'
---

# A TLA+ model of MergeTree transactions for finding bugs with TLC {#mergetree-transactions-tla-design}

Revision 1, 2026-09-17. Baseline: upstream `ClickHouse/ClickHouse` master at commit `2c24b6b9291e`. The transaction
sources listed in the code map below are identical between `d1ba1699a271` (2026-09-16) and this commit; every
function name in this document refers to that tree.

## Goal {#goal}

Build a TLA+ specification of the MergeTree transaction algorithm that is faithful to the current C++ implementation
at the granularity where crashes and thread interleavings matter, and check it with TLC against the properties a user
of transactions relies on: an acknowledged `COMMIT` is durable, reads inside a transaction are snapshot-isolated,
parts and transaction-log entries are garbage-collected only when nobody can need them, and two transactions cannot
both remove the same part. The expected outcome is a list of counterexample traces, each mapped back to a sequence
of C++ calls, or evidence that the bounded model has none.

The model is written so that a `ReplicatedMergeTree` model can later be built on top of the same Keeper and disk
modules; that extension is out of scope here.

## Why the RFC is not the source {#why-the-rfc-is-not-the-source}

The original RFC (issue #22086) describes the MVCC idea: a `tid` per writing transaction, `creation_tid` and
`removal_tid` per part, CSNs allocated at commit, visibility by comparing CSNs with a snapshot. The implementation
kept that core and added everything the model actually has to reason about: a commit whose Keeper response is lost
(`unknown_state_list`), a rollback that touches several parts without a common lock, a per-part removal lock in
memory, a `txn_version.txt` file that is rewritten atomically with an optimistic `storing_version`, deferred
persistence for parts that no transaction has touched, transaction-log truncation through `tail_ptr`, mutations
tied to a transaction, and a restart procedure that repairs metadata from the Keeper log. The RFC's replicated
section was never implemented (`StorageReplicatedMergeTree` throws `NOT_IMPLEMENTED`). The RFC therefore
contributes the invariants; the transitions come from the code.

## Scope of the first version {#scope}

Covered:

- One server, one Keeper, one local disk, one `MergeTree` table in an `Atomic` database.
- Client operations inside an explicit transaction: `BEGIN`, `INSERT`, `SELECT`, `ALTER TABLE DROP PARTITION`,
  `ALTER TABLE DETACH PARTITION` (identical to `DROP` for the model, the clone into `detached/` is not modelled),
  `ALTER TABLE UPDATE`/`DELETE` (a mutation), `COMMIT`, `ROLLBACK`, and the automatic rollback on any exception.
- The same operations without a transaction (`NonTransactionalTID`), interleaved with transactional ones.
- Background merges, which run as their own transaction, and the mutation executor.
- The transaction-log updating thread, including log truncation, and the outdated-parts cleanup thread.
- Failures: Keeper response lost or session expired at commit, server crash at any point with loss of every
  write that was not fsynced, a disk write that throws, and an exception thrown by a query between any two steps.
- Server restart with metadata repair from disk and Keeper.

Not covered in the first version, to be added later as separate work items:

- Every other partition operation: `ATTACH`, `MOVE`, `REPLACE`, `FETCH`, `FREEZE`, `UNFREEZE`, `DROP PART`,
  `DROP DETACHED`. The `detached/` directory itself is not modelled.
- `KILL TRANSACTION` and `KILL MUTATION` as client commands. The internal `killMutation` call made by a rollback
  is modelled.
- Backups and restores.
- `SYSTEM` commands: `STOP MERGES`, `START MERGES`, `SYNC TRANSACTION LOG`, `RESTART REPLICA`, `DROP ... CACHE`.
- `OPTIMIZE`, `TRUNCATE`, `ALTER ... MODIFY` and other metadata alters, projections, lightweight deletes and
  patch parts, `ReplacingMergeTree` and the other special engines.
- Implicit transactions (`implicit_transaction = 1`): they are a `BEGIN` plus `COMMIT` or rollback around one
  query and add nothing to the state space.
- Disk corruption (bit flips or partial writes inside `txn_version.txt` and `mutation_N.txt`) and memory
  corruption (bit flips in the in-memory `VersionInfo` or `tid_to_csn`, allocation failure inside `noexcept`
  paths). The disk module is designed so that corruption can be added as one more outcome of a write.
- `ReplicatedMergeTree`. The Keeper module is designed for it.
- Weak-memory effects. The C++ code reads some fields with `memory_order_relaxed` and relies on a strict reload
  (`failback_with_strict_load_csn`); TLA+ models sequentially consistent shared state. The model reproduces the
  observable race (a lookup in `tid_to_csn` that misses a concurrent commit) as an explicit interleaving instead.

## Structure of the model {#structure}

The specification is split into modules with one responsibility each. Every module owns a disjoint set of
variables; the root module `MergeTreeTransactions.tla` declares all variables, extends the others and defines
`Init`, `Next` and the fairness used by the liveness configuration.

| Module | Models | C++ counterpart |
|---|---|---|
| `Keeper.tla` | The CSN log as a sequence of `csn-NNN` znodes, `tail_ptr`, session state, the three outcomes of a request | `zkutil::ZooKeeper` as used by `TransactionLog` |
| `Disk.tla` | Per-part `txn_version.txt` and per-mutation `mutation_N.txt` as a durable copy plus a page-cache copy, `tmp` files, fsync, crash | `VersionMetadataOnDisk::storeInfoToDataPartStorage`, `MergeTreeMutationEntry::writeCSN` |
| `TxnLog.tla` | `TransactionLog` in-memory state and the updating thread | `src/Interpreters/TransactionLog.cpp` |
| `Parts.tla` | Part states, in-memory `VersionInfo`, the removal lock, deferred persistence, visibility, removability | `VersionMetadata.cpp`, `VersionMetadataOnDisk.cpp`, `VersionInfo.cpp`, `MergeTreeData` working set |
| `Server.tla` | Transactions as step machines: begin, insert, select, drop, mutate, commit, rollback, merge, mutation executor, cleanup, restart | `MergeTreeTransaction.cpp`, `StorageMergeTree.cpp`, `MergeTreeData.cpp`, `MergePlainMergeTreeTask.cpp`, `MutatePlainMergeTreeTask.cpp` |
| `Client.tla` | Sessions, the outcome the client observed for each `COMMIT`, the part sets each `SELECT` returned | `InterpreterTransactionControlQuery.cpp`, `executeQuery.cpp` |
| `Invariants.tla` | Every property from the section on invariants | |
| `MC_<Scenario>.tla`, `MC_<Scenario>.cfg` | One per row of the scenario matrix: constants, enabled actions, state constraints, symmetry | |

## Entities and state variables {#entities}

Every variable is listed with the field or function it is taken from. Fixed finite sets: `Sessions`, `Tids`
(transaction identifiers, drawn in order), `Parts` (the part universe below), `Mutations`.

### Transactions {#entities-transactions}

`txn[t]` for `t \in Tids`, from `MergeTreeTransaction`:

- `state \in {Absent, Running, Committing, Committed, RolledBack}`, together with `csn` when `Committed`. In the
  code this is the single atomic `csn` holding `UnknownCSN`, `CommittingCSN`, a real CSN or `RolledBackCSN`.
- `snapshot`: the CSN read from `latest_snapshot` at `beginTransaction`.
- `creating`, `removing`: sequences of parts, the fields `creating_parts` and `removing_parts`.
- `mutations`: the set of mutations started by the transaction.
- `owner \in {Session(k), Merge, MutationExec}`: who holds the `MergeTreeTransactionHolder`.
- `pc` plus a work list: the position inside a multi-step operation (`afterCommit`, `rollback`, a batch of
  `removeOldPart` calls). Steps are separated exactly where the code has no lock held across them.

### Transaction log {#entities-txnlog}

From `TransactionLog`, all in memory and lost on crash:

- `tid_to_csn`: the loaded part of the Keeper log.
- `latest_snapshot`, `local_tid_counter`, `last_loaded_entry`.
- `running_list`: the set of transactions in `Running` or `Committing` state.
- `snapshots_in_use`: a bag of CSNs, one per running transaction, from which `getOldestSnapshot` reads the minimum.
- `tail_ptr`: the in-memory copy.
- `unknown_state_list`, `unknown_state_list_loaded`: the two lists of `tryFinalizeUnknownStateTransactions`.
- `server_completely_started`: gates `removeOldEntries`.

### Keeper {#entities-keeper}

- `zk_log`: a sequence of records `[csn, tid]`, one per `csn-NNN` znode, `tid` empty for the placeholder created at
  startup by `loadLogFromZooKeeper`.
- `zk_seq`: the sequential-name counter.
- `zk_tail_ptr`: the `tail_ptr` znode.
- `session \in {Alive, Expired}`.

Keeper is linearizable and durable. Its only nondeterminism is the outcome of a request as seen by the server.

### Parts {#entities-parts}

`part[p]` for `p \in Parts`, from `IMergeTreeDataPart` and `VersionMetadataOnDisk`:

- `pstate \in {Absent, Temporary, PreActive, Active, Outdated, Deleted}`: `DataPartState` plus physical removal.
- `mem`: the in-memory `VersionInfo`: `creation_tid`, `creation_csn`, `removal_tid`, `removal_csn`,
  `storing_version`.
- `lock`: `removal_tid_lock_hash`, a tid or `0`.
- `deferrable`, `deferred`: `is_persist_deferrable` and `deferred_persist_info`.

The part universe is fixed and its covering relation is given as a constant:

| Part | Role | Covers |
|---|---|---|
| `P1`, `P2` | base parts of one partition, produced by `INSERT` | |
| `M12` | result of a merge of `P1` and `P2` | `P1`, `P2` |
| `P1m`, `P2m` | results of a mutation of `P1` and `P2` | `P1` resp. `P2` |
| `E` | the empty part a non-transactional `DROP PARTITION` creates to cover the partition | every other part |

Data is not modelled. A part is identified by its name, and "the rows a `SELECT` sees" is the set of base parts
obtained by expanding the covering relation.

### Disk {#entities-disk}

`disk[p]` for parts and `mdisk[m]` for mutations:

- `durable`: the `VersionInfo` (resp. mutation record) that survives a crash, or `None`.
- `cached`: what a reader sees before a crash, or `None`. Writes go here; `Fsync` copies it to `durable`; `Crash`
  discards it.
- `tmp`: whether `txn_version.txt.tmp` exists, with the same two layers.
- `dir`: whether the part directory itself exists, with the same two layers, so that a crash can lose a
  `Temporary` or `PreActive` part.

The constant `FSYNC_PART_DIRECTORY` decides whether the rename in `storeInfoToDataPartStorage` is durable without
a directory fsync. With it off, a crash after the rename may leave the durable layer in the state before the
rename: `tmp` present and the old file present, which `loadMetadata` case 2 treats as a rolled-back part.

### Mutations {#entities-mutations}

`mut[m]`: `tid`, `csn` (in memory), `registered` (present in `current_mutations_by_version`), `done`, `killed`,
and the on-disk record in `mdisk[m]` with `tid` and `csn`. `writeCSN` appends without fsync.

### Client {#entities-client}

`client[k]` for `k \in Sessions`:

- `current`: the transaction bound to the session, or none.
- `outcome`: the result the client received for the last `COMMIT`: `None`, `Acked`, `Error`, `UnknownStatus`.
- `observed`: a sequence of part sets, one per `SELECT` executed inside the current transaction. Reset at `BEGIN`.
- `wait_mode`: the setting `wait_changes_become_visible_after_commit_mode`, a constant per scenario.

### Background actors {#entities-background}

The updating thread, the cleanup thread, the merge selector and the mutation executor are actors with a `pc` each
and no other state. There is one of each.

## Actions {#actions}

Each action is one function or one step of a function. The table gives the C++ location; the text explains what
the action does in the model. Preconditions that the code enforces with a `chassert` become invariants, not
preconditions, so that a violated assertion shows up as a counterexample.

### Client and session {#actions-client}

| Action | C++ |
|---|---|
| `Begin(k)` | `TransactionLog::beginTransaction` |
| `InsertWrite(k, p)` | `MergedBlockOutputStream` constructor, `setAndStoreCreationTID` |
| `InsertPreActive(k, p)` | `MergeTreeData::renameTempPartAndReplace` |
| `InsertCommit(k, p)` | `MergeTreeData::Transaction::commit`, `addNewPartAndRemoveCovered` |
| `Select(k)` | `getVisibleDataPartsVector`, `filterVisibleDataParts`, `VersionMetadata::isVisible` |
| `DropStart(k)` | `StorageMergeTree::dropPartition`, `removePartsFromWorkingSet(txn)` |
| `DropLockOne(k, p)` | `MergeTreeTransaction::removeOldPart`, `lockRemovalTID` then `setAndStoreRemovalTID` |
| `DropOutdate(k)` | the state loop at the end of `removePartsFromWorkingSet` |
| `MutateStart(k, m)` | `StorageMergeTree::startMutation`, `addMutation` |
| `CommitBefore(k)` | `MergeTreeTransaction::beforeCommit` |
| `CommitCreateCSN(k)` | `TransactionLog::commitTransaction`, the `multi` with one sequential `create` |
| `CommitStoreCreation(k, p)`, `CommitStoreRemoval(k, p)`, `CommitStoreMutation(k, m)` | `afterCommit`, one `setAndStore...CSN` or `setMutationCSN` each |
| `CommitFlip(k)` | `afterCommit`, `csn.exchange(assigned_csn)` |
| `CommitFinalize(k)` | `finalizeCommittedTransaction`, erase from `running_list` and `snapshots_in_use` |
| `CommitAck(k)` | `executeCommit` returns; `waitForCSNLoaded` for the default wait mode |
| `CommitUnknown(k)` | the `catch` in `commitTransaction`: append to `unknown_state_list`, throw `UNKNOWN_STATUS_OF_TRANSACTION` or return `CommittingCSN` |
| `RollbackStart(k)` | `TransactionLog::rollbackTransaction`, `MergeTreeTransaction::rollback`, the CAS to `RolledBackCSN` |
| `RollbackKillMutation(k, m)` | `killMutation` |
| `RollbackMarkCreated(k, p)` | `setAndStoreCreationCSN(RolledBackCSN)` |
| `RollbackOutdateCreated(k, p)` | `removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part})` |
| `RollbackRestore(k, p)` | `restoreAndActivatePart` |
| `RollbackUnlock(k, p)` | `setAndStoreRemovalTID(EmptyTID)` then `unlockRemovalTID` |
| `RollbackFinalize(k)` | erase from `running_list` and `snapshots_in_use`, `afterFinalize` |
| `Fail(k)` | any exception between two steps of a query; leads to `RollbackStart` through `txn->onException()` |

`Begin` takes `snapshot := latest_snapshot`, allocates the next tid with `start_csn = snapshot`, inserts the
snapshot into `snapshots_in_use` and the transaction into `running_list`, all in one step because the code holds
`running_list_mutex` throughout.

`InsertWrite` creates the part in state `Temporary` with `creation_tid = t` and writes `txn_version.txt` through the
disk module. `InsertCommit` computes the covered parts from the covering relation (`getActivePartsToReplace` and
`getCoveredOutdatedParts` filtered by visibility), records the part in `creating`, and for every covered part runs
the same lock-and-store steps as `DropLockOne` before flipping states. A client `INSERT` never covers anything in
the chosen universe; the step exists because merges and mutations reuse it.

`Select` is one step: it evaluates `isVisible(snapshot, tid)` for every part in `Active` or `Outdated` state and
appends the result to `observed`. The slow path of `isVisible` reads `tid_to_csn`; the model performs that read in
the same step, which is exact because the code takes the value under `TransactionLog::mutex` and never caches it
for the purpose of visibility (only `updateCSNIfNeeded` writes CSNs back, and that is a separate action).

`DropStart` selects the visible parts of the partition. `DropLockOne` runs per part: CAS on `lock` from `0` to
`t` (a failure throws `SERIALIZATION_ERROR`, which is a `Fail`), then `setAndStoreRemovalTID(t)`, which goes
through `updateInfoWithRefreshDataThenStoreAndSetMetadata` and the disk module. `DropOutdate` flips every part of
the batch to `Outdated` in one step under `lockParts`.

`CommitBefore` waits for the transaction's mutations to be done (modelled as a precondition `done` or `killed`),
then CAS `Unknown -> Committing`. `CommitCreateCSN` is the commit point; its three outcomes are in the failure
section. The `afterCommit` steps run in the order of the code: creations, removals, mutations, then `CommitFlip`.
`CommitFinalize` removes the transaction from the running list and releases the snapshot, and `CommitAck` delivers
`Acked` to the client. With `wait_mode = WAIT_UNKNOWN`, `CommitUnknown` returns `CommittingCSN` and the client
blocks in `waitStateChange` until the updating thread finalizes the transaction; with any other mode the client
receives `UnknownStatus` and the transaction is detached from the session.

`RollbackStart` is the CAS `Unknown -> RolledBack`; if the transaction is already `RolledBack` (concurrent
`killMutation`) or already `Committed`, rollback does nothing. The subsequent steps run in the code's order:
mutations killed, created parts marked `RolledBackCSN` on disk, created parts outdated, removed parts restored to
`Active` (unless created by the same transaction), removed parts cleared on disk and unlocked.

`Fail(k)` is enabled between any two steps of `Insert`, `Drop`, `Mutate`, `Select` and before `CommitBefore`. It
models `SERIALIZATION_ERROR`, `STALE_VERSION` from the disk module, a disk write fault outside `noexcept`, and
any unrelated exception. It is the only way a query ends without completing.

### Updating thread {#actions-updating}

| Action | C++ |
|---|---|
| `UpdReconnect` | `runUpdatingThread`, `expired()` branch, `sync` |
| `UpdLoadNewEntries` | `loadNewEntries`, `loadEntries` |
| `UpdRemoveOldEntriesSetTail` | `removeOldEntries` up to `tail_ptr.store` |
| `UpdRemoveOldEntriesDelete(csn)` | `removeOldEntries`, one `tryRemove` and one `tid_to_csn.erase` |
| `UpdSwapUnknownLists` | `tryFinalizeUnknownStateTransactions`, the two swaps |
| `UpdFinalizeUnknown(t)` | `tryFinalizeUnknownStateTransactions`, one transaction: `getCSN` then `finalizeCommittedTransaction` or `assertTIDIsNotOutdated` plus `rollbackTransaction` |

One iteration of the thread is the sequence reconnect, load, remove old, swap, finalize each. Any Keeper request
inside it may fail with a hardware error, which ends the iteration and starts a new one; `unknown_state_list`
entries survive the failed iteration. `UpdRemoveOldEntriesSetTail` is enabled only after
`server_completely_started` and sets `tail_ptr := getOldestSnapshot()`, which is `latest_snapshot` when no
transaction runs. `UpdRemoveOldEntriesDelete` removes entries with `tid.start_csn < tail_ptr`, keeps the entry
with `csn = latest_snapshot`, and is one action per entry because the code issues one request per entry.

### Cleanup thread {#actions-cleanup}

| Action | C++ |
|---|---|
| `CleanupGrab(p)` | `grabOldParts`, `VersionMetadata::canBeRemoved` |
| `CleanupDelete(p)` | `clearOldPartsFromFilesystem`, part removed from disk and from memory |

`CleanupGrab` is enabled for an `Outdated` part when `canBeRemoved` holds with `getOldestSnapshot` and no `Select`
of that part is in flight (the `isSharedPtrUnique` check). `CleanupDelete` sets `pstate := Deleted` and removes the
directory in both disk layers.

### Merge {#actions-merge}

| Action | C++ |
|---|---|
| `MergeBegin` | `scheduleDataProcessingJob`, `beginTransaction` with `autocommit = false` |
| `MergeSelect` | `selectPartsToMerge` with `txn`, sources from `Active` and `Outdated` visible to the merge transaction |
| `MergeWrite` | `MergeTask`, `setAndStoreCreationTID` on `M12` |
| `MergeFinish` | `MergePlainMergeTreeTask::finish`, `renameMergedTemporaryPart`, `Transaction::commit` with covered parts locked through `removeOldPart` |
| `MergeCommit*` | the same `Commit*` steps with `throw_on_unknown_status = false` |
| `MergeFail` | exception anywhere before `MergeCommitBefore`; the holder rolls the transaction back |

A merge transaction is an ordinary transaction with `owner = Merge`; it never issues `Select` and its `Commit` has
no client. `MergeSelect` requires both sources to be visible to the merge's own snapshot, which excludes parts with
an uncommitted creation or removal, and requires no other merge in flight.

### Mutation executor {#actions-mutation}

| Action | C++ |
|---|---|
| `MutSelect(m, p)` | `selectPartsToMutate`: for a transactional mutation `isVisible(first_mutation_tid.start_csn, tid)`, for a non-transactional one `isVisible(MaxCommittedCSN, EmptyTID)`; `tryGetTransactionForMutation` |
| `MutWrite(m, p)` | `MutateTask`, `setAndStoreCreationTID` on `Pm` with the mutation's tid |
| `MutFinish(m, p)` | `MutatePlainMergeTreeTask::executeStep`, `renameTempPartAndReplaceUnlocked` plus `Transaction::commit` under `lockParts`, the source part locked and stored through `removeOldPart` |
| `MutDone(m)` | `updateMutationEntriesErrors`, `is_done` |
| `MutFail(m)` | exception in the executor: `txn->onException()` |
| `KillMutation(m)` | `StorageMergeTree::killMutation`: unregister, `rollbackTransaction` if the transaction runs, remove the file |

The mutation result belongs to the transaction that started the mutation (`creating` gets `Pm`, `removing` gets
`P`), so the client's `Commit` and `Rollback` cover it. A mutation whose transaction is already `Committed` when
`MutSelect` runs proceeds without a transaction pointer, as the code allows when `csn` is known; with `csn`
unknown and the transaction gone the code throws `LOGICAL_ERROR`, which the model records as an invariant
violation.

### Non-transactional queries {#actions-nontransactional}

| Action | C++ |
|---|---|
| `NtInsert(p)` | as `Insert*` with `NonTransactionalTID`, `creation_csn = NonTransactionalCSN` at creation |
| `NtDropLock` | `NonTransactionalRemovalLocks::lock`: lock every part of the batch, refuse if any has an uncommitted creation |
| `NtDropStore(p)` | `NonTransactionalRemovalLocks::store`: `setAndStoreRemovalTID(NonTransactionalTID)` per part, `removal_csn = NonTransactionalCSN` immediately, then unlock |
| `NtDropCover` | `dropPartition` without a transaction: the empty part `E` created and committed over the partition |
| `NtMerge*` | as `Merge*` with `txn = nullptr` |
| `NtMutate*` | as `Mut*` with `NonTransactionalTID` |

Non-transactional removal is refused with `SERIALIZATION_ERROR` when a part's creation is not committed; the
model keeps this as a precondition of `NtDropLock` and additionally as an invariant on the shape
`creation_csn = 0, removal_csn = NonTransactionalCSN`, which `validateInfo` rejects.

### Disk {#actions-disk}

| Action | C++ |
|---|---|
| `StoreInfo(p, info)` | `updateInfoWithRefreshDataThenStoreAndSetMetadata`: read `storing_version` (from `deferred` or `cached`), compare, write `tmp`, fsync `tmp`, rename, `setInfo` |
| `StoreDeferred(p, info)` | the deferred branch of `storeInfoUnlocked` |
| `Fsync(p)` | copies `cached` to `durable` for one file; enabled at any time (the kernel writes back) |
| `Crash` | discards every `cached` layer; see the failure section |

`StoreInfo` is one action in the model because `persisted_info_mutex` serializes the read-compare-write sequence
per part. Its version check fails when `cached.storing_version` differs from `info.storing_version`, which makes the
caller reload and retry; after `MAX_RETRIES` failures the caller throws `STALE_VERSION`, recorded as an invariant
violation because the code treats it as unreachable. The write of `tmp` plus fsync makes `tmp` durable; the rename
makes the new content `cached`, and `durable` only when `FSYNC_PART_DIRECTORY` is on or a later `Fsync` runs.

### Restart {#actions-restart}

`Crash` followed by `Restart`, the latter as a sequence of steps:

| Step | C++ |
|---|---|
| `RestartLoadLog` | `TransactionLog::loadLogFromZooKeeper`: creates one placeholder `csn-` znode, loads `tid_to_csn`, `latest_snapshot`, `tail_ptr` |
| `RestartLoadPart(p)` | `MergeTreeData::loadDataPart`: `VersionMetadataOnDisk::loadMetadata` (the four cases), `updateCSNIfNeeded`, `validateInfo`, store if updated, then `Outdated` plus `preparePartForRemoval` when `creation_csn = RolledBackCSN` or `removal_csn /= 0`, else `Active` |
| `RestartLoadMutation(m)` | `StorageMergeTree::loadMutations`: write `csn` if the log has it, delete the file otherwise |
| `RestartDone` | `server_completely_started := TRUE` |

`updateCSNIfNeeded` on a part with `creation_tid = t` and no CSN asks `tryGetCSN`, which returns `RolledBackCSN`
when the log has no entry and no transaction with that tid is running. After a restart nothing is running, so any
part whose creating transaction has no log entry is rolled back, and any part with a removal tid but no log entry
gets its `removal_tid` cleared. Parts whose directory did not survive the crash are absent.

## Failure model {#failures}

Every failure class is a constant in the scenario configuration and a bounded counter in the state, so that a
scenario can enable one class at a time and TLC explores at most `N` occurrences per trace.

### Keeper {#failures-keeper}

`CommitCreateCSN` has three outcomes:

- `Ok`: the znode is appended to `zk_log`, the response arrives, `allocated_csn` is known.
- `FailBefore`: nothing is appended, a hardware error is raised.
- `LostAfter`: the znode is appended, then a hardware error is raised.

Both error outcomes take the same `catch` branch: the transaction goes to `unknown_state_list` and the client
gets `UnknownStatus` (or waits, with `WAIT_UNKNOWN`). The updating thread resolves it. The same three outcomes apply
to the `set` of `tail_ptr` and each `tryRemove` in `removeOldEntries`, where they abort the iteration.
`SessionExpire` may happen at any time; the next `UpdReconnect` establishes a new session and runs `sync`. The
model treats Keeper as one server, so `sync` is a no-op; the `LostAfter` outcome is what the two-list scheme in
`tryFinalizeUnknownStateTransactions` defends against, and the scenario `Keeper` exists to test that defence.

### Crash and restart {#failures-crash}

`Crash` is enabled in every state while the restart counter is below `RESTARTS_MAX`. It clears every in-memory
variable of the server (transactions, log, parts, actors, clients) and every `cached` disk layer. `durable` layers
and Keeper survive. The client outcome recorded before the crash is kept, because the durability properties are
about what a client was told.

### Disk write faults {#failures-disk}

`StoreInfo` may throw instead of writing. Where the call site is `noexcept` (`afterCommit`, `rollback`,
`finalizeCommittedTransaction`), the C++ runtime terminates the process. The model records this as the state
`aborted = TRUE` and the property `Aborted` says it is unreachable; a counterexample is a trace to a server
abort, which is a finding in its own right. Where the call site can throw (`removeOldPart` inside a query,
`setAndStoreCreationTID` at part creation), the fault becomes a `Fail` of that query.

### Query faults {#failures-query}

`Fail(k)`, `MergeFail` and `MutFail` are enabled between any two steps of the respective operation while the
query-fault counter is below `QUERY_FAULTS_MAX`.

## Invariants and properties {#invariants}

Properties are stated over the client history and the durable layer where possible, and only over internal
variables where the code's own assertions are the subject.

### Durability and atomicity of the acknowledgement {#invariants-durability}

- `AckedIsDurable`: if `client[k].outcome = Acked` for transaction `t`, then `zk_log` contains an entry for `t`,
  and in every later state, including after any number of crashes and restarts, every part in `creating(t)` is
  `Active`, or `Outdated` with a committed removal, or `Deleted` after a committed removal; and no part in
  `removing(t)` is ever `Active` again.
- `ErrorIsAbsent`: if `client[k].outcome = Error`, then `zk_log` contains no entry for `t`, and no part in
  `creating(t)` is `Active` once `RollbackFinalize` has run or a restart has completed.
- `UnknownResolvesByLog`: a transaction finalized from `unknown_state_list` ends `Committed` if and only if
  `zk_log` contains its entry.
- `Atomicity`: for any transaction `u` with `snapshot >= csn(t)` and any `Select` of `u` executed after
  `CommitFlip(t)`, either all parts of `creating(t)` (or their covering parts) are in the observed set, or none
  is. This is the "no partially visible commit" property.

### Snapshot isolation {#invariants-isolation}

- `StableRead`: any two entries of `client[k].observed` within one transaction expand to the same set of base
  parts through the covering relation.
- `NoUncommittedRead`: no observed part has `creation_tid` in state `Running`, `Committing` or `RolledBack`
  unless it is the reader's own transaction.
- `NoDoubleRead`: an observed set never contains both a part and a part that covers it.
- `NoLostRead`: a part whose creation is committed with `csn <= snapshot` and whose removal is not committed with
  `csn <= snapshot` is in the observed set, itself or through a covering part.

### Safety of part removal and log truncation {#invariants-cleanup}

- `NoPrematureDelete`: a part goes to `Deleted` only if it is not visible to any running transaction and not
  visible to any transaction that could still begin with the current `latest_snapshot`.
- `NoResurrection`: after `RestartDone`, no part whose durable metadata has a committed removal is `Active`, and no
  part whose creating transaction has no log entry is `Active`.
- `LogEntryNeeded`: an entry `csn -> t` is removed from `zk_log` only if no part's durable metadata and no
  mutation's durable record mentions `t` without the corresponding CSN. This is the property the comment in
  `removeOldEntries` doubts ("we write CSNs into data parts without fsync").
- `NoOutdatedLookup`: `assertTIDIsNotOutdated` never throws, that is, no lookup happens for a tid with
  `start_csn < tail_ptr` that is absent from `tid_to_csn`.

### Write-write conflicts {#invariants-conflicts}

- `SingleRemover`: at most one transaction ever commits a removal of a given part, and `lock` is either `0` or
  equal to `mem.removal_tid`.
- `NoLostData`: the set of base parts reachable from `Active` parts through the covering relation never loses a
  base part whose removal is not committed.
- `MutationOnVisible`: `MutSelect(m, p)` only picks parts visible to the mutation's transaction.

### Assertions from the code {#invariants-code}

Every `chassert` and `LOGICAL_ERROR` on the modelled paths, as a state predicate:

- `validateInfo` in full, for every part's in-memory and durable metadata.
- The four assertions at the top of `VersionInfo::isVisible`.
- `snapshots_in_use` is sorted and has the same size as `running_list` (`getOldestSnapshot`).
- `creation_csn <= removal_csn`, `creation_tid.start_csn <= creation_csn`, `removal_tid.start_csn <= removal_csn`.
- `preparePartForRemoval`: an `Outdated` part with a transactional creation has a `removal_tid`.
- `Aborted` (no server termination), `NoStaleVersion` (`STALE_VERSION` unreachable), `NoUnknownMutationCSN`
  (the `LOGICAL_ERROR` in `selectPartsToMutate`).

### Liveness {#invariants-liveness}

Only in the `Live` scenario, with weak fairness on the updating thread, the cleanup thread and the client's
`CommitAck`:

- Every transaction in `Committing` or in `unknown_state_list` is eventually `Committed` or `RolledBack`.
- Every `Outdated` part with a committed removal is eventually `Deleted`, provided no transaction runs forever.

## Vacuity checks {#vacuity}

Each invariant ships with a witness: a scenario configuration in which the invariant is deliberately weakened or a
guard in the model is removed, and TLC must then report a violation. The witnesses are part of the deliverable:

- `NoUncommittedRead` with the `creation_csn` check in `isVisible` removed.
- `UnknownResolvesByLog` with the two-list swap in `UpdSwapUnknownLists` collapsed to one list.
- `SingleRemover` with the CAS in `DropLockOne` replaced by an unconditional write.
- `NoPrematureDelete` with `getOldestSnapshot` replaced by `latest_snapshot`.
- `LogEntryNeeded` with the `server_completely_started` gate removed.
- `AckedIsDurable` with `CommitAck` moved before `CommitCreateCSN`.

An invariant without a witness that TLC can violate is not accepted into `Invariants.tla`.

## Scenario matrix and bounds {#scenarios}

Constants, all set per scenario: `Sessions` (2, symmetric), `TXN_MAX` (3 or 4, tids symmetric), `Parts` (the six
above), `Mutations` (at most one), `CSN_MAX` as a state constraint, `RESTARTS_MAX`, `KEEPER_FAULTS_MAX`,
`DISK_FAULTS_MAX`, `QUERY_FAULTS_MAX` (each 0 or 1), `FSYNC_PART_DIRECTORY`, `WAIT_MODE`.

| Scenario | Enabled operations | Faults | Checks |
|---|---|---|---|
| `Base` | `Begin`, `Insert*`, `Select`, `Drop*`, `Commit*`, `Rollback*` | none | isolation, conflicts |
| `Merge` | `Base` + `Merge*` + `Cleanup*` | none | `NoDoubleRead`, `NoPrematureDelete`, cleanup |
| `Mutation` | `Base` + `Mutate*`, `Mut*`, `KillMutation` | none | `MutationOnVisible`, rollback of mutations |
| `NonTxn` | `Base` + `Nt*` | none | durability and cleanup under mixed load |
| `Keeper` | `Base` + `Merge*` | Keeper, both wait modes | `UnknownResolvesByLog`, the two-list race |
| `Crash` | `Base` + `Merge*` + `Cleanup*` + `UpdRemoveOldEntries*` | restart, both `FSYNC_PART_DIRECTORY` values | `NoResurrection`, `LogEntryNeeded` |
| `DiskFault` | `Base` + `Merge*` | disk write | `Aborted`, `NoStaleVersion` |
| `QueryFault` | `Base` + `Mutate*` | query | rollback between steps |
| `All` | everything | every class at most once | regression, `TXN_MAX = 3` |
| `Live` | `Base` | Keeper | liveness under weak fairness |

Expected wall-clock on the development machine: minutes for the narrow scenarios, tens of minutes for `Keeper`
and `Crash`, hours for `All`. The README records the measured time, state count and distinct-state count of
every run; the numbers in this paragraph are estimates and are replaced by measurements after the first run. If
`All` does not finish overnight, it is rerun with `Sessions = 1` plus the background actors.

## Files and tooling {#files}

Everything lives in `utils/tla/transactions/` on the branch `tla/mergetree-transactions`, worktree `txn-tla`,
based on upstream master:

- The modules from the structure table.
- `run_tlc.sh <Scenario> [workers]`: downloads `tla2tools.jar` into `tmp/` if missing, runs `MC_<Scenario>` with
  `-workers auto` unless overridden, writes `tmp/tla/<Scenario>/tlc.log` and the counterexample trace if any, and
  exits non-zero on a violation.
- `README.md`: goal, scope and the "not covered" list, the code map (action, file, function, one line each), the
  run table (scenario, date, commit, states, distinct states, time, result), and the counterexample log (scenario,
  trace file, C++ call sequence, verdict: model defect or code defect, follow-up).

No CI job in this version. A later change can add the `Base` scenario as a fast check.

## Keeping the model faithful {#fidelity}

Three checks, all mandatory before a counterexample is reported as a code defect:

1. Defect injection: the vacuity witnesses above, plus one per counterexample found (remove the guard the trace
   exploits and confirm TLC still finds it; restore it and confirm TLC finds the original trace again).
2. Known traces: the sequences exercised by the integration test `test_transactions` and by the stateless tests
   that use `transaction_force_unknown_state_after_commit` and `transaction_after_commit_pause` are written as
   TLA+ trace expressions and must be accepted by the model.
3. Code-map review: a reviewer with the C++ tree open checks every row of the code map against the action's
   definition, in particular the step boundaries and every precondition that is not a `chassert` in the code.

A counterexample is mapped to C++ by replacing each action with the code-map row; if a step has no
counterpart, the model is wrong and is fixed first. A confirmed code defect is reproduced, where the fail points
allow it, as an integration test.

## Development order {#development-order}

1. `Keeper.tla`, `Disk.tla` with their own small `MC` configurations and unit invariants.
2. `TxnLog.tla`, `Parts.tla`, `Client.tla`, `Server.tla` with `Begin`, `Insert*`, `Select`, `Commit*`,
   `Rollback*`; the `Base` scenario green; the isolation and conflict witnesses.
3. `Drop*`, then `Merge*` and `Cleanup*`; the `Merge` scenario.
4. `Nt*`; the `NonTxn` scenario.
5. Keeper faults and the updating thread; the `Keeper` scenario, both wait modes.
6. `Crash`, `Restart*`, `UpdRemoveOldEntries*`; the `Crash` scenario, both `FSYNC_PART_DIRECTORY` values.
7. `Mutate*`, `Mut*`, `KillMutation`; the `Mutation` and `QueryFault` scenarios.
8. Disk write faults; the `DiskFault` scenario.
9. `All` and `Live`.
10. Code-map review, known-trace check, README with measured numbers.

Each step ends with a TLC run whose result is recorded before the next step starts. A counterexample is not
"fixed" in the model until the fidelity checks above have shown that the model, not the code, is wrong.

## Future extensions {#future}

- `ReplicatedMergeTree`: several servers, a replicated log in Keeper, `tid` in log entries, fetches, and
  `snapshot` defined as the largest CSN whose entries are all executed locally. Reuses `Keeper.tla`, `Disk.tla`
  and `Invariants.tla`.
- The uncovered operations listed in the scope section, each as an extension of `Server.tla` and one more
  scenario row.
- Disk and memory corruption as extra outcomes of `StoreInfo` and of `setInfo`.
- Weak-memory reads of `creation_csn` and `removal_csn` as an explicit stale-read action, if a future
  implementation reintroduces relaxed loads on the visibility path.
