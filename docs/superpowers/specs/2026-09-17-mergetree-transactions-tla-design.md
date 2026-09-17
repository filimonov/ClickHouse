---
description: 'Design of a TLA+ model of MergeTree transactions as implemented in upstream ClickHouse master: which entities, transitions, failures and invariants the model covers, how it is kept faithful to the C++ code, how TLC runs are bounded so that they finish, and what is deliberately left out of the first version. The model exists to find bugs in the implementation, not to document it.'
sidebar_label: 'MergeTree transactions, TLA+ model'
sidebar_position: 20
slug: /superpowers/specs/mergetree-transactions-tla-design
title: 'A TLA+ model of MergeTree transactions for finding bugs with TLC'
doc_type: 'design'
---

# A TLA+ model of MergeTree transactions for finding bugs with TLC {#mergetree-transactions-tla-design}

Revision 2, 2026-09-17. Baseline: upstream `ClickHouse/ClickHouse` master at commit `2c24b6b9291e`. The transaction
sources listed in the code map below are identical between `d1ba1699a271` (2026-09-16) and this commit; every
function name in this document refers to that tree.

Revision 2 folds the first review round (20 findings, 17 major). The main changes: `SET SNAPSHOT` is modelled;
"committed" is defined by the Keeper record, not by the transaction object; a ghost history survives crashes and
log truncation so that durability properties remain checkable; `Select`, metadata stores, mutation registration
and kill, and the cleanup thread are split into the steps the code actually has; the merge blocker, part
reservations and the parts lock are modelled so that impossible schedules are not explored; read-only commits,
read-your-writes, non-transactional lock phases and the exact `validateInfo` predicate are stated as the code has
them; the unbounded observation history is replaced by a bounded monitor; a payload version per base fragment
makes mutations checkable; a server abort on a `noexcept` write fault is a transition into the crash path, not an
expected-green invariant; every property gets a witness.

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
- Client operations inside an explicit transaction: `BEGIN`, `SET TRANSACTION SNAPSHOT`, `INSERT`, `SELECT`,
  `ALTER TABLE DROP PARTITION`, `ALTER TABLE DETACH PARTITION` (identical to `DROP` for the model, the clone into
  `detached/` is not modelled), `ALTER TABLE UPDATE`/`DELETE` (a mutation), `COMMIT`, `ROLLBACK`, and the
  automatic rollback on any exception.
- Implicit transactions (`implicit_transaction = 1`) as a wrapper scenario that fixes the order begin, query,
  commit or rollback, acknowledgement, reusing the same storage actions.
- The same operations without a transaction (`NonTransactionalTID`), interleaved with transactional ones.
- Background merges, which run as their own transaction, and the mutation executor.
- The transaction-log updating thread, including log truncation, and the outdated-parts cleanup thread.
- The merge blocker taken by `DROP PARTITION`, the set of parts reserved by running merges and mutations, and the
  parts lock, so that the model does not explore schedules the code excludes.
- Failures: Keeper response lost or session expired at commit, server crash at any point with loss of every
  write that was not fsynced, a disk write that throws (including inside `noexcept` paths, where the process
  terminates and restarts), and an exception thrown by a query between any two steps.
- Server restart with metadata repair from disk and Keeper, including parts that finish loading after the
  server is marked started.

Not covered in the first version, to be added later as separate work items:

- Every other partition operation: `ATTACH`, `MOVE`, `REPLACE`, `FETCH`, `FREEZE`, `UNFREEZE`, `DROP PART`,
  `DROP DETACHED`. The `detached/` directory itself is not modelled.
- `KILL TRANSACTION` and `KILL MUTATION` as client commands. The internal `killMutation` call made by a rollback
  is modelled, in steps.
- Backups and restores.
- `SYSTEM` commands: `STOP MERGES`, `START MERGES`, `SYNC TRANSACTION LOG`, `RESTART REPLICA`, `DROP ... CACHE`.
- `OPTIMIZE`, `TRUNCATE`, `ALTER ... MODIFY` and other metadata alters, projections, lightweight deletes and
  patch parts, `ReplacingMergeTree` and the other special engines.
- A transaction that touches two tables. `afterCommit` and `rollback` iterate over a set of storages and a
  failure between the per-storage mutation-CSN writes is a real window; it is deferred to a narrow two-table
  scenario in version 1.1, after the single-table scenarios have measured run times.
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
| `Disk.tla` | Per-part `txn_version.txt` and per-mutation `mutation_N.txt` as a durable copy plus a page-cache copy, `tmp` files, fsync, crash; a `Durable` mode that collapses the two layers for scenarios without crashes | `VersionMetadataOnDisk::storeInfoToDataPartStorage`, `MergeTreeMutationEntry::writeCSN` |
| `TxnLog.tla` | `TransactionLog` in-memory state and the updating thread | `src/Interpreters/TransactionLog.cpp` |
| `Parts.tla` | Part states, in-memory `VersionInfo`, the removal lock, deferred persistence, pins, the three-step metadata store, visibility, removability | `VersionMetadata.cpp`, `VersionMetadataOnDisk.cpp`, `VersionInfo.cpp`, `MergeTreeData` working set |
| `Locks.tla` | The merge blocker, the reservation set of merging and mutating parts, the parts lock | `ActionBlocker`, `currently_merging_mutating_parts`, `lockParts` |
| `Server.tla` | Transactions as step machines: begin, set snapshot, insert, select, drop, mutate, commit, rollback, merge, mutation executor, cleanup, restart | `MergeTreeTransaction.cpp`, `StorageMergeTree.cpp`, `MergeTreeData.cpp`, `MergePlainMergeTreeTask.cpp`, `MutatePlainMergeTreeTask.cpp` |
| `Client.tla` | Sessions, the outcome the client observed for each `COMMIT`, the bounded read monitor | `InterpreterTransactionControlQuery.cpp`, `executeQuery.cpp` |
| `History.tla` | Ghost variables that survive crash and log truncation: outcomes with their tid, the set of tids ever committed in Keeper, per-transaction effect sets, unknown-state decisions, committed removers per part | |
| `Invariants.tla` | Every property from the section on invariants, each with its witness | |
| `MC_<Scenario>.tla`, `MC_<Scenario>.cfg` | One per row of the scenario matrix: constants, part universe, enabled actions, guards, symmetry | |

## Entities and state variables {#entities}

Every variable is listed with the field or function it is taken from. Fixed finite sets per scenario: `Sessions`,
`Tids` (transaction identifiers, drawn in order), `Parts` (the part universe of the scenario), `Mutations`.

### Transactions {#entities-transactions}

`txn[t]` for `t \in Tids`, from `MergeTreeTransaction`:

- `state \in {Absent, Running, Committing, Committed, RolledBack}`, together with `csn` when `Committed`. In the
  code this is the single atomic `csn` holding `UnknownCSN`, `CommittingCSN`, a real CSN or `RolledBackCSN`. The
  transaction object's state is not what "committed" means for the properties; see the section on commit stages.
- `snapshot`: the CSN read from `latest_snapshot` at `beginTransaction`, changed by `SET TRANSACTION SNAPSHOT`.
- `protected_snapshot`: the value inserted into `snapshots_in_use` at `beginTransaction`. It is a separate field
  because `setSnapshot` changes `snapshot` and leaves the `snapshots_in_use` entry alone.
- `creating`, `removing`: sequences of parts, the fields `creating_parts` and `removing_parts`.
- `mutations`: the set of mutations attached by `addMutation`.
- `owner \in {Session(k), Merge, MutationExec}`: who holds the `MergeTreeTransactionHolder`.
- `pc` plus a work list: the position inside a multi-step operation (`afterCommit`, `rollback`, a batch of
  `removeOldPart` calls, a `Select`). Steps are separated exactly where the code has no lock held across them.

### Commit stages {#entities-commit-stages}

Three facts about a transaction are distinct in the code and are distinct in the model:

- `CommittedInLog(t)`: `zk_log` contains an entry for `t`. This is the commit point (`/// Commit point` in
  `commitTransaction`) and the definition of "committed" used by every property.
- `CsnLoaded(t)`: `tid_to_csn` contains `t`. From this moment `isVisible` on other transactions' snapshots can
  return true for parts created by `t`, while `txn[t].state` may still be `Committing`.
- `Finalized(t)`: `csn.exchange(assigned_csn)` has run (`CommitFlip`), after every per-part CSN store.

A transaction that is `CommittedInLog` but not `Finalized` is the window that the properties on atomicity and
uncommitted reads must cover; a property that starts checking at `Finalized` would miss it.

### Transaction log {#entities-txnlog}

From `TransactionLog`, all in memory and lost on crash:

- `tid_to_csn`: the loaded part of the Keeper log.
- `latest_snapshot`, `local_tid_counter`, `last_loaded_entry`.
- `running_list`: the set of transactions in `Running` or `Committing` state.
- `snapshots_in_use`: a bag of `protected_snapshot` values, from which `getOldestSnapshot` reads the minimum.
- `tail_ptr`: the in-memory copy; `updated_tail_ptr`: the flag that gates the first advance.
- `unknown_state_list`, `unknown_state_list_loaded`: the two lists of `tryFinalizeUnknownStateTransactions`.
- `server_completely_started` and `async_loading_jobs`: the two gates of `removeOldEntries`.

### Keeper {#entities-keeper}

- `zk_log`: a sequence of records `[csn, tid]`, one per `csn-NNN` znode, `tid` empty for the placeholder created at
  startup by `loadLogFromZooKeeper`.
- `zk_seq`: the sequential-name counter.
- `zk_tail_ptr`: the `tail_ptr` znode.
- `session \in {Alive, Expired}`.

Keeper is linearizable and durable. Its only nondeterminism is the outcome of a request as seen by the server.

### Parts {#entities-parts}

`part[p]` for `p \in Parts`, from `IMergeTreeDataPart` and `VersionMetadataOnDisk`:

- `pstate \in {Absent, Temporary, PreActive, Active, Outdated, Deleting, Deleted}`: `DataPartState` plus
  physical removal. `Deleting` is the state `grabOldParts` sets before the filesystem removal; a failed removal
  returns the part to `Outdated` (`rollbackDeletingParts`).
- `mem`: the in-memory `VersionInfo`: `creation_tid`, `creation_csn`, `removal_tid`, `removal_csn`,
  `storing_version`.
- `lock`: `removal_tid_lock_hash`, a tid or `0`.
- `deferrable`, `deferred`: `is_persist_deferrable` and `deferred_persist_info`.
- `pins`: the set of abstract owners holding a `shared_ptr` to the part: `Txn(t)` for a part in `creating` or
  `removing` of a live transaction, `Rollback(t)` for a rollback work list, `Merge`, `MutationExec`,
  `Select(k)` for a captured read set. `isSharedPtrUnique` is `pins = {}`.
- `store_pc`, `store_retries`: the position of an in-flight metadata store (see the disk actions).

Base fragments and payloads. Each base part carries a fragment (its rows) with a version counter `ver`, starting
at 0. A mutation result carries the same fragment with `ver + 1`; a deleting mutation carries a tombstone. A merge
result carries the union of its sources' fragments with their versions. The "rows a `SELECT` sees" is the set of
(fragment, version) pairs obtained by expanding the covering relation, which makes a mutation that publishes the
wrong content, or a merge that resurrects an old version, observable.

The part universe is a constant of each scenario. The largest one:

| Part | Role | Covers |
|---|---|---|
| `P1`, `P2` | base parts of one partition, produced by `INSERT` | |
| `M12` | result of a merge of `P1` and `P2` | `P1`, `P2` |
| `P1m`, `P2m` | results of a mutation of `P1` and `P2` | `P1` resp. `P2` |
| `E` | the empty part a non-transactional `DROP PARTITION` creates to cover the partition | every other part |

Scenarios that do not enable merges omit `M12`; scenarios that do not enable mutations omit `P1m`, `P2m`;
scenarios that do not enable non-transactional drops omit `E`.

### Disk {#entities-disk}

`disk[p]` for parts and `mdisk[m]` for mutations. In `Layered` mode (crash and disk-fault scenarios):

- `durable`: the `VersionInfo` (resp. mutation record) that survives a crash, or `None`.
- `cached`: what a reader sees before a crash, or `None`. Writes go here; `Fsync` copies it to `durable`; `Crash`
  discards it.
- `tmp`: whether `txn_version.txt.tmp` exists, with the same two layers.
- `dir`: whether the part directory itself exists, with the same two layers, so that a crash can lose a
  `Temporary` or `PreActive` part.

In `Durable` mode (every other scenario) `cached` and `durable` are one field and `Fsync` is a no-op. The constant
`FSYNC_PART_DIRECTORY` decides whether the rename in `storeInfoToDataPartStorage` is durable without a directory
fsync. With it off, a crash after the rename may leave the durable layer in the state before the rename: `tmp`
present and the old file present, which `loadMetadata` case 2 treats as a rolled-back part.

### Mutations {#entities-mutations}

`mut[m]`: `mstate \in {Absent, Prepared, Registered, Selected, Applied, Done, Killed}`, `tid`, `csn` (in memory),
and the on-disk record in `mdisk[m]` with `tmp_file`, `file`, `tid`, `csn`. `writeCSN` appends without fsync.
`Prepared` is after `prepareMutationEntry` (final file name, `addMutation` done) and before the insertion into
`current_mutations_by_version`; `Registered` is after that insertion.

### Locks {#entities-locks}

- `merges_blocker`: a counter; `stopMergesAndWait` increments it and waits until `reserved = {}`; new merge and
  mutation selections are disabled while it is non-zero.
- `reserved`: the set of parts held in `currently_merging_mutating_parts` by the running merge or mutation task.
- `parts_lock`: the owner of `lockParts`, or none. `DropStart` and `DropOutdate` are one critical section under it;
  `MutFinish` holds it across rename and commit; `SelectCapture` takes and releases it inside the step.

### Client {#entities-client}

`client[k]` for `k \in Sessions`:

- `current`: the transaction bound to the session, or none.
- `outcome`: the result the client received for the last `COMMIT`: `None`, `Acked`, `Error`, `UnknownStatus`,
  with the tid it refers to (also recorded in the history module).
- `read`: the bounded read monitor: for the current transaction, the (fragment, version) set of the first
  completed `SELECT`, of the last completed `SELECT`, and the in-flight capture. Nothing else about reads is
  kept, so repeated `SELECT` does not grow the state.
- `wait_mode`: the setting `wait_changes_become_visible_after_commit_mode`, a constant per scenario.

### History {#entities-history}

Ghost variables that no `Crash`, `Restart` or log truncation clears. They exist only to state properties and are
never read by an action:

- `h_outcome[t]`: the client outcome delivered for `t`, if any.
- `h_committed`: the set of tids that were ever `CommittedInLog`, kept after the entry is truncated.
- `h_effects[t]`: `creating`, `removing` and `mutations` of `t` as of `CommitCreateCSN`, so that durability can be
  checked after the transaction object is gone.
- `h_unknown[t]`: for a transaction resolved from `unknown_state_list`, whether the decision was `Committed` or
  `RolledBack`, and whether `t \in h_committed` at that moment.
- `h_removers[p]`: the set of tids (including `NonTransactionalTID`) that committed a removal of `p`.
- `h_content[t]`: the (fragment, version) set visible at `txn[t].snapshot` when the transaction began, for the
  no-lost-data property.

### Background actors {#entities-background}

The updating thread, the cleanup thread, the merge task and the mutation executor are actors with a `pc` each
and a work list. There is one of each.

## Actions {#actions}

Each action is one function or one step of a function. The table gives the C++ location; the text explains what
the action does in the model. Preconditions that the code enforces with a `chassert` become invariants, not
preconditions, so that a violated assertion shows up as a counterexample. Every metadata write below goes through
the three-step store described under the disk actions.

### Client and session {#actions-client}

| Action | C++ |
|---|---|
| `Begin(k)` | `TransactionLog::beginTransaction` |
| `SetSnapshot(k, c)` | `executeSetSnapshot`, `MergeTreeTransaction::setSnapshot` |
| `InsertWrite(k, p)` | `MergedBlockOutputStream` constructor, `setAndStoreCreationTID` |
| `InsertPreActive(k, p)` | `MergeTreeData::renameTempPartAndReplace` |
| `InsertCommit(k, p)` | `MergeTreeData::Transaction::commit`, `addNewPartAndRemoveCovered` |
| `SelectCapture(k)` | `getVisibleDataPartsVector`: `getDataPartsVectorForInternalUsage` under `lockParts`, pins taken |
| `SelectCheck(k, p)` | `filterVisibleDataParts`, one `VersionMetadata::isVisible` call, no lock |
| `SelectFinish(k)` | the read set is recorded in the monitor, pins released |
| `DropStart(k)` | `StorageMergeTree::dropPartition`: `stopMergesAndWait`, then `lockParts`, then visible parts of the partition |
| `DropLockOne(k, p)` | `MergeTreeTransaction::removeOldPart`, `lockRemovalTID` then `setAndStoreRemovalTID` |
| `DropOutdate(k)` | the state loop at the end of `removePartsFromWorkingSet`, still under `lockParts`; blocker released |
| `MutPrepare(k, m)` | `prepareMutationEntry`: temporary file, rename to `mutation_N.txt`, `addMutation` |
| `MutRegister(k, m)` | `startMutation` under `currently_processing_in_background_mutex`: insert, then re-check `ROLLED_BACK` and unregister if so |
| `CommitBefore(k)` | `MergeTreeTransaction::beforeCommit` |
| `CommitCreateCSN(k)` | `TransactionLog::commitTransaction`, the `multi` with one sequential `create` |
| `CommitStoreCreation(k, p)`, `CommitStoreRemoval(k, p)`, `CommitStoreMutation(k, m)` | `afterCommit`, one `setAndStore...CSN` or `setMutationCSN` each |
| `CommitFlip(k)` | `afterCommit`, `csn.exchange(assigned_csn)` |
| `CommitFinalize(k)` | `finalizeCommittedTransaction`, erase from `running_list` and `snapshots_in_use` |
| `CommitAck(k)` | `executeCommit` returns; `waitForCSNLoaded` for the default wait mode |
| `CommitUnknown(k)` | the `catch` in `commitTransaction`: append to `unknown_state_list`, throw `UNKNOWN_STATUS_OF_TRANSACTION` or return `CommittingCSN` |
| `RollbackStart(k)` | `TransactionLog::rollbackTransaction`, `MergeTreeTransaction::rollback`, the CAS to `RolledBackCSN` |
| `RollbackKill*(k, m)` | `killMutation`, in the steps of the mutation section |
| `RollbackMarkCreated(k, p)` | `setAndStoreCreationCSN(RolledBackCSN)` |
| `RollbackOutdateCreated(k, p)` | `removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part})` |
| `RollbackRestore(k, p)` | `restoreAndActivatePart` |
| `RollbackUnlock(k, p)` | `setAndStoreRemovalTID(EmptyTID)` then `unlockRemovalTID` |
| `RollbackFinalize(k)` | erase from `running_list` and `snapshots_in_use`, `afterFinalize` |
| `Fail(k)` | any exception between two steps of a query; leads to `RollbackStart` through `txn->onException()` |

`Begin` takes `snapshot := latest_snapshot`, `protected_snapshot := snapshot`, allocates the next tid with
`start_csn = snapshot`, inserts `protected_snapshot` into `snapshots_in_use` and the transaction into
`running_list`, all in one step because the code holds `running_list_mutex` throughout. It is disabled once
`local_tid_counter` reaches the scenario's bound, so the state space is finite by construction rather than cut by
a constraint.

`SetSnapshot(k, c)` sets `txn.snapshot := c` for any `c` that is a CSN present in `zk_log` or `latest_snapshot`,
above `MaxReservedCSN`; the code accepts any such number. `protected_snapshot` and `snapshots_in_use` are
unchanged, which is exactly what the model has to exercise against `Cleanup*` and `UpdRemoveOldEntries*`.

`InsertWrite` creates the part in state `Temporary` with `creation_tid = t` and stores `txn_version.txt`.
`InsertCommit` computes the covered parts from the covering relation (`getActivePartsToReplace` and
`getCoveredOutdatedParts` filtered by visibility), records the part in `creating` (pin `Txn(t)`), and for every
covered part runs the same lock-and-store steps as `DropLockOne` before flipping states. A client `INSERT` never
covers anything in the chosen universes; the step exists because merges and mutations reuse it.

`SelectCapture` takes `lockParts`, copies the set of `Active` and `Outdated` parts, pins them with `Select(k)`,
releases the lock. `SelectCheck(k, p)` evaluates `isVisible(snapshot, tid)` for one captured part against the
current `mem` and `tid_to_csn`; between two checks any other action may run, which is the interleaving the code
allows. `SelectFinish` expands the visible parts to (fragment, version) pairs, updates the monitor, drops the pins.

`DropStart` increments `merges_blocker`, waits until `reserved = {}` (a precondition), takes `parts_lock`, and
selects the visible parts of the partition. `DropLockOne` runs per part: CAS on `lock` from `0` to `t` (a failure
throws `SERIALIZATION_ERROR`, which is a `Fail` that also releases the lock and the blocker), then
`setAndStoreRemovalTID(t)`. `DropOutdate` flips every part of the batch to `Outdated`, releases `parts_lock` and
the blocker.

`MutPrepare` writes the temporary file, renames it, and attaches the mutation to the transaction. `MutRegister`
inserts it into the map and, under the same mutex, re-checks the transaction: if it is already `RolledBack`, the
entry is unregistered again, its file removed, and the query fails with `INVALID_TRANSACTION`. `RollbackStart`
may run between the two steps; that is the window the re-check exists for.

`CommitBefore` requires every attached mutation to be `Done` or `Killed` (`waitForMutation`), then CAS
`Unknown -> Committing`. `CommitCreateCSN` is the commit point; its three outcomes are in the failure section; on
`Ok` it also records `h_committed` and `h_effects`. The `afterCommit` steps run in the order of the code:
creations, removals, mutations, then `CommitFlip`. `CommitFinalize` removes the transaction from the running list
and releases the snapshot, and `CommitAck` delivers `Acked` to the client. A read-only transaction (`creating`,
`removing` and `mutations` all empty at `CommitBefore`) skips `CommitCreateCSN` and takes `csn := snapshot`, as
`commitTransaction` does. With `wait_mode = WAIT_UNKNOWN`, `CommitUnknown` returns `CommittingCSN` and the client
blocks in `waitStateChange` until the updating thread finalizes the transaction; with any other mode the client
receives `UnknownStatus` and the transaction is detached from the session.

`RollbackStart` is the CAS `Unknown -> RolledBack`; if the transaction is already `RolledBack` (concurrent
`killMutation`) or already `Committed`, rollback does nothing. The subsequent steps run in the code's order:
mutations killed, created parts marked `RolledBackCSN` on disk, created parts outdated, removed parts restored to
`Active` (unless created by the same transaction), removed parts cleared on disk and unlocked. The work lists pin
their parts with `Rollback(t)` until `RollbackFinalize`.

`Fail(k)` is enabled between any two steps of `Insert`, `Drop`, `Mut`, `Select` and before `CommitBefore`. It
models `SERIALIZATION_ERROR`, `STALE_VERSION` from the store, a disk write fault outside `noexcept`, and any
unrelated exception. It is the only way a query ends without completing; it releases any lock or blocker the
query holds.

### Implicit transactions {#actions-implicit}

The `Implicit` scenario replaces the free client with a wrapper: `Begin`, exactly one query (`Insert*`, `Select*`,
`Drop*` or `Mut*`), then `Commit*` if the query completed or `Rollback*` if it failed, then the acknowledgement
to the client. The order matches `executeQuery`: the implicit begin before the interpreter, the commit inside
the query-finish callback before the response is sent, the rollback in the exception callbacks.

### Updating thread {#actions-updating}

| Action | C++ |
|---|---|
| `UpdReconnect` | `runUpdatingThread`, `expired()` branch, `sync` |
| `UpdLoadNewEntries` | `loadNewEntries`, `loadEntries`; sets `CsnLoaded` for the loaded tids |
| `UpdRemoveOldEntriesSetTail` | `removeOldEntries` up to `tail_ptr.store` |
| `UpdRemoveOldEntriesDelete(csn)` | `removeOldEntries`, one `tryRemove` and one `tid_to_csn.erase` |
| `UpdSwapUnknownLists` | `tryFinalizeUnknownStateTransactions`, the two swaps |
| `UpdFinalizeUnknown(t)` | `tryFinalizeUnknownStateTransactions`, one transaction: `getCSN` then `finalizeCommittedTransaction` or `assertTIDIsNotOutdated` plus `rollbackTransaction`; records `h_unknown` |

One iteration of the thread is the sequence reconnect, load, remove old, swap, finalize each. Any Keeper request
inside it may fail with a hardware error, which ends the iteration and starts a new one; `unknown_state_list`
entries survive the failed iteration. `UpdRemoveOldEntriesSetTail` is enabled only when
`server_completely_started` holds and, for the first advance after start (`updated_tail_ptr = FALSE`), when
`async_loading_jobs = 0`; it sets `tail_ptr := getOldestSnapshot()`, which is `latest_snapshot` when no
transaction runs. `UpdRemoveOldEntriesDelete` removes entries with `tid.start_csn < tail_ptr`, keeps the entry
with `csn = latest_snapshot`, and is one action per entry because the code issues one request per entry.

### Cleanup thread {#actions-cleanup}

| Action | C++ |
|---|---|
| `CleanupGrab(p)` | `grabOldParts`: `canBeRemoved`, `isSharedPtrUnique`, state `Deleting` |
| `CleanupDeleteOk(p)` | `clearPartsFromFilesystemAndRollbackIfError` success: directory removed in both layers, `removePartsFinally`, `Deleted` |
| `CleanupDeleteFail(p)` | the same function's error path: `rollbackDeletingParts`, back to `Outdated` |

`CleanupGrab` is enabled for an `Outdated` part when `canBeRemoved` holds with `getOldestSnapshot` (over
`protected_snapshot` values) and `pins = {}`.

### Merge {#actions-merge}

| Action | C++ |
|---|---|
| `MergeBegin` | `scheduleDataProcessingJob`, `beginTransaction` with `autocommit = false` |
| `MergeSelect` | `selectPartsToMerge` with `txn`: `merges_blocker = 0`, sources from `Active` and `Outdated` visible to the merge transaction and not in `reserved`; sources reserved, pinned with `Merge` |
| `MergeWrite` | `MergeTask`, `setAndStoreCreationTID` on `M12` |
| `MergeFinish` | `MergePlainMergeTreeTask::finish`, `renameMergedTemporaryPart`, `Transaction::commit` with covered parts locked through `removeOldPart`; reservation released |
| `MergeCommit*` | the same `Commit*` steps with `throw_on_unknown_status = false` |
| `MergeFail` | exception anywhere before `MergeCommitBefore`; reservation released; the holder rolls the transaction back |

A merge transaction is an ordinary transaction with `owner = Merge`; it never issues `Select` and its `Commit` has
no client. `MergeSelect` requires both sources to be visible to the merge's own snapshot, which excludes parts with
an uncommitted creation or removal, and requires no other merge in flight.

### Mutation executor {#actions-mutation}

| Action | C++ |
|---|---|
| `MutSelect(m, p)` | `selectPartsToMutate`: `merges_blocker = 0`, `p` not in `reserved`, `m` `Registered`; for a transactional mutation the transaction is looked up (`tryGetTransactionForMutation`) or, if gone, `mut.csn` decides: `RolledBackCSN` skips, `UnknownCSN` is a `LOGICAL_ERROR`; the selected part is recorded in `h_selected[m]`, reserved, pinned |
| `MutWrite(m, p)` | `MutateTask`, `setAndStoreCreationTID` on `Pm` with the mutation's tid |
| `MutFinish(m, p)` | `MutatePlainMergeTreeTask::executeStep`, `renameTempPartAndReplaceUnlocked` plus `Transaction::commit` under `lockParts`, the source part locked and stored through `removeOldPart`; reservation released |
| `MutDone(m)` | `updateMutationEntriesErrors`, `is_done` |
| `MutFail(m)` | exception in the executor: `txn->onException()`, reservation released |
| `KillUnregister(m)` | `killMutation` under the background mutex: erase from the map |
| `KillRollbackTxn(m)` | `killMutation`: `rollbackTransaction` if the transaction is still running |
| `KillCancelTask(m)` | `cancelPartMutations`: an in-flight `MutWrite` becomes `MutFail` |
| `KillRemoveFile(m)` | `removeFile`, `Killed` |

The visibility test in `MutSelect` (`isVisible(first_mutation_tid.start_csn, tid)` for a transactional mutation,
`isVisible(MaxCommittedCSN, EmptyTID)` for a non-transactional one) is part of the action, and the property
`MutationOnVisible` is stated over `h_selected`, not over the guard, so that removing the guard falsifies it.
The mutation result belongs to the transaction that started the mutation (`creating` gets `Pm`, `removing` gets
`P`), so the client's `Commit` and `Rollback` cover it. The three-entry deadlock check in
`getIncompleteMutationsStatusUnlocked` is modelled in the `MutationChain` scenario only: `waitForMutation`
returns with a failure when a transactional mutation depends on an earlier one of the same transaction with a
non-transactional mutation in between.

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
`creation_csn = 0, removal_csn = NonTransactionalCSN`, which `validateInfo` rejects. During `NtDropLock` the lock
is held with `mem.removal_tid` still empty, and during `NtDropStore` the tid is written before the unlock; the
lock invariant is phase-aware for that reason.

### Disk and the metadata store {#actions-disk}

`updateInfoWithRefreshDataThenStoreAndSetMetadata` is three steps, because `persisted_info_mutex` covers only the
middle one:

| Action | C++ |
|---|---|
| `StoreRead(p)` | `getInfo` on the first attempt, `loadMetadata` on a retry; the update function applied; `updateCSNIfNeeded`; `validateInfo` |
| `StorePersist(p)` | `storeInfo` under `persisted_info_mutex`: compare `storing_version` with `cached` (or `deferred`), on mismatch `TOO_OLD_VERSION` and back to `StoreRead` with `store_retries + 1`; else write `tmp`, fsync `tmp`, rename; or the deferred branch |
| `StorePublish(p)` | `setInfo` under `version_info_mutex`, ignored if the stored version is lower than the current in-memory one |
| `Fsync(p)` | copies `cached` to `durable` for one file; enabled at any time |
| `Crash` | discards every `cached` layer; see the failure section |

Between any two of the three steps another updater of the same part may run; that is the stale-version race.
After `MAX_RETRIES` mismatches the store throws `STALE_VERSION`, which is a `Fail` outside `noexcept` and a
process termination inside it. The write of `tmp` plus fsync makes `tmp` durable; the rename makes the new content
`cached`, and `durable` only when `FSYNC_PART_DIRECTORY` is on or a later `Fsync` runs.

### Restart {#actions-restart}

`Crash` (or `ProcessDown`, see the failure section) followed by `Restart`, the latter as a sequence of steps:

| Step | C++ |
|---|---|
| `RestartLoadLog` | `TransactionLog::loadLogFromZooKeeper`: creates one placeholder `csn-` znode, loads `tid_to_csn`, `latest_snapshot`, `tail_ptr` |
| `RestartLoadPart(p)` | `MergeTreeData::loadDataPart`: `VersionMetadataOnDisk::loadMetadata` (the four cases), `updateCSNIfNeeded`, `validateInfo`, store if updated, then `Outdated` plus `preparePartForRemoval` when `creation_csn = RolledBackCSN` or `removal_csn /= 0`, else `Active`; decrements `async_loading_jobs` when it is the last part of the table |
| `RestartLoadMutation(m)` | `StorageMergeTree::loadMutations`: write `csn` if the log has it, delete the file otherwise |
| `RestartDone` | `server_completely_started := TRUE`; enabled before every `RestartLoadPart` has run, because tables load asynchronously |

`updateCSNIfNeeded` on a part with `creation_tid = t` and no CSN asks `tryGetCSN`, which returns `RolledBackCSN`
when the log has no entry and no transaction with that tid is running. After a restart nothing is running, so any
part whose creating transaction has no log entry is rolled back, and any part with a removal tid but no log entry
gets its `removal_tid` cleared. Parts whose directory did not survive the crash are absent. The updating thread
starts at `RestartLoadLog`, so `UpdRemoveOldEntries*` can interleave with `RestartLoadPart`, which is the race
behind the `async_loading_jobs` gate.

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
variable of the server (transactions, log, parts, locks, actors, clients) and every `cached` disk layer. `durable`
layers, Keeper and the history module survive.

### Disk write faults {#failures-disk}

`StorePersist` may throw instead of writing. Where the call site is `noexcept` (`afterCommit`, `rollback`,
`finalizeCommittedTransaction`), the C++ runtime terminates the process. The model has the transition
`ProcessDown`, which is `Crash` reached only from such a fault, and continues with the normal `Restart`. The
conformance property `DownOnlyByNoexceptFault` states that `ProcessDown` is reached only this way; the safety
properties then check that the restart recovers a consistent state. In scenarios without disk faults, `NoProcessDown`
is an ordinary invariant. Where the call site can throw (`removeOldPart` inside a query, `setAndStoreCreationTID`
at part creation), the fault becomes a `Fail` of that query.

### Query faults {#failures-query}

`Fail(k)`, `MergeFail` and `MutFail` are enabled between any two steps of the respective operation while the
query-fault counter is below `QUERY_FAULTS_MAX`.

## Invariants and properties {#invariants}

Properties are stated over the history module, the client monitor and the durable layer where possible, and only
over internal variables where the code's own assertions are the subject. "Committed" always means
`t \in h_committed`, that is, `CommittedInLog` at some point. Every property is listed with its witness: the change
to the model under which TLC must report a violation. A property without a witness is not accepted into
`Invariants.tla`.

### Durability and atomicity of the acknowledgement {#invariants-durability}

| Property | Statement | Witness |
|---|---|---|
| `AckedWriteIsDurable` | `h_outcome[t] = Acked` and `h_effects[t]` non-empty implies `t \in h_committed`, and in every later state each part of `h_effects[t].creating` is `Active`, or `Outdated`/`Deleting`/`Deleted` with a committed removal, and no part of `h_effects[t].removing` is `Active` again | `CommitAck` moved before `CommitCreateCSN` |
| `AckedReadOnly` | `h_outcome[t] = Acked` with empty effects implies `t \notin h_committed` and `txn[t].csn = snapshot` | read-only branch removed from `CommitBefore` |
| `ErrorIsAbsent` | `h_outcome[t] = Error` implies `t \notin h_committed`, and no part of `h_effects[t].creating` is `Active` once `RollbackFinalize(t)` has run or a restart has completed | `RollbackOutdateCreated` skipped |
| `UnknownResolvesByLog` | `h_unknown[t] = Committed` iff `t \in h_committed` at the decision | the two-list swap collapsed to one list |
| `Atomicity` | for any transaction `u` with `snapshot >= csn(t)`, every completed `SelectFinish` of `u` after `CsnLoaded(t)` contains either all fragments of `h_effects[t].creating` (through covering) or none | `CommitStoreCreation` for one part skipped |
| `RollbackRestores` | after `RollbackFinalize(t)`, every part of `h_effects[t].removing` not created by `t` is visible again to a transaction begun afterwards, and no part of `h_effects[t].creating` is ever visible to any transaction other than `t` | `RollbackRestore` skipped |

### Snapshot isolation {#invariants-isolation}

| Property | Statement | Witness |
|---|---|---|
| `StableRead` | the first and last read of a transaction differ only by fragments that `t` itself created or removed (`h_effects` of `t` so far) | `snapshot` replaced by `latest_snapshot` in `SelectCheck` |
| `ReadYourWrites` | after `InsertCommit(t, p)` every later read of `t` contains `p`'s fragments; after `DropOutdate(t)` no later read of `t` contains the dropped fragments | the `creation_tid = current_tid` clause removed from `isVisible` |
| `NoUncommittedRead` | no fragment in a read of `t` comes from a part whose `creation_tid` is not in `h_committed` and not `t` and not `NonTransactionalTID` | the `creation_csn` lookup in `SelectCheck` returns `snapshot` |
| `NoDoubleRead` | a read never contains two versions of the same fragment | `NoDoubleRead` witness: `SelectCheck` ignores `removal_csn` |
| `NoLostRead` | a fragment whose creating part is committed with `csn <= snapshot` and whose removal is not committed with `csn <= snapshot` is in the read | `SelectCapture` skips `Outdated` parts |
| `NoLostVisibleData` | for a running `t`, the (fragment, version) set visible at `txn[t].snapshot`, recomputed after every action of another actor, never loses an element of `h_content[t]` | `CleanupGrab` ignores `getOldestSnapshot` |

### Safety of part removal and log truncation {#invariants-cleanup}

| Property | Statement | Witness |
|---|---|---|
| `NoPrematureDelete` | a part enters `Deleting` only if it is not visible at `txn[u].snapshot` for any running `u` (the actual snapshot, not the protected one) | `SetSnapshot` enabled with cleanup; this is the property expected to fail on the code |
| `NoResurrection` | after `RestartDone` and after every `RestartLoadPart`, no part whose durable metadata has a committed removal is `Active`, and no part whose creating tid is absent from `h_committed` is `Active` | `updateCSNIfNeeded` returns `UnknownCSN` instead of `RolledBackCSN` |
| `LogEntryNeeded` | an entry `csn -> t` leaves `zk_log` only if no durable part metadata and no durable mutation record mentions `t` without the corresponding CSN | the `async_loading_jobs` gate removed |
| `NoOutdatedLookup` | `assertTIDIsNotOutdated` never throws | `tail_ptr` set to `latest_snapshot` instead of the oldest snapshot |
| `DeletionRollbackSafe` | a part returned to `Outdated` by `CleanupDeleteFail` is later deleted only through `CleanupGrab` again | `CleanupDeleteFail` sets `Deleted` |

### Write-write conflicts {#invariants-conflicts}

| Property | Statement | Witness |
|---|---|---|
| `SingleRemover` | `Cardinality(h_removers[p]) <= 1` | the CAS in `DropLockOne` replaced by an unconditional write |
| `LockConsistent` | `lock = 0`, or `lock = t` transactional with `mem.removal_tid \in {Empty, t}`, or `lock = NonTransactionalTID` with `mem.removal_tid \in {Empty, NonTransactionalTID}` | `NtDropStore` unlocks before the store |
| `MutationOnVisible` | every `(m, p)` in `h_selected` was visible to the mutation's transaction at selection | the visibility test removed from `MutSelect` |
| `MutationContent` | after a committed mutation `m` of `p`, every read with `snapshot >= csn` sees `p`'s fragment at `ver + 1` (or the tombstone), never at `ver` | `MutFinish` publishes `ver` instead of `ver + 1` |
| `NoOrphanMutation` | a `Registered` mutation whose transaction is `RolledBack` or absent and whose `csn` is unknown does not exist after `MutRegister` completes | the re-check in `MutRegister` removed |

### Assertions from the code {#invariants-code}

Every `chassert` and `LOGICAL_ERROR` on the modelled paths, transcribed as the code has them, each with the
witness "the corresponding guard in the model is removed":

- `validateInfo`, exactly: with `creation_csn = 0`, `removal_csn = 0` and `removal_tid \in {Empty, creation_tid}`;
  with `creation_csn /= 0`, `removal_csn = 0 \/ removal_csn = NonTransactionalCSN \/ creation_csn <= removal_csn`,
  and for a transactional creation `creation_tid.start_csn <= creation_csn`; with `removal_csn /= 0`,
  `removal_tid /= Empty` and `removal_tid.start_csn <= removal_csn`; the `DummyTID`/`RolledBackCSN` shape exempt.
- The four assertions at the top of `VersionInfo::isVisible`.
- `snapshots_in_use` is sorted and has the same size as `running_list` (`getOldestSnapshot`).
- `preparePartForRemoval`: an `Outdated` part with a transactional creation has a `removal_tid`.
- `NoStaleVersion` (`STALE_VERSION` unreachable), `NoUnknownMutationCSN` (the `LOGICAL_ERROR` in
  `selectPartsToMutate`), `NoProcessDown` in scenarios without disk faults, `DownOnlyByNoexceptFault` in the
  scenario with them.

### Liveness {#invariants-liveness}

Only in the `Live` scenario, with weak fairness on the updating thread, the cleanup thread and the client's
`CommitAck`, and stated only for transactions begun while `Begin` was enabled:

- Every transaction in `Committing` or in `unknown_state_list` is eventually `Committed` or `RolledBack`.
- Every `Outdated` part with a committed removal is eventually `Deleted`, provided no transaction runs forever.
- In `MutationChain`: a transactional mutation that waits on a non-transactional one that waits on the same
  transaction is eventually reported as failed by `waitForMutation`, never waited on forever.

## Scenario matrix and bounds {#scenarios}

Constants, all set per scenario: `Sessions` (1 or 2, symmetric), `TID_MAX` (the bound on `Begin`), `Parts` (the
universe of the scenario), `Mutations`, `CSN_MAX` (a guard on `CommitCreateCSN`, not a state constraint),
`RESTARTS_MAX`, `KEEPER_FAULTS_MAX`, `DISK_FAULTS_MAX`, `QUERY_FAULTS_MAX` (each 0 or 1), `DISK_MODE`
(`Durable` or `Layered`), `FSYNC_PART_DIRECTORY`, `WAIT_MODE`. State is finite because every unbounded counter has
a guard and every history variable is bounded by `TID_MAX` and `Parts`.

| Scenario | Universe | Enabled operations | Faults | Checks |
|---|---|---|---|---|
| `Base` | `P1`, `P2` | `Begin`, `Insert*`, `Select*`, `Drop*`, `Commit*`, `Rollback*` | none | isolation, conflicts, read-your-writes |
| `SetSnapshot` | `P1`, `P2` | `Base` + `SetSnapshot` + `Cleanup*` + `UpdRemoveOldEntries*` | none | `NoPrematureDelete`, `NoOutdatedLookup` |
| `Merge` | + `M12` | `Base` + `Merge*` + `Cleanup*` | none | `NoDoubleRead`, `NoPrematureDelete`, `Atomicity` |
| `Mutation` | + `P1m`, `P2m` | `Base` + `Mut*`, `Kill*` | none | `MutationOnVisible`, `MutationContent`, `NoOrphanMutation` |
| `MutationChain` | `P1`, `P1m` | one session, three mutation entries txn, non-txn, txn | none | the deadlock report, liveness |
| `NonTxn` | + `E` | `Base` + `Nt*` | none | `LockConsistent`, `SingleRemover`, durability under mixed load |
| `Implicit` | `P1`, `P2` | the implicit wrapper | query | acknowledgement ordering |
| `Keeper` | + `M12` | `Base` + `Merge*` | Keeper, both wait modes | `UnknownResolvesByLog`, the two-list race |
| `Crash` | + `M12` | `Base` + `Merge*` + `Cleanup*` + `UpdRemoveOldEntries*`, `Layered` disk | restart, both `FSYNC_PART_DIRECTORY` values | `NoResurrection`, `LogEntryNeeded` |
| `DiskFault` | + `M12` | `Base` + `Merge*`, `Layered` disk | disk write | `DownOnlyByNoexceptFault`, recovery after `ProcessDown`, `NoStaleVersion` |
| `QueryFault` | + `P1m` | `Base` + `Mut*` | query | rollback between steps |
| `Live` | `P1`, `P2` | `Base` | Keeper | liveness under weak fairness |

An `All` scenario is not planned until the narrow ones have measured run times; the README records for every run
the date, commit, states, distinct states, wall-clock time and result, and the matrix is adjusted from those
numbers. `TID_MAX` starts at 3 and `Sessions` at 2; a scenario that does not finish in one hour is rerun with
`Sessions = 1` before its bounds are reconsidered.

## Files and tooling {#files}

Everything lives in `utils/tla/transactions/` on the branch `tla/mergetree-transactions`, worktree `txn-tla`,
based on upstream master:

- The modules from the structure table.
- `run_tlc.sh <Scenario> [workers]`: downloads `tla2tools.jar` into `tmp/` if missing, runs `MC_<Scenario>` with
  `-workers auto` unless overridden, writes `tmp/tla/<Scenario>/tlc.log` and the counterexample trace if any, and
  exits non-zero on a violation.
- `witness.sh <Scenario> <Property>`: applies the witness of a property (a named override in the `MC` module),
  runs TLC, and exits zero only if TLC reports a violation of exactly that property.
- `README.md`: goal, scope and the "not covered" list, the code map (action, file, function, one line each), the
  run table (scenario, date, commit, states, distinct states, time, result), the witness table (property, witness,
  last verified), and the counterexample log (scenario, trace file, C++ call sequence, verdict: model defect or
  code defect, follow-up).

No CI job in this version. A later change can add the `Base` scenario as a fast check.

## Keeping the model faithful {#fidelity}

Three checks, all mandatory before a counterexample is reported as a code defect:

1. Defect injection: every witness in the invariant tables, run by `witness.sh`, plus one per counterexample found
   (remove the guard the trace exploits and confirm TLC still finds it; restore it and confirm TLC finds the
   original trace again).
2. Known traces: the sequences exercised by the integration test `test_transactions` and by the stateless tests
   that use `transaction_force_unknown_state_after_commit`, `transaction_after_commit_pause` and
   `mt_pause_before_register_mutation` are written as TLA+ trace expressions and must be accepted by the model.
3. Code-map review: a reviewer with the C++ tree open checks every row of the code map against the action's
   definition, in particular the step boundaries and every precondition that is not a `chassert` in the code.

A counterexample is mapped to C++ by replacing each action with the code-map row; if a step has no
counterpart, the model is wrong and is fixed first. A confirmed code defect is reproduced, where the fail points
allow it, as an integration test.

## Development order {#development-order}

1. `Keeper.tla`, `Disk.tla` with their own small `MC` configurations and unit invariants.
2. `TxnLog.tla`, `Parts.tla` (including the three-step store), `Locks.tla`, `History.tla`, `Client.tla`,
   `Server.tla` with `Begin`, `Insert*`, `Select*`, `Commit*`, `Rollback*`; the `Base` scenario green; every
   isolation and conflict witness red.
3. `SetSnapshot`, `Cleanup*`, `UpdRemoveOldEntries*`; the `SetSnapshot` scenario.
4. `Drop*`, then `Merge*`; the `Merge` scenario.
5. `Nt*`; the `NonTxn` scenario.
6. Keeper faults and the rest of the updating thread; the `Keeper` scenario, both wait modes.
7. `Crash`, `Restart*` with asynchronous part loading; the `Crash` scenario, both `FSYNC_PART_DIRECTORY` values.
8. `Mut*`, `Kill*`; the `Mutation`, `MutationChain` and `QueryFault` scenarios.
9. Disk write faults and `ProcessDown`; the `DiskFault` scenario.
10. `Implicit` and `Live`.
11. Code-map review, known-trace check, README with measured numbers.

Each step ends with a TLC run of the scenario and of every witness it enables, with the results recorded before
the next step starts. A counterexample is not "fixed" in the model until the fidelity checks above have shown that
the model, not the code, is wrong.

## Future extensions {#future}

- `ReplicatedMergeTree`: several servers, a replicated log in Keeper, `tid` in log entries, fetches, and
  `snapshot` defined as the largest CSN whose entries are all executed locally. Reuses `Keeper.tla`, `Disk.tla`,
  `History.tla` and `Invariants.tla`.
- A two-table scenario with one part per table, no merge and no crash, for the per-storage loops in
  `afterCommit` and `rollback`.
- The uncovered operations listed in the scope section, each as an extension of `Server.tla` and one more
  scenario row.
- Disk and memory corruption as extra outcomes of `StorePersist` and of `StorePublish`.
- Weak-memory reads of `creation_csn` and `removal_csn` as an explicit stale-read action, if a future
  implementation reintroduces relaxed loads on the visibility path.
