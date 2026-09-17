---
description: 'Design of a TLA+ model of MergeTree transactions as implemented in upstream ClickHouse master: which entities, transitions, failures and invariants the model covers, how it is kept faithful to the C++ code, how TLC runs are bounded so that they finish, and what is deliberately left out of the first version. The model exists to find bugs in the implementation, not to document it.'
sidebar_label: 'MergeTree transactions, TLA+ model'
sidebar_position: 20
slug: /superpowers/specs/mergetree-transactions-tla-design
title: 'A TLA+ model of MergeTree transactions for finding bugs with TLC'
doc_type: 'design'
---

# A TLA+ model of MergeTree transactions for finding bugs with TLC {#mergetree-transactions-tla-design}

Revision 4, 2026-09-17. Baseline: upstream `ClickHouse/ClickHouse` master at commit `2c24b6b9291e`. The transaction
sources listed in the code map below are identical between `d1ba1699a271` (2026-09-16) and this commit; every
function name in this document refers to that tree.

Revision 4 folds the third review round (5 findings, 10 witnesses that could not reach their target):
`NoOrphanMutation`, `MutationOnVisible`, `ReadYourWrites` and `NoLostVisibleData` no longer reject legal traces
(rollback before `killMutation`, skipped invisible candidates, insert-then-drop in one transaction, content
recaptured at `SET TRANSACTION SNAPSHOT`); a `MutationCrash` scenario covers the unsynced mutation-CSN append and
the written-but-unattached window against restart and log truncation; every invalid witness was replaced or its
property removed (`RegrabBeforeDelete`, the `preparePartForRemoval` row); the witness contract allows two named
changes. Revision 2 folded the first review round (20 findings). Revision 3 folded the second round (8 incorrect
folds, 9 new findings): the history module now records effects continuously, the assigned CSN, the snapshot and the rollback
completion, so that every durability and rollback property is stated over ghost state that survives crash and log
truncation; `SET TRANSACTION SNAPSHOT` accepts the values the code accepts; the mutation entry is prepared in two
steps with the orphan-file window; metadata stores have per-caller frames; `KILL TRANSACTION` is a client action
because the mutation-registration race needs it; non-transactional readers are an actor; `STALE_VERSION` is an
allowed failure, not an invariant; every scenario lists its updater actions; the witness contract is "run with
only the target property checked" and every witness was re-derived under it; `validateInfo` and `isVisible`
assertions are transcribed completely.

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
- `KILL TRANSACTION` issued from another session. It is one call of `rollbackTransaction` and it is the only way
  to reach the rollback-during-registration window of `startMutation` that the code defends against, so it is in
  scope although the general `KILL` family is not.
- Implicit transactions (`implicit_transaction = 1`) as a wrapper scenario that fixes the order begin, query,
  commit or rollback, acknowledgement, reusing the same storage actions.
- The same operations without a transaction (`NonTransactionalTID`), interleaved with transactional ones,
  including non-transactional `SELECT`, which reads `Active` parts only.
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
- `KILL MUTATION` as a client command and `KILL QUERY`. The internal `killMutation` call made by a rollback is
  modelled, in steps.
- Backups and restores.
- `SYSTEM` commands: `STOP MERGES`, `START MERGES`, `SYNC TRANSACTION LOG`, `RESTART REPLICA`, `DROP ... CACHE`.
- `OPTIMIZE`, `TRUNCATE`, `ALTER ... MODIFY` and other metadata alters, projections, lightweight deletes and
  patch parts, `ReplacingMergeTree` and the other special engines.
- A transaction that touches two tables. `afterCommit` and `rollback` iterate over a set of storages and a
  failure between the per-storage mutation-CSN writes is a real window. It stays out of version 1 by the
  single-table scope decision; a narrow two-table scenario (one part per table, no merge, no crash) is the first
  item of version 1.1.
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
| `Parts.tla` | Part states, in-memory `VersionInfo`, the removal lock, deferred persistence, pins, per-caller store frames, visibility, removability | `VersionMetadata.cpp`, `VersionMetadataOnDisk.cpp`, `VersionInfo.cpp`, `MergeTreeData` working set |
| `Locks.tla` | The merge blocker, the reservation set of merging and mutating parts, the parts lock | `ActionBlocker`, `currently_merging_mutating_parts`, `lockParts` |
| `Server.tla` | Transactions as step machines: begin, set snapshot, insert, select, drop, mutate, commit, rollback, kill, merge, mutation executor, cleanup, restart | `MergeTreeTransaction.cpp`, `StorageMergeTree.cpp`, `MergeTreeData.cpp`, `MergePlainMergeTreeTask.cpp`, `MutatePlainMergeTreeTask.cpp` |
| `Client.tla` | Sessions, the outcome the client observed for each `COMMIT`, the bounded read monitor | `InterpreterTransactionControlQuery.cpp`, `executeQuery.cpp` |
| `History.tla` | Ghost variables that survive crash and log truncation; the section on history lists them | |
| `Invariants.tla` | Every property from the section on invariants, each with its witness | |
| `MC_<Scenario>.tla`, `MC_<Scenario>.cfg` | One per row of the scenario matrix: constants, part universe, enabled actions, guards, symmetry, witness overrides | |

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
  `commitTransaction`) and the definition of "committed" used by every property. Both the `Ok` and the
  `LostAfter` outcome of `CommitCreateCSN` establish it and record `t` in `h_committed` and `h_csn[t]`.
- `CsnLoaded(t)`: `tid_to_csn` contains `t`. From this moment `isVisible` on other transactions' snapshots can
  return true for parts created by `t`, while `txn[t].state` may still be `Committing`. Recorded in
  `h_loaded[t]`.
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
- `frames`: the set of in-flight metadata stores on this part, one record per caller: `owner`, `tentative`
  (the `VersionInfo` the caller intends to store), `pc \in {Read, Persist, Publish}`, `retries`. Two callers can
  be suspended around the same part at once; that is the stale-version race.

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

`mut[m]`: `mstate \in {Absent, Written, Attached, Registered, Selected, Applied, Done, Killed}`, `tid`, `csn` (in
memory), and the on-disk record in `mdisk[m]` with `tmp_file`, `file`, `tid`, `csn`. `writeCSN` appends without
fsync. `Written` is after `entry.commit` (the final file exists) and before `addMutation`; `Attached` is after
`addMutation` and before the insertion into `current_mutations_by_version`; `Registered` is after that insertion.

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
never read by an action. All are bounded by `Tids`, `Parts` and `Mutations`:

- `h_outcome[t]`: the client outcome delivered for `t`, if any.
- `h_committed`: the set of tids that were ever `CommittedInLog`, kept after the entry is truncated.
- `h_csn[t]`: the CSN assigned at `CommitCreateCSN` (`Ok` or `LostAfter`), or the snapshot for a read-only commit.
- `h_snapshot[t]`: `txn[t].snapshot` as of `CommitBefore` or `RollbackStart`.
- `h_loaded[t]`: whether `CsnLoaded(t)` has ever held.
- `h_creating[t]`, `h_removing[t]`, `h_mutations[t]`: the effect sets of `t`, updated at `InsertCommit`,
  `DropLockOne`, `MergeFinish`, `MutFinish` and `MutPrepareAttach`, so that they exist for transactions that fail
  or roll back before commit. `h_effects[t]` denotes the three together.
- `h_rolled_back[t]`: set at `RollbackFinalize(t)`.
- `h_unknown[t]`: for a transaction resolved from `unknown_state_list`, whether the decision was `Committed` or
  `RolledBack`, and whether `t \in h_committed` at that moment.
- `h_removers[p]`: the set of tids (including `NonTransactionalTID`) that committed a removal of `p`.
- `h_selected`: the set of `(m, p, visible)` triples recorded by `MutSelect` for the parts it actually selected,
  where `visible` is the result of the visibility test at selection. Candidates that the test rejects are not
  recorded, because the code skips them without selecting.
- `h_content[t]`: the (fragment, version) set visible at `txn[t].snapshot`, captured at `Begin` and recaptured
  at every `SetSnapshot(t)`, so that a loss caused by choosing an older snapshot is not attributed to the next
  action of another actor.

### Background actors {#entities-background}

The updating thread, the cleanup thread, the merge task, the mutation executor and the non-transactional reader are
actors with a `pc` each and a work list. There is one of each.

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
| `MutPrepareWrite(k, m)` | `prepareMutationEntry`: temporary file, sync, `entry.commit` (rename to `mutation_N.txt`) |
| `MutPrepareAttach(k, m)` | `prepareMutationEntry`: `txn->addMutation` |
| `MutRegister(k, m)` | `startMutation` under `currently_processing_in_background_mutex`: insert, then re-check `ROLLED_BACK` and unregister if so |
| `CommitBefore(k)` | `MergeTreeTransaction::beforeCommit` |
| `CommitCreateCSN(k)` | `TransactionLog::commitTransaction`, the `multi` with one sequential `create` |
| `CommitReadOnly(k)` | `commitTransaction`, the `isReadOnly` branch: no Keeper request, `csn := snapshot` |
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
| `KillTransaction(k, t)` | `InterpreterKillQueryQuery` for `KILL TRANSACTION`: `rollbackTransaction(t)` from session `k`, which runs the `Rollback*` steps concurrently with whatever `t`'s own session is doing |
| `Fail(k)` | any exception between two steps of a query; leads to `RollbackStart` through `txn->onException()` |

`Begin` takes `snapshot := latest_snapshot`, `protected_snapshot := snapshot`, allocates the next tid with
`start_csn = snapshot`, inserts `protected_snapshot` into `snapshots_in_use` and the transaction into
`running_list`, all in one step because the code holds `running_list_mutex` throughout. It is disabled once
`local_tid_counter` reaches the scenario's bound, so the state space is finite by construction rather than cut by
a constraint.

`SetSnapshot(k, c)` sets `txn.snapshot := c` for any `c` in `(MaxReservedCSN, CSN_MAX]`, or `c = NonTransactionalCSN`,
or `c = EverythingVisibleCSN`, exactly the values `executeSetSnapshot` accepts. `c` may be a CSN that is not yet
allocated or one that log truncation has removed. `protected_snapshot` and `snapshots_in_use` are unchanged, which
is what the model has to exercise against `Cleanup*` and `UpdRemoveOldEntries*`. Isolation properties skip a
transaction whose snapshot is `EverythingVisibleCSN`, because that value is an introspection mode by design.

`InsertWrite` creates the part in state `Temporary` with `creation_tid = t` and stores `txn_version.txt`.
`InsertCommit` computes the covered parts from the covering relation (`getActivePartsToReplace` and
`getCoveredOutdatedParts` filtered by visibility), records the part in `creating` and in `h_creating[t]` (pin
`Txn(t)`), and for every covered part runs the same lock-and-store steps as `DropLockOne` before flipping states.
A client `INSERT` never covers anything in the chosen universes; the step exists because merges and mutations
reuse it.

`SelectCapture` takes `lockParts`, copies the set of `Active` and `Outdated` parts, pins them with `Select(k)`,
releases the lock. `SelectCheck(k, p)` evaluates `isVisible(snapshot, tid)` for one captured part against the
current `mem` and `tid_to_csn`; between two checks any other action may run, which is the interleaving the code
allows. `SelectFinish` expands the visible parts to (fragment, version) pairs, updates the monitor, drops the pins.

`DropStart` increments `merges_blocker`, waits until `reserved = {}` (a precondition), takes `parts_lock`, and
selects the visible parts of the partition. `DropLockOne` runs per part: CAS on `lock` from `0` to `t` (a failure
throws `SERIALIZATION_ERROR`, which is a `Fail` that also releases the lock and the blocker), then
`setAndStoreRemovalTID(t)`; the part enters `removing` and `h_removing[t]`. `DropOutdate` flips every part of the
batch to `Outdated`, releases `parts_lock` and the blocker.

`MutPrepareWrite` writes the temporary file and renames it to its final name. `MutPrepareAttach` attaches the
mutation to the transaction (`h_mutations[t]`). A `Fail` between the two leaves `mutation_N.txt` on disk with no
in-memory counterpart until the entry's destructor removes it (`is_registered = FALSE`), which the model performs
as part of that `Fail`; a `Crash` in the same window leaves the file for `RestartLoadMutation` to find.
`MutRegister` inserts the entry into the map and, under the same mutex, re-checks the transaction: if it is
already `RolledBack`, the entry is unregistered again, its file removed, and the query fails with
`INVALID_TRANSACTION`. `KillTransaction` or a `RollbackStart` from another mutation's `MutFail` may run between
`MutPrepareAttach` and `MutRegister`; that is the window the re-check exists for.

`CommitBefore` requires every attached mutation to be `Done` or `Killed` (`waitForMutation`), records
`h_snapshot[t]`, then CAS `Unknown -> Committing`. A transaction with empty effects takes `CommitReadOnly` instead
of `CommitCreateCSN`, as `commitTransaction` does through `isReadOnly`; it records `h_csn[t] := snapshot` and does
not enter `h_committed`. `CommitCreateCSN` is the commit point; its three outcomes are in the failure section; on
`Ok` and `LostAfter` it records `h_committed` and `h_csn`. The `afterCommit` steps run in the order of the code:
creations, removals, mutations, then `CommitFlip`. `CommitFinalize` removes the transaction from the running list
and releases the snapshot, and `CommitAck` delivers `Acked` to the client. With `wait_mode = WAIT_UNKNOWN`,
`CommitUnknown` returns `CommittingCSN` and the client blocks in `waitStateChange` until the updating thread
finalizes the transaction; with any other mode the client receives `UnknownStatus` and the transaction is detached
from the session.

`RollbackStart` is the CAS `Unknown -> RolledBack`; if the transaction is already `RolledBack` (concurrent
`killMutation` or `KillTransaction`) or already `Committed`, rollback does nothing. The subsequent steps run in the
code's order: mutations killed, created parts marked `RolledBackCSN` on disk, created parts outdated, removed
parts restored to `Active` (unless created by the same transaction), removed parts cleared on disk and unlocked.
The work lists pin their parts with `Rollback(t)` until `RollbackFinalize`, which sets `h_rolled_back[t]`.

`Fail(k)` is enabled between any two steps of `Insert`, `Drop`, `MutPrepare*`, `MutRegister`, `Select` and before
`CommitBefore`. It models `SERIALIZATION_ERROR`, `STALE_VERSION` from the store, a disk write fault outside
`noexcept`, and any unrelated exception. It is the only way a query ends without completing; it releases any lock
or blocker the query holds.

### Non-transactional reader {#actions-ntselect}

`NtSelectCapture`, `NtSelectCheck(p)`, `NtSelectFinish`: `getVisibleDataPartsVector` with `txn = nullptr`, which
takes `Active` parts only, then `isVisible` with `current_tid = NonTransactionalTID`, which returns
`removal_tid.isEmpty()`. The result goes to a one-slot monitor `nt_read`. This actor exists because the effect of
`restoreAndActivatePart` (and of forgetting it) is visible only to readers of the `Active` set.

### Implicit transactions {#actions-implicit}

The `Implicit` scenario replaces the free client with a wrapper: `Begin`, exactly one query (`Insert*`, `Select*`,
`Drop*` or `Mut*`), then `Commit*` if the query completed or `Rollback*` if it failed, then the acknowledgement
to the client. The order matches `executeQuery`: the implicit begin before the interpreter, the commit inside
the query-finish callback before the response is sent, the rollback in the exception callbacks.

### Updating thread {#actions-updating}

| Action | C++ |
|---|---|
| `UpdReconnect` | `runUpdatingThread`, `expired()` branch, `sync` |
| `UpdLoadNewEntries` | `loadNewEntries`, `loadEntries`; sets `CsnLoaded` and `h_loaded` for the loaded tids |
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
`assertTIDIsNotOutdated` is reached only from `UpdFinalizeUnknown` (`getCSNAndAssert` has no other caller in the
baseline), so its property lives in scenarios that enable both Keeper faults and log truncation.

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
| `MergeFinish` | `MergePlainMergeTreeTask::finish`, `renameMergedTemporaryPart`, `Transaction::commit` with covered parts locked through `removeOldPart`; reservation released; `h_creating`, `h_removing` updated |
| `MergeCommit*` | the same `Commit*` steps with `throw_on_unknown_status = false` |
| `MergeFail` | exception anywhere before `MergeCommitBefore`; reservation released; the holder rolls the transaction back |

A merge transaction is an ordinary transaction with `owner = Merge`; it never issues `Select` and its `Commit` has
no client. `MergeSelect` requires both sources to be visible to the merge's own snapshot, which excludes parts with
an uncommitted creation or removal, and requires no other merge in flight.

### Mutation executor {#actions-mutation}

| Action | C++ |
|---|---|
| `MutSelect(m, p)` | `selectPartsToMutate`: `merges_blocker = 0`, `p` not in `reserved`, `m` `Registered`; the visibility test (`isVisible(first_mutation_tid.start_csn, tid)` for a transactional mutation, `isVisible(MaxCommittedCSN, EmptyTID)` for a non-transactional one) is evaluated; a part that fails it is skipped and nothing is recorded; a part that passes it is selected and `(m, p, result)` is recorded in `h_selected`; for a transactional mutation the transaction is looked up (`tryGetTransactionForMutation`) or, if gone, `mut.csn` decides: `RolledBackCSN` skips, `UnknownCSN` is a `LOGICAL_ERROR`; the selected part is reserved and pinned |
| `MutWrite(m, p)` | `MutateTask`, `setAndStoreCreationTID` on `Pm` with the mutation's tid |
| `MutFinish(m, p)` | `MutatePlainMergeTreeTask::executeStep`, `renameTempPartAndReplaceUnlocked` plus `Transaction::commit` under `lockParts`, the source part locked and stored through `removeOldPart`; reservation released; `h_creating`, `h_removing` updated |
| `MutDone(m)` | `updateMutationEntriesErrors`, `is_done` |
| `MutFail(m)` | exception in the executor: `txn->onException()`, reservation released |
| `KillUnregister(m)` | `killMutation` under the background mutex: erase from the map |
| `KillRollbackTxn(m)` | `killMutation`: `rollbackTransaction` if the transaction is still running |
| `KillCancelTask(m)` | `cancelPartMutations`: an in-flight `MutWrite` becomes `MutFail` |
| `KillRemoveFile(m)` | `removeFile`, `Killed` |

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
| `NtDropStore(p)` | `NonTransactionalRemovalLocks::store`: `setAndStoreRemovalTID(NonTransactionalTID)` per part, `removal_csn = NonTransactionalCSN` immediately, then unlock; `h_removers[p]` updated |
| `NtDropCover` | `dropPartition` without a transaction: the empty part `E` created and committed over the partition |
| `NtMerge*` | as `Merge*` with `txn = nullptr` |
| `NtMutate*` | as `Mut*` with `NonTransactionalTID` |

Non-transactional removal is refused with `SERIALIZATION_ERROR` when a part's creation is not committed; the
model keeps this as a precondition of `NtDropLock` and additionally as an invariant on the shape
`creation_csn = 0, removal_csn = NonTransactionalCSN`, which `validateInfo` rejects. During `NtDropLock` the lock
is held with `mem.removal_tid` still empty, and during `NtDropStore` the tid is written before the unlock; the
lock invariant is phase-aware for that reason.

### Disk and the metadata store {#actions-disk}

`updateInfoWithRefreshDataThenStoreAndSetMetadata` is three steps per caller, because `persisted_info_mutex`
covers only the middle one and `version_info_mutex` only the last one:

| Action | C++ |
|---|---|
| `StoreRead(p, f)` | frame `f` enters: `getInfo` on the first attempt, `loadMetadata` on a retry; the update function applied to the frame's `tentative`; `updateCSNIfNeeded`; `validateInfo` |
| `StorePersist(p, f)` | `storeInfo` under `persisted_info_mutex`: compare `tentative.storing_version` with `cached` (or `deferred`); on mismatch `TOO_OLD_VERSION`, `retries + 1`, back to `StoreRead`; else write `tmp`, fsync `tmp`, rename; or the deferred branch |
| `StorePublish(p, f)` | `setInfo` under `version_info_mutex`, ignored if the stored version is lower than the current in-memory one; the frame leaves |
| `Fsync(p)` | copies `cached` to `durable` for one file; enabled at any time |
| `Crash` | discards every `cached` layer; see the failure section |

Between any two of the three steps of one frame, another frame on the same part may take any of its steps; that is
the stale-version race. After `MAX_STORE_RETRIES` mismatches the store fails with `STALE_VERSION`. The model's
bound is 2 (the C++ value is 20; the second collision already exhibits every distinct interleaving, and the bound
is documented as a refinement parameter in the README). `STALE_VERSION` is an allowed outcome: outside `noexcept`
it is a `Fail` of the query, inside `noexcept` it is `ProcessDown`. The write of `tmp` plus fsync makes `tmp`
durable; the rename makes the new content `cached`, and `durable` only when `FSYNC_PART_DIRECTORY` is on or a
later `Fsync` runs.

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
behind the `async_loading_jobs` gate. While the server is down (between `Crash` and `RestartLoadPart(p)`) the
part-state properties are evaluated on the durable layer, see the invariant section.

## Failure model {#failures}

Every failure class is a constant in the scenario configuration and a bounded counter in the state, so that a
scenario can enable one class at a time and TLC explores at most `N` occurrences per trace.

### Keeper {#failures-keeper}

`CommitCreateCSN` has three outcomes:

- `Ok`: the znode is appended to `zk_log`, the response arrives, `allocated_csn` is known.
- `FailBefore`: nothing is appended, a hardware error is raised.
- `LostAfter`: the znode is appended (`h_committed`, `h_csn` recorded), then a hardware error is raised.

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
`ProcessDown`, which is `Crash` reached only from such a fault or from `STALE_VERSION` in the same call sites, and
continues with the normal `Restart`. The conformance property `DownOnlyByNoexceptFault` states that `ProcessDown`
is reached only this way; the safety properties then check that the restart recovers a consistent state. In
scenarios without disk faults, `NoProcessDown` is an ordinary invariant. Where the call site can throw
(`removeOldPart` inside a query, `setAndStoreCreationTID` at part creation), the fault becomes a `Fail` of that
query.

### Query faults {#failures-query}

`Fail(k)`, `MergeFail` and `MutFail` are enabled between any two steps of the respective operation while the
query-fault counter is below `QUERY_FAULTS_MAX`.

## Invariants and properties {#invariants}

Properties are stated over the history module, the client monitors and the durable layer where possible, and only
over internal variables where the code's own assertions are the subject. "Committed" always means
`t \in h_committed`. A part-state clause is evaluated on `part[p].pstate` while the server is up and has loaded
`p`, and on the durable layer of `disk[p]` while it is down or has not yet loaded `p` (a durable committed removal
counts as `Outdated`, a durable rolled-back creation or a missing directory counts as `Absent`).

Witness contract: for each property `Q` the `MC` module has an override `Witness_Q` that changes one or two named
actions as stated in the table. `witness.sh Scenario Q` runs TLC with `Witness_Q` applied and only `Q` checked, in a scenario
whose enabled actions include those the witness needs, and must report a violation of `Q`. Other properties may
also fail under the witness; they are not checked in that run. A property without a passing witness is not
accepted into `Invariants.tla`.

### Durability and atomicity of the acknowledgement {#invariants-durability}

| Property | Statement | Witness (scenario) |
|---|---|---|
| `AckedWriteIsDurable` | `h_outcome[t] = Acked` and `h_effects[t]` non-empty implies `t \in h_committed`, and in every later state each part of `h_creating[t]` is `Active`, or `Outdated`/`Deleting`/`Deleted` with `h_removers[p] /= {}`, and no part of `h_removing[t]` is `Active` in any later state | `CommitAck` moved before `CommitCreateCSN` and `Fail` allowed after it (`Crash`) |
| `AckedReadOnly` | `h_outcome[t] = Acked` with empty `h_effects[t]` implies `t \notin h_committed` and `h_csn[t] = h_snapshot[t]` | `CommitReadOnly` replaced by `CommitCreateCSN` for empty-effect transactions (`Base`) |
| `ErrorIsAbsent` | `h_outcome[t] = Error` implies `t \notin h_committed`, and once `h_rolled_back[t]` or a restart has completed no part of `h_creating[t]` is `Active` | `RollbackOutdateCreated` skipped (`Base`; `Error` is delivered when `CommitBefore` finds the transaction cancelled by `KillTransaction`, or in `Keeper` after `FailBefore` with `WAIT_UNKNOWN`) |
| `UnknownResolvesByLog` | `h_unknown[t] = Committed` iff `t \in h_committed` at the decision | the two-list swap collapsed to one list (`Keeper`) |
| `Atomicity` | for any transaction `u` with `h_snapshot`-or-current snapshot `>= h_csn[t]`, every `SelectFinish` of `u` completed after `h_loaded[t]` contains either all fragments of `h_creating[t]` and none of `h_removing[t]`, or none of the former and all of the latter (through covering) | `SelectCheck` uses `mem` only and skips the `tid_to_csn` lookup (`Merge`, where `DROP` supplies multi-part removals) |
| `RollbackRestores` | once `h_rolled_back[t]`, every part of `h_removing[t]` not in `h_creating[t]` is `Active` and visible to `NtSelect`, and no fragment of `h_creating[t]` appears in any read of a transaction other than `t` | `RollbackRestore` skipped (`NonTxn`, which has `NtSelect`) |

### Snapshot isolation {#invariants-isolation}

Stated for transactions whose snapshot is not `EverythingVisibleCSN`.

| Property | Statement | Witness (scenario) |
|---|---|---|
| `StableRead` | the first and last read of `t` differ only by fragments of parts in `h_creating[t]` or `h_removing[t]` at the time of the last read | `SelectCheck` uses `latest_snapshot` instead of `txn.snapshot` (`Base`) |
| `ReadYourWrites` | after `InsertCommit(t, p)` every later read of `t` contains `p`'s fragments until a `DropOutdate(t)` covering `p`; after `DropOutdate(t)` no later read of `t` contains the dropped fragments (the removal clause precedes the own-creation clause in `isVisible`, so own removal wins) | the `creation_tid = current_tid` clause removed from `isVisible` (`Base`) |
| `NoUncommittedRead` | no fragment in a read of `t` comes from a part whose `creation_tid` is not in `h_committed`, not `t` and not `NonTransactionalTID` | `SelectCheck` treats `creation_csn = 0` as `creation_csn = snapshot` (`Base`) |
| `NoDoubleRead` | a read never contains two versions of the same fragment | `SelectCheck` ignores `removal_csn` and `removal_tid` (`Merge`) |
| `NoLostRead` | a fragment whose creating part is committed with `h_csn <= snapshot` and whose removal is not committed with `h_csn <= snapshot` is in the read | `SelectCapture` skips `Outdated` parts (`Base`, through a transactional `DROP` in flight) |
| `NoLostVisibleData` | for a running `t`, the (fragment, version) set visible at `txn[t].snapshot`, recomputed after every action of another actor, never loses an element of `h_content[t]` (which `SetSnapshot(t)` recaptures) | `CleanupGrab` uses `latest_snapshot` instead of `getOldestSnapshot` (`Merge`) |

### Safety of part removal and log truncation {#invariants-cleanup}

| Property | Statement | Witness (scenario) |
|---|---|---|
| `NoPrematureDelete` | a part enters `Deleting` only if it is not visible at `txn[u].snapshot` for any running `u` (the actual snapshot, not the protected one) | `CleanupGrab` uses `latest_snapshot` instead of `getOldestSnapshot` (`Merge`); the baseline `SetSnapshot` scenario is expected to violate this property on the code as it is, which is a finding, not a witness |
| `NoResurrection` | after every `RestartLoadPart(p)`, `p` is not `Active` if its durable metadata has a committed removal or a non-transactional removal, or if its `creation_tid` is transactional and not in `h_committed` | `updateCSNIfNeeded` returns `UnknownCSN` instead of `RolledBackCSN` for a tid absent from the log (`Crash`) |
| `LogEntryNeeded` | an entry `csn -> t` leaves `zk_log` only if no durable part metadata and no durable mutation record mentions `t` without the corresponding CSN | the `async_loading_jobs` gate removed (`Crash`) |
| `NoOutdatedLookup` | `assertTIDIsNotOutdated` never throws | `UpdRemoveOldEntriesSetTail` uses `latest_snapshot` instead of `getOldestSnapshot` (`Keeper`, which enables truncation and unknown-state finalization) |
| `MutationRecovered` | after `RestartLoadMutation(m)`, `m` is present iff its tid is non-transactional or in `h_committed`, and a present transactional `m` has `csn = h_csn[tid]`; this is expected to fail on the code as it is when the log entry was truncated before the unsynced `writeCSN` append reached the durable layer, which is a finding | `RestartLoadMutation` keeps an entry whose tid has no log entry (`MutationCrash`) |

### Write-write conflicts {#invariants-conflicts}

| Property | Statement | Witness (scenario) |
|---|---|---|
| `SingleRemover` | `Cardinality(h_removers[p]) <= 1` | the CAS in `DropLockOne` replaced by an unconditional write of `lock`, so a second transaction overwrites a committed remover's lock and stores its own `removal_tid` (`Base`) |
| `LockConsistent` | `lock = t` transactional implies `mem.removal_tid \in {Empty, t}`; `lock = NonTransactionalTID` implies `mem.removal_tid \in {Empty, NonTransactionalTID}`; `lock = 0` implies `mem.removal_tid = Empty` or `mem.removal_csn /= 0` | `RollbackUnlock` unlocks before clearing `removal_tid` (`Base`) |
| `MutationOnVisible` | every selected `(m, p, visible)` in `h_selected` has `visible = TRUE` | `MutSelect` selects and records a candidate regardless of the visibility result (`Mutation`) |
| `MutationContent` | after a committed mutation `m` of `p`, every read with `snapshot >= h_csn` sees `p`'s fragment at `ver + 1` (or the tombstone), never at `ver` | `MutFinish` publishes `ver` instead of `ver + 1` (`Mutation`) |
| `NoOrphanMutation` | once `h_rolled_back[t]` holds and every `MutRegister` of `t` has completed or failed, no mutation of `t` is `Registered` or `Selected` (a rollback in progress may still see registered mutations, because `killMutation` runs after the CAS to `RolledBackCSN`) | the re-check in `MutRegister` removed (`Mutation`, with `KillTransaction` between `MutPrepareAttach` and `MutRegister`, so that `RollbackKill*` finds nothing and `MutRegister` registers afterwards) |

### Assertions from the code {#invariants-code}

Every `chassert` and `LOGICAL_ERROR` on the modelled paths, transcribed as the code has them. Each is an invariant
`Assert_<name>`; its witness is the removal of the guard that the code relies on, named in the last column.

| Assertion | Statement | Witness (scenario) |
|---|---|---|
| `validateInfo`, running creator | if a transaction with `creation_tid` is running and `creation_csn \notin {0, RolledBackCSN}` and the transaction's `csn` is not `CommittingCSN`, then `creation_csn` equals the transaction's `csn` | `CommitStoreCreation` writes `h_csn + 1` (`Base`) |
| `validateInfo`, no creation CSN | `creation_csn = 0` implies `removal_csn = 0` and `removal_tid \in {Empty, creation_tid}` | `NtDropLock` skips the uncommitted-creation refusal, so a non-transactional removal stores `removal_csn = NonTransactionalCSN` on a part with `creation_csn = 0` (`NonTxn`) |
| `validateInfo`, order | `creation_csn /= 0` implies (`removal_csn = 0` or `removal_csn = NonTransactionalCSN` or `creation_csn <= removal_csn`) and (`creation_tid` non-transactional or `creation_tid.start_csn <= creation_csn`) | `CommitStoreCreation` writes `CSN_MAX` instead of `h_csn`, so a later committed removal by another transaction has a smaller CSN (`Base`) |
| `validateInfo`, removal | `removal_csn /= 0` implies `removal_tid /= Empty` and `removal_tid.start_csn <= removal_csn` | `DropLockOne` locks in memory only and skips the `setAndStoreRemovalTID` store, so `CommitStoreRemoval` later stores `removal_csn` with `removal_tid = Empty` (`Base`) |
| `validateInfo`, exempt shape | the `DummyTID`/`RolledBackCSN`/empty-removal shape produced by `loadMetadata` case 2 is skipped | not a property, a definition |
| `isVisible`, fast path | `removal_csn /= 0` implies `creation_csn /= 0`; both CSNs are `0`, `NonTransactionalCSN` or above `MaxReservedCSN` | `CommitStoreCreation` skipped for a part both created and removed by `t`, with `CommitStoreRemoval` running before `UpdLoadNewEntries`, so `updateCSNIfNeeded` cannot repair `creation_csn` (`Base`) |
| `isVisible`, slow path | on entry to the slow path at least one CSN is `0`, and `current_tid` is neither the creator nor the remover | the `creation_tid = current_tid` clause removed (`Base`) |
| `getOldestSnapshot` | `snapshots_in_use` sorted, same size as `running_list` | `SetSnapshot` also rewrites `protected_snapshot` (`SetSnapshot`) |
| `NoUnknownMutationCSN` | `MutSelect` never meets a transactional mutation with no running transaction and `csn = 0` | the re-check in `MutRegister` removed, so an orphaned registered mutation reaches `MutSelect` after its transaction is gone (`Mutation`) |

The assertion in `preparePartForRemoval` (an `Outdated` part with a transactional creation has a `removal_tid`) is
not listed: on the modelled paths a part becomes `Outdated` at load only through `removal_csn /= 0` or
`RolledBackCSN`, both of which imply the required shape, so no witness can falsify it. It is kept as a comment in
`Server.tla` next to `RestartLoadPart`, not as a property.
| `NoProcessDown` (scenarios without disk faults) and `DownOnlyByNoexceptFault` (with them) | `ProcessDown` is reached only from a `StorePersist` fault or `STALE_VERSION` inside `afterCommit`, `rollback` or `finalizeCommittedTransaction` | `Fail` allowed inside `afterCommit` (`DiskFault`) |

### Liveness {#invariants-liveness}

Only in the `Live` and `MutationChain` scenarios, with weak fairness on the updating thread, the cleanup thread,
the mutation executor and the client's `CommitAck`, and stated only for transactions begun while `Begin` was
enabled:

| Property | Statement | Witness (scenario) |
|---|---|---|
| `CommitResolves` | a transaction in `Committing` or in `unknown_state_list` is eventually `Committed` or `RolledBack` | `UpdSwapUnknownLists` never moves entries to the loaded list (`Live`) |
| `OutdatedEventuallyDeleted` | an `Outdated` part with a committed removal is eventually `Deleted`, provided no transaction runs forever | `CleanupGrab` requires `pins /= {}` (`Live`) |
| `ChainReported` | a transactional mutation that waits on a non-transactional one that waits on the same transaction is eventually reported as failed by `waitForMutation` | the deadlock check removed from `waitForMutation` (`MutationChain`) |

## Scenario matrix and bounds {#scenarios}

Constants, all set per scenario: `Sessions` (1 or 2, symmetric), `TID_MAX` (the bound on `Begin`), `Parts` (the
universe of the scenario), `Mutations`, `CSN_MAX` (a guard on `CommitCreateCSN`, not a state constraint),
`RESTARTS_MAX`, `KEEPER_FAULTS_MAX`, `DISK_FAULTS_MAX`, `QUERY_FAULTS_MAX` (each 0 or 1), `MAX_STORE_RETRIES`
(2), `DISK_MODE` (`Durable` or `Layered`), `FSYNC_PART_DIRECTORY`, `WAIT_MODE`. State is finite because every
unbounded counter has a guard and every history variable is bounded by `TID_MAX`, `Parts` and `Mutations`.

`Updater` below means `UpdLoadNewEntries` alone; `Updater+GC` adds `UpdRemoveOldEntries*`; `Updater+Unknown` adds
`UpdReconnect`, `UpdSwapUnknownLists`, `UpdFinalizeUnknown`. `Store` (the three-step metadata store) is enabled
everywhere.

| Scenario | Universe | Enabled actions | Faults | Checks |
|---|---|---|---|---|
| `Base` | `P1`, `P2` | `Begin`, `Insert*`, `Select*`, `Drop*`, `Commit*`, `Rollback*`, `KillTransaction`, `Updater` | none | isolation, conflicts, read-your-writes, code assertions |
| `SetSnapshot` | `P1`, `P2` | `Base` + `SetSnapshot` + `Cleanup*` + `Updater+GC` | none | `NoPrematureDelete`, `NoLostVisibleData` |
| `Merge` | + `M12` | `Base` + `Merge*` + `Cleanup*` + `Updater+GC` | none | `NoDoubleRead`, `NoPrematureDelete`, `Atomicity`, `RegrabBeforeDelete` |
| `Mutation` | + `P1m`, `P2m` | `Base` + `MutPrepare*`, `MutRegister`, `Mut*`, `Kill*` | none | `MutationOnVisible`, `MutationContent`, `NoOrphanMutation`, `NoUnknownMutationCSN` |
| `MutationChain` | `P1`, `P1m` | one session, three mutation entries txn, non-txn, txn, `Updater` | none | `ChainReported` |
| `NonTxn` | + `E` | `Base` + `Nt*` + `NtSelect*` + `Cleanup*` | none | `LockConsistent`, `SingleRemover`, `RollbackRestores`, durability under mixed load |
| `Implicit` | `P1`, `P2` | the implicit wrapper, `Updater` | query | acknowledgement ordering |
| `Keeper` | + `M12` | `Base` + `Merge*` + `Updater+GC` + `Updater+Unknown` | Keeper, both wait modes | `UnknownResolvesByLog`, `NoOutdatedLookup`, the two-list race |
| `Crash` | + `M12` | `Base` + `Merge*` + `Cleanup*` + `Updater+GC` + `Restart*`, `Layered` disk | restart, both `FSYNC_PART_DIRECTORY` values | `NoResurrection`, `LogEntryNeeded`, `AckedWriteIsDurable` across restart |
| `MutationCrash` | `P1`, `P1m` | one session, `Begin`, `Insert*`, `MutPrepare*`, `MutRegister`, `Mut*`, `Commit*`, `Rollback*`, `Updater+GC`, `Restart*`, `Layered` disk | restart | `MutationRecovered`, `LogEntryNeeded` for mutation records, the written-but-unattached window, the unsynced `writeCSN` append |
| `DiskFault` | + `M12` | `Base` + `Merge*` + `Restart*`, `Layered` disk | disk write | `DownOnlyByNoexceptFault`, recovery after `ProcessDown` |
| `QueryFault` | + `P1m` | `Base` + `MutPrepare*`, `MutRegister`, `Mut*` | query | rollback between steps, the orphan-file window |
| `Live` | `P1`, `P2` | `Base` + `Cleanup*` + `Updater+GC` + `Updater+Unknown` | Keeper | `CommitResolves`, `OutdatedEventuallyDeleted` |

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
- `witness.sh <Scenario> <Property>`: applies `Witness_<Property>`, checks only that property, runs TLC, and exits
  zero only if TLC reports a violation of it.
- `README.md`: goal, scope and the "not covered" list, the code map (action, file, function, one line each), the
  run table (scenario, date, commit, states, distinct states, time, result), the witness table (property, witness,
  scenario, last verified), the refinement parameters (`MAX_STORE_RETRIES` model 2 versus C++ 20), and the
  counterexample log (scenario, trace file, C++ call sequence, verdict: model defect or code defect, follow-up).

No CI job in this version. A later change can add the `Base` scenario as a fast check.

## Keeping the model faithful {#fidelity}

Three checks, all mandatory before a counterexample is reported as a code defect:

1. Defect injection: every witness in the invariant tables, run by `witness.sh`, plus one per counterexample found
   (remove the guard the trace exploits and confirm TLC still finds it; restore it and confirm TLC finds the
   original trace again).
2. Known traces: the sequences exercised by the integration test `test_transactions` and by the stateless tests
   that use `transaction_force_unknown_state_after_commit`, `transaction_after_commit_pause`,
   `mt_pause_before_register_mutation` (`04516_mutation_kill_transaction_race.sh`) and
   `mt_throw_after_mutation_commit` are written as TLA+ trace expressions and must be accepted by the model.
3. Code-map review: a reviewer with the C++ tree open checks every row of the code map against the action's
   definition, in particular the step boundaries and every precondition that is not a `chassert` in the code.

A counterexample is mapped to C++ by replacing each action with the code-map row; if a step has no
counterpart, the model is wrong and is fixed first. A confirmed code defect is reproduced, where the fail points
allow it, as an integration test.

## Development order {#development-order}

1. `Keeper.tla`, `Disk.tla` with their own small `MC` configurations and unit invariants.
2. `TxnLog.tla`, `Parts.tla` (including store frames), `Locks.tla`, `History.tla`, `Client.tla`, `Server.tla`
   with `Begin`, `Insert*`, `Select*`, `Drop*`, `Commit*`, `Rollback*`, `KillTransaction`, `UpdLoadNewEntries`;
   the `Base` scenario green; every `Base` witness red.
3. `SetSnapshot`, `Cleanup*`, `UpdRemoveOldEntries*`; the `SetSnapshot` scenario.
4. `Merge*`; the `Merge` scenario.
5. `Nt*`, `NtSelect*`; the `NonTxn` scenario.
6. Keeper faults and the rest of the updating thread; the `Keeper` scenario, both wait modes.
7. `Crash`, `Restart*` with asynchronous part loading; the `Crash` scenario, both `FSYNC_PART_DIRECTORY` values.
8. `MutPrepare*`, `MutRegister`, `Mut*`, `Kill*`; the `Mutation`, `MutationChain`, `MutationCrash` and
   `QueryFault` scenarios.
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
  `afterCommit` and `rollback` (version 1.1, first item).
- The uncovered operations listed in the scope section, each as an extension of `Server.tla` and one more
  scenario row.
- Disk and memory corruption as extra outcomes of `StorePersist` and of `StorePublish`.
- Weak-memory reads of `creation_csn` and `removal_csn` as an explicit stale-read action, if a future
  implementation reintroduces relaxed loads on the visibility path.
