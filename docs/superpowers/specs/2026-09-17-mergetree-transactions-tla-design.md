---
description: 'Design of a TLA+ model of MergeTree transactions as implemented in upstream ClickHouse master: which entities, transitions, failures and invariants the model covers, how it is kept faithful to the C++ code, how TLC runs are bounded so that they finish, and what is deliberately left out of the first version. The model exists to find bugs in the implementation, not to document it.'
sidebar_label: 'MergeTree transactions, TLA+ model'
sidebar_position: 20
slug: /superpowers/specs/mergetree-transactions-tla-design
title: 'A TLA+ model of MergeTree transactions for finding bugs with TLC'
doc_type: 'design'
---

# A TLA+ model of MergeTree transactions for finding bugs with TLC {#mergetree-transactions-tla-design}

Revision 7, 2026-09-17. Baseline: upstream `ClickHouse/ClickHouse` master at commit `2c24b6b9291e`. The transaction
sources listed in the code map below are identical between `d1ba1699a271` (2026-09-16) and this commit; every
function name in this document refers to that tree.

Revision 7 folds the sixth review round, which calibrated the model against the 23 defect-fixing upstream commits
of the last two years (4 caught, 12 missed in scope, 6 out of scope, table in the calibration section) and one
Altinity fix (PR 2396). The misses were systematic and are closed as classes, not one by one: the
non-transactional removal batch is a step machine with a contract (durable stamping on success, no change on
refusal, oracle-valid refusal cause); the transaction mutex is a variable so that the enrol-then-store window of
`removeOldPart` exists; cleanup validates metadata (`hasValidMetadata`) with a no-false-corruption property; the
mutation file has an owner, a move transfers it, and a registered mutation must keep its file; the waiting client
is gated on an explicit notification; `KILL MUTATION` is a client action; the legacy metadata format is a disk
value; the `DummyTID` assertion is a property; `CommittedMutationApplied` is restated over sources only; the
policy for a store fault inside `noexcept` is a constant so that the model can validate the retry-in-place fix.
Revision 6 folded the fifth review round, which asked which building blocks are load-bearing and how the model
could be sabotaged from either side: the restart loader follows the coverage tree of `loadDataPartsFromDisk`
(children of a non-active cover are reloaded as active candidates); `Atomicity` and `RollbackRestores` tolerate
later legal removals; a declarative visibility oracle computed from history, not from part metadata, backs
`MutationOnVisible` and `NoLostRead`; three properties close holes that plausible code bugs would slip through
(`CommittedMutationApplied`, `FlipAfterStores`, `NoSpuriousStaleVersion`); `MutationRecovered` is green except
for the narrowly named truncation case; `down_cause` is ghost state; `MutWait` is fair; the non-transactional
reader is removed as ornamental and mis-modelled; pairwise scenarios for subsystems that share state are added;
bounds may be reduced only while every witness of the scenario stays red; two-change witnesses must be minimal;
accepted risks are listed in their own section so that nothing is excluded silently. Revision 5 folded the
fourth review round, which read the spec as its implementer (12 implementability defects, 2
fidelity findings, 3 unreachable witnesses): the read monitor keeps parts as well as fragments so that
`Atomicity` is decidable; `RollbackRestores` and `NoLostVisibleData` are action properties scoped to the step they
are about; `MutationContent` is a state invariant on part payloads; `loaded_parts`, `loaded_mutations`,
`down_cause`, `last_error` and the mutation `fail_reason` are declared; `Error` has delivering actions; natural
exceptions (`Refuse`) are separated from injected ones (`Fail`); mutation completion is the predicate the code
computes, not a flag; restart loads parts covered by another on-disk part as `Outdated` with a non-transactional
removal, as `loadDataPartsFromDisk` does; `MutationRecovered` is split into a finding and a witnessable property;
the three remaining witnesses use the two-change allowance and `chassert` semantics are spelled out. Revision 4
folded the third review round (5 findings, 10 witnesses that could not reach their target):
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
  to reach the rollback-during-registration window of `startMutation` that the code defends against.
- `KILL MUTATION` issued from another session: the `Kill*` steps applied to a registered mutation regardless of
  its transaction's state. It is the only way to reach the window in which a transactional mutation's entry is
  erased while its transaction is between `CommitCreateCSN` and `CommitStoreMutation`; on the baseline
  `setMutationCSN` then throws `LOGICAL_ERROR` inside `noexcept`, which is the second defect fixed by Altinity
  PR 2396.
- The legacy `txn_version.txt` format without `storing_version`, as a disk value that `loadMetadata` must accept.
- Implicit transactions (`implicit_transaction = 1`) as a wrapper scenario that fixes the order begin, query,
  commit or rollback, acknowledgement, reusing the same storage actions.
- The same writing operations without a transaction (`NonTransactionalTID`), interleaved with transactional
  ones. A non-transactional `SELECT` (the `Active` set without any visibility filter) is not modelled: no property
  needs it, and the one that used it in an earlier revision is now stated on part states directly.
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
- `KILL QUERY`.
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
| `History.tla` | Ghost variables that survive crash and log truncation, including `down_cause` and `h_truncated`; the section on history lists them | |
| `Invariants.tla` | The declarative visibility oracle `OracleVisible`, and every property from the section on invariants, each with its witness | |
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
- `mutex`: the holder of `MergeTreeTransaction::mutex`, or none. `removeOldPart` holds it from the enrolment of
  the part in `removing` through the store of `removal_tid`; `rollback` holds it while copying the work lists.
  Making it a variable is what lets the model contain the pre-fix window of upstream `09d4267611b3`, where the
  store ran after the mutex was released.
- `csn_notified`: whether the last change of `csn` was followed by `notify_all`. `waitStateChange` in the code
  wakes only on a notification (upstream `f6ad379c8301`); the model gates the waiting client on this flag rather
  than on the state change itself.
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
- `loaded_parts`, `loaded_mutations`: the objects `RestartLoadPart` and `RestartLoadMutation` have processed since
  the last `Crash`; both are the full universe while no crash has happened. The invariant preamble uses them to
  decide whether a part-state clause reads memory or the durable layer.

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
  (the `VersionInfo` the caller intends to store), `pc \in {Read, Persist, Publish}`, `retries`, and
  `interfered`, set when another frame's `StorePersist` on the same part succeeds between this frame's
  `StoreRead` and its `StorePersist`. Two callers can be suspended around the same part at once; that is the
  stale-version race, and `interfered` is what lets a property tell a legitimate `TOO_OLD_VERSION` from a
  spurious one.

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

A `txn_version.txt` value may also be `Legacy`: the pre-`storing_version` format that
`VersionInfo::readFromMultiLineBuffer` maps to a non-transactional creation. It is placed only by the initial
state of a scenario that opts in (`LEGACY_PARTS`), never written by an action, and exists so that the restart
path is checked against the format the code promises to accept (upstream `aea1c111e0a8`).

In `Durable` mode (every other scenario) `cached` and `durable` are one field and `Fsync` is a no-op. The constant
`FSYNC_PART_DIRECTORY` decides whether the rename in `storeInfoToDataPartStorage` is durable without a directory
fsync. With it off, a crash after the rename may leave the durable layer in the state before the rename: `tmp`
present and the old file present, which `loadMetadata` case 2 treats as a rolled-back part.

### Mutations {#entities-mutations}

`mut[m]`: `mstate \in {Absent, Written, Attached, Registered, Killed}`, `tid`, `csn` (in memory), `fail_reason`
(`None` or `Deadlock`, the `latest_fail_reason` that `getIncompleteMutationsStatusUnlocked` reports),
`file_owner \in {None, Preparing, Map}` (which C++ `MergeTreeMutationEntry` object owns the file: the local in
`prepareMutationEntry`, or the entry in `current_mutations_by_version`; the destructor of an owner that is not
registered removes the file, and `MutRegister` moves ownership, upstream `2903f6d48693` and `8e5e5a69150e`), and
the on-disk record in `mdisk[m]` with `tmp_file`, `file`, `tid`, `csn`. `writeCSN` appends without fsync.
`Written` is after `entry.commit` (the final file exists) and before `addMutation`; `Attached` is after
`addMutation` and before the insertion into `current_mutations_by_version`; `Registered` is after that insertion.

Completion is not a state but the predicate the code computes in `getIncompleteMutationsStatusUnlocked`:
`MutationDone(m)` holds when every part visible to the mutation's transaction (or, for a non-transactional
mutation, visible at `MaxCommittedCSN`) has a data version at or above the mutation's version, that is, when every
visible source part of the partition has been replaced by its mutated result. `is_done` in the code is set by
`markFinishedMutations` from the same computation and is bookkeeping only; `waitForMutation` returns when
`MutationDone(m)` holds, when `m` is killed, or when `fail_reason /= None`.

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
  with the tid it refers to (also recorded in the history module). Only the `Commit*` actions and `CommitError`
  write it; the failure of any other query is recorded in `last_error` and never in `outcome`.
- `last_error`: the error code of the last failed query in the session (`SERIALIZATION_ERROR`,
  `INVALID_TRANSACTION`, `STALE_VERSION`, `LOGICAL_ERROR`, `Injected`), or `None`.
- `read`: the bounded read monitor: for the current transaction, the first completed `SELECT`, the last completed
  `SELECT`, and the in-flight capture, each as a pair of the set of parts found visible and the (fragment,
  version) set they expand to. Parts are kept because `Atomicity` must tell a fragment read from a merge result
  from the same fragment read from a source. Nothing else about reads is kept, so repeated `SELECT` does not grow
  the state.
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
  `DropEnrol`, `MergeFinish`, `MutFinish` and `MutPrepareAttach`, so that they exist for transactions that fail
  or roll back before commit. `h_effects[t]` denotes the three together.
- `h_rolled_back[t]`: set at `RollbackFinalize(t)`.
- `h_unknown[t]`: for a transaction resolved from `unknown_state_list`, whether the decision was `Committed` or
  `RolledBack`, and whether `t \in h_committed` at that moment.
- `h_removers[p]`: the set of tids that committed a removal of `p`: a transactional `t` with `p \in h_removing[t]`
  is added at `CommitCreateCSN` (`Ok` or `LostAfter`), `NonTransactionalTID` at `NtBatchStore(p)`.
- `h_selected`: the set of `(m, p, visible)` triples recorded by `MutSelect` for the parts it actually selected,
  where `visible` is the result of the visibility test at selection. Candidates that the test rejects are not
  recorded, because the code skips them without selecting.
- `h_content[t]`: the (fragment, version) set visible at `txn[t].snapshot`, captured at `Begin` and recaptured
  at every `SetSnapshot(t)`, so that a loss caused by choosing an older snapshot is not attributed to the next
  action of another actor.
- `h_truncated`: the set of tids whose `zk_log` entry `UpdRemoveOldEntriesDelete` has removed.
- `h_batch`: for the non-transactional removal batch in flight, its target set and each target's `mem` and
  stored metadata as of `NtBatchStart`; `h_batch_outcome \in {None, Done, Refused}` set by the step that ends the
  batch. Cleared when the next batch starts.
- `h_prepared_files`: the set of mutations whose preparing query failed after `MutPrepareWrite`, so that a
  property can state what must be true of their files afterwards.
- `down_cause \in {None, StoreFault, RetryExhausted, Other}`: why the last `ProcessDown` happened, set by the
  transition that takes the server down and cleared by `RestartDone`. It is ghost state precisely so that `Crash`
  does not erase it.

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
| `DropEnrol(k, p)` | `MergeTreeTransaction::removeOldPart`: take `txn.mutex`, `checkIsNotCancelled`, `lockRemovalTID`, push to `removing` and `h_removing[t]` |
| `DropStore(k, p)` | `MergeTreeTransaction::removeOldPart`: `setAndStoreRemovalTID(t)` through the three-step store, then release `txn.mutex`; the two rows together are what earlier revisions called `DropLockOne` |
| `KillMutation(k, m)` | `InterpreterKillQueryQuery` for `KILL MUTATION`: the `Kill*` steps of the mutation section on a `Registered` mutation, from session `k`, regardless of the state of the mutation's transaction |
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
| `CommitError(k)` | `executeCommit` throws: `INVALID_TRANSACTION` from `beforeCommit` when the transaction was cancelled (`KillTransaction`), or "Transaction was rolled back" after `WAIT_UNKNOWN` resolved a `FailBefore` commit by rollback; sets `outcome := Error` and `h_outcome[t] := Error` |
| `Refuse(k)` | a natural exception the code throws on the modelled path: `SERIALIZATION_ERROR` from `lockRemovalTID`, `NtBatchPreflight` or `NtBatchLock`, `STALE_VERSION` from the store, `INVALID_TRANSACTION` from `MutRegister` or `checkIsNotCancelled`, `LOGICAL_ERROR` from `validateInfo`; always enabled where the code throws, not counted as a fault; sets `last_error`, then `RollbackStart` through `onException` |
| `RollbackStart(k)` | `TransactionLog::rollbackTransaction`, `MergeTreeTransaction::rollback`, the CAS to `RolledBackCSN` |
| `RollbackKill*(k, m)` | `killMutation`, in the steps of the mutation section |
| `RollbackMarkCreated(k, p)` | `setAndStoreCreationCSN(RolledBackCSN)` |
| `RollbackOutdateCreated(k, p)` | `removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part})` |
| `RollbackRestore(k, p)` | `restoreAndActivatePart` |
| `RollbackUnlock(k, p)` | `setAndStoreRemovalTID(EmptyTID)` then `unlockRemovalTID` |
| `RollbackFinalize(k)` | erase from `running_list` and `snapshots_in_use`, `afterFinalize` |
| `KillTransaction(k, t)` | `InterpreterKillQueryQuery` for `KILL TRANSACTION`: `rollbackTransaction(t)` from session `k`, which runs the `Rollback*` steps concurrently with whatever `t`'s own session is doing |
| `Fail(k)` | an injected exception between two steps of a query, counted by `QUERY_FAULTS_MAX`; leads to `RollbackStart` through `txn->onException()` |

`Begin` takes `snapshot := latest_snapshot`, `protected_snapshot := snapshot`, allocates the next tid with
`start_csn = snapshot`, inserts `protected_snapshot` into `snapshots_in_use` and the transaction into
`running_list`, all in one step because the code holds `running_list_mutex` throughout. It is disabled once
`local_tid_counter` reaches the scenario's bound, so the state space is finite by construction rather than cut by
a constraint.

`SetSnapshot(k, c)` sets `txn.snapshot := c` for any `c` in `(MaxReservedCSN, CSN_MAX]`, or `c = NonTransactionalCSN`,
or `c = EverythingVisibleCSN`, exactly the values `executeSetSnapshot` accepts. `c` may be a CSN that is not yet
allocated or one that log truncation has removed. `protected_snapshot` and `snapshots_in_use` are unchanged, which
is what the model has to exercise against `Cleanup*` and `UpdRemoveOldEntries*`. The action also recaptures
`h_content[t]` at the new snapshot. Isolation properties skip a transaction whose snapshot is
`EverythingVisibleCSN`, because that value is an introspection mode by design.

`InsertWrite` creates the part in state `Temporary` with `creation_tid = t` and stores `txn_version.txt`.
`InsertCommit` computes the covered parts from the covering relation (`getActivePartsToReplace` and
`getCoveredOutdatedParts` filtered by visibility), records the part in `creating` and in `h_creating[t]` (pin
`Txn(t)`), and for every covered part runs the same lock-and-store steps as `DropEnrol` and `DropStore` before flipping states.
A client `INSERT` never covers anything in the chosen universes; the step exists because merges and mutations
reuse it.

`SelectCapture` takes `lockParts`, copies the set of `Active` and `Outdated` parts, pins them with `Select(k)`,
releases the lock. `SelectCheck(k, p)` evaluates `isVisible(snapshot, tid)` for one captured part against the
current `mem` and `tid_to_csn`; between two checks any other action may run, which is the interleaving the code
allows. `SelectFinish` expands the visible parts to (fragment, version) pairs, updates the monitor, drops the pins.

`DropStart` increments `merges_blocker`, waits until `reserved = {}` (a precondition), takes `parts_lock`, and
selects the visible parts of the partition. Per part, `DropEnrol` takes the transaction mutex, checks the
transaction is not cancelled, runs the CAS on `lock` from `0` to `t` (a failure throws `SERIALIZATION_ERROR`,
which is a `Refuse` that also releases the mutex, the lock and the blocker) and enrols the part in `removing` and
`h_removing[t]`; `DropStore` runs `setAndStoreRemovalTID(t)` and releases the mutex. `RollbackStart` needs no
mutex, but the rollback's list copy does, so on the baseline the rollback cannot pass between enrolment and
store; with the pre-fix behaviour of `09d4267611b3` (mutex released before the store) it can, and `LockConsistent`
then fails after the late store. `DropOutdate` flips every part of the batch to `Outdated`, releases `parts_lock`
and the blocker.

`MutPrepareWrite` writes the temporary file and renames it to its final name; `file_owner := Preparing`.
`MutPrepareAttach` attaches the mutation to the transaction (`h_mutations[t]`). A `Fail` between
`MutPrepareWrite` and `MutRegister` destroys the preparing owner: `MutDestroyOwner(m)` removes the file iff
`file_owner = Preparing` (the destructor's `is_temp = FALSE, is_registered = FALSE` branch) and records `m` in
`h_prepared_files`; a `Crash` in the same window leaves the file for `RestartLoadMutation` to find.
`MutRegister` inserts the entry into the map, moving ownership (`file_owner := Map`), and, under the same
mutex, re-checks the transaction: if it is already `RolledBack`, the entry is unregistered again, its file
removed, and the query ends with a `Refuse` (`INVALID_TRANSACTION`). With the pre-fix behaviour of
`8e5e5a69150e` (a move that leaves the source owning the file) the preparing owner's destructor runs after
registration and deletes the registered mutation's file, which `RegisteredMutationHasDurableFile` reports. `KillTransaction` or a `RollbackStart` from another mutation's `MutFail` may run between
`MutPrepareAttach` and `MutRegister`; that is the window the re-check exists for.

`CommitBefore` requires, for every attached mutation `m`, `MutationDone(m)` or `mstate = Killed` or
`fail_reason /= None` (`waitForMutation`; a failed wait ends the commit with a `Refuse`), records
`h_snapshot[t]`, then CAS `Unknown -> Committing`. A transaction with empty effects takes `CommitReadOnly` instead
of `CommitCreateCSN`, as `commitTransaction` does through `isReadOnly`; it records `h_csn[t] := snapshot` and does
not enter `h_committed`. `CommitCreateCSN` is the commit point; its three outcomes are in the failure section; on
`Ok` and `LostAfter` it records `h_committed` and `h_csn`. The `afterCommit` steps run in the order of the code:
creations, removals, mutations, then `CommitFlip`, which also sets `csn_notified` (`csn.notify_all()`).
`CommitStoreMutation(k, m)` on a mutation that `KillMutation` has already unregistered throws `LOGICAL_ERROR`
from `setMutationCSN` on the baseline; inside `noexcept` that is `ProcessDown` with `down_cause = Other`, which
`NoProcessDown` reports. `CommitFinalize` removes the transaction from the running list and releases the
snapshot, and `CommitAck` delivers `Acked` to the client. With `wait_mode = WAIT_UNKNOWN`, `CommitUnknown` returns
`CommittingCSN` and the client blocks in `waitStateChange`, which in the model is enabled only when
`csn_notified` holds for the transaction's current `csn`; with any other mode the client receives `UnknownStatus`
and the transaction is detached from the session. A store fault inside `afterCommit` is handled according to
`NOEXCEPT_STORE_FAULT_POLICY`, see the disk section.

`RollbackStart` is the CAS `Unknown -> RolledBack` followed by `csn.notify_all()` (`csn_notified`); if the
transaction is already `RolledBack` (concurrent `killMutation` or `KillTransaction`) or already `Committed`,
rollback does nothing. `RollbackCopyLists` then takes `txn.mutex` to copy the work lists and releases it. The
subsequent steps run in the code's order: mutations killed, created parts marked `RolledBackCSN` on disk, created
parts outdated, removed parts restored to `Active` (unless created by the same transaction), removed parts
cleared on disk and unlocked. The work lists pin their parts with `Rollback(t)` until `RollbackFinalize`, which
sets `h_rolled_back[t]`.

A query ends without completing in exactly two ways. `Refuse(k)` is the exception the code itself throws at a
modelled point (`SERIALIZATION_ERROR` when a removal lock is held or a non-transactional removal meets an
uncommitted creation, `STALE_VERSION` after `MAX_STORE_RETRIES`, `INVALID_TRANSACTION` from `MutRegister` or from
`checkIsNotCancelled` after `KillTransaction`, `LOGICAL_ERROR` from `validateInfo`); it is enabled whenever the
code would throw, in every scenario, and is not counted by any fault constant. `Fail(k)` is an injected exception
enabled between any two steps of `Insert`, `Drop`, `MutPrepare*`, `MutRegister`, `Select` and before
`CommitBefore`, counted by `QUERY_FAULTS_MAX`; it stands for a disk write fault outside `noexcept` and any
unrelated exception. Both set `last_error`, release any lock or blocker the query holds, and lead to
`RollbackStart` through `onException`.

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
| `CleanupValidate(p)` | `IMergeTreeDataPart::assertHasValidVersionMetadata` → `VersionMetadata::hasValidMetadata`: compare `mem` with the stored record; a mismatch is `CORRUPTED_DATA` unless it is one of the transient shapes the code allows (memory holds a CSN learned from the log while the stored record still has `UnknownCSN`; `RolledBackCSN` in memory only; `NonTransactionalCSN` in memory not yet stored); a missing stored record is accepted only for the `DummyTID`/`RolledBackCSN` shape or a missing directory; a failure is `CleanupDeleteFail` |
| `CleanupDeleteOk(p)` | `clearPartsFromFilesystemAndRollbackIfError` success: directory removed in both layers, `removePartsFinally`, `Deleted` |
| `CleanupDeleteFail(p)` | the same function's error path, or a `CleanupValidate` failure: `rollbackDeletingParts`, back to `Outdated` |

`CleanupGrab` is enabled for an `Outdated` part when `canBeRemoved` holds with `getOldestSnapshot` (over
`protected_snapshot` values) and `pins = {}`. `CleanupValidate` exists because two upstream fixes
(`3cc9b0936320`, `271f99510f1a`) were false `CORRUPTED_DATA` or `CANNOT_OPEN_FILE` at exactly this point, which
kept parts, and once a `DROP TABLE`, retrying forever.

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
| `MutWait(k, m)` | `waitForMutation` inside `CommitBefore` or a synchronous `ALTER`: returns when `MutationDone(m)`, `Killed` or `fail_reason /= None`; the deadlock check of `getIncompleteMutationsStatusUnlocked` sets `fail_reason := Deadlock` when a transactional `m` depends on an earlier mutation of the same transaction with a non-transactional mutation between them |
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
| `NtBatchStart(B)` | the caller of `NonTransactionalRemovalLocks` (`removePartsFromWorkingSet` without a transaction, or `Transaction::commit` of a non-transactional covering part) fixes the target set `B` of a removal batch; records `h_batch` |
| `NtBatchPreflight(p)` | `NonTransactionalRemovalLocks::lock`, per part: `isRemoved` skips the part; `isCreatedByUncommittedTransaction` (which consults `tid_to_csn`, not only `mem.creation_csn`, upstream `65e4e2b5bf69`) refuses the whole batch with `SERIALIZATION_ERROR` before anything is locked or stored |
| `NtBatchLock(p)` | `NonTransactionalRemovalLocks::lock`, per part: `lockRemovalTID(NonTransactionalTID)`; a held lock refuses the whole batch, and every lock taken so far is released by the destructor (upstream `86b6861a1a8e`) |
| `NtBatchStore(p)` | `NonTransactionalRemovalLocks::store`, per part: `setAndStoreRemovalTID(NonTransactionalTID)` through the three-step store, `removal_csn = NonTransactionalCSN` immediately, then unlock; `h_removers[p]` updated |
| `NtBatchEnd` | the last store done: `h_batch_outcome := Done`; a `Refuse` anywhere in the batch sets `Refused` |
| `NtDropCover` | `dropPartition` without a transaction: the empty part `E` created and committed over the partition through `NtBatch*` on the covered parts |
| `NtMerge*` | as `Merge*` with `txn = nullptr`; `MergeFinish` runs `NtBatch*` on the sources |
| `NtMutate*` | as `Mut*` with `NonTransactionalTID`; `MutFinish` runs `NtBatch*` on the source |

The batch is a step machine because five of the upstream fixes of the last two years were about it: a covered
part stamped in memory only (`ba2ee3239b8d`), a batch refused halfway with earlier members already stamped
(`ab40e11d3c73`, `f8f46fb1eb14`, `86b6861a1a8e`), and a refusal that trusted `mem.creation_csn = 0` although the
log already had the commit (`65e4e2b5bf69`). The contract in the invariant section states what the batch must
guarantee, independently of which step the code runs first. During `NtBatchLock` the lock is held with
`mem.removal_tid` still empty, and during `NtBatchStore` the tid is written before the unlock; the lock invariant
is phase-aware for that reason.

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
it is a `Fail` of the query, inside `noexcept` it is handled by the policy below. The write of `tmp` plus fsync
makes `tmp` durable; the rename makes the new content `cached`, and `durable` only when `FSYNC_PART_DIRECTORY` is
on or a later `Fsync` runs.

`NOEXCEPT_STORE_FAULT_POLICY \in {Terminate, Retry}` decides what a `StorePersist` fault or `STALE_VERSION`
inside `afterCommit`, `rollback` or `finalizeCommittedTransaction` does. `Terminate` is the baseline:
`ProcessDown`. `Retry` is the behaviour of Altinity PR 2396: the store is retried in place up to
`NOEXCEPT_RETRY_BUDGET` times while the transaction stays in `running_list` and holds its snapshot, and only an
exhausted budget is `ProcessDown`; the model exists to show that the retry is sound, that is, that every safety
property holds with the policy set to `Retry`, in particular that `tail_ptr` cannot advance past the retrying
transaction's entry (`LogEntryNeeded`) and that a `KillMutation` during the retry window is tolerated. Under
`Retry`, `CommitStoreMutation` on an unregistered mutation is a logged warning, not `LOGICAL_ERROR`, as in that
PR; under `Terminate` it is `ProcessDown` with `down_cause = Other`.

### Restart {#actions-restart}

`Crash` (or `ProcessDown`, see the failure section) followed by `Restart`, the latter as a sequence of steps:

| Step | C++ |
|---|---|
| `RestartLoadLog` | `TransactionLog::loadLogFromZooKeeper`: creates one placeholder `csn-` znode, loads `tid_to_csn`, `latest_snapshot`, `tail_ptr` |
| `RestartLoadPart(p)` | `loadDataPartsFromDisk` builds the coverage tree from the part names on disk and loads the roots first as `Active` candidates through `MergeTreeData::loadDataPart` (`VersionMetadataOnDisk::loadMetadata` with its four cases, `updateCSNIfNeeded`, `validateInfo`, store if updated, then `Outdated` plus `preparePartForRemoval` when `creation_csn = RolledBackCSN` or `removal_csn /= 0`, else `Active`). If a root does not load as `Active`, its children are pushed back onto the same queue and loaded as `Active` candidates in turn. Children of a root that did load as `Active` are left for `loadOutdatedDataParts`, which runs asynchronously after `RestartDone`, loads them with `to_state = Outdated` and calls `preparePartForRemoval`, which stores a non-transactional removal (`setAndStoreNonTransactionalRemovalTID`) when the part has none. Adds `p` to `loaded_parts`; the asynchronous outdated pass is what `async_loading_jobs` counts |
| `RestartLoadMutation(m)` | `StorageMergeTree::loadMutations`: write `csn` if the log has it, delete the file otherwise; a non-transactional file left by a failed preparation is registered as a real mutation, which is the pre-fix behaviour of `2903f6d48693` that `NoUnattachedMutationAfterFailure` targets; adds `m` to `loaded_mutations` |
| `RestartDone` | `server_completely_started := TRUE`, `down_cause := None`; enabled before every `RestartLoadPart` has run, because tables load asynchronously |

`RestartLoadPart` on a `Legacy` record maps it to a non-transactional creation and continues; on a tmp-only
directory it produces the `DummyTID`/`RolledBackCSN` shape, and the order of checks matters: `validateInfo` must
recognise that shape before anything calls `isNonTransactional` on the tid, because `isNonTransactional` asserts
on `DummyTID` (upstream `f5f4635154a0`); the model keeps that order as the property
`Assert_IsNonTransactionalDomain`. `updateCSNIfNeeded` on a part with `creation_tid = t` and no CSN asks
`tryGetCSN`, which returns `RolledBackCSN` when the log has no entry and no transaction with that tid is running. After a restart nothing is running, so any
part whose creating transaction has no log entry is rolled back, and any part with a removal tid but no log entry
gets its `removal_tid` cleared. Parts whose directory did not survive the crash are absent. The updating thread
starts at `RestartLoadLog`, so `UpdRemoveOldEntries*` can interleave with `RestartLoadPart`, which is the race
behind the `async_loading_jobs` gate. While the server is down (between `Crash` and `RestartLoadPart(p)`) the
part-state properties are evaluated on the durable layer, see the invariant section. The coverage tree is the
path to watch: after a crash between `MergeFinish` (the merged part renamed into place) and the merge's
`CommitCreateCSN`, the loader finds `M12` on disk as the root, loads it as rolled back, and only then reloads
`P1` and `P2` as `Active` candidates; after a crash between the merge's `CommitCreateCSN` and its
`CommitStoreCreation`, `M12` loads as `Active` from the log and `P1`, `P2` become `Outdated` with a
non-transactional removal in the asynchronous pass. Whether every reader sees the partition's fragments exactly
once across both orders is what `NoLostVisibleData`, `NoDoubleRead` and `NoResurrection` decide in the `Crash`
scenario.

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
`ProcessDown`, which is `Crash` reached only from such a fault (`down_cause := StoreFault`) or from
`STALE_VERSION` in the same call sites (`down_cause := RetryExhausted`), and continues with the normal `Restart`.
Any other transition that would take the server down inside those call sites sets `down_cause := Other`. The
conformance property `DownOnlyByNoexceptFault` states that `down_cause` is never `Other`; the safety properties
then check that the restart recovers a consistent state. In scenarios without disk faults, `NoProcessDown`
(`down_cause = None` always) is an ordinary invariant. Where the call site can throw
(`removeOldPart` inside a query, `setAndStoreCreationTID` at part creation), the fault becomes a `Fail` of that
query.

### Query faults {#failures-query}

`Fail(k)`, `MergeFail` and `MutFail` are enabled between any two steps of the respective operation while the
query-fault counter is below `QUERY_FAULTS_MAX`. `Refuse` is not a fault and is enabled in every scenario wherever
the code throws.

## Invariants and properties {#invariants}

Properties are stated over the history module, the client monitors and the durable layer where possible, and only
over internal variables where the code's own assertions are the subject. "Committed" always means
`t \in h_committed`. A part-state clause is evaluated on `part[p].pstate` while `p \in loaded_parts`, and on the
durable layer of `disk[p]` otherwise (a durable committed removal counts as `Outdated`, a durable rolled-back
creation or a missing directory counts as `Absent`); a mutation clause likewise uses `loaded_mutations`.

Two kinds of property are used. A state invariant is checked in every state (`INVARIANT` in the `.cfg`). An
action property is checked on every step and may refer to the step's pre- and post-state
(`PROPERTY [][...]_vars`); it is the form used where the obligation is about one step, such as "the step that
completes a rollback" or "a step of another actor", because the declared state does not record which action ran
last and adding a `last_actor` variable would multiply states for no other purpose.

Visibility oracle. `Invariants.tla` defines `OracleVisible(p, s, u)` from history alone: `p` is visible at
snapshot `s` to transaction `u` iff (`creation_tid(p) = u`, or `creation_tid(p) = NonTransactionalTID`, or
`creation_tid(p) \in h_committed` with `h_csn[creation_tid(p)] <= s`) and not (`u \in` the removers of `p` in
`h_removing`, or some `r \in h_removers[p]` with `r = NonTransactionalTID` or `h_csn[r] <= s`). It never reads
`part[p].mem`, `tid_to_csn` or the result of `isVisible`, so a property stated over it is independent of the
implementation of visibility in `Parts.tla`; `MutationOnVisible` and `NoLostRead` use it.

Witness contract: for each property `Q` the `MC` module has an override `Witness_Q` that changes one or two named
actions as stated in the table. `witness.sh Scenario Q` runs TLC with `Witness_Q` applied and only `Q` checked, in a scenario
whose enabled actions include those the witness needs, and must report a violation of `Q`. Other properties may
also fail under the witness; they are not checked in that run. A two-change witness must be minimal: restoring
either change alone must make the run green again, and `witness.sh` checks that. A property without a passing
witness is not accepted into `Invariants.tla`.

Bound contract: a scenario's bounds (`Sessions`, `TID_MAX`, `Parts`, `Mutations`, fault counters) may be reduced
from the values in the matrix only if every witness of every property checked in that scenario is still red
under the reduced bounds; the README records, per scenario, the smallest bounds at which that held.

### Durability and atomicity of the acknowledgement {#invariants-durability}

| Property | Statement | Witness (scenario) |
|---|---|---|
| `AckedWriteIsDurable` | `h_outcome[t] = Acked` and `h_effects[t]` non-empty implies `t \in h_committed`, and in every later state each part of `h_creating[t]` is `Active`, or `Outdated`/`Deleting`/`Deleted` with `h_removers[p] /= {}`, and no part of `h_removing[t]` is `Active` in any later state | `CommitAck` moved before `CommitCreateCSN` and `Fail` allowed after it (`Crash`) |
| `AckedReadOnly` | `h_outcome[t] = Acked` with empty `h_effects[t]` implies `t \notin h_committed` and `h_csn[t] = h_snapshot[t]` | `CommitReadOnly` replaced by `CommitCreateCSN` for empty-effect transactions (`Base`) |
| `ErrorIsAbsent` | `h_outcome[t] = Error` implies `t \notin h_committed`, and once `h_rolled_back[t]` or a restart has completed no part of `h_creating[t]` is `Active` | `RollbackOutdateCreated` skipped (`Base`; `Error` is delivered when `CommitBefore` finds the transaction cancelled by `KillTransaction`, or in `Keeper` after `FailBefore` with `WAIT_UNKNOWN`) |
| `UnknownResolvesByLog` | `h_unknown[t] = Committed` iff `t \in h_committed` at the decision | the two-list swap collapsed to one list (`Keeper`) |
| `Atomicity` | for any transaction `u` whose current snapshot `s` is `>= h_csn[t]`, every `SelectFinish` of `u` completed after `h_loaded[t]` has a visible-parts set `V` such that, with `C = {p \in h_creating[t] : no r \in h_removers[p] \ {t} has h_csn[r] <= s}` (created parts not since removed by another committed transaction visible to `u`), either `C \subseteq V` and `V \cap h_removing[t] = {}`, or `C \cap V = {}` and `h_removing[t] \subseteq V`; stated over the parts component of the read monitor, not the fragments, so a merge result and its sources are distinguishable | `SelectCheck` uses `mem` only and skips the `tid_to_csn` lookup (`Merge`, where `DROP` supplies multi-part removals) |
| `RollbackRestores` | action property on `RollbackFinalize(t)`: in its post-state every part `p` of `h_removing[t]` not in `h_creating[t]` is `Active`, unless `lock \notin {0, t}` (another running transaction locked it after `RollbackUnlock`) or `h_removers[p] \ {t} /= {}`; and, as a state invariant, no part of `h_creating[t]` is ever in the visible-parts set of a read by a transaction other than `t` | `RollbackRestore` skipped (`Base`) |
| `CommittedMutationApplied` | action property on `CommitCreateCSN(t)` with outcome `Ok` or `LostAfter`: for every `m \in h_mutations[t]` that is neither `Killed` nor has `fail_reason /= None`, every part `p` of the mutated partition that is a source of `m` (its payload version is below `m`'s version, so `m`'s own results and later results are excluded) and satisfies `OracleVisible(p, h_snapshot[t], t)` in the pre-state is in `h_removing[t]`, and a result of `m` covering `p` is in `h_creating[t]`; stated over history and the oracle, not over `MutationDone`, so a wrong completion predicate in `CommitBefore` is caught | `MutationDone` in `CommitBefore` reversed to hold when some source is unmutated (`Mutation`) |

### Snapshot isolation {#invariants-isolation}

Stated for transactions whose snapshot is not `EverythingVisibleCSN`.

| Property | Statement | Witness (scenario) |
|---|---|---|
| `StableRead` | the first and last read of `t` differ only by fragments of parts in `h_creating[t]` or `h_removing[t]` at the time of the last read | `SelectCheck` uses `latest_snapshot` instead of `txn.snapshot` (`Base`) |
| `ReadYourWrites` | after `InsertCommit(t, p)` every later read of `t` contains `p`'s fragments until a `DropOutdate(t)` covering `p`; after `DropOutdate(t)` no later read of `t` contains the dropped fragments (the removal clause precedes the own-creation clause in `isVisible`, so own removal wins) | the `creation_tid = current_tid` clause removed from `isVisible` (`Base`) |
| `NoUncommittedRead` | no fragment in a read of `t` comes from a part whose `creation_tid` is not in `h_committed`, not `t` and not `NonTransactionalTID` | `SelectCheck` treats `creation_csn = 0` as `creation_csn = snapshot` (`Base`) |
| `NoDoubleRead` | a read never contains two versions of the same fragment | `SelectCheck` ignores `removal_csn` and `removal_tid` (`Merge`) |
| `NoLostRead` | every part `p` with `OracleVisible(p, snapshot, t)` is in the visible-parts set of the read, itself or through a covering part that is also oracle-visible | `SelectCapture` skips `Outdated` parts (`Base`, through a transactional `DROP` in flight) |
| `NoLostVisibleData` | action property: for every step that is not an action of `t` itself, and for every running `t`, the (fragment, version) set visible at `txn[t].snapshot` in the post-state contains every element of `h_content[t]` that is not a fragment of a part in `h_removing[t]` (own drops are allowed to remove content; `SetSnapshot(t)` recaptures `h_content[t]`) | `CleanupGrab` uses `latest_snapshot` instead of `getOldestSnapshot` (`Merge`) |

### Safety of part removal and log truncation {#invariants-cleanup}

| Property | Statement | Witness (scenario) |
|---|---|---|
| `NoPrematureDelete` | a part enters `Deleting` only if it is not visible at `txn[u].snapshot` for any running `u` (the actual snapshot, not the protected one) | `CleanupGrab` uses `latest_snapshot` instead of `getOldestSnapshot` (`Merge`); the baseline `SetSnapshot` scenario is expected to violate this property on the code as it is, which is a finding, not a witness |
| `NoResurrection` | after every `RestartLoadPart(p)`, `p` is not `Active` if its durable metadata has a committed removal or a non-transactional removal, or if its `creation_tid` is transactional and not in `h_committed` | `updateCSNIfNeeded` returns `UnknownCSN` instead of `RolledBackCSN` for a tid absent from the log (`Crash`) |
| `LogEntryNeeded` | an entry `csn -> t` leaves `zk_log` only if no durable part metadata and no durable mutation record mentions `t` without the corresponding CSN | the `async_loading_jobs` gate removed (`Crash`) |
| `NoOutdatedLookup` | `assertTIDIsNotOutdated` never throws | `UpdRemoveOldEntriesSetTail` uses `latest_snapshot` instead of `getOldestSnapshot` (`Keeper`, which enables truncation and unknown-state finalization) |
| `MutationNotResurrected` | for `m \in loaded_mutations` after a restart, `m` is absent if its tid is transactional and not in `h_committed` | `RestartLoadMutation` keeps an entry whose tid has no log entry (`MutationCrash`) |
| `MutationRecovered` | for `m \in loaded_mutations` after a restart with `tid \in h_committed`: `m` is present with `csn = h_csn[tid]`, unless `tid \in h_truncated` and the durable record of `m` has no `csn` (the unsynced `writeCSN` append was lost after truncation); that named exception is the expected finding, checked separately by `MutationRecoveredStrict`, which drops the exception and is expected red on the baseline | `RestartLoadMutation` writes `csn := 0` instead of the log's value (`MutationCrash`) |
| `NoFalseCorruption` | `CleanupValidate(p)` fails only if `mem` and the stored record disagree on a field that history says cannot be transient: a differing `creation_tid` or `removal_tid`, a stored CSN that differs from a non-zero memory CSN other than through the transient shapes listed in the action, or a missing record for a part that is not in the `DummyTID` shape and whose directory exists; under this property the code's false `CORRUPTED_DATA` (`3cc9b0936320`) and `CANNOT_OPEN_FILE` (`271f99510f1a`) are violations | the "memory has CSN, disk has `UnknownCSN`" exemption removed from `CleanupValidate` (`Merge`) |
| `RegisteredMutationHasDurableFile` | `mstate = Registered` implies the mutation file exists in the `cached` layer, until `Killed` or `RestartLoadMutation` decides otherwise | `MutRegister` moves ownership without clearing the source, so `MutDestroyOwner` deletes the registered file (`Mutation`) |
| `NoUnattachedMutationAfterFailure` | for `m \in h_prepared_files`, the mutation file is absent once the failing query has ended; and after a restart no mutation in `loaded_mutations` has a tid whose transaction never attached it (`m \notin h_mutations[tid]`) unless the tid is non-transactional and `m \notin h_prepared_files` | `MutDestroyOwner` does not remove the file (`QueryFault`, then `MutationCrash` for the restart clause) |
| `LegacyLoads` | a part whose stored record is `Legacy` loads as `Active` with `creation_tid = NonTransactionalTID` | `RestartLoadPart` treats `Legacy` as a parse failure (`Crash` with `LEGACY_PARTS`) |

### Write-write conflicts {#invariants-conflicts}

| Property | Statement | Witness (scenario) |
|---|---|---|
| `SingleRemover` | `Cardinality(h_removers[p]) <= 1` | the CAS in `DropEnrol` replaced by an unconditional write of `lock`, so a second transaction overwrites a committed remover's lock and stores its own `removal_tid`; the `chassert` in `setAndStoreRemovalTID` that the previous tid is empty is an invariant in the model, not a refusal, and is not checked in a witness run (`Base`) |
| `LockConsistent` | `lock = t` transactional implies `mem.removal_tid \in {Empty, t}`; `lock = NonTransactionalTID` implies `mem.removal_tid \in {Empty, NonTransactionalTID}`; `lock = 0` implies `mem.removal_tid = Empty` or `mem.removal_csn /= 0` | `RollbackUnlock` unlocks before clearing `removal_tid` (`Base`) |
| `MutationOnVisible` | every selected `(m, p, visible)` in `h_selected` satisfies `OracleVisible(p, start_csn(tid(m)), tid(m))` (for a non-transactional `m`: `OracleVisible(p, MaxCommittedCSN, EmptyTID)`), evaluated in the state of the selection; the recorded `visible` flag is kept for diagnosis only, so a wrong `isVisible` that returns `TRUE` is caught | `MutSelect` selects and records a candidate regardless of the visibility result (`Mutation`) |
| `MutationContent` | state invariant on payloads: a part created by `MutFinish(m, p)` carries every fragment of `p` at `ver + 1` (or the tombstone), a part created by `MergeFinish` carries the union of its sources' payloads unchanged; reads are covered by `NoDoubleRead` and `NoLostRead` on top of this | `MutFinish` publishes `ver` instead of `ver + 1` (`Mutation`) |
| `NoOrphanMutation` | once `h_rolled_back[t]` holds and every `MutRegister` of `t` has completed or failed, no mutation of `t` is `Registered` or `Selected` (a rollback in progress may still see registered mutations, because `killMutation` runs after the CAS to `RolledBackCSN`) | the re-check in `MutRegister` removed (`Mutation`, with `KillTransaction` between `MutPrepareAttach` and `MutRegister`, so that `RollbackKill*` finds nothing and `MutRegister` registers afterwards) |
| `NtBatchDone` | action property on `NtBatchEnd` with outcome `Done`: every target of `h_batch` that was not already removed has `removal_tid = NonTransactionalTID` and `removal_csn = NonTransactionalCSN` in its stored record (durable in `Layered` mode), `lock = 0`, and `NonTransactionalTID \in h_removers[p]` | `NtBatchStore` skipped for covered parts of `NtMerge*` (`NonTxnCrash`) |
| `NtBatchRefusedUnchanged` | action property on the `Refuse` that ends a batch (`h_batch_outcome := Refused`): every target's `mem`, stored record and `lock` equal their values recorded in `h_batch` at `NtBatchStart` | `NtBatchStore` runs per part before all `NtBatchLock` steps are done (`NonTxn`) |
| `NtRefusalJustified` | a batch is refused only if some target `p` has `creation_tid(p)` transactional and `\notin h_committed`, or `lock(p) \notin {0, NonTransactionalTID}` at the refusing step | `NtBatchPreflight` decides from `mem.creation_csn = 0` alone, without `tid_to_csn` (`NonTxn`) |

### Assertions from the code {#invariants-code}

Every `chassert` and `LOGICAL_ERROR` on the modelled paths, transcribed as the code has them. Each is an invariant
`Assert_<name>`; its witness is the removal of the guard that the code relies on, named in the last column.

| Assertion | Statement | Witness (scenario) |
|---|---|---|
| `validateInfo`, running creator | if a transaction with `creation_tid` is running and `creation_csn \notin {0, RolledBackCSN}` and the transaction's `csn` is not `CommittingCSN`, then `creation_csn` equals the transaction's `csn` | `CommitStoreCreation` writes `h_csn + 1` (`Base`) |
| `validateInfo`, no creation CSN | `creation_csn = 0` implies `removal_csn = 0` and `removal_tid \in {Empty, creation_tid}` | two changes: `NtBatchPreflight` skips the creator refusal and `StoreRead` skips the `creation_in_flight` refusal of `setAndStoreRemovalTID`, so a non-transactional removal stores `removal_csn = NonTransactionalCSN` on a part with `creation_csn = 0` (`NonTxn`) |
| `validateInfo`, order | `creation_csn /= 0` implies (`removal_csn = 0` or `removal_csn = NonTransactionalCSN` or `creation_csn <= removal_csn`) and (`creation_tid` non-transactional or `creation_tid.start_csn <= creation_csn`) | `CommitStoreCreation` writes `CSN_MAX` instead of `h_csn`, so a later committed removal by another transaction has a smaller CSN (`Base`) |
| `validateInfo`, removal | `removal_csn /= 0` implies `removal_tid /= Empty` and `removal_tid.start_csn <= removal_csn` | `DropStore` skipped, so the lock is held in memory only, so `CommitStoreRemoval` later stores `removal_csn` with `removal_tid = Empty` (`Base`) |
| `validateInfo`, exempt shape | the `DummyTID`/`RolledBackCSN`/empty-removal shape produced by `loadMetadata` case 2 is skipped | not a property, a definition |
| `isVisible`, fast path | `removal_csn /= 0` implies `creation_csn /= 0`; both CSNs are `0`, `NonTransactionalCSN` or above `MaxReservedCSN` | two changes: `CommitStoreCreation` skipped for a part both created and removed by `t`, and `StoreRead` skips `validateInfo`, so `CommitStoreRemoval` publishes `creation_csn = 0, removal_csn /= 0` before `UpdLoadNewEntries` can repair it (`Base`) |
| `isVisible`, slow path | on entry to the slow path at least one CSN is `0`, and `current_tid` is neither the creator nor the remover | the `creation_tid = current_tid` clause removed (`Base`) |
| `getOldestSnapshot` | `snapshots_in_use` sorted, same size as `running_list` | `SetSnapshot` also rewrites `protected_snapshot` (`SetSnapshot`) |
| `NoUnknownMutationCSN` | `MutSelect` never meets a transactional mutation with no running transaction and `csn = 0` | the re-check in `MutRegister` removed, so an orphaned registered mutation reaches `MutSelect` after its transaction is gone (`Mutation`) |
| `Assert_IsNonTransactionalDomain` | action property on every step that evaluates `isNonTransactional(tid)` (`RestartLoadPart`, `validateInfo` inside `StoreRead`, `NtBatchPreflight`): the argument is never `DummyTID` unless the exempt-shape check has already accepted the record in that step | the exempt-shape check in `RestartLoadPart` moved after the `isNonTransactional` call (`Crash`, on a tmp-only directory) |
| `FlipAfterStores` | action property on `CommitFlip(t)`: in its pre-state every part of `h_creating[t]` has `mem.creation_csn = h_csn[t]`, every part of `h_removing[t]` has `mem.removal_csn = h_csn[t]`, and every `m \in h_mutations[t]` not killed has `csn = h_csn[t]`; this is the contract of `waitStateChange` that `afterCommit` documents | `CommitFlip` moved before the store loops (`Base`) |
| `NoSpuriousStaleVersion` | a frame reaches `STALE_VERSION` only if `interfered` was set on each of its `MAX_STORE_RETRIES` attempts; a `TOO_OLD_VERSION` outcome with `interfered = FALSE` is a defect of the reload logic (`StoreRead` on a retry must call `loadMetadata`, not `getInfo`) | `StoreRead` on a retry uses `getInfo` instead of `loadMetadata` (`Base`) |
| `NoProcessDown` (scenarios without disk faults) | `down_cause = None` in every state | `Fail` allowed inside `afterCommit`, taking the server down with `down_cause = Other` (`Base`) |
| `DownOnlyByNoexceptFault` (scenarios with disk faults) | `down_cause /= Other` in every state | `Fail` allowed inside `afterCommit` (`DiskFault`) |

The assertion in `preparePartForRemoval` (an `Outdated` part with a transactional creation has a `removal_tid`) is
not listed: on the modelled paths a part becomes `Outdated` at load only through the covered-part path, which
stores a removal itself, or through `removal_csn /= 0` or `RolledBackCSN`, both of which imply the required shape,
so no witness can falsify it. It is kept as a comment in `Server.tla` next to `RestartLoadPart`, not as a property.

### Liveness {#invariants-liveness}

Only in the `Live` and `MutationChain` scenarios, with weak fairness on the updating thread, the cleanup thread,
the mutation executor, `MutWait` and the client's `CommitAck`, and stated only for transactions begun while
`Begin` was enabled:

| Property | Statement | Witness (scenario) |
|---|---|---|
| `CommitResolves` | a transaction in `Committing` or in `unknown_state_list` is eventually `Committed` or `RolledBack` | `UpdSwapUnknownLists` never moves entries to the loaded list (`Live`) |
| `OutdatedEventuallyDeleted` | an `Outdated` part with a committed removal is eventually `Deleted`, provided no transaction runs forever | `CleanupGrab` requires `pins /= {}` (`Live`) |
| `ChainReported` | a transactional mutation `m` that depends on an earlier mutation of the same transaction with a non-transactional mutation between them eventually has `fail_reason = Deadlock` (set by `MutWait`), so `waitForMutation` returns instead of waiting forever | the deadlock check removed from `MutWait` (`MutationChain`) |
| `ClientCommitEventuallyReturns` | with `WAIT_UNKNOWN`, a client blocked in `waitStateChange` eventually receives `Acked` or `Error`; the client is enabled only by `csn_notified`, so a state change without `notify_all` (upstream `f6ad379c8301`) blocks it forever | `CommitFlip` and `RollbackStart` change `csn` without setting `csn_notified` (`Live`) |

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
| `Merge` | + `M12` | `Base` + `Merge*` + `Cleanup*` + `Updater+GC` | none | `NoDoubleRead`, `NoPrematureDelete`, `Atomicity` |
| `Mutation` | + `P1m`, `P2m` | `Base` + `MutPrepare*`, `MutRegister`, `Mut*`, `MutWait`, `Kill*`, `KillMutation` | none | `MutationOnVisible`, `MutationContent`, `NoOrphanMutation`, `NoUnknownMutationCSN`, `RegisteredMutationHasDurableFile`, `NoProcessDown` (the `KILL MUTATION` during commit window) |
| `MutationChain` | `P1`, `P1m` | one session, three mutation entries txn, non-txn, txn, `Updater` | none | `ChainReported` |
| `NonTxn` | + `E` | `Base` + `NtInsert`, `NtBatch*`, `NtDropCover` + `Cleanup*` | none | `LockConsistent`, `SingleRemover`, `NtBatchRefusedUnchanged`, `NtRefusalJustified`, durability under mixed load |
| `NonTxnCrash` | `P1`, `P2`, `M12`, `E` | `NonTxn` + `NtMerge*` + `Restart*`, `Layered` disk | restart | `NtBatchDone` on the durable layer, `NoResurrection` of covered parts after a non-transactional merge or drop |
| `MergeMutation` | `P1`, `P2`, `M12`, `P1m` | `Base` + `Merge*` + `MutPrepare*`, `MutRegister`, `Mut*` | none | reservations and the parts lock between a merge and a mutation of the same source; `NoDoubleRead`, `MutationContent`, `CommittedMutationApplied` |
| `MutationCleanup` | `P1`, `P1m` | `Base` + `MutPrepare*`, `MutRegister`, `Mut*` + `Cleanup*` + `Updater+GC` | none | pins held by the mutation executor against cleanup; `NoPrematureDelete`, `NoLostVisibleData` |
| `SnapshotCrash` | `P1`, `P2` | `SetSnapshot` scenario + `Restart*`, `Layered` disk | restart | a snapshot set below `tail_ptr` across a restart; `NoOutdatedLookup`, `NoResurrection` |
| `Implicit` | `P1`, `P2` | the implicit wrapper, `Updater` | query | acknowledgement ordering |
| `Keeper` | + `M12` | `Base` + `Merge*` + `Updater+GC` + `Updater+Unknown` | Keeper, both wait modes | `UnknownResolvesByLog`, `NoOutdatedLookup`, the two-list race |
| `Crash` | + `M12` | `Base` + `Merge*` + `Cleanup*` + `Updater+GC` + `Restart*`, `Layered` disk | restart, both `FSYNC_PART_DIRECTORY` values, `LEGACY_PARTS` on and off | `NoResurrection`, `LogEntryNeeded`, `AckedWriteIsDurable` across restart, `LegacyLoads`, `Assert_IsNonTransactionalDomain`, `NoFalseCorruption` after restart |
| `MutationCrash` | `P1`, `P1m` | one session, `Begin`, `Insert*`, `MutPrepare*`, `MutRegister`, `Mut*`, `Commit*`, `Rollback*`, `Updater+GC`, `Restart*`, `Layered` disk | restart | `MutationNotResurrected`, `LogEntryNeeded` for mutation records, the written-but-unattached window; `MutationRecovered` run separately as the expected finding on the unsynced `writeCSN` append |
| `DiskFault` | + `M12`, `P1m` | `Base` + `Merge*` + `MutPrepare*`, `MutRegister`, `Mut*`, `KillMutation` + `Restart*`, `Layered` disk, both `NOEXCEPT_STORE_FAULT_POLICY` values | disk write | `Terminate`: `DownOnlyByNoexceptFault`, recovery after `ProcessDown`; `Retry`: every safety property, `LogEntryNeeded` under the retry window, tolerance of `KillMutation` during the retry |
| `QueryFault` | + `P1m` | `Base` + `MutPrepare*`, `MutRegister`, `Mut*` | query | rollback between steps, the orphan-file window, `NoUnattachedMutationAfterFailure` |
| `Live` | `P1`, `P2` | `Base` + `Cleanup*` + `Updater+GC` + `Updater+Unknown` | Keeper | `CommitResolves`, `OutdatedEventuallyDeleted` |

An `All` scenario is not planned until the narrow ones have measured run times; the README records for every run
the date, commit, states, distinct states, wall-clock time and result, and the matrix is adjusted from those
numbers. `TID_MAX` starts at 3 and `Sessions` at 2; a scenario that does not finish in one hour has its bounds
reduced only under the bound contract above, that is, while every witness it checks stays red, and the README
records the reduction. The pairwise rows exist because subsystems that share part metadata, locks or
transaction-log state can hide a defect from each other's narrow scenario; each pairwise row is bounded to the
smallest universe that lets both subsystems act on one part.

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
5. `Nt*`; the `NonTxn` scenario.
6. Keeper faults and the rest of the updating thread; the `Keeper` scenario, both wait modes.
7. `Crash`, `Restart*` with the coverage tree and asynchronous outdated loading; the `Crash` and `SnapshotCrash`
   scenarios, both `FSYNC_PART_DIRECTORY` values.
8. `MutPrepare*`, `MutRegister`, `Mut*`, `MutWait`, `Kill*`; the `Mutation`, `MutationChain`, `MutationCrash`,
   `MergeMutation`, `MutationCleanup` and `QueryFault` scenarios.
9. Disk write faults and `ProcessDown`; the `DiskFault` scenario.
10. `Implicit` and `Live`.
11. Code-map review, known-trace check, the code-sabotage table of the README (the fifteen hypothetical C++ bugs
    from the fifth review, each with the property and scenario that catches it, re-verified by injecting the bug
    into the model's corresponding action), the calibration table re-run (each of the 17 in-scope upstream
    fixes injected as its pre-fix behaviour into the model, the named property must go red), README with
    measured numbers.

Each step ends with a TLC run of the scenario and of every witness it enables, with the results recorded before
the next step starts. A counterexample is not "fixed" in the model until the fidelity checks above have shown that
the model, not the code, is wrong.

## Calibration against upstream fixes {#calibration}

The 29 upstream commits of the last two years that changed the behaviour of the transaction sources were read
(patches in `tmp/txn_history/`); 23 fix a defect. The table states, for each, the property and scenario that
report the pre-fix behaviour when it is substituted into the model. Revision 6 caught 4 of the 17 in-scope
defects; revision 7 closes the other 13 with the classes named in its header. The table is re-run at the end of
the development order by injecting each pre-fix behaviour into the model.

| Upstream fix | Pre-fix defect in model terms | Property (scenario) |
|---|---|---|
| `ba2ee3239b8d` | non-transactional cover left a covered part's removal in memory only | `NtBatchDone` (`NonTxnCrash`) |
| `349c46105730` | `tail_ptr` advanced while tables were still loading | `LogEntryNeeded` (`Crash`) |
| `5bd1f4a50909` | remover stored `removal_csn` before the creator's `creation_csn` | `Assert_isVisible_fast_path` (`Base`) |
| `09d4267611b3` | `removeOldPart` stored `removal_tid` after releasing the transaction mutex, racing rollback | `LockConsistent` (`Base`, with `DropStore` outside `txn.mutex`) |
| `3cc9b0936320` | `hasValidMetadata` rejected memory CSN ahead of disk | `NoFalseCorruption` (`Merge`) |
| `aea1c111e0a8` | legacy `txn_version.txt` rejected at load | `LegacyLoads` (`Crash`) |
| `f5f4635154a0` | `isNonTransactional` asserted on `DummyTID` at load | `Assert_IsNonTransactionalDomain` (`Crash`) |
| `271f99510f1a` | `hasValidMetadata` threw on a tmp-only part, cleanup retried forever | `NoFalseCorruption`, `OutdatedEventuallyDeleted` (`Crash`, `Live`) |
| `50fa1cb2772f` | `csn.exchange` before the CSN stores, waiter woke early | `FlipAfterStores` (`Base`) |
| `6e5114d9739c` | non-transactional removal stamped a part with an uncommitted creation | `validateInfo`, no creation CSN (`NonTxn`) |
| `2903f6d48693` | failed preparation left a mutation file that restart registered | `NoUnattachedMutationAfterFailure` (`QueryFault`, `MutationCrash`) |
| `8e5e5a69150e` | moved-from entry's destructor deleted the registered file | `RegisteredMutationHasDurableFile` (`Mutation`) |
| `ab40e11d3c73`, `f8f46fb1eb14`, `86b6861a1a8e` | batch refused after earlier members were stamped | `NtBatchRefusedUnchanged` (`NonTxn`) |
| `f6ad379c8301` | `csn` changed without `notify_all`, client blocked forever | `ClientCommitEventuallyReturns` (`Live`) |
| `65e4e2b5bf69` | refusal trusted `creation_csn = 0` although the log had the commit | `NtRefusalJustified` (`NonTxn`) |
| Altinity PR 2396, part 1 | store fault inside `noexcept` terminated a committed transaction's server | conformant under `Terminate`; the fix is validated under `Retry` (`DiskFault`) |
| Altinity PR 2396, part 2 | `KILL MUTATION` in the commit window, `LOGICAL_ERROR` inside `noexcept` | `NoProcessDown` (`Mutation`) |
| `d71f8329a129`, `c309b745aaf0`, `56958de54fba`, `2e070c30cc07`, `d34ccec49297`, `319e693e72f5` | relaxed loads, malformed tid, `ALTER RENAME`, `ATTACH AS REPLICATED`, corrupt tmp sidecar, `REPLACE`/`MOVE PARTITION` | out of scope, listed under accepted risks |

## Accepted risks {#accepted-risks}

Each exclusion below is a place where a defect could hide from the model. They are listed so that the exclusion is
a decision, not an omission, and each names what would reopen it.

- Two-table transactions are out of version 1 by the scope decision; the per-storage loops in `afterCommit` and
  `rollback` are therefore unchecked. Reopened by the first item of version 1.1.
- Weak-memory reads are not modelled; TLA+ is sequentially consistent. A bug that needs a relaxed load to observe
  a stale `creation_csn` while `tid_to_csn` already has the entry is invisible. Reopened by the stale-read action
  in the future extensions, if the visibility path ever regains relaxed loads on a decision.
- A fragment's payload is one version counter or a tombstone. The modelled mutations are whole-partition
  `UPDATE` and `DELETE`, for which "the right rows with the right version" and "version incremented" coincide; a
  mutation with a predicate, or a merge that reorders rows, would need a row-level payload. Reopened by adding
  two symbolic rows per fragment when partial mutations are modelled.
- `MAX_STORE_RETRIES` is 2 in the model and 20 in the code. A defect that needs three or more interferences on
  one part is out of reach; `NoSpuriousStaleVersion` covers the retry logic itself.
- The `Live` scenarios use weak fairness on background actors; a starvation that needs strong fairness to
  express (an action enabled infinitely often but never continuously) is not checked.
- The mutation record on disk is a set of fields, not a sequence of lines. A retried `writeCSN` on the append-only
  format of the baseline would produce two `csn:` lines and a parse failure at load; the model cannot see that,
  which matters for the `Retry` policy (Altinity PR 2396 rewrites the record through a temporary file for that
  reason). Reopened by modelling `mdisk[m]` as a sequence of appended records when append-based formats are
  under study.
- The `detached/` directory, `KILL QUERY`, the other partition operations, backups and `SYSTEM` commands are
  out of scope as listed; each is an action set the model does not have, not an abstraction of one it has. Of
  the 23 upstream fixes in the calibration table, 6 fall here (`ATTACH AS REPLICATED`, `REPLACE`/`MOVE
  PARTITION`, `ALTER RENAME COLUMN`, a corrupt tmp sidecar, a malformed tid, relaxed loads).

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
