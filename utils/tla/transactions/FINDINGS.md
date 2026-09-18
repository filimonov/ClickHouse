# Findings of the TLA+ model of MergeTree transactions {#findings}

What the model found, and what happened to each finding. Three kinds are recorded here and nowhere else:
counterexamples TLC produced on the baseline scenarios, defects in the model itself that were noticed while
building it, and rows of the design document that the model contradicts.

A finding is classified `code`, `property` or `model`.

- `code` — the counterexample is a behaviour the C++ really has. The entry carries the counterexample, a fix
  applicable to upstream `master`, naming file and function and what changes, and the model variant that encodes
  the fix so the property can be rechecked against it.
- `property` — the C++ produces exactly what the trace shows, and the property misstated what it meant to
  forbid. The entry says what was wrong and what the property now says. A property is never weakened to make a
  red go away; if the design document's row was the thing that was wrong, section 3 records that too.
- `model` — the model let something happen that the code refuses. The entry names the C++ guard the model was
  missing.

Baseline C++ throughout is upstream `master` `2c24b6b9291e`, checked out in this worktree.

## 1. Counterexamples on the baseline {#counterexamples}

| Id | Scenario, bounds | Property | Action sequence (short) | Classification | Resolution | Proposed code fix |
|---|---|---|---|---|---|---|
| F3 | `Merge`: two sessions, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 3`, `CSN_MAX = 36` | `NoPrematureDelete` | `t1` inserts `P1` and `P2` and commits; the merge task begins `t2` and is killed while it is publishing `M12`; the task's statement rollback stamps `RolledBackCSN` on `M12` and outdates it; `CleanupGrab` takes `M12` while `t2`, rolled back but not yet finalized, is still in `running_list` | `property` | the step quantifies over the transactions that can still issue a read, not over every entry of `running_list`. The same trace exposed model defect `M9`, the merge task's result part not being pinned, which is fixed in the same change | none, the code is right |
| F2 | `SetSnapshotF2`: one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`, `SNAPSHOT_TARGETS = {34}` | `NoPrematureDelete` | `t1` inserts `P1` and commits at CSN 34; `t2` takes snapshot 34, drops `P1` and commits at CSN 35; `t3` begins at snapshot 35 and `SET TRANSACTION SNAPSHOT 34` lowers its read snapshot without moving its `snapshots_in_use` entry; `CleanupGrab` moves `P1` to `Deleting` while `t3` can still read it | `code` | the model variant `SET_SNAPSHOT_PROTECTS` encodes the proposed fix and `MC_SetSnapshotF2Fixed` runs the same configuration green | split `getOldestSnapshot` into a locked entry point and an unlocked body, give `TransactionLog` a `setSnapshotForRunningTransaction` that moves the entry under `running_list_mutex` and refuses a target below `tail_ptr`, and hold that mutex in `removeOldEntries` from the oldest-snapshot read through the `tail_ptr` store; see below |
| F1 | `Base` at `TID_MAX = 3`, `CSN_MAX = 38`, the first run at the transaction count the scenario matrix asks for | `Atomicity` | `t1` inserts `P1` and commits at CSN 34; `t2` takes snapshot 34, drops `P1`, inserts `P2`, and `CommitCreateCSN` writes CSN 35 into the log, which the updater loads while `t2` is still storing; `t3` takes snapshot 35, locks and stores its own removal of `P2`; `t3`'s `SELECT` then reads `{}` | `property` | `AtomicityStep` now excludes the reader's own removals, `h.removing[t]`, from `C`. The design document's row states only the writer's half of the same precedence, recorded in section 3 as S1 | none, the code is right |

### F3 in full {#f3}

Found on the first run of the `Merge` scenario, at 8.9 million distinct states, on a 36-state behaviour.
Classification `property`.

**The state in which the grab happened.** `tlog.running_list = {1, 2}`, `txn[1]` is a session's transaction in
`CommitStoreCreation`, and `txn[2]` is the merge's, `state = "RolledBack"` with `rb_driver` the killing session
and `pc = "RollbackCopyLists"`: the kill has won the compare-and-exchange and the rollback body has not run yet.
`h.creator[M12] = 2`, `M12` is `Outdated` with `mem.ccsn = RolledBackCSN`, no pins and no frames, so
`CanBeRemovedImpl` is true on its first clause and `CleanupGrab` fires. `NoPrematureDeleteStep` then asks
`~OracleVisible(M12, txn[2].snapshot, 2)` and gets `FALSE`, because the oracle's first clause is that a
transaction sees what it created and the creator is `2`.

**Why the code is right.** A transaction whose state is `RolledBack` can no longer *start* a read. The next
statement on it is refused by `executeQuery` with "Cannot execute query because current transaction failed.
Expecting ROLLBACK statement", which the model has as `QueryOnCancelled`, and `SelectCapture` carries the same
`Running` guard. The part is not visible to it either: `MergeTreeData::Transaction::rollback`
(`src/Storages/MergeTree/MergeTreeData.cpp:11126`) has already stamped `RolledBackCSN` on it, and
`VersionInfo::isVisible` (`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:167`) returns false on
`snapshot_version < creation_csn` for every snapshot, the creator's own included, because `RolledBackCSN` is
above every real CSN. So there is no read the grab can spoil, and the grab is correct.

**What the property now says, and what it gives up.** `NoPrematureDeleteStep` quantifies over
`u \in tlog.running_list` with `txn[u].state = "Running"`.

The one thing this must not do is drop a read that is already **in flight** when the kill lands. `SelectCheck`
and `SelectFinish` carry no state guard, deliberately: a `SELECT` that has passed `SelectCapture` runs to
completion whatever happens to its transaction meanwhile, which is what the server does. Such a read is not
covered by this property before the narrowing either, and it is covered by a different one. `SelectCapture` pins
every part it captures with `<<"Select", k>>` and releases them only at `SelectFinish` or `Refuse`, and
`CleanupGrab` requires `part[p].pins = {}`, which is `isSharedPtrUnique` (`MergeTreeData.cpp:4150`) and is
exactly what the spec's `PinnedNotDeleted` row states. The in-flight read is protected by the pin, checked by
`PinnedNotDeleted`, and red in both `SetSnapshotFixed` and `Merge`. So the narrowing gives up a case that was
never this property's and that another property holds.

What is left is fidelity rather than a weakening, for the reason the sibling properties already show: every
other property that consults the oracle guards its antecedent on `txn[t].state = "Running"` — `NoFutureRead`,
`NoLostRead`, `Atomicity` and `NoLostVisibleData` all do, and `NoPrematureDelete` was the only one that did not.
A transaction in `running_list` still holds `getOldestSnapshot` back whatever its state, so nothing about what
the C++ protects changes. The witness stays red, in `SetSnapshotF2Fixed` and in `Merge`.

**The model defect the same trace exposed.** `MergeWrite` did not pin the result part. The merge task holds it
from the moment it is written, through `merge_task` and then through
`MergePlainMergeTreeTask::new_part` (`src/Storages/MergeTree/MergePlainMergeTreeTask.cpp:156`), until the task
object is destroyed, and that is exactly what `isSharedPtrUnique` (`MergeTreeData.cpp:4150`) tests in
`grabOldParts`. Without the pin the cleanup thread can take a result the statement rollback has just outdated
while the task that produced it is still alive. `MergeWrite` now adds `Tsk(i)` to the result's pins and
`MergeCommitFinalize` and `MergeUnwind` remove it, which are the two steps at which the task is destroyed. That
fix alone does not settle the counterexample, because the same grab is enabled again once the task is gone and
the rolled-back transaction is still in `running_list`; both changes were needed.

### F2 in full {#f2}

**Trace**: `traces/f2-set-snapshot-premature-delete.txt`, 45 states, found after 170,881 distinct states in
three seconds. That count is a first-violation count and is not reproducible; re-runs gave 177,195 and 167,361
on the same trace. Scenario `SetSnapshotF2`, classification `code`.

**Action sequence.** `Begin`, `InsertWrite`, the three store steps, `InsertPreActive`, `PublishStart`,
`PublishFlip`, `CommitBefore`, `CommitCreateCSN`, `CommitStoreCreation` with its store steps, `CommitFlip`,
`CommitFinalize`, `UpdLoadEntriesMap`, `UpdPublishSnapshot`, `CommitAck` give `t1` a committed `P1` at CSN 34
and leave `tlog.latest_snapshot` at 34. `Begin`, `DropStart`, `DropLock`, `DropEnrol` with its store steps,
`DropStore`, `DropOutdate`, `CommitBefore`, `CommitCreateCSN`, `CommitStoreRemoval` with its store steps,
`CommitFlip`, `CommitFinalize`, `UpdLoadEntriesMap`, `UpdPublishSnapshot`, `CommitAck` give `t2` a committed
removal at CSN 35 and leave `P1` `Outdated`, unpinned and `latest_snapshot` at 35. Then `Begin` starts `t3` at
snapshot 35, `SetSnapshot` lowers it to 34, and `CleanupGrab` fires.

**The state in which the grab happened.** `txn[3].snapshot = 34`, `txn[3].protected_snapshot = 35`,
`tlog.snapshots_in_use[3] = 35` and `tlog.running_list = {3}`, so `OldestSnapshot` is 35. `P1` carries
`mem = [ctid |-> 1, ccsn |-> 34, rtid |-> 2, rcsn |-> 35]`, is `Outdated` with `pins = {}` and no frames.
`CanBeRemovedImpl(P1)` is therefore true on the last clause, `rcsn <= OldestSnapshot`, that is `35 <= 35`, while
`OracleVisible(P1, 34, 3)` is true: the creator committed at 34, which is at or below 34, and the only remover
committed at 35, which is above it. The part `t3` is about to read is the part the cleanup thread just took.

**The C++ call sequence.** `InterpreterTransactionControlQuery::executeSetSnapshot`
(`src/Interpreters/InterpreterTransactionControlQuery.cpp:138`) calls `MergeTreeTransaction::setSnapshot`
(`src/Interpreters/MergeTreeTransaction.cpp:52`), which writes `snapshot` and nothing else; the background
cleanup task reaches `MergeTreeData::grabOldParts` (`src/Storages/MergeTree/MergeTreeData.cpp:4074`), which asks
`VersionMetadata::canBeRemoved` (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:272`) at `:4140`;
`canBeRemoved` calls `TransactionLog::getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:677`), which
returns `snapshots_in_use.front()`, still the snapshot `beginTransaction` inserted; the part moves to `Deleting`
and `clearPartsFromFilesystemAndRollbackIfError` (`:4566`) deletes its directory.

**Why `MC_SetSnapshot` is green and `MC_SetSnapshotF2` is not.** The scenario that owns
`SET TRANSACTION SNAPSHOT` cannot reach this shape, for two independent reasons, and both are configuration
rather than model.

The first is the number of transactions. The shape needs a part created by one committed transaction, removed
by a second committed one, and a third transaction running with its snapshot lowered between the two commit
CSNs. It cannot be fewer: while the remover is still running the part is pinned by its own `<<"Txn", t>>` entry
and `mem.rcsn` is still `UnknownCSN`, so `CleanupGrab` is disabled twice over, and once the remover has
committed, `OracleVisible` excludes it anyway, because the part is in its own `h.removing`. Three transactions
is above the exhaustive bound `TID_MAX = 2` that model defect `M4` forced on this scenario.

The second is the snapshot target. `SNAPSHOT_TARGETS = {33}` is `FirstCSN`, which is `tlog.latest_snapshot` at
initialisation, while `zk.seq` also starts at `FirstCSN` (`Keeper.tla`) so the first commit takes CSN 34. No
part is ever visible at snapshot 33, and `NoPrematureDelete` is vacuously true there however deep the search
runs: a run at the witness bounds with the target left at 33 was green, and one at those bounds with the target
at 34 was killed at 333 seconds, at 46,726,144 distinct states and depth 43 with a queue of 8.2 million still
growing, without reaching the violation, which lies at depth 45. Breadth is the wrong resource here; the violating behaviour is a single sequential run of
three transactions.

`MC_SetSnapshotF2` is that configuration: one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`,
`SNAPSHOT_TARGETS = {34}`. A second session and a second part add only breadth the violation does not use, and
removing them puts the trace within reach of a three-second run. `MC_SetSnapshotF2Fixed` is the same
configuration with `SET_SNAPSHOT_PROTECTS = TRUE` and is green on `NoPrematureDelete`, `PinnedNotDeleted`,
`NoLostVisibleData` and `NoFalseCorruption` over 367,183 distinct states. `NoFalseCorruption` is vacuously
green there: no `CleanupDeleteFail` step is reachable in this plan, which is what model defect `M7` and the
deferred witness record. `MC_SetSnapshot` and
`MC_SetSnapshotFixed` keep their bounds and their target: 33 is what the `Assert_getOldestSnapshot` and
`Assert_TailPtrNotRegressing` witnesses need, and neither of those needs a visible part.

**The mechanism.** `InterpreterTransactionControlQuery::executeSetSnapshot`
(`src/Interpreters/InterpreterTransactionControlQuery.cpp:138`) validates the requested CSN and calls
`MergeTreeTransaction::setSnapshot` (`src/Interpreters/MergeTreeTransaction.cpp:52`), whose whole body is

```cpp
void MergeTreeTransaction::setSnapshot(CSN new_snapshot)
{
    snapshot.store(new_snapshot, std::memory_order_relaxed);
}
```

The transaction holds `snapshot_in_use_it`, an iterator into `TransactionLog::snapshots_in_use`, set once by
`beginTransaction`. `setSnapshot` does not touch it, so the list keeps the snapshot the transaction started
with. `TransactionLog::getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:677`) returns
`snapshots_in_use.front()`, which is therefore above the snapshot the transaction actually reads at.

From there the part is lost in three steps. `VersionMetadata::canBeRemoved`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:272`) compares the removal CSN against
`getOldestSnapshot` and answers `true` for a part removed after the lowered snapshot but before the protected
one. `MergeTreeData::grabOldParts` (`src/Storages/MergeTree/MergeTreeData.cpp:4140`) asks exactly that
question, and on `true` moves the part out of `Outdated` and schedules its directory for removal. The running
transaction's next `SELECT` then loses the rows it could read a moment earlier, which is the isolation
guarantee `SET TRANSACTION SNAPSHOT` exists to provide.

`getOldestSnapshot` also feeds `removeOldEntries` (`src/Interpreters/TransactionLog.cpp:308`), so the same
too-high value lets the log entries of that era be truncated while a transaction is still reading at a
snapshot below the new `tail_ptr`.

**Proposed fix, applicable to upstream `master`.** Four functions change, and one member declaration.

`TransactionLog::getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:677`) splits in two. The body, with
the two `chassert`s and the `snapshots_in_use.front()` it returns, becomes a private
`getOldestSnapshotLocked() const TSA_REQUIRES(running_list_mutex)`; `getOldestSnapshot` keeps its signature and
becomes the entry point that takes `std::lock_guard lock{running_list_mutex}` and calls it. The split is what
makes the rest possible: `running_list_mutex` is a plain `std::mutex` (`src/Interpreters/TransactionLog.h:184`)
and the current `getOldestSnapshot` locks it itself, so no caller can hold it across the call.

`TransactionLog::removeOldEntries` (`:284`) takes `running_list_mutex` itself, before it reads the `tail_ptr`
znode at `:311`, and holds it through `tail_ptr.store(new_tail_ptr)` at `:321`, calling `getOldestSnapshotLocked`
in place of `getOldestSnapshot` at `:312`. Its cost is one ZooKeeper `get` and one `set` inside that critical section,
which is the price of making the computation of the new tail and its publication one step.

That price, stated plainly, because it is the fix's one real cost: `running_list_mutex` is held across two
Keeper round trips, and `beginTransaction`, `commitTransaction` and `rollbackTransaction` all take the same
mutex, so each truncation pass stalls them for as long as those two calls take. The pass runs once per
iteration of the updating thread's loop, so this is a periodic stall on the transaction-control path rather
than a per-query one. It introduces no lock-order inversion: the two Keeper calls take no `TransactionLog` lock
of their own, and no path takes `running_list_mutex` while already holding `TransactionLog::mutex`, so the
acquisition order is the one the class already has. The alternative that avoids the stall is the re-read variant
rejected two paragraphs below, which trades it for a window in which `getOldestSnapshot` reports a value below a
tail that has already been published.

`TransactionLog` gains `setSnapshotForRunningTransaction(MergeTreeTransaction & txn, CSN new_snapshot)`, which
takes `running_list_mutex` and, under it, refuses with `INVALID_TRANSACTION` when `new_snapshot < tail_ptr`,
because the log entries needed to resolve the parts of that era may already be gone and
`assertTIDIsNotOutdated` (`:656`) would raise a `LOGICAL_ERROR` on the first lookup that needs one; otherwise it
erases the transaction's `snapshot_in_use_it` from `snapshots_in_use`, re-inserts `new_snapshot` at the position
it sorts to, stores the returned iterator back into `snapshot_in_use_it`, and only then calls
`MergeTreeTransaction::setSnapshot`. Reading `tail_ptr` under `running_list_mutex` is sound only because
`removeOldEntries` now publishes it under the same mutex: without that, a check that passes can find the tail
above its snapshot a moment later, which is exactly the state the refusal exists to prevent.

`InterpreterTransactionControlQuery::executeSetSnapshot`
(`src/Interpreters/InterpreterTransactionControlQuery.cpp:138`) calls that method instead of
`MergeTreeTransaction::setSnapshot`. `setSnapshot` itself does not change; it is now called only from under the
mutex.

`MergeTreeTransaction::snapshot_in_use_it` is declared `const std::list<CSN>::iterator`
(`src/Interpreters/MergeTreeTransaction.h:134`). The fix reassigns it, so the declaration loses the `const`.

The alternative to the second change, having the new method re-read `tail_ptr` after its entry is already in
`snapshots_in_use` at the new value and roll the entry back if the tail moved past it, keeps `removeOldEntries`
as it is but leaves a window in which `getOldestSnapshot` reports a value below a tail that has already been
published, so it is worse.

`getOldestSnapshot` still returns `snapshots_in_use.front()`, and both of its `chassert`s still hold: the list
keeps one entry per running transaction, and re-inserting at the sorted position keeps it sorted. Every consumer
of `getOldestSnapshot`, `canBeRemoved` and `removeOldEntries` among them, is protected without being changed.

**The model variant.** `SET_SNAPSHOT_PROTECTS` in `Types.tla` is that fix: `SetSnapshot` in `Server.tla` moves
`protected_snapshot` and the `snapshots_in_use` entry with the snapshot, and refuses a target below
`tlog.tail_ptr`. It necessarily makes the tid order stop being the list order, which is why the sortedness
clause of `Assert_getOldestSnapshot` is conditioned on `~SET_SNAPSHOT_PROTECTS`: under the fix the C++ list is
re-sorted at insertion and the assertion holds, while the model's tid-ordered proxy for it does not. The other
two clauses, the equal membership and the per-entry equality, are checked under both variants.

### F1 in full {#f1}

Trace: `traces/atomicity-own-removal.txt`, 50 states, found after 7,982,042 distinct states.

In the final state `h.committed = {1, 2}`, `h.creating = <<{P1}, {P2}, {}>>`, `h.removing = <<{}, {P1}, {P2}>>`,
`h.csn[2] = 35`, `txn[3].snapshot = 35`, and `client[k1].last_read.parts = {}`. The reader is `t3`. Take the
committed writer `u = 2`: `h.loaded[2]` holds and `h.csn[2] = 35 <= 35`, so the clause applies. `C` was
`h.creating[2] \ h.removing[2] = {P2}`, unfiltered because `h.removers[P2]` is empty, and `Rm` was `{P1}`. With
`V = {}` neither branch holds: `C \subseteq V` fails on `P2`, `Rm \subseteq V` fails on `P1`. Red.

Both parts are invisible to `t3`, and both for reasons the C++ spells out.

`P1` is invisible because a committed removal precedes the reader's snapshot. Its record is
`ctid 1, ccsn 34, rtid 2, rcsn 0`. `VersionInfo::isVisible`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:138`) falls through its fast path and returns
`std::nullopt`, so `VersionMetadata::isVisible`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:46`) looks the removal TID up.
`TransactionLog::getCSN(removal_tid)` answers 35, because `t2` has already written its log entry, and the
closing expression at `VersionMetadata.cpp:100` requires `snapshot_version < current_removal_csn`, which
`35 < 35` fails. That is correct and intended: as far as the log is concerned `t2` is committed, and the log is
the only thing the reader can ask.

`P2` is invisible because the reader is itself removing it. Its record is `ctid 2, ccsn 35, rtid 3, rcsn 0` and
`t3` is the reader, so `VersionInfo::isVisible` reaches the own-removal clause first:

```cpp
    if (creation_csn && snapshot_version < creation_csn)
        return false;
    if (removal_csn && removal_csn <= snapshot_version)
        return false;
    if (!current_tid.isEmpty() && removal_tid == current_tid)
        return false;
```

`VersionInfo.cpp:167-172`. The third test fires and returns `false` before control ever reaches the creation
clause at `:178-183`, where `creation_csn = 35 <= 35` with an empty `removal_tid` would have made the part
visible. Own removal wins over visible creation. That is the same precedence `ReadYourWrites` already states,
and the same one the design document's `Atomicity` row already cites for the writer's own removals.

The remaining question was whether the model should have let `t3` lock `P2` at all, with `t2` still parked in
`CommitStoreCreation` and not yet done writing `ccsn` onto everything it created. It should. A transactional
removal runs `MergeTreeTransaction::removeOldPart` (`src/Interpreters/MergeTreeTransaction.cpp:213`), which
calls `lockRemovalTID` and then `setAndStoreRemovalTID` with no check on the creator at all. The one
creation-committed refusal nearby is on the non-transactional path only: the
`tid.isNonTransactional() && isCreatedByUncommittedTransaction()` guard in `setAndStoreRemovalTID`
(`VersionMetadata.cpp:172`) and the matching throw in `NonTransactionalRemovalLocks::lock`
(`MergeTreeTransaction.cpp:139`). Even that guard would not have fired here, because
`isCreatedByUncommittedTransaction` (`VersionMetadata.cpp:137`) asks `TransactionLog::getCSN(creation_tid)`,
which already answers 35 for `t2`, so the creation counts as committed. `lockRemovalTID`
(`VersionMetadata.cpp:195`) itself refuses only on a non-zero `removal_csn` or a lost compare-and-exchange, and
`P2` had neither.

So the read of `{}` is what the server returns, and the property was the thing that was wrong. `C` is the set of
`u`'s creations the reader must see, and a part the reader is dropping in its own transaction is not one of
them, for exactly the reason the row already gives for `u`'s own removals. The clause now reads

```
C == { p \in h.creating[u] \ h.removing[u] :
         /\ ~\E r \in h.removers[p] \ {u} : h.csn[r] <= s
         /\ p \notin h.removing[t] }
```

in `AtomicityStep`. A collected copy of the isolation properties, the operator `ReadOK`, carried the same
clause until the final review of plan 1 deleted it: no `.cfg` named it and no operator used it, so TLC never
evaluated it and a correction applied to one copy and not the other would have been silent. Nothing else moved. The `Rm \subseteq V` branch, the
`V \cap Rm = {}` branch and the `V \cap (h.creating[u] \cap h.removing[u]) = {}` conjunct are untouched, and the
`Atomicity` witness is still red, so the property still has teeth.

## 2. Model defects and where they get fixed {#model-defects}

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M1 | `LegacyPartRecord` gives a legacy part `mem.sv = 0` (`Parts.tla:26`) while its disk record is `Legacy` (`Disk.tla:20`), which is not `Info`, so `DiskHasInfo` is false (`Disk.tla:42`) and `StoredRecord` falls back to `EmptyInfo` with `sv = -1` (`Parts.tla:128`) | Every store on a legacy part compares an expected `-1` against a tentative `sv` of 0 in `StorePersistStep` (`Parts.tla:153`), takes the `TOO_OLD_VERSION` branch, retries `MAX_STORE_RETRIES` times and ends in `STALE_VERSION`. No legacy part can ever be written, so plan 3's `Crash` runs with a non-empty `LEGACY_PARTS` would be vacuous | plan 3, the task that enables `LEGACY_PARTS` |
| M2 | `StmtRollbackMark` and `StmtRollbackDrop` acted on `stmt.precommitted \ stmt.attached`, and `Refuse` routed to them only when that difference was non-empty (`Server.tla`, the statement-rollback actions) | The statement rollback skipped a part that `addNewPartAndRemoveCovered` had already attached to the outer transaction, where `MergeTreeData::Transaction::rollback` (`src/Storages/MergeTree/MergeTreeData.cpp:11122`) marks and removes every part of `precommitted_parts`; and a `Refuse` whose precommitted set was entirely attached left the `stmt` record standing, so the leak outlived the statement. Both are dead in `Base`, which has `QUERY_FAULTS_MAX = 0` and an empty `Covers`, so neither `Fail` nor `PublishEnrol` can refuse with a non-empty `stmt.precommitted`; plan 2 gives the sibling scenario a covering relation, which makes `PublishEnrol` live and a `SERIALIZATION_ERROR` between `PublishStart` and `PublishFlip` reachable, and both would have gone live there at once | closed by the final-review fix commit of plan 1 |
| M3 | `UpdRemoveOldEntriesDelete` re-reads `tlog.tid_to_csn` and `tlog.latest_snapshot` on every iteration, while `TransactionLog::removeOldEntries` (`src/Interpreters/TransactionLog.cpp:319-323`) snapshots both once and then loops over the copy | `latest_snapshot` only grows, so the model can delete an entry the C++ would have kept as "the latest one we fetched". The behaviour set is widened, not narrowed, which is safe for every property of plan 2: the only property that reads `h.truncated` is `LogEntryNeeded`, which is plan 3's. The same action also erases `tid_to_csn[t]` in the step that removes the znode, while the code collects `removed_entries` and erases them after the loop under `mutex`, so the model never has the code's window in which the znode is gone and the local lookup still succeeds; and `UpdRemoveOldEntriesDone` is unguarded, so a pass can end with entries the code's loop would still have removed. Both widen the behaviour set in the same direction as the re-read | plan 3, the task that enables `Crash`, either narrows all three to the code's shape or argues the widening is still sound for `LogEntryNeeded` |
| M4 | An exhaustive run of `MC_SetSnapshot` at the bounds the scenario matrix gives it does not finish: five runs at `TID_MAX = 3`, `CSN_MAX = 36` were killed, the last at 56,968,754 distinct states after 8 minutes with 4.47 million still queued. The scenario is 3.4 times `Base` at equal bounds, which puts it near 98 million and about 15 minutes | The scenario is checked exhaustively at `TID_MAX = 2`, `CSN_MAX = 35` instead, in 65 seconds, and its witnesses are shown at the matrix bounds in `MC_SetSnapshotWitness`, where a run stops at the first violation. What that costs is debt B1 below | the task that owns the state-space budget; a `VIEW` or `CONSTRAINT` that closes the gap would let the two bound sets become one again. The measurements, the three reductions applied and the one measured and rejected are in `STATE_SPACE.md`, section "The `SetSnapshot` scenario" |
| M5 | `UpdRemoveOldEntriesSetTail` sets `tlog.updated_tail_ptr` inside the branch where the tail actually moves, while `TransactionLog::removeOldEntries` stores `true` at `:302`, before it reads the znode and before the `new == old` early return | The async-loading gate stays armed in the model after a pass the code would have disarmed. Dead here: `sys.completely_started` is `TRUE` at init and `sys.async_loading_jobs` is `0` at init, and no action of this plan writes either, so the gate is a constant. It is not a one-line reorder, because the model has no action for a pass that ran and moved nothing, and adding one would add states for a behaviour nothing yet observes | plan 3, the task that enables restarts and table loading, which is what makes both gates variable |
| M6 | Under `SET_SNAPSHOT_PROTECTS` the sortedness conjunct of `Assert_getOldestSnapshot` is switched off, while `OldestSnapshot` is still `Min({...})` (`Parts.tla`) | Nothing in `MC_SetSnapshotFixed` checks that the fix keeps `front()` equal to the minimum, which is the one thing the sorted re-insert exists to preserve. The model cannot state it as written, because `Min` encodes the answer rather than the list | the task that gives `snapshots_in_use` an ordered encoding, if one is ever needed; until then `MC_SetSnapshotFixed` verifies that the fix breaks nothing, not that it preserves `front()` |
| M7 | `CleanupValidate` and the validation half of `CleanupDeleteFail` turn every refusal of `hasValidMetadata` into the rollback to `Outdated`. The code has two outcomes, not one: `hasValidMetadata` throws `CORRUPTED_DATA` on a mismatch, which is the rollback, but its catch-all (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:711-720`) *returns false*, and a false makes the `chassert` at `IMergeTreeDataPart.cpp:2928` abort the process | The model has no terminating outcome for a failed validation, so a scenario that reached one would report a recoverable rollback where the server dies. Dead today: the validation refusal is itself unreachable in every scenario of this plan, which is why `NoFalseCorruption` is vacuously green | plan 5, the task that enables `ProcessDown`, where the outcome belongs to `NoAvoidableTermination` alongside the other terminations |
| M8 | `part_is_probably_removed_from_disk` (`IMergeTreeDataPart.cpp:2873`) makes a second `remove` of the same part skip validation entirely; the model re-validates on every grab | The model can loop where the code cannot: `CleanupDeleteFail` returns a part to `Outdated`, `CleanupGrab` takes it again and the same validation fails again. Harmless while the refusal is unreachable | plan 5, the task that turns on disk faults and makes the refusal reachable |
| M9 | `MergeWrite` did not pin the merge result with `Tsk(i)`, although the task holds it from the write until the task object is destroyed (`merge_task`, then `MergePlainMergeTreeTask::new_part`, `src/Storages/MergeTree/MergePlainMergeTreeTask.cpp:156`) | The cleanup thread could take a result the task's statement rollback had just outdated while the task was still alive, which `isSharedPtrUnique` (`MergeTreeData.cpp:4150`) forbids. Found by counterexample `F3` | closed by this task: `MergeWrite` adds the pin and `MergeCommitFinalize` and `MergeUnwind` remove it |
| M10 | `Transaction::commit` recomputes `getActivePartsToReplace` inside its `NOEXCEPT_SCOPE` (`MergeTreeData.cpp:11298`) and, on a non-null `covering_part`, outdates the new part instead of activating it (`:11316`); `PublishStartEffect` and `PublishFlipEffect` model that, and it is unreachable in every scenario of this plan | The branch is written and cited but never taken, so it is checked by inspection only. It cannot be reached with the model's `Covers`, which is a fixed relation over a finite part universe: `M12`'s sources are exactly `P1` and `P2`, `MergeSelect` requires both to exist already, and `InsertWrite` requires `Absent`, so no part can appear under a cover that is already `Active`. The C++ shape it models is a new block number landing inside a range a merge has already published, which needs a covering part whose source set is a strict subset of what it covers | the plan that gives `Covers` a range shape rather than a fixed source set, or any scenario with a covering part over a source that can still be inserted |
| M11 | `MergeUnwind` can in principle win the compare-and-exchange in `MergeTreeTransaction::rollback` (`src/Interpreters/MergeTreeTransaction.cpp:382`) and become the rollback driver, and every step of the rollback machine is session-shaped (`Drives(k, t)` reads `Sess(k)`), so the transaction would sit in `RollbackCopyLists` with nothing able to advance it | No scenario of this plan reaches it. The task's own unwind triggers are a refusal from `removeOldPart` and a failed `beforeCommit`, both of which mean a `KILL` has already won the exchange, and the third, a store of the task's own ending in an error, needs a second frame on one of the task's parts, which no other actor can have while the merge's transaction is still `Running`: the result is the task's alone, and the sources are unreachable to a session because `DropLock` waits for every task's `reserved` to drain before it takes the parts lock, and the merge holds its reservation from `MergeSelect` until the task idles, so no client removal can start on them in that window. The invariant `NoTaskDrivenRollback` says so rather than letting the search wedge without a word | the plan whose scenario enables a fault on a background task, which is the disk-fault plan: it owes `DrivesA(a, t)`, the task-shaped `Rollback*` steps, and the removal of `NoTaskDrivenRollback` |

`M2` was closed rather than placed. `Refuse` now routes to `StmtRollbackMark` whenever `stmt.precommitted` is non-empty, which is what `MergeTreeData::Transaction::isEmpty` (`src/Storages/MergeTree/MergeTreeData.h:391`) tests, and both statement-rollback actions act on the whole of `stmt.precommitted`. The attached part keeps its entry in `txn[t].creating` and in `h.creating[t]`, because `MergeTreeTransaction::creating_parts` keeps its entry too; `Refuse` always ends at the outer rollback, and that rollback's second stamp of `RolledBackCSN` and second removal from the working set are both no-ops in the C++ and in the model. The entry `README.md`, section 8, used to carry for this divergence is gone with it. Nothing in `Base` changed state: the count stayed at the figure the run table records, for the reason the row above gives.

The C++ settles which side of the comparison is wrong. A part with no `txn_version.txt` is loaded by
`VersionMetadataOnDisk::loadMetadata` (`src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:49`),
which on the no-file path returns a default-constructed `VersionInfo` carrying only
`creation_tid = Tx::NonTransactionalTID` and `creation_csn = Tx::NonTransactionalCSN`. Its `storing_version`
keeps the member default, `VersionInfo::UNSTORED_VERSION = -1`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.h:20,23`). The other side of the comparison,
`getExpectedStoringVersionUnlocked` (`VersionMetadataOnDisk.cpp:247`), also returns `UNSTORED_VERSION` when the
file does not exist. Both sides are `-1`, and the first store goes through.

So the model's `mem` is the wrong side, not `StoredRecord`. The fix is to drop the `!.sv = 0` from
`LegacyPartRecord` and let it keep `EmptyInfo`'s `-1`, which is what `loadMetadata` produces. Giving the disk
record a stored `sv` of 0 instead would contradict the C++, where a legacy part has nothing persisted at all.

`M9` was closed rather than placed, in the task that found it.

One more `chassert` is latent and is not reachable here. `VersionMetadata::setAndStoreCreationCSN`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:128`) asserts that the creation CSN it overwrites is
either unset or the `NonTransactionalCSN` of the empty part `executeDropRange` creates, so a statement rollback
that stamps `RolledBackCSN` followed by a commit of the same transaction fires it: `afterCommit` would call the
function again with a real CSN on a record that already carries `RolledBackCSN`. The sequence is unreachable in
this plan, on both paths into a statement rollback. `Refuse` routes to `StmtRollbackMark` and ends at
`RollbackOnException`, and `MergeFail` routes to `MergeStmtRollbackMark` and ends at `MergeUnwind`; both continue
to the transaction rollback, and no action leads from a statement rollback back to `CommitBefore`. It is placed
with the mutation plan, where `StorageMergeTree::killMutation` cancels a task without failing the client
transaction that registered the mutation, which is the first candidate for a statement-level failure whose
transaction survives. If it becomes reachable there it needs an `Assert_` property of its own; until then this
paragraph is the record that it was looked for and not found.

## 2a. Bound-contract debts {#bound-contract-debts}

The bound contract allows bounds below the scenario matrix only while every witness of every property the
scenario checks stays red. Where that does not hold, the gap is recorded here rather than hidden by moving the
bound or dropping the property.

| Id | Scenario | What is not verified at the exhaustive bounds | Where it is verified instead |
|---|---|---|---|
| B1 | `SetSnapshot` and `SetSnapshotFixed` at `TID_MAX = 2`, `CSN_MAX = 35` | Five witnesses are green or do not finish there, and `NoPrematureDelete` itself is unreachable there, which is `B2`: `SingleRemover`, `NoUncommittedRead`, `NoLostRead`, `Assert_validateInfo_removal` and `Assert_getOldestSnapshot`'s sortedness conjunct | See the two paragraphs below |
| B3 | `Merge` at one session | Three witnesses are green there: `NoUncommittedRead`, `Assert_validateInfo_removal` and `NoSpuriousStaleVersion` | `Base`, at `TID_MAX = 3` with two sessions, for all three |
| B2 | `SetSnapshot` and `SetSnapshotFixed` at `TID_MAX = 2`, `CSN_MAX = 35` | The witnesses of `NoPrematureDelete` and of `NoLostVisibleData`, both of which change `CleanupGrab` to compare against `tlog.latest_snapshot` instead of `getOldestSnapshot`, are green there: 13,664,284 and 13,664,666 distinct states, 96 and 98 seconds, no violation | `SetSnapshotF2Fixed`, where both are red in three seconds |

The first four are `Base` properties and `Base` verifies all four at `TID_MAX = 3`: three of them are the three
rows `WITNESSES.md` already explains need a third transaction, and `Assert_validateInfo_removal` is the fourth,
which plan 1 found needs three transactions too. They are not re-verified in `SetSnapshot`, and nothing about
this scenario changes the actions those witnesses mutate.

`h.content` is in `MC_SetSnapshot`'s view although that configuration checks no property that reads it. It is
there for uniformity with the four sibling modules rather than because a property needs it, and a finer view is
sound in any case: it can only split states, never merge two that behave differently. `part.pins` is the other
way round in every module, including that one, because `CleanupGrab` reads it.

`B3` has two causes, and they are worth separating because only one of them is about the session count.

`NoUncommittedRead` and `Assert_validateInfo_removal` are the two rows plan 1 already found need three
transactions. `Merge` has `TID_MAX = 3`, but `MergeBegin` draws from the same counter as `Begin`, so the merge
spends one of the three and the scenario runs two client transactions, which is one short of what those two
shapes need. Both are verified in `Base` at three client transactions, and nothing about the merge task changes
the actions their witnesses mutate. `Assert_validateInfo_removal` is also the expensive row again: it is green
here only after 38.6 million distinct states and about five minutes, on a scenario whose own space is 5.2
million, because removing the wait opens behaviours the scenario itself does not have.

`NoSpuriousStaleVersion` is the one that is about the session count, and it is a real coverage loss rather than
a missing transaction. Its witness needs two stores on one part interfering until a frame exhausts
`MAX_STORE_RETRIES`, and at one session there is no second storer: the merge task's frames are on its own
result and on sources whose creator has already committed and whose removal the reservation and the removal lock
keep every session away from, so no two frames are ever in `Persist` on one part. The store-interference
machinery is therefore entirely unexercised in `Merge`. It is exercised in `Base`, where the witness is red, and
the scenario that would exercise it here is `Merge` at two sessions, which is the configuration that does not
finish. If a later task closes the state-space gap with a `VIEW` or a `CONSTRAINT`, this is the row that gets
paid by it.

`B2` has the same cause as `F2` and the paragraph above it: the shape both witnesses need is a part still
visible to a running transaction at a lowered snapshot, which takes three transactions and a snapshot target
above `FirstCSN`. The exhaustive bounds of `SetSnapshot` give two transactions and the target 33, so a
`CleanupGrab` that consults `latest_snapshot` grabs nothing it should not have. `SetSnapshotF2Fixed` is the
configuration that has the shape, and both witnesses are red there. The third cleanup witness,
`PinnedNotDeleted`, does not need the shape and is red at the exhaustive bounds in two seconds.

The fifth is different, and it is the reason `MC_SetSnapshotWitness` exists. `Assert_getOldestSnapshot` is the
property this scenario adds: `Base` does not enable `SetSnapshot`, so there is nowhere else its witness can be
shown. Its sortedness conjunct needs a transaction that began above `FirstCSN`, which costs a committed
transaction before the two that run concurrently, so three in total. It is red at the witness bounds, in four
seconds, and the run is in `WITNESSES.md`. The property's other two conjuncts have their own witnesses and both
are red at the exhaustive bounds.

## 3. Spec defects {#spec-defects}

Rows of `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` that the model contradicts,
with the correction the next revision folds in.

| Id | Spec row | What is wrong | Correction |
|---|---|---|---|
| S1 | `Atomicity`, section "Invariants and properties" | `C` is built from the parts the writer created and did not itself remove, minus those a later committed transaction visible to the reader removed. It never subtracts the reader's own uncommitted removals, although the precedence it cites for the writer, own removal over visible creation, is the reader's too: `VersionInfo::isVisible` returns false on `removal_tid == current_tid` (`VersionInfo.cpp:171`) before reaching the creation clause at `:178`. Counterexample F1 | Add to `C` the condition that `p` is not in the reader's own `h_removing`. `Atomicity` in `Invariants.tla` already carries it |
| S2 | the `NoAvoidableTermination` witness row | The row names scenario `Base`, but the witness it describes needs a `Fail` inside `afterCommit` that takes the server down, which requires `ProcessDown`, an action `Base` does not enable. `WITNESSES.md` defers the witness to plan 5. One of the two statements has to give | Move the row's scenario to the first one that enables `ProcessDown`. The property itself stays in `Base`, where it is checked but not yet falsifiable |
| S3 | `UpdLoadEntriesMap`, section "Updating thread" | The row puts the action at "`loadEntries` up to the `NOEXCEPT_SCOPE_STRICT` block", and then says that block is where the batch enters `tid_to_csn` under `TransactionLog::mutex`. The two halves contradict each other, and the code settles it: `TransactionLog::loadEntries` (`src/Interpreters/TransactionLog.cpp:161`) fills `tid_to_csn` and advances `last_loaded_entry` *inside* the `NOEXCEPT_SCOPE_STRICT` block, and only the block after it, under `running_list_mutex` (`:174`), moves `latest_snapshot`. An action that stopped before the block would publish nothing | Restate the boundary as "`loadEntries` through the `NOEXCEPT_SCOPE_STRICT` block". `README.md`, section 3, carries the corrected row |
| S4 | `CommitError`, section "Client and session" | The row attributes the `INVALID_TRANSACTION` of a `COMMIT` on a cancelled transaction to `beforeCommit`. `InterpreterTransactionControlQuery::executeCommit` (`src/Interpreters/InterpreterTransactionControlQuery.cpp:64`) refuses it earlier, on `getState() != RUNNING`, before `commitTransaction` is called at all, and that is the guard the model's action has. `beforeCommit`'s failed compare-and-exchange (`src/Interpreters/MergeTreeTransaction.cpp:308`) raises the same error only for a kill that lands after the interpreter's guard | Name the interpreter guard as the action's site and `beforeCommit` as the racing one. `README.md`, section 3, carries the corrected row |
| S5 | `AckedWriteIsDurable`, section "Invariants and properties" | The row admits a created part in `Outdated` only when the removal is committed, `h_removers[p] /= {}`. `DropOutdate` outdates a part under the parts lock, long before the transaction that drops it commits, so a part created by an acknowledged transaction and dropped by a still-running one is `Outdated` with no committed remover, and the literal row is red on the baseline. `Invariants.tla` therefore also admits a removal merely in flight, `part[p].lock /= EmptyTID`. The row's antecedent is also `h_effects`, which includes mutations, while the property tested only `h.creating` and `h.removing` | Admit an in-flight removal in the row, with the `DropOutdate` boundary as the reason. `Invariants.tla` carries the relaxation and cites this id; its antecedent now includes `h.mutations`, and `FlipAfterStoresStep` carries the row's mutation conjunct, both vacuous while `Mutations = {}` |
| S6 | the `SetSnapshot` row of the scenario matrix | The row lists `NoOutdatedLookup` among the properties the `SetSnapshot` scenario checks. `assertTIDIsNotOutdated` (`src/Interpreters/TransactionLog.cpp:656`) has exactly two call sites: `tryFinalizeUnknownStateTransactions` (`:387`), which is the action `UpdFinalizeUnknown`, and `getCSNAndAssert` (`:645`), which has no caller anywhere in the tree. The `SetSnapshot` scenario is `Base` plus `SetSnapshot`, the cleanup group and the updater's GC group, and enables neither, so the property is vacuous there whatever the trace. Its own witness row names scenario `Keeper`, which does enable the unknown-state group, so the two rows of the document disagree | Drop `NoOutdatedLookup` from the `SetSnapshot` row and keep it in `Keeper` and `SnapshotCrash`, which enable `UpdFinalizeUnknown`. `NoOutdatedLookup` is defined in `Invariants.tla` and is not in `MC_SetSnapshot.cfg`; `WITNESSES.md` records the vacuous run |
| S7 | the bound contract, section "Scenario matrix" | The contract gives each scenario one set of bounds and requires both that the scenario be checked exhaustively at them and that every witness be red at them. `SetSnapshot` cannot have both: exhaustive at the matrix bounds does not finish, and every bound that does finish loses a witness. The two jobs have different costs, because a witness run stops at the first violation and an exhaustive run does not, so one number cannot serve both | Give each scenario two sets of bounds, exhaustive and witness, with the rule that the witness bounds are at least the exhaustive ones and that every witness is red at the witness bounds. `MC_SetSnapshotWitness` is the first instance. Plan 1 deleted `MC_BaseWitness` for having exactly this shape, which was premature: the right correction there was to name the pattern, not to remove it |
| S8 | `StableRead`, section "Invariants and properties" | The row states that the first and last read of `t` differ only by fragments of parts in `h_creating[t]` or `h_removing[t]`, with no qualifier about the snapshot those reads were taken at. `SET TRANSACTION SNAPSHOT` makes a transaction read at a different snapshot, so a read before it and a read after it are reads at two different snapshots and the literal row is red on the baseline for the statement working as intended. `SetSnapshot` in `Server.tla` therefore restarts the read baseline, which narrows the property to the span between two `SET TRANSACTION SNAPSHOT` statements | Qualify the row by snapshot. This is a property decision, not a transcription: `MergeTreeTransaction::setSnapshot` (`src/Interpreters/MergeTreeTransaction.cpp:52`) stores one value and resets nothing, so there is no code to cite for the reset. What it models is that the row's "first read" means the first read judged at the snapshot the last read is judged at |
| S9 | the `MergeSelect` row of the "Merge" table, and the paragraph under it | Both say the sources must be visible "to the merge transaction" and "to the merge's own snapshot". The predicate the code builds asks `part->version->isVisible(tx->getSnapshot(), Tx::EmptyTID)` (`src/Storages/MergeTree/Compaction/PartsCollectors/MergeTreePartsCollector.cpp:88`): the merge's snapshot, but the **empty** tid, not the merge's own. With its own tid a part the merge's transaction had created or was removing would be judged by the own-creation and own-removal clauses of `VersionInfo::isVisible`, which is not what a merge asks. The row also omits the second refusal at `:91`, `isRemovalTIDLocked`, which the model has as `part[q].lock = EmptyTID` | Name the empty tid, and add the removal-lock test as a condition of its own. `MergeSelect` in `Server.tla` carries both and cites the two lines |
| S10 | the `MergePublish*` row | It releases `reserved[i]` at `PublishFlip`. `CurrentlyMergingPartsTagger::finalize` runs at the end of `MergePlainMergeTreeTask::finish` (`src/Storages/MergeTree/MergePlainMergeTreeTask.cpp:200`), after `transaction.commit()` at `:161` and after `commitTransaction` at `:195`, so the reservation outlives the publication and the whole commit. The difference is observable: `DropLock` waits for every reservation to drain, so a `DROP PARTITION` is held off until the merge has committed, not until it has published | Move the release to the end of the commit. `MergePublishFlip` leaves `reserved` alone and `MergeCommitFinalize` clears it with the rest of the task record |
| S11 | the `MergeFail` row | "Exception anywhere before `MergeCommitBefore`" misses the commit step itself: `commitTransaction` reaches `beforeCommit`, whose failed compare-and-exchange (`src/Interpreters/MergeTreeTransaction.cpp:308`) raises `INVALID_TRANSACTION` for a transaction a `KILL` got to first, so the task can also fail at `Commit`. "The holder rolls the transaction back" is true only when the holder wins that same exchange in `MergeTreeTransaction::rollback` (`:382`); against a `KILL` it loses and rolls nothing back, and the killer drives the body. The row also implies the statement rollback is conditional on `M12` having been renamed, where `MergeTreeData::Transaction::isEmpty` tests `precommitted_parts`, which is model defect `M2`'s closure | Give the row the `Commit` failure and the lost exchange. `MergeFailTrigger` in `Server.tla` enumerates the three triggers and `MergeUnwind` carries the exchange |

Two more rows of the "Client and session" table are coarser than the model rather than wrong: `DropStart`
bundles `stopMergesAndWait`, the wait, `lockParts` and the selection of the visible parts into one row, which the
model splits into `DropStart` and `DropLock` because the wait is where another session's work interleaves; and
`StmtRollback` is one row for the two halves of `MergeTreeData::Transaction::rollback`, the per-part
`setAndStoreCreationCSN(RolledBackCSN)` before the parts lock and the state change under it, which the model
splits into `StmtRollbackMark` and `StmtRollbackDrop` for the same reason. The design document's own rule is
"one function or one step of a function", so neither split contradicts it and neither is filed above.

### A spec row that was wrongly doubted {#withdrawn}

The `validateInfo, removal` witness row was on this list until the bounds changed, and it comes off it.

The row names one change, "`DropStore` skipped". At `TID_MAX = 2` that change alone left the run green, and the
witness was built as a two-change one, the second change stopping `EnrolBody` (`Server.tla:189`) from starting
the removal-TID store at all. `WITNESSES.md` described the two-change witness and both of its minimality halves
were green, so the row looked wrong.

At `TID_MAX = 3` the half that applies the wait change alone is red: 53,133,545 distinct states, and the trace
is exactly the shape the row names. `t2` locks `P1`, starts the removal-TID store, and returns from
`removeOldPart` without waiting for it; `t2` is then rolled back, which clears the removal TID; `t3` takes the
lock, commits, and `CommitStoreRemoval` computes a removal CSN of 35 for a record whose `rtid` is still empty,
which `validateInfo` rejects. Three transactions are the fewest that can build it, which is why two changes
looked necessary at two.

So the row was right and the model's witness was over-specified. `Assert_validateInfo_removal` is now the
one-change witness the row names, the `_only1` and `_only2` hooks are gone with the minimality check they
existed for, and nothing is owed to the next spec revision here.

### `RollbackNoLeak` and `KillerNotStranded` {#properties-without-a-row}

`RollbackNoLeak`, added by task 1, and `KillerNotStranded`, added by task 3, are both in `MC_Base.cfg`'s
`INVARIANTS` list without a witness, which the witness contract does not allow. They are not the same case.

`RollbackNoLeak` does have a spec row. The design document's `RollbackRestores` row states two things: the
action property on `RollbackFinalize`, which `RollbackRestoresStep` implements, and, "as a state invariant, no
part of `h_creating[t]` is ever in the visible-parts set of a read by a transaction other than `t`".
`RollbackNoLeak` is that second half, stated more strongly: the model quantifies over every transaction that is
not in `h.committed`, not only over the rolled-back ones the row speaks of. So it is rostered, and what it owes
is a witness of its own rather than a row. A witness is writable within `Base`, a rollback that leaves a created
part visible to another session's read, and it belongs with the plan that next touches the rollback actions.

`KillerNotStranded` appears nowhere in the design document: not as a property, not as a witness row. The ruling
recorded here is that it stays. It states something the model would otherwise not check at all, it holds on
every baseline run, and removing it would lose coverage to satisfy a bookkeeping rule. What the next spec
revision owes it is a row, with the witness that row implies: a rollback step that starts and never completes,
so that a killer parked at `KillWait` is never released. `Base` has no such step, so that witness goes to plan 5
alongside the other liveness witnesses.

Until those two witnesses exist, both properties are checked without ever having been shown to be falsifiable.
That is a debt, recorded here and in the gap table of `WITNESSES.md`, not a result.
