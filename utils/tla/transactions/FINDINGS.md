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

**Suggested order for the upstream work.** `F6` first: the fix is one statement, it is minimal, and it closes
route 3 of `F5` as a side effect, so it can go up as a patch with a functional regression test. `F9` next, and
also as a patch: its prerequisite is one statement a user can type, `SET TRANSACTION SNAPSHOT 3`, and the
predicate the fix needs, `VersionMetadata::isCreationCommitted`, is already in the file and already used for
the same purpose on the non-transactional path, so the change is a refusal at one call site with a regression
test that runs two sessions. `F2` third, and as an issue rather than a patch: the counterexample is solid and the synchronisation it needs is a design
question, because the snapshot registry and the cleanup pass have to be ordered against each other and the
obvious guard holds `running_list_mutex` across two Keeper round trips. `F5` last, because what it needs first
is an executable reproduction; the fix direction, preventing publication into a range whose removal a running
transaction holds locked, is a design change and not a patch that can be written from this entry.

| Id | Scenario, bounds | Property | Action sequence (short) | Classification | Resolution | Proposed code fix |
|---|---|---|---|---|---|---|
| F12 | `CrashF12`: one session, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 1`, `CSN_MAX = 36`, `DISK_MODE = "Layered"`, all three fsync settings `FALSE`, which is what upstream runs by default | `NoNonTxnRebirth` | `t1` inserts `P1` and the creation record naming `t1` is written; the part takes its final name and a writeback makes that name durable; the server crashes while `t1` is still running, and the part directory survives holding neither `txn_version.txt` nor its temporary file, because those two are dentries in the part's own directory and nothing synced it; the loader takes the arm meant for parts written before transactions existed and gives the part a non-transactional creation with `NonTransactionalCSN`, so an uncommitted part is `Active` and visible at every snapshot for ever | `code`, provisional | `MC_CrashF12` produces it, trace `traces/f12-uncommitted-part-reborn-nontransactional.txt`, 13 states, 1,731 distinct, a first-violation count. The fix variant `CREATION_TID_STORE_SYNCS_DIR` runs the same configuration green in `MC_CrashF12Fixed`, 21,804,360 distinct in 3 min 24 s. See below | two, and the second is the one to write first: take the directory guard unconditionally for the store that writes the creation TID, and stop the loader treating a directory with no metadata as pre-transactional on a table that has used transactions. See below |
| F11 | `CrashUnsynced`: one session, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 1`, `CSN_MAX = 36`, `DISK_MODE = "Layered"`, all three fsync settings `FALSE`, which is what upstream runs by default | `LogEntryNeeded` | `t1` inserts `P1` and commits; a writeback publishes the part directory's final name and the creation record, which names `t1` and carries no CSN; the server crashes before `afterCommit` stores the CSN; the restart's loader finds the CSN in the log and writes it back through `storeInfo`, which without `fsync_part_directory` leaves the rename in the page cache; `removeOldEntries` then moves the tail and prunes `t1`'s entry, so the only durable copy of the record is again the one without a CSN and the log no longer has the CSN either | `code` | `MC_CrashUnsynced` produces it, trace `traces/f11-truncated-entry-unsynced-csn.txt`, 22 states, 28,844 distinct, a first-violation count. The fix variant `ENTRY_KEPT_UNTIL_CSN_DURABLE` runs the same configuration green in `MC_CrashUnsyncedFixed`, 37,303,778 distinct in 8 min 44 s. See below | make the startup repair durable, and keep the entry until it is. `VersionMetadata::loadAndUpdateMetadata` writes a recovered CSN through `storeInfo`, whose directory guard is taken only under `fsync_part_directory`; that write is the one `removeOldEntries` relies on and it must be synced. Beside it, a per-part "record written, rename not yet fsynced" bit, set where that guard is skipped and cleared when the directory is synced, lets the removal loop keep an entry whose CSN only such a record carries. See below |
| F10 | `CrashF10`: one session, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 2`, `CSN_MAX = 36`, `DISK_MODE = "Layered"`, `FSYNC_AFTER_INSERT = TRUE`, `FSYNC_OUTER_RENAME = TRUE`, `FSYNC_PART_DIRECTORY = FALSE` | `AckedWriteIsDurable` | `t1` inserts `P1`, commits and is told `Acked`; the server crashes with the rename of `txn_version.txt` not yet durable; `RestartLoadPart` reads the tmp-only directory as `DummyTID` with `RolledBackCSN`, so the acknowledged part loads `Outdated` and the cleanup thread may then remove it | `property` | expected red wherever the metadata rename is unsynced; green in `MC_Crash`, which sets all three settings | set `fsync_part_directory`, which closes this shape because the guard spans the `replaceFile`, AND `fsync_after_insert`, without which the rows are unsynced even when the metadata is; neither is on by default |
| F9 | `SetSnapshotF9Sibling`: two sessions, one part, `TID_MAX = 2`, `CSN_MAX = 35`, `SNAPSHOT_TARGETS = {3}`, `SET_SNAPSHOT_PROTECTS = FALSE`, the baseline | `Assert_validateInfo` | `t1` inserts `P1` and stays `Running`, so `P1` is `Active` with `creation_csn` unset; `t2` lowers its snapshot to `EverythingVisibleCSN`, which makes `P1` visible to it at once, where at an ordinary snapshot `VersionMetadata::isVisible` asks the log and answers invisible while the creation CSN is unknown; `t2` drops the partition, the skip in `removePartsFromWorkingSet` does not apply because the creation is unset rather than rolled back, `lockRemovalTID` grants the lock, and `setAndStoreRemovalTID` validates `removal_tid = t2` beside `creation_csn = 0`, which `validateInfo` rejects with `LOGICAL_ERROR`, "creation_csn is not set while removal_tid is not ...", before anything is stored. The finding has two shapes: a client exception on this one, and a termination on the second, where the creator stamps `RolledBackCSN` between the skip and the store, the store succeeds, and the remover's commit raises inside the `noexcept` `afterCommit` | `code` | `MC_SetSnapshotF9Sibling` produces it, trace `traces/f9-uncommitted-creation-removal-validateinfo.txt`, 15 states; 20,161, 21,580, 20,545, 22,609 and 22,618 distinct on five runs, a first-violation count. The second shape's trace is `traces/f9-rollback-after-skip-window.txt`, 29 states. The fix variant `REMOVAL_REFUSES_UNCOMMITTED_CREATION` runs the same configuration green in `MC_SetSnapshotF9SiblingFixed`, 1,858,364 and 1,858,346 distinct on two runs with the restored roster, which is the counting noise this module shows on every run. See below | refuse the removal rather than the read: for a transactional remover, `!tid.isNonTransactional()`, `VersionMetadata::lockRemovalTID` must throw `SERIALIZATION_ERROR` when `creation_tid != remover_tid && !isCreationCommitted()`, which covers a creation still in flight and a creation already rolled back alike. See below |
| F8 | `SetSnapshotF8`: one session, one part, `TID_MAX = 2`, `CSN_MAX = 35`, `SNAPSHOT_TARGETS = {3}`, the whole roster | `RollbackNoLeak` | `t1` inserts `P1` and is rolled back; `t2` lowers its snapshot to `EverythingVisibleCSN` and its `SELECT` returns `P1`, a part no committed transaction ever created | `property` | the C++ does exactly this, and by design: `VersionInfo::isVisible` returns true for every part at that snapshot before it looks at any CSN (`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:157-158`, "Special snapshot for introspection purposes"). `MC_SetSnapshotF8` produces it, trace `traces/f8-everything-visible-rollback-leak.txt`, 24 states. The design document's isolation and rollback rows carry no qualifier for it, which is spec defect S18 | none for the read itself; what needs the fix is the write path, which is `F9` |
| F6 | `NonTxnF6`: one session, `Parts = {P1, P2, E}`, `TID_MAX = 2`, `CSN_MAX = 35` | `Assert_validateInfo` | a non-transactional `DROP PARTITION` publishes the empty part `E` `Active`; `t1`, which had `P2` precommitted, publishes it and `Transaction::commit` finds `E` covering it, so `P2` goes `Outdated` and is never attached to `t1`; `t1` then drops the partition transactionally, and `P2` is visible to it through its own `creation_tid`, so the drop enrols it; `t1` commits, `afterCommit` stores `removal_csn` on a record whose `creation_csn` is still zero, and `validateInfo` raises `LOGICAL_ERROR` inside `MergeTreeTransaction::afterCommit`, which is declared `noexcept` (`src/Interpreters/MergeTreeTransaction.cpp:321`), so the process terminates | `code` | the model variant `OBSOLETE_IS_ROLLED_BACK` encodes the proposed fix and `MC_NonTxnFixed` runs the same configuration with it; `MC_NonTxnF6` produces the finding, trace `traces/f6-obsolete-part-removal-csn-noexcept.txt`, 44 states; 120,405 distinct on the run that produced it and 117,204 on a re-run, a first-violation count | stamp the obsolete part `RolledBackCSN` the way `MergeTreeData::Transaction::rollback` stamps a part that does not make it in, in the pre-`NOEXCEPT_SCOPE` loop that already knows the covering part (`src/Storages/MergeTree/MergeTreeData.cpp:11278-11282`) and **not** in the obsolete branch itself (`:11305-11317`), which is inside the scope. See below |
| F5 | `NonTxnF5`: one session, `Parts = {P1, P2, E}`, `TID_MAX = 2`, `CSN_MAX = 35` | `ActiveSetShape` | a non-transactional `INSERT` publishes `P1`; a non-transactional `DROP PARTITION` publishes the empty part `E` over it and removes `P1` through the batch; a second non-transactional `INSERT` of `P2` is published while `E` is `Active`, so `Transaction::commit`'s covering branch marks `P2` `Outdated` instead; `t1` then runs a transactional `DROP PARTITION`, which sees `E` and `P2` (both visible) and enrols both; `ROLLBACK` restores both to `Active`, and `E` covers `P2` | `code` | recorded, not fixed: the obvious local fix trades this violation for a `RollbackRestores` one, which is what makes the finding interesting. Trace `traces/f5-rollback-restores-into-covered-range.txt`, 42 states; 106,922 distinct on the run that produced it and 99,234 on a re-run, a first-violation count. See below | `MergeTreeData::restoreAndActivatePart` (`src/Storages/MergeTree/MergeTreeData.cpp:7325`) must not reactivate blindly; the prevention belongs at publication time. See below |
| F4 | `NonTxnF4`: two sessions, `Parts = {P1, P2, E}`, `TID_MAX = 2`, `CSN_MAX = 35` | `NoLostVisibleData` | a non-transactional `INSERT` publishes `P1`; a non-transactional `DROP PARTITION` writes `E`, publishes it and locks `P1` in its removal batch; `t1` begins, capturing `P1`'s fragment as its content; the batch then stores `removal_tid = NonTransactionalTID` on `P1`, which makes it invisible to `t1` at once | `property` | the C++ produces exactly what the trace shows; the row states a guarantee the feature does not give against a non-transactional writer. Spec defect S13 is the qualifier it needs. Trace `traces/f4-nontxn-drop-loses-visible-data.txt`, 17 states; 51,572 distinct on the run that produced it and 52,002 on a re-run, which is the first-violation noise every such count carries | none, and none is available inside the design: a non-transactional removal has no CSN for a snapshot to be compared against, so the row changes, not the code |
| F3 | `Merge`: two sessions, `Parts = {P1, P2, M12}`, `Tasks = {i1}`, `TID_MAX = 3`, `CSN_MAX = 36` | `NoPrematureDelete` | `t1` inserts `P1` and `P2` and commits; the merge task begins `t2` and is killed while it is publishing `M12`; the task's statement rollback stamps `RolledBackCSN` on `M12` and outdates it; `CleanupGrab` takes `M12` while `t2`, rolled back but not yet finalized, is still in `running_list` | `property` | the step quantifies over the transactions that can still issue a read, not over every entry of `running_list`. The same trace exposed model defect `M9`, the merge task's result part not being pinned, which is fixed in the same change | none, the code is right |
| F2 | `SetSnapshotF2`: one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`, `SNAPSHOT_TARGETS = {34}` | `NoPrematureDelete` | `t1` inserts `P1` and commits at CSN 34; `t2` takes snapshot 34, drops `P1` and commits at CSN 35; `t3` begins at snapshot 35 and `SET TRANSACTION SNAPSHOT 34` lowers its read snapshot without moving its `snapshots_in_use` entry; `CleanupGrab` moves `P1` to `Deleting` while `t3` can still read it | `code` | the model variant `SET_SNAPSHOT_PROTECTS` encodes the proposed fix and `MC_SetSnapshotF2Fixed` runs the same configuration green | split `getOldestSnapshot` into a locked entry point and an unlocked body, give `TransactionLog` a `setSnapshotForRunningTransaction` that moves the entry under `running_list_mutex`, refuses a target below `tail_ptr` and refuses a transaction no longer in `running_list`, hold that mutex in `removeOldEntries` from the oldest-snapshot read through the `tail_ptr` store, **and** order `grabOldParts`' removal decision against the registry, because moving the entry alone leaves the decision already in flight; see below |
| F1 | `Base` at `TID_MAX = 3`, `CSN_MAX = 38`, the first run at the transaction count the scenario matrix asks for | `Atomicity` | `t1` inserts `P1` and commits at CSN 34; `t2` takes snapshot 34, drops `P1`, inserts `P2`, and `CommitCreateCSN` writes CSN 35 into the log, which the updater loads while `t2` is still storing; `t3` takes snapshot 35, locks and stores its own removal of `P2`; `t3`'s `SELECT` then reads `{}` | `property` | `AtomicityStep` now excludes the reader's own removals, `h.removing[t]`, from `C`. The design document's row states only the writer's half of the same precedence, recorded in section 3 as S1 | none, the code is right |

### F7, withdrawn {#f7}

Reported in round 1 as a code defect on `Atomicity` and **withdrawn in round 1's review**. The counterexample
was produced by the model, not by the code: `NtBatchStore` wrote `h.removers[p]` in its own step, one or more
steps after `StorePublish` had made the record visible, and every property that reads the visibility oracle was
therefore blind for that window. With the ghost written at the publishing step, which is where it belongs
because a non-transactional removal has no commit point, the shape is green and so is every torn-read shape:
`h.csn[NonTransactionalTID]` is `NonTransactionalCSN`, below every snapshot, so a target stamped before the
read ends is excluded from the writer's set that `AtomicityStep` compares against. `MC_NonTxnF7` no longer
violates -- 75.3 million distinct states in 600 seconds with no violation -- and the module is deleted. The
ghost lag is model defect M16.

What survives the withdrawal is a mechanism and a gap, and neither is a defect claim.

The mechanism is real. `MergeTreeData::getVisibleDataPartsVector`
(`src/Storages/MergeTree/MergeTreeData.cpp:9797`) builds the parts vector under `lockParts` inside
`getDataPartsVectorForInternalUsage` and then calls `filterVisibleDataParts` (`:9819`) with no lock held, so a
reader can judge one target of a batch before it is stamped and another after. Both callers of the batch hold
`lockParts` for its whole life, so filtering inside the lock -- which `getVisibleDataPartsVectorUnlocked`
(`:9782`) already does for callers that have one to give it -- would make the batch atomic to readers.

The gap is that no property here states that. `Atomicity` cannot: it is stated over a committed writer `u`, and
a non-transactional statement has no `u`. Nothing in the code or in the spec promises a non-transactional
statement is atomic to readers either, so the honest reading is that this is the same limit spec defect S13
records for `StableRead`, `NoLostRead`, `NoFutureRead` and `NoLostVisibleData`, one row longer. A property that
did state it -- a read never contains a strict, non-empty subset of the targets of one batch -- is writable and
is owed to plan 4, task 5 (isolation properties revisited with mutations), which is the task that
next restates the properties over what a read captured; until it exists, the mechanism is recorded and
unchecked.

### F12 in full: an uncommitted part reborn as an ancient one {#f12}

`VersionMetadataOnDisk::loadMetadata` ends with the arm for parts written before transactions existed
(`src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:87-93`): a part directory with no
`txn_version.txt` and no temporary file gets `creation_tid = Tx::NonTransactionalTID` and
`creation_csn = Tx::NonTransactionalCSN`. `VersionInfo::isVisible` answers true for such a record at every
snapshot, so the part is visible to everyone, for ever, and no rollback can reach it.

Model defect `M34` argued that arm unreachable for a part a transaction created, and the argument is about
**order** and is correct as far as it goes: `MergedBlockOutputStream` calls `setAndStoreCreationTID`
immediately after `createDirectories`, and `MergeTreeData::loadDataParts` skips directories whose name begins
with `tmp`, so by the time the loader will look at a part its metadata has been written. What the argument does
not cover is **durability**, and that is this finding.

The part directory's own name is a dentry in the **parent** directory. `txn_version.txt` and
`txn_version.txt.tmp` are dentries in the **part's** directory. They are different directories and the kernel
writes them back independently. In the write path nothing syncs the part's directory unless
`fsync_part_directory` is set: `createFile` is `open` plus `close`
(`src/Common/filesystemHelpers.cpp:319-325`), `buf->sync` is `fdatasync` on the descriptor
(`src/IO/WriteBufferFromFileDescriptor.cpp:121`), which makes the file's contents durable and neither of its
names, and the directory `SyncGuardPtr` at `VersionMetadataOnDisk.cpp:360-362` is taken only under that
setting. So a crash that keeps the parent's dentry for the part and loses the part's own dentries leaves
exactly the state `M34` argued could not arise.

The trace is thirteen steps and the transaction is neither committed nor rolled back when the crash lands: it
is still running. `Begin`, `InsertWrite`, the three steps of the creation-TID store, `InsertPreActive`,
`PublishStart`, `FsyncParent`, `Crash`, the restart, `RestartLoadPart`.

This is the opposite direction from findings `F10` and `F11`, and the worse one. Those lose committed data.
This publishes uncommitted data, and it publishes it at a CSN that makes it visible to snapshots taken before
the part was ever written.

**Why this is a code defect and not a documented consequence of the settings.** `fsync_part_directory` and
`fsync_after_insert` are documented as trading durability for speed, and what they trade away is **loss**: a
crash may take rows that were acknowledged. Neither documents that a crash can make rows that were never
committed visible to every reader for ever. That is not a weaker durability guarantee, it is a different
guarantee being broken, and it breaks with no setting having promised it. The classification is provisional
only because the qualifier belongs to whoever owns the settings' contract, not because the behaviour is in
doubt.

The unsafe step is the loader's last arm. On a table that has never used transactions, a part directory
without `txn_version.txt` really is a part from before the feature existed, and calling it non-transactional is
right. On a table that has used them, a part directory without `txn_version.txt` is **indistinguishable** from
one whose metadata dentries a crash took, and the arm cannot tell the two apart
(`src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:87-93`).

**Two fixes, and the second is the one to write first.**

1. Refuse the legacy arm on a table that has ever stored transactional metadata, and treat such a part as
   broken and detached instead. This needs a table-level marker written once, when transactions are first
   used, in the way `format_version` is written once. It is the fix with the better failure mode, because a
   detached part is visible to nobody, and it closes the misreading rather than one of its causes.
2. Take the directory guard unconditionally for the store that writes the creation TID, independent of
   `fsync_part_directory`. It is one `SyncGuardPtr` on one store rather than on every metadata write, and it
   closes exactly the dentry whose loss is misread.

The second is primary for an upstream patch: it is local, it is one line at a call site that already has the
guard, and it needs no new on-disk state. The first is the one that makes the arm safe rather than making its
precondition rare, and it is what a design review should ask for.

**The model variant is the second**, `CREATION_TID_STORE_SYNCS_DIR`, because it is a statement about the disk
layer and this model has one. `MC_CrashF12Fixed` runs the same configuration green, 21,804,360 distinct states.
The first has no variant here: the model has one table and no format marker, so "has this table ever used
transactions" is not a question it can ask.

### F11 in full: a log entry pruned while the only durable CSN is missing {#f11}

The code documents this window and does not close it. `TransactionLog::removeOldEntries` opens by calling the
tail safe, and qualifies it (`src/Interpreters/TransactionLog.cpp:286-289`):

> Try to update tail pointer. It's (almost) safe to set it to the oldest snapshot because if a transaction
> released snapshot, then CSN is already written into metadata. Why almost? Because on server startup we do not
> have the oldest snapshot (it's simply equal to the latest one), but it's possible that some CSNs are not
> written into data parts (and we will write them during startup).

and then says what is missing (`:305-307`):

> Also we write CSNs into data parts without fsync, so it's theoretically possible that we wrote CSN, finished
> transaction, removed its entry from the log, but after that server restarts and CSN is not actually saved to
> metadata on disk. We should store a bit more entries in ZK and keep outdated entries for a while.

The counterexample takes the branch the first comment names. It is not the case where the CSN was written before
the crash and lost; it is the case where the **startup itself** writes the CSN, and writing it during startup is
not enough, because that write is not synced either.

The C++ call sequence, in the order the trace takes it:

1. `MergedBlockOutputStream`'s constructor (`src/Storages/MergeTree/MergedBlockOutputStream.cpp:74`) calls
   `setAndStoreCreationTID`, which writes `creation_tid` with no CSN through
   `VersionMetadataOnDisk::storeInfoToDataPartStorage` (`VersionMetadataOnDisk.cpp:340`): temporary file,
   `buf->sync`, rename. Without `fsync_part_directory` the guard at `:361-363` is not taken, so the rename is a
   dentry the page cache still owns.
2. `MergeTreeData::renameTempPartAndReplaceImpl` publishes the part under its final name, and
   `MergeTreeData::Transaction::commit` makes it `Active`.
3. `TransactionLog::commitTransaction` (`:419`) appends the sequential `csn-` znode. The transaction is
   committed from this point, whatever the client is told.
4. The kernel writes back the part's own directory and its parent, so the final name and the record naming
   `t1` with no CSN are both durable. The server dies before `afterCommit` (`MergeTreeTransaction.cpp:321`)
   stores the CSN.
5. The restart loads the log, and `MergeTreeData::loadDataPart` reaches `loadAndUpdateMetadata`
   (`VersionMetadata.cpp:601`), whose `updateCSNIfNeeded` finds `t1`'s CSN in the log and whose `storeInfo`
   writes it back. Unsynced again, for the same reason as step 1.
6. `isServerCompletelyStarted` becomes true, `removeOldEntries` moves `tail_ptr` to `getOldestSnapshot`, which
   with no running transaction is `latest_snapshot`, and the removal loop prunes `t1`'s entry.

The state `LogEntryNeeded` reports is the one at the end of step 6: the entry is gone and the only durable copy
of the record is the one without a CSN. That is the **obligation** violated, not the harm; the harm needs one
more crash, which `RESTARTS_MAX = 1` does not allow.

**The harm is exhibited rather than argued.** `MC_CrashHarm` is the same configuration at `RESTARTS_MAX = 2`
checking one property, `F11Harm`, which says the loader never stamps `RolledBackCSN` over a creation whose
transaction committed and whose entry was pruned. It is red in 25 states, 125,290 distinct, trace
`traces/f11-harm-committed-creation-rolled-back.txt`: the six steps above, then a second crash that takes the
loader's unsynced repair with it, then a second restart where `tryGetCSN` answers `RolledBackCSN` for a tid the
log no longer has and the part is stamped rolled back. `loadDataPart` deactivates such a part at `:2686-2691`
and the cleanup thread may then remove it.

`F11Harm`'s antecedent requires the durable record to **name** the committed creator and to carry no CSN, and
requires the resolution to be what produces `RolledBackCSN`. Without those conjuncts the property also fires on
finding `F10`'s tmp-only shape, whose `RolledBackCSN` comes from the `DummyTID` record rather than from the
pruned entry; the first run of it did exactly that, in 24 states, and the trace pinned a bystander. The
conjuncts are what make the trace this finding's.

The finding stands independently of the two model defects the same world produced. In the trace the `Fsync`
that makes the payload durable is **state 4**, before the crash, so the part's rows are legitimately durable
and `M48`, which let a sync after a crash bring rows back, cannot be what put the part in the antecedent. The
trace was re-taken after both `M48` and `M49` were repaired and is the same twenty-two steps.

The transaction in the trace is never acknowledged. The crash lands at the commit point, before
`executeCommit` reaches `waitForCSNLoaded`, so `h.outcome[t1]` is `None` and `AckedWriteIsDurable` says nothing
about this shape. That is the argument for `LogEntryNeeded` being a property of its own rather than a corollary
of the durability row.

**The fix.** It has two halves, and the first is what makes the second terminate.

The write that `removeOldEntries` relies on is the one `VersionMetadata::loadAndUpdateMetadata` performs when
`updateCSNIfNeeded` recovers a CSN from the log. It reaches `storeInfoToDataPartStorage`, which takes its
directory `SyncGuardPtr` only under `fsync_part_directory`
(`src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:360-362`). That guard is the natural hook:
the startup repair is precisely the case the truncation comment leans on, so it is the one write that must be
durable whatever the setting says.

The second half is the TODO's own "keep outdated entries for a while". A part whose record was written with
that guard skipped carries a "record written, rename not yet fsynced" bit, set where the guard is skipped and
cleared when the directory is synced, and the removal loop keeps an entry whose CSN only such a record carries.
Without the first half nothing clears the bit when `fsync_part_directory` is off, and bounded retention
becomes an unbounded log; with it, the retention lasts exactly as long as the repair takes to reach the disk.

**What neither half reaches.** The comment at `src/Interpreters/TransactionLog.cpp:304` names the other case
in the same breath: a table that was not attached during startup, for example one detached permanently. Its
parts are not loaded, so no in-memory record exists to carry the bit and nothing can report them. The fsync of
the startup repair covers attached tables and only those. For the rest the TODO's own first proposal is the
only one that applies, a retention window that keeps entries for a while rather than exactly as long as some
part needs them, and this model has no variant for it: the model has one table and cannot express a table that
is absent at startup. That is stated here rather than left for a reader to discover from the green.

**The model variant reads only what the process can see.** `UnsyncedCsnOwners` is built from `part[p].mem` and
that bit, and deliberately not from `disk[p].durable`. An earlier form of this variant did read the durable
layer, which is the layer a crash keeps and which no running server can inspect: at the removal step of this
finding's own trace the process's `txn_version.txt` already carries the CSN, so a set derived from that
comparison would have been empty and the entry pruned exactly as it is today. That variant was an oracle rather
than a fix, and the entry said so of `MergeTreeData`. `MC_CrashUnsyncedFixed` runs green under the re-derived
variant, 37,303,778 distinct states, with the eleven history-free rows of the `Crash` roster on it.

The other proposal, holding `tail_ptr` back to the oldest start CSN of such a transaction, has **no variant
here**, and the reason is a model defect rather than a judgement: `RestartLoadLog` does not rebuild
`tlog.tid_start`, so after a restart the model does not know the start CSN the C++ reads out of the znode. That
is `M47`, and its placement carries the variant with it.

Setting `fsync_part_directory` also closes this window, because it closes every window in which a metadata
rename is not durable. It is not offered as this finding's fix: it is finding `F10`'s mitigation, it is off by
default, it costs an fsync on every metadata store, and it leaves the truncation logic depending on a setting
rather than on an invariant it enforces itself.

### F10 in full: an acknowledged part discarded at restart {#f10}

`MC_CrashF10` is the `Crash` scenario with the metadata rename left unsynced and the other two settings on, so
that this one loss is the only one in play. It is red on `AckedWriteIsDurable`, trace
`traces/f10-acked-part-discarded-without-fsync.txt`, a twenty-five step behaviour. The distinct-state count at
the violation is not reproducible, as no first-violation count is: three runs of the module gave 97,483,
141,070 and 395,095, which is worker scheduling deciding which branch is fingerprinted first, not a change in
the model.

That combination of settings is not one upstream can produce. `FSYNC_OUTER_RENAME` and
`FSYNC_PART_DIRECTORY` are one setting there, `fsync_part_directory`, so a real server has the metadata rename
and the part directory's rename either both synced or both unsynced. The module splits them for one reason: with
both unsynced the part is lost outright in fewer steps, so that is the violation TLC reports, and the
reclassification this finding is about never gets to the surface. The split makes the deeper shape the
shallowest one. What the finding claims about upstream is therefore the shape and not the configuration: at
`fsync_part_directory = 0` both losses are live, and the run with all three settings off, 494,552 distinct
states, is the one that matches a real default installation.

The twenty-five steps are: `t1` begins, `InsertWrite` creates `P1`, the creation-TID store runs its three
steps, `InsertPreActive` gives the part directory its final name, `P1` is published `Active`, `t1` commits
through `CommitCreateCSN`, `afterCommit` stores the creation CSN, the flip and the finalize run, the updating
thread loads the entry and publishes the snapshot, and `CommitAck` tells the client the write is durable. Then
`Crash`. At `FSYNC_PART_DIRECTORY = FALSE` each store made the tmp file durable and left the rename in the
cached layer alone, so what survives is a part directory under its final name holding a
`txn_version.txt.tmp` and no `txn_version.txt`. `RestartLoadPart` reads that through `LoadedRecord`'s tmp-only
case, which is `VersionMetadataOnDisk::loadMetadata`'s second case: `DummyTID` with `RolledBackCSN`, whose
comment says "Transaction was not committed if *.tmp file was not renamed, so we should complete rollback by
removing part". The part loads `Outdated` in the violating state, with `creation_tid = DummyTID` and
`creation_csn = RolledBackCSN`, while `h.outcome[t1]` is `Acked`. `CanBeRemovedWith` accepts a rolled-back
creation at once, so the cleanup thread then removes it; the property fires at the load, one step before that.

The loader is not skipping a lookup it could have made. It **cannot** make one: the creating TID died with the
rename, and the code refuses on purpose to read the tmp file for it. `updateCSNIfNeeded` consults the log only
where `creation_csn` is unset (`VersionMetadata.cpp:392-410`), and `RolledBackCSN` is not unset, so an extant
Keeper entry cannot help. The loading tree reaches the same verdict independently, classifying a tmp-only
directory as rolled back without consulting Keeper (`MergeTreeData.cpp:2256-2267`).

Two windows, not one, and the settings separate them. With the metadata rename unsynced the part survives and
is reclassified, which is the shape above. With the data files unsynced the part does not survive at all: the
same module at `FSYNC_AFTER_INSERT = FALSE` is red on the same invariant, at a first-violation count of
494,552, with the part absent rather than deleted, because `loadColumnsChecksumsIndexes` would fail and
`MergeTreeData.cpp:2618-2676` marks such a part broken. So `fsync_part_directory` alone mitigates the tmp-only
metadata shape and does nothing for row durability; the fix needs both settings.

What upstream promises is the question this finding turns on, and the answer is that it promises nothing here.
Neither `fsync_after_insert` nor `fsync_part_directory` is on by default (`MergeTreeSettings.cpp:642`, `:646`),
and the tree says so in as many words in the durability section of the NATS engine's documentation
(`src/Storages/NATS/StorageNATS.cpp:1681`): a plain process kill does not expose it, because the kernel keeps
the page cache, while a device-level power loss or an unclean host or kernel reset does. That is why the
classification is `property` and not `code`. What the finding adds to the documented limitation is the shape:
the failure mode is not "the newest data is missing" but "the loader reclassifies an acknowledged part as
rolled back and the cleanup thread deletes it", with `loadMetadata`'s comment asserting the opposite of what
Keeper says.

It duplicates neither of its neighbours. `F8` is the `EverythingVisibleCSN` read leak that `RollbackNoLeak`
catches and has nothing to do with fsync. The `removeOldEntries` TODO (`TransactionLog.cpp:284-307`) is a
different mechanism: there a final `txn_version.txt` survives with its TID and loses its CSN after the Keeper
entry has been pruned, so the TID cannot be resolved; here the final file never existed, the surviving
temporary file's TID is deliberately discarded, and no log truncation is involved at all. That TODO is
`LogEntryNeeded`'s target, and `LogEntryNeeded` is not in the tree yet.

Spec defect `S19` records that the design document's `Crash` row asks for `AckedWriteIsDurable` across a
restart at both values of `FSYNC_PART_DIRECTORY`, which the storage cannot give at `FALSE`.

### F9 in full: removing a part whose creation is not committed {#f9}

`SET TRANSACTION SNAPSHOT 3` is not a point on the CSN line. `Tx::EverythingVisibleCSN`
(`src/Common/TransactionID.h:41`) makes `VersionInfo::isVisible` return true for every part before it looks at
a creation or a removal CSN (`VersionInfo.cpp:157-158`), under a comment that says what it is for: "Special
snapshot for introspection purposes". `executeSetSnapshot` accepts it by name
(`src/Interpreters/InterpreterTransactionControlQuery.cpp:144`). Two things follow. The read is `F8` below and
is intended. The write is this finding and is not.

**The route.** A transaction at that snapshot sees a part whose creating transaction is still running. At
every other snapshot `VersionInfo::isVisible` answers `std::nullopt` for a part with no CSN, and its caller
`VersionMetadata::isVisible` then asks the log and returns invisible while the answer is `Tx::UnknownCSN`
(`VersionMetadata.cpp:86-87`); it does not wait for the creator. Here it
never gets that far. `DROP PARTITION` in that transaction
collects `Active` and `Outdated` visible parts (`src/Storages/MergeTree/MergeTreeData.cpp:8241-8255`) and
hands them to `removePartsFromWorkingSet`, whose enrolment loop skips a part whose `creation_csn` is
`Tx::RolledBackCSN` (`:7044-7046`) — and an unfinished creation carries `Tx::UnknownCSN`, not
`Tx::RolledBackCSN`, so the skip does not apply. `MergeTreeTransaction::removeOldPart` then calls
`lockRemovalTID`, which refuses a removal already locked or already committed and nothing else
(`VersionMetadata.cpp:195-248`), and `setAndStoreRemovalTID`, which validates the record it is about to store.
That record has `creation_csn = 0` and a `removal_tid` that is not the `creation_tid`, and `validateInfo`
raises `LOGICAL_ERROR`, "creation_csn is not set while removal_tid is not {}" (`:555-557`). The invalid record
is never written: `validateInfo` runs before `storeInfo` (`:345-347`).

**Shape 1, an exception to the client.** The store is outside the `NOEXCEPT_SCOPE` in `removeOldPart`, which
covers the two enrolment vectors and nothing else (`MergeTreeTransaction.cpp:222-225`), so the `LOGICAL_ERROR`
leaves `DROP PARTITION` as an ordinary query failure: the query's exception callback calls `txn->onException`
(`executeQuery.cpp:3372`) and the error reaches the client. It is still a logical error reached by two
ordinary statements, and it leaves the part in the transaction's `removing_parts` with the removal lock taken,
because the `NOEXCEPT_SCOPE` ran first. `MC_SetSnapshotF9Sibling` produces it, trace
`traces/f9-uncommitted-creation-removal-validateinfo.txt`, 15 states.

**Shape 2, a termination.** If the creating transaction rolls back between the `:7044` skip reading its
`creation_csn` and the store reading it again, the record is `creation_csn = RolledBackCSN` beside
`removal_tid = t2`, which `validateInfo` accepts, since `removal_csn` is not set yet, and which is therefore
stored. The remover's commit then stamps a real removal CSN over it, and `creation_csn > removal_csn`
(`:561-565`) raises inside the `noexcept` `afterCommit` (`MergeTreeTransaction.cpp:321`), which terminates the
process. The window is real: `MergeTreeTransaction::rollback` stamps the CSN through `setAndStoreCreationCSN`
with no data-parts lock held (`:412-417`), while the skip and the store are two separate reads of the same
version metadata. The trace is `traces/f9-rollback-after-skip-window.txt`, 29 states, produced on
`SetSnapshotF9SiblingFixed` under the witness `F9FixInFlightOnly`, which narrows the fix below to the
in-flight half alone: red on `Assert_validateInfo` at 417,821 distinct. The frame the assertion fires on is
`op |-> "RemovalCSN"`, `err |-> "LOGICAL_ERROR"`, `noexcept_owner |-> TRUE`, which is the guard of
`NoexceptFrameDown`, the one step that writes `h.down_cause`. That the shape terminates is a verdict and not
an inference from the frame: the same witness checked against `NoAvoidableTermination` alone is **red at
493,742 distinct in 5 s**, on a thirty-state behaviour of the same shape whose last step is
`NoexceptFrameDown`, leaving `down_cause |-> "Other"` and the server `Down`. The 29-state trace is one step
shorter because it stops at the assertion, which is why its last state still reads `down_cause |-> "None"`.
Both rows are on `MC_SetSnapshotF9SiblingFixed`'s roster. The witness therefore
carries two things at once: the second shape of this finding, and the argument that a fix narrowed to the
in-flight half alone is not enough.

**The fix.** Refuse the removal, not the read. For a transactional remover, and only for one, which is the
qualifier `!tid.isNonTransactional()`, `VersionMetadata::lockRemovalTID` should throw `SERIALIZATION_ERROR`
when `creation_tid != remover_tid && !isCreationCommitted()`. `SERIALIZATION_ERROR` is retryable and is the
error its two neighbouring refusals already use. `VersionMetadata::isCreationCommitted` (`:150-159`) is
already in the file and is already read for this purpose on the non-transactional path, through
`isCreatedByUncommittedTransaction` and the refusal inside `setAndStoreRemovalTID` (`:179-184`) whose comment
gives the same reason: a record `validateInfo` rejects and that cannot be repaired on restart. It answers
false for both shapes above, a creation still in flight and a creation already rolled back, and covering both
is what makes the refusal more than the `:7044` skip repeated one frame down: a check written over
`isCreatedByUncommittedTransaction` instead would answer false for a creation already rolled back and so would
miss shape 2, the window that terminates. The own-creation exemption is necessary: a transaction that inserts
a part and then drops it has `creation_tid = removal_tid` and `creation_csn = 0`, which `validateInfo`
accepts.

The qualifier is load-bearing too. `lockRemovalTID` has two non-transactional callers,
`NonTransactionalRemovalLocks::lock` (`MergeTreeTransaction.cpp:145`) and
`VersionMetadataOnDisk::setAndStoreNonTransactionalRemovalTID` (`VersionMetadataOnDisk.cpp:103`), and that
path tolerates a rolled-back creation deliberately: `preparePartForRemoval` lets a part whose `creation_csn`
is `Tx::RolledBackCSN` past its own check and gives it a non-transactional removal TID while parts are being
loaded (`src/Storages/MergeTree/MergeTreeData.cpp:2516-2542`). An unqualified refusal would start throwing
there.

Refusing rather than waiting for the creator to finish, because a wait would happen with the data-parts lock
and the remover's transaction mutex both held, would need cancellation handling, and would have to
re-evaluate committed against rolled back after waking anyway. Refusing is what the optimistic conflict
handling around it already does.

The model variant is `REMOVAL_REFUSES_UNCOMMITTED_CREATION`, a disjunct of `EnrolRefused` in `Server.tla`, and
it matches that policy: it is on the transactional enrolment paths only, it exempts a non-transactional
creation and a creation the remover made itself, and it refuses when the effective creation CSN — the one in
memory, or the log's answer for the creating TID — is `UnknownCSN` or `RolledBackCSN`, which is
`!isCreationCommitted` written out. `MC_SetSnapshotF9SiblingFixed` runs `MC_SetSnapshotF9Sibling`'s
configuration green under it. What the variant does not model is what a literal change to `lockRemovalTID`
would do to the non-transactional batch, which has a `CreatedByUncommitted` guard of its own in
`NtBatchPreflight`. The
alternative, making `isVisible` hide a rolled-back or uncommitted creation at `EverythingVisibleCSN` too,
closes `F8` and `F9` together and is one line, but it changes what the introspection snapshot shows, which is a
product decision rather than a bug fix.

**What this finding was in round 3b and is not now.** It was stated over a part whose creation had *already*
been rolled back when the drop enrolled it, and that shape is unreachable: the `:7044` skip catches it. The
model enrolled every visible part with no creation test, which was the asymmetry — the same skip was cited at
`NtDropWrite` for the non-transactional batch and missing from `DropLock`. `DropLock` in `Server.tla`
now carries it, `MC_SetSnapshotF9` no longer produces anything and is deleted, and the finding is the sibling
the round-3b reviewers pointed at, re-derived and confirmed at the bounds above.

### F8 in full: the introspection snapshot reads a rolled-back creation {#f8}

Everything means everything, the parts a rolled-back transaction created included. Such a part keeps
`creation_csn = Tx::RolledBackCSN` and sits `Outdated`; `getVisibleDataPartsVectorForInternalUsage` collects
`Active` and `Outdated` alike and filters by `isVisible`, which says yes at this snapshot before it looks at
any CSN. So a transaction there reads data that no committed transaction ever produced. The model states that
as `RollbackNoLeak`, and it is red.

The classification is `property` rather than `code`: the comment beside `VersionInfo.cpp:157-158` says this is
deliberate, and the regression test asserts it, comparing a snapshot-3 read against all `Active` and
`Outdated` parts (`tests/queries/0_stateless/01173_transaction_control_queries.sql:81`). What is wrong is the
design document, whose isolation and rollback rows are stated without the qualifier. That is spec defect
`S18`.

`RollbackNoLeak` keeps its unqualified statement, so that it is checked as the document words it at every
ordinary snapshot, and the two modules that run the introspection target leave the row off their roster: it is
red in `MC_SetSnapshotF8`, which is the module that owns the finding, and off `MC_SetSnapshotF2SpecialEV`'s
roster, which is verifying `F2`'s fix at the same target. That is the treatment `MC_NonTxnDrop` already gets
for `ActiveSetShape` and finding `F5`. The three termination rows are a different matter and are back on
`MC_SetSnapshotF2SpecialEV`'s roster: `F9` needs a second session to hold a creation open while another
transaction drops it, and that module has one session.

### F6 in full {#f6}

A precommitted part that `Transaction::commit` finds already covered takes the obsolete branch
(`src/Storages/MergeTree/MergeTreeData.cpp:11305-11317`): a warning is logged, `remove_time` is set to zero,
and the part is moved to `Outdated`. Nothing is stamped on its version metadata, and the loop above
(`:11279`) has already skipped `addNewPartAndRemoveCovered` for it, so it is not in `creating_parts` either and
`afterCommit` will never give it a creation CSN. It keeps `creation_tid = t` and `creation_csn = 0` for the
rest of its life.

Three things follow, and the third is the violation.

`VersionMetadata::canBeRemoved` returns false for an empty `removal_tid`, so `grabOldParts` never takes the
part however old it is, `remove_time = 0` notwithstanding. `VersionInfo::isVisible` returns true for it to its
own creator, through the `creation_tid == current_tid` clause, so `t` still sees it. And if `t` then removes
it -- a `DROP PARTITION` in the same transaction is enough -- `removeOldPart` succeeds, because
`setAndStoreRemovalTID` with a transactional TID writes no removal CSN and `{creation_csn = 0, removal_tid = t,
removal_csn = 0}` passes `validateInfo`. The commit is where it breaks: `afterCommit` calls
`setAndStoreRemovalCSN(csn)`, the record becomes `{creation_csn = 0, removal_csn = csn}`, and `validateInfo`'s
rule that a zero creation CSN implies a zero removal CSN raises `LOGICAL_ERROR`. `validateInfo` throws rather
than asserting, so this is not a debug-only path, and the exception has nowhere to go: `afterCommit` is
declared `noexcept` (`src/Interpreters/MergeTreeTransaction.cpp:321`), and so is its caller
`TransactionLog::finalizeCommittedTransaction` (`src/Interpreters/TransactionLog.cpp:490`). The throw therefore
terminates the process. It is the `noexcept` declarations that do this, not the `NOEXCEPT_SCOPE` macro, which
belongs to `Transaction::commit` a layer below and is where the fix goes rather than where the throw happens.

`NonTxn` is the first scenario that reaches it, because it is the first in which the covering branch is live
at all: the empty part of a non-transactional `DROP PARTITION` is the first `Active` part that can appear over
a range another session is publishing into. Model defect `M10` already records that the same branch is dead in
`Merge` for the same reason.

**The fix.** Stamp the obsolete part the way the statement rollback stamps a part that does not make it in:
`setAndStoreCreationCSN(Tx::RolledBackCSN)` before `modifyPartState(part, Outdated)`. That is the one
transition `setAndStoreCreationCSN`'s own `chassert` exempts by name (`VersionMetadata.cpp:125-130`), for the
neighbouring case of an empty part added to a transaction and immediately rolled back. It makes the part
invisible to everyone including its creator, so nothing can enrol it; it makes `canBeRemoved` true, so the
cleanup thread takes it instead of leaving it for ever; and it closes route 3 of finding F5 as a side effect,
because an invisible part is not one a `DROP PARTITION` enrols and restores.

The stamp is unconditional: it runs on every obsolete part the branch takes, whether or not the commit that
takes the branch has a transaction. That is not an incidental detail. Route 3 of finding `F5` is reproduced by
a **non-transactional** commit, so the claim that this fix closes it holds only if the stamp also runs when
`txn` is null. The code permits exactly that: `setAndStoreCreationCSN`'s `chassert`
(`VersionMetadata.cpp:125-130`) admits both shapes the branch can present, `creation_csn == 0` for a part a
transaction created and `Tx::NonTransactionalCSN` for one a non-transactional statement created, and the second
is the case the comment beside it was written for. The model variant is
`OBSOLETE_IS_ROLLED_BACK`.

**Where the stamp goes.** Not where the obsolete branch is. `NOEXCEPT_SCOPE` opens at
`src/Storages/MergeTree/MergeTreeData.cpp:11289` and the branch sits inside it, so a `setAndStore...` written
there would put a disk write on a path where an exception terminates the process, which is the very outcome the
finding is about. The stamp belongs before the scope, beside the covered-parts collection that was moved out of
it for the same reason: the comment at `:11234-11237` says so in as many words, "call
`addNewPartAndRemoveCovered` before `NOEXCEPT_SCOPE`, because `lockRemovalTID` inside it can throw
`SERIALIZATION_ERROR`". The covering part is already computed there, so the branch has everything it needs a
scope earlier.

**Why the downstream repair does not save it, and why that is a race.** `updateCSNIfNeeded`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:392-410`) runs on every store and does fill a
missing `creation_csn` from the transaction log, so it looks like the repair, and sometimes it is one. What it
asks is `tryGetCSN` (`VersionMetadata.cpp:476-482`), whose **first** step is `TransactionLog::getCSN`, a lookup
in the loaded map `tid_to_csn` under `TransactionLog::mutex` (`TransactionLog.cpp:615-625`). If the updating
thread has already loaded this transaction's commit entry into that map, the lookup answers the real CSN, the
record is repaired, and nothing terminates. The running-list test that follows is reached only when the map has
nothing.

The termination therefore needs the window before the entry is loaded, and that window is the common case on
this path, because the storing thread is the committing thread itself. `afterCommit` is called at
`src/Interpreters/TransactionLog.cpp:508`, and the transaction is erased from `running_list` only afterwards,
at `:511-515`; with no entry in `tid_to_csn` yet, `tryGetCSN` still finds the transaction running and returns
`UnknownCSN` rather than the CSN that was just allocated. The model's trace shows exactly that state,
`tlog.tid_to_csn = <<0, 0>>` at the violating step. A reproducer has to keep the updating thread away from the
entry for the length of the commit, which is what makes this a race rather than a certainty.

**The two guards that look as though they should have caught it.** Both test `isNonTransactional` and so watch
a different door. `isCreationCommitted` (`VersionMetadata.cpp:155`), which `isCreatedByUncommittedTransaction`
reads, consults the log only for a creation TID that is not non-transactional; and the `creation_in_flight`
refusal inside `setAndStoreRemovalTID`'s update function (`:179`) fires only when the removing `tid` is
non-transactional. F6's removal is transactional, by the part's own creator, so neither guard is on its path.
That is the answer to the question of whether the code already refuses this shape somewhere: it refuses the
non-transactional sibling of it, and only that.

### F5 in full {#f5}

Found on the third run of the `NonTxn` scenario. Three different behaviours reach it, and separating them is
what turned a suspected model defect into a finding.

**The common shape.** A transactional removal is rolled back, and `MergeTreeTransaction::rollback` restores the
part through `MergeTreeData::restoreAndActivatePart` (`src/Storages/MergeTree/MergeTreeData.cpp:7325`). That
function takes `lockParts`, returns early if the part is already `Active`, and otherwise calls `modifyPartState`
to `Active`. It never asks whether the part's range is still free. If anything published inside that range while
the part sat `Outdated`, the restore puts two intersecting parts in the active set, which is the
`LOGICAL_ERROR` "Part {} intersects part {}" the next `getActivePartsToReplace` raises, and which
`ActiveSetShape`'s first clause states.

**Route 1: a covering part published over a range whose removal is in flight.** `t1` drops `P1`
transactionally, which locks `P1`'s removal and moves it to `Outdated` before the transaction commits. A
non-transactional `DROP PARTITION` then runs: its covered set is `getActivePartsToReplace`'s output, `P1` is no
longer `Active`, so the set is empty, the batch has nothing to conflict with, and the empty part `E` is
published `Active`. `t1` rolls back and `P1` returns to `Active` under `E`.

Collecting the covered *Outdated* parts as well was implemented and measured as a fix, on the reading that the
covered-outdated collection at `MergeTreeData.cpp:11248` sits inside an `if (txn)` and so never runs for a
non-transactional commit. It closes route 1 and nothing else, so it is not in the tree; the note stays because
the asymmetry is real and a fix will have to include it.

**Route 2: a part published inside the range of a covering part whose removal is in flight.** The mirror image.
`E` is `Active` over `P1`; `t1` drops the partition transactionally, which outdates `E`; a non-transactional
`INSERT` of `P2` is then published, finds no `Active` covering part, and goes `Active`; `t1` rolls back and `E`
returns to `Active` over `P2`.

**Route 3, which is the committed reproducer.** No removal race at all. A non-transactional `INSERT` of `P2`
loses to `E`, so `Transaction::commit`'s covering branch marks `P2` `Outdated` (`MergeTreeData.cpp:11316`)
without giving it any removal metadata. `P2` is therefore still visible to every reader, and a transactional
`DROP PARTITION` sees both `E` and `P2`, enrols both, and its rollback restores both. Two parts that were never
`Active` together become `Active` together.

**Why the obvious fix is not proposed as sufficient.** Making `restoreAndActivatePart` consult
`getActivePartsToReplace` and decline to reactivate a covered part removes the intersection, and replaces it
with a rollback that did not restore the data it removed, which is exactly what `RollbackRestores` states. The
two properties cannot both hold once a publication has been allowed into the range, so the fix has to be
prevention rather than repair: a publication must not change the active set inside a range whose removal a
running transaction holds locked, and the non-transactional path must see that lock. That is the same
conclusion the covered-outdated asymmetry points at, from the other side. The model variant is therefore
`MC_NonTxnF5`, which produces the finding, rather than a `Fixed` module claiming a fix that was not shown.
What this paragraph gives is a direction and not a patch: an implementer cannot write it from here, and what it
needs first is an executable reproduction at the SQL level. That is why the upstream order above puts `F5`
last.

**A second defect the same routes expose, which no property here catches.** The obsolete branch leaves the
precommitted part `Outdated` with empty version metadata and `remove_time = 0`. `VersionMetadata::canBeRemoved`
returns false for an empty `removal_tid`, so `grabOldParts` never takes it however old it is, and
`VersionInfo::isVisible` returns true for it, so every reader still sees it beside the part that covers it. The
property that would catch it is "an `Outdated` part is either removable or has a live remover", which no
scenario states yet; it is owed to plan 5, task 3 (liveness), whose `OutdatedEventuallyDeleted` is that statement in its
liveness form.

### F4 in full {#f4}

`NoLostVisibleData` says that the fragments a running transaction could read at its own snapshot never
disappear except by its own drops. A non-transactional removal makes them disappear. `NtBatchStore` writes
`removal_tid = NonTransactionalTID`, and `VersionInfo::isVisible`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:171`) returns false for that value before it looks at
any snapshot, so the part leaves every reader's view at once, whatever snapshot the reader holds. The cleanup
thread then agrees: `VersionMetadata::canBeRemoved` returns true for a non-transactional `removal_tid` without
consulting `getOldestSnapshot` at all, so the part can be deleted from disk under the running transaction.

This is the documented limit of the feature rather than a repairable defect. A non-transactional removal has no
commit sequence number, so there is nothing for a snapshot to be compared against, and giving it one would make
it transactional. `NoPrematureDelete` stays green through the same behaviour, because it is stated over the
oracle and the oracle agrees the data is gone; `NoLostVisibleData` is the only property that sees it, and it
sees it correctly.

What the finding changes is the spec, not the code: the snapshot-isolation rows are stated without a qualifier
about concurrent non-transactional writers, and three of them are falsified by one. That is spec defect S13,
and it is why neither `MC_NonTxnDrop.cfg` nor `MC_NonTxnInsert.cfg` checks `StableRead`, `NoLostRead`,
`NoFutureRead` or `NoLostVisibleData`;
`MC_NonTxnF4` is where the last of the four is shown.

### F3 in full {#f3}

Found on the first run of the `Merge` scenario, at 8.9 million distinct states, on a 36-state behaviour.
Classification `property`.

**The state in which the grab happened.** `tlog.running_list = {1, 2}`, `txn[1]` is a session's transaction in
`CommitStoreCreation`, and `txn[2]` is the merge's, `state = "RolledBack"` with `rb_driver` the killing session
and `pc = "RollbackCopyLists"`: the kill has won the compare-and-exchange and the rollback body has not run yet.
`h.creator[M12] = 2`, `M12` is `Outdated` with `mem.ccsn = RolledBackCSN`, no pins and no frames, so
`CanBeRemovedImpl` is true on its first clause and the cleanup pass takes it. `NoPrematureDeleteStep` then asks
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
`CleanupDecide` requires `part[p].pins = {}`, which is `isSharedPtrUnique` (`MergeTreeData.cpp:4150`) and is
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
`VersionMetadata::canBeRemoved` (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:272`) at `:4142`;
`canBeRemoved` calls `TransactionLog::getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:677`), which
returns `snapshots_in_use.front`, still the snapshot `beginTransaction` inserted; the part moves to `Deleting`
and `clearPartsFromFilesystemAndRollbackIfError` (`:4566`) deletes its directory.

**Why `MC_SetSnapshot` is green and `MC_SetSnapshotF2` is not.** The scenario that owns
`SET TRANSACTION SNAPSHOT` cannot reach this shape, for two independent reasons, and both are configuration
rather than model.

The first is the number of transactions. The shape needs a part created by one committed transaction, removed
by a second committed one, and a third transaction running with its snapshot lowered between the two commit
CSNs. It cannot be fewer: while the remover is still running the part is pinned by its own `<<"Txn", t>>` entry
and `mem.rcsn` is still `UnknownCSN`, so `CleanupDecide` is disabled twice over, and once the remover has
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
`snapshots_in_use.front`, which is therefore above the snapshot the transaction actually reads at.

From there the part is lost in three steps. `VersionMetadata::canBeRemoved`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:272`) compares the removal CSN against
`getOldestSnapshot` and answers `true` for a part removed after the lowered snapshot but before the protected
one. `MergeTreeData::grabOldParts` (`src/Storages/MergeTree/MergeTreeData.cpp:4142`) asks exactly that
question, and on `true` moves the part out of `Outdated` and schedules its directory for removal. The running
transaction's next `SELECT` then loses the rows it could read a moment earlier, which is the isolation
guarantee `SET TRANSACTION SNAPSHOT` exists to provide.

`getOldestSnapshot` also feeds `removeOldEntries` (`src/Interpreters/TransactionLog.cpp:308`), so the same
too-high value lets the log entries of that era be truncated while a transaction is still reading at a
snapshot below the new `tail_ptr`.

**Proposed fix, applicable to upstream `master`.** Five functions change, one member declaration, and one
member is added. The count is worth stating, because the first version of this entry said four and named only
the snapshot-registry half; the cleanup half, which is what closes the second shape, adds `grabOldParts` and a
way for it to ask for the oldest snapshot without relocking.

`TransactionLog::getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:677`) splits in two. The body, with
the two `chassert`s and the `snapshots_in_use.front` it returns, becomes a private
`getOldestSnapshotLocked`, declared `const TSA_REQUIRES(running_list_mutex)`; `getOldestSnapshot` keeps its signature and
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

**The two reserved snapshots, which the refusal must not reject.** The refusal above is about log retention,
and it must not be applied to the two values `executeSetSnapshot` accepts by name,
`Tx::NonTransactionalCSN = 1` and `Tx::EverythingVisibleCSN = 3`
(`src/Interpreters/InterpreterTransactionControlQuery.cpp:144`, `src/Common/TransactionID.h:38,41`). Both are
below any real `tail_ptr`, which is at least `MaxReservedCSN`, so a blanket `new_snapshot < tail_ptr` refusal
rejects two statements the server accepts today; that is a compatibility change and not a fix.

They cannot simply be registered either. `removeOldEntries` computes the new tail from the registry and raises
`LOGICAL_ERROR` when it is below the old one (`src/Interpreters/TransactionLog.cpp:313`), so an entry at 1 or 3
takes the server down on the next truncation pass. The fix therefore keeps **two horizons** on the same
registry entry:

- the **cleanup horizon**, what `canBeRemoved` compares a removal CSN against, takes the new value, special
  values included. That is what protects what the transaction can read: at `EverythingVisibleCSN` every part is
  visible (`VersionInfo::isVisible` returns true before any CSN test,
  `src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:157-158`), so nothing may be deleted under it, and a
  horizon of 3 refuses every real removal CSN, which is exactly that.
- the **retention horizon**, what `removeOldEntries` may move `tail_ptr` to, keeps the era the transaction
  began in. A special value never enters it.

The retention horizon also has to be *lowered and never raised*. `beginTransaction` registers
`latest_snapshot` (`TransactionLog.cpp:403`) and `getOldestSnapshot` falls back to `getLatestSnapshot` on an
empty list (`:679-680`), so today every entry is at or below `latest_snapshot` and the tail can never pass it.
A fix that registered an accepted target above `latest_snapshot` — the code accepts any CSN above
`MaxReservedCSN`, a future one included — would break that: the tail would follow the entry up, and the first
pass after that transaction ended would compute a lower tail and raise "Got unexpected tail_ptr" at `:313`.
That is **finding `F2`'s third shape**, produced by the model before the variant was corrected, trace
`traces/f2-future-target-tail-regression.txt`, nine states. `std::min` on the retention value is what closes
it.

**The transaction that is no longer registered, which the new method must refuse.** `SET TRANSACTION SNAPSHOT`
is reachable on a transaction that has already been rolled back, and the fix has to survive that. `executeSetSnapshot`
(`src/Interpreters/InterpreterTransactionControlQuery.cpp:138-149`) tests only that a transaction exists and
that the target is not a reserved CSN; it carries no state check, unlike `executeCommit` and `executeRollback`
beside it. `executeQueryImpl`'s refusal of a query on a failed transaction
(`src/Interpreters/executeQuery.cpp:2690-2695`) exempts `ASTTransactionControl` by name, so that a failed
transaction can still be rolled back, and `SET TRANSACTION SNAPSHOT` is one of those statements. On that path
`TransactionLog::rollbackTransaction` has already erased the transaction's entry from `snapshots_in_use`
(`src/Interpreters/TransactionLog.cpp:583`). Today the statement is harmless there, because
`MergeTreeTransaction::setSnapshot` writes one atomic and touches no list. Under the fix it is not:
`setSnapshotForRunningTransaction` erases and re-inserts `snapshot_in_use_it`, and erasing an iterator that
`rollbackTransaction` has already erased is undefined behaviour. The method must therefore verify, under
`running_list_mutex` and before it touches `snapshot_in_use_it`, that the transaction is still **registered**,
that is still in `running_list`, and refuse with `INVALID_TRANSACTION` when it is not. Registered is the word:
the state becomes `ROLLED_BACK` at `src/Interpreters/MergeTreeTransaction.cpp:382` and the registry entry is
erased only later, at `src/Interpreters/TransactionLog.cpp:578`, so a membership test still accepts a
transaction that is already rolled back. That is enough for the iterator, which is what the check is for;
refusing every state that is not `RUNNING` would need the transaction's state read under the same mutex, and
`MergeTreeTransaction::getState` does not take it. The check belongs under that mutex rather
than in the interpreter: a state test taken before the lock leaves the same window it exists to close.

The model does not reach that path. `SetSnapshot` is guarded on `txn[Cur(k)].state = "Running"`
, so no behaviour offers the statement to a transaction another session has already killed,
and the refusal above is checked by inspection rather than by search. That narrowing is model defect `M17`.

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

**`F2`'s second shape: the cleanup decision and the grab are two moments.** Moving the `snapshots_in_use`
entry under `running_list_mutex` makes every cleanup decision taken *after* the move see the lowered snapshot.
It does not cover a decision already taken, and that is enough to lose the part.
`MergeTreeData::grabOldParts` holds `lockParts` across its whole pass, but it asks `canBeRemoved`
(`src/Storages/MergeTree/MergeTreeData.cpp:4142`), which takes `running_list_mutex` for the length of
`getOldestSnapshot` and **releases** it (`TransactionLog.cpp:677`), and it moves the parts it accepted to
`Deleting` only at the end of the pass (`:4190-4194`). Neither `MergeTreeTransaction::setSnapshot` nor the
proposed `setSnapshotForRunningTransaction` takes a parts lock, so nothing orders a snapshot lowering against
that interval: the pass decides at oldest snapshot 35, the transaction lowers to 34, and the part is moved to
`Deleting` under a transaction that reads it at 34.

The model now says so rather than hiding it. `CleanupDecide` reads the removal condition, records the candidate
and takes the parts lock; `CleanupGrab` performs the state change; `CleanupAbandon` is the pass leaving the part
where it is. With the fix reduced to its `snapshots_in_use` half, the witness `SnapshotEntryOnly` on
`MC_SetSnapshotF2Fixed`, `NoPrematureDelete` is **red** after 280,957 distinct states in four seconds, on the
46-state trace `traces/f2-cleanup-decision-grab-window.txt`. That is the second shape, and it is the reason the
proposed fix is not the two functions the entry first named.

**What the fix has to be.** One guard from the oldest-snapshot read through the state change, or a
revalidation of `canBeRemoved` immediately before `modifyPartState` under the guard that also covers the state
change: `grabOldParts` must not act on a decision the snapshot registry has since invalidated. The
`snapshots_in_use` half stays as described above, because it is what makes any later decision correct; the
cleanup half is what makes the decision in flight correct. `SET_SNAPSHOT_PROTECTS` encodes both halves: the
`SetSnapshot` action moves the entry and refuses a target below the tail, and `CleanupGrab` re-evaluates the
removal condition in the same step as the state change, which is the model's way of writing "under one guard".
**The cleanup half, concretely.** `grabOldParts` already holds `lockParts` for its whole pass. Inside it, and
after the candidates have been collected, it acquires an opaque registry guard from `TransactionLog` —
a small RAII type that holds `running_list_mutex` and exposes `getOldestSnapshotLocked` — obtains the oldest
snapshot through it without relocking, revalidates every candidate against that value, moves the ones that
still pass to `Deleting`, and only then releases the guard. `canBeRemoved` needs an overload that takes the
oldest snapshot rather than fetching it, or the revalidation has to be written out at the call site; either
way the decision and the state change are under one guard, which is what the second shape requires.

Its cost is the mirror of the first half's, and it is larger. `running_list_mutex` is held for the length of
the final phase of a `grabOldParts` pass rather than for one lookup, and `beginTransaction`,
`commitTransaction` and `rollbackTransaction` all take that mutex, so a cleanup pass blocks every
transaction-control statement while it finishes. It is deadlock-free: no `running_list_mutex` critical section
in `TransactionLog.cpp` calls into `MergeTreeData` or takes `lockParts`, so the acquisition order
`lockParts` then `running_list_mutex` is the only one that exists. Contention, not inversion, is the design
question a reviewer has to weigh, and it is why this is filed as an issue with a synchronisation requirement
rather than as a patch.

**What the green runs establish, exactly.** `MC_SetSnapshotF2Fixed` is green at 309,987 distinct states and red
under `SnapshotEntryOnly`, which is the fix reduced to its registry half. That covers both shapes of `F2` at an
ordinary target, at one session, one part, `TID_MAX = 3` and `SNAPSHOT_TARGETS = {34}`, against an abstraction
in which the registry move is one step and the revalidation is in the same step as the state change. It does
**not** establish the tail-publication mutex (model defect `M18`), the sorted re-insert (`M6`), a C++
revalidation that is not atomic with the state change (`M19`), or the special snapshots. The special values are
covered by two modules of their own: `MC_SetSnapshotF2Special` at `NonTransactionalCSN` beside an ordinary
target, green over 116,020 states with the whole roster, and `MC_SetSnapshotF2SpecialEV` at
`EverythingVisibleCSN`, green over 125,673 states with `RollbackNoLeak`, the one row finding `F8` falsifies,
left out.

One refinement survives, and it is the limit of what a two-step model can say: the re-evaluation and the state
change are one action here, so the model checks a fix in which nothing can intervene between them. A C++ fix
that re-checked `canBeRemoved` and then took a different lock to change the state would have the same window
one level down. Model defect `M19` records it. Plan 3's `SnapshotCrash` runs with `SET_SNAPSHOT_PROTECTS = TRUE`
and therefore rests on this variant, which is why it is stated here and not only in `STATE_SPACE.md`.

`getOldestSnapshot` still returns `snapshots_in_use.front`, and both of its `chassert`s still hold: the list
keeps one entry per running transaction, and re-inserting at the sorted position keeps it sorted. Every consumer
of `getOldestSnapshot`, `canBeRemoved` and `removeOldEntries` among them, is protected without being changed.

**The model variant.** `SET_SNAPSHOT_PROTECTS` in `Types.tla` is that fix: `SetSnapshot` in `Server.tla` moves
`protected_snapshot` and the `snapshots_in_use` entry with the snapshot, and refuses a target below
`tlog.tail_ptr`. It necessarily makes the tid order stop being the list order, which is why the sortedness
clause of `Assert_getOldestSnapshot` is conditioned on `~SET_SNAPSHOT_PROTECTS`: under the fix the C++ list is
re-sorted at insertion and the assertion holds, while the model's tid-ordered proxy for it does not. The other
two clauses, the equal membership and the per-entry equality, are checked under both variants.

**What `MC_SetSnapshotF2Fixed`'s green covers, and what it does not.** It covers the refusal. It does not cover
the publication that refusal's soundness rests on. `UpdRemoveOldEntriesSetTail` reads
`OldestSnapshot` and writes `zk.tail` and `tlog.tail_ptr` in one step, where the C++ has a real window between
`getOldestSnapshot` (`TransactionLog.cpp:312`) and `tail_ptr.store(new_tail_ptr)` (`:321`); `SetSnapshot` is
likewise one step here. The model therefore cannot tell a fix that holds `running_list_mutex` across the
truncation pass from one that does not, and that extension is the one part of the proposal whose cost two
paragraphs above are spent justifying. The green says that moving the `snapshots_in_use` entry with the
snapshot and refusing a target below the tail closes this counterexample; it says nothing about whether the
mutex has to be held across the pass. That gap is model defect `M18`.

**The reproduction at two transactions.** `MC_SetSnapshotF2` needs three transactions, because the creator,
the remover and the reader are all transactions there. A non-transactional `INSERT` creates the part without
one, and with that the whole shape fits two: `MC_NonTxnF2` is the insert half of `NonTxn` plus
`SET TRANSACTION SNAPSHOT` and the cleanup thread, one session, one part, `TID_MAX = 2`, `CSN_MAX = 35`,
`SNAPSHOT_TARGETS = {33, 34}`, and its **baseline** is red on `NoPrematureDelete` after 9,132 distinct states
and on `NoLostVisibleData` after 9,193. The trace is 30 states:
`NtInsertWrite` and its store steps publish `P1`; `t1` drops it transactionally and commits at CSN 34; `t2`
begins at 34, lowers its snapshot to 33 with `SET TRANSACTION SNAPSHOT`, and `CleanupGrab` takes `P1`, which is
still visible to `t2` at 33 because a non-transactional creation is visible at every snapshot.

The target has to be 33 here, and that is the part of the shape worth keeping in mind for later scenarios.
`MC_SetSnapshotF2`'s bounds argument says a target of `FirstCSN` makes the property vacuous, because nothing is
ever visible at 33 when every part is created by a transaction that commits at 34 or above. A part created
outside a transaction breaks that: it is visible at every snapshot, so the smallest target is the dangerous one
rather than the empty one. The two traces are `traces/f2-nontxn-creator-two-transactions.txt` and
`traces/f2-nontxn-creator-lost-visible-data.txt`, and they close debt `B2`.

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
guard on `tid.isNonTransactional` and `isCreatedByUncommittedTransaction` in `setAndStoreRemovalTID`
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
| M1 | `LegacyPartRecord` gives a legacy part `mem.sv = 0` (`Parts.tla:27`) while its disk record is `Legacy` (`Disk.tla:20`), which is not `Info`, so `DiskHasInfo` is false (`Disk.tla:42`) and `StoredRecord` falls back to `EmptyInfo` with `sv = -1` (`Parts.tla:155`) | Every store on a legacy part compares an expected `-1` against a tentative `sv` of 0 in `StorePersistStep` (`Parts.tla:241`), takes the `TOO_OLD_VERSION` branch, retries `MAX_STORE_RETRIES` times and ends in `STALE_VERSION`. No legacy part can ever be written, so plan 3's `Crash` runs with a non-empty `LEGACY_PARTS` would be vacuous | **closed**. `StoredRecord` gained an arm that reads a `Legacy` disk record as `LegacyInfo`, so both sides of the comparison are 0 and the first store on a legacy part goes through. `MC_CrashLegacy` is the run that makes the closure worth something: `LEGACY_PARTS = {P1}` with the whole roster plus `LegacyLoads` |
| M2 | `StmtRollbackMark` and `StmtRollbackDrop` acted on `stmt.precommitted \ stmt.attached`, and `Refuse` routed to them only when that difference was non-empty (`Server.tla`, the statement-rollback actions) | The statement rollback skipped a part that `addNewPartAndRemoveCovered` had already attached to the outer transaction, where `MergeTreeData::Transaction::rollback` (`src/Storages/MergeTree/MergeTreeData.cpp:11122`) marks and removes every part of `precommitted_parts`; and a `Refuse` whose precommitted set was entirely attached left the `stmt` record standing, so the leak outlived the statement. Both are dead in `Base`, which has `QUERY_FAULTS_MAX = 0` and an empty `Covers`, so neither `Fail` nor `PublishEnrol` can refuse with a non-empty `stmt.precommitted`; plan 2 gives the sibling scenario a covering relation, which makes `PublishEnrol` live and a `SERIALIZATION_ERROR` between `PublishStart` and `PublishFlip` reachable, and both would have gone live there at once | closed by the final-review fix commit of plan 1 |
| M3 | `UpdRemoveOldEntriesDelete` re-reads `tlog.tid_to_csn` and `tlog.latest_snapshot` on every iteration, while `TransactionLog::removeOldEntries` (`src/Interpreters/TransactionLog.cpp:319-323`) snapshots both once and then loops over the copy | `latest_snapshot` only grows, so the model can delete an entry the C++ would have kept as "the latest one we fetched". The behaviour set is widened, not narrowed, which is safe for every property of plan 2: the only property that reads `h.truncated` is `LogEntryNeeded`, which is plan 3's. The same action also erases `tid_to_csn[t]` in the step that removes the znode, while the code collects `removed_entries` and erases them after the loop under `mutex`, so the model never has the code's window in which the znode is gone and the local lookup still succeeds; and `UpdRemoveOldEntriesDone` is unguarded, so a pass can end with entries the code's loop would still have removed. Both widen the behaviour set in the same direction as the re-read. One half of it does not widen: the code collects `removed_entries` and erases them after the loop under `mutex` (`src/Interpreters/TransactionLog.cpp:324` and `:349`), so the code has a state, znode gone and the local lookup still succeeding, that the model does not. That is a narrowing, and the superset argument plan 3 offers for `LogEntryNeeded` is property-specific: it says nothing about that window and nothing about a pass interrupted part-way | **not closed in plan 3, task 1**, and re-placed in plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), beside the truncation-tail computed-and-published-in-one-step defect that task already owns. The task that made the truncation pass observable did not fix it: `NoOutdatedLookup` is non-vacuous from the Keeper-faults commit on, which is what this row's placement anticipated, but observing the pass and correcting it are different changes. The fix is stated rather than argued and is unchanged: capture the candidate map and the latest CSN once, delete the znodes one entry at a time, erase the local map in one step over the entries that were removed, and enable `UpdRemoveOldEntriesDone` only when the captured traversal is finished. What it costs is why it moved: three new `tlog` fields, a rewrite of all three `UpdRemoveOldEntries*` actions, a projection change in every module that enables `UpdaterGCNext`, and a re-measurement with witness sweeps of `SetSnapshot`, `SetSnapshotFixed`, `Merge`, `Keeper` and `KeeperUnknownWait` |
| M4 | An exhaustive run of `MC_SetSnapshot` at the bounds the scenario matrix gives it does not finish: five runs at `TID_MAX = 3`, `CSN_MAX = 36` were killed, the last at 56,968,754 distinct states after 8 minutes with 4.47 million still queued. The scenario is 3.4 times `Base` at equal bounds, which puts it near 98 million and about 15 minutes | The scenario is checked exhaustively at `TID_MAX = 2`, `CSN_MAX = 35` instead, in 65 seconds, and its witnesses are shown at the matrix bounds in `MC_SetSnapshotWitness`, where a run stops at the first violation. What that costs is debt B1 below | plan 5, task 4 (budget and calibration), which names itself the owner of `M4`; a `VIEW` or `CONSTRAINT` that closes the gap would let the two bound sets become one again. The measurements, the three reductions applied and the one measured and rejected are in `STATE_SPACE.md`, section "The `SetSnapshot` scenario" |
| M5 | `UpdRemoveOldEntriesSetTail` sets `tlog.updated_tail_ptr` inside the branch where the tail actually moves, while `TransactionLog::removeOldEntries` stores `true` at `:302`, before it reads the znode and before the `new == old` early return | The async-loading gate stays armed in the model after a pass the code would have disarmed. It was dead when this row was written, because `sys.completely_started` was `TRUE` and `sys.async_loading_jobs` `0` at init and nothing then in the model wrote either; the restart actions write both, which is what the placement anticipated. It is not a one-line reorder, because the model had no action for a pass that ran and moved nothing | **closed** in plan 3, task 3 (what a restart must not do), by splitting off `UpdRemoveOldEntriesArm`: the pass that gets past the gate, stores the flag and returns because the tail did not move. Its guard is the early return itself, `RetentionHorizon = zk.tail`, so the two actions partition the passes rather than overlapping. The action is **behaviourally dead in every configuration built so far**, and the discriminator is the placeholder znode `TransactionLog::loadLogFromZooKeeper` creates before it reads the children (`src/Interpreters/TransactionLog.cpp:190`): it raises `latest_snapshot` strictly above `tail_ptr`, so the first pass after a start always moves the tail, and every later pass finds the flag already stored. `Merge` measures identical to the state with the action added, which is that emptiness shown rather than argued. It goes live where a running transaction can hold a retention entry at the current tail, which is `SET TRANSACTION SNAPSHOT` under `SET_SNAPSHOT_PROTECTS` beside a restart: plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`) |
| M6 | Under `SET_SNAPSHOT_PROTECTS` the sortedness conjunct of `Assert_getOldestSnapshot` is switched off, while `OldestSnapshot` is still `Min({...})` (`Parts.tla`) | Nothing in `MC_SetSnapshotFixed` checks that the fix keeps `front` equal to the minimum, which is the one thing the sorted re-insert exists to preserve. The model cannot state it as written, because `Min` encodes the answer rather than the list | plan 5, task 4 (budget and calibration), which names itself the owner of `M6` and gives `snapshots_in_use` an ordered encoding if one is ever needed; until then `MC_SetSnapshotFixed` verifies that the fix breaks nothing, not that it preserves `front` |
| M7 | `CleanupValidate` and the validation half of `CleanupDeleteFail` turn every refusal of `hasValidMetadata` into the rollback to `Outdated`. The code has two outcomes, not one: `hasValidMetadata` throws `CORRUPTED_DATA` on a mismatch, which is the rollback, but its catch-all (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:711-720`) *returns false*, and a false makes the `chassert` at `IMergeTreeDataPart.cpp:2928` abort the process | The model has no terminating outcome for a failed validation, so a scenario that reached one would report a recoverable rollback where the server dies. Dead today: the validation refusal is itself unreachable in every scenario of this plan, which is why `NoFalseCorruption` is vacuously green | plan 5, task 2 (`ProcessDown` policies `Terminate` versus `Retry`, `NoAvoidableTermination`, `KillRetry`, `Implicit`), which names itself the owner of `M7` and enables `ProcessDown`: the outcome belongs to `NoAvoidableTermination` alongside the other terminations |
| M8 | `part_is_probably_removed_from_disk` (`IMergeTreeDataPart.cpp:2873`) makes a second `remove` of the same part skip validation entirely; the model re-validates on every grab | The model can loop where the code cannot: `CleanupDeleteFail` returns a part to `Outdated`, `CleanupGrab` takes it again and the same validation fails again. Harmless while the refusal is unreachable | plan 5, task 2 (`ProcessDown` policies `Terminate` versus `Retry`, `NoAvoidableTermination`, `KillRetry`, `Implicit`), which names itself the owner of `M8`; the disk faults that make the refusal reachable are turned on one task earlier, in plan 5, task 1 (disk write faults in the store frames, `StoreRetry`, `NoSpuriousStaleVersion` under faults, the task-driven rollback machine) |
| M9 | `MergeWrite` did not pin the merge result with `Tsk(i)`, although the task holds it from the write until the task object is destroyed (`merge_task`, then `MergePlainMergeTreeTask::new_part`, `src/Storages/MergeTree/MergePlainMergeTreeTask.cpp:156`) | The cleanup thread could take a result the task's statement rollback had just outdated while the task was still alive, which `isSharedPtrUnique` (`MergeTreeData.cpp:4150`) forbids. Found by counterexample `F3` | closed by this task: `MergeWrite` adds the pin and `MergeCommitFinalize` and `MergeUnwind` remove it |
| M10 | `Transaction::commit` recomputes `getActivePartsToReplace` inside its `NOEXCEPT_SCOPE` (`MergeTreeData.cpp:11298`) and, on a non-null `covering_part`, outdates the new part instead of activating it (`:11316`); `PublishStartEffect` and `PublishFlipEffect` model that, and it is unreachable in every scenario of this plan | The branch is written and cited but never taken, so it is checked by inspection only. It cannot be reached with the model's `Covers`, which is a fixed relation over a finite part universe: `M12`'s sources are exactly `P1` and `P2`, `MergeSelect` requires both to exist already, and `InsertWrite` requires `Absent`, so no part can appear under a cover that is already `Active`. The C++ shape it models is a new block number landing inside a range a merge has already published, which needs a covering part whose source set is a strict subset of what it covers | plan 4, task 4 (merges with mutations and the covering relation in range shape), which names itself the owner of `M10`: it gives `Covers` a range shape rather than a fixed source set, which is what puts a covering part over a source that can still be inserted |
| M11 | `MergeUnwind` can in principle win the compare-and-exchange in `MergeTreeTransaction::rollback` (`src/Interpreters/MergeTreeTransaction.cpp:382`) and become the rollback driver, so the transaction would sit in `RollbackCopyLists` with nothing able to advance it. The wedge was the shape of the bodies, `Drives(k, t)` reading `Sess(k)`; since they take their driver as an argument it is the missing wrapper instead, because no disjunct of any `Next` instantiates a `Rollback*A` with `Tsk(i)` | No scenario of this plan reaches it, and the argument has two halves rather than one. Two of `MergeFail`'s triggers are about the transaction: a `RolledBack` state seen at `PublishStart` or at `Commit`, and the `state = "RolledBack"` disjunct of `EnrolRefused`. Those do mean a `KILL` has already won the exchange, so `MergeUnwind` loses it. The other two are not about the transaction at all and are excluded by the reservation instead. `EnrolRefused`'s second disjunct, a source that is locked or already carries a removal CSN, says nothing about who holds the transaction; and a store of the task's own ending in an error needs a second frame on one of the task's parts. Both need another actor to have touched a part the merge holds, and none can: the result is the task's alone, `MergeSelect` takes each source with `lock = EmptyTID`, `DropLock` waits for every task's `reserved` to drain before it takes the parts lock, and the merge holds its reservation from `MergeSelect` until the task idles, so no client removal can start on a source in that window. Nothing else clears a removal lock: a commit leaves it set, a rollback clears the removal `TID` with it, and the non-transactional batch, which does clear it, is not enabled in `Merge`. The invariant `NoTaskDrivenRollback` says so rather than letting the search wedge without a word | **half closed** in `b0b64a6f8bcf`, which produces `DrivesA(a, t)` and the six actor-parameterized `Rollback*A` bodies this row owes, with the session names as wrappers and `Upd*` names for the updating thread; `NoTaskDrivenRollback` stays on the roster, because no background task drives a rollback yet. plan 5, task 1 (disk write faults in the store frames, `StoreRetry`, `NoSpuriousStaleVersion` under faults, the task-driven rollback machine), which names itself the owner of `M11`, is where a fault on a background task makes the shape reachable and `NoTaskDrivenRollback` is removed |

`M2` was closed rather than placed. `Refuse` now routes to `StmtRollbackMark` whenever `stmt.precommitted` is non-empty, which is what `MergeTreeData::Transaction::isEmpty` (`src/Storages/MergeTree/MergeTreeData.h:391`) tests, and both statement-rollback actions act on the whole of `stmt.precommitted`. The attached part keeps its entry in `txn[t].creating` and in `h.creating[t]`, because `MergeTreeTransaction::creating_parts` keeps its entry too; `Refuse` always ends at the outer rollback, and that rollback's second stamp of `RolledBackCSN` and second removal from the working set are both no-ops in the C++ and in the model. The entry `README.md`, section 8, used to carry for this divergence is gone with it. Nothing in `Base` changed state: the count stayed at the figure the run table records, for the reason the row above gives.

The C++ settles which side of the comparison was wrong, and it is the stored side. A legacy
`txn_version.txt` exists and is parsed by `VersionInfo::readFromMultiLineBuffer`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:106-113`), whose old-format branch sets
`storing_version` from a local initialised to 0. The other side of the comparison,
`getExpectedStoringVersionUnlocked` (`VersionMetadataOnDisk.cpp:246`), reads the same file through the same
parser and gets the same 0. So `LegacyInfo` keeping `sv = 0` is right and what was missing was the arm in
`StoredRecord` that reads a `Legacy` disk record. A part with **no** file is the other case and is unaffected:
there both sides are `VersionInfo::UNSTORED_VERSION = -1`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.h:20,23`).

`M3` is argued rather than narrowed, which is what the task that states `LogEntryNeeded` owes it, and the
argument is tighter than the one the placement anticipated. Two of the three departures cost nothing and one is
not covered.

The per-iteration re-read of `latest_snapshot` is **behaviourally identical** to the single load the code does
at `src/Interpreters/TransactionLog.cpp:332`. `UpdRemoveOldEntriesSetTail` leaves the updating thread at
`sys.updater_pc = "Delete"` and `UpdRemoveOldEntriesDone` is what returns it to `"Idle"`, and the only two
actions that write `tlog.latest_snapshot` are `UpdPublishSnapshot`, which requires `"PublishSnapshot"`, and
`RestartLoadLog`, which requires the server to be down. A `Crash` inside a pass resets `sys` and so ends the
pass rather than running inside it. So the value cannot move between two iterations of one pass, and the model
reads on every iteration exactly what the code copied once. The same counter pins `tlog.tail_ptr`, which only
`UpdRemoveOldEntriesSetTail` writes.

`UpdRemoveOldEntriesDone` being unguarded adds behaviours that remove **fewer** entries than the code's loop
would. `LogEntryNeeded` is violated by removing an entry that is still needed, so it is monotone in the set of
removals and a behaviour that removes fewer cannot falsify it where the code's does not: the extra behaviours
produce no red of their own. Nor do they lose the code's, because ending the pass is a choice and the branch
that runs the loop to its end is still in the search.

What is **not** covered is the one departure that narrows. The code erases `tid_to_csn` after the loop under
`mutex` (`src/Interpreters/TransactionLog.cpp:324` and `:349`) where `UpdRemoveOldEntriesDelete` erases in the
step that removes the znode, so the code has a state the model does not: the znode gone and the local lookup
still succeeding. A `LogEntryNeeded` violation that needs that state is invisible here. That is `M3`'s own
placement and it stays there.

`M9` was closed rather than placed, in the task that found it.

One more `chassert` is latent and is not reachable here. `VersionMetadata::setAndStoreCreationCSN`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:128`) asserts that the creation CSN it overwrites is
either unset or the `NonTransactionalCSN` of the empty part `executeDropRange` creates, so a statement rollback
that stamps `RolledBackCSN` followed by a commit of the same transaction fires it: `afterCommit` would call the
function again with a real CSN on a record that already carries `RolledBackCSN`. The sequence is unreachable in
this plan, on both paths into a statement rollback. `Refuse` routes to `StmtRollbackMark` and ends at
`RollbackOnException`, and `MergeFail` routes to `MergeStmtRollbackMark` and ends at `MergeUnwind`; both continue
to the transaction rollback, and no action leads from a statement rollback back to `CommitBefore`. It is placed in
plan 4, task 2 (`KILL MUTATION` and the registration re-check), where `StorageMergeTree::killMutation` cancels a task without failing the client
transaction that registered the mutation, which is the first candidate for a statement-level failure whose
transaction survives. If it becomes reachable there it needs an `Assert_` property of its own; until then this
paragraph is the record that it was looked for and not found.

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M12 | `CreationInFlight` (`Parts.tla`) re-evaluates `isCreatedByUncommittedTransaction` on every store attempt, where `VersionMetadata::setAndStoreRemovalTID` computes it once outside the metadata update lock (`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:172`) and passes the flag into the update function (`:179`) | Nothing in this plan: both read the same record on attempt 1, and a creation can only become committed, so the two agree. They come apart once `UpdRemoveOldEntriesDelete` can take a commit back out of `tid_to_csn`, which makes a committed creation look uncommitted again to the re-evaluation and not to the C++ | plan 5, task 4 (budget and calibration), which names itself the owner of `M12`: it runs the non-transactional batch beside `Updater+GC`, the configuration that enables the truncation pass with `NtBatch*`, and it owes the flag as a frame field |
| M13 | `DropLock` was written as `client[k].pc = "DropWait" /\ \A i \in Tasks : task[i].reserved = {} /\ sys.parts_lock = NoActor` on one line. A `\A` body extends as far to the right as it can, so the parts-lock test was inside the quantifier | With `Tasks = {}`, which is `Base`'s and `NonTxn`'s configuration, the quantifier is vacuously true and the action lost its `lockParts` guard altogether, so a transactional `DROP PARTITION` could take the parts lock while another actor held it. `Merge`, whose `Tasks` is a singleton, was unaffected, which is why it survived two scenarios | **fixed in this task**, by writing the three conjuncts out. It moves `Base`'s committed figures; `STATE_SPACE.md` carries the re-measurement |
| M14 | `NoLostRead` and `NoFutureRead` compare a read against the oracle in the state where `SelectFinish` runs, not in the state where `SelectCapture` took the parts | Among transactions the two states agree, because nothing another transaction does between them can change what is visible at the reader's fixed snapshot. A non-transactional writer can: a part inserted after the read started is oracle-visible and legitimately absent from it (`NoLostRead`), and a part removed after `SelectCheck` decided it is legitimately still in it (`NoFutureRead`). Neither is a lost or a future read; both are the properties reading the wrong instant | plan 4, task 5 (isolation properties revisited with mutations), which names itself the owner of `M14`: give `SelectCapture` a ghost of the parts that were `Active` or `Outdated` when it ran and state both properties over it. Both witnesses survive the change, because neither hook is about which parts existed. Until then the two properties are not checked in `NonTxn`, which `WITNESSES.md` records |
| M15 | The `NonTxn` scenario has no exhaustive run, and after the split only half of it had one. `MC_NonTxnInsert` finishes green at 15,787,838 distinct states; `MC_NonTxnDrop` did not, at any configuration tried in the task that wrote it: 32.5 and 44.1 million distinct at `CSN_MAX = 35`, and 56,703,779 after 9 min 30 s at `TID_MAX = 2`, `CSN_MAX = 34`, with the queue growing throughout | The half of the scenario that contains the removal batch was not verified exhaustively at all | **not closed: a coverage decomposition**, placed in plan 5, task 4 (budget and calibration). Task 5 gave the half two finishing configurations rather than one: `MC_NonTxnDrop` at `TID_MAX = 1` with the cleanup group, green at 1,112,076 distinct states in 11 seconds, and `MC_NonTxnDropTwo` at `TID_MAX = 2` without it, green at 47,958,711 in 7 min 34 s. The lever the task was told to try first, a `CONSTRAINT` bounding the non-transactional queries a behaviour issues, was measured and rejected: it cut two per cent. What each pair of bounds costs the witnesses is in debt `B4` and in `STATE_SPACE.md`. What the pair does not do is exhaust their conjunction: no finishing run has the cleanup group and the second transaction at once, and `F2` is the standing demonstration that cleanup beside one more transaction can be decisive. The conjunction is therefore an open bound, stated on the `NonTxnDropTwo` row of `README.md`'s assurance table and in `STATE_SPACE.md`, and its budget is plan 5, task 4 (budget and calibration) |
| M16 | `NtBatchStore` wrote `h.removers[p]` in its own step, one or more steps after `StorePublish` published the record that makes the removal visible | Every property that reads the visibility oracle was blind for that window, which produced a counterexample on `Atomicity` that this task first reported as code finding F7. A non-transactional removal has no commit point, so the publication IS the moment it takes effect | **fixed in this round**: `StorePublishStep` writes the ghost when it publishes a record whose `removal_tid` is `NonTransactionalTID`, which is the same rule `CommitCreateEffect` follows for a transactional removal |
| M17 | `SetSnapshot` is guarded on `txn[Cur(k)].state = "Running"`, while `executeSetSnapshot` (`src/Interpreters/InterpreterTransactionControlQuery.cpp:138-149`) has no state check and `executeQueryImpl` exempts `ASTTransactionControl` from its refusal of a query on a failed transaction (`src/Interpreters/executeQuery.cpp:2690-2695`), so the C++ accepts the statement on a rolled-back transaction | The model cannot reach `SET TRANSACTION SNAPSHOT` on a transaction whose `snapshot_in_use_it` `rollbackTransaction` has already erased (`src/Interpreters/TransactionLog.cpp:583`). That is the path on which finding `F2`'s proposed `setSnapshotForRunningTransaction` would erase an iterator that is gone, so the running-list check the fix needs is checked by inspection and not by search | **not closed in plan 3, task 1**, and re-placed in plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), which is the next task to re-measure `SetSnapshot`. Two reasons, and the second is the one that matters. `Keeper` sets `SNAPSHOT_TARGETS = {}`, so `SetSnapshot` is disabled there whatever its guard says. And under the baseline the relaxation is behaviourally dead wherever it is enabled: `SetSnapshot` writes `txn[t].snapshot`, `h.content[t]` and the session's read baseline and nothing else, because `SET_SNAPSHOT_PROTECTS` is `FALSE` and the two registry entries are left alone. Every operator that reads any of the three is dead for a rolled-back `t`. `SelectCapture` requires `txn[Cur(k)].state = "Running"`, and it is the only way into `SelectCheck` and `SelectFinish`, which are what read `SnapshotFor(t)` and write the read baseline. `NoLostVisibleDataStep` is the only reader of `h.content[t]` and carries the same `Running` guard, as do `StableReadStep`, `ReadYourWritesStep`, `NoUncommittedReadStep`, `NoFutureReadStep`, `NoLostReadStep` and `AtomicityStep`. `NoPrematureDeleteStep` quantifies over `running_list` but excludes a transaction whose state is not `Running`. The other readers of `txn[t].snapshot` are `DropLock` and `MergeSelect`, and neither is reachable: `SetSnapshot` requires `client[k].pc = "Idle"` and acts on that session's own transaction, so it cannot run while the same session sits at `DropWait`, and a merge's snapshot belongs to a transaction no session holds. Where the relaxation is NOT dead is under the proposed fix, where `moves_entry` lowers `snapshots_in_use[t]` and `retention_in_use[t]` for a transaction still in `running_list`, which moves both horizons and is the erase-an-iterator-that-is-gone hazard this row names. So the scenario that must model it is `SetSnapshotFixed`, not `Keeper` |
| M18 | `UpdRemoveOldEntriesSetTail` reads `OldestSnapshot` and publishes `zk.tail` and `tlog.tail_ptr` in one step, where `TransactionLog::removeOldEntries` has a window between `getOldestSnapshot` (`src/Interpreters/TransactionLog.cpp:312`) and `tail_ptr.store(new_tail_ptr)` (`:321`) | `MC_SetSnapshotF2Fixed` cannot distinguish a fix that holds `running_list_mutex` across the truncation pass from one that does not, so its green verifies `SetSnapshot`'s refusal of a target below the tail and not the publication that refusal's soundness rests on. Finding `F2`'s fix argument turns on exactly that mutex extension | plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), where the truncation pass runs beside the non-transactional batch and the tail is what a `Crash` can interrupt; splitting the action into a read step and a publish step belongs with it |
| M19 | `CleanupGrab` re-evaluates the removal condition and moves the part to `Deleting` in one action (`Server.tla`), so the model checks a fix in which nothing can intervene between the re-check and the state change | The decision and the grab are now two actions, which is what finding `F2`'s second shape needed, but the re-check inside the grab is still atomic with it. A C++ fix that revalidated `canBeRemoved` and then took a different lock to change the state would have the same window one level down, and the model would report it green | plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), which is the task `F2`'s own text names as resting on this variant and the first that interrupts the cleanup pass part-way; until then the fix `SET_SNAPSHOT_PROTECTS` encodes is read as "one guard across both", which is what `FINDINGS.md`'s `F2` entry proposes |
| M20 | `NtDropWrite` writes the empty part first and the batch refuses only later, in `NtBatchPreflight`, while `StorageMergeTree::dropPartitionImpl` calls `checkPartsCanBeRemovedNonTransactionally` (`src/Storages/StorageMergeTree.cpp:3204`) before `createEmptyDataParts` (`:3214`) | The model can write and publish an empty part for a conflict the code refuses before anything is written. Nothing of this plan reads it, because no property here is stated over a write that is later abandoned; it becomes observable as soon as a `Crash` can land between the write and the refusal | plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), which puts the layered disk and `Crash` around exactly these steps: it adds the early refusal as a step of its own and keeps the later recomputation, so that a conflict appearing after the check stays reachable |
| M21 | `MergeBegin` requires `sys.merges_blocker` already clear, while `StorageMergeTree::scheduleDataProcessingJob` calls `beginTransaction` (`src/Storages/StorageMergeTree.cpp:2222`) before it tests `merges_blocker.isCancelled` (`:2240`) and returns | The model has no transient read-only transaction that begins, finds the blocker set and is rolled back at once. Such a transaction holds an entry in `snapshots_in_use` while it lives, so it can hold `getOldestSnapshot` back and delay a cleanup or a truncation the model performs immediately | plan 5, task 1 (disk write faults in the store frames, `StoreRetry`, `NoSpuriousStaleVersion` under faults, the task-driven rollback machine), which is where a background task first fails part-way and the transient transaction of a refused merge becomes a shape worth having |
| M23 | `CleanupDecide` and `CleanupGrab` take one part per pass, where `MergeTreeData::grabOldParts` collects every removable part under one `lockParts` (`src/Storages/MergeTree/MergeTreeData.cpp:4126-4194`) and moves them together | The model interleaves other actors between two grabs of one pass, which the code's lock forbids, and it has no state in which several parts are `Deleting` from the same pass. No property of this plan reads the set of parts in `Deleting`, so nothing sees it today | plan 5, task 4 (budget and calibration), which owns the budget: taking the set per pass multiplies the states of every cleanup-enabled scenario, so the change and the budget have to be decided together |
| M22 | The batch rebuilds its remaining targets with `SetToSeq(b.locked)` and `SetToSeq(nlocked)` , in `NtBatchPreflight` and `NtBatchLock`, and `SetToSeq` is a `CHOOSE` over every bijection, so the rebuilt order need not agree with the order the targets were locked in, where the C++ walks its `locked_parts` vector in that order | Nothing of this plan reads the order: every property over the batch is stated over the set of targets. It becomes observable when a `Crash` can interrupt the batch part-way, because which targets were stored before the interruption then depends on the order | plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), which is where the batch is first interrupted; it either carries the order in the frame or states the property over the set and says so |
| M24 | `DropLock` enrolled every visible part, while `removePartsFromWorkingSet` skips a part whose `creation_csn` is `Tx::RolledBackCSN` before it calls `MergeTreeTransaction::removeOldPart` (`src/Storages/MergeTree/MergeTreeData.cpp:7044-7046`) | A transactional `DROP PARTITION` at `EverythingVisibleCSN` enrolled a part whose creation was already rolled back and stamped a removal CSN over it, which is a shape the code cannot reach. That was finding `F9` as round 3b stated it, and it was wrong. The comment above `NtDropWrite` cited the same skip for the non-transactional batch, so the model knew about it and applied it in one place only | closed in the round that re-derived `F9`; `DropLock` now walks the enrolment set and outdates the visible set, which is the split the two loops of `removePartsFromWorkingSet` have |
| M25 | `DropLock` computes the enrolment set once, for the whole batch, where the C++ tests each part's `creation_csn` in the loop iteration that enrols it | The model's window between the test and the lock is wider than the code's. It is a window in the code too, because `MergeTreeTransaction::rollback` stamps `RolledBackCSN` with no data-parts lock held (`src/Interpreters/MergeTreeTransaction.cpp:412-417`), so the widening changes how easily finding `F9`'s second shape is reached and not whether it exists | plan 5, task 4 (budget and calibration), which owns the cost of taking the set per pass; the same abstraction is `M23`'s |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M26 | The updating thread's three passes each start from `sys.updater_pc = "Idle"` and are otherwise unordered, while `TransactionLog::runUpdatingThread` runs `loadNewEntries(); removeOldEntries(); tryFinalizeUnknownStateTransactions();` as one loop body (`src/Interpreters/TransactionLog.cpp:252-254`) | That ordering is the whole of what the two-list scheme buys: a transaction appended to `unknown_state_list` must wait for an iteration whose load ran after the append. Without it the model swaps twice with no load between, and `UpdFinalizeUnknown` rolls back a transaction that is in `h.committed`. It was red on `UnknownResolvesByLog` at 12,081 distinct states on the baseline, at the first exhaustive run of the `Keeper` scenario; the counterexample is `traces/m26-unknown-swap-without-load.txt`, sixteen states, two `UpdSwapUnknownLists` with no `UpdLoadEntriesMap` between them | **closed** in the Keeper-faults commit. `sys.load_since_swap` records that `loadNewEntries` has completed since the last swap; `UpdLoadEntriesMap` sets it, the new `UpdLoadNothing` sets it when the read finds nothing new (`loadEntries` over an empty range, `:277-278`), and `UpdSwapUnknownLists` requires and clears it. It does not over-fix: with the two lists collapsed the `UnknownResolvesByLog` witness is still red. `UpdLoadNothing` is in `UpdaterUnknownNext` alone, so no scenario without the unknown-state group gains an action |
| M27 | `CommitAck` and `CommitUnknownResolved` were both enabled on a session parked in `waitStateChange`, so one return of `executeCommit` was two model transitions | Every resolution of an unknown-state transaction was explored twice under `WAIT_MODE = "WAIT_UNKNOWN"`, and the two actions delivered the same outcome by two routes. Reachable only with a Keeper fault, because only `CommitUnknown` sets `client.waiting` to `"ForState"` | **closed** in the Keeper-faults commit, by `client[k].waiting /= "ForState"` in `CommitAck`. No scenario that leaves `KEEPER_FAULTS_MAX` at 0 changes, because `txn[t].pc` never reaches `"CommitUnknown"` there. It has no `Witness` hook and cannot usefully have one: the two actions agree on every variable they write in the one case where both are enabled, `state = "Committed"`, so no property can tell them apart and a hook that restored the duplicate would be green by construction. The measurement says the same thing — on the tree of that round the distinct count is identical at 71,323,386 with and without the fix, and only the generated count moves, from 336,015,390 to 330,565,902 |
| M28 | `RollbackFinalizeA` leaves `Upd` in `txn[t].holders` when the updating thread rolls an unknown-state transaction back, while the committed branch removes it through `CommitFinalizeEffect(Upd, t)`. In the C++ both branches destroy the same `MergeTreeTransactionPtr`, which the local `list` of `tryFinalizeUnknownStateTransactions` owns until the function returns (`src/Interpreters/TransactionLog.cpp:358`, `:378`) | A rolled-back unknown-state transaction keeps a holder for ever. No action and no property of this plan reads `txn.holders` except `Holds(i)`, which reads a task's, so the effect is extra states rather than a wrong answer; an action later guarded on an empty holder set would wedge on it. The asymmetry also makes the committed branch release the pointer one step earlier than the C++ does, which destroys it only when the pass ends | **closed** in the M28 commit, by one clause in `RollbackFinalizeA`: the updating thread's holder is released there when the driver is `Upd`, which is what `CommitFinalizeEffect(Upd, t)` does on the other branch of the same pass. A session's and a task's holders are still left alone, because those come from a holder object that `MergeTreeTransaction::rollback` does not destroy. A third path was missing and was added with `M31`: when another driver wins the CAS inside the window the guard release opens, `UpdRollbackLost` releases the holder, because the pass is done with that entry whoever won. All three release at the transaction's finalize or at the pass's notice of loss, where the C++ releases when the pass's local list is destroyed; that is one step early and costs nothing, because no action and no property reads a holder set between the two. The clause is live, which an unchanged exhaustive count could not show, and was measured in a scratch copy at one session, one part, `Tasks = {}`, `TID_MAX = 1` and `CSN_MAX = 34`: an invariant forbidding `h.unknown[t] = "RolledBack"` with `h.rolled_back[t]` is violated at 3,160 distinct states, so the updater-driven rollback finalizes there; an invariant saying `Upd` is gone afterwards is green over that whole space at 4,676 distinct with the queue drained; and the same invariant is violated at 3,137 with the clause alone removed. The two violated counts stop at the first violation and are not reproducible; the green one explores its whole space and is reproducible |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M29 | `CommitUnknownResolved` cleared `client.current` and released the session's holder on both branches, while `InterpreterTransactionControlQuery::executeCommit` throws "Transaction {} was rolled back" at `:103`, before `setCurrentTransaction(NO_TRANSACTION_PTR)` at `:114` | The model invented a successful detach on the rolled-back branch: the session was free to begin another transaction at once, and the `ROLLBACK` the client actually has to issue was not required of it. The exception does reach `onException` (`src/Interpreters/executeQuery.cpp:3396-3398`), but that path's `TransactionLog::rollbackTransaction` fails its CAS on a CSN that is already `RolledBackCSN` and returns at `:544-548` without touching the session | **closed** in the lifecycle commit. Only the committed branch detaches and drops the holder; the rolled-back branch leaves the transaction bound, and the client's way out is `RollbackStart`'s already-rolled-back branch |
| M30 | `UpdFinalizeUnknown` went from `CommittingCSN` straight to `RolledBackCSN` with `rb_driver = Upd` in one action, and said no actor could observe the intermediate value. The C++ takes two steps: the state guard's lambda CASes `CommittingCSN` to `UnknownCSN` and notifies (`src/Interpreters/MergeTreeTransaction.cpp:315-317`, destroyed at `TransactionLog.cpp:388`), and only then does `rollbackTransaction` (`:389`) CAS `UnknownCSN` to `RolledBackCSN` (`MergeTreeTransaction.cpp:381-382`) | The load-bearing fact is that `MergeTreeTransaction::getState` returns `RUNNING` at `UnknownCSN` (`MergeTreeTransaction.cpp:59-61`). The transaction is running again for the length of that window, which is why the waiting client takes a second `waitStateChange` (`InterpreterTransactionControlQuery.cpp:93-100`) and why `tryGetRunningTransaction` can hand it to a `KILL TRANSACTION` (`InterpreterKillQueryQuery.cpp:404-407`) that wins the CAS. Collapsing the two steps suppressed a real race between the updater, the waiter and a killer, and made the updating thread the only possible rollback driver | **closed** in the lifecycle commit. `UpdFinalizeUnknown` releases the guard and stops, `UpdRollbackStart` is the CAS the updater attempts, and `UpdRollbackLost` is that CAS failing and the pass going on to its next entry. The race is reached and not merely permitted: a scratch invariant forbidding a session to drive such a rollback is violated in two steps, `UpdFinalizeUnknown` then `KillTransaction`, at 1,715 distinct states, and one forbidding the updating thread to be left holding a transaction another driver rolled back is violated at 1,689. The window is worth 29 per cent of `Keeper` |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M31 | `UpdRollbackLost` was enabled in the state `UpdRollbackStart` produces: its guard was `tlog.finalizing = t`, a state that is not `Running` and a CSN of `RolledBackCSN`, and the updating thread's own successful CAS establishes all three | The thread could lose a compare-exchange it had just won and release the pass while still driving that rollback. `UpdFinalizeUnknown` could then start a second entry, so two updater-driven rollbacks would run at once and compete for the single `Upd` frame on a part, and `UpdFinalizeDone` could leave the pass entirely while `Upd` rollback frames were open. All of that is one thread inside `tryFinalizeUnknownStateTransactions`. The configurations run with deadlock checking off, so the shape would not have surfaced as a red by itself | **closed** in the pass-ownership commit, by `txn[t].rb_driver /= Upd`. The pass marker also stopped being cleared by whoever finishes the rollback: `RollbackFinalizeA` clears `tlog.finalizing` only when the driver is `Upd`, because the marker belongs to the pass and `UpdRollbackLost` is where the pass notices it has lost and lets go. An invariant saying the pass is still inside a transaction the updating thread is driving is green over the whole scenario and red at 54,336 distinct states without the conjunct |
| M32 | Six actions of the client's commit machine were guarded on `client[k].pc = "Commit"` alone, so a session parked in `waitStateChange` at `InterpreterTransactionControlQuery.cpp:90` could run the per-part stores, the flip, the finalize, the read-only branch and the CSN allocation beside the updating thread that was already running them | One `executeCommit` became two concurrent commit machines on one transaction. It is the same defect class as `M27`, which fixed one of the seven sites and stopped there. It was also the whole of the `WAIT_UNKNOWN` budget problem: three rounds walked a reduction ladder, found every rung cheap, and concluded that the cost was the wait mode | **closed** in the pass-ownership commit. The operator `InCommit(k)` is `client[k].pc = "Commit"` and not parked, and every one of the seven actions uses it. An action property saying a parked client runs none of them is green with the guard and red at 66,839 distinct states without it. `MC_KeeperUnknownWait` then finished at the matrix bounds for the first time, 11,537,098 against a run that had not finished at 71 million, which closed debt `B8` |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M33 | `DiskAfterCrash` copied the durable layers onto the cached ones part by part, with no relation between the record and the directory that holds it | A part whose `dir_durable` was `FALSE` could come back with `cached.kind = "Info"`, which is a `txn_version.txt` inside a part directory that did not survive | closed in the layered-disk task: at a crash a part whose directory did not survive loses its whole disk record. The loss shape itself is kept, which is the point of the layered directory: a published part whose directory was never fsynced still vanishes at a crash, and `DurablePState` reports it `Absent` |
| M34 | `OnDisk` showed the loader every directory `dir_cached` records, and `InsertWrite`, `MergeWrite`, `NtInsertWrite` and `NtDropWrite` create that directory while the part is still `Temporary` | A crash between the write and the rename left a part directory in place with no metadata file, which `LoadedRecord`'s fourth case turns into an ancient non-transactional creation, so a part created by an in-flight transaction loaded `Active` and a later reader saw it: red on `NoUncommittedRead` at 18,485 distinct states. The shape is unreachable upstream, and the order is what makes it so. `MergedBlockOutputStream` calls `setAndStoreCreationTID` (`src/Storages/MergeTree/MergedBlockOutputStream.cpp:74`) immediately after `createDirectories` (`:70`), with the comment at `:72-73` saying the part may still have a temporary name; `MergeTreeData::renameTempPartAndReplaceImpl` (`src/Storages/MergeTree/MergeTreeData.cpp:6924`) publishes the final name only later, through `preparePartForCommit` (`:6972`), which renames at `:6873`; and `loadDataParts` skips every directory whose name starts with `tmp` (`:2964`). So `txn_version.txt` is written before the directory has a name the loader will look at, and a directory that still has the temporary name is not read at all | closed in the layered-disk task, in two rounds. First by a crash-time rule, a part still `Temporary` losing its disk record; then structurally, by giving the disk a `named` bit for the final name, which is what `OnDisk` now reads and what the crash requires. **Its scope, stated precisely, because finding `F12` sits in what it leaves open.** The argument above is about ORDER and it holds: the creation record is written before the part has a name the loader looks at. It is not about DURABILITY. The part directory's own name is a dentry in the parent and the metadata's names are dentries in the part directory, the two are written back independently, and a parent writeback without a part-directory writeback reaches exactly the state this row argued could not arise. The order hole is closed; the durability hole became expressible only when `M50` stopped the model asserting the temporary name durable. The `tmp_` directory that a real restart finds and cleans up is not an action of the model: the loader's view of it is "never existed", which is what losing the record expresses |
| M35 | `RestartLoadPart` pushed the whole of `Covers[p]` back onto the load queue when a root did not load `Active` | A covered part whose directory the cleanup thread had deleted was loaded anyway, and `LoadedRecord`'s fourth case made it `Active`: a part resurrected out of nothing, red on `AckedWriteIsDurable` at 44,729,071 distinct states. `loadDataPartsFromDisk` pushes `part->children`, which are nodes of the `PartLoadingTree` built from the part names on disk, so a child with no directory is not a node | closed in the layered-disk task: the children are filtered by `OnDisk` |
| M36 | `PState`, the invariant preamble, read the durable layer for every part outside `sys.loaded_parts` | `loaded_parts` is what the loader has processed, not what the server holds, so a part created after the restart was judged by its durable layer, which says nothing about the state it is in: red on `ErrorIsAbsent` at 438,654 distinct states. The same mistake was in the `S5` in-flight disjunct of `AckedWriteIsDurable`, which the brief guarded on `loaded_parts` | closed in the layered-disk task: `InMemory(p)` is the disjunction of `loaded_parts` and a non-`Absent` record, and both sites read it |
| M37 | The four creating actions require `h.creator[p] = EmptyTID`, a permanent prohibition on reissuing a part name, which assumes upstream never does. It does: a restart rebuilds the block counter from the maximum block number among the parts it loaded (`StorageMergeTree.cpp:236` calling `getMaxBlockNumber`, `MergeTreeData.cpp:2212-2220`) and new inserts allocate from that counter (`StorageMergeTree.cpp:4049-4058`), so if the highest-numbered part was physically removed and no higher part survived, the same name is produced again | Without the guard, every history field is keyed by the part name, so `h.creating[t]` for a finished transaction points at a part a later transaction created, which was red on `ErrorIsAbsent`. With the guard the model cannot reach real name reuse at all. The guard is the cheaper error of the two and it is in the tree; what it hides is a behaviour whose properties nothing here would state correctly anyway | **placed**: plan 3, task 5 (part incarnation), a task of its own, before the sweep re-measures everything once. The sound repair is a part incarnation, a generation counter beside the name, with every `h` field keyed by the pair. Plan 3, task 3 measured the sizing rather than estimating it and did not attempt the change: eight `h` fields are keyed by `Parts`, `OracleVisible` is keyed on `h.creator` and seven properties read it, four creating actions carry the guard and the merge result is a fifth site, thirty-one `MC_*` modules project a `Parts`-indexed row in their `VIEW`, and every scenario and every witness row needs re-measuring, because the part universe stops being a set of names. The permanent guard stays until then, and what it hides is reachable **now**: in `Crash` at `RESTARTS_MAX = 1` the cleanup thread can remove `P1` and a post-restart `InsertWrite` would reissue the name without it |
| M38 | `NoLostVisibleDataStep` read `txn[t].state = "Running"` in the pre-state and the view in the post-state, so the step that destroyed the transaction was judged as a step that shrank its view | Red on the `Crash` step itself, for a transaction that no longer exists. Nothing but `CrashEffect` puts a live transaction into `Absent` | closed in the layered-disk task: the antecedent also requires the transaction not to be `Absent` in the post-state |
| M45 | `DiskWithFilesSynced` wrote `tmp_durable = tmp_cached`, which is the `M43` class at a sibling site. After a store has renamed the tmp file away, `tmp_cached` is FALSE and `tmp_durable` is TRUE, so the assignment LOWERED it | An fsync of the data files silently discarded the one thing a crash could still have recovered, and the tmp-only shape vanished on every branch where `Fsync` had fired, which is the shape finding `F10` is about. The operator's own comment claimed it left the record's durability alone | closed in the layered-disk task's second fix round. The sweep over the other sync operators found no sibling: `DiskWithDirSynced` moves the pair to its cached value, which is what publishing the rename means, and `DiskWithParentSynced` touches only bits that never fall in a reachable state. The distinction is fenced by the action property `DurabilityMonotone`, which is red at 461 distinct states with this defect restored |
| M46 | `DurablePState` ignored `payload_durable`, so it reported a part durably `Active` whose rows did not survive | It disagreed with `RestartLoadPart`, which marks such a part broken and leaves it out of the working set, so the invariant preamble and the loader gave two answers for one part. Dead wherever `FSYNC_AFTER_INSERT` is on, live in the unsynced world, which is where the restart properties will be stated | closed in the layered-disk task's second fix round: a part without durable rows is not durably active |
| M41 | The disk had one `dir` bit for the part directory, which stood for both its temporary name and its final one, so a sync that landed before `renameTempPartAndReplace` was carried across the publication | A sync before the rename can preserve only the `tmp_` name, which the loader skips (`MergeTreeData.cpp:2964`), and a writeback after it can make the final name durable while the inner `txn_version.txt` rename is not. Neither shape was expressible, and the first `F10` trace went through the inexpressible one | closed in the layered-disk task's fix round: `named_cached` and `named_durable` beside `dir_*`, written by the four actions that rename a part into place, and `FsyncParent` for the parent's dentries against `FsyncDir` for the part's own |
| M42 | Payload durability was invented. `HistoryInit` asserted that the data files are fsynced before the directory rename, and `RestartLoadPart` restored `h.payload[p]` unconditionally | `finalizePartAsync` takes `fsync_after_insert` (`MergeTreeDataWriter.cpp:1176-1179`), a setting of its own that defaults to false (`MergeTreeSettings.cpp:642`), so a crash can take the rows of an acknowledged insert. The model recovered them, which made every green at `FSYNC_PART_DIRECTORY = TRUE` a statement about version metadata only | closed in the layered-disk task's fix round: `payload_durable` on the disk record under the new constant `FSYNC_AFTER_INSERT`, and a part whose data files did not survive is broken at load rather than recovered |
| M43 | `DiskWithMetaSynced` promoted the metadata record's durable layer, which is a rename and therefore a dentry that no fsync of a file can publish | The free `Fsync` could close the rename-not-durable window that `DiskWithInfo` opens, so any property placed in the unsynced world could be satisfied by a sync that would not have helped the real system. Pre-existing, and reachable for the first time here, because `SyncNext` puts `Fsync` into the model's first `Layered` scenario | closed in the layered-disk task's fix round: `Fsync` promotes file content and the payload, and only the two directory syncs promote a rename |
| M44 | `RestartLoadPart` cleared `deferrable` on the arm that defers the record | `is_persist_deferrable` is cleared after a real write and nowhere else: the deferral branch returns at `VersionMetadataOnDisk.cpp:206-211`, before the assignment at `:241`, which is the convention `StorePersistStep` already follows. The next non-transactional store on such a part would write a `txn_version.txt` where the code updates the deferred record again | closed in the layered-disk task's fix round |
| M40 | `CrashEffect` keeps `tlog.local_tid_counter` where `TransactionLog::loadLogFromZooKeeper` resets it to `Tx::MaxReservedLocalTID` (`src/Interpreters/TransactionLog.cpp:219`) | The code may reuse a local number because a TID is `(start_csn, local_tid, host)` and `loadLogFromZooKeeper` creates a placeholder znode first (`:190`), which raises `latest_snapshot`, so every transaction begun after a restart has a start CSN above every one begun before it. The model's `Tids` ARE the distinct transactions, so reusing one would merge two of them, and every history field is keyed by the tid | the plan that needs a restart to produce more transactions than `TID_MAX` allows, which is none of them |
| M39 | `AckedWriteIsDurable` admitted a removed part in `Deleting` or `Deleted` and not in `Absent` | `DurablePState` has no `Deleted` value and reports a missing directory as `Absent`, so a part the cleanup thread had removed before a crash read back as `Absent` and the arm that exists for a removed part did not cover it | closed in the layered-disk task: the arm admits `Absent` as well, and the committed remover is what separates it from a part the crash lost |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M47 | `RestartLoadLog` rebuilds `tid_to_csn`, `latest_snapshot` and `tail_ptr` from Keeper and leaves `tlog.tid_start` at the value `DownEffect` reset it to, which is `UnknownCSN` for every transaction. `TransactionLog::removeOldEntries` reads `elem.second.tid.start_csn` out of the entry it deserialised from the znode (`src/Interpreters/TransactionLog.cpp:336`), so the code knows the start CSN of every transaction whose entry survived | Every pre-restart transaction looks infinitely old to `UpdRemoveOldEntriesDelete`'s `tid_start[t] < tail_ptr` guard, so the model prunes entries the code would test properly. It also leaves finding `F11`'s first proposed fix, holding the tail back to the oldest start CSN that a part still needs, with no variant, because the model has no start CSN to hold it to. Finding `F11` does **not** depend on it: in its trace the real start CSN is 33 and the new tail is 35, so the entry is prunable with the guard evaluated correctly | plan 3, task 6 (budget, witness sweep, documents and debts), **fix first, then sweep**, so the one-line change is re-measured once by that task's sweep rather than twice. The fix is keeping `tid_start` across `DownEffect` the way `local_tid_counter` is kept and for the same reason, that the code recovers it and the model has no other source. What it costs is the re-measurement: `ProcessDown` is reachable in every scenario, `DownEffect` is what it runs, and `tlog.tid_start` is in every view, so every committed figure moves |
| M48 | `DiskWithFilesSynced` set `payload_durable` unconditionally, and the payload had no cached layer to set it from, where the metadata record, the temporary file and the two directory names all have one | An `Fsync` after a crash raised `payload_durable` on a part whose data files the crash had taken, so a part that loaded **broken** could satisfy a durability antecedent a moment later. That is the `M43` and `M45` class at a third site, and the sweep those two came from could not reach it, because there was no cached bit for the assignment to be compared against. It produced a false red on `LogEntryNeeded` in `MC_CrashUnsyncedFixed` that read as the fix failing | **closed in plan 3, task 3 (what a restart must not do)**, structurally rather than by a guard: `payload_cached` beside `payload_durable`, written by `DiskWithDir`, carried down by `DiskAfterCrash` like the other three layers, and `DiskWithFilesSynced` assigns from it. Inventing a payload is now unrepresentable. `BaseSmall` and `Merge` are identical to the state and to the generated count afterwards, because `DiskTypeOK`'s non-layered clause pins the new bit to its durable counterpart |
| M49 | `DurablePState`, the invariant preamble, read `disk[p].durable` as it stands, where `RestartLoadPart` reads the temporary metadata file when there is no `txn_version.txt` (`VersionMetadataOnDisk.cpp:77-84`) and then runs `updateCSNIfNeeded` over what it read | The preamble and the loader answered differently for the same part, which is the `M46` class. A tmp-only directory came out durably `Active` where the loader loads it rolled back, and so did a creation whose CSN the log resolves to `RolledBackCSN`. Both were red on `ErrorIsAbsent` in the unsynced world, and both would have been read as findings | **closed in plan 3, task 3 (what a restart must not do)**, structurally: `LoadedRecordFrom` takes a record and a temporary-file bit rather than a part, `LoadedRecord` and `DurableLoadedRecord` are it applied to the two layers, and `DurablePState` resolves what it reads exactly as the loader does. The two cannot drift, which a second near-copy would have let them do |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M50 | `DiskWithInfo` set `tmp_durable = TRUE` in the unsynced case, which asserts that the temporary metadata file's **name** survives a crash. `createFile` is `open` plus `close` (`src/Common/filesystemHelpers.cpp:319-325`) and `buf->sync` is `fdatasync` on the descriptor (`src/IO/WriteBufferFromFileDescriptor.cpp:121`), so the file's contents are durable and neither of its names is; both names are dentries in the part's own directory, which only the guard at `VersionMetadataOnDisk.cpp:360-362` syncs | The fourth arm of `LoadedRecordFrom`, a part directory that survived under its final name holding no metadata at all, was unreachable, so the model could not express the outcome `loadMetadata` gives it. That outcome is finding `F12`, and it is the one direction the unsynced world was never checked in: not committed data lost, but uncommitted data published | **closed in plan 3, task 3 (what a restart must not do)**, round 1b: the durable layer does not move at all in the unsynced case, and the arm is reachable, shown by a scratch invariant red at 1,009 distinct states rather than assumed. `Merge`, `BaseSmall` and `CrashLegacy` are identical to the state and to the generated count afterwards, because the branch that changed is the one `Durable` mode and `FSYNC_PART_DIRECTORY = TRUE` never take |

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M51 | The store's disk effect was one action, so the model had no state between `createFile` and `replaceFile` (`VersionMetadataOnDisk.cpp:348-363`). A directory writeback that lands in that window leaves the part directory holding the temporary name and not the final one, which is a third durable outcome beside the two the model had, neither name and the final name | Before `M50` the model reached that outcome by asserting `tmp_durable` after every unsynced store, which was wrong about the dentry and right about the shape. `M50` removed the wrongness and the shape together: the temporary file became unreachable, `LoadedRecordFrom`'s tmp-only arm died, finding `F10`'s module produced the shallower "creation CSN never reached the durable layer" trace instead of the one its entry describes, and `Assert_IsNonTransactionalDomain`'s witness went green where it had been red at 1,570 states | **closed in plan 3, task 3 (what a restart must not do)**, round 2, in `787e8a1ee243`, by splitting the store's disk effect into `StorePersistStep`, which writes the temporary file, and `StoreRenameStep`, which renames it over `txn_version.txt`. The window is what `FsyncDir` and `Crash` can now land in. Both restored facts were re-measured rather than assumed: `MC_CrashF10`'s trace goes through `StorePersist`, `FsyncDir`, `StoreRename` and ends with the temporary name durable and the final one not, and the witness is red again at 3,424 states. The split also had to take `persisted_info_mutex` with it, and that was not foreseen: without a guard stopping a second store entering the window, a frame parked at the rename wrote a record computed long before over a newer one, which `MC_Merge` caught as a red on `Assert_validateInfo`. `VersionMetadataOnDisk::storeInfo` holds that mutex across the whole of `storeInfoUnlocked` (`:281-284`), so the guard is the code's and not a patch. Neither `DurabilityMonotone` nor `DiskTypeOK` needed extending for the split: the property's antecedent is the three sync actions and `DiskWithTmp` is not one of them, its record-and-temporary-file pair already covers what a sync may publish, and the non-layered clause already ties the two temporary-file bits together, which `DiskWithTmp` respects by making the name durable at once when there is no page cache |

## 2a. Bound-contract debts {#bound-contract-debts}

The bound contract allows bounds below the scenario matrix only while every witness of every property the
scenario checks stays red. Where that does not hold, the gap is recorded here rather than hidden by moving the
bound or dropping the property.

Task 5 of this plan closed every row of this table with a run, or moved what is left of it into a named later
task; the final-review dispatch then reopened `B2` as a placed coverage debt and added `B6`, and the closing
round added `B7`, so the table below is not the one task 5 left. A row closed by an argument is not closed: what pays a debt is a witness that fires at some stated
bounds, a model change, or a bound documented as a bound with the witnesses verified at it.

| Id | Scenario | What was not verified at the exhaustive bounds | Outcome |
|---|---|---|---|
| B1 | `SetSnapshot` and `SetSnapshotFixed` at `TID_MAX = 2`, `CSN_MAX = 35` | Five witnesses were green or did not finish there: `SingleRemover`, `NoUncommittedRead`, `NoLostRead`, `Assert_validateInfo_removal` and `Assert_getOldestSnapshot`'s sortedness conjunct | **closed but for one row**, in commit `8477e3a08794`. All five were run in `MC_SetSnapshotWitness` on the committed tree: `SingleRemover` red at 7,913,163 distinct states in 50 s, `NoUncommittedRead` red at 2,948,589 in 20 s, `NoLostRead` red at 1,315,030 in 9 s, the sortedness conjunct red at 407,927 in 3 s, the one figure in that table taken earlier on the same unchanged module. `Assert_validateInfo_removal` did not fire: killed at 108,439,476 distinct states after 674 s. That one row is **placed in plan 5, task 4 (budget and calibration)**, with that count |
| B2 | `SetSnapshot` and `SetSnapshotFixed` at `TID_MAX = 2`, `CSN_MAX = 35` | The witnesses of `NoPrematureDelete` and of `NoLostVisibleData` were green there, at 13,664,284 and 13,664,666 distinct states. Those two counts are from before model defect `M13`'s repair and were **not re-run after it**; the scenario has since measured 12,766,799 rather than 13,622,631, so the figures are the record of why the debt was raised, not of the tree it is closed on | **placed in plan 5, task 4 (budget and calibration)**, after the probe narrowed it. `MC_NonTxnF2` is that probe: the F2 shape with a non-transactional creator, which costs no transaction, at `TID_MAX = 2`, one session, one part, `SNAPSHOT_TARGETS = {33, 34}`. The **baseline** is red on both properties there, `NoPrematureDelete` after 9,132 distinct states and `NoLostVisibleData` after 9,193, in under a second each, with traces `traces/f2-nontxn-creator-two-transactions.txt` (30 states) and `traces/f2-nontxn-creator-lost-visible-data.txt`. A property whose baseline is violated at these bounds is not one the bounds make vacuous, which is what the debt doubted. The target had to be 33 rather than 34: a non-transactional creation is visible at every snapshot, so `FirstCSN` is no longer the value at which nothing is visible, which is the one thing `MC_SetSnapshotF2`'s bounds argument relied on. What the probe does **not** do is fire the two witnesses inside `SetSnapshot` itself: `MC_NonTxnF2` is a different module with a different action set, so what is shown is that these bounds do not make the properties vacuous, not that the scenario that owns them can falsify them. Firing them in `SetSnapshot` itself needs a third transaction, which is `M4`'s budget question, so the two rows go to plan 5, task 4 (budget and calibration) with the others rather than being counted as paid |
| B3 | `Merge` at one session | Three witnesses were green there: `NoUncommittedRead`, `Assert_validateInfo_removal` and `NoSpuriousStaleVersion` | **closed but for one row**, in commit `8477e3a08794`. `MC_MergeWitness` is `MC_Merge` at two sessions, for witness runs only: `NoSpuriousStaleVersion` red at 1,766,818 distinct states in 14 s, which is the row that was a real coverage loss rather than a missing transaction, and `NoUncommittedRead` red at 6,065,808 in 46 s. `Assert_validateInfo_removal` did not fire: killed at 34,184,268 after 279 s, **placed in plan 5, task 4 (budget and calibration)** |
| B4 | `NonTxnDrop` at `TID_MAX = 2`, `CSN_MAX = 34`, two sessions | The half that carries the removal batch had no finishing configuration, so its witnesses were unfired rather than red, and the rest of the `NonTxn` sweep was the undivided module's, carried and not reproducible | **closed but for four rows**, in commit `8477e3a08794` and the fix rounds after it. The half now has two finishing configurations, `MC_NonTxnDrop` at one transaction with the cleanup group and `MC_NonTxnDropTwo` at two without it (see `M15`), and the whole sweep was re-run at them: twelve witnesses red in the first, `Atomicity` red in the second, and the three rows that need the cleanup group and a second transaction at once, `SingleRemover`, `Assert_validateInfo_order` and `NoPrematureDelete`, red in `MC_NonTxnWitness` at 672,101, 41,585 and 644,152 distinct states. `NoDoubleRead` and `NoUncommittedRead` fire in no configuration that finishes: green by full exploration in both drop configurations and killed unfired in `MC_NonTxnWitness` at 40,810,177 and 33,628,474 distinct states. Both are **placed in plan 5, task 4 (budget and calibration)**, with those counts. The fix rounds added the five rows the first sweep had missed: `Assert_getOldestSnapshot_size` red at 2 states, its two `SetSnapshot`-sited siblings vacuously green, `NoNtStoreError` red at 2,466 through an alias on an existing one-site hook, and `Assert_validateInfo_removal` green over the whole witness-mutated space at 25,686,095 distinct states with nothing left on the queue, which is a verdict and a third placement for this debt. The fourth is `SingleRemover` on `NonTxnDropTwo`, whose witness-mutated space is larger than that scenario's own and was still growing at 65,525,357 when it was stopped. `NoFalseCorruption` is not a budget row: it is structurally unreachable here and is deferred to plan 3, task 4 (`SnapshotCrash` and `NonTxnCrash`), which builds the `NonTxnCrash` scenario |
| B6 | Every scenario that enables the cleanup group, after the cleanup decision and grab were split into two actions in the final-review fix commit | The witness sweeps were not re-run at the new action set. The split holds the parts lock from `CleanupDecide` to `CleanupGrab`, which removes interleavings, so a row that was red before it is not thereby red after it | **closed**, in the sweep commit that re-ran it. Every row of every cleanup-enabled scenario was re-run: 83 witness rows across `SetSnapshot`, `SetSnapshotWitness`, `SetSnapshotFixed`, `SetSnapshotF2Fixed`, `Merge`, `MergeWitness`, `NonTxnDrop`, `NonTxnInsert` and `NonTxnWitness`, plus the minimality halves of the three two-change witnesses, and the four rows of `NonTxnDropTwo` as the control that a scenario without the cleanup group does not move. **No row changed colour**: 63 red rows stayed red, 20 green rows stayed green, and all six minimality halves stayed green. The twelve exhaustive runs were re-run with them, all green or still producing their finding. The counts moved by about ten per cent in the direction the extra step predicts, and they are in `WITNESSES.md` and in `README.md`'s last table. The five rows the sweep records as **killed unfired** were not re-run, because a budget stop is not a verdict to re-derive: `Assert_validateInfo_removal` in `SetSnapshot`, in `SetSnapshotWitness` and in `MergeWitness`, `SingleRemover` in `NonTxnDropTwo`, and `NoDoubleRead` and `NoUncommittedRead` in `NonTxnWitness`. All five keep their documented counts, and the four of them that carry a plan 5, task 4 (budget and calibration) placement stay at it |
| B7 | `MergeWitness`, after `MergeBegin` gained the retention-registry entry | The two-session witness table's three rows, `NoSpuriousStaleVersion`, `NoUncommittedRead` and `Assert_validateInfo_removal`, were not re-run at the changed action. The one-session `Merge` sweep was re-run in full afterwards and no row changed colour, which is evidence about the change and not about this module | **placed in plan 5, task 4 (budget and calibration)**, the task that already owns this module's `Assert_validateInfo_removal` row. The three rows keep the counts `B3` records, and those counts are from before the horizon change |
| B8 | `KeeperUnknownWait` at `TID_MAX = 2`, `CSN_MAX = 35` | The configuration would not finish at the matrix bounds, so it was committed one transaction below them and the four witnesses that need a third transaction were unverified there | **closed** in `6a22f692e1a1`, by removing the reason rather than by paying the debt. The bound was a symptom of model defect `M32`: a client parked in `waitStateChange` could run the commit machine beside the updating thread, and that duplicated machine was most of the space. With the guard in place the matrix bounds finish at 11,537,098 distinct states in 1 min 55 s, where they had not finished at 71 million, and `MC_KeeperUnknownWait` differs from `MC_Keeper` in `WAIT_MODE` alone, as it was meant to. Nothing is owed |
| B10 | every scenario that checks `NoLostVisibleData` | `VisibleFrags` reads the visibility oracle and `part[p].pstate`, not the loaded `VersionInfo`. After `RestartLoadPart` has replaced a committed creator with `DummyTID` and `RolledBackCSN`, a real read already rejects the part while the property still counts it visible, because it is `Outdated`; the property fires only once the cleanup thread changes the state. So a visibility loss caused by the loader escapes it for as long as no cleanup is scheduled | **re-placed, with the repair written out and its cost measured**: plan 3, task 6 (budget, witness sweep, documents and debts). The repair is one conjunct, `IsVisibleImpl(r, txn[t].snapshot, t)` beside the oracle clause in `VisibleFrags`, which makes the property read what `filterVisibleDataParts` would return rather than what history says should be there. It shrinks the set on the right of a subset, so it can only strengthen the property. What it costs is why it moved rather than landing here. Eighteen configurations check `NoLostVisibleData` and every one of them has to be re-run before a strengthened form can be committed: `MC_Crash`, `MC_CrashLegacy`, `MC_CrashWitness`, `MC_Merge`, `MC_MergeWitness`, `MC_Keeper`, `MC_KeeperUnknownWait`, `MC_KeeperWitness`, `MC_KeeperUnknownWaitWitness`, `MC_SetSnapshotFixed`, `MC_SetSnapshotWitness`, `MC_SetSnapshotF2Fixed`, `MC_SetSnapshotF2Special`, `MC_SetSnapshotF2SpecialEV`, `MC_SetSnapshotF8`, `MC_SetSnapshotF9SiblingFixed`, `MC_NonTxnF2` and `MC_NonTxnF4`. Five are witness-only and three are finding modules that stop at the first violation, so ten run exhaustively, and they include the two largest in the tree. Plan 3, task 3 did not attempt it, and says so rather than shipping it unverified. The half that is answered stands: `MC_CrashF10` catches the same loader reclassification through `AckedWriteIsDurable` at the load itself, one step before any cleanup |
| B9 | `Crash` and `CrashF10` | The debt asked for the twenty-five rows of the `Crash` roster that the unsynced world was assumed to leave checkable. **That premise was wrong**, which is the answer rather than a reason to keep the debt open | **closed** in plan 3, task 3 (what a restart must not do). An unsynced metadata write can be lost in either direction, so every property relating the history variables to what the server holds is falsified there by construction. Measured red: `NoLostRead`, through a lost creation CSN leaving the loader with a tmp-only directory, and `NoResurrection` and `NoFutureRead`, through a lost removal, on top of the three the debt named. Predicted by the same argument and **not run**: `Atomicity`, `NoDoubleRead`, `NoUncommittedRead` and `RollbackNoLeak`. `ActiveSetShape` was predicted with them and the prediction was **wrong**: it is among the rows green below, because `RestartTableStart` puts every covered part in `outdated_queue` and `RestartLoadPart` gives an outdated-pass part `Outdated` unconditionally, so the loader cannot produce a covered part and its covering part `Active` at once however the removal was lost. `MC_CrashUnsynced` is therefore a finding module carrying `TypeOK`, `LogEntryNeeded` and `DurabilityMonotone`, which is `MC_CrashF10`'s shape. The payment is `MC_CrashUnsyncedFixed`, the same configuration under finding `F11`'s fix, carrying every row of the `Crash` roster that reads no history variable: `LockConsistent`, `ActiveSetShape`, `Assert_validateInfo`, `Assert_isVisible_fast`, `Assert_getOldestSnapshot`, `NoSpuriousStaleVersion`, `KillerNotStranded`, `NoTaskDrivenRollback`, `Assert_IsNonTransactionalDomain`, `PinnedNotDeleted` and `Assert_TailPtrNotRegressing`. **The green it rests on is from before the store split, and the post-split run does not finish.** At 37,271,456 distinct states the eleven rows were green; after `M51` restored the temporary-file window the same configuration was killed at 87,275,779 with 3.68 million still queued. The rung that owns that size is the window itself, not a bound: with nothing synced, the temporary file's two bits vary independently across the rename and the three sync actions can land on either side of it, which is the same factor that took `MC_CrashF12Fixed` from 21,804,360 to 63,191,040. Re-paying `B9` at bounds that finish is **placed in plan 3, task 6 (budget, witness sweep, documents and debts)** with that count, because the lever is a bound question and that task owns them.

Three further things this payment is **not** evidence for, said here rather than left to the reader. It runs at `TID_MAX = 1`, which does not exercise the multi-transaction merge shapes. `loadDataPartsFromDisk` walks the roots of the tree `PartLoadingTree::build` makes from the part names (`src/Storages/MergeTree/MergeTreeData.cpp:2993`), and requeues a root's children only when the root did not load `Active` (`:2827-2836`); everything else reaches the working set through the asynchronous outdated pass (`:3343`). A green at one transaction therefore says nothing about a covering part and its sources racing across two. The history-relating rows are not checked in that world and cannot be. And the module runs under `ENTRY_KEPT_UNTIL_CSN_DURABLE`, a fix no upstream server has, so the eleven rows are verified against the fixed system rather than against the unsynced default; the default is `MC_CrashUnsynced`, which stops at the first violation and checks nothing else |
| B5 | `NonTxnWitness` at `TID_MAX = 2`, `CSN_MAX = 35`, two sessions | The two-change witness `Assert_validateInfo_nocreation` was red, but its minimality was unverified: both half-runs were cut at 24.3 and 24.5 million distinct states with no violation | **closed** in commit `8477e3a08794`. The witness was re-run in `MC_NonTxnDrop`, which finishes: red at 1,895 distinct states, and both halves green by **full exploration** at 601,859 and 1,112,063, which is a verdict rather than a run that ran out of budget. The witness is minimal |

What plan 5, task 4 (budget and calibration) inherits is **eight** rows, two from B2, one from B1, one from B3 and four
from B4. Six of them are the same shape: a witness that removes a guard or a wait, on a scenario whose space is
already near the budget, so the witness run is larger than the exhaustive run it belongs to.

| Row | Scenario | What it reached |
|---|---|---|
| `Assert_validateInfo_removal` | `SetSnapshotWitness` | killed unfired at 108,439,476 after 674 s |
| `Assert_validateInfo_removal` | `MergeWitness` | killed unfired at 34,184,268 after 279 s |
| `Assert_validateInfo_removal` | `NonTxnDrop` | green over the whole witness-mutated space, 26,978,659 after the cleanup split and 25,686,095 before it, nothing on the queue: a verdict that one transaction is not enough |
| `NoDoubleRead` | `NonTxnWitness` | killed unfired at 40,810,177 after 342 s; verified in `Merge` |
| `NoUncommittedRead` | `NonTxnWitness` | killed unfired at 33,628,474 after 269 s; verified in `Base` at `TID_MAX = 3` |
| `SingleRemover` | `NonTxnDropTwo` | killed unfired at 65,525,357 with the queue growing, past the scenario's own 47,958,711; red in `MC_NonTxnWitness` and in `Base` |
| `NoPrematureDelete` | `SetSnapshot`, `SetSnapshotFixed` | green at the exhaustive bounds; red in `MC_SetSnapshotF2Fixed` and in `MC_NonTxnF2`, both of which are other modules. Needs a third transaction in this scenario |
| `NoLostVisibleData` | `SetSnapshot`, `SetSnapshotFixed` | green at the exhaustive bounds; red in `MC_SetSnapshotF2Fixed` and in `MC_NonTxnF2`. The same third transaction |

Every one of the eight is red somewhere, in `Base`, in `Merge` for `NoDoubleRead`, or in a sibling module for
the two of B2, so what is unverified is not that the property can be falsified but that it can be falsified
**in that scenario**. The task closes them with a view, a constraint
or a bound that fits the witness run, not by re-running them unchanged. Five of the six are unfired kills and
one, `Assert_validateInfo_removal` in the drop half, is a finished green, which is the stronger statement of
the two: there the witness provably cannot fire at one transaction.

One note on the `SetSnapshot` views survives the closing of these rows, because it is about the projections
rather than about the bounds. `h.content` is in `MC_SetSnapshot`'s view although that configuration checks no
property that reads it. It is there for uniformity with the sibling modules rather than because a property
needs it, and a finer view is sound in any case: it can only split states, never merge two that behave
differently. `part.pins` is the other way round in every module, including that one, because `CleanupDecide`
reads it.

## 3. Spec defects {#spec-defects}

Rows of `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` that the model contradicts,
with the correction the next revision folds in.

| Id | Spec row | What is wrong | Correction |
|---|---|---|---|
| S1 | `Atomicity`, section "Invariants and properties" | `C` is built from the parts the writer created and did not itself remove, minus those a later committed transaction visible to the reader removed. It never subtracts the reader's own uncommitted removals, although the precedence it cites for the writer, own removal over visible creation, is the reader's too: `VersionInfo::isVisible` returns false on `removal_tid == current_tid` (`VersionInfo.cpp:171`) before reaching the creation clause at `:178`. Counterexample F1 | Add to `C` the condition that `p` is not in the reader's own `h_removing`. `Atomicity` in `Invariants.tla` already carries it |
| S2 | the `NoAvoidableTermination` witness row | The row names scenario `Base`, but the witness it describes needs a `Fail` inside `afterCommit` that takes the server down, which requires `ProcessDown`, an action `Base` does not enable. `WITNESSES.md` defers the witness to plan 5. One of the two statements has to give | Move the row's scenario to the first one that enables `ProcessDown`. The property itself stays in `Base`, where it is checked but not yet falsifiable |
| S3 | `UpdLoadEntriesMap`, section "Updating thread" | The row puts the action at "`loadEntries` up to the `NOEXCEPT_SCOPE_STRICT` block", and then says that block is where the batch enters `tid_to_csn` under `TransactionLog::mutex`. The two halves contradict each other, and the code settles it: `TransactionLog::loadEntries` (`src/Interpreters/TransactionLog.cpp:161`) fills `tid_to_csn` and advances `last_loaded_entry` *inside* the `NOEXCEPT_SCOPE_STRICT` block, and only the block after it, under `running_list_mutex` (`:174`), moves `latest_snapshot`. An action that stopped before the block would publish nothing | Restate the boundary as "`loadEntries` through the `NOEXCEPT_SCOPE_STRICT` block". `README.md`, section 3, carries the corrected row |
| S4 | `CommitError`, section "Client and session" | The row attributes the `INVALID_TRANSACTION` of a `COMMIT` on a cancelled transaction to `beforeCommit`. `InterpreterTransactionControlQuery::executeCommit` (`src/Interpreters/InterpreterTransactionControlQuery.cpp:64`) refuses it earlier, unless `getState` is `RUNNING`, before `commitTransaction` is called at all, and that is the guard the model's action has. `beforeCommit`'s failed compare-and-exchange (`src/Interpreters/MergeTreeTransaction.cpp:308`) raises the same error only for a kill that lands after the interpreter's guard | Name the interpreter guard as the action's site and `beforeCommit` as the racing one. `README.md`, section 3, carries the corrected row |
| S5 | `AckedWriteIsDurable`, section "Invariants and properties" | The row admits a created part in `Outdated` only when the removal is committed, `h_removers[p] /= {}`. `DropOutdate` outdates a part under the parts lock, long before the transaction that drops it commits, so a part created by an acknowledged transaction and dropped by a still-running one is `Outdated` with no committed remover, and the literal row is red on the baseline. `Invariants.tla` therefore also admits a removal merely in flight, `part[p].lock /= EmptyTID`. The row's antecedent is also `h_effects`, which includes mutations, while the property tested only `h.creating` and `h.removing` | Admit an in-flight removal in the row, with the `DropOutdate` boundary as the reason. `Invariants.tla` carries the relaxation and cites this id; its antecedent now includes `h.mutations`, and `FlipAfterStoresStep` carries the row's mutation conjunct, both vacuous while `Mutations = {}` |
| S6 | the `SetSnapshot` row of the scenario matrix | The row lists `NoOutdatedLookup` among the properties the `SetSnapshot` scenario checks. `assertTIDIsNotOutdated` (`src/Interpreters/TransactionLog.cpp:656`) has exactly two call sites: `tryFinalizeUnknownStateTransactions` (`:387`), which is the action `UpdFinalizeUnknown`, and `getCSNAndAssert` (`:645`), which has no caller anywhere in the tree. The `SetSnapshot` scenario is `Base` plus `SetSnapshot`, the cleanup group and the updater's GC group, and enables neither, so the property is vacuous there whatever the trace. Its own witness row names scenario `Keeper`, which does enable the unknown-state group, so the two rows of the document disagree | Drop `NoOutdatedLookup` from the `SetSnapshot` row and keep it in `Keeper` and `SnapshotCrash`, which enable `UpdFinalizeUnknown`. `NoOutdatedLookup` is defined in `Invariants.tla` and is not in `MC_SetSnapshot.cfg`; `WITNESSES.md` records the vacuous run. The vacuity half of this row is retired by the Keeper-faults commit: `UpdFinalizeUnknown` exists, `MC_Keeper.cfg` checks the property, and `WITNESSES.md` carries its verdict there rather than `GREEN (vacuous)` |
| S7 | the bound contract, section "Scenario matrix" | The contract gives each scenario one set of bounds and requires both that the scenario be checked exhaustively at them and that every witness be red at them. `SetSnapshot` cannot have both: exhaustive at the matrix bounds does not finish, and every bound that does finish loses a witness. The two jobs have different costs, because a witness run stops at the first violation and an exhaustive run does not, so one number cannot serve both | Give each scenario two sets of bounds, exhaustive and witness, with the rule that the witness bounds are at least the exhaustive ones and that every witness is red at the witness bounds. `MC_SetSnapshotWitness` is the first instance. Plan 1 deleted `MC_BaseWitness` for having exactly this shape, which was premature: the right correction there was to name the pattern, not to remove it |
| S8 | `StableRead`, section "Invariants and properties" | The row states that the first and last read of `t` differ only by fragments of parts in `h_creating[t]` or `h_removing[t]`, with no qualifier about the snapshot those reads were taken at. `SET TRANSACTION SNAPSHOT` makes a transaction read at a different snapshot, so a read before it and a read after it are reads at two different snapshots and the literal row is red on the baseline for the statement working as intended. `SetSnapshot` in `Server.tla` therefore restarts the read baseline, which narrows the property to the span between two `SET TRANSACTION SNAPSHOT` statements | Qualify the row by snapshot. This is a property decision, not a transcription: `MergeTreeTransaction::setSnapshot` (`src/Interpreters/MergeTreeTransaction.cpp:52`) stores one value and resets nothing, so there is no code to cite for the reset. What it models is that the row's "first read" means the first read judged at the snapshot the last read is judged at |
| S9 | the `MergeSelect` row of the "Merge" table, and the paragraph under it | Both say the sources must be visible "to the merge transaction" and "to the merge's own snapshot". The predicate the code builds calls `isVisible` with the merge's snapshot and `Tx::EmptyTID` (`src/Storages/MergeTree/Compaction/PartsCollectors/MergeTreePartsCollector.cpp:88`): the merge's snapshot, but the **empty** tid, not the merge's own. With its own tid a part the merge's transaction had created or was removing would be judged by the own-creation and own-removal clauses of `VersionInfo::isVisible`, which is not what a merge asks. The row also omits the second refusal at `:91`, `isRemovalTIDLocked`, which the model has as `part[q].lock = EmptyTID` | Name the empty tid, and add the removal-lock test as a condition of its own. `MergeSelect` in `Server.tla` carries both and cites the two lines |
| S10 | the `MergePublish*` row | It releases `reserved[i]` at `PublishFlip`. `CurrentlyMergingPartsTagger::finalize` runs at the end of `MergePlainMergeTreeTask::finish` (`src/Storages/MergeTree/MergePlainMergeTreeTask.cpp:200`), after `transaction.commit` at `:161` and after `commitTransaction` at `:195`, so the reservation outlives the publication and the whole commit. The difference is observable: `DropLock` waits for every reservation to drain, so a `DROP PARTITION` is held off until the merge has committed, not until it has published | Move the release to the end of the commit. `MergePublishFlip` leaves `reserved` alone and `MergeCommitFinalize` clears it with the rest of the task record |
| S11 | the `MergeFail` row | "Exception anywhere before `MergeCommitBefore`" misses the commit step itself: `commitTransaction` reaches `beforeCommit`, whose failed compare-and-exchange (`src/Interpreters/MergeTreeTransaction.cpp:308`) raises `INVALID_TRANSACTION` for a transaction a `KILL` got to first, so the task can also fail at `Commit`. "The holder rolls the transaction back" is true only when the holder wins that same exchange in `MergeTreeTransaction::rollback` (`:382`); against a `KILL` it loses and rolls nothing back, and the killer drives the body. The row also implies the statement rollback is conditional on `M12` having been renamed, where `MergeTreeData::Transaction::isEmpty` tests `precommitted_parts`, which is model defect `M2`'s closure | Give the row the `Commit` failure and the lost exchange. `MergeFailTrigger` in `Server.tla` enumerates the three triggers and `MergeUnwind` carries the exchange |

Two more rows of the "Client and session" table are coarser than the model rather than wrong: `DropStart`
bundles `stopMergesAndWait`, the wait, `lockParts` and the selection of the visible parts into one row, which the
model splits into `DropStart` and `DropLock` because the wait is where another session's work interleaves; and
`StmtRollback` is one row for the two halves of `MergeTreeData::Transaction::rollback`, the per-part
`setAndStoreCreationCSN(RolledBackCSN)` before the parts lock and the state change under it, which the model
splits into `StmtRollbackMark` and `StmtRollbackDrop` for the same reason. The design document's own rule is
"one function or one step of a function", so neither split contradicts it and neither is filed above.

| Id | Spec row | What is wrong | Correction |
|---|---|---|---|
| S12 | `NtBatchRefusedUnchanged`, section "Invariants and properties" | The row states that on a refusal "every target's `mem`, stored record and `lock` equal their values recorded in `h_batch` at `NtBatchStart`". That is a statement about the whole world, and a concurrent actor falsifies it without the batch having written anything: `MergeTreeTransaction::rollback` clears a removal lock and stores `RolledBackCSN` without taking `lockParts`, so it runs beside a batch that holds it. The first `NonTxn` run was red on exactly that | State it over what the batch can write: no target carries a non-transactional removal it did not already carry, in memory or in the stored record, and no target is still locked by the batch. Equality is kept on the one field of the row a concurrent rollback cannot reach, the creation TID, which is written only on an `Absent` part; `ccsn` and `sv` come out, because the rollback writes both |
| S13 | the "Snapshot isolation" section, and the `NonTxn` row of the scenario matrix | The section is qualified only by "for transactions whose snapshot is not `EverythingVisibleCSN`". Concurrent non-transactional writes falsify four of its rows on the baseline: `StableRead` (a non-transactional `INSERT` between two reads of one transaction is visible to both), `NoLostRead` and `NoFutureRead` (model defect M14), and `NoLostVisibleData` (finding F4). The matrix's `NonTxn` row already does not name any of them, which is consistent with the section needing the qualifier and not with the section as written | Qualify the section: the isolation rows hold among transactional actors, and a concurrent non-transactional write is outside them. Say which rows survive unqualified, which are `ReadYourWrites`, `NoUncommittedRead` and `NoDoubleRead`, and name the scenario each of the other four is shown in. `Atomicity` is a fifth case and needs its own sentence: it survives unqualified, and only because it cannot see a non-transactional statement at all, being stated over a committed writer `u` that such a statement does not have. The qualifier the section owes it is therefore not an exemption but an admission -- no row here states that a non-transactional statement is atomic to readers, and the code does not make it one; see the withdrawal of F7 |
| S18 | the "Snapshot isolation" section and the `RollbackRestores` row | Both are stated over a transaction's snapshot without a qualifier for `Tx::EverythingVisibleCSN`, which `SET TRANSACTION SNAPSHOT` accepts and which makes `VersionInfo::isVisible` return true for every part before it looks at any CSN (`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:157-158`). A transaction at that snapshot reads uncommitted and rolled-back data by design, which finding `F8` shows | Qualify both: the isolation guarantees and the rollback-visibility guarantee hold at an ordinary snapshot, and `EverythingVisibleCSN` is an introspection escape hatch outside them. The model states it by leaving `RollbackNoLeak` out of the roster of the module that uses that target, and by `MC_SetSnapshotF8`, which shows the read |

One narrowing of the model against the code is deliberate and is argued in section 2 rather than here, because
it is a model defect and not a disagreement with the design document: `SetSnapshot` is guarded on `Running`
while `executeSetSnapshot` has no state check and `executeQueryImpl` exempts `ASTTransactionControl` from its
refusal of a query on a failed transaction (`src/Interpreters/executeQuery.cpp:2690-2695`). Row `M17` names
every operator that would be dead under the relaxation on the baseline, and the one configuration under which
it would not be.

| Id | Spec row | What is wrong | Correction |
|---|---|---|---|
| S20 | `ProcessDown`, section "Disk write faults" | The row reads "the effect of `Crash` on every server variable" and the prose says "its effect is that of `Crash` plus `down_cause := cause`", which the model took to include the cached disk layers. A process dying does not empty the kernel's page cache: the unsynced write survives it and is lost only to a loss of the page cache, which the tree describes as a device-level power loss or an unclean host or kernel reset (`src/Storages/NATS/StorageNATS.cpp:1681`). As written the model was pessimistic in a way that could manufacture a durability finding from a mere exception | Read "every server variable" literally, which is what it says: `disk` and `mdisk` are not server variables. `ProcessDown` resets the server's memory and leaves both disk layers standing; only `Crash` collapses the cached layers onto the durable ones. The model does this now |
| S19 | the `Crash` row of the scenario matrix | The row asks for `AckedWriteIsDurable` across a restart and for both values of `FSYNC_PART_DIRECTORY`. At `FALSE` nothing on the write path is fsynced, so an acknowledged `INSERT` may not survive a crash and the property is false by construction rather than by a defect; that is finding `F10`. `NoPrematureDelete` and `NoLostVisibleData` fall in the same window | Split the row at the constants. `MC_Crash` carries the whole roster with all three settings on; `MC_CrashF10` is the finding module and carries `AckedWriteIsDurable` alone with the metadata rename unsynced. The other twenty-five rows are checkable in the unsynced world and are checked nowhere there yet: that module is `MC_CrashUnsynced`, the `Crash` roster minus the three rows `F10` falsifies by construction, and it is placed in plan 3, task 3 (what a restart must not do), which must calibrate its bounds. The row's other targets, `NoResurrection`, `LogEntryNeeded`, `LegacyLoads` and `Assert_IsNonTransactionalDomain`, are operators of that task and exist in no `.tla` or `.cfg` file yet; they belong on the unsynced module, which is where the tmp-only load shape lives |

### A spec row that was wrongly doubted {#withdrawn}

The `validateInfo, removal` witness row was on this list until the bounds changed, and it comes off it.

The row names one change, "`DropStore` skipped". At `TID_MAX = 2` that change alone left the run green, and the
witness was built as a two-change one, the second change stopping `EnrolBody` from starting
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
part visible to another session's read, and it belongs with plan 3, task 6 (budget, witness sweep, documents and debts).

Rewriting the rollback actions turned out not to be what that witness needs, so it is re-placed rather than
paid here. Skipping the outdating, which is what the `ErrorIsAbsent` hook on `RollbackOutdateCreatedA` already
does, leaves the created part `Active` but not visible: `RollbackMarkCreated` has stamped `RolledBackCSN` on
it, and `VersionInfo::isVisible` returns false on `snapshot_version < creation_csn`. Skipping the marking
instead does not help either, because `TryGetCsn` answers `RolledBackCSN` for a transaction that has left
`running_list`. Making the part visible to another session's read means changing what `IsVisibleImpl` decides,
not what the rollback writes, so the witness is a visibility hook and belongs with the sweep.


`KillerNotStranded` appears nowhere in the design document: not as a property, not as a witness row. The ruling
recorded here is that it stays. It states something the model would otherwise not check at all, it holds on
every baseline run, and removing it would lose coverage to satisfy a bookkeeping rule. What the next spec
revision owes it is a row, with the witness that row implies: a rollback step that starts and never completes,
so that a killer parked at `KillWait` is never released. `Base` has no such step, so that witness goes to
plan 5, task 3 (liveness), alongside the other liveness witnesses.

Until those two witnesses exist, both properties are checked without ever having been shown to be falsifiable.
That is a debt, recorded here and in the gap table of `WITNESSES.md`, not a result.
