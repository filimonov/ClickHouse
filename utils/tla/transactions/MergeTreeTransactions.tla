---- MODULE MergeTreeTransactions ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Invariants

TypeOK == KeeperTypeOK /\ DiskTypeOK /\ HistoryTypeOK /\ PartsTypeOK /\ ServerTypeOK

Init == KeeperInit /\ DiskInit /\ HistoryInit /\ PartsInit /\ ServerInit

\* ---- action groups (scenario modules compose their own Next from these)
ClientNext == \E k \in Sessions :
  \/ Begin(k) \/ CommitBefore(k) \/ CommitError(k) \/ CommitCreateCSN(k) \/ CommitReadOnly(k) \/ CommitFlip(k)
  \/ CommitFinalize(k) \/ CommitAck(k) \/ CommitUnknown(k) \/ CommitUnknownResolved(k)
  \/ FrameFail(k) \/ Refuse(k) \/ Fail(k)
  \/ RollbackStart(k) \/ KillReturn(k) \/ RollbackOnException(k) \/ RollbackReturn(k) \/ QueryOnCancelled(k) \/ SelectCapture(k) \/ SelectFinish(k) \/ PublishFlip(k) \/ StmtRollbackDrop(k)
  \/ DropStart(k) \/ DropLock(k) \/ DropOutdate(k)
  \/ (\E p \in Parts : InsertWrite(k, p) \/ InsertPreActive(k, p) \/ PublishStart(k, p) \/ PublishEnrol(k, p)
                       \/ PublishStore(k, p) \/ StmtRollbackMark(k, p) \/ SelectCheck(k, p)
                       \/ DropEnrol(k, p) \/ DropStore(k, p) \/ CommitStoreCreation(k, p) \/ CommitStoreRemoval(k, p))
  \/ (\E c \in RealCSNs \cup {NonTransactionalCSN, EverythingVisibleCSN} : SetSnapshot(k, c))
  \/ (\E m \in Mutations : MutPrepareWrite(k, m) \/ MutPrepareAttach(k, m) \/ MutRegister(k, m)
                           \/ CommitStoreMutation(k, m) \/ MutWait(k, m) \/ KillMutation(k, m))
  \/ (\E t \in Tids : KillTransaction(k, t) \/ RollbackCopyLists(k, t) \/ RollbackFinalize(k, t)
                      \/ (\E p \in Parts : RollbackMarkCreated(k, t, p) \/ RollbackOutdateCreated(k, t, p)
                                           \/ RollbackRestore(k, t, p) \/ RollbackUnlock(k, t, p))
                      \/ (\E m \in Mutations : RollbackKill(k, t, m)))
StoreNext == \E p \in Parts : \/ Fsync(p)
                              \/ (\E o \in FrameOwners : StoreRead(p, o) \/ StorePersist(p, o) \/ StoreRename(p, o) \/ StorePublish(p, o) \/ StoreRetry(p, o))
UpdaterNext == UpdLoadEntriesMap \/ UpdPublishSnapshot
UpdaterGCNext == UpdRemoveOldEntriesArm \/ UpdRemoveOldEntriesSetTail \/ UpdRemoveOldEntriesDone
                 \/ (\E c \in RealCSNs : UpdRemoveOldEntriesDelete(c))
UpdaterUnknownNext ==
  \/ UpdReconnect \/ UpdLoadNothing \/ UpdSwapUnknownLists \/ UpdFinalizeDone
  \/ (\E t \in Tids : UpdFinalizeUnknown(t) \/ UpdRollbackStart(t) \/ UpdRollbackLost(t)
                      \/ UpdCommitFlip(t) \/ UpdCommitFinalize(t)
                      \/ UpdRollbackCopyLists(t) \/ UpdRollbackFinalize(t)
                      \/ (\E p \in Parts : UpdCommitStoreCreation(t, p) \/ UpdCommitStoreRemoval(t, p)
                                           \/ UpdRollbackMarkCreated(t, p) \/ UpdRollbackOutdateCreated(t, p)
                                           \/ UpdRollbackRestore(t, p) \/ UpdRollbackUnlock(t, p)))
KeeperFaultNext ==
  \/ KeeperSessionExpire
  \/ (\E k \in Sessions, b \in BOOLEAN : CommitKeeperFault(k, b))
  \/ (\E i \in Tasks, b \in BOOLEAN : MergeCommitKeeperFault(i, b))
CleanupNext == \E p \in Parts : CleanupDecide(p) \/ CleanupGrab(p) \/ CleanupAbandon(p)
                                 \/ CleanupValidate(p) \/ CleanupDeleteOk(p) \/ CleanupDeleteFail(p)
TaskNext == \E i \in Tasks :
  \/ MergeBegin(i) \/ MergeSelect(i) \/ MergeWrite(i) \/ MergeRename(i)
  \/ MergePublishStart(i) \/ MergePublishFlip(i)
  \/ MergeCommitBefore(i) \/ MergeCommitCreateCSN(i) \/ MergeCommitReadOnly(i)
  \/ MergeCommitFlip(i) \/ MergeCommitFinalize(i) \/ MergeCommitUnknown(i)
  \/ MergeFail(i) \/ MergeStmtRollbackDrop(i) \/ MergeUnwind(i)
  \/ (\E q \in Parts : MergePublishEnrol(i, q) \/ MergePublishStore(i, q) \/ MergeStmtRollbackMark(i, q)
                       \/ MergeCommitStoreCreation(i, q) \/ MergeCommitStoreRemoval(i, q))
  \/ MutFail(i)
  \/ (\E m \in Mutations, p \in Parts : MutSelect(i, m, p) \/ MutWrite(i, m, p) \/ MutRename(i, m, p))
  \/ (\E m \in Mutations : KillCancelTask(m, i))
MutationNext == \E m \in Mutations : MutDestroyOwner(m) \/ KillUnregister(m) \/ KillRollbackTxn(m) \/ KillRemoveFile(m) \/ KillRetry(m)
\* The target set of a batch is always computed by its caller, never chosen: an "\E B \in SUBSET Parts" would
\* give the model 2^|Parts| branches the code does not have. NtDropPublish computes it and starts the batch.
NtInsertNext == \E k \in Sessions, p \in Parts : NtInsertWrite(k, p) \/ NtInsertPublish(k, p)
NtDropNext == \/ (\E k \in Sessions, p \in Parts : NtDropWrite(k, p) \/ NtDropPublish(k, p) \/ NtDropFlip(k, p)
                                                   \/ NtDropUnwindMark(k, p))
              \/ (\E k \in Sessions : NtDropUnwindDrop(k))
              \/ (\E p \in Parts : NtBatchPreflight(p) \/ NtBatchLock(p) \/ NtBatchStore(p))
              \/ NtBatchEnd
NtNext == NtInsertNext \/ NtDropNext
FaultNext == Crash \/ ProcessDown("Other")
RestartNext == RestartLoadLog \/ RestartTableStart \/ RestartTablePublished \/ RestartOutdatedDone \/ RestartDone
               \/ (\E p \in Parts : RestartLoadPart(p))
               \/ (\E m \in Mutations : RestartLoadMutation(m))

AllNext == ClientNext \/ StoreNext \/ UpdaterNext \/ UpdaterGCNext \/ UpdaterUnknownNext \/ CleanupNext
           \/ TaskNext \/ MutationNext \/ NtNext \/ FaultNext \/ RestartNext \/ KeeperFaultNext
Spec == Init /\ [][AllNext]_vars

\* the Base scenario (spec matrix): client, store, updater load/publish, noexcept termination
BaseNext == ClientNext \/ StoreNext \/ UpdaterNext \/ ProcessDown("Other")
BaseSpec == Init /\ [][BaseNext]_vars

\* the SetSnapshot scenario (spec matrix): Base + SetSnapshot + Cleanup* + Updater+GC
SetSnapshotNext == BaseNext \/ UpdaterGCNext \/ CleanupNext
SetSnapshotSpec == Init /\ [][SetSnapshotNext]_vars

\* the Merge scenario (spec matrix): Base + Merge* + Cleanup* + Updater+GC
MergeNext == BaseNext \/ TaskNext \/ CleanupNext \/ UpdaterGCNext
MergeSpec == Init /\ [][MergeNext]_vars

\* the Keeper scenario (spec matrix): Base + Merge* + Updater+GC + Updater+Unknown, Keeper faults
KeeperNext == BaseNext \/ TaskNext \/ UpdaterGCNext \/ UpdaterUnknownNext \/ KeeperFaultNext
KeeperSpec == Init /\ [][KeeperNext]_vars

SyncNext == \E p \in Parts : Fsync(p) \/ FsyncDir(p) \/ FsyncParent(p)
\* the Crash scenario (spec matrix): Base + Merge* + Cleanup* + Updater+GC + Restart*, Layered disk
CrashNext == BaseNext \/ TaskNext \/ CleanupNext \/ UpdaterGCNext \/ RestartNext \/ FaultNext \/ SyncNext
CrashSpec == Init /\ [][CrashNext]_vars

\* the NonTxn scenario (spec matrix): Base + NtInsert, NtBatch*, NtDropCover + Cleanup*. It is kept for the
\* modules that produce findings F4 to F7, which need both halves in one behaviour; no exhaustive run of it
\* finishes, which is why the scenario is checked as the two halves below.
NonTxnNext == BaseNext \/ NtNext \/ CleanupNext
NonTxnSpec == Init /\ [][NonTxnNext]_vars

\* The two halves the scenario is checked as. Each is a strict sub-scenario of NonTxn, so neither loses a race
\* to a bound reduction: NonTxnDrop keeps the whole of the removal batch and the transaction it races, and
\* NonTxnInsert keeps a non-transactional INSERT racing a transaction. What only the whole scenario has is a
\* behaviour that needs both, which is finding F5's route 2 and is produced by MC_NonTxnF5 instead.
NonTxnDropNext == BaseNext \/ NtDropNext \/ CleanupNext
NonTxnDropSpec == Init /\ [][NonTxnDropNext]_vars
\* The drop half at two transactions, without the cleanup thread. The half with the cleanup thread does not
\* finish at two transactions, and the witnesses that need a second transaction are exactly the ones it loses;
\* this is where they are shown. The cleanup properties are then out of its roster rather than vacuous in it.
NonTxnDropTwoNext == BaseNext \/ NtDropNext
NonTxnDropTwoSpec == Init /\ [][NonTxnDropTwoNext]_vars
NonTxnInsertNext == BaseNext \/ NtInsertNext \/ CleanupNext
NonTxnInsertSpec == Init /\ [][NonTxnInsertNext]_vars
====
