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
  \/ CommitFinalize(k) \/ CommitAck(k) \/ CommitUnknown(k) \/ FrameFail(k) \/ Refuse(k) \/ Fail(k)
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
                              \/ (\E o \in FrameOwners : StoreRead(p, o) \/ StorePersist(p, o) \/ StorePublish(p, o) \/ StoreRetry(p, o))
UpdaterNext == UpdLoadEntriesMap \/ UpdPublishSnapshot
UpdaterGCNext == UpdRemoveOldEntriesSetTail \/ (\E c \in RealCSNs : UpdRemoveOldEntriesDelete(c))
UpdaterUnknownNext == UpdReconnect \/ UpdSwapUnknownLists \/ (\E t \in Tids : UpdFinalizeUnknown(t))
CleanupNext == \E p \in Parts : CleanupGrab(p) \/ CleanupValidate(p) \/ CleanupDeleteOk(p) \/ CleanupDeleteFail(p)
TaskNext == \E i \in Tasks : MergeBegin(i) \/ MergeSelect(i) \/ MergeWrite(i) \/ MergeRename(i) \/ MergeFail(i) \/ MutFail(i)
                             \/ (\E m \in Mutations, p \in Parts : MutSelect(i, m, p) \/ MutWrite(i, m, p) \/ MutRename(i, m, p))
                             \/ (\E m \in Mutations : KillCancelTask(m, i))
MutationNext == \E m \in Mutations : MutDestroyOwner(m) \/ KillUnregister(m) \/ KillRollbackTxn(m) \/ KillRemoveFile(m) \/ KillRetry(m)
NtNext == \/ (\E p \in Parts : NtInsert(p) \/ NtBatchPreflight(p) \/ NtBatchLock(p) \/ NtBatchStore(p))
          \/ (\E B \in SUBSET Parts : NtBatchStart(B))
          \/ NtBatchEnd \/ NtDropCover
FaultNext == Crash \/ NoexceptFrameDown \/ (\E c \in {"StoreFault", "RetryExhausted", "Other"} : ProcessDown(c))
RestartNext == RestartLoadLog \/ RestartTableStart \/ RestartTablePublished \/ RestartOutdatedDone \/ RestartDone
               \/ (\E p \in Parts : RestartLoadPart(p))
               \/ (\E m \in Mutations : RestartLoadMutation(m))

AllNext == ClientNext \/ StoreNext \/ UpdaterNext \/ UpdaterGCNext \/ UpdaterUnknownNext \/ CleanupNext
           \/ TaskNext \/ MutationNext \/ NtNext \/ FaultNext \/ RestartNext
Spec == Init /\ [][AllNext]_vars

\* the Base scenario (spec matrix): client, store, updater load/publish, noexcept termination
BaseNext == ClientNext \/ StoreNext \/ UpdaterNext \/ NoexceptFrameDown
BaseSpec == Init /\ [][BaseNext]_vars
====
