---- MODULE Server ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Parts
VARIABLES sys, client, stmt, mut, task
vars == <<zk, disk, mdisk, h, part, tlog, txn, sys, client, stmt, mut, task>>

\* ============================================================ records
TxnStates == {"Absent", "Running", "Committing", "Committed", "RolledBack"}
Holders == Actors
TxnPcs == {"Idle", "CommitCreateCSN", "CommitStoreCreation", "CommitStoreRemoval", "CommitStoreMutation",
           "CommitFlip", "CommitFinalize", "RollbackCopyLists", "RollbackKill", "RollbackMarkCreated",
           "RollbackOutdateCreated", "RollbackRestore", "RollbackUnlock", "RollbackFinalize"}
\* holders: the pointer holders, Session(k) for the client's MergeTreeTransactionHolder and Task(i) for a
\* background task. rb_driver: the caller that won the compare_exchange in MergeTreeTransaction::rollback and
\* therefore runs the rollback body. They are different things: a KILL wins the exchange, it destroys no
\* shared_ptr, so it must name a driver and leave the holders alone.
TxnRecord == [state : TxnStates, csn : AllCSNs, snapshot : AllCSNs, protected_snapshot : AllCSNs,
              creating : Seq(Parts), removing : Seq(Parts), mutations : SUBSET Mutations,
              holders : SUBSET Holders, rb_driver : Holders \cup {NoActor},
              mutex : Holders \cup {NoActor}, csn_notified : BOOLEAN,
              pc : TxnPcs, work : Seq(Parts)]
AbsentTxn == [state |-> "Absent", csn |-> UnknownCSN, snapshot |-> UnknownCSN, protected_snapshot |-> UnknownCSN,
              creating |-> <<>>, removing |-> <<>>, mutations |-> {}, holders |-> {}, rb_driver |-> NoActor,
              mutex |-> NoActor, csn_notified |-> FALSE, pc |-> "Idle", work |-> <<>>]

ClientPcs == {"Idle", "InsertWrite", "InsertPreActive", "PublishStart", "PublishEnrol", "PublishStore", "PublishFlip",
              "SelectCheck", "DropWait", "DropEnrol", "DropStore", "DropOutdate",
              "MutPrepareWrite", "MutPrepareAttach", "MutRegister",
              "Commit", "Rollback", "RollbackWait", "KillWait", "Refuse", "StmtRollbackMark", "StmtRollbackDrop",
              \* the non-transactional queries: an INSERT and the three steps of a DROP PARTITION
              "NtInsertWrite", "NtDropWrite", "NtDropFlip", "NtDropUnwindMark", "NtDropUnwindDrop"}
\* a read: the visible parts and the fragments they expand to, tagged with the visible root they came through
ReadType == [parts : SUBSET Parts, frags : SUBSET (Parts \X Parts \X (0..3))]
NoRead == [parts |-> {}, frags |-> {}]
Errors == {"None", "SERIALIZATION_ERROR", "INVALID_TRANSACTION", "STALE_VERSION", "LOGICAL_ERROR", "Injected"}
ClientRecord == [current : Tids \cup {EmptyTID}, outcome : Outcomes, outcome_tid : Tids \cup {EmptyTID},
                 last_error : Errors, first_read : ReadType, last_read : ReadType,
                 capture : SUBSET Parts, captured0 : SUBSET Parts, checked : SUBSET Parts, batch : SUBSET Parts,
                 waiting : {"None", "ForState", "ForLoad"}, pc : ClientPcs, work : Seq(Parts),
                 part : Parts \cup {"None"}, stale_interferences : 0..MAX_STORE_RETRIES, rb_detach : BOOLEAN, holds_blocker : BOOLEAN]
IdleClient == [current |-> EmptyTID, outcome |-> "None", outcome_tid |-> EmptyTID, last_error |-> "None",
               first_read |-> NoRead, last_read |-> NoRead, capture |-> {}, captured0 |-> {}, checked |-> {}, batch |-> {},
               waiting |-> "None", pc |-> "Idle", work |-> <<>>, part |-> "None", stale_interferences |-> 0, rb_detach |-> FALSE, holds_blocker |-> FALSE]

\* The `attached` component is gone. It recorded the precommitted parts addNewPartAndRemoveCovered had already
\* put into the outer transaction's creating_parts, and it was written by PublishStart and read by no action
\* once the statement rollback stopped excluding them: MergeTreeData::Transaction::rollback
\* (src/Storages/MergeTree/MergeTreeData.cpp:11126) marks and removes every part of precommitted_parts, attached
\* or not, which is model defect M2's closure and holds for the merge task's unwind as well as for the client's.
StmtRecord == [precommitted : SUBSET Parts, covered : SUBSET Parts, work : Seq(Parts)]
NoStmt == [precommitted |-> {}, covered |-> {}, work |-> <<>>]

MutStates == {"Absent", "Written", "Attached", "Registered", "Unregistered", "Killed"}
MutRecord == [mstate : MutStates, tasks : SUBSET Tasks, tid : AllTids, csn : AllCSNs,
              fail_reason : {"None", "Deadlock"}, file_owner : SUBSET {"Preparing", "Map"}, kill_retries : 0..NOEXCEPT_RETRY_BUDGET]
AbsentMutRecord == [mstate |-> "Absent", tasks |-> {}, tid |-> EmptyTID, csn |-> UnknownCSN,
                    fail_reason |-> "None", file_owner |-> {}, kill_retries |-> 0]

TaskPcs == {"Idle", "Select", "Write", "Rename", "PublishStart", "PublishEnrol", "PublishStore", "PublishFlip",
            "Commit", "Fail", "StmtRollbackMark", "StmtRollbackDrop", "Unwind"}
TaskRecord == [kind : {"Idle", "Merge", "Mutation"}, pc : TaskPcs,
               txn : Tids \cup {EmptyTID}, mutation : Mutations \cup {"None"}, source : Parts \cup {"None"},
               result : Parts \cup {"None"}, reserved : SUBSET Parts]
IdleTask == [kind |-> "Idle", pc |-> "Idle", txn |-> EmptyTID, mutation |-> "None", source |-> "None",
             result |-> "None", reserved |-> {}]

\* One NonTransactionalRemovalLocks object. `owner` is the actor whose thread runs the batch, which is the
\* frame owner of every store the batch starts; it is read only while `active`.
\* One object, not a function of actors, because a batch runs under lockParts from end to end: both callers
\* hold it (removePartsFromWorkingSet takes an acquired_lock at MergeTreeData.cpp:7034, Transaction::commit
\* takes one at :11219), so two batches can never overlap.
BatchType == [active : BOOLEAN, targets : Seq(Parts), cursor : 0..(Cardinality(Parts) + 1), phase : {"Lock", "Store"},
              locked : SUBSET Parts, skipped : SUBSET Parts, owner : FrameOwners]
NoBatchRec == [active |-> FALSE, targets |-> <<>>, cursor |-> 0, phase |-> "Lock", locked |-> {}, skipped |-> {},
               owner |-> <<"Cleanup", 0>>]

TLogRecord == [tid_start : [Tids -> AllCSNs], tid_to_csn : [Tids -> AllCSNs], latest_snapshot : LogCSNs,
               local_tid_counter : 0..TID_MAX, last_loaded_entry : LogCSNs, running_list : SUBSET Tids,
               snapshots_in_use : [Tids -> AllCSNs], tail_ptr : LogCSNs, updated_tail_ptr : BOOLEAN,
               unknown_state_list : SUBSET Tids, unknown_state_list_loaded : SUBSET Tids]
TLogInit == [tid_start |-> [t \in Tids |-> UnknownCSN], tid_to_csn |-> [t \in Tids |-> UnknownCSN],
             latest_snapshot |-> FirstCSN, local_tid_counter |-> 0, last_loaded_entry |-> FirstCSN, running_list |-> {},
             snapshots_in_use |-> [t \in Tids |-> UnknownCSN], tail_ptr |-> MaxReservedCSN, updated_tail_ptr |-> FALSE,
             unknown_state_list |-> {}, unknown_state_list_loaded |-> {}]

SysRecord == [server : {"Down", "LogUp", "TableLoading", "TableUp"}, completely_started : BOOLEAN,
              async_loading_jobs : 0..1, loaded_parts : SUBSET Parts, loaded_mutations : SUBSET Mutations,
              restarts : 0..RESTARTS_MAX, keeper_faults : 0..KEEPER_FAULTS_MAX, disk_faults : 0..DISK_FAULTS_MAX,
              query_faults : 0..QUERY_FAULTS_MAX, merges_blocker : 0..Cardinality(Sessions),
              parts_lock : Actors \cup {NoActor}, nt_batch : BatchType,
              updater_pc : {"Idle", "PublishSnapshot", "SetTail", "Delete", "Swap", "Finalize"},
              \* cleanup_part is the part MergeTreeData::grabOldParts has grabbed; "None" while the cleanup
              \* thread holds nothing. cleanup_pc is where that one grabbed part is in
              \* clearOldPartsFromFilesystem: "Validate" before assertHasValidVersionMetadata, "Delete" after it.
              cleanup_pc : {"Idle", "Validate", "Delete"}, cleanup_part : Parts \cup {"None"}]
SysInit == [server |-> "TableUp", completely_started |-> TRUE, async_loading_jobs |-> 0,
            loaded_parts |-> Parts, loaded_mutations |-> Mutations, restarts |-> 0, keeper_faults |-> 0,
            disk_faults |-> 0, query_faults |-> 0, merges_blocker |-> 0, parts_lock |-> NoActor, nt_batch |-> NoBatchRec,
            updater_pc |-> "Idle", cleanup_pc |-> "Idle", cleanup_part |-> "None"]

ServerTypeOK ==
  /\ txn \in [Tids -> TxnRecord]
  /\ tlog \in TLogRecord
  /\ sys \in SysRecord
  /\ client \in [Sessions -> ClientRecord]
  /\ stmt \in [Actors -> StmtRecord]
  /\ mut \in [Mutations -> MutRecord]
  /\ task \in [Tasks -> TaskRecord]

ServerInit ==
  /\ txn = [t \in Tids |-> AbsentTxn]
  /\ tlog = TLogInit
  /\ sys = SysInit
  /\ client = [k \in Sessions |-> IdleClient]
  /\ stmt = [a \in Actors |-> NoStmt]
  /\ mut = [m \in Mutations |-> AbsentMutRecord]
  /\ task = [i \in Tasks |-> IdleTask]

\* ============================================================ helpers
Sess(k) == <<"Session", k>>
Tsk(i) == <<"Task", i>>
Up == sys.server = "TableUp"
Cur(k) == client[k].current
HasTxn(k) == client[k].current /= EmptyTID
Effects(t) == txn[t].creating /= <<>> \/ txn[t].removing /= <<>> \/ txn[t].mutations /= {}
Visible(q, t) == IsVisibleImpl(q, txn[t].snapshot, t)
SnapshotFor(t) == IF Witness("StableRead") THEN tlog.latest_snapshot ELSE txn[t].snapshot
\* getActivePartsToReplace plus getCoveredOutdatedParts filtered by visibility
CoveredNow(p, t) == { q \in Parts : q \in Expand({p}) /\ q /= p /\ part[q].pstate \in {"Active", "Outdated"}
                                    /\ (part[q].pstate = "Active" \/ Visible(q, t)) }
\* getActivePartsToReplace's other output, the covering part: an Active part that already contains p's range.
\* Transaction::commit computes it twice, once before the NOEXCEPT_SCOPE (MergeTreeData.cpp:11246) and once
\* inside it (:11298), and on a non-empty answer it skips addNewPartAndRemoveCovered (:11282) and marks p
\* Outdated instead of Active (:11316). Both calls are under the same acquired_parts_lock, so they agree; the
\* window the check exists for is the one between renameTempPartAndReplace, which leaves p PreActive without
\* the lock, and the commit that takes it.
CoveringNow(p) == { c \in Parts : c /= p /\ part[c].pstate = "Active" /\ p \in Expand({c}) }
PinsWithout(pin) == [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {pin}]]
StartFrame(p, o, op, val, nx) == WithFrame(p, NewFrame(o, op, val, nx))
\* the next phase of the commit machine, skipping empty lists
NextCommitPc(t, after) ==
  IF after = "CommitStoreCreation" THEN (IF txn[t].removing /= <<>> THEN "CommitStoreRemoval" ELSE "CommitFlip")
  ELSE "CommitFlip"
NextCommitWork(t, after) == IF after = "CommitStoreCreation" THEN txn[t].removing ELSE <<>>
FirstCommitPc(t) == IF txn[t].creating /= <<>> THEN "CommitStoreCreation"
                    ELSE IF txn[t].removing /= <<>> THEN "CommitStoreRemoval" ELSE "CommitFlip"
FirstCommitWork(t) == IF txn[t].creating /= <<>> THEN txn[t].creating ELSE txn[t].removing
NextRollbackPc(t, after) ==
  CASE after = "RollbackCopyLists"     -> (IF txn[t].creating /= <<>> THEN "RollbackMarkCreated"
                                          ELSE IF txn[t].removing /= <<>> THEN "RollbackRestore" ELSE "RollbackFinalize")
    [] after = "RollbackMarkCreated"   -> "RollbackOutdateCreated"
    [] after = "RollbackOutdateCreated" -> (IF txn[t].removing /= <<>> THEN "RollbackRestore" ELSE "RollbackFinalize")
    [] after = "RollbackRestore"       -> "RollbackUnlock"
    [] after = "RollbackUnlock"        -> "RollbackFinalize"
NextRollbackWork(t, after) ==
  CASE after \in {"RollbackCopyLists"} -> (IF txn[t].creating /= <<>> THEN txn[t].creating ELSE txn[t].removing)
    [] after = "RollbackMarkCreated"   -> txn[t].creating
    [] after = "RollbackOutdateCreated" -> txn[t].removing
    [] after = "RollbackRestore"       -> txn[t].removing
    [] after = "RollbackUnlock"        -> <<>>

\* ============================================================ actor-generic bodies
\* A session is not the only actor that publishes a part and commits a transaction: a background merge does both
\* on its own thread, with the same C++ functions underneath. The bodies below are those functions, constraining
\* only the shared variables and taking the actor `a` where the session-shaped versions read `Sess(k)`. Each
\* client action is that body plus the session's guard and its `client'` clause; each merge action is the same
\* body plus the task's guard and its `task'` clause.

\* MergeTreeData::Transaction::commit (src/Storages/MergeTree/MergeTreeData.cpp:11219), first half under
\* lockParts: the covered parts are collected at :11246 and addNewPartAndRemoveCovered attaches p to the outer
\* transaction at :11280.
\* A part that already has a covering part is not attached to the outer transaction at all: the loop at :11279
\* calls addNewPartAndRemoveCovered only when covering_parts[idx] is null, so nothing enrols its covered parts,
\* nothing appends it to creating_parts, and afterCommit never stamps a creation CSN on it.
PublishStartEffect(a, t, p) ==
  LET obsolete == CoveringNow(p) /= {}
      C == IF obsolete THEN {} ELSE CoveredNow(p, t) IN
  /\ sys' = [sys EXCEPT !.parts_lock = a]
  /\ stmt' = [stmt EXCEPT ![a].covered = C, ![a].work = SetToSeq(C)]
  /\ txn' = IF obsolete THEN txn ELSE [txn EXCEPT ![t].creating = Append(@, p)]
  /\ h' = IF obsolete THEN h ELSE [h EXCEPT !.creating[t] = @ \cup {p}]
  /\ part' = IF obsolete THEN part ELSE [part EXCEPT ![p].pins = @ \cup {<<"Txn", t>>}]

\* MergeTreeTransaction::removeOldPart (src/Interpreters/MergeTreeTransaction.cpp:213), the granting branch:
\* the transaction's mutex, lockRemovalTID, the enrolment into removing_parts, and the start of the
\* removal-TID store. EnrolRefused and EnrolError are the two refusing branches.
EnrolGrantEffect(a, t, q) ==
  /\ part' = [StartFrame(q, a, "RemovalTID", t, FALSE) EXCEPT ![q].lock = t, ![q].pins = @ \cup {<<"Txn", t>>}]
  /\ txn' = [txn EXCEPT ![t].mutex = a, ![t].removing = Append(@, q)]
  /\ h' = [h EXCEPT !.removing[t] = @ \cup {q}]
EnrolRefused(t, q) ==
  \/ txn[t].state = "RolledBack"
  \/ ((part[q].lock /= EmptyTID \/ part[q].mem.rcsn /= UnknownCSN) /\ ~Witness("SingleRemover"))
EnrolError(t) == IF txn[t].state = "RolledBack" THEN "INVALID_TRANSACTION" ELSE "SERIALIZATION_ERROR"

\* the NOEXCEPT_SCOPE state loop of Transaction::commit (MergeTreeData.cpp:11288-11356)
\* OBSOLETE_IS_ROLLED_BACK is finding F6's fix: the obsolete part is stamped RolledBackCSN as well as moved to
\* Outdated, which is what makes it invisible to its own creator and removable by the cleanup thread. It is
\* written as a direct change to mem rather than as a store of its own, which is a refinement: the C++ would
\* call setAndStoreCreationCSN and go through the three steps. That costs nothing here, because RolledBackCSN in
\* memory over any stored record is exempt in both ValidateMetadataOK and RealDisagreement, and it keeps the fix
\* variant to one line; plan 5, task 4 (budget and calibration), which re-runs the calibration against the
\* upstream fixes, owes the steps if the fix is adopted.
PublishFlipEffect(a, p, C) ==
  LET obsolete == CoveringNow(p) /= {} IN
  /\ part' = [q \in Parts |->
                IF q = p
                THEN [part[q] EXCEPT !.pstate = IF obsolete THEN "Outdated" ELSE "Active",
                                     !.mem = IF obsolete /\ OBSOLETE_IS_ROLLED_BACK
                                             THEN [@ EXCEPT !.ccsn = RolledBackCSN] ELSE @]
                ELSE IF q \in C /\ ~Witness("ActiveSetShape") THEN [part[q] EXCEPT !.pstate = "Outdated"]
                ELSE part[q]]
  /\ stmt' = [stmt EXCEPT ![a] = NoStmt]
  /\ sys' = [sys EXCEPT !.parts_lock = NoActor]
  \* Under the fix the obsolete part is abandoned, so the visibility oracle must stop calling it visible: it is
  \* invisible to its creator and to everyone else, which is the whole point of the stamp. In the baseline it
  \* stays visible to its creator, which is what finding F6 is about, so the ghost is written only under the fix.
  /\ h' = IF obsolete /\ OBSOLETE_IS_ROLLED_BACK THEN [h EXCEPT !.abandoned = @ \cup {p}] ELSE h

\* the commit point: TransactionLog::commitTransaction's one sequential create
CommitCreateEffect(t) ==
  /\ KeeperCanAppend /\ zk.session = "Alive"
  /\ zk' = KeeperAppended(t)
  /\ h' = [h EXCEPT !.committed = @ \cup {t}, !.csn[t] = KeeperNextCsn,
                    !.removers = [p \in Parts |-> IF p \in h.removing[t] THEN @[p] \cup {t} ELSE @[p]]]
  /\ txn' = [txn EXCEPT ![t].pc = FirstCommitPc(t), ![t].work = FirstCommitWork(t)]

\* afterCommit: one setAndStoreCreationCSN / setAndStoreRemovalCSN per part; start the frame, then advance
CommitStoreEffect(a, t, p, op, phase) ==
  LET val == IF Witness("Assert_validateInfo_creator") /\ op = "CreationCSN" THEN h.csn[t] + 1
             ELSE IF Witness("Assert_validateInfo_order") /\ op = "CreationCSN" THEN CSN_MAX
             ELSE h.csn[t]
      skip == (Witness("Assert_isVisible_fast") \/ Witness("Assert_isVisible_fast_only1"))
              /\ op = "CreationCSN" /\ p \in h.removing[t] IN
  /\ txn[t].pc = phase /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ \/ /\ ~skip /\ ~HasFrame(p, a) /\ ApplyOp(op, val, part[p].mem) /= part[p].mem
        /\ part' = StartFrame(p, a, op, val, TRUE)
        /\ UNCHANGED txn
     \/ /\ (skip \/ FrameDone(p, a, op, val))
        /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN NextCommitWork(t, phase) ELSE Tail(@),
                               ![t].pc = IF Tail(txn[t].work) = <<>> THEN NextCommitPc(t, phase) ELSE phase]
        /\ UNCHANGED part

CommitFlipEffect(t) ==
  txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = h.csn[t], ![t].csn_notified = TRUE,
                      ![t].pc = "CommitFinalize", ![t].work = <<>>]

\* afterFinalize. `a` is the holder this step destroys: NoActor for a session, whose
\* MergeTreeTransactionHolder outlives the commit and is released at CommitAck, and Tsk(i) for a merge task,
\* whose holder is destroyed with the task. Removing NoActor from a set of actors is a no-op, and so is
\* removing it from a set of pins.
CommitFinalizeEffect(a, t) ==
  /\ tlog' = [tlog EXCEPT !.running_list = @ \ {t}, !.snapshots_in_use[t] = UnknownCSN]
  /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].creating = <<>>, ![t].removing = <<>>, ![t].mutations = {},
                         ![t].holders = @ \ {a}]
  /\ part' = [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {<<"Txn", t>>, a}]]

\* MergeTreeData::Transaction::rollback (src/Storages/MergeTree/MergeTreeData.cpp:11122), which is what the
\* destructor of the statement transaction runs, in two halves: the per-part setAndStoreCreationCSN of
\* RolledBackCSN at :11126, and removePartsFromWorkingSet for the same set under lockParts at :11181. Both
\* loops run over the whole of precommitted_parts, including a part addNewPartAndRemoveCovered has already
\* attached to the outer transaction, because precommitted_parts is emptied only by clear() at the end of
\* commit. The marking phase is a set-membership test plus a barrier rather than the work-queue idiom every
\* other multi-part loop uses: the C++ loop is synchronous and ordered, and the two shapes differ only when
\* more than one part is precommitted at once. No scenario through this plan reaches that, because a statement
\* publishes one part and a merge publishes one result; the queue conversion belongs to the first scenario in
\* which a statement can publish several, which is the mutation task of a later plan.
StmtRollbackMarkStart(a, p) ==
  /\ ~HasFrame(p, a) /\ part[p].mem.ccsn /= RolledBackCSN
  /\ part' = StartFrame(p, a, "CreationCSN", RolledBackCSN, TRUE)
StmtRollbackMarkDone(a) == \A q \in stmt[a].precommitted : FrameDone(q, a, "CreationCSN", RolledBackCSN)
\* h.abandoned records that these parts' creation was rolled back, which is the RolledBackCSN the marking phase
\* has just stored on each of them. The visibility oracle reads it; see History.tla.
StmtRollbackDropEffect(a) ==
  /\ part' = [p \in Parts |-> IF p \in stmt[a].precommitted THEN [part[p] EXCEPT !.pstate = "Outdated"] ELSE part[p]]
  /\ stmt' = [stmt EXCEPT ![a] = NoStmt]
  /\ h' = [h EXCEPT !.abandoned = @ \cup stmt[a].precommitted]

\* ============================================================ client: begin, set snapshot
Begin(k) ==
  /\ Up /\ ~HasTxn(k) /\ client[k].pc = "Idle"
  /\ tlog.local_tid_counter < TID_MAX
  /\ LET t == tlog.local_tid_counter + 1
         s == tlog.latest_snapshot IN
     \* beginTransaction pushes onto running_list and snapshots_in_use under one lock and keeps the iterator
     \* (src/Interpreters/TransactionLog.cpp:216-226), so the two move together. The witness of
     \* Assert_getOldestSnapshot's first conjunct breaks that lockstep at this one site.
     /\ tlog' = [tlog EXCEPT !.local_tid_counter = t, !.tid_start[t] = s, !.running_list = @ \cup {t},
                             !.snapshots_in_use[t] = IF Witness("Assert_getOldestSnapshot_size") THEN @ ELSE s]
     /\ txn' = [txn EXCEPT ![t] = [AbsentTxn EXCEPT !.state = "Running", !.snapshot = s, !.protected_snapshot = s,
                                     !.holders = {Sess(k)}]]
     /\ client' = [client EXCEPT ![k].current = t, ![k].first_read = NoRead, ![k].last_read = NoRead]
     \* the fragments visible at the snapshot this transaction started with; SetSnapshot recaptures it.
     /\ h' = [h EXCEPT !.content[t] = Frags({ r \in Parts : part[r].pstate \in {"Active", "Outdated"}
                                                           /\ OracleVisible(r, s, t) })]
  /\ UNCHANGED <<zk, disk, mdisk, part, sys, stmt, mut, task>>

\* executeSetSnapshot (src/Interpreters/InterpreterTransactionControlQuery.cpp:138) then
\* MergeTreeTransaction::setSnapshot (src/Interpreters/MergeTreeTransaction.cpp:52): one relaxed store into
\* `snapshot`. `protected_snapshot` and the snapshots_in_use entry are deliberately left alone, which is the
\* behaviour NoPrematureDelete is expected to catch; SET_SNAPSHOT_PROTECTS is the proposed fix (FINDINGS F2).
\* The `Running` guard is a narrowing: executeSetSnapshot checks only that a transaction exists, unlike
\* executeCommit and executeRollback just above it, so the code accepts the statement on a transaction another
\* session has already killed. Modelling that would add a snapshot write on a transaction whose entry the
\* finalizers have already cleared, which is a rollback-path question rather than a snapshot one; it is left to
\* plan 3, task 1 (Keeper faults at commit, the unknown-state pass, updater-driven commit and rollback), which
\* is where a transaction reaches the model in an unknown or rolled-back state. Model defect M17 records it.
SetSnapshot(k, c) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].state = "Running"
  /\ c \in SNAPSHOT_TARGETS
  \* setSnapshot stores the value it is given; storing the value the transaction already holds changes nothing
  \* the server can observe, so the model does not make it a step. Without this guard the action is enabled at
  \* every idle point of every running transaction and, because it also restarts the read baseline below, each
  \* of those no-ops produces a state.
  /\ c /= txn[Cur(k)].snapshot
  \* Two witnesses, because Assert_getOldestSnapshot states three things. `moves_protected` is the change the
  \* design document names, the fix applied to both halves of the same C++ object, and it falsifies the
  \* sortedness clause, which needs a transaction that began above FirstCSN and therefore a third transaction.
  \* `moves_entry` moves only the snapshots_in_use entry and leaves protected_snapshot, the ghost recording
  \* what beginTransaction inserted, where it was; that falsifies the per-entry clause with one transaction and
  \* the tail-pointer property with two, which is what makes both reachable at this scenario's bounds.
  /\ LET t == Cur(k)
         moves_protected == SET_SNAPSHOT_PROTECTS \/ Witness("Assert_getOldestSnapshot")
         moves_entry == moves_protected \/ Witness("Assert_getOldestSnapshot_entry") IN
     /\ (SET_SNAPSHOT_PROTECTS => c >= tlog.tail_ptr)
     /\ txn' = [txn EXCEPT ![t].snapshot = c, ![t].protected_snapshot = IF moves_protected THEN c ELSE @]
     /\ tlog' = [tlog EXCEPT !.snapshots_in_use[t] = IF moves_entry THEN c ELSE @]
     /\ h' = [h EXCEPT !.content[t] = Frags({ r \in Parts : part[r].pstate \in {"Active", "Outdated"}
                                                           /\ OracleVisible(r, c, t) })]
     \* The read baseline restarts, exactly as it does at Begin. StableRead compares a read against the first
     \* read taken at the snapshot it was judged by, and SET TRANSACTION SNAPSHOT is the sanctioned way to
     \* change that snapshot: a read before it and a read after it are reads at two different snapshots, and
     \* requiring them to agree would make the statement itself a violation. The first run of this scenario
     \* was red on StableRead for exactly that trace.
     /\ client' = [client EXCEPT ![k].first_read = NoRead, ![k].last_read = NoRead]
  /\ UNCHANGED <<zk, disk, mdisk, part, sys, stmt, mut, task>>

\* ============================================================ client: insert and publication
InsertWrite(k, p) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].state = "Running"
  /\ part[p].pstate = "Absent" /\ IsBase(p)
  /\ disk' = DiskWithDir(p)
  /\ part' = [StartFrame(p, Sess(k), "CreateTID", Cur(k), FALSE) EXCEPT ![p].pstate = "Temporary", ![p].deferrable = FALSE]
  /\ h' = [h EXCEPT !.creator[p] = Cur(k)]
  /\ client' = [client EXCEPT ![k].pc = "InsertWrite", ![k].part = p]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, stmt, mut, task>>

InsertPreActive(k, p) ==
  /\ client[k].pc = "InsertWrite" /\ client[k].part = p /\ FrameDone(p, Sess(k), "CreateTID", Cur(k))
  /\ sys.parts_lock = NoActor
  /\ part' = [part EXCEPT ![p].pstate = "PreActive"]
  /\ stmt' = [stmt EXCEPT ![Sess(k)].precommitted = @ \cup {p}]
  /\ client' = [client EXCEPT ![k].pc = "PublishStart"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, mut, task>>

\* Transaction::commit, first part under lockParts: covered parts, attach to the outer transaction
PublishStart(k, p) ==
  /\ client[k].pc = "PublishStart" /\ client[k].part = p /\ p \in stmt[Sess(k)].precommitted
  /\ sys.parts_lock = NoActor /\ txn[Cur(k)].state = "Running"
  /\ LET t == Cur(k) IN
     /\ PublishStartEffect(Sess(k), t, p)
     /\ client' = [client EXCEPT ![k].pc = IF CoveredNow(p, t) = {} THEN "PublishFlip" ELSE "PublishEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, mut, task>>

\* removeOldPart, first half (shared by Drop and Publish): mutex, checkIsNotCancelled, lockRemovalTID, enrol
\* nextpc is the client pc after success; on refusal the client goes to Refuse with last_error set
EnrolBody(k, q, nextpc) ==
  LET t == Cur(k) IN
  /\ txn[t].mutex = NoActor
  /\ \/ /\ txn[t].state = "RolledBack"
        /\ client' = [client EXCEPT ![k].last_error = EnrolError(t), ![k].pc = "Refuse"]
        /\ UNCHANGED <<txn, part, h>>
     \/ /\ txn[t].state = "Running" /\ EnrolRefused(t, q)
        /\ client' = [client EXCEPT ![k].last_error = EnrolError(t), ![k].pc = "Refuse"]
        /\ UNCHANGED <<txn, part, h>>
     \/ /\ txn[t].state = "Running" /\ ~EnrolRefused(t, q)
        /\ EnrolGrantEffect(Sess(k), t, q)
        /\ client' = [client EXCEPT ![k].pc = nextpc]

\* removeOldPart, second half: the store ran, release the mutex, next part or the phase's end
StoreDoneBody(k, q) ==
  LET t == Cur(k) IN
  \* the Assert_validateInfo_removal witness lets removeOldPart return without waiting for the removal-TID store
  \* it started, so a rollback can clear the removal TID under a later remover and CommitStoreRemoval then stores
  \* a removal CSN on a record that has none
  /\ (FrameDone(q, Sess(k), "RemovalTID", t) \/ Witness("Assert_validateInfo_removal"))
  /\ txn' = [txn EXCEPT ![t].mutex = NoActor]

PublishEnrol(k, q) ==
  /\ client[k].pc = "PublishEnrol" /\ sys.parts_lock = Sess(k) /\ stmt[Sess(k)].work /= <<>> /\ Head(stmt[Sess(k)].work) = q
  /\ EnrolBody(k, q, "PublishStore")
  /\ UNCHANGED <<zk, disk, mdisk, tlog, sys, stmt, mut, task>>
PublishStore(k, q) ==
  /\ client[k].pc = "PublishStore" /\ sys.parts_lock = Sess(k) /\ Head(stmt[Sess(k)].work) = q
  /\ StoreDoneBody(k, q)
  /\ LET w == Tail(stmt[Sess(k)].work) IN
     /\ stmt' = [stmt EXCEPT ![Sess(k)].work = w]
     /\ client' = [client EXCEPT ![k].pc = IF w = <<>> THEN "PublishFlip" ELSE "PublishEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, mut, task>>
PublishFlip(k) ==
  /\ client[k].pc = "PublishFlip" /\ sys.parts_lock = Sess(k)
  /\ PublishFlipEffect(Sess(k), client[k].part, stmt[Sess(k)].covered)
  /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, mut, task>>

\* MergeTreeData::Transaction::rollback (src/Storages/MergeTree/MergeTreeData.cpp:11122): creation_csn :=
\* RolledBackCSN for every part of precommitted_parts, then removePartsFromWorkingSet for all of them under
\* lockParts. Both loops run over the whole set, including a part that addNewPartAndRemoveCovered has already
\* attached to the outer transaction: precommitted_parts is emptied only by clear() at the end of commit.
\* The attached part stays in txn[t].creating and in h.creating[t], because MergeTreeTransaction::creating_parts
\* keeps its entry too, and nothing but afterFinalize clears it. That entry is harmless: Refuse always ends at
\* the outer rollback, whose MergeTreeTransaction::rollback (src/Interpreters/MergeTreeTransaction.cpp:377)
\* re-stores RolledBackCSN over the same creating_parts list, which
\* VersionMetadata::setAndStoreCreationCSN (VersionMetadata.cpp:123) turns into a no-op when the value already
\* stands, and re-calls removePartsFromWorkingSet, which the code documents as doing nothing for a part already
\* out of the working set. The model reproduces both no-ops: RollbackMarkCreated's FrameDone disjunct is
\* enabled at once for a part already at RolledBackCSN, and RollbackOutdateCreated leaves an Outdated part alone.
StmtRollbackMark(k, p) ==
  /\ client[k].pc = "StmtRollbackMark" /\ p \in stmt[Sess(k)].precommitted
  /\ \/ /\ StmtRollbackMarkStart(Sess(k), p)
        /\ UNCHANGED client
     \/ /\ FrameDone(p, Sess(k), "CreationCSN", RolledBackCSN) /\ StmtRollbackMarkDone(Sess(k))
        /\ client' = [client EXCEPT ![k].pc = "StmtRollbackDrop"]
        /\ UNCHANGED part
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>
StmtRollbackDrop(k) ==
  /\ client[k].pc = "StmtRollbackDrop" /\ sys.parts_lock = NoActor
  /\ StmtRollbackDropEffect(Sess(k))
  /\ client' = [client EXCEPT ![k].pc = "Rollback"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, sys, mut, task>>

\* ============================================================ client: select
SelectCapture(k) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ sys.parts_lock = NoActor /\ txn[Cur(k)].state = "Running"
  /\ LET S == { p \in Parts : part[p].pstate \in {"Active", "Outdated"} /\ ~(Witness("NoLostRead") /\ part[p].pstate = "Outdated") } IN
     /\ part' = [p \in Parts |-> IF p \in S THEN [part[p] EXCEPT !.pins = @ \cup {<<"Select", k>>}] ELSE part[p]]
     /\ client' = [client EXCEPT ![k].capture = S, ![k].captured0 = S, ![k].checked = {}, ![k].pc = "SelectCheck"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>
SelectCheck(k, p) ==
  /\ client[k].pc = "SelectCheck" /\ p \in client[k].captured0 /\ p \notin client[k].checked
  /\ LET t == Cur(k)
         s == IF Witness("NoFutureRead") /\ part[p].mem.ccsn = UnknownCSN THEN tlog.latest_snapshot ELSE SnapshotFor(t)
         vis == IsVisibleImpl(p, s, t) IN
     client' = [client EXCEPT ![k].checked = @ \cup {p}, ![k].capture = IF vis THEN @ ELSE @ \ {p}]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, sys, stmt, mut, task>>
SelectFinish(k) ==
  /\ client[k].pc = "SelectCheck" /\ client[k].checked = client[k].captured0
  \* An empty root contributes nothing, for FragsOf's reason; the tuples keep the root they came through, so
  \* that NoDoubleRead can ask whether one fragment arrived through two of them.
  /\ LET V == client[k].capture
         R == [parts |-> V,
               frags |-> UNION { IF part[c].payload.tomb THEN {}
                                 ELSE { <<c, q, part[q].payload.ver>> : q \in Expand({c}) } : c \in V }] IN
     /\ client' = [client EXCEPT ![k].first_read = IF @ = NoRead THEN R ELSE @, ![k].last_read = R,
                                 ![k].capture = {}, ![k].captured0 = {}, ![k].checked = {}, ![k].pc = "Idle"]
     /\ part' = PinsWithout(<<"Select", k>>)
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>

\* ============================================================ client: drop partition
DropStart(k) ==     \* stopMergesAndWait: take the blocker, then wait
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].state = "Running"
  /\ sys' = [sys EXCEPT !.merges_blocker = @ + 1]
  /\ client' = [client EXCEPT ![k].pc = "DropWait", ![k].holds_blocker = TRUE]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, stmt, mut, task>>
DropLock(k) ==      \* reservations drained, lockParts, the visible parts of the partition
  \* The three conjuncts are written out rather than run together on one line: a \A body extends as far to the
  \* right as it can, so "\A i \in Tasks : task[i].reserved = {} /\ sys.parts_lock = NoActor" puts the parts-lock
  \* test INSIDE the quantifier, where an empty Tasks makes it vacuous and the action loses its lockParts guard
  \* altogether. That is model defect M13; it was invisible in Merge, whose Tasks is a singleton, and in Base,
  \* where nothing else held the parts lock long enough to collide, and the NonTxn batch found it at once.
  /\ client[k].pc = "DropWait"
  /\ (\A i \in Tasks : task[i].reserved = {})
  /\ sys.parts_lock = NoActor
  /\ LET t == Cur(k)
         V == { p \in Parts : part[p].pstate \in {"Active", "Outdated"} /\ Visible(p, t) } IN
     /\ sys' = [sys EXCEPT !.parts_lock = Sess(k)]
     /\ client' = [client EXCEPT ![k].pc = IF V = {} THEN "DropOutdate" ELSE "DropEnrol", ![k].work = SetToSeq(V), ![k].batch = V]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, stmt, mut, task>>
DropEnrol(k, q) ==
  /\ client[k].pc = "DropEnrol" /\ sys.parts_lock = Sess(k) /\ client[k].work /= <<>> /\ Head(client[k].work) = q
  /\ EnrolBody(k, q, "DropStore")
  /\ UNCHANGED <<zk, disk, mdisk, tlog, sys, stmt, mut, task>>
DropStore(k, q) ==
  /\ client[k].pc = "DropStore" /\ sys.parts_lock = Sess(k) /\ Head(client[k].work) = q
  /\ StoreDoneBody(k, q)
  /\ LET w == Tail(client[k].work) IN
     client' = [client EXCEPT ![k].work = w, ![k].pc = IF w = <<>> THEN "DropOutdate" ELSE "DropEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, stmt, mut, task>>
DropOutdate(k) ==
  /\ client[k].pc = "DropOutdate" /\ sys.parts_lock = Sess(k)
  /\ LET D == client[k].batch IN
     /\ part' = [p \in Parts |-> IF p \in D /\ part[p].pstate = "Active" THEN [part[p] EXCEPT !.pstate = "Outdated"] ELSE part[p]]
     /\ sys' = [sys EXCEPT !.merges_blocker = @ - 1, !.parts_lock = NoActor]
     /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].work = <<>>, ![k].batch = {}, ![k].holds_blocker = FALSE]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, stmt, mut, task>>

\* ============================================================ client: commit
CommitBefore(k) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].pc = "Idle" /\ txn[Cur(k)].state = "Running"
  /\ LET t == Cur(k) IN
     /\ txn' = [txn EXCEPT ![t].state = "Committing", ![t].csn = CommittingCSN, ![t].csn_notified = FALSE,
                            ![t].pc = IF Effects(t) THEN "CommitCreateCSN" ELSE "CommitFlip"]
     /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
     /\ client' = [client EXCEPT ![k].pc = "Commit"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, stmt, mut, task>>
\* executeCommit on a transaction cancelled by KILL TRANSACTION: INVALID_TRANSACTION from beforeCommit
CommitError(k) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].state = "RolledBack"
  /\ LET t == Cur(k) IN
     /\ client' = [client EXCEPT ![k].outcome = "Error", ![k].outcome_tid = t, ![k].last_error = "INVALID_TRANSACTION"]
     /\ h' = [h EXCEPT !.outcome[t] = "Error"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, txn, sys, stmt, mut, task>>
\* the commit point: one sequential create (Ok outcome; faults are plan 3's)
CommitCreateCSN(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitCreateCSN"
     /\ CommitCreateEffect(t)
  /\ UNCHANGED <<disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
\* the isReadOnly branch: csn := snapshot, no Keeper request
CommitReadOnly(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitFlip" /\ ~Effects(t) /\ txn[t].state = "Committing"
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = txn[t].snapshot, ![t].csn_notified = TRUE, ![t].pc = "CommitFinalize"]
     /\ h' = [h EXCEPT !.csn[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
CommitStore(k, p, op, phase) ==
  /\ client[k].pc = "Commit"
  /\ CommitStoreEffect(Sess(k), Cur(k), p, op, phase)
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
CommitStoreCreation(k, p) == CommitStore(k, p, "CreationCSN", "CommitStoreCreation")
CommitStoreRemoval(k, p) == CommitStore(k, p, "RemovalCSN", "CommitStoreRemoval")
CommitFlip(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ Effects(t) /\ txn[t].state = "Committing"
     /\ (txn[t].pc = "CommitFlip" \/ (Witness("FlipAfterStores") /\ txn[t].pc \in {"CommitStoreCreation", "CommitStoreRemoval"}))
     /\ CommitFlipEffect(t)
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, stmt, mut, task>>
CommitFinalize(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitFinalize"
     /\ CommitFinalizeEffect(NoActor, t)
     /\ client' = [client EXCEPT ![k].waiting = IF WAIT_MODE = "ASYNC" THEN "None" ELSE "ForLoad"]
  /\ UNCHANGED <<zk, disk, mdisk, h, sys, stmt, mut, task>>
CommitAck(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].state = "Committed" /\ txn[t].pc = "Idle"
     /\ (client[k].waiting = "ForLoad" => tlog.latest_snapshot >= txn[t].csn)      \* waitForCSNLoaded
     /\ client' = [client EXCEPT ![k].outcome = "Acked", ![k].outcome_tid = t, ![k].current = EmptyTID,
                                 ![k].waiting = "None", ![k].pc = "Idle"]
     /\ txn' = [txn EXCEPT ![t].holders = @ \ {Sess(k)}]
     /\ h' = [h EXCEPT !.outcome[t] = "Acked"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, stmt, mut, task>>
CommitUnknown(k) == FALSE

\* ============================================================ client: refuse, fail, rollback, kill
\* a frame owned by the session ended in Error: the query fails with that error
FrameFail(k) ==
  /\ \E p \in Parts : FrameError(p, Sess(k)) /\ ~FrameOf(p, Sess(k)).noexcept_owner
     /\ LET f == FrameOf(p, Sess(k)) IN
        /\ client' = [client EXCEPT ![k].last_error = f.err, ![k].stale_interferences = f.interferences, ![k].pc = "Refuse"]
        /\ part' = WithoutFrame(p, Sess(k))
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>
\* Refuse: release what the query holds, statement rollback if needed, then the transaction rollback.
\* The statement rollback runs whenever the statement transaction is non-empty, because
\* MergeTreeData::Transaction::rollback (src/Storages/MergeTree/MergeTreeData.cpp:11122) is what the destructor
\* of that object runs, and isEmpty tests precommitted_parts, not the parts still unattached to the outer
\* transaction.
Refuse(k) ==
  /\ client[k].pc = "Refuse" /\ HasTxn(k)
  /\ LET t == Cur(k) IN
     /\ txn' = [txn EXCEPT ![t].mutex = IF @ = Sess(k) THEN NoActor ELSE @]
     /\ sys' = [sys EXCEPT !.parts_lock = IF @ = Sess(k) THEN NoActor ELSE @,
                           !.merges_blocker = IF client[k].holds_blocker THEN @ - 1 ELSE @]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.frames = { f \in @ : f.owner /= Sess(k) }, !.pins = @ \ {<<"Select", k>>}]]
     /\ client' = [client EXCEPT ![k].pc = IF stmt[Sess(k)].precommitted /= {} THEN "StmtRollbackMark" ELSE "Rollback",
                                 ![k].work = <<>>, ![k].batch = {}, ![k].capture = {}, ![k].captured0 = {}, ![k].checked = {},
                                 ![k].holds_blocker = FALSE]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, stmt, mut, task>>
\* an injected exception between two steps of a query
Fail(k) ==
  /\ sys.query_faults < QUERY_FAULTS_MAX /\ HasTxn(k)
  /\ client[k].pc \in {"InsertWrite", "InsertPreActive", "PublishStart", "PublishEnrol", "PublishStore",
                       "SelectCheck", "DropWait", "DropEnrol", "DropStore"}
  /\ sys' = [sys EXCEPT !.query_faults = @ + 1]
  /\ client' = [client EXCEPT ![k].last_error = "Injected", ![k].pc = "Refuse"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, stmt, mut, task>>
\* a query other than a TCL statement on a transaction that was rolled back (by KILL or by a failed query):
\* "Cannot execute query because current transaction failed. Expecting ROLLBACK statement"
QueryOnCancelled(k) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle" /\ txn[Cur(k)].state = "RolledBack"
  /\ client' = [client EXCEPT ![k].last_error = "INVALID_TRANSACTION"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, sys, stmt, mut, task>>
\* the explicit ROLLBACK statement: executeRollback
RollbackStart(k) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Idle"
  /\ LET t == Cur(k) IN
     /\ \/ /\ txn[t].state = "Running" /\ txn[t].pc = "Idle"           \* rollbackTransaction, then detach when it returns
           /\ txn' = [txn EXCEPT ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                                  ![t].pc = "RollbackCopyLists", ![t].rb_driver = Sess(k)]
           /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
           /\ client' = [client EXCEPT ![k].pc = "RollbackWait", ![k].rb_detach = TRUE]
        \/ /\ txn[t].state = "RolledBack"                                   \* already rolled back: detach now
           /\ client' = [client EXCEPT ![k].current = EmptyTID]
           /\ txn' = [txn EXCEPT ![t].holders = @ \ {Sess(k)}]              \* the holder's destructor
           /\ UNCHANGED h
        \/ /\ txn[t].state = "Committing"                                   \* LOGICAL_ERROR, no detach
           /\ client' = [client EXCEPT ![k].last_error = "LOGICAL_ERROR"]
           /\ UNCHANGED <<txn, h>>
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, stmt, mut, task>>
\* the end of a failed query: txn->onException() = rollbackTransaction; the session keeps the transaction bound
RollbackOnException(k) ==
  /\ Up /\ HasTxn(k) /\ client[k].pc = "Rollback"
  /\ LET t == Cur(k) IN
     /\ \/ /\ txn[t].state = "Running" /\ txn[t].pc = "Idle"
           /\ txn' = [txn EXCEPT ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                                  ![t].pc = "RollbackCopyLists", ![t].rb_driver = Sess(k)]
           /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
           /\ client' = [client EXCEPT ![k].pc = "RollbackWait", ![k].rb_detach = FALSE]
        \/ /\ txn[t].state /= "Running"                                     \* killed meanwhile, or already committing
           /\ client' = [client EXCEPT ![k].pc = "Idle"]
           /\ UNCHANGED <<txn, h>>
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, stmt, mut, task>>
\* rollbackTransaction returned to the session
RollbackReturn(k) ==
  /\ client[k].pc = "RollbackWait" /\ txn[Cur(k)].pc = "Idle"
  /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].current = IF client[k].rb_detach THEN EmptyTID ELSE @, ![k].rb_detach = FALSE]
  \* the explicit ROLLBACK detaches the session, which destroys its MergeTreeTransactionHolder; the rollback
  \* driven by a failed query does not, because the session keeps the transaction bound until it detaches
  /\ txn' = [txn EXCEPT ![Cur(k)].holders = IF client[k].rb_detach THEN @ \ {Sess(k)} ELSE @]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, stmt, mut, task>>
\* KILL TRANSACTION: the CAS, then the killer drives the rollback steps. The killer is a session running a
\* query, so it is between statements of its own (executeQuery); InterpreterKillQueryQuery looks the victim up
\* by tid hash and calls onException on whatever it finds, so the victim may be the killer's own transaction,
\* which the KILL does not detach. MergeTreeTransaction::rollback wins the compare_exchange on csn, which makes
\* its caller the only actor that runs the rollback body; the kill destroys no shared_ptr, so it names
\* rb_driver and leaves holders alone.
KillTransaction(k, t) ==
  /\ Up /\ client[k].pc = "Idle" /\ txn[t].state = "Running" /\ txn[t].pc = "Idle"
  /\ txn' = [txn EXCEPT ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                         ![t].pc = "RollbackCopyLists", ![t].rb_driver = Sess(k)]
  /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
  /\ client' = [client EXCEPT ![k].pc = "KillWait"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, stmt, mut, task>>
\* InterpreterKillQueryQuery calls onException, which rolls the transaction back on the killer's own thread,
\* so the KILL query returns only once the rollback it drives has finished.
KillReturn(k) ==
  /\ client[k].pc = "KillWait"
  /\ \A t \in Tids : txn[t].rb_driver /= Sess(k)
  /\ client' = [client EXCEPT ![k].pc = "Idle"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, sys, stmt, mut, task>>

\* the rollback machine: driven by the one caller that won the compare_exchange in MergeTreeTransaction::rollback
Drives(k, t) == txn[t].rb_driver = Sess(k)
RollbackCopyLists(k, t) ==
  /\ Drives(k, t) /\ txn[t].pc = "RollbackCopyLists" /\ txn[t].mutex = NoActor
  /\ part' = [p \in Parts |-> IF p \in Range(txn[t].creating) \cup Range(txn[t].removing)
                              THEN [part[p] EXCEPT !.pins = @ \cup {<<"Rollback", t>>}] ELSE part[p]]
  /\ txn' = [txn EXCEPT ![t].pc = NextRollbackPc(t, "RollbackCopyLists"), ![t].work = NextRollbackWork(t, "RollbackCopyLists")]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
\* per-part phases: start a frame or a state change, advance when done
RollbackMarkCreated(k, t, p) ==
  /\ Drives(k, t) /\ txn[t].pc = "RollbackMarkCreated" /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ \/ /\ ~HasFrame(p, Sess(k)) /\ part[p].mem.ccsn /= RolledBackCSN
        /\ part' = StartFrame(p, Sess(k), "CreationCSN", RolledBackCSN, TRUE)
        /\ UNCHANGED txn
     \/ /\ FrameDone(p, Sess(k), "CreationCSN", RolledBackCSN)
        /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN NextRollbackWork(t, "RollbackMarkCreated") ELSE Tail(@),
                               ![t].pc = IF Tail(txn[t].work) = <<>> THEN NextRollbackPc(t, "RollbackMarkCreated") ELSE @]
        /\ UNCHANGED part
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
RollbackOutdateCreated(k, t, p) ==
  /\ Drives(k, t) /\ txn[t].pc = "RollbackOutdateCreated" /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ sys.parts_lock = NoActor
  /\ part' = [part EXCEPT ![p].pstate = IF @ \in {"Active", "PreActive"} /\ ~Witness("ErrorIsAbsent") THEN "Outdated" ELSE @]
  /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN NextRollbackWork(t, "RollbackOutdateCreated") ELSE Tail(@),
                         ![t].pc = IF Tail(txn[t].work) = <<>> THEN NextRollbackPc(t, "RollbackOutdateCreated") ELSE @]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
RollbackRestore(k, t, p) ==
  /\ Drives(k, t) /\ txn[t].pc = "RollbackRestore" /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ sys.parts_lock = NoActor
  /\ part' = [part EXCEPT ![p].pstate = IF p \notin Range(txn[t].creating) /\ @ = "Outdated" /\ ~Witness("RollbackRestores")
                                        THEN "Active" ELSE @]
  /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN NextRollbackWork(t, "RollbackRestore") ELSE Tail(@),
                         ![t].pc = IF Tail(txn[t].work) = <<>> THEN NextRollbackPc(t, "RollbackRestore") ELSE @]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
\* setAndStoreRemovalTID(EmptyTID) then unlockRemovalTID (the witness unlocks first)
RollbackUnlock(k, t, p) ==
  /\ Drives(k, t) /\ txn[t].pc = "RollbackUnlock" /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ \/ /\ ~HasFrame(p, Sess(k)) /\ part[p].mem.rtid /= EmptyTID
        /\ part' = [StartFrame(p, Sess(k), "RemovalTID", EmptyTID, TRUE) EXCEPT ![p].lock = IF Witness("LockConsistent") THEN EmptyTID ELSE @]
        /\ UNCHANGED txn
     \/ /\ FrameDone(p, Sess(k), "RemovalTID", EmptyTID)
        /\ part' = [part EXCEPT ![p].lock = EmptyTID]
        /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN <<>> ELSE Tail(@),
                               ![t].pc = IF Tail(txn[t].work) = <<>> THEN "RollbackFinalize" ELSE @]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
RollbackFinalize(k, t) ==
  /\ Drives(k, t) /\ txn[t].pc = "RollbackFinalize"
  /\ tlog' = [tlog EXCEPT !.running_list = @ \ {t}, !.snapshots_in_use[t] = UnknownCSN]
  \* holders is deliberately left alone. MergeTreeTransaction::rollback destroys no shared_ptr: the holder goes
  \* away when its owner's MergeTreeTransactionHolder is destroyed, which is RollbackReturn and the detaching
  \* branch of RollbackStart for a session, and MergeCommitFinalize or MergeUnwind for a background task. With
  \* sessions alone that was invisible, because the session's next step detached anyway; a merge task holds its
  \* transaction across the rollback it does not drive, and clearing the set here would lose that.
  /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].creating = <<>>, ![t].removing = <<>>, ![t].mutations = {},
                         ![t].rb_driver = NoActor]
  /\ part' = [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {<<"Txn", t>>, <<"Rollback", t>>}]]
  /\ h' = [h EXCEPT !.rolled_back[t] = TRUE]
  /\ UNCHANGED <<zk, disk, mdisk, sys, client, stmt, mut, task>>
\* a noexcept frame (afterCommit / rollback) ended in Error: the process terminates (Terminate policy)
NoexceptFrameDown ==
  /\ \E p \in Parts, o \in FrameOwners : FrameError(p, o) /\ FrameOf(p, o).noexcept_owner
  /\ h' = [h EXCEPT !.down_cause = "Other"]
  /\ sys' = [sys EXCEPT !.server = "Down"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, txn, client, stmt, mut, task>>

\* ============================================================ updating thread
UpdLoadEntriesMap ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ LET new == { c \in DOMAIN zk.log : c > tlog.last_loaded_entry } IN
     /\ new /= {}
     /\ tlog' = [tlog EXCEPT !.tid_to_csn = [t \in Tids |-> IF \E c \in new : zk.log[c] = t THEN CHOOSE c \in new : zk.log[c] = t ELSE @[t]],
                             !.last_loaded_entry = Max(new)]
     /\ h' = [h EXCEPT !.loaded = [t \in Tids |-> @[t] \/ \E c \in new : zk.log[c] = t]]
     /\ sys' = [sys EXCEPT !.updater_pc = "PublishSnapshot"]
  /\ UNCHANGED <<zk, disk, mdisk, part, txn, client, stmt, mut, task>>
UpdPublishSnapshot ==
  /\ sys.updater_pc = "PublishSnapshot"
  /\ tlog' = [tlog EXCEPT !.latest_snapshot = tlog.last_loaded_entry]
  /\ sys' = [sys EXCEPT !.updater_pc = "Idle"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, txn, client, stmt, mut, task>>

\* removeOldEntries up to tail_ptr.store (src/Interpreters/TransactionLog.cpp:284-316).
\* The guard is `new /= old`, not `new > old`: the regressing case is what Assert_TailPtrNotRegressing catches,
\* and a guard that excluded it would make the property vacuous.
\* The updating thread runs `loadNewEntries(); removeOldEntries(); tryFinalizeUnknownStateTransactions();` in
\* one loop iteration on one thread, so removeOldEntries never interleaves with itself: one pass sets the tail
\* and then walks the removal loop to its end before another pass can start. The model gives the pass the
\* program counter sys.updater_pc, which UpdLoadEntriesMap already uses for the same reason: SetTail takes the
\* thread from "Idle" to "Delete", each removal is one step of the "Delete" phase, and UpdRemoveOldEntriesDone
\* ends the pass. Client steps still interleave freely between them, because the updater is a separate thread;
\* what the counter removes is a second pass starting inside the first, which the C++ cannot do.
\* The removal loop runs only when the tail actually moved: removeOldEntries returns at :317 when the new value
\* equals the old one, before the loop is reached.
UpdRemoveOldEntriesSetTail ==
  /\ sys.server \in {"LogUp", "TableLoading", "TableUp"} /\ sys.updater_pc = "Idle"
  /\ zk.session = "Alive"
  /\ sys.completely_started
  /\ (tlog.updated_tail_ptr \/ sys.async_loading_jobs = 0)
  /\ LET nt == IF Witness("NoOutdatedLookup") THEN tlog.latest_snapshot ELSE OldestSnapshot IN
     /\ nt /= zk.tail
     /\ zk' = KeeperWithTail(nt)
     /\ tlog' = [tlog EXCEPT !.tail_ptr = nt, !.updated_tail_ptr = TRUE]
     /\ sys' = [sys EXCEPT !.updater_pc = "Delete"]
  /\ UNCHANGED <<disk, mdisk, h, part, txn, client, stmt, mut, task>>

\* removeOldEntries, one iteration of the removal loop (src/Interpreters/TransactionLog.cpp:319-341).
\* The loop walks the loaded map tid_to_csn, not the znode list, so an entry the updater has not loaded is not a
\* candidate; it skips an entry whose tid.start_csn is at or above the new tail, and always keeps the entry whose
\* csn is the latest loaded one. ZNONODE counts as removed, which is why no `c \in DOMAIN zk.log` guard appears.
\* The C++ snapshots tid_to_csn and latest_snapshot once and then loops; the model re-reads them per iteration,
\* so it can delete an entry the C++ would have kept as "the latest one we fetched". That widens the behaviour
\* set; see FINDINGS.md section 2, model defect M3, for why it is sound here and where it gets settled.
UpdRemoveOldEntriesDelete(c) ==
  /\ sys.updater_pc = "Delete"
  /\ zk.session = "Alive"
  /\ c /= tlog.latest_snapshot
  /\ \E t \in Tids :
     /\ tlog.tid_to_csn[t] = c
     /\ tlog.tid_start[t] < tlog.tail_ptr
     /\ zk' = KeeperRemoved(c)
     /\ tlog' = [tlog EXCEPT !.tid_to_csn[t] = UnknownCSN]
     /\ h' = [h EXCEPT !.truncated = @ \cup {t}]
  /\ UNCHANGED <<disk, mdisk, part, txn, sys, client, stmt, mut, task>>

\* the removal loop ends and the thread leaves removeOldEntries (src/Interpreters/TransactionLog.cpp:341).
\* Unguarded, so the model can end a pass with entries the code's loop would still have removed. That widens
\* the behaviour set, which is safe for every safety property here; see FINDINGS.md section 2, M3.
UpdRemoveOldEntriesDone ==
  /\ sys.updater_pc = "Delete"
  /\ sys' = [sys EXCEPT !.updater_pc = "Idle"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>

\* ============================================================ store steps as root actions
StoreRead(p, o) == /\ Up /\ HasFrame(p, o) /\ StoreReadStep(p, o)
                   /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, stmt, mut, task>>
StorePersist(p, o) == /\ Up /\ HasFrame(p, o) /\ StorePersistStep(p, o)
                      /\ UNCHANGED <<zk, mdisk, h, tlog, txn, sys, client, stmt, mut, task>>
StorePublish(p, o) == /\ Up /\ HasFrame(p, o) /\ StorePublishStep(p, o)
                      /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, sys, client, stmt, mut, task>>
Fsync(p) == /\ Layered /\ disk' = DiskWithMetaSynced(p)
            /\ UNCHANGED <<zk, mdisk, h, part, tlog, txn, sys, client, stmt, mut, task>>

\* ============================================================ the cleanup thread
\* MergeTreeData::grabOldParts, src/Storages/MergeTree/MergeTreeData.cpp:4074, under lockParts: an Outdated part
\* whose version canBeRemoved (:4140), that nobody else holds (isSharedPtrUnique, :4150), and that is not an
\* empty part still covering an Outdated one (:4158, "First remove all covered parts, then remove covering empty
\* part"), moves to Deleting. The removal-time and mutation-parent conditions at :4167 are time and
\* zero-copy-replication bookkeeping and are not modelled; `force` covers them.
\* The model grabs one part per action where the code grabs a set under one lock. The only cross-part coupling
\* the lock provides is the atomicity of the state change, and no property of this plan reads the set of parts in
\* Deleting, so the refinement is recorded rather than removed. Placement if it ever matters: plan 5, task 3
\* (liveness), whose OutdatedEventuallyDeleted is the first property stated over parts on their way out.
CleanupGrab(p) ==
  /\ Up /\ sys.cleanup_pc = "Idle" /\ sys.parts_lock = NoActor
  /\ part[p].pstate = "Outdated"
  /\ (IF Witness("NoPrematureDelete") \/ Witness("NoLostVisibleData")
      THEN CanBeRemovedWith(p, tlog.latest_snapshot) ELSE CanBeRemovedImpl(p))
  /\ (part[p].pins = {} \/ Witness("PinnedNotDeleted"))
  /\ part[p].frames = {}
  /\ ~(part[p].payload.tomb /\ \E q \in Expand({p}) \ {p} : part[q].pstate = "Outdated")
  /\ part' = [part EXCEPT ![p].pstate = "Deleting"]
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Validate", !.cleanup_part = p]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, client, stmt, mut, task>>

\* chassert(assertHasValidVersionMetadata()) in IMergeTreeDataPart::remove, IMergeTreeDataPart.cpp:2928, on the
\* path clearPartsFromFilesystemAndRollbackIfError (MergeTreeData.cpp:4566) takes for each grabbed part.
\* This action and the validation half of CleanupDeleteFail model a DEBUG_OR_SANITIZER_BUILD: chassert expands
\* to abortOnFailedAssertion there and to (void)sizeof(!(x)) in release (base/base/defines.h:84-102), which does
\* not evaluate the argument at all, so a release build never validates and never refuses.
\* One outcome is not modelled. hasValidMetadata returns false from its catch-all (VersionMetadata.cpp:711-720)
\* rather than throwing, and a false there makes the chassert abort the process; the model turns every refusal
\* into the rollback to Outdated instead. Model defect M7 places that in plan 5, with ProcessDown and
\* NoAvoidableTermination.
CleanupValidate(p) ==
  /\ Up /\ sys.cleanup_pc = "Validate" /\ sys.cleanup_part = p
  /\ ValidateMetadataOK(p)
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Delete"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, client, stmt, mut, task>>

\* the success path of clearPartsFromFilesystemAndRollbackIfError: the directory is gone in both layers and
\* removePartsFinally (MergeTreeData.cpp:4217-4240) erases the part from data_parts_indexes under lockParts.
CleanupDeleteOk(p) ==
  /\ Up /\ sys.cleanup_pc = "Delete" /\ sys.cleanup_part = p
  /\ sys.parts_lock = NoActor                                 \* removePartsFinally takes lockParts at :4223
  /\ disk' = DiskWithoutDir(p)
  /\ part' = [part EXCEPT ![p].pstate = "Deleted", ![p].deferred_on = FALSE, ![p].deferred = EmptyInfo]
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Idle", !.cleanup_part = "None"]
  /\ UNCHANGED <<zk, mdisk, h, tlog, txn, client, stmt, mut, task>>

\* rollbackDeletingParts, MergeTreeData.cpp:4204-4215: back to Outdated, under lockParts. Two producers: the CORRUPTED_DATA that
\* hasValidMetadata throws, and the filesystem error of clearPartsFromFilesystemImpl. The second needs a disk
\* fault, which is plan 5's; until then the disjunct is FALSE and is written out so the action is complete.
CleanupDeleteFail(p) ==
  /\ Up /\ sys.cleanup_part = p
  /\ sys.parts_lock = NoActor                                 \* rollbackDeletingParts takes lockParts at :4207
  /\ \/ (sys.cleanup_pc = "Validate" /\ ~ValidateMetadataOK(p))
     \/ (sys.cleanup_pc = "Delete" /\ FALSE)      \* the filesystem-error path: plan 5, DISK_FAULTS_MAX > 0
  /\ part' = [part EXCEPT ![p].pstate = "Outdated"]
  /\ sys' = [sys EXCEPT !.cleanup_pc = "Idle", !.cleanup_part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, client, stmt, mut, task>>

\* ============================================================ the background merge task
\* A merge is the first actor that is not a session. It owns its transaction outright: a
\* MergeTreeTransactionHolder built with autocommit = false (StorageMergeTree.cpp:2226-2227), so Tsk(i) is the
\* whole of txn[t].holders; it issues no SELECT; and it commits with throw_on_unknown_status = false
\* (MergePlainMergeTreeTask.cpp:195), which matters only from the Keeper-fault plan on.
\* Holds(i) is the shared_ptr that keeps the transaction alive. Every step of the task reads it, which is what
\* makes txn.holders a field the model uses rather than only writes.
Holds(i) == Tsk(i) \in txn[task[i].txn].holders

\* StorageMergeTree::scheduleDataProcessingJob (src/Storages/StorageMergeTree.cpp:2203): beginTransaction at
\* :2226 under the transactions_enabled gate. sys.merges_blocker is the merges_blocker.isCancelled() check at
\* :2240, which a transactional DROP PARTITION holds through stopMergesAndWait.
MergeBegin(i) ==
  /\ Up /\ task[i].kind = "Idle" /\ task[i].pc = "Idle"
  /\ sys.merges_blocker = 0
  /\ tlog.local_tid_counter < TID_MAX
  /\ LET t == tlog.local_tid_counter + 1
         s == tlog.latest_snapshot IN
     /\ tlog' = [tlog EXCEPT !.local_tid_counter = t, !.tid_start[t] = s, !.running_list = @ \cup {t},
                             !.snapshots_in_use[t] = s]
     /\ txn' = [txn EXCEPT ![t] = [AbsentTxn EXCEPT !.state = "Running", !.snapshot = s,
                                     !.protected_snapshot = s, !.holders = {Tsk(i)}]]
     /\ task' = [task EXCEPT ![i].kind = "Merge", ![i].txn = t, ![i].pc = "Select"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, sys, client, stmt, mut>>

\* selectPartsToMerge with a transaction (StorageMergeTree.cpp:1680) through the predicate of
\* Compaction/PartsCollectors/MergeTreePartsCollector.cpp:85-92: every source must be visible at the merge's
\* snapshot with the EMPTY tid rather than the merge's own, must not be locked for removal, and must pass
\* canUsePartInMerges (:98), which is where currently_merging_mutating_parts excludes a part another task has
\* reserved. The reservation itself is taken by CurrentlyMergingPartsTagger's constructor
\* (StorageMergeTree.cpp:918-923), whose LOGICAL_ERROR "Tagging already tagged part" is the reservation clause
\* of ActiveSetShape.
\* The universe gives each covering part exactly one source set, so the merge is always the full cover.
MergeSelect(i) ==
  /\ Up /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Select" /\ sys.merges_blocker = 0
  /\ \E r \in Parts :
     /\ Covers[r] /= {} /\ part[r].pstate = "Absent"
     /\ \A q \in Covers[r] :
        /\ part[q].pstate \in {"Active", "Outdated"}
        /\ IsVisibleImpl(q, txn[task[i].txn].snapshot, EmptyTID)
        /\ part[q].lock = EmptyTID
        /\ \A j \in Tasks : q \notin task[j].reserved
     /\ task' = [task EXCEPT ![i].pc = "Write", ![i].reserved = Covers[r], ![i].result = r]
     /\ part' = [q \in Parts |-> IF q \in Covers[r] THEN [part[q] EXCEPT !.pins = @ \cup {Tsk(i)}] ELSE part[q]]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, stmt, mut>>

\* MergeTask: setAndStoreCreationTID on the result. Its payload is empty of its own: a covering part's content
\* is its sources', read through Expand, so only `tomb` matters and a merge result is not a tombstone.
MergeWrite(i) ==
  /\ Up /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Write"
  /\ LET r == task[i].result IN
     /\ part[r].pstate = "Absent"
     /\ disk' = DiskWithDir(r)
     \* the task holds the result from here on: mergePartsToTemporaryPart's part through merge_task, and then
     \* MergePlainMergeTreeTask::new_part (src/Storages/MergeTree/MergePlainMergeTreeTask.cpp:156), until the
     \* task object is destroyed. That is what isSharedPtrUnique (MergeTreeData.cpp:4150) sees, and without it
     \* the cleanup thread can take a result the statement rollback has just outdated while the task still
     \* holds it.
     /\ part' = [StartFrame(r, Tsk(i), "CreateTID", task[i].txn, FALSE) EXCEPT
                   ![r].pstate = "Temporary", ![r].deferrable = FALSE, ![r].pins = @ \cup {Tsk(i)}]
     /\ h' = [h EXCEPT !.creator[r] = task[i].txn]
     /\ task' = [task EXCEPT ![i].pc = "Rename"]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, client, stmt, mut>>

\* renameMergedTemporaryPart (src/Storages/MergeTree/MergeTreeDataMergerMutator.cpp:526), called from
\* MergePlainMergeTreeTask::finish (:160): the result enters the statement transaction as PreActive.
MergeRename(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Rename"
  /\ LET r == task[i].result IN
     /\ FrameDone(r, Tsk(i), "CreateTID", task[i].txn)
     /\ sys.parts_lock = NoActor
     /\ part' = [part EXCEPT ![r].pstate = "PreActive"]
     /\ stmt' = [stmt EXCEPT ![Tsk(i)].precommitted = @ \cup {r}]
     /\ task' = [task EXCEPT ![i].pc = "PublishStart"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, mut>>

\* transaction.commit() at MergePlainMergeTreeTask.cpp:161, which is the same Transaction::commit a session's
\* INSERT runs. The Running guard is addNewPart's checkIsNotCancelled (MergeTreeTransaction.cpp:207): on a
\* transaction a KILL has already rolled back this action is disabled and MergeFail takes the task instead.
MergePublishStart(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "PublishStart"
  /\ sys.parts_lock = NoActor /\ txn[task[i].txn].state = "Running"
  /\ LET r == task[i].result IN
     /\ r \in stmt[Tsk(i)].precommitted
     /\ PublishStartEffect(Tsk(i), task[i].txn, r)
     /\ task' = [task EXCEPT ![i].pc = IF CoveringNow(r) = {} /\ CoveredNow(r, task[i].txn) /= {}
                                       THEN "PublishEnrol" ELSE "PublishFlip"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, client, mut>>

MergePublishEnrol(i, q) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "PublishEnrol" /\ sys.parts_lock = Tsk(i)
  /\ stmt[Tsk(i)].work /= <<>> /\ Head(stmt[Tsk(i)].work) = q
  /\ LET t == task[i].txn IN
     /\ txn[t].mutex = NoActor
     /\ \/ /\ EnrolRefused(t, q)
           /\ task' = [task EXCEPT ![i].pc = "Fail"]
           /\ UNCHANGED <<txn, part, h>>
        \/ /\ ~EnrolRefused(t, q) /\ txn[t].state = "Running"
           /\ EnrolGrantEffect(Tsk(i), t, q)
           /\ task' = [task EXCEPT ![i].pc = "PublishStore"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, sys, stmt, client, mut>>

MergePublishStore(i, q) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "PublishStore" /\ sys.parts_lock = Tsk(i)
  /\ stmt[Tsk(i)].work /= <<>> /\ Head(stmt[Tsk(i)].work) = q
  /\ LET t == task[i].txn IN
     /\ (FrameDone(q, Tsk(i), "RemovalTID", t) \/ Witness("Assert_validateInfo_removal"))
     /\ txn' = [txn EXCEPT ![t].mutex = NoActor]
  /\ LET w == Tail(stmt[Tsk(i)].work) IN
     /\ stmt' = [stmt EXCEPT ![Tsk(i)].work = w]
     /\ task' = [task EXCEPT ![i].pc = IF w = <<>> THEN "PublishFlip" ELSE "PublishEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, mut>>

\* reserved[i] is NOT released here: CurrentlyMergingPartsTagger::finalize runs at the end of
\* MergePlainMergeTreeTask::finish (:200), after transaction.commit() at :161 and after commitTransaction at
\* :195. That is spec defect S10.
MergePublishFlip(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "PublishFlip" /\ sys.parts_lock = Tsk(i)
  /\ PublishFlipEffect(Tsk(i), task[i].result, stmt[Tsk(i)].covered)
  /\ task' = [task EXCEPT ![i].pc = "Commit"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, mut>>

\* TransactionLog::commitTransaction(txn_, throw_on_unknown_status = false), MergePlainMergeTreeTask.cpp:195
MergeCommitBefore(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit"
  /\ txn[task[i].txn].pc = "Idle" /\ txn[task[i].txn].state = "Running"
  /\ LET t == task[i].txn IN
     /\ txn' = [txn EXCEPT ![t].state = "Committing", ![t].csn = CommittingCSN, ![t].csn_notified = FALSE,
                            ![t].pc = IF Effects(t) THEN "CommitCreateCSN" ELSE "CommitFlip"]
     /\ h' = [h EXCEPT !.snapshot[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
MergeCommitCreateCSN(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "CommitCreateCSN"
  /\ CommitCreateEffect(task[i].txn)
  /\ UNCHANGED <<disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
MergeCommitStore(i, p, op, phase) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit"
  /\ CommitStoreEffect(Tsk(i), task[i].txn, p, op, phase)
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
MergeCommitStoreCreation(i, p) == MergeCommitStore(i, p, "CreationCSN", "CommitStoreCreation")
MergeCommitStoreRemoval(i, p) == MergeCommitStore(i, p, "RemovalCSN", "CommitStoreRemoval")
MergeCommitFlip(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit"
  /\ Effects(task[i].txn) /\ txn[task[i].txn].state = "Committing"
  /\ (txn[task[i].txn].pc = "CommitFlip"
      \/ (Witness("FlipAfterStores") /\ txn[task[i].txn].pc \in {"CommitStoreCreation", "CommitStoreRemoval"}))
  /\ CommitFlipEffect(task[i].txn)
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, stmt, mut, task>>
\* the isReadOnly branch of commitTransaction. Dead for a merge: the task always creates its result, so
\* Effects(t) holds at the commit. It is written because the commit machine has the branch and because the
\* covering-part branch of PublishStartEffect would make it live if it were reachable (model defect M10): a
\* result that already has a covering part is never attached to the transaction, which then commits with
\* nothing in creating or removing.
MergeCommitReadOnly(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit"
  /\ txn[task[i].txn].pc = "CommitFlip" /\ ~Effects(task[i].txn) /\ txn[task[i].txn].state = "Committing"
  /\ LET t == task[i].txn IN
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = txn[t].snapshot, ![t].csn_notified = TRUE,
                            ![t].pc = "CommitFinalize"]
     /\ h' = [h EXCEPT !.csn[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
\* merge_mutate_entry->finalize() at the end of MergePlainMergeTreeTask::finish (:200): the reservation, the
\* source parts the future part held, and the transaction holder all go here, not at the publication.
MergeCommitFinalize(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Commit" /\ txn[task[i].txn].pc = "CommitFinalize"
  /\ CommitFinalizeEffect(Tsk(i), task[i].txn)
  /\ task' = [task EXCEPT ![i] = IdleTask]
  /\ UNCHANGED <<zk, disk, mdisk, h, sys, client, stmt, mut>>

\* An exception on the merge's own thread. MergePlainMergeTreeTask::executeStep rethrows (:70-74) and the task
\* object is destroyed, which runs the statement transaction's rollback, then the tagger's release and then
\* MergeTreeTransactionHolder's destructor. Three triggers are live in this plan:
\*   - removeOldPart refused, which is checkIsNotCancelled (MergeTreeTransaction.cpp:219) or lockRemovalTID
\*     (:221) throwing, and which MergePublishEnrol routes to the Fail counter;
\*   - the same checkIsNotCancelled reached through addNewPart (:207) from Transaction::commit, and the failed
\*     compare-and-exchange of beforeCommit (:308) reached through commitTransaction, both of which show up as
\*     a task parked at PublishStart or Commit on a transaction a KILL has already rolled back;
\*   - a metadata store of the task's own that ended in an error, which is FrameFail's shape for a session.
\* An injected exception at an arbitrary other point needs QUERY_FAULTS_MAX > 0 and belongs to the fault plan.
MergeFailTrigger(i) ==
  LET t == task[i].txn IN
  \/ task[i].pc = "Fail"
  \/ (task[i].pc \in {"PublishStart", "Commit"} /\ txn[t].state = "RolledBack")
  \/ (task[i].pc \in {"Select", "Write", "Rename", "PublishStart", "PublishEnrol", "PublishStore",
                      "PublishFlip", "Commit"}
      /\ \E p \in Parts : FrameError(p, Tsk(i)) /\ ~FrameOf(p, Tsk(i)).noexcept_owner)
MergeFail(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ MergeFailTrigger(i)
  /\ LET t == task[i].txn IN
     \* what the query holds; the source parts stay pinned until the tagger is finalized in MergeUnwind
     /\ txn' = [txn EXCEPT ![t].mutex = IF @ = Tsk(i) THEN NoActor ELSE @]
     /\ sys' = [sys EXCEPT !.parts_lock = IF @ = Tsk(i) THEN NoActor ELSE @]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.frames = { f \in @ : f.owner /= Tsk(i) }]]
     /\ task' = [task EXCEPT ![i].pc = IF stmt[Tsk(i)].precommitted /= {} THEN "StmtRollbackMark" ELSE "Unwind"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, client, stmt, mut>>
MergeStmtRollbackMark(i, p) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "StmtRollbackMark" /\ p \in stmt[Tsk(i)].precommitted
  /\ \/ /\ StmtRollbackMarkStart(Tsk(i), p)
        /\ UNCHANGED task
     \/ /\ FrameDone(p, Tsk(i), "CreationCSN", RolledBackCSN) /\ StmtRollbackMarkDone(Tsk(i))
        /\ task' = [task EXCEPT ![i].pc = "StmtRollbackDrop"]
        /\ UNCHANGED part
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, stmt, mut>>
MergeStmtRollbackDrop(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "StmtRollbackDrop" /\ sys.parts_lock = NoActor
  /\ StmtRollbackDropEffect(Tsk(i))
  /\ task' = [task EXCEPT ![i].pc = "Unwind"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, sys, client, mut>>
\* the tagger releases the reservation and the source parts, and MergeTreeTransactionHolder's destructor calls
\* TransactionLog::rollbackTransaction, whose compare_exchange in MergeTreeTransaction::rollback
\* (src/Interpreters/MergeTreeTransaction.cpp:382) it loses to a KILL that got there first. Winning it makes the
\* task the rollback driver, and the rollback machine's steps are all session-shaped, so the winning branch is
\* not yet executable; NoTaskDrivenRollback is the invariant that says so rather than letting it wedge quietly.
\* It is unreachable here, for two reasons rather than one. The triggers that read the transaction's state -- a
\* RolledBack transaction at PublishStart or Commit, and the RolledBack disjunct of EnrolRefused -- do mean a
\* KILL has already won. The other two say nothing about the transaction: EnrolRefused's second disjunct, a
\* source that is locked or already carries a removal CSN, and a store of the task's own ending in an error.
\* What excludes those is the reservation: the result is the task's alone, MergeSelect takes each source with
\* lock = EmptyTID, and DropLock waits for every task's reserved set to drain before it takes the parts lock, so
\* no client removal can start on a source while the merge holds it. Nothing else clears a removal lock in this
\* scenario: a commit leaves it set, a rollback clears the removal TID with it, and the non-transactional batch,
\* which does clear it, is not enabled in Merge.
MergeUnwind(i) ==
  /\ task[i].kind = "Merge" /\ Holds(i) /\ task[i].pc = "Unwind"
  /\ LET t == task[i].txn
         won == txn[t].state = "Running" IN
     /\ txn' = [txn EXCEPT ![t].state = IF won THEN "RolledBack" ELSE @,
                            ![t].csn = IF won THEN RolledBackCSN ELSE @,
                            ![t].csn_notified = IF won THEN TRUE ELSE @,
                            ![t].pc = IF won THEN "RollbackCopyLists" ELSE @,
                            ![t].rb_driver = IF won THEN Tsk(i) ELSE @,
                            ![t].holders = @ \ {Tsk(i)}]
     /\ h' = [h EXCEPT !.snapshot[t] = IF won THEN txn[t].snapshot ELSE @]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {Tsk(i)}]]
     /\ task' = [task EXCEPT ![i] = IdleTask]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, sys, client, stmt, mut>>


\* ============================================================ non-transactional queries and the removal batch
\* A query that runs with no transaction writes Tx::NonTransactionalTID into the parts it creates and removes.
\* The removals go through NonTransactionalRemovalLocks (src/Interpreters/MergeTreeTransaction.cpp:106-160),
\* which exists because such a removal cannot be rolled back: the whole batch is locked first and written
\* afterwards, so a conflict on one covered part cannot leave the earlier ones durably removed by a statement
\* that then fails. Four upstream fixes are about exactly that (ab40e11d3c73, f8f46fb1eb14, 86b6861a1a8e, and
\* ba2ee3239b8d for the memory-only stamp), and NtBatchRefusedUnchanged is what they buy.

\* VersionInfo::isRemoved, src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:199
InfoIsRemoved(info) == info.rtid = NonTransactionalTID \/ info.ccsn = RolledBackCSN \/ info.rcsn /= UnknownCSN

\* VersionMetadata::isCreatedByUncommittedTransaction, VersionMetadata.cpp:137: a missing creation CSN is not
\* enough, the transaction log decides (upstream 65e4e2b5bf69). The witness of NtRefusalJustified is exactly the
\* pre-fix form, which trusts mem.creation_csn alone. The two Assert_validateInfo_nocreation names disable the
\* refusal entirely, which is that witness's first change.
\* NoNtStoreError is a third name on the same one-site change, and it is an alias rather than a hook of its own
\* because the same removed refusal falsifies both properties by two different routes: with the preflight
\* refusal gone, a target whose creation is still in flight reaches the store phase, where setAndStoreRemovalTID
\* refuses it instead (the CreationInFlight branch of StoreReadStep) and parks the batch's frame in Error. That
\* is the state NoNtStoreError forbids, and it is what the guard exists to say cannot happen while the refusal
\* is where the code puts it.
CreatedByUncommitted(p) ==
  /\ ~Witness("Assert_validateInfo_nocreation") /\ ~Witness("Assert_validateInfo_nocreation_only1")
  /\ ~Witness("NoNtStoreError")
  /\ part[p].mem.ccsn = UnknownCSN
  /\ part[p].mem.ctid \in Tids
  /\ (Witness("NtRefusalJustified") \/ LookupCsn(part[p].mem.ctid) = UnknownCSN)

BatchTarget(p) == LET b == sys.nt_batch IN
  b.active /\ b.cursor \in 1..Len(b.targets) /\ b.targets[b.cursor] = p

\* NonTransactionalRemovalLocks constructed and its lock loop about to start over B, on actor a. The parts
\* lock is taken here rather than by the caller, because every caller of a batch holds lockParts in the code:
\* removePartsFromWorkingSet (MergeTreeData.cpp:7034) takes an acquired_lock and Transaction::commit
\* (MergeTreeData.cpp:11219) takes one. Writing it here keeps the one sys' assignment an action may have.
\* An empty B is the code's empty parts_to_remove: lock and store both do nothing and the batch is done,
\* so it starts in the Store phase with the cursor already past the end and NtBatchEnd is its only step.
StartBatch(a, B) ==
  /\ sys' = [sys EXCEPT !.parts_lock = a,
                        !.nt_batch = [active |-> TRUE, targets |-> SetToSeq(B),
                                      cursor |-> IF B = {} THEN 0 ELSE 1,
                                      phase |-> IF B = {} THEN "Store" ELSE "Lock",
                                      locked |-> {}, skipped |-> {}, owner |-> a]]
  /\ h' = [h EXCEPT !.batch = [targets |-> B,
                               before |-> [p \in Parts |-> <<part[p].mem, StoredRecord(p), part[p].lock>>]],
                    !.batch_outcome = "None"]

\* The refusal branch shared by NtBatchPreflight and NtBatchLock: the destructor releases every lock the batch
\* still holds (NonTransactionalRemovalLocks::~NonTransactionalRemovalLocks, MergeTreeTransaction.cpp:106,
\* upstream 86b6861a1a8e) and nothing that was stored is undone, because store drains as it goes.
RefuseBatch ==
  /\ part' = [q \in Parts |-> IF q \in sys.nt_batch.locked THEN [part[q] EXCEPT !.lock = EmptyTID] ELSE part[q]]
  /\ sys' = [sys EXCEPT !.nt_batch = NoBatchRec]
  /\ h' = [h EXCEPT !.batch_outcome = "Refused"]

\* NonTransactionalRemovalLocks::lock, MergeTreeTransaction.cpp:121-146, the two branches that are steps: the
\* already-removed skip (:131) and the uncommitted-creator refusal (:139). The third outcome, "proceed to
\* lockRemovalTID", is not a step of its own; NtBatchLock carries its guard.
\* At the end of the lock phase the target list is replaced by the locked list, because store drains
\* locked_parts and never revisits a target the preflight skipped; it drains from the back
\* (MergeTreeTransaction.cpp:153-155), so the cursor counts down.
NtBatchPreflight(p) ==
  /\ Up /\ sys.nt_batch.phase = "Lock" /\ BatchTarget(p)
  /\ LET b == sys.nt_batch
         last == b.cursor = Len(b.targets) IN
     \/ /\ InfoIsRemoved(part[p].mem)
        /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.skipped = @ \cup {p},
                                                     !.phase = IF last THEN "Store" ELSE "Lock",
                                                     !.targets = IF last THEN SetToSeq(b.locked) ELSE @,
                                                     !.cursor = IF last THEN Cardinality(b.locked) ELSE b.cursor + 1]]
        /\ UNCHANGED <<part, h>>
     \/ /\ ~InfoIsRemoved(part[p].mem) /\ CreatedByUncommitted(p)
        /\ RefuseBatch
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, stmt, mut, task>>

\* lockRemovalTID, VersionMetadata.cpp:195: a held lock or a non-zero removal CSN is SERIALIZATION_ERROR. The
\* removal-CSN half is already excluded by InfoIsRemoved above, so what is left here is the held lock.
NtBatchLock(p) ==
  /\ Up /\ sys.nt_batch.phase = "Lock" /\ BatchTarget(p)
  /\ ~InfoIsRemoved(part[p].mem) /\ ~CreatedByUncommitted(p)
  /\ LET b == sys.nt_batch
         last == b.cursor = Len(b.targets)
         nlocked == b.locked \cup {p} IN
     \/ /\ part[p].lock /= EmptyTID
        /\ RefuseBatch
     \/ /\ part[p].lock = EmptyTID
        /\ part' = [part EXCEPT ![p].lock = NonTransactionalTID]
        /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.locked = nlocked,
                                                     !.phase = IF last THEN "Store" ELSE "Lock",
                                                     !.targets = IF last THEN SetToSeq(nlocked) ELSE @,
                                                     !.cursor = IF last THEN Cardinality(nlocked) ELSE b.cursor + 1]]
        /\ UNCHANGED h
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, stmt, mut, task>>

\* NonTransactionalRemovalLocks::store, MergeTreeTransaction.cpp:150: pop the back, setAndStoreRemovalTID
\* through the three-step store, unlock in the SCOPE_EXIT. The tid is written before the unlock, which is what
\* the NonTransactionalTID clause of LockConsistent is stated over.
\* The witness of NtBatchRefusedUnchanged is the shape the class exists to prevent, and it is the whole of the
\* difference between store after lock and a store folded into the lock loop: under it a target is stored
\* and unlocked as soon as it is locked, so a conflict on a later target refuses a batch whose earlier members
\* are already durably removed. The cursor is deliberately left alone there, because the lock phase is still
\* walking the target list.
NtBatchStore(p) ==
  /\ Up /\ sys.nt_batch.active /\ p \in sys.nt_batch.locked
  /\ LET b == sys.nt_batch
         a == b.owner
         early == Witness("NtBatchRefusedUnchanged") /\ b.phase = "Lock" IN
     /\ (b.phase = "Store" => BatchTarget(p))
     /\ (b.phase = "Lock" => early)
     /\ \/ /\ ~HasFrame(p, a) /\ ApplyOp("RemovalTID", NonTransactionalTID, part[p].mem) /= part[p].mem
           /\ part' = StartFrame(p, a, "RemovalTID", NonTransactionalTID, FALSE)
           /\ UNCHANGED <<sys, h>>
        \* h.removers[p] is NOT written here. StorePublish writes it, in the step that publishes the record,
        \* because that is the step at which the removal takes effect for a reader.
        \/ /\ FrameDone(p, a, "RemovalTID", NonTransactionalTID)
           /\ part' = [part EXCEPT ![p].lock = EmptyTID]
           /\ sys' = [sys EXCEPT !.nt_batch = [b EXCEPT !.locked = @ \ {p},
                                                         !.cursor = IF early THEN @ ELSE @ - 1]]
           /\ UNCHANGED h
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, client, stmt, mut, task>>

NtBatchEnd ==
  /\ sys.nt_batch.active /\ sys.nt_batch.phase = "Store" /\ sys.nt_batch.cursor = 0
  /\ sys' = [sys EXCEPT !.nt_batch = NoBatchRec]
  /\ h' = [h EXCEPT !.batch_outcome = "Done"]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, txn, client, stmt, mut, task>>

\* INSERT outside a transaction: setAndStoreCreationTID(Tx::NonTransactionalTID), which sets creation_csn to
\* NonTransactionalCSN in the same update function (VersionMetadata.cpp:261). A base part covers nothing, so
\* getActivePartsToReplace returns nothing and Transaction::commit runs no batch; the two steps are the write
\* and the publication under lockParts.
\* deferrable stays TRUE: a never-transactional part with no txn_version.txt defers the record, which is the
\* shape NoFalseCorruption's witness is about.
NtInsertWrite(k, p) ==
  /\ Up /\ ~HasTxn(k) /\ client[k].pc = "Idle"
  /\ part[p].pstate = "Absent" /\ IsBase(p)
  /\ disk' = DiskWithDir(p)
  /\ part' = [StartFrame(p, Sess(k), "CreateTID", NonTransactionalTID, FALSE) EXCEPT ![p].pstate = "Temporary"]
  /\ h' = [h EXCEPT !.creator[p] = NonTransactionalTID]
  /\ client' = [client EXCEPT ![k].pc = "NtInsertWrite", ![k].part = p]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, stmt, mut, task>>
\* renameTempPartAndReplace and Transaction::commit under one lockParts, collapsed into one step because the
\* batch is empty and there is nothing to interleave with between them. PublishFlipEffect is the same commit
\* body a transactional publication runs, and it is used rather than an unconditional "Active" for its covering
\* branch: a part that already has a covering part is marked Outdated instead (MergeTreeData.cpp:11282, :11316),
\* which is what happens to an INSERT whose publication loses to a DROP PARTITION that covered its range while
\* it was writing. Setting "Active" instead made the empty part and the inserted one both Active over the same
\* range, which is the "Part {} intersects part {}" LOGICAL_ERROR of ActiveSetShape.
NtInsertPublish(k, p) ==
  /\ client[k].pc = "NtInsertWrite" /\ client[k].part = p /\ sys.parts_lock = NoActor
  /\ FrameDone(p, Sess(k), "CreateTID", NonTransactionalTID)
  /\ PublishFlipEffect(Sess(k), p, {})
  /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, mut, task>>

\* DROP PARTITION without a transaction (the spec's NtDropCover), written as the three steps the query has:
\* StorageMergeTree::dropPartition's else branch (src/Storages/StorageMergeTree.cpp:3186) makes an empty part
\* covering the partition with initCoverageWithNewEmptyParts, then renameAndCommitEmptyParts renames it and
\* calls MergeTreeData::Transaction::commit with a null transaction, which locks the covered parts as one batch
\* and flips the states once the batch is stored.
NtDropWrite(k, e) ==
  /\ Up /\ ~HasTxn(k) /\ client[k].pc = "Idle"
  /\ Covers[e] /= {} /\ part[e].pstate = "Absent"
  /\ \E q \in Covers[e] : part[q].pstate = "Active"
  /\ disk' = DiskWithDir(e)
  /\ part' = [StartFrame(e, Sess(k), "CreateTID", NonTransactionalTID, FALSE) EXCEPT
                ![e].pstate = "Temporary", ![e].payload = [ver |-> 0, tomb |-> TRUE]]
  /\ h' = [h EXCEPT !.creator[e] = NonTransactionalTID]
  /\ client' = [client EXCEPT ![k].pc = "NtDropWrite", ![k].part = e]
  /\ UNCHANGED <<zk, mdisk, tlog, txn, sys, stmt, mut, task>>

\* The covered set is getActivePartsToReplace's output and nothing else. The covered OUTDATED parts that a
\* transactional publication also collects are inside the `if (txn)` at MergeTreeData.cpp:11248, so a
\* non-transactional commit never touches them; CoveredNow, which PublishStartEffect uses, is the transactional
\* shape and is deliberately not reused here. removePartsFromWorkingSet, the batch's other caller, additionally
\* filters out a part whose creation_csn is RolledBackCSN (MergeTreeData.cpp:7043-7045); Transaction::commit
\* does not, and such a part is skipped by InfoIsRemoved in the preflight instead.
\* Covering the Outdated ones as well was tried as a fix for finding F5 and does not close it; the finding's
\* entry in FINDINGS.md says why, and why the fix has to be at publication time rather than in the batch.
NtDropPublish(k, e) ==
  /\ client[k].pc = "NtDropWrite" /\ client[k].part = e /\ sys.parts_lock = NoActor
  /\ FrameDone(e, Sess(k), "CreateTID", NonTransactionalTID)
  /\ LET C == { q \in Parts : q \in Expand({e}) /\ q /= e /\ part[q].pstate = "Active" } IN
     /\ part' = [part EXCEPT ![e].pstate = "PreActive"]
     /\ stmt' = [stmt EXCEPT ![Sess(k)].precommitted = {e}, ![Sess(k)].covered = C]
     /\ StartBatch(Sess(k), C)
     /\ client' = [client EXCEPT ![k].pc = "NtDropFlip"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, mut, task>>

\* The batch ended. On Done the NOEXCEPT_SCOPE of Transaction::commit flips the states; on Refused the
\* exception leaves Transaction::commit with the acquired_parts_lock, the query fails with SERIALIZATION_ERROR,
\* and MergeTreeData::Transaction's destructor then runs the statement rollback, which is the two steps below.
NtDropFlip(k, e) ==
  /\ client[k].pc = "NtDropFlip" /\ client[k].part = e /\ ~sys.nt_batch.active
  /\ \/ /\ h.batch_outcome = "Done"
        /\ PublishFlipEffect(Sess(k), e, stmt[Sess(k)].covered)
        /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
     \/ /\ h.batch_outcome = "Refused"
        /\ sys' = [sys EXCEPT !.parts_lock = IF @ = Sess(k) THEN NoActor ELSE @]
        /\ client' = [client EXCEPT ![k].pc = "NtDropUnwindMark", ![k].last_error = "SERIALIZATION_ERROR"]
        /\ UNCHANGED <<part, stmt, h>>
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, mut, task>>

\* MergeTreeData::Transaction::rollback (MergeTreeData.cpp:11122) on the empty part the refused DROP wrote:
\* setAndStoreCreationCSN(RolledBackCSN) at :11126, then removePartsFromWorkingSet under lockParts at :11181.
\* The first step is the one transition VersionMetadata::setAndStoreCreationCSN's chassert exempts by name
\* (VersionMetadata.cpp:125-130, "a temporary empty part with NO_TRANSACTION_PTR ... added to a Transaction and
\* immediately rolled back"), which is exactly this shape: RolledBackCSN written over NonTransactionalCSN.
\* Leaving it out is not a harmless simplification. RolledBackCSN is what makes the abandoned empty part
\* invisible to everybody (isVisible returns false on snapshot < creation_csn) and removable by the cleanup
\* thread; without it the part sits Outdated, still visible, and a later transactional DROP PARTITION enrols it
\* and restores it to Active on rollback, over the parts it covers.
NtDropUnwindMark(k, e) ==
  /\ client[k].pc = "NtDropUnwindMark" /\ e \in stmt[Sess(k)].precommitted
  /\ \/ /\ StmtRollbackMarkStart(Sess(k), e)
        /\ UNCHANGED client
     \/ /\ FrameDone(e, Sess(k), "CreationCSN", RolledBackCSN) /\ StmtRollbackMarkDone(Sess(k))
        /\ client' = [client EXCEPT ![k].pc = "NtDropUnwindDrop"]
        /\ UNCHANGED part
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>
NtDropUnwindDrop(k) ==
  /\ client[k].pc = "NtDropUnwindDrop" /\ sys.parts_lock = NoActor
  /\ StmtRollbackDropEffect(Sess(k))
  /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, txn, sys, mut, task>>

\* ============================================================ stubs for later plans
UpdReconnect == FALSE
UpdSwapUnknownLists == FALSE
UpdFinalizeUnknown(t) == FALSE
MutPrepareWrite(k, m) == FALSE
MutPrepareAttach(k, m) == FALSE
MutRegister(k, m) == FALSE
MutSelect(i, m, p) == FALSE
MutWrite(i, m, p) == FALSE
MutRename(i, m, p) == FALSE
MutWait(k, m) == FALSE
MutFail(i) == FALSE
MutDestroyOwner(m) == FALSE
KillUnregister(m) == FALSE
KillRollbackTxn(m) == FALSE
KillCancelTask(m, i) == FALSE
KillRemoveFile(m) == FALSE
KillMutation(k, m) == FALSE
RollbackKill(k, t, m) == FALSE
CommitStoreMutation(k, m) == FALSE
StoreRetry(p, o) == FALSE
KillRetry(m) == FALSE
Crash == FALSE
ProcessDown(cause) == FALSE
RestartLoadLog == FALSE
RestartTableStart == FALSE
RestartLoadPart(p) == FALSE
RestartLoadMutation(m) == FALSE
RestartTablePublished == FALSE
RestartOutdatedDone == FALSE
RestartDone == FALSE
====
