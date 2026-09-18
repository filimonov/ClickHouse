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
              "Commit", "Rollback", "RollbackWait", "KillWait", "Refuse", "StmtRollbackMark", "StmtRollbackDrop"}
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

StmtRecord == [precommitted : SUBSET Parts, covered : SUBSET Parts, attached : SUBSET Parts, work : Seq(Parts)]
NoStmt == [precommitted |-> {}, covered |-> {}, attached |-> {}, work |-> <<>>]

MutStates == {"Absent", "Written", "Attached", "Registered", "Unregistered", "Killed"}
MutRecord == [mstate : MutStates, tasks : SUBSET Tasks, tid : AllTids, csn : AllCSNs,
              fail_reason : {"None", "Deadlock"}, file_owner : SUBSET {"Preparing", "Map"}, kill_retries : 0..NOEXCEPT_RETRY_BUDGET]
AbsentMutRecord == [mstate |-> "Absent", tasks |-> {}, tid |-> EmptyTID, csn |-> UnknownCSN,
                    fail_reason |-> "None", file_owner |-> {}, kill_retries |-> 0]

TaskRecord == [kind : {"Idle", "Merge", "Mutation"}, pc : {"Idle", "Select", "Write", "Rename", "Publish", "Commit", "Fail"},
               txn : Tids \cup {EmptyTID}, mutation : Mutations \cup {"None"}, source : Parts \cup {"None"},
               reserved : SUBSET Parts]
IdleTask == [kind |-> "Idle", pc |-> "Idle", txn |-> EmptyTID, mutation |-> "None", source |-> "None", reserved |-> {}]

BatchType == [active : BOOLEAN, targets : Seq(Parts), cursor : 0..(Cardinality(Parts) + 1), phase : {"Lock", "Store"}, locked : SUBSET Parts, skipped : SUBSET Parts]
NoBatchRec == [active |-> FALSE, targets |-> <<>>, cursor |-> 0, phase |-> "Lock", locked |-> {}, skipped |-> {}]

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
              cleanup_pc : {"Idle", "Validate", "Delete"}]
SysInit == [server |-> "TableUp", completely_started |-> TRUE, async_loading_jobs |-> 0,
            loaded_parts |-> Parts, loaded_mutations |-> Mutations, restarts |-> 0, keeper_faults |-> 0,
            disk_faults |-> 0, query_faults |-> 0, merges_blocker |-> 0, parts_lock |-> NoActor, nt_batch |-> NoBatchRec,
            updater_pc |-> "Idle", cleanup_pc |-> "Idle"]

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
Up == sys.server = "TableUp"
Cur(k) == client[k].current
HasTxn(k) == client[k].current /= EmptyTID
Effects(t) == txn[t].creating /= <<>> \/ txn[t].removing /= <<>> \/ txn[t].mutations /= {}
Visible(q, t) == IsVisibleImpl(q, txn[t].snapshot, t)
SnapshotFor(t) == IF Witness("StableRead") THEN tlog.latest_snapshot ELSE txn[t].snapshot
\* getActivePartsToReplace plus getCoveredOutdatedParts filtered by visibility
CoveredNow(p, t) == { q \in Parts : q \in Expand({p}) /\ q /= p /\ part[q].pstate \in {"Active", "Outdated"}
                                    /\ (part[q].pstate = "Active" \/ Visible(q, t)) }
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

\* ============================================================ client: begin, set snapshot
Begin(k) ==
  /\ Up /\ ~HasTxn(k) /\ client[k].pc = "Idle"
  /\ tlog.local_tid_counter < TID_MAX
  /\ LET t == tlog.local_tid_counter + 1
         s == tlog.latest_snapshot IN
     /\ tlog' = [tlog EXCEPT !.local_tid_counter = t, !.tid_start[t] = s, !.running_list = @ \cup {t},
                             !.snapshots_in_use[t] = s]
     /\ txn' = [txn EXCEPT ![t] = [AbsentTxn EXCEPT !.state = "Running", !.snapshot = s, !.protected_snapshot = s,
                                     !.holders = {Sess(k)}]]
     /\ client' = [client EXCEPT ![k].current = t, ![k].first_read = NoRead, ![k].last_read = NoRead]
     /\ h' = [h EXCEPT !.content[t] = { <<q, part[q].payload.ver>> : q \in
                { r \in Parts : part[r].pstate \in {"Active", "Outdated"} /\ OracleVisible(r, s, t) } }]
  /\ UNCHANGED <<zk, disk, mdisk, part, sys, stmt, mut, task>>

SetSnapshot(k, c) == FALSE

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
  /\ LET t == Cur(k)
         C == CoveredNow(p, t) IN
     /\ sys' = [sys EXCEPT !.parts_lock = Sess(k)]
     /\ stmt' = [stmt EXCEPT ![Sess(k)].covered = C, ![Sess(k)].attached = @ \cup {p}, ![Sess(k)].work = SetToSeq(C)]
     /\ txn' = [txn EXCEPT ![t].creating = Append(@, p)]
     /\ h' = [h EXCEPT !.creating[t] = @ \cup {p}]
     /\ part' = [part EXCEPT ![p].pins = @ \cup {<<"Txn", t>>}]
     /\ client' = [client EXCEPT ![k].pc = IF C = {} THEN "PublishFlip" ELSE "PublishEnrol"]
  /\ UNCHANGED <<zk, disk, mdisk, tlog, mut, task>>

\* removeOldPart, first half (shared by Drop and Publish): mutex, checkIsNotCancelled, lockRemovalTID, enrol
\* nextpc is the client pc after success; on refusal the client goes to Refuse with last_error set
EnrolBody(k, q, nextpc) ==
  LET t == Cur(k)
      enrolled == StartFrame(q, Sess(k), "RemovalTID", t, FALSE) IN
  /\ txn[t].mutex = NoActor
  /\ \/ /\ txn[t].state = "RolledBack"
        /\ client' = [client EXCEPT ![k].last_error = "INVALID_TRANSACTION", ![k].pc = "Refuse"]
        /\ UNCHANGED <<txn, part, h>>
     \/ /\ txn[t].state = "Running"
        /\ (part[q].lock /= EmptyTID \/ part[q].mem.rcsn /= UnknownCSN) /\ ~Witness("SingleRemover")
        /\ client' = [client EXCEPT ![k].last_error = "SERIALIZATION_ERROR", ![k].pc = "Refuse"]
        /\ UNCHANGED <<txn, part, h>>
     \/ /\ txn[t].state = "Running"
        /\ (part[q].lock = EmptyTID /\ part[q].mem.rcsn = UnknownCSN) \/ Witness("SingleRemover")
        /\ part' = [enrolled EXCEPT ![q].lock = t, ![q].pins = @ \cup {<<"Txn", t>>}]
        /\ txn' = [txn EXCEPT ![t].mutex = Sess(k), ![t].removing = Append(@, q)]
        /\ h' = [h EXCEPT !.removing[t] = @ \cup {q}]
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
\* the NOEXCEPT_SCOPE state loop of Transaction::commit
PublishFlip(k) ==
  /\ client[k].pc = "PublishFlip" /\ sys.parts_lock = Sess(k)
  /\ LET p == client[k].part
         C == stmt[Sess(k)].covered IN
     /\ part' = [q \in Parts |-> IF q = p THEN [part[q] EXCEPT !.pstate = "Active"]
                                 ELSE IF q \in C /\ ~Witness("ActiveSetShape") THEN [part[q] EXCEPT !.pstate = "Outdated"]
                                 ELSE part[q]]
     /\ stmt' = [stmt EXCEPT ![Sess(k)] = NoStmt]
     /\ sys' = [sys EXCEPT !.parts_lock = NoActor]
     /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].part = "None"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, mut, task>>

\* MergeTreeData::Transaction::rollback: creation_csn := RolledBackCSN per precommitted part, then outdate under lockParts
StmtRollbackMark(k, p) ==
  /\ client[k].pc = "StmtRollbackMark" /\ p \in stmt[Sess(k)].precommitted \ stmt[Sess(k)].attached
  /\ \/ /\ ~HasFrame(p, Sess(k)) /\ part[p].mem.ccsn /= RolledBackCSN
        /\ part' = StartFrame(p, Sess(k), "CreationCSN", RolledBackCSN, TRUE)
        /\ UNCHANGED client
     \/ /\ FrameDone(p, Sess(k), "CreationCSN", RolledBackCSN)
        /\ client' = [client EXCEPT ![k].pc = "StmtRollbackDrop"]
        /\ UNCHANGED part
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, stmt, mut, task>>
StmtRollbackDrop(k) ==
  /\ client[k].pc = "StmtRollbackDrop" /\ sys.parts_lock = NoActor
  /\ LET R == stmt[Sess(k)].precommitted \ stmt[Sess(k)].attached IN
     /\ part' = [p \in Parts |-> IF p \in R THEN [part[p] EXCEPT !.pstate = "Outdated"] ELSE part[p]]
     /\ stmt' = [stmt EXCEPT ![Sess(k)] = NoStmt]
     /\ client' = [client EXCEPT ![k].pc = "Rollback"]
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, mut, task>>

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
  /\ LET V == client[k].capture
         R == [parts |-> V, frags |-> UNION { { <<c, q, part[q].payload.ver>> : q \in Expand({c}) } : c \in V }] IN
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
  /\ client[k].pc = "DropWait" /\ \A i \in Tasks : task[i].reserved = {} /\ sys.parts_lock = NoActor
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
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitCreateCSN" /\ KeeperCanAppend /\ zk.session = "Alive"
     /\ LET c == KeeperNextCsn IN
        /\ zk' = KeeperAppended(t)
        /\ h' = [h EXCEPT !.committed = @ \cup {t}, !.csn[t] = c,
                          !.removers = [p \in Parts |-> IF p \in h.removing[t] THEN @[p] \cup {t} ELSE @[p]]]
        /\ txn' = [txn EXCEPT ![t].pc = FirstCommitPc(t), ![t].work = FirstCommitWork(t)]
  /\ UNCHANGED <<disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
\* the isReadOnly branch: csn := snapshot, no Keeper request
CommitReadOnly(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitFlip" /\ ~Effects(t) /\ txn[t].state = "Committing"
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = txn[t].snapshot, ![t].csn_notified = TRUE, ![t].pc = "CommitFinalize"]
     /\ h' = [h EXCEPT !.csn[t] = txn[t].snapshot]
  /\ UNCHANGED <<zk, disk, mdisk, part, tlog, sys, client, stmt, mut, task>>
\* afterCommit: one setAndStoreCreationCSN / setAndStoreRemovalCSN per part; start the frame, then advance when it is done
CommitStore(k, p, op, phase) ==
  LET t == Cur(k)
      val == IF Witness("Assert_validateInfo_creator") /\ op = "CreationCSN" THEN h.csn[t] + 1
             ELSE IF Witness("Assert_validateInfo_order") /\ op = "CreationCSN" THEN CSN_MAX
             ELSE h.csn[t]
      skip == (Witness("Assert_isVisible_fast") \/ Witness("Assert_isVisible_fast_only1"))
              /\ op = "CreationCSN" /\ p \in h.removing[t] IN
  /\ client[k].pc = "Commit" /\ txn[t].pc = phase /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
  /\ \/ /\ ~skip /\ ~HasFrame(p, Sess(k)) /\ ApplyOp(op, val, part[p].mem) /= part[p].mem
        /\ part' = StartFrame(p, Sess(k), op, val, TRUE)
        /\ UNCHANGED txn
     \/ /\ (skip \/ FrameDone(p, Sess(k), op, val))
        /\ txn' = [txn EXCEPT ![t].work = IF Tail(@) = <<>> THEN NextCommitWork(t, phase) ELSE Tail(@),
                               ![t].pc = IF Tail(txn[t].work) = <<>> THEN NextCommitPc(t, phase) ELSE phase]
        /\ UNCHANGED part
  /\ UNCHANGED <<zk, disk, mdisk, h, tlog, sys, client, stmt, mut, task>>
CommitStoreCreation(k, p) == CommitStore(k, p, "CreationCSN", "CommitStoreCreation")
CommitStoreRemoval(k, p) == CommitStore(k, p, "RemovalCSN", "CommitStoreRemoval")
CommitFlip(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ Effects(t) /\ txn[t].state = "Committing"
     /\ (txn[t].pc = "CommitFlip" \/ (Witness("FlipAfterStores") /\ txn[t].pc \in {"CommitStoreCreation", "CommitStoreRemoval"}))
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = h.csn[t], ![t].csn_notified = TRUE, ![t].pc = "CommitFinalize", ![t].work = <<>>]
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, sys, client, stmt, mut, task>>
CommitFinalize(k) ==
  /\ LET t == Cur(k) IN
     /\ client[k].pc = "Commit" /\ txn[t].pc = "CommitFinalize"
     /\ tlog' = [tlog EXCEPT !.running_list = @ \ {t}, !.snapshots_in_use[t] = UnknownCSN]
     /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].creating = <<>>, ![t].removing = <<>>, ![t].mutations = {}]
     /\ part' = PinsWithout(<<"Txn", t>>)
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
\* Refuse: release what the query holds, statement rollback if needed, then the transaction rollback
Refuse(k) ==
  /\ client[k].pc = "Refuse" /\ HasTxn(k)
  /\ LET t == Cur(k)
         unattached == stmt[Sess(k)].precommitted \ stmt[Sess(k)].attached IN
     /\ txn' = [txn EXCEPT ![t].mutex = IF @ = Sess(k) THEN NoActor ELSE @]
     /\ sys' = [sys EXCEPT !.parts_lock = IF @ = Sess(k) THEN NoActor ELSE @,
                           !.merges_blocker = IF client[k].holds_blocker THEN @ - 1 ELSE @]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.frames = { f \in @ : f.owner /= Sess(k) }, !.pins = @ \ {<<"Select", k>>}]]
     /\ client' = [client EXCEPT ![k].pc = IF unattached /= {} THEN "StmtRollbackMark" ELSE "Rollback",
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
           /\ UNCHANGED <<txn, h>>
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
  /\ UNCHANGED <<zk, disk, mdisk, h, part, tlog, txn, sys, stmt, mut, task>>
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
  /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].creating = <<>>, ![t].removing = <<>>, ![t].mutations = {},
                         ![t].holders = {}, ![t].rb_driver = NoActor]
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

\* ============================================================ store steps as root actions
StoreRead(p, o) == /\ Up /\ HasFrame(p, o) /\ StoreReadStep(p, o)
                   /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, stmt, mut, task>>
StorePersist(p, o) == /\ Up /\ HasFrame(p, o) /\ StorePersistStep(p, o)
                      /\ UNCHANGED <<zk, mdisk, h, tlog, txn, sys, client, stmt, mut, task>>
StorePublish(p, o) == /\ Up /\ HasFrame(p, o) /\ StorePublishStep(p, o)
                      /\ UNCHANGED <<zk, disk, mdisk, h, tlog, txn, sys, client, stmt, mut, task>>
Fsync(p) == /\ Layered /\ disk' = DiskWithMetaSynced(p)
            /\ UNCHANGED <<zk, mdisk, h, part, tlog, txn, sys, client, stmt, mut, task>>

\* ============================================================ stubs for later plans
UpdReconnect == FALSE
UpdRemoveOldEntriesSetTail == FALSE
UpdRemoveOldEntriesDelete(c) == FALSE
UpdSwapUnknownLists == FALSE
UpdFinalizeUnknown(t) == FALSE
CleanupGrab(p) == FALSE
CleanupValidate(p) == FALSE
CleanupDeleteOk(p) == FALSE
CleanupDeleteFail(p) == FALSE
MergeBegin(i) == FALSE
MergeSelect(i) == FALSE
MergeWrite(i) == FALSE
MergeRename(i) == FALSE
MergeFail(i) == FALSE
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
NtInsert(p) == FALSE
NtBatchStart(B) == FALSE
NtBatchPreflight(p) == FALSE
NtBatchLock(p) == FALSE
NtBatchStore(p) == FALSE
NtBatchEnd == FALSE
NtDropCover == FALSE
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
