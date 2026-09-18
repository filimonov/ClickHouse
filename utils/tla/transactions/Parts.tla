---- MODULE Parts ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Types, Keeper, Disk, History
\* part : [Parts -> PartRecord]; tlog : the in-memory TransactionLog; txn : [Tids -> TxnRecord] (declared here
\* because visibility and validation read them; their record types live in Server).
VARIABLES part, tlog, txn

PStates == {"Absent", "Temporary", "PreActive", "Active", "Outdated", "Deleting", "Deleted"}
Pins == { <<"Txn", t>> : t \in Tids } \cup { <<"Rollback", t>> : t \in Tids }
        \cup { <<"Select", k>> : k \in Sessions } \cup { <<"Task", i>> : i \in Tasks }
Actors == { <<"Session", k>> : k \in Sessions } \cup { <<"Task", i>> : i \in Tasks }
FrameOwners == Actors \cup {<<"Updater", 0>>, <<"Cleanup", 0>>, <<"Restart", 0>>}
StoreOps == {"CreateTID", "CreationCSN", "RemovalTID", "RemovalCSN"}
FrameType == [owner : FrameOwners, op : StoreOps, val : AllTids \cup AllCSNs, tentative : VersionInfoType,
              pc : {"Read", "Persist", "Publish", "Error"}, err : {"None", "LOGICAL_ERROR", "STALE_VERSION", "IO"},
              retries : 0..MAX_STORE_RETRIES, interferences : 0..MAX_STORE_RETRIES, interfered : BOOLEAN,
              noexcept_retries : 0..NOEXCEPT_RETRY_BUDGET, noexcept_owner : BOOLEAN]
PayloadType == [ver : 0..3, tomb : BOOLEAN]
PartRecord == [pstate : PStates, mem : VersionInfoType, lock : AllTids,
               deferrable : BOOLEAN, deferred_on : BOOLEAN, deferred : VersionInfoType,
               pins : SUBSET Pins, frames : SUBSET FrameType, payload : PayloadType]
AbsentPartRecord == [pstate |-> "Absent", mem |-> EmptyInfo, lock |-> EmptyTID, deferrable |-> TRUE,
                     deferred_on |-> FALSE, deferred |-> EmptyInfo, pins |-> {}, frames |-> {}, payload |-> [ver |-> 0, tomb |-> FALSE]]
LegacyPartRecord == [AbsentPartRecord EXCEPT !.pstate = "Active", !.deferrable = FALSE,
                     !.mem = [EmptyInfo EXCEPT !.ctid = NonTransactionalTID, !.ccsn = NonTransactionalCSN, !.sv = 0]]

PartsInit == part = [p \in Parts |-> IF p \in LEGACY_PARTS THEN LegacyPartRecord ELSE AbsentPartRecord]
PartsTypeOK == /\ part \in [Parts -> PartRecord]
               /\ \A p \in Parts : \A f, g \in part[p].frames : f.owner = g.owner => f = g

\* The fragments a set of visible roots expands to, each tagged with the payload version it carries. A merge
\* replaces a root by a covering one without losing a fragment, so a property stated over fragments survives it
\* where one stated over roots does not. It lives here rather than in Invariants.tla because Begin and
\* SetSnapshot capture h.content with it.
Frags(V) == { <<q, part[q].payload.ver>> : q \in Expand(V) }

\* ---- transaction log lookups (TransactionLog::getCSN, getOldestSnapshot, tryGetCSN)
LookupCsn(t) == IF t = NonTransactionalTID THEN NonTransactionalCSN
                ELSE IF t \in Tids THEN tlog.tid_to_csn[t] ELSE UnknownCSN
OldestSnapshot == IF tlog.running_list = {} THEN tlog.latest_snapshot
                  ELSE Min({ tlog.snapshots_in_use[t] : t \in tlog.running_list })
TryGetCsn(t) == IF LookupCsn(t) /= UnknownCSN THEN LookupCsn(t)
                ELSE IF t \in tlog.running_list THEN UnknownCSN ELSE RolledBackCSN

\* ---- VersionInfo::isVisible fast path: "TRUE" | "FALSE" | "UNKNOWN"
InfoIsVisible(info, s, u) ==
  IF info.rtid = NonTransactionalTID THEN "FALSE"
  ELSE IF u = NonTransactionalTID THEN (IF info.rtid = EmptyTID THEN "TRUE" ELSE "FALSE")
  ELSE IF s = EverythingVisibleCSN THEN "TRUE"
  ELSE IF info.ccsn /= UnknownCSN /\ s < info.ccsn THEN "FALSE"
  ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= s THEN "FALSE"
  ELSE IF u /= EmptyTID /\ info.rtid = u THEN "FALSE"
  ELSE IF info.ccsn /= UnknownCSN /\ info.ccsn <= s /\ info.rtid = EmptyTID THEN "TRUE"
  ELSE IF info.ccsn /= UnknownCSN /\ info.ccsn <= s /\ info.rcsn /= UnknownCSN /\ s < info.rcsn THEN "TRUE"
  ELSE IF u /= EmptyTID /\ info.ctid = u /\ ~Witness("ReadYourWrites") THEN "TRUE"
  ELSE "UNKNOWN"

\* ---- VersionMetadata::isVisible: fast path, then the slow path with the log lookup
IsVisibleImpl(p, s, u) ==
  LET info == part[p].mem
      fast == InfoIsVisible(info, s, u)
  IN IF fast /= "UNKNOWN" THEN fast = "TRUE"
     ELSE IF info.ctid \in Tids /\ s <= tlog.tid_start[info.ctid] THEN FALSE
     ELSE LET ccsn == IF info.ccsn /= UnknownCSN THEN info.ccsn
                      ELSE IF Witness("NoUncommittedRead") THEN s
                      ELSE IF Witness("Atomicity") THEN UnknownCSN
                      ELSE LookupCsn(info.ctid)
          IN IF ccsn = UnknownCSN THEN FALSE
             ELSE LET rcsn == IF info.rtid = EmptyTID THEN info.rcsn
                              ELSE IF Witness("Atomicity") THEN info.rcsn
                              ELSE LookupCsn(info.rtid)
                  IN ccsn <= s /\ (rcsn = UnknownCSN \/ s < rcsn)

\* ---- VersionMetadata::canBeRemoved
CanBeRemovedImpl(p) ==
  LET info == part[p].mem IN
  IF info.rtid = NonTransactionalTID THEN TRUE
  ELSE IF info.ccsn = RolledBackCSN THEN TRUE
  ELSE IF info.rtid = EmptyTID THEN FALSE
  ELSE LET ccsn == IF info.ccsn /= UnknownCSN THEN info.ccsn ELSE LookupCsn(info.ctid) IN
       IF ccsn = UnknownCSN THEN FALSE
       ELSE IF OldestSnapshot < ccsn THEN FALSE
       ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= OldestSnapshot THEN TRUE
       ELSE LET rcsn == IF info.rcsn /= UnknownCSN THEN info.rcsn ELSE LookupCsn(info.rtid) IN
            rcsn /= UnknownCSN /\ rcsn <= OldestSnapshot

\* ---- VersionMetadata::validateInfo (the exempt shape included); txn[t].csn is the transaction object's csn
ValidateInfoOK(info) ==
  \/ (info.ccsn = RolledBackCSN /\ info.ctid = DummyTID /\ info.rtid = EmptyTID /\ info.rcsn = UnknownCSN)
  \/ /\ info.ctid /= EmptyTID
     /\ (info.ctid \in tlog.running_list /\ info.ccsn \notin {UnknownCSN, RolledBackCSN} /\ txn[info.ctid].csn /= CommittingCSN
           => txn[info.ctid].csn = info.ccsn)
     /\ (info.ccsn = UnknownCSN => info.rcsn = UnknownCSN /\ info.rtid \in {EmptyTID, info.ctid})
     /\ (info.ccsn /= UnknownCSN =>
           /\ (info.rcsn = UnknownCSN \/ info.rcsn = NonTransactionalCSN \/ info.ccsn <= info.rcsn)
           /\ (info.ctid \notin Tids \/ tlog.tid_start[info.ctid] <= info.ccsn))
     /\ (info.rcsn /= UnknownCSN =>
           /\ info.rtid /= EmptyTID
           /\ (info.rtid \notin Tids \/ tlog.tid_start[info.rtid] <= info.rcsn))

\* ---- VersionMetadata::updateCSNIfNeeded: [status |-> "Ok" | "Retry", info |-> ...]
\* "Retry" is the stale-removal-lock case: the loaded removal_tid disagrees with the in-memory lock.
UpdateCsnIfNeeded(p, info) ==
  LET i1 == IF info.ccsn = UnknownCSN /\ info.ctid /= EmptyTID /\ TryGetCsn(info.ctid) /= UnknownCSN
            THEN [info EXCEPT !.ccsn = TryGetCsn(info.ctid)] ELSE info
  IN IF i1.rcsn = UnknownCSN /\ i1.rtid /= EmptyTID
     THEN LET r == TryGetCsn(i1.rtid) IN
          IF r = RolledBackCSN THEN [status |-> "Ok", info |-> [i1 EXCEPT !.rtid = EmptyTID]]
          ELSE IF r /= UnknownCSN THEN [status |-> "Ok", info |-> [i1 EXCEPT !.rcsn = r]]
          ELSE IF part[p].lock /= EmptyTID /\ part[p].lock /= i1.rtid THEN [status |-> "Retry", info |-> i1]
          ELSE [status |-> "Ok", info |-> i1]
     ELSE [status |-> "Ok", info |-> i1]

\* ---- the update functions of setAndStore*: applied to the freshly read base on every attempt
ApplyOp(op, val, base) ==
  CASE op = "CreateTID"   -> IF base.ctid = val THEN base
                             ELSE [base EXCEPT !.ctid = val, !.ccsn = IF val = NonTransactionalTID THEN NonTransactionalCSN ELSE @]
    [] op = "CreationCSN" -> IF base.ccsn = val THEN base ELSE [base EXCEPT !.ccsn = val]
    [] op = "RemovalTID"  -> IF base.rtid = val THEN base
                             ELSE [base EXCEPT !.rtid = val, !.rcsn = IF val = NonTransactionalTID THEN NonTransactionalCSN ELSE @]
    [] op = "RemovalCSN"  -> IF base.rcsn = val THEN base ELSE [base EXCEPT !.rcsn = val]

\* ---- frames
HasFrame(p, o) == \E f \in part[p].frames : f.owner = o
FrameOf(p, o) == CHOOSE f \in part[p].frames : f.owner = o
NewFrame(o, op, val, nx) == [owner |-> o, op |-> op, val |-> val, tentative |-> EmptyInfo, pc |-> "Read", err |-> "None",
                             retries |-> 0, interferences |-> 0, interfered |-> FALSE, noexcept_retries |-> 0, noexcept_owner |-> nx]
WithFrame(p, f) == [part EXCEPT ![p].frames = { g \in @ : g.owner /= f.owner } \cup {f}]
WithoutFrame(p, o) == [part EXCEPT ![p].frames = { g \in @ : g.owner /= o }]
FrameDone(p, o, op, val) == ~HasFrame(p, o) /\ ApplyOp(op, val, part[p].mem) = part[p].mem
FrameError(p, o) == HasFrame(p, o) /\ FrameOf(p, o).pc = "Error"
StoredRecord(p) == IF part[p].deferred_on THEN part[p].deferred
                   ELSE IF DiskHasInfo(p) THEN DiskInfo(p) ELSE EmptyInfo

\* ---- the three-step store (updateInfoWithRefreshDataThenStoreAndSetMetadata); each step is an action of the root
\* StoreRead: getInfo on attempt 1, loadMetadata on a retry (or the witness's getInfo), the op applied, updateCSNIfNeeded,
\* validateInfo. A validation failure parks the frame in Error(LOGICAL_ERROR) for the owner to consume.
StoreReadStep(p, o) ==
  LET f == FrameOf(p, o)
      base == IF f.retries = 0 \/ Witness("NoSpuriousStaleVersion") THEN part[p].mem ELSE StoredRecord(p)
      applied == ApplyOp(f.op, f.val, base)
      upd == UpdateCsnIfNeeded(p, applied)
  IN /\ f.pc = "Read"
     /\ IF applied = base
        THEN part' = WithFrame(p, [f EXCEPT !.tentative = base, !.pc = "Publish"])      \* nothing to store
        ELSE IF upd.status = "Retry"
        THEN IF f.retries < MAX_STORE_RETRIES
             THEN part' = WithFrame(p, [f EXCEPT !.retries = @ + 1])
             ELSE part' = WithFrame(p, [f EXCEPT !.pc = "Error", !.err = "STALE_VERSION"])
        ELSE IF ValidateInfoOK(upd.info) \/ Witness("Assert_isVisible_fast") \/ Witness("Assert_isVisible_fast_only2")
        THEN part' = WithFrame(p, [f EXCEPT !.tentative = upd.info, !.pc = "Persist"])
        ELSE part' = WithFrame(p, [f EXCEPT !.pc = "Error", !.err = "LOGICAL_ERROR"])

\* StorePersist: under persisted_info_mutex, compare storing_version, then defer or write tmp+fsync+rename
StorePersistStep(p, o) ==
  LET f == FrameOf(p, o)
      expected == StoredRecord(p).sv
      newinfo == [f.tentative EXCEPT !.sv = expected + 1]
      others == { g \in part[p].frames : g.owner /= o /\ g.pc = "Persist" }
      bumped == { [g EXCEPT !.interfered = TRUE] : g \in others }
      keep == { g \in part[p].frames : g.owner /= o /\ g.pc /= "Persist" }
  IN /\ f.pc = "Persist"
     /\ IF part[p].deferrable /\ ~Involved(f.tentative)
        THEN \* deferred persistence: no file, the record lives in deferred
             /\ part' = [WithFrame(p, [f EXCEPT !.pc = "Publish", !.tentative = newinfo]) EXCEPT ![p].deferred_on = TRUE, ![p].deferred = newinfo]
             /\ UNCHANGED disk
        ELSE IF expected /= f.tentative.sv
        THEN \* TOO_OLD_VERSION
             /\ IF f.retries < MAX_STORE_RETRIES
                THEN part' = WithFrame(p, [f EXCEPT !.pc = "Read", !.retries = @ + 1,
                                                    !.interferences = IF f.interfered THEN @ + 1 ELSE @, !.interfered = FALSE])
                ELSE part' = WithFrame(p, [f EXCEPT !.pc = "Error", !.err = "STALE_VERSION",
                                                    !.interferences = IF f.interfered THEN @ + 1 ELSE @])
             /\ UNCHANGED disk
        ELSE \* the write; every other persisting frame on p learns of the interference
             /\ disk' = DiskWithInfo(p, newinfo)
             /\ part' = [part EXCEPT ![p].frames = keep \cup bumped \cup {[f EXCEPT !.pc = "Publish", !.tentative = newinfo]},
                                     ![p].deferrable = FALSE, ![p].deferred_on = FALSE, ![p].deferred = EmptyInfo]

\* StorePublish: setInfo under version_info_mutex, ignored if the stored version is lower than the current one
StorePublishStep(p, o) ==
  LET f == FrameOf(p, o) IN
  /\ f.pc = "Publish"
  /\ part' = [WithoutFrame(p, o) EXCEPT ![p].mem = IF f.tentative.sv < part[p].mem.sv THEN part[p].mem ELSE f.tentative]
====
