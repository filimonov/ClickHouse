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
              pc : {"Read", "Persist", "Publish", "Error"},
              err : {"None", "LOGICAL_ERROR", "STALE_VERSION", "SERIALIZATION_ERROR", "IO"},
              retries : 0..MAX_STORE_RETRIES, interferences : 0..MAX_STORE_RETRIES, interfered : BOOLEAN,
              noexcept_retries : 0..NOEXCEPT_RETRY_BUDGET, noexcept_owner : BOOLEAN]
PayloadType == [ver : 0..3, tomb : BOOLEAN]
PartRecord == [pstate : PStates, mem : VersionInfoType, lock : AllTids,
               deferrable : BOOLEAN, deferred_on : BOOLEAN, deferred : VersionInfoType,
               pins : SUBSET Pins, frames : SUBSET FrameType, payload : PayloadType]
AbsentPartRecord == [pstate |-> "Absent", mem |-> EmptyInfo, lock |-> EmptyTID, deferrable |-> TRUE,
                     deferred_on |-> FALSE, deferred |-> EmptyInfo, pins |-> {}, frames |-> {}, payload |-> [ver |-> 0, tomb |-> FALSE]]
\* The record VersionInfo::readFromMultiLineBuffer produces for the pre-storing_version format (upstream
\* aea1c111e0a8): a non-transactional creation. Its storing_version is 0 and not -1, because the FILE EXISTS;
\* -1 is the value StoredRecord returns when there is no record at all. Model defect M1 was exactly this
\* confusion: a legacy part carried mem.sv = 0 while StoredRecord fell through to EmptyInfo with sv = -1, so
\* every store on it took TOO_OLD_VERSION and ended in STALE_VERSION, and no legacy part could ever be written.
LegacyInfo == [EmptyInfo EXCEPT !.ctid = NonTransactionalTID, !.ccsn = NonTransactionalCSN, !.sv = 0]
LegacyPartRecord == [AbsentPartRecord EXCEPT !.pstate = "Active", !.deferrable = FALSE, !.mem = LegacyInfo]

PartsInit == part = [p \in Parts |-> IF p \in LEGACY_PARTS THEN LegacyPartRecord ELSE AbsentPartRecord]
PartsTypeOK == /\ part \in [Parts -> PartRecord]
               /\ \A p \in Parts : \A f, g \in part[p].frames : f.owner = g.owner => f = g

\* The fragments a set of visible roots expands to, each tagged with the payload version it carries. A merge
\* replaces a root by a covering one without losing a fragment, so a property stated over fragments survives it
\* where one stated over roots does not. It lives here rather than in Invariants.tla because Begin and
\* SetSnapshot capture h.content with it.
\* FragsOf is one root's contribution, and it is empty for an empty part, whose rows_count is zero: that is what
\* a non-transactional DROP PARTITION publishes over the partition it drops, and an empty covering part covers
\* rows that no longer exist. No part is a tombstone before the non-transactional task, so this is a no-op here.
FragsOf(c) == IF part[c].payload.tomb THEN {} ELSE { <<q, part[q].payload.ver>> : q \in Expand({c}) }
Frags(V) == UNION { FragsOf(c) : c \in V }

\* ---- transaction log lookups (TransactionLog::getCSN, getOldestSnapshot, tryGetCSN)
LookupCsn(t) == IF t = NonTransactionalTID THEN NonTransactionalCSN
                ELSE IF t \in Tids THEN tlog.tid_to_csn[t] ELSE UnknownCSN
\* The cleanup horizon: getOldestSnapshot as canBeRemoved asks it (TransactionLog.cpp:677). It may be one of
\* the two special snapshots, which is what protects a part a transaction can still see at one of them.
OldestSnapshot == IF tlog.running_list = {} THEN tlog.latest_snapshot
                  ELSE Min({ tlog.snapshots_in_use[t] : t \in tlog.running_list })
\* The retention horizon: the value removeOldEntries may move tail_ptr to. It is NOT the cleanup horizon, and
\* separating the two is the second half of finding F2's fix. A transaction reading at a special snapshot still
\* needs the log entries of the era it began in, and a tail at NonTransactionalCSN or EverythingVisibleCSN
\* would regress past every entry, which TransactionLog.cpp:313 raises a LOGICAL_ERROR for. Under the baseline
\* the two horizons are equal in every reachable state, because nothing moves a registry entry after Begin.
RetentionHorizon == IF tlog.running_list = {} THEN tlog.latest_snapshot
                    ELSE Min({ tlog.retention_in_use[t] : t \in tlog.running_list })
\* The NoResurrection witness is the spec's row: a tid absent from the log resolves to unknown rather than
\* rolled back, so a part whose creating transaction never committed loads Active instead of dead.
TryGetCsn(t) == IF LookupCsn(t) /= UnknownCSN THEN LookupCsn(t)
                ELSE IF t \in tlog.running_list THEN UnknownCSN
                ELSE IF Witness("NoResurrection") THEN UnknownCSN
                ELSE RolledBackCSN

\* ---- VersionInfo::isVisible fast path: "TRUE" | "FALSE" | "UNKNOWN"
InfoIsVisible(info, s, u) ==
  IF info.rtid = NonTransactionalTID THEN "FALSE"
  ELSE IF u = NonTransactionalTID THEN (IF info.rtid = EmptyTID THEN "TRUE" ELSE "FALSE")
  ELSE IF s = EverythingVisibleCSN THEN "TRUE"
  ELSE IF info.ccsn /= UnknownCSN /\ s < info.ccsn THEN "FALSE"
  \* The NoDoubleRead witness is the spec's row, "SelectCheck ignores removal_csn and removal_tid": with the two
  \* removal tests of the fast path and the removal lookup of the slow path gone, a reader sees a merge result
  \* and the sources it covers at once. The first line above is deliberately left alone: removing it would also
  \* change what a non-transactional removal means, and the non-transactional task needs that path honest.
  ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= s /\ ~Witness("NoDoubleRead") THEN "FALSE"
  ELSE IF u /= EmptyTID /\ info.rtid = u /\ ~Witness("NoDoubleRead") THEN "FALSE"
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
             ELSE LET rcsn == IF Witness("NoDoubleRead") THEN UnknownCSN
                              ELSE IF info.rtid = EmptyTID THEN info.rcsn
                              ELSE IF Witness("Atomicity") THEN info.rcsn
                              ELSE LookupCsn(info.rtid)
                  IN ccsn <= s /\ (rcsn = UnknownCSN \/ s < rcsn)

\* ---- VersionMetadata::canBeRemoved, with the snapshot it compares against as a parameter, so that the
\* cleanup thread can be given a different one (the NoPrematureDelete witness passes tlog.latest_snapshot).
CanBeRemovedWith(p, oldest) ==
  LET info == part[p].mem IN
  IF info.rtid = NonTransactionalTID THEN TRUE
  ELSE IF info.ccsn = RolledBackCSN THEN TRUE
  ELSE IF info.rtid = EmptyTID THEN FALSE
  ELSE LET ccsn == IF info.ccsn /= UnknownCSN THEN info.ccsn ELSE LookupCsn(info.ctid) IN
       IF ccsn = UnknownCSN THEN FALSE
       ELSE IF oldest < ccsn THEN FALSE
       ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= oldest THEN TRUE
       ELSE LET rcsn == IF info.rcsn /= UnknownCSN THEN info.rcsn ELSE LookupCsn(info.rtid) IN
            rcsn /= UnknownCSN /\ rcsn <= oldest
CanBeRemovedImpl(p) == CanBeRemovedWith(p, OldestSnapshot)

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
                   ELSE IF DiskHasInfo(p) THEN DiskInfo(p)
                   ELSE IF disk[p].cached.kind = "Legacy" THEN LegacyInfo
                   ELSE EmptyInfo
\* The same read against the layer a crash keeps. It is what the invariant preamble judges an unloaded part by
\* and what the truncation pass consults under TAIL_WAITS_FOR_DURABLE_CSN; the deferred record is deliberately
\* not consulted, because a record that was never written to a file cannot survive anything.
DurableRecord(p) == IF disk[p].durable.kind = "Info" THEN disk[p].durable.info
                    ELSE IF disk[p].durable.kind = "Legacy" THEN LegacyInfo
                    ELSE EmptyInfo
\* No txn_version.txt of any format and no deferred record: readMetadata would throw CANNOT_OPEN_FILE.
NoStoredRecord(p) == ~part[p].deferred_on /\ disk[p].cached.kind = "None"

\* the part directories the loader can see, and the roots of the coverage tree it builds from their names
\* (MergeTreeData::loadDataParts, src/Storages/MergeTree/MergeTreeData.cpp:2857 region). It is the FINAL name
\* rather than the directory's existence, because the walk skips every directory whose name starts with tmp
\* (:2964), so a part directory that still carries its temporary name is not a candidate.
OnDisk(p) == DiskNamed(p)
DiskRoot(p) == OnDisk(p) /\ ~\E c \in Parts : c /= p /\ OnDisk(c) /\ p \in Expand({c})

\* VersionMetadataOnDisk::loadMetadata (src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:49) and
\* its four cases: the record that is there (:63-67); the legacy format, which the model carries as its own disk
\* kind; the tmp-only directory, which becomes DummyTID with RolledBackCSN (:77-84) after the tmp file is
\* removed (:58-60); and the directory with neither, which becomes a non-transactional creation (:90-93).
\* Taken against a layer rather than against the part, so that the invariant preamble can ask the same question
\* of the layer a crash keeps and cannot answer it differently from the loader.
LoadedRecordFrom(r, tmp) ==
  IF r.kind = "Info" THEN r.info
  ELSE IF r.kind = "Legacy" THEN LegacyInfo
  ELSE IF tmp THEN [EmptyInfo EXCEPT !.ctid = DummyTID, !.ccsn = RolledBackCSN, !.sv = -1]
  ELSE [EmptyInfo EXCEPT !.ctid = NonTransactionalTID, !.ccsn = NonTransactionalCSN, !.sv = -1]
LoadedRecord(p) == LoadedRecordFrom(disk[p].cached, disk[p].tmp_cached)
DurableLoadedRecord(p) == LoadedRecordFrom(disk[p].durable, disk[p].tmp_durable)
\* The shape loadMetadata case 2 produces, short-circuited by both validateInfo and hasValidMetadata.
DummyRolledBackShape(info) ==
  info.ccsn = RolledBackCSN /\ info.ctid = DummyTID /\ info.rtid = EmptyTID /\ info.rcsn = UnknownCSN

\* VersionMetadata::hasValidMetadata, src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:632-721, reached
\* through IMergeTreeDataPart::assertHasValidVersionMetadata (IMergeTreeDataPart.cpp:2863), which returns true
\* for a part that was never involved in a transaction and for a Temporary one. A mismatch throws CORRUPTED_DATA;
\* a CANNOT_OPEN_FILE whose directory is gone is accepted (:706).
\* The NoFalseCorruption witness removes two exemptions the spec's row names: the deferred record counts as a
\* stored record, and a NonTransactionalCSN held only in memory is transient.
ValidateMetadataOK(p) ==
  LET m == part[p].mem
      r == StoredRecord(p)
      have == IF Witness("NoFalseCorruption") THEN DiskHasInfo(p) ELSE ~NoStoredRecord(p) IN
  \/ ~Involved(m)
  \/ part[p].pstate = "Temporary"
  \/ DummyRolledBackShape(m)
  \/ (~have /\ ~DiskDirExists(p))
  \/ /\ have
     /\ m.ctid = r.ctid
     /\ (m.rtid = r.rtid \/ m.rtid = NonTransactionalTID)
     /\ (m.ccsn = r.ccsn \/ m.ccsn = RolledBackCSN \/ r.ccsn = UnknownCSN)
     /\ (m.rcsn = r.rcsn \/ (m.rcsn = NonTransactionalCSN /\ ~Witness("NoFalseCorruption")) \/ r.rcsn = UnknownCSN)
     /\ ~(r.rcsn /= UnknownCSN /\ r.rtid = EmptyTID)

\* spec #invariants-cleanup, NoFalseCorruption: the disagreements history says cannot be transient. It keeps both
\* exemptions unconditionally, which is what makes the witness above red rather than merely different.
RealDisagreement(p) ==
  LET m == part[p].mem
      r == StoredRecord(p)
      have == ~NoStoredRecord(p) IN
  \/ (have /\ m.ctid /= r.ctid)
  \/ (have /\ m.rtid /= r.rtid /\ m.rtid /= NonTransactionalTID)
  \/ (have /\ m.ccsn /= r.ccsn /\ m.ccsn /= RolledBackCSN /\ r.ccsn /= UnknownCSN)
  \/ (have /\ m.rcsn /= r.rcsn /\ m.rcsn /= NonTransactionalCSN /\ r.rcsn /= UnknownCSN)
  \/ (have /\ r.rcsn /= UnknownCSN /\ r.rtid = EmptyTID)
  \/ (~have /\ DiskDirExists(p) /\ ~DummyRolledBackShape(m))

\* VersionMetadata::setAndStoreRemovalTID, src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:161: a
\* non-transactional removal of an object whose transactional creation has not committed is refused with
\* SERIALIZATION_ERROR, because removal_csn becomes NonTransactionalCSN in the same update function while
\* creation_csn is still zero, which is the shape validateInfo rejects and a restart cannot repair (upstream
\* 6e5114d9739c). The predicate is read twice. Once outside the metadata update lock, on the in-memory record,
\* through isCreatedByUncommittedTransaction (:172), which consults the transaction log rather than trusting
\* creation_csn alone (upstream 65e4e2b5bf69); and once inside the update function, on the record that attempt
\* read (:179). The model re-evaluates the outer half on every attempt where the C++ computes it once before the
\* first; both read the same record on attempt 1, and a creation can only become committed, so the two agree on
\* every behaviour of this plan. They come apart once the truncation pass can take a commit back out of
\* tid_to_csn, which is model defect M12.
CreationInFlight(p, f, base) ==
  /\ f.op = "RemovalTID" /\ f.val = NonTransactionalTID
  /\ part[p].mem.ccsn = UnknownCSN /\ part[p].mem.ctid \in Tids
  /\ LookupCsn(part[p].mem.ctid) = UnknownCSN
  /\ base.ccsn = UnknownCSN /\ base.ctid \in Tids
  /\ ~Witness("Assert_validateInfo_nocreation_only2")
  /\ ~Witness("Assert_validateInfo_nocreation")

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
        \* the refusal lives in the update function, which runs before updateCSNIfNeeded and validateInfo
        \* (VersionMetadata.cpp:334-346) and after its own no-op early return (:177)
        ELSE IF CreationInFlight(p, f, base)
        THEN part' = WithFrame(p, [f EXCEPT !.pc = "Error", !.err = "SERIALIZATION_ERROR"])
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

\* StorePublish: setInfo under version_info_mutex, ignored if the stored version is lower than the current one.
\* A non-transactional removal has no commit point: the record becoming visible IS the removal taking effect,
\* because VersionInfo::isVisible tests removal_tid = NonTransactionalTID before it looks at any snapshot
\* (src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:142). The oracle's ghost is therefore written here,
\* in the step that publishes the record, and not by the batch a step later. A transactional removal's ghost is
\* written at CommitCreateEffect for the same reason: that is the step at which it becomes visible to others.
\* Writing it a step late made every property that reads the oracle blind for one step, which is what produced
\* the Atomicity counterexample this task first reported as finding F7.
StorePublishStep(p, o) ==
  LET f == FrameOf(p, o)
      newmem == IF f.tentative.sv < part[p].mem.sv THEN part[p].mem ELSE f.tentative IN
  /\ f.pc = "Publish"
  /\ part' = [WithoutFrame(p, o) EXCEPT ![p].mem = newmem]
  /\ h' = IF newmem.rtid = NonTransactionalTID /\ part[p].mem.rtid /= NonTransactionalTID
          THEN [h EXCEPT !.removers[p] = @ \cup {NonTransactionalTID}]
          ELSE h
====
