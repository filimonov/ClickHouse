---- MODULE Types ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
  Sessions, Parts, Tasks, Mutations,
  TID_MAX, CSN_MAX, Covers,
  RESTARTS_MAX, KEEPER_FAULTS_MAX, DISK_FAULTS_MAX, QUERY_FAULTS_MAX,
  MAX_STORE_RETRIES, NOEXCEPT_RETRY_BUDGET, NOEXCEPT_STORE_FAULT_POLICY,
  DISK_MODE, FSYNC_PART_DIRECTORY, FSYNC_AFTER_INSERT, FSYNC_OUTER_RENAME, LEGACY_PARTS, WAIT_MODE, WITNESS_NAME,
  SNAPSHOT_TARGETS, SET_SNAPSHOT_PROTECTS, OBSOLETE_IS_ROLLED_BACK,
  REMOVAL_REFUSES_UNCOMMITTED_CREATION, ENTRY_KEPT_UNTIL_CSN_DURABLE,
  CREATION_TID_STORE_SYNCS_DIR

ASSUME Covers \in [Parts -> SUBSET Parts]
ASSUME NOEXCEPT_STORE_FAULT_POLICY \in {"Terminate", "Retry"}
ASSUME DISK_MODE \in {"Durable", "Layered"}
ASSUME WAIT_MODE \in {"WAIT", "WAIT_UNKNOWN", "ASYNC"}
ASSUME FSYNC_AFTER_INSERT \in BOOLEAN
\* Upstream gates both part-directory syncs with the one setting fsync_part_directory: the guard inside
\* storeInfoToDataPartStorage for the txn_version.txt rename, and the guard inside renameTo for the part
\* directory's own name. The model separates them into FSYNC_PART_DIRECTORY and FSYNC_OUTER_RENAME because the
\* two renames lose different things and each loss has to be exhibitable on its own; a scenario that means to
\* model the setting binds both to the same value.
ASSUME FSYNC_OUTER_RENAME \in BOOLEAN
ASSUME LEGACY_PARTS \subseteq Parts

Witness(name) == WITNESS_NAME = name
NoActor == <<"None", "none">>

\* Transaction identifiers: one host, so an integer suffices; start_csn lives in tlog.tid_start.
\* Tids are never reused (the code resets local_tid_counter at each log load and relies on
\* (start_csn, local_tid, host) for uniqueness; the model keeps the counter monotonic, which is
\* the same set of distinct transactions under a different naming).
EmptyTID            == 0
NonTransactionalTID == -1
DummyTID            == -2
Tids                == 1..TID_MAX
AllTids             == Tids \cup {EmptyTID, NonTransactionalTID, DummyTID}
IsTransactional(t)  == t \in Tids

\* Commit sequence numbers
UnknownCSN           == 0
NonTransactionalCSN  == 1
CommittingCSN        == 2
EverythingVisibleCSN == 3
MaxReservedCSN       == 32
FirstCSN             == MaxReservedCSN + 1
RolledBackCSN        == CSN_MAX + 1
RealCSNs             == FirstCSN..CSN_MAX
AllCSNs              == {UnknownCSN, NonTransactionalCSN, CommittingCSN, EverythingVisibleCSN, RolledBackCSN} \cup RealCSNs
LogCSNs              == MaxReservedCSN..CSN_MAX      \* values tail_ptr / latest_snapshot can take

\* The two reserved CSNs SET TRANSACTION SNAPSHOT accepts. They are not points on the CSN line: reading at
\* EverythingVisibleCSN makes every part visible (VersionInfo::isVisible returns true at once,
\* src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:157-158), and reading at NonTransactionalCSN leaves a
\* non-transactionally created part visible until a transaction's removal of it commits. Both therefore have to
\* hold the cleanup thread back, and neither may be used as the log's retention horizon, because a tail at 1 or
\* 3 is below every log entry and removeOldEntries raises a LOGICAL_ERROR on a tail that regresses
\* (src/Interpreters/TransactionLog.cpp:313).
\* What the cleanup horizon protects at EverythingVisibleCSN is the committed parts the snapshot reads, and only
\* those. A part whose creation was rolled back is visible at that snapshot and is removable all the same:
\* canBeRemoved returns true on creation_csn = RolledBackCSN before it looks at the horizon at all
\* (src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:280-282), which CanBeRemovedWith reproduces.
IsSpecialSnapshot(c) == c \in {NonTransactionalCSN, EverythingVisibleCSN}

\* SET TRANSACTION SNAPSHOT refuses a reserved CSN other than these two
\* (InterpreterTransactionControlQuery::executeSetSnapshot, src/Interpreters/InterpreterTransactionControlQuery.cpp:144).
\* The set is a scenario bound, not a refinement: the code accepts any CSN above MaxReservedCSN.
ASSUME SNAPSHOT_TARGETS \subseteq (RealCSNs \cup {NonTransactionalCSN, EverythingVisibleCSN})
ASSUME SET_SNAPSHOT_PROTECTS \in BOOLEAN
\* FALSE is the baseline: Transaction::commit's obsolete branch (src/Storages/MergeTree/MergeTreeData.cpp:11305)
\* moves a precommitted part that already has a covering part to Outdated and stamps nothing on it, so it keeps
\* creation_csn = 0 for ever. TRUE is the fix proposed by finding F6: stamp it RolledBackCSN there, the way
\* MergeTreeData::Transaction::rollback stamps a part that does not make it in.
ASSUME OBSOLETE_IS_ROLLED_BACK \in BOOLEAN
\* FALSE is the baseline: VersionMetadata::lockRemovalTID refuses a removal already locked or already
\* committed and nothing else (src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:195-248), so a
\* transactional remover may lock a part whose creation is not committed. TRUE is the fix proposed by finding
\* F9: for a transactional remover, and only for one, throw SERIALIZATION_ERROR when the creating TID is not
\* the removing TID and the creation is not committed. The qualifier matters: lockRemovalTID's two other
\* callers pass Tx::NonTransactionalTID, and that path tolerates a rolled-back creation on purpose, so an
\* unqualified refusal would throw during part loading. The predicate is
\* VersionMetadata::isCreationCommitted (:150-159), which the file already has and today reads only on the
\* non-transactional path, through isCreatedByUncommittedTransaction and the refusal inside
\* setAndStoreRemovalTID (:179-184). It covers both shapes of the finding, a creation still in flight and a
\* creation already rolled back.
ASSUME REMOVAL_REFUSES_UNCOMMITTED_CREATION \in BOOLEAN
\* FALSE is the baseline: TransactionLog::removeOldEntries prunes every entry below the new tail
\* (src/Interpreters/TransactionLog.cpp:333-347), and the comment at :286-289 calls that almost safe, qualifying
\* it with the CSNs a startup writes into data parts. Those writes are not fsynced, so an entry can be pruned
\* while the only durable copy of its CSN is still the record without one. TRUE is the fix proposed by finding
\* F11 and by the code's own TODO at :305-307, "keep outdated entries for a while": the removal loop skips an
\* entry whose CSN is carried only by a record the server has written and not yet made durable, which it knows
\* from part[p].mem and the part's meta_unsynced bit and from nothing else. It is stated on the removal rather
\* than on the tail because holding the tail back would need the start CSN of a transaction the model does not
\* recover across a restart, which is model defect M47. Setting fsync_part_directory also closes the window,
\* but that is finding F10's mitigation: it is off by default and it syncs every metadata store, where this
\* keeps the entry until the one store that matters has reached the disk.
ASSUME ENTRY_KEPT_UNTIL_CSN_DURABLE \in BOOLEAN
\* FALSE is the baseline: storeInfoToDataPartStorage takes its directory guard only under fsync_part_directory
\* (VersionMetadataOnDisk.cpp:360-362), so the creation record's two names are dentries in the part's own
\* directory that nothing syncs, and a crash can leave the directory holding neither. TRUE is the fix proposed
\* by finding F12: take that guard unconditionally for the store that writes the creation TID, which is the one
\* whose absence the loader reads as a part older than transactions. It is narrower than turning
\* fsync_part_directory on, which syncs every metadata store; this syncs the one dentry whose loss is
\* misread.

\* Covering relation: Covers[p] = direct children of p. Expand gives the base parts under a set.
RECURSIVE ExpandSeen(_, _)
ExpandSeen(S, Seen) ==
  IF S = {} THEN {}
  ELSE LET p == CHOOSE q \in S : TRUE
           rest == ExpandSeen(S \ {p}, Seen \cup {p})
       IN IF p \in Seen THEN rest
          ELSE IF Covers[p] = {} THEN {p} \cup rest
          ELSE ExpandSeen(Covers[p], Seen \cup {p}) \cup rest
Expand(S) == ExpandSeen(S, {})
Overlap(p, q) == p /= q /\ (Expand({p}) \cap Expand({q}) /= {})
IsBase(p) == Covers[p] = {}

\* VersionInfo: creation_tid, creation_csn, removal_tid, removal_csn, storing_version
VersionInfoType == [ctid : AllTids, ccsn : AllCSNs, rtid : AllTids, rcsn : AllCSNs, sv : -1..(2 * MAX_STORE_RETRIES + 8)]
EmptyInfo == [ctid |-> EmptyTID, ccsn |-> UnknownCSN, rtid |-> EmptyTID, rcsn |-> UnknownCSN, sv |-> -1]

\* VersionInfo::wasInvolvedInTransaction
Involved(info) == \/ info.ctid /= NonTransactionalTID
                  \/ (info.rcsn = UnknownCSN /\ info.rtid \notin {NonTransactionalTID, EmptyTID})
                  \/ info.rcsn \notin {NonTransactionalCSN, UnknownCSN}
\* VersionInfo::isRemoved, src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:199
InfoIsRemoved(info) == info.rtid = NonTransactionalTID \/ info.ccsn = RolledBackCSN \/ info.rcsn /= UnknownCSN

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : y >= x
Range(s) == { s[i] : i \in DOMAIN s }
SetToSeq(S) == CHOOSE s \in [1..Cardinality(S) -> S] : \A x \in S : \E n \in DOMAIN s : s[n] = x
====
