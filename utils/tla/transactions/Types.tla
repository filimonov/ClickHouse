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
  DISK_MODE, FSYNC_PART_DIRECTORY, LEGACY_PARTS, WAIT_MODE, WITNESS_NAME,
  SNAPSHOT_TARGETS, SET_SNAPSHOT_PROTECTS

ASSUME Covers \in [Parts -> SUBSET Parts]
ASSUME NOEXCEPT_STORE_FAULT_POLICY \in {"Terminate", "Retry"}
ASSUME DISK_MODE \in {"Durable", "Layered"}
ASSUME WAIT_MODE \in {"WAIT", "WAIT_UNKNOWN", "ASYNC"}
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

\* SET TRANSACTION SNAPSHOT refuses a reserved CSN other than these two
\* (InterpreterTransactionControlQuery::executeSetSnapshot, src/Interpreters/InterpreterTransactionControlQuery.cpp:144).
\* The set is a scenario bound, not a refinement: the code accepts any CSN above MaxReservedCSN.
ASSUME SNAPSHOT_TARGETS \subseteq (RealCSNs \cup {NonTransactionalCSN, EverythingVisibleCSN})
ASSUME SET_SNAPSHOT_PROTECTS \in BOOLEAN

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

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : y >= x
Range(s) == { s[i] : i \in DOMAIN s }
SetToSeq(S) == CHOOSE s \in [1..Cardinality(S) -> S] : \A x \in S : \E n \in DOMAIN s : s[n] = x
====
