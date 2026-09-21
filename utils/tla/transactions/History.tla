---- MODULE History ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Types
\* h : one record of ghost fields, never read by an action, never cleared by Crash
VARIABLE h

Outcomes == {"None", "Acked", "Error", "UnknownStatus"}
\* h.batch is the snapshot NtBatchStart records: which parts the batch targets, and, per part, the in-memory
\* record, the stored record and the removal lock as they stood when the batch was constructed. The two batch
\* properties compare against it, so it carries every field they read and no other.
NoBatch == [targets |-> {}, before |-> [p \in Parts |-> <<EmptyInfo, EmptyInfo, EmptyTID>>]]

HistoryInit == h = [
  outcome     |-> [t \in Tids |-> "None"],
  committed   |-> {},
  csn         |-> [t \in Tids \cup {NonTransactionalTID} |-> IF t = NonTransactionalTID THEN NonTransactionalCSN ELSE UnknownCSN],
  snapshot    |-> [t \in Tids |-> UnknownCSN],
  loaded      |-> [t \in Tids |-> FALSE],
  creating    |-> [t \in Tids |-> {}],
  removing    |-> [t \in Tids |-> {}],
  mutations   |-> [t \in Tids |-> {}],
  rolled_back |-> [t \in Tids |-> FALSE],
  unknown     |-> [t \in Tids |-> "None"],
  removers    |-> [p \in Parts |-> {}],
  creator     |-> [p \in Parts |-> IF p \in LEGACY_PARTS THEN NonTransactionalTID ELSE EmptyTID],
  \* A ghost for the payload's VALUE, which no action ever changes after creation. Whether the payload survives
  \* a crash is not a ghost: it is disk[p].payload_durable, and RestartLoadPart refuses a part whose data files
  \* did not survive rather than restoring this field for it. The four actions that create a part write it
  \* beside h.creator.
  payload     |-> [p \in Parts |-> [ver |-> 0, tomb |-> FALSE]],
  selected    |-> {},
  content     |-> [t \in Tids |-> {}],
  truncated   |-> {},
  abandoned   |-> {},
  batch       |-> NoBatch,
  batch_outcome |-> "None",
  prepared_files |-> {},
  down_cause  |-> "None"]

HistoryTypeOK ==
  /\ h.outcome \in [Tids -> Outcomes]
  /\ h.committed \subseteq Tids
  /\ h.csn \in [Tids \cup {NonTransactionalTID} -> AllCSNs]
  /\ h.snapshot \in [Tids -> AllCSNs]
  /\ h.loaded \in [Tids -> BOOLEAN]
  /\ h.creating \in [Tids -> SUBSET Parts]
  /\ h.removing \in [Tids -> SUBSET Parts]
  /\ h.mutations \in [Tids -> SUBSET Mutations]
  /\ h.rolled_back \in [Tids -> BOOLEAN]
  /\ h.unknown \in [Tids -> {"None", "Committed", "RolledBack"}]
  /\ h.removers \in [Parts -> SUBSET (Tids \cup {NonTransactionalTID})]
  /\ h.creator \in [Parts -> AllTids]
  /\ h.payload \in [Parts -> [ver : 0..3, tomb : BOOLEAN]]
  /\ h.selected \subseteq (Mutations \X Parts \X BOOLEAN)
  /\ h.content \in [Tids -> SUBSET (Parts \X Nat)]
  /\ h.truncated \subseteq Tids
  /\ h.abandoned \subseteq Parts
  /\ h.batch.targets \subseteq Parts
  /\ h.batch.before \in [Parts -> (VersionInfoType \X VersionInfoType \X AllTids)]
  /\ h.batch_outcome \in {"None", "Done", "Refused"}
  /\ h.prepared_files \subseteq Mutations
  /\ h.down_cause \in {"None", "StoreFault", "RetryExhausted", "Other"}

\* The declarative visibility oracle: history only, never part metadata (spec, invariants preamble).
OracleVisible(p, s, u) ==
  LET c == h.creator[p] IN
  /\ c /= EmptyTID
  \* h.abandoned is the parts a statement rollback stamped with RolledBackCSN. For a transactional creator the
  \* clause below already excludes them, because a rolled-back transaction is not in h.committed and the
  \* properties that read the oracle quantify over running transactions (finding F3). A non-transactional
  \* creator has no such handle: without this exclusion the c = NonTransactionalTID clause says the empty part
  \* of a refused DROP PARTITION is visible to everyone, when VersionInfo::isVisible returns false for it on
  \* snapshot_version < creation_csn (src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:167) and
  \* RolledBackCSN is above every snapshot. That is F3's counterexample in its non-transactional form.
  /\ p \notin h.abandoned
  /\ \/ c = u
     \/ c = NonTransactionalTID
     \/ (c \in h.committed /\ h.csn[c] <= s)
  /\ ~ (u \in Tids /\ p \in h.removing[u])
  /\ ~ (\E r \in h.removers[p] : r = NonTransactionalTID \/ h.csn[r] <= s)
====
