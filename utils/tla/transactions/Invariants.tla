---- MODULE Invariants ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Server

Proj(F) == { <<f[2], f[3]>> : f \in F }      \* drop the root of a fragment tuple

\* ---- isolation (spec #invariants-isolation): evaluated when a read completes (SelectFinish), over the
\* read that step produces (client'[k].last_read) and the history as of that step. A read taken earlier is
\* not re-judged after the reader's own later effects, which is what a state invariant would wrongly do.
Own(t) == h.creating[t] \cup h.removing[t]
\* one action property per named property, so that a violation names the property
StableReadStep == \A k \in Sessions : SelectFinish(k) => LET t == Cur(k) IN
  txn[t].snapshot /= EverythingVisibleCSN /\ txn[t].state = "Running" /\ client[k].first_read /= NoRead =>
    (Proj(client[k].first_read.frags) \ Frags(Own(t))) = (Proj(client'[k].last_read.frags) \ Frags(Own(t)))
StableRead == [][StableReadStep]_vars
ReadYourWritesStep == \A k \in Sessions : SelectFinish(k) => LET t == Cur(k) IN txn[t].snapshot /= EverythingVisibleCSN /\ txn[t].state = "Running" =>
  /\ \A p \in h.creating[t] \ h.removing[t] : part[p].pstate \in {"Active", "Outdated"} => p \in client'[k].last_read.parts
  /\ \A p \in h.removing[t] : p \notin client'[k].last_read.parts
ReadYourWrites == [][ReadYourWritesStep]_vars
NoUncommittedReadStep == \A k \in Sessions : SelectFinish(k) => LET t == Cur(k) IN txn[t].snapshot /= EverythingVisibleCSN /\ txn[t].state = "Running" =>
  \A p \in client'[k].last_read.parts : h.creator[p] \in h.committed \cup {t, NonTransactionalTID}
NoUncommittedRead == [][NoUncommittedReadStep]_vars
NoFutureReadStep == \A k \in Sessions : SelectFinish(k) => LET t == Cur(k) IN txn[t].snapshot /= EverythingVisibleCSN /\ txn[t].state = "Running" =>
  \A p \in client'[k].last_read.parts : OracleVisible(p, txn[t].snapshot, t)
NoFutureRead == [][NoFutureReadStep]_vars
NoLostReadStep == \A k \in Sessions : SelectFinish(k) => LET t == Cur(k)
                                                              s == txn[t].snapshot
                                                              R == client'[k].last_read IN s /= EverythingVisibleCSN /\ txn[t].state = "Running" =>
  \A p \in Parts : (part[p].pstate \in {"Active", "Outdated"} /\ OracleVisible(p, s, t))
     => (p \in R.parts \/ \E c \in R.parts : p \in Expand({c}) /\ OracleVisible(c, s, t))
NoLostRead == [][NoLostReadStep]_vars
NoDoubleReadStep == \A k \in Sessions : SelectFinish(k) =>
  \A f1, f2 \in client'[k].last_read.frags : f1[2] = f2[2] => f1[1] = f2[1]
NoDoubleRead == [][NoDoubleReadStep]_vars
\* spec #invariants-isolation, the `Atomicity` row. The row builds C from the parts the committed writer u
\* created and did not itself remove, minus those a later committed transaction visible to the reader removed,
\* "which the code hides by giving own removal priority". That priority is the reader's too: VersionInfo::isVisible
\* (src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:171) returns false for removal_tid == current_tid before
\* ever reaching the creation clause at :178, so a part the reader is itself dropping is invisible to it however
\* the writer that created it committed. The reader's own removals are therefore out of C as well. The row states
\* only the writer's half; see FINDINGS.md section 3.
AtomicityStep == \A k \in Sessions : SelectFinish(k) => LET t == Cur(k)
                                                             s == txn[t].snapshot
                                                             V == client'[k].last_read.parts IN s /= EverythingVisibleCSN /\ txn[t].state = "Running" =>
  \A u \in h.committed : h.loaded[u] /\ s >= h.csn[u] /\ u /= t =>
    LET C == { p \in h.creating[u] \ h.removing[u] :
                 /\ ~\E r \in h.removers[p] \ {u} : h.csn[r] <= s
                 /\ p \notin h.removing[t] }
        Rm == h.removing[u] \ h.creating[u] IN
    /\ (C \subseteq V /\ V \cap Rm = {}) \/ (C \cap V = {} /\ Rm \subseteq V)
    /\ V \cap (h.creating[u] \cap h.removing[u]) = {}
Atomicity == [][AtomicityStep]_vars
\* Stated without an exemption for EverythingVisibleCSN, unlike AtomicityStep above, and deliberately so: at
\* that snapshot the code really does return the parts a rolled-back transaction created, and the design
\* document's isolation and rollback rows promise otherwise. Finding F8 is that disagreement and spec defect
\* S18 is the row it belongs to, so the property is left stating what the document says and the modules that
\* run the introspection target leave the row off their roster rather than the property losing it. What is
\* asserted here therefore still holds, and is checked, at every ordinary snapshot.
RollbackNoLeak == \A k \in Sessions : \A t \in Tids : Cur(k) /= EmptyTID /\ Cur(k) /= t /\ t \notin h.committed =>
  client[k].last_read.parts \cap h.creating[t] = {}

\* ---- the part-state preamble (spec, "Invariants and properties")
\* A part-state clause is evaluated on part[p].pstate while p \in sys.loaded_parts and on the durable layer of
\* disk[p] otherwise. A durable committed or non-transactional removal counts as Outdated; a durable rolled-back
\* creation, and a missing directory, count as Absent. Before the first crash loaded_parts is the whole
\* universe, so every scenario without one is unaffected.
DurableRecord(p) ==
  IF disk[p].durable.kind = "Info" THEN disk[p].durable.info
  ELSE IF disk[p].durable.kind = "Legacy" THEN LegacyInfo
  ELSE EmptyInfo
DurablePState(p) ==
  IF ~disk[p].dir_durable THEN "Absent"
  ELSE LET r == DurableRecord(p) IN
       IF r.ccsn = RolledBackCSN THEN "Absent"
       ELSE IF r.rcsn /= UnknownCSN \/ r.rtid = NonTransactionalTID THEN "Outdated"
       ELSE "Active"
\* The in-memory record is authoritative wherever the running server holds one, and not only for a part the
\* loader reached: a part created after the restart is not in loaded_parts and its durable layer says nothing
\* about the state it is in.
InMemory(p) == p \in sys.loaded_parts \/ part[p].pstate /= "Absent"
PState(p) == IF InMemory(p) THEN part[p].pstate ELSE DurablePState(p)

\* ---- durability (Durable disk mode in Base)
\* The antecedent is the spec row's h_effects, which includes the mutations a transaction registered; the
\* mutation disjunct is vacuous while Mutations = {} and live from plan 4.
\* The Outdated case is weaker than the spec's row, which admits Outdated only for a committed removal
\* (h_removers[p] /= {}). A removal merely in flight, that is part[p].lock /= EmptyTID, is admitted here too,
\* because DropOutdate outdates a part before the transaction that drops it commits, and the literal row is
\* therefore red on the baseline. Recorded as spec defect S5 in FINDINGS.md, section 3.
\* These two read PState and every other property reads part[p].pstate, because every other property is about
\* what an actor of the running server does, and an actor cannot act while the server is down.
\* The in-flight disjunct the S5 relaxation adds reads part[p].lock, which is meaningless for a part the
\* server does not hold in memory, so it is guarded on InMemory.
AckedWriteIsDurable == \A t \in Tids :
  h.outcome[t] = "Acked" /\ ((h.creating[t] \cup h.removing[t]) /= {} \/ h.mutations[t] /= {}) =>
  /\ t \in h.committed
  /\ \A p \in h.creating[t] : \/ PState(p) = "Active"
                             \/ (PState(p) = "Outdated"                                                          \* S5: removal in flight or committed
                                 /\ ((InMemory(p) /\ part[p].lock /= EmptyTID) \/ h.removers[p] /= {}))
                             \* The durable layer has no Deleted: a part the cleanup thread removed before a
                             \* crash reads back as Absent, so the arm that admits a removed part admits that
                             \* too. What separates it from a part the crash lost is the committed remover.
                             \/ (PState(p) \in {"Deleting", "Deleted", "Absent"} /\ h.removers[p] /= {})
  /\ \A p \in h.removing[t] : PState(p) /= "Active"
ErrorIsAbsent == \A t \in Tids : h.outcome[t] = "Error" =>
  t \notin h.committed /\ (h.rolled_back[t] => \A p \in h.creating[t] : PState(p) /= "Active")

\* ---- conflicts (spec #invariants-conflicts)
SingleRemover == \A p \in Parts : Cardinality(h.removers[p]) <= 1
LockConsistent == \A p \in Parts :
  LET l == part[p].lock
      m == part[p].mem IN
  /\ (l \in Tids => m.rtid \in {EmptyTID, l})
  /\ (l = NonTransactionalTID => m.rtid \in {EmptyTID, NonTransactionalTID})
  /\ (l = EmptyTID => m.rtid = EmptyTID \/ m.rcsn /= UnknownCSN)
\* Three clauses. The first is the "Part {} intersects part {}" LOGICAL_ERROR of the active parts set and is the
\* one the ActiveSetShape witness attacks. The second is CurrentlyMergingPartsTagger's "Tagging already tagged
\* part" (src/Storages/StorageMergeTree.cpp:918-921); it is vacuous at one task and is checked in the scenario
\* that runs a merge and a mutation side by side. The third is grabOldParts' rule at MergeTreeData.cpp:4158,
\* "First remove all covered parts, then remove covering empty part": an empty covering part must not be Active
\* over a part it covers that is still Active. It is implied by the first while Covers is two levels deep, and
\* it is written anyway because it is a rule of its own that a deeper covering relation would separate.
ActiveSetShape == /\ \A p, q \in Parts : part[p].pstate = "Active" /\ part[q].pstate = "Active" => ~Overlap(p, q)
                  /\ \A i, j \in Tasks : i /= j => task[i].reserved \cap task[j].reserved = {}
                  /\ \A p \in Parts : part[p].pstate = "Active" /\ part[p].payload.tomb =>
                       \A q \in Expand({p}) \ {p} : part[q].pstate /= "Active"
\* Not a property of the server: a bound guard. MergeUnwind can in principle win the compare-and-exchange in
\* MergeTreeTransaction::rollback and become the rollback driver. Every step of the machine takes its driver as
\* an argument, and the wrappers that exist are the session's and the updating thread's; no disjunct of any
\* Next instantiates a step with Tsk(i), so the transaction would sit in RollbackCopyLists with nothing able to
\* advance it. No
\* scenario of this plan reaches it: the two triggers that read the transaction's state can only find it already
\* rolled back, and the two that do not -- a source locked or already removed, and a store of the task's own in
\* error -- are excluded by the reservation, which keeps every other actor off the task's parts. Model defect
\* M11 in FINDINGS.md carries the argument. This invariant says so instead of letting the search wedge without a
\* word. The plan that enables a fault on a background task owes the task-driven rollback steps and takes this
\* out, by adding the task wrappers rather than by changing the bodies.
NoTaskDrivenRollback == \A t \in Tids : \A i \in Tasks : txn[t].rb_driver /= Tsk(i)

\* ---- code assertions (spec #invariants-code)
\* validateInfo is a chassert, so both the record a part carries and the record a store computed before it
\* persists anything are subject to it: a frame the model parks in Error(LOGICAL_ERROR) because its record
\* failed validation is that assertion firing, not a refusal the code is free to make.
Assert_validateInfo ==
  /\ \A p \in Parts : part[p].pstate /= "Absent" /\ part[p].mem.ctid /= EmptyTID => ValidateInfoOK(part[p].mem)
  /\ \A p \in Parts : \A f \in part[p].frames : f.err /= "LOGICAL_ERROR"
Assert_isVisible_fast == \A p \in Parts : LET m == part[p].mem IN
  /\ (m.rcsn /= UnknownCSN => m.ccsn /= UnknownCSN)
  /\ m.ccsn \in {UnknownCSN, NonTransactionalCSN, RolledBackCSN} \cup RealCSNs
  /\ m.rcsn \in {UnknownCSN, NonTransactionalCSN} \cup RealCSNs
\* TransactionLog::getOldestSnapshot, src/Interpreters/TransactionLog.cpp:677-686. Three conjuncts, one per
\* witness name; the witness table lists them as Assert_getOldestSnapshot_size, _entry and the bare name.
\*   1. the running list and the snapshot bag have the same members, which is
\*      chassert(running_list.size() == snapshots_in_use.size()).
\*   2. each entry is the value beginTransaction inserted. This one has no C++ counterpart: protected_snapshot
\*      is a model ghost, and the clause is what makes "the entry did not follow the snapshot" observable.
\*   3. the bag is sorted. snapshots_in_use is a list in insertion order and Begin draws tids from a monotone
\*      counter while latest_snapshot never decreases, so "sorted" is "non-decreasing in the tid order".
\* Conjunct 3 is deliberately STRONGER than the assertion it comes from: the chassert compares the first two
\* elements only, snapshots_in_use.front() <= *++snapshots_in_use.begin(). The model checks the property that
\* assertion approximates, over every pair, and that is what licenses OldestSnapshot == Min(...) (Parts.tla) as
\* a model of front(): front() is the minimum exactly when the list is sorted.
\* Under SET_SNAPSHOT_PROTECTS the proposed fix re-inserts the entry at its sorted position, so the tid order is
\* no longer the list order and conjunct 3 does not apply; the C++ assertion still does. That leaves the fix
\* variant with nothing checking that front() stays the minimum, which is recorded in FINDINGS.md section 2.
\* The fourth conjunct is about the second registry the fix adds, the retention horizon. It has no C++
\* assertion of its own; it states the construction the fix depends on, that every running transaction has a
\* retention value, that the value is never one of the two special snapshots, that it never exceeds
\* latest_snapshot, which is what makes getOldestSnapshot's empty-list fallback (TransactionLog.cpp:679-680)
\* safe and is the clause finding F2's third shape falsified, and that it never sits above the cleanup horizon.
\* Under the baseline the two registries are equal everywhere, so the conjunct is free there.
Assert_getOldestSnapshot ==
  /\ tlog.running_list = { t \in Tids : tlog.snapshots_in_use[t] /= UnknownCSN }
  /\ \A t \in tlog.running_list : tlog.snapshots_in_use[t] = txn[t].protected_snapshot
  /\ \A t \in tlog.running_list :
       /\ tlog.retention_in_use[t] /= UnknownCSN
       /\ ~IsSpecialSnapshot(tlog.retention_in_use[t])
       /\ tlog.retention_in_use[t] <= tlog.latest_snapshot
       /\ (~IsSpecialSnapshot(tlog.snapshots_in_use[t]) =>
             tlog.retention_in_use[t] <= tlog.snapshots_in_use[t])
  /\ ~SET_SNAPSHOT_PROTECTS =>
       \A t1, t2 \in tlog.running_list : t1 < t2 => tlog.snapshots_in_use[t1] <= tlog.snapshots_in_use[t2]
NoAvoidableTermination == h.down_cause \in {"None", "RetryExhausted"}
NoSpuriousStaleVersion == \A k \in Sessions : client[k].last_error = "STALE_VERSION" => client[k].stale_interferences = MAX_STORE_RETRIES
\* A KILL query blocks at KillWait until the rollback it drives finishes, and the runner disables TLC's deadlock
\* check (an exhausted TID_MAX leaves a legitimate idle terminal state), so a killer that could never be released
\* would pass unnoticed. Only RollbackFinalize clears rb_driver, so a killer parked on a transaction whose
\* rollback machine has already gone idle is stranded; when no transaction names it, KillReturn is enabled.
KillerNotStranded == \A k \in Sessions : client[k].pc = "KillWait" =>
  \A t \in Tids : txn[t].rb_driver = Sess(k) => txn[t].pc /= "Idle"

\* ---- action properties
\* removeOldEntries, src/Interpreters/TransactionLog.cpp:312-314: "Got unexpected tail_ptr {}, oldest snapshot is
\* {}, it's a bug". A LOGICAL_ERROR on a modelled path is an invariant, not a precondition (spec, "Actions").
\* Stated over the retention horizon, which is the value removeOldEntries publishes. Under the baseline it is
\* the cleanup horizon, so this is the same statement the property carried before; under the fix the two come
\* apart at a special snapshot, and the second conjunct is what forbids such a value ever reaching the tail.
TailPtrNotRegressingStep == UpdRemoveOldEntriesSetTail =>
  (RetentionHorizon >= zk.tail /\ ~IsSpecialSnapshot(RetentionHorizon))
Assert_TailPtrNotRegressing == [][TailPtrNotRegressingStep]_vars

\* TransactionLog::assertTIDIsNotOutdated, src/Interpreters/TransactionLog.cpp:656-675: the LOGICAL_ERROR
\* "Trying to get CSN for too old TID". It is reached from tryFinalizeUnknownStateTransactions
\* (src/Interpreters/TransactionLog.cpp:387), which is UpdFinalizeUnknown, and from getCSNAndAssert (:645),
\* which has no caller in the tree, so the property is vacuous in every scenario that leaves UpdFinalizeUnknown
\* disabled. See FINDINGS.md, spec defect S6.
NoOutdatedLookupStep == \A t \in Tids : UpdFinalizeUnknown(t) =>
  (tlog.tid_to_csn[t] /= UnknownCSN \/ tlog.tail_ptr <= tlog.tid_start[t])
NoOutdatedLookup == [][NoOutdatedLookupStep]_vars

\* spec #invariants-durability, UnknownResolvesByLog. Stated on the step that decides: the decision the pass
\* takes for t agrees with whether t is in h.committed, which is the fact the two-list scheme buys. It is the
\* property the comment at TransactionLog.cpp:360-372 argues for in prose.
\* A red here has two possible causes and they are different findings: the swap resolved a transaction whose
\* entry was not yet loaded (the two-list scheme), or the truncation pass removed the entry of a committed one
\* before the pass read it, which NoOutdatedLookup also reports. Read the trace for which before classifying.
UnknownResolvesByLogStep == \A t \in Tids :
  UpdFinalizeUnknown(t) => ((h'.unknown[t] = "Committed") <=> (t \in h.committed))
UnknownResolvesByLog == [][UnknownResolvesByLogStep]_vars

\* spec #invariants-isolation, NoLostVisibleData. The fragments visible to a running transaction at its own
\* snapshot never shrink, except by its own drops; SetSnapshot recaptures h.content, so a transaction that
\* deliberately reads an older snapshot is judged against that snapshot's content, not against its first one.
\* It is stated over all running transactions rather than over "a step that is not an action of t", because the
\* two exemptions the spec's row gives for t's own steps, h.removing[t] and the SetSnapshot recapture, already
\* cover every way t can shrink its own view. Both exemptions are read in the post-state, which is what makes
\* that true: the step that enrols t's own removal is the step that adds the part to h.removing[t], and the
\* SetSnapshot step is the step that rewrites h.content[t], so a pre-state read of either would make t's own
\* drop and t's own SET TRANSACTION SNAPSHOT false positives. Both were, until the first run of this scenario.
\* A merge transaction, which never reads, has h.content = {} and is therefore vacuously covered.
VisibleFrags(t) == Frags({ r \in Parts : part[r].pstate \in {"Active", "Outdated"}
                                        /\ OracleVisible(r, txn[t].snapshot, t) })
\* The post-state `Absent` clause voids the promise on the one step that destroys the transaction rather than
\* shrinking its view: a transaction a crash took with it has no view left to preserve, and h.content is a
\* ghost the crash does not clear. Nothing but CrashEffect puts a live transaction into `Absent`, so the clause
\* costs no other step.
NoLostVisibleDataStep ==
  \A t \in Tids : (txn[t].state = "Running" /\ txn[t].snapshot /= EverythingVisibleCSN
                   /\ txn'[t].state /= "Absent") =>
    (h.content[t]' \ Frags(h.removing[t])') \subseteq VisibleFrags(t)'
NoLostVisibleData == [][NoLostVisibleDataStep]_vars

\* ---- the cleanup thread (spec #invariants-cleanup)
\* Stated over the oracle, not over VersionMetadata::canBeRemoved, so that a wrong canBeRemoved is caught rather
\* than assumed; the snapshot is the actual one a running transaction reads at, not the protected one.
\* The quantifier is over the transactions that can still START a read, not over every entry of running_list.
\* A transaction whose state is RolledBack is still in running_list until RollbackFinalize, and it still holds
\* the oldest snapshot back, but it can no longer begin one: SelectCapture requires `Running`, which is
\* executeQuery refusing the next statement with "Cannot execute query because current transaction failed"
\* (the QueryOnCancelled action). A read already IN FLIGHT when the KILL lands is a different matter, and it is
\* not this property that protects it: SelectCheck and SelectFinish carry no state guard, so such a read runs to
\* completion. What protects it is the pin. SelectCapture pins every captured part with <<"Select", k>> and
\* releases them only at SelectFinish or Refuse, and CleanupDecide requires part[p].pins = {}, which is
\* isSharedPtrUnique at MergeTreeData.cpp:4150 and is what PinnedNotDeleted states. So the narrowing gives up
\* nothing: the in-flight read was never covered by this property and is covered by another one.
\* Without the narrowing the oracle's "a transaction sees what it created" clause fires for the creator of a
\* part whose creation was rolled back, which is a part the C++ makes invisible to everyone, its creator
\* included: VersionInfo::isVisible returns false on `snapshot_version < creation_csn`
\* (src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:167) and RolledBackCSN is above every snapshot.
\* Counterexample F3.
\* Stated on CleanupGrab, the step that moves the part to Deleting, and not on CleanupDecide, which only
\* accepts it: the guarantee is about the moment the data goes, and a SET TRANSACTION SNAPSHOT between the two
\* is finding F2's second shape.
NoPrematureDeleteStep == \A p \in Parts : CleanupGrab(p) =>
  \A u \in tlog.running_list : txn[u].state = "Running" => ~OracleVisible(p, txn[u].snapshot, u)
NoPrematureDelete == [][NoPrematureDeleteStep]_vars
\* isSharedPtrUnique, MergeTreeData.cpp:4150, as a property rather than only as the guard of the action. It is
\* stated on the grab although the guard is in CleanupDecide, which is strictly stronger, and what makes it
\* hold is what canBeRemoved accepts rather than the parts lock. Six actions add pins, and four of them
\* cannot reach a part the cleanup has accepted:
\*   SelectCapture requires sys.parts_lock = NoActor, so it cannot run inside the hold. PublishStart requires
\*     the same, and every caller of EnrolGrantEffect requires the parts lock to be its own, so none of those
\*     can run inside it either. MergeWrite pins its result part, which it requires to be Absent, where the
\*     cleanup's candidate is Outdated. The two that remain are the ones the argument is about:
\*   MergeSelect does not take the lock, and it judges its sources visible at the merge's snapshot with the
\*     empty TID and refuses a removal-locked one. A part canBeRemoved accepts has a removal committed at or below the oldest
\*     snapshot, or a creation stamped RolledBackCSN, and neither is visible at an ORDINARY snapshot. The
\*     merge's snapshot is always an ordinary one, because MergeBegin registers latest_snapshot as Begin does,
\*     so a merge cannot select it. The qualifier is what finding F8 costs this argument: at
\*     EverythingVisibleCSN a rolled-back creation is visible, and a SELECT there does capture it. That reader
\*     is SelectCapture, which takes the parts lock, so it cannot run inside the hold, and what it captured
\*     before the hold it pinned.
\*   RollbackCopyLists does not either, and it pins the whole of a transaction's creating and removing lists at
\*     the START of the rollback, before the marking phase stamps RolledBackCSN. So by the time the rolled-back
\*     creation makes the part removable, the pin is already on it and CleanupDecide refuses.
\* The argument is about the snapshot the cleanup compares against, so the NoPrematureDelete and
\* NoLostVisibleData witnesses, which make CleanupDecide read latest_snapshot instead of OldestSnapshot, break
\* it: they let a part still visible to a running transaction be accepted, and a MergeSelect between the
\* decision and the grab could then pin it. Those two witnesses do not check this property, and a witness that
\* raised the snapshot while checking it would be red for a reason the C++'s single lockParts forbids. Model
\* defect M23 is where the one-part-per-pass abstraction that permits it is placed.
PinnedNotDeletedStep == \A p \in Parts : CleanupGrab(p) => part[p].pins = {}
PinnedNotDeleted == [][PinnedNotDeletedStep]_vars
\* the validation refusal is justified only by a disagreement history says cannot be transient
NoFalseCorruptionStep == \A p \in Parts : CleanupDeleteFail(p) => RealDisagreement(p)
NoFalseCorruption == [][NoFalseCorruptionStep]_vars

\* ---- the non-transactional removal batch (spec #invariants-conflicts)
\* Stated on the step that ends a batch with Refused: the batch wrote nothing. This is what the four upstream
\* fixes about half-applied batches are for (ab40e11d3c73, f8f46fb1eb14, 86b6861a1a8e, and ba2ee3239b8d for the
\* memory-only stamp): without a transaction there is no rollback, so a batch that refuses must leave no trace.
\* The spec row states it as "every target's mem, stored record and lock equal their values recorded in h_batch
\* at NtBatchStart", which is a statement about the whole world and is falsified by any concurrent actor that
\* touches a target for its own reasons -- a rollback clearing a removal lock or stamping RolledBackCSN takes
\* no lockParts, so it can run beside the batch. Recorded as spec defect S12. What the batch itself can write is
\* exactly a non-transactional removal TID, the NonTransactionalCSN that comes with it in the same update
\* function, and its own lock, so the property is stated over those three and is immune to what another actor
\* does. h.batch.before is still what "was it already there" is judged against, which is why it is recorded.
NtBatchRefusedUnchangedStep ==
  (sys.nt_batch.active /\ ~sys'.nt_batch.active /\ h'.batch_outcome = "Refused") =>
    \A p \in h.batch.targets :
      /\ (part'[p].mem.rtid = NonTransactionalTID => h.batch.before[p][1].rtid = NonTransactionalTID)
      /\ (StoredRecord(p)'.rtid = NonTransactionalTID => h.batch.before[p][2].rtid = NonTransactionalTID)
      /\ part'[p].lock /= NonTransactionalTID
      \* The one field of the spec row's equality that no concurrent actor can write, restored here because
      \* spec defect S12's argument does not reach it. A creation TID is written only by the "CreateTID" op,
      \* and every action that issues one -- InsertWrite, MergeWrite, NtInsertWrite, NtDropWrite -- requires
      \* an Absent part, which a batch target is not. The clause is therefore a tautology today and is kept
      \* so that an action that did write it would be caught here.
      \* The other fields of the spec row stay out, and not for convenience: MergeTreeTransaction::rollback
      \* runs beside a batch without taking lockParts, and it writes ccsn (RolledBackCSN) and, through the
      \* store it performs, sv. Requiring equality on either would make this property state something the
      \* server does not promise, which is what S12 records.
      /\ part'[p].mem.ctid = h.batch.before[p][1].ctid
      /\ StoredRecord(p)'.ctid = h.batch.before[p][2].ctid
NtBatchRefusedUnchanged == [][NtBatchRefusedUnchangedStep]_vars

\* A refusal is justified by an uncommitted creator as the transaction log sees it, or by a lock somebody else
\* holds. Both are read in the pre-state, which is the state the refusing step judged.
NtRefusalJustifiedStep ==
  (sys.nt_batch.active /\ ~sys'.nt_batch.active /\ h'.batch_outcome = "Refused") =>
    \E p \in h.batch.targets :
      \/ (part[p].mem.ccsn = UnknownCSN /\ part[p].mem.ctid \in Tids
          /\ LookupCsn(part[p].mem.ctid) = UnknownCSN)
      \/ part[p].lock /= EmptyTID
NtRefusalJustified == [][NtRefusalJustifiedStep]_vars

\* Not a property of the server: a bound guard, of the shape NoTaskDrivenRollback has. NtBatchStore has no
\* branch that consumes a frame parked in Error, so a store that failed would leave the batch with nothing able
\* to advance it. In the C++ such an exception leaves store, the SCOPE_EXIT and the destructor release the
\* locks, and the parts already drained keep their stored removal -- which is a half-applied batch and would be
\* a finding, not a refusal. No behaviour of this plan's scenario reaches it: the preflight refuses every
\* creation still in flight before anything is locked, so the SERIALIZATION_ERROR of setAndStoreRemovalTID
\* cannot fire in the store phase, and no other actor can store on a part the batch holds locked, so neither
\* can STALE_VERSION. This invariant says so rather than letting the search wedge without a word. The plan that
\* gives the batch a disk or query fault owes the action and takes this out.
NoNtStoreError == sys.nt_batch.active /\ sys.nt_batch.phase = "Store" =>
  \A p \in Parts : ~FrameError(p, sys.nt_batch.owner)

\* One conjunct per actor that can reach RollbackFinalizeA, the way FlipAfterStoresStep has one per actor that
\* can reach CommitFlipEffect. The updating thread drives a rollback of its own out of the unknown-state pass,
\* and quantifying over Sessions alone would leave that one unobserved.
RollbackRestoresOn(t) ==
  \A p \in h.removing[t] \ h.creating[t] :
    part'[p].pstate = "Active" \/ part[p].lock \notin {EmptyTID, t} \/ (h.removers[p] \ {t}) /= {}
RollbackRestoresStep ==
  /\ \A k \in Sessions, t \in Tids : RollbackFinalize(k, t) => RollbackRestoresOn(t)
  /\ \A t \in Tids : UpdRollbackFinalize(t) => RollbackRestoresOn(t)
RollbackRestores == [][RollbackRestoresStep]_vars
\* afterCommit stores every CSN before the state flip, and it does so on whatever thread is committing. The
\* property therefore has one conjunct per actor that can reach CommitFlipEffect: a session through CommitFlip
\* and a background task through MergeCommitFlip. Quantifying over Sessions alone would leave the task's flip
\* unobserved, which is what a merge committing on its own thread does.
FlipStoresDone(t) ==
  /\ \A p \in h.creating[t] : part[p].mem.ccsn = h.csn[t]
  /\ \A p \in h.removing[t] : part[p].mem.rcsn = h.csn[t]
  \* the spec row's third conjunct; vacuous while Mutations = {}, live from plan 4
  /\ \A m \in h.mutations[t] : mut[m].mstate /= "Killed" => mut[m].csn = h.csn[t]
FlipAfterStoresStep ==
  /\ \A k \in Sessions : CommitFlip(k) => FlipStoresDone(Cur(k))
  /\ \A i \in Tasks : MergeCommitFlip(i) => FlipStoresDone(task[i].txn)
FlipAfterStores == [][FlipAfterStoresStep]_vars
====
