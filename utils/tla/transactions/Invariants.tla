---- MODULE Invariants ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Server

IsoApplies(k) == Cur(k) /= EmptyTID /\ txn[Cur(k)].snapshot /= EverythingVisibleCSN
Frags(V) == { <<q, part[q].payload.ver>> : q \in Expand(V) }
Proj(F) == { <<f[2], f[3]>> : f \in F }      \* drop the root of a fragment tuple

\* ---- isolation (spec #invariants-isolation): evaluated when a read completes (SelectFinish), over the
\* read that step produces (client'[k].last_read) and the history as of that step. A read taken earlier is
\* not re-judged after the reader's own later effects, which is what a state invariant would wrongly do.
Own(t) == h.creating[t] \cup h.removing[t]
ReadOK(k, R) ==
  LET t == Cur(k)
      s == txn[t].snapshot IN
  txn[t].snapshot /= EverythingVisibleCSN /\ txn[t].state = "Running" =>
  /\ \* StableRead
     (client[k].first_read /= NoRead =>
        (Proj(client[k].first_read.frags) \ Frags(Own(t))) = (Proj(R.frags) \ Frags(Own(t))))
  /\ \* ReadYourWrites
     /\ \A p \in h.creating[t] \ h.removing[t] : part[p].pstate \in {"Active", "Outdated"} => p \in R.parts
     /\ \A p \in h.removing[t] : p \notin R.parts
  /\ \* NoUncommittedRead
     \A p \in R.parts : h.creator[p] \in h.committed \cup {t, NonTransactionalTID}
  /\ \* NoFutureRead
     \A p \in R.parts : OracleVisible(p, s, t)
  /\ \* NoLostRead
     \A p \in Parts : (part[p].pstate \in {"Active", "Outdated"} /\ OracleVisible(p, s, t))
        => (p \in R.parts \/ \E c \in R.parts : p \in Expand({c}) /\ OracleVisible(c, s, t))
  /\ \* NoDoubleRead
     \A f1, f2 \in R.frags : f1[2] = f2[2] => f1[1] = f2[1]
  /\ \* Atomicity
     \A u \in h.committed : h.loaded[u] /\ s >= h.csn[u] /\ u /= t =>
       LET V == R.parts
           C == { p \in h.creating[u] \ h.removing[u] :
                    /\ ~\E r \in h.removers[p] \ {u} : h.csn[r] <= s
                    /\ p \notin h.removing[t] }
           Rm == h.removing[u] \ h.creating[u] IN
       /\ (C \subseteq V /\ V \cap Rm = {}) \/ (C \cap V = {} /\ Rm \subseteq V)
       /\ V \cap (h.creating[u] \cap h.removing[u]) = {}
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
RollbackNoLeak == \A k \in Sessions : \A t \in Tids : Cur(k) /= EmptyTID /\ Cur(k) /= t /\ t \notin h.committed =>
  client[k].last_read.parts \cap h.creating[t] = {}

\* ---- durability (Durable disk mode in Base)
AckedWriteIsDurable == \A t \in Tids : h.outcome[t] = "Acked" /\ (h.creating[t] \cup h.removing[t]) /= {} =>
  /\ t \in h.committed
  /\ \A p \in h.creating[t] : \/ part[p].pstate = "Active"
                             \/ (part[p].pstate = "Outdated" /\ (part[p].lock /= EmptyTID \/ h.removers[p] /= {}))   \* removal in flight or committed
                             \/ (part[p].pstate \in {"Deleting", "Deleted"} /\ h.removers[p] /= {})
  /\ \A p \in h.removing[t] : part[p].pstate /= "Active"
ErrorIsAbsent == \A t \in Tids : h.outcome[t] = "Error" =>
  t \notin h.committed /\ (h.rolled_back[t] => \A p \in h.creating[t] : part[p].pstate /= "Active")

\* ---- conflicts (spec #invariants-conflicts)
SingleRemover == \A p \in Parts : Cardinality(h.removers[p]) <= 1
LockConsistent == \A p \in Parts :
  LET l == part[p].lock
      m == part[p].mem IN
  /\ (l \in Tids => m.rtid \in {EmptyTID, l})
  /\ (l = NonTransactionalTID => m.rtid \in {EmptyTID, NonTransactionalTID})
  /\ (l = EmptyTID => m.rtid = EmptyTID \/ m.rcsn /= UnknownCSN)
ActiveSetShape == /\ \A p, q \in Parts : part[p].pstate = "Active" /\ part[q].pstate = "Active" => ~Overlap(p, q)
                  /\ \A i, j \in Tasks : i /= j => task[i].reserved \cap task[j].reserved = {}

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
Assert_getOldestSnapshot == /\ tlog.running_list = { t \in Tids : tlog.snapshots_in_use[t] /= UnknownCSN }
                            /\ \A t \in tlog.running_list : tlog.snapshots_in_use[t] = txn[t].protected_snapshot
NoAvoidableTermination == h.down_cause \in {"None", "RetryExhausted"}
NoSpuriousStaleVersion == \A k \in Sessions : client[k].last_error = "STALE_VERSION" => client[k].stale_interferences = MAX_STORE_RETRIES
\* A KILL query blocks at KillWait until the rollback it drives finishes, and the runner disables TLC's deadlock
\* check (an exhausted TID_MAX leaves a legitimate idle terminal state), so a killer that could never be released
\* would pass unnoticed. Only RollbackFinalize clears rb_driver, so a killer parked on a transaction whose
\* rollback machine has already gone idle is stranded; when no transaction names it, KillReturn is enabled.
KillerNotStranded == \A k \in Sessions : client[k].pc = "KillWait" =>
  \A t \in Tids : txn[t].rb_driver = Sess(k) => txn[t].pc /= "Idle"

\* ---- action properties
RollbackRestoresStep == \A k \in Sessions, t \in Tids : RollbackFinalize(k, t) =>
  \A p \in h.removing[t] \ h.creating[t] :
    part'[p].pstate = "Active" \/ part[p].lock \notin {EmptyTID, t} \/ (h.removers[p] \ {t}) /= {}
RollbackRestores == [][RollbackRestoresStep]_vars
FlipAfterStoresStep == \A k \in Sessions : CommitFlip(k) =>
  LET t == Cur(k) IN /\ \A p \in h.creating[t] : part[p].mem.ccsn = h.csn[t]
                     /\ \A p \in h.removing[t] : part[p].mem.rcsn = h.csn[t]
FlipAfterStores == [][FlipAfterStoresStep]_vars
====
