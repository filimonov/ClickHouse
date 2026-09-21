---- MODULE MC_Merge ----
EXTENDS MergeTreeTransactions
\* The three part names, declared so that CoversDef can spell them; the cfg binds each to the model value of
\* the same name that Parts is built from.
CONSTANTS P1, P2, M12
\* The covering relation the scenario exists for: M12 covers P1 and P2, which are the only base parts a client
\* can insert (InsertWrite requires IsBase). One covering part means exactly one merge is possible, which is
\* why Tasks is a singleton; the argument is in STATE_SPACE.md.
CoversDef == [p \in Parts |-> IF p = M12 THEN {P1, P2} ELSE {}]
\* One session, so this is the identity group and the SYMMETRY line is a no-op. It is kept so that the cfg is
\* the same shape as its siblings and raising the session count needs no other edit.
SymSessions == Permutations(Sessions)

\* Fingerprint projection for this scenario only; see STATE_SPACE.md. It is BaseView plus everything the merge
\* task, the cleanup thread and the truncation pass read or write: task, h.content (NoLostVisibleData),
\* part.payload (FragsOf and the third clause of ActiveSetShape), part.pins (CleanupDecide's isSharedPtrUnique
\* guard), tlog.tail_ptr, tlog.updated_tail_ptr, h.truncated, sys.cleanup_pc and sys.cleanup_part. zk was
\* already kept whole in BaseView, so zk.tail comes with it.
\*
\* Of BaseView's three justifications, two survive unchanged and one does not.
\*   1. Fields no action and no property of this scenario reads: txn.csn_notified, client.outcome,
\*      client.outcome_tid, and last_error beyond the one value NoSpuriousStaleVersion tests. h.content and
\*      part.pins have left this group, which is why they are in the projection.
\*   2. Fields that are a function of the ones kept: the durable disk layer, which DISK_MODE = "Durable" keeps
\*      equal to the cached layer.
\*   3. Fields no action of this scenario writes: mdisk, mut, the mutation and non-transactional parts of h,
\*      the unknown-state part of tlog, the fault counters and loading fields of sys, a frame's
\*      noexcept_retries.
\* What does NOT survive is BaseView's claim that a read's frags are a function of its parts. That held because
\* Covers was empty and every payload was constant, so a root expanded to itself. With M12 covering P1 and P2 a
\* read of {M12} and a read of {P1, P2} have the same fragments and different parts, and a read of {M12} taken
\* before and after a payload change would have the same parts and different fragments. NoDoubleRead is stated
\* over frags alone. Both first_read.frags and last_read.frags are therefore in the fingerprint.
FrameKey(f) == <<f.owner, f.op, f.val, f.tentative, f.pc, f.err, f.retries, f.interferences, f.interfered,
                 f.noexcept_owner>>
MergeView ==
  << zk,
     [p \in Parts |-> <<disk[p].cached, disk[p].tmp_cached, disk[p].dir_cached>>],
     <<h.outcome, h.committed, h.csn, h.loaded, h.creating, h.removing,
       h.rolled_back, h.removers, h.creator, h.abandoned, h.down_cause, h.content, h.truncated>>,
     [p \in Parts |-> <<part[p].pstate, part[p].mem, part[p].lock, part[p].deferrable,
                        part[p].deferred_on, part[p].deferred, part[p].pins, part[p].payload,
                        { FrameKey(f) : f \in part[p].frames }>>],
     <<tlog.tid_start, tlog.tid_to_csn, tlog.latest_snapshot, tlog.local_tid_counter,
       tlog.last_loaded_entry, tlog.running_list, tlog.snapshots_in_use, tlog.retention_in_use,
       tlog.tail_ptr, tlog.updated_tail_ptr>>,
     [t \in Tids |-> <<txn[t].state, txn[t].csn, txn[t].snapshot, txn[t].protected_snapshot,
                       txn[t].creating, txn[t].removing, txn[t].mutations, txn[t].holders,
                       txn[t].rb_driver, txn[t].mutex, txn[t].pc, txn[t].work>>],
     <<sys.server, sys.parts_lock, sys.merges_blocker, sys.updater_pc, sys.cleanup_pc, sys.cleanup_part>>,
     [k \in Sessions |-> <<client[k].current, client[k].last_error = "STALE_VERSION",
                           client[k].first_read.parts, client[k].last_read.parts,
                           client[k].first_read.frags, client[k].last_read.frags,
                           client[k].capture, client[k].captured0, client[k].checked,
                           client[k].batch, client[k].waiting, client[k].pc, client[k].work,
                           client[k].part, client[k].stale_interferences, client[k].rb_detach,
                           client[k].holds_blocker>>],
     stmt, task >>
====
