---- MODULE MC_KeeperWitness ----
EXTENDS MergeTreeTransactions
\* MC_Keeper at two sessions, for witness runs only. A witness run stops at the first violation and can afford
\* bounds an exhaustive run cannot, which is the two-bound-sets rule of spec defect S7; this module is those
\* witness bounds. run_tlc.sh is not meant to be pointed at it.
\* The three part names, declared so that CoversDef can spell them; the cfg binds each to the model value of
\* the same name that Parts is built from.
CONSTANTS P1, P2, M12
\* The covering relation the scenario exists for: M12 covers P1 and P2, which are the only base parts a client
\* can insert (InsertWrite requires IsBase). One covering part means exactly one merge is possible, which is
\* why Tasks is a singleton; the argument is in STATE_SPACE.md.
CoversDef == [p \in Parts |-> IF p = M12 THEN {P1, P2} ELSE {}]
\* Two sessions here, so the group is a real permutation of them.
SymSessions == Permutations(Sessions)

\* Fingerprint projection for this scenario only; see STATE_SPACE.md. It is MergeView plus everything the
\* Keeper faults and the unknown-state pass read or write: the four unknown-state fields of tlog and h.unknown.
\* zk is kept whole in BaseView, so zk.session comes with it, and h.outcome and client.waiting were already in
\* MergeView.
\*
\* Of MergeView's three justifications the first two survive and the third loses two of its members.
\*   1. Fields no action and no property of this scenario reads: client.outcome and client.outcome_tid, and
\*      last_error beyond the one value NoSpuriousStaleVersion tests.
\*   2. Fields that are a function of the ones kept: the durable disk layer, which DISK_MODE = "Durable" keeps
\*      equal to the cached layer; and txn.csn_notified, which CommitUnknownResolved reads and which is TRUE in
\*      exactly the states where txn.state is "Committed" or "RolledBack", because every action that writes one
\*      writes the other in the same record.
\*   3. Fields no action of this scenario writes: mdisk, mut, the mutation and non-transactional parts of h,
\*      the loading fields of sys, a frame's noexcept_retries. The unknown-state part of tlog and the
\*      keeper_faults counter have LEFT this group, because this scenario writes both; the first is in the
\*      projection and the second is a function of the faults already taken, which zk.session and the
\*      unknown-state fields record.
\* MergeView's correction about frags stands unchanged: with M12 covering P1 and P2 a read's fragments are not
\* a function of its parts, so both first_read.frags and last_read.frags are in the fingerprint.
FrameKey(f) == <<f.owner, f.op, f.val, f.tentative, f.pc, f.err, f.retries, f.interferences, f.interfered,
                 f.noexcept_owner>>
KeeperWitnessView ==
  << zk,
     [p \in Parts |-> <<disk[p].cached, disk[p].tmp_cached, disk[p].dir_cached>>],
     <<h.outcome, h.committed, h.csn, h.loaded, h.creating, h.removing,
       h.rolled_back, h.removers, h.creator, h.abandoned, h.down_cause, h.content, h.truncated, h.unknown>>,
     [p \in Parts |-> <<part[p].pstate, part[p].mem, part[p].lock, part[p].deferrable,
                        part[p].deferred_on, part[p].deferred, part[p].pins, part[p].payload,
                        { FrameKey(f) : f \in part[p].frames }>>],
     <<tlog.tid_start, tlog.tid_to_csn, tlog.latest_snapshot, tlog.local_tid_counter,
       tlog.last_loaded_entry, tlog.running_list, tlog.snapshots_in_use, tlog.retention_in_use,
       tlog.tail_ptr, tlog.updated_tail_ptr,
       tlog.unknown_state_list, tlog.unknown_state_list_loaded, tlog.unknown_ready, tlog.finalizing>>,
     [t \in Tids |-> <<txn[t].state, txn[t].csn, txn[t].snapshot, txn[t].protected_snapshot,
                       txn[t].creating, txn[t].removing, txn[t].mutations, txn[t].holders,
                       txn[t].rb_driver, txn[t].mutex, txn[t].pc, txn[t].work>>],
     <<sys.server, sys.parts_lock, sys.merges_blocker, sys.updater_pc, sys.load_since_swap,
       sys.cleanup_pc, sys.cleanup_part>>,
     [k \in Sessions |-> <<client[k].current, client[k].last_error = "STALE_VERSION",
                           client[k].first_read.parts, client[k].last_read.parts,
                           client[k].first_read.frags, client[k].last_read.frags,
                           client[k].capture, client[k].captured0, client[k].checked,
                           client[k].batch, client[k].waiting, client[k].pc, client[k].work,
                           client[k].part, client[k].stale_interferences, client[k].rb_detach,
                           client[k].holds_blocker>>],
     stmt, task >>
====
