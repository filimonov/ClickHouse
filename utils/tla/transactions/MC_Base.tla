---- MODULE MC_Base ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
SymSessions == Permutations(Sessions)

\* Fingerprint projection for this scenario only; see STATE_SPACE.md. Three kinds of field are left out:
\* fields no action and no property of Base reads (h.content, h.snapshot, txn.csn_notified, client.outcome,
\* client.outcome_tid, part.pins, and last_error beyond the one value NoSpuriousStaleVersion tests), fields that
\* are a function of the kept ones (the durable layer, which equals the cached layer under DISK_MODE = "Durable",
\* and a read's frags, which Covers = empty and a constant payload make a function of its parts), and fields that
\* no Base action writes (mdisk, mut, task, the mutation and non-transactional parts of h, the tail-pointer and
\* unknown-state parts of tlog, the fault counters and loading fields of sys, a frame's noexcept_retries).
\* Each of the three depends on the configuration below, so a scenario with other constants needs its own view.
FrameKey(f) == <<f.owner, f.op, f.val, f.tentative, f.pc, f.err, f.retries, f.interferences, f.interfered,
                 f.noexcept_owner>>
BaseView ==
  << zk,
     [p \in Parts |-> <<disk[p].cached, disk[p].tmp_cached, disk[p].dir_cached>>],
     <<h.outcome, h.committed, h.csn, h.loaded, h.creating, h.removing,
       h.rolled_back, h.removers, h.creator, h.down_cause>>,
     [p \in Parts |-> <<part[p].pstate, part[p].mem, part[p].lock, part[p].deferrable,
                        part[p].deferred_on, part[p].deferred, { FrameKey(f) : f \in part[p].frames }>>],
     <<tlog.tid_start, tlog.tid_to_csn, tlog.latest_snapshot, tlog.local_tid_counter,
       tlog.last_loaded_entry, tlog.running_list, tlog.snapshots_in_use>>,
     [t \in Tids |-> <<txn[t].state, txn[t].csn, txn[t].snapshot, txn[t].protected_snapshot,
                       txn[t].creating, txn[t].removing, txn[t].mutations, txn[t].holders,
                       txn[t].mutex, txn[t].pc, txn[t].work>>],
     <<sys.server, sys.parts_lock, sys.merges_blocker, sys.updater_pc>>,
     [k \in Sessions |-> <<client[k].current, client[k].last_error = "STALE_VERSION",
                           client[k].first_read.parts, client[k].last_read.parts,
                           client[k].capture, client[k].captured0, client[k].checked,
                           client[k].batch, client[k].waiting, client[k].pc, client[k].work,
                           client[k].part, client[k].stale_interferences, client[k].rb_detach,
                           client[k].holds_blocker>>],
     stmt >>
====
