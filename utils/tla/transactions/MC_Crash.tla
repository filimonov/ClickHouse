---- MODULE MC_Crash ----
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

\* Fingerprint projection for this scenario only; see STATE_SPACE.md. It is MergeView plus everything the
\* crash, the three sync actions and the restart loader read or write: the durable disk layers, h.payload, and
\* the phase, the loading queues and the restart counter in sys. tlog.last_loaded_entry was already in
\* MergeView, which RestartLoadLog rewrites.
\*
\* Of MergeView's three justifications, two survive and the second does not.
\*   1. Fields no action and no property of this scenario reads: txn.csn_notified, client.outcome,
\*      client.outcome_tid, and last_error beyond the one value NoSpuriousStaleVersion tests.
\*   2. DEAD. MergeView's second group was "fields that are a function of the ones kept: the durable layer,
\*      which DISK_MODE = "Durable" keeps equal to the cached layer". This scenario is Layered, so the two
\*      layers are independent: Fsync and FsyncDir move one without the other, and the durable layer is what
\*      the crash keeps and what the invariant preamble reads for an unloaded part. The clause that replaces
\*      it is the disk row of the projection below, which now carries both layers of the record, of the tmp
\*      file and of the directory.
\*   3. Fields no action of this scenario writes: mdisk, mut, the mutation and non-transactional parts of h,
\*      the unknown-state part of tlog, the fault counters of sys, a frame's noexcept_retries. sys.mut_queue
\*      and sys.loaded_mutations have NOT left this group although the restart writes them, because Mutations
\*      is empty here and both stay {} in every reachable state.
\*   4. One field this scenario writes and no property or action of it reads: sys.load_since_swap.
\*      UpdLoadEntriesMap writes it and SysDown resets it, so group 3 does not cover it; its only two readers,
\*      UpdSwapUnknownLists and UpdFinalizeUnknown, are in UpdaterUnknownNext, which CrashNext does not
\*      enable. The view is the fence that decides which counterexamples survive, so the reason is written
\*      rather than left to the reader.
\* MergeView's correction about frags stands unchanged: with M12 covering P1 and P2 a read's fragments are not
\* a function of its parts, so both first_read.frags and last_read.frags are in the fingerprint.
FrameKey(f) == <<f.owner, f.op, f.val, f.tentative, f.pc, f.err, f.retries, f.interferences, f.interfered,
                 f.noexcept_owner>>
CrashView ==
  << zk,
     [p \in Parts |-> <<disk[p].cached, disk[p].tmp_cached, disk[p].dir_cached, disk[p].named_cached,
                        disk[p].durable, disk[p].tmp_durable, disk[p].dir_durable, disk[p].named_durable,
                        disk[p].payload_durable, disk[p].payload_cached>>],
     <<h.outcome, h.committed, h.csn, h.loaded, h.creating, h.removing,
       h.rolled_back, h.removers, h.creator, h.abandoned, h.down_cause, h.content, h.truncated, h.payload>>,
     [p \in Parts |-> <<part[p].pstate, part[p].mem, part[p].lock, part[p].deferrable,
                        part[p].deferred_on, part[p].deferred, part[p].pins, part[p].payload,
                        { FrameKey(f) : f \in part[p].frames }>>],
     <<tlog.tid_start, tlog.tid_to_csn, tlog.latest_snapshot, tlog.local_tid_counter,
       tlog.last_loaded_entry, tlog.running_list, tlog.snapshots_in_use, tlog.retention_in_use,
       tlog.tail_ptr, tlog.updated_tail_ptr>>,
     [t \in Tids |-> <<txn[t].state, txn[t].csn, txn[t].snapshot, txn[t].protected_snapshot,
                       txn[t].creating, txn[t].removing, txn[t].mutations, txn[t].holders,
                       txn[t].rb_driver, txn[t].mutex, txn[t].pc, txn[t].work>>],
     <<sys.server, sys.parts_lock, sys.merges_blocker, sys.updater_pc, sys.cleanup_pc, sys.cleanup_part,
       sys.completely_started, sys.async_loading_jobs, sys.loaded_parts, sys.load_queue,
       sys.outdated_queue, sys.restarts>>,
     [k \in Sessions |-> <<client[k].current, client[k].last_error = "STALE_VERSION",
                           client[k].first_read.parts, client[k].last_read.parts,
                           client[k].first_read.frags, client[k].last_read.frags,
                           client[k].capture, client[k].captured0, client[k].checked,
                           client[k].batch, client[k].waiting, client[k].pc, client[k].work,
                           client[k].part, client[k].stale_interferences, client[k].rb_detach,
                           client[k].holds_blocker>>],
     stmt, task >>
====
