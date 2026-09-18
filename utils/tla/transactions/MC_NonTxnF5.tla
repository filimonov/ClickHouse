---- MODULE MC_NonTxnF5 ----
EXTENDS MergeTreeTransactions
\* The three part names, declared so that CoversDef can spell them; the cfg binds each to the model value of
\* the same name that Parts is built from.
CONSTANTS P1, P2, E
\* The covering relation the scenario exists for: E is the empty part a non-transactional DROP PARTITION
\* publishes over P1 and P2, which are the only base parts an insert can create (InsertWrite and NtInsertWrite
\* both require IsBase).
CoversDef == [p \in Parts |-> IF p = E THEN {P1, P2} ELSE {}]
SymSessions == Permutations(Sessions)

\* This module exists to produce finding F5 and nothing else: the NonTxn scenario at NT_COVER_OUTDATED = FALSE,
\* cut to one session, checking only ActiveSetShape, and deliberately RED. MC_NonTxnFixed is the green run of
\* the same scenario with the proposed fix. The shape is MC_SetSnapshotF2's, for the same reason.
\* Fingerprint projection for this scenario only; see STATE_SPACE.md. It is MergeView's shape, because Covers
\* is non-empty here too and a read's frags are therefore not a function of its parts, with three changes:
\*   - `task` is dropped, because Tasks = {} and no action of this scenario writes it;
\*   - tlog.tail_ptr, tlog.updated_tail_ptr and h.truncated are dropped, because Updater+GC is off and no
\*     action of this scenario writes them;
\*   - sys.nt_batch, h.batch and h.batch_outcome are added, because the batch actions read all three and the
\*     two batch properties read the last two.
\* h.removers was already in MergeView's tuple and is read by the visibility oracle, which NtBatchStore writes.
\* client.last_error is still kept only as the one boolean NoSpuriousStaleVersion tests: NtDropFlip writes
\* "SERIALIZATION_ERROR" into it, and no action and no property of this scenario reads that value.
FrameKey(f) == <<f.owner, f.op, f.val, f.tentative, f.pc, f.err, f.retries, f.interferences, f.interfered,
                 f.noexcept_owner>>
NonTxnF5View ==
  << zk,
     [p \in Parts |-> <<disk[p].cached, disk[p].tmp_cached, disk[p].dir_cached>>],
     <<h.outcome, h.committed, h.csn, h.loaded, h.creating, h.removing,
       h.rolled_back, h.removers, h.creator, h.abandoned, h.down_cause, h.content,
       h.batch, h.batch_outcome>>,
     [p \in Parts |-> <<part[p].pstate, part[p].mem, part[p].lock, part[p].deferrable,
                        part[p].deferred_on, part[p].deferred, part[p].pins, part[p].payload,
                        { FrameKey(f) : f \in part[p].frames }>>],
     <<tlog.tid_start, tlog.tid_to_csn, tlog.latest_snapshot, tlog.local_tid_counter,
       tlog.last_loaded_entry, tlog.running_list, tlog.snapshots_in_use>>,
     [t \in Tids |-> <<txn[t].state, txn[t].csn, txn[t].snapshot, txn[t].protected_snapshot,
                       txn[t].creating, txn[t].removing, txn[t].mutations, txn[t].holders,
                       txn[t].rb_driver, txn[t].mutex, txn[t].pc, txn[t].work>>],
     <<sys.server, sys.parts_lock, sys.merges_blocker, sys.updater_pc, sys.cleanup_pc, sys.cleanup_part,
       sys.nt_batch>>,
     [k \in Sessions |-> <<client[k].current, client[k].last_error = "STALE_VERSION",
                           client[k].first_read.parts, client[k].last_read.parts,
                           client[k].first_read.frags, client[k].last_read.frags,
                           client[k].capture, client[k].captured0, client[k].checked,
                           client[k].batch, client[k].waiting, client[k].pc, client[k].work,
                           client[k].part, client[k].stale_interferences, client[k].rb_detach,
                           client[k].holds_blocker>>],
     stmt >>
====
