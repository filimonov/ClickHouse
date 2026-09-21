---- MODULE MC_SetSnapshotF9Sibling ----
EXTENDS MergeTreeTransactions
\* Finding F9: a transactional DROP PARTITION at EverythingVisibleCSN removes a part whose creation is still
\* in flight. The skip removePartsFromWorkingSet applies before enrolment tests creation_csn against
\* RolledBackCSN (src/Storages/MergeTree/MergeTreeData.cpp:7044-7046) and a creation that has not finished
\* carries Tx::UnknownCSN, so the skip does not apply; at this snapshot isVisible returns true before it looks
\* at any CSN (src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:157-158), so the reader does not take the
\* log lookup an ordinary snapshot makes, which answers invisible while the creation CSN is unknown; and
\* lockRemovalTID refuses only a removal already locked or already committed
\* (src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:195-248), never a creation that has not
\* committed.
\* The trace this module produces is the finding's first shape and stops at the validation error, which is
\* where the C++ raises it: setAndStoreRemovalTID validates the record before storing it, so nothing is
\* written, and the LOGICAL_ERROR leaves DROP PARTITION as an exception to the client, because the store sits
\* outside removeOldPart's NOEXCEPT_SCOPE. The second shape, where the creator rolls back between the skip and
\* the store and the remover's commit then raises inside the noexcept afterCommit, is reached under the
\* F9FixInFlightOnly witness on MC_SetSnapshotF9SiblingFixed and is that trace, not this one.
\* Two sessions, because the shape needs a creator that is still Running while another transaction drops;
\* TID_MAX = 2, one part, and SNAPSHOT_TARGETS = {3}. SET_SNAPSHOT_PROTECTS is FALSE, the baseline, so that a
\* red here is a red of the code as it stands and not of finding F2's fix variant.
\* The roster is the three rows the shape is about: TypeOK, Assert_validateInfo and the NoAvoidableTermination
\* that the assertion inside the noexcept afterCommit produces. RollbackNoLeak is left out because finding F8
\* falsifies it at this target for a different reason, and ErrorIsAbsent because an ordinary SERIALIZATION_ERROR
\* between the two sessions would fire it first and hide the row the module is for.
\* Expected red on Assert_validateInfo.
CoversDef == [p \in Parts |-> {}]
SymSessions == Permutations(Sessions)

\* The view is MC_SetSnapshot's, verbatim. What it depends on is which actions are enabled and which
\* properties are checked; the same actions are enabled here and the properties checked are the cleanup ones,
\* whose two fields part.pins and h.content the projection keeps. Only the constants below differ.
\* Fingerprint projection for this scenario only; see STATE_SPACE.md. It is BaseView plus the fields
\* SET TRANSACTION SNAPSHOT and log truncation make live: h.truncated, tlog.tail_ptr, tlog.updated_tail_ptr,
\* zk.tail, sys.cleanup_pc and sys.cleanup_part. zk as a whole was already kept, so zk.tail comes with it; the
\* rest are named below. Three kinds of field are left out:
\* fields no action and no property of this scenario reads (h.snapshot, txn.csn_notified, client.outcome,
\* client.outcome_tid, and last_error beyond the one value NoSpuriousStaleVersion tests), fields that are a function
\* of the kept ones (the durable layer, which equals the cached layer under DISK_MODE = "Durable", and a read's
\* frags, which Covers = empty and a constant payload make a function of its parts), and fields that no enabled
\* action writes (mdisk, mut, task, part.payload, the mutation and non-transactional parts of h, the
\* unknown-state part of tlog, the fault counters and loading fields of sys, a frame's noexcept_retries; the
\* tail-pointer part of tlog is no longer among them, because UpdRemoveOldEntriesSetTail writes it).
\* Each of the three depends on the configuration below, so a scenario with other constants needs its own view.
\* What each of them depends on, though, is only which actions are enabled and which properties are checked, so
\* raising TID_MAX or CSN_MAX alone leaves the argument intact.
\* part.pins and h.content are back in the projection, and they are the two fields the cleanup thread makes
\* live. part.pins is read by CleanupDecide's isSharedPtrUnique guard and by PinnedNotDeleted, so two states that
\* differ only in a pin no longer have the same successors; h.content is read by NoLostVisibleData, which the
\* Fixed and Witness configurations check. Both were left out while neither the actions nor the properties
\* existed, and the argument for leaving them out was explicitly conditioned on that, so it is the condition
\* that changed, not the argument. Their cost is measured in STATE_SPACE.md.
FrameKey(f) == <<f.owner, f.op, f.val, f.tentative, f.pc, f.err, f.retries, f.interferences, f.interfered,
                 f.noexcept_owner>>
SetSnapshotF9SiblingView ==
  << zk,
     [p \in Parts |-> <<disk[p].cached, disk[p].tmp_cached, disk[p].dir_cached>>],
     <<h.outcome, h.committed, h.csn, h.loaded, h.creating, h.removing,
       h.rolled_back, h.removers, h.creator, h.abandoned, h.down_cause, h.truncated, h.content>>,
     [p \in Parts |-> <<part[p].pstate, part[p].mem, part[p].lock, part[p].deferrable,
                        part[p].deferred_on, part[p].deferred, part[p].pins,
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
                           client[k].capture, client[k].captured0, client[k].checked,
                           client[k].batch, client[k].waiting, client[k].pc, client[k].work,
                           client[k].part, client[k].stale_interferences, client[k].rb_detach,
                           client[k].holds_blocker>>],
     stmt >>
====
