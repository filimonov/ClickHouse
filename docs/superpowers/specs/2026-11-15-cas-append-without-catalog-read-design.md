---
description: 'Design for removing the catalog read from a positive CAS ref-log append and the manifest re-read from `PartWriteTxn::promote`. A ref runtime admitted for a catalog row in state `Removing` is closed for writes from the moment it exists, and stays closed. `stageManifest` checks that the body it writes is readable. One `INSERT` drops from 23 to 19 object-store requests. No format change and no setting.'
sidebar_label: 'CAS: append without the catalog read'
sidebar_position: 117
slug: /superpowers/specs/cas-append-without-catalog-read-design
title: 'CAS: ref append without the catalog read, promote without the manifest re-read'
doc_type: 'reference'
---

# CAS: ref append without the catalog read — design {#cas-append-without-catalog-read}

- Date: 2026-11-15, revision 3. Status: design, not implemented. No test and no model was run; correct clocks are assumed.
- Code: worktree `cas-pr1`, commit `b716a9b636d` (same tree as `97123265e03`). Every code `path:line` was read there.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `L:` = `CA/Pool/CasRefLedger.cpp`. `C:` = `CA/Pool/CasRefCatalog.cpp`. `W:` = `CA/Pool/CasPartWriteTxn.cpp`. `F:` = `CA/Formats/CasPartManifestFormat.cpp`.
- Supersedes [section 7 of the one-request append design](2026-11-14-cas-one-request-ref-append-design.md#deferred): none of its five conditions is needed to remove the read.
- **Positive append**: a chunk with a new binding or `SetPublishedAt` (`L:3772-3776`). **Runtime**: a `RefTableRuntime`. **Lane closed**: its `removal_admission_closed` is set.

## 1. Decision and effect {#decision}

1. **Story 1, read removal.** `commitRefChunk` loses its `positive_append` block (`L:3824-3858`): a positive append
   sends no catalog `GET`. The terminal branch (`L:3783-3822`) is unchanged.
2. **Story 1, closed runtime.** `acquireRefTableRuntime` takes the catalog state its caller read. For `Removing` it
   closes the lane of the runtime it returns, for good ([§3](#removing-runtime)). This lands before item 1.
3. **Story 2.** `PartWriteTxn::promote` sends no manifest `GET`. `stageManifest` decodes the bytes it is about to
   write and records the blob leaves of the body; `promote` uses the leaves ([§5](#promote)).

| Requests | Today | After story 1 | After story 2 |
|---|---|---|---|
| Positive append | 2 | 1 | 1 |
| New part without blobs | 8 | 5 | 4 |
| `INSERT`, 5 new blobs | 23 | 20 | 19 |
| `INSERT`, 5 existing blobs | 18 | 15 | 14 |
| Serial round trips, new blobs / existing blobs | 11 / 10 | 8 / 7 | 7 / 6 |

- Requests that disappear, by number in window A of `tmp/reflog1/reports/insert-request-ledger.md` (`master` worktree):
  #2, #20, #22 are the catalog `GET` before the precommit, promote and `Move` appends; #19 is the manifest `GET`.
- "Today" is measured; the other columns are that ledger minus the removed requests, not measured. Unchanged: the
  terminal append (2 requests), cold admission, the recovery confirmation, the blob protocol.

## 2. Why the catalog read can go {#safety}

No positive append is in flight when a row leaves `Live`, except in [§4](#residual), where nothing can erase the row.

1. **One writer of `Live -> Removing`.** `beginRemoving` (`C:413-457`), called only from `dropNamespaceImpl` (`L:5685`).
2. **The drop closes and drains first.** It sets the flag and waits for an idle lane before it sends the write
   (`L:5632-5639`); enqueue checks the flag under the lock of the push (`L:2156-2172`). After a sent write with an
   unknown outcome the lane stays closed (`L:5745-5760`) and the runtime is not evicted (`L:1820`).
3. **A row is erased only after a folded terminal record.** `deleteCompletedRemovingAtSnapshot` (`C:459-480`) has one
   caller (`CA/Gc/CatalogLifecycleReconciler.cpp:93`) and needs a `Removing` row with `cleanup_evidence` and no hold
   (`:33-41`). Only a folded terminal record gives evidence (`CA/Gc/CasGc.cpp:3410-3423`).
4. **`Removing` grants nothing.** GC folds a `Removing` row like a `Live` one (`CA/Gc/CasGc.cpp:251-254`). The janitor
   deletes only objects of a life the catalog does not name (`CA/Gc/CasNamespaceJanitor.cpp:86`, `:140`).
5. **The read is not atomic with the `PUT`.** The stamp, the state snapshot, the preparation and the arming sit between
   them (`L:3919-4057`, create at `L:4145`). The create's own operation carries the lease check (`L:4040`).
6. **A positive op on a terminal state fails replay before the `PUT`**: a birth (`CA/Pool/CasRefProtocol.cpp:458-461`),
   an owner transition (`:288-289`), `SetPublishedAt` (`:425-426`).

| Actor | Can it change the row under an open lane | Without the read |
|---|---|---|
| Own `DROP`, same runtime | yes | nothing: fact 2 |
| Late copy of own `beginRemoving`, same mount generation | yes | nothing: the lane stays closed |
| Late copy from an earlier mount generation | yes | [§4](#residual): the stop comes later, nothing is deleted |
| Decommission (`CA/Tools/CasDecommission.cpp:391`, `:397`) | no: its claim needs a terminal mount slot (`CA/Pool/CasPool.cpp:807-852`) | nothing |
| A second process on the same server root | no: the mount claim waits out or refuses a live holder (`CA/Pool/CasPool.cpp:797-805`). With `unsafe_remount_no_delay` (`:780-793`) it takes the slot; its recovery then closes the old chain with a tombstone (`L:1176-1190`) that the old writer's next create meets (`L:4123-4140`) | nothing: the read was never the fence |
| GC erase; namespace janitor | erase: only a `Removing` row with a folded terminal. The janitor never writes the catalog | nothing: facts 3, 4 |
| Orphan manifest sweep; GC ref cleanup | neither writes the catalog; both treat `Removing` like `Live` (`CA/Gc/CasOrphanManifestSweep.cpp:176`, `:576`; `CA/Gc/CasGc.cpp:1721`, `:1746`, `:3999`) | nothing |
| Re-creation of the name | needs the erase first; mints a new incarnation (`C:328-329`) | nothing |
| Another server root | changes rows of its own prefix only (inferred from the single callers above) | nothing |
| `Move`; `RENAME TABLE`; `cancelStalledCreating`, `completeCreation`, `reconcileStaleCreator` | `Move` writes no catalog; rename drops the source through `dropNamespace` (`CA/ContentAddressedTransaction.cpp:1337`); the last three touch only `Creating` rows (`C:586`, `:621`, `:739`) | nothing |
| Superseded writer whose clock undercounts a pause | its successor may publish `Removing` | one early detector less; accepted as row 1 of section 8 of the one-request design |

## 3. A runtime for a `Removing` row refuses writes {#removing-runtime}

**Rule.** `acquireRefTableRuntime` (`L:686-762`) alone constructs a runtime (`L:727`). The rule applies there, where
a runtime is constructed or the resident one is returned. It gets one parameter: the `NsState` its caller read for
this exact life. For `Removing`, inside its `ref_queue_mutex` section (`L:699-736`), it sets two fields on the runtime
it returns, new or resident: `removal_admission_closed` and a new latch, `removing_observed`. Nothing clears the latch.
`Live` changes neither field. No caller passes `Creating` (paths 1 to 5 below); the rule `chassert`s that. A release
build handles `Creating` as `Live`: an open lane on such a row is today's behaviour, and a latched lane would refuse
the writes of a new table until a remount.

**A complete catalog cut applies the same rule.** `reconcileCatalogCut` closes and latches a resident runtime whose
exact life (namespace and incarnation) reads `Removing` in the cut. It has two callers: every GC round, which runs
only on the GC leader (`Pool::reconcileRefCatalogCut`), and the reconcile inside `namespaceLife`. A resident runtime of
another life of the same name is left alone.

**A removal is bound to the runtime it closed.** After its drain, `dropNamespaceImpl` sends every request under the
fence generation of that runtime, with the liveness check `commitRefChunk` uses. A `DROP` that spans a remount fails
with `NETWORK_ERROR` before it sends `beginRemoving`; the row stays `Live` and a retry starts on the new runtime.

**The reopen honours the latch.** A failed drop reopens the lane from a catalog read it took before it locks (`L:5719`,
lock at `L:5732`, clear at `L:5735`); another caller can read `Removing` and close the lane in between. The guard at
`L:5733` gets one more term, `!rt->removing_observed`. So no runtime that the rule closed has an open lane again.

| # | Path that reaches the constructor | Catalog state known there | Lane after this change |
|---|---|---|---|
| 1 | Cold read admission, `acquireReadableRefTableRuntime` (`L:826`) | `Live`, read twice (`L:796`, `L:811-824`); any other row gets no runtime | open |
| 2 | Cold write admission, `namespaceLife` (`L:5397`) | `Live`: the row read (`L:5383-5393`) or `resolveNamespaceLife`, which returns only a `Live` row (`L:1329-1339`); `Removing` is refused before (`L:5387-5390`) | open |
| 3 | `acquireMutableRefTableRuntime` (`L:841`), when the runtime `namespaceLife` used was evicted in between | none of its own: the life came from path 2 or from a resident runtime with an open lane (`L:5344-5345`) | open; passes `Live` |
| 4 | Presence probe, `namespaceStillLogicallyPresent` (`L:5504`) | `Removing` (`L:5493-5503`) | closed and latched; today open |
| 5 | `dropNamespaceImpl` (`L:5602`) | `Live` or `Removing` (`L:5558-5600`) | `Live`: open until `L:5634`. `Removing`: closed and latched; today open from `L:5602` to `L:5634` |

- Three readers can see `Removing` without reaching either rule, and change no resident runtime. All three are unchanged:
  - cold read admission returns no runtime for a `Removing` row (`L:796-797`);
  - cold write admission refuses it, "is Removing" (`L:5387-5390`);
  - the recovery confirmation (`L:1696-1705`) runs after every walk and uses only the row's presence: a `Removing`
    row keeps the runtime as it is. Closing there instead is rejected: `namespaceLife` reads the flag before it waits
    for the recovery (`L:5301-5345`).
- So a runtime can have an open lane while its row is `Removing`: it was admitted as `Live` and the row turned later.
  An own drop closes that runtime before it sends the write (fact 2 of [§2](#safety)). On the GC leader the next GC
  round closes it. What remains is [§4](#residual).
- An item queued before the lane closes still commits; a drop drains it (`L:5635-5638`), then removes it (`L:5789-5802`).

What the closed lane does, with existing code:

- **Writes fail with `NETWORK_ERROR`** from `throwCasWriteRetryLater` (`CA/Backend/CasRequests.cpp:93-97`), the code
  and retry class of the removed refusal ("exact catalog life is no longer Live", `L:3848-3850`).
- **A latched runtime has its own text**, on the warm and the cold branch of `namespaceLife` and for a caller that
  holds the runtime: "CAS namespace '…' is Removing in the catalog: writes are refused and reads see no data, and its
  removal has not finished; see the CAS troubleshooting page". It names no command: what finishes a removal depends on
  the operation that started it. A lane closed by a removal in progress in this server keeps the texts "creation
  waits for its terminal fold …" and "positive ref mutation admission is closed …".
- **The latch resolves an ambiguous `beginRemoving`.** It clears `removal_write_may_land`: the row was read
  `Removing`, so the write landed.
- **One warning per closed lane.** When the latch closes a lane that was open, the server logs "its catalog row is
  Removing, so this server closes it: writes are refused and reads see no data". A runtime whose lane a drop already
  closed logs nothing, so an ordinary `DROP` is silent. A runtime built for a row that is already `Removing` logs it
  too: the line alone does not mean a stuck removal.
- **Reads see no files.** Warm read admission returns no runtime (`L:777-778`), as cold read admission does for a
  `Removing` row (`L:796-797`). Kept: the removed read set this same flag, so it stopped reads too, and a "writes only"
  flag is new state. Today a probe-created runtime serves reads that a cold read refuses; that goes.
- **The terminal append still passes** (`L:2125`, `L:2164`) and requires the flag (`L:3795`). A retried `DROP` is the exit.
- **The probe still answers `true`** (`L:5518-5519`). Its resident fast path (`L:5448-5455`) no longer matches, so each
  probe of a `Removing` namespace without a terminal record costs one catalog `GET`.
- **The row never returns to `Live`**: `Live` is written only over a `Creating` row (`C:621-649`); a re-created name is a new life (`L:717-723`).

## 4. Residual: a late `beginRemoving` after a remount {#residual}

Accepted. No catalog revision and no binding of `Live -> Removing` to the mount generation are added.

1. A removal sends the conditional catalog write of `beginRemoving`; no answer arrives. The removal fails
   (`L:5750-5759`). Four operations remove a namespace: `DROP TABLE`, the source side of a table rename that moves
   the data, `SYSTEM UNFREEZE` and `SYSTEM CAS DROP POOL MEMBER`.
2. The removal is not retried to completion. The disk remounts; the runtimes are discarded (`L:1901-1916`).
3. A writer opens the table. Cold admission reads `Live`; the lane is open.
4. The request of step 1 lands: the row is `Removing`. It lands only while the catalog still carries the token of
   step 1: no other catalog write in the pool since, or a store whose token repeats when the bytes repeat.
5. Before this change: the next positive append reads `Removing`, closes the lane and fails; reads see no files.
   After: appends are acknowledged and reads work. GC folds the appends (fact 4). No terminal record exists, so no
   evidence, no erase, no janitor delete. No acknowledged append is deleted.
6. The stop comes when the server reads the row. Reliable: a restart, a remount, a retried `DROP` of the table, and
   on the GC leader its next GC round. A server that is not the GC leader keeps serving the table until one of the
   first three. From then on writes fail with "is Removing in the catalog" and reads see no files. The data stays in
   the store.

The late request is the only way in. A removal that itself spans a remount sends nothing ([§3](#removing-runtime)).

- **Not reliable stops.** An eviction needs an idle runtime and a full cache, and a writer between `namespaceLife` and
  its slot lookup (`L:835-841`) rebuilds an open runtime with no catalog read. A probe answers from its fast path.
- **What the operator sees.** After `gc_stuck_removal_rounds` (10, `CA/Pool/CasPool.h:166`) rounds, every GC round logs
  "CAS GC namespace removal is stuck: namespace='…', life_id=…, …; cleanup evidence is absent because terminal has not
  folded" (`CA/Gc/CasGc.cpp:217-242`) and increments `CASGCStuckRemovals` (`:1598-1610`).
- **What the operator does.** No tool returns a row to `Live`. What finishes the removal depends on the operation
  that started it (`docs/en/antalya/cas/operations/troubleshooting.md`):

| Operation | What finishes the removal |
|---|---|
| `DROP TABLE`, `Atomic` database | the server retries the drop itself |
| `DROP TABLE`, `Ordinary` database | the failed drop re-attaches the table; `DROP TABLE` again |
| `SYSTEM CAS DROP POOL MEMBER` | a rerun |
| Source side of a table rename | no statement: the parts already moved to the destination namespace, which is `Live`; a `DROP` of the old name or a second rename would strand them |
| `SYSTEM UNFREEZE` | a rerun, only while another namespace under the backup name still has a part |

## 5. `promote` without the manifest re-read {#promote}

**Today** (`W:869-881`): `promote` reads the body, fails retry-later if it is absent, decodes it (`W:877`), checks
`refMatchesBody` and `manifestNamespaceMatches`, and copies the body into the append closure. The closure uses only
the `BlobRef` of each `Blob` entry, for the dependency proofs (`W:965-1006`).

**Change 1, validation at staging.** After its size caps (`W:642-683`) and before the `PUT` (`W:696`), `stageManifest`
runs the readers' own expression on the bytes it is about to write: `decodePartManifest(openObject(FormatId::PartManifest, encoded))`.
A failure propagates with the decoder's code, `CORRUPTED_DATA`: the code `promote` raises today for the same body and
every reader raises (`CA/Pool/CasManifestReader.cpp:75`). Nothing is written, no id is returned.

- **Why.** The encoder accepts what the decoder rejects (`F:90-136` against `F:139-307`): an entry path that is
  absolute or has an empty, `.` or `..` segment (`F:212-222`), and a line over the 64 KiB cap (`F:186`,
  `CA/Formats/CasTextFormat.cpp:373`, `CA/Formats/CasFormat.cpp:155`). A path reaches the entry unchanged from the disk
  transaction (`CA/Parts/PartPathParser.cpp:256-270`, `CA/ContentAddressedTransaction.cpp:1004`). Without the re-read
  such a body would be committed and every later reader would reject it.
- **Why decode, not validators shared by encoder and decoder.** One call covers every check the decoder has, now and
  later; a shared validator covers the checks someone listed. Repoint already round-trips a body (`CA/Parts/PartFolderAccess.cpp:693`).
- **Cost.** For a promoted manifest, the decode `promote` does today, moved to staging. New: one decode of a manifest
  that is never promoted, and a higher staging peak (risk 5 of [§9](#risks)). A bad body now fails before the blob uploads.

**Change 2, leaves.** `stageManifest` records, per minted id, the `BlobRef` of each `Blob` entry of the body it built,
in entry order. `staged_manifest_ids` (`W:743`) becomes that map: one container answers "staged here" and "which
leaves". `promote` looks the id up before `commit_state = Uncertain` (`W:904`); the closure reads the leaves through
`this`. No path reads the manifest. The closure after the change, in order:

1. `created` (`W:916`).
2. The ref already commits `id.ref`: no-op (`W:923-925`).
3. New: the lookup found no entry. `LOGICAL_ERROR` "promote: manifest '{}' was not staged by this transaction and is
   not the current committed manifest of ref '{}'", arguments as in `precommitAdd` (`W:817-824`), whose rule and
   place this is. It precedes step 4, so a foreign uncommitted id never gets a refusal of step 4 instead.
4. `policy.expected` (`W:927-934`), then the live precommit (`W:948-953`, `NETWORK_ERROR`).
5. The leaf loop (`W:965-1006`), over the recorded leaves.
6. The different-manifest refusal (`W:1017-1025`), then the ops.

| Check today | Why `promote` no longer needs it for a manifest this transaction staged |
|---|---|
| Body present | `stageManifest` returns an id only after its create committed or met identical bytes; a different occupant, `Refused` and `GaveUp` throw (`W:696-730`). The body of a live precommit is deleted neither by the writer (`W:1267-1272`) nor by the orphan sweep (`CA/Gc/CasOrphanManifestSweep.cpp:299-301`); the closure re-proves that precommit (`W:948`) |
| Body decodes | change 1 decoded the same bytes |
| `refMatchesBody`, `manifestNamespaceMatches` | the body is built from the id's own `ref` and namespace (`W:672-686`), and `promote` already requires `id.root_namespace == target_ns` (`W:864-867`) |
| Blob leaves of the durable body | the durable bytes are the encoding of the entries the leaves are taken from |

- **Callers.** One production caller, `promoteBuild` (`CA/Parts/PartFolderAccess.cpp:317`), reached from
  `CA/ContentAddressedTransaction.cpp:485` (staged at `:470`) and from `PreparedPartWrite::promote`
  (`CA/Parts/PartFolderAccess.cpp:413`), which only `prepareEntries` constructs, after its own `stageManifest` (`:460-462`).
  `prepareEntries` serves `publishEntries` (`:478`) and relink (`CA/ContentAddressedMetadataStorage.cpp:2467`, `:2335`).
- **No caller promotes a manifest staged by another object or process.** A `PartWriteTxn` is never persisted and
  never resumed after a restart (`CA/Pool/CasPartWriteTxn.h:246-259`). So no case keeps the read.
- **Memory.** One `BlobRef` per blob entry (at most 1,048,576, `W:61`) until the transaction is destroyed. The body is still not retained.
- **Commit-recorded region.** Nothing after a durable commit may throw before the caller has recorded it
  (`CA/Parts/PartFolderAccess.cpp:305-325`), and a throw before `W:904` proves that nothing was committed. Validation
  runs before any durable object; the lookup sits before `W:904`; nothing is added after `appendRefOps`.

## 6. Tests {#tests}

Written first. A `LOGICAL_ERROR` expectation is a death test. New names are proposals. "From outside" means
`CasRefCatalog::casUpdate`, as `:4936`: the stand-in for the late request of [§4](#residual). Every wait for a parked
thread, a hook signal or a refusal is bounded (as `:4930`). A scope guard releases each parked thread before a join.

| Change | Test (`T/gtest_cas_ref_writer.cpp` unless named) | Asserts |
|---|---|---|
| Closed runtime | new `CASRefWriterNamespaceRemoval.RuntimeAdmittedForARemovingRowRefusesWrites` | a ref `owned` is committed; then the row is set to `Removing` from outside and the disk remounts (as `:5997`); the probe answers `true`; then a write fails `NETWORK_ERROR` "is Removing" with no `_log` create, `resolveRef` of `owned` finds nothing (today the probe's runtime serves it), and `dropNamespace` completes |
| | new `.ResumedDropOverARemovingRowAdmitsNoWriter` | same row and remount, then no probe and no read: the drop's own acquisition creates the runtime. `recovery_pre_first_request_hook_for_test` parks the drop's thread only, once. A writer on its own thread, started after the park, is refused "is Removing" within a bounded wait. A scope guard releases the drop. Red if `L:5602` passes `Live` |
| | new `.RemovingObservationWinsOverAnEarlierLiveReopenRead` | a drop whose post-close read fails (as `:5213`); a new test hook between its fresh read (`L:5719`) and its lock (`L:5732`) sets the row `Removing` from outside and runs the probe; after the drop fails, a write is refused "is Removing". Red without the latch term, and red if the rule skips a resident runtime: the only test that discriminates the resident branch |
| Read removal | flip `CASRefWriterAppendLane.PositiveAppendIsOneCatalogReadAndOneCreate` (`:1946`) to `.PositiveAppendIsOneCreate` | the journal holds one entry, the `_log` `PUT`; catalog `GET` count unchanged |
| | flip `.PartPublishCostsOneCatalogReadPerLogCreateAndNoCkptRequest` (`:1992`) | two log creates, zero catalog `GET` |
| | rewrite `CASRefWriterNamespaceRemoval.CachedPositiveWriterCannotAppendAfterRemovingIsPublished` (`:4882`) as `.RowTurnedRemovingUnderAnOpenLaneKeepsItsAppends` | the row turns `Removing` from outside under a parked leader; the append commits; after more than 10 GC rounds and a janitor pass the row, `_ckpt`, every log and the manifest exist and `CASGCStuckRemovals` grew; after a remount a write fails "is Removing" |
| | new `.QueuedAppendDrainsBeforeTheTerminalOfADropThatObservedRemoving` | a part's precommit is durable; its `promote` append is the leader, parked before the carve (`ref_pre_carve_hook_for_test`, `L:3298`) with its item queued; the row turns `Removing` from outside; a drop starts on its own thread. Barrier: a new one-shot hook in `dropNamespaceImpl`, right after `acquireRefTableRuntime` returns (`L:5602`), with no lock held; it only signals. By then the rule has closed the lane (path 5 of [§3](#removing-runtime)); `removal_attempt_hook_for_test` (`L:5641`) fires only after the drain. After the signal a new write is refused "is Removing" and no terminal record exists. Released, the `promote` commits; then the terminal record follows it and removes its ref. Pins: an item queued before the close commits, and the terminal record comes after it; red while the per-append read exists. It does not discriminate the resident branch: without it the drop still closes the lane at `L:5634`, before its drain wait |
| | new `.OwnDropRefusesAWriterThatHoldsTheRuntimeWithoutACatalogRead` | a writer parked after it captured the runtime (`append_after_runtime_capture_hook_for_test`), then a complete drop; released, it fails "is Removing"; zero catalog `GET` and no `_log` create in between |
| Promote | new `CASPartWriteTxn.PromoteSendsNoManifestGet` (`T/gtest_cas_part_write.cpp`) | `getCount` of the manifest key is 0 after stage, precommit, promote |
| | new `.StageManifestRefusesAnEntryPathWithDotDot`, `.StageManifestRefusesARecordLineOverTheLineCap` | `stageManifest` throws `CORRUPTED_DATA`; no object is created; no id exists, so `promote` is not reachable |
| | rewrite `.ManifestCapEncodedBytesJustUnderStagesSuccessfully` (`T/gtest_cas_part_write.cpp:2314`) | today it stages one entry with a path of about 256 MiB: one line far over 64 KiB, a body no reader decodes. The rewrite reaches the size with many entries whose lines stay under the cap. `.ManifestCapEncodedBytesOverThrowsBeforeBodyWrite` (`:2345`) is unchanged: the size cap fires first |
| | new `.PromoteOfAManifestStagedElsewhereIsRefused`, death twin | transaction A stages and precommits a body with no `Blob` entry (`stageManifest({})`); transaction B, which staged nothing, promotes A's id: `LOGICAL_ERROR` with the text "was not staged by this transaction" (step 3 of [§5](#promote)); nothing is appended. Red without step 3: the precommit is live and the leaf loop has no leaf, so the `promote` commits. With a `Blob` entry the test is green today on "no dependency proof" (`W:969-974`, pinned by `.MissingDependencyProofFailsClosed`, `T/gtest_cas_part_write.cpp:1332`) |
| | keep `CASPartWriteTxnStageManifestRetry.BudgetExhaustionMapsToNetworkError` (`:2584`), `CASPartWriteTxn.StageManifestSentThenGaveUpSaysUncertain` (`:3228`); new `CASPartWriteTxnStageManifestRetry.RefusedPutReturnsNoId` | a staging `PUT` that gives up or is refused throws from `stageManifest`: no id exists to promote |
| Stateless | `tests/queries/0_stateless/05485_cas_ref_append_requests` | reference `0 0 3 3` becomes `0 0 3 0` on both lines (`CASOtherGet` counts the catalog reads; expected, to confirm by a run). Story 2 adds one `SELECT` of `CASManifestGet` for the `INSERT` only, expected 0. The `OPTIMIZE` value is not derived here: a merge reads its source manifests; it is added only after it is measured |

- Kept: `.PresenceProbeStaysTrueThroughRemovingUntilTerminalRetrySucceeds` (`:6076`), `CASRefWriterRecovery.ConfirmationCostsOneCatalogReadPerInstall`
  (`:1929`, still 3), `CASPromoteRepublish.PromoteSameManifestIsIdempotent` (`T/gtest_cas_promote_republish.cpp:121`: a committed foreign id).
- Docs, by a search of `docs/en/antalya/cas` for "catalog", the request numbers and `promote`:
  `architecture/manifests-and-refs.md:274-288` (the paragraph and three table rows: 2 to 1, 8 to 4);
  `operations/migration.md:194` ("2 requests instead of 4"); `architecture/part-lifecycle.md:56` (the `GET` arrow) and
  `:98` ("Reads and revalidates the precommit manifest body once"); `architecture/namespaces.md:139-141` ("later writes
  to the table fail": now at a restart or remount). `operations/troubleshooting.md` gains the warning of [§4](#residual).
- Code texts that story 2 makes false: the comment and `LOG_INFO` naming "a body-absent precommit" (`CA/ContentAddressedMetadataStorage.cpp:2355-2366`);
  the comment on the closure's capture of `body` (`W:906-913`); the contracts in `CA/Pool/CasPartWriteTxn.h:205-212`, `:230-279`.
- **No model covers [§2](#safety), and none is added.** `docs/superpowers/models/CaRefCatalogCore.tla` (`master`
  worktree) checks that an erase needs terminal evidence (`:640`) and lets writes run under `removing` (`:320-324`),
  but it keeps one bit per incarnation and cannot state "an acknowledged append stays reachable".

## 7. Compatibility {#compat}

- No format change, no generation change, no setting.
- Mixed versions: an older build still sends both reads. That is harmless: each server root appends only to its own namespaces.
- No `ProfileEvents` counter is specific to either read. `CASOtherGet` (the catalog key is in the `Other` class,
  `CA/Backend/CasInstrumentedBackend.cpp:119-136`) drops by one per positive append, `CASManifestGet` by one per
  `promote`, `CASRequestAttempt` by both. `CASRefQueueWaitMicroseconds` drops (the read: 0.37 ms per append, local stand).
- The refusal text "exact catalog life is no longer Live" disappears. A write to a `Removing` table says "is Removing".
- A `Removing` table without a terminal record is no longer readable after a presence probe, as after a cold read.
- A part whose manifest does not decode fails with the same `CORRUPTED_DATA` as today, at staging and no longer at `promote`.

## 8. Staging {#staging}

Each commit builds and passes alone. Gate for commits 1 to 3: the `CAS*` gtest suites and the CAS stateless tests.

| # | Commit | State after it |
|---|---|---|
| 1 | `acquireRefTableRuntime` takes the observed state, closes the lane and sets the latch for `Removing`; five call sites; the latch term in the reopen guard and its test hook; the three closed-runtime tests | no runtime admitted for a `Removing` row accepts writes; the per-append read still runs |
| 2 | Delete the `positive_append` block; flip, rewrite and add the read-removal tests, with the barrier hook of the drop; `05485` reference | a positive append is one request; [§4](#residual) is the only schedule that changes |
| 3 | `stageManifest` validates its bytes and records blob leaves; `promote` uses them; promote tests; `05485` `SELECT`; the code texts of [§6](#tests) | `promote` sends no `GET`; an undecodable body fails at staging |
| 4 | Docs of [§6](#tests) | docs match the code |

- Commit 1 precedes commit 2, else one commit leaves a `Removing` runtime writable. Commit 3 is independent. After it, measure the `INSERT` again (expected 19 and 14 requests).

## 9. Risks and limits {#risks}

| # | Risk | Precondition or bound |
|---|---|---|
| 1 | Acknowledged inserts land in a table whose row is `Removing`; the stop comes at a restart, a remount, a retried `DROP`, or the next GC round on the GC leader ([§4](#residual)). For a rename source and for `SYSTEM UNFREEZE` no statement finishes the removal | a `beginRemoving` request that the store applies after a whole remount, with the catalog token of its send still current |
| 2 | `promote` commits a ref whose manifest body is gone. The first reader gets `FILE_DOESNT_EXIST` and fsck reports it. GC's fold notices it only when it meets a manifest edge it has not folded yet (`CASGCFold.CommittedMissingBodyClampsCursorAndRecordsAnomaly`, `T/gtest_cas_gc_fold.cpp:161`); a `promote` adds no manifest edge, so a body deleted after its precommit was folded is not noticed by GC | the body was deleted outside the protocol, or by a defect. The window grows: today from the `promote` read to the append, after story 2 from `stageManifest` to `promote`, across the blob uploads |
| 3 | A new production caller of `beginRemoving`, of the erase, or of `CasRefCatalog::casUpdate` (`C:354`, tests only today) | invalidates fact 1 or 3 of [§2](#safety); the argument must be redone |
| 4 | A probe of a `Removing` namespace without a terminal record costs one catalog `GET` each time | how often `existsDirectory` runs on such a table is not measured |
| 5 | Staging decodes every manifest it writes | for a promoted manifest, the decode `promote` does today. New: one decode of repoint's scratch manifest, staged and never promoted (`CA/ContentAddressedTransaction.cpp:416`). New: the staging peak. Today staging holds the body, its text (at most 256 MiB, `W:62`) and the sealed bytes (`W:673-684`); validation adds the opened text (at most 256 MiB) and the decoded body while those are held, so at the cap the peak about doubles (from the sizes; not measured). An ordinary part is far from the cap: the manifest of the measured `INSERT` is a 4331-byte `PUT` (#1 of window A) |

- Not established: any request count after the change; whether other tests depend on the removed reads through a request ordinal or a fault
  counter; how long a sent `beginRemoving` can stay in flight; whether `MergeTree` ever writes a part file under a path the decoder rejects.
