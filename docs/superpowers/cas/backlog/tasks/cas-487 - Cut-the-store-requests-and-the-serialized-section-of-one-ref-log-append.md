---
id: CAS-487
title: Cut the store requests and the serialized section of one ref-log append
status: To Do
assignee: []
created_date: '2026-11-14 10:18'
updated_date: '2026-11-21 08:23'
labels:
  - cas
  - performance
  - ref-ledger
  - 'area:ref-ledger'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'impact:cost'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: task
ordinal: 592000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A new part costs three ref-log appends on 26.8 (precommit, promote, `Move`) against two on 26.6. Each append is four store requests run one after another inside the namespace's ref lane: GET ref catalog, PUT `_log/<txn>`, GET `_ckpt`, PUT `_ckpt`. The number of appends stays: folding promote and `Move` was studied and not built (docs/superpowers/reports/2026-11-14-cas-promote-move-one-append-feasibility.md, CAS-485). This task makes one append cheaper and its serialized section shorter.

Measured (local A/B, release, RustFS; `master/tmp/lanes/ci/perf_26_6_vs_26_8/ab/RESULTS.md`):
- INSERT 21 -> 25 store requests, wall 11.2 -> 15.0 ms; second INSERT x1.50; OPTIMIZE x1.16.
- Of about 6 ms added per INSERT, 1.9 ms is store time. The rest is wait in the ref batch queue: 8.5 -> 15.5 ms.
- So the lane's serialized section, not the request count alone, sets the price.

Counted from the code, not measured (one writer, warm namespace, without blobs):
- 26.8 today: 1 manifest PUT + 4 + 1 manifest GET + 4 + 4 = 14 requests per part.
- Without the catalog GET (CAS-176.1) and the `_ckpt` GET (CAS-176.2): 2 per append, 8 per part. That is below the 10 of 26.6.
- Without the promote's re-read of the manifest this build staged (`CasPartWriteTxn.cpp:873` against `:696`): 7. The comment at `:869-870` calls the read a deliberate fail-closed check; decide before dropping.

Where the time is serialized:
- Ref lane: one flush per namespace at a time; a flush holds the lane for all four requests (`CasRefLedger.cpp:3550`, `:3982`, `:4217`). Appends of different parts share a chunk, so cost amortises under concurrency and is worst for one writer.
- `DataPartsLock`: on a plain `MergeTree` INSERT and a plain mutation the rename runs under the lock (`MergeTreeSink.cpp:418`, `:449`; `MutatePlainMergeTreeTask.cpp:133-134`), so the `Move` append and its four requests run under it. `PartsLockHoldMicroseconds` was 317 on local object storage (CAS-346); not measured on S3.
- Each `_log` object also costs GC one GET in the fold, one delete and one tick toward the snapshot threshold.

Existing pieces this task ties together: CAS-176.1 (catalog GET), CAS-176.2 (optimistic `_ckpt` from a remembered body and token), CAS-287 (combine window on the lane), CAS-78 (state copy per queued item), CAS-473 (cold admission guard), CAS-167 and CAS-71.5 (`_ckpt` coalescing and the hot-key lane).

Constraints: no on-store format change; no change to the upstream writer order; the `_ckpt` frontier still admits a transaction before any waiter wakes (variant Y of the report acknowledges above the frontier and is rejected).

Update 2026-11-21: Part A is merged (0aa837e942e): an ordinary append is one create of `_log/{epoch,seq}`, `_ckpt` is written only at creation, snapshot publication and the generation 3 stamp. Remaining scope: AC #1 per-append timeline on S3 (queue wait, request time, DataPartsLock time), AC #3 decision on the promote manifest re-read, AC #4 A/B against 26.6. AC #2 is superseded by the delivered append (catalog GET: CAS-488). The 4-request description above is stale.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Per-append timeline measured on S3 for a single-writer INSERT: time in queue, time per request, time under `DataPartsLock`
- [ ] #2 CAS-176.1 and CAS-176.2 landed or rejected with a measured reason; requests per part re-counted by `05396`-style oracles
- [ ] #3 Decision recorded on the promote's manifest re-read
- [ ] #4 A/B against 26.6 repeated: INSERT wall and ref batch queue wait reported
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-14, study of the target 'one store request per ordinary append' (`_ckpt` written only at seal and snapshot). Two independent codex consults (gpt-6-astra, gpt-6.1-sol), same prompt, agree. Raw reports, prompt and the recovered 2026-07-27 spec: docs/superpowers/reports/2026-11-14-cas-one-request-ref-append-consults/.

History: `_ckpt` was born 2026-07-28 (`eeadbc1887e`) with `life_epoch`, `checkpoint_snapshot_id`, `last_epoch_seal` and two writers, the snapshot publisher and the sealer. `committed_through` and the write per append came 2026-08-02 (`4f06f419638`, `357cf7b963f`, generation 8 -> 9) to make recovery and GC independent of `LIST`. The price, about two serial requests per chunk, was accepted in writing.

What the per-append write carries today:
1. Exact end of logical history for every reader that is not the writer: read-only recovery, GC fold, orphan sweep, REBUILD, fsck stop at it (`CasRefProtocol.cpp:1058`, `CasGc.cpp:2872`).
2. Frozen work per GC round. A serial chase under an active writer gave zero completed rounds in 42 minutes (`CasGc.cpp:2439`).
3. Missing-log witness: a 404 at or below the frontier is corruption and holds destruction (`CasRefProtocol.cpp:1068`, `CasGc.cpp:2904`).
4. Store-side fence on the acknowledgement: a superseded writer whose create re-lands in a reclaimed seal slot gets `SuccessorSeal` or `Superseded` and does not acknowledge, even when its local lease check passes (`CasRefCkpt.cpp:125`, `:276`, `3e3b94522d8`, CAS-366).

Verdict: one request per append is not reachable as an optimisation. It is a protocol change with five separate prices:
- Old binaries on the same pool stop at a frontier that no longer advances; an old GC can certify a round complete and delete data owned by acknowledged logs above it. Needs a one-way reader-floor raise and exclusion of running old processes.
- Fence replacement: keep the closing seal of every dead epoch forever (model B2, one object per table per remount, `CaRefTableSnapshotLogCore_RESULTS.md:233`), or rely on the post-response lease recheck that already exists (`CasRequests.cpp:868`, `CasRefLedger.cpp:4261`) and accept dependence on the lease clock.
- GC and read-only readers must chase to a fresh exact 404, with parallel windows and a budget; +1 GET per quiet namespace per round.
- Missing-log detection is lost above the last snapshot or seal. The tail is not hard-bounded by 256: snapshot publication is asynchronous with backoff.
- The catalog GET is a separate question (below).
Estimate from both consults: 800-1800 product lines, 1500-3500 test lines, plus model work. Not planned.

Rejected: issuing the `_log` PUT and the `_ckpt` PUT concurrently. A reader can see a frontier whose log does not exist; if the log never lands the contradiction is permanent.

Compatible ladder (no change of meaning on the store):
- Step 1, CAS-176.2: `_ckpt` body and token from memory, conditional PUT, re-read on conflict and before a cached no-op or refusal. Reuse `CasHotKeys` (`CasHotKeys.cpp:197`). Positive append 4 -> 3, removal 3 -> 2. 150-350 product lines.
- Step 2, CAS-176.1: drop the catalog GET. Positive append 3 -> 2; a part 14 -> 8. Open point found by both consults: a late `Live -> Removing` write of the previous epoch can land after the successor recovered and does not touch `_ckpt`. That is R4 of the lease inventory, CAS-454. The GET does not close it fully today either (GET-to-PUT race). Settle CAS-454 first or accept in writing.
- After step 2 an append is two serial PUTs. That is the floor without a protocol change.

Not studied (controller's idea, no review): one conditional PUT on a head object that inlines the unsnapshotted tail. It would keep the fence, the exact frontier and the missing-log witness at one request, and needs the same reader-floor raise; the 2026-08-02 decision rejected a segment+head design for object count.

Owner decision 2026-11-14 (second): a protocol change is acceptable for the one-request append, because 26.8 already changes the protocol. The earlier recommendation 'not now, frozen format' is withdrawn.

Verified in cas-pr1-clean: G_BUILD = 2 (CA/Formats/CasFormat.h:16), 1 on altinity/antalya-26.6. The first writable mount raises min_reader_generation 1 -> 2, one-way (CA/Pool/CasPool.cpp:598); docs/en/antalya/cas/configuration.md:346-352 describes it. No altinityantalya 26.8 release tag exists, so the change can still ship with the first 26.8 release. An already mounted 26.6 GC records an undecodable _ckpt in a per-namespace 'undecodable' map (26.6 CA/Gc/CasGc.cpp:1751) and does not fail the round; whether that hold suppresses every deletion that depends on the namespace is NOT established. decodeRefCkpt also honours '!'-prefixed fields as fail-closed.

Shapes under study, second consult running (prompt tmp/ckpt-study/prompt2.txt, outputs sol2.out and astra2.out): A = log-only append, _ckpt written only by the sealer and the snapshot publisher, readers chase to the first exact 404; fence either A1 (cleanup never deletes the closing seal of a dead epoch) or A2 (existing post-response lease recheck). E = one conditional PUT on _ckpt carrying the records since the last materialized log, materialized every K records or on idle; keeps the fence, the exact frontier and the missing-log witness. B = head object with the whole tail inline, no _log objects.

Reasoning so far (controller, unreviewed): S3 offers only create-if-absent and if-match, so a store-side fence on one request needs either an occupied key (A1 retention) or a rewritten key (E, B). The 42-minute GC stall came from a serial walker; appends are serial in one lane, so a walker with parallel windows outruns the writer.

Second consult 2026-11-14, premise 'protocol change allowed' (outputs: docs/superpowers/reports/2026-11-14-cas-one-request-ref-append-consults/*-round2.md). Both models agree on the facts below; neither ran a test or a model.

Shapes: A = log-only append, readers chase to a fresh exact 404 (A1 with retained closing seals, A2 with the lease recheck only); E = one conditional PUT on _ckpt with the records inline, materialized later; B = head object with the whole tail.

Facts both read:
- Already mounted 26.6 GC: an undecodable _ckpt closes the destructive gate pool-wide, other namespaces still fold (26.6 CA/Gc/CasGc.cpp:1751, :2788, :3508). So a stamped _ckpt is fail-closed for it.
- Generation 2 alone does not exclude 26.8 builds without the change. Needs generation 3 or a '!'-prefixed field in _ckpt. The _ckpt encoder stamps G_WRITE_FLOOR = 1 today (CA/Formats/CasRefCkptFormat.cpp:100). The stamp must be an explicit precondition of the first new-semantics append: recovery can skip the _ckpt write (IdenticalSkip, CA/Pool/CasRefCkpt.cpp:309; no seal for terminal or never-born streams, CasRefLedger.cpp:1236). A marker does not revoke a warm cached reader or an already planned old cleanup.
- Quiet namespace in GC today: one _ckpt GET and zero successor GETs (CasGc.cpp:2872, gtest_cas_gc_bounded_walk.cpp:315). The comment near CasGc.cpp:2460 that says the expected-next key is read is stale. Shape A adds one GET per quiet namespace per round.
- A1 retention: one closing-seal object per closed epoch. planRefCleanup (CA/Pool/CasRefProtocol.cpp:814, :827) and NamespaceJanitor (CA/Gc/CasNamespaceJanitor.cpp:140) both delete seals today and get ids, not record kinds. Today's _ckpt fence also ends when the janitor reclaims a dead life (CasNamespaceJanitor.cpp:121, :152), so retention scoped to the life equals today's guarantee; retention across reclaimed lives would be stronger than today.
- A2 assumption: CLOCK_BOOTTIME advances across the pause. The same assumption already carries namespace-file writes, mountpoint writes and relink confirmation (CA/Backend/CasRequests.cpp:868, CA/Pool/CasPlainObjects.cpp:18).
- Chase: a fresh exact 404 on the validated chain is a valid frozen cut for the destructive gate; a speculative 404 past a seal is not. No unconditional bound against the writer: budget exhaustion must leave the round unproven.
- E: cas_ref_ckpt caps are 64 KiB per object and 4 KiB per line (CA/Formats/CasFormat.cpp:131, :136); one ref transaction can be 20 MiB. GC must fold inline records directly: deferring them loses a blob on a quiet namespace in three rounds (CasBlobInDegree.cpp:545, :559, :576). Several chunks in one log object break the one-id-one-object arithmetic (CasRefProtocol.cpp:941, :961).
- Catalog GET: the only Live -> Removing writer is beginRemoving via dropNamespaceImpl (CasRefCatalog.cpp:413, CasRefLedger.cpp:5572). The open case is the predecessor's delayed DROP write. Two candidate replacements: commit the terminal RemoveNamespace record before the catalog transition, or an epoch-qualified claim in the Live row at takeover.

Model estimates (not measured): A2 800-1600 product lines, A1 900-2000, E 1500-3000, B 1200-4000. Both models rank E first on properties kept.

Controller's reading: both models underweight CAS-167. _ckpt is the hot key that draws 429 on GCS (175 and 161 per node in 8 minutes) and 409 on AWS, and is 21% of all PUTs on otel.demo. E and B keep one _ckpt write per append; A removes it and subsumes CAS-167. Recommendation to the owner: shape A, fence = closing seal retained for the life of the namespace, generation 3, terminal-record-before-catalog for the GET. Owner decision pending on the fence (retain seals or lease recheck only).

Owner design input 2026-11-14 (third): no permanent seal retention. A late write of a superseded writer is covered by the mount lease (the fence is the existing post-response recheck). The epoch can still be closed by an empty log / tombstone, which is today's EpochSeal; it is cleaned as today. Owner's model: ids are sequential inside an open epoch; at an epoch change the new writer knows the last log (wrote it, or read from the snapshot to the first 404) and always publishes a new snapshot; _ckpt points to the last epoch that has a write and its snapshot; a cold read takes the snapshot from _ckpt and reads logs sequentially to the first 404.

Controller notes: (1) the tombstone is needed independently of clocks: a PUT sent under a valid lease can land after the successor read its 404; the lease guards the acknowledgement, not the landing; GC that chases the old epoch would fold a record the successor's snapshot excludes. Create-if-absent in that slot makes the late record either adopted or refused (CasRefLedger.cpp:1269-1307). (2) Today's recovery seals every numeric epoch between the last written one and the live one (CasRefLedger.cpp:1236-1350, ++epoch), each with a _ckpt publish. (3) A mandatory snapshot at epoch change is new: today takeover writes only seals and _ckpt; GC still needs the deltas, so it must still cross epochs. To be priced in the spec as an option.

Owner requirement 2026-11-14 (fourth): an epoch without writes must leave no trace and cost nothing. Owner also confirmed: no mandatory snapshot at epoch change, keep crossing epochs through the tombstone.

Read in cas-pr1-clean: EpochSeal content is the namespace name, its own id {writer_epoch, ref_sequence}, exactly one op of kind EpochSeal with no payload, and prev_epoch_seal only at sequence 1 (makeEpochSealTxn, CA/Pool/CasRefLedger.cpp:153-175; CA/Formats/CasRefLogFormat.h:23-31). Its effect is positional. Today an empty epoch does leave a trace: (a) the grammar requires prev_epoch_seal to name the IMMEDIATELY preceding epoch (validateEpochSealGrammarStructural, CasRefLogFormat.h:176-186), so recovery walks every numeric epoch and creates a seal at {E,1} for each empty one, each followed by a _ckpt publish (CasRefLedger.cpp:1221-1350); (b) sealing happens in ensureRefTableRecovered, which read paths call too (resolveRef :315, listRefs :390, hasAnyRefWithPrefix :413), so a table that is only read after a restart still gets a seal; (c) the writer epoch is allocated per mount (CA/Pool/CasServerRoot.cpp:884-890).

Controller proposals, unreviewed: L1 = seal lazily, at the first append of the new epoch, not at recovery; the seal names the successor epoch (forward link) and the backlink may skip epochs; old seals without the forward link keep the numeric +1 walk. Cost: one small PUT per (namespace, epoch that writes). L2 = no separate seal: the first append of the new epoch is created in the old epoch's next slot and carries the epoch switch; zero extra requests, larger grammar change (seal is no longer a one-op transaction). Residual of lazy sealing: between recovery-for-read and the first write, a predecessor request that lands after the lease expired is history for GC and other readers but missing from the owner's in-memory state; covered only by the lease assumption.

Owner 2026-11-14 (fifth): L1 (lazy tombstone with a forward link) looks right; verify with codex and model in TLA+. Third consult running (tmp/ckpt-study/prompt3.txt, sol3.out, astra3.out).

TLA+ model written: docs/superpowers/models/CaRefLogOnlyAppendCore.tla, 13 configs, runner run_reflogonlyappend.sh. Bounds: 3 epochs, 3 slots per epoch, 3 appends. Run 2026-11-14 with the pinned TLC jar, all 13 rows as expected (tmp/ckpt-study/tlc-battery-2.txt):
- green: safe_lease (216058 distinct states), safe_lateland (2107062), safe_lateland_eager (3707068), safe_clocklies_nocleanup (4606454). Invariants: acked append reachable by a cold read, GC folds a prefix of history, every writer holds a prefix of history, an epoch that was not asked to write is named by no object; action property: history only grows.
- expected red, residuals: res_readwindow (INV_OWNER_EXACT: append sent by epoch 1, takeover, epoch 2 recovers for reading, the request lands; the owner's memory lacks a record every other reader sees); res_clocklies (INV_ACKED_REACHABLE: epoch 2 seals, snapshots, GC folds, cleanup deletes the seal, a superseded epoch 1 with an undercounting clock creates in the reclaimed slot and acknowledges). The same clock config with cleanup disabled is green, so the loss needs the reclaimed seal.
- sabotages red: sab_sealblind (seal overwrites an occupant: GC folded a record that stops being history), sab_sealblind_monotone, sab_sealskip (new epoch appends without closing the old one: acknowledged appends unreachable).
- witnesses reachable: a seal is written, a seal skips a numeric epoch, a late record is adopted, a seal is cleaned.
Abstractions: atomic chain read, no namespace birth or DROP, GC is one cursor, snapshot is a ghost copy.

Third consult 2026-11-14 on L1 (outputs: docs/superpowers/reports/2026-11-14-cas-one-request-ref-append-consults/*-round3.md). Both models agree; neither ran anything.

1. The sparse forward-linked chain is workable. Needed rules: transitions strictly increasing and reciprocal (the backlink of {W,1} names exactly the seal whose forward link selected W); an ambiguous seal never authorizes the first append; a deposed writer never turns a chain that reached V>W into a backward link; a terminal (RemoveNamespace) stream is an absorbing end.
2. The read window is NOT covered by the lease. With correct clocks: E sends a ref removal under a valid lease, the lease expires, W takes over and recovers for reading (slot absent), the removal lands, GC folds it and deletes the blob, W still resolves the ref (CasRefProtocol.cpp:237, :749; CasBlobInDegree.cpp:545, :559; CasRefLedger.cpp:339). No code imposes a landing deadline on a sent request (CasRequestBudget.h:19; CasPool.cpp:880-890 says the former materialization wait was replaced by recovery seals). astra: a stale confirmExactRef can then authorize a relink receiver's new ref to a deleted blob (CasRefLedger.cpp:509-573, CasPartWriteTxn.cpp:571, :980); end-to-end path not established. Today's eager seal closes it without a network assumption.
3. 'Eager seal only after an unclean stop' needs transitive ancestry: E unclean, R never reads the namespace and stops clean, W sees R's farewell. Clean farewell is detectable (UINT64_MAX watermark, CasServerRootFormats.h:83, CasServerRoot.cpp:1097; teardown drains lanes, CasPool.cpp:1126) but prior-unclean classification is local and only logged (CasPool.cpp:898).
4. _ckpt can stay untouched at an epoch change for an already-born life; the birth-time life_epoch publication must stay (creation in C, first birth in W>C: CasRefCatalog.cpp:631, CasRefLedger.cpp:3834). Snapshot publication must carry its own chain evidence; the codec and merge reject sparse epoch jumps today (CasRefCkptFormat.cpp:76, CasRefCkpt.cpp:39-50).
5. GC cursor on a seal whose successor was absent must resume at {W,1}, not {E,n+2}; the cursor does not store the record kind (CasGc.cpp:2795, :2851). After cleanup deletes that seal, the later snapshot's backlink chain can supply the witness; no permanent retention needed. Orphan sweep probes {E+1,1} (CasOrphanManifestSweep.cpp:274-320).
6. Reads mutate today beyond sealing: resolveRef/listRefs trigger stale-precommit maintenance and snapshot scheduling, which append (CasRefLedger.cpp:323, :392, :414, :5070).
7. W dying between its seal and {W,1} leaves a seal-only epoch W: an epoch that tried to write leaves one object.
8. Old readers: a seal-local gate does not protect a reader that stops at the old frontier before the seal; needs pool floor, _ckpt stamp, and a critical !next_epoch field with the new compatibility generation in the header.
9. L2 judged riskier than L1 by both.

TLA+ model extended (17 configs, all as expected, tmp/ckpt-study/tlc-battery-4.txt plus the sab_selective_immediate run; summary in docs/superpowers/models/CaRefLogOnlyAppendCore_RESULTS.md): added a tracked ref and blob with GC deletion (res_readwindow_gcdelete red = finding 2), clean and unclean takeovers, a selective rule 'seal at recovery iff the chain ends in an epoch <= lastUnclean' (safe_lateland_selective green, 8344780 states, with INV_OWNER_EXACT and INV_NO_DELETE_SERVABLE), and the immediate-predecessor sabotage (red = finding 3).

Controller recommendation, owner decision pending: L1 plus the selective eager seal with a durable monotone marker 'greatest epoch that ended without a clean farewell'.

2026-11-15: Part A is implemented on branch `work/antalya-26.8/cas-reflog-one-request-clean` in the worktree `cas-pr1`, tip `e80936fd437`, 13 commits over `802077e5c1a`. Not pushed, not merged. Unit gates on that tree: `CAS*` 3132 passed (plain), 3136 passed (ASan). Measured on a local RustFS stand (store requests, median): `INSERT` 25 -> 18 requests and 22.2 -> 12.8 ms; `OPTIMIZE FINAL` 41 -> 35; mutation 38 -> 32; `DROP TABLE SYNC` 33 -> 23; first `INSERT` after a restart 24 -> 19. Format generation 3: older builds refuse the pool after the first writable mount of the new build.

Not in this change (Part B): the catalog `GET` of a positive append stays, so a positive append is still 2 requests plus that read. Open at 2026-11-15: the integration rerun on the final binary and one red integration test. Both are tracked in the lane ledger `tmp/reflog1/LEDGER.md` of the `master` worktree until closed.

2026-11-15, update: the branch tip is 97123265e03 (14 commits over 802077e5c1a, not pushed). Unit gates on that tree: CAS* 3137 (plain), 3141 (ASan). It also contains CAS-489, CAS-494 and CAS-495. Open: a decision on ordering with PR 2542 (read-ahead bounded by the round's listing), which would remove the remaining read-ahead waste of the orphan sweep (about 8 reads per page per namespace).

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831: an ordinary append is one create of `_log/{epoch,seq}` under generation 3, `_ckpt` is written only at creation, by snapshot publication and by the generation 3 stamp. INSERT 25 to 14 requests, 22.2 to 11.6 ms (local RustFS, median of 25). Open: the per-append timeline with queue wait and `DataPartsLock` time, and the A/B against 26.6.

2026-11-17: Children after PR 2831: CAS-488 partly (one test criterion open), CAS-489 and CAS-494 done, CAS-495 partly; CAS-490 (rolling upgrade: an owner decision), CAS-491, CAS-492, CAS-493 open.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: Task notes: Part A merged 2026-11-17 as 0aa837e942e (PR 2831): an append is one create of _log/{epoch,seq}; INSERT 25 -> 14 requests. The description still describes the 4-request append. Changes: area set to ref-ledger; size unset -> medium (M): measurement on S3 and an A/B plus one decision; risk set to low; confidence set to solid; impact:cost; type unset -> task: remaining work is a measurement and a decision; description: Update paragraph appended. Size: M. Links: Children: CAS-488 (partly), CAS-489 and CAS-494 (done per notes), CAS-495, CAS-490, CAS-491, CAS-492, CAS-493. Would settle it: Which of AC #1-#4 remain: notes name the per-append timeline (queue wait, DataPartsLock time) and the A/B against 26.6 as open; the promote manifest re-read decision (AC #3) is not mentioned.
---
<!-- COMMENTS:END -->
