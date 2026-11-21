---
id: CAS-508
title: >-
  CAS copies: staged entries carry their provenance; the publish operation
  discharges the source proof
status: To Do
assignee: []
created_date: '2026-11-20 02:53'
updated_date: '2026-11-21 08:25'
labels:
  - cas
  - writer
  - design
  - 'area:write-path'
  - 'complexity:large'
  - groomed-20261121
  - grooming-20261121-changed
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - 'risk:high'
  - 'confidence:plausible'
dependencies: []
priority: medium
type: enhancement
ordinal: 613000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Goal: make it impossible, by construction, to commit a part that names a blob it "borrowed" from another part without the source having been confirmed, and remove the hand-synchronized bookkeeping in the metadata transaction that produced three defects in a week. Two refactoring steps, both on the WRITE path of the content-addressed disk. Deferred from PR 2917 by the owner's decision of 2026-11-20: do after that PR merges, as its own PR(s).

BACKGROUND (what a "borrow" is and why it needs a proof)
Some operations make a new part name blobs that already exist in the pool, without reading or uploading bytes: `createHardLink` inside a metadata transaction (mutations that hardlink unchanged columns, `ATTACH PARTITION FROM`, `REPLACE PARTITION`, clones, projections) and the rename of a part's ref into another table namespace (`republishRef`: `RENAME TABLE` in an Ordinary database, moves between tables). Replica fetch by relink is the third such path and confirms on the sender. The garbage collector (GC) counts references per blob from each table namespace's ref log and deletes a body at zero. A borrow is only safe if, at some moment AFTER the destination's precommit is durable, the source ref was still the committed current binding: then every GC cut that sees the source dropped also sees the destination. Without that confirmation a copy and a drop of its source inside one GC round lose data: defect CAS-504, reproduced; the unit matrix `CASInterleave` (`src/Disks/tests/gtest_cas_gc_round_interleave.cpp`) shows 168 cells red and 56 real losses with the confirmation disabled.

STRUCTURAL CAUSE (why this area kept producing findings)
`PartWriteTxn::adoptEvidence` (`CA/Pool/CasPartWriteTxn.cpp`) accepts a bare `ManifestEntry` and records trusted evidence; `promote` accepts that. Neither knows which source supplied the blob or whether anybody confirmed it. So the duty to confirm lives in every CALLER, and in the staging code of `CA/ContentAddressedTransaction.cpp`, which keeps three parallel structures per staged part and must update all of them in every operation: the vector of staged entries (`st.entries`), the pending uploads (`st.pending_blobs`), and the map from an entry's path to its copy source (`copy_sources`; called `borrowed_sources` before a rename), plus the set of removed committed paths (`content_removed`). CA/ = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
Defects that came from that hand synchronization, each fixed locally in PR 2917:
- `moveFile` moved the only pending-upload record of a blob to another part; a hardlink of that staged file left behind had no record, no source and no evidence; on a committed part the repoint promoted it before any upload (a part naming a body never uploaded).
- A transaction that hardlinks from a committed part and then renames that part kept the copy proof pointing at the old ref; the commit failed after the durable rename.
- A validation "every blob entry has an upload record or a source" scanned the pending list once per entry (quadratic).
- `unlinkFile` and `moveFile` dropped a staged OVERWRITE of a committed file without recording the path in `content_removed`; with "a part with nothing to publish publishes nothing" that kept the old file silently.
- The projection re-key (`moveDirectory` of `.tmp_proj` onto `.proj`) merged entries silently when rewritten through put / erase helpers.

ALREADY DONE in PR 2917 (do not redo)
- Step 1: `republishRef` (`CA/Parts/PartFolderAccess.cpp`) is a three-state machine (source resolved / prepared / destination committed) that never deletes a committed destination; it removes only its own exact precommit or the exact source it set out to move.
- Step 2: one local confirmation on a pinned table runtime with one deadline (`Pool::confirmPinnedRef`, `CasRefLedger`); the source is resolved once and the handle shared by all borrowers of that source in the transaction; `ConfirmLockMode`, the warm-up read and the retry loop are gone. The remote relink confirmation is unchanged (resident-only, try-only).
- Known leftover: the blocking acquisition of the table's `state_mutex` in the confirmation is not bounded by the deadline (a plain `std::mutex`; a timed mutex would touch about 150 spellings and the tests' fake clock cannot drive it). Latency, then a refusal; correctness unaffected.

STEP 3: a staged file and its provenance are ONE value
One path-indexed staging container per staged part. A value is exactly one of: inline bytes; a blob with a SHARED pending payload (the bytes to upload, owned through its last user, with one shared "publication attempted" flag); a blob borrowed from an exact source handle. Hardlink copies the value; file and projection renames change its path; removal of a committed path is recorded in the same structure. The manifest, the distinct upload requests and the distinct borrowed sources are DERIVED from the surviving entries at publication (group by the full blob ref so locally available bytes satisfy same-hash aliases). A transaction-owned move of a SOURCE updates the shared handle once from the move's exact result; an ordinary repoint or a name reuse by someone else must never retarget a borrow.
Disappears: `copy_sources` and its extract / re-insert on rename, the synchronized put / erase / re-key / clear helpers, `copySourceOf`, `findPendingBlob`, `adoptStagedBlob(..., copy_pending)`, the per-part copies of pending records, the "neither upload nor source" validation, the separate bookkeeping of removed paths where it duplicates the container.
Must be preserved: overwrite order; the order in which sources are confirmed (the staged vector defines it today: decide and state the new order; an earlier simplification review advised against a path-ordered map for exactly this reason, so use an ordered sequence or keep an explicit order); one confirmation per distinct source; no object-store request per blob; a second part naming the same staged body checks it with `HEAD` and does not upload again; the staged bytes stay readable until the commit ends.
Covering tests (in `src/Disks/tests/gtest_ca_transaction.cpp`): `ProjectionRekeyKeepsTheCopySource`, `StagedCopyConfirmsTheOriginalSource`, `EntriesTheDestinationUploadsCarryNoSource`, `ManyEntriesFromOneSourceConfirmItOnceBetweenPrecommitAndPromote`, `TheSecondPartThatNamesAStagedBodyChecksItAndDoesNotUploadIt`, `ACommittedPartUploadsWhatItKeepsWhenTheOtherPartFails`, plus the 16-shape table of "an operation separates an entry from its upload record" from the review of the `moveFile` fix (hardlink across parts, copy of a copy, `removeDirectory`, re-keys, rewrite after hardlink). Add: a self-hardlink followed by a rename of the committed part; a second part borrowing the same renamed source; overwrite of a committed file followed by unlink / move-out.

STEP 4: the publish operation discharges the proof
Replace the local "bare entry in, trusted evidence out" route (`adoptEvidence(entry)`) by preparation from the typed entries of step 3. Confirmation becomes MANDATORY inside local publication, outside the append closure; trusted evidence is granted only for the leaves covered by that exact durable precommit. `ContentAddressedTransaction::confirmCopySources` and the caller-managed confirmation in `republishRef` are deleted; remote relink keeps its separate prepared handle (its caller does the interserver confirmation). A whole-ref rename still confirms its source once even for an all-inline manifest. For one source fanned out to several destinations in one transaction: prepare the dependent destination precommits BEFORE the first shared confirmation, then publish in the existing part order; a positive answer covers exactly the precommits durable at that moment (caching a Yes for a part not yet precommitted would reopen the GC gap).
Second half of step 4, separable and riskier: an EDITED committed part assembles its final merged manifest first, precommits THAT manifest, uploads under its edge and promotes with `replaceIfCurrent(base)`; the atomic base check covers carried-forward entries. This removes today's sequence scratch precommit -> upload -> second build -> repoint -> scratch abandon. Keep canonical equality and the zero-mutation no-op for an unchanged rewrite. This changes the ORDER OF STORE REQUESTS on the main write path (every insert into / mutation of a committed part): it is a protocol change, not a refactor: it needs its own review, a TLA+ check of the repoint against a concurrent GC cut, and a soak run.
Invariant after step 4: a local publish may commit a borrowed blob only after its exact destination precommit is durable and its source binding has been confirmed under that protection.
Tests to add: two destinations observing both precommits at the single confirmation; the matrix `CASInterleave` stays green (its hardlink path renames the DESTINATION, so it does not cover the self-rename case).

FOR AND AGAINST (as weighed on 2026-11-20)
For: closes the class (a fourth no-bytes copy path written later cannot forget the confirmation: it will not compile); removes the source of the five defects above; slightly less code.
Against: it is the main write path (every insert and merge goes through `publishStaging`); the edited-part half changes the request sequence; several days of work in code that soak and CI have exercised; no urgency: the third full review round found nothing new in this area after steps 1 and 2; remaining exposures are fail-closed refusals of a live source under cache pressure or lock contention, and shapes no SQL caller produces (checked callers under `src/Storages/MergeTree/`: `MutateTask.cpp`, `DataPartStorageOnDiskFull.cpp`, `MergeTreeData.cpp`; `src/Databases/DatabaseOnDisk.cpp`).
Do it earlier if a new no-bytes copy path is planned (for example for backups).

SUGGESTED ORDER
(a) Step 3 alone, behaviour-preserving, proven by the existing transaction tests and the request-count tests. (b) Step 4 first half (typed preparation, confirmation inside publication). (c) Step 4 second half (edited-part path) with a model check and a soak. Each as its own PR or commit series.

CONSTRAINTS (owner's, standing)
No object-store request per blob on copy paths; the writer's `HEAD` before `PUT` stays and an existing body is reused without an upload unless a deletion intent for exactly that incarnation exists; a condemned object is never read back to revive it; the on-store format is frozen; no new setting; errors propagate, no fallback path; nothing under `src/Storages/` changes; `LOGICAL_ERROR` expectations are death tests in debug / sanitizer builds.

REFERENCES
PR https://github.com/Altinity/ClickHouse/pull/2917 (steps 1 and 2 and the local fixes). Design document `docs/superpowers/specs/2026-11-19-cas-gc-body-delete-zero-proof.md`, sections "Copies without reading the body" and "Why it is safe". Related: CAS-504 (the data loss), CAS-505 (coverage), CAS-506 (GC rebuild). The design consult's own text for steps 3 and 4 is appended in the notes of this task.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
OWNER 2026-11-20: deferred until PR 2917 merges ('Да, откладываем').

Appendix: the design consult's text for steps 3 and 4 (gpt-6.2-astra, 2026-11-20, written against commit 584eb55d85f of branch work/antalya-26.8/cas-blob-intents; paths relative to src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/; line numbers have moved since):

3. **Stage a file and its provenance as one value.** Use one path-indexed staging container. A value is inline bytes, a blob with a shared pending payload, or a blob borrowed from an exact source handle. Hardlink copies the value. File and projection renames change its path. A pending payload owns its staging resource and shared publication-attempt flag through its last user. Generate the manifest, distinct upload requests, and distinct borrowed sources from the surviving entries. Group by full `BlobRef` so locally materializable bytes can satisfy same-hash aliases. This removes `copy_sources`, the synchronized `putEntry`/`eraseEntry`/`rekeyEntry`/`clearEntries` bookkeeping, `copySourceOf`, `findPendingBlob`, `adoptStagedBlob(..., copy_pending)`, the per-part copies of pending records, and the quadratic “neither upload nor source” scan. `ContentAddressedTransaction.cpp:200–285,325–375,455–461,1326–1378,1729–1745`.

   Share each exact source handle across **all** its borrowers in the transaction. A successful transaction-owned move updates that handle once from the move’s exact result. A same-namespace `Move` preserves the manifest. Cross-namespace republication supplies a new manifest over the same entries. Carry the expected source into `republishRef` and return the exact destination observed or created. Today `renameCommittedRef` checks S, calls a function that resolves S again, then resolves D again to discover what supposedly moved. Either resample can belong to another writer. Apply the receipt to other parts borrowing S as well as the moved overlay, including the table-move loop. Ordinary repoints and name reuse must never retarget a borrow. A scratch binding with outstanding borrowers must remain protected until their handoff if the move would otherwise discard it. `Parts/PartFolderAccess.cpp:634–657`, `ContentAddressedTransaction.cpp:1434–1435,1538–1549,1590–1622`.

4. **Make the publish operation discharge the proof.** Replace the local bare `adoptEvidence(entry)` route with preparation from those typed entries. Make confirmation mandatory inside local publication, outside the append closure. Grant trusted evidence only for the covered leaves of that exact durable precommit. Delete `ContentAddressedTransaction::confirmCopySources` and the caller-managed confirmation in `republishRef`. Keep remote relink’s separate prepared handle, whose caller performs the interserver confirmation. A whole-ref republish still confirms S once even for an all-inline manifest, as its rename contract requires. `Parts/PartFolderAccess.cpp:455–487`, `ContentAddressedMetadataStorage.cpp:2349–2364,2493`, `src/Storages/MergeTree/DataPartsExchange.cpp:1637–1671`.

   For an edited committed part, assemble the final merged manifest first and precommit **that** manifest. Upload under its edge and promote with `replaceIfCurrent(base)`. The atomic base check covers carried-forward entries. This removes the scratch-precommit → upload → second-build repoint → scratch-abandon sequence. Create the build for its final namespace. Preserve canonical equality and the zero-mutation no-op for a pure unchanged rewrite. If staged writer bytes require materialization, do their normal checks even when the final entries compare equal. `ContentAddressedTransaction.cpp:493–546`, `Parts/PartFolderAccess.cpp:738–775`, `Pool/CasPartWriteTxn.cpp:742–751,923–930`.

   For one source fanned out to multiple destinations in one transaction, prepare the dependent destination precommits before the first shared confirmation. Then publish in the existing part order and confirm at first use. A positive result covers exactly the precommits already durable at that point. Caching a `Yes` for a later, as-yet-unprepared part would reopen the GC gap.

   `gtest_ca_transaction.cpp` covers one confirmation for many files, borrowed-file chains, refusal after an actual source rewrite, projection rekeying, overwritten borrows, and payloads split between parts (995, 1029, 1063, 1210, 1242, 1357, 1395). `TheSecondPartThatNamesAStagedBodyChecksItAndDoesNotUploadIt` (1475) protects writer reuse. Add a self-hardlink followed by committed-part rename, a second part borrowing the same renamed source, and a two-destination test that observes both precommits at the single confirmation. Keep `gtest_cas_gc_round_interleave.cpp`’s `CopyAndDropKeepEveryNamedBody` (799) as the end-to-end GC check. Its hardlink path renames the *destination*, so it does not cover (b) (469–472).

Two more instances of the same staging bookkeeping class, seen on 2026-11-20 and NOT fixed in PR 2917 (no production caller reaches them): (1) `removeRecursive` on a projection subdirectory of a staged part does nothing to that part's staged or committed files: if the caller keeps the part, the files stay; (2) a projection re-key (`.tmp_proj` to `.proj`) onto a `.proj` name that the COMMITTED part already holds merges the two directories: the guard added in PR 2917 checks staged entries only. Step 3 (one container whose value carries its provenance, removals recorded in the same structure) should make both impossible; add tests for them.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:09
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: copy_sources / adoptEvidence still referenced 16 times in ContentAddressedTransaction.cpp of the intents worktree; no typed staging container yet. Changes: area set to write-path; size unset -> large (L): steps 3 and 4 on the main write path, several days plus a model check and soak for 4b; type unset -> enhancement: hardening refactor of the write path, step 4b changes store request order; priority high -> medium: the task itself says no urgency: remaining exposures are fail-closed refusals, and the owner deferred it until PR 2917 merges. Size: L. Links: Related CAS-504, CAS-505, CAS-506; do after PR 2917 merges..
---

author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21 (addendum): added risk:high, confidence:plausible.
---
<!-- COMMENTS:END -->
