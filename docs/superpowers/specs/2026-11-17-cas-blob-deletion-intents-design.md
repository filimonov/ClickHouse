---
description: 'Design for CAS blob publication without the per-blob `Clean` marker. A writer publishes a body with one `PUT` and writes nothing else. GC alone warns writers, with an immutable deletion intent keyed by the body incarnation, created only when a committed condemned row graduates. A writer reuses a body after `HEAD`, intent `GET`, `HEAD`. Bodies of the new protocol carry one `Content-Type` mark; a legacy body is re-uploaded on its first reuse. Ships as format generation 4 behind an operator command.'
sidebar_label: 'CAS: blob deletion intents'
sidebar_position: 118
slug: /superpowers/specs/cas-blob-deletion-intents-design
title: 'CAS: blob publication without a birth marker, deletion intents'
doc_type: 'reference'
---

# CAS: blob publication without a birth marker, deletion intents — design {#cas-blob-deletion-intents}

- Date: 2026-11-17. Status: design, not implemented. No test and no model of this design was run.
- Code: worktree `cas-pr1`, commit `a966a374b17`. Every code `path:line` was read there.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `M/` = `docs/superpowers/models/`. `D/` = `docs/en/antalya/cas/`.
  `W:` = `CA/Pool/CasPartWriteTxn.cpp`. `G:` = `CA/Gc/CasGc.cpp`. `N:` = `CA/Gc/CasBlobInDegree.cpp`.
  `B:` = `CA/Backend/CasObjectStorageBackend.cpp`. `Q:` = `CA/Backend/CasRequests.cpp`. `X:` = `CA/ContentAddressedTransaction.cpp`.
- Terms. **Body**: the object at a blob key `B`. **Incarnation** `t`: the canonical identity ([§6.1](#identity)) of the
  body now at `B`. **Intent** `I(B,t)`: the object of [§2.1](#intent-object). **Marker**: today's `<blob>.meta`.
  **Marked**: a body whose `HEAD` shows the mark of [§2.2](#mark). **Legacy**: an unmarked body. **Edge**: a precommit
  or committed ref whose manifest names `B`. **Cut**: the reference snapshot one GC round folds.
  **Learned**: [§3.1](#learned). **Marker mode**, **intent mode**: the two GC modes of [§3.3](#gc-rules).

## 1. Decision and effect {#decision}

1. **No birth marker.** A writer publishes a body with one unconditional `PUT` after its `HEAD`. It writes no
   `Clean` marker. Writers never read or write `.meta`, and never create, change or delete an intent.
2. **GC warns with immutable intents.** `I(B,t)` is keyed by the body incarnation, has constant bytes, and is
   created and deleted only by GC (and deleted by `cas-fsck`). It exists only for bodies on their way to deletion.
3. **Write-ahead GC.** A condemned row is committed first, with no intent. A later round whose cut still shows
   in-degree 0 creates the intent; the confirmed create graduates the row. A round after that commit deletes.
4. **Reuse by three reads.** `HEAD B`, `GET I(B,t)`, `HEAD B`: adopt iff the intent is absent and both `HEAD`s show
   the same incarnation. Anything else publishes a fresh body from the local source.
5. **One mark.** A body published under this protocol carries one exact `Content-Type`. A legacy body is never
   adopted: it is re-uploaded on its first reuse, marked. That branch is permanent.
6. **One identity.** A canonical token identity is used for intent keys and for every comparison; the raw token is
   kept only for `If-Match`.
7. **Verified streaming.** Every streaming publication hashes the payload it sends and refuses before the object
   is completed.
8. **Generation 4, operator switch.** `gc/state` is stamped generation 4, then `_pool_meta` gets
   `blob_protocol = intents`, by `SYSTEM CAS ENABLE BLOB INTENTS` after every older build is stopped.

**Why.** The marker is paid by every blob and read by few. On a stand, one `INSERT ... SELECT` of 201.6 M rows into
a 109-column `ReplicatedMergeTree` wrote, on one replica (`system.blob_storage_log`): 54,997 bodies in 2,047
thread-seconds and 54,997 markers in 1,325 thread-seconds; ref logs and manifests took 50. The marker is 39 % of
upload thread time and 49 % of all `PUT`s (54,997 of 111,486). It bought 7,029 avoided body uploads.

**Cost.**

- Safety now depends on body tokens never recurring at a key ([§5](#assumptions), A1). Today's rule "absent marker,
  republish" (CAS-360, option C) did not.
- Clean reuse costs 3 reads instead of 2.
- A legacy body is uploaded again on its first reuse: once per reused blob, `k` times for `k` concurrent writers.
- Writers of the new build do not deduplicate until the switch.
- A second kind of object (`blob-intents/`), generation 4, an operator step, no downgrade.

**Budget**, per blob and writer attempt on the successful uncontended path: first creation one `PUT` and at most
3 reads; clean reuse reads only; reuse after condemnation at most 2 `PUT`s and 3 reads. The ledger meets it;
contended and ambiguous paths are counted apart.

**Request ledger.** Counted from the code, not measured. A publication counts as one `PUT` whatever its transport:
a multipart upload is a create, parts and a complete; a staged copy is one `CopyObject` or a multipart copy; a
re-publication from S3 staging also streams the staging object with one `GET` that is not a blob-key request.

Writer, per blob and attempt, successful and uncontended:

| Case | Today | New |
|---|---|---|
| First creation | `HEAD`, body `PUT`, marker `PUT`: 1 read, 2 `PUT` (`W:424`, `:509`, `:368`) | `HEAD`, `PUT`: 1 read, 1 `PUT` |
| Clean reuse | `HEAD`, marker `GET`: 2 reads (`W:424`, `:446`) | `HEAD`, intent `GET` (404), `HEAD`: 3 reads |
| Reuse after condemnation | `HEAD`, marker `GET`, body `PUT`, marker `PUT`: 2 reads, 2 `PUT` (`W:473`, `:373`) | `HEAD`, intent `GET` (200), `PUT`: 2 reads, 1 `PUT` |
| Body present, marker absent | `HEAD`, marker `GET` (404), 2 `PUT` (`W:475`) | not a case: an absent intent adopts |
| Legacy body, or writer not learned | — | `HEAD`, `PUT`: 1 read, 1 `PUT`; once per blob after the switch |

Writer, contended and ambiguous (new protocol):

| Case | Requests |
|---|---|
| Second `HEAD` shows another token or no body | 3 reads, 1 `PUT`. No restart |
| Publication unanswered, it landed, the retry adopts it | `HEAD`, `PUT`, `HEAD`, `GET`, `HEAD`: 4 reads, 1 `PUT` |
| Publication unanswered, it did not land | 2 reads, 2 `PUT`; each further unanswered attempt adds 1 `HEAD` and 1 `PUT`, at most 8 attempts inside one request window (`W:406`, `:296`) |
| `k` writers on one absent or legacy body | 1 `HEAD` and 1 `PUT` each: `k` uploads |

GC, per reclaimed blob:

| Step | Today | New |
|---|---|---|
| Condemn | `HEAD`; marker `GET` + `PUT` (`G:2194`, `CA/Gc/CasGcMetaWriter.cpp:45-51`): 3 | `HEAD`: 1 |
| Graduate | marker `GET` (`G:2271`): 1 | intent `PUT` with `If-None-Match`: 1; plus 1 `GET` when the intent already exists |
| Delete | `HEAD` + `DELETE` (`G:591-593`); marker `GET` + `DELETE` (`CA/Gc/CasGcMetaWriter.cpp:59-62`): 4 | `HEAD`, body `DELETE`, intent `DELETE`: 3; plus 1 marker `DELETE` for a legacy body |
| Total | 8 | 5 (6 for a legacy body) |
| Blob referenced again before graduation | 3, and a `Condemned` marker that forces one upload | 1, and no intent |

## 2. Objects and formats {#formats}

`G_BLOB_INTENTS = 4`; `G_BUILD` 3 → 4 (`CA/Formats/CasFormat.h:16`); `mount_reader_floor` → `G_BLOB_INTENTS`
(`CA/Pool/CasPool.h:450`). Frozen and unchanged: the blob envelope, manifests, ref logs, the `.meta` codec
(`FormatId::BlobMeta`, used only by marker-mode GC and the tools).

### 2.1 The intent object {#intent-object}

- **Key.** `<pool>/blob-intents/<algo>/<hex[0:2]>/<hex>/<dialect>/<tag-hex>`. The first four segments mirror
  `Layout::blobKey` (`CA/Formats/CasLayout.cpp:35-38`); `<dialect>/<tag-hex>` is the canonical identity
  ([§6.1](#identity)). New `Layout::blobIntentKey(ref, identity)`, `blobIntentsPrefix`, `parseBlobIntentKey`.
- **Parse.** `parseBlobIntentKey` accepts only the spelling `blobIntentKey` produces: known algo word, shard equal
  to `hex[0:2]`, dialect from `etag | generation | emulated`, lowercase even-length hex that decodes to a valid
  tag. Anything else is not an intent.
- **Length.** At most 14 + 6 + 4 + 64 + 1 + 10 + 1 + 256 = 356 bytes after the pool prefix. A writable mount
  refuses a pool prefix longer than 1024 − 356 bytes.
- **Bytes.** Constant: the header line and an empty body object, `{"type":"cas_blob_intent","v":4}\n{}\n`
  (under 40 bytes). No round, no epoch, no size, no token: the key is the identity. A constant makes the confirmation of
  [§3.3](#gc-rules) G3 a byte comparison.
- **Registry.** `FormatId::BlobIntent = 26`, type `cas_blob_intent`, `TextFamily::Control`,
  `CompressionPolicy::PinnedRaw` (bytes are compared), object cap 4 KiB, line cap 1 KiB; change points `{4,4}`
  (`CA/Formats/CasFormat.cpp`, `TRAITS` and `changePoints`). A later generation must keep accepting this record as
  a confirmation.
- **Request class.** New `CasNs::BlobIntent` in `classifyCasNs` (`CA/Backend/CasInstrumentedBackend.cpp:119-136`),
  tested before the blob rule: `CASBlobIntentGet`, `CASBlobIntentPut`, `CASBlobIntentDelete`, `CASBlobIntentList`.

### 2.2 The body mark {#mark}

- `Content-Type: application/x-clickhouse-cas-blob-v2`, compared exactly. Any other value, or none, is unmarked.
  `v2` names the protocol: `markers` is 1, `intents` is 2.
- The mark is part of the object: an unconditional `PUT` or copy replaces it with the body.
- `HEAD` must return size, raw token and mark from one response ([§6.2](#mark-rules)).

### 2.3 Pool meta, `gc/state`, run rows, mount slot, epoch counter {#control-formats}

| Object | Change | Gate |
|---|---|---|
| `_pool_meta` | `min_reader_generation = 4`, raised by every writable mount as today (`CA/Pool/CasPool.cpp:628-640`, `:904`). New keys: `blob_protocol` (absent = markers; `intents`), monotone; `token_dialect` (`etag`, `generation`, `emulated`), written once with `intents` | additive keys (`CA/Formats/CasPoolMetaFormat.cpp:143`); older builds stop at the floor first (`:155-158`) |
| `gc/state` | `GcState` gains `generation`, decoded from the header `v`, encoded back unchanged (`CA/Formats/CasGcStateFormat.cpp:42`, `:60`); change point `{4,4}`. The stamp sets it to 4 | header 4 |
| `cas_run` condemned row | new bool `intent` beside `pending` and `confirmed` (`CA/Formats/CasRecordStreamFormat.cpp:198-205`, `:290-298`); in memory, bit 2 of `flags` (`N:266`, `:287`). Written `true` only by an intent-mode graduation. A run written in intent mode has header 4; change point `{4,4}` | header 4 |
| Mount slot (`cas_mount_lease`) | new `protocol_generation`, 4 in every body this build writes | additive: older decoders skip it (`CA/Formats/CasServerRootFormats.cpp:223-224`) |
| `epoch` counter (`cas_epoch`) | critical key `!protocol_generation`, written with the allocation of a writable mount | an older build fails the allocation with `UNKNOWN_FORMAT_VERSION` (`CA/Formats/CasTextFormat.cpp:315-322`) |

### 2.4 What an older build does {#older-builds}

| It meets | Result |
|---|---|
| `_pool_meta` at floor 4, at mount or read-only open | `UNKNOWN_FORMAT_VERSION`: `CAS pool meta: pool requires reader generation 4 but this build supports at most 3` |
| Floor 4 while already mounted | keeps running; cannot remount |
| `gc/state` at header 4 | its lease read-modify-write decodes and throws (`G:5214`): no round starts. A round already past that read fails its `If-Match` commit (`G:1326`); its deletes went out before the commit |
| A marked body | ignores `Content-Type`. With no marker at the key it republishes, unmarked, and writes `Clean` (`W:475`, `:542`) |
| A body with no marker, before the switch | writer: republishes once (`W:475`). GC: creates `Condemned` on the absent key (`CA/Gc/CasGcMetaWriter.cpp:48-49`) |
| An intent | never reads the prefix |
| A latched root | cannot allocate an epoch for it |

## 3. Protocol {#protocol}

### 3.1 Learning the switch {#learned}

A writer has **learned** the switch when all hold for its `Pool`:

1. It read `_pool_meta` with `blob_protocol = intents`.
2. The `token_dialect` recorded there equals its backend's dialect.
3. Its mark probe passed ([§6.2](#mark-rules)).

- Read points: at mount; then, while (1) is false, a re-read by the lease renewer every 60 s, and one when a
  writer meets a marked body (at most once per 10 s); at once on the server that ran the switch.
- Monotone: once learned, never unlearned, never re-read. A stale answer is the conservative one.
- (2) false: the mount stays not learned and logs an error. (3) false: [§6.2](#mark-rules).
- **Mark bit.** One bool per pending blob, `PendingBlob::marked` (`CA/ContentAddressedTransaction.h:157-166`),
  set to "learned" when the staging sink is created, copied with the record (`X:252`). Every publication of that
  blob uses it. `BlobSource` carries it next to `publication_attempted` (`CA/Pool/CasPartWriteTxn.h:26-38`).

### 3.2 Writer {#writer-rules}

Replaces `PartWriteTxn::ensureBlobPresent`, `W:274-570`. The loop, its single frozen deadline and the cap of 8
attempts stay (`W:296`, `:406`, `:415-423`).

| # | Step | Outcome and what follows | Replaces |
|---|---|---|---|
| W1 | The precommit edge is durable before the first request | else `LOGICAL_ERROR`, unchanged | `W:277-281`, order at `X:472-474` |
| W2 | `HEAD B` | Absent → W6, first-publication form. Present → check the size against the envelope length and the source (`CORRUPTED_DATA` exception, unchanged) and take token and mark from this response. An invalid token is an exception ([§6.1](#identity)) | `W:424-444` |
| W3 | Body unmarked, or writer not learned | → W6. No other request. Not learned and marked: also trigger the re-read of [§3.1](#learned) | `W:446-475` |
| W4 | Body marked and writer learned: `GET I(B,t)` | Any object at the key → W6. Authoritative 404 → W5. A read that gives up is an exception, never absence | marker `GET`, `W:446` |
| W5 | `HEAD B` again | Same canonical identity and valid size → adopt: `Materialized`, `CASBlobBodyPutAvoided`, event `BlobReuseAdopt`. Absent or another identity → W6, once, no return to W2. Invalid size or token → exception | `W:452-471` |
| W6 | Publish: one unconditional publication, sent once (`W:299`, `:509`), under a fresh envelope (`W:322-331`), marked iff the mark bit is set, digest-checked ([§6.3](#source-check)) | Answered success → `Materialized`. Unanswered → next loop iteration from W2 with the backoff (`W:511-540`). Deterministic failure → exception | `W:478-509` |
| W7 | After W6 | No request. The marker reconcile is deleted | `W:352-404`, `:542` |
| W8 | Reads of W2, W4, W5 | go through `op.head` and `op.read` to the store; no cache may answer them | — |

- **Verbatim staged copy** (`W:482-488`): only as the first publication of the staging object, only after W2
  found no body. W5 and every later attempt stream.
- Admission checks before each proof stay (`W:454`, `:466`, `:545`, `:562`).
- The writer contains no marker code: `loadMeta`, `reconcileMetaClean`, `validateMetaSize`, `meta_key` go.

### 3.3 GC {#gc-rules}

| # | Rule | Replaces |
|---|---|---|
| G1 | **Mode.** After the round acquired or renewed its lease (`G:787`) it reads `_pool_meta` with an exact `GET`. `intents` → intent mode, for this round and every later round of this `Gc` object. Otherwise marker mode for this round only: today's code, unchanged. Intent mode with `gc/state` below generation 4, or with another `token_dialect`, is an exception before any destructive step | the cached `store->poolMeta` |
| G2 | **Condemn.** Trigger unchanged: a fresh transition to in-degree 0 (`N:637-648`). `HEAD B` (`G:2194`). Present → a non-pending row with the observed token, size and round. Absent → no row. No other request | marker write, `G:2227` |
| G3 | **Graduate.** For a row an earlier round committed (`condemn_round < current_round`), not pending, with in-degree 0 in this round's cut, when destruction is not suppressed and the budget admits (`N:559-571`): `PUT I(B,t)` with `If-None-Match: *` and the canonical record (`op.create`). Confirmed → the row gets `pending` and `intent` in this round's run and this round's commit (`G:1326`); nothing is deleted in this round. Not confirmed → the row is carried unchanged, counted, the round continues | marker read, `G:2264-2287`; `N:572-583` |
| G4 | **Delete.** For a row read with `pending` and `intent` from the adopted state, after this round's cut showed in-degree 0 (`N:545-557`). (1) `HEAD B`. (2) Absent → `Gone`. Canonical identity equals the row's → `DELETE B If-Match <raw token of this HEAD>`: `Removed`, `Gone` or `Mismatch`. Another identity → `Mismatch`, no body request. (3) After any answered outcome of (2): `DELETE I(B,t)`; 404 is success. (4) If (2) removed an unmarked body: `DELETE <blob>.meta`, best effort, failures counted | `G:583-595`; marker delete `G:634-640` |
| G5 | **A delete job that did not get an answer** for its `HEAD`, body `DELETE` or intent `DELETE` throws. The round fails before its commit, the row stays pending (`G:752-762`), and the next round starts again at (1). An unanswered body `DELETE` never leads to the intent `DELETE` | — |
| G6 | **Spare.** In-degree above 0 (`N:520-543`): the row is dropped. No request. An intent stays where it is | — |
| G7 | **Supersede.** A touched row at in-degree 0 whose `HEAD` shows another canonical identity `u` (`N:612-631`): the row becomes a candidate for `(B,u)`. If the old row had `intent`: one `DELETE I(B,t)`, best effort | `matches` at `N:616` |
| G8 | **Inherited rows.** In intent mode a row with `pending` and without `intent` is a candidate: G3 applies, G4 does not | [§7.5](#inherited) |
| G9 | **Never.** GC never deletes an intent on a spare, never derives a row from an intent, never reads or writes `.meta` in intent mode except G4 (4) | — |

**G3: what confirms.** The gate consumes the result of its own create:

| Result | Confirmed |
|---|---|
| `Committed`: an answered success, or an unanswered attempt whose resolve read found equal bytes (`Q:1127-1128`) | yes |
| `Conflict` with an observed `Object` whose bytes equal the canonical record. A clean refusal returns `Conflict`, not `Committed` (`Q:1118-1119`); `create` resolves with a body read (`Q:1154-1158`) | yes |
| `Conflict` with `ProvenAbsent`, `NotObserved` or other bytes; `GaveUp`; any exception | no |
| A request that was sent, a job that finished, a drained queue | no. Today's marker worker counts a failed job as completed (`CA/Gc/CasGcMetaWriter.cpp:91-99`) |

- **Read-ahead.** A create may be issued ahead of the gate only for a carried row that no delta or retirement of
  this round touches (the rule of today's marker hints, `G:3667-3700`). A touched row is created at the gate. Reason:
  an intent over a blob that is then spared costs its next writer one upload. `GcReadAhead` gains a create verb
  (`CA/Gc/CasGcReadAhead.h:53-57`); the hint key needs the row's token, which `admit` does not get today (`G:3676`).
- **Removed in intent mode:** the marker pool and phase `meta_pool_wait` (`G:781`, `:1276-1283`). Both stay for
  marker mode.
- **Budgets.** Graduation and redelete budgets apply as today. No new count bound.

### 3.4 Cleanup of intents without a row {#fsck-rules}

Row-less intents arise by design ([§8.2](#residuals)). `cas-fsck` handles them; GC has no circuit.

| # | Rule |
|---|---|
| F1 | `cas-fsck` lists `blob-intents/`. A key that `parseBlobIntentKey` rejects is `intent_malformed`: reported, never deleted |
| F2 | For a listed intent: exact `GET` of the intent first, **then** `HEAD B`. Same canonical identity → `intent_current`. Absent, or another identity → `intent_stale`. Intent gone at the `GET` → skip |
| F3 | Fix mode (`cas-fsck --fix stale-intents`): `DELETE` each `intent_stale` key, using the two reads of F2 made in that order in the same run. Never from the listing alone, never with the `HEAD` first |
| F4 | On a pool with `blob_protocol = intents` every `.meta` is `legacy_marker`. `cas-fsck --fix legacy-markers` deletes them unconditionally. Refused on a pool without `intents` |
| F5 | Intents are accounted objects in every class total. `body_without_meta` and `meta_without_body` (`CA/Tools/CasFsck.cpp:1206-1222`) are not computed on a switched pool |

This is the first mutating option of `cas-fsck`.

### 3.5 Recovery and rebuild {#recovery}

| Event | State left | What happens next |
|---|---|---|
| GC leader dies after the create of G3, before its commit | row not pending; intent exists | the next round creates again, gets the equal occupant, confirms |
| Same, and the blob is referenced before the next round | row dropped by G6; intent over a live body | the next writer of that content uploads once; the old intent is then stale |
| Dies after the body `DELETE`, before the intent `DELETE` | row pending | next round: `HEAD` absent, intent `DELETE`, row dropped |
| Dies after the intent `DELETE`, before the commit | row pending | next round: `HEAD` absent, intent `DELETE` answers 404, row dropped |
| A create lands after its row left the state | a row-less intent | stale if `t` is gone; else it costs one upload. F3 removes it |
| Writer dies after its `PUT` | a marked body, its precommit edge abandoned | the next writer adopts it; GC condemns it when the edge is removed |
| `SYSTEM CAS GC REBUILD`, `cas-gc-rebuild` | no condemned row (`G:4762-4765`) | intents stand. No candidate is recovered from an intent: it proves a past condemnation, not zero references now |

- Every `gc/state` body this build creates (lease bootstrap `G:5204-5211`, rebuild) has header 4. Every body it
  rewrites keeps the generation it read (`G:5222`, `:5268`, `:1300`).
- A rebuild reads `_pool_meta` and never lowers `blob_protocol`.

## 4. Invariants and the safety argument {#safety}

### 4.1 Invariants {#invariants}

| # | Invariant | Provided by |
|---|---|---|
| I1 | **Non-recurrence.** Once `t` stops being the incarnation of `B`, it never is again | A1 |
| I2 | **Content.** Every body ever at `B` has the pool's envelope length and the payload that hashes to `B` | [§6.3](#source-check), A6 |
| I3 | **Edge before observe.** A writer's edge on `B` is durable before its first `HEAD B`; it commits only while that precommit is the live owner | W1; `W:965-970` |
| I4 | **Intent permanence.** `DELETE I(B,t)` is sent only after an authoritative observation that `B` is absent or has another canonical identity, itself made after evidence that `t` had been current: the committed row (G4, G7) or the intent read first (F2). So while `t` is current, no such request was ever sent, and an existing `I(B,t)` stays | G4, G6, G7, G9, F3; I1 |
| I5 | **Delete evidence.** Every `DELETE B If-Match t` follows, in this order: a confirmation that `I(B,t)` exists; the commit of the pending row; a complete cut, taken from that committed state, with in-degree 0; a `HEAD B` that shows `t` | G3, G4; `frontier_complete`, `G:3548-3551` |
| I6 | **Adoption.** A writer records an adoption only of a marked body, when learned, after W4 answered 404 and W5 showed the identity of W2 | W3 to W5 |
| I7 | **Marked means new.** A marked body was published by a learned writer, so after the switch was written | [§3.1](#learned), [§6.2](#mark-rules), A4 |
| I8 | **Goal.** A committed ref never names a blob key with no body | [§4.2](#argument) |

### 4.2 The argument {#argument}

**Adoption.** Writer `W`: edge at `Te`, W2 sees `t` at `T1`, W4 answers 404 at `T2`, W5 sees `t` at `T3`.

1. By I1, `t` was current during `[T1, T3]`.
2. Suppose a delete removes `t` at `Td > T3`. By I5 it has a confirmation at `Tc`, a commit `K` and a cut `C`,
   `Tc < K < C < Td`. The row's token was observed at its condemn `HEAD`, so by I1 `t` was current from then to `Td`.
3. By I4 the intent exists during `[Tc, Td)`. `W` read it absent at `T2 < Td`, so `T2 < Tc`.
4. So `Te < T2 < Tc < K < C`: the edge is durable before the cut. A complete cut counts it, the row is spared,
   contradiction. If the edge was removed before `C`, `W` cannot commit (I3).

**Publication.** `W` publishes `t'` under a fresh envelope after `Te`. `t'` is new (A1), so no earlier authority names it. Any
delete of `t'` needs a condemn `HEAD` after its appearance, then `Tc`, `K`, `C`: the cut is after `Te` and counts
the edge. A late delete of an older token misses: the condition is exact.

**Cleanup.** A `DELETE I(B,t)` is sent only when `t` is gone for good (I4, I1), so it can land at any time.

**No-bytes references.** `adoptEvidence` (`W:572-587`; callers `X:260`, `X:1262`,
`CA/Parts/PartFolderAccess.cpp:457`) adds an edge with no request. It needs a live source edge, which the cut
counts. Unchanged by this design; so is its assumption that the source edge outlives the new precommit.

**Where evidence must be positive, where absence is accepted:**

| Decision | Evidence | Why |
|---|---|---|
| Graduation | positive: the create answered, or an equal occupant read | an unanswered create may never land; then nothing warns the writer |
| Body delete | positive: the committed row, a present body with the row's identity | — |
| Adoption | positive: two `HEAD`s with one identity, the mark | — |
| Intent absent at W4 | absence, authoritative 404 only | sound by I4 and the second `HEAD`; a failed read is not absence |
| Body absent at W2, G2, G4, F2 | absence, authoritative 404 only | each leads to an action that is safe if a body appears later: publish, no row, intent cleanup under I1 |
| A `LIST` | nominates intents (F1) and mount slots ([§7.3](#command)) only | a missing key delays cleanup; it never authorizes a body delete |

### 4.3 Schedules each rule defeats {#schedules}

| # | Schedule | Defeated by |
|---|---|---|
| S1 | **No second `HEAD`.** GC confirms `I(B,t)`, commits pending, takes cut `C`. `W` adds its edge after `C`, sees `t`. GC deletes `t`, then `I(B,t)`. `W` reads the intent: 404, and would adopt a missing body | W5 |
| S2 | **Cleanup reads the body first.** fsck sees `B` absent. A writer publishes `t`. GC condemns `t`, creates `I(B,t)`, proceeds to delete. fsck then finds the intent and deletes it on its old observation. A writer adopts `t`; the delete lands | F2 order; I4 |
| S3 | **Spare with cleanup.** Leader `G1` holds a valid delete of `t` and stalls. `W1` adds an edge, sees the intent, starts its upload. `G2` spares the row and deletes the intent. `W2` adopts `t`. `G1`'s delete lands; `W1` never finishes | G6 |
| S4 | **Unanswered create.** The create is sent and never applied. GC counts it, commits pending, deletes. A writer in between read no intent and adopted | G3 table |
| S5 | **Deposed leader.** `G1` sends the create, gets no answer, loses the lease. It cannot graduate. `G2` confirms on its own, commits, waits for a later cut. `G1`'s late create only adds the same warning. A late delete of `G1` under older valid authority finds the intent still in place, so every writer since replaced `t` | G3; I4 |
| S6 | **Same token, condemned again.** Pending row spared, intent stays; references vanish; a new row names the same current `t`. The create is refused by the equal occupant | G3 row 2: a new commit and a new cut give a fresh order. No round or epoch in the bytes is needed |
| S7 | **Two absent publishers.** `W` stages `X`, sees `B` absent. `W`'s source turns into `Y` of equal length. `P` publishes `X` and commits. `W`'s first publication lands `Y` over `P`'s data | [§6.3](#source-check): every streaming publication is hashed, not only overwrites |
| S8 | **Spelling.** GC confirms `I(B,t)`. A later `HEAD` renders the same incarnation with other quoting. A textual compare reads "replaced", cleanup deletes a live intent | [§6.1](#identity): one identity for every comparison |
| S9 | **Ambiguous body delete.** The delete is unanswered and not applied; GC removes the intent; a writer adopts; a reissued delete lands | G5 |

Considered and not adopted: adopting at W5 when the token changed to `u`. It is safe under I1: `u` first appeared
between the two `HEAD`s, so every authority on `u` has a cut after the edge. It would widen the adoption proof for
a rare case.

## 5. Assumptions and the contract with the store {#assumptions}

| # | Assumption | What provides it | What stays assumed |
|---|---|---|---|
| A1 | A body token never recurs at a key | One publication call site, `W:509` through `Q:780`. Streaming: a fresh 128-bit tag per attempt (`W:326`, `:497`), one send (`W:299`, `B:894-900`). Verbatim copy: the staging tag is minted once (`X:797`), and the copy happens at most once per staging object because hardlink copies share the flag (`CA/ContentAddressedTransaction.h:162-165`, `X:252`, `:300`; `CA/Pool/CasPartWriteTxn.h:38`), only after an absent `HEAD` (`W:482`). Generation dialect: the store's own counter | No transport reissue after a request may have been applied (redirect, re-sign, reconnect). The store applies one request at most once. Random tags do not collide. A generation is never reused. A removed delete marker on a versioned bucket is external manipulation |
| A2 | Reads are authoritative: a `HEAD` or `GET` reflects every write the store acknowledged, for every client | the store; W8 keeps caches out | proxies and caches between a server and the store |
| A3 | A conditional delete removes only the named incarnation | the capability probe (`CA/Backend/CasProbe.cpp:47`); contract at `CA/Backend/CasBackend.h:120-139` | the probe samples; it does not certify non-recurrence (`:137-139`) |
| A4 | `HEAD` attributes are coherent: size, token and `Content-Type` describe one incarnation, and `Content-Type` is what the publication of that incarnation sent | the mark probe samples it per mount ([§6.2](#mark-rules)) | a store or proxy that synthesizes, retains or rewrites the type on paths the probe does not exercise |
| A5 | One token dialect and one spelling per pool, across clients | `token_dialect` in `_pool_meta`, checked at mount and at learning; canonical identity | two clients that render one tag with different bytes beyond the quote pair |
| A6 | A staging object holds the bytes hashed at staging | the staging key is 32 random characters under the writer's epoch (`X:950`), written once, known to one transaction | corruption inside the store; hash collisions |
| A7 | Cuts are complete: every live or precommit edge is in a cut that authorizes a delete | `frontier_complete` suppresses destruction otherwise (`G:3548-3551`) | unchanged, not re-proved here |
| A8 | No older build starts protocol work after the switch | operator precondition ([§7.2](#procedure), step 3); floor, latch and declarations enforce and diagnose | [§7.7](#transition-risks), X1 |
| A9 | Control objects are not deleted or rolled back | — | external manipulation is unsupported |

This dependence is a shipping contract. A store whose tokens can recur must not host a pool under this protocol.

## 6. Token identity, mark, source check {#mechanisms}

### 6.1 Token identity {#identity}

- **Canonical identity** = (dialect word, lowercase hex of the opaque tag). The opaque tag is the token value
  with at most one balanced pair of double quotes removed. No case folding: tags are opaque, and folding could
  merge two incarnations.
- **Raw token**: the value as the store returned it. Used only as `If-Match` (`G:593`).
- **Used for:** intent keys; W2 against W5; row matching at G4 and G7; fsck F2. New `Etag::canonical`;
  `PersistedEtag::matches` compares canonical identities instead of renderings (`CA/Backend/CasEtag.h:85-88`).
  Rows keep the token as captured; both sides are canonicalised at compare time.
- **Validation**, in `isIncarnationValue` (`CA/Backend/CasEtag.cpp:10-32`), which `CasRequests::tryMint` applies
  to every observation (`Q:337-342`). Rejected for the `etag` and `emulated` dialects: empty; a `W/` prefix; an
  unmatched or inner quote; a control byte; leading or trailing whitespace; `*`; a comma; an opaque tag over 128
  bytes. `generation` keeps canonical positive decimal. A rejected value is an exception (`Q:344-349`); nobody
  acts on it.
- **Quotes.** The generation dialect already strips one pair (`CA/Backend/CasObjectStorageBackend.h:175-182`); the
  `etag` dialect keeps them in the raw token because `If-Match` needs them.
- **Dialect.** Recorded once in `_pool_meta` with the switch. A writable mount whose backend dialect differs is
  refused: `CAS pool records token dialect '<a>', this disk uses '<b>'`.
- Intent keys are derived only from a body `HEAD`, never from a listing or a write response.

### 6.2 The mark {#mark-rules}

- **Set per request**, by the publication, from the mark bit. Never as a configured extra header: the HTTP client
  applies extra headers after the SDK's and would override the choice (`src/IO/S3/PocoHTTPClient.cpp:611-622`).
- **Three paths, four constants.** The type is hard-coded `binary/octet-stream` at `PutObject`
  (`src/IO/WriteBufferFromS3.cpp:775`), multipart create (`:442`), `CopyObject` (`src/IO/S3/copyS3File.cpp:748`)
  and the multipart-copy create (`:148`). Each takes the type from a new optional `WriteSettings` field, default
  unchanged. The CAS backend sets it in `blobPublicationWriteSettings` (`B:894-900`) for `writeObject`
  (`B:923-928`) and `copyObject` (`B:969-973`), and the staging sink sets it at `X:951`.
- **Copy.** Tested on MinIO `RELEASE.2025-04-22` and RustFS 1.0.3: a default-directive `CopyObject` keeps the
  source's type and ignores the request's; a multipart copy inherits nothing and takes the type of its create.
  AWS S3, GCS and Ceph are not tested; the probe decides per mount. The staging object, the copy request and the multipart create
  all use the one mark bit, so the result is the same either way. The copy stays on the default directive: no
  `object_metadata` is passed (`src/IO/S3/copyS3File.cpp:737-740`).
- **Read.** `HeadObject` already returns the type; it is dropped at `src/IO/S3/getObjectInfo.cpp:87-93`.
  `ObjectInfo`, `ObjectMetadata`, `Backend::RawMeta` (`CA/Backend/CasBackend.h:161`, filled at `B:230-238`) and
  `Meta` (`CA/Backend/CasWriteResult.h:29`) gain it. Token, size and mark come from one response.
- **Emulated and in-memory backends** keep a mark per object and replace it with the body ([§11](#open), Q3).
- **Probe**, in `runCapabilityProbe` (`CA/Backend/CasProbe.cpp:47`, called at `CA/Pool/CasPool.cpp:546`), with the
  disk's own client and the publication settings, on scratch keys:

| # | Step | Must read |
|---|---|---|
| P1 | marked `PutObject` | marked, exact value |
| P2 | marked multipart upload (one part) | marked |
| P3 | default-directive copy of a marked source | marked |
| P4 | multipart copy with a marked create | marked |
| P5 | unmarked `PutObject`; unmarked multipart upload | unmarked |
| P6 | unmarked `PutObject`, copy and multipart copy, each over a marked object | unmarked |

- **Preservation failure** (P1 to P4): the mount never learns: it publishes unmarked and never adopts. WARNING at
  mount, `blob_mark_probe` in `system.cas_mounts`; the switch command refuses on this disk.
- **False positive** (P5, P6): the switch command refuses. A writable mount of a switched pool fails:
  `the store reports the blob mark on an object published without it (<step>)`.
- `skip_access_check` skips the probe (`CA/Pool/CasPool.cpp:526`, `:550`): such a mount never learns.
- **What the probe cannot show.** It samples one client at one mount. It says nothing about another server's
  client or proxy, about a path it does not run, or about a later change of the store. It cannot show that no
  legacy body ever reads marked: a metadata-only edit of an existing object is outside it ([§7.7](#transition-risks), X4).
- No timestamp and no user metadata are used. `Last-Modified` errs in the dangerous direction: every copy tool
  resets it.

### 6.3 Source check {#source-check}

- **Rule.** Every streaming publication hashes exactly the payload bytes it forwards, with the pool algorithm in
  its streaming convention (`W:116-130`), and compares with the key's digest before the object is completed.
- **Seam.** `StreamingBlobPublication` gains the expected `BlobRef` (`CA/Backend/CasBackend.h:54-59`). The check
  sits beside the length check (`B:933-950`), before `out->finalize` (`B:951`): for S3 the completing request,
  `PutObject` or `CompleteMultipartUpload`, is issued inside `preFinalize` (`src/IO/WriteBufferFromS3.cpp:164`,
  `:192`, `:205`). Mismatch: `out->cancel`, `CORRUPTED_DATA`, nothing published. The emulated path checks before
  its rename (`B:505`).
- **Covers:** local staging, reopened by path for every attempt (`X:337-340`); S3 staging re-streamed
  (`X:308-332`); memory sources. So every overwrite of a present body, and every streamed first publication.
- **Verbatim staged copy** (`B:955-973`) is not hashed. It rests on A6 and on I1's once-only rule.
- Cost: no request; one more hash pass per streamed body.
- Outside the proof: hash collisions, corruption inside the store.

## 7. Transition {#transition}

One-time, upward only, in place, no data copy. Mixed versions on one pool stay unsupported.

### 7.1 Stages {#stages}

| Stage | Durable condition | Invariant |
|---|---|---|
| T0 | no mount of this build yet | today's protocol |
| T1 | floor 4; `blob_protocol` absent | Every delete authority is marker authority. Writers of this build never adopt, publish unmarked, write no marker. No intent and no marked body exists |
| T2 | `gc/state` at generation 4 | T1, and no older build can commit a GC round |
| T3 | `blob_protocol = intents` | (a) A marked token is under intent authority only. (b) No writer adopts an unmarked body. (c) No body is deleted without I5 |

- **T1 is safe with today's GC and today's writers.** A body without a marker, and a stale `Clean` or `Condemned`
  over a newer body, are states today's protocol already reaches after a writer dies. An older writer republishes
  a marker-less body once (`W:475`); marker-mode GC creates `Condemned` on an absent key
  (`CA/Gc/CasGcMetaWriter.cpp:48-49`) and graduates on it.
- **T3 (a).** Marked means published after the switch (I7). Marker authority on `t` needs two committed marker-mode
  rounds after `t` appeared: one that condemns, a later one that graduates (`N:559`). An older build cannot commit
  after the stamp, which precedes the switch. A round of this build that graduates took its mode after its lease
  (G1), so after the switch: intent mode. A round that sampled before the switch can condemn a marked token; the
  row is a candidate and goes through G3.
- **T3 (b).** W3. A late delete under marker authority names a pre-switch token; a writer that meets it publishes
  over it, and the delete misses.

### 7.2 Operator procedure {#procedure}

1. Stop every server of the pool, replace the binaries, start them: the order of the generation 3 upgrade
   (`D/operations/migration.md`, "Upgrading a pool to reader generation 3").
2. Check `system.cas_mounts`: every row not `retired` shows `protocol_generation = 4`, and `blob_mark_probe = 'ok'`
   on the server that will run the command.
3. Confirm that no process of an older build runs against the pool: servers, `clickhouse-disks`, tools.
4. Run `SYSTEM CAS ENABLE BLOB INTENTS '<disk>'` on one server.
5. Optional, later: `cas-fsck --fix legacy-markers`.

**Points of no return.** The first writable mount of this build raises the floor: older builds cannot mount, as
with generation 3. Step 4 changes the protocol: it cannot be undone, and the legacy branch W3 stays forever.

### 7.3 The switch command {#command}

`SYSTEM CAS ENABLE BLOB INTENTS '<disk>'`. Each step is monotone; a rerun after any failure completes it.

| Step | Action | Refuses when |
|---|---|---|
| 1 | Checks | the disk is not mounted writable; its mark probe did not pass; `min_reader_generation < 4`; a mount slot declares `protocol_generation < 4` or cannot be decoded, unless its owner is a verified retirement. The message names the `server_root_id`: `mount slot of server root '<id>' declares protocol generation <n>, need 4; start that server once on this build or remove the member` |
| 2 | Stamp: read-modify-write of `gc/state` to generation 4, every other field kept, created at 4 if absent | — |
| 3 | Switch: read-modify-write of `_pool_meta`: `blob_protocol = intents`, `token_dialect` = this backend's dialect | step 2 did not land |
| 4 | This server learns | — |

- Slots are found by exact reads for the server roots the catalog names, and by one `LIST` of `gc/server-roots/`
  for roots without namespaces. A slot left by an older build blocks until that server starts once on this build
  or is removed with `SYSTEM CAS DROP POOL MEMBER`.
- **The checks do not prove that no older writer exists.** A process that read `_pool_meta` before the floor was
  raised and then paused before claiming a brand-new root has no slot; a listing can omit a slot. Step 3 of the
  procedure carries that claim. The checks catch the common mistakes.
- The stamp does not need the GC lease. A round in flight loses its `If-Match` commit (`G:1326`) and retries; the
  leader's next renewal keeps the generation.

### 7.4 What each build does {#builds}

| Actor | T1 | T2 | T3 |
|---|---|---|---|
| Writer, this build | W2, W3, W6 unmarked | same | learned: W2 to W7, marked. Not learned yet: as T1 |
| GC, this build | marker mode | marker mode, keeps generation 4 | intent mode from the first round that reads `intents` after its lease |
| Writer, older build | cannot mount; one already mounted runs today's protocol | same | must not exist (A8) |
| GC, older build | runs while it holds a readable `gc/state` | refused at the lease read | refused |
| Tools, older build | refused at the floor | refused | refused |

**Refusals.**

| Who | Message |
|---|---|
| Older build, mount or read-only open | `CAS pool meta: pool requires reader generation 4 but this build supports at most 3` |
| Older build, GC lease read after the stamp | `CAS cas_gc_state: object requires reader generation 4 but this build supports at most 3` |
| Older build, epoch allocation on a latched root | `CAS server-epoch: critical key '!protocol_generation' is not understood by this build` |
| The command, a slot below 4 | `mount slot of server root '<id>' declares protocol generation <n>, need 4; start that server once on this build or remove the member` |
| The command, probe not passed | `blob mark probe failed on this disk: <step>` |
| Writable mount, dialect differs | `CAS pool records token dialect '<a>', this disk uses '<b>'` |
| Writable mount of a switched pool, false positive | `the store reports the blob mark on an object published without it (<step>)` |
| Intent-mode round, `gc/state` below 4 | `pool uses blob intents but gc/state is at generation <n>; rebuild it with this build` |
| Publication, digest differs | `CORRUPTED_DATA`: `payload of <key> does not hash to its key; upload aborted, nothing published` |
| `cas-fsck --fix legacy-markers`, pool not switched | `markers are protocol state until the pool uses blob intents` |

The first three texts are produced by existing gates (`CA/Formats/CasFormat.cpp:102-109`,
`CA/Formats/CasPoolMetaFormat.cpp:155-158`, `CA/Formats/CasTextFormat.cpp:319-321`); the rest are new.

### 7.5 Death in the middle, inherited state {#inherited}

| Point | State | Next |
|---|---|---|
| Between the stamp and the switch | generation 4, marker mode | older GC is out; rerun the command |
| Switch sent, answer lost | switched or not | settled by an exact read; if it lands later, the first round that reads it is in intent mode |
| A round of this build sampled before the switch | one marker-mode round after it | its deletes name rows committed before the switch; its condemnations are candidates |

| Object at the switch | Writer | GC |
|---|---|---|
| Legacy body, any marker | W3: re-upload on the first reuse with bytes | the intent pipeline when it reaches in-degree 0 |
| Candidate row | — | G3 creates the intent from the row's token |
| Pending row (no `intent`) | re-upload on reuse; the old delete then misses | G8: demoted, confirmed by G3, deleted a round later. Never executed on marker authority |
| `Clean` and `Condemned` markers | never read | removed with a legacy body (G4 (4)); the rest by F4 |

**Legacy is permanent.** Five paths add a reference with no local bytes and no request: hardlink of a committed
part (`X:1262`), adoption of a blob staged in the same transaction (`X:260`), and `prepareEntries`
(`CA/Parts/PartFolderAccess.cpp:447-470`) under `republishRef` (`:482`), `repointRef` (`:664`) and the relink
fetch (`CA/ContentAddressedMetadataStorage.cpp:2404`). A body kept alive only by mutations, renames and fetches is
never re-uploaded and stays unmarked. Its protection is the in-degree, as today.

### 7.6 Tables and tools {#tools}

| Surface | Change |
|---|---|
| `system.cas_mounts` (`src/Storages/System/StorageSystemContentAddressedMounts.cpp:40-63`) | `protocol_generation` per slot (0 for an older body); `blob_protocol` (`markers`, `intents`); `blob_mark_probe` for this server (`ok`, the failed step, `skipped`) |
| Server log | WARNING at the floor raise (exists); one line at the stamp, the switch and when a mount learns; ERROR on a dialect mismatch |
| `cas-inspect` | renders an intent, a body's mark, `blob_protocol`, `token_dialect`, a slot's `protocol_generation`, a row's `intent` (`CA/Tools/CasInspect.cpp:417`) |
| `cas-fsck`, `SYSTEM CAS FSCK` | classes `intent_current`, `intent_stale`, `intent_malformed`, `legacy_marker`; lists `blob-intents/` beside `blobs/` (`CA/Tools/CasFsck.cpp:892-913`); fix options of [§3.4](#fsck-rules) |
| `SYSTEM CAS GC REBUILD`, `cas-gc-rebuild` | [§3.5](#recovery) |

### 7.7 Residual risks {#transition-risks}

| # | Risk | Exposure |
|---|---|---|
| X1 | An older writer works after the switch: an operator mistake, a process paused before claiming a new root, a slot the listing omitted. It adopts a marked body on a stale `Clean` (`W:452`) while intent-mode GC deletes it: a committed ref to a deleted body | keys that still carry a decodable `Clean`; shrinks as G4 (4) and F4 remove markers. Not detected |
| X2 | `gc/state` deleted, then an older build bootstraps it (`G:5197-5211`) and runs marker GC over marked bodies | external manipulation, unsupported |
| X3 | Marked bodies under older control objects: control objects restored from before the switch, or marked bodies copied into an unswitched prefix. A marked token can sit under marker authority. G8 covers committed rows, not a delete already sent | unsupported |
| X4 | The mark set on a legacy body by a metadata-only operation while marker authority on its token is outstanding | external manipulation |
| X5 | A store or path that loses the mark | cost: no reuse; the probe reports it |
| X6 | No reuse by this build before the switch; one upload per reused legacy blob after it | cost; minutes with the procedure of 7.2 |
| X7 | A reader that pins the body `ETag` across ranged reads sees an overwrite. The payload offset and bytes do not change | not audited. Today the same overwrite happens on a condemned republish; here once per reused legacy blob |

## 8. Not done, and residuals accepted {#scope}

### 8.1 Not done {#not-done}

| Not done | Reason |
|---|---|
| A mutable per-hash marker that names the target token | fewer stored states, but conditional GC transitions on a shared object and an observation order to preserve; not model-checked. It has the same dependence on A1 |
| An inventory of bodies | its premise, "no edge in the run means unreferenced", fails on a rebuilt pool (CAS-33.3) |
| A GC circuit for row-less intents | recorded rate of pending rows spared on one production node: 117 in 16.8 days; a circuit can be added with no format change |
| `Last-Modified` as the classifier | resets on copy, restore and sync; multipart stamping differs per store |
| A new pool prefix | copies all data; the in-place transition needs no drain |
| Removing the legacy branch | [§7.5](#inherited) |
| Keeping a row on a spare | breaks the zero-I/O shard carry (`G:3714`) and forces a fold every round (`G:4402`) |
| The writer deletes an intent after its own upload | safe only on an unequivocal answer; a misread outcome loses data; saves under 40 bytes |
| A hash of the token as key segment; case folding | not injective; folding merges incarnations |
| Changing the first `HEAD` or adding conditional creation of bodies | decision-1 |

### 8.2 Residuals accepted {#residuals}

| Residual | Effect | Handling |
|---|---|---|
| A body that lands after the fold processed its writer's edge removal has no edge, no row and no intent | bytes leak until the content is written again and dies | existing gap, CAS-33.2, unchanged. This design promises reclamation only of a body whose transition to zero GC observes while the body is present |
| Row-less intents: spare of a pending row (by design), a failed round followed by a spare, a late or stale create, a supersede whose intent `DELETE` failed, a rebuild | over a live body: one upload by the next writer; over a dead token: under 40 bytes | F3 |
| Legacy markers left under marked bodies | 90 bytes each | F4 |
| The verbatim staged copy is not hashed | A6 | stated |
| Token form per store is taken from the client library | A5 | an integration test records it per store ([§10.2](#tests)) |

## 9. Effects {#effects}

### 9.1 Tasks and decisions {#backlog}

| Task | Effect |
|---|---|
| CAS-384, CAS-330, CAS-334, CAS-345, CAS-29.3, CAS-439 | moot: they exist for the writer's marker or its residuals |
| CAS-326 | moot after the switch: intents carry no round |
| DRAFT-8, CAS-30, CAS-331 | answered by this design: a lazy warning keyed by incarnation; no shared marker to protect |
| CAS-360 | reversed: an absent warning now adopts. This is its rejected option B over an immutable per-incarnation object; B failed on recurrence, which CAS-361 and CAS-400 (`a966a374b17`) close in code and A1 states as contract |
| CAS-361, CAS-400 | prerequisites, done. CAS-361's sentence on an empty result `ETag` is dropped: nothing consumes a publication token |
| CAS-197 | delivered by [§6.1](#identity), except the recorded token form per store |
| DRAFT-53 | delivered for streamed publications ([§6.3](#source-check)); open for the verbatim copy |
| CAS-33.2, CAS-33.3 | unchanged; CAS-33.2 records that the class is the same under both protocols |
| CAS-329 | its request cut is delivered differently (8 → 5); "delete without `HEAD`" stays open |
| CAS-29.4, CAS-336, CAS-284.5 | re-target from marker reads and deletes to intent creates and deletes |
| CAS-10, CAS-11, CAS-289.6 | fsck and GC byte classes: intents replace `.meta` |
| CAS-5, CAS-43 | emitters move: `BlobReuseResurrect` on "intent present"; the raced-condemn count is the spare of G6 |
| CAS-18, CAS-17, CAS-321.7 | re-measure; CAS-18's text "the protocol is fixed" is amended |
| CAS-187, CAS-238, CAS-240, CAS-375, CAS-175, CAS-231 | texts, comments and model guidance follow the new sequence |

| Decision | How this design answers it |
|---|---|
| decision-1, invariant 5 of `docs/superpowers/cas/AGENTS.md` | the `HEAD` before every `PUT` stays. A durable write step is removed and reads are added, as a protocol change with a model, not as an optimization. Record it there |
| decision-4 | generation 4 with an in-place upgrade path; older generations stay readable; no downgrade |
| decision-2 | a `LIST` only nominates |
| decision-7 | the body delete stays conditional; intent and marker deletes are unconditional single-key deletes of objects no reader depends on |
| "Mixed versions on one pool are not supported" (`D/configuration.md`) | kept, and now a stated precondition of the switch |

### 9.2 Tests {#test-effects}

| Suite | Deleted | Changed | Added |
|---|---|---|---|
| Writer gtests (`T/gtest_cas_part_write.cpp`, `gtest_cas_upload_detached.cpp`, `gtest_cas_s3_staging.cpp`) | `Clean` create, absent-marker republish, marker counters | adopt and republish fixtures, request shapes, attempt counts | [§10.2](#tests) |
| GC gtests (`gtest_cas_gc_meta_writer.cpp`, `gtest_cas_gc_round.cpp`, `gtest_cas_gc_read_ahead.cpp`) | none: marker mode stays | each marker-mode test pins `blob_protocol` absent | an intent-mode twin per schedule |
| `gtest_cas_blob_meta*.cpp` | none | — | — |
| `gtest_cas_fsck.cpp` | `BodyWithoutMetaIsBenign` on a switched pool | `MetaWithoutBodyIsAdvisoryNotHard` | intent classes, fix order |
| Integration | — | `test_cas_s3` (marker counters, `HEAD` per duplicate), `test_cas_gcs` (budget test; two tests that use `.meta` writes as their vehicle move to the intent create) | upgrade test |
| Stateless | — | `05485_cas_ref_append_requests` reference; fsck-zero tests rely on F5 | one request-count pin |
| `utils/cas-yardstick`, `utils/ca-soak` | — | cards `cycle`, `wide_insert`, `mutation`, `dup_content`, `attach_200` and the reference; soak cards S16 and S41 | — |

### 9.3 Docs and counters {#docs}

- `D/architecture/blob-protocol.md`: the sequence, the adoption rule, the writer-versus-GC race, request budget;
  section "The `.meta` sidecar" becomes the legacy note. `D/architecture/garbage-collection.md`: phases 9, 11, 12,
  the condemnation diagram, the cost tables. `D/architecture/storage-layout.md`: `blob-intents/`.
  `D/operations/migration.md`: "Upgrading a pool to reader generation 4" with 7.2 and 7.7 in plain words.
  `D/operations/monitoring.md`, `debugging.md`, `configuration.md`, `bucket-requirements.md` (the store must
  return `Content-Type` on `HEAD` and keep it on copy), `architecture/correctness.md`, `design-history.md`.
- Removed: `CASMetaCreateClean`, `CASMetaAbsentRepublish`, `CASMetaResurrectClean`
  (`src/Common/ProfileEvents.cpp:968-970`).
- Kept for marker mode: `CASMetaPut`, `CASMetaCompareSwap`, `CASMetaDelete`, `CASGCMetaOps`,
  `CASGCMetaWriteAnomaly`, `CASGCCondemnMarkerUnconfirmedCarry`; `cas_gc_meta_pool_size`.
- New: the four `CASBlobIntent*` request counters; `CASBlobReuseIntentPresent`, `CASBlobReuseLegacyRepublish`,
  `CASBlobReuseChangedAtSecondHead`, `CASBlobPublishDigestMismatch`; `CASGCIntentCreated`,
  `CASGCIntentConfirmedExisting`, `CASGCIntentUnconfirmedCarry`, `CASGCIntentDeleted`, `CASGCIntentLeftOnSpare`,
  `CASGCInheritedPendingDemoted`, `CASGCLegacyMarkerDeleted`.
- Changed meaning: `CASBlobPut`, `CASBlobGet`, `CASBlobHead` no longer include marker traffic; `CASBlobHead` is two
  per reused blob.

## 10. Verification {#verification}

### 10.1 TLA+ {#tla}

Required before implementation: `M/CaBlobIntentCore.tla` and `M/CaBlobIntentTransition.tla`, with a runner and a
results file, and a table "action → store requests it stands for" (CAS-375). No model of this design exists yet;
`M/CaBlobPublishCore.tla` and `M/CaGcCondemnMarkerGate.tla` stay as the record of today's protocol.

Model content:

- Every request is three actions: send, apply, respond. A sender may stop waiting; the apply may come later.
- Two GC actors with lease loss and lost commits; rows, cuts, spares, supersede, same-token re-condemnation.
- The create of G3 with the refused-create resolve read as its own action.
- The cleanup of F2 as two observations and a delete.
- Publication: fresh envelopes, content-derived and generation tokens, two absent publishers, a source that
  changes after staging, staging aliases.
- Transition: older and new writers, older and new GC, stamp and switch as separate actions, learned flags, mark
  bits set at staging, inherited rows, a paused pre-mount older actor, a linker that adds an edge from a live
  source edge with no observation.
- Read counters that are not reset on contention.

| Row | Expected |
|---|---|
| `safe_base`, `safe_base_generation` | green |
| `safe_late_put`, `safe_late_create`, `safe_late_intent_delete`, `safe_late_body_delete` | green |
| `safe_refused_create`, `safe_spare_recondemn`, `safe_lost_commit`, `safe_cleanup`, `safe_integrity`, `safe_budget` | green; `safe_budget` checks 1/1, 3/0 and 2/1 reads and `PUT`s on uncontended paths |
| `safe_transition`, `safe_stale_marker_round`, `safe_inherited_demoted`, `safe_unlearned_after_switch`, `safe_late_old_requests`, `safe_linker` | green |
| `alt_adopt_changed_token` | green; documents the alternative of [§4.3](#schedules) |
| `sab_no_second_head` (S1), `sab_cleanup_head_first` (S2), `sab_spare_deletes_intent` (S3), `sab_attempted_create_confirms` (S4), `sab_absent_conflict_confirms`, `sab_delete_on_graduation_cut`, `sab_ambiguous_delete_cleans_intent` (S9), `sab_cleanup_raw_spelling` (S8) | red on `INV_COMMITTED_REF_HAS_BODY`, and on `INV_INTENT_PERMANENT` or `INV_DELETE_EVIDENCE` where the rule breaks first |
| `sab_resend_envelope`, `sab_staging_alias_two_copies` | red on `INV_NO_RECURRENCE` |
| `sab_unchecked_stream` (S7) | red on `INV_KEY_NAMES_PAYLOAD` |
| `sab_delete_despite_indegree`, `sab_writer_deletes_intent`, `sab_candidate_from_intent` | red |
| `sab_adopt_unmarked`, `sab_mark_before_learned`, `sab_no_stamp`, `sab_mode_cached`, `sab_switch_with_old_writer`, `sab_inherited_executed`, `sab_lost_stamp_old_bootstrap` | red on `INV_COMMITTED_REF_HAS_BODY` or `INV_MARKED_NEVER_MARKER_AUTHORITY` |
| `witness_*`: adoption, publish over an intent, spare with a standing intent, re-condemnation on an existing intent, late create after its row left, stale intent cleaned, legacy re-upload, changed token at W5, a deposed leader's delete lands | each reachable |

Not modelled, to be covered by tests: token spelling beyond two renderings, multipart transport, the mark probe.

### 10.2 Tests {#tests}

Tests are written first. A `LOGICAL_ERROR` expectation is a death test in a `*DeathTest` suite.

| Layer | Tests | Pins |
|---|---|---|
| gtest, writer, on the etag-faithful fakes (`EtagFaithfulPublicationBackend`, `ContentEtagBackend`) | `CASBlobIntentWriter.FirstCreationIsHeadAndOnePut`, `.CleanReuseIsHeadGetHead`, `.IntentPresentPublishesFreshOnce`, `.LegacyBodyIsRepublishedMarkedWithNoMetaRequest`, `.UnlearnedWriterNeverAdoptsAndPublishesUnmarked`, `.ChangedTokenAtSecondHeadPublishesWithoutRestart`, `.BodyGoneAtSecondHeadPublishes`, `.LandedAmbiguousPublicationIsAdoptedInFourReads`, `.FailedIntentReadIsNotAbsence`, `.WriterNeverMutatesIntents`, `.MarkBitIsSharedByHardlinkCopies` | W1 to W8; S1; ledger |
| gtest, GC | `CASGcIntents.CondemnIsOneHead`, `.GraduationCreatesAndCommitsPending`, `.EqualOccupantConfirms`, `.AbsentOrDivergentOccupantCarries`, `.UnansweredCreateCarries`, `.DeleteIsThreeRequests`, `.FailedIntentDeleteFailsTheRoundAndTheNextFinishes`, `.UnansweredBodyDeleteKeepsTheIntent`, `.MismatchDeletesTheOldIntent`, `.SpareLeavesTheIntent`, `.RecondemnationConfirmsOnTheStandingIntent`, `.SupersedeDeletesTheOldIntentOnce`, `.InheritedPendingIsDemotedBeforeAnyDelete`, `.LegacyBodyDeleteRemovesItsMarker`, `.ModeIsReadAfterTheLease`, `.RebuildKeepsGenerationFourAndReadsNoIntent` | G1 to G9; S3 to S6, S9; 5 requests |
| gtest, identity and formats | canonical identity and each rejected form; intent key round trip, rejected spellings, key length; `BlobIntent` in the format battery; `_pool_meta`, `gc/state` generation kept by every writer, run row `intent`, slot and epoch fields; read ceiling 3 rejects each | [§2](#formats), [§6.1](#identity); S8 |
| gtest, source check and probe | `CASBlobPublishDigest.FlippedByteIsRefusedBeforeCompletion`, `.StaleSourceOfASecondAbsentPublisherIsRefused`, `.S3StagedRestreamIsVerified`; one probe test per row P1 to P6 with a fake that fails it | [§6.2](#mark-rules), [§6.3](#source-check); S7 |
| gtest, fsck | classes; `FixReadsTheIntentBeforeTheBody` with a hook that publishes between the reads; `FixRefusesLegacyMarkersOnAnUnswitchedPool` | F1 to F5; S2 |
| Stateless (`add-test`) | requests of one `INSERT` of new content (1 `HEAD`, 1 `PUT` per blob, no intent request), of a duplicate (2 `HEAD`, 1 intent `GET`, no `PUT`), and of a duplicate after `DROP` and two GC rounds | ledger |
| Integration, RustFS and the GCS emulator | the mark on each publication path, single and multipart; the recorded token form on `HEAD`; an upgrade test like `test_cas_ref_move_compat`: an older image writes, stop, new image, the command refuses a slot below 4, switch, an older server is refused, a legacy blob is uploaded once and then reused with three reads; `test_cas_gc_s3`: 5 requests per reclaimed blob | [§6.2](#mark-rules), [§7](#transition) |
| Soak, 2 hours, with process kills | fsck: zero dangling refs; `intent_stale` does not grow without bound; no `CASBlobPublishDigestMismatch` | I8 |

### 10.3 Measurements {#measurements}

- Request ledger per `INSERT`, new and duplicate content, before and after.
- Yardstick cards with a new reference.
- A wide insert like the stand's, before and after: marker `PUT`s (54,997 → 0), upload thread time, wall time. The
  request saving is not a wall-time claim until measured.
- A fully duplicated second insert on a legacy pool (one upload per blob), and a third (three reads per blob).
- GC requests per reclaimed blob in both modes. CPU of the publication hash on a large body.

## 11. Open questions {#open}

| # | Question | Default if nobody answers |
|---|---|---|
| Q1 | Should the floor raise to 4 wait for the switch command, so the binary upgrade stays reversible until then? | No: every writable mount raises it, as generation 3 does |
| Q2 | Should the verbatim staged copy carry a source-token precondition (`x-amz-copy-source-if-match`) instead of resting on A6? | No precondition: it needs the staging object's token, which costs a `HEAD` or new plumbing |
| Q3 | Where does the emulated backend over `LocalObjectStorage` keep the mark? It ignores attributes (`src/Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.cpp:575`) | A `user.` extended attribute set on the temporary file before the rename (`B:505`); a file system without it fails P1 and the disk runs without reuse |
| Q4 | Does the tightened token grammar apply to every key or only to blob keys? | Every key: one grammar in `isIncarnationValue` |
| Q5 | Is `SYSTEM CAS ENABLE BLOB INTENTS` the command name, and is an offline twin needed? | That name; no offline twin, because the command needs the mount's probe result |
| Q6 | Does a pool created by this build start switched? | Yes: `blob_protocol = intents`, `token_dialect` and `gc/state` at 4 from creation, when the probe passes; otherwise it starts unswitched and reports the probe |

## 12. Deviations and findings {#findings}

Places where a requirement did not map onto the code as worded, and what this spec does instead.

| # | Finding | What the spec does |
|---|---|---|
| D1 | The content type is hard-coded at four sites, not only in the copy code: `src/IO/WriteBufferFromS3.cpp:442`, `:775` and `src/IO/S3/copyS3File.cpp:148`, `:748`. `HEAD` drops the type at `src/IO/S3/getObjectInfo.cpp:87-93`, and `ObjectMetadata` has no field for it. The mark therefore changes shared S3 code, outside `CA/` | one optional `WriteSettings` field and one optional metadata field, defaults unchanged ([§6.2](#mark-rules)) |
| D2 | A run row cannot tell an intent-confirmed pending row from an inherited one: marker mode writes `pending` and `confirmed` together (`N:577-578`). Demotion needs a discriminator | row field `intent`, a `cas_run` change at generation 4 ([§2.3](#control-formats), G8) |
| D3 | "GC removes a marker together with its row": a row does not record whether a marker exists, and a marked body can sit under a stale `Clean` | GC deletes `<blob>.meta` only when the body it removed was unmarked; 6 requests for a legacy body, 5 otherwise (G4) |
| D4 | "The disk stays on the old protocol" after a failed mark probe: this build's writer has no marker protocol to stay on | such a mount never learns: it publishes unmarked and never adopts ([§6.2](#mark-rules)) |
| D5 | "A false-positive result refuses the protocol" does not say what a mount of an already switched pool does | the command refuses; a writable mount of a switched pool fails ([§6.2](#mark-rules)) |
| D6 | The check "before `preFinalize`" has no seam of that name in the CAS backend: `publish` calls `finalize` (`B:951`), which issues the completing request | the check sits before `finalize`, beside the length check (`B:940`) |
| D7 | `cas-fsck` has no mutating mode today (`runFsck`, `CA/Tools/CasFsck.h:268-270`) | F3 and F4 add the first two |
| D8 | The emulated backend has nowhere to keep a mark | Q3 |
| D9 | A GC round does not read `_pool_meta` today; a writer does not re-read it after mount (`CA/Pool/CasPool.cpp:630-631`) | one `GET` per marker-mode round (G1); a 60 s re-read while not learned ([§3.1](#learned)) |
| D10 | The stamp needs `gc/state` to carry its generation through every writer; `encodeGcState` writes the write floor today (`CA/Formats/CasGcStateFormat.cpp:42`) | `GcState::generation` ([§2.3](#control-formats)) |
| D11 | No TLA+ model of the write-ahead shape, of the cleanup as separate reads, or of the transition exists | [§10.1](#tla) lists them as required, none as done |
| D12 | The supersede branch compares tokens textually (`N:616`) and has no intent step | canonical compare and one best-effort intent `DELETE` (G7) |
