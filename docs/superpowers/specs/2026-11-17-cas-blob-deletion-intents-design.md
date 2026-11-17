---
description: 'Design for CAS blob publication without the per-blob `Clean` marker. A writer publishes a body with one `PUT` and writes nothing else. GC alone warns writers, with an immutable deletion intent keyed by the body incarnation, created only when a committed condemned row graduates. A writer reuses a body after `HEAD`, intent `GET`, `HEAD`. Bodies of the new protocol carry one `Content-Type` mark; a legacy body is re-uploaded on its first reuse. Ships as format generation 4: an operator command switches an upgraded pool, and a pool created by this build switches itself.'
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
  `S3:` = `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp`.
- Terms. **Body**: the object at a blob key `B`. **Incarnation** `t`: the canonical identity ([§6.1](#identity)) of the
  body now at `B`. **Intent** `I(B,t)`: the object of [§2.1](#intent-object). **Marker**: today's `<blob>.meta`.
  **Marked**: a body whose `HEAD` shows the mark of [§2.2](#mark). **Legacy**: an unmarked body. **Edge**: a precommit
  or committed ref whose manifest names `B`. **Cut**: the reference snapshot one GC round folds.
  **Learned**: [§3.1](#learned). **Marker mode**, **intent mode**: the two GC modes of [§3.3](#gc-rules).
  **Switched**: a pool whose `_pool_meta` has `blob_protocol = intents`. **Answered**: the store returned a result,
  success or error. **Refused**: an answered error, such as 403. **Unanswered**: no result inside the request
  policy.

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
   kept only for `If-Match`. Every actor that uses identities first checks the pool's recorded token dialect.
7. **Verified streaming.** Every streaming publication hashes the payload it sends and refuses before the object
   is completed.
8. **Generation 4, one switch.** `_pool_meta` gets `blob_protocol = intents` once no older build can commit a GC
   round. On an upgraded pool the operator runs `SYSTEM CAS ENABLE BLOB INTENTS` after every older build is
   stopped: it stamps `gc/state` generation 4, then switches. A pool created by this build never had an older
   member: its first writable mount switches it before admitting a writer, or fails.

**Why.** The marker is paid by every blob and read by few. On a stand, one `INSERT ... SELECT` of 201.6 M rows into
a 109-column `ReplicatedMergeTree` wrote, on one replica (`system.blob_storage_log`): 54,997 bodies in 2,047
thread-seconds and 54,997 markers in 1,325 thread-seconds; ref logs and manifests took 50. The marker is 39 % of
upload thread time and 49 % of all `PUT`s (54,997 of 111,486). It bought 7,029 avoided body uploads.

**Cost.**

- Adoption now depends on body tokens never recurring at a key ([§5](#assumptions), A1), as today's exact-token
  delete already does (`B:889-893`; contract at `CA/Backend/CasBackend.h:120-139`). Today's writer never trusts
  absence: an absent marker means republish (`W:473-475`), and only a `Clean` marker read after the `HEAD` adopts
  (`W:446-452`). The new rule adopts on an absent intent, which is sound only while the intent is keyed by an
  incarnation that names one body (I1, I4).
- Clean reuse costs 3 reads instead of 2.
- A legacy body is uploaded again on its first reuse: once per reused blob, `k` times for `k` concurrent writers.
- Writers of the new build do not deduplicate until the switch.
- A second kind of object (`blob-intents/`), generation 4, an operator step on upgraded pools, no downgrade.

**Budget**, per blob and writer attempt on the successful uncontended path: first creation one `PUT` and at most
3 reads; clean reuse reads only; reuse after condemnation at most 2 `PUT`s and 3 reads. The ledger meets it;
contended and ambiguous paths are counted apart.

**Request ledger.** Counted from the code, not measured. Three kinds of request are kept apart:

- **Protocol operations** on the blob key and its intent: `HEAD B`, `GET I`, one publication of `B`; for GC `HEAD B`,
  `DELETE B`, intent `PUT`, intent `DELETE`. The budget and the first columns count these.
- **Publication transport.** One publication is one `PutObject`; or a multipart upload (create, parts, complete)
  above `max_single_part_upload_size`, 32 MiB by default (`src/IO/S3Defines.h:26`); or one `CopyObject`; or a
  multipart copy above `max_single_operation_copy_size`, 32 MiB by default (`src/IO/S3Defines.h:34`). Each counts
  as one `PUT`.
- **Other physical requests**, not on the blob key. A server-side copy first reads the size of the staging object
  with one `HEAD` (`S3:1115`, `src/IO/S3/getObjectInfo.cpp:203`). A streamed publication from S3 staging reads the
  staging object with one `GET` (`X:308-332`). Listed in the last column.

Writer, per blob and attempt, successful and uncontended:

| Case | Today | New | Other physical requests |
|---|---|---|---|
| First creation, streamed | `HEAD`, body `PUT`, marker `PUT`: 1 read, 2 `PUT` (`W:424`, `:509`, `:368`) | `HEAD`, `PUT`: 1 read, 1 `PUT` | S3 staging: 1 staging `GET` |
| First creation, staged copy | same | same | 1 staging `HEAD`: 2 reads in all |
| Clean reuse | `HEAD`, marker `GET`: 2 reads (`W:424`, `:446`) | `HEAD`, intent `GET` (404), `HEAD`: 3 reads | — |
| Reuse after condemnation | `HEAD`, marker `GET`, body `PUT`, marker `PUT`: 2 reads, 2 `PUT` (`W:473`, `:373`) | `HEAD`, intent `GET` (200), `PUT`: 2 reads, 1 `PUT` | S3 staging: 1 staging `GET` |
| Body present, marker absent | `HEAD`, marker `GET` (404), 2 `PUT` (`W:475`) | not a case: an absent intent adopts | — |
| Legacy body, writer not learned, or a token that does not canonicalise | — | `HEAD`, `PUT`: 1 read, 1 `PUT`; for a legacy body once per blob after the switch | S3 staging: 1 staging `GET` |

Writer, contended and ambiguous (new protocol), by starting state:

| Starting state and event | Requests |
|---|---|
| Marked body, learned, no intent; the second `HEAD` shows another token or no body | 3 reads, 1 `PUT`. No restart |
| Body absent; the publication is unanswered and landed; mark bit set, writer learned, no intent appears | `HEAD`, `PUT`, `HEAD`, `GET`, `HEAD`: 4 reads, 1 `PUT` |
| Same, mark bit unset (staged before learning) | the landed body is unmarked and is never adopted: `HEAD`, `PUT` per attempt |
| Body absent; the publication is unanswered and did not land | `HEAD`, `PUT` per attempt: 2 reads, 2 `PUT` after two attempts |
| Legacy body; unanswered publications | `HEAD`, `PUT` per attempt |
| Marked body with an intent; unanswered publications | `HEAD`, intent `GET`, `PUT` per attempt |
| Any | at most 8 attempts inside one request window (`W:406`, `:296`) |
| `k` writers on one absent or legacy body | 1 `HEAD` and 1 `PUT` each: `k` uploads |

GC, per reclaimed blob. Round overhead is outside these numbers: the lease read-modify-write, the `_pool_meta`
`GET` of G1 (new, one per round), run and seal objects, the `gc/state` commit.

| Step | Today | New |
|---|---|---|
| Condemn | `HEAD`; marker `GET` + `PUT` (`G:2194`, `CA/Gc/CasGcMetaWriter.cpp:45-51`): 3 | `HEAD`: 1 |
| Graduate | marker `GET` (`G:2271`): 1 | intent `PUT` with `If-None-Match`: 1; plus 1 `GET` when the intent already exists. An unanswered create adds a resolve read and may be reissued by the request policy (`Q:1097-1150`) |
| Delete | `HEAD` + `DELETE` (`G:591-593`); marker `GET` + `DELETE` (`CA/Gc/CasGcMetaWriter.cpp:59-62`): 4 | `HEAD`, body `DELETE`, intent `DELETE`: 3. The intent `DELETE` is at most one request: rows of a round share batch requests ([§3.3](#gc-rules)). A legacy body adds a marker `HEAD` and `DELETE`: 5 |
| Total | 8 | 5 (7 for a legacy body) |
| Body absent or replaced at the delete | 1 or 3 | `HEAD`, intent `DELETE`: 2. No body `DELETE` |
| Supersede (G7) | `HEAD`: 1 | `HEAD`, and 1 intent `DELETE` when the row had `intent` |
| Blob referenced again before graduation | 3, and a `Condemned` marker that forces one upload | 1, and no intent |

## 2. Objects and formats {#formats}

`G_BLOB_INTENTS = 4`; `G_BUILD` 3 → 4 (`CA/Formats/CasFormat.h:16`); `mount_reader_floor` → `G_BLOB_INTENTS`
(`CA/Pool/CasPool.h:450`). Frozen and unchanged: the blob envelope, manifests, ref logs, the `.meta` codec
(`FormatId::BlobMeta`, used only by marker-mode GC and the tools). Valid field values and the behaviour on
anything else: [§3.6](#failure-contract).

### 2.1 The intent object {#intent-object}

- **Key.** `<pool>/blob-intents/<algo>/<hex[0:2]>/<hex>/<dialect>/<tag-hex>`. The first four segments mirror
  `Layout::blobKey` (`CA/Formats/CasLayout.cpp:35-38`); `<dialect>/<tag-hex>` is the canonical identity
  ([§6.1](#identity)). New `Layout::blobIntentKey(ref, identity)`, `blobIntentsPrefix`, `parseBlobIntentKey`.
- **Parse.** `parseBlobIntentKey` accepts only the spelling `blobIntentKey` produces: known algo word, shard equal
  to `hex[0:2]`, dialect from `etag | generation | emulated`, lowercase even-length hex that decodes to a tag the
  grammar of [§6.1](#identity) accepts for that dialect. Anything else is not an intent.
- **Length.** The tag cap of [§6.1](#identity) counts the bytes of the opaque tag, after the quotes are removed:
  128 for `etag` and `emulated`, 20 for `generation`. The key is at most 14 + 6 + 4 + 64 + 1 + 10 + 1 + 256 = 356
  bytes after the pool prefix. `blobIntentKey` throws `LOGICAL_ERROR` on a complete key over 1024 bytes. The
  switch command, the sequence of [§7.4](#fresh-pool) and a writable mount of a switched pool refuse a pool
  prefix longer than 1024 − 356 bytes, so it cannot fire. An unswitched pool with a longer prefix keeps mounting.
- **Bytes.** Constant: the header line and an empty body object, `{"type":"cas_blob_intent","v":4}\n{}\n`
  (under 40 bytes). No round, no epoch, no size, no token: the key is the identity. A constant makes the confirmation of
  [§3.3](#gc-rules) G3 a byte comparison.
- **One body, many lives.** An intent is created with `If-None-Match`; every object ever at an intent key is
  that byte string. The key can be deleted and created again: a late create, a new condemnation of `t` after F3.
  `Layout::writeOnceBlobIntentKey(ref, identity)` still mints a `WriteOnceKey`, the capability for a delete with
  no precondition ([§3.3](#gc-rules)): the delete is sent only when `t` is gone for good (I4, I1), so it is
  right for every life of the key, a recreated one included. The contracts of `WriteOnceKey`
  (`CA/Primitives/CasWriteOnceKey.h:10-14`) and `removeManyWriteOnce` (`CA/Backend/CasBackend.h:197-202`) widen
  from "the one body the key ever held" to "one byte string in every life, and a delete that stays right".
- **Registry.** `FormatId::BlobIntent = 26`, type `cas_blob_intent`, `TextFamily::Control`,
  `CompressionPolicy::PinnedRaw` (bytes are compared), object cap 4 KiB, line cap 1 KiB; change points `{4,4}`
  (`CA/Formats/CasFormat.cpp`, `TRAITS` and `changePoints`). A later generation must keep accepting this record as
  a confirmation.
- **Request class.** New `CasNs::BlobIntent` in `classifyCasNs` (`CA/Backend/CasInstrumentedBackend.cpp:119-136`),
  tested before the blob rule: `CASBlobIntentGet`, `CASBlobIntentPut`, `CASBlobIntentDelete`, `CASBlobIntentList`.

### 2.2 The body mark {#mark}

- `Content-Type: application/x-clickhouse-cas-blob-v2`, compared exactly. Any other value, or none, is unmarked.
  `v2` names the protocol: `markers` is 1, `intents` is 2.
- The mark is part of the object and uses no user metadata: an unconditional `PUT` or copy replaces it.

### 2.3 Pool meta, `gc/state`, run rows, mount slot, epoch counter {#control-formats}

| Object | Change | Gate |
|---|---|---|
| `_pool_meta` | `min_reader_generation = 4`, raised by every writable mount as today (`CA/Pool/CasPool.cpp:628-640`, `:904`). New keys: `blob_protocol` (absent = markers; `intents`), monotone; `token_dialect` (`etag`, `generation`, `emulated`), written once, in the write that sets `intents`; `created_generation` (`4`), written only at creation by this build ([§7.4](#fresh-pool)). `PoolMeta` carries all three, so every read-modify-write keeps them (`CA/Pool/CasPoolMeta.cpp:69-86`) | additive keys: an unknown key is skipped (`CA/Formats/CasPoolMetaFormat.cpp:141`); older builds stop at the floor first (`:155-158`) |
| `gc/state` | `GcState` gains `generation`, decoded from the header `v`. Its default is 4, so only a decode yields a lower value and no construction site can write header 3. `encodeGcState` writes it with the explicit-version header writer (`CA/Formats/CasTextFormat.h:153`), not the write floor it stamps today (`CA/Formats/CasGcStateFormat.cpp:42`); the decoder is `:60`. Change point `{4,4}`. The stamp sets it to 4 | header 4 |
| `cas_run` condemned row | new bool `intent` beside `pending` and `confirmed` (`CA/Formats/CasRecordStreamFormat.cpp:198-205`, `:290-298`). In the packed row it is flag value 4, and the check `flags > 3` widens to `flags > 7` (`N:266`, `:287`). Written `true` only by an intent-mode graduation, with `pending = true` and `confirmed = false`: no marker was confirmed, and nothing reads `confirmed` (`CA/Gc/CasBlobInDegree.h:41-42`). A run written by an intent-mode round has an explicit header 4; a marker-mode run keeps today's header. Change point `{4,4}` | header 4 |
| Mount slot (`cas_mount_lease`) | new `protocol_generation`, 4 in every body this build writes | additive: older decoders skip it (`CA/Formats/CasServerRootFormats.cpp:223-224`) |
| `epoch` counter (`cas_epoch`) | critical key `!protocol_generation`, written with the allocation of a writable mount | an older build fails the allocation with `UNKNOWN_FORMAT_VERSION` (`CA/Formats/CasTextFormat.cpp:315-322`) |

### 2.4 What an older build does {#older-builds}

| It meets | Result |
|---|---|
| `_pool_meta` at floor 4, at mount or read-only open | refused at the floor ([§7.5](#builds)) |
| Floor 4 while already mounted, or in a tool that already opened the pool | keeps running on the copy it read; cannot remount. Nothing revokes it: [§7.2](#procedure) step 3 covers servers and tools |
| `gc/state` at header 4 | its lease read-modify-write decodes and throws (`G:5214`): no round starts. A round already past that read fails its `If-Match` commit (`G:1326`); its deletes went out before the commit |
| A `cas_run` at header 4 | rejected at the run header (`CA/Formats/CasRecordStreamFormat.cpp:155`). Only an intent-mode round writes one, and by then `gc/state` already refuses the older GC |
| A mount slot with `protocol_generation` | skips the key. If it rewrites the slot, the key is lost: the encoder writes only the fields it knows (`CA/Formats/CasServerRootFormats.cpp:155`). The slot then declares 0 and blocks the command, which names the root |
| The epoch counter with `!protocol_generation` | cannot allocate an epoch for that root (`CA/Formats/CasServerRootFormats.cpp:146`, `CA/Formats/CasTextFormat.cpp:319`) |
| A marked body | ignores `Content-Type`. With no marker at the key it republishes, unmarked, and writes `Clean` (`W:475`, `:542`). With a stale `Clean` at the key it adopts (`W:452`): [§7.8](#transition-risks), X1 |
| An intent | its writer and GC never read the prefix; its `cas-inspect` answers `cas-inspect: unrecognized key layout '<key>'` (`CA/Tools/CasInspect.cpp:562`) |

## 3. Protocol {#protocol}

### 3.1 Learning the switch {#learned}

A writer has **learned** the switch when all hold for its `Pool`:

1. It read `_pool_meta` with an exact `GET` and found `blob_protocol = intents`.
2. The `token_dialect` recorded there equals its backend's dialect.
3. Its mark probe passed, the token form included ([§6.2](#mark-rules)); or the pool has `created_generation = 4`
   and the probe was skipped or found only false presence ([§7.4](#fresh-pool)).

- Read points: at mount; then, while (1) is false, a re-read by the lease renewer every 60 s, and one when a
  writer meets a marked body (at most once per 10 s); at once on the server that ran the switch.
- Monotone: once learned, never unlearned, never re-read. A stale answer is the conservative one.
- A read that fails, or that does not decode: not learned; the next read point tries again.
- (2) false: the dialect rule ([§6.1](#identity)). (3) false: [§6.2](#mark-rules).
- A mount that is not learned compares no identity: it publishes unmarked and never adopts.
- **Mark bit.** One bool per pending blob, `PendingBlob::marked` (`CA/ContentAddressedTransaction.h:157-166`),
  set to "learned" when the staging sink is created (`X:939-975`), copied with the record (`X:252`). Every
  publication of that blob uses it. The path to each request is in [§6.2](#mark-rules).

### 3.2 Writer {#writer-rules}

Replaces `PartWriteTxn::ensureBlobPresent`, `W:274-570`. The loop, its single frozen deadline and the cap of 8
attempts stay (`W:296`, `:406`, `:415-423`).

| # | Step | Outcome and what follows | Replaces |
|---|---|---|---|
| W1 | The precommit edge is durable before the first request | else `LOGICAL_ERROR`, unchanged | `W:277-281`, order at `X:472-474` |
| W2 | `HEAD B` | Absent → W6, first-publication form. Present → check the size against the envelope length and the source (`CORRUPTED_DATA` exception, unchanged) and take token and mark from this response | `W:424-444` |
| W3 | **Not adoptable:** body unmarked, or writer not learned, or (marked and learned) a token that does not canonicalise ([§6.1](#identity)) | → W6. No other request. Not learned and marked: also trigger the re-read of [§3.1](#learned). A rejected token is counted (`CASBlobReuseTokenRejected`) and logged, one ERROR per mount; it is never an exception | `W:446-475` |
| W4 | Body marked, writer learned, token canonical: `GET I(B,t)` | Any object at the key → W6, event `BlobReuseResurrect`. An answered 404 → W5 | marker `GET`, `W:446` |
| W5 | `HEAD B` again | Same canonical identity and valid size → adopt: `Materialized`, `CASBlobBodyPutAvoided`, event `BlobReuseAdopt`. Absent, another identity, or a token that does not canonicalise → W6, once, no return to W2. The same identity with an invalid size → `CORRUPTED_DATA` exception | `W:452-471` |
| W6 | Publish: one unconditional publication (a body is never created conditionally), sent once (`W:299`, `:509`), under a fresh envelope (`W:322-331`), marked iff the mark bit is set, digest-checked ([§6.3](#source-check)) | Answered success → `Materialized`. Unanswered → next loop iteration from W2 with the backoff (`W:511-540`). Answered failure → exception | `W:478-509` |
| W7 | After W6 | No request. The marker reconcile is deleted | `W:352-404`, `:542` |
| W8 | **Reads go to the store.** W2 is sent after the edge of W1, W4 after the answer of W2, W5 after the answer of W4, each through `op.head` or `op.read` (`Q:601-609`) | No cache answers them, and no step takes the answer of a request sent before the event it must follow | — |

- **A read without a usable answer.** A `HEAD B` or `GET I(B,t)` that is refused or unanswered is an exception:
  the part write fails and nothing is adopted. It is never read as absence.
- **Verbatim staged copy** (`W:482-488`) is selected only for the first publication of the staging object, and
  only when W2 answered absent. Every other publication streams. The once-only rule serves A1. The absent
  answer keeps the one unhashed publication ([§6.3](#source-check)) off a body the writer saw. It does not
  prove the key empty at the copy: a body another writer lands in between is replaced, and I2 then rests on A6.
  Today the copy also runs over a present body whose marker is absent (`W:425`, `:475`, `:482`); that ends.
- Admission checks before each proof stay (`W:454`, `:466`, `:545`, `:562`).
- The writer contains no marker code: `loadMeta`, `reconcileMetaClean`, `validateMetaSize`, `meta_key` go.

### 3.3 GC {#gc-rules}

| # | Rule | Replaces |
|---|---|---|
| G1 | **Mode and dialect.** After the round acquired or renewed its lease (`G:787`) and before any other step, it reads `_pool_meta` with an exact `GET`. Answered and decoded without `blob_protocol` → marker mode for this round: today's code, unchanged. `blob_protocol = intents` → intent mode for this round; it requires `gc/state` at generation 4, else the round fails. `intents` with a `token_dialect` other than the backend's: the round fails and this mount leaves GC (below). A 404, a refused or unanswered read, a body that does not decode, an unknown word: the round fails. A failed round runs no fold and sends no request after the read. No outcome falls back to marker mode. Every round reads again | the cached `store->poolMeta` |
| G2 | **Condemn.** Trigger unchanged: a fresh transition to in-degree 0 (`N:637-648`). `HEAD B` (`G:2194`). Present → a non-pending row with the observed token, size and round; in intent mode only for a token that canonicalises ([§3.6](#failure-contract)). Absent → no row. No other request | marker write, `G:2227` |
| G3 | **Graduate.** For a row an earlier round committed (`condemn_round < current_round`), not pending, with in-degree 0 in this round's cut, when destruction is not suppressed and the budget admits (`N:559-571`): `PUT I(B,t)` with `If-None-Match: *` and the canonical record (`op.create`). Confirmed → the row gets `pending` and `intent` in this round's run and this round's commit (`G:1326`); nothing is deleted in this round. Not confirmed → the row is carried unchanged, counted, the round continues | marker read, `G:2264-2287`; `N:572-583` |
| G4 | **Delete.** For a row read with `pending` and `intent` from the adopted state, after this round's cut showed in-degree 0 (`N:545-557`). (1) `HEAD B`. (2) Absent → `Gone`. Canonical identity equals the row's → `DELETE B If-Match <raw token of this HEAD>` (`op.remove`): `Removed`, `Gone` or `Mismatch`. Another identity → `Mismatch`, no body request. (3) After any answered outcome of (2): delete `I(B,t)` with the write-once verb below; an absent key is success. (4) If (2) removed an unmarked body: delete `<blob>.meta` with `op.removeCurrent`, best effort, failures counted. (5) The row leaves the state only after (2) and (3) both answered. After `Mismatch` it is dropped, not superseded, as today (`G:601-603`) | `G:583-595`; marker delete `G:634-640` |
| G5 | **A delete job without a usable answer throws.** That is an unanswered or refused `HEAD`, body `DELETE` or intent `DELETE`. The round fails before its commit, the row stays pending (`G:752-762`), and the next round starts again at (1). A body `DELETE` without an answer never leads to the intent `DELETE` | — |
| G6 | **Spare.** In-degree above 0 (`N:520-543`): the row is dropped. No request. An intent stays where it is | — |
| G7 | **Supersede.** A touched row at in-degree 0 whose `HEAD` shows another canonical identity `u` (`N:612-631`): the row becomes a candidate for `(B,u)`. A row token that does not canonicalise never equals a live token, so a canonical `u` supersedes it too ([§3.6](#failure-contract)). If the old row had `intent`: one delete of `I(B,t)` with the write-once verb, best effort; a failure is counted (`CASGCIntentDeleteFailed`). A row without `intent` gets no delete, though an earlier round may have left `I(B,t)`: a failed round, a spare of a pending row, a late create. Over a superseded token that intent protects nothing, F3 removes it, and removing it here would cost a request per supersede | `matches` at `N:616` |
| G8 | **Inherited rows.** In intent mode a row with `pending` and without `intent` is a candidate: G3 applies, G4 does not. The demotion is decided before the pending branch of `settleEntry` (`N:545`) and before read-ahead picks its keys | [§7.6](#inherited) |
| G9 | **Never.** GC never deletes an intent on a spare, never derives a row from an intent, never reads or writes `.meta` in intent mode except G4 (4) | — |
| G10 | **Reads go to the store.** The `GET` of G1 and the `HEAD`s of G2 and G7 are sent in this round, after its lease; a read-ahead `HEAD` of the same round qualifies. The `HEAD` of G4 is sent after the cut. No cache answers them | — |

- **Order inside one round.** G1 follows the lease. G2, G3, G6, G7 and G8 run in the fold: G3 in `settleEntry`
  (`N:559-583`), G7 in `closeBlob` (`N:612-631`). G4 runs in the delete batch after the fold (`G:659-762`).
- **After `Mismatch`.** The body now at `B` was published by a writer that met the intent. Its edge protects it,
  and a later transition to zero makes a new row (G2). If it landed without an edge it is the residual of
  [§8.2](#residuals).
- **A `HEAD` without a usable answer** at G2 or G7 fails the round before its commit.
- **A mount with another dialect leaves GC.** Neither dialect changes during a mount, so the first round that
  fails the dialect check is the last: the background loop writes no further lease and no heartbeat
  (`CA/Gc/CasGcScheduler.cpp:522-526`), and `SYSTEM CAS GC RUN` there is refused with the dialect text. So is
  `SYSTEM CAS GC REBUILD`: it makes the same check on its own `_pool_meta` read, before its lease (`G:4660`).
  The lease stops moving, so another member's loop takes it after its two observations (`G:5244-5247`). One
  ERROR: `CAS gc: pool records token dialect '<a>', this disk uses '<b>'; GC stops on this mount and its lease
  is left to expire`. No slot format change.

**G3: what confirms.** The gate consumes the result of its own create:

| Result | Confirmed |
|---|---|
| `Committed`: an answered success, or an unanswered attempt whose resolve read found equal bytes (`Q:1127-1128`) | yes |
| `Conflict` with an observed `Object` whose bytes equal the canonical record. A clean refusal returns `Conflict`, not `Committed` (`Q:1118-1119`); `create` resolves with a body read (`Q:1154-1158`) | yes |
| `Conflict` with `ProvenAbsent`, `NotObserved` or other bytes; `GaveUp`; any exception | no |
| A request that was sent, a job that finished, a drained queue | no. Today's marker worker counts a failed job as completed (`CA/Gc/CasGcMetaWriter.cpp:91-99`) |

**G4, G7, F3: the delete verbs.**

- **Intent.** Keys are minted by `Layout::writeOnceBlobIntentKey` ([§2.1](#intent-object)) and deleted by
  `removeChunkWriteOnceOrOneByOne` (`G:541-556`) over `CasOperation::removeManyWriteOnce` (`Q:634`). That is one
  batch request for up to `bulkDeleteChunkKeys` keys, or one `DELETE` per key on a store without batch delete.
- **Policy.** `Retry::standard()`. A reissue resends the chunk. An absent key is success
  (`CA/Backend/CasBackend.h:197-202`). An exhausted policy or a refusal throws.
- **Batching in G4.** The body step runs per row in the delete pool, as today. The intent keys of the rows whose
  body step answered are then deleted in chunks. If a chunk throws, G5 applies to the batch.
- **A refusal that persists** (no delete right on `blob-intents/`) fails every round at that row, like a refused
  body delete today.
- **Marker.** `.meta` is rewritten in place, `Clean` to `Condemned` (`CA/Gc/CasGcMetaWriter.cpp:45-51`), so it has
  no path to `WriteOnceKey`. G4 (4) and F4 use `CasOperation::removeCurrent` (`Q:656`): a `HEAD`, then a delete
  conditional on what it saw. 2 requests; 1 when the marker is absent.

**Other rules.**

- **Read-ahead.** A create may be issued ahead of the gate only for a carried row that no delta or retirement of
  this round touches (the rule of today's marker hints, `G:3667-3700`). A touched row is created at the gate. Reason:
  an intent over a blob that is then spared costs its next writer one upload. A create issued ahead also
  precedes the suppression and budget checks of G3; a row that then does not graduate keeps its intent, at the
  same cost. `GcReadAhead` gains a create verb (`CA/Gc/CasGcReadAhead.h:53-57`); the hint key needs the row's
  token, which `admit` does not get today (`G:3676`).
- **Marker mode only:** the marker pool and phase `meta_pool_wait` (`G:781`, `:1276-1283`).
- **Budgets.** Graduation and redelete budgets apply as today. No new count bound.

### 3.4 Cleanup of intents without a row {#fsck-rules}

Row-less intents arise by design ([§8.2](#residuals)). `cas-fsck` handles them; GC has no circuit.

| # | Rule |
|---|---|
| F0 | **Admission.** Every `cas-fsck` run reads `_pool_meta` with an exact `GET` before it lists or reads any intent; a failed read stops the run. `intents` with the backend's `token_dialect` → the run processes intents (F1 to F3). `intents` with another dialect → the run stops: no class is computed, no delete is sent. Not switched → the run skips F1 to F3 entirely: it lists, reads and deletes no intent, even if the pool is switched while it runs. This holds on a read-only open, which is how the tool opens the pool (`programs/disks/CommandFsck.cpp:53-56`) |
| F1 | `cas-fsck` lists `blob-intents/`. A key that `parseBlobIntentKey` rejects is `intent_malformed`: reported, never deleted |
| F2 | For a listed intent: exact `GET` of the intent first, **then** `HEAD B`, sent after the answer of that `GET`. Bytes other than the canonical record → `intent_malformed`. Same canonical identity → `intent_current`. Absent, or another canonical identity → `intent_stale`. Intent gone at the `GET` → skip. A refused or unanswered read, or a `HEAD` token that does not canonicalise → `intent_unread`: counted, never stale |
| F3 | Fix mode (`cas-fsck --fix stale-intents`): delete each `intent_stale` key with the write-once verb of [§3.3](#gc-rules), using the two reads of F2 made in that order in the same run. Never from the listing alone, never with the `HEAD` first |
| F4 | On a switched pool every `.meta` is `legacy_marker`. `cas-fsck --fix legacy-markers` deletes them with `op.removeCurrent`. Refused on a pool that is not switched |
| F5 | Intents are accounted objects in every class total. `body_without_meta` and `meta_without_body` (`CA/Tools/CasFsck.cpp:1206-1222`) are not computed on a switched pool |
| F6 | **A fix run stays on the read-only open.** It sends its deletes through the tool's own operation (`CA/Tools/CasFsck.cpp:447`): no mount, no lease, no probe. It deletes intent keys under F3 and `.meta` keys under F4, and writes nothing. A refused or unanswered delete stops the run with an error and the count deleted so far; a rerun continues |

- These are the first mutating options of `cas-fsck`. The rule "a read-only open never mutates the pool"
  (`CA/Pool/CasPool.cpp:445-446`) narrows to: it never writes, and it deletes only under an explicit `--fix`.
  The backend of a read-only open is the same writable object storage (`CA/Pool/CasPool.cpp:577-582`).
- An F3 delete needs no leadership: the run's own two reads justify it (I4).

### 3.5 Recovery and rebuild {#recovery}

| Event | State left | What happens next |
|---|---|---|
| GC leader dies after the create of G3, before its commit | row not pending; intent exists | the next round creates again, gets the equal occupant, confirms |
| Same, and the blob is referenced before the next round | row dropped by G6; intent over a live body | the next writer of that content uploads once; the old intent is then stale |
| Dies after the body `DELETE`, before the intent `DELETE` | row pending | next round: `HEAD` absent, intent `DELETE`, row dropped |
| Dies after the intent `DELETE`, before the commit | row pending | next round: `HEAD` absent, intent `DELETE` finds no key, row dropped |
| A create lands after its row left the state | a row-less intent | over a live body it costs one upload and stays until `t` is gone; F3 removes it once it is stale |
| Writer dies after its `PUT` | a marked body, its precommit edge abandoned | the next writer adopts it; GC condemns it when the edge is removed |
| `SYSTEM CAS GC REBUILD`, `cas-gc-rebuild` | no condemned row (`G:4762-4765`) | intents stand. No candidate is recovered from an intent: it proves a past condemnation, not zero references now |

- Every `gc/state` body this build creates (lease bootstrap `G:5204-5211`, rebuild, the stamp of the command) has
  header 4. Every body it rewrites keeps the generation it read (`G:5222`, `:5268`, `:1300`).
- A rebuild reads `_pool_meta` and never lowers `blob_protocol`.

### 3.6 Failure contract {#failure-contract}

Request failures are stated with each rule; refusal texts are in [§7.5](#builds). A decode failure of
`_pool_meta` stops every actor that reads it: a mount is refused, learning stays not learned, a GC round fails,
a tool run stops.

| Field | Valid | Missing | Unknown or malformed |
|---|---|---|---|
| `_pool_meta.blob_protocol` | `intents` | marker protocol | decode fails, `UNKNOWN_FORMAT_VERSION`. Never read as markers |
| `_pool_meta.token_dialect` | `etag`, `generation`, `emulated`; present iff `blob_protocol = intents` | with `intents`: decode fails, `CORRUPTED_DATA` | unknown word: `UNKNOWN_FORMAT_VERSION`. Present without `intents`: `CORRUPTED_DATA` |
| `_pool_meta.created_generation` | `4` | the pool was created by an older build: no automatic switch | below 4: `CORRUPTED_DATA`. Above 4: refused with the existing newer-format error, `UNKNOWN_FORMAT_VERSION` (`checkCompatibility`, `CA/Formats/CasFormat.cpp:102-109`). Both are checked after the floor check (`CA/Formats/CasPoolMetaFormat.cpp:155-158`), which refuses first when the later creator also raised the floor |
| `cas_run` row `intent` | `true` only with `pending = true` | run header below 4: `false`. Header 4: `CORRUPTED_DATA`, like a missing `confirmed` (`CA/Formats/CasRecordStreamFormat.cpp:304-307`) | present in a run with header below 4, present on a non-condemned record (`:310-311`), or `true` without `pending`: `CORRUPTED_DATA` |
| Packed row flags | values 1 (`pending`), 2 (`confirmed`), 4 (`intent`); 4 only with 1 | — | `flags > 7`, or 4 without 1: `CORRUPTED_DATA` |
| A row with `intent`, read by a marker-mode round | never | — | the round fails, `CORRUPTED_DATA` |
| A token that does not canonicalise, in intent-mode GC | — | — | It justifies no request, and ends the row unless G7 supersedes it. From the `HEAD` of G2: no row. From the `HEAD` of G7: no new row; the old row is dropped and its intent stays. As the own token of a row (a candidate or pending row inherited from marker mode): the row equals no live token. If the G7 `HEAD` of this round shows a canonical `u`, G7 applies and the row becomes a candidate `(B,u)`, with no intent delete. Otherwise the row is dropped: `u` does not canonicalise either, the body is absent, or the row was not touched and no `HEAD` showed a change. From the `HEAD` of G4: no body delete, no intent delete, the row is dropped. Counted (`CASGCIntentTokenRejected`), one ERROR per round. GC never deletes such a body ([§8.2](#residuals)). Writer: W3, W5. `cas-fsck`: F2 |
| Intent object bytes | the canonical record | — | other bytes at an intent key. Writer: an intent (W4 → W6). GC: not a confirmation; row carried, `CASGCIntentUnconfirmedCarry`, one ERROR per round. `cas-fsck`: `intent_malformed`, never deleted |

## 4. Invariants and the safety argument {#safety}

### 4.1 Invariants {#invariants}

| # | Invariant | Provided by |
|---|---|---|
| I1 | **Non-recurrence.** Once `t` stops being the incarnation of `B`, it never is again | A1 |
| I2 | **Content.** Every body ever at `B` has the pool's envelope length and the payload that hashes to `B` | [§6.3](#source-check), A6 |
| I3 | **Edge before observe.** A writer's edge on `B` is durable before its first `HEAD B`; it commits only while that precommit is the live owner | W1; `W:965-970` |
| I4 | **Intent permanence.** A delete of `I(B,t)` is sent only on evidence that `t` is gone from `B`: an answered observation that `B` is absent or has another canonical identity, or an answered `Removed` of that exact incarnation. That evidence is later than evidence that `t` had been current: the committed row (G4, G7) or the intent read first (F2). So while `t` is current, no such request was ever sent, and an existing `I(B,t)` stays | G4, G6, G7, G9, F0, F3; I1; A2 (the observation is not stale); A5 |
| I5 | **Delete evidence.** Every `DELETE B If-Match t` under intent authority follows, in this order: a confirmation that `I(B,t)` exists; the commit of the pending row; a complete cut, taken from that committed state, with in-degree 0; a `HEAD B` that shows `t`. Every delete of a marked token is under intent authority. On a pool created by this build every delete is, marked token or not: no marker-mode round and no older build ever runs there ([§7.4](#fresh-pool)) | G1, G3, G4; T3 (a); `frontier_complete`, `G:3548-3551` |
| I6 | **Adoption.** A writer records an adoption only of a marked body, when learned, after W4 answered 404 and W5 showed the identity of W2 | W3 to W5, W8 |
| I7 | **Marked means new**, on an upgraded pool. A marked body was published by a learned writer, so after the switch was written. A pool created by this build does not need I7 (I5): there a mark may be assumed or false | [§3.1](#learned), [§6.2](#mark-rules), A4 |
| I8 | **Goal.** A committed ref never names a blob key with no body | [§4.2](#argument) |

### 4.2 The argument {#argument}

**Adoption.** Writer `W`: edge at `Te`, W2 sees `t` at `T1`, W4 answers 404 at `T2`, W5 sees `t` at `T3`.

1. By I1, `t` was current during `[T1, T3]`.
2. Suppose a delete removes `t` at `Td > T3`. `t` reads marked (I6), so by I5 the delete has a confirmation at `Tc`, a
   commit `K` and a cut `C`, `Tc < K < C < Td`: on an upgraded pool because the mark is true (I7), on a pool created
   by this build for every token. The row's token was observed at its condemn `HEAD`, so by I1 `t` was current from
   then to `Td`.
3. By I4 the intent exists during `[Tc, Td)`. `W` read it absent at `T2 < Td`, so `T2 < Tc`.
4. By W8, `Te < T2`. So `Te < T2 < Tc < K < C`: the edge is durable before the cut. A complete cut counts it, the
   row is spared, contradiction. If the edge was removed before `C`, `W` cannot commit (I3).

**Publication.** `W` publishes `t'` under a fresh envelope after `Te`. `t'` is new (A1), so no earlier authority
names it. Any delete of `t'` needs a condemn `HEAD` after its appearance, then a later committed round and its
cut (`Tc`, `K`, `C` under intent authority; the marker graduation under marker authority): the cut is after
`Te` and counts the edge. A late delete of an older token misses: the condition is exact.

**Cleanup.** A delete of `I(B,t)` is sent only when `t` is gone for good (I4, I1), so it can land at any time.

**No-bytes references.** `adoptEvidence` (`W:572-587`; callers `X:260`, `X:1262`,
`CA/Parts/PartFolderAccess.cpp:457`) adds an edge with no request. It needs a live source edge, which the cut
counts. Unchanged by this design; so is its assumption that the source edge outlives the new precommit.

**Where evidence must be positive, where absence is accepted:**

| Decision | Evidence | Why |
|---|---|---|
| GC mode | positive: an answered, decoded `_pool_meta` read after the lease | marker mode on a switched pool graduates without an intent |
| Intent processing in `cas-fsck` | positive: `intents` and the run's own dialect, read in the same run before the first intent | a run of another dialect calls a live intent stale |
| Graduation | positive: the create answered, or an equal occupant read | an unanswered create may never land; then nothing warns the writer |
| Body delete | positive: the committed row, a present body with the row's identity | — |
| Adoption | positive: two `HEAD`s with one identity, the mark | — |
| Intent absent at W4 | absence, an answered 404 only | sound by I4 and the second `HEAD`; a failed read is not absence |
| Body absent at W2, G2, G4, F2 | absence, an answered 404 only | each leads to an action that is safe if a body appears later: publish, no row, intent cleanup under I1 |
| A `LIST` | nominates intents (F1) and mount slots ([§7.3](#command)) only (decision-2) | a missing key delays cleanup; it never authorizes a body delete |

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
| S10 | **Cleanup in another dialect.** GC confirms `I(B,t)`, commits pending, takes its delete cut. A cleanup client configured for another dialect, started after the switch or before it, reads the intent, then `HEAD B`: the same body renders as another identity. It calls the intent stale and deletes it. A writer adds its edge, reads no intent, adopts; the delete lands | F0; the dialect rule of [§6.1](#identity) |
| S11 | **Mode from a failed read.** After the switch, three rounds of a new `Gc` object take "not intents" from a 404 or a failed read. The first condemns a marked `t` and writes `Condemned`; the second graduates on the marker, with no intent; a learned writer adopts after the third's cut; the third deletes | G1 |
| S12 | **A stale read.** G7 or F2 takes the identity at `B` from a cache that shows `u` while `t` is current, and deletes a live intent. Or W4 takes a 404 from a request sent before the writer's edge, and GC confirmed in between | W8, G10, F2 |

## 5. Assumptions and the contract with the store {#assumptions}

| # | Assumption | What provides it | What stays assumed |
|---|---|---|---|
| A1 | A body token never recurs at a key | One publication call site, `W:509` through `Q:780`. Streaming: a fresh 128-bit tag per attempt (`W:326`, `:497`), one send (`W:299`, `B:894-900`). Verbatim copy: the staging tag is minted once (`X:797`), and the copy happens at most once per staging object because hardlink copies share the flag (`CA/ContentAddressedTransaction.h:162-165`, `X:252`, `:300`; `CA/Pool/CasPartWriteTxn.h:38`). Generation dialect: the store's own counter | No transport reissue after a request may have been applied (redirect, re-sign, reconnect). The store applies one request at most once. Random tags do not collide. A generation is never reused. A removed delete marker on a versioned bucket is external manipulation. The `emulated` dialect over local files: see below |
| A2 | Reads are linearizable per key: a `HEAD` or `GET` sent after a write was applied reflects it, for every client, whether or not the writer got an answer. Safety needs it at: the mode read of G1 (a switch that was applied without an answer is seen by every later round); the adoption reads W2, W4, W5; the delete-site `HEAD` of G4, the supersede `HEAD` of G7 and the body `HEAD` of F2, where an absent or other-identity answer authorizes an intent delete (I4). Learning and the admission read of F0 do not: a stale answer there is only conservative (not learned; intents skipped). A read that overlaps the write may show either state: a round whose mode read overlaps the switch is the marker-mode round of T3 | the store; W8, G10 and F2 keep caches and earlier requests out | proxies and caches between a server and the store |
| A3 | A conditional delete removes only the named incarnation | the capability probe (`CA/Backend/CasProbe.cpp:47`); contract at `CA/Backend/CasBackend.h:120-139` | the probe samples; it does not certify non-recurrence (`:137-139`) |
| A4 | `HEAD` attributes are coherent: size, token and `Content-Type` describe one incarnation, and `Content-Type` is what the publication of that incarnation sent | the mark probe samples it per mount; the integration suite covers the multipart paths per store ([§6.2](#mark-rules)) | a store or proxy that synthesizes, retains or rewrites the type on a path neither exercises, for another server's client, or after a later change of the store. The probe samples one client at one mount |
| A5 | One token dialect and one spelling per pool, across clients | `token_dialect` in `_pool_meta`, checked by every actor that uses an identity ([§6.1](#identity)); canonical identity | two clients that render one tag with different bytes beyond the quote pair |
| A6 | A staging object holds the bytes hashed at staging | the staging key is 32 random characters under the writer's epoch (`X:950`), written once, known to one transaction | corruption inside the store; hash collisions |
| A7 | Cuts are complete: every live or precommit edge is in a cut that authorizes a delete | `frontier_complete` suppresses destruction otherwise (`G:3548-3551`) | unchanged, not re-proved here |
| A8 | No older build starts protocol work after the switch | operator precondition ([§7.2](#procedure), step 3); floor, latch and declarations enforce and diagnose. A pool created by this build never had an older member ([§7.4](#fresh-pool)) | [§7.8](#transition-risks), X1 |
| A9 | Control objects are not deleted, rolled back or copied between pools | — | external manipulation is unsupported. A foreign `_pool_meta` with `created_generation` would make an upgraded pool read as created by this build, which drops the slot check and the mark guard |

This dependence is a shipping contract. A store whose tokens can recur must not host a pool under this protocol.

**The `emulated` dialect over local files** is for tests and single-node development, and gives A1 only inside
these limits. Its token is the file mtime in nanoseconds plus an in-process counter that is dropped 2 s later
(`B:400`, `:457`, `:578`). A delete and a re-create inside one mtime tick can therefore repeat a token after that
drop or after a restart. The contract comment that calls these tokens a never-reused sequence
(`CA/Backend/CasBackend.h:135-136`) holds for the in-memory backend only and is corrected with the implementation.

## 6. Token identity, mark, source check {#mechanisms}

### 6.1 Token identity {#identity}

- **Canonical identity** = (dialect word, lowercase hex of the opaque tag). The opaque tag is the token value
  with at most one balanced pair of double quotes removed. No case folding: tags are opaque, and folding could
  merge two incarnations.
- **Raw token**: the value as the store returned it. Used only as `If-Match` (`G:593`).
- **Used for:** intent keys; W2 against W5; row matching at G4 and G7; fsck F2. New `Etag::canonical`, and
  `sameIncarnation(row token, live token)`, which compares canonical identities: a token that does not canonicalise
  equals nothing. Rows keep the token as captured; both sides are canonicalised at compare time. Marker mode keeps
  `PersistedEtag::matches`, a textual compare (`CA/Backend/CasEtag.h:85-88`).
- **Grammar, two levels.**
  - `isIncarnationValue` (`CA/Backend/CasEtag.cpp:10-32`), which `CasRequests::tryMint` applies to every
    observation of every key (`Q:337-342`): unchanged.
  - Canonicalisation, applied only where this protocol needs an identity: W3 and W5, G2 to G7 in intent mode, F2,
    intent key construction and parsing, the probe. Rejected for `etag` and `emulated`: empty; a `W/` prefix; an
    unmatched or inner double quote; a control byte; leading or trailing whitespace; `*`; a comma; an opaque tag
    over 128 bytes. For `generation`: anything but canonical positive decimal of at most 20 digits.
  - **A token that does not canonicalise is not an identity.** Nobody compares it, builds a key from it,
    deletes on it or throws for it. The writer publishes fresh (W3, W5), GC ends the row or supersedes it
    ([§3.6](#failure-contract)), `cas-fsck` reports `intent_unread` (F2). The cost is in [§8.2](#residuals).
- **Why the grammar is not tightened in `tryMint`.** `tryMint` sees no pool state. A tighter grammar there would
  change the result of every `HEAD` and `GET` at the binary swap: a store that returns a now-rejected form would
  stop working before any switch. With two levels the swap changes no read, control objects keep today's
  grammar, and the tight form applies where an identity is used, which starts at the switch. The switch
  command and the sequence of [§7.4](#fresh-pool) refuse a store whose probe tokens do not canonicalise.
- **Quotes.** The generation dialect already strips one pair (`CA/Backend/CasObjectStorageBackend.h:175-182`); the
  `etag` dialect keeps them in the raw token because `If-Match` needs them.
- Intent keys are derived only from a body `HEAD`, never from a listing or a write response.

**The dialect rule.** `token_dialect` is recorded once in `_pool_meta`, in the write that sets `intents`. The
dialect is a property of the client configuration (`CA/Backend/CasObjectStorageBackend.h:110`, `B:139`,
`S3:817-820`), so two configurations of one bucket can differ. Every actor that compares identities, or deletes
on their strength, first reads `_pool_meta` with an exact `GET` and refuses unless its backend dialect equals the
recorded one. A mismatch sends no delete and no intent request. An actor whose read showed an unswitched pool
compares no canonical identity and touches no intent until a read of its own shows `intents` with its dialect.

| Actor | On a mismatch |
|---|---|
| Writable mount of a switched pool | the mount is refused |
| Learning, on a mount that was up before the switch | not learned for the life of the mount; one ERROR ([§3.1](#learned)) |
| Each GC round; `SYSTEM CAS GC REBUILD` | the round fails and the mount leaves GC (G1); the rebuild is refused |
| Every `cas-fsck` run, and `cas-gc-dryrun`, on a read-only open too | the run stops (F0) |
| The switch command and the sequence of [§7.4](#fresh-pool), when `_pool_meta` already records another dialect | refused; the sequence fails the mount |

Text: [§7.5](#builds). A read-only open that only reads bodies and control objects is not refused: it compares
no identity.

### 6.2 The mark {#mark-rules}

- **Set per request**, by the publication, from the mark bit. Never as a configured extra header: the HTTP client
  applies extra headers after the SDK's and would override the choice (`src/IO/S3/PocoHTTPClient.cpp:611-622`).
- **Four requests define an object's type** (rows 7 and 8 below). **Not reached by a publication:** the type
  of an `UploadPart` (`src/IO/WriteBufferFromS3.cpp:546`, `src/IO/S3/copyS3File.cpp:596`) does not define the
  object's. The buffered-copy `PutObject`
  (`src/IO/S3/copyS3File.cpp:506`) needs the fallback that `NativeOnly` forbids (`:979-982`, `:1005`). The GCS
  compose (`src/IO/S3/Client.cpp:614-624`) is skipped by the single-attempt client that publications use (`B:897`).
- **Copy.** Tested on MinIO `RELEASE.2025-04-22` and RustFS 1.0.3: a default-directive `CopyObject` keeps the
  source's type and ignores the request's; a multipart copy inherits nothing and takes the type of its create.
  AWS S3, GCS and Ceph are not tested. The staging object, the copy request and the multipart create all use the
  one mark bit, so the result is the same either way. The copy stays on the default directive: no
  `object_metadata` is passed (`src/IO/S3/copyS3File.cpp:737-740`).
- **Emulated backend over local files:** one permission bit of the object file that file creation never sets, an
  execute bit: files are created with mode `0666` under the server `umask` (`src/Daemon/BaseDaemon.cpp:265`). The
  publish path sets or clears it explicitly on the temporary file before the atomic rename (`B:513-560`); `HEAD` reads
  it with `stat` (`B:732-749`). No file-system feature is needed, and a rename replaces the mark with the body. A copy
  that does not preserve modes loses the mark and cannot add it: one upload per blob. The in-memory backend keeps a
  mark per object.

**Plumbing.** The bit travels through these signatures and no others.

| # | Signature | Change |
|---|---|---|
| 1 | `PendingBlob` (`CA/ContentAddressedTransaction.h:157-166`) | field `marked`, set at the staging sink, copied by a hardlink (`X:252`) |
| 2 | `BlobSource` (`CA/Pool/CasPartWriteTxn.h:26-38`) | field `marked`, set beside `publication_attempted` (`X:300`) |
| 3 | `BlobPublishRequest` (`CA/Backend/CasBackend.h:74-78`) | field `marked`, set at the one publication call (`W:509`) |
| 4 | `ObjectStorageBackend::blobPublicationWriteSettings` (`B:894-900`) | takes `marked`; sets the content type in the `WriteSettings` it returns, for `writeObject` (`B:923-928`) and `copyObject` (`B:969-973`) |
| 5 | Staging sink (`X:951`) | passes `WriteSettings` with the content type to `writeObject`; today it passes none |
| 6 | `WriteSettings` (`src/IO/WriteSettings.h`), shared | one optional field, the object content type; empty means `binary/octet-stream` |
| 7 | `WriteBufferFromS3` (`src/IO/WriteBufferFromS3.cpp:119`), shared | reads the field at `:442` and `:775` |
| 8 | `copyS3File` (`src/IO/S3/copyS3File.cpp:1011`), shared | one optional trailing parameter with today's value as default, used at `:148` and `:748`; `S3ObjectStorage::copyObject` passes the field (`S3:1122-1137`) |
| 9 | `S3::ObjectInfo`, `ObjectMetadata`, shared | one optional field, the content type of the `HEAD` response, which is dropped today (`src/IO/S3/getObjectInfo.cpp:87-93`; copied at `S3:973`, `:1012`) |
| 10 | `Backend::RawMeta` (`CA/Backend/CasBackend.h:161`, filled at `B:230-238`), `Meta` (`CA/Backend/CasWriteResult.h:29`) | field `marked`. Token, size and mark come from one response |

The shared, non-CAS change is rows 6 to 9. No other caller sets the write field or reads the metadata field, so
every request of every other caller keeps its bytes.

**Probe**, in `runCapabilityProbe` (`CA/Backend/CasProbe.cpp:47`, called at `CA/Pool/CasPool.cpp:546`), with the
disk's own client, through `op.publish` and `op.head`, on scratch keys under the per-mount `_probe/` prefix. It
needs no knob in shared code: every object is far below the single-part and single-copy sizes.

| # | Step | Must read | A failure is |
|---|---|---|---|
| P1 | marked `PutObject` | marked, exact value | preservation |
| P2 | unmarked `PutObject` | unmarked | false presence |
| P3 | unmarked `PutObject` over the marked object of P1 | unmarked | false presence |
| P4 | `CopyObject` of a marked source, written as the staging sink writes it, to a new key | marked | preservation |
| P5 | `CopyObject` of an unmarked source to a new key | unmarked | false presence |
| P6 | `CopyObject` of an unmarked source over a marked object | unmarked | false presence |
| P7 | every `HEAD` of P1 to P6 | a token that canonicalises ([§6.1](#identity)), with size and type in the same response | token form |

- P4 to P6 run only where the backend supports the copy: native mode and `supportsCopyMode(NativeOnly)`
  (`B:955-967`, `S3:822-827`).
- **Preservation or token-form failure:** the mount never learns: it publishes unmarked and never adopts. WARNING
  at mount, `blob_mark_probe` in `system.cas_mounts`. The switch command refuses on this disk; the sequence of
  [§7.4](#fresh-pool) fails the mount.
- **False presence:** on an upgraded pool the switch command refuses and a writable mount of a switched pool
  fails ([§7.5](#builds)); on a pool created by this build it is a WARNING and the mount learns ([§7.4](#fresh-pool)).
- Only P2 and P5 guard I7: an unmarked publication must never read marked. P3 and P6 overwrite a marked object,
  which exists only after the switch, so the new token has no marker authority. On an upgraded pool they refuse
  all the same: a store that keeps a replaced object's type is outside A4.
- `skip_access_check` skips the probe (`CA/Pool/CasPool.cpp:526`, `:550`): on an upgraded pool the mount never
  learns and the command refuses; on a pool created by this build the mark is assumed ([§7.4](#fresh-pool)).

**Paths and where each is verified.**

| Publication path | Used when | S3, dialect `etag` | GCS, dialect `generation` | Emulated over local files | In-memory |
|---|---|---|---|---|---|
| `PutObject` | streamed body up to `max_single_part_upload_size` | mount probe | mount probe | — | — |
| Multipart upload | streamed body above it | integration suite | integration suite | — | — |
| `CopyObject` | S3 staging, first publication, up to `max_single_operation_copy_size` | mount probe | mount probe | not available (`B:955-959`) | not available |
| Multipart copy | S3 staging, first publication, above it | integration suite | not used: no multipart copy on GCS (`src/IO/S3/Client.cpp:1046-1049`) | — | — |
| File write and rename (`B:505-560`) | every publication | — | — | mount probe, P1 to P3 | — |
| In-memory map | every publication | — | — | — | gtests, P1 to P3 |

- **Multipart paths are not probed at mount.** Forcing them needs a part-size and a copy-size knob in shared
  code. The integration suite verifies them per store, in both directions ([§10.2](#tests)).
- **A lost mark on a multipart path costs uploads, not safety.** The body reads unmarked, so it is never adopted
  and is uploaded again on each reuse. Both defaults are 32 MiB, so this affects every body above that size on
  such a store. `CASBlobReuseLegacyRepublish` that keeps growing on a switched pool shows it.

### 6.3 Source check {#source-check}

- **Rule.** Every streaming publication hashes exactly the payload bytes it forwards, with the pool algorithm in
  its streaming convention (`W:116-130`), and compares with the key's digest before the object is completed.
- **Seam.** `StreamingBlobPublication` gains the expected `BlobRef` (`CA/Backend/CasBackend.h:54-59`). The check
  sits beside the length check (`B:933-950`), before `out->finalize` (`B:951`): for S3 the completing request,
  `PutObject` or `CompleteMultipartUpload`, is issued inside `preFinalize` (`src/IO/WriteBufferFromS3.cpp:164`,
  `:192`, `:205`). Mismatch: `out->cancel`, `CORRUPTED_DATA`, nothing published. The emulated path checks before
  its rename (`B:560`).
- **Covers:** local staging, reopened by path for every attempt (`X:337-340`); S3 staging re-streamed
  (`X:308-332`); memory sources. So every overwrite of a body the writer saw, and every streamed first
  publication.
- **Verbatim staged copy** (`B:955-973`) is not hashed. It rests on A6 and runs at most once per staging object;
  its selection rule is in [§3.2](#writer-rules).
- **A reader during an overwrite** sees the same payload. Body reads are not pinned to a token: CAS builds its
  `StoredObject` without an `etag` (`CA/ContentAddressedMetadataStorage.cpp:2050`, `:2170`), `readObject` passes
  that empty value on (`S3:404`), and `If-Match` is sent only for a non-empty one
  (`src/IO/ReadBufferFromS3.cpp:604-605`). A ranged read that spans an overwrite therefore continues in the new
  body, where the payload offset and bytes are equal by I2. Today the same happens on a condemned republish.
- Cost: no request; one more hash pass per streamed body.

## 7. Transition {#transition}

One-time, upward only, in place, no data copy. Mixed versions on one pool stay unsupported
(`D/configuration.md`); that is now a stated precondition of the switch.

### 7.1 Stages {#stages}

| Stage | Durable condition | Invariant |
|---|---|---|
| T0 | no mount of this build yet | today's protocol |
| T1 | floor 4; `blob_protocol` absent | Every delete authority is marker authority. Writers of this build never adopt, publish unmarked, write no marker. No intent and no marked body exists |
| T2 | `gc/state` at generation 4; on a pool created by this build, from its first GC lease; absent before ([§7.4](#fresh-pool)) | T1, and no older build can commit a GC round |
| T3 | `blob_protocol = intents` | (a) A marked token is under intent authority only. (b) No writer adopts an unmarked body. (c) No marked body is deleted without I5 |

- **T1 is safe with today's GC and today's writers.** A body without a marker, and a stale `Clean` or `Condemned`
  over a newer body, are states today's protocol already reaches after a writer dies. An older writer republishes
  a marker-less body once (`W:475`); marker-mode GC creates `Condemned` on an absent key
  (`CA/Gc/CasGcMetaWriter.cpp:48-49`) and graduates on it.
- **T2 can come early.** A `gc/state` that this build creates has header 4 ([§3.5](#recovery)), so a pool whose
  GC never ran reaches T2 at the first lease of this build. That only removes older GC sooner.
- **T3 (a).** A pool created by this build never has marker authority, so (a) holds there for every token without I7.
  On an upgraded pool, marked means published after the switch (I7). Marker authority on `t` needs two committed
  marker-mode rounds after `t` appeared: one that condemns, a later one that graduates (`N:559`). An older build
  cannot commit after the stamp, which precedes the switch. A round of this build that graduates took its mode after
  its lease (G1), so after the switch (A2): intent mode. A round that sampled before the switch can condemn a marked
  token; the row is a candidate and goes through G3.
- **T3 (b), and marker authority after the switch.** Two kinds of round still delete without an intent after
  the switch: a round of this build that took marker mode before it, and an older round already past its lease
  read. Both delete only rows committed pending before the switch, so only unmarked tokens. No writer adopts an
  unmarked body (W3): one that meets it publishes over it, and the delete misses. A pending row such a round
  leaves behind is demoted (G8).

### 7.2 Operator procedure {#procedure}

For a pool that had a member of an older build. A pool created by this build needs none ([§7.4](#fresh-pool)).

1. Stop every server of the pool, replace the binaries, start them: the order of the generation 3 upgrade
   (`D/operations/migration.md`, "Upgrading a pool to reader generation 3").
   **First point of no return:** the first writable mount of this build raises the floor to 4. The raise is
   durable even if that mount then fails (`CA/Pool/CasPool.cpp:898-904`). Older builds can no longer mount.
2. Check `system.cas_mounts`: every row not `retired` shows `protocol_generation = 4`, and `blob_mark_probe = 'ok'`
   on the server that will run the command.
3. Confirm that no process of an older build runs against the pool: servers, `clickhouse-disks`, tools, including
   any that opened the pool before step 1 and still run.
4. Run `SYSTEM CAS ENABLE BLOB INTENTS '<disk>'` on one server.
   **Second point of no return:** the `_pool_meta` write of the command. From that write a server can learn and
   publish a marked body, whether or not the command has returned. The protocol cannot be switched back, and the
   legacy branch W3 stays forever.
5. Optional, later: `cas-fsck --fix legacy-markers`.

### 7.3 The switch command {#command}

`SYSTEM CAS ENABLE BLOB INTENTS '<disk>'`. Each step is monotone; a rerun after any failure completes it.

| Step | Action | Refuses when |
|---|---|---|
| 1 | Checks | the disk is not mounted writable; its mark probe did not pass (false presence included) or was skipped under `skip_access_check`; the pool prefix is too long ([§2.1](#intent-object)); `min_reader_generation < 4`; `_pool_meta` already records another `token_dialect`; a mount slot declares `protocol_generation < 4` or cannot be decoded, unless its owner is a verified retirement. The message names the `server_root_id` |
| 2 | Stamp: read-modify-write of `gc/state`. Generation 4 or above: no write. Below 4: the same fields with generation 4. Absent: created at generation 4 with `gc_shards` from `_pool_meta` and lease owner 0 | — |
| 3 | Switch: read-modify-write of `_pool_meta`, decided on the body it reads. No `blob_protocol`: write `intents` and this backend's dialect. `intents` with the same dialect: no write, success. `intents` with another dialect: refused | the stamp is not confirmed. This applies to the command only: the sequence of [§7.4](#fresh-pool) has no stamp |
| 4 | This server learns | — |

- **The stamp is confirmed** by an answered write, or by the resolve read of the read-modify-write that shows
  generation 4. A sent request is not a confirmation. If the policy is exhausted the command fails with the
  request error and has written nothing else; a rerun continues.
- **The switch is confirmed** by an answered write, or by a read of the read-modify-write that shows `intents` with
  this backend's dialect (`Q:1166-1222`): an unanswered switch is resolved by that exact read, never assumed. Until
  then this server does not learn. On an exhausted policy the command fails with the request error and the sequence of
  [§7.4](#fresh-pool) fails the mount; a rerun, or the next mount, decides on the body it reads.
- **Two commands at once** converge: each read-modify-write decides again on the body the other wrote
  (`CA/Pool/CasPoolMeta.cpp:69-86`). With two dialects, one command wins and the other is refused.
- Slots are found by exact reads for the server roots the catalog names, and by one `LIST` of `gc/server-roots/`
  for roots without namespaces. A slot left by an older build blocks until that server starts once on this build
  or is removed with `SYSTEM CAS DROP POOL MEMBER`.
- **The checks do not prove that no older writer exists** (X1). Step 3 of the procedure carries that claim.
- The stamp does not need the GC lease. A round in flight loses its `If-Match` commit (`G:1326`) and retries; the
  leader's next renewal keeps the generation.
- **A stamp-created `gc/state` is a free lease.** Owner 0 means "never held" (`CA/Formats/CasGcStateFormat.h:17`)
  and no `Gc` has that id (`G:374`). `acquireOrRenewLease` takes a body with owner 0 as it takes an absent one,
  with or without `allow_steal`; today every owner but its own is an incumbent (`G:5220`, `:5244-5247`), which
  would refuse the first `SYSTEM CAS GC RUN`. The command keeps the create: an absent `gc/state` on an upgraded
  pool is what a late older build would bootstrap at its own generation.

### 7.4 A pool created by this build {#fresh-pool}

A writable mount of a pool that this build created ends with the pool switched, or fails. No operator command.

- **No older member, no stamp.** `_pool_meta` is born at floor 4, which no older build decodes
  (`CA/Formats/CasPoolMetaFormat.cpp:155-158`), so A8 holds without anyone's statement. For the same reason no
  `gc/state` below generation 4 can appear: only this build creates one here, always with header 4
  ([§3.5](#recovery)). The invariant of T2 holds from birth, and the sequence sends no `gc/state` request. The first
  GC lease creates it under its own owner, as today (`G:5197-5211`), so a manual round right after creation leads.
- **Creation.** The pool is created unswitched: `_pool_meta` with `min_reader_generation = 4` and
  `created_generation = 4`, no `blob_protocol` (`CA/Pool/CasPool.cpp:583-588`).
- **Sequence.** A writable `Pool::open` that reads `created_generation = 4` on a pool that is not switched runs the
  switch and the learning, steps 3 and 4 of [§7.3](#command), without the stamp. It runs after `mountWritable` returns
  (`CA/Pool/CasPool.cpp:611`) and before `open` returns: before any writer or GC round of this mount exists.
  `Pool::openForDecommission` shares `mountWritable` (`:1102`) and runs no probe; it does not run the sequence. There
  is no slot check.
- **A failed sequence fails the mount.** It needs a pool prefix within the limit ([§2.1](#intent-object)), a
  confirmed switch ([§7.3](#command)), and a probe that did not fail preservation or the token form. Such a
  probe failure, an exhausted policy or a refusal at the switch: `Pool::open` throws ([§7.5](#builds), or the
  request error). No mount is admitted on an unswitched pool of this build. Reason: such a mount would run
  without reuse, and its GC in marker mode, until some later mount switched the pool, with nothing to show it.
  So a store whose probe fails preservation or the token form cannot host a new pool, and marker mode never
  runs on one: before the switch the pool has no body, no `gc/state` and no GC round.
- **The probe guards only cost here.** Every delete in this pool is under intent authority (I5), so a body that reads
  marked without having been published marked is adopted safely. Under `skip_access_check` the sequence switches
  without the probe and the mount learns with the mark assumed: one log line, `blob_mark_probe = 'assumed'`; so does
  every later mount of this pool whose probe is skipped ([§3.1](#learned)). A probe that runs and finds false presence
  logs a WARNING and proceeds: skipping admits nothing that running refuses. One that finds the mark not preserved
  still fails the mount: a pool that silently never reuses is a hidden fallback.

| State | Durable objects | A writable mount that finds it |
|---|---|---|
| N0 | nothing, or `_probe/` debris | creates the empty catalog, then `_pool_meta` |
| N1 | the canonical empty catalog | creates `_pool_meta` |
| N2 | `_pool_meta`: floor 4, `created_generation = 4`, not switched | switches, or fails |
| N3 | `blob_protocol = intents` | learns: T3 |

- **Restart.** A mount resumes at the state it reads. Every step is a create-if-absent or a read-modify-write that
  decides on the current body, so a step that already landed is not repeated.
- **Concurrent creators.** One `_pool_meta` create wins and the other validates the winner's body
  (`CA/Pool/CasPoolMeta.cpp:176-182`). Both mounts then run the sequence and converge as two commands do. A
  creator with another dialect is refused at the switch, by the dialect rule.
- **An older build that races the creation.** If its `_pool_meta` wins, the pool has no `created_generation`: it
  is an upgraded pool and needs [§7.2](#procedure). If it loses, it stops at the floor.
- **The residual checks stay as they are.** `_pool_meta` is the first object written after the catalog, so the
  only states without it are N0 and N1, which the bootstrap accepts today (`CA/Pool/CasPool.cpp:459-466`,
  `:567-576`; `CA/Backend/CasSentinelProbe.cpp:83-90`). The scratch objects of the mark probe sit under the
  per-mount `_probe/` prefix, which the check ignores.

### 7.5 Refusals {#builds}

The only registry of refusal texts. The first three come from existing gates (`CA/Formats/CasFormat.cpp:102-109`,
`CA/Formats/CasPoolMetaFormat.cpp:155-158`, `CA/Formats/CasTextFormat.cpp:319-321`); the rest are new. What an
older build does: [§2.4](#older-builds).

| Who | Code | Message |
|---|---|---|
| Older build, mount or read-only open | `UNKNOWN_FORMAT_VERSION` | `CAS pool meta: pool requires reader generation 4 but this build supports at most 3` |
| Older build, GC lease read after the stamp | `UNKNOWN_FORMAT_VERSION` | `CAS cas_gc_state: object requires reader generation 4 but this build supports at most 3` |
| Older build, epoch allocation on a latched root | `UNKNOWN_FORMAT_VERSION` | `CAS server-epoch: critical key '!protocol_generation' is not understood by this build` |
| Any actor of the dialect rule | `BAD_ARGUMENTS` | `CAS pool records token dialect '<a>', this disk uses '<b>': identities are not comparable, no delete is sent` |
| Any reader of `_pool_meta`, unknown word | `UNKNOWN_FORMAT_VERSION` | `CAS pool meta: blob_protocol '<word>' is not understood by this build` |
| GC round, `_pool_meta` absent | `INVALID_STATE` | `CAS pool meta '<key>' is absent; the GC round cannot take its mode` |
| Marker-mode round, a row with `intent` | `CORRUPTED_DATA` | `condemned row carries an intent but the pool is not switched` |
| Intent-mode round, `gc/state` below 4 | `INVALID_STATE` | `pool uses blob intents but gc/state is at generation <n>; run SYSTEM CAS ENABLE BLOB INTENTS again` |
| The command, a slot below 4 | `INVALID_STATE` | `mount slot of server root '<id>' declares protocol generation <n>, need 4; start that server once on this build or remove the member` |
| The command: probe not passed or skipped. The sequence: preservation or token form failed | `NOT_IMPLEMENTED` | `blob mark probe failed on this disk: <step>`; for the command the step is `skipped` under `skip_access_check` |
| The command, and a writable mount of a switched pool without `created_generation`; false presence | `NOT_IMPLEMENTED` | `the store reports the blob mark on an object published without it (<step>)` |
| The command, the sequence, a writable mount of a switched pool; pool prefix too long | `BAD_ARGUMENTS` | `pool prefix of <n> bytes leaves no room for blob intent keys; the limit is <m>` |
| Publication, digest differs | `CORRUPTED_DATA` | `payload of <key> does not hash to its key; upload aborted, nothing published` |
| `cas-fsck --fix legacy-markers`, pool not switched | `BAD_ARGUMENTS` | `markers are protocol state until the pool uses blob intents` |

### 7.6 Inherited state {#inherited}

| Object at the switch | Writer | GC |
|---|---|---|
| Legacy body, any marker | W3: re-upload on the first reuse with bytes | the intent pipeline when it reaches in-degree 0 |
| Candidate row | — | G3 creates the intent from the row's token |
| Pending row (no `intent`) | re-upload on reuse; the old delete then misses | G8: demoted, confirmed by G3, deleted a round later. Never executed on marker authority |
| `Clean` and `Condemned` markers | never read | removed with a legacy body (G4 (4)); the rest by F4 |

- **The `intent` field rides every carrier of a row:** the text record (`CA/Formats/CasRecordStreamFormat.cpp:198-205`,
  `:290-298`), the packed row (`N:262-290`), the conversions between them (`N:327`, `:498`) and the run writer
  (`N:661-664`). A carrier that drops it turns an intent row into an inherited one, which G8 demotes: the safe
  direction.

**Legacy is permanent.** Five paths add a reference with no local bytes and no request: hardlink of a committed
part (`X:1262`), adoption of a blob staged in the same transaction (`X:260`), and `prepareEntries`
(`CA/Parts/PartFolderAccess.cpp:447-470`) under `republishRef` (`:482`), `repointRef` (`:664`) and the relink
fetch (`CA/ContentAddressedMetadataStorage.cpp:2404`). A body kept alive only by mutations, renames and fetches is
never re-uploaded and stays unmarked. Its protection is the in-degree, as today.

### 7.7 Tables and tools {#tools}

| Surface | Change |
|---|---|
| `system.cas_mounts` (`src/Storages/System/StorageSystemContentAddressedMounts.cpp:40-63`) | `protocol_generation` per slot (0 for an older body); `blob_protocol` (`markers`, `intents`); `blob_mark_probe` for this server (`ok`, the failed step, `skipped`, `assumed`) |
| Server log | WARNING at the floor raise (exists); one line at the stamp, the switch, when a mount learns and when it assumes the mark; ERROR on a dialect mismatch, when a mount leaves GC, and on a rejected token |
| `SYSTEM CAS ENABLE BLOB INTENTS` | one new `ASTSystemQuery` type beside `CAS_FSCK` (`src/Parsers/ASTSystemQuery.h:173-175`): an additive change in the shared parser |
| `cas-inspect` | renders an intent, a body's mark, `blob_protocol`, `token_dialect`, `created_generation`, a slot's `protocol_generation`, a row's `intent` (`CA/Tools/CasInspect.cpp:417`) |
| `cas-fsck`, `SYSTEM CAS FSCK` | classes `intent_current`, `intent_stale`, `intent_malformed`, `intent_unread`, `legacy_marker`; lists `blob-intents/` beside `blobs/` (`CA/Tools/CasFsck.cpp:892-913`). Only `cas-fsck` gets the fix options of [§3.4](#fsck-rules) |
| `cas-gc-dryrun` (`programs/disks/CommandCaGcDryRun.cpp`, `G:5061`) | reads `_pool_meta` as G1 does and stops on the same failures and on a dialect mismatch. In intent mode it previews only rows with `pending` and `intent`; an inherited pending row is shown as a candidate (G8) |

### 7.8 Residual risks {#transition-risks}

| # | Risk | Exposure |
|---|---|---|
| X1 | An older writer works after the switch: an operator mistake, a process paused before claiming a new root, a tool left open, a slot the listing omitted. It adopts a marked body on a stale `Clean` (`W:452`) while intent-mode GC deletes it: a committed ref to a deleted body | keys that still carry a decodable `Clean`; shrinks as G4 (4) and F4 remove markers. Not detected. Not possible on a pool created by this build, with one exception of the same class, also unsupported: a process of a dropped pool that still runs against its cleared and reused prefix. An older one can bootstrap `gc/state` (X2) and run marker GC; a new writer whose mark is assumed or false then adopts its unmarked body |
| X2 | `gc/state` deleted, then an older build bootstraps it (`G:5197-5211`) and runs marker GC over marked bodies | external manipulation, unsupported |
| X3 | Marked bodies under older control objects: control objects restored from before the switch, or marked bodies copied into an unswitched prefix. A marked token can sit under marker authority. G8 covers committed rows, not a delete already sent | unsupported |
| X4 | The mark set on a legacy body by a metadata-only operation while marker authority on its token is outstanding | external manipulation |

## 8. Not done, and residuals accepted {#scope}

### 8.1 Not done {#not-done}

| Not done | Reason |
|---|---|
| A mutable per-hash marker that names the target token | fewer stored states, but conditional GC transitions on a shared object and an observation order to preserve; not model-checked. It has the same dependence on A1 |
| An inventory of bodies | its premise, "no edge in the run means unreferenced", fails on a rebuilt pool (CAS-33.3) |
| A GC circuit for row-less intents | recorded rate of pending rows spared on one production node: 117 in 16.8 days; a circuit can be added with no format change |
| `Last-Modified` as the classifier | resets on copy, restore and sync; multipart stamping differs per store |
| A new pool prefix | copies all data; the in-place transition needs no drain |
| Keeping a row on a spare | breaks the zero-I/O shard carry (`G:3714`) and forces a fold every round (`G:4402`) |
| The writer deletes an intent after its own upload | safe only on an unequivocal answer; a misread outcome loses data; saves under 40 bytes |
| A hash of the token as key segment; case folding | not injective; folding merges incarnations |
| Adopting at W5 when the token changed to `u` | safe under I1: `u` first appeared between the two `HEAD`s, so every authority on `u` has a cut after the edge. It would widen the adoption proof for a rare case |

### 8.2 Residuals accepted {#residuals}

| Residual | Effect | Handling |
|---|---|---|
| A body that lands after the fold processed its writer's edge removal has no edge, no row and no intent | bytes leak until the content is written again and dies | existing gap, CAS-33.2, unchanged. This design promises reclamation only of a body whose transition to zero GC observes while the body is present with a token that canonicalises |
| A body whose token does not canonicalise, on a switched pool. Exposure: a store whose tokens leave the accepted grammar on some path: one P7 does not sample (multipart), a proxy that weakens tags, a later change of the store | never adopted: one upload per reuse. Never condemned: the bytes leak until a writer of that content publishes a body whose token canonicalises; a row that still names the old token is then superseded (G7) | not reclaimed. `CASBlobReuseTokenRejected`, `CASGCIntentTokenRejected` and their ERROR lines show it; F2 reports `intent_unread` |
| Row-less intents: spare of a pending row (by design), a failed round followed by a spare, a late or stale create, a supersede whose intent delete failed, a rebuild | over a live body: one upload by the next writer; over a dead token: under 40 bytes. Their number grows until F3 runs | F3, run by the operator or on a schedule |
| Legacy markers left under marked bodies | 90 bytes each | F4 |
| Token form per store is taken from the client library | A5 | P7 at mount; an integration test records it per store ([§10.2](#tests)) |

## 9. Effects {#effects}

The implementation plan carries the plan-level tables: effects on tasks, test effects per suite, the doc files.

### 9.1 Decisions {#backlog}

| Decision | How this design answers it |
|---|---|
| decision-1, invariant 5 of `docs/superpowers/cas/AGENTS.md` | the `HEAD` before every `PUT` stays, and a body is never created conditionally (W6). A durable write step is removed and reads are added, as a protocol change with a model, not as an optimization. Record it there |
| decision-4 | generation 4 with an in-place upgrade path; older generations stay readable; no downgrade |
| decision-7 | the body delete stays conditional. The intent delete has no precondition and is typed as a write-once key. The marker delete is `removeCurrent`: conditional on its own `HEAD` |

### 9.2 Bucket requirements and counters {#docs}

- `D/operations/bucket-requirements.md`: the store returns `Content-Type` on `HEAD` and keeps it on copy; the
  credentials may write and delete under `blob-intents/`, with batch delete where the store has it; one token
  dialect per pool across all clients, tools included.
- Removed: `CASMetaCreateClean`, `CASMetaAbsentRepublish`, `CASMetaResurrectClean`
  (`src/Common/ProfileEvents.cpp:968-970`).
- Kept for marker mode: `CASMetaPut`, `CASMetaCompareSwap`, `CASMetaDelete`, `CASGCMetaOps`,
  `CASGCMetaWriteAnomaly`, `CASGCCondemnMarkerUnconfirmedCarry`; `cas_gc_meta_pool_size`.
- New: the four `CASBlobIntent*` request counters; `CASBlobReuseIntentPresent`, `CASBlobReuseLegacyRepublish`,
  `CASBlobReuseChangedAtSecondHead`, `CASBlobReuseTokenRejected`, `CASBlobPublishDigestMismatch`; `CASGCIntentCreated`,
  `CASGCIntentConfirmedExisting`, `CASGCIntentUnconfirmedCarry`, `CASGCIntentDeleted`, `CASGCIntentDeleteFailed`,
  `CASGCIntentLeftOnSpare`, `CASGCIntentTokenRejected`, `CASGCInheritedPendingDemoted`,
  `CASGCLegacyMarkerDeleted`.
- Changed meaning: `CASBlobPut`, `CASBlobGet`, `CASBlobHead` no longer include marker traffic; `CASBlobHead` is two
  per reused blob.

## 10. Verification {#verification}

### 10.1 TLA+ {#tla}

Required before implementation: `M/CaBlobIntentCore.tla` and `M/CaBlobIntentTransition.tla`, with a runner and a
results file, and a table "action → store requests it stands for" (CAS-375). `M/CaBlobPublishCore.tla` and
`M/CaGcCondemnMarkerGate.tla` stay as the record of today's protocol.

Model content:

- Every request is three actions: send, apply, respond. A sender may stop waiting; the apply may come later.
- Two GC actors with lease loss and lost commits; rows, cuts, spares, supersede, same-token re-condemnation.
- The create of G3 with the refused-create resolve read as its own action.
- The cleanup of F2 as two observations and a delete, by a client of the pool's dialect or of another, admitted
  before the switch or after it.
- The mode read of G1 as its own action, with the outcomes answered, absent and failed.
- Publication: fresh envelopes, content-derived and generation tokens, two absent publishers, a source that
  changes after staging, staging aliases.
- Transition: older and new writers, older and new GC, stamp and switch as separate actions, learned flags, mark
  bits set at staging, inherited rows, a paused pre-mount older actor, a linker that adds an edge from a live
  source edge with no observation.
- A pool created by this build: creation and switch as separate actions, two creators, a crash after each; no
  writer and no GC round before the switch.
- `INV_DELETE_EVIDENCE` ranges over deletes under intent authority. A marker-authority delete after the switch
  names an unmarked token.
- Read counters that are not reset on contention.

| Row | Expected |
|---|---|
| `safe_base`, `safe_base_generation` | green |
| `safe_late_put`, `safe_late_create`, `safe_late_intent_delete`, `safe_late_body_delete` | green |
| `safe_refused_create`, `safe_spare_recondemn`, `safe_lost_commit`, `safe_cleanup`, `safe_integrity`, `safe_budget` | green; `safe_budget` checks 1/1, 3/0 and 2/1 reads and `PUT`s on uncontended paths |
| `safe_transition`, `safe_stale_marker_round`, `safe_inherited_demoted`, `safe_unlearned_after_switch`, `safe_late_old_requests`, `safe_linker`, `safe_fresh_pool` | green |
| `alt_adopt_changed_token` | green; documents the alternative of [§8.1](#not-done) |
| `sab_no_second_head` (S1), `sab_cleanup_head_first` (S2), `sab_spare_deletes_intent` (S3), `sab_attempted_create_confirms` (S4), `sab_absent_conflict_confirms`, `sab_delete_on_graduation_cut`, `sab_ambiguous_delete_cleans_intent` (S9), `sab_cleanup_raw_spelling` (S8), `sab_cleanup_foreign_dialect` and `sab_cleanup_admitted_unswitched` (S10), `sab_stale_read` (S12) | red on `INV_COMMITTED_REF_HAS_BODY`, and on `INV_INTENT_PERMANENT` or `INV_DELETE_EVIDENCE` where the rule breaks first |
| `sab_resend_envelope`, `sab_staging_alias_two_copies` | red on `INV_NO_RECURRENCE` |
| `sab_unchecked_stream` (S7) | red on `INV_KEY_NAMES_PAYLOAD` |
| `sab_delete_despite_indegree`, `sab_writer_deletes_intent`, `sab_candidate_from_intent` | red |
| `sab_adopt_unmarked`, `sab_mark_before_learned`, `sab_no_stamp`, `sab_mode_cached`, `sab_mode_on_failed_read` (S11), `sab_switch_with_old_writer`, `sab_inherited_executed`, `sab_lost_stamp_old_bootstrap` | red on `INV_COMMITTED_REF_HAS_BODY` or `INV_MARKED_NEVER_MARKER_AUTHORITY` |
| `witness_*`: adoption, publish over an intent, spare with a standing intent, re-condemnation on an existing intent, late create after its row left, stale intent cleaned, legacy re-upload, changed token at W5, a deposed leader's delete lands, a new pool reaches T3 after a crash at N2 | each reachable |

Not modelled, to be covered by tests: token spelling beyond two renderings, a token that does not canonicalise,
the lease hand-over of a mount with another dialect, multipart transport, the mark probe.

### 10.2 Tests {#tests}

Tests are written first. A `LOGICAL_ERROR` expectation is a death test in a `*DeathTest` suite. Today's
marker-mode GC tests stay and run on a pool as an older build leaves it: no `created_generation`, no `blob_protocol`.

| Layer | Tests | Pins |
|---|---|---|
| gtest, writer, on the etag-faithful fakes (`EtagFaithfulPublicationBackend`, `ContentEtagBackend`) | `CASBlobIntentWriter.FirstCreationIsHeadAndOnePut`, `.CleanReuseIsHeadGetHead`, `.IntentPresentPublishesFreshOnce`, `.LegacyBodyIsRepublishedMarkedWithNoMetaRequest`, `.UnlearnedWriterNeverAdoptsAndPublishesUnmarked`, `.ChangedTokenAtSecondHeadPublishesWithoutRestart`, `.BodyGoneAtSecondHeadPublishes`, `.SameTokenInvalidSizeAtSecondHeadIsCorrupted`, `.LandedAmbiguousPublicationIsAdoptedInFourReads`, `.FailedIntentReadIsNotAbsence`, `.WriterNeverMutatesIntents`, `.MarkBitIsSharedByHardlinkCopies`, `.VerbatimCopyIsSelectedOnlyAfterAnAbsentFirstHead`, `.UncanonicalTokenPublishesFreshAndNeverThrows`, `.UncanonicalTokenAtSecondHeadPublishesFresh` | W1 to W7; S1; ledger |
| gtest, reads reach the store | `CASBlobIntentWriter.AdoptionReadsReachTheStoreAfterAPrimedAbsence`: read `I(B,t)` absent through the same pool, let GC confirm the intent and commit, then write. Assert the backend saw both `HEAD`s and the intent `GET` of this attempt, and that the writer publishes | W8; S12 |
| gtest, GC | `CASGcIntents.CondemnIsOneHead`, `.GraduationCreatesAndCommitsPending`, `.EqualOccupantConfirms`, `.AbsentOrDivergentOccupantCarries`, `.UnansweredCreateCarries`, `.DeleteIsThreeRequests`, `.IntentDeletesShareABatchRequest`, `.FailedIntentDeleteFailsTheRoundAndTheNextFinishes`, `.RefusedIntentDeleteKeepsTheRowPending`, `.UnansweredBodyDeleteKeepsTheIntent`, `.MismatchDeletesTheOldIntentAndDropsTheRow`, `.SpareLeavesTheIntent`, `.RecondemnationConfirmsOnTheStandingIntent`, `.SupersedeDeletesTheOldIntentOnce`, `.InheritedPendingIsDemotedBeforeAnyDelete`, `.LegacyBodyDeleteRemovesItsMarkerWithHeadAndDelete`, `.RebuildKeepsGenerationFourAndReadsNoIntent`, `.DryRunFollowsTheModeAndShowsNoInheritedDelete`, `.UncanonicalTokenWritesNoRowAndDropsAnInheritedOne`, `.UncanonicalRowTokenIsSupersededByACanonicalBody`, `.UncanonicalTokenAtTheDeleteHeadDropsTheRowWithNoRequest` | G2 to G10; S3 to S6, S9; 5 requests |
| gtest, GC mode | `CASGcIntentsMode.ModeIsReadAfterTheLeaseInEveryRound`, `.AbsentPoolMetaFailsTheRound`, `.FailedPoolMetaReadFailsTheRound`, `.UnknownProtocolWordFailsTheRound`, `.IntentModeBelowGenerationFourFails`, `.MarkerModeRoundFailsOnARowWithIntent`. Each failing case asserts zero requests after the read. `.StampCreatedStateIsAFreeLease`: a manual round leads at once | G1; S11; [§7.3](#command) |
| gtest, dialect | `CASBlobIntentDialect.MountOfASwitchedPoolIsRefused`, `.LearningStaysNotLearned`, `.GcRoundFailsAndTheMountLeavesGc` (no later lease write or heartbeat; `SYSTEM CAS GC RUN` and `REBUILD` are refused; another member takes the lease), `.FsckOnAReadOnlyOpenStops`, `.FsckStartedBeforeTheSwitchTouchesNoIntent` (the pool is switched in another dialect during the run), `.DryRunStops`, `.SwitchWithAnotherRecordedDialectIsRefused`. Each asserts zero delete and zero intent requests | the dialect rule; F0; S10 |
| gtest, identity and formats | canonical identity and each rejected form, with today's grammar unchanged for control keys; a generation over 20 digits; intent key round trip, rejected spellings, key length; `BlobIntent` in the format battery; `gc/state` generation kept by every writer; run row `intent` and the flag byte, each invalid combination; slot and epoch fields; read ceiling 3 rejects each | [§2](#formats), [§3.6](#failure-contract), [§6.1](#identity); S8 |
| gtest, `_pool_meta` | `blob_protocol`, `token_dialect` and `created_generation` survive `admitOrValidate` and `raiseMinReaderGeneration` (`CA/Pool/CasPoolMeta.cpp:98-123`) and every other read-modify-write; an unknown word, and each invalid pair, fails the decode | [§2.3](#control-formats), [§3.6](#failure-contract) |
| gtest, source check and probe | `CASBlobPublishDigest.FlippedByteIsRefusedBeforeCompletion`, `.StaleSourceOfASecondAbsentPublisherIsRefused`, `.S3StagedRestreamIsVerified`; one probe test per row P1 to P7 with a fake that fails it; the copy rows are skipped without copy support; `CASBlobIntentSwitch.CommandRefusesUnderSkipAccessCheck` | [§6.2](#mark-rules), [§6.3](#source-check); S7 |
| gtest, fsck | classes; `FixReadsTheIntentBeforeTheBody` with a hook that publishes between the reads; `FailedReadIsNeverStale`; `UncanonicalHeadTokenIsUnread`; `RefusedDeleteStopsTheRun`; `FixRefusesLegacyMarkersOnAnUnswitchedPool`. The failure cases assert that no key outside `intent_stale` was deleted | F0 to F6; S2 |
| gtest, pool created by this build | `CASBlobIntentNewPool.EndsSwitchedBeforeTheFirstWriter`, `.SequenceSendsNoGcStateRequest`, `.ManualGcRoundLeadsRightAfterCreation`, `.CrashAfterPoolMetaResumes`, `.LostSwitchAnswerResumes`, `.FailedSwitchFailsTheMountAndRestartResumes`, `.FailedProbeFailsTheMount`, `.SkippedProbeSwitchesWithTheMarkAssumed`, `.SkippedProbeMountLearnsOnAPoolCreatedByThisBuild`, `.FalsePresenceWarnsAndTheMountLearns`, `.ConcurrentCreatorsConverge`, `.CreatorWithAnotherDialectIsRefused`, `.DecommissionOpenDoesNotRunTheSequence`, `.UpgradedPoolIsNotSwitchedAutomatically` | [§7.4](#fresh-pool) |
| Stateless (`add-test`) | requests of one `INSERT` of new content (1 `HEAD`, 1 `PUT` per blob, no intent request), of a duplicate (2 `HEAD`, 1 intent `GET`, no `PUT`), and of a duplicate after `DROP` and two GC rounds | ledger |
| Integration, RustFS and the GCS emulator | the mark on each publication path the store has: single, multipart upload above 32 MiB, multipart copy above 32 MiB; marked reads marked and unmarked reads unmarked; the recorded token form on `HEAD`; an upgrade test like `test_cas_ref_move_compat`: an older image writes, stop, new image, GC rounds in marker mode, the command refuses a slot below 4, switch, an older server is refused, a legacy blob is uploaded once and then reused with three reads; `test_cas_gc_s3`: at most 5 requests per reclaimed blob | [§6.2](#mark-rules), [§7](#transition) |
| Soak, 2 hours, with process kills | `cas-fsck --fix stale-intents` every 10 minutes while writers and GC run; fsck: zero dangling refs; at the end, with writers and GC stopped, one more fix run and then a report with `intent_stale = 0`; no `CASBlobPublishDigestMismatch` | I8; F3 |

### 10.3 Measurements {#measurements}

- Before and after: the request ledger per `INSERT` of new and of duplicate content; the yardstick cards; a wide
  insert like the stand's (marker `PUT`s 54,997 → 0, upload thread time, wall time). The request saving is not a
  wall-time claim until measured.
- A fully duplicated second insert on a legacy pool (one upload per blob), and a third (three reads per blob).
- GC requests per reclaimed blob in both modes. CPU of the publication hash on a large body.

## 11. Open questions {#open}

| # | Question | Default if nobody answers |
|---|---|---|
| Q1 | Should the floor raise to 4 wait for the switch command, so the binary upgrade stays reversible until then? | No: every writable mount raises it, as generation 3 does |
| Q2 | Should the verbatim staged copy carry a source-token precondition (`x-amz-copy-source-if-match`) instead of resting on A6? | No precondition. The copy already reads the staging object with a `HEAD` (`S3:1115`), but `copyS3File` has no source-condition parameter: it would be one more change in shared code |
| Q3 | Is `SYSTEM CAS ENABLE BLOB INTENTS` the command name, and is an offline twin needed? | That name; no offline twin, because the command needs the mount's probe result |
