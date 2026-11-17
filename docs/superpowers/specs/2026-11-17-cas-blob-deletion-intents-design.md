---
description: 'Design for CAS blob publication without the per-blob `Clean` marker. A writer publishes a body with one `PUT` and writes nothing else. GC alone warns writers, with an immutable deletion intent keyed by the body incarnation, created only when a committed condemned row graduates. A writer reuses a body after `HEAD`, intent `GET`, `HEAD`. Ships as format generation 4: the build runs only this protocol, and a pool of an older build is moved once, offline, by `clickhouse-disks cas-migrate-pool`.'
sidebar_label: 'CAS: blob deletion intents'
sidebar_position: 118
slug: /superpowers/specs/cas-blob-deletion-intents-design
title: 'CAS: blob publication without a birth marker, deletion intents'
doc_type: 'reference'
---

# CAS: blob publication without a birth marker, deletion intents — design {#cas-blob-deletion-intents}

- Date: 2026-11-17. Status: design, not implemented. The steady state is model-checked
  (`M/CaBlobIntentProtocol.tla`, results in `M/CaBlobIntentProtocol_RESULTS.md`); the migration is not. No test was run.
- Code: worktree `cas-pr1`, commit `a966a374b17`. Every code `path:line` was read there.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`. `M/` =
  `docs/superpowers/models/`. `D/` = `docs/en/antalya/cas/`. `W:` = `CA/Pool/CasPartWriteTxn.cpp`. `G:` =
  `CA/Gc/CasGc.cpp`. `N:` = `CA/Gc/CasBlobInDegree.cpp`. `MW:` = `CA/Gc/CasGcMetaWriter.cpp`. `SR:` =
  `CA/Pool/CasServerRoot.h`. `B:` = `CA/Backend/CasObjectStorageBackend.cpp`. `Q:` = `CA/Backend/CasRequests.cpp`.
  `X:` = `CA/ContentAddressedTransaction.cpp`. `S3:` =
  `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp`.
- Terms. **Body**: the object at a blob key `B`. **Incarnation** `t`: the canonical identity ([§6.1](#identity)) of
  the body now at `B`. **Intent** `I(B,t)`: the object of [§2.1](#intent-object). **Marker**: today's `<blob>.meta`.
  **Edge**: a precommit or committed ref whose manifest names `B`. **Cut**: the reference snapshot one GC round folds.
  **Older build**: `G_BUILD` at most 3, the marker protocol. **Migrated**: `_pool_meta` has
  `min_reader_generation = 4`. **The tool**: `clickhouse-disks cas-migrate-pool` ([§7](#migration)). **Answered**: the
  store returned a result, success or error. **Refused**: an answered error, such as 403. **Unanswered**: no result
  inside the request policy.

## 1. Decision and effect {#decision}

1. **No birth marker.** A writer publishes a body with one unconditional `PUT` after its `HEAD`. It writes no `Clean`
   marker. Writers never read or write `.meta`, and never create, change or delete an intent.
2. **GC warns with immutable intents.** `I(B,t)` is keyed by the body incarnation, has constant bytes, and is created
   only by GC and the tool, deleted only by GC and `cas-fsck`. It exists only for bodies on their way to deletion and
   for those the migration covers.
3. **Write-ahead GC.** A condemned row is committed first, with no intent. A later round whose cut still shows
   in-degree 0 creates the intent; the confirmed create graduates the row. A round after that commit deletes.
4. **Reuse by three reads.** `HEAD B`, `GET I(B,t)`, `HEAD B`: adopt iff the intent is absent and both `HEAD`s show
   the same incarnation. Anything else publishes a fresh body from the local source.
5. **One identity.** A canonical token identity is used for intent keys and for every comparison; the raw token is
   kept only for `If-Match`. A pool records one token dialect, and every open checks it.
6. **Verified streaming.** Every streaming publication hashes the payload it sends and refuses before the object is
   completed.
7. **Generation 4, one protocol, offline migration.** The build has no marker code in the writer and one GC. It
   refuses a pool below generation 4 at every open and names the tool. A pool of an older build is moved once, with
   every member stopped, by the tool; a new pool is created at generation 4. Upward only. This avoids a second GC
   mode, a legacy branch in the writer, a mark on bodies and a change in shared S3 code.

**Why.** The marker is paid by every blob and read by few. On a stand, one `INSERT ... SELECT` of 201.6 M rows into a
109-column `ReplicatedMergeTree` wrote, on one replica (`system.blob_storage_log`): 54,997 bodies in 2,047
thread-seconds and 54,997 markers in 1,325 thread-seconds; ref logs and manifests took 50. The marker is 39 % of
upload thread time and 49 % of all `PUT`s (54,997 of 111,486). It bought 7,029 avoided body uploads.

**Cost.**

- Adoption now depends on body tokens never recurring at a key ([§5](#assumptions), A1), as today's exact-token delete
  already does (`B:889-893`; contract at `CA/Backend/CasBackend.h:120-139`). Today's writer never trusts absence: an
  absent marker means republish (`W:473-475`), and only a `Clean` marker read after the `HEAD` adopts (`W:446-452`).
  Adopting on an absent intent is sound only while the intent is keyed by an incarnation that names one body (I1, I4).
- Clean reuse costs 3 reads instead of 2.
- The pool is offline for the migration: every server stopped, then the tool's waits and about 2 requests per blob.
- A second kind of object (`blob-intents/`), generation 4, no downgrade, no mixed versions on one pool.

**Budget**, per blob and writer attempt on the successful uncontended path: first creation one `PUT` and at most 3
reads; clean reuse reads only; reuse after condemnation at most 2 `PUT`s and 3 reads. The ledger meets it.

**Request ledger.** Counted from the code, not measured. One publication counts as one `PUT`, whatever its
transport: `PutObject`, multipart upload, `CopyObject` or multipart copy (above 32 MiB, `src/IO/S3Defines.h:26`, `:34`).
The last column lists requests on other keys: the staging `HEAD` of a server-side copy (`S3:1115`,
`src/IO/S3/getObjectInfo.cpp:203`) and the staging `GET` of a publication streamed from S3 staging (`X:308-332`).

Writer, per blob and attempt, successful and uncontended:

| Case | Today | New | Other physical requests |
|---|---|---|---|
| First creation, streamed | `HEAD`, body `PUT`, marker `PUT`: 1 read, 2 `PUT` (`W:424`, `:509`, `:368`) | `HEAD`, `PUT`: 1 read, 1 `PUT` | S3 staging: 1 staging `GET` |
| First creation, staged copy | same | same | 1 staging `HEAD`: 2 reads in all |
| Clean reuse | `HEAD`, marker `GET`: 2 reads (`W:424`, `:446`) | `HEAD`, intent `GET` (404), `HEAD`: 3 reads | — |
| Reuse after condemnation | `HEAD`, marker `GET`, body `PUT`, marker `PUT`: 2 reads, 2 `PUT` (`W:473`, `:373`) | `HEAD`, intent `GET` (200), `PUT`: 2 reads, 1 `PUT` | S3 staging: 1 staging `GET` |
| Body present, marker absent | `HEAD`, marker `GET` (404), 2 `PUT` (`W:475`) | not a case: an absent intent adopts | — |
| A token that does not canonicalise | — | `HEAD`, `PUT`: 1 read, 1 `PUT` | S3 staging: 1 staging `GET` |

Writer, contended and ambiguous, by starting state:

| Starting state and event | Requests |
|---|---|
| Body with no intent; the second `HEAD` shows another token or no body | 3 reads, 1 `PUT`. No restart |
| Body absent; the publication is unanswered and landed; no intent appears | `HEAD`, `PUT`, `HEAD`, `GET`, `HEAD`: 4 reads, 1 `PUT` |
| Body absent; the publication is unanswered and did not land | `HEAD`, `PUT` per attempt: 2 reads, 2 `PUT` after two attempts |
| Body with an intent; unanswered publications | `HEAD`, intent `GET`, `PUT` per attempt |
| Any | at most 8 attempts inside one request window (`W:406`, `:296`) |
| `k` writers on one absent body | 1 `HEAD` and 1 `PUT` each: `k` uploads |

GC, per reclaimed blob, without the round overhead (lease, run and seal objects, the `gc/state` commit):

| Step | Today | New |
|---|---|---|
| Condemn | `HEAD`; marker `GET` + `PUT` (`G:2194`, `MW:45-51`): 3 | `HEAD`: 1 |
| Graduate | marker `GET` (`G:2271`): 1 | intent `PUT` with `If-None-Match`: 1; plus 1 `GET` when the intent already exists. An unanswered create adds a resolve read and may be reissued by the request policy (`Q:1097-1128`) |
| Delete | `HEAD` + `DELETE` (`G:591-593`); marker `GET` + `DELETE` (`MW:59-62`): 4 | `HEAD`, body `DELETE`, intent `DELETE`: 3. The intent `DELETE` is at most one request: rows of a round share batch requests ([§3.2](#gc-rules)) |
| Total | 8 | 5 |
| Body absent or replaced at the delete | 1 or 3 | `HEAD`, intent `DELETE`: 2. No body `DELETE` |
| Supersede (G7) | `HEAD`: 1 | `HEAD`, and 1 intent `DELETE` when the row was pending |
| Blob referenced again before graduation | 3, and a `Condemned` marker that forces one upload | 1, and no intent |

Migration, once per pool ([§7.1](#tool)):

| Item | Requests |
|---|---|
| Per `.meta` object | 1 `GET`, 1 `DELETE` (`If-Match`), and 2 keys of a `LIST` page of 1,000 (the body key is listed too) |
| Per blob with a marker that is not `Clean`, or with a condemned row | 1 `HEAD`; with a body present 1 intent `PUT`; 1 more `GET` when the intent exists (a rerun) |
| With `--intents-for-all-bodies`, per other body | 1 `HEAD`, 1 intent `PUT` |
| Per run object the adopted seal names | 1 `GET` |
| Per mount slot | 1 `GET` per observation and 1 at M7; 1 `PUT` when it is fenced |
| Fixed | the catalog, seal and `_pool_meta` reads; `gc/state` and `gc/hb` read twice at M2 and once at M7, `gc/state` also at M3; one `gc/state` `PUT`, one `_pool_meta` `PUT` |

Estimate for one table of 62,000 blobs, each with a marker: 124 `LIST` pages, 62,000 `GET`, 62,000 `DELETE`, about
124,100 requests; plus 2 per condemned blob (1,240 at 1 %) and the runs; about 248,000 with
`--intents-for-all-bodies`. Requests per key are independent and run in parallel. Duration is not measured.

## 2. Objects and formats {#formats}

`G_BLOB_INTENTS = 4`; `G_BUILD` 3 → 4 (`CA/Formats/CasFormat.h:16`). `mount_reader_floor` → 4
(`CA/Pool/CasPool.h:450`): the generation a pool must already have, not one a mount raises to. Unchanged: the blob
envelope, manifests, ref logs, runs and seals, the mount slot, the epoch counter. The `.meta` codec
(`FormatId::BlobMeta`) stays for the tool and `cas-inspect` only. Field validity: [§3.5](#failure-contract).

### 2.1 The intent object {#intent-object}

| Aspect | Rule |
|---|---|
| Key | `<pool>/blob-intents/<algo>/<hex[0:2]>/<hex>/<dialect>/<tag-hex>`. The first four segments mirror `Layout::blobKey` (`CA/Formats/CasLayout.cpp:35-38`); `<dialect>/<tag-hex>` is the canonical identity ([§6.1](#identity)). New `Layout::blobIntentKey(ref, identity)`, `blobIntentsPrefix`, `parseBlobIntentKey`. |
| Parse | `parseBlobIntentKey` accepts only the spelling `blobIntentKey` produces: known algo word, shard equal to `hex[0:2]`, dialect `etag`, `generation` or `emulated`, lowercase even-length hex that decodes to a tag the grammar of [§6.1](#identity) accepts for that dialect. Anything else is not an intent. |
| Length | The tag cap counts the bytes of the opaque tag: 128 for `etag` and `emulated`, 20 for `generation`. The key is at most 14 + 6 + 4 + 64 + 1 + 10 + 1 + 256 = 356 bytes after the pool prefix. Pool creation and the tool refuse a pool prefix longer than 1024 − 356 bytes; `blobIntentKey` throws `LOGICAL_ERROR` over 1024 bytes. |
| Bytes | Constant: `{"type":"cas_blob_intent","v":4}\n{}\n` (under 40 bytes). No round, no epoch, no size, no token: the key is the identity. A constant makes the confirmation of [§3.2](#gc-rules) G3 a byte comparison. |
| One body, many lives | An intent is created with `If-None-Match`. The key can be deleted and created again (a late create, a new condemnation of `t` after F3), always with that byte string. `Layout::writeOnceBlobIntentKey` still mints a `WriteOnceKey`, the capability for a delete with no precondition: the delete is sent only when `t` is gone for good (I4, I1), so it is right for every life of the key. The contracts of `WriteOnceKey` (`CA/Primitives/CasWriteOnceKey.h:10-14`) and `removeManyWriteOnce` (`CA/Backend/CasBackend.h:197-202`) widen from "the one body the key ever held" to "one byte string in every life". |
| Registry | `FormatId::BlobIntent = 26`, type `cas_blob_intent`, `TextFamily::Control`, `CompressionPolicy::PinnedRaw` (bytes are compared), object cap 4 KiB, line cap 1 KiB; change points `{4,4}` (`CA/Formats/CasFormat.cpp`). A later generation must keep accepting this record as a confirmation. |
| Request class | New `CasNs::BlobIntent` in `classifyCasNs` (`CA/Backend/CasInstrumentedBackend.cpp:119-136`), tested before the blob rule: `CASBlobIntentGet`, `CASBlobIntentPut`, `CASBlobIntentDelete`, `CASBlobIntentList`. |

### 2.2 Pool meta and `gc/state` {#control-formats}

| Object | Change | Written by |
|---|---|---|
| `_pool_meta` | `min_reader_generation = 4`. New key `token_dialect` (`etag`, `generation`, `emulated`), present iff the floor is 4, written once in the write that sets the floor. `PoolMeta` carries it, so every read-modify-write keeps it (`CA/Pool/CasPoolMeta.cpp:69-86`). A mount no longer raises the floor: the raise at `CA/Pool/CasPool.cpp:628-640`, `:904` goes | pool creation (`CA/Pool/CasPool.cpp:583-587`); the tool, M8 |
| `gc/state` | header 4, written with the explicit-version header writer (`CA/Formats/CasTextFormat.h:153`), not the write floor it stamps today (`CA/Formats/CasGcStateFormat.cpp:42`). Fields unchanged. The decoder (`:60`) reports the header generation | every writer of this build: lease bootstrap (`G:5204-5211`), renewal, commit, rebuild; the tool, M6. Header 4 is the barrier against an older GC ([§2.3](#gate)) |

### 2.3 The generation gate {#gate}

| Who meets what | Result |
|---|---|
| An older build, `_pool_meta` at floor 4: mount, read-only open, tools | refused at the floor (`CA/Formats/CasPoolMetaFormat.cpp:155-158`) |
| An older build that still runs, `gc/state` at header 4 | its lease read-modify-write decodes and throws (`G:5214`): no round starts. A round already past that read fails its `If-Match` commit (`G:1326`); its deletes went out before the commit ([§7.4](#late)) |
| This build, `_pool_meta` below floor 4: writable mount, read-only open, decommission, every tool but `cas-migrate-pool` | refused; the message names the tool ([§7.5](#refusals)) |
| This build, a `gc/state` below header 4 on a migrated pool | the GC round, the rebuild and the dry run fail. Only deletion of `gc/state` followed by an older bootstrap produces it: unsupported (A9) |
| An older build, an intent | its writer and GC never read the prefix; its `cas-inspect` answers `cas-inspect: unrecognized key layout '<key>'` (`CA/Tools/CasInspect.cpp:562`) |

## 3. Protocol {#protocol}

### 3.1 Writer {#writer-rules}

Replaces `PartWriteTxn::ensureBlobPresent`, `W:274-570`. The loop, its single frozen deadline and the cap of 8
attempts stay (`W:296`, `:406`, `:415-423`).

| # | Step | Outcome and what follows | Replaces |
|---|---|---|---|
| W1 | The precommit edge is durable before the first request | else `LOGICAL_ERROR`, unchanged | `W:277-281`, order at `X:472-474` |
| W2 | `HEAD B` | Absent → W6, first-publication form. Present → check the size against the envelope length and the source (`CORRUPTED_DATA` exception, unchanged) and take the token from this response | `W:424-444` |
| W3 | **Not adoptable:** a token that does not canonicalise ([§6.1](#identity)) | → W6. No other request. Counted (`CASBlobReuseTokenRejected`) and logged, one ERROR per mount; never an exception | `W:446-475` |
| W4 | Token canonical: `GET I(B,t)` | Any object at the key → W6, event `BlobReuseResurrect`. An answered 404 → W5 | marker `GET`, `W:446` |
| W5 | `HEAD B` again | Same canonical identity and valid size → adopt: `Materialized`, `CASBlobBodyPutAvoided`, event `BlobReuseAdopt`. Absent, another identity, or a token that does not canonicalise → W6, once, no return to W2. The same identity with an invalid size → `CORRUPTED_DATA` exception | `W:452-471` |
| W6 | Publish: one unconditional publication (a body is never created conditionally), sent once (`W:299`, `:509`), under a fresh envelope (`W:322-331`), digest-checked ([§6.2](#source-check)) | Answered success → `Materialized`. Unanswered → next loop iteration from W2 with the backoff (`W:511-540`). Answered failure → exception | `W:478-509` |
| W7 | After W6 | No request. The marker reconcile is deleted | `W:352-404`, `:542` |
| W8 | **Reads go to the store.** W2 is sent after the edge of W1, W4 after the answer of W2, W5 after the answer of W4, each through `op.head` or `op.read` (`Q:601-609`) | No cache answers them, and no step takes the answer of a request sent before the event it must follow | — |

- **A read without a usable answer.** A `HEAD B` or `GET I(B,t)` that is refused or unanswered is an exception: the
  part write fails and nothing is adopted. It is never read as absence.
- **Verbatim staged copy** (`W:482-488`) is selected only for the first publication of the staging object (A1), and
  only when W2 answered absent: that keeps the one unhashed publication ([§6.2](#source-check)) off a body the writer
  saw. It does not prove the key empty at the copy: a body another writer lands in between is replaced, and I2 then
  rests on A6. Today the copy also runs over a present body whose marker is absent (`W:425`, `:475`, `:482`).
- Admission checks before each proof stay (`W:454`, `:466`, `:545`, `:562`). The writer's marker code goes: `loadMeta`,
  `reconcileMetaClean`, `validateMetaSize`, `meta_key`.

### 3.2 GC {#gc-rules}

One mode. The marker writer (`MW:`), its pool and the phase `meta_pool_wait` (`G:781`, `:1276-1283`) are deleted.

| # | Rule | Replaces |
|---|---|---|
| G1 | **Admission.** A round runs on a pool this build opened: generation 4 and the recorded dialect, both checked at the open ([§6.1](#identity)). A `gc/state` it decodes below header 4 fails the round with no request after that read | — |
| G2 | **Condemn.** Trigger unchanged: a fresh transition to in-degree 0 (`N:637-648`). `HEAD B` (`G:2194`). Present with a token that canonicalises → a non-pending row with the observed token, size and round. Absent, or a token that does not canonicalise → no row. No other request | marker write, `G:2227` |
| G3 | **Graduate.** For a row an earlier round committed (`condemn_round < current_round`), not pending, with in-degree 0 in this round's cut, when destruction is not suppressed and the budget admits (`N:559-571`): `PUT I(B,t)` with `If-None-Match: *` and the canonical record (`op.create`). Confirmed → the row gets `pending` in this round's run and this round's commit (`G:1326`); nothing is deleted in this round. Not confirmed → the row is carried unchanged, counted, the round continues | marker read, `G:2264-2287`; `N:572-583` |
| G4 | **Delete.** For a row read with `pending` from the adopted state, after this round's cut showed in-degree 0 (`N:545-557`). (1) `HEAD B`. (2) Absent → `Gone`. Canonical identity equals the row's → `DELETE B If-Match <raw token of this HEAD>` (`op.remove`): `Removed`, `Gone` or `Mismatch`. Another identity → `Mismatch`, no body request. (3) After any answered outcome of (2): delete `I(B,t)` with the write-once verb below; an absent key is success. (4) The row leaves the state only after (2) and (3) both answered. After `Mismatch` it is dropped, not superseded, as today (`G:601-603`) | `G:583-595`; marker delete `G:634-640` |
| G5 | **A delete job without a usable answer throws.** That is an unanswered or refused `HEAD`, body `DELETE` or intent `DELETE`. The round fails before its commit, the row stays pending (`G:752-762`), and the next round starts again at (1). A body `DELETE` without an answer never leads to the intent `DELETE` | — |
| G6 | **Spare.** In-degree above 0 (`N:520-543`): the row is dropped. No request. An intent stays where it is | — |
| G7 | **Supersede.** A touched row at in-degree 0 whose `HEAD` shows another canonical identity `u` (`N:612-631`): the row becomes a candidate for `(B,u)`. A row token that does not canonicalise never equals a live token, so a canonical `u` supersedes it too ([§3.5](#failure-contract)). If the old row was pending: one delete of `I(B,t)` with the write-once verb, best effort; a failure is counted (`CASGCIntentDeleteFailed`). A row that was not pending gets no delete, though `I(B,t)` may exist: the tool made it, a round failed, a pending row was spared, a create landed late. Over a superseded token that intent protects nothing, F3 removes it, and removing it here would cost a request per supersede | `matches` at `N:616` |
| G8 | **Pending means confirmed.** A pending row's intent was confirmed: by G3 of this build, or, for a row committed before the migration, by M5 for the token current at that read ([§7.3](#inherited)). Either way the delete waits for a cut of this build at in-degree 0 (G4) | — |
| G9 | **Never.** GC never deletes an intent on a spare and never reads or writes `.meta`. A round derives no row from an intent; the rebuild does ([§3.4](#recovery)) | — |
| G10 | **Reads go to the store.** The `HEAD`s of G2 and G7 are sent in this round, after its lease (`G:787`); a read-ahead `HEAD` of the same round qualifies. The `HEAD` of G4 is sent after the cut. No cache answers them | — |

**G3: what confirms.** The gate consumes the result of its own create:

| Result | Confirmed |
|---|---|
| `Committed`: an answered success, or an unanswered attempt whose resolve read found equal bytes (`Q:1127-1128`) | yes |
| `Conflict` with an observed `Object` whose bytes equal the canonical record. A clean refusal returns `Conflict`, not `Committed` (`Q:1118-1119`); `create` resolves with a body read (`Q:1154-1158`) | yes |
| `Conflict` with `ProvenAbsent`, `NotObserved` or other bytes; `GaveUp`; any exception | no |
| A request that was sent, a job that finished, a drained queue | no. Today's marker worker counts a failed job as completed (`MW:91-99`) |

| Topic | Rule |
|---|---|
| Order inside one round | G2, G3, G6 and G7 run in the fold: G3 in `settleEntry` (`N:559-583`), G7 in `closeBlob` (`N:612-631`). G4 runs in the delete batch after the fold (`G:659-762`). A `HEAD` without a usable answer at G2 or G7 fails the round before its commit. |
| After `Mismatch` | The body now at `B` was published by a writer that met the intent. Its edge protects it, and a later transition to zero makes a new row (G2). Without an edge it is the late body of [§8.2](#residuals). |
| The intent delete (G4, G7, F3) | Keys are minted by `Layout::writeOnceBlobIntentKey` and deleted by `removeChunkWriteOnceOrOneByOne` (`G:541-556`) over `CasOperation::removeManyWriteOnce` (`Q:634`): one batch request for up to `bulkDeleteChunkKeys` keys, or one `DELETE` per key on a store without batch delete. Policy `Retry::standard()`; a reissue resends the chunk; an absent key is success; an exhausted policy or a refusal throws. In G4 the body step runs per row in the delete pool, as today; the intent keys of the rows whose body step answered are then deleted in chunks, and G5 applies to a chunk that throws. A refusal that persists (no delete right on `blob-intents/`) fails every round at that row, like a refused body delete today. |
| Read-ahead | A create may be issued ahead of the gate only for a carried row that no delta or retirement of this round touches (the rule of today's marker hints, `G:3667-3700`). A touched row is created at the gate: an intent over a blob that is then spared costs its next writer one upload. A create issued ahead also precedes the suppression and budget checks of G3; a row that then does not graduate keeps its intent, at the same cost. `GcReadAhead` gains a create verb (`CA/Gc/CasGcReadAhead.h:53-57`); the hint key needs the row's token, which `admit` does not get today (`G:3676`). |
| Budgets | Graduation and redelete budgets apply as today. No new count bound. |

### 3.3 Cleanup of intents without a row {#fsck-rules}

Row-less intents arise by design ([§8.2](#residuals)). `cas-fsck` handles them; GC has no circuit.

| # | Rule |
|---|---|
| F0 | **Admission.** The dialect check of the open ([§6.1](#identity)); it holds on a read-only open, which is how `cas-fsck` opens the pool (`programs/disks/CommandFsck.cpp:53-56`) |
| F1 | `cas-fsck` lists `blob-intents/`. A key that `parseBlobIntentKey` rejects is `intent_malformed`: reported, never deleted |
| F2 | For a listed intent: exact `GET` of the intent first, **then** `HEAD B`, sent after the answer of that `GET`. Bytes other than the canonical record → `intent_malformed`. Same canonical identity → `intent_current`. Absent, or another canonical identity → `intent_stale`. Intent gone at the `GET` → skip. A refused or unanswered read, or a `HEAD` token that does not canonicalise → `intent_unread`: counted, never stale |
| F3 | Fix mode (`cas-fsck --fix stale-intents`): delete each `intent_stale` key with the write-once verb of [§3.2](#gc-rules), using the two reads of F2 made in that order in the same run. Never from the listing alone, never with the `HEAD` first |
| F4 | Every `.meta` is `legacy_marker`: debris that no actor reads. `cas-fsck --fix legacy-markers` deletes each with `op.removeCurrent` (`Q:656`): a `HEAD`, then a delete conditional on what it saw |
| F5 | Intents are accounted objects in every class total. `body_without_meta` and `meta_without_body` (`CA/Tools/CasFsck.cpp:1206-1222`) are removed |
| F6 | **A fix run stays on the read-only open.** It sends its deletes through its own operation (`CA/Tools/CasFsck.cpp:447`): no mount, no lease, no probe. It deletes intent keys under F3 and `.meta` keys under F4, and writes nothing. A refused or unanswered delete stops the run with an error and the count deleted so far; a rerun continues |

These are the first mutating options of `cas-fsck`. The rule "a read-only open never mutates the pool"
(`CA/Pool/CasPool.cpp:445-446`) narrows to: it never writes, and it deletes only under an explicit `--fix`. An F3
delete needs no leadership: the run's own two reads justify it (I4). `cas-fsck` and `SYSTEM CAS FSCK` report the
classes and list `blob-intents/` beside `blobs/` (`CA/Tools/CasFsck.cpp:892-913`); only `cas-fsck` has the fix options.

### 3.4 Recovery and rebuild {#recovery}

| Event | State left | What happens next |
|---|---|---|
| GC leader dies after the create of G3, before its commit | row not pending; intent exists | the next round creates again, gets the equal occupant, confirms |
| Same, and the blob is referenced before the next round | row dropped by G6; intent over a live body | the next writer of that content uploads once; the old intent is then stale |
| Dies after the body `DELETE`, before the intent `DELETE` | row pending | next round: `HEAD` absent, intent `DELETE`, row dropped |
| Dies after the intent `DELETE`, before the commit | row pending | next round: `HEAD` absent, intent `DELETE` finds no key, row dropped |
| A create lands after its row left the state | a row-less intent | over a live body it costs one upload and stays until `t` is gone; F3 removes it once it is stale |
| Writer dies after its `PUT` | a body, its precommit edge abandoned | the next writer adopts it; GC condemns it when the edge is removed |
| `SYSTEM CAS GC REBUILD`, `cas-gc-rebuild` | no condemned row from the fold (`G:4762-4765`); intents stand | after its fold the rebuild lists `blob-intents/` and sends `HEAD B` per intent. An intent whose identity equals the body's, at rebuilt in-degree 0, yields a candidate row for that token. G3 confirms it on the intent as the equal occupant; the delete still needs the commit, a later cut and the `HEAD` of G4. Any other intent is left to F3 |

**Rows from intents.** An intent read over the current body is the confirmation G3 would get, so a candidate derived
from it is safe. Only the rebuild derives rows so; `cas-fsck` reports the observation as `intent_current`. A round
derives none: its trigger is a fresh transition to zero (G2), and a `LIST` per round costs requests for nothing.
The tool derives none either: it makes intents from rows and markers.

### 3.5 Failure contract {#failure-contract}

Request failures: with each rule. Generations: [§2.3](#gate). Refusal texts: [§7.5](#refusals).

| Field | Valid | Missing | Unknown or malformed |
|---|---|---|---|
| `_pool_meta.token_dialect` | `etag`, `generation`, `emulated`; present iff the floor is 4 | at floor 4: decode fails, `CORRUPTED_DATA` | unknown word: `UNKNOWN_FORMAT_VERSION`. Present below floor 4: `CORRUPTED_DATA` |
| A token that does not canonicalise, in GC | — | — | It justifies no request, and ends the row unless G7 supersedes it. From the `HEAD` of G2: no row. From the `HEAD` of G7: no new row; the old row is dropped and its intent stays. As the own token of an inherited row: the row equals no live token. If the G7 `HEAD` of this round shows a canonical `u`, G7 applies and the row becomes a candidate `(B,u)`, with no intent delete. Otherwise the row is dropped. From the `HEAD` of G4: no body delete, no intent delete, the row is dropped. Counted (`CASGCIntentTokenRejected`), one ERROR per round. GC never deletes such a body ([§8.2](#residuals)). Writer: W3, W5. `cas-fsck`: F2. The tool: no intent ([§7.1](#tool), M5) |
| Intent object bytes | the canonical record | — | other bytes at an intent key. Writer: an intent (W4 → W6). GC and the tool: not a confirmation, and the rebuild derives no row; GC carries the row (`CASGCIntentUnconfirmedCarry`, one ERROR per round), the tool fails. `cas-fsck`: `intent_malformed`, never deleted |

## 4. Invariants and the safety argument {#safety}

### 4.1 Invariants {#invariants}

| # | Invariant | Provided by |
|---|---|---|
| I1 | **Non-recurrence.** Once `t` stops being the incarnation of `B`, it never is again | A1 |
| I2 | **Content.** Every body ever at `B` has the pool's envelope length and the payload that hashes to `B` | [§6.2](#source-check), A6 |
| I3 | **Edge before observe.** A writer's edge on `B` is durable before its first `HEAD B`; it commits only while that precommit is the live owner | W1; `W:965-970` |
| I4 | **Intent permanence.** A delete of `I(B,t)` is sent only on evidence that `t` is gone from `B`: an answered observation that `B` is absent or has another canonical identity, or an answered `Removed` of that exact incarnation. That evidence is later than evidence that `t` had been current: the committed row (G4, G7) or the intent read first (F2). So while `t` is current, no such request was ever sent, and an existing `I(B,t)` stays | G4, G6, G7, G9, F3; the tool deletes no intent; I1; A2 (the observation is not stale); A5 |
| I5 | **Delete evidence.** Every `DELETE B If-Match t` this build sends follows: a confirmation that `I(B,t)` exists and the commit of the pending row; then a complete cut with in-degree 0, taken by a later round of this build; then a `HEAD B` that shows `t`. The confirmation is G3's, committed after it; for a row committed before the migration it is M5's, and the cut is after the migration | G3, G4, G8, M5; the pending branch of `settleEntry` runs only at in-degree 0 of this round's merge (`N:520-557`); `frontier_complete`, `G:3548-3551` |
| I6 | **Adoption.** A writer records an adoption only after W4 answered 404 and W5 showed the identity of W2 | W3 to W5, W8 |
| I7 | **Inherited authority.** No writer of this build runs before the floor is raised. At that write, `I(B,t)` exists for every `t` that is current at its key and that a delete sent by an older build can still name, outside R1 of [§7.4](#late). It protects reuse of `t`, not a ref that already names `t` (R3) | the order M5 before M8; [§7.4](#late) |
| I8 | **Goal.** Every ref this build commits by observation names a body protected by this protocol; a ref copied without observation is as safe as its source. Outside the residuals of [§8.2](#residuals), a committed ref never names a blob key with no body | [§4.2](#argument) |

### 4.2 The argument {#argument}

**Adoption.** Writer `W` of this build: edge at `Te`, W2 sees `t` at `T1`, W4 answers 404 at `T2`, W5 sees `t` at
`T3`. By I1, `t` was current during `[T1, T3]`. Suppose a delete removes `t` at `Td > T3`.

1. **This build sent it.** By I5 the delete has a confirmation at `Tc` and a cut `C` of this build, `Tc < C < Td`.
   The row's token was observed at its condemn `HEAD`, so by I1 `t` was current from then to `Td`. By I4 the intent
   exists during `[Tc, Td)`. `W` read it absent at `T2 < Td`, so `T2 < Tc`. By W8, `Te < T2`. So
   `Te < T2 < Tc < C`: the edge is durable before the cut. A complete cut counts it and the row is spared:
   contradiction. If the edge was removed before `C`, `W` cannot commit (I3).
2. **An older build sent it**, before the migration. By I7 the intent existed before `W` ran, and by I4 it stays while
   `t` is current. W4 could not answer 404: contradiction, outside R1.

**Publication.** `W` publishes `t'` under a fresh envelope after `Te`. `t'` is new (A1), so no earlier authority names
it, of this build or an older one. Any delete of `t'` needs a condemn `HEAD` after its appearance, then a later
committed round and its cut: the cut is after `Te` and counts the edge. A late delete of an older token misses: the
condition is exact. **Cleanup.** A delete of `I(B,t)` is sent only when `t` is gone for good (I4, I1), so it can land
at any time.

**No-bytes references.** `adoptEvidence` (`W:572-587`; callers `X:260`, `X:1262`, `CA/Parts/PartFolderAccess.cpp:457`)
adds an edge with no request. It needs a live source edge, which the cut counts, so this build never deletes under
it. Against an older delete already in flight the copy is exactly as safe as its source (R3 of [§7.4](#late)): the
intent protects reuse through W2 to W5, not a reference that already exists.

**Where evidence must be positive, where absence is accepted:**

| Decision | Evidence | Why |
|---|---|---|
| Graduation | positive: the create answered, or an equal occupant read | an unanswered create may never land; then nothing warns the writer |
| Intent absent at W4 | absence, an answered 404 only | sound by I4 and the second `HEAD`; a failed read is not absence |
| Body absent at W2, G2, G4, F2, M5 | absence, an answered 404 only | each leads to an action that is safe if a body appears later: publish, no row, intent cleanup under I1, no intent |
| A `LIST` | nominates intents (F1), candidate rows in the rebuild ([§3.4](#recovery)), markers and mount slots ([§7.4](#late)) only (decision-2) | a missing key delays cleanup, leaks bytes or leaves debris; it never authorizes a body delete |

### 4.3 Schedules each rule defeats {#schedules}

| # | Schedule | Defeated by |
|---|---|---|
| S1 | **No second `HEAD`.** GC confirms `I(B,t)`, commits pending, takes cut `C`. `W` adds its edge after `C`, sees `t`. GC deletes `t`, then `I(B,t)`. `W` reads the intent: 404, and would adopt a missing body | W5 |
| S2 | **Cleanup reads the body first.** fsck sees `B` absent. A writer publishes `t`. GC condemns `t`, creates `I(B,t)`, proceeds to delete. fsck then finds the intent and deletes it on its old observation. A writer adopts `t`; the delete lands | F2 order; I4 |
| S3 | **Spare with cleanup.** Leader `G1` holds a valid delete of `t` and stalls. `W1` adds an edge, sees the intent, starts its upload. `G2` spares the row and deletes the intent. `W2` adopts `t`. `G1`'s delete lands; `W1` never finishes | G6 |
| S4 | **Unanswered create.** The create is sent and never applied. GC counts it, commits pending, deletes. A writer in between read no intent and adopted | G3 table |
| S5 | **Deposed leader.** `G1` sends the create, gets no answer, loses the lease. It cannot graduate. `G2` confirms on its own, commits, waits for a later cut. `G1`'s late create only adds the same warning. A late delete of `G1` under older valid authority finds the intent still in place, so every writer since replaced `t` | G3; I4 |
| S6 | **Same token, condemned again.** Pending row spared, intent stays; references vanish; a new row names the same current `t`. The create is refused by the equal occupant | G3 row 2: a new commit and a new cut give a fresh order. No round or epoch in the bytes is needed |
| S7 | **Two absent publishers.** `W` stages `X`, sees `B` absent. `W`'s source turns into `Y` of equal length. `P` publishes `X` and commits. `W`'s first publication lands `Y` over `P`'s data | [§6.2](#source-check): every streaming publication is hashed, not only overwrites |
| S8 | **Spelling.** GC confirms `I(B,t)`. A later `HEAD` renders the same incarnation with other quoting. A textual compare reads "replaced", cleanup deletes a live intent | [§6.1](#identity): one identity for every comparison |
| S9 | **Ambiguous body delete.** The delete is unanswered and not applied; GC removes the intent; a writer adopts; a reissued delete lands | G5 |
| S10 | **Cleanup in another dialect.** GC confirms `I(B,t)`, commits pending, takes its delete cut. A cleanup client configured for another dialect reads the intent, then `HEAD B`: the same body renders as another identity. It calls the intent stale and deletes it. A writer adds its edge, reads no intent, adopts; the delete lands | the dialect rule of [§6.1](#identity) |
| S11 | **A stale read.** G7 or F2 takes the identity at `B` from a cache that shows `u` while `t` is current, and deletes a live intent. Or W4 takes a 404 from a request sent before the writer's edge, and GC confirmed in between | W8, G10, F2 |
| S12 | **Floor before intents.** The floor is raised while a body under older delete authority has no intent. A writer of this build adopts it; the older delete lands | [§7.1](#tool): M5 before M8 |
| S13 | **Inherited row with no intent.** The tool's create for an inherited pending row is sent and never applied. The floor is raised; a writer reads no intent and adopts; the first round at in-degree 0 deletes | M5: a create that is not confirmed fails the run before M8 |

## 5. Assumptions and the contract with the store {#assumptions}

| # | Assumption | What provides it | What stays assumed |
|---|---|---|---|
| A1 | A body token never recurs at a key | One publication call site, `W:509` through `Q:780`. Streaming: a fresh 128-bit tag per attempt (`W:326`, `:497`), one send (`W:299`, `B:894-900`). Verbatim copy: the staging tag is minted once (`X:797`), and the copy happens at most once per staging object because hardlink copies share the flag (`CA/ContentAddressedTransaction.h:162-165`, `X:252`, `:300`; `CA/Pool/CasPartWriteTxn.h:26-38`). Generation dialect: the store's own counter | No transport reissue after a request may have been applied (redirect, re-sign, reconnect). The store applies one request at most once. Random tags do not collide. A generation is never reused. A removed delete marker on a versioned bucket is external manipulation. The `emulated` dialect over local files (tests, single-node development): its token is the file mtime in nanoseconds plus an in-process counter dropped 2 s later (`B:400`, `:457`, `:578`), so a delete and a re-create inside one mtime tick can repeat a token. The contract comment that calls these tokens a never-reused sequence (`CA/Backend/CasBackend.h:135-136`) holds for the in-memory backend only and is corrected with the implementation |
| A2 | Reads are linearizable per key: a `HEAD` or `GET` sent after a write was applied reflects it, for every client, whether or not the writer got an answer. Safety needs it at: the adoption reads W2, W4, W5; the delete-site `HEAD` of G4, the supersede `HEAD` of G7 and the body `HEAD` of F2, where an absent or other-identity answer authorizes an intent delete (I4); the tool's reads M2 to M5 and M7 | the store; W8, G10 and F2 keep caches and earlier requests out | proxies and caches between a server and the store |
| A3 | A conditional delete removes only the named incarnation | the capability probe (`CA/Backend/CasProbe.cpp:47`); contract at `CA/Backend/CasBackend.h:120-139` | the probe samples; it does not certify non-recurrence (`:137-139`) |
| A4 | The size and the token of one `HEAD` describe one incarnation | the store | a proxy that mixes two responses |
| A5 | One token dialect and one spelling per pool, across clients | `token_dialect` in `_pool_meta`, checked at every open ([§6.1](#identity)); canonical identity | two clients that render one tag with different bytes beyond the quote pair |
| A6 | A staging object holds the bytes hashed at staging | the staging key is 32 random characters under the writer's epoch (`X:950`), written once, known to one transaction | corruption inside the store; hash collisions |
| A7 | Cuts are complete: every live or precommit edge is in a cut that authorizes a delete | `frontier_complete` suppresses destruction otherwise (`G:3548-3551`) | unchanged, not re-proved here |
| A8 | No older build works on a migrated pool | operator precondition: every older process is stopped before the tool runs. The floor and the `gc/state` header enforce; M2, M7 and the fences diagnose | a suspended older process, one started inside M7's window: R2, R4 of [§7.4](#late) |
| A9 | Control objects are not deleted, rolled back or copied between pools | — | external manipulation is unsupported. A deleted `gc/state` lets a late older bootstrap create one below header 4, which this build refuses ([§2.3](#gate)) |

This dependence is a shipping contract. A store whose tokens can recur must not host a pool under this protocol.

## 6. Token identity, source check {#mechanisms}

### 6.1 Token identity {#identity}

| Item | Rule |
|---|---|
| Canonical identity | (dialect word, lowercase hex of the opaque tag). The opaque tag is the token value with at most one balanced pair of double quotes removed. No case folding: tags are opaque, and folding could merge two incarnations. |
| Raw token | The value as the store returned it. Used only as `If-Match` (`G:593`). |
| Used for | Intent keys; W2 against W5; row matching at G4 and G7; fsck F2; the tool's M5. New `Etag::canonical`, and `sameIncarnation(row token, live token)`, which compares canonical identities. Rows keep the token as captured; both sides are canonicalised at compare time. It replaces `PersistedEtag::matches`, a textual compare (`CA/Backend/CasEtag.h:85-88`). |
| Grammar, two levels | `isIncarnationValue` (`CA/Backend/CasEtag.cpp:10-32`), which `CasRequests::tryMint` applies to every observation of every key (`Q:337-342`), is unchanged: it serves control objects too, and the tool's reads of an older pool. Canonicalisation applies only where this protocol needs an identity. It rejects, for `etag` and `emulated`: empty; a `W/` prefix; an unmatched or inner double quote; a control byte; leading or trailing whitespace; `*`; a comma; an opaque tag over 128 bytes. For `generation`: anything but canonical positive decimal of at most 20 digits. |
| A token that does not canonicalise is not an identity | Nobody compares it, builds a key from it, deletes on it or throws for it. The writer publishes fresh (W3, W5), GC ends the row or supersedes it ([§3.5](#failure-contract)), `cas-fsck` reports `intent_unread` (F2), the tool creates no intent. The cost is in [§8.2](#residuals). |
| Probe | The capability probe (`CA/Backend/CasProbe.cpp:47`, called at `CA/Pool/CasPool.cpp:546`) also requires that the token of its own scratch object canonicalises. A failure refuses the writable mount, pool creation and the tool: on such a store no body is reused or reclaimed. `skip_access_check` skips it (`:526`). |
| Quotes | The generation dialect already strips one pair (`CA/Backend/CasObjectStorageBackend.h:175-182`); the `etag` dialect keeps them in the raw token because `If-Match` needs them. Intent keys are derived only from a body `HEAD`, never from a listing or a write response. |

**The dialect rule.** `token_dialect` is recorded once in `_pool_meta`, in the write that sets floor 4. The dialect is
a property of the client configuration (`CA/Backend/CasObjectStorageBackend.h:110`, `B:139`, `S3:817-820`), so two
configurations of one bucket can differ. Every open of a migrated pool is refused unless its backend dialect equals
the recorded one: writable mount, read-only open, decommission open, the tool. Neither value changes during an open,
so every actor that compares identities inherits the check: writers, GC rounds, the rebuild, `cas-fsck`,
`cas-gc-dryrun`. A refused open sends no request on a blob or an intent.

### 6.2 Source check {#source-check}

| Item | Rule |
|---|---|
| Rule | Every streaming publication hashes exactly the payload bytes it forwards, with the pool algorithm in its streaming convention (`W:116-130`), and compares with the key's digest before the object is completed. |
| Seam | `StreamingBlobPublication` gains the expected `BlobRef` (`CA/Backend/CasBackend.h:54-59`). The check sits beside the length check (`B:933-950`), before `out->finalize` (`B:951`): for S3 the completing request, `PutObject` or `CompleteMultipartUpload`, is issued inside `preFinalize` (`src/IO/WriteBufferFromS3.cpp:164`, `:192`, `:205`). Mismatch: `out->cancel`, `CORRUPTED_DATA`, nothing published. The emulated path checks before its rename (`B:560`). |
| Covers | Local staging, reopened by path for every attempt (`X:337-340`); S3 staging re-streamed (`X:308-332`); memory sources. So every overwrite of a body the writer saw, and every streamed first publication. |
| Verbatim staged copy | Not hashed (`B:955-973`). It rests on A6 and runs at most once per staging object; its selection rule is in [§3.1](#writer-rules). |
| A reader during an overwrite | Sees the same payload. Body reads are not pinned to a token: CAS builds its `StoredObject` without an `etag` (`CA/ContentAddressedMetadataStorage.cpp:2050`, `:2170`), and `If-Match` is sent only for a non-empty one (`S3:404`, `src/IO/ReadBufferFromS3.cpp:604-605`). A ranged read that spans an overwrite continues in the new body, where offset and bytes are equal by I2. Today's condemned republish does the same. |
| Cost | No request; one more hash pass per streamed body. |

## 7. Migration {#migration}

One-time, offline, upward only, in place. Existing bodies are adopted by the ordinary writer rule, with no copy.

### 7.1 The tool {#tool}

`clickhouse-disks cas-migrate-pool [--grace-seconds N] [--intents-for-all-bodies] [--dry-run]`, on a disk configured
read-only like the other offline tools (`programs/disks/CommandCaGcRebuild.cpp:55-59`). It is the only code of this
build that opens a pool below generation 4 (`Pool::openForMigration`): no mount, no slot of its own, no writer, no GC.
The operator stops every server and tool of the pool, runs it, then starts the servers on this build.

| # | Step | Writes | Refuses or fails when |
|---|---|---|---|
| M1 | **Check.** Decode `_pool_meta`. Floor 4 with this backend's dialect → go to M9. Run the capability probe unless `skip_access_check` or `--dry-run` | probe scratch keys | floor below 3 (Q3); floor 4 with another dialect; pool prefix too long ([§2.1](#intent-object)); probe failure |
| M2 | **No live member.** Slots: every key under `gc/server-roots/` (one `LIST`) and the slot of every server root the catalog names (exact `GET`). A terminal slot passes: a farewell or `gc_fenced` (`SR:489-505`). Any other slot is watched on the tool's own clock for `mountObservationThresholdMs` (`SR:382-387`), with the disk's `mount_lease_ttl_ms` as the observer lease and the tool's poll step as the cadence, then fenced with the incarnation-guarded replace GC uses for a dead mount (`SR:425-466`). GC: the lease pair of `gc/state` and the pair of `gc/hb` are read first. Then the grace interval, then every slot and both pairs are read again | fences | a slot token changed, a fence met a conflict, a slot does not decode: names the `server_root_id`. A GC pair moved: names the lease owner |
| M3 | **Read the GC state.** `gc/state` with its token `e0`, the seal it names, every run the seal names. Collect every condemned row, candidate or pending | — | a read fails |
| M4 | **Read the markers.** `LIST` of `blobs/`; exact `GET` of every `.meta` key. Class: `Clean`, or not `Clean` (`Condemned`, or a body that does not decode) | — | a `GET` is refused or unanswered |
| M5 | **Convert.** For every blob with a marker that is not `Clean` or with a condemned row, and with `--intents-for-all-bodies` for every body M4 listed: `HEAD B`. Absent, or a token that does not canonicalise → nothing: such a body is never adopted (W3). Else `PUT I(B,t)` with `If-None-Match` for the current `t`, confirmed by the table of G3. So every inherited pending row whose body is still its token has a confirmed intent, or the run fails | intents | a create is not confirmed |
| M6 | **Fence the older GC.** Replace `gc/state` with `If-Match e0`: header 4, every field as read. No `gc/state` at M3: create it with `If-None-Match`, header 4, lease owner 0, `gc_shards` from `_pool_meta` | `gc/state` | a conflict: the run restarts at M2 |
| M7 | **Check again.** Immediately before M8: read every slot (the `LIST` and the catalog reads of M2) and both GC pairs again. `gc/state` must still have the token M6 wrote (on a rerun that skipped M6, the token M3 read) and the lease pair M2 saw, `gc/hb` its pair, every slot the token M2 left | — | anything moved, or a slot that is not terminal appeared: an older member started during the run. Names it. The operator stops that server and runs the tool again |
| M8 | **Raise the floor.** Read-modify-write of `_pool_meta`: `min_reader_generation = 4` and `token_dialect`. **Completion mark** | `_pool_meta` | the body it reads has floor 4 with another dialect |
| M9 | **Remove markers.** Delete every `.meta` read at M4 with `If-Match` the token of that read (`deleteMetaExact`, `CA/Pool/CasBlobMeta.cpp:42`). One that changed since is left to F4. On a rerun of a migrated pool: the code of F4 | deletes | a delete is refused or unanswered: rerun |

- **Order:** M2 before M3 to M5; M5 confirmed before M6; M7 after M6 and right before M8; M8 last of the protocol
  writes; M9 only after M8. So the tool deletes nothing before the pool is migrated. It prints the counts of each
  step, then `migration complete`. `--dry-run` runs M1 without the probe, the read-only slot scan
  (`probeNonTerminalMountSlots`, `SR:505`), M3 and M4, and writes nothing.
- **The lease after M6.** `gc/state` keeps the dead leader's pair, or owner 0. The background loop of the first member
  takes it after its two observations, as from any stalled leader (`G:5244-5268`); a manual round before is refused.
- **The grace interval is a heuristic.** `--grace-seconds`, default 120 s, the default `request_window_ms`
  (`CA/ContentAddressedSettings.cpp:90`). It detects a live GC leader only if it exceeds the `gc/hb` cadence,
  `gc_interval_sec / 4` (`CA/Gc/CasGcScheduler.h:209`); set it to at least twice that. It does not bound when the
  store applies a request, nor an older member's own request window, which is configurable.
- **`--intents-for-all-bodies`**, off by default. It blocks reuse of every current incarnation M4 enumerates, so no
  older delete can hit a body adopted after the migration. It does not cover a ref that already exists (R3), a token
  that reappears after the scan (case (e)), or a body the listing omits (case (d)). Cost: about twice the tool's
  requests; one upload per old blob on its first reuse; up to one stale intent per blob for F3. Use it on a pool that
  ran a 26.6.x GC (case (a)) or whose `gc/state` was rebuilt. The default leaves it off: it pays for R1, which needs a
  marker anomaly and a delete applied after the whole offline window, and leaves R2 and R4 open.

### 7.2 Restart and completion {#restart}

| Crash | Pool state | Next |
|---|---|---|
| Before M6 | still generation 3. Added: fences, intents. An older build mounts and works as before: it reads neither, and a fence is what its own GC writes for a dead mount. This build refuses | rerun, from M1 |
| After M6, before M8 | `gc/state` at header 4, floor 3. No older GC round can start, so an older build that mounts writes and never reclaims; this build refuses | rerun only. M6 is skipped when `gc/state` already has header 4: the run that wrote it had finished M5 |
| After M8 | migrated. Markers may remain: debris | rerun, or `cas-fsck --fix legacy-markers` |

- **Points of no return, in order.** M6: from then on an older build's GC is refused, so the pool cannot go back to
  the older build with reclamation. M8: an older build is refused at every open. Nothing this design supports leads
  back from either.
- **A rerun repeats M2 to M5** against what it reads. Every write is a create-if-absent, a conditional replace or a
  read-modify-write that decides on the current body, so a step that landed is not repeated, and two runs at once
  converge: the loser of M6 restarts and then skips M6. A run with another dialect that loses M8 is refused; F3
  removes the intents it made.

### 7.3 Inherited state {#inherited}

| Object at migration | After the migration |
|---|---|
| Body with a `Clean` marker or none, no row | adopted by W2 to W5 |
| Candidate row | carried as it is. G3 creates, meets the tool's intent as the equal occupant, confirms |
| Pending row | carried as it is. M5 confirmed `I(B,t)` for the token current at its `HEAD`. The first round of this build whose cut shows in-degree 0 deletes it (G4). A body replaced since gives `Mismatch` |
| Marker that is not `Clean`, no row (a spare, a rebuild) | an intent with no row. The next writer of that content uploads once; F3 removes the intent when it is stale |
| Every marker | deleted at M9 |

Rows are kept because dropping them would leak the bodies: GC makes a row only on a fresh transition to in-degree 0
(`N:637-648`), which a body already at zero does not have again. A pending row needs no rewrite: `settleEntry` reaches
its pending branch only when the merge of this round gives in-degree 0 (`N:520-557`), so the cut is always one of this
build, and the confirmation is M5's (I5).

### 7.4 Requests that land after the members stopped {#late}

M2 proves that no member renews. It revokes nothing a member already sent, and nothing bounds when a sent request is
applied (`D/operations/debugging.md:306`).

| Late request of an older build | Lands as | Outcome |
|---|---|---|
| GC `DELETE B If-Match t` | removes `t` if it is still current | Where `I(B,t)` exists before the first writer of this build (I7), no such writer adopts `t` (W4), and the intent stays while `t` is current (I4). It does not protect a ref that already names `t`, nor a copy of that ref made with no request: R3. Coverage: below |
| Writer marker write: the `Clean` create or the `Condemned` to `Clean` swap (`W:352-404`) | a `.meta` object, or refused | nothing reads `.meta`: debris for F4. Before M4's read of that key it turns a marker `Clean`: case (b) |
| Writer body `PUT` | a new incarnation `u`, if A1 held in that build; else case (e) | no authority names `u`: that needs two committed older rounds after `u` appeared, and none commits after M6. An intent M5 made for the replaced token is stale. With no edge left it is the late body of [§8.2](#residuals) |
| GC marker write or delete (`MW:45-51`, `:57-63`) | a `.meta` object, or its removal | only a committed pending row authorizes a delete; a marker does not. Debris |
| GC `gc/state` commit or lease write (`G:1326`, `G:5194`) | refused | `If-Match` a token older than M6's write. Header-4 bytes never equal an older body, so a content-derived token cannot recur. Landing before M6 it makes M6 conflict, and the run restarts. A late bootstrap create finds the key present |
| Slot renewal or farewell | refused on a fenced slot; before the fence, a token change | M2 or M7 reads it as a live member and refuses |
| `_pool_meta` read-modify-write | refused, or absorbed | conditional; M8 decides on the body it reads |

**Which tokens a late delete can name.** Only the token of a row read with `pending` from a committed state
(`N:545-557`, `G:583-595`). A build with the graduation gate set `pending` only after it read a `Condemned` marker
(`G:2264-2287`). From that read on, while `t` is current: GC never writes `Clean` (`MW:39-41`), and deletes a marker
only after its body delete answered removed or gone (`G:634-640`), only a `Condemned` of an earlier round, with
`If-Match` (`MW:57-63`); a writer writes `Clean` only after it published a new body (`W:509`, `:542`). So the marker
stays `Condemned` whether the row is carried, spared (`N:520-543`) or lost in a rebuild (`G:4762-4765`), and M4 and M5
turn it into the intent. A row still committed yields the intent through M3 too.

Five cases defeat that. In (a) to (d) the marker is not `Condemned` while `t` is current and under authority:

| # | Case |
|---|---|
| (a) | The pool ran a 26.6.x GC: a marker delete it sent can remove a marker GC relies on, and the rows it scheduled are deleted without a fresh marker read (`D/operations/debugging.md:305-307`) |
| (b) | A writer's `Clean` over a condemned body, written after the graduation read by `reconcileMetaClean` (`W:352-404`), whose read-modify-write fallback writes `Clean` over any marker that is not `Clean`: applied late on a store with content-derived tokens, or by the writer that published `t` itself. Both are accepted residuals of the marker protocol |
| (c) | A marker delete of an earlier incarnation that lands after `gc/state` was rebuilt at a lower round: the guard compares rounds (`MW:54-63`) |
| (d) | The `LIST` of M4 did not return the marker |
| (e) | The token recurred: an older build without the once-only staged copy (`CA/ContentAddressedTransaction.h:162-165`) copied one staging object twice, and the second copy lands late over a newer body |

- **R1, a late delete with no intent.** In (a) to (d) a row that is still committed yields the intent, so the tool
  misses `t` only if no committed row names `B` any more (a spare, a rebuild); in (e) it misses `t` unless M5 observed
  `t` itself. The miss matters only while a delete of `t` sent by a stopped leader is still in flight: a writer of
  this build can then adopt `t` and the delete can remove it. The result is a committed ref with no body, which
  `cas-fsck` reports.
- **R3, an inherited reference.** A ref committed under the older protocol whose body is under an outstanding older
  delete stays exposed after the migration, for example after case (b) let an older writer adopt `t`. A ref this
  build copies from it with no request (hardlink, relink fetch: `adoptEvidence`) inherits the exposure. The intent
  blocks reuse of `t`, not these refs. The migration neither creates nor repairs the exposure.
- **R4, an older server started during the migration.** M2 and M7 see its slot and refuse. Not covered: a server
  whose mount reads `_pool_meta` before M8 and claims its slot after M7's read. It then runs as the third row of the
  table below.
- **`LIST`.** The tool lists `blobs/` and `gc/server-roots/` on a pool with no member, the one listing this project
  lets nominate. Every marker is then read by exact key, and M9 deletes objects no actor reads. A missed `Clean`
  marker is debris; a missed marker that is not `Clean` is case (d). A missed slot is a member not checked: the catalog
  reads cover every root that owns a namespace, and only the listing sees a root that owns none.

**R2, a member that was suspended, not stopped.** M2 and M7 check renewals, not processes.

| Suspended | What it can still send after the migration | What stops it, and when |
|---|---|---|
| Mounted writer | requests admitted before the pause: late requests of the table above. Its mount plane checks the lease on its own boot clock before every attempt (`CA/Pool/CasMountRuntime.cpp:156-170`), so after a pause longer than the lease it starts nothing. On a host whose boot clock stood still it runs the marker writer: it publishes bodies with `Clean` markers and adopts any body whose key carries a decodable `Clean`, its own marker included (`W:452`) | its next renewal, refused on the fenced slot (`SR:461-463`). Its remount reads `_pool_meta` at floor 4 and stays down (`CA/Pool/CasPool.cpp:142-166`) |
| GC leader inside a round | its body deletes, with no lease check (`G:710`): late deletes, as above. Its commit is refused | its next lease read, which decodes header 4 and throws (`G:5214`). The scheduler logs `CA GC round failed (will retry next tick)` every tick (`CA/Gc/CasGcScheduler.cpp:489`) |
| Inside its mount sequence, after its `_pool_meta` read | it claims its slot, which a fence leaves reclaimable by the same server (`SR:464-466`), and runs as a mounted marker writer with a live lease. It never reads `_pool_meta` again (`CA/Pool/CasPool.cpp:630-631`). It adopts any body whose key carries a decodable `Clean` while GC of this build deletes under an intent it never reads: a committed ref with no body | nothing in the store. Only its GC fails |

**The operator precondition: every process of an older build is stopped, not suspended, before the tool runs, and
stays stopped.**

### 7.5 Refusals {#refusals}

The only registry of refusal texts. The first two come from existing gates.

| Who | Code | Message |
|---|---|---|
| Older build, any open of a migrated pool | `UNKNOWN_FORMAT_VERSION` | `CAS pool meta: pool requires reader generation 4 but this build supports at most 3` |
| Older build, GC lease read | `UNKNOWN_FORMAT_VERSION` | `CAS cas_gc_state: object requires reader generation 4 but this build supports at most 3` |
| This build, any open of a pool below generation 4 | `UNKNOWN_FORMAT_VERSION` | `CAS pool meta: pool is at reader generation <n>, this build needs 4; stop every server of the pool and run 'clickhouse-disks cas-migrate-pool'` |
| This build, GC: `gc/state` below header 4 | `INVALID_STATE` | `cas_gc_state is at generation <n> in a generation 4 pool` |
| Any open, dialect | `BAD_ARGUMENTS` | `CAS pool records token dialect '<a>', this disk uses '<b>': identities are not comparable` |
| The tool, a live member | `INVALID_STATE` | `mount slot of server root '<id>' changed during the migration; stop that server and run the tool again`, or `GC lease of owner <id> moved during the migration; stop that server and run the tool again` |
| The tool, floor below 3 | `BAD_ARGUMENTS` | `pool is at reader generation <n>; mount it once with a generation 3 build first` |
| Pool creation and the tool, prefix too long | `BAD_ARGUMENTS` | `pool prefix of <n> bytes leaves no room for blob intent keys; the limit is <m>` |
| Writable mount, pool creation, the tool: probe token | `NOT_IMPLEMENTED` | `the store returns a token that cannot serve as a blob identity: <token>` |
| Publication, digest differs | `CORRUPTED_DATA` | `payload of <key> does not hash to its key; upload aborted, nothing published` |

## 8. Not done, and residuals accepted {#scope}

### 8.1 Not done {#not-done}

| Not done | Reason |
|---|---|
| An online in-place switch: a mark on bodies (`Content-Type`), a mount probe, an operator command | a change in shared S3 code, two GC modes, a permanent legacy branch in the writer, and an argument for every mixed state |
| A new pool prefix with a data copy | every body is downloaded and uploaded: the envelope carries the pool id (`CA/Formats/CasPoolMetaFormat.h:23-24`); double storage meanwhile |
| A mutable per-hash marker that names the target token | fewer stored states, but conditional GC transitions on a shared object and an observation order to preserve; not model-checked. It has the same dependence on A1 |
| An inventory of bodies | its premise, "no edge in the run means unreferenced", fails on a rebuilt pool (CAS-33.3) |
| A GC circuit for row-less intents | recorded rate of pending rows spared on one production node: 117 in 16.8 days; a circuit can be added with no format change |
| Keeping a row on a spare | breaks the zero-I/O shard carry (`G:3714`) and forces a fold every round (`G:4402`) |
| The writer deletes an intent after its own upload | safe only on an unequivocal answer; a misread outcome loses data; saves under 40 bytes |
| Adopting at W5 when the token changed to `u` | safe under I1: `u` first appeared between the two `HEAD`s, so every authority on `u` has a cut after the edge. It would widen the adoption proof for a rare case |

### 8.2 Residuals accepted {#residuals}

| Residual | Effect | Handling |
|---|---|---|
| R1 to R4 of [§7.4](#late) | a committed ref with no body | R1, R3: none; `cas-fsck` reports the dangling ref. R2, R4: the operator precondition; M7 narrows R4 to its window |
| A body that lands after the fold processed its writer's edge removal has no edge, no row and no intent | bytes leak until the content is written again and dies | existing gap, CAS-33.2, unchanged. This design promises reclamation only of a body whose transition to zero GC observes while the body is present with a token that canonicalises |
| A body whose token does not canonicalise. Exposure: a store whose tokens leave the accepted grammar on a path the probe does not sample (multipart), a proxy that weakens tags, a later change of the store | never adopted: one upload per reuse. Never condemned: the bytes leak until a writer of that content publishes a body whose token canonicalises; a row that still names the old token is then superseded (G7) | not reclaimed. `CASBlobReuseTokenRejected`, `CASGCIntentTokenRejected` and their ERROR lines show it; F2 reports `intent_unread` |
| Row-less intents: spare of a pending row (by design), a failed round followed by a spare, a late or stale create, a supersede whose intent delete failed, a rebuild, the tool's intents over spared or superseded tokens, and with `--intents-for-all-bodies` up to one per body | over a live body: one upload by the next writer; over a dead token: under 40 bytes. Their number grows until F3 runs | F3, run by the operator or on a schedule |
| Markers written late by an older build, or left by a crashed M9 | 90 bytes each | F4 |
| Token form per store is taken from the client library | A5 | the probe at mount; an integration test records it per store ([§10.2](#tests)) |

## 9. Effects {#effects}

### 9.1 Decisions {#backlog}

| Decision | How this design answers it |
|---|---|
| decision-1, invariant 5 of `docs/superpowers/cas/AGENTS.md` | the `HEAD` before every `PUT` stays, and a body is never created conditionally (W6). A durable write step is removed and reads are added, as a protocol change with a model, not as an optimization. Record it there |
| decision-2 | a `LIST` nominates intents, markers and slots; no listing authorizes a body delete ([§7.4](#late)) |
| decision-4 | generation 4. The compatibility path is the offline tool, not a reader that accepts both formats: this build reads no pool below 4. Record the amendment; no downgrade |
| decision-7 | the body delete stays conditional. The intent delete has no precondition and is typed as a write-once key. Marker deletes are conditional: `If-Match` in M9, `removeCurrent` in F4 |

### 9.2 Backlog, tests, docs {#docs}

- **Backlog** (the plan carries the per-task tables). Moot: every task that exists for the marker or its residuals,
  in the writer and in GC (CAS-384, CAS-330, CAS-334, CAS-345, CAS-29.3, CAS-439, CAS-326). Re-targeted from marker
  requests to intent requests: CAS-29.4, CAS-336, CAS-284.5. fsck and byte classes (CAS-10, CAS-11, CAS-289.6):
  intents replace `.meta`, the pairing counters go. Unchanged: CAS-33.2, CAS-33.3. New: the tool, the generation gate.
- **Tests.** No marker protocol survives in the build, so every marker-era gtest is deleted or re-targeted to intents
  (`T/gtest_cas_gc_meta_writer.cpp`, `T/gtest_cas_blob_meta.cpp`, the marker cases of the GC round, read-ahead,
  part-write and fsck suites). None is pinned to a pool of an older build. The only fixtures in the older format are
  the tool's: a generation-3 pool with markers, rows in each state, slots and a `gc/state`.
  `T/gtest_cas_blob_meta_format.cpp` stays for the codec the tool reads. Every other fixture seeds generation 4.
- **Docs.** `D/operations/migration.md`: "Migrating a pool to generation 4" (stop every server, run the tool, start;
  what it refuses; the two points of no return, and that after M6 the only way on is to rerun the tool; downtime;
  `--intents-for-all-bodies`; R1 to R4 in plain words). `D/configuration.md`: no mixed versions;
  `gc_meta_pool_size` (`CA/ContentAddressedSettings.cpp:86`) is accepted and ignored.
  `D/architecture/blob-protocol.md`, `garbage-collection.md`: rewritten. `D/bucket-requirements.md`: rights under
  `blob-intents/`, batch delete, one token dialect per pool across clients and tools. `D/operations/troubleshooting.md`.
- **Counters.** Removed: the six `CASMeta*` counters (three at `src/Common/ProfileEvents.cpp:968-970`),
  `CASGCMetaOps`, `CASGCMetaWriteAnomaly`, `CASGCCondemnMarkerUnconfirmedCarry`. New: four `CASBlobIntent*` request
  counters; `CASBlobReuseIntentPresent`, `CASBlobReuseChangedAtSecondHead`, `CASBlobReuseTokenRejected`,
  `CASBlobPublishDigestMismatch`; `CASGCIntentCreated`, `CASGCIntentConfirmedExisting`, `CASGCIntentUnconfirmedCarry`,
  `CASGCIntentDeleted`, `CASGCIntentDeleteFailed`, `CASGCIntentLeftOnSpare`, `CASGCIntentTokenRejected`. `CASBlobPut`,
  `CASBlobGet`, `CASBlobHead` no longer include marker traffic; `CASBlobHead` is two per reused blob.

## 10. Verification {#verification}

### 10.1 TLA+ {#tla}

**Steady state: done.** `M/CaBlobIntentProtocol.tla`; runs, traces and the action-to-request table in
`M/CaBlobIntentProtocol_RESULTS.md` (commit `10f9f0f0021`). Its `Start=T1` configurations and
`M/CaBlobIntentFreshPool.tla` model an online switch this design dropped. `sab_candidate_from_intent` is green, with
the witness `wit_derived_row_deletes`: it is the alternative of [§3.4](#recovery) that must stay green, not a sabotage.
`M/CaBlobPublishCore.tla` and `M/CaGcCondemnMarkerGate.tla` stay as the record of the marker protocol.

**Migration: required before implementation.** `M/CaBlobIntentMigration.tla`, small: one blob key; a marker (absent,
`Clean`, `Condemned`); a committed row (none, candidate, pending); requests of stopped older members in flight, one of
each kind of [§7.4](#late); older members that start, or resume inside their mount sequence; the tool's steps M2 to
M9 as separate actions, with a crash after each and a rerun; then one writer, one linker and one GC of this build.

(a) Configurations that must be green:

| Row | Expected |
|---|---|
| `mig_base`, `mig_crash_after_each_step`, `mig_two_runs` | the pool ends below generation 4 and migratable, or migrated |
| `mig_late_old_delete`, `mig_late_old_put`, `mig_late_old_marker_write`, `mig_late_old_gc_commit`, `mig_late_old_clean` | green on `INV_COMMITTED_REF_HAS_BODY`, with the anomalies of cases (a) to (e) switched off |
| `mig_spared_row_marker_only`, `mig_rebuilt_state_marker_only` | the marker alone yields the intent |
| `mig_inherited_pending_executed` | the first round of this build at in-degree 0 deletes an inherited pending row, on M5's intent |
| `mig_member_started_before_recheck` | M7 refuses |
| `sab_mig_floor_before_intents` (S12), `sab_mig_rows_only`, `sab_mig_pending_without_confirmed_intent` (S13), `sab_mig_gc_state_not_rewritten`, `sab_mig_no_recheck` | red on `INV_DELETE_EVIDENCE` or `INV_COMMITTED_REF_HAS_BODY` |

(b) Admitted failing histories. Each is switched on by its own constant and must be RED on
`INV_COMMITTED_REF_HAS_BODY` when allowed: a green run means the model lacks the history, not that the residual is
closed.

| Row | Residual |
|---|---|
| `res_r1_marker_anomaly` | R1, cases (a) to (c): no `Condemned` marker, the row spared or rebuilt away, an older delete in flight |
| `res_r1_missed_listing`, `res_r1_token_recurs` | R1, cases (d) and (e); red also with `--intents-for-all-bodies` |
| `res_r3_inherited_ref` | R3: an older writer adopts on a late `Clean`, the row is spared, the migration runs, the linker copies the ref, the older delete lands; red also with `--intents-for-all-bodies` |
| `res_r4_started_in_window` | R4: a member reads `_pool_meta` before M8 and claims its slot after M7 |
| `res_r2_resumed_in_mount_sequence` | R2, third row |

Not modelled, to be covered by tests: token spelling beyond two renderings, a token that does not canonicalise,
multipart transport, the slot observation of M2 and M7, the rebuild's derivation of rows from intents beyond the
steady-state alternative.

### 10.2 Tests {#tests}

Tests are written first. A `LOGICAL_ERROR` expectation is a death test in a `*DeathTest` suite.

| Layer | Tests | Pins |
|---|---|---|
| gtest, writer, on the etag-faithful fakes (`EtagFaithfulPublicationBackend`, `ContentEtagBackend`) | `CASBlobIntentWriter.FirstCreationIsHeadAndOnePut`, `.CleanReuseIsHeadGetHead`, `.IntentPresentPublishesFreshOnce`, `.ChangedTokenAtSecondHeadPublishesWithoutRestart`, `.BodyGoneAtSecondHeadPublishes`, `.SameTokenInvalidSizeAtSecondHeadIsCorrupted`, `.LandedAmbiguousPublicationIsAdoptedInFourReads`, `.FailedIntentReadIsNotAbsence`, `.WriterNeverMutatesIntents`, `.NoMetaRequestOnAnyPath`, `.VerbatimCopyIsSelectedOnlyAfterAnAbsentFirstHead`, `.UncanonicalTokenPublishesFreshAndNeverThrows`, `.UncanonicalTokenAtSecondHeadPublishesFresh` | W1 to W7; S1; ledger |
| gtest, reads reach the store | `CASBlobIntentWriter.AdoptionReadsReachTheStoreAfterAPrimedAbsence`: read `I(B,t)` absent through the same pool, let GC confirm the intent and commit, then write. Assert the backend saw both `HEAD`s and the intent `GET` of this attempt, and that the writer publishes | W8; S11 |
| gtest, GC | `CASGcIntents.CondemnIsOneHead`, `.GraduationCreatesAndCommitsPending`, `.EqualOccupantConfirms`, `.AbsentOrDivergentOccupantCarries`, `.UnansweredCreateCarries`, `.DeleteIsThreeRequests`, `.IntentDeletesShareABatchRequest`, `.FailedIntentDeleteFailsTheRoundAndTheNextFinishes`, `.RefusedIntentDeleteKeepsTheRowPending`, `.UnansweredBodyDeleteKeepsTheIntent`, `.MismatchDeletesTheOldIntentAndDropsTheRow`, `.SpareLeavesTheIntent`, `.RecondemnationConfirmsOnTheStandingIntent`, `.SupersedeDeletesTheOldIntentOnce`, `.NoMetaRequestInARound`, `.RebuildWritesHeaderFour`, `.RebuildDerivesACandidateFromACurrentIntentAtZero`, `.RebuildDerivesNoRowFromAStaleIntent`, `.UncanonicalTokenWritesNoRow`, `.UncanonicalRowTokenIsSupersededByACanonicalBody`, `.UncanonicalTokenAtTheDeleteHeadDropsTheRowWithNoRequest` | G2 to G10; S3 to S6, S9; 5 requests |
| gtest, generation gate | `CASBlobIntentGeneration.WritableMountOfAGeneration3PoolIsRefusedAndNamesTheTool`, `.ReadOnlyOpenIsRefused`, `.DecommissionOpenIsRefused`, `.NewPoolIsCreatedAtGeneration4WithItsDialect`, `.MountNeverRaisesTheFloor`, `.GcStateBelowHeader4FailsTheRoundWithNoRequest`, `.ReadCeiling3RefusesPoolMetaAndGcState` | [§2.3](#gate), G1 |
| gtest, dialect | `CASBlobIntentDialect.WritableMountIsRefused`, `.ReadOnlyOpenIsRefused` (so `cas-fsck`, `cas-gc-dryrun` and `cas-gc-rebuild` never run), `.ToolOnAMigratedPoolOfAnotherDialectIsRefused`. Each asserts zero delete and zero intent requests | the dialect rule; S10 |
| gtest, identity and formats | canonical identity and each rejected form, with today's grammar unchanged for control keys; a generation over 20 digits; intent key round trip, rejected spellings, key length; `BlobIntent` in the format battery; the `gc/state` header 4 and runs unchanged; `token_dialect` survives `admitOrValidate` (`CA/Pool/CasPoolMeta.cpp:98-123`) and every other read-modify-write; an unknown word and each invalid pair fail the decode; the probe refuses a token that does not canonicalise | [§2](#formats), [§3.5](#failure-contract), [§6.1](#identity); S8 |
| gtest, source check | `CASBlobPublishDigest.FlippedByteIsRefusedBeforeCompletion`, `.StaleSourceOfASecondAbsentPublisherIsRefused`, `.S3StagedRestreamIsVerified` | [§6.2](#source-check); S7 |
| gtest, fsck | classes; `FixReadsTheIntentBeforeTheBody` with a hook that publishes between the reads; `FailedReadIsNeverStale`; `UncanonicalHeadTokenIsUnread`; `RefusedDeleteStopsTheRun`; `LegacyMarkersAreDebrisAndTheFixDeletesThem`. The failure cases assert that no key outside `intent_stale` and `.meta` was deleted | F0 to F6; S2 |
| gtest, the tool, on generation-3 fixtures | `CASMigratePool.RefusesASlotThatRenews`, `.FencesASilentSlotAfterTheThreshold`, `.RefusesAMovingGcLease`, `.RefusesAPoolBelowGeneration3`, `.CondemnedMarkerGetsAnIntentForTheCurrentToken`, `.UndecodableMarkerCountsAsCondemned`, `.EveryCondemnedRowGetsAnIntent` (candidate and pending), `.AbsentBodyAndUncanonicalTokenGetNone`, `.UnconfirmedCreateFailsTheRun`, `.RowsAreCarriedAsTheyAre`, `.GcStateIsRewrittenAtHeader4WithItsFieldsAsRead`, `.LateOldGcCommitRestartsTheRun`, `.RecheckRefusesASlotThatAppearedAfterTheGcStateRewrite`, `.RecheckRefusesAMovedGcPair`, `.FloorIsTheLastProtocolWrite`, `.NoMarkerIsDeletedBeforeTheFloor`, `.CrashAfterEachStepThenRerunCompletes` (one case per step), `.RerunOfAMigratedPoolOnlyRemovesMarkers`, `.TwoRunsConverge`, `.DryRunWritesNothingAndSendsNoProbe`, `.AllBodiesOptionGivesEveryListedBodyAnIntent`, `.OnlyThisCommandReachesOpenForMigration`, `.InheritedCandidateConfirmsOnTheStandingIntentAndTheNextRoundDeletes`, `.InheritedPendingRowIsDeletedByTheFirstRoundAtZero`, `.CleanMarkedBodyIsAdoptedWithNoUpload`, `.LateOldDeleteMissesAfterARepublish` | [§7](#migration); S12, S13 |
| Stateless (`add-test`) | requests of one `INSERT` of new content (1 `HEAD`, 1 `PUT` per blob, no intent request), of a duplicate (2 `HEAD`, 1 intent `GET`, no `PUT`), and of a duplicate after `DROP` and two GC rounds | ledger |
| Integration, RustFS and the GCS emulator | an upgrade test like `test_cas_ref_move_compat`: an older image writes, drops and runs GC until candidate and pending rows exist; one older server left up makes the tool refuse and name it; all stopped, the tool completes; the new image starts and the older image is refused; data reads back; a duplicate insert reuses with three reads and no upload; `cas-fsck` reports no dangling ref and no marker. The recorded token form on `HEAD` per store, multipart upload and multipart copy above 32 MiB included. `test_cas_gc_s3`: at most 5 requests per reclaimed blob | [§7](#migration), [§6.1](#identity) |
| Soak, 2 hours, with process kills | `cas-fsck --fix stale-intents` every 10 minutes while writers and GC run; fsck: zero dangling refs; at the end, with writers and GC stopped, one more fix run and then a report with `intent_stale = 0`; no `CASBlobPublishDigestMismatch` | I8; F3 |

### 10.3 Measurements {#measurements}

- Before and after: the request ledger per `INSERT` of new and of duplicate content; the yardstick cards; a wide
  insert like the stand's (marker `PUT`s 54,997 → 0, upload thread time, wall time). The request saving is not a
  wall-time claim until measured. GC requests per reclaimed blob. CPU of the publication hash on a large body.
- The tool on a pool like the stand's: requests per step, wall time; then a duplicated insert: three reads, no upload.

## 11. Open questions {#open}

| # | Question | Default if nobody answers |
|---|---|---|
| Q1 | Is `cas-migrate-pool` the command name? | That name |
| Q2 | Should the verbatim staged copy carry a source-token precondition (`x-amz-copy-source-if-match`) instead of resting on A6? | No precondition. The copy already reads the staging object with a `HEAD` (`S3:1115`), but `copyS3File` has no source-condition parameter: it would be a change in shared code |
| Q3 | Should the tool accept a pool below generation 3? | No: it is mounted once by a generation-3 build first, so the tool has one input format |
| Q4 | Should a condemned row get a new field for "intent confirmed"? | No field: every pending row's intent was confirmed, by G3 or by M5 (G8) |
| Q5 | Should `--intents-for-all-bodies` be the default? | No: [§7.1](#tool) |
| Q6 | Should the store stop a suspended older member (R2, R4)? Two partial guards: the tool writes a critical key into every root's epoch counter, so an older mount sequence fails its allocation; slots declare a generation and GC of this build deletes nothing while a live slot of an older build exists | Neither: the operator precondition stands. Each adds a format change and narrows R2 without closing it (a brand-new root has no counter; a slot the listing omits). M7 needs no format change |
| Q7 | Should M2 fence a silent slot, or only refuse and leave it to the operator? | Fence: it uses the rule GC already has, and it makes a suspended mounted writer fail at its next renewal |
| Q8 | Should markers be removed before the floor is raised? | After: before M8 the tool then deletes nothing, a rerun still finds the markers, and a run that stopped before M6 leaves a pool an older build can use as before |
| Q9 | Is one dialect check per open enough for GC, or should each round read `_pool_meta` again? | Per open: neither value changes during an open, and a round saves one `GET` |
| Q10 | How does `clickhouse-disks` reach a pool that the disk's own `startup` refuses (`CA/ContentAddressedMetadataStorage.cpp:842-883`)? | The disks client starts the disk with the pool open deferred for this command only; no server path reaches `Pool::openForMigration` |
| Q11 | Is 120 s the right default grace? | Yes, as a heuristic at default settings ([§7.1](#tool)) |
