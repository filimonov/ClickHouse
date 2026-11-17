---
description: 'Design for CAS blob publication without the per-blob `Clean` marker. A writer publishes a body with one `PUT` and writes nothing else. GC alone warns writers, with an immutable deletion intent keyed by the body incarnation, created in the round that finds no reference and kept until a later cut after the body delete. A writer reuses a body after `HEAD` and intent `GET`. Ships as format generation 4: the build runs only this protocol, and a pool of an older build is moved once, offline, by `clickhouse-disks cas-migrate-pool`.'
sidebar_label: 'CAS: blob deletion intents'
sidebar_position: 118
slug: /superpowers/specs/cas-blob-deletion-intents-design
title: 'CAS: blob publication without a birth marker, deletion intents'
doc_type: 'reference'
---

# CAS: blob publication without a birth marker, deletion intents — design {#cas-blob-deletion-intents}

- Date: 2026-11-17. Status: design, not implemented. No test was run. Models: [§10.1](#tla).
- Code: worktree `cas-pr1`, commit `ac144e848aa`. Every code `path:line` was read there.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`. `M/` =
  `docs/superpowers/models/`. `D/` = `docs/en/antalya/cas/`. `W:` = `CA/Pool/CasPartWriteTxn.cpp`. `G:` =
  `CA/Gc/CasGc.cpp`. `N:` = `CA/Gc/CasBlobInDegree.cpp`. `MW:` = `CA/Gc/CasGcMetaWriter.cpp`. `SR:` =
  `CA/Pool/CasServerRoot.h`. `B:` = `CA/Backend/CasObjectStorageBackend.cpp`. `Q:` = `CA/Backend/CasRequests.cpp`.
  `X:` = `CA/ContentAddressedTransaction.cpp`. `S3:` = `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp`.
- Terms. **Body**: the object at a blob key `B`. **Incarnation** `t`: the canonical identity ([§6.1](#identity)) of
  the body now at `B`. **Intent** `I(B,t)`: the object of [§2.1](#intent-object). **Marker**: today's `<blob>.meta`.
  **Edge**: a precommit or committed ref whose manifest names `B`. **Cut**: the reference snapshot one GC round folds.
  **Row**: the condemned entry GC keeps for a key in its runs, one per key: `candidate(t)`, `pending(t)` or
  `standing(t)` ([§3.2](#gc-rules)). **Orphan intent**: an intent with no row for its key. **Older build**:
  `G_BUILD` at most 3, the marker protocol. **Migrated**: `_pool_meta` has `min_reader_generation = 4`. **The tool**:
  `clickhouse-disks cas-migrate-pool` ([§7](#migration)). **Answered**: the store returned a result, success or error.
  **Refused**: an answered error, such as 403. **Unanswered**: no result inside the request policy.

## 1. Decision and effect {#decision}

1. **No birth marker.** A writer publishes a body with one unconditional `PUT` after its `HEAD`. It writes no `Clean`
   marker. Writers never read or write `.meta`, and never create, change or delete an intent.
2. **GC warns with immutable intents** keyed by the body incarnation: created by GC and the tool, deleted only by GC.
3. **Immediate condemnation, three states.** The round whose cut first shows no reference creates the intent; its
   commit records `pending(t)` if the create is confirmed. A later round deletes the body and records `standing(t)`.
   A round whose cut is taken after that commit and still shows no reference deletes the intent.
4. **Reuse by two reads.** `HEAD B`, then `GET I(B,t)`: adopt iff the intent is absent; else publish fresh.
5. **Orphan intents are adopted.** GC turns an intent with no row into a row. Nothing deletes an intent on absence.
6. **One identity.** One canonical token identity for intent keys and comparisons; one token dialect per pool.
7. **Verified streaming.** Every streaming publication hashes its payload and refuses before the object completes.
8. **Generation 4, offline migration.** One protocol, no marker code. A pool below generation 4 is refused at every
   open; the tool moves it once, with every member stopped. Upward only.

**Why.** The marker is paid by every blob and read by few. On a stand, one `INSERT ... SELECT` of 201.6 M rows into a
109-column `ReplicatedMergeTree` wrote, on one replica (`system.blob_storage_log`): 54,997 bodies in 2,047
thread-seconds and 54,997 markers in 1,325 thread-seconds; ref logs and manifests took 50. The marker is 39 % of
upload thread time and 49 % of all `PUT`s (54,997 of 111,486). It bought 7,029 avoided body uploads.

**Cost.**

- Adoption depends on body tokens never recurring at a key (A1), as today's exact-token delete does (`B:894-897`).
  Today's writer adopts only on a `Clean` marker (`W:446-452`); this one adopts on an absent intent, which is sound
  only because the intent names one incarnation and outlives its body (I1, I4).
- An intent is written for every blob with no reference for one round. A blob referenced again after such a round
  keeps a standing intent over a live body: its next reuser by observation uploads once more. A copy without bytes
  (hardlink, relink fetch) is unaffected.
- The pool is offline for the migration: every server stopped, then the tool's waits and about 2 requests per blob.
- A second kind of object (`blob-intents/`), generation 4, no downgrade, no mixed versions on one pool.

**Budget**, per blob and writer attempt on the successful uncontended path: birth 1 read and 1 `PUT`; clean reuse
2 reads; reuse after condemnation 2 reads and 1 `PUT`. A blob born once, reused `k` times and then reclaimed costs
`7 + 2k` requests, against `11 + 2k` today. The ledger meets it.

**Request ledger.** Counted from the code, not measured. One publication counts as one `PUT`, whatever its
transport: `PutObject`, multipart upload, `CopyObject` or multipart copy (above 32 MiB, `src/IO/S3Defines.h:26`, `:34`).
The last column lists requests on other keys: the staging `HEAD` of a server-side copy (`S3:1115`,
`src/IO/S3/getObjectInfo.cpp:203`) and the staging `GET` of a publication streamed from S3 staging (`X:314-338`).

Writer, per blob and attempt, successful and uncontended:

| Case | Today | New | Other physical requests |
|---|---|---|---|
| First creation | `HEAD`, body `PUT`, marker `PUT`: 1 read, 2 `PUT` (`W:424`, `:509`, `:368`) | `HEAD`, `PUT`: 1 read, 1 `PUT` | S3 staging: 1 staging `GET`; staged copy: 1 staging `HEAD` |
| Clean reuse | `HEAD`, marker `GET`: 2 reads (`W:424`, `:446`) | `HEAD`, intent `GET` (404): 2 reads | — |
| Reuse after condemnation | `HEAD`, marker `GET`, body `PUT`, marker `PUT`: 2 reads, 2 `PUT` (`W:473`, `:373`) | `HEAD`, intent `GET` (200), `PUT`: 2 reads, 1 `PUT` | S3 staging: 1 staging `GET` |
| A token that does not canonicalise | — | `HEAD`, `PUT`: 1 read, 1 `PUT` | S3 staging: 1 staging `GET` |

Writer, contended and ambiguous, by starting state; at most 8 attempts inside one request window (`W:406`, `:296`):

| Starting state and event | Requests |
|---|---|
| Body absent; the publication is unanswered and landed; no intent appears | `HEAD`, `PUT`, `HEAD`, `GET`: 3 reads, 1 `PUT` |
| Body absent; the publication is unanswered and did not land | `HEAD`, `PUT` per attempt: 2 reads, 2 `PUT` after two attempts |
| Body with an intent; unanswered publications | `HEAD`, intent `GET`, `PUT` per attempt |
| `k` writers on one absent body | 1 `HEAD` and 1 `PUT` each: `k` uploads |

GC, per reclaimed blob, without the round overhead (lease, run and seal objects, the `gc/state` commit):

| Step | Today | New |
|---|---|---|
| Condemn | `HEAD`; marker `GET` + `PUT` (`G:2194`, `MW:45-51`): 3 | `HEAD`, intent `PUT` with `If-None-Match`: 2. Plus 1 `GET` when the intent already exists. An unanswered create adds a resolve read and may be reissued by the request policy (`Q:1097-1128`) |
| Graduate | marker `GET` (`G:2271`): 1 | none: the condemning round's create is the confirmation |
| Delete | `HEAD` + `DELETE` (`G:591-593`); marker `GET` + `DELETE` (`MW:59-62`): 4 | `HEAD`, body `DELETE`: 2 |
| Remove the intent | — | intent `DELETE` in a later round: 1. Rows of a round share batch requests ([§3.2](#gc-rules)) |
| Total; rounds from "no reference" to the body delete | 8; 3 rounds | 5; 2 rounds |
| Re-created key: condemn `u`, remove `I(B,t)` | — | `HEAD`, intent `DELETE`, intent `PUT`: one round, one commit |
| Blob referenced again after one round at in-degree 0 | 3, and a `Condemned` marker that forces one upload | 2, and a standing intent that forces one upload |
| Orphan intent adopted | — | 1 `HEAD`, plus its share of one `LIST` page per round |

Migration, once per pool ([§7.1](#tool)):

| Item | Requests |
|---|---|
| Per `.meta` object | 1 `GET`, 1 `DELETE` (`If-Match`), and 2 keys of a `LIST` page of 1,000 (the body key is listed too) |
| Per blob with a marker that is not `Clean`, or with a condemned row | 1 `HEAD`; 1 intent `PUT` for the current token when a body is present; 1 intent `PUT` for the row's token when an inherited pending row's body is absent or has another token; 1 more `GET` per intent that exists (a rerun) |
| With `--intents-for-all-bodies`, per other body | 1 `HEAD`, 1 intent `PUT` |
| Per run object the adopted seal names | 1 `GET` |
| Per mount slot | 1 `GET` per observation and 1 at M7; 1 `PUT` when it is fenced |
| Fixed | the catalog, seal and `_pool_meta` reads; `gc/state` and `gc/hb` read twice at M2 and once at M7, `gc/state` also at M3; one `gc/state` `PUT`, one `_pool_meta` `PUT` |

## 2. Objects and formats {#formats}

`G_BLOB_INTENTS = 4`; `G_BUILD` 3 → 4 (`CA/Formats/CasFormat.h:16`). `mount_reader_floor` → 4
(`CA/Pool/CasPool.h:450`): the generation a pool must already have, not one a mount raises to. Unchanged: the blob
envelope, manifests, ref logs, seals, the mount slot, the epoch counter. The `.meta` codec (`FormatId::BlobMeta`)
stays for the tool and `cas-inspect` only. Field validity: [§3.5](#failure-contract).

### 2.1 The intent object {#intent-object}

| Aspect | Rule |
|---|---|
| Key | `<pool>/blob-intents/<algo>/<hex[0:2]>/<hex>/<dialect>/<tag-hex>`. The first four segments mirror `Layout::blobKey` (`CA/Formats/CasLayout.cpp:35-38`); `<dialect>/<tag-hex>` is the canonical identity ([§6.1](#identity)). New `Layout::blobIntentKey(ref, identity)`, `blobIntentsPrefix`, `parseBlobIntentKey`. |
| Parse | `parseBlobIntentKey` accepts only the spelling `blobIntentKey` produces: known algo word, shard equal to `hex[0:2]`, dialect `etag`, `generation` or `emulated`, lowercase even-length hex that decodes to a tag the grammar of [§6.1](#identity) accepts for that dialect. Anything else is not an intent. |
| Length | The tag cap counts the bytes of the opaque tag: 128 for `etag` and `emulated`, 20 for `generation`. The key is at most 14 + 6 + 4 + 64 + 1 + 10 + 1 + 256 = 356 bytes after the pool prefix. Pool creation and the tool refuse a pool prefix longer than 1024 − 356 bytes; `blobIntentKey` throws `LOGICAL_ERROR` over 1024 bytes. |
| Bytes | Constant: `{"type":"cas_blob_intent","v":4}\n{}\n` (under 40 bytes). No round, no epoch, no size, no token: the key is the identity. A constant makes the confirmation of [§3.2](#gc-rules) a byte comparison. |
| One body, many lives | An intent is created with `If-None-Match`. The key can be deleted and created again (a late create of a deposed leader), always with that byte string. `Layout::writeOnceBlobIntentKey` still mints a `WriteOnceKey`, the capability for a delete with no precondition: the delete is sent only after `t` was observed gone from `B` (I4, I1), so it is right for every life of the key. The contracts of `WriteOnceKey` (`CA/Primitives/CasWriteOnceKey.h:10-14`) and `removeManyWriteOnce` (`CA/Backend/CasBackend.h:205-210`) widen from "the one body the key ever held" to "one byte string in every life". |
| Registry | `FormatId::BlobIntent = 26`, type `cas_blob_intent`, `TextFamily::Control`, `CompressionPolicy::PinnedRaw` (bytes are compared), object cap 4 KiB, line cap 1 KiB; change points `{4,4}` (`CA/Formats/CasFormat.cpp`). A later generation must keep accepting this record as a confirmation. |
| Request class | New `CasNs::BlobIntent` in `classifyCasNs` (`CA/Backend/CasInstrumentedBackend.cpp:119-136`), tested before the blob rule: `CASBlobIntentGet`, `CASBlobIntentPut`, `CASBlobIntentDelete`, `CASBlobIntentList`. |

### 2.2 Pool meta, `gc/state`, runs {#control-formats}

| Object | Change | Written by |
|---|---|---|
| `_pool_meta` | `min_reader_generation = 4`. New key `token_dialect` (`etag`, `generation`, `emulated`), present iff the floor is 4, written once in the write that sets the floor. `PoolMeta` carries it, so every read-modify-write keeps it (`CA/Pool/CasPoolMeta.cpp:69-86`). A mount no longer raises the floor: the raise at `CA/Pool/CasPool.cpp:628-640`, `:904` goes | pool creation (`CA/Pool/CasPool.cpp:583-587`); the tool, M8 |
| `gc/state` | header 4, written with the explicit-version header writer (`CA/Formats/CasTextFormat.h:153`), not the write floor it stamps today (`CA/Formats/CasGcStateFormat.cpp:42`). The decoder (`:60`) reports the header generation. New field `intent_sweep_cursor`, beside `manifest_sweep_cursor` (`CA/Formats/CasGcStateFormat.h:37`); absent decodes as empty | every writer of this build: lease bootstrap (`G:5204-5211`), renewal, commit, rebuild; the tool, M6. Header 4 is the barrier against an older GC ([§2.3](#gate)) |
| Run rows | A condemned row gains the bit `standing`, beside `pending` and `confirmed` (`CA/Formats/CasRecordStreamFormat.cpp:200-204`); absent decodes as false, so an inherited row is `candidate` or `pending` | GC of this build |

### 2.3 The generation gate {#gate}

| Who meets what | Result |
|---|---|
| An older build, `_pool_meta` at floor 4: mount, read-only open, tools | refused at the floor (`CA/Formats/CasPoolMetaFormat.cpp:155-158`) |
| An older build that still runs, `gc/state` at header 4 | its lease read-modify-write decodes and throws (`G:5214`): no round starts. A round already past that read fails its `If-Match` commit (`G:1326`); its deletes went out before the commit ([§7.4](#late)) |
| This build, `_pool_meta` below floor 4: writable mount, read-only open, decommission, every tool but `cas-migrate-pool` | refused; the message names the tool ([§7.5](#refusals)) |
| This build, a `gc/state` below header 4 on a migrated pool | the GC round, the rebuild and the dry run fail. Only deletion of `gc/state` followed by an older bootstrap produces it: unsupported (A9) |

## 3. Protocol {#protocol}

### 3.1 Writer {#writer-rules}

Replaces `PartWriteTxn::ensureBlobPresent`, `W:274-570`. The loop, its single frozen deadline and the cap of 8
attempts stay (`W:296`, `:406`, `:415-423`).

| # | Step | Outcome and what follows | Replaces |
|---|---|---|---|
| W1 | The precommit edge is durable before the first request | else `LOGICAL_ERROR`, unchanged | `W:277-281`, order at `X:479-481` |
| W2 | `HEAD B` | Absent → W5, first-publication form. Present → check the size against the envelope length and the source (`CORRUPTED_DATA` exception, unchanged) and take the token from this response | `W:424-444` |
| W3 | **Not adoptable:** a token that does not canonicalise ([§6.1](#identity)) | → W5. No other request. Counted (`CASBlobReuseTokenRejected`) and logged, one ERROR per mount; never an exception | `W:446-475` |
| W4 | Token canonical: `GET I(B,t)` | Any object at the key → W5, event `BlobReuseResurrect`. An answered 404 → adopt: `Materialized`, `CASBlobBodyPutAvoided`, event `BlobReuseAdopt` | marker `GET` and adoption, `W:446-471` |
| W5 | Publish: one unconditional publication (a body is never created conditionally), sent once (`W:299`, `:509`), under a fresh envelope (`W:322-331`), digest-checked ([§6.2](#source-check)) | Answered success → `Materialized`. Unanswered → next loop iteration from W2 with the backoff (`W:511-540`). Answered failure → exception | `W:478-509` |
| W6 | After W5 | No request. The marker reconcile is deleted | `W:352-404`, `:542` |
| W7 | **Reads go to the store.** W2 is sent after the edge of W1, W4 after the answer of W2, each through `op.head` or `op.read` (`Q:601-609`) | No cache answers them, and no step takes the answer of a request sent before the event it must follow | — |

- **A read without a usable answer.** A `HEAD B` or `GET I(B,t)` that is refused or unanswered is an exception: the
  part write fails and nothing is adopted. It is never read as absence.
- **Verbatim staged copy** (`W:482-488`) is selected only for the first publication of the staging object (A1), and
  only when W2 answered absent: that keeps the one unhashed publication ([§6.2](#source-check)) off a body the writer
  saw. It does not prove the key empty at the copy: a body another writer lands in between is replaced, and I2 then
  rests on A6.
- Admission checks before each proof stay (`W:454`, `:466`, `:545`, `:562`). The writer's marker code goes: `loadMeta`,
  `reconcileMetaClean`, `validateMetaSize`, `meta_key`.

### 3.2 GC {#gc-rules}

One mode. The marker writer (`MW:`), its pool and the phase `meta_pool_wait` (`G:781`, `:1276-1283`) are deleted.
A row is in one of three states. "Zero" below means: this round's cut shows in-degree 0 for the key.

| Row | Recorded when | At zero | At in-degree above 0 |
|---|---|---|---|
| none | — | a fresh transition to zero: G2 | — |
| `candidate(t)` | G2, G3 or G7 could not confirm the create, or G9 adopted an intent over a body with token `t` | G3: create again | dropped; an intent that landed stays (G6) |
| `pending(t)` | the create was confirmed | G4: `HEAD`, body `DELETE` → `standing(t)` | dropped; the intent stays (G6) |
| `standing(t)` | the body delete answered, or the body was absent or replaced | G8: intent `DELETE`, row dropped | kept, with its intent (G6) |
| any, `HEAD` shows `u ≠ t` | — | G7: `standing`/`pending`/`candidate` of `t` → `pending(u)` or `candidate(u)` in one commit | as its state |

| # | Rule | Replaces |
|---|---|---|
| G1 | **Admission.** A round runs on a pool this build opened: generation 4 and the recorded dialect, both checked at the open ([§6.1](#identity)). A `gc/state` it decodes below header 4 fails the round with no request after that read | — |
| G2 | **Condemn and create.** Trigger unchanged: a fresh transition to zero (`N:637-648`). `HEAD B` (`G:2194`). Present with a token that canonicalises: `PUT I(B,t)` with `If-None-Match: *` and the canonical record (`op.create`) in this round. Confirmed (table below) → `pending(t)` in this round's commit. Not confirmed, or over the creation budget → `candidate(t)`, never counted as confirmed. Absent, or a token that does not canonicalise → no row. Nothing is deleted in this round | marker write, `G:2227` |
| G3 | **Retry.** `candidate(t)` committed by an earlier round (`condemn_round < current_round`), at zero, when destruction is not suppressed and the budget admits (`N:559-571`): the create of G2 again. Confirmed → `pending(t)`; else the row is carried, counted | marker read, `G:2264-2287`; `N:572-583` |
| G4 | **Delete the body.** `pending(t)` read from the adopted state, at zero (`N:545-557`). (1) `HEAD B`. (2) Same canonical identity → `DELETE B If-Match <raw token of this HEAD>` (`op.remove`). Absent → no body request. Another canonical identity `u` → G7. A token that does not canonicalise → the row is carried unchanged. (3) After the answer, or on absence: `standing(t)` in this round's commit, whatever `Removed`, `Gone` or `Mismatch` said. No intent request | `G:583-595`; marker delete `G:634-640` |
| G5 | **A delete job without a usable answer throws.** That is an unanswered or refused `HEAD`, body `DELETE` or intent `DELETE`. The round fails before its commit and the row keeps its state (`G:752-762`). A body `DELETE` without an answer never leads to `standing` | — |
| G6 | **Spare.** In-degree above 0 (`N:520-543`): `candidate` and `pending` rows are dropped, with no request; `standing(t)` is kept. An intent stays where it is in every case | — |
| G7 | **Transition.** A row of `t` at zero whose `HEAD B` of this round shows another canonical identity `u`: `DELETE I(B,t)`, then `PUT I(B,u)` as in G2; the row becomes `pending(u)` or `candidate(u)` in this round's one commit. The `HEAD` is step (1) of G4 for a pending row; for a key touched in this round's window it is the supersede `HEAD` (`N:612-631`), for a row in any state. **Order:** a row whose `HEAD` shows `u` takes G7, never G4 (2) or G8, as the supersede check runs before `settleEntry` today (`N:632`). A plain intent delete first can spend the round that folded the key's last transition, and then `u` is never condemned | `matches` at `N:616` |
| G8 | **Remove the intent.** `standing(t)` read from the adopted state, at zero, no G7 in this round: `DELETE I(B,t)`; the row leaves the state after the answer. No `HEAD`. This is the only rule that removes the intent of a body that is gone | — |
| G9 | **Adopt orphan intents.** Each round that does not suppress destruction lists one page of `blob-intents/` from `intent_sweep_cursor`, as the orphan manifest sweep pages the manifest prefix (`CA/Gc/CasOrphanManifestSweep.cpp:700`, `G:3609-3614`, `:1302-1303`), and stores the next cursor in its commit. A listed key that parses, for a blob with no row in the adopted state (one row per key): `HEAD B`. Same canonical identity → `candidate(t)`; G3 confirms on the equal occupant. Absent, or another canonical identity → `standing(t)`. A token that does not canonicalise → no row. The listing only nominates: the state comes from the `HEAD`, and any removal still needs G7 or G8 | — |
| G10 | **Pending means confirmed.** A pending row's intent was confirmed: by G2, G3 or G7 of this build, or, for a row committed before the migration, by M5 for the row's token ([§7.3](#inherited)). This build treats both alike | — |
| G11 | **Never.** GC never deletes an intent on a spare, on an absent body, or in the job that deletes the body, and never reads or writes `.meta` | — |
| G12 | **Reads go to the store.** The `HEAD`s of G2, G7 and G9 are sent in this round, after its lease (`G:787`); a read-ahead `HEAD` of the same round qualifies. The `HEAD` of G4 is sent after the cut. No cache answers them | — |

**What confirms a create** (G2, G3, G7, M5). The gate consumes the result of its own create:

| Result | Confirmed |
|---|---|
| `Committed`: an answered success, or an unanswered attempt whose resolve read found equal bytes (`Q:1127-1128`) | yes |
| `Conflict` with an observed `Object` whose bytes equal the canonical record. A clean refusal returns `Conflict`, not `Committed` (`Q:1118-1119`); `create` resolves with a body read (`Q:1154-1158`) | yes |
| `Conflict` with `ProvenAbsent`, `NotObserved` or other bytes; `GaveUp`; any exception | no |
| A request that was sent, a job that finished, a drained queue | no. Today's marker worker counts a failed job as completed (`MW:91-99`) |

| Topic | Rule |
|---|---|
| Order inside one round | G2, G3, G6, G7 and G9 run in the fold or beside it: G2 and G7 in `closeBlob` (`N:590-648`), G3 in `settleEntry` (`N:559-583`). G4 and G8 run in the delete batch after the fold (`G:659-762`). A `HEAD` without a usable answer at G2, G7 or G9 fails the round before its commit. |
| After `Mismatch` | The body now at `B` was published by a writer that met the intent, after its edge. That edge is not in this cut, so a later window touches the key, and G7 condemns the body when the key next reaches zero. A body that lands after its writer's edge removal was folded is the late body of [§8.2](#residuals). |
| The intent delete (G7, G8) | Keys are minted by `Layout::writeOnceBlobIntentKey` and deleted by `removeChunkWriteOnceOrOneByOne` (`G:541-556`) over `CasOperation::removeManyWriteOnce` (`Q:634`): one batch request for up to `bulkDeleteChunkKeys` keys, or one `DELETE` per key on a store without batch delete. Policy `Retry::standard()`; a reissue resends the chunk; an absent key is success; an exhausted policy or a refusal throws (G5). A refusal that persists (no delete right on `blob-intents/`) fails every round at that row. |
| Budgets | The graduation budget bounds the creates of G2, G3 and G7; a row over it is recorded as `candidate`. The redelete budget bounds G4. G9 is bounded by its page size, a pool setting beside `manifest_sweep_list_budget_keys` (`CA/Pool/CasPool.h:93`). |

### 3.3 `cas-fsck` {#fsck-rules}

`cas-fsck` reports intents and deletes none. Orphan intents are G9's.

| # | Rule |
|---|---|
| F0 | **Admission.** The dialect check of the open ([§6.1](#identity)); it holds on a read-only open, which is how `cas-fsck` opens the pool (`programs/disks/CommandFsck.cpp:53-56`) |
| F1 | `cas-fsck` lists `blob-intents/`. A key that `parseBlobIntentKey` rejects is `intent_malformed` |
| F2 | For a listed intent: exact `GET` of the intent, then `HEAD B`. Bytes other than the canonical record → `intent_malformed`. Same canonical identity → `intent_current`. Another canonical identity → `intent_replaced`. Absent → `intent_body_gone`. A refused or unanswered read, or a `HEAD` token that does not canonicalise → `intent_unread` |
| F3 | Every `.meta` is `legacy_marker`: debris that no actor reads. `cas-fsck --fix legacy-markers` deletes each with `op.removeCurrent` (`Q:656`): a `HEAD`, then a delete conditional on what it saw |
| F4 | Intents are accounted objects in every class total. `body_without_meta` and `meta_without_body` (`CA/Tools/CasFsck.cpp:1206-1222`) are removed |
| F5 | **A fix run stays on the read-only open.** It sends its deletes through its own operation (`CA/Tools/CasFsck.cpp:447`): no mount, no lease, no probe. It deletes `.meta` keys under F3 and writes nothing. A refused or unanswered delete stops the run with an error and the count deleted so far; a rerun continues |

The rule "a read-only open never mutates the pool" (`CA/Pool/CasPool.cpp:445-446`) narrows to: it never writes, and
it deletes only `.meta` under an explicit `--fix`. `cas-fsck` and `SYSTEM CAS FSCK` list `blob-intents/` beside
`blobs/` (`CA/Tools/CasFsck.cpp:892-913`).

### 3.4 Recovery and rebuild {#recovery}

| Event | State left | What happens next |
|---|---|---|
| GC leader dies after the create of G2, before its commit | no row; the intent exists | G9 adopts it: `candidate(t)` over the live body, then G3 confirms on the equal occupant |
| Same, and the blob is referenced before the next round | an orphan intent over a live body | the next reuser by observation uploads once (`u`); G9 adopts `I(B,t)` as `standing(t)`; G8 removes it at the next zero |
| Dies after the body `DELETE`, before the commit | `pending(t)` | next round at zero: `HEAD` absent → `standing(t)` |
| Dies after the intent `DELETE` of G8, before the commit | `standing(t)` | next round at zero: the delete finds no key, success, row dropped |
| A create lands after its row left the state | an orphan intent | over a live body it costs one upload; G9 adopts it |
| `SYSTEM CAS GC REBUILD`, `cas-gc-rebuild` | no row from the fold (`G:4762-4765`); intents stand | the rebuild resets `intent_sweep_cursor`, as it resets `manifest_sweep_cursor` (`G:5028`). G9 then adopts every intent. A lost `candidate` with no intent is the rebuild leak of today (CAS-33.3) |

### 3.5 Failure contract {#failure-contract}

Request failures: with each rule. Generations: [§2.3](#gate). Refusal texts: [§7.5](#refusals).

| Field | Valid | Missing | Unknown or malformed |
|---|---|---|---|
| `_pool_meta.token_dialect` | `etag`, `generation`, `emulated`; present iff the floor is 4 | at floor 4: decode fails, `CORRUPTED_DATA` | unknown word: `UNKNOWN_FORMAT_VERSION`. Present below floor 4: `CORRUPTED_DATA` |
| A token that does not canonicalise, in GC | — | — | It names no intent and no row of its own, and never shows that `t` is gone. From the `HEAD` of G2 or G9: no row. From the `HEAD` of G4: the row is carried unchanged, with no request. From the supersede `HEAD`: no transition. As the own token of an inherited row: the row has no intent key; G4 never matches it, G7 condemns a canonical `u` with no intent delete, and G8 drops it with no request. Counted (`CASGCIntentTokenRejected`), one ERROR per round. GC never deletes such a body ([§8.2](#residuals)). Writer: W3. `cas-fsck`: F2. The tool: no intent ([§7.1](#tool), M5) |
| Intent object bytes | the canonical record | — | other bytes at an intent key. Writer: an intent (W4 → W5). GC and the tool: not a confirmation; GC carries the row (`CASGCIntentUnconfirmedCarry`, one ERROR per round), the tool fails. `cas-fsck`: `intent_malformed` |

## 4. Invariants and the safety argument {#safety}

### 4.1 Invariants {#invariants}

| # | Invariant | Provided by |
|---|---|---|
| I1 | **Non-recurrence.** Once `t` stops being the incarnation of `B`, it never is again | A1 |
| I2 | **Content.** Every body ever at `B` has the pool's envelope length and the payload that hashes to `B` | [§6.2](#source-check), A6 |
| I3 | **Edge before observe.** A writer's edge on `B` is durable before its first `HEAD B`; it commits only while that precommit is the live owner | W1; `W:965-970` |
| I4 | **Intent permanence.** A delete of `I(B,t)` is sent only (a) by G8, at a cut taken after the commit of `standing(t)` that shows in-degree 0, or (b) by G7, after a `HEAD B` of that round showed another canonical identity. Both follow an observation that `t` is gone from `B`. Nothing else deletes an intent | G6, G7, G8, G11; the writer, the tool and `cas-fsck` delete none; I1; A2 |
| I5 | **Delete evidence.** Every `DELETE B If-Match t` this build sends follows: a confirmation that `I(B,t)` exists; the commit of `pending(t)`; a complete cut with in-degree 0, taken after that commit; then a `HEAD B` that shows `t`. For a row committed before the migration the confirmation is M5's, and the cut is after the migration | G2, G3, G4, G10, M5; the pending branch of `settleEntry` runs only at in-degree 0 of this round's merge (`N:520-557`); `frontier_complete`, `G:3548-3551` |
| I6 | **Adoption.** A writer records an adoption only after W2 showed `t` and W4, sent after that answer, answered 404 | W2 to W4, W7 |
| I7 | **Inherited authority.** No writer of this build runs before the floor is raised. At that write, `I(B,t)` exists for every `t` that is current at its key and that a delete sent by an older build can still name, outside R1 of [§7.4](#late), and for the token of every inherited pending row. It protects reuse of `t`, not a ref that already names `t` (R3) | the order M5 before M8; [§7.4](#late) |
| I8 | **Goal.** Every ref this build commits by observation names a body protected by this protocol; a ref copied without observation is as safe as its source. Outside the residuals of [§8.2](#residuals), a committed ref never names a blob key with no body | [§4.2](#argument) |

### 4.2 The argument {#argument}

**Adoption.** Writer `W` of this build: edge at `Te`, W2 sees `t` at `T1`, W4 answers 404 at `T2`; `Te < T1 < T2`
(W7). Suppose a delete removes body `t` at `Td > T1`.

1. **This build sent it.** By I5 there is a confirmed create of `I(B,t)` at `Tc`, the commit of `pending(t)` after it,
   and a zero cut `C` after that commit, `Tc < C < Td`.
   - If `T2 < Tc`: `Te < T2 < Tc < C`, so `C` counts `W`'s edge and G6 spares the row: contradiction. If the edge was
     removed before `C`, `W` cannot commit (I3).
   - If `Tc < T2`: the intent existed at `Tc` and was absent at `T2`, so it was deleted in `(Tc, T2)`. By G7 that needs
     a `HEAD` showing another identity `u`: before `Td` that contradicts I1; after `Td`, `B` holds `u`, which landed
     after `Te`, and the publication argument below covers it. By G8 it needs a zero cut after the `standing(t)`
     commit, which follows the answered body delete, so after `Td > T1 > Te`. That cut counts `W`'s edge: contradiction.
2. **An older build sent it**, before the migration. By I7 the intent existed before `W` ran, and by I4 it stays while
   `t` is current. W4 could not answer 404: contradiction, outside R1.

**Publication.** `W` publishes `t'` under a fresh envelope after `Te`. `t'` is new (A1), so no earlier authority names
it, of this build or an older one. Any delete of `t'` needs a condemn `HEAD` after its appearance, then a committed
`pending(t')` and a later cut: the cut is after `Te` and counts the edge. A late delete of an older token misses: the
condition is exact. The same holds for `u` after G7: its `pending(u)` commit follows the `HEAD` that saw `u`, so the
delete cut follows the edge of every writer that observed `t` before `u` landed.

**No-bytes references.** `adoptEvidence` (`W:572-587`; callers `X:266`, `X:1269`, `CA/Parts/PartFolderAccess.cpp:457`)
adds an edge with no request. It needs a live source edge, which the cut counts. Against an older delete in flight the
copy is as safe as its source (R3 of [§7.4](#late)): the intent protects reuse, not a reference that already exists.

**Where evidence must be positive, where absence is accepted:**

| Decision | Evidence | Why |
|---|---|---|
| `pending(t)` | positive: the create answered, or an equal occupant read | an unanswered create may never land; then nothing warns the writer |
| Intent absent at W4 | absence, an answered 404 only | sound by I4: the intent outlives the body until a cut that sees every writer that observed it |
| Body absent at W2, G2, G4, G9, M5 | absence, an answered 404 only | each leads to an action that is safe if a body appears later: publish, no row, `standing`, no intent for the current body |
| Body absent, for an intent delete | never | only a cut can show that no writer is between its `HEAD` and its `GET` |
| A `LIST` | nominates intents (G9, F1), markers and mount slots ([§7.4](#late)) only (decision-2) | a missing key delays reclamation, leaks bytes or leaves debris; it never authorizes a delete |

### 4.3 Schedules each rule defeats {#schedules}

Writer `W`, GC actors `G1`, `G2`, body `t`. S1 to S7 are reduced traces of models with the rule removed.

| # | Schedule | Defeated by |
|---|---|---|
| S1 | **Intent removed in the body-delete job.** `W` adds its edge after `G1`'s cut and sees `t`. `G1` deletes `B`, then `I(B,t)`. `W` reads 404, adopts a missing body | G8, G11 |
| S2 | **Removal cut taken before the `standing` commit.** `G2` takes its cut (0), then `W` adds its edge and sees `t`. `G1` deletes `t` and commits `standing(t)`. `G2` reads `standing(t)` and deletes the intent on its earlier cut. `W` reads 404 | G8: lease read first, cut second, same round |
| S3 | **Removal at positive in-degree.** `W` sees `t`; `G1` deletes `t`, commits `standing(t)`; `G1`'s next cut holds `W`'s edge, and it deletes the intent anyway. Deleting because the body is absent at that point is the same schedule | G6, G8 |
| S4 | **Spare removes the intent.** `G1` commits `pending(t)`. `W` sees `t`. `G1` sends `DELETE B`. `G2` sees `W`'s edge, spares and deletes the intent. `W` reads 404; `G1`'s delete lands | G6 |
| S5 | **Delete on the condemning round's cut.** `G1`'s cut shows 0. `W` sees `t`, reads 404, adopts. `G1` creates the intent and deletes `t` in the same round | G4: a cut after the `pending` commit |
| S6 | **Unanswered create counted.** `G1` sends the create and commits `pending(t)`. `W` reads 404 before it lands, adopts. `G1`'s next cut, taken before `W`'s edge, deletes `t` | the confirmation table |
| S7 | **Adoption deletes at once.** `W` sees `t`. `G1` deletes `t`; the row is lost (rebuild, or a deposed commit). `G2` adopts `I(B,t)` and deletes it in the same round. `W` reads 404 | G9: adoption deletes nothing; G8 needs a later zero |
| S8 | **Deposed leader.** `G1` sends the create, gets no answer, loses the lease. `G2` confirms on its own, commits, waits for a later cut. `G1`'s late create adds the same warning. A late intent delete of `G1` was sent only after `t` was gone | confirmation table; I4 |
| S9 | **Ambiguous body delete.** The delete is unanswered and not applied; the round commits `standing(t)`; G8 removes the intent; `W` adopts; a reissued delete lands | G5 |
| S10 | **Two absent publishers.** `W` stages `X`, sees `B` absent. `W`'s source turns into `Y` of equal length. `P` publishes `X` and commits. `W`'s first publication lands `Y` over `P`'s data | [§6.2](#source-check): every streaming publication is hashed |
| S11 | **Spelling or dialect.** One incarnation renders as two identities, through other quoting or a client of another dialect. G7 reads "replaced" and deletes a live intent | [§6.1](#identity): one identity for every comparison, one dialect per pool |
| S12 | **A stale read.** G7 or G9 takes the identity at `B` from a cache, or W4 takes a 404 from a request sent before the writer's edge | W7, G12 |
| S13 | **Floor before intents.** The floor is raised while a body under older delete authority has no intent. A writer of this build adopts it; the older delete lands | [§7.1](#tool): M5 before M8 |
| S14 | **Inherited row with no intent.** The tool's create for an inherited pending row is sent and never applied. The floor is raised; a writer reads no intent and adopts; the first round at zero deletes | M5: a create that is not confirmed fails the run before M8 |

## 5. Assumptions and the contract with the store {#assumptions}

| # | Assumption | What provides it | What stays assumed |
|---|---|---|---|
| A1 | A body token never recurs at a key | One publication call site, `W:509` through `Q:780`. Streaming: a fresh 128-bit tag per attempt (`W:326`, `:497`), one send (`W:299`, `B:898-904`). Verbatim copy: the staging tag is minted once (`X:804`), and the copy happens at most once per staging object because hardlink copies share the flag (`CA/ContentAddressedTransaction.h:162-165`, `X:258`, `:306`; `CA/Pool/CasPartWriteTxn.h:26-38`). Generation dialect: the store's own counter | No transport reissue after a request may have been applied (redirect, re-sign, reconnect). The store applies one request at most once. Random tags do not collide. A generation is never reused. A removed delete marker on a versioned bucket is external manipulation. The `emulated` dialect over local files (tests, single-node development): its token is the file mtime in nanoseconds plus an in-process counter dropped 2 s later (`B:400`, `:457`, `:582`), so a delete and a re-create inside one mtime tick can repeat a token. The contract comment that calls these tokens a never-reused sequence (`CA/Backend/CasBackend.h:143-144`) holds for the in-memory backend only and is corrected with the implementation |
| A2 | Reads are linearizable per key: a `HEAD` or `GET` sent after a write was applied reflects it, for every client, whether or not the writer got an answer. Safety needs it at: the adoption reads W2 and W4; the `HEAD`s of G4, G7 and G9, whose answer decides `standing` or a transition; the tool's reads M2 to M5 and M7 | the store; W7 and G12 keep caches and earlier requests out | proxies and caches between a server and the store |
| A3 | A conditional delete removes only the named incarnation | the capability probe (`CA/Backend/CasProbe.cpp:47`); contract at `CA/Backend/CasBackend.h:128-147` | the probe samples; it does not certify non-recurrence (`:145-147`) |
| A4 | The size and the token of one `HEAD` describe one incarnation | the store | a proxy that mixes two responses |
| A5 | One token dialect and one spelling per pool, across clients | `token_dialect` in `_pool_meta`, checked at every open ([§6.1](#identity)); canonical identity | two clients that render one tag with different bytes beyond the quote pair |
| A6 | A staging object holds the bytes hashed at staging | the staging key is 32 random characters under the writer's epoch (`X:957`), written once, known to one transaction | corruption inside the store; hash collisions |
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
| Used for | Intent keys; row matching at G4 and G7; the classes of F2; the tool's M5. New `Etag::canonical`, and `sameIncarnation(row token, live token)`, which compares canonical identities. Rows keep the token as captured; both sides are canonicalised at compare time. It replaces `PersistedEtag::matches`, a textual compare (`CA/Backend/CasEtag.h:85-88`). |
| Grammar, two levels | `isIncarnationValue` (`CA/Backend/CasEtag.cpp:10-32`), which `CasRequests::tryMint` applies to every observation of every key (`Q:337-342`), is unchanged: it serves control objects too, and the tool's reads of an older pool. Canonicalisation applies only where this protocol needs an identity. It rejects, for `etag` and `emulated`: empty; a `W/` prefix; an unmatched or inner double quote; a control byte; leading or trailing whitespace; `*`; a comma; an opaque tag over 128 bytes. For `generation`: anything but canonical positive decimal of at most 20 digits. |
| A token that does not canonicalise is not an identity | Nobody builds a key from it, deletes on it or throws for it. The writer publishes fresh (W3), GC records no row of its own for it ([§3.5](#failure-contract)), `cas-fsck` reports `intent_unread` (F2), the tool creates no intent. The cost is in [§8.2](#residuals). |
| Probe | The capability probe (`CA/Backend/CasProbe.cpp:47`, called at `CA/Pool/CasPool.cpp:546`) also requires that the token of its own scratch object canonicalises. A failure refuses the writable mount, pool creation and the tool: on such a store no body is reused or reclaimed. `skip_access_check` skips it (`:526`). |
| Quotes | The generation dialect already strips one pair (`CA/Backend/CasObjectStorageBackend.h:175-182`); the `etag` dialect keeps them in the raw token because `If-Match` needs them. Intent keys are derived only from a body `HEAD` or a row token, never from a write response. G9 takes the identity from the listed key, parsed by `parseBlobIntentKey`, and compares it with a body `HEAD`. |

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
| Seam | `StreamingBlobPublication` gains the expected `BlobRef` (`CA/Backend/CasBackend.h:55-60`). The check sits beside the length check (`B:937-954`), before `out->finalize` (`B:955`): for S3 the completing request, `PutObject` or `CompleteMultipartUpload`, is issued inside `preFinalize` (`src/IO/WriteBufferFromS3.cpp:164`, `:192`, `:205`). Mismatch: `out->cancel`, `CORRUPTED_DATA`, nothing published. The emulated path checks before its rename (`B:564`). |
| Covers | Local staging, reopened by path for every attempt (`X:343-347`); S3 staging re-streamed (`X:314-338`); memory sources. So every overwrite of a body the writer saw, and every streamed first publication. |
| Verbatim staged copy | Not hashed (`B:959-977`). It rests on A6 and runs at most once per staging object; its selection rule is in [§3.1](#writer-rules). |
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
| M5 | **Convert.** For every blob with a marker that is not `Clean` or with a condemned row, and with `--intents-for-all-bodies` for every body M4 listed: `HEAD B`. A current token that canonicalises → `PUT I(B,t)` with `If-None-Match` for it. Absent, or a token that does not canonicalise → no intent for the current body: such a body is never adopted (W3). In addition, for every inherited pending row whose token canonicalises and whose body is absent or has another identity: `PUT I(B, row token)`. Every create is confirmed by the table of [§3.2](#gc-rules). So every inherited pending row has a confirmed intent for its own token, or the run fails | intents | a create is not confirmed |
| M6 | **Fence the older GC.** Replace `gc/state` with `If-Match e0`: header 4, every field as read. No `gc/state` at M3: create it with `If-None-Match`, header 4, lease owner 0, `gc_shards` from `_pool_meta` | `gc/state` | a conflict: the run restarts at M2 |
| M7 | **Check again.** Immediately before M8: read every slot (the `LIST` and the catalog reads of M2) and both GC pairs again. `gc/state` must still have the token M6 wrote (on a rerun that skipped M6, the token M3 read) and the lease pair M2 saw, `gc/hb` its pair, every slot the token M2 left | — | anything moved, or a slot that is not terminal appeared: an older member started during the run. Names it. The operator stops that server and runs the tool again |
| M8 | **Raise the floor.** Read-modify-write of `_pool_meta`: `min_reader_generation = 4` and `token_dialect`. **Completion mark** | `_pool_meta` | the body it reads has floor 4 with another dialect |
| M9 | **Remove markers.** Delete every `.meta` read at M4 with `If-Match` the token of that read (`deleteMetaExact`, `CA/Pool/CasBlobMeta.cpp:42`). One that changed since is left to F3. On a rerun of a migrated pool: the code of F3 | deletes | a delete is refused or unanswered: rerun |

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
  requests; one upload per old blob on its first reuse; up to one orphan intent per blob for G9. Use it on a pool that
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
  converge: the loser of M6 restarts and then skips M6. A run with another dialect that loses M8 is refused; its
  intents name another identity than any body `HEAD` of this pool, so G9 adopts them as `standing`.

### 7.3 Inherited state {#inherited}

| Object at migration | After the migration |
|---|---|
| Body with a `Clean` marker or none, no row | adopted by W2 to W4 |
| Candidate row | carried as `candidate(t)`. G3 creates, meets the tool's intent as the equal occupant when `t` is current, confirms |
| Pending row | carried, and treated exactly like a `pending(t)` of this build (G10): M5 confirmed `I(B, row token)`. The first round whose cut shows in-degree 0 runs G4 |
| Marker that is not `Clean`, no row (a spare, a rebuild) | an orphan intent. The next writer of that content uploads once; G9 adopts it |
| Every marker | debris, deleted at M9 |

Rows are kept because dropping them would leak the bodies: GC makes a row only on a fresh transition to in-degree 0
(`N:637-648`), which a body already at zero does not have again. A pending row needs no rewrite: `settleEntry` reaches
its pending branch only when the merge of this round gives in-degree 0 (`N:520-557`), so the cut is always one of this
build, and the confirmation is M5's (I5). Because M5 covers the row's own token, every body delete this build sends has
a confirmed intent, also when the token recurs (case (e)).

### 7.4 Requests that land after the members stopped {#late}

M2 proves that no member renews. It revokes nothing a member already sent, and nothing bounds when a sent request is
applied (`D/operations/debugging.md:306`).

| Late request of an older build | Lands as | Outcome |
|---|---|---|
| GC `DELETE B If-Match t` | removes `t` if it is still current | Where `I(B,t)` exists before the first writer of this build (I7), no such writer adopts `t` (W4), and the intent stays while `t` is current (I4). It does not protect a ref that already names `t`, nor a copy of that ref made with no request: R3. Coverage: below |
| Writer marker write: the `Clean` create or the `Condemned` to `Clean` swap (`W:352-404`) | a `.meta` object, or refused | nothing reads `.meta`: debris for F3. Before M4's read of that key it turns a marker `Clean`: case (b) |
| Writer body `PUT` | a new incarnation `u`, if A1 held in that build; else case (e) | no authority names `u`: that needs two committed older rounds after `u` appeared, and none commits after M6. An intent M5 made for the replaced token is an orphan for G9. With no edge left it is the late body of [§8.2](#residuals) |
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
  `t` itself or a pending row names it. The miss matters only while a delete of `t` sent by a stopped leader is still
  in flight: a writer of this build can then adopt `t` and the delete can remove it. The result is a committed ref
  with no body, which `cas-fsck` reports.
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
| A second `HEAD` before adoption (three reads) | not needed once the intent outlives the body until a cut after the `standing` commit; it costs one read per reuse |
| Write-ahead GC: a round that only records the row, the create in the next round | one round longer to the body delete, and the same requests |
| The intent delete in the job that deletes the body | a writer between its `HEAD` and its `GET` adopts a missing body (S1) |
| An intent delete because the body is absent (`cas-fsck`, or GC with no cut) | loses a blob: `G1` deletes `t` with its pending row still uncommitted, `W` sees `t` in between; `cas-fsck` reads the intent, then `B` absent, deletes the intent; `W` reads 404, adopts a missing body |
| An intent delete when the body is absent and `gc/state` has no row | loses a blob: `G1` holds a delete of `t`; `W` sees `t`; `G2` sees `W`'s edge, spares, drops the row; `G1`'s delete lands; `cas-fsck` finds the intent, no body, no row, deletes the intent; `W` reads 404, adopts. Only a cut sees a writer in flight |
| An intent delete at positive in-degree when the body has another identity | safe, but one `HEAD` per round per referenced key; G7 removes it at the next zero at no extra request |
| Removal of an intent once every writer that could read it has finished | needs per-writer progress records and their reading; too complex for the gain |
| An online in-place switch: a mark on bodies (`Content-Type`), a mount probe, an operator command | a change in shared S3 code, two GC modes, a permanent legacy branch in the writer, and an argument for every mixed state |
| A new pool prefix with a data copy | every body is downloaded and uploaded: the envelope carries the pool id (`CA/Formats/CasPoolMetaFormat.h:23-24`); double storage meanwhile |
| An inventory of bodies | its premise, "no edge in the run means unreferenced", fails on a rebuilt pool (CAS-33.3) |
| The writer deletes an intent after its own upload | safe only on an unequivocal answer; a misread outcome loses data; saves under 40 bytes |

### 8.2 Residuals accepted {#residuals}

| Residual | Effect | Handling |
|---|---|---|
| R1 to R4 of [§7.4](#late) | a committed ref with no body | R1, R3: none; `cas-fsck` reports the dangling ref. R2, R4: the operator precondition; M7 narrows R4 to its window |
| A body that lands after the fold processed its writer's edge removal has no edge, no row and no intent | bytes leak until the content is written again and dies | existing gap, CAS-33.2, unchanged. This design promises reclamation only of a body whose transition to zero GC observes while the body is present with a token that canonicalises |
| A body whose token does not canonicalise. Exposure: a store whose tokens leave the accepted grammar on a path the probe does not sample (multipart), a proxy that weakens tags, a later change of the store | never adopted: one upload per reuse. Never condemned: the bytes leak until a writer of that content publishes a body whose token canonicalises; a row that still names the old token is then moved by G7 | not reclaimed. `CASBlobReuseTokenRejected`, `CASGCIntentTokenRejected` and their ERROR lines show it; F2 reports `intent_unread` |
| A standing intent over a live body: a blob referenced again after one round at zero, a spare of a pending row, a late create, the tool's intents over spared tokens, and with `--intents-for-all-bodies` up to one per body | one upload by the next reuser by observation; under 40 bytes until G9's cursor reaches it and G8 removes it at the next zero | by design |
| Markers written late by an older build, or left by a crashed M9 | 90 bytes each | F3 |
| Token form per store is taken from the client library | A5 | the probe at mount; an integration test records it per store ([§10.2](#tests)) |

## 9. Effects {#effects}

### 9.1 Decisions {#backlog}

| Decision | How this design answers it |
|---|---|
| decision-1, invariant 5 of `docs/superpowers/cas/AGENTS.md` | the `HEAD` before every `PUT` stays, and a body is never created conditionally (W5). A durable write step is removed and a read is added, as a protocol change with a model, not as an optimization. Record it there |
| decision-2 | a `LIST` nominates intents, markers and slots; no listing authorizes a delete ([§4.2](#argument)) |
| decision-4 | generation 4. The compatibility path is the offline tool, not a reader that accepts both formats: this build reads no pool below 4. Record the amendment; no downgrade |
| decision-7 | the body delete stays conditional. The intent delete has no precondition and is typed as a write-once key. Marker deletes are conditional: `If-Match` in M9, `removeCurrent` in F3 |

### 9.2 Backlog, tests, docs {#docs}

- **Backlog** (the plan carries the per-task tables). Moot: every task that exists for the marker or its residuals,
  in the writer and in GC (CAS-384, CAS-330, CAS-334, CAS-345, CAS-29.3, CAS-439, CAS-326). Re-targeted from marker
  requests to intent requests: CAS-29.4, CAS-336, CAS-284.5. fsck and byte classes (CAS-10, CAS-11, CAS-289.6):
  intents replace `.meta`, the pairing counters go. Unchanged: CAS-33.2, CAS-33.3. New: the tool, the generation gate,
  the intent sweep.
- **Tests.** Every marker-era gtest is deleted or re-targeted to intents (`T/gtest_cas_gc_meta_writer.cpp`,
  `T/gtest_cas_blob_meta.cpp`, the marker cases of the GC round, read-ahead, part-write and fsck suites). Only the
  tool's fixtures are generation 3; `T/gtest_cas_blob_meta_format.cpp` stays for the codec the tool reads.
- **Docs.** `D/operations/migration.md`: "Migrating a pool to generation 4" (stop, run the tool, start; refusals; the
  points of no return; downtime; `--intents-for-all-bodies`; R1 to R4). `D/configuration.md`: no mixed versions;
  `gc_meta_pool_size` (`CA/ContentAddressedSettings.cpp:86`) is ignored; the intent sweep page size.
  `D/architecture/blob-protocol.md`, `garbage-collection.md`: rewritten. `D/bucket-requirements.md`: `LIST` and delete
  rights under `blob-intents/`, batch delete, one token dialect per pool. `D/operations/troubleshooting.md`.
- **Counters.** Removed: the six `CASMeta*` counters (three at `src/Common/ProfileEvents.cpp:968-970`),
  `CASGCMetaOps`, `CASGCMetaWriteAnomaly`, `CASGCCondemnMarkerUnconfirmedCarry`. New: four `CASBlobIntent*` request
  counters; `CASBlobReuseIntentPresent`, `CASBlobReuseTokenRejected`, `CASBlobPublishDigestMismatch`;
  `CASGCIntentCreated`, `CASGCIntentConfirmedExisting`, `CASGCIntentUnconfirmedCarry`, `CASGCIntentDeleted`,
  `CASGCIntentLeftOnSpare`, `CASGCIntentAdopted`, `CASGCIntentTokenRejected`. `CASBlobPut`, `CASBlobGet`,
  `CASBlobHead` no longer include marker traffic.

## 10. Verification {#verification}

### 10.1 TLA+ {#tla}

| Evidence | Shape | Status |
|---|---|---|
| `M/CaBlobIntentProtocol.tla`, `M/CaBlobIntentProtocol_RESULTS.md` (commit `10f9f0f0021`) | an earlier shape: three reads, write-ahead GC, the intent deleted in the body-delete job | committed. Its row `sab_candidate_from_intent` is green, with the witness `wit_derived_row_deletes`: a row derived from a current intent is safe, which G9 relies on |
| `M/CaBlobIntentMigration.tla`, `M/CaBlobIntentMigration_RESULTS.md` (commit `b55dde0bd7d`) | the migration of [§7](#migration) before the M5 rule for a pending row's own token | committed, 70 configurations: the green set, crash after each step, two runs, each late kind, liveness to `migration complete`; every residual R1 to R4 red when switched on; six of seven tool sabotages red, the seventh (markers removed before the floor) red only on the invariant that restates it |
| A scratch study of two-read reuse | the intent deleted at a later zero cut, not in the body-delete job | 89 configurations, all completed, both dialects. Green with two GC actors, late landings, lost commits, rebuild, spare and re-condemnation, three writers. Red as required: S1 to S3; the intent delete on an absent body, with and without the "no row" condition |
| A scratch study of immediate condemnation | [§3.2](#gc-rules), with the two-read writer | 66 configurations, 64 completed; one generation run is red on a seed artifact and was not re-run. Green, both dialects where run: the base, each late kind (the late create finished only in a variant with one more `HEAD`), re-created key, orphan adoption, rebuild. The six sabotages S1, S3 to S7 red on both dialects. Liveness: the re-created key is condemned with G7 before G8, and never without. Liveness of orphan adoption inconclusive: every counterexample is the model round leaving its work phase by the empty exit before adoption, a fairness artifact, not a protocol trace |

**Required before implementation.** One committed model of this shape, replacing the scratch studies:
- the green set and the six sabotages of the immediate study, on both dialects;
- the liveness runs, adoption re-run with the empty exit disabled while an orphan waits, or with strong fairness;
- the runs not made: three writers; every late kind at once; `cas-fsck` running; the generation dialect for the base,
  late create and late body delete; a lost commit and a late create under the final rules; a stale G9 listing;
- G7 and G8 as written: the studies send a `HEAD` in every `standing` round; here only a touched key gets one;
- the migration model re-run with the M5 rule for a pending row's own token, with case (e) on.

Not modelled, to be covered by tests: token spelling beyond two renderings, a token that does not canonicalise,
multipart transport, the slot observation of M2 and M7, sharded runs, budgets and read-ahead.

### 10.2 Tests {#tests}

Tests are written first. A `LOGICAL_ERROR` expectation is a death test in a `*DeathTest` suite.

| Layer | Tests | Pins |
|---|---|---|
| gtest, writer, on the etag-faithful fakes (`EtagFaithfulPublicationBackend`, `ContentEtagBackend`) | `CASBlobIntentWriter.FirstCreationIsHeadAndOnePut`, `.CleanReuseIsHeadAndGet`, `.IntentPresentPublishesFreshOnce`, `.NoSecondHeadOnAnyPath`, `.LandedAmbiguousPublicationIsAdoptedInThreeReads`, `.FailedIntentReadIsNotAbsence`, `.WriterNeverMutatesIntents`, `.NoMetaRequestOnAnyPath`, `.VerbatimCopyIsSelectedOnlyAfterAnAbsentFirstHead`, `.UncanonicalTokenPublishesFreshAndNeverThrows`, `.AdoptionReadsReachTheStoreAfterAPrimedAbsence` (read `I(B,t)` absent through the same pool, let GC confirm and commit, then write: the backend sees this attempt's `HEAD` and `GET`, and the writer publishes) | W1 to W7; S12; ledger |
| gtest, GC states | `CASGcIntents.CondemnCreatesInTheSameRound`, `.ConfirmedCreateCommitsPending`, `.EqualOccupantConfirms`, `.UnansweredCreateCommitsCandidate`, `.CandidateRetriesTheCreate`, `.PendingDeletesTheBodyOnlyOnALaterCut`, `.BodyDeleteCommitsStandingAndKeepsTheIntent`, `.AbsentBodyAtTheDeleteCommitsStanding`, `.StandingRemovesTheIntentOnALaterZeroCut`, `.StandingKeepsTheIntentAtPositiveInDegree`, `.SpareDropsPendingAndKeepsTheIntent`, `.UnansweredBodyDeleteFailsTheRoundAndKeepsPending`, `.FailedIntentDeleteFailsTheRound`, `.IntentDeletesShareABatchRequest`, `.FiveRequestsPerReclaimedBlob`, `.NoMetaRequestInARound`; re-created key: `CASGcIntents.RecreatedKeyMovesStandingToPendingInOneCommit` (`HEAD`, intent `DELETE`, intent `PUT`, one commit), `.TransitionRunsBeforeThePlainIntentDelete` (a key whose last transition is folded in the `standing` round still gets `u` condemned), `.PendingWithAnotherTokenAtTheDeleteHeadTransitions` | G2 to G8, G11; S3 to S6, S9 |
| gtest, orphan intents | `CASGcIntents.OrphanOverTheCurrentBodyBecomesACandidate`, `.OrphanOverAnAbsentBodyBecomesStanding`, `.OrphanOverAnotherTokenBecomesStanding`, `.AdoptionDeletesNothingInItsRound`, `.SweepSkipsAKeyWithARow`, `.SweepCursorAdvancesOnePagePerRound`, `.RebuildResetsTheIntentCursor`, `.NoIntentDeleteOnAnAbsentBodyWithoutACut`, `.UncanonicalTokenWritesNoRow`, `.UncanonicalRowTokenIsMovedByACanonicalBody` | G9, G11; S1, S7 |
| gtest, generation gate | `CASBlobIntentGeneration.WritableMountOfAGeneration3PoolIsRefusedAndNamesTheTool`, `.ReadOnlyOpenIsRefused`, `.DecommissionOpenIsRefused`, `.NewPoolIsCreatedAtGeneration4WithItsDialect`, `.MountNeverRaisesTheFloor`, `.GcStateBelowHeader4FailsTheRoundWithNoRequest`, `.ReadCeiling3RefusesPoolMetaAndGcState`; `CASBlobIntentDialect.WritableMountIsRefused`, `.ReadOnlyOpenIsRefused` (so `cas-fsck`, `cas-gc-dryrun` and `cas-gc-rebuild` never run), `.ToolOnAMigratedPoolOfAnotherDialectIsRefused`. Each asserts zero delete and zero intent requests | [§2.3](#gate), G1; the dialect rule; S11 |
| gtest, identity and formats | canonical identity and each rejected form, with today's grammar unchanged for control keys; a generation over 20 digits; intent key round trip, rejected spellings, key length; `BlobIntent` in the format battery; the `gc/state` header 4 and `intent_sweep_cursor`; the run bit `standing`; `token_dialect` survives `admitOrValidate` (`CA/Pool/CasPoolMeta.cpp:98-123`) and every other read-modify-write; an unknown word and each invalid pair fail the decode; the probe refuses a token that does not canonicalise | [§2](#formats), [§3.5](#failure-contract), [§6.1](#identity); S11 |
| gtest, source check | `CASBlobPublishDigest.FlippedByteIsRefusedBeforeCompletion`, `.StaleSourceOfASecondAbsentPublisherIsRefused`, `.S3StagedRestreamIsVerified` | [§6.2](#source-check); S10 |
| gtest, fsck | classes; `FailedReadIsUnread`; `UncanonicalHeadTokenIsUnread`; `NoIntentIsEverDeleted`; `RefusedDeleteStopsTheRun`; `LegacyMarkersAreDebrisAndTheFixDeletesThem`. The failure cases assert that no key outside `.meta` was deleted | F0 to F5 |
| gtest, the tool, on generation-3 fixtures | `CASMigratePool.RefusesASlotThatRenews`, `.FencesASilentSlotAfterTheThreshold`, `.RefusesAMovingGcLease`, `.RefusesAPoolBelowGeneration3`, `.CondemnedMarkerGetsAnIntentForTheCurrentToken`, `.UndecodableMarkerCountsAsCondemned`, `.EveryCondemnedRowGetsAnIntent` (candidate and pending), `.PendingRowWithAnAbsentOrReplacedBodyGetsAnIntentForItsOwnToken`, `.AbsentBodyAndUncanonicalTokenGetNoIntentForTheCurrentBody`, `.UnconfirmedCreateFailsTheRun`, `.RowsAreCarriedAsTheyAre`, `.GcStateIsRewrittenAtHeader4WithItsFieldsAsRead`, `.LateOldGcCommitRestartsTheRun`, `.RecheckRefusesASlotThatAppearedAfterTheGcStateRewrite`, `.RecheckRefusesAMovedGcPair`, `.FloorIsTheLastProtocolWrite`, `.NoMarkerIsDeletedBeforeTheFloor`, `.CrashAfterEachStepThenRerunCompletes` (one case per step), `.RerunOfAMigratedPoolOnlyRemovesMarkers`, `.TwoRunsConverge`, `.DryRunWritesNothingAndSendsNoProbe`, `.AllBodiesOptionGivesEveryListedBodyAnIntent`, `.OnlyThisCommandReachesOpenForMigration`, `.InheritedCandidateConfirmsOnTheToolsIntent`, `.InheritedPendingRowIsDeletedByTheFirstRoundAtZero`, `.CleanMarkedBodyIsAdoptedWithNoUpload`, `.LateOldDeleteMissesAfterARepublish` | [§7](#migration); S13, S14 |
| Stateless (`add-test`) | requests of one `INSERT` of new content (1 `HEAD`, 1 `PUT` per blob, no intent request), of a duplicate (1 `HEAD`, 1 intent `GET`, no `PUT`), and of a duplicate after `DROP` and two GC rounds | ledger |
| Integration, RustFS and the GCS emulator | an upgrade test like `test_cas_ref_move_compat`: an older image writes, drops and runs GC until candidate and pending rows exist; one older server left up makes the tool refuse and name it; all stopped, the tool completes; the new image starts and the older image is refused; data reads back; a duplicate insert reuses with two reads and no upload; `cas-fsck` reports no dangling ref and no marker. The recorded token form on `HEAD` per store, multipart upload and multipart copy above 32 MiB included. `test_cas_gc_s3`: at most 5 requests per reclaimed blob | [§7](#migration), [§6.1](#identity) |
| Soak, 2 hours, with process kills | writers and GC run; fsck: zero dangling refs; at the end, with writers stopped and GC running until its sweep has passed every intent and two more rounds, a report with no `intent_body_gone`; no `CASBlobPublishDigestMismatch` | I8; G8, G9 |

### 10.3 Measurements {#measurements}

- Before and after: the request ledger per `INSERT` of new and of duplicate content; the yardstick cards; a wide
  insert like the stand's (marker `PUT`s 54,997 → 0, upload thread time, wall time). The request saving is not a
  wall-time claim until measured. GC requests per reclaimed blob. CPU of the publication hash on a large body. Standing
  intents over live bodies, and the uploads they cause, when equal parts are dropped and re-inserted within one round.
- The tool on a pool like the stand's: requests per step, wall time; then a duplicated insert: two reads, no upload.

## 11. Open questions {#open}

| # | Question | Default if nobody answers |
|---|---|---|
| Q1 | Is `cas-migrate-pool` the command name? | That name |
| Q2 | Should the verbatim staged copy carry a source-token precondition (`x-amz-copy-source-if-match`) instead of resting on A6? | No precondition. The copy already reads the staging object with a `HEAD` (`S3:1115`), but `copyS3File` has no source-condition parameter: it would be a change in shared code |
| Q3 | Should the tool accept a pool below generation 3? | No: it is mounted once by a generation-3 build first, so the tool has one input format |
| Q4 | Should a condemned row get a new field for "intent confirmed"? | No field: `pending` means confirmed (G10); `candidate` means not |
| Q5 | Should `--intents-for-all-bodies` be the default? | No: [§7.1](#tool) |
| Q6 | Should the store stop a suspended older member (R2, R4)? Two partial guards: the tool writes a critical key into every root's epoch counter, so an older mount sequence fails its allocation; slots declare a generation and GC of this build deletes nothing while a live slot of an older build exists | Neither: the operator precondition stands. Each adds a format change and narrows R2 without closing it (a brand-new root has no counter; a slot the listing omits). M7 needs no format change |
| Q7 | Should M2 fence a silent slot, or only refuse and leave it to the operator? | Fence: it uses the rule GC already has, and it makes a suspended mounted writer fail at its next renewal |
| Q8 | Should markers be removed before the floor is raised? | After: before M8 the tool then deletes nothing, a rerun still finds the markers, and a run that stopped before M6 leaves a pool an older build can use as before |
| Q9 | Is one dialect check per open enough for GC, or should each round read `_pool_meta` again? | Per open: neither value changes during an open, and a round saves one `GET` |
| Q10 | How does `clickhouse-disks` reach a pool that the disk's own `startup` refuses (`CA/ContentAddressedMetadataStorage.cpp:842-883`)? | The disks client starts the disk with the pool open deferred for this command only; no server path reaches `Pool::openForMigration` |
| Q11 | Is 120 s the right default grace? | Yes, as a heuristic at default settings ([§7.1](#tool)) |
| Q12 | Should G9 run in the GC round, or should `cas-fsck` hand orphan keys to the server through `SYSTEM CAS FSCK`? | In the round: the manifest sweep already pages a prefix per round with a cursor in `gc/state`. If that proves too costly, `cas-fsck` hands the keys to the server, which decides by G9; never a direct delete. The page is a key count, as the manifest sweep's: the round has no time budget at this commit |
