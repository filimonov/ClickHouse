---
description: 'Four request cuts in the CAS GC round and one measurement: carry the confirmed condemn marker, decode each manifest body once per round, free the pinned HEAD read-ahead window, drain dead-life stream debris in batch deletes, and attribute the LIST requests per logical page.'
sidebar_label: 'CAS GC request cuts'
sidebar_position: 101
slug: /superpowers/specs/cas-gc-request-cuts-design
title: 'CAS GC: four request cuts and one LIST measurement'
doc_type: 'reference'
---

# CAS GC: four request cuts and one LIST measurement — design (rev.2) {#cas-gc-request-cuts}

- Date: 2026-09-29. Status: rev.2, for re-review, then plan. Implementation base: `altinity/antalya-26.6`.
- Parent: `2026-09-25-cas-gc-rounds-in-minutes-design.md`. This spec takes A0, A2, A4, the batch-delete half of B4, and the measurement half of CAS-29.13. B1–B3, the janitor settings, stage C and the bounded-listing fix stay in the parent.
- Backlog: CAS-29.3 (A0), CAS-29.5 (A2), CAS-29.12 (A4), CAS-29.9 (B4), CAS-29.13 (measurement only).
- Measurements cited: `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md` F3, F4, F6, F24.
- Revision history: rev.1 → rev.2 closes codex round 1 (11 findings, all closed).
  The CAS-29.13 listing fix moved to the parent spec; this spec keeps its measurement.

## 1. Rule {#rule}

Each change moves the moment of a read, memoizes an immutable body, or sends fewer requests for the same deletes.

- **No decision moves except A0's**, and A0's changed decision is proven safe in section 2. On the same input, `objects_deleted`, `objects_spared`, `objects_replaced`, the outcome logs, the cursors and the carried rows match the current code, except the `marker_confirmed` bit A0 persists.
- **One counting change**: B4's `janitor_deleted` counts keys a successful batch left absent (section 5).
- **No new setting.** A4's bound is a fixed internal constant.
- **No change** to the on-S3 format (frozen since 26.6.4), the run row layout (A0 uses flag bit 1, value 2), LIST trust, deletion licences, exact-token deletes of mutable objects, HEAD-before-PUT, or phase order.
- **Acceptance per item**: a failing-first gtest with a counting backend asserts the request count and outcome equality against a no-change oracle. The ca-soak S11 delete phase records `cas_gc_log` phase durations and ProfileEvents before and after.

## 2. A0: carry the confirmed condemn marker {#a0}

**Site.** `settleEntry` in `CasBlobInDegree.cpp`. When the graduation gate confirms the marker but `graduationAvailable()` is false, the row is carried with `marker_confirmed = false`. After a restart or leader change every carried row pays the synchronous `.meta` GET again (F3: 200k GETs, four hours, round 1379).

**Change.** In that branch carry a copy with `marker_confirmed = true`. The supersede path keeps minting its fresh row with the flag false, so a flag never outlives its token.

**Decision change.** Today a fresh leader re-reads the marker at graduation and carries the row when the marker is absent, `Clean` or unreadable. With A0 the row graduates on the persisted confirmation.

**Why it is safe.** Graduation authorizes one thing: a later exact-token delete of token T. It is unsafe only if a writer holds a live reference to T when that delete lands.
1. **Adopters after the confirmation.** A same-token adopter must first observe a non-`Condemned` marker. Once `Condemned` is durable, only two transitions leave it (`CasGcMetaWriter.cpp`, header of `writeCondemnedMeta`). A writer sets `Clean` only after displacing the body with a fresh incarnation. GC removes the meta only after T is confirmed deleted or absent. Each upload writes a fresh envelope header (`CasBlobEnvelopeFormat.h`), so the new body's token differs from T. The delete then observes a different token or nothing, and ends `Replaced` or absent.
2. **Adopters before the confirmation.** Their edge landed before the marker turned `Condemned`. Graduation in a later round and the delete one round after that both fold from cuts that postdate the confirmation, so they see the edge and spare the row. This is the argument the gate already uses today.
3. **Failed marker GET.** An unreadable meta at graduation cannot undo a past durable `Condemned`. Points 1 and 2 hold without reading it.
4. **Delayed marker write.** A scheduled retry that stamps `Condemned` over a writer's fresh incarnation creates no flag for T. The flag comes only from a confirmation of T, and the delete stays exact-token.
5. **Leader change, executor handoff.** The flag reaches the run only through the round's single `gc/state` commit. A deposed leader's uncommitted run is never read. A confirmation read by any leader was a true observation when it was made.

This is the certificate `delete_pending` rows already carry: they persist `marker_confirmed = true`, and the delete fires in a later round without a re-check.

**Tests, failing first.**
- **Carry.** N condemned rows, graduation budget 1, counting gate callback. Round 1 makes N calls, graduates 1 and carries N−1 flagged rows. A fresh `Gc` graduates the next row with 0 calls. Today it makes N−1.
- **Republish and clear.** Confirm T, exhaust the budget, start a fresh `Gc`. A writer republishes the hash (token T′) and clears the marker to `Clean`, then the row graduates. The next round's delete ends `Replaced` and T′ is still present.
- **Unreadable meta and delayed write.** A meta GET that throws at graduation: the flagged row graduates and a present T is deleted. A delayed marker retry lands over T′: T′ survives.
- **Supersede.** A flagged row superseded by T′ yields a row with `marker_confirmed = false`.

## 3. A4: decode each manifest body once per round {#a4}

**Site.** `Gc::foldManifestEdges` calls `reads.takeRead`, `decodePartManifest`, `refMatchesBody`, `manifestNamespaceMatches` and the algo-admission check, then emits deltas, `RootAdd`/`RootRemove` events and, for `sign < 0`, `mf_cleanup[id] = got->etag`. `CASRefManifestBodyFoldGets == CASRefEmittedEdges` (F4). A part published and dropped in one round is fetched once per edge.

**Cache value.** A memo `ManifestId → ManifestFold`, local to the fold next to the round's `GcReadAhead`, and gone when the fold ends. `ManifestFold` holds the body `Etag` and one `{BlobRef ref; UInt128 source_id; String path}` per `Blob`-placement entry, in body order. It holds no inline bytes and no sign or ordinal. A hit rebuilds `BlobDelta`s with the current call's `sign` and `txn_ordinal` into the caller's per-log staging buffers. It emits the same events and, for `sign < 0`, sets `mf_cleanup[id]` from the cached `Etag`. A clamp still discards the staged buffers. The rebuild path passes no memo.

**Why a hit equals a fresh read.** The manifest key is write-once: staging rejects different bytes at the same key and never re-owns a dropped identity (`CasPartWriteTxn.cpp`). Nothing deletes a folded body before the fold ends: writers delete only never-precommitted staged bodies, which no edge names, and GC deletes bodies after the fold (`mf_cleanup`, orphan sweep phase). Algo admission is monotone (`CasPool.h`). A stale cached `Etag` would only make the exact-token delete mismatch and keep the body. Absence is never memoized, and a body that fails validation throws as today.

**Bound.** A fixed 64 MiB per fold. Each insert is charged 256 B per manifest, plus the `Etag` bytes, plus 64 B (`kBlobDeltaSize`) and the path length per entry. 64 MiB is 1/8 of what `rebuild_edge_budget` (8,000,000 deltas × 64 B = 512 MB) already lets a rebuild hold, and it fits 262,144 zero-entry manifests, above the 190k `delete_tmp` repoints per day (F2). A manifest whose own charge exceeds the budget is folded without being memoized. Past the budget the oldest insert is evicted first. `rebuild_edge_budget` is not reused: it counts routed deltas, not bytes.

**Read-ahead.** The log hint loop skips memo hits. When an eviction lands between that skip and the take, the take reads inline: one `CASGCReadAheadMiss` and one GET, with the same outcome.

**Counters.** `CASRefManifestBodyFoldGets` counts real GETs. A new `CASRefManifestBodyMemoHits` counts hits. Their sum equals today's `CASRefManifestBodyFoldGets` on the same input. The saving is edges minus distinct `ManifestId`s per round, so the stand run records both before any saving is claimed (audit section 6, F4).

**Tests, failing first.** Each compares deltas, events, `mf_cleanup` tokens and outcomes against the no-memo oracle.
- `publish` then `drop` in one round: 1 GET instead of 2, and the `drop` hit reproduces the `-1` deltas and the cleanup token.
- One manifest across two transactions, repoint, republication, adoption, ATTACH, and a later edge that clamps the log.
- Bound: empty manifests are charged 256 B each and evict past the bound; a large inline payload is charged no inline bytes; an oversized manifest is never memoized.
- An absent body is re-probed on the next edge; an eviction between hint skip and take produces one inline read.

## 4. A2: free the pinned HEAD read-ahead window {#a2}

**Site.** `topUpHeadHints` and `head_blob` in `Gc::fold`. `head_candidates[shard]` is a superset in `BlobRef` order (algo number, then digest). The merge takes candidates in that same order. `topUpHeadHints` throttles on `reads.pending()`, which also counts untaken read slots. When the window fills with hints the merge has already passed, every later take is a miss until the window drains.

**Change, in the caller only.** `GcReadAhead` is unchanged, and the order is the reducer's typed `BlobRef` order, never key strings.
- **Per-shard state.** `next_hint` is the first unsubmitted candidate position. `outstanding` is the queue of positions hinted but not taken.
- **Take `ref`.** Let `p` be the lower bound of `ref` among the candidates, from the last take onward. `discardHead` every outstanding position below `p`. The take hits if `p` is outstanding and equals `ref`, and reads inline otherwise. Then set `next_hint = max(next_hint, p + 1)`, which skips unsubmitted candidates the merge has passed.
- **Top up.** At shard start and after each take, hint from `next_hint` while `outstanding.size() < reads.window()`. Untaken intake read slots, from a clamp or an A4 eviction, never pin the head window.
- **Shard transition.** `discardHead` every outstanding position of the finished shard before switching.
- **Order check.** A take below the last take is a `chassert`. In release builds that take is an inline HEAD, the no-read-ahead behaviour.

A discarded candidate is below every future take, so no sequential walk would have issued its HEAD. `discard`'s wait and exception drop are therefore correct.

**Expectation.** The outstanding set is always the contiguous range after the last take position `p`. A take at `q` hits exactly when `q ≤ p + window`, and the first take hits when `q < window`. Each test states Miss and Wasted as numbers computed by a ten-line reference model of this rule. There is no blanket "near zero" target. F24 shows the effect is small on otel.demo (`Miss` 65k against `Hit` 24.2M), so the stand run records it without a target.

**Tests, failing first.** Outcomes equal the concurrency-1 oracle; Miss and Wasted equal the model. Cases: gaps of one, several and more than three windows; mixed algorithms in one shard (`XXH3_128` sorts before `Sha256` by `BlobRef`, while their key strings sort the other way); `BlobRef` ranges that descend across a shard transition; a take absent from the candidates; leftover untaken read slots from a clamped log.

## 5. B4: dead-life stream debris in batch deletes {#b4}

**Site.** `NamespaceJanitor::runOnePage` in `CasNamespaceJanitor.cpp` sends one exact-token DELETE per dead-life key, preceded by a HEAD only when the listing carried no token.

**Eligibility.** Only keys that `parseRefObjectKey` parses as `RefObjectKind::Log` or `Snap` are batched, and only after passing today's checks: life not resolved by the catalog cut, cut not ambiguous, deletes not suppressed. The key is minted as a `WriteOnceKey` from the parsed identity. The minted key must equal the listed key, and a key that differs stays on the per-key path. `_ckpt`, `_files` and every other state object keep today's exact-token per-key handling. That includes the replaced-after-HEAD case in `gtest_cas_namespace_janitor.cpp` (`TokenlessListHeadsDeadKeysAndRetainsConcurrentReplacement`), which stays as a regression.

**Why a stream key needs no token.** A stream key names one physical life and holds the one body ever written there: the `WriteOnceKey` contract the ref cleanup's bulk delete already relies on. A dropped life never comes back: a same-name recreate, ATTACH or recovery mints a fresh incarnation under a different prefix (`CasRefCatalog.cpp`, `CasRefCatalogFormat.h`). So nothing live writes under the dead physical life's stream keys.

**Flow.**
- Eligible keys collect in listing order into chunks of `clamp(gc_bulk_delete_chunk_keys, 1, kBulkDeleteMaxKeys)`. Each chunk goes out through `op.removeManyWriteOnce` directly. `removeChunkWriteOnceOrOneByOne` is not used, because it reports only request counts.
- `op.admitted()` is checked before each chunk, as the per-key loop checks it before each delete. If it fails, set `page_decided = false` and stop.
- **Success.** Every key of the chunk is absent, and `deleted += chunk.size()`. This is the one counting change: a batch reply does not tell removed from already absent.
- **Any exception** (partial failure, exhausted retries, NOT_IMPLEMENTED): that chunk's keys run through today's per-key path, HEAD only without a listed token and then exact-token remove. So `deleted`, `leaked` and anomalies are exactly today's for that chunk. Keys the failed batch did delete come back absent and are not counted. After NOT_IMPLEMENTED, the rest of the page also takes the per-key path.
- The per-key fallback keeps its admission check before every delete. The final `op.admitted()` check before cursor publication stays, and the cursor rule is unchanged.

**Requests per page.** With S stream keys and M state keys, of which H lack a listed token: today S + M DELETEs plus H HEADs. After: ⌈S/chunk⌉ bulk requests, plus M DELETEs, plus HEADs for the tokenless state keys.

**Tests, failing first.**
- A dead life with S stream keys and M state keys meets the formula above, with a token-listing and a tokenless backend.
- A same-name recreation committed between LIST and delete keeps the new incarnation's keys.
- Half a chunk deleted then a throw, exhausted retries, NOT_IMPLEMENTED: `deleted` and `leaked` equal today's per-key counts.
- Admission loss before a chunk, inside the fallback, and after the last chunk: no cursor is published.

## 6. CAS-29.13: measure and attribute the LIST requests per page {#list-page}

**This spec only measures.** The bounded, non-prefetching listing and the caller-termination contract are a verification item in the parent spec (section 11). Any fix there must meet these three constraints:
- **"Truncates walks on stores that ignore `start_after`."** A resumed page bounded to `limit` returns the first keys again, the `key <= cursor` filter drops them all, and the walk ends early (`listUnder`'s comment; the default iterator in `IObjectStorage.cpp`). Test through `ObjectStorageBackend`.
- **"Does not stop S3 prefetch."** `max_keys` sets the request's `MaxKeys`, not the enumeration limit, and the async iterator schedules the next batch on load (`ObjectStorageIteratorAsync.cpp`). Needs a transport-level request-count test.
- **"Changes persisted cursors and completion decisions."** A trailing empty page moves the janitor cursor, the orphan sweep's `wrapped`, and `deletePrefixWholesale` at exactly its budget. A full-page convention costs ⌊N/1000⌋ + 1 requests for N keys.

**Measurement.** An integration test with RustFS (`with_rustfs`) seeds 20k keys under one ref prefix. It runs a round whose only large LIST is the global walk. It records `DiskS3ListObjects / CASRefGlobalListPages`; otel.demo's 14.4k / 1,543 is 9.3, not 3. A trace line per `listUnder` call gives cursor-empty, keys returned and the `DiskS3ListObjects` delta around the call. The breakdown attributes the requests to the `limit + 1` first page, the async prefetch past the page's last key, resumed iterators at the store's default page, retries (`CASRequestAttempt` minus calls, plus SDK retries), and other LIST callers in the same phase: the audit figure is scoped to the `defer_decision` phase, not to the walk. The table goes into the pull request and the parent's verification item. No listing behaviour changes here.

## 7. Tests, gates, documentation {#tests}

- **Gates.** Suites are named for the `CAS*` gate filter. The gtest gate runs under ASan and TSan on the exact pushed tree. `LOGICAL_ERROR` expectations are death tests.
- **Oracle.** Each test runs one input through the current path and the changed path, and compares outcomes and carried rows as well as counts. For A0 the rows differ only in the flag.
- **Stand run.** ca-soak S11 delete phase, before and after. It records `cas_gc_log` per-phase durations and these ProfileEvents: `CASRefManifestBodyFoldGets`, `CASRefManifestBodyMemoHits`, `CASRefEmittedEdges`, `CASGCReadAheadMiss`, `CASGCReadAheadWasted`, `DiskS3DeleteObjects`, `DiskS3HeadObject`, `DiskS3ListObjects`, plus distinct `ManifestId`s per round.
- **Docs.** `docs/en/antalya/cas/architecture/garbage-collection.md` covers janitor batch deletes, the `janitor_deleted` meaning and the memo counter. The changelog gets one Performance Improvement entry.

## 8. Not in this spec {#not-here}

Stage B1–B3, the janitor settings, stage C, multi-node GC, the `_ckpt` and `.meta` protocol questions, and the `delete_tmp` repoint elision (CAS-83) stay in the parent spec and the backlog. The CAS-29.13 bounded listing and caller-termination fix moves to the parent's verification items, carrying the section 6 constraints.
