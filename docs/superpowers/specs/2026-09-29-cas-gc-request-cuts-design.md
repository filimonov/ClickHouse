---
description: 'Five request cuts in the CAS GC round that move no decision and add no setting: carry the confirmed condemn marker, decode each manifest body once per round, free the pinned HEAD read-ahead window, drain dead-life debris in batch deletes, and one LIST request per 1000 keys.'
sidebar_label: 'CAS GC request cuts'
sidebar_position: 101
slug: /superpowers/specs/cas-gc-request-cuts-design
title: 'CAS GC: five request cuts that move no decision'
doc_type: 'reference'
---

# CAS GC: five request cuts that move no decision — design (rev.1) {#cas-gc-request-cuts}

- Date: 2026-09-29
- Status: design approved in conversation; spec for codex review, then plan
- Parent: `2026-09-25-cas-gc-rounds-in-minutes-design.md` (stages A/B/C). This spec takes its cheapest measured items and nothing else: A0, A2, A4, the batch-delete half of B4, and the LIST page fix listed there as a verification item (backlog CAS-29.13). Stage A3 (`pending_deletes` fan-out) landed with PR #2351; B1–B3, the janitor settings of B4 and stage C stay in the parent.
- Backlog: CAS-29.3 (A0), CAS-29.5 (A2), CAS-29.12 (A4), CAS-29.9 (B4), CAS-29.13.
- Measurements cited: `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md` (F3, F4, F6, F24) and the parent spec section 1.

## 1. Rule {#rule}

One branch, one wave. Every change either moves the moment of a read, memoizes an immutable body, or issues fewer requests for the same deletes. Nothing else changes:

- no decision of the round moves: on the same input, `objects_deleted`, `objects_spared`, `objects_replaced`, `entries_graduated`, the carried rows and the outcome logs are identical to the current code;
- no new setting; the one bound needed (A4) reuses `rebuild_edge_budget`;
- no change to the on-S3 format (frozen since 26.6.4), the run row layout (A0 uses a flag bit the row already carries), LIST trust, deletion licences, exact-token deletes or HEAD-before-PUT;
- no change to phase order or cursors.

Acceptance is the same for every item: a gtest with a counting backend asserts the request count and the outcome equality against the current code; a run on the ca-soak stand (S11, delete phase) records `cas_gc_log` phase durations and the ProfileEvents deltas before and after.

## 2. A0: carry the confirmed condemn marker {#a0}

Site: `settleEntry`, `CasBlobInDegree.cpp` (the graduation gate, `if (e.marker_confirmed || !confirm_condemned_marker || confirm_condemned_marker(e))`). When the gate confirmed the marker but `graduationAvailable()` is false, the row is pushed to `still_retired` unchanged, so the run keeps `marker_confirmed = false` and only the in-process memo remembers the confirmation. After a restart or a leadership change every carried row pays the synchronous `.meta` GET again: audit F3, 200k GETs and four hours in round 1379.

Change: in that branch, push a copy with `marker_confirmed = true`. The run row already serializes the flag (bit 2 of the flags byte, `CasBlobInDegree.cpp` encode/decode), so nothing about the format moves. Rows whose marker was not confirmed are carried exactly as today. The gate's semantics do not change; A0 only stops discarding a confirmation the round already paid for.

Test (failing first): a run with N condemned rows, graduation budget 1, gate callback counting its calls; round 1 confirms all N (N calls), graduates 1, carries N−1; a fresh `Gc` (empty memo) over the carried run graduates the next row with 0 gate calls for the carried rows. Today: N−1 calls.

## 3. A4: decode each manifest body once per round {#a4}

Site: `Gc::foldManifestEdges` (`CasGc.cpp:1265`): `reads.takeRead(manifestKey)`, then `decodePartManifest`, `refMatchesBody`, `manifestNamespaceMatches`, then the per-entry deltas. `CASRefManifestBodyFoldGets == CASRefEmittedEdges` (audit F4): a body is fetched once per edge, and a round longer than a part's life folds the same manifest for its `+1` and its `-1`.

Change: a memo `ManifestId → decoded, validated entry list` that lives in the round object and dies with it. Two touch points:

1. the hint site (the log decode hints every edge's manifest key through `reads.hintRead`): a memo hit is not hinted, otherwise the read-ahead fetches the body anyway and the GET is spent;
2. `foldManifestEdges`: a memo hit skips `takeRead` and the validations (they ran on the first read of the same immutable body) and applies the entries exactly as a fresh read would, in the same order.

Bound: the sum of memoized entries is capped by `rebuild_edge_budget` (`CasPool.h`, default 8,000,000; the same limit that bounds the rebuild's in-memory edges, so the memo can never hold more than a rebuild already may). Past the bound the memo evicts the oldest manifest and the next fold of it re-reads. Absence (`!got`) is never memoized: an absent body is an observation the caller decides on each time, as today. A body that fails validation throws as today and is not memoized.

Counters: `CASRefManifestBodyFoldGets` counts only real GETs; a new `CASRefManifestBodyMemoHits` counts hits. Expected on otel.demo: manifest GETs in `fold_ref_intake` halve (F4).

Test (failing first): one round folding a log with `publish` and `drop` of the same part: 1 GET (today 2), the emitted deltas equal the current code's in the same order; a memo bound of 1 entry (via the existing `setRebuildEdgeBudgetForTest`) forces eviction and the re-read is counted as a GET; an absent body is re-probed on the second edge.

## 4. A2: free the pinned HEAD read-ahead window {#a2}

Site: `topUpHeadHints` (`CasGc.cpp:1825`) hints blob keys of the shard's candidate superset a window deep; `GcReadAhead::takeHead`. The merge walks candidates in hash order and skips those the superset overshot; when the window is full of hints whose keys sort before the key being taken, every take is a miss until those hints drain (`CASGCReadAheadMiss` high on mass-removal rounds; audit F24 small on otel.demo, AWS soak 128 s vs 42 s for `fold_reduce` with a free window).

Change, in `GcReadAhead`: before a top-up, and on a `takeHead` miss while `pending() == window()`, discard every pending head hint whose key sorts before the key being taken (each counted as `CASGCReadAheadWasted`, the futures waited as in the destructor), then top up. Reads (`hintRead`/`takeRead`) are untouched.

Test (failing first): a candidate superset with gaps against the sequential oracle: identical outcomes; `CASGCReadAheadMiss` within one window of zero (today: one miss per overshot key while the window is pinned); `CASGCReadAheadWasted` equals the number of overshot keys.

## 5. B4: dead-life debris drained in batch deletes {#b4}

Site: `CasNamespaceJanitor.cpp`, the per-key loop over the page (`op.head` when the listing carried no etag, then `op.remove(key, etag)`), one HEAD + one DELETE per key, 1.3–7.4 s per 1000-key page.

Change: dead-life `_log` and `_snap` keys are write-once and never reborn at the same key, so the keys of a page that pass today's checks (recognized key, life not resolved by the catalog cut, page not ambiguous, deletes not suppressed) are collected and deleted through the existing `removeChunkWriteOnceOrOneByOne` in chunks of `gc_bulk_delete_chunk_keys`, `op.admitted()` checked before every chunk exactly as the per-key loop checks it before every delete. Everything else stays: one page per round, `kListPageLimit` keys, the cursor rule, `page_decided`, the anomaly and `leaked` accounting per chunk outcome. The parent's `gc_janitor_pages_per_round` / `gc_janitor_page_keys` settings are not introduced.

Test (failing first): a dead life with P keys drains in ⌈P/1000⌉ delete requests (today P HEAD + P DELETE); a page with a live life on it still deletes none of that life's keys; a lost admission mid-page stops at the chunk boundary and keeps the cursor.

## 6. CAS-29.13: one LIST request per 1000 keys {#list-page}

Site: `ObjectStorageBackend::listUnder` (`CasObjectStorageBackend.cpp`): the first page asks the store for `limit + 1` keys (the extra key proves "more"), a resumed page asks for the store's default page, and the S3 iterator (`S3IteratorAsync`) prefetches the next batch. The global walk in `Gc` (`kListPageLimit = 1000`) sees ~3 `S3ListObjects` per logical page (audit F6: 14.4k requests for 1,543 pages).

Change, in two steps:

1. Measure first: on RustFS with 20k keys under one prefix, record `DiskS3ListObjects / CASRefGlobalListPages` (expected 2–3) and keep the number in the plan.
2. Ask the store for exactly `limit` keys per page and treat a page filled to `limit` as "maybe more"; the next call with the cursor proves the end by returning empty. Bound the iterator to the requested page so it cannot prefetch past it (`max_keys = limit` on every call, first and resumed alike). Semantics kept: cursor = last key returned, `start_after` used where honored, the `key <= cursor` filter for stores that ignore it. Callers that relied on `next_cursor` being empty at the end get one extra empty page, which `walk` already handles as the terminating page.

Acceptance: the ratio from step 1 is ≤ 1.1 after step 2 on the same run; a walk over N keys with page 1000 makes ⌈N/1000⌉ + 1 requests in a counting-backend gtest.

## 7. Tests, gates, documentation {#tests}

- Suites named for the `CAS*` gate filter; the gtest gate under ASan and TSan on the exact tree pushed; `LOGICAL_ERROR` expectations are death tests.
- Outcome-equality oracle: each test runs the same input through the current path and the changed path and compares outcomes and carried rows, not only counts.
- Stand run: ca-soak S11 delete phase before and after, `cas_gc_log` per-phase durations and ProfileEvents (`CASRefManifestBodyFoldGets`, `CASRefManifestBodyMemoHits`, `CASGCReadAheadMiss`, `CASGCReadAheadWasted`, `DiskS3ListObjects`, `DiskS3DeleteObjects`, `DiskS3HeadObject`) attached to the pull request.
- Docs: `docs/en/antalya/cas/architecture/garbage-collection.md` where behaviour is observable (janitor batch deletes, the memo counter); `docs/en/operations/system-tables/cas_gc_log.md` unchanged. Changelog: one Performance Improvement entry.

## 8. Not in this spec {#not-here}

Stage B1–B3 (per-life discovery, cleanup range and licence), the janitor settings, stage C (deadline, pacing, removed budgets), multi-node GC, the `_ckpt` and `.meta` protocol questions, and the `delete_tmp` repoint elision (CAS-83) stay in the parent spec and the backlog.
