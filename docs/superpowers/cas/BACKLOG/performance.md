---
description: 'Live backlog — read/write path performance, write-path optimization candidates, and scalability findings. Grouped by write path, read path and caches, S3 request budget, memory, and open design questions.'
sidebar_label: 'Performance'
sidebar_position: 8
slug: /superpowers/cas/backlog/performance
title: 'CAS Backlog — Performance'
doc_type: 'guide'
---

# CAS Backlog — Performance {#performance}

Part of the [CAS live backlog](/superpowers/cas/backlog). Two documents absorb most of the detail this file
used to carry, and are the first read for anything below: the
[otel.demo CAS S3 budget audit](/superpowers/reports/otel-demo-cas-s3-budget-audit) (findings F1-F31,
2026-09-25, live-workload numbers) and `docs/superpowers/cas/umbrella-roadmap.md` (the live tracker for
decided/scheduled work). Items here are the ones the audit and roadmap do not fully cover, plus the
audit-covered ones kept as a pointer. `2031-triage.md` (a separate, dated Russian-language triage ledger)
independently re-verified most 2031-triage-tagged items below; not re-cited per item, only where it changed a
verdict.

## Write path (publish, remove, ref lane) {#write-path}

### Read / write path {#read-write}

#### Mandatory blob `HEAD` {#mandatory-blob-head-cost} — KEEP

Protocol decided (2026-08-23): one `HEAD` before every publish decision, no conditional creation, no
metadata GET on a fresh miss. Performance acceptance is still blocked: the only measurement
([report](/superpowers/cas/unconditional-blob-publication-performance)) is target-only, not a matched
before/after pair. Not superseded by the 2026-09-25 audit, which measures the reads around a publish
(`{#ref-catalog-read-per-commit}` below), not the `HEAD` cost itself. Needs: a matched before/after
benchmark, human-accepted.

- **[ckpt-read-policy] Modular `_ckpt` first-attempt view: conservative / cached / prefetch** — DESIRABLE — USER-DIRECTED design shape, 2026-08-03: `_ckpt` handling must be modular/replaceable. Protocol-adjacent (touches the commit path); ships only as an explicit reviewed decision with matched before/after numbers. The blob-publication decision above does not authorize changing this separate mutable-control-object fence.

  Cost being addressed: every committed ref-log chunk pays `GET _ckpt` + token-CAS serially after the log `PUT`, so a lone `INSERT` pays +4 serial RTTs. A pluggable policy chooses only where `publishCkpt`'s first attempt gets its `{body, token}` view — the invariant core (retry-after-conflict always does a whole-body exact re-read, `lifeEpochWouldDecrease` re-checked after any re-read, durability order `log PUT → _ckpt CAS → ack`) is shared and policy-independent.

  Policies: (1) **conservative** = today, fresh GET per publish; (2) **cached** = seed with one GET on first touch, then serve from the writer's own last winning CAS and go straight to PUT-if-match (expected to almost always hit, since lease exclusivity excludes cross-process writers — a miss is a signal, not noise); (3) **prefetch** = one paginated LIST at mount seeds all `_ckpt` views, then memory-only + PUT-if-match (LIST is a pure hint here, correctness still rides the conditional write). Mandatory cache-invalidation edges for (2)/(3): fence-generation change, wedge, remount supersession, `catalog_life_invalidated`. Always-exact-read, out of the policy seam: recovery's `_ckpt` sample and GC-fold's frontier GET.

  Effect: +4 → +2 RTTs per lone insert. MEASUREMENT PRECONDITION: the stage-1 1.59x figure predates `_ckpt` (measured before it landed) — re-run the wide-insert baseline on current HEAD before benching policies against it.

  Decided 2026-09-25: see the audit F31 (docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f31); the three items are one owner decision.
- **[write-path stage 1] parallel intra-part blob upload — DONE.** `CasBlobUploadPool`/`fanOutBlobUploads`
  fan out a part's PUTs/dedup-HEADs. CA wide-insert wall 58.41s → 30.26s (3.0x → 1.59x vs plain S3). Evidence:
  `fff1c21989d6` + `4829435a157b` (cas-gc-rebuild only, not on antalya-26.6). Residual = stage 2 (postponed)
  and dedup HEAD/GET traffic (`[B121/B202]` below).
- **[TXN-ONE-PIPELINE]** — KEEP, HARD/structural. Single `dispatch` funnel + `precommit()`/`commit()`
  two-phase contract, replacing the two staging pipelines that caused the `01603` abort-ordering bug; the
  correct invariant is per-state-domain, not a total order. `commit` implicitly running `precommit` when not
  called (with `CasImplicitPrecommitInCommit` observability), plus a de-patching pass removing accumulated
  eager-dispatch/read-your-writes workarounds from non-CA files (`docs/superpowers/cas/upstream-patch-inventory.md`).
  Not landed on either branch (`CasImplicitPrecommitInCommit`, the acceptance marker, absent from both). Lands
  before codecs v3 and the source-layout refactoring.
- **[B121/B202/one-GET-open] read request-count reduction** — KEEP, design pass. Inline-by-size (drop the
  file-type predicate, inline < ~512 KiB, weigh the wide-part-medium-column regression, `.bin` carve-out), a
  per-blob-GET cost cut, one-GET part open. Companion to the (landed, opt-in) file-cache disk for re-read-heavy
  workloads. (An orphaned 2026-08-04-triage finding covers the same class via a measured DownloadPart/
  relink-fetch dominant read cost — folded in as confirmation.) Not landed on either branch. Related, not
  superseding: audit F9 (view rebuild) and F15/F16 (LIST on directory probes) below.
- **[B98]/[promote-recreate]** — DONE (provenance only). Unconditional streaming `publishBlob` removed the
  conditional-overwrite API and the tokened promote gate. Evidence: `940b1685bf96` (both cas-gc-rebuild and
  antalya-26.6). Emulated materialization is also DONE (`fe80d150eec7`, `emuPublishBlobAtomically` now
  streams instead of materializing the full body).
- **[R1/X1] ephemeral reader pin** — KEEP, design-only/VERIFY. Per-server-owned namespaces narrow the window and a live ref resolving to an absent object surfaces `FILE_DOESNT_EXIST` (`INV-NO-DANGLE`), so for normal MergeTree this is covered by `DataPart` lifetime. Cross-node GC fence for a ref-less reader;
  audit whether such a reader path exists at all before building it.
- **[ch128ctx] slot-bound blob-hash middle tier** — KEEP, small spec. `cityHash128(content) ∥
  xxh3_64(part_name, file_name) ∥ size` (256-bit; variable-width `BlobDigest` already supports it) closes the
  cross-slot dedup-collision vector at ~zero cost. The realistic adversarial dedup vector is attacker-crafted content deduped into a victim's future blob. Every load-bearing dedup survives: relink/carry-forward are
  reference-based; retry idempotency, same-name replica writes, and snapshot-upload→TTL-move prepayment are
  same-slot; only cross-slot content coincidence is lost (an explicit non-goal, `01 §what-it-does-not-buy`).
  Middle tier of `cityHash128` → `ch128ctx` → `sha256`. Main touch: the staged-blob hasher/request construction needs `(part_name, file_name)` context before
  `ensureBlobPresent`. Not landed on either branch. Origin: `10-backups.md §multi-disk` (2026-07-14).
- **[codex-26] `casAppendObject` before any concurrent appender** — KEEP, LOW/latent. A fresh-token/
  stale-payload lost-update shape (2026-07-17 codex-review triage, finding №26). Not reachable today
  (single production appender, `MergeTreeMutationEntry::writeCSN`; single-writer lease); the gap is documented
  in-code (`Pool/CasPlainObjects.cpp:16`, both branches) but not closed.

### Every committed ref chunk re-GETs and rescans the pool-global ref catalog (2031-triage CAS-112) {#ref-catalog-read-per-commit}

`commitRefChunk` performs a fresh, unconditional whole-object read of `cas/ref_catalog` immediately
before id allocation for every positive (state-growing) chunk
(`Pool/CasRefLedger.cpp:3248-3262`, gate `positive_append` at `:3194-3198`).
`CasRefCatalog::read` is a plain `backend.get` plus a full `decodeRefCatalog` plus a
`CatalogLifeIndex` build with no caching whatsoever (`Pool/CasRefCatalog.cpp:26-49`), and the row
lookup is a linear `std::find_if` over all entries (`Pool/CasRefLedger.cpp:3255-3261` at the caller,
helper `findEntry` at `Pool/CasRefCatalog.cpp:166-172`). A single part publish issues two positive
appends — the precommit (`Pool/CasPartWriteTxn.cpp:1338`) and the promote
(`Pool/CasPartWriteTxn.cpp:955`/`:1070`) — so a lone `INSERT` pays at least two full catalog GETs
whose body size and decode cost scale with the number of namespaces in the pool, entirely unrelated
to the table being written. Flat combining amortizes this across concurrent appends (one GET per
committed chunk, not per item), and the warm path does NOT re-read the catalog in
`namespaceLife`/`acquireMutableRefTableRuntime` (cached runtime, `Pool/CasRefLedger.cpp:620-631`,
`:4691-4718`) — so the cost is per chunk commit, not per API call.

This is the READ counterpart of {#ref-catalog-write-hotspot} (which is about creation-time CAS
contention on the same object) and the same shape as {#ckpt-read-policy} (a fresh control-object GET
per commit): the fix should be considered together with that policy seam, since both are
"exact-read-per-commit" of a singleton whose only mutators are lifecycle transitions under lease
exclusivity. Correctness note for any caching design: this read is a deliberate fence — it closes
the window in which another actor publishes `Removing` after the cached runtime's own admission — so
a cache needs the same invalidation edges {#ckpt-read-policy} lists (fence-generation change, wedge,
remount supersession, `catalog_life_invalidated`) plus the terminal-removal path staying always-exact
(`Pool/CasRefLedger.cpp:3223-3235`). Not correctness-affecting today; pure request-count/scale cost.

Decided 2026-09-25: see the audit F31 (docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f31); the three items are one owner decision.

### Write-path optimization candidates after stage 1 {#writepath-candidates-post-stage1} — KEEP, trimmed

Stage 1 took the wide 10M×30col×500part CA-S3 `INSERT` from 58.41s to 30.26s; still ~87% network-bound.
Remaining candidates:

Reports: `docs/superpowers/reports/2026-07-23-cas-wide-insert-baseline.md` (baseline),
`docs/superpowers/reports/2026-07-24-cas-wide-insert-stage1-effect.md` (stage-1 effect). The older 268.8
`HEAD`/part estimate predates the unconditional-publication rewrite.

1. **S3-native staging on the wide-insert profile** — MEASURE. Feature exists (opt-in, native-only same-store
   copy on the first absent publication). Local staging then upload moves every blob's bytes twice; native
   staging may cut wall on S3 backends. Flip the setting and compare.
2. **S3 client concurrency/connection tuning** — MEASURE. 16-33 concurrent PUT threads may be client-capped.
3. **Inline-placement threshold tuning** — INVESTIGATE THEN MEASURE. Small part files inline into the manifest
   (`CaInlinePlacement` machinery). ~239 PUT/part; first verify the threshold is a setting (not a pinned
   format constant), then measure PUT-count and wall deltas; a higher threshold could fold the small tail
   (marks, minor streams) into the manifest.

- (5) **Unconditional manifest `GET` on promote** — part of the 108.7 `GET`/part during insert;
  separate long-standing item. Verification semantics of the write path → under the spirit of the
  protocol veto; do not touch without an explicit user go-ahead. Status: DECISION NEEDED (present
  risk analysis to user).

  Decided 2026-09-25: see the audit F31 (docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f31); the three items are one owner decision.

One former item here is resolved elsewhere, not by this file: "repoint waste on part removal" (formerly: repoints against `delete_tmp_*` refs ≈ 22% of the writer `PUT` class) is
superseded by audit [F2](/superpowers/reports/otel-demo-cas-s3-budget-audit#f2) and
`umbrella-roadmap.md`'s "Remove a part in one transaction" (decided).

### Stage 2 (concurrent commitPart) — POSTPONED by user decision (2026-07-24) {#stage2-concurrent-commitpart-postponed} — KEEP, historical

Postponed: "too strong/unpredictable an effect on upstream/generic code." Design: bounded concurrent dispatch
of `ReplicatedMergeTreeSink`'s per-partition commit (`max_concurrent_part_commits_per_insert`, default 1 =
dormant), ~100-150 line patch, full hazard inventory done (shared dedup-cache state, quorum ordering) but not
closed. Not landed on either branch (`max_concurrent_part_commits_per_insert` absent from both); not touched
by the 2026-09-25 audit. Formerly separate, now folded in as the same requirement:
**[cas-commit-pool-anti-deadlock]** (a `cas_commit_concurrency`-sized bounded worker pool to avoid deadlock).

Agreed scoping (before postponement): (a) `ReplicatedMergeTreeSink` ONLY (the measured path), then (b) a
non-replicated leg, then (c) the `MergeTreeSink` counterpart as a follow-up. MUST: `deduplication_async_inserts_cache_version`'s
per-iteration reset is a shared member (`ReplicatedMergeTreeSink.cpp:455`) and must become per-task before any
fan-out. MUST-remeasure precondition: even a perfect stage 2 was not expected to reach 1.0x because blob
presence checks consumed ~12% of wall — that estimate must be remeasured under the current mandatory-`HEAD`
protocol before this is scheduled again. Full path anatomy, the remaining verification items (shared Keeper
session, shared dedup caches), the quorum-ordering recommendation, the non-replicated `MergeTreeSink` hazard,
and the rejected alternatives:
`docs/superpowers/cas/history/2026-09-26-stage2-concurrent-commitpart-hazards.md`.

Revisit only with an explicit user go-ahead.

### `DataPartsLock`-held DROP/REPLACE PARTITION covering-part publish (2031-triage CAS-048) {#covering-part-publish-under-datapartslock} — KEEP

`MergeTreeData::removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper` publishes the empty
covering part's whole remote commit inside the caller's `DataPartsLock`. P3: the part is empty, the trigger
is DDL-only, failure is loud. Fix needs a 3-phase caller restructuring (compute range under lock, release,
publish, re-acquire and re-validate) — a design task, not a code move. Cross-referenced by
`umbrella-roadmap.md` §2 "Shorter locks in MergeTree".

Details: docs/superpowers/cas/2031-triage.md#cas-048

### `[cas-part-commit-runs-under-parts-lock]` On a CA disk the whole S3 publish of an inserted part (blob fan-out, manifest staging, ledger append) runs while `MergeTreeSink::commitPart` holds the table's `DataPartsLock`, so concurrent inserts into one table serialize on seconds of S3 I/O {#cas-part-commit-runs-under-parts-lock}

Measured 2026-09-04 on the parallel stateless lane (RustFS, binary 9bf134686af), table `dedup_test` of
`02434_cancel_insert_when_client_dies` / `02435_rollback_cancelled_queries` (many concurrent inserts into
one table): 61 inserts averaging 107 s, `PartsLockWaitMicroseconds` 3 528 s against
`PartsLockHoldMicroseconds` 195 s over a 270 s window, i.e. the lock was held 72% of the window by
inserts. Where the holder was (`system.trace_log` type `Real`, samples inside `commitPart` without a
`SharedMutex::lock` frame): `finalizeConditionalWrite < nativeConditionalPut < ... <
CasRefLedger::stagingPutIfAbsent < PartWriteTxn::stageManifest < ContentAddressedTransaction::publishStaging
< ContentAddressedTransaction::commit < DiskObjectStorageTransaction::commit <
DataPartStorageOnDiskFull::commitTransaction < MergeTreeData::Transaction::commit < MergeTreeSink::commitPart`
(246 samples), the same chain through `CasRefLedger::flushRefBatch` (263), `fanOutBlobUploads <
uploadPendingBlobs < publishStaging` (140). Of 12 634 samples on the test's query threads, 5 719 waited
for the lock and 717 held it inside the CAS commit.

Why: `MergeTreeSink::commitPart` takes `lockParts()` (`MergeTreeSink.cpp:376`), calls
`renameTempPartAndAdd` with `rename_in_transaction=false` and then `transaction.commit(lock)`
(`:408`) inside the same scope. On a plain object-storage disk that commit is local metadata work; on
a CA disk `ContentAddressedTransaction::commit` is where the blobs are uploaded (fan-out, HEAD-before-PUT),
the manifest is staged (conditional PUT) and the ref-ledger append is flushed, about 1 to 3 s per part
at 54 ms per PUT. Every concurrent insert into the table waits for that. The upstream comment above
the call says the rename must stay under the lock (covered-parts race with merges), so the lock scope
itself is not ours to move (upstream-coupling rule).

Fix direction (CAS side only): perform the upload half before the lock. The blob fan-out and the
manifest staging depend only on the part's content, which is final when the writer finalizes, so they
can run at write-buffer finalize / `finalizePart` time (before `commitPart` takes the lock), leaving
only the ledger publish (one combined `_log` write per flush batch) under the lock. Check the
replicated sink (`ReplicatedMergeTreeSink.cpp:1022-1053`) for the same shape before deciding the
seam. Verification: rerun the two tests on the lane and read `PartsLockHoldMicroseconds` per insert;
the general win is every multi-writer table on CA storage, not just these tests.

### Unbounded pool-wide snapshot-publish fan-out (2031-triage CAS-051) {#snapshot-publish-fanout-unbounded} — KEEP

The single-in-flight gate on background snapshot publishes is per-table only; no pool-wide limiter, so an
ingest wave crossing the trigger threshold (256 log entries / 1 MiB) on N tables starts N concurrent
whole-namespace re-encodes at once. Fail-soft (retried on next trigger, per-table backoff), not a
correctness item. Owed: a pool-wide limiter under the existing per-table gate. (A previously-claimed
pending-count leak here does not exist — closed by `829ad698ef6`.) Confirmed still open on antalya-26.6.

Details: docs/superpowers/cas/2031-triage.md#cas-051

### Standalone write on a committed part pays a second, throwaway manifest body (2031-triage CAS-056) {#standalone-write-scratch-manifest-cost} — KEEP

A single-file write/unlink on a committed part stages a scratch manifest for the EDGE-BEFORE-OBSERVE closure,
then republishes a second, merged manifest body — 2 manifest PUTs, ~4 ledger appends, one GC deletion for one
changed file; a mutation multiplies this by part count. Also emits one audit row per carried-forward leaf on
repoint. Fix (protocol-adjacent, needs a go-ahead): stage the merged manifest once via the existing two-phase
`prepareEntries`+`promote` handle. Distinct from audit F2 (the higher-volume `delete_tmp_*` repoint case).
Compounds `BACKLOG/gc.md`'s `{#ca-log-tables-restart-cost}` (the same audit-row volume feeds the
restart-health-gate cost there).

Details: docs/superpowers/cas/2031-triage.md#cas-056

### Part staging is a linear-scanned vector, O(F²) path compares (2031-triage CAS-116) {#staging-vector-quadratic-path-scans} — KEEP

`PartStaging::entries` has no by-path index; every stage/move upserts by rescanning the vector. Real but
small: a 3000-file part costs single-digit ms of string compares against 3000 blob PUTs on the same path. P3,
owed only if file-count-per-part grows an order of magnitude: index `entries` by path. Confirmed unfixed on
antalya-26.6 (same struct shape, no index).

Details: docs/superpowers/cas/2031-triage.md#cas-116

### Conditional-write lane: no jitter, excluded from cross-thread retry pacing (2031-triage CAS-119) {#conditional-write-retry-pacing-and-jitter} — KEEP

Two residuals of the (settled) single-attempt design: (1) `backoffBeforeAttempt` is purely deterministic, so
a store-wide 503 episode produces synchronized reissue waves from one part's fan-out — fix: jitter the
backoff, same shape the S3 client already uses. (2) the single-attempt client clone doesn't share the disk's
shared-slowdown state with the parent. Neither is a correctness issue (bounded,
`Unresolved`-not-`Committed`). Confirmed unfixed on antalya-26.6. Adjacent: audit
[F26](/superpowers/reports/otel-demo-cas-s3-budget-audit#f26) (GC LIST bursts correlate with throttling).
Related, already tracked: issue #2244 (closed 2026-09-14, `7f932d31352`/`37c9bd4356b`; the lease/remount
ops had the OPPOSITE problem — no retries at all — now fixed, no topic-file anchor) and
`[timeout-retry RFC residuals]` in `BACKLOG/ref-protocol.md`.

Details: docs/superpowers/cas/2031-triage.md#cas-119

### Blob-upload pool: raw reference outlives it in `clickhouse-local` (umbrella review M6) {#blob-upload-pool-teardown-order} — KEEP

`clickhouse-local` calls `shutdownBlobUploadPool()` before `global_context->shutdown()` — the reverse of
`clickhouse-server`/`clickhouse-disks` — so a merge mid-fan-out at process exit can use-after-free the pool.
Confirmed present on both branches (`programs/local/LocalServer.cpp:918` vs `:922`). Fix: reorder in
`LocalServer::cleanup()`; longer term, a `shared_ptr` or use-counter instead of call-order discipline. P2, not
the same as the tracked backpressure item CAS-047.

## Read path and caches {#read-path-and-caches}

### Part-folder view cache byte budget is inoperative (2031-triage CAS-045) {#part-folder-cache-weight-always-256} — KEEP

`PartFolderView::estimatedBytes` returns `256 + manifest_size`, hardwired to 0 by both producers (also 0 on
antalya-26.6), so the 64 MiB budget degenerates to a 262144-entry cap, above the real 10000-entry cap, and
the oversized-entry bypass metric can never fire. Both dead: `CurrentMetrics::CASPartFolderCacheBytes`
reports the same fiction, and the oversized-entry bypass at `Parts/PartFolderAccess.cpp:226` compares 256
against `part_folder_cache_max_entry_bytes` so `CASPartFolderViewOversizedBypasses` can only ever be zero.
Memory-accounting only, P2, no correctness impact. Owed: weigh the view from its decoded body, delete the
dead `manifest_size` field; a gtest should assert a manifest with a large inline body weighs more than an
empty one (today `gtest_cas_part_folder_view.cpp:50` passes `manifest_size=1000` by hand, which is why the
unit tests never noticed). Same family as `formats-and-storage.md`'s
`{#manifest-inline-budget-no-spill}`. Adjacent: audit F9/F31 (view rebuild frequency) at
`{#read-path-repeated-view-lookup-per-open}`.

Details: docs/superpowers/cas/2031-triage.md#cas-045

### Ref-table cache budget is admission-only, not tunable, can underflow (2031-triage CAS-053) {#ref-table-cache-budget-admission-only} — KEEP

`enforceRefTableCacheBudget` runs only on cold recovery, never on in-place growth, so a hot table set can sit
above the 256 MiB default indefinitely. Untunable (no `ContentAddressedSettings` entry, no metric);
`total -= c.weight` is an unclamped subtraction that can underflow and evict every idle table in one pass
(`clampedCounterSub` exists, unused here). No correctness impact (an evicted table just re-recovers from the
durable snapshot+log on next touch — `gtest_cas_ref_writer.cpp:2025` pins this). P3.
Confirmed unfixed on antalya-26.6 (`CasRefLedger.cpp:1736`, antalya-26.6-specific line number).

Details: docs/superpowers/cas/2031-triage.md#cas-053

### `createHardLink` per-file manifest `HEAD` (2031-triage CAS-055) {#hardlink-per-file-forcefresh-head} — DONE (provenance kept)

The id-keyed manifest decode cache serves a `ForceFresh` hit with no request at all, closing this without the
originally-proposed per-transaction memo. Evidence: `b4ea16701ab1` (cas-gc-rebuild only, not on
antalya-26.6); `Pool/CasManifestReader.cpp:43-65` (`readManifestShared`).

### File-cache disk over CA never invalidates GC-reclaimed blobs (2031-triage CAS-084) {#file-cache-stale-after-gc-reclaim} — KEEP

The opt-in `<type>cache</type>` wrapper rides the cached router for reads but CA reclamation bypasses it, so
a reclaimed blob's cache entry is never invalidated. Not a correctness problem (content-addressed keys, a
size-bounded cache, LRU eviction); the residual is degraded hit rate and lingering local bytes. Confirmed no
invalidation hook exists on either branch. Fix: a reclamation-side hook that doesn't route CA deletes through
the cache.

Details: docs/superpowers/cas/2031-triage.md#cas-084

### Dedup presence cache charged 64 B/entry against a real ~176 B (2031-triage CAS-115) {#dedup-cache-weight-constant-64} — DONE (provenance kept)

The unconditional blob-publication rewrite deleted the presence cache entirely — no entry weight to correct.
Evidence: `940b1685bf96` (both cas-gc-rebuild and antalya-26.6); confirmed by grep, `presence_cache`/
`PresenceCache` has zero hits under `ContentAddressed/` on either branch.

### `readManifest`/`get`/`getStream` incarnation-token cache mismatch {#cache-get-head-token-mismatch} — KEEP

Formerly under the 2026-08-04 orphan triage. Independently confirmed open during the docs-consolidation
verdict audit; not re-verified against current call sites in this pass. The manifest decode cache above is
provably immune (keyed by the write-once `ManifestId`, not a mutable token); if this risk is real it lives in
a different cache/call path. Needs a fresh code-location pass before scheduling.

### Part-file open repeats path-parse + route + view lookup 2-3× (2031-triage CAS-118) {#read-path-repeated-view-lookup-per-open} — KEEP

`prepareRead` re-derives `(ref, file)` from scratch up to three times per open (in-manifest probe, blob-window
plan, file-size). CPU/lock traffic only, not requests — every repeat hits the warm in-memory cache, none
reaches object storage. Fix: derive one view/`Route` once in `prepareRead` and pass it to both callees. Ordering:
after the request-count items above. Adjacent to the owner-decided view-seeding fix in
`{#ref-catalog-read-per-commit}` (which reduces rebuild frequency, not per-open re-derivation).

Details: docs/superpowers/cas/2031-triage.md#cas-118

### `readLine` assembles every decoded record byte-at-a-time (2031-triage CAS-127) {#readline-per-byte-per-record-string} — KEEP

`Cas::readLine`, the sole line reader for every v3 text format (ref snapshot, log, manifest, fold seal, ref
catalog, GC outcomes), builds each line with a per-character loop and no `reserve`. Constant factor only, no
correctness/cap change, but sits on the decode side of every GC round and part-manifest open. Fix: scan for
`'\n'` and `append` whole chunks, reuse a caller-owned `String&`. Confirmed unfixed on antalya-26.6. Related,
already tracked: `{#writepath-cost-txn-final}` covers the WRITE-side allocation audit; this is the
read/decode side, which that item does not mention.

Details: docs/superpowers/cas/2031-triage.md#cas-127

### `[cas-decode-per-row-scratch]` The `cas_run` reader rebuilds its scratch every row; the writer does not — KEEP (scoped: scratch reuse DONE, two sub-issues open) {#cas-decode-per-row-scratch}

**Found while reading the decode path during the wire-keys phase-3 review (2026-08-30).**

Every decoded `cas_run` row allocates a `String` for the line through `readLine`, constructs a
`JsonObjectReader` — which owns a `std::vector<String> seen_keys` that grows as keys are read — and
destroys both. The write side already solved this: `SourceEdgeRunWriter` holds a reused
`CasJsonWriter scratch`, documented as keeping memory bounded by the largest line ever assembled
rather than by record count. The read side never received the same treatment.

Two related pieces of waste sit in the same place. `JsonObjectReader::nextKey` rejects duplicate keys
with `std::find` over that vector, comparing whole strings, which is quadratic in the keys on a row;
since every format's key set is fixed and already enumerated by the shared collectors, the check
could be a bit per known key, with no allocation and no string comparison. And `readString` returns
by value, so each wide field is a fresh allocation.

**This is not a regression from the wire-key cut** — it costs the same on both sides and appears in
no delta. It was in fact the leading pre-measurement hypothesis for where the cut's cost would land,
and the measurement refuted it: the hypothesis predicts that the format with the most keys per row
suffers most, and that format (`cas_fold_seal`) decodes 7 points cheaper than its byte growth, the
best of the five. Worth doing as a straightforward win, not as a fix for anything the cut caused.

**Scoped down, `cas-gc-rebuild` only:** `b55e44595e65`/`13e55acdc950` add `JsonObjectReader::reset()`
reuse, closing the main ask above. Still open on both branches: `nextKey`'s `std::find(seen_keys...)`
remains O(keys) (`CasTextFormat.cpp:223-224`); `readString()` still returns by value.

### `[cas-decode-register-pressure]` WITHDRAWN — the finding was a build-flag artifact {#cas-decode-register-pressure}

**Raised 2026-08-30 on an assembly review, withdrawn the same day.**

The item claimed that the wire-key cut raised register pressure in `SourceEdgeRunReader::next`, on
the evidence that spill density rose from 22.1% to 25.8% with new spills inside the hot loop. That
comparison was taken across two binaries built with different frame-pointer settings: the after side
reserved `rbp`, the before side did not. On correctly matched binaries spill density **falls** in
every decode symbol inspected and does not move in the encode symbol. There is nothing here to fix.

Kept as a withdrawn entry rather than deleted, because the reasoning that produced it was published
and someone may come looking for it.

## S3 request budget {#s3-request-budget}

### Pool-wide catalog write hot spot {#ref-catalog-write-hotspot} — KEEP (scoped: read-side timeout-cooperation fix DONE, catalog-growth question open)

Every table creation writes the same `cas/ref_catalog` object, serializing a table-creation-heavy lane through
one CAS loop (137/250 S3 timeouts on the CA-s3 lane named this key). Design isn't wrong (catalog exists
because pool `LIST` is unreliable) but the cost wasn't exposed before. Open: creation-only or also
read-mints; retry deadline vs. contention; sharding without losing single-object GC-snapshot atomicity.
Write-time counterpart of `{#ref-catalog-read-per-commit}` (audit F20 measures the read side); tracked live
as `umbrella-roadmap.md` §2 "Catalog write hotspot" (hot-key lane phase B, issue #2343) — not superseded, the
roadmap points back here.

**Merged in from `[ref-catalog-write-hotspot]` (BACKLOG.md), same anchor, later measurement.**

**Found by the first full local run of the stateless suite on CAS storage (2026-08-30), 11,137 tests.**

One test failed with `Code: 499 ... Timeout ... key cas_s3/cas/ref_catalog, object size 41454`. The
S3 client retried twice more and both retries timed out at the same size, so this is one logical
write, not three failures.

What makes it worth recording is the negative half: across 11,137 tests, **`ref_catalog` was the
only object class whose write ever timed out.** No blob, no manifest, no ref log. The catalog is
pool-wide, mutable, and rewritten whenever a namespace is created or dropped — and a full stateless
suite creates and drops tables continuously, so the catalog is both the hottest write in the pool and
the one that grows with the number of namespaces that have ever existed in it.

This is **not** established as a defect. The run had a load average above 20 with a saturated
single-node object store, and a 41 KB write timing out under that is plausible on its own. What is
established is where the pressure lands.

**Worth measuring before deciding anything:** how catalog size and rewrite frequency scale with
namespace churn, and whether the write is proportional to the whole catalog or to the change. If it
is the whole catalog on every change, the cost is quadratic in namespace count over a workload's
lifetime, and a busy pool reaches the timeout on merit rather than by luck.

This finding is the argument for the full lane existing at all: the 41-test CAS selector that stood
in for it could never have produced this, because it never creates enough namespaces to grow the
catalog.

**Reads time out too, and the rate grows with the run (measured 2026-09-04, parallel stateless lane
on RustFS, binary 9bf134686af). Root cause found: the S3 client's adaptive first-attempt timeout.**
`AWSClient` logged `Failed to make request to ...cas_s3/cas/ref_catalog: will be retried,
Poco::TimeoutException` for GETs of this one key: 1 222 of 1 233 such lines in a 30-minute window were
this key; 24, 260, 305, 401, 436, 468, 455 per 10-minute window over the run. The logged stack throws
in `SocketImpl::receiveBytes` under `HTTPClientSession::receiveResponse`, i.e. waiting for the response
headers. With `s3_use_adaptive_timeouts` (default on) the first attempt of every request runs under
`TimeoutsForFirstAttempt`: 200 ms to the first byte for GET (`src/IO/ConnectionTimeouts.cpp`),
saturated against the request timeout, so the CAS `attempt_timeout_ms` of 5 000 never applies to the
first try. The catalog is rewritten through conditional PUTs 467 times a minute (hot-key lane submits,
`CASHotKeyCacheStarts` delta), it had grown to 104 KB / 612 entries, and a PUT on RustFS costs 54 ms
or more; a GET that lands while the object is being rewritten waits behind it and trips the 200 ms
fuse. The retry is the CAS engine's own (`Retry::standard`, jittered backoff 0-200 ms first), and it
succeeds. Cost: every hit shows as exactly 3 `S3ReadRequestsErrors`; `CREATE TABLE` with one hit
p50 233 ms against 0 ms without (2 731 of 8 943 statements in 30 minutes, 32%), with two hits
p50 962 ms (57 statements). Not caused by write contention: `PreconditionFailed` on the catalog and
`CASHotKeyQueueWaitMicroseconds` are flat over the run while the timeout rate grows fivefold; what
grows is the object and therefore the window a GET spends behind a PUT.

Fix direction (user ruling 2026-09-04: keep the adaptive fuse, it exists to abandon a bad connection
to real S3 quickly; make the CAS retry cooperate with it instead of fighting it). Today the engine's
reissue is a brand-new request: `ReadBufferFromS3` starts at attempt 1 (`max_single_read_retries` is 1
for CAS reads), `PocoHTTPClient` sees `first_attempt` again and applies the 200 ms fuse again, and the
engine sleeps `Retry::backoff(attempt)` (0-200 ms jitter) before it. Upstream's own retry does the
opposite: attempt 2 runs on a fresh connection with the full timeouts (`Client.cpp:847` bumps
`setClickhouseAttemptNumber`). Two changes, both in the CAS engine plus one small seam:
(1) thread the engine's attempt number into the request — `readSettingsFor(profile, timeout, attempt)`
and the write twin carry it in `ReadSettings`/`WriteSettings`, `ReadBufferFromS3::sendRequest` /
the write path seed `setClickhouseAttemptNumber` from it (upstream seam, consult first) — so the
reissue gets the full `attempt_timeout_ms` on a new connection; (2) classify a first-attempt
`Poco::TimeoutException` as a connection-quality retry: reissue at once, no backoff, still under the
deadline gate; backoff stays for attempt ≥ 2 and for every store-side fault. Expected on the lane:
a hit costs the 200 ms fuse only, the double-hit case (p50 962 ms) disappears. The catalog growth
itself stays the hotspot above.

**Closed for the timeout-cooperation half, `cas-gc-rebuild` only:** `9a6bcb68aca` plus the CAS R2
series thread the engine's own attempt number into `ReadBufferFromS3::sendRequest`
(`src/IO/ReadBufferFromS3.cpp:594`), and `CasRequests.cpp:987-1032` classifies a connect-failure hint
for immediate reissue. The catalog-growth/quadratic-rewrite question above is untouched on both
branches.

### Every blob body has a `.meta` sibling: two objects per part file (2031-triage CAS-117) {#per-blob-meta-sibling-object-count} — KEEP

Every fresh/adopted blob gets a paired `.meta` freshness marker, doubling object count and LIST enumeration
for `.bin`/`.mrk*`/`primary.idx` (small metadata inlines and pays nothing, so this is not a "wide part of
small files" issue generally). On top of that each body carries a padded envelope, a large relative inflation
of stored bytes for a tiny `.mrk` file — the envelope's own open question is `formats-and-storage.md`'s
`{#blob-envelope-never-read-back}`. P3. Owed: decide whether the marker can be folded (e.g. only on
`Condemned`) — protocol-adjacent, needs a go-ahead; cheap now: report the body/`.meta` split in
`SYSTEM CAS FSCK`. GC-side companion: audit [F7](/superpowers/reports/otel-demo-cas-s3-budget-audit#f7),
tracked as `umbrella-roadmap.md` §2 GC "Cheaper GC per garbage blob (decide)".

Details: docs/superpowers/cas/2031-triage.md#cas-117

### `[hot-key-lane-phase-a-followups]` Phase A of the hot-key lane landed on both `cas-gc-rebuild` and `altinity/antalya-26.6`; what its reviews deferred (2026-09-04) {#hot-key-lane-phase-a-followups}

Formerly `[ref-catalog-cas-starvation-under-parallel-writers]` {#ref-catalog-cas-starvation} — one
process's CREATE/DROP writers starved each other on the ref-catalog compare-and-swap. Seen on the local
CA-s3 stateless lane (run 2, ~10 parallel jobs): `01039_mergetree_exec_time`'s `CREATE TABLE` failed
after 78.9 s with "CAS ref catalog update: gave up at the policy deadline"; RCA in
`docs/superpowers/cas/2026-09-04-ref-catalog-starvation-rca.md`. **DONE, superseded:** the hot-key lane
(`Backend/CasHotKeys.{h,cpp}`, commit `4ec755474fb`, an ancestor of both `cas-gc-rebuild` and
`altinity/antalya-26.6`; design `docs/superpowers/specs/2026-09-04-cas-hot-key-write-lane-design.md`
revision 34; tests `gtest_cas_hot_keys.cpp`) fixed it; anchor kept here as an alias, still cited by
`docs/superpowers/plans/2026-09-04-cas-hot-key-lane-phase-a.md:2325` and the design spec.

Branch `cas-hot-key-lane` (12 commits over `e59fe7e8e4b`), `CAS*` gate 2406/2406, reviewed per task (opus/lite),
whole-branch (opus: mergeable) and end to end (codex `gpt-5.6-sol` high: three majors, folded). **Landed
and merged on both branches** (merged 2026-09-05; not "not yet merged" as this entry originally said
before this grooming pass).

Still outstanding, three weeks after the 2026-09-05 merge (as of 2026-09-25/26), not "before or right
after merge" as this entry originally said:
- The new death-test twin `CASRefCatalogDeathTest.AStaleHintCasAdmitEntryAdmitsWhenTheStoreHasRoomAborts` has never
  executed: `lane-g` has no debug or sanitizer build. Run `CASRefCatalog*:CASHotKeys.*` in an ASan build once.
- The acceptance measurement of the design's Task 7 (ten minutes of the parallel stateless suite on the CA-s3 lane:
  `DROP TABLE`/`CREATE TABLE` percentiles, `PreconditionFailed` on `ref_catalog`, the `CASHotKey*` and
  `CASRequestConflictPause` deltas) is the gate for `{#hot-key-lane-phase-b}`.

Deferred by the reviews (all triaged as not blocking the merge; the first is the one the measurement needs):
- The catalog loop's own conflict pauses (`op.pause` in `casUpdateImpl`) have no `ProfileEvents` counter; the engine's
  `CASRequestConflictPause` counts only `readModifyWrite`'s. Add one so the hottest key's pacing is visible.
- Same-thread reentrant `submit` while holding self-waits until the deadline (the spec's callback rule is stated, not
  enforced); a thread-local in-hold flag throwing `LOGICAL_ERROR` would make it immediate. Note: a `LOGICAL_ERROR`
  aborts debug and sanitizer builds.
- `CasRequests.h` pulls `Common/CacheBase.h` into every includer; a forward declaration of `CasHotKeys` plus an
  out-of-line `CasRequests` destructor would confine it.
- `CASRequestConflictPause`'s description says "no transport fault preceded it"; accurate: "in the same inner write".
- `kWaitSlice` in `Backend/` vs the directory's `SCREAMING_SNAKE` constants.
- The `Leave` guard's comment says "allocates nothing"; true only under the lock (the log line after it allocates).
- Tests that discriminate weakly (each carries a deterministic sibling, so none is a false-pass risk today):
  `CleanConflictsBeforeAFault...` (probabilistic; assert the ConflictPause/Reissue deltas),
  `AConflictThatSettledAFault...` (sleep bound non-discriminating; the Reissue delta carries it),
  `ABaseReadThatFails...` (pins only `sent_any` absolutely), `ACachedStartPastTheDeadlineSendsNothing`
  (`decided == 1` is the discriminator, closed by the final review), the corruption test's "break again" arm,
  `AFailedEnqueue...` (only the `inserted == true` arm), the GC-erase test ("exactly" in prose, "at most" in code),
  the "nothing of ours landed" assertion (give the external a distinct `removal_started_round`).
- Unused `ProfileEvents` externs in `gtest_cas_ref_catalog.cpp` and `gtest_cas_pool.cpp` (plan-mandated).
- The two remaining items of `task-1-review.md` (the SDD workspace under `.superpowers/sdd/`, git-ignored; the
  review files there are the only record of them if the workspace is deleted).

Related: `[hot-key-lane-phase-b]`; `[stateless-lane-wall-time-is-drop-table]`.

### `[stateless-lane-wall-time-is-drop-table]` Measured on the CA-s3 stateless lane (2026-09-04): DROP TABLE is the suite's largest wall-time sink; the server is not CPU-bound {#stateless-lane-wall-time-is-drop-table}

`system.trace_log` over ten minutes of the parallel suite (~10 jobs): ~280 CPU samples against 630k
Real samples — the server is waiting, not computing; 21% of the CPU samples are stack unwinding and
symbolisation for exceptions (every expected 412/404/timeout builds a full `StackTrace`). Query threads'
Real samples: 44% waiting in `InterpreterDropQuery::executeToTable` for the synchronous data drop, 25%
in `TaskTracker::waitAll` under `fanOutBlobUploads`/`commit`/`moveDirectory` (the part-commit round
trips), 14% in `finalizeConditionalWrite` (conditional PUT wait), 13% in `poll`. `system.query_log`,
same window: DROP TABLE n=1436 p50 2.4 s p90 11.9 s max 34.7 s; CREATE TABLE p50 184 ms p90 391 ms;
INSERT p50 253 ms p90 541 ms. S3 calls: GET 11 095, HEAD 2 966, PUT 1 597, LIST 206, DELETE 25.
The background drop workers themselves sit in a futex inside `DatabaseCatalog::dropTableDataTask`, not
in S3 — the drop chain is serialised somewhere (the namespace removal through the hot `ref_catalog`
door with the conflict backoff up to 5 s fits the p90/max shape). Placement: profile one DROP on the CA
disk (S3 operations and CAS conflicts per dropped table) as the first measurement of the
`{#ref-catalog-cas-starvation}` follow-up; second target = the part-commit round trips (upload fan-out
+ conditional PUTs); third = skip `StackTrace` capture for expected 412s.

Design for the first target: `docs/superpowers/specs/2026-09-04-cas-hot-key-write-lane-design.md`
(revision 34, phase A, 2026-09-04; accepted by the codex review of revision 33 with no CRITICAL or MAJOR), which supersedes the fix sketch below where they differ; the
deferred phase B is `{#hot-key-lane-phase-b}`.

**Status (2026-09-26):** the first target (`ref_catalog` write hotspot) is closed by
`[hot-key-lane-phase-a-followups]` above. Targets 2 (part-commit round trips: upload fan-out +
conditional PUTs) and 3 (skip `StackTrace` capture for expected 412s) are still open, and target 2 is
continued by `[drop-path-head-of-line-and-repoint-ramp]` in `gc.md`.

### `[hot-key-lane-phase-b]` Hot-key lane phase B: combining, GCS spacing, the hold clamp, the GC erase, `_ckpt` (2026-09-04) {#hot-key-lane-phase-b}

Designed to revision 26 of the hot-key lane spec (commit `26bde9f9604`, 13.5k words) and deferred from
the phase A landing (revision 27) by owner decision on 2026-09-04. Not started. Phase A fixed the seams
so that none of this reopens the callers: `submit`'s signature, `Decide = DecideOnObject`, the engine's
`WriteResult` returned unchanged, the caller's `Conflict` loop and pause rule, the queue of `Item`s with
the guard as the single remover, the hold as base, decide, write, settle. Each sub-item below has its
own gate; none is started on the documentation's say-so.

**0. Caller-side freeze in `dropNamespaceImpl` (do first, not phase B proper).** `cancelStalledCreating`'s
creator-fence read (`isCreatorFenceTerminal(cancel_op, ...)`, `CasRefLedger.cpp:5121`) runs under a
default `Retry::standard()` of its own while `resolveNamespaceLife` freezes once and passes its policy
down (`:1384`). Inside a hold that read can keep the key for up to its own 90 s after its caller's
window. Freeze once at `dropNamespaceImpl`'s entry and pass it to `cancelStalledCreating` and its read.
One-line class; makes the clamp (item 3) unnecessary for phase A.

**1. Combining.** Gate: the phase A acceptance run's `CASHotKeyQueueWaitMicroseconds` per submission
and `PUT` rate on `ref_catalog`; combining pays when the queue wait, not the write, dominates a
submission at p90. The design, with the rules twenty-two rounds established (each was lost once in a
rewrite and found again as a critical, so they are listed): the holder takes the items queued behind it
that were submitted on the same `CasRequests` (the leader's per-attempt fence gate then covers every
member: same atomics, and a re-arm between two admissions is caught by whichever generation is older),
are not under `single_attempt` (leader or member; `Retry::once` permits one attempt and a batch may
reissue), carry no `Liveness` closure (the engine consults one before every physical attempt and the
batch's write is gated on the leader alone; such an item holds alone), and whose bound deadline is not
earlier than the leader's; allocation (`BatchOutcome`, the members vector) before taking, `taken` set in
the same critical section, so a failed allocation takes nothing; "a leader takes this item" and "this
item's caller leaves" decided under one mutex against `taken`, exclusive (the use-after-free the flag
prevents was found by opus round 17). Chain: each member's `decide` on `Object{bytes = candidate, etag =
base etag}` (the etag is a placeholder no `decide` may read as an identity of the bytes), after the
member's own `gate(reservedFor(0, 2))` (`FenceLost`/`NoBudget` written as its `GaveUp` with `sent_any =
false`, `decide` skipped); a member's exception is held; a decline stops the chain so the landed object is
exactly its input; a taken item the chain did not reach is `Conflict{NotObserved}`. Settle: `Committed`
gives each contributor a three-way `gate(0)` as `postCommit` does (`FenceLost` → `GaveUp{FenceLost,
sent_any = true}`; `NoBudget` → `GaveUp{Deadline, Lease, sent_any = true}`; else `Committed{combined
etag, attempts_sent = 0}`), delivers held exceptions, and gives the declined member
`Declined{landed object}`; `Conflict` → every member `Conflict{seen, 0, any_ambiguous}`; `Refused` → the
same `Refused` to every member; `GaveUp` with `sent_any = false` → members `Conflict{NotObserved}`;
`GaveUp` with `sent_any = true` or an exception → members `GaveUp{Unresolved, sent_any = true}` with
their own `Source`. Held verdicts are delivered only from a landed batch (as-if-serial: a batch that does
not land is where serial execution would have landed a prefix). The leader's guard settles unsettled
taken members the same way, settle and erase in one critical section. The as-if-serial argument covers
the lane's key alone; a `decide`'s other reads must not observe a key a batch member may write. A
member's `decide` runs on the leader's thread while its caller is parked (disjoint parts of the member's
operation; the caller's step 1 touches `admitted_generation`, the fence and clock closures, the bound).
`written` is a prefix state the store never held once later members followed (only test callers read
it). Members carry `attempts_sent = 0`; the pool's attempt counters are the leader's. A member's reads
inside the hold need the clamp (item 3). Invariants HK4, HK7, HK8 and tests 1 to 4, 12 and 16 of
revision 26.

**2. `Generation` spacing.** Gate: 429/`SlowDown` count on the catalog key over the parallel stateless
suite on the `gcs` lane after phase A; the owner's position is "GCS will say if we are too fast". The
design: `write_spacing_ms` as a constructor argument (`Pool` passes 1000 on the `Generation` dialect, 0
elsewhere); `Lane::last_write_end_ms` optional, set by a no-throw guard around the engine's write on
return and unwind (the lane exists then, the leader's item is in its queue, nothing allocates); before
the write, `op.pause` the smaller of the remainder and the leader's remaining window, and let the verb's
own entry gate decide (a pause that consumed the window ends `GaveUp{Deadline, sent_any = false}`);
erasure becomes "erase a lane whose queue is empty and whose last write is older than the interval",
swept at enter and leave, with the residue after the pool's last activity bounded by the write rate.
INV-HK9 and tests 17, 18 of revision 26. Ten rounds of findings went into where this lives; above the
engine it is one place, inside `readModifyWrite` it was three call sites and two result families.

**3. The hold clamp.** One optional clamp deadline on `CasOperation`, honoured as a minimum at its
twelve `policy.bind` sites, set by the lane for the hold and by a leader for a member's `decide`,
cleared after; a read the clamp refuses gives up as at its own policy deadline, reported with
`GaveUp::Source::Policy`. Required by combining (a member's nested read runs under the leader's hold);
for phase A, item 0 is the caller-side answer.

**4. The GC erase into the lane.** `deleteCompletedRemovingAtSnapshot`'s body from "refresh authority"
through the `replace` becomes one `submit` whose `decide` refreshes authority, checks `op.admitted()`,
`throwIfAmbiguous` and the exact row, and returns the erased candidate; the mandatory resolution read
after `Committed` stays authoritative for the reconciler's next selection. Four prerequisites: the
refresh reads `gc/state` through the erase's own operation, not a private one, so the clamp reaches it;
the `decide` keeps `throwIfAmbiguous` and the absent-catalog refusal; `CompletedRemovingDeleteResult`
needs the catalog cut the call ends on for `FencedOut` and `EntryChanged`, which a `decide` exception
carries nowhere, so the erase captures the base it was shown or re-reads; the double refresh a
cached-base re-run causes is one extra `gc/state` `GET` inside the hold. Its operation carries a
`Liveness` closure (the cached `authority_held`), so it holds alone and never combines. The authority
gap across the engine's internal reissue (a TTL on the GC lease) stays its own item. Until then the
erase races the lane as it races everything today and costs the lane one stale cache entry per erase.

**5. `_ckpt` and `gc/state` through the lane.** One-call change per site (`readModifyWrite` to `submit`
in a `Conflict` loop with the pause rule). Gate: the audit of the `decide` contract's condition 1 for
`publishCkptContribution`, whose `decide` counts its runs and records its decline's reason in captured
locals that the caller's outcome reads, so a re-run on a fresh base doubles what it reports; move both
out of the closure first. Operations that carry a `Liveness` closure will not combine (item 1) but do
serialize and use the cache. Half 2's go/no-go for `_ckpt` left on `readModifyWrite`: 40·M requests per
second per contended key for M writers, and 429/`SlowDown` not above today's.

**How the design got here, for whoever picks this up.** Twenty-six revisions in one day; findings per
review round never fell below eight; the document tripled while the core (FIFO ticket, `taken`
handshake, cache as a hint the `PUT` validates, engine result unchanged) was judged sound from revision
18 on. About 60% of the MAJOR+ findings after revision 21 were specific to combining, 20% to spacing and
erasure. Rules for the next round of this work: a review revision only removes or tightens, never adds a
feature; a new feature goes in its own revision and costs at least one round; a reviewer verifies
against the checklist above and names the failing scenario; prose and pseudocode slips are MINOR; after
three rounds the remaining questions are answered by code and tests, not by another revision.

Cross-reference: `docs/superpowers/cas/BACKLOG/gcs.md#gcs-hot-control-keys-429` independently proposes a
narrower `_ckpt`-only pacing scheme for GCS; reconcile before starting either.

## Memory {#memory}

### Write-path allocation and ref-table commit-path cost (2026-07-16, TXN-Final campaign) {#writepath-cost-txn-final} — KEEP

- **[write-path-alloc-audit]** — During the TXN-Final full CA-default stateless run, `system.trace_log` showed the CA write path dominates the Memory (allocation-sampling) trace: `ContentAddressedTransaction::tryCreateWriteBuffer` (~489k samples) + `writeFile` (~488k), then `CaInlineWriteBuffer` (~322k) and `CaContentWriteBuffer` (~165k). CPU was clean (NO CAS symbol in the top-15 CPU stacks) — so this is NOT a CPU or correctness issue, purely an allocation-volume observation. **Confirmed and quantified by audit
  [F18](/superpowers/reports/otel-demo-cas-s3-budget-audit#f18)**: every stream double-allocates its write
  buffer (content + spill sink) for parts with a 10 KB/1.5 KB median size — tracked live as
  `[ca-write-buffer-allocation-concentration]` and `umbrella-roadmap.md` §2 "Write buffers for tiny parts."
  Not a leak in either measurement. Still open, not covered by F18: per-file overhead beyond the double
  buffer (finalize closure, captured `owner shared_ptr`); by-value header maps in the emulated test path; a
  pre-/post-TXN-ONE-PIPELINE allocation baseline.
- **[ref-table-copy-commit-path]** — The #1 CPU stack in the same soak was a full-value copy of the committed
  `RefTableState` map on every ref-op commit, O(refs) per commit. Confirmed still present at HEAD on both
  branches (`Pool/CasRefLedger.cpp:3130`/`:3170` on cas-gc-rebuild). I/O-bound workloads don't feel it (the
  2026-09-25 audit shows no CAS CPU/mutex hotspot on its stand); a scalability smell for insert/mutation-heavy
  loads. Owed: pass by const-ref / diff incrementally / copy-on-write.

### `[ca-write-buffer-allocation-concentration]` Two write-buffer constructors account for essentially all CAS allocation activity {#ca-write-buffer-allocation-concentration}

**Found by profiling a one-hour chaos soak (2026-08-31) against `system.trace_log`.**

**Read the counts as a ratio, not as a census.** The same export that produced them was
CAS-filtered (`WHERE stack LIKE '%DB::Cas::%'`), so no non-CAS allocator appears here and this is
not a ranking of server-wide allocation; and it summed 38 snapshots of a cumulative table, so every
absolute number is inflated by roughly the number of snapshots a sample survived into. Neither
defect touches the finding, because both act as a near-uniform multiplier across the frames being
compared, and the finding is the **200-fold gap between second and third place** — a ratio that no
plausible per-frame variation in either defect can manufacture. The Memory profile also rests on
25k-154k samples rather than the CPU profile's few hundred. Do not quote the absolute sample counts
anywhere; quote the ratio.

Aggregating CAS frames by sample count, the Memory profile is not merely dominated by two frames —
everything else is invisible next to them:

| frame | Memory samples |
|---|---:|
| `CaContentWriteBuffer::CaContentWriteBuffer` | 85,991 |
| `CaInlineWriteBuffer::CaInlineWriteBuffer` | 35,600 |
| next entry (`PartWriteTxn::promote`) | 175 |

A factor of 200 between second and third place. Reading the constructor explains it: each content
write allocates its streaming buffer via `clampCasWriteBufferSize`, and then a **second per-stream
spill buffer**, and creates a temp directory and a random temp path. Every blob write pays this.

**What this does NOT establish.** These are allocation samples, not time. The Real profile's top is
the upload wait — `ensureBlobPresent` and `fanOutBlobUploads` at ~116,000 samples each against 3,750
CPU samples, a ratio near 31:1 — so the write path is overwhelmingly I/O-bound and buffer allocation
is not visibly on the critical path. A pooling change could remove a great deal of allocation churn
and move the wall clock by nothing at all.

That caution is not hypothetical: on this same campaign two changes that plainly removed waste
measured exactly zero, and one that plainly removed a copy measured a reproducible *regression*. The
one that paid was the one whose mechanism predicted which formats would improve.

**Measure before changing anything:** what fraction of a blob write's wall time is buffer
construction, from a scoped profile of the write path rather than from the sample counts above. If it
is under a percent, this entry should be closed as "concentrated but not costly" rather than acted
on.

**Retracted: there is no usable CPU profile from this soak, so nothing here ranks by CPU.**
An earlier revision of this entry ranked `ObjectStorageBackend::nativeHead` in "the CPU top" and
then explained the mechanism. Three defects, each fatal on its own, mean no CPU ranking from this
run may be cited:

1. **Filtered by construction.** The export ran `WHERE stack LIKE '%DB::Cas::%'`, so every frame not
   named `DB::Cas::*` — hashing, compression, serialization, the whole of the generic engine — was
   excluded before aggregation. Asking why checksums are absent from that file is asking why a file
   does not contain what it was built to exclude.
2. **No samples to speak of.** Over the hour the `CPU` trace type accrued **369 samples**, against
   963,049 for `Real`. The CPU profiler fires on thread CPU time, and these threads spent almost
   none: the workload is S3-bound. A few hundred samples cannot support a ranking.
3. **Multiply counted.** The dump queried the cumulative `system.trace_log` every ten minutes and
   the frame aggregate summed 38 such snapshots, so the same sample is counted once per snapshot it
   survived into. The arithmetic shows it plainly: the retained stacks hold 1,460 samples while the
   top frame claims 4,713.

The `Real` profile does not share defects 2 and 3's severity — 963k samples — but is subject to
defect 1, and remains the basis for the only claim worth keeping from this run: the write path waits
far more than it computes.

**What a real answer needs:** a CPU-bound workload (bulk insert of large parts, where content
hashing actually has bytes to chew) profiled with an unfiltered query and a single end-of-run
snapshot. Until that exists, this entry asserts nothing about where CAS spends CPU.

### Suffix allowlist buffers big index files whole in memory (2031-triage CAS-014) {#part-file-suffix-allowlist-memory} — KEEP

`partFileMustStayBlob` doesn't know the shipped default names (`primary.cidx`, `.cmrk4`, skip-index files),
so those buffer whole into memory via `CaInlineWriteBuffer` before the 1 MiB spill cap applies — no
correctness/bloat defect, just avoidable memory + a double write for wide indexes. Unchanged since
`c623713479f`; confirmed still missing on antalya-26.6. Fix: add the default names to the allowlist; log when
an unknown extension takes the buffered path. Cross-referenced by `BACKLOG/formats-and-storage.md` and
`umbrella-roadmap.md` §3.

Details: docs/superpowers/cas/2031-triage.md#cas-014

## Full-scale scenario findings (ca-soak) {#full-scale-scenario-findings}

### S23: idle-pool RSS growth of 500 MB is still not separated from boot warm-up or telemetry churn {#s23-idle-rss-growth}

(Alias: `s23-idle-baseline-measures-telemetry`.)

**Found by rerunning S23 at `--scale full` (2026-08-31), which FAILED where `dev` and `ci` were
inconclusive.** The `dev` window is 4 minutes at 5 s per minute; `full` is 15 real minutes, and that
is what made the verdict discriminate.

`memory flat over idle window` failed with `idle_rss_growth` = **500,146,176 bytes**. Per node,
resident went 677 MB to 1,178 MB on `ch1` and 641 MB to 1,087 MB on `ch2`. It is not only allocator
noise: ClickHouse's own `mem_tracking` roughly doubled too, 337 MB to 666 MB.

What makes it worth a look is what the pool held while this happened: **nothing**. `pool_shape` is
zero across every class — no blobs, no manifests, no refs, no roots, no files — `leftover_ca_tables`
is empty, `max_s3_ops_per_round` is 0, and GC deleted nothing across 11 successful rounds.

**Not yet established as a leak, and the reason is the window.** Fifteen minutes cannot separate a
server settling into its steady state — thread pools, caches and arenas materializing after boot —
from growth that does not stop. Peak resident (1,158 MB) sits close to final (1,181 MB), which is
weak evidence for a plateau.

**There is no `trace_log` evidence for this, and three separate things must change before a rerun
can produce any.** Asked whether the trace told us where the 500 MB went, the answer is no:

1. **The artifact dump does not collect it.** `predown_dump.sh` loops `for tt in CPU Real` — the
   `Memory` trace type is never queried, so no allocation stacks reach the run directory.
2. **There would be nothing to collect.** `total_memory_profiler_step` — the server-level knob that
   samples background and idle allocations — defaults to **0**, meaning off, and `ca-soak`'s
   `profiling.xml` sets only the two query profilers. The per-query `memory_profiler_step` (4 MiB)
   cannot fire during an idle window because no query is running. This is why the earlier one-hour
   chaos soak did have ~25k `Memory` samples while an idle run would have none: those came from
   queries.
3. **What was collected is not time-scoped — but the capability exists and is simply unused.**
   `predown_dump.sh` already accepts `FROM_TS`/`TO_TS` and folds them into a `${WINDOW}` clause on
   every trace query; the scenario runner just never passes them. So S23's `Real` aggregate spans the
   whole server lifetime, and although it is full of CAS write frames — `commit` 380 samples,
   `publishStaging` 378, `publishBlob` 365, `fanOutBlobUploads` 368, and
   `CityHash128BlobHashingWriteBuffer::nextImpl` 172 — those most plausibly belong to the setup phase
   and cannot be attributed to the idle minutes either way. The pool was empty by the end. Fixing
   this is a matter of the card passing the window it already knows, not of adding a feature.

**Items 1 and 2 are now fixed and verified (2026-08-31).** `predown_dump.sh` dumps `Memory` alongside
`CPU` and `Real`, and `configs/memory.xml` sets `total_memory_profiler_step` to 4 MiB. The setting
had to go in `memory.xml` rather than `profiling.xml`, because the latter is mounted into `users.d`
where a server setting is ignored. Verified on a live cluster: the setting reports `changed = 1`, a
write workload produced **1,064 background** `Memory` samples where the count would previously have
been zero, and the dump wrote 1.27 MB of `Memory` stacks with no error output.

**Experiment that decides it,** with those three fixed first: set `total_memory_profiler_step` to a
few MiB on the server, have the dump query the `Memory` trace type, and scope the aggregate to the
idle window by `event_time`. Then run one idle scenario with a 60-plus-minute window, sampling RSS
and `mem_tracking` each minute, and read the shape of the curve rather than its endpoints. A plateau
means the verdict's threshold is wrong for a freshly booted server; continued linear growth on an
empty pool means a leak, and the `Memory` stacks will say whose. Do not file a leak against the
product on the 15-minute number alone.

**Second probe (measured 2026-08-31, using the `Memory` trace collection enabled the same day).**
Fifteen idle minutes on an empty pool, all tables dropped, the trace aggregate scoped to the window
itself. RSS did NOT accumulate this time: +19 MB drift inside an oscillation of roughly plus or minus
80 MB (1,033 to 1,193 MB). No CAS frame appears in the background allocation top at all. What does
appear is an INSERT — `MergeTreeSink::consume` into `MergeTreeDataWriter::writeTempPart`, 1,705
samples — and `system.part_log` names the destinations: about 1,055 inserts across eight system log
tables in fifteen minutes, `metric_log` alone accounting for 43.98 MiB, `trace_log` for 88,215 rows.
An idle server is busy writing telemetry about itself, and part of that is self-inflicted: the 10 ms
query profiler in `configs/profiling.xml` plus the memory sampling produce the very `trace_log` rows
whose flush then allocates.

**This reframes S23's verdict rather than settling it.** A verdict named "memory flat over idle
window" is, at this profiling configuration, mostly measuring the cost of observing the server —
roughly 176 MB/hour of system-log data before any CAS work exists. Either the threshold accounts for
that churn, or the scenario quiets the profiler for the duration of the idle measurement.

**What the second probe does NOT establish, and the reason is that it is not S23's experiment.** It
began at 1,102 MB, which is where the first probe *ended* (1,178 MB), because a write workload had run
before the tables were dropped. The first probe began at 677 MB on a freshly booted server and climbed.
So the two runs converge on the same plateau by different routes, and the plateau is stable — but that
the climb from 677 MB stops there is plausible, not shown. **Still needed: a FRESH BOOT idled 60+
minutes, RSS sampled every minute, read as a curve.** Until then nothing here may be recorded as a leak,
and nothing may be recorded as warm-up either.

### A single unreachable manifest survives GC to fixpoint, three rounds running {#s10-manifest-residual}

Found at `--scale full` (2026-08-31), same run as the (now-fixed) S10 patch-part premise bug; residual
arises from the lightweight-delete workload, not patch content. `fsck_final`: `unreachable: 1, dangling:
0` against `reachable: 163`, classified `leak` as `unreachable:_manifests: 1` — not `pipeline`, not
`bookkeeping`. `reclaimable_drain_check` agrees it is reclaimable. `gc_fixpoint_history = [1, 1, 1]` —
GC hit fixpoint three times without moving it.

One object is small but the shape is not benign — a reclaimable, unreachable manifest that survives
repeated fixpoints is by definition not being reclaimed. Also worth a glance in the same run:
`graduation_drain_history` is a run of ones with a single **128** in it.

Before calling it a product defect: identify the object and what still points at it, and re-derive
whether the workload can legitimately leave it (a manifest published after the fold's coverage seal is
bookkeeping, not a leak) — not yet done.

### A 500-attempt retry budget converts RustFS's read-concurrency limit into an indefinite stall {#soak-retry-budget-livelock}

**Observed live 2026-08-31 while verifying an S21 fix; the merge sat at progress 0.11 having read
20.67 MiB of 1.10 GiB, and advanced 0 bytes in 40 seconds.**

- RustFS says exactly what is wrong, 3,884 times in twelve minutes:
  `SlowDown: disk read concurrency limit reached, please reduce your request rate`. It is a
  **concurrency** ceiling, not a volume problem — the store answers a direct probe in 1.2 ms while
  refusing the merge's reads.
- The client does not pace itself: `s3_max_get_rps` and `s3_max_get_burst` are both **0**.
- The retry budget is **500** (`s3_retry_attempts`), and the log shows `Attempt 19/501`.

So a persistent 503 becomes neither success nor failure. Every thread keeps retrying, concurrency
therefore never falls back under the ceiling, and the operation cannot finish or give up. 104 threads
were parked in `RetryRequestSleep` at once.

**The failure mode is already named in this repo, for a different trigger.** `configs/storage_conf.xml`
carries a B187 comment describing "the 500x5s retry storm that wedges the merge finalize", caused by
rustfs closing mid-body on a conditional PUT, and mitigates *that* path with
`expect_continue_min_bytes = 65536`. The read path has no equivalent mitigation, so the same wedge
returns through the concurrency limit. Two workarounds interacting: a retry budget raised to survive
one rustfs defect turns a second rustfs behaviour into a livelock.

**Fix, in the order the evidence supports it.** Set `s3_max_get_rps` so the client stays under the
store's ceiling — this is precisely what the error message asks for, and it is a rig configuration
gap, not a product defect. Then reconsider whether 500 attempts is right: a budget that large cannot
distinguish "retry until the transient clears" from "wait forever", and a persistent, self-caused 503
deserves to surface as an error long before attempt 500. Both are wider than S21 — every scenario
doing concurrent reads at scale is exposed.

**The ceiling IS configurable, and raising it fixes the stall.** `RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS`
is the knob — found by extracting `RUSTFS_*` names from the `1.0.0-rc.3` binary, alongside
`RUSTFS_OBJECT_DISK_PERMIT_WAIT_TIMEOUT`, `RUSTFS_OBJECT_DISK_DEGRADED_READ_CAP`,
`RUSTFS_OBJECT_DISK_READ_TIMEOUT` and `RUSTFS_OBJECT_DISK_WRITE_ABSOLUTE_CAP`. The binary also carries
the write-side twin of the message, `foreground write concurrency limit reached`.

Measured, not assumed:

| ceiling | outcome for the same 1.1 GiB seven-part merge |
|---|---|
| default | never completes: 20 MiB read, 0 bytes written across a 40 s window, 3,884 503s in 12 min |
| **256** | **completes**: `system.part_log` shows `MergeParts` at 1,211.7 s with `error = 0` |

`configs/rustfs.env` is set to 256 for that reason. A trial at 1024 was botched — rustfs was restarted
mid-merge, destroying the experiment — so 1024 carries no evidence and is not used.

**Two things this does not settle.** 503s still occur at 256 (579 in three minutes) while the host is
completely idle: load 0.82, iowait 0%, on NVMe. So the ceiling is reached for reasons other than
physical disk saturation, and what those are is unknown. And the client still does not pace itself at
all, so every refusal becomes a long wait rather than an error — `s3_max_get_rps` remains the other
half of the fix.

**A hypothesis raised and then withdrawn, recorded so nobody re-runs it:** a permit leak in rustfs, on
the grounds that progress came in a burst after each restart and then appeared to stop. The merge's
completion refutes it. What actually happened is that the merge was slow and *uneven*, and a single
90-second window that fell in a pause was sampled, concluding throughput was zero. Read a curve, not
one interval.

### RSS growth during a large upload is a fraction of the blob, not a constant {#s01-rss-scales}

**Measured 2026-09-01 across two scales of S01.** At `ci` (512 MiB blob) RSS growth during the upload
was **exactly 0**. At `full` (8 GiB blob) it was **2.228 GiB — 28% of the blob**. Growth tracks the
blob rather than staying flat, so it is not query-pipeline noise.

The verdict passes either way, because its threshold is "growth < blob size" — which would admit 99%
just as happily. It catches full materialization and nothing short of it.

**Where the memory actually goes, from `Memory` trace samples taken over the same run:** 42% in
`SerializationString::deserializeBinaryBulkWithSizeStream` under `MergeTreeReaderWide::readData`, 6%
in `ColumnString::shrinkToFit` under `MergeTreeSequentialSource::generate` inside `MergeTask` — that
is the MERGE reading String columns. Only **7%** falls in the CAS write path (`publishBlob`,
`PartWriteTxn`). So the card's headline verdict, which exists to prove the write path streams,
is dominated by the read side of the merge that builds the part.

**Two things to settle before tightening anything.** Whether the 28% is buffering by design or the
same effect Altinity#2233 reports (RSS growing 0.98 GiB on a 0.50 GiB blob, i.e. ABOVE the blob) —
measuring growth at three blob sizes answers it. And whether the verdict should measure the write path
specifically rather than whole-server RSS, which is a different verdict and needs its own design.

**Caveat on the trace evidence:** 139 samples, with 43% landing in generic thread-pool frames that were
not decomposed. Enough to show where the bulk sits, not enough to apportion precisely.

### 1,200 committed refs repointed outside a transaction during sparse-write GC {#s05-standalone-repoints}

**Found 2026-09-01, S05 at `--scale full`** (10,000 tables, one insert each). Sixteen verdicts, zero
anomalies — the card ran cleanly and caught product behaviour, not a harness fault.

`CASRefRepoint == 0 on the non-transactional path` observed **1,200**. The card's own note: "unexpected
standalone repoint of a committed ref during sparse-write GC — investigate which op took the
`repointRef` path".

It does NOT reproduce at `dev` or `ci`, so whatever takes that path needs either the object count or
the sparse-write shape that only `full` produces.

**First question to answer:** which operation calls `repointRef` outside a transaction. The count is
suspiciously close to a per-table figure for a 10,000-table pool, so start by checking whether it
scales with tables, with parts, or with GC rounds. Related: audit
[F2](/superpowers/reports/otel-demo-cas-s3-budget-audit#f2) (`gc.md`'s `[PART-REMOVAL-REPOINT]`), where
`delete_tmp_*` repoints were measured at ~22% of the writer PUT class — if the same call site is
responsible, these are one finding, not two.

## Later / design questions {#later-design-questions}

### Scalability findings from the full-scale campaign {#scale-findings} — KEEP, trimmed

O(N)-amplification findings for the capacity model / future S3-budget push:

- **[idle-scratch-debris]** MINOR — idle GC leaves local scratch files uncollected (1→21 MiB over an idle
  window with zero inserts).
- **[scratch=full-part]** DESIRABLE — a 100 GiB merge spills 93 GiB to local scratch before upload; a part larger than local free scratch cannot be written. Largely addressed by opt-in S3-native staging, local path still doesn't stream-hash. (An orphaned 2026-08-04-triage finding covers the same cas_scratch spill class, citable across 3 sources — folded in as confirmation.)
- **[replicated double-spill]** DESIRABLE — a replica re-merges and re-spills its own full scratch instead of
  adopting the leader's uploaded blob (186 GiB for one deduped 100 GiB blob). (An orphaned 2026-08-04-triage finding covers the same shared-pool `OPTIMIZE FINAL` re-merge/re-spill class — folded in as confirmation.)
- **[wide-part O(columns)]** DESIRABLE — S07 20000-col `OPTIMIZE FINAL` stalled in an S3 retry storm from
  ephemeral-port exhaustion. Not covered by the 2026-09-25 audit (different workload shape). (An orphaned 2026-08-04-triage finding covers the same S07 20000-column finding verbatim — folded in as confirmation.)
- **[partitioned-INSERT O(partitions)]** DESIRABLE — ~10s per 256-partition insert; related to the postponed
  stage 2, not resolved by its postponement.
- **[S11 capacity]** WATCH — GC doesn't reclaim during the delete phase; same O(N)-GC-lag family as
  `umbrella-roadmap.md` §2 GC stage B, not itself resolved by it.
- **[Capacity model]** DOC/DESIRABLE — needs a validated GC-cadence/snapshot-size estimate at production load
  (live-AWS data point: a round is 30-40s). Audit F29 adds one data point (GC snapshot run growth with
  backlog) but not the requested model.
- **[physical-footprint amplification]** VERIFY, still needs re-derivation (flagged 2026-08-31): the 400×
  `pool_bytes`/`logical_bytes` figure is blamed on `rustfs#3231` (overwrite-version retention) and is
  rustfs-specific and tiny-object-specific — a directory plus an `xl.meta`, roughly an 8 KB floor for an
  800-byte object, is the whole story there. Not a safety issue (dangling=0). It does not generalise: S01
  full-scale measured ~215 GB of logical content into a 308.8 GB rustfs volume, ~1.4× amplification, on the
  same backend; check the pin (`rustfs:1.0.0-rc.3`) against the upstream fix before re-citing either figure.
  The 2026-09-25 audit uses a real S3 bucket, not rustfs — neither confirms nor refutes this.

Formerly here, now deleted — **[startup O(refs)]** ("~152k S3 ops to start a 10k-table server"):
**SUPERSEDED-BY** audit [F15](/superpowers/reports/otel-demo-cas-s3-budget-audit#f15)/[F16](/superpowers/reports/otel-demo-cas-s3-budget-audit#f16)/[F21](/superpowers/reports/otel-demo-cas-s3-budget-audit#f21),
which root-cause it to one call chain (`existsDirectory` → `classifyDirectory` fallthrough → one LIST per
part file, reproduced live: 77k LISTs in 3 minutes, `503 Slow Down`) with a concrete fix and open issue #2439.

### New findings from the 2026-08-04 orphaned-open triage {#orphan-triage-2026-08-04} — KEEP, trimmed

- **[putblob-uncertainty-exhaustion-abort]** DESIRABLE — sustained ambiguity can still exhaust
  `ensureBlobPresent`'s eight-observation loop; verify the combined wall-time bound. Not landed on either
  branch.
- **[manifest-trust-promote-path]** — **DONE.** `TrustedManifest` leaves now skip per-leaf `HEAD`/`loadMeta`
  entirely, trusting the durable manifest edge (matches the relink trust model). Evidence: `8fe6331a4311` +
  `d910ea10339a` (cas-gc-rebuild only, not on antalya-26.6); `Pool/CasPartWriteTxn.cpp:927`
  (`PartWriteTxn::promote`).
- **[cas-commit-pool-anti-deadlock]** — merged into `{#stage2-concurrent-commitpart-postponed}` above.
- **[hot-part-blob-trickle-warmer]** DESIRABLE — speculative age-based trickle warmer ahead of snapshot;
  concrete driver, not built.
- **[ca-trycommit-retry-loses-staged-state] (B82)** DESIRABLE — a `tryCommit` retry can drop staged
  `writeFile`/`createHardLink` state because the reset `metadata_transaction` has no `operations_to_execute`
  entry to refill in-memory staging maps. Needs a fresh reproduction against current HEAD before filing
  formally (naming has since drifted; not directly searchable on antalya-26.6's older code shape).
- **[cache-get-head-token-mismatch]** — moved to `{#cache-get-head-token-mismatch}` under "Read path and caches".
