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
  antalya-26.6). Emulated materialization remains tracked separately under
  `[emulated-resurrect-should-spill-to-disk]`.
- **[R1/X1] ephemeral reader pin** — KEEP, design-only/VERIFY. Cross-node GC fence for a ref-less reader;
  audit whether such a reader path exists at all before building it.
- **[ch128ctx] slot-bound blob-hash middle tier** — KEEP, small spec. `cityHash128(content) ∥
  xxh3_64(part_name, file_name) ∥ size` (256-bit; variable-width `BlobDigest` already supports it) closes the
  cross-slot dedup-collision vector at ~zero cost. Every load-bearing dedup survives: relink/carry-forward are
  reference-based; retry idempotency, same-name replica writes, and snapshot-upload→TTL-move prepayment are
  same-slot; only cross-slot content coincidence is lost (an explicit non-goal, `01 §what-it-does-not-buy`).
  Main touch: the staged-blob hasher/request construction needs `(part_name, file_name)` context before
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

One former item here is resolved elsewhere, not by this file: "repoint waste on part removal" is
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
Related, already tracked: `BACKLOG.md`'s `{#issue-2244-lease-retry-asymmetry}` (the lease/remount ops have
the OPPOSITE problem — no retries at all) and `[timeout-retry RFC residuals]` in `BACKLOG/ref-protocol.md`.

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

## S3 request budget {#s3-request-budget}

### Pool-wide catalog write hot spot {#ref-catalog-write-hotspot} — KEEP

Every table creation writes the same `cas/ref_catalog` object, serializing a table-creation-heavy lane through
one CAS loop (137/250 S3 timeouts on the CA-s3 lane named this key). Design isn't wrong (catalog exists
because pool `LIST` is unreliable) but the cost wasn't exposed before. Open: creation-only or also
read-mints; retry deadline vs. contention; sharding without losing single-object GC-snapshot atomicity.
Write-time counterpart of `{#ref-catalog-read-per-commit}` (audit F20 measures the read side); tracked live
as `umbrella-roadmap.md` §2 "Catalog write hotspot" (hot-key lane phase B, issue #2343) — not superseded, the
roadmap points back here.

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

### Suffix allowlist buffers big index files whole in memory (2031-triage CAS-014) {#part-file-suffix-allowlist-memory} — KEEP

`partFileMustStayBlob` doesn't know the shipped default names (`primary.cidx`, `.cmrk4`, skip-index files),
so those buffer whole into memory via `CaInlineWriteBuffer` before the 1 MiB spill cap applies — no
correctness/bloat defect, just avoidable memory + a double write for wide indexes. Unchanged since
`c623713479f`; confirmed still missing on antalya-26.6. Fix: add the default names to the allowlist; log when
an unknown extension takes the buffered path. Cross-referenced by `BACKLOG/formats-and-storage.md` and
`umbrella-roadmap.md` §3.

Details: docs/superpowers/cas/2031-triage.md#cas-014

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
