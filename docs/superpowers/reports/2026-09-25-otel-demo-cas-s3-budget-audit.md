---
description: 'S3 request budget audit of the otel.demo CAS stand (2026-09-25): per-verb and per-object-kind daily volumes, attribution to writer, merges and GC phases, and twelve ranked findings on repeated or unnecessary work.'
sidebar_label: 'otel.demo CAS S3 budget audit'
sidebar_position: 100
slug: /superpowers/reports/otel-demo-cas-s3-budget-audit
title: 'otel.demo CAS S3 budget audit'
doc_type: 'reference'
---

# otel.demo CAS S3 budget audit, 2026-09-25, replica chi-otel-otel-0-0 only {#otel-demo-cas-s3-budget-audit}

Sources: `system.metric_log` (S3 verbs per day), `system.blob_storage_log` 2026-09-23 (writes and deletes by key kind),
`system.events` since the 2026-09-24 16:41 restart (CAS semantic counters per hour), `system.part_log` and
`system.query_log` 2026-09-23 (attribution), `system.cas_gc_log` phase rows (GC per phase and verb), code reads cited inline.

## 1. The workload {#workload}

| | per day (09-23) |
|---|---|
| `NewPart` | 142,770, of which `system.*` log tables 122,680 (86%), user db `claude_otel` 20,090 with 3.3 rows per part |
| `MergeParts` | 53,927 |
| `RemovePart` | 196,310 |
| `AsyncInsertFlush` queries | 6,781, 887 ms avg, of which 748 ms waiting for the ref lane |
| `ref_repoint` events | 190,853, 100% on `delete_tmp_*` refs |
| ref-lane flushes | ~19k/h, 3 mutations per flush, queue wait 10-12k s per hour (lane ~100% busy) |

## 2. S3 requests per day, 09-23, this host {#requests-per-day}

| verb | per day | $/day (AWS list price) | split |
|---|---|---|---|
| PUT | 2.30M | 11.5 | manifest 562k, blob `.meta` 511k, ref `_ckpt` 477k, ref `_log` 474k, blob 184k, gc 10k |
| GET | 6.39M | 2.6 | manifest ~2.9M (GC fold ~1.85M, part-folder views ~0.5M, part publish ~0.5M), blob/meta ~1.5M (GC graduation re-check and meta reads), ref objects ~1.3M (GC log bodies ~0.85M, writer ~0.5M), other ~0.7M |
| HEAD | 657k | 0.3 | writer HEAD-before-PUT ~180k (the 404s: `S3ReadRequestsErrors` == `CASBlobHeadMiss` exactly), GC condemn HEAD ~250k, GC pending_deletes HEAD (5k per round then, 200k per round now) |
| LIST | 384k | 1.9 | GC ref-prefix LIST (14.4k requests per round at 4.6M keys), `CASRootList` baseline 155 per 10 min (22k/day) from `listMounts`/`serverRootsPrefix` walks and `rootsPrefix` directory listings |
| DELETE keys | 1.4M | 0 | manifests 1.06M (both replicas' manifests, leader GC deletes both), blobs 135k, meta 135k, `_log` 135k |

Total ≈ $16/day for 43 MB/day of user data. Per part publish the pool pays ~16 PUTs including GC's share.

## 2a. Write path versus GC, 09-23, this host {#writer-vs-gc}

Attribution: GC = fold manifest and log body GETs (`CASRefManifestBodyFoldGets`, `CASRefLogBodyGets`), graduation
meta re-checks (bounded by `entries_graduated`), condemn HEADs (`candidates_marked`, equal to `CASBlobHead`),
`pending_deletes` HEADs (`entries_redeleted`), condemn-marker PUTs (`CASMetaCompareSwap`), `gc/` objects, the
ref-prefix LIST (`S3ListObjects` minus the 22k/day `listMounts` baseline), every DELETE request. Write path = the rest:
part publish and merges (manifest, ref, checkpoint and catalog reads, blob and meta PUTs, HEAD-before-PUT), part-folder
views, data reads through the cache disk. 28 GC rounds that day, count caps at 5000, rounds ~50 min.

| verb | total | write path | GC | GC share | $ write | $ GC |
|---|---|---|---|---|---|---|
| GET | 6.39M | 3.2M | 3.2M (fold manifests 2.12M, fold logs 0.96M, graduation ≤0.14M) | 50% | 1.28 | 1.28 |
| HEAD | 657k | 194k | 463k (condemn 327k, pending 135k) | 70% | 0.08 | 0.18 |
| PUT | 2.30M | 1.96M | 337k (condemn markers 327k, `gc/` 10k) | 15% | 9.8 | 1.7 |
| LIST | 384k | 22k | 362k | 94% | 0.11 | 1.8 |
| DELETE requests | 271k | 0 | 271k (blobs 135k, meta 135k, ~1.4k batches of manifests and logs) | 100% | 0 | 0 |
| **requests** | **10.0M** | **5.4M** | **4.6M** | **46%** | **11.3** | **5.0** |

Per part written (197k publishes and merges): the write path spends ~27 requests, GC ~23. Per garbage blob (327k
condemned that day): GC spends ~14 requests, of which ~9 are its share of intake (manifest and log bodies) and 5-6
the blob's own lifecycle (F7). In dollars GC is 31%; in requests 46%; in wall-clock it is one thread at 96% duty plus
16 workers, while the write path's ref lane is at 100% (10-12k queue-seconds per hour, 748 ms of an 887 ms insert).

What moves the ratio: the spec's A4 halves GC GETs (manifest memo), B removes the LIST (GC $ from 5.0 to ~2.2), C
does not change volume; on the write side F2 (repoint elision) and F5 (lazy `_ckpt`) together remove ~30% of PUTs,
which is where three quarters of the dollars are. The condemn-marker PUT per garbage blob (327k/day, 15% of PUTs) is
the one GC PUT and is protocol.

## 2b. GC cost per phase {#gc-cost-per-phase}

Phase rows carry only the round thread's ProfileEvents (`[gc-phase-rows-lose-worker-requests]`); the worker side is
filled in from the day's semantic counters: read-ahead GETs (`CASRefLogBodyGets`, `CASRefManifestBodyFoldGets`,
`CASGCReadAheadHit`), condemn HEADs (`candidates_marked`), condemn-marker PUTs (`CASMetaCompareSwap`), meta deletes
(`entries_redeleted`), and the LIST, which the S3 iterator issues off-thread (`S3ListObjects` minus the 22k/day
baseline). Prices: PUT and LIST $0.005 per 1000, GET and HEAD $0.0004 per 1000, DELETE free.

Healthy day, 09-15: 360 rounds, 778k blobs condemned, 964k `_log` keys and 1.07M manifests deleted.

| phase | wall s | share | requests | per unit | $ |
|---|---|---|---|---|---|
| `pending_deletes` | 33,358 | 52% | 765k HEAD + 765k DELETE + 765k meta DELETE | 44 ms per blob, serial | 0.31 |
| `fold_ref_intake` | 19,326 | 30% | 3.18M GET (0.97M logs, 2.14M manifest bodies) | 20 ms per log, 3.3 GET per log | 1.27 |
| `fold_reduce` | 5,363 | 8% | 778k HEAD (read-ahead) + 779k marker PUT + 35k GET | 6.9 ms per condemned blob | 4.21 |
| `ref_object_cleanup` | 4,095 | 6% | 3.7k batch DELETE + 40k GET (2 revalidation reads per chunk) | ~1 ms per key | 0.02 |
| `manifest_deletes` | 1,079 | 2% | 1.25k batch DELETE | ~1 ms per key | 0 |
| `defer_decision` | 1,057 | 2% | 34k LIST | 94 pages per round | 0.17 |
| other 12 phases | 340 | <1% | ~6k GET | | 0 |
| **round total** | **64,580** | 75% duty | **~9.5M** | | **6.0** |

Degraded day, 09-23: 28 rounds, 327k condemned, caps at 5000, 4.3M keys in the LIST.

| phase | wall s | share | requests | per unit | $ |
|---|---|---|---|---|---|
| `defer_decision` | 31,868 | 42% | 362k LIST | 13.4k requests per round, 88 ms each | 1.81 |
| `fold_ref_intake` | 27,736 | 37% | 3.08M GET (0.96M logs, 2.12M manifests) | 29 ms per log | 1.23 |
| `fold_reduce` | 8,800 | 12% | 327k HEAD + 327k marker PUT + 285k graduation GET | 27 ms per condemned; ~7,100 s of it is the inline graduation GET (F3) | 1.89 |
| `pending_deletes` | 6,226 | 8% | 135k HEAD + 135k DELETE + 135k meta DELETE | 46 ms per blob | 0.05 |
| `manifest_deletes` | 1,008 | 1% | 1.08k batch DELETE (1.06M keys) | | 0 |
| `ref_object_cleanup` | 226 | <1% | 227 batch DELETE (135k keys, capped) + 824 GET | | 0 |
| **round total** | **75,909** | 88% duty | **~4.6M** | | **5.0** |

Reading: time and money sit in different phases. Money is the condemn-marker PUT (70% of GC dollars on the healthy
day, protocol) and the intake GETs; time is the serial `pending_deletes` on a healthy day and the LIST plus the
inline graduation GET on a degraded one. The write-once batch families (`ref_object_cleanup`, `manifest_deletes`) are
free in both. Per garbage blob on the healthy day GC spends ~9 requests: 3.3 GET of intake, 1 HEAD + 1 PUT to
condemn, 1 HEAD + 1 DELETE + 1 meta DELETE to reclaim.

## 3. Findings, ranked by what they save {#findings}

Labels: CONFIRMED = read in code and matched by counters; PLAUSIBLE = counters fit, code path not fully read.

### F1. CONFIRMED, deliberate on this stand. System log tables on the CAS disk are 86% of all parts {#f1}
`system.blob_storage_log`, `part_log`, `cas_log`, `trace_log`, `metric_log`, ... each ~11k parts/day. Every flush is a
manifest, a ref-log append, a `_ckpt` overwrite, blob PUTs with HEAD-before-PUT, then two removals (merge source, then
`delete_tmp` repoint), then GC work on all of it. `cas_log` logging CAS events onto CAS is an amplifier.
On otel.demo this is deliberate: the stand exists to push CAS and GC with a tiny-part churn workload, so the logs stay
where they are. The finding is a product note, not a stand action: a production deployment that uses CAS as the default
disk inherits this churn for free, so the documentation should recommend a local `<storage_policy>` for `system.*` log
tables (the backlog already recommends it for CI lanes), and the stand's numbers are the argument.

### F2. CONFIRMED. `delete_tmp` repoint on every part removal (backlog `[PART-REMOVAL-REPOINT]`) {#f2}
190,853 repoints/day, all on `delete_tmp_*`. Each costs: 1 manifest PUT (0 entries, ~6.5 KB), 1 ref-log append
(shares a flush), 1 `_ckpt` write, then in GC: 2 manifest body GETs (-1 for the old owner, +1 for the new), 1 more
manifest delete, 1 more log record to intake. Per day ≈ 190k PUT + 380k GET + 190k DELETE keys and ~27% of the ref
lane's mutations. The rename-to-`delete_tmp` is followed by removal of the same ref within seconds.
Saves: ~8% PUT, ~6% GET, a quarter of ref-lane load, a third of GC intake work. Action: elide the repoint when directory
removal follows (the same-transaction supersede-clear already does this for unlink+rmdir in one txn).

### F3. CONFIRMED. Carried condemned rows never persist `marker_confirmed` {#f3}
`settleEntry` (`CasBlobInDegree.cpp:485-513`): when `confirm_condemned_marker(e)` succeeds but the graduation budget
is exhausted, the entry is carried as `e` unchanged, with `marker_confirmed = false`. The in-process memo hides the
cost until a restart or leadership change; then every carried entry pays one synchronous `loadMeta` GET again. On
otel.demo: 200k GETs, 4 hours, in round 1379. One-line fix: set `e.marker_confirmed = true` on the carried copy once
confirmed (the run is rewritten anyway). Stage A's read-ahead then only covers rows condemned in the last round.
Saves: the whole post-restart GET storm; per garbage blob one request of six.

### F4. CONFIRMED. One manifest body GET per emitted edge, no reuse within a round {#f4}
`CASRefManifestBodyFoldGets == CASRefEmittedEdges` (1,346,534 since restart). Per part lifetime the fold reads
manifest A on publish (+1), A again on repoint (-1), B on repoint (+1), B again on drop (-1): four GETs of two bodies.
With five-hour rounds, publish, repoint and drop of most system-log parts fall into ONE round. A per-round cache keyed
by `ManifestId` (bodies are immutable, write-once keys) halves this; with F2 it quarters. ~1.85M GET/day on this host,
29% of all GETs, and the dominant term of `fold_ref_intake` (6000-6800 s per round).
Action: memoize decoded edge lists per `ManifestId` for the round inside `foldManifestEdges`, bounded by memory
(edge count), spill to re-read on cap.

### F5. CONFIRMED. `_ckpt` overwritten on every ref-lane flush {#f5}
`CASRefCheckpointPublished == CASRefBatchFlushes` (18.7k/h each). 477k PUT/day, 21% of PUTs, and one of the two PUTs
on the lane's critical path per flush. The checkpoint names the recovery triple; a lazier checkpoint (every N flushes
or every T seconds, plus on snapshot publish) only lengthens the recovery walk by at most N logs.
Saves: up to 20% of PUTs and roughly 40% of per-flush lane latency (the lane is the insert bottleneck: 748 ms of an
887 ms insert is queue wait). Protocol semantics question for the owner: recovery cost bound N.

### F6. PLAUSIBLE. Three S3 LIST requests per 1000-key logical page {#f6}
`CASRefGlobalListPages` = 1543 per round (1000 keys each) but `S3ListObjects` during `defer_decision` = 14.4k per round,
i.e. ~320 keys per S3 request. `listUnder` asks the store for `limit+1` on the first page and the storage default on
resumed pages (`CasObjectStorageBackend.cpp:1038-1108`, `object_storage->iterate(..., max_keys=store_page, ...)`);
the S3 iterator's own page size (`list_object_keys_size` / async prefetch) decides the rest. If confirmed, the current
LIST costs 3x in requests and latency (1530 s per round). Stage B removes the global LIST, but per-life LISTs and the
janitor use the same path. Action: verify `max_keys` propagation into `S3ObjectStorage::iterate`; one request per
1000 keys is the target.

### F7. CONFIRMED. Six GC requests per garbage blob {#f7}
condemn HEAD (read-ahead), condemn marker PUT to `.meta`, graduation `loadMeta` GET (F3 removes it for carried rows),
`pending_deletes` HEAD, conditional DELETE, `.meta` DELETE. The minimum the protocol needs is: marker PUT, exact-token
DELETE, meta DELETE. Two candidates, both protocol-step changes for the owner: (a) conditional DELETE with `If-Match`
instead of HEAD + DELETE (412 vs 404 gives the same `Mismatch`/`Gone` classification; AWS supports it, capability
must be probed per store); (b) batch the `.meta` deletes (they are unconditional after the blob delete succeeded)
through the existing 1000-key batch path. DELETE is free on AWS, so (b) saves requests and thread time, not dollars.

### F8. PLAUSIBLE. Two manifest PUTs per part publish {#f8}
`CASManifestPut` 592k/day against 197k parts written here + 191k repoints = 388k. The remaining ~200k/day are not
attributed; candidates are a staged body plus a promoted body per publish, or `tmp_` parts that never commit.
Action: attribute before acting (`cas_log` `manifest_put` events per `ref_name` pattern).

### F9. CONFIRMED. Per part publish: 5 GETs; per merge: 10.7 GETs, 2.9 part-folder view misses {#f9}
`NewPart`: 1 manifest GET, 2 ref-object GETs, 2 "other" GETs (catalog / checkpoint reads), 2 part-folder view
invalidations. `MergeParts`: 3.8 manifest GETs, 2.9 folder-view rebuilds. The folder view is invalidated on every
write to the table, so on a table with 11k inserts/day every merge rebuilds it. ~1.3M GET/day. Action: keep the view
across writes that only add refs (invalidate per ref, not per namespace); measure first.

### F10. CONFIRMED, not a defect. `S3ReadRequestsErrors` 7.8k/h are the writer's HEAD-before-PUT misses {#f10}
Exactly equal to `CASBlobHeadMiss` in every 10-minute bucket. Expected protocol cost; not GC.

### F11. CONFIRMED, not a defect. Manifest and `_log` delete counts are 2x this host's uploads {#f11}
The leader GC deletes both replicas' manifests (`manifests/<server_root_id>/...`) and both replicas' ref logs. No
repeated deletes; the `deleted_or_absent` outcome cannot distinguish, and does not need to.

### F12. CONFIRMED. `CASRootList` 155 per 10 minutes at all times {#f12}
22k LIST/day outside GC from `listMounts` / server-roots walks (`CasServerRoot.cpp:1036, 1140, 1186`) and `rootsPrefix`
directory listings (`CasPool.cpp:1926`). $0.11/day; only worth a look if `cas_mounts` is polled by a dashboard.

### F13. CONFIRMED. `cas_mounts.pending_reclaim` goes negative after a restart {#f13}
Observed `-388242` on 2026-09-25 13:37: the counter is condemned minus executed deletes for the current process
(`CasGcScheduler.h`), and after a restart the process deletes a backlog it never condemned. Spec C5 replaces it with
the seal's `CondemnedSummary`; until then the column is not a backlog measure.

### F14. CONFIRMED. With `cas_gc_round_ref_cleanup_budget = 200000` the `_log` population still grows
Listed keys 4,576,640 (08:32) then 4,584,514 (13:29) while three rounds deleted 600k keys: both replicas append
(~440k/day each on this stand), so ~880k new keys/day against 600k deleted at three rounds per day. The budget must
be 0 for this family; the cleanup is batch deletes (200k keys in 200 s), so an unbounded pass over 4.6M keys is
about 75 minutes once, not a risk.

### F15. CONFIRMED. Every restart issues one S3 LIST per part file: 77k LISTs in three minutes, S3 answers `503 Slow Down` {#f15}
Restart of 2026-09-25 15:38 (and the one of 09-24 16:41, same 61k-LIST burst in `metric_log`): `S3ListObjects`
19k + 40k + 18k in 15:40-15:42, all `CASRootList`; 537 `503 Slow Down` / `Service Unavailable` lines, 139 failed
uploads in `blob_storage_log` (108 of them ref-lane writes), `Ready for connections` 2 min 11 s after start.
Call chain from `trace_log` (111 of 116 sampled LIST stacks): `IMergeTreeDataPart::loadColumnsChecksumsIndexes` →
`checkConsistency` → `MergeTreeDataPartChecksum::checkSize` → `existsDirectory(<part>/<file>)` (upstream asks this
for every checksum entry to skip projection directories) → `ContentAddressedMetadataStorage::existsDirectory` →
`classifyDirectory` falls through to `TableSubdir` (a part-file path that is not a projection dir is not returned by
the part branch, so `parseTableFilePath` matches on the uuid) → `CasPlainObjects::listNamespaceFiles` → one LIST of
the namespace's verbatim-files prefix. 1,672 parts × ~40 files ≈ 67k LISTs, plus outdated parts loaded by
`AsyncLoader`. Fix: give `classifyDirectory` a `PartFile` shape for `<table>/<part>/<file>` and answer
`existsDirectory` from the part-folder view (`view->hasDirectory(file)`, the same call `ProjectionDir` uses); no LIST.
A LIST-heavy startup also delays `Ready for connections` and, at 10k tables, would throttle the whole pool.

### F16. CONFIRMED. The constant 155 LISTs per 10 minutes are `clearOldTemporaryDirectories` listing table-level files {#f16}
`MergeTreeData::clearOldTemporaryDirectories` → `iterateDirectory(table dir)` → `listDirectory` (`TableDir` shape),
once per table per minute: 26 tables ≈ 156 per 10 min, matching the baseline exactly. $0.11/day here; at 10k tables
it is 60k LISTs per hour.

Why a LIST at all: the subdirectories (part names, `detached`) do come from the ref table, `listRefs(ns)`, in memory
with no request. The LIST is for the other half of the answer, the table-level verbatim files (`format_version.txt`,
`mutation_*.txt`, `deduplication_logs/...`, TTL and similar). They are plain objects under `roots/<ns>/files/<name>`
written by `putNamespaceFile`, with no index anywhere, not in manifests and not in `_ckpt`, so `listNamespaceFiles`
is a LIST of that prefix and `listDirectory` must merge it with the refs on every call. The same call is what F15's
`TableSubdir` fall-through reaches once per part file.

Fix within the current layout: the namespace includes `server_root_id` (`liveNamespace` =
`serverPrefix() + mirroredArchiveNamespace(uuid)`), so a table's verbatim files on this node are written only by this
node. Keep the file-name set in memory per namespace life: one LIST on first use after start, then write-through
invalidation in `putNamespaceFile` / `removeNamespaceFile`. That removes the baseline LISTs and most of F15's startup
burst; F15 still needs the `PartFile` shape so a question about a file inside a part never reaches the table-file
listing. The proper fix, an index of table-level files next to the refs so a cold start needs no LIST either, is a
format change and stays out of scope by the owner's decision.

### F17. CONFIRMED. The restart was an immediate termination: two `SIGTERM` 0.4 ms apart {#f17}
Pod log: `Received termination signal (Terminated)` and `Received second termination signal (Terminated). Immediately
terminate.` at 15:38:27.595 and 15:38:27.596. Consequences: no shutdown drain (the ref lane's in-flight `_ckpt` write
was cancelled, `WriteBufferFromS3 was canceled`), `StatusFile ... unclean restart`, the CAS mount lease looked stale
so the new process observed the predecessor's write-token for 36.5 s before reclaiming (15:38:31 → 15:39:11), and the
mount opened as "predecessor whose death was not proven clean" with a recovery seal. The GC round in progress
(1383: 2 h 38 min of intake and reduce) was lost, which is inherent to the one-pass round and is what spec C3 bounds.
Who sends the second signal is not visible from the server: check the pod's `preStop` hook and whether the operator's
restart path signals the process group as well as PID 1. The `Listen ... Address already in use` warnings at 15:38:10
and 15:40:39 are the usual dual-stack artifact (`::` and `0.0.0.0` both configured), not related.

## 4. What the GC stages in the spec fix, and what they do not {#spec-coverage}

- Stage A (parallelism) removes hours from graduation and redelete; F3 is a one-line addition that removes the
  post-restart GET storm entirely and should go first.
- Stage B (per-life discovery, cleanup listing its own range) removes the O(keys) LIST; F6 matters for whatever LIST
  remains.
- Stage C (deadline) bounds the round; it does not reduce request volume.
- F1, F2, F4, F5 are request-volume levers outside the spec: F1 is configuration, F2 and F4 are writer/fold changes
  with no protocol impact, F5 and F7 are protocol-step questions for the owner.

## 5. Conclusions and proposals {#conclusions}

The pool is correct and expensive where the protocol says so, and slow where nothing says so; the slowness feeds
itself. Ranked by what the number says about the system, not by its size.

| # | number | what it means | proposal | where it lives |
|---|---|---|---|---|
| 1 | 748 ms of an 887 ms insert is ref-lane queue wait; lane at 100% | the write protocol, not GC, is the user-visible ceiling: one `_log` plus one `_ckpt` PUT per flush, a repoint per part removal | F2 repoint elision (writer task, schedule next to stage A); F5 lazy `_ckpt` (owner decision on recovery bound N) | outside the GC spec |
| 2 | 880k new `_log` keys/day vs 600k deleted under a 200k cap; cleanup costs ~1 ms per key and $0 | budgets were set by phase name, not by unit cost; the cap on the cheapest family caused the loop | stand: `cas_gc_round_ref_cleanup_budget = 0` now; product: spec C2 removes the cap; rule: every budget carries its unit cost next to it | stand config + spec C2 |
| 3 | `pending_deletes` 44 ms per blob, serial, 52% of a healthy day | three requests for one DELETE, no parallelism; the "healthy" state was already at the edge | spec A3 (fan-out, PR #2351); F7 `If-Match` DELETE and batched meta deletes (owner decision) | spec A3, open question |
| 4 | 7,100 of 8,800 s of `fold_reduce` are an inline GET that a persisted flag would remove | `marker_confirmed` exists in the run format but is never set on carry; in-process memory hid a durable-state gap | spec A0, one line | spec A0 |
| 5 | 3.3 GET per log in intake, 2.2 of them manifest bodies read up to four times per part lifetime; 29% of all GETs | structural and self-reinforcing: the longer the round, the more publish/repoint/drop of one part share a round | spec A4 (per-round body memo); F2 removes half the reads at the source | spec A4 + writer task |
| 6 | 88 ms per LIST request, ~3 requests per 1000-key page | if confirmed, every LIST in the system is 3x, including the janitor and stage B's per-life lists | verify `max_keys` in `S3ObjectStorage::iterate` before B | spec verification item |
| 7 | 77k LISTs and `503 Slow Down` on every restart | one S3 LIST per part file at load, a classifier fall-through (F15); the same path is the constant 155 LIST / 10 min (F16) | `PartFile` shape answered from the part-folder view; cache namespace files | writer/disk task, outside the GC spec |
| 8 | `pending_reclaim = -388,242` | the only backlog column an operator has is process-local and goes negative after a restart | spec C5 (from the seal's `CondemnedSummary`) | spec C5 |

Not a concern: $16/day, 70% of GC dollars in condemn-marker PUTs, HEAD-before-PUT misses. Protocol by design, and it
held: `dangling = 0`, invariants clean, no loss in a week of deliberate churn.

Order of execution proposed: (2) stand config today; (4) A0 and (5) A4 as the first two stage-A tasks; (3) A3 with
PR #2351; F2 as a writer task in parallel with stage A; then B, C; F5 and F7 after the owner decides.

## 6. Verification items {#verification-items}

- F6: confirm `max_keys` handling in `S3ObjectStorage::iterate` and the disk's `list_object_keys_size`.
- F8: attribute the ~200k/day unexplained manifest PUTs.
- F4: measure the within-round duplicate ratio directly (count distinct `ManifestId` per round in the fold) before
  sizing the cache.
- Replica chi-otel-otel-0-1 was not queried (no port); its fetch/relink budget is unmeasured.
