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

## 3. Findings, ranked by what they save {#findings}

Labels: CONFIRMED = read in code and matched by counters; PLAUSIBLE = counters fit, code path not fully read.

### F1. CONFIRMED. System log tables on the CAS disk are 86% of all parts {#f1}
`system.blob_storage_log`, `part_log`, `cas_log`, `trace_log`, `metric_log`, ... each ~11k parts/day. Every flush is a
manifest, a ref-log append, a `_ckpt` overwrite, blob PUTs with HEAD-before-PUT, then two removals (merge source, then
`delete_tmp` repoint), then GC work on all of it. `cas_log` logging CAS events onto CAS is an amplifier.
Saves: ~85% of parts, ref mutations, manifests and GC work on this stand. Action: `<storage_policy>` for system logs on a
local disk (the backlog already recommends it for CI lanes). Operational, no code.

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

## 4. What the GC stages in the spec fix, and what they do not {#spec-coverage}

- Stage A (parallelism) removes hours from graduation and redelete; F3 is a one-line addition that removes the
  post-restart GET storm entirely and should go first.
- Stage B (per-life discovery, cleanup listing its own range) removes the O(keys) LIST; F6 matters for whatever LIST
  remains.
- Stage C (deadline) bounds the round; it does not reduce request volume.
- F1, F2, F4, F5 are request-volume levers outside the spec: F1 is configuration, F2 and F4 are writer/fold changes
  with no protocol impact, F5 and F7 are protocol-step questions for the owner.

## 5. Verification items {#verification-items}

- F6: confirm `max_keys` handling in `S3ObjectStorage::iterate` and the disk's `list_object_keys_size`.
- F8: attribute the ~200k/day unexplained manifest PUTs.
- F4: measure the within-round duplicate ratio directly (count distinct `ManifestId` per round in the fold) before
  sizing the cache.
- Replica chi-otel-otel-0-1 was not queried (no port); its fetch/relink budget is unmeasured.
