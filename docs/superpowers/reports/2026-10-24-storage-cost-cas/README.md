---
description: 'CAS research (R4) of the storage cost calculator: request graphs of a part write by entry branch, table drop, GC rounds and reclamation stages, heartbeat, mount lease, local staging, cold reads and server start on the 26.8 binary, with source-cited rules, predictions and reconciliation.'
sidebar_label: 'Storage cost CAS (R4)'
sidebar_position: 56
slug: /superpowers/reports/storage-cost-cas
title: 'Storage cost calculator — CAS graphs, GC and lease (R4)'
doc_type: 'reference'
---

# Storage cost calculator — CAS graphs, GC and lease (R4) {#cas}

Every CAS request graph below reconciles on the `cas` policy of the pinned binary. The binary
is built from `09d0772a842c5d73cf3ef35b4c8b017d838215a0`: `antalya-26.8` merged into the CAS disk-transactions branch.
Every `path:line` cite is at this commit. Deterministic cases match the exact integer per operation for caller
`r1`; GC terms are fitted with spans and checked on distinct hold-outs; the lease rate is source-pinned and its fresh
validation case passes with the idle-table cleanup listing it predicts. Cold reads and server starts reconcile
exactly on `cas` and `cas_cached` against a per-stream and per-part rule, each with a hold-out. Results:
§[Cases and reconciliation](#reconciliation).

- **Report run:** `cost-20261024T201554Z` at rig commit `80e7b064858`: every cited case (108 passes, all ok),
  alone on the rig under `tmp/yardstick.lock`. Failed records kept for consumed edges: the first lease passes
  (`cost-20261024T082909Z`) and the consumed hold-outs (runs `cost-20261024T074131Z`, `cost-20261024T075656Z`; count
  verdicts, unaffected by other prefixes). 122 records. Source commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`.
- **Rig drift during the run:** `utils/cas-yardstick/server.sh` was replaced at 20:31:13Z by the version of
  `eda204f6472` (stop by pidfile, process-name check before a kill). Pass 60 of 108
  (`gc-lifecycle/drop-60s-12/k3`) and later started with it; pass 59 (`drop-60s-12/k2`) stopped its server with it.
  Only the last-resort SIGKILL path differs, so no measured request path changed. The records state
  `rig_commit` `80e7b064858` and lack `store_version`, `store_sha256` and `rig_dirty`: the run started before
  `9cc0121c86d` added them. The store process ran
  through the whole run on the pinned RustFS 1.0.0-beta.9 binary, sha256 `bf4aca66af21…`.
- **Binary:** `tmp/bin/26.8/clickhouse`, sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668`.
- **Cards:** `utils/cas-yardstick/yardstick/cost/cards/cas.py` (`CasBranches`, `CasDrop`, `GcLifecycle`, `GcIdle`,
  `GcScale`, `Lease`, `GcUnderLoad`) and `cards/ops.py` (`ColdSelect`, `Startup`, on `cas` and `cas_cached`);
  manifests in `experiments/`, predictions in `predictions/`.
- **Cut to meet the campaign deadline** (D14 in [LEDGER.md](../2026-10-23-storage-cost-calculator/LEDGER.md#decisions)): `gc-scale.mounts-2.10k`, `gc-scale.refgrowth-low.10k`,
  `gc-scale.refgrowth-high.10k`. The `mounts` and `ref_growth` terms of `gc_round_requests` are unsupported.
- **Cut (D14):** `gc-under-load.default` (manifest and prediction only; coverage `status: cut`).

## Request origin {#origin}

A request is maintenance when it starts inside one of the caller's own GC rounds (`system.cas_gc_log` Start..Finish
of one `round_id`) or touches a control family flagged maintenance; every other request is foreground
(`cards/cas_probe.py` `attribute`). This is time-window attribution, not causal tracing: a concurrent insert inside a round counts as
maintenance, and the background `_snap` publisher (`Pool/CasRefLedger.cpp:4205`) outside a round counts as
foreground (it is caused by the inserts' log growth). Exact foreground claims therefore come from GC-off cases.
The rig's own inventory query (`system.remote_data_paths`, which makes the server read manifests) runs in the card's
teardown, after the window and before the drop, so its requests are teardown traffic, never workload.

| Family | Key shape | Category | Maintenance | Source |
|---|---|---|---|---|
| `blob` | `blobs/<algo>/<2 hex>/<hex>` | active (payload) | no | `Formats/CasLayout.h:115` |
| `blob_meta` | blob key + `.meta` | metadata | no | `Formats/CasLayout.h:120` |
| `manifest` | `cas/manifests/<ns>/<epoch>-<seq>/<ordinal>.zst` | active (payload) | no | `Formats/CasLayout.h:292` |
| `ref_stream` | `cas/ns/stream/<life>/{_log,_snap}/…` | metadata | no | `Formats/CasLayout.h:159` |
| `ref_ckpt` | `cas/ns/state/<life>/_ckpt` | metadata | no | `Formats/CasLayout.h:199` |
| `ns_files` | `cas/ns/state/<life>/_files/<name>` | metadata | no | `Formats/CasLayout.h:255` |
| `ref_catalog` | `cas/ref_catalog` | metadata | no | `Formats/CasLayout.h:473` |
| `lease` | `gc/server-roots/<srid>/{owner,epoch,mount}` | metadata | yes | `Formats/CasLayout.h:420` |
| `gc_hb` | `gc/hb` | metadata | yes | `Formats/CasLayout.h:344` |
| `gc` | `gc/state`, `gc/maintenance_state`, `gc/gen/…` | metadata | yes | `Formats/CasLayout.h:332` |
| `pool_meta` | `_pool_meta` | metadata | yes | `Formats/CasLayout.h:466` |
| `roots` | `roots/<srid>/…` loose mountpoint objects | metadata | yes | `Formats/CasLayout.h:397` |

Paths are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` unless they start with `src/`. The regexes are
`CAS_FAMILIES` in `cards/cas_probe.py` (coefficient `cas_key_families`). `gc_hb` is its own family so heartbeat rows
and round rows are disjoint by key. `roots/` holds the server's loose files, such as the
startup write probe (`Formats/CasLayout.h:317`–`:329`, `ContentAddressedTransaction.cpp:906`), and the mount-safety
empty-root listing; no MergeTree part or table file lands there.

## Operation graphs {#operation-graphs}

### Part files: blob or inline {#placement}

- **Blob files.** `.bin`, `.mrk`/`.mrk2`/`.mrk3`, `.cmrk`/`.cmrk2`/`.cmrk3`, `primary.idx` and `data.packed` stay
  blobs (`ContentAddressedTransaction.cpp:67`–`:80`). Every other part file up to 1 MiB is inline in the manifest
  (`:106`, `:1010`) and costs no request of its own.
- **Compact part: 1 blob.** `data.bin` only. Compact marks are `data.cmrk4` and the primary index is `primary.cidx`;
  neither matches the list, so both are inline. A Compact part with all entries inline does not exist on this build.
- **Wide part: 2 blobs per substream.** One `.bin` and one `.cmrk2` per stream. `String` has two streams (`.bin`,
  `.size.bin`), as do `Nullable` (`.null.bin`), `Array` (`.size0.bin`) and `LowCardinality` (`.dict.bin`).
- **Duplicates collapse.** Files of one part with equal bytes are one upload (`ContentAddressedTransaction.cpp:302`).
  Example: equal-size `CODEC(NONE)` columns share one `.cmrk2` blob.
- **Placement evidence.** `system.remote_data_paths` shows an inline entry with an empty remote path
  (`ContentAddressedMetadataStorage.cpp:2003`); the card reads it in its teardown.

### Blob entry branches {#branches}

| Branch | Requests per blob file | `system.cas_log` evidence | Source |
|---|---|---|---|
| fresh | HEAD (404), PUT body, PUT `.meta` (create-only) | `blob_put`, `publication_reason = absent` | `Pool/CasPartWriteTxn.cpp:397`, `:490`, `:349`, `:528` |
| hash hit | HEAD (200), GET `.meta` | `blob_reuse_adopt`, outcome `observed` | `:419`, `:443` |
| adoption (hardlinked file) | none | `blob_reuse_adopt`, outcome `adopt`, reason `manifest-trust`, at promote | `:553`, `:944` |
| inline | none | empty remote path | `ContentAddressedTransaction.cpp:1010` |

Two rarer branches of a present body: a missing `.meta` is backfilled with one more create-only PUT
(`Pool/CasPartWriteTxn.cpp:427`–`:437`), and a body GC has condemned is republished (PUT body, `.meta` replaced with
If-Match; `:457`, `:354`). They occur when content returns while its earlier copy is being reclaimed; the
calculator does not price them (the hash-hit fraction counts only Clean bodies). A body above the first upload buffer
is a multipart upload (lifecycle report, `multipart_sizing_rule`).

### Publishing a part {#publish}

Per part, besides its blobs (`cas_publish_requests`, `cas_ref_requests_per_rename`):

1. Manifest body: one create-only PUT, no preliminary HEAD (`Pool/CasPartWriteTxn.cpp:646`).
2. Precommit ref transaction.
3. Blob uploads (branches above).
4. Manifest GET: promote reads the body once (`:823`).
5. Promote ref transaction.
6. Rename ref transaction: the part moves from `tmp_insert_*` to its name (`ContentAddressedTransaction.cpp:1306`).

A ref transaction is a catalog GET (`Pool/CasRefLedger.cpp:3423`–`:3429`, the positive append), a create-only `_log`
PUT (`:3320`), and a `_ckpt` GET and If-Match PUT (`Pool/CasRefCkpt.cpp:187`, `:195`). These are the uncontended
counts the cases measure. Concurrent ref operations of one table can share a transaction (`Pool/CasRefLedger.cpp:2852`),
a `_ckpt` contribution with nothing new writes nothing (`Pool/CasRefCkpt.cpp:247`–`:254`), and failed or ambiguous
writes are retried (manifest: resolve-before-reissue, `Pool/CasPartWriteTxn.cpp:639`; blob: up to 8 publications,
`:387`). Retries are attempts with their own outcomes in the ledger; the cells price the uncontended path. The first transaction of a table life in a process reads
`_ckpt` once more. Totals per part: PUT 7, GET 7 (+1 cold), plus the blob branches. After more than 256 log records
or 1 MiB of tail a `_snap` snapshot is published in the background (`Pool/CasPool.h:296`).

### Mutation {#mutation}

`ALTER TABLE … UPDATE` of one column of a Wide part:

- **Table file protocol, once per mutation.** `tmp_mutation_N.txt` is written, renamed to `mutation_N.txt`, and the
  temporary removed (`src/Storages/MergeTree/MergeTreeMutationEntry.cpp:73`, `:105`): HEAD 3, PUT 2, GET 1, DELETE 1
  on `ns_files`.
- **Reads.** The source manifest GET and one ranged GET per mutated column file.
- **New part.** The mutated `.bin` is fresh. Its `.cmrk2` is a hash hit when the marks bytes are unchanged
  (`CODEC(NONE)` here). The 4 unchanged columns are adopted: 8 files, no request. Then the publish graph.

### Merge {#merge}

A merge reads each source part's manifest and every source blob file once (one ranged GET per file at these sizes),
publishes the result with fresh blobs only, and removes each source part's ref once the part leaves the working set
(a `_log` PUT and a `_ckpt` GET and PUT per source, no catalog GET). `gc-lifecycle.merge`, 8 Compact sources: GET 31,
PUT 25, HEAD 1 beyond the inserts.

### Drop {#drop}

`DROP TABLE … SYNC` removes each part's ref at call time (`ContentAddressedTransaction.cpp:1072`), one `ref_drop`
event per part (`Parts/PartFolderAccess.cpp:816`). The ref queue may batch them: 5 parts gave 6 transactions (one per
part plus the terminal) in every pass, 20 parts gave 3 in a GC-on smoke pass. The table also costs a catalog PUT,
`format_version.txt` HEAD/GET/DELETE and 2 LISTs. The calculator prices one transaction per part (the upper bound).

### GC round {#gc-round}

- **Scheduling.** A round starts `cas_gc_interval_sec` after the previous round finished (`Gc/CasGcScheduler.cpp:349`).
  Measured gap from Finish to the next Start: 10.0000–10.0001 s at a 10 s interval.
- **Deferred round.** With nothing new, a round re-adopts the sealed generation without a fold (`:303`, `:413`): 16
  requests (LIST 3, `gc` GET 8 and PUT 1, `lease` GET 1, `ref_catalog` GET 3), plus one heartbeat pulse. After 8
  deferred rounds in a row the next one folds anyway (`Gc/CasGc.cpp:353`, `gc_fold_max_defer_rounds = 8` at
  `Pool/CasPool.h:163`); the `gc` cell prices an idle pool as 8 deferred and 1 fold per 9 rounds. `gc-idle` ran 3
  rounds and saw only deferred ones.
- **Fold round.** A round with anything new folds: a fixed base of GC state, catalog and mount reads, one GET per
  ref log record it walks (`Gc/CasGc.cpp:2872`) of each table's tail since its last snapshot, and one GET per manifest
  edge those records name (`Gc/CasGc.cpp:1586`, `:2970`). In `gc-scale` every
  record is new (3 per inserted part); in `gc-lifecycle.drop` each of 3 folding rounds read all 63 records (189 GETs).
  Blob objects add no request: the fold reads manifests and logs, not blob listings.
- **Past the snapshot threshold.** With more than 256 records a `_snap` is published, and the fold of
  `gc-scale.objects-100k` (1479 records, 5 snapshots) read 3156 stream objects and batch-deleted covered ones. That
  regime belongs to the `ref_growth` dimension, cut by D14; the fitted rule holds up to 147 records (10k objects).
- **Manual round.** `SYSTEM CAS GC RUN <disk>` runs one synchronous round (`:336`); `SYSTEM CAS GC STOP/START`
  pause the scheduler (the `gc-scale` and `gc-idle` setups use them).

### Reclamation stages {#reclaim}

Each round after a retirement (drop, or removal of merged-away parts) advances the cohort one stage:

1. **Condemn and manifest delete.** Owner-removed manifests are deleted and zero-in-degree blobs condemned.
2. **Graduation.** Condemned blobs become `delete_pending` (`ContentAddressedSettings.cpp:65`).
3. **Blob delete.** Exact-token deletes of the body and `.meta` (`:66`).

This holds while the per-round budgets are not exhausted and the round is not clamped. Otherwise entries carry to the
next round unchanged (`Gc/CasBlobInDegree.cpp:550`–`:566`; a clamped pass neither graduates nor deletes,
`Gc/CasGc.cpp:3455`–`:3459`). A re-referenced blob is spared (`Gc/CasBlobInDegree.cpp:543`), and an absent body or
a changed token skips the DELETE (`Gc/CasGc.cpp:585`–`:587`).

Per retired blob: HEAD 2, `.meta` GET 3 and PUT 1, DELETE 2. A round graduates at most
`cas_gc_round_graduation_budget` and redeletes at most `cas_gc_round_redelete_budget` blobs (5000 each; 0 means
unbounded, `Gc/CasBlobInDegree.h:286`–`:287`). More retired blobs per round than either budget is the
`backlog_grows` state of the `gc` cell.

### Heartbeat {#heartbeat}

The leader pulses `gc/hb` every `max(50 ms, interval / 4)` (`Gc/CasGcScheduler.cpp:81`, `:495`) and once per round
on acquiring the lease (`:182`). A pulse is a GET and a conditional PUT (`Gc/CasGc.cpp:5076`). At a 10 s interval:
0.8 requests/s plus 2 per round.

### Mount lease {#lease}

The renewer replaces the mount object with If-Match every `cas_mount_renew_period_ms` (10 000 ms default,
`ContentAddressedSettings.cpp:74`), counted from the last committed attempt start (`Pool/CasMountRuntime.cpp:690`; `Pool/CasServerRoot.cpp:1623`,
`:1638`): start to start every 10 s while an attempt takes less than the period. The committed lease predictions
state the rate as `1 / (10 s + attempt latency)`; that text double counts the latency, though their numeric band
(0.095–0.1005 per s) holds the 0.1 per s the source gives. It runs with GC off. Rounds read the mount objects too, which those rounds pay.

### Local staging {#staging}

`cas_staging_backend` defaults to `local` (`ContentAddressedSettings.cpp:87`). Every blob file of a part is written
to the scratch directory while hashed (`ContentAddressedTransaction.cpp:984`), uploaded at commit and removed after it
(`:593`). The staging peak is the blob bytes of the parts committing at the same time (`staging_peak_bytes_rule`).

### Cold reads {#reads}

A part's manifest is read once when the server loads the part; the part view is retained
(`Parts/PartFolderAccess.cpp:159`), so a query resolves blob keys without a request. Statistics, `checksums.txt` and
the other small files are inline and cost nothing (`ContentAddressedTransaction.cpp:67`,
`ContentAddressedMetadataStorage.cpp:2000`). Per part read, without the cache:

- **First read: 2 `GetObject` per stream.** The stream's marks blob, loaded once into the mark cache
  (`src/Storages/MergeTree/MergeTreeMarksLoader.cpp:109`), and one `.bin` range
  (`src/Storages/MergeTree/MergeTreeReaderStream.cpp:270`). No statistics GET, unlike `s3`
  (`statistics.packed` is inline).
- **Later reads: 1 `GetObject` per stream**, the `.bin` range.
- **With the cache** (`cas_cached`): the first read downloads one segment per `.bin` stream and one per marks blob not
  yet cached. The cache keys segments by object path
  (`src/Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.cpp:41`), and equal files share a blob, so
  equal marks of several parts download once: at most 2 misses per stream. An identical read sends no request.

The mark cache keys by part path, so without the cache equal marks blobs are fetched once per part file.

### Startup {#startup}

`Cluster.stop(1)`, `Cluster.start(1)` and a `count()`, inside the window. Every request, all outcomes:

- **Fixed per start** (`cas_startup_requests`): disk access checks of the disk and, with the cache, of the cache
  disk (`src/Disks/IDisk.cpp:254`); the pool write probe on `_probe/` (create-only PUTs, two of them answered 412);
  the mount claim on `gc/server-roots/` (`Pool/CasMountRuntime.cpp:690`); `_pool_meta`, catalog, `_ckpt` and table
  file reads; and one ref transaction. `cas`: GET 32, PUT 11, HEAD 6, DELETE 3, LIST 7. `cas_cached`: GET 35,
  PUT 12, HEAD 11, DELETE 4, LIST 7. Of these, 4xx answers (`cas_startup_error_requests`): GET 4 and HEAD 3 (5 with
  the cache) are 404, PUT 2 and DELETE 1 are 412. A mount renewal (one PUT every 10 s) can fall inside a start.
- **Per loaded part** (`cas_startup_gets_per_part`): the manifest GET; a Wide part also reads its first column's marks
  blob for the index granularity (`src/Storages/MergeTree/MergeTreeDataPartWide.cpp:60`). Compact 1, Wide 2. The
  load reads no other blob: `data.cmrk4`, `primary.cidx`, `checksums.txt` and the rest are inline.
- **Log replay** (`cas_startup_gets_per_log_record`): the table's refs are recovered by replaying its log since the
  last snapshot (`Pool/CasRefLedger.cpp:441`), one GET per record: 3 per inserted part in the cases (precommit,
  promote, rename).

## Cases and reconciliation {#reconciliation}

Foreground counts are for caller `r1`, attempts per operation; every pass of a case gave the same integers.
`HEAD`/`PUT`/`GET` are `HeadObject`/`PutObject`/`GetObject`.

### Entry branches and table operations {#results-branches}

| Case | Role | Passes | Predicted = measured foreground | Branch population (fresh / hash hit / adoption / inline) |
|---|---|---|---|---|
| `cas-branches.fresh` | fit | 3/3 ok | HEAD 1, PUT 9, GET 8 | 1 / 0 / 0 / 10 (Compact: `data.bin` only) |
| `cas-branches.inline` | hold-out (`cas_objects_per_part`, Compact) | 3/3 ok | HEAD 1, PUT 9, GET 8 | 1 / 0 / 0 / 10 |
| `cas-branches.hash-hit` | fit | 3/3 ok | HEAD 2, PUT 16, GET 16 | 1 / 1 / 0 / 20; second part's blob keys = first part's |
| `cas-branches.adoption` | fit | 3/3 ok | HEAD 15, PUT 38, GET 19, DELETE 1 | 11 / 1 / 8 / 18; adopted columns `k`, `u2`, `u3`, `u4` |
| `cas-branches.staging-peak` | fit | 3/3 ok | HEAD 6, PUT 19, GET 8 | 6 / 0 / 0 / 9 (5 equal `.cmrk2` collapse to one) |
| `cas-branches.mix-holdout` | hold-out | 0/3: consumed | predicted HEAD 16, PUT 39; measured 18, 43 | predicted 16 fresh, measured 18: `String` has a `.size` stream |
| `cas-branches.mix-holdout-2` | hold-out (replacement) | 3/3 ok | HEAD 22, PUT 51, GET 8 | 22 / 0 / 0 / 9 |
| `cas-drop.table-sync` | fit | 3/3 ok | HEAD 6, PUT 58, GET 48, DELETE 1, LIST 2 | 5 inserts, then DROP of 5 parts |

`cas-drop.table-sync` was first predicted from a GC-on smoke pass (drop GET 9, PUT 7) and corrected to the measured
fit (GET 12, PUT 13) before its counted run; see §[Changes](#changes).

Staging: the scratch poller saw 160 068 975 B in every pass, exactly the part's staged blob files (5 × 32 012 225 B
`.bin` plus 5 × 1 570 B `.cmrk2`). The runner's 0.5 s directory sampler caught it in 1 of 3 passes of the report run
(160.35 MB, allocated; 0.46 MB in the other two).

### GC {#results-gc}

| Case | Role | Passes | Predicted | Measured |
|---|---|---|---|---|
| `gc-idle.populated` | fit | 3/3 ok | 16 requests per idle round; heartbeat 0.85–1.1 req/s | 16.0; 0.937 / 0.944 / 0.952 req/s; all rounds Deferred; Finish to next Start 10.0000–10.0001 s; outside the rounds only `gc_hb` (and the lease) |
| `gc-scale.objects-1k` | fit | 3/3 ok | fold 48, Deferred 18 | 48, 18; fold 296–336 ms |
| `gc-scale.objects-10k` | fit | 3/3 ok | fold 224, Deferred 18 | 224, 18; fold 545–759 ms |
| `gc-scale.objects-5k` | hold-out (requests) | 3/3 ok | fold 128, Deferred 18 | 128, 18 |
| `gc-scale.objects-3k` | hold-out (requests, duration) | 3/3 ok | fold 88, Deferred 18; fold 219–452 ms | 88, 18; 333–344 ms |
| `gc-scale.objects-30k` | hold-out | 0/3: consumed | fold 620 | 1259 (past the snapshot threshold) |
| `gc-scale.objects-100k` | fit (K=1) | 0/1: consumed | fold 521–2000 | 3689 (past the snapshot threshold) |
| `gc-lifecycle.drop` | fit | 3/3 ok | stage rounds 1 / 1 / 3 | 1 / 1 / 3; condemn 8.36–8.59 s (0.79–0.81 periods), blob delete 28.46–29.38 s (2.72–2.77) |
| `gc-lifecycle.merge` | hold-out (stage rounds) | 3/3 ok | 1–2 / 1–2 / 3–4 | 1 / 1 / 3; 0.78–0.87 and 2.77–2.80 periods |
| `gc-lifecycle.drop-60s` | hold-out | 0/3: consumed | periods 0–1.1 and 2–3.1; foreground PUT 70–110 | 0.980–0.994 and 2.978–2.992 periods; PUT 113 |
| `gc-lifecycle.drop-60s-12` | hold-out (replacement) | 3/3 ok | periods 0–1.1 and 2–3.1; PUT 108–135 | 0.960–0.970 and 2.953–2.967 periods (condemn 58.0–58.8 s, blob delete 178.3–179.5 s) |

`objects-3k` was predicted from the fit of run `cost-20261024T074131Z` (255.2 ms + 5.356 ms per part); the report
run refits on its own 1k and 10k passes (`coefficients.json`). Fold durations vary between runs on the shared host:
the 10k folds took 407–417 ms in run `cost-20261024T104750Z` and 545–759 ms in the report run, which widens the
duration tolerance to 141.7 ms. Request counts did not change.

Fold rounds per op and family, without the one heartbeat pulse each round carries: GET 17 + 1 per new manifest + 1
per ref log record, PUT 5, LIST 4. Deferred rounds: GET 12, PUT 1, LIST 3.

### Lease {#results-lease}

| Case | Role | Passes | Predicted | Measured |
|---|---|---|---|---|
| `lease.idle-10min` (run `cost-20261024T201554Z`) | validation | 3/3 ok | renewals 59–61; 0.095–0.1005 per s; cleanup listing GET 1–2, LIST 1–2 | 60 renewals each, 0.09999 per s; GET 1, LIST 1 |
| `lease.idle-10min` (run `cost-20261024T082909Z`) | validation | 0/3: consumed | no foreground | 60 renewals each; GET 1, LIST 1 unpredicted |
| `lease.idle-20min` (run `cost-20261024T082909Z`) | validation | 0/1: consumed | no foreground | 120 renewals; GET 3, LIST 3 unpredicted |

The mount object takes one `PutObject` per renewal and nothing else. The GET and LIST are the table cleanup listing,
priced in the `lease` cell.

### Cold reads and startup {#results-reads}

Run `cost-20261024T201554Z`; foreground of caller `r1`, every outcome. Predictions committed at `80e7b064858`,
after one uncited smoke pass of the fit cases (`cost-20261024T200614Z`, `cost-20261024T200941Z`); the three hold-outs
had not run.

| Case | Role | Passes | Table | Predicted | Measured |
|---|---|---|---|---|---|
| `cold-select.cas-off-wide` | fit | 3/3 ok | wide, 4 Wide parts, 3 streams | GET 24 (2 × 3 × 4) | GET 24 |
| `cold-select.cas-off-narrow` | fit | 3/3 ok | narrow, 4 Wide parts, 3 streams | GET 24 | GET 24 |
| `cold-select.cas-warm-off-wide` | fit | 3/3 ok | wide; repeated read | GET 12; first read 24 | GET 12; first read 24 |
| `cold-select.cas-warm-off-narrow` | fit | 3/3 ok | narrow; repeated read | GET 12; first read 24 | GET 12; first read 24 |
| `cold-select.cas-holdout-narrow-3` | hold-out | 3/3 ok | narrow, 3 parts of 70 000 rows, 2 streams; repeated read | GET 6; first read 12 | GET 6; first read 12 |
| `cold-select.cas-on-wide` | fit | 3/3 ok | `cas_cached`, wide | no request; misses 13 | none; misses 13 |
| `cold-select.cas-on-narrow` | fit | 3/3 ok | `cas_cached`, narrow | no request; misses 17 | none; misses 17 |
| `cold-select.cas-on-narrow-2` | fit | 3/3 ok | `cas_cached`, narrow, 60 000 rows | no request; misses 17 | none; misses 17 |
| `cold-select.cas-on-wide-2` | fit | 3/3 ok | `cas_cached`, wide, 2 parts of 60 000 rows | no request; misses 7 | none; misses 7 |
| `cold-select.cas-holdout-on-narrow-3` | hold-out | 3/3 ok | `cas_cached`, narrow, 3 parts, 2 streams | no request; misses 7–12 | none; misses 9 |
| `startup.cas-intact` | fit | 3/3 ok | `cas`, 3 Compact | GET 44, PUT 11–12 | GET 44, PUT 11, HEAD 6, DELETE 3, LIST 7 |
| `startup.cas-intact` | fit | 3/3 ok | `cas_cached`, 3 Compact | GET 47, PUT 12–13 | GET 47, PUT 12, HEAD 11, DELETE 4, LIST 7 |
| `startup.cas-compact-5` | fit | 3/3 ok | `cas`, 5 Compact | GET 52, PUT 11–12 | GET 52, PUT 11, HEAD 6, DELETE 3, LIST 7 |
| `startup.cas-compact-5` | fit | 3/3 ok | `cas_cached`, 5 Compact | GET 55, PUT 12–13 | GET 55, PUT 12, HEAD 11, DELETE 4, LIST 7 |
| `startup.cas-wide-parts` | fit | 3/3 ok | `cas`, 2 Wide | GET 42, PUT 11–12 | GET 42, PUT 11, HEAD 6, DELETE 3, LIST 7 |
| `startup.cas-wide-parts` | fit | 3/3 ok | `cas_cached`, 2 Wide | GET 45, PUT 12–13 | GET 45, PUT 12, HEAD 11, DELETE 4, LIST 7 |
| `startup.cas-wide-parts-3` | fit | 3/3 ok | `cas`, 3 Wide | GET 47, PUT 11–12 | GET 47, PUT 11, HEAD 6, DELETE 3, LIST 7 |
| `startup.cas-wide-parts-3` | fit | 3/3 ok | `cas_cached`, 3 Wide | GET 50, PUT 12–13 | GET 50, PUT 12, HEAD 11, DELETE 4, LIST 7 |
| `startup.cas-holdout-wide-4` | hold-out | 3/3 ok | `cas`, 4 Wide (narrow shape) | GET 52, PUT 11–12 | GET 52, PUT 11, HEAD 6, DELETE 3, LIST 7 |
| `startup.cas-holdout-wide-4` | hold-out | 3/3 ok | `cas_cached`, 4 Wide (narrow shape) | GET 55, PUT 12–13 | GET 55, PUT 12, HEAD 11, DELETE 4, LIST 7 |

Without the cache every pass sent exactly the predicted GETs: per part read 2 per stream first, then 1. With the cache
the measured pass sent nothing; the first read missed one segment per `.bin` stream and part plus one per marks blob
not already cached (1.08 to 1.5 per part and stream). A start took 2.1–3.1 s (`startup_seconds`). The start's fixed
requests include 404 and 412 answers: the write probe's create-only PUTs and GETs of `_log` records that do not exist
yet.

## Coefficients {#coefficients}

Measured (owner `cas`, `coefficients.json`); each has a fit and a distinct hold-out in `coverage.json`:

| Coefficient | Value | Fit | Hold-out | Validity |
|---|---|---|---|---|
| `cas_objects_per_part[shape][part_type]` | distinct uploads of the shape generators: narrow Compact 1, narrow Wide 8; wide Compact 1, wide Wide 41.33; types Compact 2, types Wide 75 (data-dependent, see the validation report) | `fresh`, `adoption`, `staging-peak`; `objects-per-part.*` (validation report) | `inline`, `mix-holdout-2`; `objects-per-part.*-2` (validation report) | 100–4 000 000 rows per insert |
| `cas_branch_population[write][branch]` | per part write, e.g. one-column mutation: fresh 1, hash hit 1, adoption 8, inline 9 | `fresh`, `hash-hit`, `adoption`, `staging-peak` | `mix-holdout-2` | as above |
| `gc_round_requests[term]` | fold GET 17 + 1 per new manifest + 1 per tail log record, PUT 5, LIST 4; Deferred GET 12, PUT 1, LIST 3; per blob object 0 | `objects-1k`, `objects-10k` (residual 0) | `objects-5k`, `objects-3k` | 1 017–9 949 objects |
| `gc_round_duration[term]` | fold 276.2 ms + 6.962 ms per new part (±141.7 ms, the largest fit or hold-out residual); Deferred 45.3 ms | `objects-1k`, `objects-10k` | `objects-3k` | as above |
| `reclaim_delay_periods[stage]` | condemn 0.8076, manifest delete 0.8051, blob delete 2.7543 periods (±1, the phase) | `gc-lifecycle.drop` | `gc-lifecycle.merge`, `drop-60s-12` | interval 10–60 s |
| `cas_gets_per_stream_read` | 2 (marks blob and `.bin` range of a first read) | `cas-off-wide`, `cas-off-narrow`, `cas-warm-off-*` (first read) | `cas-holdout-narrow-3` | 2–3 streams per part read |
| `cas_warm_gets_per_stream_read` | 1 (the `.bin` range) | `cas-warm-off-wide`, `cas-warm-off-narrow` | `cas-holdout-narrow-3` | as above |
| `cas_cached_miss_gets_per_stream_read` | 2, the bound; equal marks blobs miss once | `cas-on-wide`, `cas-on-narrow`, `cas-on-narrow-2`, `cas-on-wide-2` | `cas-holdout-on-narrow-3` | as above |
| `cas_startup_requests[cache][op]`, `cas_startup_error_requests[cache][op]` | answered 2xx: `cas` GET 28, PUT 9, HEAD 3, DELETE 2, LIST 7; `cas_cached` GET 31, PUT 10, HEAD 6, DELETE 3, LIST 7 (±1 PUT, a mount renewal). Answered 4xx: GET 4, PUT 2, HEAD 3 (`cas_cached` 5), DELETE 1 | `startup.cas-intact`, `cas-compact-5`, `cas-wide-parts`, `cas-wide-parts-3` | `startup.cas-holdout-wide-4` | one table |
| `cas_startup_gets_per_part[part_type]` | Compact 1, Wide 2 | as above | `startup.cas-holdout-wide-4` | 100–100 000 rows per insert: starts measured at 20 000–100 000, placement verified from 100 (`cas-branches.inline`) |
| `cas_startup_gets_per_log_record` | 1 (source rule, measured at 6–15 records) | as above | `startup.cas-holdout-wide-4` | one table; the rule holds below the 256-record snapshot threshold |

The `mounts` and `ref_growth` terms of `gc_round_requests` are unsupported (cut by D14). `reclaim_delay_s[stage]`
is stored as `reclaim_delay_periods[stage]`: seconds are periods × (interval + round duration), so one
fit serves every interval.

`lease_requests_per_s` is source-pinned instead of measured: 1 / `cas_mount_renew_period_s`
(`Pool/CasMountRuntime.cpp:690`), validated by the fresh `lease.idle-10min` passes (§[Lease](#results-lease)).

Pinned (registry, source-derived, reconciled exactly by the named cases): `cas_key_families` (`gc-idle`),
`gc_scheduler_rule` and `gc_heartbeat_requests_per_s` (`gc-idle`), `cas_branch_requests` (`hash-hit`, `adoption`),
`cas_publish_requests` and `cas_ref_requests_per_rename` (`fresh`), `cas_foreground_ref_removal_requests`
(`cas-drop.table-sync`), `staging_rule` and `staging_peak_bytes_rule` (`staging-peak`).

## Formulas {#formulas}

`formulas.json` (owner `cas`, variant `cas`): cells `inserts`, `merges`, `mutations`, `deletes`, `gc`, `lease`,
`storage` and `local_disk`. Every request row is per month (730 h). Applicability lists the families that occur:

| Operation | Family | State |
|---|---|---|
| `inserts` | `objects_per_part_by_shape`, `cas_branches` | supported |
| `inserts` | `multipart_graph` | unsupported_cut: a blob above the first upload buffer is priced as one `PutObject` (§[Gaps](#gaps)) |
| `merges` | `cas_branches`, `lifecycle` (fan-in), `thresholds` (result part type) | supported |
| `mutations`, `deletes`, `local_disk` | `cas_branches` | supported |
| `storage` | `objects_per_part_by_shape`, `cas_gc`, `lifecycle` | supported |
| `gc` | `cas_gc`, `lifecycle` (retired parts) | supported |
| `lease` | `cas_lease` | supported |
| `reads` | `reads_cache` | supported |
| `recovery` | `startup_recovery` | supported |

The `lease` cell holds the idle-time traffic of a server: the mount lease renewal and, per table, the MergeTree
cleanup thread's listing of the table's files (a catalog GET and a LIST per run, every `cas_table_cleanup_period_s`:
305 s idle, down to 60 s under load as an assumption). Both accrue per wall-clock second whatever the workload, so one
cell; `storage` holds byte-month rows only.

`gc` carries `backlog_grows` when retired blobs per round exceed the graduation or the redelete budget (0 means
unbounded).

`reads` follows the `s3` cell without the statistics term. Without the cache: every part read costs
`cas_warm_gets_per_stream_read` × `read_streams_per_part`, and each first read of a part (new parts and every active
part after a restart) adds `cas_gets_per_stream_read` − `cas_warm_gets_per_stream_read` per stream. With the cache: part
reads × `cache_miss_fraction` × `cas_cached_miss_gets_per_stream_read` × streams, the bound of 2 per stream.

`recovery` per restart and replica: `cas_startup_requests` per op, plus (`active_parts` + `outdated_parts`) ×
`cas_startup_gets_per_part[part_type]`, plus `cas_tables` × `cas_ref_log_tail_records` log GETs, plus one GET per
retained mutation entry on one replica (`src/Storages/StorageMergeTree.cpp:1655`; replicated tables keep them in
Keeper). With replicas > 1 a restarted replica also fetches its missed parts
(`catchup_requests_per_missed_part[cas]`, replication report). The start's 404 and 412 answers are `client_error`
rows with their status, so the billing preset decides whether they are billed.

The cells read the measured tables through registry derivations (`cas_blob_uploads_per_part`,
`cas_merge_result_blob_uploads`, `gc_round_requests_<term>`, `gc_round_duration_<term>`,
`reclaim_periods_blob_delete`), whose rules are the lookups. A cell describes a month, not a pass, so the per-pass
check is each card's own exact assertion set; the measured-fixture builder lists these edges as coefficient-only.

Assumed inputs (registry, `basis: assumption`, each with its reason): `cas_hash_hit_fraction` 0 (no repeated
content), `cas_mutation_marks_hit_fraction` 0 (compressed marks change with the data), `cas_ref_log_tail_records`
128 (half the snapshot threshold), `cas_blob_bytes` 1 MiB and `cas_manifest_bytes` 13 000 B (measured 13 019 and
13 751 B). Every part written is retired once; merges are level-1 merges of inserted parts at the lifecycle report's
`merge_effective_fan_in`.

## Changes found while measuring {#changes}

| Found by | Defect | Fix |
|---|---|---|
| smoke `cost-20261024T072831Z` | Compact part has one blob, not three: `data.cmrk4` and `primary.cidx` are inline | graph and predictions from the placement rule |
| run `cost-20261024T074131Z` | the adoption check read `cas_log` hashes as bare hex; they are `<algo>:<hex>` | card fix (`9974efc8938`), re-run |
| same | the 0.5 s directory sampler missed the staging plateau in 2 of 3 passes | 5 ms scratch poller in the card, re-run |
| same | DROP predicted from a GC-on smoke pass | fit corrected to the measured 6 transactions for 5 parts, re-run |
| same | `mix-holdout` missed the `String` `.size` stream | hold-out consumed; `mix-holdout-2` predicted before its run |
| same | `objects-100k` and `objects-30k` fold past the snapshot threshold | rule restricted to ≤ 147 records; `objects-30k` consumed; `objects-5k` predicted before its run |
| run `cost-20261024T075656Z` | `drop-60s` foreground PUT range assumed 70–110, measured 113 (the graph gives 113) | consumed; `drop-60s-12` with derived ranges predicted before its run |
| run `cost-20261024T082909Z` | the lease cases expected no foreground; an idle table lists its files once per cleanup run | lease rate made source-pinned with both cases as validation; the listing recorded as a gap |
| smoke `cost-20261024T200614Z` | `cold-select` and `startup` read `system.remote_data_paths` inside the window; on a CAS disk that query reads the catalog, lists the table and probes roots (GET 17, HEAD 6, LIST 6 per read) | card fix: CAS policies snapshot the layout in the teardown (`test_cas_policies_snapshot_the_layout_in_the_teardown_before_the_drop`) |

## Gaps and limitations {#gaps}

- **Idle-table cleanup listing (priced in `lease`).** With nothing to clean the cleanup thread sleeps
  `max_cleanup_delay_period` (300 s) plus up to 10 s (`src/Storages/MergeTree/IMergeTreeCleanupThread.cpp:134`–`:149`,
  `MergeTreeSettings.cpp:1682`, `:1685`); its temporary-directory pass (`MergeTreeCleanupThread.cpp:43`–`:47`) lists
  the table (`MergeTreeData.cpp:4028`). Under load the thread runs every 30 s but the listing waits its 60 s
  interval; the 60–310 s range of `cas_table_cleanup_period_s` is an assumption beyond the idle value. The first
  lease predictions expected no foreground and their passes failed on `ledger.unpredicted`; those edges are
  `consumed`, and `lease.idle-10min` was predicted afresh with the listing.
- **Shape cards on `cas`.** `objects-per-part`, `mutation` and `drop` ran on `cas` and `cas_cached` in the
  validation report (`docs/superpowers/reports/2026-10-24-storage-cost-validation`); `cas_branch_population` keeps the
  CAS-owned hold-out `cas-branches.mix-holdout-2`.
- **Cold reads of Compact parts** are priced at the Wide plan of 2 GETs per stream, an upper bound: a Compact part
  has one blob (`data.bin`) and inline marks.
- **Cached misses** are priced at the bound of 2 per stream. The cases measured fewer (1.08 to 1.5 per part and
  stream) because their equal marks files share one blob across columns and parts; distinct marks reach the bound.
- **Startup with more tables.** The fixed start requests were measured with one table on the disk
  (`cas_tables` 1); catalog, `_ckpt` and table file reads per extra table are not measured. Outdated parts and retained
  mutation entries are priced from the source like active parts; the cases have neither. The log replay assumes no
  snapshot within the tail (`cas_ref_log_tail_records` ≤ 256).
- **Multipart blobs.** A blob above the first upload buffer (32 MiB) is a multipart upload; the `inserts` cell prices
  one `PutObject` per blob. `staging-peak` blobs were 32 012 225 B, just below the buffer.
- **Snapshot regime.** Past 256 ref log records since a table's last snapshot, a fold reads each record about twice
  and batch-deletes covered ones (`objects-30k`: 1079 stream GETs for 445 objects; `objects-100k`: 3156 for 1484).
  That is the `ref_growth` dimension (cut). The cell assumes a tail of 128 records per table (`cas_ref_log_tail_records`,
  assumption: half the snapshot threshold).
- **Batched ref removals.** DROP removed 5 parts in 6 transactions in every pass, but 20 parts in 3 in a GC-on smoke
  pass. The `deletes` cell prices one transaction per part, the upper bound.
- **Attribution under GC.** A foreground request that starts inside a GC round is counted as maintenance. The GC-on
  cases therefore predict foreground as ranges; the exact graphs come from the GC-off cases.
- **Reclaim requests per object** (HEAD 2, `.meta` GET 3 and PUT 1, DELETE 2 per blob; GET 2 per manifest) come
  from `gc-lifecycle.drop` maintenance rows and are priced as constants; the integrated GC cost under load is not
  validated (`gc-under-load.default` is cut by D14).
- **Concurrent runs.** The `s3` report's run `cost-20261024T075800Z` overlapped run `cost-20261024T075656Z` on the same
  RustFS; counts are per pass prefix and unaffected, timings may be.

## Review {#review}

Review findings and their dispositions: [REVIEW-HISTORY.md](REVIEW-HISTORY.md).

## How to re-run {#rerun}

```bash
utils/cas-yardstick/rustfs-bin.sh ensure
python3 utils/cas-yardstick/yardstick.py cost run \
  --binary tmp/bin/26.8/clickhouse --manifests docs/superpowers/reports/2026-10-24-storage-cost-cas \
  --cases <experiment ids> --policies cas,cas_cached --runs-dir tmp/cas-yardstick/cost-runs
python3 -m unittest discover -s utils/cas-yardstick/tests -t utils/cas-yardstick -p 'test_cost_cards_cas.py'
```

`cost run` takes `tmp/yardstick.lock` itself. Exporting a run into this directory is a manual step: [Reproduce](../../cost-model/storage-cost-model.md#reproduce), step 3.
