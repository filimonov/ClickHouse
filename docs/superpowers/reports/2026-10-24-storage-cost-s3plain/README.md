---
description: 'Operation graphs and capability matrix of the s3_plain_rewritable disk (R3) for the storage cost calculator: directory objects, renames, file-by-file removal, startup listing, refused mutations, with source-cited rules, predictions and reconciliation on the 26.8 binary.'
sidebar_label: 'Storage cost s3plain graphs (R3)'
sidebar_position: 56
slug: /superpowers/reports/storage-cost-s3plain
title: 'Storage cost calculator — s3_plain_rewritable graphs and capability matrix (R3)'
doc_type: 'reference'
---

# Storage cost calculator — s3_plain_rewritable graphs and capability matrix (R3) {#s3plain}

`s3_plain_rewritable` (`s3plain`) writes every part file as an object, empty files included, plus one directory
object per part directory (2 `PutObject` and a `HeadObject` per new part), and removes a part file by file with a
server-side copy each: 4 requests per removed file against one `DeleteObjects` per part on `s3`. It cannot run
mutations. Every measured request count reconciles exactly with the graph below after two hold-out corrections (empty-file blobs, one empty part per dropped part).

| Item | Value |
|---|---|
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (`tmp/bin/26.8/clickhouse`) |
| Source commit | `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, the source of the measured binary: `antalya-26.8` merged into the CAS disk-transactions branch. Every `path:line` cite is at this commit |
| Runs | report run `cost-20261024T164541Z` (rig `855db66b453`); consumed hold-outs from `cost-20261024T103803Z`; `measurements.jsonl` holds 179 records (137 ok, 30 `UNSUPPORTED`, 12 `FAILED` of consumed hold-outs) |
| Predictions | fit `0f1ad12f2a2`; hold-outs, validations and insert re-fits `2f20606ffe6`; fresh hold-outs `types-holdout-single` and `drop.partition-2` and the `types-fit` re-fit `6bc64458fd8`; the 1 000-row validation `8679a5b7813`; each before the first run of its case |
| Policies | `s3plain` (`plain_rewritable` metadata over the RustFS bucket) and `s3plain_cached` (a `cache` disk over it) |
| Cuts | P2 cases (`detach-attach`, `cache-eviction`): cut to meet the campaign deadline (D14 in [LEDGER.md](../2026-10-23-storage-cost-calculator/LEDGER.md#decisions)), no manifest |

Cases reuse the `s3` and lifecycle manifests unchanged (same `params`, `settings`, `controls`) with the plain
policies. `merge-only.narrow-compact-removal` and `merge-only.wide-wide-removal` are new: their window also holds the
removal of the merged sources. The cards are those of the `s3` report plus `boundaries`; the gate is
`utils/cas-yardstick/yardstick/cost/cards/s3plain.py`.

## Capability matrix {#capability-matrix}

| Operation | `s3plain` | Deciding source |
|---|---|---|
| `storage`, `reads` | supported | a file is an object under its directory's random prefix (`Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.cpp:34`); reads resolve keys from the in-memory layout (`MetadataStorageFromPlainRewritableObjectStorage.cpp:359`) |
| `inserts`, `merges` | supported | new part directories and the rename from `tmp_` (`Storages/MergeTree/DataPartStorageOnDiskBase.cpp:983`) are directory operations of the plain layout |
| `mutations` | **not supported when the workload mutates** | `DiskObjectStorage::supportsHardLinks` is false for a plain metadata storage (`Disks/DiskObjectStorage/DiskObjectStorage.cpp:805`), so `checkMutationIsPossible` refuses every mutation (`Storages/MergeTree/MergeTreeData.cpp:6240`). A lightweight `DELETE` runs as `ALTER UPDATE _row_exists` by default (`Interpreters/InterpreterDeleteQuery.cpp:200`, `Core/Settings.cpp:5070`) and hits the same check. Lightweight `UPDATE` needs `enable_block_number_column` (`MergeTreeData.cpp:12630`), off by default (`MergeTreeSettings.cpp:2013`) |
| `deletes` | supported | `DROP PARTITION` is one of the partition commands a plain-rewritable disk allows (`MergeTreeData.cpp:8249`); TTL expiry is a merge; `DROP TABLE` removes the directories |
| `recovery` | supported | startup rebuilds the layout by listing `__meta/` (`MetadataStorageFromPlainRewritableObjectStorage.cpp:138`) |
| `local_disk` | supported | the layout is held in memory (`MetadataStorageFromPlainRewritableObjectStorage.h:87`): no local file per object; only the cache uses the local disk |

The `mutations` cell is `not_supported` only when the workload mutates: `mutations_per_month`,
`mutated_compact_parts_per_month` and `mutated_wide_parts_per_month` all zero leaves the column comparable, and any
non-zero value disables it with the reason below.

Paths are under `src/` at `09d0772a842`. Every mutation case runs on both plain policies and produces an `UNSUPPORTED`
record with this reason and its cites. The server answers the same: `Mutations are not supported for immutable
disk` (`SUPPORT_IS_DISABLED`).

Not cost operations, recorded for presets: `FREEZE` and the partition commands outside the allowed list are refused
(`MergeTreeData.cpp:8279`); `ATTACH`/`REPLACE PARTITION FROM` hard-link by server-side copy (`plain_copy_on_hardlink`,
`MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:456`).

### Replication {#replication}

`ReplicatedMergeTree` on `s3plain` is applicable. No gate refuses it, and a fetch writes the part through the disk
like an insert (`Storages/MergeTree/DataPartsExchange.cpp:1254`), so each replica pays the directory requests of its
own copy. Replicated mutations are refused by the same `checkMutationIsPossible` gate. `s3plain` stays in
`model_variants` of the replication cards in `coverage.json`; the runs belong to the replication report.

## Operation graphs {#operation-graphs}

Paths are under `src/` at `09d0772a842`. `OPS` is
`Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp`,
`MS` is `MetadataStorageFromPlainRewritableObjectStorage.cpp` in the same directory.

- **Directory create:** `PutObject` of `__meta/<random>/prefix.path` holding the logical path, then a `HeadObject`
  for its etag (`OPS:111`, `OPS:125`). An insert creates `tmp_insert_<part>/`
  (`Storages/MergeTree/MergeTreeDataWriter.cpp:974`), a merge `tmp_merge_<part>/` (`Storages/MergeTree/MergeTask.cpp:586`);
  a projection is one more directory.
- **Directory rename:** one `PutObject` per non-virtual directory of the subtree, rewriting `prefix.path` (`OPS:239`).
- **Part files:** one object per file. Unlike `s3`, empty files get an object too
  (`Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313`): a Wide part of the types shape has 6 more objects
  per `JSON` column with two typed paths (166 and 170 against 154 and 158 on `s3`), and the empty part that covers a
  dropped or expired part has 11 objects (`s3`: 8). `plain_objects_per_part` carries the plain counts (§Formulas).
- **Drop and expiry:** each dropped or expired part gets its own empty covering part (`drop.partition-2`, `-3`); a
  partitioned part has `partition.dat` and `minmax_<key>.idx` besides the shape's files.
- **Part removal** (`Storages/MergeTree/DataPartStorageOnDiskBase.cpp:1120`, `:1266`): rename to `delete_tmp_`
  (`PutObject`), then per file a copy aside to `__root/<random>` (`HeadObject` for the size,
  `Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:1082`, then `CopyObject`; `OPS:390`), the file's
  `DeleteObject` (`OPS:393`), the copy's `DeleteObject` at finalize (`OPS:414`), and last the `prefix.path`
  `DeleteObject` (`OPS:297`). No `DeleteObjects` batch. A file past 32 MiB is copied by multipart copy
  (`IO/S3/copyS3File.cpp:673`), in `min_upload_part_size` pieces (`:347`); no case measured it.
- **Startup** (`MS:94`–`:184`): two existence probes (`ListObjectsV2`, `max-keys=1`), listings of `__root/` and
  `__meta/`, then per directory one `GetObject` of `prefix.path` and one listing of its files. The disk access check
  writes, reads twice and unlinks `__root/clickhouse_access_check_<uuid>` (`Disks/IDisk.cpp:254`); the cache disk adds
  a check. Part loads read the same files as on `s3`.
- **Merges and reads** send the same `GetObject`s as `s3`.

### Graph corrections from the hold-outs {#graph-corrections}

Two hold-outs predicted from the first fit missed and were consumed; both corrections are properties of
`s3_plain_rewritable` itself:

- **Empty files are objects.** `DiskObjectStorageTransaction` creates a blob for an empty file unless the metadata
  storage supports empty files without blobs (`Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313`, `:400`);
  the plain-rewritable storage does not, `s3` metadata does. A Wide part with two `JSON` columns of two typed paths
  has 12 more objects than on `s3` (`objects-per-part.types-holdout`: 348 `PutObject` against 324 predicted). The
  fresh hold-out `types-holdout-single` was exact at 174, but it backs no edge. The cells count these objects with
  `empty_files_per_wide_part` (§Formulas).
- **One empty covering part per dropped part.** `DROP PARTITION` over three parts wrote three empty parts of 11
  objects and removed all six (`drop.partition-3`: 45 `PutObject`, 75 `HeadObject`, 72 `CopyObject`, 150
  `DeleteObject` against 17 / 51 / 50 / 104 predicted for one empty part). `initCoverageWithNewEmptyParts` makes one
  empty part per covered part (`Storages/StorageMergeTree.cpp:2720`, called at `:3044`);
  fresh hold-out `drop.partition-2` exact.

## Cases and reconciliation {#reconciliation}

Deterministic cases compare every op of the window exactly. Fit cases of the first run carry assumption ranges; every other prediction is exact. Values are equal in every ok pass.

| Case | Role | Predicted (`s3plain` / `s3plain_cached`) | Measured | Verdict |
|---|---|---|---|---|
| `big-insert-multipart.multipart` | validation | 1 CompleteMultipartUpload, 1 CreateMultipartUpload, 1 HeadObject, 16 PutObject, 3 UploadPart | 1 CompleteMultipartUpload, 1 CreateMultipartUpload, 1 HeadObject, 16 PutObject, 3 UploadPart | exact |
| `boundaries.compact-below-bytes` | validation | 1 HeadObject, 13 PutObject | 1 HeadObject, 13 PutObject | exact |
| `boundaries.wide-above-bytes` | validation | 1 HeadObject, 21 PutObject | 1 HeadObject, 21 PutObject | exact |
| `cold-select.cache-off` | validation | 1–400 GetObject, 0–40 HeadObject, 0–0 ListObjectsV2 | 184 GetObject | within the fit ranges |
| `cold-select.cache-off-types` | validation | 58 GetObject | 58 GetObject | exact |
| `cold-select.cache-on` | validation | 0–400 GetObject, 0–40 HeadObject | no request | within the fit ranges |
| `cold-select.cache-on-types` | validation | no request | no request | exact |
| `drop.partition` | fit, validation | 0–200 CopyObject, 0–400 DeleteObject, 0–40 DeleteObjects, 0–200 GetObject, 0–200 HeadObject, 0–20 ListObjectsV2, 0–100 PutObject | 24 CopyObject, 50 DeleteObject, 25 HeadObject, 15 PutObject | within the fit ranges |
| `drop.partition-2` | holdout, validation | 48 CopyObject, 100 DeleteObject, 50 HeadObject, 30 PutObject | 48 CopyObject, 100 DeleteObject, 50 HeadObject, 30 PutObject | exact |
| `drop.partition-3` | consumed hold-out | 50 CopyObject, 104 DeleteObject, 51 HeadObject, 17 PutObject | 72 CopyObject, 150 DeleteObject, 75 HeadObject, 45 PutObject | falsified (consumed) |
| `drop.table-sync` | reported, no edge | 0–200 CopyObject, 0–400 DeleteObject, 0–40 DeleteObjects, 0–200 GetObject, 0–200 HeadObject, 0–20 ListObjectsV2, 0–100 PutObject | 23 CopyObject, 50 DeleteObject, 23 HeadObject, 4 PutObject | within the fit ranges |
| `drop.ttl` | reported, no edge | 0–200 CopyObject, 0–400 DeleteObject, 0–40 DeleteObjects, 0–200 GetObject, 0–200 HeadObject, 0–20 ListObjectsV2, 0–100 PutObject | 23 CopyObject, 48 DeleteObject, 24 HeadObject, 15 PutObject | within the fit ranges |
| `merge-only.narrow-compact-removal` | fit, holdout, validation | 88 CopyObject, 184 DeleteObject, 64 GetObject, 89 HeadObject, 29 PutObject / 88 CopyObject, 184 DeleteObject, 24 GetObject, 89 HeadObject, 29 PutObject | 88 CopyObject, 184 DeleteObject, 64 GetObject, 89 HeadObject, 29 PutObject / 88 CopyObject, 184 DeleteObject, 24 GetObject, 89 HeadObject, 29 PutObject | exact |
| `merge-only.narrow-compact-sources` | fit, validation | 0–40 CopyObject, 0–40 DeleteObject, 0–10 DeleteObjects, 0–2000 GetObject, 0–10 HeadObject, 0–20 ListObjectsV2, 19–60 PutObject | 64 GetObject, 1 HeadObject, 21 PutObject / 24 GetObject, 1 HeadObject, 21 PutObject | within the fit ranges |
| `merge-only.narrow-wide-sources` | fit, validation | 0–40 CopyObject, 0–40 DeleteObject, 0–10 DeleteObjects, 0–2000 GetObject, 0–10 HeadObject, 0–20 ListObjectsV2, 19–60 PutObject | 33 GetObject, 1 HeadObject, 21 PutObject / 18 GetObject, 1 HeadObject, 21 PutObject | within the fit ranges |
| `merge-only.wide-compact-sources` | fit, validation | 0–40 CopyObject, 0–40 DeleteObject, 0–10 DeleteObjects, 0–2000 GetObject, 0–10 HeadObject, 0–20 ListObjectsV2, 89–130 PutObject | 129 GetObject, 1 HeadObject, 91 PutObject / 9 GetObject, 1 HeadObject, 91 PutObject | within the fit ranges |
| `merge-only.wide-wide-removal` | holdout, validation | 267 CopyObject, 537 DeleteObject, 243 GetObject, 268 HeadObject, 94 PutObject / 267 CopyObject, 537 DeleteObject, 123 GetObject, 268 HeadObject, 94 PutObject | 267 CopyObject, 537 DeleteObject, 243 GetObject, 268 HeadObject, 94 PutObject / 267 CopyObject, 537 DeleteObject, 123 GetObject, 268 HeadObject, 94 PutObject | exact |
| `merge-only.wide-wide-sources-3` | holdout, validation | 243 GetObject, 1 HeadObject, 91 PutObject / 123 GetObject, 1 HeadObject, 91 PutObject | 243 GetObject, 1 HeadObject, 91 PutObject / 123 GetObject, 1 HeadObject, 91 PutObject | exact |
| `mutation.compact-update-one-column` | gate | `UNSUPPORTED` | `UNSUPPORTED` × 6 | refused with the cited reason |
| `mutation.lightweight-delete` | gate | `UNSUPPORTED` | `UNSUPPORTED` × 6 | refused with the cited reason |
| `mutation.update-mixed` | gate | `UNSUPPORTED` | `UNSUPPORTED` × 6 | refused with the cited reason |
| `mutation.wide-delete` | gate | `UNSUPPORTED` | `UNSUPPORTED` × 6 | refused with the cited reason |
| `mutation.wide-update-one-column` | gate | `UNSUPPORTED` | `UNSUPPORTED` × 6 | refused with the cited reason |
| `objects-per-part.narrow-compact` | fit, validation | 3 HeadObject, 39 PutObject | 3 HeadObject, 39 PutObject | exact |
| `objects-per-part.narrow-compact-1k` | validation | 3 HeadObject, 39 PutObject | 3 HeadObject, 39 PutObject | exact |
| `objects-per-part.narrow-wide` | fit, validation | 3 HeadObject, 63 PutObject | 3 HeadObject, 63 PutObject | exact |
| `objects-per-part.types-fit` | reported, no edge | 4 HeadObject, 196 PutObject | 4 HeadObject, 196 PutObject | exact |
| `objects-per-part.types-holdout` | consumed hold-out | 4 HeadObject, 324 PutObject | 4 HeadObject, 348 PutObject | falsified (consumed) |
| `objects-per-part.types-holdout-single` | reported, no edge | 2 HeadObject, 174 PutObject | 2 HeadObject, 174 PutObject | exact |
| `objects-per-part.wide-compact` | fit, validation | 3 HeadObject, 39 PutObject | 3 HeadObject, 39 PutObject | exact |
| `objects-per-part.wide-wide` | holdout, validation | 3 HeadObject, 273 PutObject | 3 HeadObject, 273 PutObject | exact |
| `startup.intact` | fit, validation | 0–10 CopyObject, 0–20 DeleteObject, 0–4 DeleteObjects, 0–200 GetObject, 0–20 HeadObject, 1–40 ListObjectsV2, 0–10 PutObject | 1 CopyObject, 2 DeleteObject, 53 GetObject, 1 HeadObject, 9 ListObjectsV2, 1 PutObject / 2 CopyObject, 4 DeleteObject, 39 GetObject, 2 HeadObject, 9 ListObjectsV2, 2 PutObject | within the fit ranges |
| `startup.wide-parts` | holdout, validation | 1 CopyObject, 2 DeleteObject, 107 GetObject, 1 HeadObject, 8 ListObjectsV2, 1 PutObject / 2 CopyObject, 4 DeleteObject, 28 GetObject, 2 HeadObject, 8 ListObjectsV2, 2 PutObject | 1 CopyObject, 2 DeleteObject, 107 GetObject, 1 HeadObject, 8 ListObjectsV2, 1 PutObject / 2 CopyObject, 4 DeleteObject, 28 GetObject, 2 HeadObject, 8 ListObjectsV2, 2 PutObject | exact |

**Edges on fewer than three passes.** Timing misses of the report run leave these case-policy pairs with fewer than K = 3 passes behind their edges; each remaining pass is exact:

| Case | Policy | Passes behind the edges | Missing passes |
|---|---|---|---|
| `merge-only.wide-wide-removal` | `s3plain_cached` | 2 | k2 `window_crossing` |

**Inputs edited after the first runs.** The prediction `inputs` (model inputs, not the expected counts) gained `insert_rows` and `merge_result_part_type`, and `drop.partition-2` changed `expired_parts_per_month` from 1 to 2: its committed `expected` already encoded two dropped parts (30 `PutObject`, 50 `HeadObject`), the inputs did not. No expected count changed.

## Coefficients {#coefficients}

| Coefficient | Evidence | Value |
|---|---|---|
| `plain_meta_objects_per_part` | derivation, verified | 1 + `projections` (one directory per part and per projection, `OPS:111`); measured narrow: 1, wide: 1, types: 2 |
| `plain_dir_create_requests` | source (Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:125) | PutObject: 1, HeadObject: 1 |
| `plain_rename_requests` | source (Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:239) | 1 |
| `plain_copy_on_hardlink` | source (Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:456) | HeadObject: 1, CopyObject: 1 |
| `plain_unlink_requests` | source (Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:390) | HeadObject: 1, CopyObject: 1, DeleteObject: 2 |
| `plain_dir_remove_requests` | source (Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:297) | PutObject: 1, DeleteObject: 1 |
| `plain_empty_part_objects` | source (Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313) | 11 |
| `plain_objects_per_part` | derivation, verified | Compact: `objects_per_compact_part`; Wide: `plain_objects_per_wide_part` (§Formulas); measured narrow/compact: 11, narrow/wide: 19, wide/compact: 11, wide/wide: 89 |
| `plain_expired_part_extra_files` | source (Storages/MergeTree/MergeTreePartition.cpp:470) | 2 |
| `plain_startup_requests` | measured | 0/fixed/PutObject: 1, 0/fixed/HeadObject: 1, 0/fixed/GetObject: 5, 0/per_dir/GetObject: 1, 0/fixed/CopyObject: 1, 0/fixed/DeleteObject: 2, 0/fixed/ListObjectsV2: 6, 0/per_dir/ListObjectsV2: 1, 1/fixed/PutObject: 2, 1/fixed/HeadObject: 2, 1/fixed/GetObject: 6, 1/per_dir/GetObject: 1, 1/fixed/CopyObject: 2, 1/fixed/DeleteObject: 4, 1/fixed/ListObjectsV2: 6, 1/per_dir/ListObjectsV2: 1 |
| `plain_local_metadata_bytes_per_object` | source (Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h:87) | 0 |

Every s3plain coefficient a cell uses has fit and hold-out edges in `coverage.json` per plain policy; `plain_copy_on_hardlink` is used by no cell (no supported operation hard-links). The reused `s3` merge `GetObject` counts (`merge_gets_per_source_part`, `_cached`) have s3plain fit and hold-out edges; the other reused `s3` coefficients have s3plain `validation` edges.

## Formulas and applicability {#formulas}

`formulas.json` (owner `s3plain`, variant `s3plain`) takes the committed `s3` cells and adds the plain requests:

| Cell | Change from `s3` | Families |
|---|---|---|
| `storage` | none | `lifecycle` |
| `inserts` | per part: `plain_meta_objects_per_part` × (`plain_dir_create_requests` + `plain_rename_requests`) | `objects_per_part_by_shape`, `thresholds`, `multipart_graph` |
| `merges` | directory requests of the result; sources removed file by file (`plain_unlink_requests`, `plain_dir_remove_requests`) instead of `DeleteObjects`; a removed file past 32 MiB is copied aside by multipart copy (predicted from source, not measured) | `lifecycle` |
| `mutations` | unconditional `not_supported` state with the gate's reason; no rows | — |
| `deletes` | one empty covering part of `plain_empty_part_objects` files per expired part, with its directory requests; expired parts (shape files + `plain_expired_part_extra_files`) and empty parts removed file by file; large files of expired parts copied by multipart copy as in `merges` | `mutation_deletes` |
| `reads` | none | `reads_cache` |
| `recovery` | the `s3` fixed term (`startup_requests`) replaced by `plain_startup_requests`: fixed + per part directory; part loads cover active and outdated parts (`Storages/MergeTree/MergeTreeData.cpp:3656`) | `startup_recovery` |
| `local_disk` | no metadata files (`plain_local_metadata_bytes_per_object` = 0); the cache size | `objects_per_part_by_shape` |

All `objects_per_part` reads of the `s3` cells read `plain_objects_per_part`: the `s3` part formulas
(`s3` report, §Formulas from source) plus one object per empty file of a Wide part, `plain_objects_per_wide_part` =
`objects_per_wide_part` + `empty_files_per_wide_part`. Verification points: 11 / 19 objects per narrow Compact / Wide
part, 11 / 89 wide, and per types Wide part 166 (`types-fit`, 154 + 12) and 170 (`types-holdout`, 158 + 12, from its
348 `PutObject` for two parts with two directories each). The types preset sets `empty_files_per_wide_part` = 12: 6
per `JSON` column with two typed paths. The cells reuse `s3`-owned
coefficients where the plain requests equal the `s3` ones (merge and read `GetObject`s, `startup_gets_per_part`); the
plain records add `validation` edges for them.

## Assumptions {#assumptions}

- **One table per server** in the fixed startup term: the table and `detached` directories sit in `fixed`. Reason:
  the startup cards run one table; each further table adds 2 `GetObject` and 2 `ListObjectsV2` per start.
- **Expired parts and merge sources keep the table's directory count** (`plain_meta_objects_per_part` = 1 +
  `projections`).
- **Partitioned expired parts with one partition key column** (`plain_expired_part_extra_files` = 2). Reason: `DROP
  PARTITION` needs a partition key; a TTL-only table has `ttl.txt` instead (one file: `drop.ttl` measured 12).
- **Verified range.** `plain_objects_per_part` was verified for `insert_rows` 1 000–400 000 (the 1 000-row
  validation `objects-per-part.narrow-compact-1k` is exact). `plain_startup_requests` was measured on one replica; its validity `replicas` 1–3 is an
  extrapolation: the counts are per replica, and a start of a replicated table adds Keeper requests, not S3 ones.
- The `s3` cell assumptions (merge results Wide, local merges per replica, TTL path for expiry) carry over.

## Open items and limits {#open-items}

- **Multipart copy on removal: predicted from source, not measured.** A part file past 32 MiB is copied aside by
  multipart copy (`IO/S3/copyS3File.cpp:673`, pieces of `min_upload_part_size`, `:347`). Per large removed file the
  cells price `CreateMultipartUpload`, `multipart_copy_parts` = ceil(`object_bytes` / `min_upload_part_size`)
  `UploadPartCopy` and `CompleteMultipartUpload` in place of the `CopyObject`; the `HeadObject` and both
  `DeleteObject`s stay. Merge sources count `large_objects_per_part`, expired parts `large_objects_per_merge_result`.
  No case measured it.
- **TTL-only expiry** removes parts with `ttl.txt` (one extra file), not the two partition files the cell assumes.
- **Lightweight `UPDATE`** (patch parts) is possible on `s3plain` with both block columns enabled; no cell prices it.
- **Replication** applicability is recorded here; its requests are measured by the replication report.
- **Lifecycle seeds** (`small-inserts`, `merge-natural`, `merge-forced` with `s3plain` policies) stay `planned`: the
  `s3plain` merges are measured by `merge-only` cases instead.
- **Removal at the window edge.** The report run has 1 `FAILED` passes, all timing: 1 `window_crossing`. A `window_crossing` is an unlink or cache read that finished after the window; an `assertions_failed` pass is a removal tail after the window or one retried request. The cards wait for the outdated parts to leave `system.parts`, not for the copies' deletes. They are not in `measurements.jsonl`; §Reconciliation lists the edges they thin out.
- **Empty files of other `JSON` layouts.** `empty_files_per_wide_part` is 6 per `JSON` column with two typed paths
  in the measured tables; other layouts are not measured.
- **Replication applicability** is recorded in §Replication and by `s3plain` in the replication cards'
  `model_variants`; `coverage/1` has no per-card `variants` field with a source reference (`additionalProperties: false`).

## Review {#review}

Review findings and their dispositions: [REVIEW-HISTORY.md](REVIEW-HISTORY.md).

## Reproduce {#reproduce}

```
utils/cas-yardstick/rustfs-bin.sh ensure
python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-s3plain --cases all --policies s3plain,s3plain_cached \
  --repeats 3 --runs-dir tmp/cas-yardstick/cost-runs
```

`cost run` takes `tmp/yardstick.lock` itself. Exporting a run into this directory is a manual step: [Reproduce](../../cost-model/storage-cost-model.md#reproduce), step 3.
