---
description: 'Operation graphs of the s3 disk (R2) for the storage cost calculator: objects per part by shape and type, multipart, merges, mutations, drops, cold reads with and without the cache, startup and local metadata, with source-cited rules, predictions and reconciliation on the 26.8 binary.'
sidebar_label: 'Storage cost s3 graphs (R2)'
sidebar_position: 55
slug: /superpowers/reports/storage-cost-s3
title: 'Storage cost calculator — s3 disk operation graphs (R2)'
doc_type: 'reference'
---

# Storage cost calculator — s3 disk operation graphs (R2) {#s3}

Every request count of the `s3` disk reconciles exactly on the pinned binary: 28 cases on `s3` and `s3_cached`, K=3,
150 records, all ok. All ten hold-out cases land on their committed predictions; one earlier hold-out band
(`bytes_per_get` of `cold-select.cache-off-types`) was falsified, consumed and replaced. The variant's eight cells are
in `formulas.json`; their coefficients are in `coefficients.json`.

| Item | Value |
|---|---|
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (`tmp/bin/26.8/clickhouse`) |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Report run | `cost-20261024T075800Z`, rig commit `9658a83a36e`; `measurements.jsonl` holds its 150 records |
| Earlier runs | `cost-20261024T073405Z` (fit run), `cost-20261024T074526Z` (first hold-out run; its 3 `FAILED` records of `cold-select.cache-off-types` are kept in `measurements.jsonl` as the consumed edge), `cost-20261024T075016Z` (cache-fill fix, fresh hold-outs) |
| Predictions | fit and first hold-outs `80cd02f1c27`; fresh hold-outs `202d45aadbc`, `5519040a680`; each before the first run of its case |
| Store | RustFS behind the ledger proxy; every record replays from `raw/<card>/<run_id>/ledger.jsonl.gz` |
| Cuts | `detach-attach.default` and `cache-eviction.2x` (P2): cut by owner directive D14, no manifest |

Cards: `utils/cas-yardstick/yardstick/cost/cards/shapes.py` (`objects-per-part`, `big-insert-multipart`) and
`cards/ops.py` (`mutation`, `drop`, `cold-select`, `startup`); merges use the lifecycle card `merge-forced` with
manifests of this report. Requests are attributed to part files through `system.remote_data_paths` snapshots before
and after the window (`requests_by_file`, `gets_by_file` in the records); keys no snapshot lists are `unlisted`.

## Operation graphs {#operation-graphs}

Paths are under `tmp/src-26.8/src/`.

### Insert {#insert}

- **One object per part file.** The disk transaction writes every file of a part as its own blob
  (`Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:274`); a file that fits the 32 MiB first upload buffer
  is one `PutObject` (`IO/WriteBufferFromS3.cpp:188`). Nothing else is sent: no `HeadObject` after an upload
  (`check_objects_after_upload` is false, `IO/S3Defines.h:55`). `insert_tx_checks` = 0 in every insert window.
- **Metadata files: 9 per part.** `primary.cidx` (`Storages/MergeTree/MergeTreeDataPartWriterOnDisk.cpp:119`),
  `count.txt`, `serialization.json`, `statistics.packed`, `columns.txt`, `columns_substreams.txt`,
  `metadata_version.txt`, `default_compression_codec.txt`, `checksums.txt`
  (`Storages/MergeTree/MergedBlockOutputStream.cpp:407`–`:493`). A partitioned table adds `partition.dat` and
  `minmax_<key>.idx` (13 objects per Compact part in `drop.partition`); a TTL adds `ttl.txt`.
- **Compact part:** `data.bin` and `data.cmrk4` (`Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp:53`, `:59`):
  11 objects for any column count.
- **Wide part:** a `.bin` and a marks file per stream (`Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:140`,
  `:224`). Streams per column type, read from `objects-per-part.types-fit`:

  | Type | Streams | Files |
  |---|---|---|
  | `UInt64` | value | 2 |
  | `String` | chars, sizes (`DataTypes/Serializations/SerializationString.cpp:346`) | 4 |
  | `Nullable(UInt64)` | value, null map (`DataTypes/Serializations/SerializationNullable.cpp:53`) | 4 |
  | `Array(UInt32)` | value, `size0` (`DataTypes/Serializations/SerializationArray.cpp:268`) | 4 |
  | `LowCardinality(String)` | keys, dictionary (`DataTypes/Serializations/SerializationLowCardinality.cpp:86`) | 4 |
  | `Map(String, UInt64)` | `keys`, `keys.size`, `values`, `size0` (`DataTypes/DataTypeMap.cpp:77`) | 8 |
  | `JSON` with paths `a` (Int64) and `b` (String) | object structure, shared data, per path: dynamic structure, variant discriminators, the variant streams (`DataTypes/Serializations/SerializationObject.cpp:198`, `:261`) | 24 |

- **Skip indexes and projections.** Every skip index of a part is packed into one `skp_idx.packed`
  (`Storages/MergeTree/MergeTreeIndicesSerialization.h:16`). A projection is a part of its own under
  `<name>.proj/`, Compact here: 10 objects (the Compact files without `statistics.packed`).
- **Shapes measured.** narrow = 5 `UInt64` columns, wide = 40, types = the 21-column mix of
  `objects-per-part.types-fit` (`k` plus 5 `Nullable(UInt64)`, 5 `Array(UInt32)`, 5 `LowCardinality(String)`, 3 `Map`,
  2 `JSON`, a `minmax` index, one projection). The part type of every insert is computed from the registry predicate
  `part_type_predicate` and asserted against `system.parts`.

### Multipart {#multipart}

A file past the first buffer goes multipart once a second buffer is detached (`IO/WriteBufferFromS3.cpp:397`,
`:407`); later buffers are 16 MiB (`Common/BufferAllocationPolicy.cpp:51`). `big-insert-multipart.multipart`:
`s.bin` of 600 000 100-byte values with `CODEC(NONE)` is 60 001 850 B (one 25-byte frame per 8192-row granule; the
sizes are a separate stream), uploaded as 33 554 432 + 16 777 216 + 9 670 202 B: one `CreateMultipartUpload`,
three `UploadPart`, one `CompleteMultipartUpload` and 14 `PutObject` for the part's other files.

### Merge {#merge}

A merge writes its result part (objects as an insert of that type) and reads its sources. Reads per source part,
measured with `OPTIMIZE FINAL` on 2 to 8 sources:

| Source | Without cache | With cache |
|---|---|---|
| Compact, 5 columns | 8 (columns + 3) | 3 |
| Compact, 40 columns | 43 (columns + 3) | 3 |
| Wide, 5 columns | 11 (2 × columns + 1) | 6 (columns + 1) |
| Wide, 40 columns (hold-out) | 81 predicted, 81 measured | 41 predicted, 41 measured |

A Compact reader seeks per column inside `data.bin` (`Storages/MergeTree/MergeTreeReaderCompact.cpp:219`); a Wide
reader opens one stream per file and reads up to the last mark it needs
(`Storages/MergeTree/MergeTreeReaderStream.cpp:270`). Through the cache, a file is fetched in cache segments, so
small files cost one request each. Merged-away sources are outdated until `old_parts_lifetime`, then removed with one
`DeleteObjects` per part (keys batched by 1000: `IO/S3Defines.h:52`,
`Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:599`). These counts replace the assumption ranges of
the lifecycle report: its 64 `GetObject` for 8 sources (`merge-forced.default`) and 16 for 2 are 8 per 5-column
Compact source.

### Mutation {#mutation}

- **Compact part: full rewrite.** A part that is not Wide is mutated by rewriting all columns
  (`Storages/MergeTree/MutateTask.cpp:908`): 11 new objects, none kept (asserted by object count:
  `full_rewrite.<table>.kept_objects = 0`).
- **Wide part: changed streams only.** Untouched streams are hard links to the same objects
  (`Storages/MergeTree/MutateTask.cpp:1247`); `checksums.txt`, `columns.txt` and the other metadata files are always
  rewritten (`:1162`). `UPDATE u1 = u1 + 1` writes 9 objects per part (`u1.bin`, `u1.cmrk2` and 7 metadata files) and
  keeps 10; a lightweight `DELETE` writes `_row_exists.bin`, `_row_exists.cmrk2` and the same 7; `ALTER DELETE`
  rewrites all 19.
- **Per statement:** one `mutation_<n>.txt` object in the table directory
  (`Storages/MergeTree/MergeTreeMutationEntry.cpp:73`).
- **Afterwards:** the source part is outdated; with `old_parts_lifetime = 1` in the cards, its unshared objects go
  in one `DeleteObjects` per part inside the window.
- **Reads per part** (`mutation_gets_per_part`, narrow shape): delete compact: 27, delete wide: 16, lightweight_delete compact: 30, lightweight_delete wide: 28, update_column compact: 26, update_column wide: 7, delete compact: 27 (cache), delete wide: 10 (cache), lightweight_delete compact: 29 (cache), lightweight_delete wide: 5 (cache), update_column compact: 26 (cache), update_column wide: 2 (cache).
- A part whose rows the predicate never matches is not rewritten: the first smoke run of `mutation.wide-delete`
  hit this branch (`k % 10 = 0` against odd `k` only), so the manifests insert contiguous `k`.

### Drop and expiry {#drop}

- `DROP PARTITION` covers the dropped parts with an empty part (`Storages/StorageMergeTree.cpp:2949`, `:2742`):
  8 `PutObject`, then removes the dropped and the empty part (`:3064`): one `DeleteObjects` each.
- TTL expiry (`TTL d + INTERVAL 1 SECOND`, `merge_with_ttl_timeout = 1`): the TTL merge writes an empty part (8
  objects); the source and the empty part are removed: 8 `PutObject`, 2 `DeleteObjects`.
- `DROP TABLE SYNC`: one `DeleteObjects` per part and one `DeleteObject` for the table's `format_version.txt`
  (`Storages/StorageMergeTree.cpp:529`).

### Cold read and cache {#reads}

- **Statistics first.** A query that does not reuse the cached selectivity estimator loads the parts' statistics
  (`Storages/MergeTree/MergeTreeData.cpp:988`, `:997`): one ranged `GetObject` of `statistics.packed` per column
  statistics file per part (`Storages/MergeTree/IMergeTreeDataPart.cpp:1386`). Every column has statistics
  (`auto_statistics_types`, `Storages/Statistics/Statistics.cpp:933`).
- **Then one `.bin` range and one marks file per stream read** per part
  (`Storages/MergeTree/MergeTreeReaderWide.cpp:246`, `Storages/MergeTree/MergeTreeReaderStream.cpp:270`).
  `length(a1)` reads only `a1.size0`; a `Nullable` column reads its null map too.
- **Rule:** `GetObject` per part read = `stats_gets_per_column` × columns + `gets_per_stream_read` × streams =
  columns + 2 × streams. `cold-select.cache-off`: 4 parts × (40 + 2 × 3) = 184; hold-out `cold-select.cache-off-types`:
  2 × (21 + 2 × 4) = 58, predicted and measured.
- **Bytes per GET.** The statistics reads ask for the archive from their offset to its end (`Range: bytes=764-`), so
  their bytes follow the archive size: 67 993 B per GET on the wide table, 33 211 B on the types table.
- **With the cache** (`s3_cached`, `cache_on_write_operations` set in the disk but
  `enable_filesystem_cache_on_write_operations` off by default, `Core/Settings.cpp:7282`): inserts do not fill the
  cache. A cold read through the cache downloads segments: 5 GETs per part read on the wide table, 7 on the types
  table; the next identical read sends no request (`cold-select.cache-on*`, measured pass: 0 requests). The cache
  volume allocates 1.016 to 1.026 bytes per downloaded byte (`Interpreters/FileCache/FileSegment.cpp:418`).

### Startup {#startup}

`Cluster.stop(1)`, `Cluster.start(1)` and a `count()` per table, inside the window:

- **Disk access checks:** a probe file is written, read twice (whole and with an offset) and removed
  (`Disks/IDisk.cpp:217`, `:254`). Without the cache two checks run: 2 `PutObject`, 4 `GetObject`, 2 `DeleteObject`.
  The cache disk adds a third, whose two reads are one `GetObject` through the cache. Each start also reads one
  1-byte object, the table's `format_version.txt`: 5 fixed `GetObject` without the cache, 6 with it.
- **Part load** (`Storages/MergeTree/IMergeTreeDataPart.cpp:1504`): 9 small files per part (8 metadata files and the
  marks the index granularity is read from) and the statistics refresh at table start
  (`Storages/MergeTree/MergeTreeData.cpp:3289`): `statistics.packed` once per column plus its index. Narrow Compact:
  15 per part (50 = 3 × 15 + 5); hold-out wide Wide: 50 per part (105 = 2 × 50 + 5), predicted and measured. With the
  cache, the archive is one segment: 10 per part (36 and 26).
- `startup_seconds` 2.1–3.1 s for these tables (start to answer plus the loads).

### Local disk {#local-disk}

- **Metadata:** one local file per object under `<replica>/s3_meta/`, 110–121 B apparent, 4096 B allocated on ext4
  (`Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.cpp:116`); the types tables allocate 4407 B per
  object.
- **Write calls:** the server's write syscalls (`/proc/<pid>/io` `syscw`) per object written fit
  `a + b / objects_per_part` with `a` = 1.177 and `b` = 26.0 per part (max residual 1.068). They
  include log flushes and the embedded Keeper; the EBS report converts calls to device I/O.

## Cases and reconciliation {#reconciliation}

Deterministic cases compare every request op of the window exactly (`ledger.unpredicted` forbids any other op). Fit
predictions are source-derived where the source fixes the count (object writes) and assumption ranges where the fit
measures it (merge, mutation and read `GetObject`, deletes); every measured count is now explained by the graph above.
Values are identical in all three repeats and both policies unless two are shown (`s3` / `s3_cached`).

| Case | Role | Predicted | Measured (`s3` / `s3_cached`) |
|---|---|---|---|
| `big-insert-multipart.multipart` | validation | 1 CompleteMultipartUpload, 1 CreateMultipartUpload, 14 PutObject, 3 UploadPart; `upload_part_sizes` | 1 CompleteMultipartUpload, 1 CreateMultipartUpload, 14 PutObject, 3 UploadPart |
| `cold-select.cache-off` | fit | 1–400 GetObject | 184 GetObject |
| `cold-select.cache-off-types` | consumed hold-out, fit, holdout | 58 GetObject | 58 GetObject |
| `cold-select.cache-off-types-2` | holdout, validation | 58 GetObject; `bytes_per_get` | 58 GetObject |
| `cold-select.cache-on` | fit | 0–400 GetObject | no request |
| `cold-select.cache-on-types` | fit, holdout | no request; `cache_fill_io_per_byte` | no request |
| `cold-select.cache-on-types-2` | holdout, validation | no request; `warm_get_requests`, `warm_get_bytes_per_part`, `cache_fill_io_per_byte` | no request |
| `drop.partition` | fit | 0–40 DeleteObject, 1–40 DeleteObjects, 0–200 GetObject, 0–100 PutObject | 2 DeleteObjects, 8 PutObject |
| `drop.table-sync` | fit | 0–40 DeleteObject, 1–40 DeleteObjects, 0–200 GetObject, 0–100 PutObject | 1 DeleteObject, 2 DeleteObjects |
| `drop.ttl` | fit | 0–40 DeleteObject, 1–40 DeleteObjects, 0–200 GetObject, 0–100 PutObject | 2 DeleteObjects, 8 PutObject |
| `merge-forced.narrow-compact-sources` | fit, validation | 0–400 GetObject, 107 PutObject | 64 GetObject, 107 PutObject / 24 GetObject, 107 PutObject |
| `merge-forced.narrow-wide-sources` | fit, validation | 0–2000 GetObject, 76 PutObject | 33 GetObject, 76 PutObject / 18 GetObject, 76 PutObject |
| `merge-forced.wide-compact-sources` | fit, validation | 0–2000 GetObject, 122 PutObject | 129 GetObject, 122 PutObject / 9 GetObject, 122 PutObject |
| `merge-forced.wide-wide-sources` | holdout, validation | 162 GetObject, 267 PutObject | 162 GetObject, 267 PutObject / 82 GetObject, 267 PutObject |
| `mutation.compact-update-one-column` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 2 DeleteObjects, 52 GetObject, 23 PutObject |
| `mutation.lightweight-delete` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 4 DeleteObjects, 116 GetObject, 42 PutObject / 4 DeleteObjects, 68 GetObject, 42 PutObject |
| `mutation.wide-delete` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 4 DeleteObjects, 86 GetObject, 62 PutObject / 4 DeleteObjects, 74 GetObject, 62 PutObject |
| `mutation.wide-update-one-column` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 2 DeleteObjects, 14 GetObject, 19 PutObject / 2 DeleteObjects, 4 GetObject, 19 PutObject |
| `objects-per-part.mid-wide` | holdout, validation | 98 PutObject; `objects_total`, `insert_tx_checks`, `metadata_files_per_object`, `metadata_allocated_per_object`, `write_syscalls_per_object` | 98 PutObject |
| `objects-per-part.narrow-compact` | fit | 33 PutObject | 33 PutObject |
| `objects-per-part.narrow-wide` | fit | 57 PutObject | 57 PutObject |
| `objects-per-part.types-fit` | fit | 30–400 PutObject | 176 PutObject |
| `objects-per-part.types-holdout` | fit, holdout | 316 PutObject; `objects_total`, `insert_tx_checks` | 316 PutObject |
| `objects-per-part.wide-compact` | fit | 33 PutObject | 33 PutObject |
| `objects-per-part.wide-compact-2` | holdout | 33 PutObject; `objects_total`, `insert_tx_checks`, `object_size_histogram` | 33 PutObject |
| `objects-per-part.wide-wide` | fit, holdout | 267 PutObject; `objects_total`, `insert_tx_checks`, `metadata_files_per_object`, `metadata_allocated_per_object`, `metadata_bytes_per_object` | 267 PutObject |
| `startup.intact` | fit | 0–4 DeleteObject, 0–4 DeleteObjects, 0–200 GetObject, 0–10 HeadObject, 0–4 PutObject | 2 DeleteObject, 50 GetObject, 2 PutObject / 3 DeleteObject, 36 GetObject, 3 PutObject |
| `startup.wide-parts` | holdout | 2 DeleteObject, 105 GetObject, 2 PutObject; `active_parts` | 2 DeleteObject, 105 GetObject, 2 PutObject / 3 DeleteObject, 26 GetObject, 3 PutObject |

Every assertion held in every record (part types from the registry predicate, `object_writes == objects_total`, full rewrite of Compact mutations, dropped objects deleted, restart before the cold read, `window.crossing` empty).

## Coefficients {#coefficients}

| Coefficient | Evidence | Value | Tolerance | Fit | Hold-out |
|---|---|---|---|---|---|
| `objects_per_part` | measured | narrow/compact: 11; narrow/wide: 19; types/compact: 22; types/wide: 154; wide/compact: 11; wide/wide: 89 | 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.types-fit`, `objects-per-part.wide-compact` | `objects-per-part.types-holdout`, `objects-per-part.wide-wide` |
| `object_size_distribution` | measured | narrow/compact/1MiB_32MiB: 0.0909; narrow/compact/4KiB_1MiB: 0.0909; narrow/compact/gt_32MiB: 0.0; narrow/compact/le_4KiB: 0.8182; narrow/wide/1MiB_32MiB: 0.2632; narrow/wide/4KiB_1MiB: 0.0526; narrow/wide/gt_32MiB: 0.0; narrow/wide/le_4KiB: 0.6842; wide/compact/1MiB_32MiB: 0.0909; wide/compact/4KiB_1MiB: 0.0909; wide/compact/gt_32MiB: 0.0; wide/compact/le_4KiB: 0.8182; wide/wide/1MiB_32MiB: 0.0; wide/wide/4KiB_1MiB: 0.4607; wide/wide/gt_32MiB: 0.0; wide/wide/le_4KiB: 0.5393 | abs 0.05, rel 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.wide-compact`, `objects-per-part.wide-wide` | `objects-per-part.wide-compact-2` |
| `insert_tx_checks` | measured | 0 | 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.wide-compact` | `objects-per-part.types-holdout`, `objects-per-part.wide-wide` |
| `metadata_bytes_per_object` | measured | 4096 | abs 0, rel 0.08 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.wide-compact` | `objects-per-part.mid-wide`, `objects-per-part.wide-wide` |
| `metadata_iops_per_object_write` | measured | 1.1767 | abs 1.068, rel 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.types-fit`, `objects-per-part.types-holdout`, `objects-per-part.wide-compact`, `objects-per-part.wide-wide` | `objects-per-part.mid-wide` |
| `metadata_iops_per_part_write` | measured | 25.979 | abs 1.068, rel 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.types-fit`, `objects-per-part.types-holdout`, `objects-per-part.wide-compact`, `objects-per-part.wide-wide` | `objects-per-part.mid-wide` |
| `stats_gets_per_column` | measured | 1 | 0 | `cold-select.cache-off` | `cold-select.cache-off-types` |
| `gets_per_stream_read` | measured | 2 | 0 | `cold-select.cache-off` | `cold-select.cache-off-types` |
| `bytes_per_get` | measured | wide: 67992.8; types: 33210.8 | abs 0, rel 0.25 | `cold-select.cache-off`, `cold-select.cache-off-types` | `cold-select.cache-off-types (consumed)`, `cold-select.cache-off-types-2` |
| `shape_columns` | source | narrow: 5; wide: 40; types: 21 | 0 | `cold-select.cache-off`, `cold-select.cache-off-types` | — |
| `cached_miss_gets_per_part_read` | measured | wide: 5; types: 7 | 0 | `cold-select.cache-on`, `cold-select.cache-on-types` | `cold-select.cache-on-types-2` |
| `cached_miss_bytes_per_part_read` | measured | wide: 1003955.0; types: 914467.5 | abs 0, rel 0.25 | `cold-select.cache-on`, `cold-select.cache-on-types` | `cold-select.cache-on-types-2` |
| `cache_fill_io_per_byte` | measured | 1.0159 | abs 0.02, rel 0 | `cold-select.cache-on` | `cold-select.cache-on-types` |
| `startup_gets_per_part` | measured | 0/narrow: 15; 0/wide: 50; 1/narrow: 10; 1/wide: 10 | 0 | `startup.intact` | `startup.wide-parts` |
| `startup_requests` | measured | 0/PutObject: 2; 0/GetObject: 5; 0/DeleteObject: 2; 1/PutObject: 3; 1/GetObject: 6; 1/DeleteObject: 3 | 0 | `startup.intact` | `startup.wide-parts` |
| `merge_gets_per_source_part` | measured | narrow/compact: 8; narrow/wide: 11; wide/compact: 43; wide/wide: 81 | 0 | `merge-forced.narrow-compact-sources`, `merge-forced.narrow-wide-sources`, `merge-forced.wide-compact-sources` | `merge-forced.wide-wide-sources` |
| `merge_gets_per_source_part_cached` | measured | narrow/compact: 3; narrow/wide: 6; wide/compact: 3; wide/wide: 41 | 0 | `merge-forced.narrow-compact-sources`, `merge-forced.narrow-wide-sources`, `merge-forced.wide-compact-sources` | `merge-forced.wide-wide-sources` |
| `delete_calls_per_removed_part` | source | 1 | 0 | `drop.partition`, `drop.table-sync`, `drop.ttl`, `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | — |
| `empty_part_objects` | source | 8 | 0 | `drop.partition`, `drop.ttl` | — |
| `mutation_entry_objects` | source | 1 | 0 | `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | — |
| `mutation_objects_written` | source | delete/compact: 11; delete/wide: 19; lightweight_delete/compact: 11; lightweight_delete/wide: 9; update_column/compact: 11; update_column/wide: 9 | 0 | `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | — |
| `mutation_gets_per_part` | source | 0/delete/compact: 27; 0/delete/wide: 16; 0/lightweight_delete/compact: 30; 0/lightweight_delete/wide: 28; 0/update_column/compact: 26; 0/update_column/wide: 7; 1/delete/compact: 27; 1/delete/wide: 10; 1/lightweight_delete/compact: 29; 1/lightweight_delete/wide: 5; 1/update_column/compact: 26; 1/update_column/wide: 2 | 0 | `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | — |

Source-derived (`source`) counts are exact on the deterministic cards listed as fit; they need no hold-out. Every measured coefficient has a fit edge and a distinct hold-out edge in `coverage.json`, both `done`, per policy.

## Formulas and applicability {#formulas}

`formulas.json` (owner `s3`, variant `s3`) prices eight cells per month from registry inputs; `cache_enabled`
branches the reads, merges, mutation reads, startup and local-disk rows (`*_cached` policies measured the
`cache_enabled = 1` branch). Applicability lists the families that back each cell's coefficients:

| Cell | Families | Rows |
|---|---|---|
| `storage` | `lifecycle` | active `data_bytes` × replicas; outdated: merge sources written per second × `old_parts_lifetime` (uses `merge_effective_fan_in`) |
| `inserts` | `objects_per_part_by_shape`, `thresholds`, `multipart_graph` | `PutObject`, multipart rows, `HeadObject` (`insert_tx_checks`), follower fetch `GetObject` |
| `merges` | `lifecycle` | result `PutObject`, source `GetObject` (cache branch), one `DeleteObjects` per source |
| `mutations` | `mutation_deletes` | `PutObject` per part and per statement, `GetObject` per part, `DeleteObjects` |
| `deletes` | `mutation_deletes` | empty covering part `PutObject`, two `DeleteObjects` per expired part |
| `reads` | `reads_cache` | `GetObject` per part read (statistics rule or cached misses), cache-fill MiB/s |
| `recovery` | `startup_recovery`, `replication` | startup rows; catch-up `GetObject` from `catchup_requests_per_missed_part[s3]` |
| `local_disk` | `objects_per_part_by_shape` | metadata capacity plus cache size, metadata write calls |

The `recovery` cell's catch-up row reads `catchup_requests_per_missed_part`, owned by the replication report
(pending: that report defines its value); the follower `GetObject` row of `inserts` uses this report's
`objects_per_part` (the phase-0 replicated fetch read every object of the part once) until the replication report's
`acquire_requests[s3][insert]` replaces it.

## Assumptions {#assumptions}

Each is `basis: assumption` in the registry or the predictions:

- **Merge results are Wide** in the `merges` cell, and sources have the inserted part type. Reason: results past
  10 MiB are Wide by the predicate; small merges of Compact parts are a minority of merged bytes.
- **Each replica merges locally** (`merges` × `replicas`). Reason: `acquisition_rule` defaults to local execution;
  the fetch share belongs to the replication report.
- **Expiry follows the TTL path** for `deletes` (empty covering part, two `DeleteObjects` per part). Reason: TTL and
  `DROP PARTITION` measured the same counts; `DROP TABLE` differs and is not a steady-state flow.
- **Parts leave retention at their final size** (`expired_parts_per_month` = parts inserted / `merge_size_ratio`).
- **Mutation counts are the narrow shape's** (5 `UInt64` columns). Reason: only that shape was mutated; a wide-shape
  full rewrite writes `objects_per_part[wide][…]`, which the table does not encode.
- **A query's statistics reads recur.** The rule charges `statistics.packed` reads on every part read. Reason: a
  changed part set (any insert or merge) invalidates the cached estimator (`MergeTreeData.cpp:983`–`:988`); a warm
  server with no part changes reuses it, which this report does not measure.
- **Cached misses per part read** are measured for the cold-select query shapes (2–3 columns read); other queries
  read other segment counts.

## Open items and limits {#open-items}

- **Periodic statistics refresh.** `refreshStatistics` reloads every active part's statistics every
  `refresh_statistics_interval` (300 s, `Storages/MergeTree/MergeTreeSettings.cpp:2362`;
  `Storages/MergeTree/MergeTreeData.cpp:3442`). Its request rate is a background cost per replica that no cell
  prices yet: about `active_parts` × (columns + 1) `GetObject` per 300 s at the startup counts.
- **`bytes_per_get`** is measured for the wide and types tables only, not the narrow shape; no cell uses it.
- **Merges of types parts** and their startup counts are not measured: those table cells are absent, so a `types`
  shape makes the merge and recovery cells unavailable.
- **Local write calls** are process syscalls, not device I/O.

## Review {#review}

REVIEW_SECTION

## Reproduce {#reproduce}

```
utils/cas-yardstick/rustfs-bin.sh ensure
YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-s3 --cases all --policies s3,s3_cached --repeats 3 \
  --port-base 19800 --proxy-port-base 19141 --runs-dir tmp/cas-yardstick/cost-runs
```
