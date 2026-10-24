---
description: 'Operation graphs of the s3 disk (R2) for the storage cost calculator: objects per part by shape and type, multipart, merges, mutations, drops, cold reads with and without the cache, startup and local metadata, with source-cited rules, predictions and reconciliation on the 26.8 binary.'
sidebar_label: 'Storage cost s3 graphs (R2)'
sidebar_position: 55
slug: /superpowers/reports/storage-cost-s3
title: 'Storage cost calculator — s3 disk operation graphs (R2)'
doc_type: 'reference'
---

# Storage cost calculator — s3 disk operation graphs (R2) {#s3}

Every request count of the `s3` disk reconciles on the pinned binary: 48 cases on `s3` and `s3_cached`, K=3, four
report runs, each alone on the host. 239 records are ok; the 25 `FAILED` records belong to the
consumed hold-outs and validations (§Review). All 14 remaining hold-out cases land on their committed predictions. Mutation
counts vary between repeats (§Mutation); every other count is identical in all repeats.
The variant's eight cells are in `formulas.json`, their coefficients in `coefficients.json`.

| Item | Value |
|---|---|
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (`tmp/bin/26.8/clickhouse`) |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Report runs | `cost-20261024T094831Z` (09:48–10:16 UTC) for every case but five, `cost-20261024T121249Z` for the hold-out `mutation.update-mixed-4`, `cost-20261024T115024Z` for the consumed `mutation.update-mixed-3`, `cost-20261024T123052Z` for the 1000-row validations and `cost-20261024T132311Z` for the small-part cases; each alone on the host under `tmp/yardstick.lock`, rig commit `7f9ef7459c8`. `measurements.jsonl` holds their 239 records |
| Consumed hold-outs | `drop.partition-3` (falsified count), `mutation.update-mixed` and `mutation.update-mixed-2` (exact counts missed by the repeat spread), `mutation.update-mixed-3` (its band was a stochastic tolerance, which the mutation card did not apply; the card now refuses a manifest mode it does not run), `mutation.compact-small` and `mutation.compact-30k` (falsified forms of the granule rule). All their records stay in `measurements.jsonl`, 25 of them `FAILED` |
| Invalid runs | `cost-20261024T073405Z`, `cost-20261024T074526Z`, `cost-20261024T075016Z`, `cost-20261024T075431Z`, `cost-20261024T075800Z`, `cost-20261024T083200Z`, `cost-20261024T084830Z` overlapped CAS or EBS runs on the same host and RustFS. No number of this report comes from them |
| Predictions | fit predictions `80cd02f1c27` were written after exploratory runs; hold-outs `80cd02f1c27`, `202d45aadbc`, `5519040a680`, `09e9319a11f`, `b04768eaa9e`, each before the first run of its case. The invalid runs executed most hold-outs before the report run; no predicted count changed after that (later commits only added registry inputs and the commit ids), so the report run tests each committed count |
| Withdrawn hold-outs | the `bytes_per_get` band of `cold-select.cache-off-types` and the `cache_fill_io_per_byte` check of `cold-select.cache-on-types` failed in an invalid run and were revised; both cases are fits now, `cold-select.cache-off-types-2` and `cold-select.cache-on-types-2` are the hold-outs |
| Store | RustFS behind the ledger proxy; every record replays from `raw/<card>/<run_id>/ledger.jsonl.gz` |
| Cuts | `detach-attach.default` and `cache-eviction.2x` (P2): cut by owner directive D14, no manifest |

Cards: `utils/cas-yardstick/yardstick/cost/cards/shapes.py` (`objects-per-part`, `big-insert-multipart`) and
`cards/ops.py` (`mutation`, `drop`, `merge-only`, `cold-select`, `startup`). Requests are attributed to part files
through `system.remote_data_paths` snapshots before and after the window (`requests_by_file` in the records); keys no
snapshot lists are `unlisted`. Row counts run after the window: over lightweight-deleted parts a `count()` reads
`_row_exists`.

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
  `:224`). Streams per column type, read from `objects-per-part.types-fit-wide`:

  | Type | Streams | Files |
  |---|---|---|
  | `UInt64` | value | 2 |
  | `String` | chars, sizes (`DataTypes/Serializations/SerializationString.cpp:692`) | 4 |
  | `Nullable(UInt64)` | value, null map (`DataTypes/Serializations/SerializationNullable.cpp:53`) | 4 |
  | `Array(UInt32)` | value, `size0` (`DataTypes/Serializations/SerializationArray.cpp:268`) | 4 |
  | `LowCardinality(String)` | keys, dictionary (`DataTypes/Serializations/SerializationLowCardinality.cpp:86`) | 4 |
  | `Map(String, UInt64)` | `keys`, `keys.size`, `values`, `size0` (`DataTypes/DataTypeMap.cpp:77`) | 8 |
  | `JSON` with paths `a` (Int64) and `b` (String) | object structure, shared data, per path: dynamic structure, variant discriminators, the variant streams (`DataTypes/Serializations/SerializationObject.cpp:198`, `:261`) | 24 |

- **Skip indexes and projections.** A skip index whose serialized size stays under `packed_skip_index_max_bytes`
  (1 MiB, `Storages/MergeTree/MergeTreeSettings.cpp:2051`) is packed into the part's one `skp_idx.packed`
  (`Storages/MergeTree/MergeTreeIndicesSerialization.h:16`); a larger one spills to its own files, and full-text
  indexes are never packed (`Storages/MergeTree/MergeTreeDataPartWriterOnDisk.cpp:170`). The measured indexes are
  packed. A projection is a part of its own under `<name>.proj/`, Compact here: 10 objects.
- **Shapes.** narrow = 5 `UInt64` columns, wide = 40, types = the 21-column schema of `objects-per-part.types-fit`
  (`k` plus 5 `Nullable(UInt64)`, 5 `Array(UInt32)`, 5 `LowCardinality(String)`, 3 `Map`, 2 `JSON`, a `minmax` index,
  one projection). The `types` row of `objects_per_part` holds for that schema only; `types-holdout` (another mix)
  validates the per-stream rule above: 158 objects per Wide part, predicted and measured. The part type of every
  insert is computed from the registry predicate `part_type_predicate` and asserted against `system.parts`.

### Multipart {#multipart}

A file past the first buffer goes multipart once a second buffer is detached (`IO/WriteBufferFromS3.cpp:397`,
`:407`). The next buffers are 16 MiB and double every 500 buffers up to 5 GiB (`Common/BufferAllocationPolicy.cpp:51`,
`:80`–`:92`); the registry's `multipart_parts` covers that recurrence. `big-insert-multipart.multipart`: `s.bin` of
600 000 100-byte values with `CODEC(NONE)` is 60 001 850 B (one 25-byte frame per 8192-row granule; the sizes are a
separate stream), uploaded as 33 554 432 + 16 777 216 + 9 670 202 B: one `CreateMultipartUpload`, three `UploadPart`,
one `CompleteMultipartUpload` and 14 `PutObject` for the part's other files. Merge and mutation outputs use the same
writer: their large files are multipart too (the `merges` and `mutations` cells price them).

### Merge {#merge}

`merge-only` inserts its sources in the setup; the window holds only `OPTIMIZE TABLE … FINAL`. A merge writes its
result part (objects as an insert of that type) and reads its sources:

| Source | Without cache | With cache |
|---|---|---|
| Compact, 5 columns | 8 (columns + 3) | 3 |
| Compact, 40 columns | 43 (columns + 3) | 3 |
| Wide, 5 columns | 11 (2 × columns + 1) | 6 (columns + 1) |
| Wide, 40 columns, 3 sources (hold-out) | 81 predicted, 81 measured | 41 predicted, 41 measured |

A Compact reader seeks per column inside `data.bin` (`Storages/MergeTree/MergeTreeReaderCompact.cpp:219`); a Wide
reader opens one stream per file and reads up to the last mark it needs
(`Storages/MergeTree/MergeTreeReaderStream.cpp:270`). Through the cache, a file is fetched in cache segments, so a
small file costs one request. Merged-away sources stay outdated until `old_parts_lifetime` (480 s), then go with one
`DeleteObjects` per part (keys batched by 1000: `IO/S3Defines.h:52`,
`Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:599`); the cell prices that removal as its own flow
(`merge_removals_per_month`). These counts replace the lifecycle report's assumption ranges: its 64 `GetObject` for 8
sources (`merge-forced.default`) and 16 for 2 are 8 per 5-column Compact source.

### Mutation {#mutation}

- **Compact part: full rewrite.** A part that is not Wide is mutated by rewriting all columns
  (`Storages/MergeTree/MutateTask.cpp:908`): 11 new objects, none kept (asserted by object count:
  `full_rewrite.<table>.kept_objects = 0`).
- **Wide part: changed streams only.** Untouched streams are hard links to the same objects
  (`Storages/MergeTree/MutateTask.cpp:1247`); `checksums.txt`, `columns.txt` and the other metadata files are always
  rewritten (`:1162`). `UPDATE u1 = u1 + 1` writes 9 objects per part (`u1.bin`, `u1.cmrk2` and 7 metadata files) and
  keeps 10; a lightweight `DELETE` writes `_row_exists.bin`, `_row_exists.cmrk2` and the same 7; `ALTER DELETE`
  rewrites all 19.
- **Mutation entries.** A plain `MergeTree` writes one `mutation_<n>.txt` object per statement
  (`Storages/MergeTree/MergeTreeMutationEntry.cpp:73`) and, past `finished_mutations_to_keep` (100,
  `Storages/MergeTree/MergeTreeSettings.cpp:1769`), removes the oldest with one `DeleteObject`
  (`Storages/StorageMergeTree.cpp:2394`, `:2415`). A replicated table keeps its entries in Keeper
  (`Storages/StorageReplicatedMergeTree.cpp:9034`): no object.
- **Afterwards:** the source part is outdated; with `old_parts_lifetime = 1` in the cards its unshared objects go in
  one `DeleteObjects` per part inside the window. Parts the cleanup removes in the same pass can share a call: 3
  calls for 4 parts in one repeat of `mutation.wide-delete`, 5 for 6 in `mutation.update-mixed`. One call per removed
  part is the upper bound the cells price.
- **Reads per part** (`mutation_gets_per_part`, narrow shape, the least over the repeats): delete compact: 3, delete wide: 16, lightweight_delete compact: 3, lightweight_delete wide: 14, update_column compact: 2, update_column wide: 7, delete compact: 3 (cache), delete wide: 10 (cache), lightweight_delete compact: 3 (cache), lightweight_delete wide: 3 (cache), update_column compact: 2 (cache), update_column wide: 2 (cache). For a
  Compact part the table holds the reads besides `data.bin`. A mutation reads `data.bin` with one `GetObject` plus two
  seeks per granule (`Storages/MergeTree/MergeTreeReaderCompact.cpp:219`), a trailing granule counting when at least
  half full, at least one pair: `mutation_compact_gets_per_full_granule` = 2, so 1 + 2 × max(1, round(rows /
  `index_granularity`)). The first form, full granules only, was falsified by its hold-out `mutation.compact-30k` (4
  pairs at 3.66 granules, consumed). The rounded form fits 1000, 10 000, 30 000 and 100 000 rows (1, 1, 4 and 12
  pairs); its hold-outs `mutation.compact-20k` and `mutation.compact-45k` (5.49 granules: 5 pairs), predicted before
  their run, land exactly (12 and 24 `GetObject` for two parts). A Wide part reads one `.bin` range per stream whatever its size. A Compact part read takes one more `GetObject`
  of `data.bin` in some repeats: the table's tolerance is 1 per part. The exact hold-outs `mutation.update-mixed` and
  `mutation.update-mixed-2` missed by that spread (82 `GetObject` against 80 in one of six records) and are consumed;
  `mutation.update-mixed-3` stated the spread as a stochastic tolerance, which the deterministic card ignores (3 of
  its 6 records fail an exact DeleteObjects check while inside the band): consumed. The hold-out
  `mutation.update-mixed-4` (2 Compact and 3 Wide parts, another column) states the same spread as ranges, committed
  before its first run, and lands inside them.
- A part whose rows the predicate never matches is not rewritten: the first smoke run of `mutation.wide-delete`
  hit this branch (`k % 10 = 0` against odd `k` only), so the manifests insert contiguous `k`.

### Drop and expiry {#drop}

- `DROP PARTITION` covers each dropped part with its own empty part (`Storages/StorageMergeTree.cpp:2949`, `:2724`,
  `:2742`): 8 `PutObject` per dropped part, then removes the dropped and the empty parts (`:3064`), one
  `DeleteObjects` each. `drop.partition-3` predicted one empty part for three and missed (24 `PutObject`, 6
  `DeleteObjects`); the hold-out `drop.partition-2` then landed on 16 and 4.
- TTL expiry (`TTL d + INTERVAL 1 SECOND`, `merge_with_ttl_timeout = 1`): the TTL merge writes an empty part (8
  objects); the source and the empty part are removed. A TTL merge may take several expired parts
  (`Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.cpp:27`): the cell prices
  `expiry_operations_per_month` empty parts and `expired_parts_per_month` + operations removals.
- `DROP TABLE SYNC`: one `DeleteObjects` per part and one `DeleteObject` for the table's `format_version.txt`
  (`Storages/StorageMergeTree.cpp:529`); a validation case, not a monthly flow.

### Reads and cache {#reads}

- **First read of a part.** Statistics part pruning (`Storages/MergeTree/MergeTreeDataSelectExecutor.cpp:840`)
  builds each part's estimates once and keeps them on the part (`Storages/MergeTree/IMergeTreeDataPart.cpp:1475`):
  loading them reads `statistics.packed` once per column statistics file (`:1386`). The marks of each stream are
  loaded once into the mark cache (`Storages/MergeTree/MergeTreeMarksLoader.cpp:109`). A cold part read is
  `stats_gets_per_column` × columns + `gets_per_stream_read` × streams = columns + 2 × streams: 4 parts × (40 + 2 × 3)
  = 184 (`cold-select.cache-off`), hold-out 2 × (21 + 2 × 4) = 58 (`cold-select.cache-off-types`).
- **Later reads.** With estimates and marks cached, a part read is one `.bin` range per stream
  (`Storages/MergeTree/MergeTreeReaderStream.cpp:270`): `warm_gets_per_stream_read` = 1; 12 for 4 parts × 3 streams
  (`cold-select.warm-off`), hold-out 8 for 2 × 4 (`cold-select.warm-off-types`). Statistics pruning runs on every
  candidate part, before primary-key pruning (`Processors/QueryPlan/ReadFromMergeTree.cpp:3317`); the cell charges it
  on parts read.
- **Statistics refresh.** Every `refresh_statistics_interval` (300 s, `Storages/MergeTree/MergeTreeSettings.cpp:2362`)
  a table whose active parts changed since the last refresh reloads every active part's statistics
  (`Storages/MergeTree/MergeTreeData.cpp:3425`, `:3442`); an unchanged set skips it. The startup cases measure one
  refresh: columns + 1 per part (the archive index once). The cell charges columns per part per refresh while
  inserts arrive.
- **Bytes per GET.** The statistics reads ask for the archive from their offset to its end (`Range: bytes=764-`), so
  their bytes follow the archive size: 67 993 B per GET on the wide table, 33 211 B on the types table.
- **With the cache** (`s3_cached`; `enable_filesystem_cache_on_write_operations` is off by default,
  `Core/Settings.cpp:7282`, so inserts do not fill it): a cold read downloads segments, 5 GETs per part read on the
  wide table, 7 on the types table (`cached_miss_gets_per_part_read`); an identical read sends no request
  (`cold-select.cache-on*`). The cache allocates 1.016 to 1.026 bytes per downloaded byte
  (`Interpreters/FileCache/FileSegment.cpp:418`).

### Startup {#startup}

`Cluster.stop(1)`, `Cluster.start(1)` and a `count()` per table, inside the window:

- **Disk access checks:** a probe file is written, read twice (whole and with an offset) and removed
  (`Disks/IDisk.cpp:217`, `:254`). Without the cache two checks run: 2 `PutObject`, 4 `GetObject`, 2 `DeleteObject`.
  The cache disk adds a third, whose two reads are one `GetObject` through the cache. Each start also reads one
  1-byte object outside the parts (by its size, the table's `format_version.txt`): 5 fixed `GetObject` without the
  cache, 6 with it.
- **Part load** (`Storages/MergeTree/IMergeTreeDataPart.cpp:1504`): 9 small files per part (8 metadata files and the
  marks the index granularity is read from) and the statistics refresh at table start
  (`Storages/MergeTree/MergeTreeData.cpp:3289`): `statistics.packed` once per column plus its index. Narrow Compact:
  15 per part (50 = 3 × 15 + 5); hold-out wide Wide: 50 per part (105 = 2 × 50 + 5). With the cache, the archive is
  one segment: 10 per part (36 and 26).
- **History.** Outdated parts load in the background after start (`Storages/MergeTree/MergeTreeData.cpp:3234`) with
  the same reads per part, and a plain `MergeTree` reads its retained `mutation_<n>.txt` files
  (`Storages/StorageMergeTree.cpp:1655`). The cards have no history; the cell prices both from `outdated_parts` and
  `retained_mutation_entries` as assumptions.
- `startup_seconds` 2.1–3.2 s for these tables (start to answer plus the loads).

### Local disk {#local-disk}

- **Metadata:** one local file per object under `<replica>/s3_meta/`, 110–121 B apparent, 4096 B allocated on ext4
  (`Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.cpp:116`); the types tables allocate 4407 B per
  object. The capacity row holds the metadata of active parts and of outdated parts kept for `old_parts_lifetime`
  (`Storages/MergeTree/MergeTreeData.cpp:4230`–`:4257`), plus the cache size; headroom is the billing contract's.
- **Write calls and cache fill** are measured (`metadata_iops_per_object_write`, `metadata_iops_per_part_write`,
  `cache_fill_io_per_byte`) but not priced: they are process syscalls and allocation growth, not device I/O, and the
  EBS report owns the conversion to provisioned IOPS and throughput.

## Cases and reconciliation {#reconciliation}

Deterministic cases compare every request op of the window with the prediction (`ledger.unpredicted` forbids any
other op); fit predictions of counts the fit measures are assumption ranges, hold-outs are exact. The measured
fixtures (`utils/storage-cost-calculator/test/pipeline_eval.test.js`) then evaluate the cells with each prediction's
registry inputs and must reproduce every fit and hold-out record exactly. Values are identical in all three repeats and
both policies unless two are shown (`s3` / `s3_cached`).

| Case | Role | Predicted | Measured (`s3` / `s3_cached`) |
|---|---|---|---|
| `big-insert-multipart.multipart` | — | 1 CompleteMultipartUpload, 1 CreateMultipartUpload, 14 PutObject, 3 UploadPart; `upload_part_sizes` | 1 CompleteMultipartUpload, 1 CreateMultipartUpload, 14 PutObject, 3 UploadPart |
| `cold-select.cache-off` | fit | 1–400 GetObject | 184 GetObject |
| `cold-select.cache-off-types` | fit, holdout | 58 GetObject | 58 GetObject |
| `cold-select.cache-off-types-2` | holdout, validation | 58 GetObject; `bytes_per_get` | 58 GetObject |
| `cold-select.cache-on` | fit | 0–400 GetObject | no request |
| `cold-select.cache-on-types` | fit | no request; `cache_fill_io_per_byte` | no request |
| `cold-select.cache-on-types-2` | holdout | no request; `warm_get_requests`, `warm_get_bytes_per_part`, `cache_fill_io_per_byte` | no request |
| `cold-select.warm-off` | fit | 1–200 GetObject | 12 GetObject |
| `cold-select.warm-off-types` | holdout | 8 GetObject | 8 GetObject |
| `drop.partition` | fit | 0–40 DeleteObject, 1–40 DeleteObjects, 0–200 GetObject, 0–100 PutObject | 2 DeleteObjects, 8 PutObject |
| `drop.partition-2` | holdout | 4 DeleteObjects, 16 PutObject | 4 DeleteObjects, 16 PutObject |
| `drop.partition-3` | consumed hold-out | 4 DeleteObjects, 8 PutObject | 6 DeleteObjects, 24 PutObject |
| `drop.table-sync` | validation | 0–40 DeleteObject, 1–40 DeleteObjects, 0–200 GetObject, 0–100 PutObject | 1 DeleteObject, 2 DeleteObjects |
| `drop.ttl` | fit | 0–40 DeleteObject, 1–40 DeleteObjects, 0–200 GetObject, 0–100 PutObject | 2 DeleteObjects, 8 PutObject |
| `merge-only.narrow-compact-1k` | fit | 8–80 GetObject, 11–19 PutObject | 64 GetObject, 11 PutObject / 24 GetObject, 11 PutObject |
| `merge-only.narrow-compact-2k` | holdout | 64 GetObject, 11 PutObject | 64 GetObject, 11 PutObject / 24 GetObject, 11 PutObject |
| `merge-only.narrow-compact-sources` | fit | 64 GetObject, 19 PutObject | 64 GetObject, 19 PutObject / 24 GetObject, 19 PutObject |
| `merge-only.narrow-wide-sources` | fit | 33 GetObject, 19 PutObject | 33 GetObject, 19 PutObject / 18 GetObject, 19 PutObject |
| `merge-only.wide-compact-sources` | fit | 129 GetObject, 89 PutObject | 129 GetObject, 89 PutObject / 9 GetObject, 89 PutObject |
| `merge-only.wide-wide-sources-3` | holdout | 243 GetObject, 89 PutObject | 243 GetObject, 89 PutObject / 123 GetObject, 89 PutObject |
| `mutation.compact-10k` | fit, validation | 1–2 DeleteObjects, 8–10 GetObject, 23 PutObject | 2 DeleteObjects, 8 GetObject, 23 PutObject |
| `mutation.compact-20k` | holdout | 1–2 DeleteObjects, 12–14 GetObject, 23 PutObject | 2 DeleteObjects, 12 GetObject, 23 PutObject |
| `mutation.compact-30k` | consumed hold-out | 1–2 DeleteObjects, 16–18 GetObject, 23 PutObject | 2 DeleteObjects, 20 GetObject, 23 PutObject |
| `mutation.compact-45k` | holdout | 1–2 DeleteObjects, 24–26 GetObject, 23 PutObject | 2 DeleteObjects, 24 GetObject, 23 PutObject |
| `mutation.compact-small` | consumed hold-out | 1–2 DeleteObjects, 52–54 GetObject, 23 PutObject | 2 DeleteObjects, 8 GetObject, 23 PutObject |
| `mutation.compact-small-2` | fit, validation | 1–2 DeleteObjects, 8–10 GetObject, 23 PutObject | 2 DeleteObjects, 8 GetObject, 23 PutObject |
| `mutation.compact-update-one-column` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 2 DeleteObjects, 52 GetObject, 23 PutObject |
| `mutation.lightweight-delete` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 4 DeleteObjects, 82 GetObject, 42 PutObject / 4 DeleteObjects, 60–62 GetObject, 42 PutObject |
| `mutation.update-mixed` | consumed hold-out | 6 DeleteObjects, 99 GetObject, 62 PutObject | 5–6 DeleteObjects, 99 GetObject, 62 PutObject / 5–6 DeleteObjects, 84–85 GetObject, 62 PutObject |
| `mutation.update-mixed-2` | consumed hold-out | 6 DeleteObjects, 80 GetObject, 60 PutObject | 6 DeleteObjects, 80–82 GetObject, 60 PutObject / 6 DeleteObjects, 60 GetObject, 60 PutObject |
| `mutation.update-mixed-3` | consumed hold-out | 5 DeleteObjects, 92 GetObject, 53 PutObject | 3–5 DeleteObjects, 92 GetObject, 53 PutObject / 4–5 DeleteObjects, 82 GetObject, 53 PutObject |
| `mutation.update-mixed-4` | holdout, validation | 2–5 DeleteObjects, 73–75 GetObject, 51 PutObject | 4–5 DeleteObjects, 73 GetObject, 51 PutObject / 5 DeleteObjects, 58 GetObject, 51 PutObject |
| `mutation.wide-delete` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 3–4 DeleteObjects, 86 GetObject, 62 PutObject / 4 DeleteObjects, 74–76 GetObject, 62 PutObject |
| `mutation.wide-update-one-column` | fit | 0–40 DeleteObject, 0–40 DeleteObjects, 0–400 GetObject, 4–200 PutObject | 2 DeleteObjects, 14 GetObject, 19 PutObject / 2 DeleteObjects, 4 GetObject, 19 PutObject |
| `objects-per-part.mid-wide` | holdout, validation | 98 PutObject; `objects_total`, `insert_tx_checks`, `metadata_files_per_object`, `metadata_allocated_per_object`, `write_syscalls_per_object` | 98 PutObject |
| `objects-per-part.narrow-compact` | fit | 33 PutObject | 33 PutObject |
| `objects-per-part.narrow-compact-1k` | validation | 33 PutObject | 33 PutObject |
| `objects-per-part.narrow-wide` | fit | 57 PutObject | 57 PutObject |
| `objects-per-part.types-fit` | fit, validation | 30–400 PutObject | 176 PutObject |
| `objects-per-part.types-fit-compact` | fit | 22 PutObject | 22 PutObject |
| `objects-per-part.types-fit-wide` | fit | 154 PutObject | 154 PutObject |
| `objects-per-part.types-holdout` | fit, validation | 316 PutObject; `objects_total`, `insert_tx_checks` | 316 PutObject |
| `objects-per-part.wide-compact` | fit | 33 PutObject | 33 PutObject |
| `objects-per-part.wide-compact-2` | holdout | 33 PutObject; `objects_total`, `insert_tx_checks`, `object_size_histogram` | 33 PutObject |
| `objects-per-part.wide-wide` | fit, holdout | 267 PutObject; `objects_total`, `insert_tx_checks`, `metadata_files_per_object`, `metadata_allocated_per_object`, `metadata_bytes_per_object` | 267 PutObject |
| `startup.intact` | fit | 0–4 DeleteObject, 0–4 DeleteObjects, 0–200 GetObject, 0–10 HeadObject, 0–4 PutObject | 2 DeleteObject, 50 GetObject, 2 PutObject / 3 DeleteObject, 36 GetObject, 3 PutObject |
| `startup.small` | validation | 2 DeleteObject, 50 GetObject, 2 PutObject | 2 DeleteObject, 50 GetObject, 2 PutObject / 3 DeleteObject, 36 GetObject, 3 PutObject |
| `startup.wide-parts` | holdout | 2 DeleteObject, 105 GetObject, 2 PutObject; `active_parts` | 2 DeleteObject, 105 GetObject, 2 PutObject / 3 DeleteObject, 26 GetObject, 3 PutObject |

Every assertion held in every ok record (part types from the registry predicate, `object_writes == objects_total`, full rewrite of Compact mutations, dropped objects deleted, restart before the cold read, `window.crossing` empty).

## Coefficients {#coefficients}

| Coefficient | Evidence | Value | Tolerance | Fit | Hold-out |
|---|---|---|---|---|---|
| `objects_per_part` | measured | narrow/compact: 11; narrow/wide: 19; types/compact: 22; types/wide: 154; wide/compact: 11; wide/wide: 89 | 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.types-fit-compact`, `objects-per-part.types-fit-wide`, `objects-per-part.wide-compact` | `objects-per-part.wide-compact-2`, `objects-per-part.wide-wide` |
| `object_size_distribution` | measured | narrow/compact/1MiB_32MiB: 0.0909; narrow/compact/4KiB_1MiB: 0.0909; narrow/compact/gt_32MiB: 0.0; narrow/compact/le_4KiB: 0.8182; narrow/wide/1MiB_32MiB: 0.2632; narrow/wide/4KiB_1MiB: 0.0526; narrow/wide/gt_32MiB: 0.0; narrow/wide/le_4KiB: 0.6842; wide/compact/1MiB_32MiB: 0.0909; wide/compact/4KiB_1MiB: 0.0909; wide/compact/gt_32MiB: 0.0; wide/compact/le_4KiB: 0.8182; wide/wide/1MiB_32MiB: 0.0; wide/wide/4KiB_1MiB: 0.4607; wide/wide/gt_32MiB: 0.0; wide/wide/le_4KiB: 0.5393 | abs 0.05, rel 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.wide-compact`, `objects-per-part.wide-wide` | `objects-per-part.wide-compact-2` |
| `insert_tx_checks` | measured | 0 | 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.wide-compact` | `objects-per-part.wide-compact-2`, `objects-per-part.wide-wide` |
| `metadata_bytes_per_object` | measured | 4096 | abs 0, rel 0.08 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.wide-compact` | `objects-per-part.mid-wide`, `objects-per-part.wide-wide` |
| `metadata_iops_per_object_write` | measured | 1.1727 | abs 0.939, rel 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.types-fit`, `objects-per-part.types-holdout`, `objects-per-part.wide-compact`, `objects-per-part.wide-wide` | `objects-per-part.mid-wide` |
| `metadata_iops_per_part_write` | measured | 31.758 | abs 0.939, rel 0 | `objects-per-part.narrow-compact`, `objects-per-part.narrow-wide`, `objects-per-part.types-fit`, `objects-per-part.types-holdout`, `objects-per-part.wide-compact`, `objects-per-part.wide-wide` | `objects-per-part.mid-wide` |
| `stats_gets_per_column` | measured | 1 | 0 | `cold-select.cache-off` | `cold-select.cache-off-types` |
| `gets_per_stream_read` | measured | 2 | 0 | `cold-select.cache-off` | `cold-select.cache-off-types` |
| `bytes_per_get` | measured | wide: 67992.8; types: 33210.8 | abs 0, rel 0.25 | `cold-select.cache-off`, `cold-select.cache-off-types` | `cold-select.cache-off-types-2` |
| `shape_columns` | source | narrow: 5; wide: 40; types: 21 | 0 | `cold-select.cache-off`, `cold-select.cache-off-types` | `cold-select.cache-off-types-2` |
| `cached_miss_gets_per_part_read` | measured | wide: 5; types: 7 | 0 | `cold-select.cache-on`, `cold-select.cache-on-types` | `cold-select.cache-on-types-2` |
| `cached_miss_bytes_per_part_read` | measured | wide: 1003955.0; types: 914467.5 | abs 0, rel 0.25 | `cold-select.cache-on`, `cold-select.cache-on-types` | `cold-select.cache-on-types-2` |
| `cache_fill_io_per_byte` | measured | 1.0208 | abs 0.01, rel 0 | `cold-select.cache-on`, `cold-select.cache-on-types` | `cold-select.cache-on-types-2` |
| `warm_gets_per_stream_read` | measured | 1 | 0 | `cold-select.warm-off` | `cold-select.warm-off-types` |
| `startup_gets_per_part` | measured | 0/narrow: 15; 0/wide: 50; 1/narrow: 10; 1/wide: 10 | 0 | `startup.intact` | `startup.wide-parts` |
| `startup_requests` | measured | 0/PutObject: 2; 0/GetObject: 5; 0/DeleteObject: 2; 1/PutObject: 3; 1/GetObject: 6; 1/DeleteObject: 3 | 0 | `startup.intact` | `startup.wide-parts` |
| `merge_gets_per_source_part` | measured | narrow/compact: 8; narrow/wide: 11; wide/compact: 43; wide/wide: 81 | 0 | `merge-only.narrow-compact-1k`, `merge-only.narrow-compact-sources`, `merge-only.narrow-wide-sources`, `merge-only.wide-compact-sources` | `merge-only.narrow-compact-2k`, `merge-only.wide-wide-sources-3` |
| `merge_gets_per_source_part_cached` | measured | narrow/compact: 3; narrow/wide: 6; wide/compact: 3; wide/wide: 41 | 0 | `merge-only.narrow-compact-1k`, `merge-only.narrow-compact-sources`, `merge-only.narrow-wide-sources`, `merge-only.wide-compact-sources` | `merge-only.narrow-compact-2k`, `merge-only.wide-wide-sources-3` |
| `delete_calls_per_removed_part` | source | 1 | 0 | `drop.partition`, `drop.ttl`, `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | `drop.partition-2`, `drop.partition-3 (consumed)` |
| `empty_part_objects` | source | 8 | 0 | `drop.partition`, `drop.ttl` | `drop.partition-2`, `drop.partition-3 (consumed)` |
| `mutation_entry_objects` | source | 1 | 0 | `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | `mutation.update-mixed-4` |
| `mutation_objects_written` | measured | delete/compact: 11; delete/wide: 19; lightweight_delete/compact: 11; lightweight_delete/wide: 9; update_column/compact: 11; update_column/wide: 9 | 0 | `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | `mutation.compact-20k`, `mutation.compact-45k`, `mutation.update-mixed (consumed)`, `mutation.update-mixed-2 (consumed)`, `mutation.update-mixed-3 (consumed)`, `mutation.update-mixed-4` |
| `mutation_compact_gets_per_full_granule` | measured | 2 | 0 | `mutation.compact-10k`, `mutation.compact-small-2`, `mutation.compact-update-one-column` | `mutation.compact-20k`, `mutation.compact-30k (consumed)`, `mutation.compact-45k` |
| `mutation_gets_per_part` | measured | 0/delete/compact: 3; 0/delete/wide: 16; 0/lightweight_delete/compact: 3; 0/lightweight_delete/wide: 14; 0/update_column/compact: 2; 0/update_column/wide: 7; 1/delete/compact: 3; 1/delete/wide: 10; 1/lightweight_delete/compact: 3; 1/lightweight_delete/wide: 3; 1/update_column/compact: 2; 1/update_column/wide: 2 | abs 1, rel 0 | `mutation.compact-update-one-column`, `mutation.lightweight-delete`, `mutation.wide-delete`, `mutation.wide-update-one-column` | `mutation.compact-20k`, `mutation.compact-45k`, `mutation.update-mixed (consumed)`, `mutation.update-mixed-2 (consumed)`, `mutation.update-mixed-3 (consumed)`, `mutation.update-mixed-4` |

Source-derived (`source`) counts are exact on the deterministic cards listed as fit; they need no hold-out. Every measured coefficient has a fit edge and a distinct hold-out edge in `coverage.json`, both `done`, per policy.

## Formulas and applicability {#formulas}

`formulas.json` (owner `s3`, variant `s3`) prices eight cells per month; `cache_enabled` branches reads, merges,
mutation reads, startup and local capacity (`*_cached` policies measured the `cache_enabled = 1` branch).
Applicability lists the families whose cards back each cell's coefficients:

| Cell | Families | Rows |
|---|---|---|
| `storage` | `lifecycle` | active `data_bytes` × replicas; outdated: merged-away bytes, ingest × (`write_amplification_rule` − 1), held for `old_parts_lifetime` |
| `inserts` | `objects_per_part_by_shape`, `thresholds`, `multipart_graph` | `PutObject`, multipart rows, `HeadObject` (`insert_tx_checks`), follower fetch `GetObject` |
| `merges` | `lifecycle` | result writes with multipart rows, source `GetObject` (cache branch), one `DeleteObjects` per removed source |
| `mutations` | `mutation_deletes` | writes per Compact and Wide part with multipart rows, `mutation_<n>.txt` writes and removals (plain `MergeTree` only), `GetObject` per part, `DeleteObjects` |
| `deletes` | `mutation_deletes` | one empty part per expiry operation, one `DeleteObjects` per expired part and per empty part |
| `reads` | `reads_cache` | warm stream reads, first reads of new parts and after restarts, statistics refreshes (no cache); cached misses (cache) |
| `recovery` | `startup_recovery` | access checks, part and history loads per restart; `not_supported` while `missed_parts_per_restart` > 0 |
| `local_disk` | `objects_per_part_by_shape` | metadata of active and outdated parts plus the cache size |

The follower `GetObject` row of `inserts` uses `objects_per_part` (the phase-0 replicated fetch read every object of
the part once) until the replication report's `acquire_requests[s3][insert]` replaces it. Catch-up after a restart
belongs to the replication report (`pending_owner: replication` in the cell's state and note): catch-up `GetObject` =
`missed_parts_per_restart` × `catchup_requests_per_missed_part[s3]`, and `startup_requests_after_restart[s3]` replaces
the startup rows. Until that report publishes both, the `recovery` cell is `not_supported` for workloads with missed
parts: the contract loader rejects a reference to a coefficient no report carries.

## Assumptions {#assumptions}

Each is `evidence.kind: assumed` in the registry or stated in a prediction:

- **Merge results are Wide** (`objects_per_part[shape][wide]`) and sources have the inserted part type. Reason:
  results past 10 MiB are Wide by the predicate; small merges of Compact parts carry few bytes.
- **Each replica merges locally** (`merges` × `replicas`). Reason: `acquisition_rule` defaults to local execution;
  the fetch share belongs to the replication report.
- **Every new part is read once** (`parts_first_read_per_month` = inserted parts + merge results), and a restart
  makes every active part cold again. Reason: any query over recent data touches new parts.
- **Statistics refresh runs every interval while inserts arrive.** Reason: any insert or merge changes the active set
  within 300 s at the rates the page models.
- **One expired part per expiry operation** by default (`expiry_operations_per_month` = expired parts).
- **Parts leave retention at their final size** (`expired_parts_per_month` = parts inserted / `merge_size_ratio`).
- **Mutation counts are the narrow shape's** (5 `UInt64` columns). The granule rule is measured on `UPDATE`; `DELETE`
  and lightweight `DELETE` of Compact parts are assumed to seek the same way.
- **Mutated parts are at their final size** (`mutated_part_rows` = `part_rows` × `merge_size_ratio`), like the
  expired parts.
- **A merge result is a first-level merge** (`merge_result_rows` = `part_rows` × `merge_effective_fan_in`) for its
  part type: most merge results are first-level, and later levels are Wide sooner.
- **Cache misses scale with missed bytes**: `cache_miss_fraction` is a byte share and multiplies the measured
  segment downloads per part read; the measured query shapes read 3–4 streams.
- **Large files** of merge results and mutated parts are inputs (`large_objects_per_merge_result`,
  `large_objects_per_mutated_part`, default 0) with `system.parts_columns` hints.
- **Outdated parts and retained mutation entries at a restart** load like active parts and one GET per entry.

## Open items and limits {#open-items}

- **`bytes_per_get`** is measured for the wide and types tables only; no cell uses it.
- **Merges and startup of the `types` shape** are not measured: those table cells are absent, so a `types` shape makes
  the merge and recovery cells unavailable.
- **Block I/O demand** of the local disk is not priced (see §Local disk).
- **Statistics pruning candidates** can exceed parts read (`ReadFromMergeTree.cpp:3317`); the cell charges parts read.
- **Small inserts.** The 1000-row validations `objects-per-part.narrow-compact-1k` and `startup.small`, predicted
  before their run, land exactly: `objects_per_part`, `insert_tx_checks`, `metadata_bytes_per_object` and the startup
  tables reach 1000 rows. `mutation.compact-small` missed and is consumed: a 1000-row Compact part mutates with 4
  `GetObject`, not 26. The granule rule above models it, so the mutation tables now reach 1000 rows. Merges of small
  sources write a Compact result: the `merges` cell takes the result's part type from the part type predicate at
  the merge result size (`merge_result_part_type`), fitted at 1000-row sources and held out at 2000
  (`merge-only.narrow-compact-1k`, `-2k`: 11 objects, 8 `GetObject` per source). `object_size_distribution`, `metadata_iops_per_object_write` and `metadata_iops_per_part_write`
  are `reported_only` in `coefficients.json`: no cell prices them, so they keep their measured row range (object sizes
  do change with rows: `data.bin` moves from the 1–32 MiB bucket to 4 KiB–1 MiB).
- **A TTL merge over several expired parts** is not measured: `drop.ttl` expires one part. The cell's split of
  `expiry_operations_per_month` and `expired_parts_per_month` follows the source; its multi-part branch is unvalidated.
- **Startup history** (outdated parts, retained `mutation_<n>.txt`) is priced from the source, not measured: the
  startup cards have no history.

## Review {#review}

One Codex round (`gpt-6-astra`, reasoning high, read-only; prompt `tmp/codex_s3_prompt.txt`, log
`tmp/codex_s3_r1.log`) on the revision built from `cost-20261024T075800Z`, a run later found invalid (it overlapped
other rigs): 18 MAJOR, 5 MINOR. 16 MAJOR are closed; M16 and M17 are closed in the formulas, and their measurement is
parked (§Open items). The card defect M2 is fixed with a regression test (`test_cost_cards_ops.WindowContentsTest`,
same commit `09e9319a11f`). Every number below comes from the report run above. No second round: one per report.

| # | Finding | Disposition |
|---|---|---|
| M1 | Mutation entries are removed past `finished_mutations_to_keep` | Closed: `mutations` prices one `DeleteObject` per new entry once 100 are kept (`mutation_entries_removed_per_month`) |
| M2 | A completion `count()` read `_row_exists` inside the lightweight-delete window (34 / 8 GETs) | Closed test-first: row counts run in the teardown, after the window; rerun |
| M3 | The types hold-out (158 objects) does not validate the table value 154 | Closed: the `types` row is the types-fit schema's, fitted by `types-fit-compact` and `types-fit-wide`; `types-holdout` is a validation of the per-stream rule; the table's hold-outs are `wide-wide` and `wide-compact-2` |
| M4 | Mutation GET counts were labelled source-derived | Closed: `mutation_objects_written` and `mutation_gets_per_part` are measured. The report run showed the counts vary between repeats (+1 `GetObject` per Compact part read, cleanup batching of `DeleteObjects`): the exact hold-outs `mutation.update-mixed` and `mutation.update-mixed-2` missed and are consumed; `mutation.update-mixed-3` stated the band as a stochastic tolerance the card does not apply and is consumed too; `mutation.update-mixed-4`, committed with ranges before its first run, is the hold-out |
| M5 | The cache-fill hold-out was reused after its failure drove a fix | Closed: that failure was in an invalid run, so no record of it is cited. `cold-select.cache-on-types` is a fit; `cold-select.cache-on-types-2` is the hold-out |
| M6 | Spans claimed 25 and 26 columns | Closed: spans count top-level columns from the manifests (`Map(String, UInt64)` is one) |
| M7 | Outdated bytes used a part-count factor | Closed: ingest × (`write_amplification_rule` − 1) held for `old_parts_lifetime` |
| M8 | Local capacity omitted outdated parts' metadata | Closed: outdated objects (removals per second × `old_parts_lifetime` × objects) are added; headroom stays with billing |
| M9 | Write syscalls and cache allocation were priced as provisioned IOPS and throughput | Closed: those rows are removed; the coefficients stay measured for the EBS report |
| M10 | Statistics refresh omitted; its rule misstated | Closed: `reads` prices the conditional refresh (columns per active part per 300 s while parts change); §Reads states the skip of unchanged sets |
| M11 | Replication marked supported in `recovery` without a row | Closed: `recovery` lists `startup_recovery` only and is `not_supported` while `missed_parts_per_restart` > 0; its state and note name `catchup_requests_per_missed_part[s3]` and `startup_requests_after_restart[s3]` with `pending_owner: replication` |
| M12 | `cache_miss_fraction` hint measured bytes, the formula used part reads | Closed: the input is a byte share; the cell multiplies segment downloads, which scale with missed bytes (assumption stated) |
| M13 | Statistics reads come from part pruning, cached per part | Closed: the graph and the `reads` cell separate first reads (statistics and marks) from warm reads, measured by `cold-select.warm-off` and its hold-out `warm-off-types` |
| M14 | Skip indexes over 1 MiB and full-text indexes are not packed | Closed: §Insert states the branch; the measured layouts are packed |
| M15 | Merge and mutation outputs can be multipart | Closed: both cells price large files as multipart uploads (inputs with `system.parts_columns` hints) |
| M16 | One TTL merge can expire several parts | Closed in the formula, measurement parked: `deletes` separates expiry operations from expired parts. The hold-out `drop.partition-3` misses (DROP PARTITION covers each part with its own empty part, `StorageMergeTree.cpp:2724`) and is consumed; `drop.partition-2` holds. A multi-part TTL merge is not measured: ordinary merges of the sources can run before expiry, so a case needs merges held off first |
| M17 | Startup omits outdated parts and mutation files | Closed in the formula, measurement parked: `recovery` loads `outdated_parts` like active ones and one GET per retained mutation entry, from the source; the startup cards have no history |
| M18 | Replicated tables keep mutation entries in Keeper | Closed: the entry rows apply only when `replicas` = 1 |
| m1 | `String` size-stream citation | Fixed: `SerializationString.cpp:692` (predictions too; no expected value changed) |
| m2 | Buffer growth after 500 parts | Fixed in §Multipart |
| m3 | Fit predictions followed the exploratory run | Stated in the header table |
| m4 | `shape_columns` evidence | Fixed: cites the manifest schema |
| m5 | "every op exactly" overstated fits | Fixed in §Reconciliation |

## Reproduce {#reproduce}

The run must be alone on the host: other rigs bias timings and share RustFS.

```
flock tmp/yardstick.lock sh -c 'utils/cas-yardstick/rustfs-bin.sh ensure; YARDSTICK_LIVE=1 python3 \
  utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-s3 --cases all --policies s3,s3_cached --repeats 3 \
  --runs-dir tmp/cas-yardstick/cost-runs'
```
