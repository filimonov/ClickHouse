---
description: 'Lifecycle research (R5) of the storage cost calculator: part-type and multipart boundaries, parts per insert, natural and forced merges, write amplification and async inserts on the 26.8 binary, with source-cited rules, predictions and reconciliation.'
sidebar_label: 'Storage cost lifecycle (R5)'
sidebar_position: 54
slug: /superpowers/reports/storage-cost-lifecycle
title: 'Storage cost calculator — lifecycle rules (R5)'
doc_type: 'reference'
---

# Storage cost calculator — lifecycle rules (R5) {#lifecycle}

Shared lifecycle definitions every variant's cost cells build on: which part type an insert or a merge produces,
how a part file is split into upload requests, how many parts an insert creates, how merges grow parts and rewrite
bytes, and when a replica fetches a result instead of producing it. Each rule cites `tmp/src-26.8` at
`09d0772a842c5d73cf3ef35b4c8b017d838215a0`; the registry quantities carrying them are in
`docs/superpowers/cost-model/inputs-registry.json` (blocks `schema`, `inserts`, `merges`, `replication`).

## Operation graphs {#operation-graphs}

### Part type {#part-type}

- **Predicate.** A part is Compact when its uncompressed bytes < `min_bytes_for_wide_part`, its rows <
  `min_rows_for_wide_part` or its level < `min_level_for_wide_part`, else Wide
  (`src/Storages/MergeTree/MergeTreeData.cpp:6399`, `:6404`). Defaults: 10 485 760 bytes outside ClickHouse Cloud
  (`MergeTreeSettings.cpp:37`), 0 rows (`:85`), level 0 (`:82`). Without adaptive granularity every part is Wide
  (`MergeTreeData.cpp:6394`).
- **Insert.** The size is the block in memory, `Block::bytes` (`MergeTreeDataWriter.cpp:848`, `:915`): 8 bytes per
  `UInt64` value. An inserted part of a plain `MergeTree` is level 0 (`:735`).
- **Merge.** The size is the sum of the inputs' on-disk uncompressed column bytes
  (`FutureMergedMutatedPart.cpp:26`); a Compact part reports its `data.bin` uncompressed size
  (`MergeTreeDataPartCompact.cpp:155`). The level is max(input levels) + 1 (`FutureMergedMutatedPart.cpp:31`). The
  result type is the minimum of the input types and the chosen type, with Wide < Compact (`:27`, `:32`): a merge
  with any Wide input is Wide.
- **Mutation.** The result keeps the source part's format (`MutateFromLogEntryTask.cpp:120`).
- **Storage type.** Packed storage follows the same predicate on the `*_full_part_storage` settings, all 0 by
  default (`MergeTreeData.cpp:6407`): every part here is Full, one object per file.

### Part files {#part-files}

A Compact part writes `data.bin` and `data.cmrk4` (`MergeTreeDataPartWriterCompact.cpp:53`, `:59`,
`MergeTreeIndexGranularityInfo.cpp:98`); a Wide part writes `<column>.bin` and `<column>.cmrk2` per column
(`MergeTreeIndexGranularityInfo.cpp:96`) and `columns_substreams.txt` (`MergeTreeDataPartWriterOnDisk.cpp:705`).
Both write `primary.cidx` only with a primary key (`MergeTreeDataPartWriterOnDisk.cpp:119`) and `count.txt`,
`serialization.json`, `statistics.packed`, `columns.txt`, `columns_substreams.txt`, `metadata_version.txt`,
`default_compression_codec.txt`, `checksums.txt` (`MergedBlockOutputStream.cpp:407`–`:493`). For the 5-column
`UInt64` tables of these cards: 11 objects per Compact part, 19 per Wide part. These counts belong to the `s3`
graph (R2); the lifecycle cards only use them to bound requests.

### Multipart sizing {#multipart-sizing}

`WriteBufferFromS3` takes buffer sizes from `ExpBufferAllocationPolicy`:

- buffer 1 is clamp(`max_single_part_upload_size`, `min_upload_part_size`, `max_upload_part_size`) = 32 MiB
  (`src/Common/BufferAllocationPolicy.cpp:51`, `src/IO/S3Defines.h:26`);
- buffers 2 and later are `min_upload_part_size` = 16 MiB (`BufferAllocationPolicy.cpp:52`, `S3Defines.h:24`);
- every 500 buffers the size doubles, up to 5 GiB (`BufferAllocationPolicy.cpp:85`, `S3Defines.h:29`–`:30`).

The upload becomes multipart only when a second buffer is detached (`WriteBufferFromS3.cpp:154`). At finalize, one
buffer of at most `max_single_part_upload_size` bytes is a single `PutObject` (`:180`). So an object of exactly
32 MiB is one `PutObject`, and 32 MiB + 8 bytes is `CreateMultipartUpload`, `UploadPart` of 32 MiB and 8 bytes,
`CompleteMultipartUpload`. Registry: `multipart_sizing_rule`, `multipart_parts`.

The cards size one file exactly: a `CODEC(NONE)` `UInt64` column of a Wide part. A block is cut before a granule
once it holds `min_compress_block_size` = 65 536 bytes (`MergeTreeDataPartWriterWide.cpp:473`,
`src/Core/Settings.cpp:131`), and a full 8192-row granule (`MergeTreeSettings.cpp:72`) is exactly 65 536 bytes. Each
block adds a 9-byte header (`src/Compression/CompressionInfo.h:17`) and a 16-byte checksum
(`CompressedWriteBuffer.cpp:39`). The file is 65 561 bytes per full granule plus 8·r + 25 for a last granule of r
rows; 4 192 704 rows give exactly 33 554 432 bytes.

### Parts per insert {#parts-per-insert}

- **Synchronous.** An insert into `MergeTree` squashes blocks until either `min_insert_block_size_rows` (1 048 449,
  `Settings.cpp:209`) or `min_insert_block_size_bytes` (268 402 944, `:232`) is reached and never splits a block
  (`src/Interpreters/Squashing.cpp:395`, `InterpreterInsertQuery.cpp:504`). An `INSERT SELECT` keeps the SELECT's
  `max_block_size` (65 409, `src/Core/Defines.h:33`) because `optimize_trivial_insert_select` is off
  (`Settings.cpp:5609`), so its parts hold 17 source blocks, 1 111 953 rows. Formatted input is parsed into blocks
  of `max_insert_block_size` = 1 048 449 rows (`Settings.cpp:178`) that each reach the minimum, so
  `ceil(rows / min_insert_block_size_rows)` parts per partition. Registry: `parts_per_insert` (that formula; ±1 part for `INSERT SELECT` just above the minimum).
- **Async.** `async_insert` is on by default in this build (`Settings.cpp:7225`). Inserts with the same query shape
  and settings share a buffer, flushed into one part per partition when its unparsed bytes reach
  `async_insert_max_data_size` (10 MiB), when `async_insert_max_query_number` inserts accumulate with deduplication
  on, or at its deadline (`AsynchronousInsertQueue.cpp:663`, `:680`). The deadline is the buffer's first insert plus
  an adaptive timeout between `async_insert_busy_timeout_min_ms` (50) and `_max_ms` (200) that grows while inserts
  are frequent (`:623`, `:758`; `Settings.cpp:7249`, `:7252`). With `wait_for_async_insert = 1` (`:7228`) a client
  waits for its flush, so only concurrent inserts share one. Registry: `async_insert_flush_rule`,
  `inserts_per_async_flush`.

### Merges and outdated parts {#merges}

- A natural merge takes 2 to `max_parts_to_merge_at_once` = 100 parts
  (`Compaction/MergeSelectors/SimpleMergeSelector.cpp:283`, `MergeTreeSettings.cpp:636`); `OPTIMIZE FINAL` takes
  every part of the partition and ignores that limit (`MergeTreeDataMergerMutator.cpp:320`). The cards stop natural
  merges with `max_parts_to_merge_at_once = 1`, which only the natural selector reads
  (`Compaction/MergeSelectorApplier.cpp:112`).
- A merged-away part is kept `old_parts_lifetime` = 480 s before removal (`MergeTreeSettings.cpp:556`,
  `MergeTreeData.cpp:4256`); the outdated bytes cost storage for that long.
- **Write amplification.** Bytes written by inserts and merges per inserted byte. The rule
  (`write_amplification_rule`) treats merges as levels of effective fan-in f: a part grown to r inserted parts has had
  its bytes rewritten L(r) = n + (r / fⁿ − 1)/(f − 1) times, n = floor(log_f r), and amplification is 1 + L(r). f is
  the measured coefficient `merge_effective_fan_in`; the registry rule is this function as a 12-level ladder.

### Acquisition: fetch or execute {#acquisition}

A replica executes a replicated merge itself unless (`MergeFromLogEntryTask.cpp`):

- `always_fetch_merged_part` is set (`:101`);
- `execute_merges_on_single_replica_time_threshold` > 0 picks another replica (`:130`); this branch applies to
  merges only (`ReplicatedMergeTreeMergeStrategyPicker.cpp:62`) and, with zero-copy,
  `remote_fs_execute_merges_on_single_replica_time_threshold` (`:105`);
- a source part is missing locally (`:155`);
- the entry is older than `prefer_fetch_merged_part_time_threshold` (3600 s) and its sources hold at least
  `prefer_fetch_merged_part_size_threshold` (10 GiB) on disk while another replica has the result (`:224`, `:233`).

A mutation follows the same rule with `always_fetch_mutated_part` (`MutateFromLogEntryTask.cpp:85`, `:105`, `:140`);
its single-replica branch never fires because the picker accepts merge entries only. A replica that already has the
part or a covering one skips the entry (`StorageReplicatedMergeTree.cpp:2695`). Registry: `acquisition_rule` and its
settings. Measuring the split is Task 5 (`replication-*` cards).
