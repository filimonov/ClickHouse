---
description: 'Lifecycle research (R5) of the storage cost calculator: part-type and multipart boundaries, parts per insert, natural and forced merges, write amplification and async inserts on the 26.8 binary, with source-cited rules, predictions and reconciliation.'
sidebar_label: 'Storage cost lifecycle (R5)'
sidebar_position: 54
slug: /superpowers/reports/storage-cost-lifecycle
title: 'Storage cost calculator — lifecycle rules (R5)'
doc_type: 'reference'
---

# Storage cost calculator — lifecycle rules (R5) {#lifecycle}

Every lifecycle rule below reconciles on the `s3` policy of the pinned binary: 15 cases × K=3, 45 records, all ok.
Part types, part sizes and upload part sequences match their predictions exactly, including both source hold-outs
(`boundaries.merge-cross`, `boundaries.multipart-equal`). The fitted hold-out `merge-natural.wide` lands inside its
committed bounds. The write amplification rule fits with effective fan-in 6.14, and merged-away parts are accounted
as outdated storage.

| Item | Value |
|---|---|
| Binary, downloaded | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` |
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Report run | `cost-20261024T064154Z`, rig commit `474392fb9ad`; `measurements.jsonl` holds its 45 records |
| Earlier runs | `cost-20261024T044126Z` (rig `199348a48f3`, 42 records): the fit data of the `merge-natural.wide` prediction. `cost-20261024T045917Z` (rig `5e8f5165369`, 45 records): the first hold-out run, before the review fixes. `cost-20261024T054012Z` (rig `9dbe21126be`) and `cost-20261024T061359Z` (rig `d53e470cdb5`), 45 records each: after the round-1 and round-2 fixes. All ok, archived as `raw/runs/<run>/measurements.jsonl.gz` |
| Predictions | source-only `2d931d33a05` (+ `fcaec9179c7` for `insert-split`), fitted `b652973295f`; each before every run that it predicts. Citations corrected in `9dbe21126be` without changing any expected value |
| Store | RustFS behind the ledger proxy; every record replays from `raw/<card>/<run_id>/ledger.jsonl.gz` |

Shared lifecycle definitions every variant's cost cells build on: which part type an insert or a merge produces,
how a part file is split into upload requests, how many parts an insert creates, how merges grow parts and rewrite
bytes, and when a replica fetches a result instead of producing it. Each rule cites `tmp/src-26.8` at
`09d0772a842c5d73cf3ef35b4c8b017d838215a0`; the registry quantities carrying them are in
`docs/superpowers/cost-model/inputs-registry.json` (blocks `schema`, `inserts`, `merges`, `replication`).

## Operation graphs {#operation-graphs}

### Part type {#part-type}

- **Predicate.** A part starts Wide and is Compact when its uncompressed bytes < `min_bytes_for_wide_part`, its
  rows < `min_rows_for_wide_part` or its level < `min_level_for_wide_part`
  (`src/Storages/MergeTree/MergeTreeData.cpp:6399`, `:6402`). Defaults: 10 485 760 bytes outside ClickHouse Cloud
  (`MergeTreeSettings.cpp:37`), 0 rows (`:85`), level 0 (`:82`).
- **Overrides.** Every part is Wide/Full without adaptive granularity (`MergeTreeData.cpp:6394`;
  adaptive needs `index_granularity_bytes` != 0 and either `enable_mixed_granularity_parts` or no non-adaptive part,
  `:11680`–`:11681`) and when a column uses
  `CODEC(Quantize('pq', ...))` (`:6428`). Registry: `part_type` branches on `adaptive_granularity` and
  `quantize_pq_codec` before the predicate.
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

`WriteBufferFromS3` takes buffer sizes from a policy (`WriteBufferFromS3.cpp:91`). With `strict_upload_part_size`
set, every buffer is that size (`BufferAllocationPolicy.cpp:98`); with the default 0 (`S3Defines.h:28`),
`ExpBufferAllocationPolicy`:

- buffer 1 is clamp(`max_single_part_upload_size`, `min_upload_part_size`, `max_upload_part_size`) = 32 MiB
  (`src/Common/BufferAllocationPolicy.cpp:51`, `src/IO/S3Defines.h:26`);
- buffers 2 and later are `min_upload_part_size` = 16 MiB (`BufferAllocationPolicy.cpp:52`, `S3Defines.h:24`);
- every 500 buffers the size doubles, up to 5 GiB (`BufferAllocationPolicy.cpp:85`, `S3Defines.h:29`–`:30`).

The upload becomes multipart when a second buffer is detached (`WriteBufferFromS3.cpp:154`) or, at finalize, when
the only buffer holds more than `max_single_part_upload_size` (`:178`–`:198`). One buffer of at most that size is a
single `PutObject` (`:180`). So an object is multipart exactly when it exceeds the smaller of the first buffer (the
strict size, else the clamped first buffer) and `max_single_part_upload_size`. A first buffer clamped below
32 MiB (`max_upload_part_size` < 32 MiB) or a strict size makes smaller objects multipart; the defaults give neither.
By default an object of exactly 32 MiB is one `PutObject`, and 32 MiB + 8 bytes is `CreateMultipartUpload`,
`UploadPart` of 32 MiB and 8 bytes, `CompleteMultipartUpload`. Registry: `multipart_sizing_rule`, `multipart_parts`
(with the strict branch; no growth step, so valid below 499 later parts) and `multipart_upload`.

The cards size one file exactly: a `CODEC(NONE)` `UInt64` column of a Wide part. A block is cut before a granule
once it holds `min_compress_block_size` = 65 536 bytes (`MergeTreeDataPartWriterWide.cpp:473`,
`src/Core/Settings.cpp:131`), and a full 8192-row granule (`MergeTreeSettings.cpp:72`) is exactly 65 536 bytes. Each
block adds a 9-byte header (`src/Compression/CompressionInfo.h:17`) and a 16-byte checksum
(`CompressedWriteBuffer.cpp:39`). The file is 65 561 bytes per full granule plus 8·r + 25 for a last granule of r
rows; 4 192 704 rows give exactly 33 554 432 bytes.

### Parts per insert {#parts-per-insert}

- **Synchronous.** An insert into `MergeTree` joins whole source blocks until either `min_insert_block_size_rows`
  (1 048 449, `Settings.cpp:209`) or `min_insert_block_size_bytes` (268 402 944, `:232`) is reached, then emits them
  as one block; it never splits a block (`src/Interpreters/Squashing.cpp:207`–`:246`, `:395`;
  `InterpreterInsertQuery.cpp:504`). With `use_strict_insert_block_limits` (off, `Settings.cpp:162`) max limits
  would split blocks; the rule does not cover that. An `INSERT SELECT` reads source blocks of `max_block_size`
  (65 409, `src/Core/Defines.h:33`, `Settings.cpp:155`) because `optimize_trivial_insert_select` is off
  (`Settings.cpp:5609`), so its parts hold 17 source blocks, 1 111 953 rows. Formatted data is parsed into blocks
  of `max_insert_block_size` = 1 048 449 rows (`Settings.cpp:178`); a native client sends its own blocks
  (`TCPHandler.cpp:1473`), which may be larger. Registry: `parts_per_insert` = ceil(rows / emitted rows) per
  partition, emitted rows = the smaller of the enabled minimums (`min_insert_block_size_rows`, the byte minimum in
  rows), each rounded up to whole source blocks (`insert_source_block_rows`: `max_block_size` for `INSERT SELECT`,
  the input `insert_data_block_rows` for data); with both minimums 0 every source block is a part
  (`Squashing.cpp:397`).
- **Async eligibility.** `async_insert` is on by default in this build (`Settings.cpp:7225`); a table's own
  `async_insert` setting also enables it (`MergeTreeData.cpp:9993`). Only an INSERT with data goes to the queue:
  `INSERT SELECT` stays synchronous (`executeQuery.cpp:2894`, native protocol `TCPHandler.cpp:1500`). Data over the
  size limit falls back to a synchronous insert: inline data whose raw bytes exceed `async_insert_max_data_size`
  (`AsynchronousInsertQueue.cpp:501`, `:528`), native data whose decoded block bytes reach it
  (`TCPHandler.cpp:1414`–`:1435`, `Squashing.cpp:395`). Registry input `insert_protocol` picks the measure. A
  native INSERT over the limit is forwarded as one combined block of its first limit's worth, then the rest; the
  registry formula does not cover that case. Every insert of the lifecycle cards except `async-inserts.*` is an
  `INSERT SELECT`.
- **Async flush.** Inserts with the same user, roles, query shape, settings and data kind share a buffer
  (`AsynchronousInsertQueue.cpp:587`). It is flushed into one part per partition when its whole entries' sizes
  reach `async_insert_max_data_size` (10 MiB): decoded `Block::bytes` for a native entry, the raw data length for
  an inline one (`AsynchronousInsertQueue.h:131`–`:138`). It also flushes when `async_insert_max_query_number`
  (450) entries accumulate with deduplication on, or at its deadline (`AsynchronousInsertQueue.cpp:663`, `:680`).
  Deduplication is on by default: `deduplicate_insert = enable` (`Settings.cpp:2357`) supersedes the legacy flags;
  with `backward_compatible_choice` the queue reads `async_insert_deduplicate` when the query setting made the
  INSERT async and `insert_deduplicate` when only the table setting did (`DeduplicateInsert.cpp:26`–`:41`,
  `TCPHandler.cpp:1495`–`:1498`). The deadline is
  the buffer's first insert plus an adaptive timeout between `async_insert_busy_timeout_min_ms` (50) and `_max_ms`
  (200) that grows while inserts are frequent (`:623`, `:739`–`:774`; `Settings.cpp:7249`, `:7252`). With
  `wait_for_async_insert = 1` (`:7228`) a client waits for its flush, so only concurrent clients share one.
- **Async estimate.** Registry `inserts_per_async_flush` is an estimate for evenly spaced arrivals (evidence
  `assumed`), not a source derivation: max(1, min(1 + rate × timeout_max, ceil(buffer / entry bytes), clients when
  waiting, 450 with deduplication)). The first insert opens the buffer and every insert before the longest deadline
  joins it. Bursts share more (`async-inserts.bursty`: 100 concurrent inserts over about 1.9 flushes, where the
  mean rate of 50/s gives 11), a shorter adaptive timeout less. `async-inserts.steady` measured 4.8–5.0 inserts per
  flush against 5. `insert_clients` counts clients waiting at once (`PendingAsyncInsert`), not query starts.

### Merges and outdated parts {#merges}

- A simple-selector merge takes 2 to `max_parts_to_merge_at_once` = 100 parts
  (`Compaction/MergeSelectors/SimpleMergeSelector.cpp:283`, `MergeTreeSettings.cpp:636`,
  `Compaction/MergeSelectorApplier.cpp:112`). Other background merges: TTL merges have their own selectors (TTL drop
  also reads the limit, `MergeSelectorApplier.cpp:80`), and a whole-partition force merge runs when
  `min_age_to_force_merge_seconds` with `min_age_to_force_merge_on_partition_only` is set
  (`MergeTreeDataMergerMutator.cpp:318`, off by default: `MergeTreeSettings.cpp:754`, `:761`). The cards' tables have
  no TTL and those settings off, so `max_parts_to_merge_at_once = 1` stops every background merge of theirs.
  `OPTIMIZE FINAL` merges each partition with final = true and selects all its parts
  (`src/Storages/StorageMergeTree.cpp:2497`, `:1831`).
- A merged-away part may be removed once it is `old_parts_lifetime` = 480 s old (`MergeTreeSettings.cpp:556`,
  `MergeTreeData.cpp:4256`); removal also waits for references and transactions and runs on the cleanup schedule
  (`:4230`–`:4259`), so 480 s is a lower bound of the retention. The lifecycle records classify these objects as
  `outdated` from their merge's `part_log` time (see [Changes](#changes)).
- **Write amplification.** Bytes written by inserts and merges per inserted byte. The rule
  (`write_amplification_rule`) treats merges as levels of effective fan-in f: a part grown to r inserted parts has had
  its bytes rewritten L(r) = n + (r / fⁿ − 1)/(f − 1) times, n = floor(log_f r), capped at 12 levels, and
  amplification is 1 + L(r). r is the row-weighted final part size, sum(r²) / sum(r) over the final parts
  (`merge_size_ratio`). f is the measured coefficient `merge_effective_fan_in`, fitted to exactly this formula
  (`lifecycle.amplification_rule`).

### Acquisition: fetch or execute {#acquisition}

A replica skips a merge or mutation entry when it already has the part or a covering one
(`StorageReplicatedMergeTree.cpp:2695`). Otherwise a merge entry goes, in order (`MergeFromLogEntryTask.cpp`):

1. **Fetch** when `always_fetch_merged_part` is set (`:101`).
2. **Try to fetch** a TTL-recompress merge assigned to another replica, quietly retrying while no replica has it,
   until `try_fetch_recompressed_part_timeout` (`:111`).
3. **Fetch** when `execute_merges_on_single_replica_time_threshold` > 0, the entry is younger than it, and the
   picker names another replica (`:130`; `ReplicatedMergeTreeMergeStrategyPicker.cpp:59`–`:63`, merge entries only).
   `remote_fs_execute_merges_on_single_replica_time_threshold` only decides whether the picker refreshes its state
   (`:105`–`:119`), not this choice.
4. **Fetch** when a source part is missing or covered by another part (`:155`, `:166`), or a patch part is missing
   (`:201`).
5. **Try to fetch**, quietly retrying, while a source part has newer metadata than the table (`:187`).
6. **Fetch** when the entry is at least `prefer_fetch_merged_part_time_threshold` (3600 s) old (inclusive), its
   sources hold at least `prefer_fetch_merged_part_size_threshold` (10 GiB) on disk, and a replica has the result
   (`:224`, `:233`).
7. With zero-copy (`allow_remote_fs_zero_copy_replication`, off), **fetch** when a replica has a covering part, and
   **try to fetch** when another replica holds the exclusive lock (`:296`–`:349`).
8. Else **merge locally**.

Every branch that does not merge returns an unsuccessful preparation, and the task then calls `executeFetch`
(`ReplicatedMergeMutateTaskBase.cpp:207`–`:208`); `need_to_check_missing_part_in_fetch = false` only makes a missing
result a quiet retry instead of an error.

A mutation entry (`MutateFromLogEntryTask.cpp`): fetch when `always_fetch_mutated_part` (`:85`), when the source is
missing or covered (`:105`, `:122`), or when the entry is at least the time threshold old and the estimated result
size, 1.1 × the source's bytes on disk with expired-TTL parts excluded (`:138`; `Compaction/CompactionStatistics.cpp:36`,
`:48`), reaches the size threshold while a replica has the result (`:140`). The single-replica branch (`:162`) never
fires: the picker accepts merge entries only. Zero-copy as for merges (`:199`). Else mutate locally, keeping the
source part type. Registry: `acquisition_rule` and its settings. Measuring the split is Task 5 (`replication-*`
cards).

## Cases and reconciliation {#reconciliation}

Deterministic cases (`mode: deterministic`) are compared exactly: every request op of the window and every
predicted observation. Merge reads are the only ranges: the `s3` graph (R2) owns exact `GetObject` counts, so the
lifecycle predictions bound them as an assumption. Values are identical in all three repeats unless a range is shown.

| Case | Role | Predicted | Measured (K=3) |
|---|---|---|---|
| `boundaries.compact-below-bytes` | fit | 235 929 rows, 9 437 160 B: Compact; 11 `PutObject` | Compact, 9 437 160 B; 11 |
| `boundaries.wide-equal-bytes` | fit | exactly 10 485 760 B: Wide (strict `<`); 19 `PutObject` | Wide; 19 |
| `boundaries.wide-above-bytes` | fit | 11 534 360 B: Wide; 19 `PutObject` | Wide; 19 |
| `boundaries.level-cross` | fit | `min_level_for_wide_part = 1`: 2 × Compact at level 0 (11 534 360 B each), merge Wide at level 1; 41 `PutObject`, 4–40 `GetObject` | as predicted; 41, 16 |
| `boundaries.merge-cross` | hold-out | 2 × Compact (6 291 480 B each), merge Wide (12 582 960 B); 41 `PutObject`, 4–40 `GetObject` | as predicted; 41, 16 |
| `boundaries.multipart-below` | fit | `x.bin` 33 554 424 B: one `PutObject`; 10 `PutObject` | one `PutObject` of 33 554 424 B; 10 |
| `boundaries.multipart-equal` | hold-out | `x.bin` 33 554 432 B: one `PutObject`; 10 `PutObject` | one `PutObject` of 33 554 432 B; 10 |
| `boundaries.multipart-above` | fit | `x.bin` 50 331 656 B: parts 33 554 432, 16 777 216, 8; 9 `PutObject` + create, 3 `UploadPart`, complete | exactly that sequence |
| `boundaries.insert-split` | fit | 2 621 122 rows: parts of 1 111 953, 1 111 953, 397 216 rows, all Wide; 57 `PutObject` | as predicted; 57 |
| `merge-forced.default` | fit | 8 × Compact, one merge of fan-in 8 to a level-1 Wide part, row amplification 2; 107 `PutObject`, 16–160 `GetObject` | as predicted; 107, 64 |
| `small-inserts.default` | fit | 1000 parts; 11 000–29 981 `PutObject` | 1000 parts, 194–197 merges, amplification 3.981–4.092; 13 150–13 175 |
| `merge-natural.default` | fit | 64 parts; 704–1901 `PutObject` | 9 merges of 6 then 2 of 8, final parts 23, 23, 6, 6, 6, amplification 2.5522 (one repeat: final parts 27, 23, 6, 6, 1, 1, amplification 2.6142); 913 |
| `merge-natural.wide` | hold-out | amplification 2.8030 ± 0.3474 (f = 6.1912 from the pre-run); fan-in TV ≤ 0.3426; result-size TV ≤ 0.3852; fan-in mean 5.06–7.36; 352–1871 `PutObject` | 5 merges of 6, 1 of 7, one final part; amplification 2.9094; TV 0.0503 and 0.0647; mean 6.17; 646 |
| `async-inserts.steady` | fit (P2) | 1000 async inserts at 20/s: 200–1000 parts | 205, 204, 205 parts |
| `async-inserts.bursty` | hold-out (P2) | 10 bursts of 100: 10–1000 parts | 19, 17, 17 parts |

Every merge case also asserts outdated byte-hours > 0: the objects of each merged-away part are reclassified
`outdated` at its `MergeParts` time (`inventory.reclassified`, one entry per merge), and the replay reproduces them.

Observed for the other workstreams:

- **Merge reads (R2).** A merge reads 8 `GetObject` per 5-column Compact source part (16 for 2 parts, 64 for 8).
- **Outdated storage.** In the natural-merge windows outdated bytes outweigh active ones: `merge-natural.default`
  3.69–3.83 M byte-hours outdated against 2.36 M active, `small-inserts.default` 1.91–1.95 M against 0.38–0.39 M.
  Merged-away parts wait at least `old_parts_lifetime`, longer than every window here.

## Coefficients {#coefficients}

`coefficients.json` holds the measured coefficients; the pinned values live as registry defaults with their source
references. Fit domain of all three: 5 `UInt64` columns, inserts of 1000–100 000 rows, back-to-back, row-weighted
final part sizes of 18–473 inserted parts; the hold-out extends it to 20 columns and 50 000 rows. Machine-readable validity
(`merge_size_ratio`, `insert_rows`) and the record's `domain` field carry it.

| Coefficient | Value | Fit | Hold-out | Tolerance |
|---|---|---|---|---|
| `merge_effective_fan_in` | 6.1437 | `small-inserts.default`, `merge-natural.default` (6 records) against `write_amplification_rule`; residuals −0.195…−0.109 and +0.150…+0.170 | `merge-natural.wide`: residual +0.091 against this value; the committed prediction (f = 6.1912, levels averaged per final part) +0.106 | abs 0.2502 (rule below), rel 0; the hold-out's committed bound was 0.3474 |
| `merge_fan_in_distribution` | 6: 0.880, 7–8: 0.115, 9–12: 0.005 | same 6 records | TV 0.0519 to this table, 0.0503 to the committed reference | TV ≤ 0.3426 |
| `merge_result_size_distribution` | 5–8: 0.828, 17–32: 0.095, 65–128: 0.032, 33–64: 0.021, 129–256: 0.016, 257–512: 0.005, 9–16: 0.002, 513+: 0.001 | same 6 records | TV 0.0638 to this table restricted to ≤ 32, 0.0647 to the committed reference | TV ≤ 0.3852 |

- **Amplification is on bytes on disk.** Row amplification is higher (5.30–5.47 against 3.98–4.09 on
  `small-inserts`): a 1000-row Compact part carries fixed-size metadata files, and merged parts compress better.
- **The fit is the registry formula.** f is fitted to 1 + L(r) at the row-weighted final part size, as
  `write_amplification_rule` computes it from `merge_size_ratio`. The committed hold-out prediction used the
  average of L over the final parts with the pre-run's f; at the hold-out's single final part both forms agree.
- **Tolerances come from the model, not K.** Merge structure was identical across repeats in `merge-natural.wide`
  and in two of three in `merge-natural.default`, and close in `small-inserts`, so K=3 spread alone gives a narrow
  band. The rules are in the hold-out
  prediction's `inputs.tolerance_rules`: amplification abs = largest fit residual + half the largest K=3 range;
  distribution TV = 2 × largest fit TV + 1 / predicted merges.
- **Validation.** `amplification-run.2h` (one 1000-row insert per second for 2 h) has its manifest and a prediction
  with the registry formula and this fit (f = 6.1437, abs 0.2502); Task 7 runs it (`coverage.json` status
  `planned`).

## Pass durations {#durations}

Median per pass on `s3` (window `wall_s` / whole pass from first server start to last stop), K=3, report run:

| Case | `wall_s` | Pass |
|---|---|---|
| `boundaries.*`, `merge-forced.default` | 0.1–0.6 s | 2.4–3.8 s |
| `async-inserts.bursty` / `.steady` | 20.1 / 50.1 s | 22.3 / 52.7 s |
| `merge-natural.default` / `.wide` | 62.7 / 63.2 s | 65.3 / 67.1 s |
| `small-inserts.default` | 67.0 s | 74.4 s |

The natural-merge cases spend 60 s of each pass in the quiet period. A full run of the 15 cases took 16–17 min
(45 passes); the five runs together about 80 min. `coverage.json` keeps the 120 s planning floor as `duration_s`.

## Changes found while measuring {#changes}

- **Part order.** `system.parts` sorted by name puts `all_1_8_1` before `all_2_2_0`; the cards list parts by level,
  then block range (`fcaec9179c7`). Found in a smoke pass of `merge-forced.default` (fit).
- **Block numbers of one INSERT.** The parts of `boundaries.insert-split` got block numbers out of squash order (the
  397 216-row remainder was `all_1_1_0`). The prediction states sizes and types sorted by size
  (`active_parts_by_rows`), not names (`fcaec9179c7`, before every recorded run).
- **Async flush count.** A buffer holds its first insert plus those arriving within the timeout, so
  `inserts_per_async_flush` uses 1 + rate × timeout (5 at 20/s, measured 4.8–5.0).
- **Rig hooks.** `closure_check.py` reads `raw/<card>/<run_id>/ledger.jsonl[.gz]` before `raw/<card>/…`, because
  every run restarts ledger seq numbers (`fd444e9f3a8`). The estimate's sample card `merge-natural` moved from
  `samples.py` to the lifecycle card of that name; the phase-0 sample manifest has no `params.replica` and no
  prediction, so a re-run of it fails closed.
- **Review fixes** (see [Review](#review)): the quiet period, the observation cutoff and the outdated
  classification changed the cards, so the report run is the fifth full run. A natural-merge pass now ends on a
  cutoff that the part log and the active parts read after it agree with, and asserts that no workload request
  starts after it inside the window (`window.no_workload_after_cutoff`).

## Gaps and limitations {#gaps}

- Only `s3` is measured; `coverage.json` keeps planned cases for `local`, `s3plain` and `cas` (each variant
  validates the shared rules in its own task). `collect_layout` maps objects through `system.remote_data_paths`; a
  variant whose objects that table does not list fails the merge cards closed.
- The hold-out `merge-natural.wide` was predicted after the pre-run, not before every run: its prediction needs the
  fit. Its first measured run (`cost-20261024T045917Z`) started after `b652973295f`; the pre-run never ran it.
- The fit covers fast back-to-back inserts only: fan-in sits at 6 to 9, and parts stop merging once their sizes
  diverge (final parts of 23 and 6 inserted parts after 64 inserts). Slower inserts and larger final sizes are the
  validation run's domain.
- Merge `GetObject` counts are assumption ranges here; R2 derives them.
- The async cases bound part counts broadly (one flush per burst up to one per insert); they do not test the flush
  sharing estimate beyond the steady case's measured 4.8–5.0 inserts per flush.
- No `DeleteObject` falls inside a window: outdated parts wait at least `old_parts_lifetime` (480 s), longer than
  every pass; the teardown drops the table.

## Review {#review}

Codex (`gpt-6-astra`, reasoning high, read-only; prompt `tmp/codex_lifecycle_prompt.txt`, log
`tmp/codex_lifecycle_r1.log`) round 1 found 12 MAJOR and 6 MINOR. It confirmed that all 45 records' request counts
reconcile with their raw windows, the fitted values reproduce, the hold-out predictions precede their runs, and the
wide hold-out tolerances reproduce from the pre-run data. Fixes are in `9dbe21126be`; the cards were re-run.

| # | Finding | Disposition |
|---|---|---|
| 1 | MAJOR: `part_type` omits the Wide overrides | Fixed: `adaptive_granularity` and `quantize_pq_codec` branches; `lifecycle_rules.test.js` |
| 2 | MAJOR: multipart omits the strict size and one-buffer multipart | Fixed: strict branch in `multipart_parts`, new `multipart_upload`, README; test |
| 3 | MAJOR: `parts_per_insert` makes `INSERT SELECT` async | Fixed: `insert_kind`, oversized data synchronous; test |
| 4 | MAJOR: squashing formula is not whole source blocks | Fixed: emitted rows rounded up to `insert_source_block_rows`; Codex's 111 195 300-row example gives 100; test |
| 5 | MAJOR: async sharing labelled source-derived | Fixed: evidence `assumed`, upper-bound estimate with payload, client and dedup caps; test |
| 6 | MAJOR: natural-merge bounds ignore TTL and force merges | Fixed: claims scoped to the simple selector; the cards' tables have no TTL and force merge off |
| 7 | MAJOR: acquisition rule misses branches, remote threshold misattributed | Fixed: ordered fetch, wait and skip branches; the remote threshold only gates the picker's refresh |
| 8 | MAJOR: mutation sizing and age boundary | Fixed: 1.1 × source bytes on disk without expired TTL parts; age inclusive |
| 9 | MAJOR: amplification fitted to another formula than the registry's | Fixed: fit to `write_amplification_rule` at the row-weighted size (f = 6.2271), hint corrected; test `test_amplification_rule_is_the_registry_formula_at_the_row_weighted_ratio`; committed prediction reported apart |
| 10 | MAJOR: hold-out TVs refer to the previous fit | Fixed: `coefficients.json` records both distances, against the committed reference and the current table |
| 11 | MAJOR: `quiesced` misses merges between polls | Fixed test-first (`test_quiet_period_counts_a_merge_finished_between_polls`): the part log's last merge end resets the quiet period; re-run |
| 12 | MAJOR: merged-away objects stay active | Fixed test-first (`test_merged_away_parts_become_outdated_at_their_merge`): `reclassify` marks them `outdated` at the merge; assertion `inventory.outdated_byte_hours>0`; re-run |
| 13 | MINOR: wrong citation anchors | Fixed in the predictions' `source_refs` and the README |
| 14 | MINOR: `old_parts_lifetime` is an eligibility age | Fixed: label and README say minimum age |
| 15 | MINOR: ladders differ past 12 levels | Fixed: the Python ladder caps at 12; test |
| 16 | MINOR: async bounds do not test batching | Parked: P2 cases with honest bounds; stated under [Gaps](#gaps); a batching bound needs a new pre-registered case |
| 17 | MINOR: two coverage spans misdescribe their cases | Fixed: `level-cross` spans its merge result, `bursty` states burst size, pause and clients |
| 18 | MINOR: the stated domain excludes the hold-out | Fixed: README and the coefficients' `domain` separate fit and hold-out |

Round 2 (prompt `tmp/codex_lifecycle_prompt_r2.txt`, log `tmp/codex_lifecycle_r2.log`) closed MAJORs 6, 8, 9, 10
and every MINOR, kept 8 MAJORs open and found one new MAJOR. It confirmed that the re-run records replay, the
checksums match, and no hold-out expected value changed after its prediction commit. Fixes are in `d53e470cdb5`; the
cards were re-run again.

| Finding | Round 2 | Disposition |
|---|---|---|
| 1 | hint can give the wrong `adaptive_granularity` | Fixed: the hint states the whole predicate (mixed granularity or no non-adaptive part) |
| 2 | a clamped first buffer below 32 MiB is missed | Fixed: `multipart_upload` compares with the smaller of the first buffer and the single-PUT limit; test |
| 3 | native admission measured as inline | Fixed: `insert_protocol`; native decoded bytes < limit, inline raw bytes ≤ limit; test |
| 4 | client blocks are not fixed; disabled minimums give unavailable | Fixed: `insert_data_block_rows` input; a disabled minimum is skipped, both off emit every block; tests |
| 5 | wrong dedup predicate; mean-rate bound; client hint | Fixed: `deduplicate_insert` (on by default) drives the 450 cap; the estimate states evenly spaced arrivals; the client hint reads `PendingAsyncInsert`; test |
| 7 | the "wait" branches fetch | Fixed: README and label say "try to fetch, quietly retrying" |
| 11 | a merge can slip past the last flush | Fixed test-first (`test_completion_waits_again_when_the_final_part_log_shows_a_later_merge`): the part log read after the final flush is the cutoff, a later merge or a running one restarts the wait |
| 12 | long runs lose part-to-object mappings | Fixed test-first (`test_layout_accumulates_across_snapshots`, `test_long_runs_snapshot_the_layout_while_inserting`): snapshots every 60 s accumulate |
| new | the 2 h prediction mixes fits | Fixed: every field recomputed from the report run's six fit records with the registry formula |

Round 3 (`tmp/codex_lifecycle_prompt_r3.txt`, `tmp/codex_lifecycle_r3.log`) closed round-2 findings 2, 3, 7, 12
and the 2 h prediction, kept 1, 4, 5, 11 open and found one MINOR. It confirmed the re-run replays, the fit
reproduces (6.2845998), every 2 h prediction field reproduces, and no hold-out expected value changed after its
prediction commit. Fixes are in `474392fb9ad`; the cards were re-run.

| Finding | Round 3 | Disposition |
|---|---|---|
| 1 | the hint's creation-date shortcut | Fixed: removed; the hint says to check for non-adaptive marks files |
| 4 | native async overflow forwards one combined block | Domain: stated as not covered in the `parts_per_insert` label and the README |
| 5 | backward-compatible dedup with table-only async reads `insert_deduplicate` | Fixed: `async_insert_source` and `insert_deduplicate`; test |
| 11 | a merge can start after the final flush | Fixed test-first (`test_completion_waits_again_when_an_active_part_has_no_part_log_row`, `test_no_workload_request_after_the_observation_cutoff`): active parts must be explained by the part log, and a request after the cutoff fails the pass |
| new MINOR | "serialized sizes" of native entries | Fixed: decoded `Block::bytes` for native, raw length for inline |

## How to re-run {#rerun}

```bash
utils/cas-yardstick/rustfs-bin.sh ensure
YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-lifecycle --policies s3 --repeats 3 \
  --cases <experiment ids> --runs-dir tmp/cas-yardstick/cost-runs
utils/cas-yardstick/rustfs-bin.sh stop
python3 utils/storage-cost-calculator/tools/closure_check.py --mode dev
```

The deterministic cases alone: `YARDSTICK_LIVE=1 python3 -m unittest tests.live_lifecycle -v` from
`utils/cas-yardstick`.
