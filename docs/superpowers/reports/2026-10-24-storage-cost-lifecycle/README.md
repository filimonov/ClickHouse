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
committed bounds. The write amplification rule fits with effective fan-in 6.11.

| Item | Value |
|---|---|
| Binary, downloaded | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` |
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Report run | `cost-20261024T045917Z`, rig commit `5e8f5165369`; `measurements.jsonl` holds its 45 records |
| Pre-run | `cost-20261024T044126Z`, rig commit `199348a48f3`: the fit data of the `merge-natural.wide` prediction, kept as `raw/runs/cost-20261024T044126Z/measurements.jsonl.gz` (42 records, all ok) |
| Predictions | source-only `2d931d33a05` (+ `fcaec9179c7` for `insert-split`), fitted `b652973295f`; each before every run that it predicts |
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
  `index_granularity_bytes` != 0 and mixed granularity allowed, `:11680`) and when a column uses
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
single `PutObject` (`:180`). The one-buffer multipart case needs a first buffer or strict size above
`max_single_part_upload_size`, which the defaults never give. So by default an object of exactly 32 MiB is one
`PutObject`, and 32 MiB + 8 bytes is `CreateMultipartUpload`, `UploadPart` of 32 MiB and 8 bytes,
`CompleteMultipartUpload`. Registry: `multipart_sizing_rule`, `multipart_parts` (with the strict branch; no growth
step, so valid below 499 later parts) and `multipart_upload`.

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
  (`Settings.cpp:5609`), so its parts hold 17 source blocks, 1 111 953 rows. Formatted data arrives in blocks of
  `max_insert_block_size` = 1 048 449 rows (`Settings.cpp:178`). Registry: `parts_per_insert` = ceil(rows / emitted
  rows) per partition, emitted rows = the smaller of `min_insert_block_size_rows` and the byte minimum in rows, each
  rounded up to whole source blocks (`insert_source_block_rows`).
- **Async eligibility.** `async_insert` is on by default in this build (`Settings.cpp:7225`); a table's own
  `async_insert` setting also enables it (`MergeTreeData.cpp:9993`). Only an INSERT with data goes to the queue:
  `INSERT SELECT` stays synchronous (`executeQuery.cpp:2894`, native protocol `TCPHandler.cpp:1500`), and data
  larger than `async_insert_max_data_size` falls back to a synchronous insert (`AsynchronousInsertQueue.cpp:501`,
  `:528`). Every insert of the lifecycle cards except `async-inserts.*` is an `INSERT SELECT`.
- **Async flush.** Inserts with the same user, roles, query shape, settings and data kind share a buffer
  (`AsynchronousInsertQueue.cpp:587`). It is flushed into one part per partition when the serialized sizes of its
  whole entries reach `async_insert_max_data_size` (10 MiB), when `async_insert_max_query_number` (450) entries
  accumulate with deduplication on (off, `Settings.cpp:2393`), or at its deadline (`:663`, `:680`). The deadline is
  the buffer's first insert plus an adaptive timeout between `async_insert_busy_timeout_min_ms` (50) and `_max_ms`
  (200) that grows while inserts are frequent (`:623`, `:739`–`:774`; `Settings.cpp:7249`, `:7252`). With
  `wait_for_async_insert = 1` (`:7228`) a client waits for its flush, so only concurrent clients share one.
- **Async estimate.** Registry `inserts_per_async_flush` is an upper-bound estimate (evidence `assumed`), not a
  source derivation: max(1, min(1 + rate × timeout_max, ceil(buffer / payload), clients when waiting, 450 with
  deduplication)). The first insert opens the buffer and every insert before the longest deadline joins it; a
  shorter adaptive timeout flushes sooner. `async-inserts.steady` measured 4.7–4.9 inserts per flush against 5.

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
2. **Wait** for a TTL-recompress merge assigned to another replica, until `try_fetch_recompressed_part_timeout`
   (`:111`).
3. **Fetch** when `execute_merges_on_single_replica_time_threshold` > 0, the entry is younger than it, and the
   picker names another replica (`:130`; `ReplicatedMergeTreeMergeStrategyPicker.cpp:59`–`:63`, merge entries only).
   `remote_fs_execute_merges_on_single_replica_time_threshold` only decides whether the picker refreshes its state
   (`:105`–`:119`), not this choice.
4. **Fetch** when a source part is missing or covered by another part (`:155`, `:166`), or a patch part is missing
   (`:201`).
5. **Wait** while a source part has newer metadata than the table (`:187`).
6. **Fetch** when the entry is at least `prefer_fetch_merged_part_time_threshold` (3600 s) old (inclusive), its
   sources hold at least `prefer_fetch_merged_part_size_threshold` (10 GiB) on disk, and a replica has the result
   (`:224`, `:233`).
7. With zero-copy (`allow_remote_fs_zero_copy_replication`, off), **fetch** when a replica has a covering part, and
   **wait** or **fetch** when another replica holds the exclusive lock (`:296`–`:349`).
8. Else **merge locally**.

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
| `small-inserts.default` | fit | 1000 parts; 11 000–29 981 `PutObject` | 1000 parts, 195–197 merges, amplification 3.951–3.990; 13 161–13 175 |
| `merge-natural.default` | fit | 64 parts; 704–1901 `PutObject` | 9 merges of 6 then 2 of 8, final parts 23, 23, 6, 6, 6; amplification 2.5522; 913 |
| `merge-natural.wide` | hold-out | amplification 2.8030 ± 0.3474 (f = 6.1912 from the pre-run); fan-in TV ≤ 0.3426; result-size TV ≤ 0.3852; fan-in mean 5.06–7.36; 352–1871 `PutObject` | 5 merges of 6, 1 of 7, one final part; amplification 2.9094; TV 0.0503 and 0.0647; mean 6.17; 646 |
| `async-inserts.steady` | fit (P2) | 1000 async inserts at 20/s: 200–1000 parts | 204, 204, 212 parts |
| `async-inserts.bursty` | hold-out (P2) | 10 bursts of 100: 10–1000 parts | 19 parts each |

Merge reads observed for R2: a merge reads 8 `GetObject` per 5-column Compact source part (16 for 2 parts, 64 for
8), on top of the Compact or Wide result's writes.

## Coefficients {#coefficients}

`coefficients.json` holds the measured coefficients; the pinned values live as registry defaults with their source
references. Fit domain of all three: 5 `UInt64` columns, inserts of 1000–100 000 rows, back-to-back; the hold-out
extends it to 20 columns and 50 000 rows. Machine-readable validity (`merge_size_ratio`, `insert_rows`) and the
record's `domain` field carry it.

| Coefficient | Value | Fit | Hold-out | Tolerance |
|---|---|---|---|---|
| `merge_effective_fan_in` | 6.1097 | `small-inserts.default`, `merge-natural.default` (6 records); residuals −0.168…−0.119 and +0.169 | `merge-natural.wide`: residual +0.080 (+0.106 against the committed f = 6.1912) | abs 0.3474, rel 0 |
| `merge_fan_in_distribution` | 6: 0.880, 7–8: 0.116, 9–12: 0.004 | same 6 records | TV 0.0503 | TV ≤ 0.3426 |
| `merge_result_size_distribution` | 5–8: 0.828, 17–32: 0.097, 65–128: 0.035, 33–64: 0.021, 129–256: 0.010, 257–512: 0.006, 513+: 0.003 | same 6 records | TV 0.0647 | TV ≤ 0.3852 |

- **Amplification is on bytes on disk.** Row amplification is higher (5.26–5.31 against 3.95–3.99 on
  `small-inserts`): a 1000-row Compact part carries fixed-size metadata files, and merged parts compress better.
- **Tolerances come from the model, not K.** Merge structure was identical across repeats (`merge-natural.*`) or
  nearly so (`small-inserts`), so K=3 spread alone would give a zero band. The rules are in the hold-out prediction's
  `inputs.tolerance_rules`: amplification abs = largest fit residual + half the largest K=3 range; distribution
  TV = 2 × largest fit TV + 1 / predicted merges.
- **Validation.** `amplification-run.2h` (one 1000-row insert per second for 2 h) has its manifest and prediction
  (`b652973295f`); Task 7 runs it (`coverage.json` status `planned`).

## Pass durations {#durations}

Median per pass on `s3` (window `wall_s` / whole pass from first server start to last stop), K=3:

| Case | `wall_s` | Pass |
|---|---|---|
| `boundaries.*`, `merge-forced.default` | 0.1–0.6 s | 2.4–3.8 s |
| `async-inserts.bursty` / `.steady` | 20.2 / 50.1 s | 22.4 / 53.5 s |
| `merge-natural.default` / `.wide` | 62.6 / 63.6 s | 65.0 / 66.2 s |
| `small-inserts.default` | 65.6 s | 71.1 s |

The natural-merge cases spend 60 s of each pass in the quiet period. The report run took 16 min for 45 passes; the
pre-run 13 min for 42. `coverage.json` keeps the 120 s planning floor as `duration_s`.

## Changes found while measuring {#changes}

- **Part order.** `system.parts` sorted by name puts `all_1_8_1` before `all_2_2_0`; the cards list parts by level,
  then block range (`fcaec9179c7`). Found in a smoke pass of `merge-forced.default` (fit).
- **Block numbers of one INSERT.** The parts of `boundaries.insert-split` got block numbers out of squash order (the
  397 216-row remainder was `all_1_1_0`). The prediction now states sizes and types sorted by size
  (`active_parts_by_rows`), not names (`fcaec9179c7`, before both runs).
- **Async flush count.** A buffer holds its first insert plus those arriving within the timeout, so
  `inserts_per_async_flush` is 1 + rate × timeout (5 at 20/s, measured 4.7–4.9), not rate × timeout.
- **Rig hooks.** `closure_check.py` reads `raw/<card>/<run_id>/ledger.jsonl[.gz]` before `raw/<card>/…`, because
  every run restarts ledger seq numbers (`fd444e9f3a8`). The estimate's sample card `merge-natural` moved from
  `samples.py` to the lifecycle card of that name; the phase-0 sample manifest has no `params.replica` and no
  prediction, so a re-run of it fails closed.

## Gaps and limitations {#gaps}

- Only `s3` is measured; `coverage.json` keeps planned cases for `local`, `s3plain` and `cas` (each variant
  validates the shared rules in its own task).
- The hold-out `merge-natural.wide` was predicted after the pre-run, not before every run: its prediction needs the
  fit. The report run (`cost-20261024T045917Z`) started after `b652973295f`, and the pre-run never ran that case.
- The fit covers fast back-to-back inserts only: fan-in sits at 6 to 9, and parts stop merging once their sizes
  diverge (final parts of 23 and 6 inserted parts after 64 inserts). Slower inserts and larger final sizes are the
  validation run's domain.
- Merge `GetObject` counts are assumption ranges here; R2 derives them.
- No `DeleteObject` falls inside a window: outdated parts wait `old_parts_lifetime` (480 s), longer than every pass;
  the teardown drops the table.

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
