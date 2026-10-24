---
description: 'Phase-0 gate of the storage cost calculator: five acceptance probes run end to end on the 26.8 binary, with source-cited predictions, ledger reconciliation, drill-down and the runtime estimate.'
sidebar_label: 'Storage cost phase-0 gate'
sidebar_position: 53
slug: /superpowers/reports/storage-cost-phase0
title: 'Storage cost calculator — phase-0 gate'
doc_type: 'reference'
---

# Storage cost calculator — phase-0 gate {#phase0-gate}

All five acceptance probes of design §5.3 pass on the pinned binary: every prediction was committed
(`3157c681db5`) before the gate run, and every deterministic count matches the ledger exactly.

| Item | Value |
|---|---|
| Binary | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` |
| Source | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Gate run | `cost-20261024T013942Z`, rig commit `50c9b33b4b0` plus the two path fixes in the commit that adds this report |
| Store | RustFS binary behind the ledger proxy, bucket `yardstick-cost` |
| Records | `measurements.jsonl` (five `measurement/1` records) |

## Gate results {#gate-results}

```
PASS probe_s3_insert
PASS probe_replicated_fetch
PASS probe_multipart_fault
PASS probe_cas_gc_cohort
PASS probe_local_volume
exit 0
```

Each PASS means: the pass is `ok`, every record assertion holds, `drill.mjs` reconciles the probe's formulas
cell against the ledger with no mismatch, and `render_check.mjs` finds every value and money line in the page
markup. Drill-downs are in `raw/<probe>/drill.{txt,json}`.

### `probe_s3_insert` {#probe-s3-insert}

One 1000-row `INSERT` into a 3-column `ReplicatedMergeTree` on the `s3` disk. Zero crossing attempts, 1000 rows.

| Caller | Op | Predicted | Ledger |
|---|---|---|---|
| r1 | `PutObject` | 11 | 11 |
| any | any other op | 0 | 0 |

### `probe_replicated_fetch` {#probe-replicated-fetch}

The same insert on r1 of a 2-replica table; r2 fetches the part.

| Caller | Op | Predicted | Ledger |
|---|---|---|---|
| r1 | `PutObject` | 11 | 11 |
| r1 | `GetObject` | 11 | 11 |
| r2 | `PutObject` | 11 | 11 |
| r2 | `GetObject` | 8..11 (9 expected) | 9 |

Other evidence: relay of r1 `from_target` 32 064 bytes, relay of r2 0 bytes; `ReplicatedPartFetches` r1 0,
r2 1; `system.part_log` on r2 has one `DownloadPart` row; rows equal on both replicas.

### `probe_multipart_fault` {#probe-multipart-fault}

One `INSERT` of 40 000 rows whose `s String CODEC(NONE)` column file is 40 939 368 bytes. The proxy drops the
first response of part 2 and answers every `CompleteMultipartUpload` and `AbortMultipartUpload` with 500
without forwarding it. The disk sets `s3_retry_attempts = 1`.

| Caller | Op | Predicted | Ledger |
|---|---|---|---|
| r1 | `CreateMultipartUpload` | 1 | 1 |
| r1 | `UploadPart` | 3 | 3 |
| r1 | `CompleteMultipartUpload` | 4 | 4 |
| r1 | `AbortMultipartUpload` | 4 | 4 |
| r1 | `PutObject` | 1..14 | 14 |
| r1 | `DeleteObjects`, `DeleteObject` | 0..1, 0..14 | 0, 0 |

| Check | Predicted | Measured |
|---|---|---|
| Distinct part identities | 2 | 2 |
| Part 2 attempts, distinct payload hashes | 2, 1 | 2, 1 (correlated as one retry chain) |
| Incomplete bytes before the abort (lower and upper replay) | 40 939 368 .. 40 953 993 | 40 939 368, 40 939 368 |
| Object at the upload key | none | none |
| Incomplete bytes after the rig's abort | 0 | 0 |
| `incomplete` byte-hours in the window | > 0 | 3 714 .. 4 769 |

The drill bills the incomplete-storage line as storage (`storage: 6.08e-9 GB-month`).

### `probe_cas_gc_cohort` {#probe-cas-gc-cohort}

20 inserts on the CAS disk, then `DROP TABLE ... SYNC` with GC rounds every 5 s. Stochastic card.

| Check | Predicted | Measured |
|---|---|---|
| r1 `PutObject` | 40..2000 | 208 |
| r1 `HeadObject` | 20..2000 | 61 |
| Cohort (blob and manifest objects r1 created) | nonempty | 40 (20 blobs, 20 manifests) |
| Cohort reclaimed by `DeleteObject(s)` | all | 40 of 40, by r1 |
| `system.cas_gc_log` `Finish` rows that deleted objects | > 0 | 2 |
| `reclaim_delay_s` (DROP to last cohort deletion) | > 0 | 12.9 |
| `garbage` byte-hours | > 0 | 1 801.6 .. 1 802.1 |
| Ledger rows with `expect_negotiated` | 0 | 0 |

### `probe_local_volume` {#probe-local-volume}

200 inserts of 20 000 rows into a 10-column table on a `local` disk, the server under `strace`.

| Check | Predicted | Measured |
|---|---|---|
| S3 requests | none | none |
| Write syscalls under the data directory | `write` only | `write` only: 3 803 calls, 248 711 389 bytes |
| Size histogram (bytes: calls) | present | 4096: 1400, 1048576: 200, 262144: 200, 2048: 200, ... |
| `proc_io.write_bytes` of r1 | > 0 | > 0 |
| Union peak of allocated bytes, r1 | > 0 | 257 196 032 |
| Device, filesystem | recorded | `dm-1`, `ext4` |

Demand carried into pricing: capacity 0.24 GiB, 1 841 syscall IOPS, 114.8 MiB/s (window averages over 2.07 s).
`allocateVolumes` picks `single: 1 x 6 GiB, 3000 IOPS, 125 MiB/s` at $0.48 per month (the IOPS-per-GiB cap
sets the size).

## Source facts established {#source-facts}

All paths are under `tmp/src-26.8/`. The prediction files list every reference with its claim.

- **A compact part on a remote disk is 11 objects.** `data.bin`, `data.cmrk4`, `primary.cidx`, `count.txt`,
  `serialization.json`, `statistics.packed`, `columns.txt`, `columns_substreams.txt`, `metadata_version.txt`,
  `default_compression_codec.txt`, `checksums.txt` (`src/Storages/MergeTree/MergedBlockOutputStream.cpp:407-493`,
  `MergeTreeDataPartWriterCompact.cpp:53`, `MergeTreeIndexGranularityInfo.cpp:98`). `statistics.packed` comes from
  `auto_statistics_types = 'basic, uniq_v2'` (`MergeTreeSettings.cpp:2046`).
- **A file up to 32 MiB is one `PutObject`, with no HEAD after it** (`src/IO/WriteBufferFromS3.cpp:180`,
  `src/IO/S3Defines.h:26`, `:55`).
- **A 32..48 MiB object is `CreateMultipartUpload` plus two parts of 32 MiB and the rest**
  (`src/Common/BufferAllocationPolicy.cpp:51-52`, `src/IO/WriteBufferFromS3.cpp:178-198`).
- **Each failing S3 call costs `(s3_retry_attempts + 1)^2` HTTP attempts.** The SDK retries inside one call
  (`src/IO/S3/Client.cpp:116`) and ClickHouse wraps the call in its own loop (`src/IO/S3/Client.cpp:823`). With
  the default 500 a failing `CompleteMultipartUpload` would be retried about 250 000 times, so the probe sets 1.
- **`CompleteMultipartUpload` is retried above the client only for `NoSuchKey` and `InvalidPart`**
  (`src/IO/S3Common.cpp:48`); the failed finalize cancels the buffer once and aborts once
  (`src/IO/WriteBuffer.cpp:76`, `:102`; `src/IO/WriteBufferFromS3.cpp:244`).
- **A `String` column writes its sizes to `s.size.bin`**, so `s.bin` holds only characters
  (`MergeTreeSettings.cpp:300`). **`hex()` of an integer drops leading zero bytes**
  (`src/Functions/FunctionsBinaryRepresentation.cpp:54`), so the generator's rows are slightly under 1 KiB:
  40 000 rows are 40 938 368 bytes, and with 40 block frames of 25 bytes the file is 40 939 368 bytes.
- **The fetch sender reads every part file once, the receiver writes every file once and reads 9 of them back**
  when it loads the part (`src/Storages/MergeTree/DataPartsExchange.cpp:491-567`, `:1094`, `:1322`;
  `IMergeTreeDataPart.cpp:1527-1567`).
- **CAS key families** (`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h`):
  payload `blobs/<algo>/<2 hex>/<hex>` and manifests `cas/manifests/...`; control state `blobs/....meta`,
  `cas/ns/stream/`, `cas/ns/state/`, `cas/ref_catalog`, `gc/state|hb|gen/...`, lease `gc/server-roots/`,
  `_pool_meta`, `roots/`. Only `.bin` and `.mrk*`/`.cmrk[1-3]` files stay blobs; other part files ride inline in
  the manifest (`ContentAddressedTransaction.cpp:77`), so a compact part on CAS is one blob plus one manifest.
- **The CAS client sends `Expect: 100-continue` only on a conditional `PUT` of at least 1 MiB**
  (`src/IO/S3/PocoHTTPClient.cpp:649-665`, `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:1281`).
  Blob publication is unconditional (`.../Backend/CasObjectStorageBackend.cpp:897`), and every conditional body in
  the probe is under 1 MiB, so no row negotiates `Expect`. Measured: 0 rows.
- **A dropped table's manifests are reclaimed by GC rounds as owner-removed bodies**
  (`src/Interpreters/ContentAddressedGarbageCollectionLog.cpp:48`); measured within 13 s at a 5 s interval.
- **Part file writes use `::write`, not `pwrite64`** (`src/IO/WriteBufferFromFileDescriptor.cpp:60`).

## Failures investigated {#failures}

- **First gate run (`cost-20261024T013312Z`): `probe_local_volume` FAILED at server start.** Cause, demonstrated
  by `strace: Can't fopen 'tmp/cas-yardstick/...'` in `server_control.log`: the trace output path was relative to
  `--runs-dir`, and `server.sh` changes into the replica directory first. Fix: `Tracer.path` is absolute. The other
  four probes passed in that run with the same counts.
- **Second run (`cost-20261024T013627Z`): no `write` under the data directory.** Cause, from the trace: the
  `local` disk path was relative too, so the server wrote the table under `r1/tmp/cas-yardstick/...`. Fix: the
  runner makes the pass directory absolute. The prediction (`write`) was right; the attribution was not.
- No prediction was changed after a measurement.

## Limitations {#limitations}

- **Device:** the local volume is `dm-1` (`ext4`) on the host's single LVM root device, shared with RustFS, the
  proxy and unrelated processes. EBS figures from it carry the host-specific label.
- **Page cache:** not dropped (no root). Syscall IOPS and MiB/s are demand at the syscall boundary; page cache and
  writeback sit between them and the device, and the window averages are not device peaks.
- **`process_io`:** replica 1 embeds Keeper, so its `/proc/<pid>/io` counters include Keeper's writes.
- **`Expect`:** the proxy answers `100 Continue` itself (D11). No probe row negotiated it; a conditional write of
  1 MiB or more would.
- **Two servers per pass:** `cost probe` runs the default two replicas for every probe; single-replica probes
  create their tables on r1 only, and r2 stays idle except for its CAS mount and GC scheduler on the CAS probe.

## Runtime estimate {#runtime-estimate}

Planned coverage (`docs/superpowers/cost-model/coverage.json`): 426 passes, 351 of them P1. Samples run once at
planned scale in `cost-20261024T014123Z` (records in `raw/estimate/`):

| Sample | Policy | `wall_s` | Shape |
|---|---|---|---|
| `merge-natural.sample` | `s3` | 7.8 | 64 inserts x 100 000 rows, 5 columns, quiesced at 5 active parts |
| `merge-natural.sample` | `cas` | 8.8 | same, quiesced at 7 active parts |
| `gc-scale.sample` | `cas` | 83.4 | 1000 inserts into a 100-column Wide table: 207 024 objects created, one GC round of 1.28 s |

| Estimate | Hours |
|---|---|
| P1 (`cost estimate`, includes the 1.25x rerun reserve) | 14.5 |
| P1 + P2 | 17.8 |
| P1 + P2 with the per-pass overhead `wall_s` omits (about 5.5 s for server start, stop and cleanup) | about 18.6 |
| Budget (§5.5) | 72 |

- **Cuts: none.** The estimate is under budget, so no P2 card, `replication-zero-copy` or types case is cut and no
  quantity or preset is disabled.
- **Basis:** 12 of the 50 cases use their card's or family's sample; the other 38 use the global median of the samples
  (about 9 s), flagged in `raw/estimate/estimate.txt`. The completion-timeout share (a tenth of `timeout_s`,
  90..180 s per pass) dominates every case, so the total is insensitive to the medians.
- **Sample shape deviation:** a compact part on CAS is one blob, so the plan's "500 inserts of a 100-column
  compact table" would create about 1000 objects. The sample uses a Wide table instead (one blob per column file).

## How to re-run {#rerun}

```
utils/cas-yardstick/rustfs-bin.sh ensure
YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost probe --binary tmp/bin/26.8/clickhouse \
    --runs-dir tmp/cas-yardstick/cost-runs > tmp/cost_probe.log 2>&1; echo $?
python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
    --runs-dir tmp/cas-yardstick/cost-runs --manifests docs/superpowers/reports/2026-10-24-storage-cost-phase0 \
    --cases merge-natural.sample,gc-scale.sample --policies s3,cas --repeats 1
python3 utils/cas-yardstick/yardstick.py cost estimate tmp/cas-yardstick/cost-runs/<run_id> \
    --coverage docs/superpowers/cost-model/coverage.json
python3 utils/storage-cost-calculator/tools/closure_check.py --mode dev
```

Layout of this directory follows `docs/superpowers/cost-model/schemas/report-directory.md`. `raw/<probe>/` holds
the ledger slice, relay logs, strace output (gzip) and the drill-down of each gate pass; `raw/estimate/` holds the
sample records; `raw/SHA256SUMS` covers everything under `raw/`.
