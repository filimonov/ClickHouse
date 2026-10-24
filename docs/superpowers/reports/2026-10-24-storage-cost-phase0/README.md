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
(`3157c681db5`) before the gate run, and every deterministic count matches the ledger exactly. The gate was re-run
after the phase-0 review fixes (`docs/superpowers/cost-model/measurement-contract.md` §Review); the numbers below are
from that run.

| Item | Value |
|---|---|
| Binary, downloaded | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` (`tmp/bin/SHA256SUMS`; record field `binary_download_sha256`) |
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (`binary_sha256`): the self-extracting download decompressed itself in place on its first start |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` (`commit`) |
| Rig commit | `8c1674054543686c66e23bf5377e14ff54d66274` (`rig_commit`), a clean tree for `utils/` |
| Gate run | `cost-20261024T024945Z`; the pre-review gate run was `cost-20261024T015941Z` |
| Store | RustFS binary behind the ledger proxy, bucket `yardstick-cost` |
| Records | `measurements.jsonl` (five `measurement/1` records); each replays exactly from its `raw/<probe>/ledger.jsonl[.gz]` (`closure_check.py`) |

## Gate results {#gate-results}

```
PASS probe_s3_insert
PASS probe_replicated_fetch
PASS probe_multipart_fault
PASS probe_cas_gc_cohort
PASS probe_local_volume
exit 0
```

Each PASS means: the pass is `ok` (a failed assertion makes it `FAILED`), cleanup succeeded, `drill.mjs`
reconciles the probe's formulas cells inside a validated contract with no problem, and `render_check.mjs` finds
every value, money line, allocation and evidence block in the page markup. Drill-downs are in
`raw/<probe>/drill.{txt,json}`.

What the drill checks:

- **Requests** per caller (or all callers), op and outcome class (`ok`, `accepted_dropped`, `client_error` = 4XX,
  `server_error`, `dropped`). An exact prediction is a `const` row; a range is an `input` row filled from the
  record's `observations.inputs`, which must be registered measured quantities. Measured foreground counts must lie
  in the prediction's range, widened by its tolerance for stochastic cards.
- **Residuals.** Every op, outcome class, storage category with byte-hours and interserver direction in the record
  must be covered by a row, or the drill fails with `unmodelled residual`.
- **Storage** rows bill each category's upper byte-hour bound as GiB-months; **transfer** rows bill both relay
  directions as `ec2_ec2_cross_az`.
- **Billing and allocation.** A record's cells are billed once; each line's money is allocated back to the cells
  by quantity. The fetch probe has two cells, `inserts` (r1 writes the part, storage r1 created) and `recovery`
  (r1 serves it, r2 fetches and loads it, storage r2 created, interserver bytes), sharing the storage tier.
- **Evidence** (limitations, inventory, ledger completeness, byte-hour bounds) is shown under every price.

The ledger is complete in all five records (`window.open_attempts` empty) and `inventory.initial` is `declared`:
RustFS cannot list incomplete uploads by directory prefix.

### `probe_s3_insert` {#probe-s3-insert}

One 1000-row `INSERT` into a 3-column `ReplicatedMergeTree` on the `s3` disk. Zero crossing attempts, 1000 rows.
Billed: 11 PUTs and 0.8 `active` byte-hours.

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

Other evidence: relay of r1 `from_target` 32 064 bytes and `to_target` 292 bytes, relay of r2 0 bytes;
`ReplicatedPartFetches` r1 0, r2 1; `system.part_log` on r2 has one `DownloadPart` row; rows equal on both
replicas.

| Line (billed once) | Money | `inserts` | `recovery` |
|---|---|---|---|
| `put_copy_post_list` (22 PUTs) | $0.00011 | $0.000055 | $0.000055 |
| `get_and_other` (20 GETs) | $0.000008 | $0 | $0.000008 |
| `storage` (1.6 byte-hours, by creator) | $4.6e-14 | $2.8e-14 | $1.9e-14 |
| `ec2_ec2_cross_az` (32 356 bytes, both directions) | $3.0e-7 | $0 | $3.0e-7 |

### `probe_multipart_fault` {#probe-multipart-fault}

One `INSERT` of 40 000 rows whose `s String CODEC(NONE)` column file is 40 939 368 bytes. The proxy drops the
first response of part 2 and answers every `CompleteMultipartUpload` and `AbortMultipartUpload` with 500
without forwarding it. The disk sets `s3_retry_attempts = 1`.

| Caller | Op | Predicted | Ledger |
|---|---|---|---|
| r1 | `CreateMultipartUpload` | 1 | 1 |
| r1 | `UploadPart` | 3 | 3 (2 `ok`, 1 `accepted_dropped`: the store took part 2, the reply was dropped) |
| r1 | `CompleteMultipartUpload` | 4 | 4 (`server_error`: injected, never sent) |
| r1 | `AbortMultipartUpload` | 4 | 4 (`server_error`: injected, never sent) |
| r1 | `PutObject` | 1..14 | 14 |
| r1 | `DeleteObjects`, `DeleteObject` | 0..1, 0..14 | 0, 0 |

| Check | Predicted | Measured |
|---|---|---|
| Distinct part identities | 2 | 2 |
| Part 2 attempts, distinct payload hashes | 2, 1 | 2, 1 (correlated as one retry chain) |
| Incomplete bytes before the abort (lower and upper replay) | 40 939 368 .. 40 953 993 | 40 939 368, 40 939 368 |
| Object at the upload key | none | none |
| Incomplete bytes after the rig's abort | 0 | 0 |
| `incomplete` byte-hours in the window | > 0 | 3 794 .. 4 849 |

The drill bills incomplete parts and the 14 PUT objects (62.6..63.8 `active` byte-hours) as storage, and the
accepted-but-dropped part upload as a PUT.

### `probe_cas_gc_cohort` {#probe-cas-gc-cohort}

20 inserts on the CAS disk, then `DROP TABLE ... SYNC` with GC rounds every 5 s. Stochastic card. Request origin
is causal: requests inside the caller's own GC rounds (`system.cas_gc_log`; r1 3 rounds, r2 3) are maintenance.
The cell prices foreground plus maintenance: 237 PUTs, 41 + 20 (404) HEADs, 381 + 11 (404) GETs, 14 LISTs,
42 + 2 deletes, and three storage categories.

| Check | Predicted | Measured |
|---|---|---|
| r1 foreground `PutObject` | 40..2000 | 196 (maintenance 40) |
| r1 foreground `HeadObject` | 20..2000 | 21 (maintenance 40) |
| Cohort (blob and manifest objects r1 created) | nonempty | 40 (20 blobs, 20 manifests) |
| Cohort reclaimed by `DeleteObject(s)` | all | 40 of 40, by r1 |
| `system.cas_gc_log` `Finish` rows that deleted objects | > 0 | 2 |
| `reclaim_delay_s` (DROP to last cohort deletion) | > 0 | 13.6 |
| `garbage` byte-hours | > 0 | 1 922.3 .. 1 922.8 |
| `active`, `metadata` byte-hours | billed | 32.4 .. 32.5, 77.3 .. 77.4 |
| Ledger rows with `expect_negotiated` | 0 | 0 |

### `probe_local_volume` {#probe-local-volume}

200 inserts of 20 000 rows into a 10-column table on a `local` disk, the server under `strace`.

| Check | Predicted | Measured |
|---|---|---|
| S3 requests | none | none |
| Write syscalls under the data directory, inside the window | `write` only | `write` only: 3 800 calls, 248 711 380 bytes |
| Size histogram (bytes: calls) | present | 4096: 1400, 256: 400, 512: 400, 1048576: 200, ... |
| `proc_io.write_bytes` of r1 | > 0 | > 0 |
| Union peak of allocated bytes, r1 | > 0 | 257 191 936 |
| Device, filesystem | recorded | `dm-1`, `ext4` |

Syscall accounting is clipped to the window: 6 data-directory calls outside it (setup and teardown) and none
crossing an edge are excluded. Demand carried into pricing: capacity 0.24 GiB, 1 944 syscall IOPS, 121.4 MiB/s
(window averages over 1.95 s). `allocateVolumes` picks `single: 1 x 6 GiB, 3000 IOPS, 125 MiB/s` at $0.48 per
month (the IOPS-per-GiB cap sets the size). The drill shows the workstation limitations under that price.

## Source facts established {#source-facts}

All paths are under `tmp/src-26.8/`. The prediction files list every reference with its claim.

- **A compact part on a remote disk is 11 objects.** `data.bin`, `data.cmrk4`, `primary.cidx`, `count.txt`,
  `serialization.json`, `statistics.packed`, `columns.txt`, `columns_substreams.txt`, `metadata_version.txt`,
  `default_compression_codec.txt`, `checksums.txt` (`src/Storages/MergeTree/MergedBlockOutputStream.cpp:407-493`,
  `MergeTreeDataPartWriterCompact.cpp:53`, `MergeTreeIndexGranularityInfo.cpp:98`). `statistics.packed` comes from
  `auto_statistics_types = 'basic, uniq_v2'` (`MergeTreeSettings.cpp:2046`).
- **A file up to 32 MiB is one `PutObject`, with no HEAD after it** (`src/IO/WriteBufferFromS3.cpp:180`,
  `src/IO/S3Defines.h:26`, `:55`).
- **A 32..48 MiB object is `CreateMultipartUpload` plus two parts: one of 32 MiB and the rest**
  (`src/Common/BufferAllocationPolicy.cpp:51-52`, `src/IO/WriteBufferFromS3.cpp:178-198`).
- **Each failing S3 call costs `(s3_retry_attempts + 1)^2` HTTP attempts.** The SDK retries inside one call
  (`src/IO/S3/Client.cpp:116`) and ClickHouse wraps the call in its own loop (`src/IO/S3/Client.cpp:823`). With
  the default 500 a failing `CompleteMultipartUpload` would be retried about 250 000 times, so the probe sets 1.
- **`CompleteMultipartUpload` is retried above the client only for `NoSuchKey`, `InvalidPart` and `InvalidPartOrder`**
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
- **Review rerun, first attempt (`cost-20261024T024148Z`), interrupted on purpose** to add `inventory.reclassified`
  before the final run; its four finished probes drilled with no problem. The interrupt left the local probe's
  server running; it was stopped and the run's prefix was verified empty.
- **Review run `cost-20261024T024451Z`** passed on an uncommitted tree; the gate was re-run as
  `cost-20261024T024945Z` after the code commit so `rig_commit` names the code that ran.
- No prediction was changed after a measurement. The multipart cell's `UploadPart 3` row became `ok 2` plus
  `accepted_dropped 1` (the fault rule drops the reply of the first part-2 attempt), and the fetch cell was split by
  operation; the predicted totals are unchanged.

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
