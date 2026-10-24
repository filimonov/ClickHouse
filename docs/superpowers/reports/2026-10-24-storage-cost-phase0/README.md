---
description: 'Phase-0 gate of the storage cost calculator: six acceptance probes run end to end on the 26.8 binary, with source-cited predictions, ledger reconciliation, drill-down and the runtime estimate.'
sidebar_label: 'Storage cost phase-0 gate'
sidebar_position: 53
slug: /superpowers/reports/storage-cost-phase0
title: 'Storage cost calculator — phase-0 gate'
doc_type: 'reference'
---

# Storage cost calculator — phase-0 gate {#phase0-gate}

All six acceptance probes pass on the pinned binary: the five of design §5.3 plus `probe_cas_expect`, which proves
the `Expect: 100-continue` path live. Every prediction was committed before the gate run (`3157c681db5` for the
five, `64dd8717f92` for `probe_cas_expect`), and every deterministic count matches the ledger exactly. The gate was
re-run after the whole-branch review fixes (`docs/superpowers/cost-model/measurement-contract.md` §Review); the
numbers below are from that run. Each pass now starts exactly its card's replicas, so one-replica probes record
`replicas: 1` and no idle server's lease or GC traffic.

| Item | Value |
|---|---|
| Binary, downloaded | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` (`tmp/bin/SHA256SUMS`; record field `binary_download_sha256`) |
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (`binary_sha256`): the self-extracting download decompressed itself in place on its first start |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` (`commit`) |
| Rig commit | `8a9806dd0b75ba3a5daca197a00a8c7b0955e70a` (`rig_commit`), a clean tree for `utils/` |
| Gate run | `cost-20261024T035215Z`; earlier gate runs: `cost-20261024T024945Z` (phase-0 review), `cost-20261024T015941Z` |
| Store | RustFS binary behind the ledger proxy, bucket `yardstick-cost` |
| Records | `measurements.jsonl` (six `measurement/1` records); each replays exactly from its `raw/<probe>/ledger.jsonl[.gz]` (`closure_check.py`) |

## Gate results {#gate-results}

```
PASS probe_s3_insert
PASS probe_replicated_fetch
PASS probe_multipart_fault
PASS probe_cas_gc_cohort
PASS probe_local_volume
PASS probe_cas_expect
exit 0
```

Each PASS means: the pass is `ok` (a failed assertion makes it `FAILED`), cleanup succeeded, `drill.mjs`
reconciles the probe's formulas cells inside a validated contract with no problem, and `render_check.mjs` finds
every value, money line, allocation and evidence block in the page markup. Drill-downs are in
`raw/<probe>/drill.{txt,json}`.

What the drill checks:

- **Requests** per caller (or all callers), op and outcome class (`ok`, `accepted_dropped`, `client_error` = 4XX,
  `server_error`, `not_sent`, `dropped`). An exact prediction is a `const` row; a range is an `input` row filled from the
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

The ledger is complete in all six records (`window.open_attempts` empty) and `inventory.initial` is `declared`:
RustFS cannot list incomplete uploads by directory prefix.

### `probe_s3_insert` {#probe-s3-insert}

One 1000-row `INSERT` into a 3-column `ReplicatedMergeTree` on the `s3` disk. Zero crossing attempts, 1000 rows.
Billed: 11 PUTs and 0.6 `active` byte-hours.

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
| r1 | `CompleteMultipartUpload` | 4 | 4 (`not_sent`: injected, never forwarded) |
| r1 | `AbortMultipartUpload` | 4 | 4 (`not_sent`: injected, never forwarded) |
| r1 | `PutObject` | 1..14 | 14 |
| r1 | `DeleteObjects`, `DeleteObject` | 0..1, 0..14 | 0, 0 |

| Check | Predicted | Measured |
|---|---|---|
| Distinct part identities | 2 | 2 |
| Part 2 attempts, distinct payload hashes | 2, 1 | 2, 1 (correlated as one retry chain) |
| Incomplete bytes before the abort (lower and upper replay) | 40 939 368 .. 40 953 993 | 40 939 368, 40 939 368 |
| Object at the upload key | none | none |
| Incomplete bytes after the rig's abort | 0 | 0 |
| `incomplete` byte-hours in the window | > 0 | 3 567 .. 4 622 |

The drill bills incomplete parts and the 14 PUT objects (58.9..61.3 `active` byte-hours) as storage, and the
accepted-but-dropped part upload as a PUT.

### `probe_cas_gc_cohort` {#probe-cas-gc-cohort}

20 inserts on the CAS disk, then `DROP TABLE ... SYNC` with GC rounds every 5 s. Stochastic card. Request origin
is causal: requests inside the caller's own GC rounds (`system.cas_gc_log`; r1 3 rounds) are maintenance, and so
are requests on the control families (lease `gc/server-roots/`, GC state, `_pool_meta`) at any time.
The cell prices foreground plus maintenance: 236 PUTs, 41 + 20 (404) HEADs, 372 + 11 (404) GETs, 14 LISTs,
42 + 2 deletes, and three storage categories. Its rows are `basis: measured` (inputs filled from the record), so the
drill reconciles them with the ledger without calling them exact.

| Check | Predicted | Measured |
|---|---|---|
| r1 foreground `PutObject` | 40..2000 | 187 (maintenance 49) |
| r1 foreground `HeadObject` | 20..2000 | 21 (maintenance 40) |
| Cohort (blob and manifest objects r1 created) | nonempty | 40 (20 blobs, 20 manifests) |
| Cohort reclaimed by `DeleteObject(s)` | all | 40 of 40, by r1 |
| `system.cas_gc_log` `Finish` rows that deleted objects | > 0 | 2 |
| `reclaim_delay_s` (DROP to last cohort deletion) | > 0 | 14.3 |
| `garbage` byte-hours | > 0 | 2 026.8 .. 2 027.4 |
| `active`, `metadata` byte-hours | billed | 30.9 .. 31.0, 76.2 .. 76.3 |
| Ledger rows with `expect_negotiated` | 0 | 0 |

### `probe_cas_expect` {#probe-cas-expect}

3 inserts on the CAS disk with GC off and the disk's `expect_continue_min_bytes` set to 1 through
`settings.server` (`storage_configuration.disks.cas.expect_continue_min_bytes`), so every conditional PUT with a
body negotiates `Expect: 100-continue` and the proxy answers `100 Continue` itself. Stochastic card.

| Check | Predicted | Measured |
|---|---|---|
| r1 foreground `PutObject` | 6..2000 | 27 (3 blobs, 24 conditional writes) |
| r1 foreground `HeadObject` | 3..2000 | 3 (404, one mandatory HEAD per blob) |
| Ledger rows with `expect_negotiated` | >= 1 | 24 |
| Window rows per op equal ledger attempts per op (one negotiated PUT is one attempt) | equal | `GetObject` 22, `HeadObject` 3, `PutObject` 27 on both sides |
| Rows with `wire_bytes_bound: upper` (negotiated and refused) | none expected | 0 |
| GC rounds in the window | 0 | 0 |

The window held no lease renewal, so maintenance is empty; the control-family rule is exercised live by
`probe_cas_gc_cohort` (9 control-family PUTs moved from foreground to maintenance).

### `probe_local_volume` {#probe-local-volume}

200 inserts of 20 000 rows into a 10-column table on a `local` disk, the server under `strace`.

| Check | Predicted | Measured |
|---|---|---|
| S3 requests | none | none |
| Write syscalls under the data directory, inside the window | `write` only | `write` only: 3 800 calls, 248 711 380 bytes |
| Size histogram (bytes: calls) | present | 4096: 1400, 256: 400, 512: 400, ... |
| `proc_io.write_bytes` of r1 | > 0 | > 0 |
| Union peak of allocated bytes, r1 | > 0 | 257 122 304 |
| Device, filesystem | recorded | `dm-1`, `ext4` |

Syscall accounting is clipped to the window: 6 data-directory calls outside it (setup and teardown) and none
crossing an edge are excluded. Demand carried into pricing: capacity 0.24 GiB, 1 932 syscall IOPS, 120.6 MiB/s
(window averages over 1.97 s). `allocateVolumes` picks `single: 1 x 6 GiB, 3000 IOPS, 125 MiB/s` at $0.48 per
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
- **The CAS client sends `Expect: 100-continue` only on a conditional `PUT` of at least `expect_continue_min_bytes`,
  1 MiB when the disk leaves it unset** (`src/IO/S3/PocoHTTPClient.cpp:649-665`,
  `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:1279`). Blob publication is unconditional
  (`.../Backend/CasObjectStorageBackend.cpp:897`), and every conditional body in the GC probe is under 1 MiB, so no
  row negotiates there (measured: 0). With the disk setting at 1 byte (`src/IO/S3AuthSettings.cpp:73`,
  `.../ObjectStorages/S3/diskSettings.cpp:186`) every conditional PUT negotiates (measured: 24 of 27 PUTs).
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
- **`Expect`:** the proxy answers `100 Continue` itself (D11) and strips the header upstream. When the store does
  not answer `ok` to a negotiated request, the row says `wire_bytes_bound: upper`: a store handling `Expect` itself
  would have refused before the body. No gate row needed it.
- **Servers per pass:** each pass starts exactly its card's replicas (`--replicas` is the most the rig may start);
  a request from another workload port fails the pass.

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
| P1 (`cost estimate`, includes the 1.25x rerun reserve) | 25.6 |
| P1 + P2 | 59.2 |
| P1 + P2 with the per-pass overhead `wall_s` omits (about 5.5 s for server start, stop and cleanup) | about 60.0 |
| Budget (§5.5) | 72 |

- **Cuts: none.** 59.2 h is under the 72 h budget, so no P2 card, `replication-zero-copy` or types case is cut and
  no quantity or preset is disabled. The margin is 12.8 h; a longer floor on a P2 family would cut that family first.
- **Basis:** every pass is budgeted at `max(measured median, duration_s)`, where `duration_s` is the case's declared
  duration in `coverage.json`: 7200 s for `amplification-run` (timeout 14 400 s), 600 s for `gc-*` and
  `replication-*`, 120 s elsewhere. All 50 cases sit on their floor (the samples' medians are 8..83 s), so the floors
  decide the total; `amplification-run`, a P2 case, alone is 30 h of P2 (4 policies x 3 repeats x 2 h, with the reserve).
  The earlier estimate (P1 14.5 h, P1 + P2 17.8 h) budgeted a tenth of `timeout_s` per pass and put
  `amplification-run` at minutes. Per-case lines are in `raw/estimate/estimate.txt`.
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
python3 utils/cas-yardstick/yardstick.py cost estimate docs/superpowers/reports/2026-10-24-storage-cost-phase0/raw/estimate
python3 utils/storage-cost-calculator/tools/closure_check.py --mode dev
```

Layout of this directory follows `docs/superpowers/cost-model/schemas/report-directory.md`. `raw/<probe>/` holds
the ledger slice, relay logs, strace output (gzip) and the drill-down of each gate pass; `raw/estimate/` holds the
sample records; `raw/SHA256SUMS` covers everything under `raw/`.
