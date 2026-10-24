---
description: 'EBS demand research (R1): local I/O bytes and request sizes per operation on the local policy, their conversion to gp3 IOPS and throughput, and the EBS operation cells.'
sidebar_label: 'EBS demand'
sidebar_position: 60
slug: /superpowers/reports/2026-10-24-storage-cost-ebs
title: 'EBS demand and conversion (R1)'
doc_type: 'reference'
---

# EBS demand and conversion (R1) {#ebs}

EBS cells price one replica's gp3 volumes from three things:

- **`io_bytes_per_op`** (measured): volume bytes per logical byte of an operation. Inserts and merges each have a
  fit and a passed hold-out.
- **`ebs_sequential_ios_per_mib`** (pinned, AWS rule): 4 I/Os per MiB. This is a lower bound. Measured request
  counts were 1.3–6.5× higher on this rig, depending on the schema and operation. The gp3 baseline of 3000 IOPS usually dominates.
- **`peak_factor`** (assumed input, default 1): the rig cannot measure it; see [Peak factor](#peak).

The mutations, deletes, reads and recovery cells are cut; see [Cut operations](#cut).

| Item | Value |
|---|---|
| Binary | antalya-26.8 `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` |
| Rig policy / model variant | `local` / `ebs` |
| Report run | `cost-20261024T145657Z`: all cases, K = 3, one rig commit |
| Earlier runs | `cost-20261024T080735Z` (fit, before the `preadv2` fix), `cost-20261024T132526Z` (fit), `cost-20261024T135713Z` and `cost-20261024T140702Z` (consumed hold-outs, see [Hold-outs](#holdouts)), `cost-20261024T140341Z`, `cost-20261024T145139Z` (fit after the F1 fix) |
| Predictions | source `d5cf507a02c`; consumed hold-outs `e145a15461a`, `dcd7f48fcf8`; hold-out `956cd4ec795` |
| Card | `utils/cas-yardstick/yardstick/cost/cards/io_sizes.py`; fit rules `utils/cas-yardstick/yardstick/cost/ebs_fit.py` |

## Measurement boundary {#boundary}

This boundary limits every coefficient here. The registry entries carry it in `evidence.ref` and `validity`.

- **Write bytes are `/proc/<pid>/io` `write_bytes`.** The kernel counts pages when they are dirtied, so a small
  append counts a whole 4 KiB page. Filesystem journal and metadata writes are not in it, and files deleted before
  writeback are over-counted. It is a process-side measure of volume writes, not a device measurement.
- **The embedded Keeper runs in r1's process.** `io-sizes.idle` quantifies it: r1 (server + Keeper) writes
  `66 492` B/s at idle, r2 (server only) `64 171` B/s. The Keeper's share is about `2 321` B/s. Its traced
  log writes are only `24.6` B/s; page rounding of small appends explains the rest of the gap.
- **Server background writes are excluded from EBS demand.** Every `io-sizes` pass idles 10 s first, and each phase
  subtracts r1's idle rate × phase time. That rate is mostly the server itself: system log tables and the server log,
  about `64 171` B/s (0.06 MiB/s, under 0.05 % of the gp3 baseline throughput). A production volume carries it,
  but the cells model no background term, and no operation is charged for it.
- **Reads are the bytes traced syscalls return, not device reads.** The page cache is not dropped (no root). Parts
  written seconds earlier are cached, so `/proc/<pid>/io` `read_bytes` is near 0. Returned bytes do not bound cold
  device reads in either direction: readahead and block rounding add bytes, cached re-reads repeat them. They are
  the operation's read demand as the server issues it. Reads include `preadv2(RWF_NOWAIT)`, the thread-pool reader's
  page-cache path (`tmp/src-26.8/src/IO/preadNoWait.cpp:54`). The first fit run missed it: a full scan traced as
  0.1 % of its bytes. `fcbd30ea391` adds it.
- **Request sizes are syscall sizes at scale 0.05.** strace (`-f -ttt -T -yy`) attributes each call to a path:
  `table` (the `local` disk), `keeper` (r1's Keeper directory, excluded) and `server` (r1's other files). Paths off
  the volume (`/proc`, `/sys`, `/etc`) are dropped (`71399e1090e`); before that, 1 244 of 1 280 insert reads were
  `/proc` and `/sys`. The kernel may merge or split requests on writeback.
- **Host diskstats are not used.** They include RustFS, the proxy and other tracks on this shared host.

## Operation graph {#graph}

| Operation | Logical bytes (denominator) | Write demand | Read demand |
|---|---|---|---|
| `inserts` | `NewPart` `size_in_bytes` of the inserted parts | net `write_bytes` | traced reads |
| `merges` | `MergeParts` `size_in_bytes` of the merge results | net `write_bytes` | traced reads of the sources |
| `reads` | `data_compressed_bytes` of the scanned active parts | net `write_bytes` (≈ 0) | traced reads |

Source facts the predictions rest on, each cited in `predictions/*.json`:

- Part files are written through `WriteBufferFromFile` with `DBMS_DEFAULT_BUFFER_SIZE` = 1 MiB
  (`src/IO/WriteBufferFromFile.h:34`, `src/Core/Defines.h:22`): no write request exceeds 1 MiB. Measured: none does.
- Local reads use `max_read_buffer_size_local_fs` = 128 KiB (`src/Core/Settings.cpp:407`), which takes precedence
  for the local filesystem (`src/Interpreters/Context.cpp:8979`), through `pread_threadpool` (`Settings.cpp:7168`),
  with `min_bytes_to_use_direct_io` = 0 (`:1820`). Measured: merge and scan reads are 128 KiB.
- `fsync_after_insert` and `fsync_part_directory` are off (`src/Storages/MergeTree/MergeTreeSettings.cpp:615`, `:619`).

## Conversion to EBS {#conversion}

AWS rule, checked 2026-10-24 at https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html: SSD
volumes count an I/O of up to 256 KiB as one IOPS. A larger I/O is split, so 1 MiB counts 4. Physically sequential
small I/Os are merged up to 256 KiB, for example 8 × 32 KiB counts 1.

The cells price IOPS as a merged sequential stream: 4 I/Os per MiB of demand (`ebs_sequential_ios_per_mib`, the
fewest I/Os the bytes can take under the rule). That is a lower bound.

The rig also counts each traced request as `ceil(size / 256 KiB)` I/Os (`ebs_max_io_bytes`). This is a syscall-level
proxy, not a device bound in either direction: writeback can split or merge requests, and the journal adds writes.
After the F1 fix it gave 5.3–26.1 I/Os per MiB across operations and schemas: insert writes 8.5–14.1 (narrow), 15–16
(mixed types) and 26 (complex types); merge reads 10–20; scans 8–16. That is 1.3–6.5× the floor. A Wide part writes one data and one marks stream per substream
(`src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:220`, enumerated at `:243`), so more substreams mean more,
smaller files. No envelope over three schemas bounds the next one, so the proxy is evidence, not a coefficient.
The gp3 volume includes 3000 IOPS, which at 4 I/Os per MiB covers 750 MiB/s of demand.

Each operation cell emits one replica's peak demand (rows scope `volume`). Billing allocates per replica and
multiplies by `replicas`.

```
mibps = logical GiB/day × 1024 / 86400 × peak_factor × Σ_dir io_bytes_per_op[op, dir]
iops  = logical GiB/day × 1024 / 86400 × peak_factor × Σ_dir io_bytes_per_op[op, dir] × ebs_ios_per_mib[op, dir]
```

`inserts` uses `ingest_rate`. `merges` uses `ingest_rate × (write_amplification_rule − 1)`. `storage` is one
`block_capacity` row: `data_bytes × (1 + ebs_headroom)`. `ebs_headroom` is an assumption (default 0.2): rig parts are
small, so the rig's peak-over-active ratio does not carry to production sizes. `local_disk` has no rows: on EBS the
data volume is the local disk.

## Results {#results}

### Fit (`io-sizes.narrow-compact`, `.narrow-wide`, `.types-holdout`, K = 3 each, report run) {#fit}

The I/O column is the syscall proxy (evidence only, not used by the cells): its largest record value, with the mean.
`Array`, `Nullable`, `Map`, `JSON` and `LowCardinality` columns add size, null-map, key, dynamic-path and dictionary
substreams, and projections and skip indices add files. Inserts count 25–26 syscall I/Os per MiB for the
complex-type schema against 8.5–14 for the narrow ones.

| Operation, direction | `io_bytes_per_op` (mean ± spread) | Syscall I/Os per MiB, proxy: max (mean) |
|---|---|---|
| `inserts, read` | 0.0009 ± 0.0004 | 1608 (1335) |
| `inserts, write` | 1.0386 ± 0.0338 | 26.11 (16.1) |
| `merges, read` | 1.0057 ± 0.0038 | 20.27 (15.91) |
| `merges, write` | 1.0138 ± 0.0094 | 11.08 (8.024) |
| `reads, read` | 1.2252 ± 0.1399 | 16.21 (10.96) |
| `reads, write` | 0.0015 ± 0.0020 | no bytes |

Request sizes (fraction of requests): `inserts.read`: <=4KiB 1.00; `inserts.write`: 1MiB 0.10, 256KiB 0.02, 512KiB 0.02, 64KiB 0.02, 8KiB 0.02, <=4KiB 0.80; `merges.read`: 128KiB 0.63, 16KiB 0.01, 32KiB 0.01, 64KiB 0.05, <=4KiB 0.29; `merges.write`: 16KiB 0.02, 1MiB 0.25, 256KiB 0.02, 32KiB 0.01, 512KiB 0.02, 8KiB 0.02, <=4KiB 0.66; `reads.read`: 128KiB 0.80, 8KiB 0.01, <=4KiB 0.18

### Hold-outs {#holdouts}

Two hold-outs were consumed before the one that counts:

1. `io-sizes.types-holdout` (predicted in `e145a15461a`, run `cost-20261024T135713Z`) failed the measured
   `ebs_ios_per_mib`. Inserts gave 25.2–26.1 against 4.2–17.7. Its `io_bytes_per_op` checks passed. Its records
   joined the fit, and `ebs_ios_per_mib` became an upper envelope.
2. `io-sizes.mixed-holdout` (predicted in `dcd7f48fcf8`) passed on `cost-20261024T140341Z`. It failed on
   `cost-20261024T140702Z`: merge reads gave 99–106 I/Os per MiB against a limit of 89. Those counts and the insert
   reads were inflated by `/proc` and `/sys` reads (Codex F1); after the fix the same schema counts 13–14. The envelope does not generalise. `ebs_ios_per_mib` became the pinned rate,
   and the F1 fix changed the read observations.

Both FAILED record sets stay in `measurements.jsonl`, and their edges are `consumed`. The fresh hold-out
`io-sizes.mixed-holdout-2` (the mixed schema, 150 000-row inserts) was predicted from the corrected fit
(`cost-20261024T145139Z`) in `956cd4ec795`, before its first run. The table below is from the report run.

| Coefficient, key | Hold-out value | Accepted | Result |
|---|---|---|---|
| `io_bytes_per_op`, `inserts.read` | 0.0007 | 0.0009 ± 0.0059 | pass |
| `io_bytes_per_op`, `inserts.write` | 1.0349 | 1.0386 ± 0.1764 | pass |
| `io_bytes_per_op`, `merges.read` | 1.0044 | 1.0057 ± 0.1131 | pass |
| `io_bytes_per_op`, `merges.write` | 1.0180 | 1.0138 ± 0.1251 | pass |
| `io_bytes_per_op`, `reads.read` | 1.3052 | 1.2252 ± 0.4074 | pass |
| `io_bytes_per_op`, `reads.write` | 0.0025 | 0.0015 ± 0.0092 | pass |
| `io_request_size_distribution`, `inserts.read` | TV 0.000 | TV ≤ 0.050 | pass |
| `io_request_size_distribution`, `inserts.write` | TV 0.087 | TV ≤ 0.281 | pass |
| `io_request_size_distribution`, `merges.read` | TV 0.056 | TV ≤ 0.574 | pass |
| `io_request_size_distribution`, `merges.write` | TV 0.140 | TV ≤ 0.437 | pass |
| `io_request_size_distribution`, `reads.read` | TV 0.143 | TV ≤ 0.672 | pass |
| `peak_factor` | 14.9–15.9 (`merge-natural.wide`) | 1.00 ± 0.105 | FAIL: consumed, the quantity became an assumed input |

### Validation outside the fit domain {#validation}

Total r1 `write_bytes` of the natural-merge cases on `local` against inserted × `io_bytes_per_op[inserts, write]` +
merged × `io_bytes_per_op[merges, write]` + idle rate × wall time:

| Record | Predicted B | Measured B | Measured / predicted |
|---|---|---|---|
| `cost-20261024T145657Z/local/merge-natural/default/k1` | 367 675 192 | 368 562 176 | 1.002 |
| `cost-20261024T145657Z/local/merge-natural/default/k2` | 367 676 325 | 368 492 544 | 1.002 |
| `cost-20261024T145657Z/local/merge-natural/default/k3` | 367 673 779 | 368 562 176 | 1.002 |
| `cost-20261024T145657Z/local/merge-natural/wide/k1` | 320 056 543 | 322 473 984 | 1.008 |
| `cost-20261024T145657Z/local/merge-natural/wide/k2` | 320 055 269 | 322 473 984 | 1.008 |
| `cost-20261024T145657Z/local/merge-natural/wide/k3` | 320 022 589 | 322 359 296 | 1.007 |
| `cost-20261024T145657Z/local/small-inserts/default/k1` | 276 497 186 | 332 800 000 | 1.204 |
| `cost-20261024T145657Z/local/small-inserts/default/k2` | 276 510 741 | 332 910 592 | 1.204 |
| `cost-20261024T145657Z/local/small-inserts/default/k3` | 276 488 078 | 332 709 888 | 1.203 |

`small-inserts.default` (1 000-row parts, outside the validity 80 000–300 000 rows) writes 21 % more than predicted:
per-part metadata files and page rounding weigh more in small parts. The page marks inputs outside the validity as
extrapolated.

### Peak factor {#peak}

The brief defines the peak factor as the 1-s peak over the mean of r1's `write_bytes` rate during natural merges.
The rig samples `/proc/<pid>/io` every second on `local` (`observations.proc_io_series`). `ebs_fit.peak_factors`
takes the window start to the last merge, net of the idle rate.

The measurement does not identify a property of the workload:

- `merge-natural.default` (fit) gives 1.0. Its inserts and all merges finish within about 1.2 s on the local SSD, so
  the active period holds one 1-s interval.
- `merge-natural.wide` (hold-out, predicted 1.0 ± 0.1) gives 15.0–15.9. One merge fires about 15 s after the
  insert burst, and that delay, not the demand, sets the mean.

The hold-out is consumed and `peak_factor` is an assumed input with default 1: provisioning for the mean rate. On
EBS a merge does not fail when it lacks throughput; it runs longer, within the provisioned rate. A measured value
needs a paced natural-merge case, and Tier 1 has none: the paced `amplification-run.2h` is cut by D14. Both
measurements stay in coverage: the fit edge is `done`, the hold-out edge `consumed`.

## Cut operations {#cut}

The `mutations`, `deletes`, `reads` and `recovery` cells are `unsupported_cut`, and their planned `io_bytes_per_op`
edges are `cut`, by owner ruling: Task 7 found that the ops cards run untraced, and the measured path (one flocked run
of the ops cards on `local` with predictions) did not fit the 18:30 cap. The reads derivation from the s3 report
(`bytes_per_get[shape]` × GETs) was not used either: `bytes_per_get` has no `narrow` key, so the default shape would
make the column unavailable. The cells have no rows, so the page prices nothing for these operations on EBS and shows
them as not modelled.

| Operation | Card that would measure it on `local` |
|---|---|
| `mutations`, `deletes` | `mutation.compact-update-one-column`, `mutation.wide-delete`, `mutation.lightweight-delete`, `drop.partition` |
| `reads` | `cold-select.cache-off`, or the s3 read plan once `bytes_per_get` covers every shape |
| `recovery` | `startup.intact`, `replication.replication-fetch` |

`storage` lists `local_volumes` as supported, because the page evaluates only operations with a listed family. Its
one row reads inputs only (`data_bytes`, the assumed `ebs_headroom`), so no coefficient backs it. The support closure
lists it as pending in dev mode, and final mode fails until the capacity overhead is measured or the cell is ruled
assumption-only.

## Review {#review}

One Codex round (`gpt-6-astra`, reasoning high, read-only; log `tmp/codex_ebs_r1.log`): 6 MAJOR, 3 MINOR.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| F1 | MAJOR | `/proc` and `/sys` reads counted as volume I/O | Fixed test-first (`71399e1090e`): only the replica's run directory and local disk count. Re-fit and fresh hold-out after the fix. |
| F2 | MAJOR | Syscall `ceil(size / 256 KiB)` is not a device-IOPS upper bound | Fixed: IOPS are priced at the pinned provider floor (`ebs_sequential_ios_per_mib`, lower bound); the syscall counts are evidence only and the README calls them a proxy. |
| F3 | MAJOR | Returned read bytes do not bound cold-device reads | Fixed in prose and registry: read demand is the bytes the server reads, with no bound direction claimed. |
| F4 | MAJOR | Baseline subtraction removes server background demand | Fixed in prose: the background (mostly the server's system logs, about 0.06 MiB/s) is quantified and stated as excluded from EBS demand; the Keeper share is the r1 − r2 difference. |
| F5 | MAJOR | `ebs_fit` cannot reproduce the published coefficients from consumed FAILED records | Fixed: the report run has `ok` records of every fit case; `ebs_fit` reproduces `coefficients.json` from `measurements.jsonl` (README §Reproduce). Consumed FAILED records stay as evidence only. |
| F6 | MAJOR | The start-timeout test and `server.sh` take a zombie for a live server | Fixed test-first (`71399e1090e`): liveness ignores state `Z`. |
| F7 | MINOR | Source references: 128 KiB local read buffer, partition loop, `columns_substreams.txt` writer, buffer-size wording | Fixed (`cea0c37c6d3`). |
| F8 | MINOR | Calibration hint omits the vector and no-wait calls | Fixed in the registry hint. |
| F9 | MINOR | README presented the failed peak hold-out as confirmation | Fixed: [Peak factor](#peak) states both values and the scheduling cause; `peak_factor` is an assumed input. |

## Reproduce {#reproduce}

```bash
flock tmp/yardstick.lock sh -c 'YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost run \
  --binary tmp/bin/26.8/clickhouse --manifests docs/superpowers/reports/2026-10-24-storage-cost-ebs \
  --cases io-sizes.idle,io-sizes.narrow-compact,io-sizes.narrow-wide,io-sizes.types-holdout,io-sizes.mixed-holdout-2,merge-natural.default,merge-natural.wide,merge-forced.default,small-inserts.default \
  --policies local --replicas 2 --repeats 3 \
  --runs-dir tmp/cas-yardstick/cost-runs'
cd utils/cas-yardstick && python3 -m yardstick.cost.ebs_fit ../../docs/superpowers/reports/2026-10-24-storage-cost-ebs/measurements.jsonl
```
