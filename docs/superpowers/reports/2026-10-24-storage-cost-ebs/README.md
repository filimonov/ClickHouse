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
  counts were 3–25× higher on this rig, depending on the schema. The gp3 baseline of 3000 IOPS usually dominates.
- **`peak_factor`** (assumed input, default 1): the rig cannot measure it; see [Peak factor](#peak).
- **`op_write_bytes`**: write bytes per mutated row (measured); drops (273 kB per part) and restarts (0) by
  assumption; reads by assumption. See [Mutations, deletes, recovery and reads](#ops).

| Item | Value |
|---|---|
| Binary | antalya-26.8 `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` |
| Rig policy / model variant | `local` / `ebs` |
| Report run | `cost-20261024T174727Z`: all cases, K = 3, one rig commit |
| Ops predictions | `be94501e681` (manifests of the s3 report on `local`) |
| Earlier runs | `cost-20261024T080735Z` (fit, before the `preadv2` fix), `cost-20261024T132526Z` (fit), `cost-20261024T135713Z` and `cost-20261024T140702Z` (consumed hold-outs, see [Hold-outs](#holdouts)), `cost-20261024T140341Z`, `cost-20261024T160737Z` (fit after the F1 fix) |
| Predictions | source `d5cf507a02c`; consumed hold-outs `e145a15461a`, `dcd7f48fcf8`; hold-out `b498967f6f3` |
| Card | `utils/cas-yardstick/yardstick/cost/cards/io_sizes.py`; fit rules `utils/cas-yardstick/yardstick/cost/ebs_fit.py` |

## Measurement boundary {#boundary}

This boundary limits every coefficient here. The registry entries carry it in `evidence.ref`; `validity` holds only
registry quantities (`insert_rows`, `mutated_part_rows`), so scale 0.05 and the warm cache are stated in
`evidence.ref`, not in `validity`.

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
Per schema it gave 11 I/Os per MiB on insert writes (narrow), 26 (complex types), and up to 106 on merge reads (mixed
types). That is 3–25× the floor. A Wide part writes one data and one marks stream per substream
(`src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:220`, enumerated at `:243`), so more substreams mean more,
smaller files. No envelope over three schemas bounds the next one, so the proxy is evidence, not a coefficient.
The gp3 volume includes 3000 IOPS, which at 4 I/Os per MiB covers 750 MiB/s of demand.

Each operation cell emits one replica's peak demand (rows scope `volume`). Billing allocates per replica and
multiplies by `replicas`.

```
mibps = logical GiB/day × 1024 / 86400 × peak_factor × Σ_dir io_bytes_per_op[op, dir]
iops  = logical GiB/day × 1024 / 86400 × peak_factor × Σ_dir io_bytes_per_op[op, dir] × ebs_ios_per_mib[op, dir]
```

`inserts` uses `ingest_rate`. `merges` uses `ingest_rate × (write_amplification_rule − 1)`. The inserts, merges and
reads cells carry a note that IOPS are a lower bound. `storage` is one `block_capacity` row:
`data_bytes × ebs_capacity_headroom`; see [Mutations, deletes, recovery and reads](#ops). `local_disk` has no rows:
on EBS the data volume is the local disk.

## Results {#results}

### Fit (`io-sizes.narrow-compact`, `.narrow-wide`, `.types-holdout`, K = 3 each, report run) {#fit}

The I/O column is the syscall proxy (evidence only, not used by the cells): its largest record value, with the mean.
`Array`, `Nullable`, `Map`, `JSON` and `LowCardinality` columns add size, null-map, key, dynamic-path and dictionary
substreams, and projections and skip indices add files. Inserts count 25–26 syscall I/Os per MiB for the
complex-type schema against 11 for the narrow ones.

| Operation, direction | `io_bytes_per_op` (mean ± spread) | Syscall I/Os per MiB, proxy: max (mean) |
|---|---|---|
| `inserts, read` | 0.0002 ± 0.0002 | no bytes |
| `inserts, write` | 1.0323 ± 0.0356 | 25.02 (14.56) |
| `merges, read` | 1.0047 ± 0.0035 | 18.93 (15.14) |
| `merges, write` | 1.0066 ± 0.0094 | 9.315 (6.219) |
| `reads, read` | 1.2249 ± 0.1396 | 15.93 (10.78) |
| `reads, write` | -0.0000 ± 0.0000 | no bytes |

Request sizes (fraction of requests): `inserts.write`: 1MiB 0.12, 256KiB 0.02, 512KiB 0.02, 64KiB 0.02, <=4KiB 0.78; `merges.read`: 128KiB 0.66, 16KiB 0.01, 32KiB 0.01, 64KiB 0.05, <=4KiB 0.26; `merges.write`: 16KiB 0.03, 1MiB 0.46, 256KiB 0.02, 32KiB 0.02, 512KiB 0.02, <=4KiB 0.44; `reads.read`: 128KiB 0.82, 8KiB 0.01, <=4KiB 0.16

### Hold-outs {#holdouts}

Two hold-outs were consumed before the one that counts:

1. `io-sizes.types-holdout` (predicted in `e145a15461a`, run `cost-20261024T135713Z`) failed the measured
   `ebs_ios_per_mib`. Inserts gave 25.2–26.1 against 4.2–17.7. Its `io_bytes_per_op` checks passed. Its records
   joined the fit, and `ebs_ios_per_mib` became an upper envelope.
2. `io-sizes.mixed-holdout` (predicted in `dcd7f48fcf8`) passed on `cost-20261024T140341Z`. It failed on
   `cost-20261024T140702Z`: merge reads gave 99–106 I/Os per MiB against a limit of 89, and insert reads were
   inflated by `/proc` reads (Codex F1). The envelope does not generalise. `ebs_ios_per_mib` became the pinned rate,
   and the F1 fix changed the read observations.

3. `io-sizes.mixed-holdout-2` (predicted from `cost-20261024T145139Z` in `956cd4ec795`) failed on
   `cost-20261024T160737Z` only because its insert-read distribution no longer exists: `c2a9eed0b85` moved the part-log
   flush out of the measured window, and those reads were the flush. Every other key passed.

The FAILED record sets stay in `measurements.jsonl`, and their edges are `consumed`. The fresh hold-out
`io-sizes.mixed-holdout-3` (the mixed schema, 180 000-row inserts) was predicted from `cost-20261024T160737Z` in
`b498967f6f3`, before its first run. The table below is from the report run.

| Coefficient, key | Hold-out value | Accepted | Result |
|---|---|---|---|
| `io_bytes_per_op`, `inserts.read` | 0.0000 | 0.0002 ± 0.0055 | pass |
| `io_bytes_per_op`, `inserts.write` | 1.0209 | 1.0323 ± 0.1794 | pass |
| `io_bytes_per_op`, `merges.read` | 1.0025 | 1.0047 ± 0.1125 | pass |
| `io_bytes_per_op`, `merges.write` | 1.0048 | 1.0066 ± 0.1245 | pass |
| `io_bytes_per_op`, `reads.read` | 1.3151 | 1.2249 ± 0.4067 | pass |
| `io_bytes_per_op`, `reads.write` | -0.0000 | -0.0000 ± 0.0051 | pass |
| `io_request_size_distribution`, `inserts.write` | TV 0.096 | TV ≤ 0.378 | pass |
| `io_request_size_distribution`, `merges.read` | TV 0.084 | TV ≤ 0.579 | pass |
| `io_request_size_distribution`, `merges.write` | TV 0.116 | TV ≤ 0.726 | pass |
| `io_request_size_distribution`, `reads.read` | TV 0.111 | TV ≤ 0.686 | pass |
| `peak_factor` | 15.0–15.9 (`merge-natural.wide`) | 1.00 ± 0.105 | FAIL: consumed, the quantity became an assumed input |

### Validation outside the fit domain {#validation}

Small parts write more per byte. 1 000-row inserts (`io-sizes.narrow-compact-1k`) write 2.29, 2.29, 2.29 bytes per byte against 1.032 for 80 000–300 000-row parts. Each part carries files that do not grow with its rows (marks, `checksums.txt`, `columns.txt`, `count.txt`, `serialization.json`, `statistics.packed`, and page rounding of each), so the model adds a fixed per-part overhead: inserts write `io_bytes_per_op[inserts, write]` + `insert_write_overhead_rows` / `insert_rows`, with `insert_write_overhead_rows` = 1258 rows. The hold-out `io-sizes.narrow-compact-10k` (predicted in `06d0354a09a` before its run, assuming the overhead is fixed per part) gave 1.1671, 1.1671, 1.1689 against 0.979–1.338: passed, so the insert-size validity is 1 000–300 000 rows.

Total r1 `write_bytes` of the natural-merge cases on `local` against inserted × `io_bytes_per_op[inserts, write]` +
merged × `io_bytes_per_op[merges, write]` + idle rate × wall time:

| Record | Predicted B | Measured B | Measured / predicted |
|---|---|---|---|
| `cost-20261024T174727Z/local/merge-natural/default/k1` | 365 242 002 | 368 566 272 | 1.009 |
| `cost-20261024T174727Z/local/merge-natural/default/k2` | 365 240 176 | 368 578 560 | 1.009 |
| `cost-20261024T174727Z/local/merge-natural/default/k3` | 365 239 689 | 368 566 272 | 1.009 |
| `cost-20261024T174727Z/local/merge-natural/wide/k1` | 317 938 806 | 322 478 080 | 1.014 |
| `cost-20261024T174727Z/local/merge-natural/wide/k2` | 317 938 418 | 322 453 504 | 1.014 |
| `cost-20261024T174727Z/local/merge-natural/wide/k3` | 317 937 237 | 322 482 176 | 1.014 |
| `cost-20261024T174727Z/local/small-inserts/default/k1` | 274 590 797 | 332 779 520 | 1.212 |
| `cost-20261024T174727Z/local/small-inserts/default/k2` | 274 585 618 | 332 640 256 | 1.211 |
| `cost-20261024T174727Z/local/small-inserts/default/k3` | 274 584 260 | 332 705 792 | 1.212 |

`small-inserts.default` (1 000-row parts with natural merges) writes about 21 % more than predicted: per-part
metadata files and page rounding weigh more in small parts.

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

## Mutations, deletes, recovery and reads {#ops}

The ops cards of the s3 report run on `local` untraced. `/proc/<pid>/io` `write_bytes` is recorded in every pass,
so `op_write_bytes` is the window's net r1 write bytes (idle rate × wall time subtracted) per unit:

| Operation | Unit | Fit | Hold-out |
|---|---|---|---|
| `deletes` | B per dropped part | 2.359e+05 ± 3.73e+04 | 1.248e+05, 1.25e+05, 1.247e+05 (fails; priced by assumption at 217 kB per part, ± 1.1e+05) |
| `mutations` | B per source row | 13.31 ± 9.77 | 26.62, 26.62, 26.62 (pass, ± 21.5) |
| `recovery` | B per restart | -3.595e+04 ± 643 | -3.701e+04, 1.99e+04, -1.03e+05 (priced 0 by assumption, ± 6.68e+03) |

Mutations are measured: `op_write_bytes[mutations]`, with a fit and a passed hold-out. Drops and restarts are priced
by explicit assumption rows (owner ruling), with both measurements kept as evidence:

- **Drops:** 273 kB written per dropped part, the largest measured record. Net writes are steady within each case,
  but per dropped part the fit gives 236 kB (records up to 273 kB) and the hold-out 125 kB, so the per-part unit does not carry between
  partition layouts. The row is an upper-bound assumption, pending a Tier 2 model keyed on part size. The hold-out
  edge is `consumed`.
- **Restarts:** priced 0. Net writes are negative after the idle rate × wall time: a restart writes less than an
  idle server, so its demand is below the rig's resolution. This is an assumption.

Both cells carry a note saying so. Reads issued by these operations (a mutation reads its source part) are not
modelled. Replica fetches are not measured on `local`.

`mutations` = (mutated Compact + Wide parts per month) × `mutated_part_rows` × `op_write_bytes[mutations]`, one
replica's mean rate × `peak_factor`, IOPS at the floor.

`reads` is a derivation (an assumption, by owner ruling), listed under the `lifecycle` family because its one
coefficient, the scan rate, is measured by the `io-sizes` card: every part read is a full scan of an average part,
`select_part_reads_per_day` × `data_bytes` / `active_parts` × `io_bytes_per_op[reads, read]` (the measured full-scan
rate), priced as IOPS at the floor. Page-cache hits are not modelled; EBS reads are priced as IOPS only (owner
decision). The s3 report's `bytes_per_get[shape]` was not used: it has no `narrow` key, the default shape.

`storage` is an explicit assumption row (owner ruling): provisioned capacity = `data_bytes` × `ebs_capacity_headroom`
(assumed input, default 1.3: free space and temporary merge output). The rig shows that headroom is needed, and
cannot size it: the `merge-natural` allocated peaks on `local` exceed the final active bytes because merged-away
parts stay until `old_parts_lifetime`, and the phase-0 `probe_local_volume` capacity peak shows the same. Its parts
are too small for the ratio to carry to production sizes.

## Review {#review}

One Codex round (`gpt-6-astra`, reasoning high, read-only; log `tmp/codex_ebs_r1.log`): 6 MAJOR, 3 MINOR.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| F1 | MAJOR | `/proc` and `/sys` reads counted as volume I/O | Fixed test-first (`71399e1090e`): only the replica's run directory and local disk count. Re-fit and fresh hold-out after the fix. |
| F2 | MAJOR | Syscall `ceil(size / 256 KiB)` is not a device-IOPS upper bound | Fixed: IOPS are priced at the pinned provider floor (`ebs_sequential_ios_per_mib`, lower bound); the syscall counts are evidence only and the README calls them a proxy. |
| F3 | MAJOR | Returned read bytes do not bound cold-device reads | Fixed in prose, the registry `evidence.ref` and the `io_sizes.py` docstring: read demand is the bytes the server issues, with no bound direction. |
| F4 | MAJOR | Baseline subtraction removes server background demand | Fixed in prose and the registry `evidence.ref`: the subtracted idle rate is server background (mostly system logs, about 0.06 MiB/s) plus about 2.3 kB/s of Keeper (r1 − r2); it is stated as excluded from EBS demand. |
| F5 | MAJOR | `ebs_fit` cannot reproduce the published coefficients from consumed FAILED records | Fixed: the report run has `ok` records of every fit case; `ebs_fit` reproduces `coefficients.json` from `measurements.jsonl` (README §Reproduce). Consumed FAILED records stay as evidence only. |
| F6 | MAJOR | The start-timeout test and `server.sh` take a zombie for a live server | Fixed test-first (`71399e1090e`): liveness ignores state `Z`. |
| F7 | MINOR | Source references: 128 KiB local read buffer, partition loop, `columns_substreams.txt` writer, buffer-size wording | Fixed (`cea0c37c6d3`). |
| F8 | MINOR | Calibration hint omits the vector and no-wait calls | Fixed in the registry hint. |
| F9 | MINOR | README presented the failed peak hold-out as confirmation | Fixed: [Peak factor](#peak) states both values and the scheduling cause; `peak_factor` is an assumed input. |


A task review of `108c7853103` added: registry refs and the card docstring still claimed an upper bound and a
Keeper-only baseline (fixed with F3/F4); the boundary was not in `validity` (stated in `evidence.ref` instead, see
[Measurement boundary](#boundary)); `SYSTEM FLUSH LOGS` ran inside the insert and merge windows (fixed in
`c2a9eed0b85` before the final report run); the test file's `__main__` guard, the lower-bound label and hint, and
cell notes (fixed).

## Reproduce {#reproduce}

```bash
flock tmp/yardstick.lock sh -c 'YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost run \
  --binary tmp/bin/26.8/clickhouse --manifests docs/superpowers/reports/2026-10-24-storage-cost-ebs \
  --cases io-sizes.idle,io-sizes.narrow-compact,io-sizes.narrow-wide --policies local --replicas 2 --repeats 3 \
  --runs-dir tmp/cas-yardstick/cost-runs'
cd utils/cas-yardstick && python3 -m yardstick.cost.ebs_fit ../../docs/superpowers/reports/2026-10-24-storage-cost-ebs/measurements.jsonl
```
