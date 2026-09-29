# CAS yardstick

Runs seven fixed workload cards on a CAS disk and on two ordinary S3 disks, and reports wall time, S3 API calls and payload bytes, cold and hot, with paired ratios. One server process per measurement, a fresh bucket prefix per measurement.

```bash
# First thing to run after any change to the tool: all cards, all policies, tiny scale, a few minutes.
python3 utils/cas-yardstick/yardstick.py run --smoke --binary build/programs/clickhouse

# The real measurement: 3 repeats of the default card set (add --compare to check against reference/CURRENT).
python3 utils/cas-yardstick/yardstick.py run --binary build/programs/clickhouse --repeats 3
```

`--smoke` exits 2 on any `FAILED` or `unavailable` cell. `--cards` defaults to `cycle`; pass a comma-separated list for more cards.

## Prerequisites

- Docker, with the pinned images pulled on first use: `rustfs/rustfs:1.0.0-rc.3` and `minio/mc:RELEASE.2025-04-16T18-13-26Z` (see `rustfs.sh`).
- A `clickhouse` binary with CAS disks (`metadata_type=cas`) and the `system.cas_gc_log` table.
- Ports from `--port-base` (19700) for the servers and 19121 for RustFS. `--endpoint URL` uses a store that is already running instead of starting RustFS; it requires `--keep-prefix`, because prefix removal only reaches the RustFS of `rustfs.sh`.

## Commands

| Command | What it does |
|---|---|
| `run` | Measures the cards, prints the table, writes `runs/<run-id>/`. Exit non-zero if any cell is `FAILED`. |
| `report runs/<id>` | Re-prints the table of a finished run. |
| `compare runs/<id> [reference]` | Compares a run with a reference run (default: the one named by `reference/CURRENT`). |
| `--list-counters` | Prints the counters behind every aggregate. |

`run` options:

| Option | Default | Meaning |
|---|---|---|
| `--binary` | required | `clickhouse` binary to start servers from. |
| `--cards` | `cycle` | Cards to run. |
| `--policies` | all six | Subset of `cas, s3, s3plain, cas_cached, s3_cached, s3plain_cached`. |
| `--repeats` | 3 | Repeats per (policy, card). |
| `--scale` | 1.0 | Multiplies every count as `max(1, round(scale * n))`. A reference is defined at 1.0 only. |
| `--gc` | `on` | `off` runs CAS with `cas_gc_enabled = 0`; the window is then labelled "GC-disabled process window". |
| `--endpoint`, `--endpoint-version` | RustFS started by `rustfs.sh` | Use a running store; the manifest records `external` and the stated version. |
| `--port-base` | 19700 | First port of the per-process server port range. |
| `--runs-dir` | `runs` | Where run directories go. |
| `--keep-prefix` | off | Keep the run's bucket prefix after the run. |
| `--compare`, `--reference` | off | Compare with `reference/CURRENT` (or the given run dir) after the run. |
| `--smoke` | off | All cards, all policies, `--scale 0.01`, `--repeats 1`. |

## Policies and disks

Each server process has one disk under test and the local default disk for system tables. A cache wrapper (`type=cache`, identical settings, one cache directory per process) is added for the `_cached` policies.

| Policy | Disk |
|---|---|
| `cas` | `object_storage`, `object_storage_type=s3`, `metadata_type=cas`, `cas_gc_interval_sec=20` |
| `s3` | `type=s3` with local metadata files |
| `s3plain` | `type=s3_plain_rewritable` (write-once `s3_plain` cannot run the cards) |

The S3 client block is identical for the three. Card tables pin `primary_key_lazy_load = 0`, `prewarm_mark_cache = 0`, `prewarm_primary_key_cache = 0`. The effective config is copied into the run directory and hashed into the manifest.

## Cards

Every card is `setup` (not measured), a cold reset where it has a fixture, `measured`, an optional hot pass, `teardown`. Row counts and active part counts are checked before and after each measured pass; a violation marks the pass `FAILED`.

| Card | Unit | Measured step | Cold reset | Merges off | Hot pass |
|---|---|---|---|---|---|
| `cycle` | cycle (200) | 8 workers x 25 iterations of: `CREATE TABLE`, 3 inserts of 1000 rows, `SELECT count(), sum(k)`, `DROP TABLE SYNC` | no | no | second pass, new table names |
| `wide_insert` | insert (500) | 500 sequential 20000-row inserts into a 30-column table (10M rows, 500 parts) | no | yes | `TRUNCATE` (not measured), then the same 500 inserts |
| `attach_200` | part (200) | `DETACH TABLE` + `ATTACH TABLE` of 200 parts, 2M rows | yes | yes | the same pair |
| `mutation` | part (100) | `ALTER TABLE ... UPDATE` of every row of 100 parts, `mutations_sync = 2` | yes | yes | the same with the next pass value |
| `lwd_concurrent` | delete (80) | 8 workers x 10 lightweight deletes on 50 parts, 5M rows; half the rows go | yes | no | 8 x 10 deletes removing the other half |
| `select_subset` | query (1) | `SELECT sum(c3)` over 200 compact parts of 10 `UInt64` columns | yes | yes | the same query |
| `dup_content` | table (20) | 20 x (`CREATE TABLE` + 1M-row insert of identical content) | no | no | 20 more tables, same content; CAS dedups against the cold pass |

Notes:

- Merges are off through `max_parts_to_merge_at_once = 1` on the table. Unlike `SYSTEM STOP MERGES` it survives `DETACH`/`ATTACH` and the cold restart, and it does not block mutations. The part-count invariants pin it.
- `lwd_concurrent` sets `lightweight_delete_mode = 'lightweight_update_force'`. The default mode registers concurrent deletes as mutations and loses some of them (ClickHouse#122083).
- Cards that create their data inside `measured` (`cycle`, `wide_insert`, `dup_content`) have no fixture to reset. Cold is the first pass on the fresh process, hot the second.

### Capability matrix

An unsupported pair prints `UNSUPPORTED (<reason>)` in every cell, gets no ratio, and is not a failure. An unexpected error on a supported pair is `FAILED`.

| Card | `cas`, `s3` (+ cached) | `s3plain`, `s3plain_cached` |
|---|---|---|
| `cycle`, `wide_insert`, `attach_200`, `select_subset`, `dup_content` | run | run |
| `mutation`, `lwd_concurrent` | run | `UNSUPPORTED (immutable disk)`: `s3_plain_rewritable` has no hard links and the server rejects `ALTER ... UPDATE` and `DELETE` with error 344 |

## What "cold" resets

Setup builds the fixture, then the server is stopped and started again on the same run directory with the cache directory emptied. After start the runner executes `SYSTEM DROP FILESYSTEM CACHE`, `DROP MARK CACHE`, `DROP UNCOMPRESSED CACHE` and `DROP PRIMARY INDEX CACHE` (where the binary has it).

Cold means: the server process, its metadata and CAS in-memory state, and the filesystem cache are cold. The store is not. Not reset:

- the OS page cache holding the `s3` disk's local metadata files;
- RustFS's own data and process caches.

## Measurement

- **Window.** The counter delta covers everything the process did between two `system.events` snapshots: the measured step, CAS GC rounds and mount renewal, obsolete-part cleanup, cache downloads, and merges where they are on. It does not isolate the requests of `measured` alone. A bounded settle (`SYSTEM FLUSH LOGS`, no running merges or unfinished mutations on the card table, at most 60 s) precedes each window.
- **API calls, not wire requests.** GET, PUT, HEAD, LIST, DELETE and COPY are sums of `DiskS3*` counters (an N-part upload is N + 2 PUT); the exact list is in `--list-counters`. Bytes are `ReadBufferFromS3Bytes` and `WriteBufferFromS3Bytes`: buffer payload, not network bytes.
- **Fresh prefix.** Every (policy, card, repeat) uses `test/<run-id>/<policy>/<card>/k<N>/` and never reuses it, because `DROP TABLE` does not empty a CAS prefix. Prefixes are deleted after the run.
- **Order.** Policies rotate per repeat so store drift is not confounded with the policy.
- **Wall.** Runner clock around `measured`. For concurrent cards it is elapsed time; per-operation latency is in `median_op_ms` and `p90_op_ms`. Per-unit values divide by the actual unit count from `invariants.tsv`.

## Output

Two blocks, uncached and cached policies. Rows are card x phase (cold, hot). Per policy: `wall`, `median_op_ms`, `p90_op_ms`, GET, PUT, HEAD, LIST, DELETE, COPY, MiB read, MiB written, and for CAS policies `GC rounds`. Then the paired ratios `cas/s3` and `cas/s3plain` for wall, GET, PUT and bytes, and a per-unit line per card.

Each cell is the median over repeats with min..max. Ratios are computed per repeat, then summarized.

| Cell or mark | Meaning |
|---|---|
| `~` | The count differs across repeats. Expected wherever merges, GC or retries are on. |
| `noisy` | Wall max/min over repeats is above 1.2. |
| `0 -> n` | A ratio with a zero denominator, shown as absolute counts. |
| `UNSUPPORTED (reason)` | The pair cannot run the card (capability matrix). |
| `FAILED` | Error, timeout, violated invariant or server that did not start. No numbers; logs kept; the run continues; exit code is non-zero. |
| `unavailable` | The binary lacks a counter behind the metric. Its ratios and comparisons are suppressed. |

**GC column.** After each pass the runner flushes logs and collects `system.cas_gc_log` rounds whose span overlaps the window into `gc_rounds.tsv`. The table shows "GC rounds: N (M overlapping)". A 20 s GC interval on a short window does not amortize reclamation; use a long GC soak for that, not this tool.

### Run directory

```
runs/<run-id>/manifest.tsv  samples.tsv  summary.tsv
runs/<run-id>/<policy>/<card>/k<N>/{cold,hot}/
    events_before.tsv events_after.tsv events_delta.tsv card.tsv invariants.tsv gc_rounds.tsv server.log
```

`events_delta.tsv` keeps every counter; the table shows aggregates. `error.txt` appears next to a `FAILED` pass.

## Reference and compare

`reference/<date>-<git-rev>/` holds `manifest.tsv`, `samples.tsv`, `summary.tsv` of an accepted run. `reference/CURRENT` names the one in force.

`compare` checks manifests first. Workload revision, scale, counts, `--gc`, repeats, config hash, build type and store must match. Otherwise it prints `incompatible: <field>` per differing field and nothing else. When compatible it prints current vs reference (median and min..max) per metric, the delta of medians in percent, and marks `|delta wall| > 10 %` and any change of a request-count median. It fails no run; there are no thresholds.

Accept a run as the reference:

```bash
cp -r runs/<run-id> utils/cas-yardstick/reference/<date>-<rev>   # keep only manifest.tsv, samples.tsv, summary.tsv
echo <date>-<rev> > utils/cas-yardstick/reference/CURRENT
```

Accept only a complete run (`--scale 1`, `--repeats 3`, six policies) with no `FAILED` cell.

### Current reference

`reference/2026-09-29-0d141ca23c6/`: `cycle` card only, all six policies, `--scale 1`, 3 repeats, GC on, RustFS 1.0.0-rc.3 on the same host, binary `26.6.2.20000.altinityantalya` (`RelWithDebInfo`). No `FAILED` cell. Other cards have no reference yet; `compare` needs a run with a matching manifest, so run `cycle` alone to compare.

### RCA context

`reference/2026-09-28-rca-context.md` holds the numbers of the earlier ad-hoc measurement (83 GET + 64 PUT per cycle on CAS, 12 + 31 on plain S3). It is context, not a reference: workload, server and store differ, and `compare` does not accept it.

Per-cycle API calls of the current reference (median over 3 repeats; cold and hot agree to within one call):

| Policy | GET | PUT | HEAD | LIST | DELETE | wall per cycle, cold / hot |
|---|---|---|---|---|---|---|
| `cas` | 83 | 64 | 5 | 7 | 1 | 21.3 / 22.9 ms |
| `s3` | 6 | 31 | 0 | 0 | 2 | 15.9 / 15.4 ms |
| `s3plain` | 6 | 44 | 36 | 0 | 67 | 83.9 / 79.6 ms |

Against the RCA numbers:

- **CAS 83 GET, 64 PUT, 7 LIST, 5 HEAD, 1 DELETE:** identical to the RCA, so the request cost of a CAS cycle did not move.
- **`s3` 6 GET instead of 12:** the RCA plain-S3 process also had a CAS disk registered, which is a likely but unverified cause. PUT (31) matches. The RCA lists 2 DeleteObjects calls, the reference 2.
- **CAS/`s3` wall 1.33x instead of 3.1x:** the window here is 200 cycles and about 4 s, with no GC round overlapping it (`GC rounds: 0`), while the RCA figure comes from 8 x 1000 cycles with GC on. This run does not test the long-window behavior.
- **`s3plain` needs 44 PUT, 36 HEAD, 67 DELETE and 31 COPY per cycle:** the RCA has no figure for this disk. Its wall time is the highest of the three.
