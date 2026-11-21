# CA soak harness

Soak test for content-addressed MergeTree: two ClickHouse servers in docker share one CAS pool, a seeded
workload runs against them under faults, and the run ends with one verdict. One entry point,
`python3 -m soak.run`, runs three profiles:

| Profile | Workload | Question it answers |
|---|---|---|
| `main` | Seeded ledger of inserts, merges, mutations and cliffs on a `ReplicatedMergeTree` (`soak/run.py`, phase-3 timeline), checked against an in-memory model at quiesced checkpoints | Do both replicas keep exactly the model's data through faults, and is the pool clean (`dangling == 0`, dryrun within the deletion pipeline) at every checkpoint? |
| `intents` | A fixed set of contents inserted and dropped again and again on both servers, cross-table copies (`ATTACH PARTITION FROM`, `MOVE PARTITION TO TABLE`, `CLONE AS`), `SYSTEM CAS GC RUN` back to back (`soak/intents.py`) | Does the blob deletion-intent protocol lose or corrupt a blob when the same content is condemned and referenced again, under kills and a deposed but alive GC leader? |
| `combined` | Both at once; while a `main` checkpoint quiesces the cluster, the intents workload pauses | Both questions in one run. The default for long runs. |

## Commands {#commands}

Run from `utils/ca-soak`. `--binary` is the server binary under test; it is bind-mounted over
`/usr/bin/clickhouse` in the stock image (`CA_SOAK_CLICKHOUSE_BINARY` in the compose files).

```bash
B=/path/to/clickhouse
# 3-minute smokes
python3 -m soak.run --profile main     --duration 3m --seed 1 --binary $B --run-dir /path/to/runs/smoke-main
python3 -m soak.run --profile intents  --duration 3m --seed 1 --binary $B --run-dir /path/to/runs/smoke-intents
# 20-minute proof run, faults compressed
python3 -m soak.run --profile combined --duration 20m --seed 14 --fault-compress --binary $B --run-dir /path/to/runs/combined-20m
# 12-hour run, in the background
D=/path/to/runs/combined-12h; mkdir -p $D
nohup python3 -m soak.run --profile combined --duration 12h --seed 20261121 --binary $B --run-dir $D > $D/stdout.txt 2>&1 &
```

Watch `tail -f $D/run.log`. The run is over when `$D/run.done` exists; its first word is `RESULT=PASS` or
`RESULT=FAIL`, and it holds the exit status (`exit=0` or `exit=1`).

Without `--profile`, `--phase 1|2|3` drives an already running stand (`docker compose up -d`) as before;
`scripts/run_soak.sh` wraps that form.

## Options of a profile run {#options}

| Option | Default | Meaning |
|---|---|---|
| `--profile` | none | `main`, `intents` or `combined` |
| `--binary` | required | server binary, bind-mounted into the stand |
| `--duration` | required | `20m`, `12h`, or seconds |
| `--seed` | required | seeds the workloads; `--chaos-seed` (default: the seed) seeds the main fault schedule |
| `--run-dir` | `logs/runs/<profile>-<timestamp>` | run directory; may exist if it holds no `run.log` |
| `--project` | `ca-soak-profile` | docker compose project; containers are `<project>-ch1-1`, `<project>-ch2-1`, ... |
| `--port-base` | 23300 | host ports: server 1 HTTP / native = base+11 / base+10, server 2 = base+21 / base+20 |
| `--compose-file` | `docker-compose.yml` | repeatable; a cloud stand: `docker-compose-awss3.yml`, `docker-compose-gcs.yml` |
| `--env-file` | none | repeatable; passed to `docker compose` (the GCS stand needs `configs/gcs.env`) |
| `--fault-compress` | off | the intents fault track every 2-5 min instead of every 5-12 min |
| `--no-chaos` | off | no faults |
| `--chaos-interval` | 90 | mean seconds between faults of the main schedule |
| `--contents` / `--intent-writers` | 40 / 4 | intents workload: distinct contents, writers per server |
| `--workers`, `--select-workers`, ... | as for `--phase 3` | main workload |
| `--drain-cap-s` | 3600 | hard cap of the end-of-run drain; reaching it fails the run |
| `--min-free-gb` | 30 | refuse to start with less free disk under the run directory |
| `--keep-stand` | off | leave the stand running after the verdict |

The stand is the compose files plus `compose.override.yml`, written into the run directory: it publishes the
servers on the run's host ports only, keeps their logs inside the containers (copied into the run directory
at the end), and mounts `configs/soak_system_logs.xml` (log rotation, ten-minute `cas_log` partitions). The
run removes the stand with `docker compose down -v` at the end, except after a drain that left the pool non-empty or with `--keep-stand`: `run.log` and `KEPT_FOR_ANALYSIS` then print the removal command. A cloud stand keeps the pool in the bucket:
bump the pool prefix in its `storage_conf_*` and `fsck_only_*` files before every run.

## Faults {#faults}

One schedule, run by one `ChaosRunner` through `chaos.ObservedFaultApplier`, one fault at a time:

- `main` and `combined`: the main schedule (`generate_chaos_schedule`: kill, restart, pause of one or both
  servers, RustFS restart or pause, a long freeze of one server) inside the phase-3 chaos window.
- `intents` and `combined`: the intents track (`intents_fault_track`): a long freeze (60-90 s, past the mount
  lease) of the GC leader, `docker kill` of one server, a short pause of one server. In `combined` it covers
  the run outside the chaos window; in `intents` it is added to a main schedule over the whole run.

The applier starts a fault only while both servers are live on the pool, aims every long freeze at the current
GC leader, and after the fault requires each affected server to serve a write and a read through the CAS disk
within 180 s. No fault starts in the last 90 s. `faults.jsonl` holds one line per fault and affected server.

## What PASS means {#pass}

`RESULT=PASS failures=0`, where failures add up:

| Source | Failure if |
|---|---|
| driver, main | a checkpoint fails (`failure.json`): replicas differ from the model, `dangling > 0`, a stale-edge blob that two detail scans report with at least two led GC rounds committed between them, a dryrun key outside the deletion pipeline, a late-PUT counter moved |
| driver, intents and the strict end | a read-back that is not a possible state of the table, a query error outside a fault's noise window, `CHECK TABLE` not 1, an online fsck hard finding (`dangling`, `chain_broken`, `corrupted_runs`, `lifeless_keys`, `intent_malformed`, `intent_unread`, `pending_without_intent`) that repeats 5 s later, a table left after the drop, a pool that does not drain (`failures.jsonl`) |
| harness | a server not running after the workload, `cas-fsck --detail` exiting non-zero or reporting stale edges after settling or after draining, a metrics db write that fails five times in a row, a stale-edge checkpoint scan whose GC round wait timed out, evidence that could not be dumped, an error of the harness itself |
| faults | a server that does not serve a read 180 s after its fault |
| log scan | a hit of `LOGICAL_ERROR`, `Logical error`, `Segmentation`, `Fatal`, `Sanitizer`, `SIGSEGV`, `SIGABRT` in a server log, or of `CORRUPTED_DATA` / `dangling` outside a fault window, unless `configs/log_allow.txt` allows it |

The strict end, for every profile: GC until the fsck summary is quiet, `cas-fsck`, drop every table on both
servers (from `system.tables`), restart each server, then GC until the pool is empty. Empty means no blob
body on the store (`reachable + unreachable` of `SYSTEM CAS FSCK`) and `intents_listed == 0`. The drain gives
up once the fsck summary has not changed for 18 committed GC rounds and 300 s, or at `--drain-cap-s`; either
ends the run as FAIL, with the leftover classified in `run.log` and `failures.jsonl`.

## Run directory {#run-directory}

| File | Content |
|---|---|
| `run.log` | timeline of every component, timestamped; ends with the `RESULT=` line |
| `run.done` | written last: `RESULT=PASS|FAIL exit=N profile=... kills=... freezes=... pauses=... failures=N` |
| `env.json` | binary, version, seeds, options, `utc_offset` (run.log is local time, server logs are UTC; the offset is also the first line of run.log) |
| `ref_catalog.tsv`, `ref_catalog.raw` | every row of the pool's namespace catalog (namespace, state, life), read before teardown |
| `srv<N>/system_cas_log.tsv`, `srv<N>/system_cas_gc_log.tsv` | the newest 2,000,000 rows of the last 2 hours, with column names, dumped before teardown |
| `KEPT_FOR_ANALYSIS` | present when the drain left the pool non-empty: the stand was NOT removed; the file holds the command that removes it |
| `metrics.db` | sqlite: `metrics` and `gc_phases` (main), `intents_samples` (one online fsck per server per minute), `faults`, `intents_counters` |
| `faults.jsonl`, `faults_summary.txt` | faults with recovery; GC rounds by outcome per server, leadership changes, rounds suppressed with `parent_unconfirmed`, pending rows over time, rounds around each freeze, the intents curve |
| `intents_curve.jsonl` | per minute and server: `intents_listed`, `dangling`, pending and condemned GC rows, age of the last fold |
| `overlap.txt` | evidence that writers met blobs inside a GC step: intent and reuse `ProfileEvents`, `cas_log` counts, adoptions by the blob's last lifecycle event, workload counters |
| `log_scan.txt` | every log-scan hit with context and its class |
| `failures.jsonl`, `failure.json` | intents failures, one JSON line each; the main checkpoint failure dump |
| `fsck_offline_{settled,drained}.txt`, `fsck_online_*.json` | fsck results of the strict end |
| `srv1/`, `srv2/` | server logs and container output |
| `compose.override.yml`, `state/` | the stand's override; workload state |

## Resources {#resources}

Measured on a 20-minute `combined` run on the local RustFS stand: each server's `/var/lib/clickhouse` grew to
about 0.5 GB, the store peaked at 0.35 GB, and the server logs copied into the run directory were 0.4 GB. The
logs rotate at 3 GB per server and the janitor keeps `cas_log` to 40 minutes; the main workload's pool is
bounded by its TTL and by `--max-pool-gb` (default 40). `run.log` prints a `disk:` line every 5 minutes.

## Unit tests {#tests}

```bash
python3 -m pytest tests -q
```
