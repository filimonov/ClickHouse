---
description: 'Measurements of the CAS wave 1 head against its base on a standalone server: throughput, GC round failures, round log lines, reclaim backlog and the GC success age.'
sidebar_label: 'CAS wave 1 measurements'
sidebar_position: 1
slug: /development/cas/cas-improvements-wave1-measurements
title: 'CAS wave 1 measurements'
doc_type: report
---

# CAS wave 1 measurements {#cas-wave1-measurements}

Wave 1 head `def742b13c0` against base `fa1b89d1529` on a standalone server. Four of five predictions held. M-4 failed on base and head alike.

## Predictions {#predictions}

Written before the first run.

| # | Prediction |
|---|---|
| M-1 | Insert and merge throughput of the churn workload stays within 10 % of the base |
| M-2 | The server log has no new `CA GC round failed` lines |
| M-3 | The server log has exactly one `Information` round line per `Finish` row of `system.cas_gc_log` with outcome `Success`, `Aborted`, `Failed` or `Stopped`, and none for the other rows |
| M-4 | The reclaim backlog (`pending_reclaim`) is 0 at the end of the idle phase, on base and on head |
| M-5 | `system.cas_mounts.last_success_age_seconds` is `NULL` before the first round and a number after it |

M-5 holds on the head only. The base reports a number before the first success.
On the base, M-3 is "0 lines, by design".
The enum value for a failed round is `Error` in `system.cas_gc_log` and in the log line. M-3 counts `Error` for "Failed".

## Setup {#setup}

| Item | Value |
|---|---|
| Base binary | `tmp/prbundle/bin/clickhouse-pr2-fa1b89d1529` |
| Head binary | `tmp/prbundle/bin/clickhouse-w1-def742b13c0` |
| Server and workload | `tmp/prbundle/measure/run.sh` and `start.sh`: one server, `MergeTree` on a local-object-storage CAS disk, `cas_gc_interval_sec` 20 |
| Load phase | 900 s: four steady insert loops (1000 rows, 0.5 s sleep) and, in the churn runs, two `CREATE`/`INSERT`/`DROP TABLE ... SYNC` loops |
| Idle phase | 120 s, sampled every 30 s |
| Runs | one per label, sequential, under `flock tmp/prbundle/build.lock` |

Run directories, under `tmp/prbundle/measure/runs/`: `W1_base_churn`, `W1_head_churn`, `W1_base_nochurn`, `W1_head_nochurn`.
Each holds `w1_predictions.tsv` (collected by `w1_collect.sh`), `w1_samples.tsv`, `w1_age.tsv`, `events_w1.tsv`, `w1_finish.tsv` and `state/server.log`.

### Default differences {#default-differences}

Both binaries ran with their own defaults. Nothing was pinned.

| Setting | Base | Head |
|---|---|---|
| Mount lease | 30 s | 120 s |
| Request window | fixed 90 s | 120 s |
| Five GC round budgets | 5000 | 0 (unbounded) |
| List page size | base default | 999 keys |
| `last_success_age_seconds` before the first success | number | `NULL` |
| `Information` line per finished round | none | `GC round finished: round=... outcome=...` |

Each run is a single sample. Differences of a few percent are within run-to-run noise.

## M-1 throughput {#m1-throughput}

Rates are per second of the load phase, from `system.events` at the end of the load phase (`events_w1_loadend.tsv`).

| Workload | Metric | Base | Head | Head vs base |
|---|---|---|---|---|
| churn | `InsertedRows`/s | 16475.6 | 17628.9 | +7.0 % |
| churn | `MergedRows`/s | 1135026 | 1153066 | +1.6 % |
| churn | `InsertQuery` total | 28112 | 31397 | +11.7 % |
| churn | churn tables created and dropped | 21639 | 24881 | +15.0 % |
| no churn | `InsertedRows`/s | 9089.1 | 9085.9 | -0.04 % |
| no churn | `MergedRows`/s | 1112451 | 1110910 | -0.14 % |

Verdict: held. The head is not slower in any row. The churn gain is inside the noise of a single run, and the workload throttles itself on query latency, so it is not claimed as a speed-up.

## M-2 round failures {#m2-round-failures}

| Run | `CA GC round failed` lines | `Aborted` Finish rows |
|---|---|---|
| `W1_base_churn` | 4 | 4 |
| `W1_head_churn` | 0 | 0 |
| `W1_base_nochurn` | 0 | 0 |
| `W1_head_nochurn` | 0 | 0 |

Verdict: held. The head has no such line. The base logged four in the churn run, for example:

```text
CA GC round failed (will retry next tick): Code: 210. DB::Exception: CAS write could not be committed (CAS ref catalog erase for namespace 'prbundle-measure/store/003/003f06c9-158c-4c7b-818c-dfbe7e4a9016@cas@': gave up at the policy deadline after zero attempt(s)); retrying later. (NETWORK_ERROR)
```

The lines are in `runs/W1_base_churn/state/server.log` (07:19:02, 07:23:15, 07:24:56, 07:29:08 local server time). Whether the larger head window or the removed budgets avoid them was not isolated.

## M-3 round log lines {#m3-round-log-lines}

The log line names a failed round `Error`, as does the `outcome` enum of `system.cas_gc_log`. M-3 counts `Error` for "Failed".
No row had outcome `Error`, `Stopped`, `Unknown`, `NotALeader` or `Deferred` in any run.

| Run | `Finish` rows `Success` | Round lines `Success` | Other rows | Other lines |
|---|---|---|---|---|
| `W1_base_churn` | 24 | 0 | 4 `Aborted` | 0 |
| `W1_base_nochurn` | 50 | 0 | 0 | 0 |
| `W1_head_churn` | 46 at the snapshot, 47 after shutdown | 47 | 0 | 0 |
| `W1_head_nochurn` | 50 | 50 | 0 | 0 |

The base has no such line, by design.

Head churn shows 47 lines against 46 rows. Round 47 explains the gap:

- `run.sh` dumped the `Finish` rows (`w1_finish.tsv`) after `SYSTEM FLUSH LOGS`. The dump ends at round 46.
- The log has `GC round finished: round=47 outcome=Success ...` at 07:58:42.546981 and `Received signal 15` at 07:58:42.598657, 52 ms later (`server.log.run1`, lines 1972880 and 1972882).
- The round finished after the dump and before the stop, so the dump could not contain it.
- A restart on the saved state (`postq.sh`, `w1_postq_finish.tsv`) shows the row: `47 Success 2026-11-13 07:58:42`. The shutdown flush wrote it.

Verdict: held. Every `Success` round has exactly one line, and the lines carry the same round numbers as the rows.

## M-4 reclaim backlog {#m4-reclaim-backlog}

`pending_reclaim` of `system.cas_mounts`, last idle sample and maximum over the run:

| Run | Last idle sample | Maximum |
|---|---|---|
| `W1_base_churn` | 152 | 2803 |
| `W1_head_churn` | 112 | 2803 |
| `W1_base_nochurn` | 86 | 1530 |
| `W1_head_nochurn` | 144 | 1524 |

Verdict: failed on base and head. No run ended at 0.

Cause, from `system.cas_gc_log` `Finish` rows with outcome `Success` (`w1_rounds.tsv`, no-churn runs):

| Run | Rounds | Sum `entries_condemned` | Sum `entries_redeleted` | Difference | Last `pending_reclaim` |
|---|---|---|---|---|---|
| `W1_base_nochurn` | 50 | 2217 | 2131 | 86 | 86 |
| `W1_head_nochurn` | 50 | 2235 | 2091 | 144 | 144 |

- `pending_reclaim` is the per-process running sum of `condemned - redeleted` (`CasGcScheduler.cpp`, `pending_reclaim.fetch_add`). It equals the table difference exactly.
- A condemned entry is redeleted two rounds later. Round 48 condemned 1444 (base) and 1380 (head); round 50 redeleted 1444 and 1380.
- Rounds 47 to 49 still condemn new entries during the idle phase, so the last rounds always leave a tail.
- This is the measured pattern. It was not checked which objects the idle-phase condemnations are (the merge old-part lifetime is the suspect).

What would confirm it: extend the idle phase until two consecutive rounds show `entries_condemned = 0`, then read `pending_reclaim`. The prediction as written (0 after 120 s) is wrong for this workload. Base and head behave the same, so this is not a head regression.

## M-5 success age {#m5-success-age}

`last_success_age_seconds` of `system.cas_mounts` (`w1_age.tsv`):

| Run | Right after start | Before stop |
|---|---|---|
| `W1_base_churn` | 0 | 19 |
| `W1_head_churn` | `NULL` | 19 |
| `W1_base_nochurn` | 0 | 13 |
| `W1_head_nochurn` | `NULL` | 13 |

Verdict: held on the head (`NULL`, then a number). The base reports 0 before any success, as stated in the predictions.

## Verdicts {#verdicts}

| # | Verdict | Key numbers |
|---|---|---|
| M-1 | held | churn +7.0 % inserted rows/s, +1.6 % merged rows/s; no churn within 0.2 % |
| M-2 | held | head 0 lines; base churn 4 |
| M-3 | held | 50/50 and 47/47 after shutdown flush; the extra line is round 47, finished 52 ms before the stop |
| M-4 | failed | backlog 86 to 152 at the end of idle, both binaries; cause is a two-round redelete lag with condemnations continuing |
| M-5 | held | head `NULL` then 13 to 19; base 0 then 13 to 19 |
