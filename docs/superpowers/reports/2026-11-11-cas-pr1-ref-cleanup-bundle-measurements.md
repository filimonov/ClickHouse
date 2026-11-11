---
description: 'Before/after measurements for the CAS ref cleanup bundle on antalya-26.8: ref cleanup under table churn, the no-churn control, and the failed GC rounds seen in the churn runs.'
sidebar_label: 'CAS: ref cleanup bundle measurements'
sidebar_position: 112
slug: /superpowers/reports/cas-pr1-ref-cleanup-bundle-measurements
title: 'CAS: ref cleanup bundle, measurements before and after'
doc_type: 'reference'
---

# CAS: ref cleanup bundle, measurements before and after {#cas-pr1-measurements}

- Date: 2026-11-11.
- Before: `2f89c5db182` (head of pull request 2805, branch `port/antalya-26.8/cas-from-26.6`).
- After: `be74be9fa7b` (branch `fix/antalya-26.8/cas-ref-cleanup-per-life-licence`).
- Design and predictions: `docs/superpowers/specs/2026-11-11-cas-pr1-ref-cleanup-bundle-design.md`, section 10.

## Result {#result}

- **Ref cleanup now deletes under table churn.** During 15 min of churn the base deletes 0 ref objects; the branch
  deletes 23968 and 22936. The largest `_log` count per namespace life at load end drops from 5639 and 5877 to 343
  and 674.
- **No cost without churn.** Deleted counts, round durations and the `ref_object_cleanup` phase agree between the
  two builds (R3, R4).
- **Every prediction of the design holds** in every run.
- **One finding, not caused by the branch.** GC rounds abort after about 80 s in the ref catalog erase while tables
  are created and dropped. It happens on both builds and is tracked as CAS-463. See [Failed GC rounds](#failed-rounds).

## Setup {#setup}

- One standalone server at a time on port 19781, fresh state directory, CAS disk on local object storage,
  `cas_gc_interval_sec` 20, `cas_gc_log` on. Both binaries come from the same build directory and flags.
- Workload, 15 min, then 2 min idle:
  - Steady part: four `MergeTree` tables, one `INSERT` of 1,000 rows into each, every 0.5 s.
  - Churn part: two loops of `CREATE TABLE`, one `INSERT` of 100 rows, `DROP TABLE ... SYNC`.
- Host: 32 cores, 91 GiB RAM. Another session ran its own measurements on the host during all runs.

| Run | Binary | Churn | Load window, UTC |
|---|---|---|---|
| R1 | before | yes | 12:31:08 - 12:46:16 |
| R3 | before | no | 12:48:25 - 13:03:27 |
| R2 | after | yes | 18:24:40 - 18:39:47 |
| R4 | after | no | 18:41:55 - 18:56:58 |
| R1b | before | yes | 18:59:08 - 19:14:17 |
| R2b | after | yes | 19:16:45 - 19:31:52 |

R1b and R2b are a back-to-back pair. R1 and R2 ran six hours apart under different ambient load (loadavg1 8.9 and
12.0), so the pair is the like-for-like comparison. Times in the sections below are server local time, UTC+1.

## Churn runs {#churn}

| Metric | R1 (before) | R1b (before) | R2 (after) | R2b (after) |
|---|---:|---:|---:|---:|
| Ref objects deleted during load | 0 | 0 | 23968 | 22936 |
| Ref objects deleted, final | 20000 | 20000 | 25258 | 25258 |
| `_log` objects at load end | 54009 | 48475 | 7592 | 21144 |
| `_log` objects, peak | 54009 | 68943 | 30621 | 33811 |
| `_log` objects, final | 33648 | 24860 | 2502 | 2741 |
| Largest `_log` count per life at load end | 5639 | 5877 | 343 | 674 |
| Largest `_log` count per life, peak | 5641 | 5877 | 887 | 1687 |
| Lives with `_log` objects at load end | 5473 | 4161 | 880 | 3336 |
| Churn tables created and dropped | 20687 | 23220 | 18938 | 20871 |
| Batch flushes at load end | 130155 | 143844 | 121855 | 131764 |
| loadavg1 during load, mean | 8.93 | 4.6 | 12.05 | 6.22 |
| loadavg1 during load, max | 21.75 | 6.14 | 15.02 | 8.01 |
| Refusal lines in the server log | 27 | 33 | 0 | 0 |
| `CASRefCleanupChunksUnderCatalogChange` | n/a | n/a | 77 | 64 |
| `CASRefCleanupNamespacesSkipped` | n/a | n/a | 0 | 0 |
| `CASRefCleanupStoppedLeaseMoved` | n/a | n/a | 0 | 0 |
| `CASRefCleanupStoppedAuthorityUnverified` | n/a | n/a | 0 | 0 |
| GC rounds | 34 | 39 | 37 | 30 |
| GC round duration p50, ms | 8321 | 4556 | 2116 | 2157 |
| GC round duration p95, ms | 18337 | 14584 | 18390 | 81852 |
| GC round duration max, ms | 81147 | 37950 | 86179 | 84468 |
| GC rounds that failed | 1 | 0 | 2 | 3 |
| `ref_object_cleanup` phase rows | 32 | 38 | 35 | 27 |
| `ref_object_cleanup` rounds that deleted | 4 | 4 | 21 | 16 |
| `ref_object_cleanup` mean, ms | 48.4 | 39.7 | 69.9 | 76.0 |
| `ref_object_cleanup` median of deleting rounds, ms | 375.6 | 367.8 | 103.6 | 80.0 |
| `ref_object_cleanup` total, ms | 1548 | 1510 | 2445 | 2052 |
| `CREATE` queries | n/a | 23225 | 18943 | 20876 |
| `CREATE` p50/p90/p99, ms | n/a | 14/19/73 | 16/27/71 | 16/26/58 |
| `CREATE` max, ms | n/a | 1078 | 5389 | 11163 |
| `DROP` queries | n/a | 23220 | 18938 | 20871 |
| `DROP` p50/p90/p99, ms | n/a | 5/7/28 | 6/14/42 | 6/15/31 |
| `DROP` max, ms | n/a | 1043 | 5241 | 11009 |
| `INSERT` queries | n/a | 29699 | 25266 | 27283 |
| `INSERT` p50/p90/p99, ms | n/a | 5/7/33 | 7/14/47 | 6/11/22 |
| `INSERT` max, ms | n/a | 299 | 391 | 1092 |

`n/a`: the counter does not exist at the base, or the query latency dump was added after R1.

- **`_log` backlog.** At the base, cleanup deletes nothing until the churn stops: every chunk is refused because the
  catalog changed (27 and 33 refusal lines). On the branch the same condition is counted
  (`CASRefCleanupChunksUnderCatalogChange` 77 and 64) and the chunk proceeds.
- **`ref_object_cleanup` phase.** The phase is bimodal: a round either deletes (about 80-380 ms) or does nothing
  (under 1 ms). The branch deletes in 21 and 16 rounds instead of 4, in smaller portions. Total phase time for the run
  is 2.4 s and 2.1 s against 1.5 s.
- **GC round duration.** The median round is 2.1-2.2 s on the branch against 4.6 s and 8.3 s at the base. At the base
  the `defer_decision` phase grows through the run with the number of listed ref keys (R1b: 55 ms at 3374 keys,
  3.5 s at 30027 keys, 10.7 s at 51823 keys; 160.6 s in total). On the branch the listing stays at 7000-9000 keys and
  the phase at 190-480 ms outside the failed rounds (21.1 s in total).
- **Churn throughput and query latency.** The branch runs created 8.5 % (R2 vs R1) and 10.1 % (R2b vs R1b) fewer
  tables, and show `CREATE`/`DROP` stalls of 5 s and 11 s. Both follow the failed GC rounds described below.

## Control runs without churn {#control}

| Metric | R3 (before) | R4 (after) |
|---|---:|---:|
| Ref objects deleted during load | 25000 | 25000 |
| Ref objects deleted, final | 25258 | 25258 |
| `_log` objects at load end | 2064 | 2253 |
| `_log` objects, peak | 3459 | 3310 |
| `_log` objects, final | 2318 | 2509 |
| Largest `_log` count per life at load end | 202 | 205 |
| Largest `_log` count per life, peak | 385 | 373 |
| Lives with `_log` objects at load end | 16 | 15 |
| Churn tables created and dropped | 0 | 0 |
| Batch flushes at load end | 26980 | 27169 |
| loadavg1 during load, mean | 5.09 | 6.29 |
| loadavg1 during load, max | 11.62 | 13.73 |
| Refusal lines in the server log | 0 | 0 |
| `CASRefCleanupChunksUnderCatalogChange` | n/a | 0 |
| `CASRefCleanupNamespacesSkipped` | n/a | 0 |
| `CASRefCleanupStoppedLeaseMoved` | n/a | 0 |
| `CASRefCleanupStoppedAuthorityUnverified` | n/a | 0 |
| GC rounds | 50 | 50 |
| GC round duration p50, ms | 248 | 276 |
| GC round duration p95, ms | 559 | 456 |
| GC round duration max, ms | 2069 | 534 |
| GC rounds that failed | 0 | 0 |
| `ref_object_cleanup` phase rows | 50 | 50 |
| `ref_object_cleanup` rounds that deleted | 25 | 26 |
| `ref_object_cleanup` mean, ms | 41.0 | 41.6 |
| `ref_object_cleanup` median of deleting rounds, ms | 78.6 | 80.7 |
| `ref_object_cleanup` total, ms | 2048 | 2082 |
| `CREATE` queries | n/a | 5 |
| `CREATE` p50/p90/p99, ms | n/a | 9/11/11 |
| `CREATE` max, ms | n/a | 11 |
| `INSERT` queries | n/a | 6383 |
| `INSERT` p50/p90/p99, ms | n/a | 6/13/29 |
| `INSERT` max, ms | n/a | 165 |

The 2069 ms maximum in R3 is one round; no round failed in either run.

## Predictions {#predictions}

| Run | Prediction | Verdict |
|---|---|---|
| R1, R1b | Deleted count stays near 0 during churn and rises only in the idle tail. The largest per-life `_log` count grows for the whole 15 min. | Holds: 0 during load, 20000 after the idle tail; per-life maximum 5639 and 5877 at load end. |
| R2, R2b | Deleted count rises during churn. `CASRefCleanupChunksUnderCatalogChange` > 0. `CASRefCleanupStoppedAuthorityUnverified` = 0. Largest per-life `_log` count at 15 min below the base. | Holds: 23968 and 22936; 77 and 64; 0; 343 and 674. |
| R3, R4 | Both delete. Deleted counts and round durations agree within noise. `CASRefCleanupChunksUnderCatalogChange` = 0 in R4. | Holds: 25000 and 25000; round p50 248 and 276 ms; 0. |

## Failed GC rounds under churn {#failed-rounds}

**What fails.** A GC round aborts in the `pre_fold_ref_drain` phase after 80-86 s with
`Code: 210 ... CAS ref catalog erase for namespace '...': gave up at the policy deadline after zero attempt(s)`.

| Run | Binary | Failed rounds | Round end, duration |
|---|---|---:|---|
| R1 | before | 1 of 34 | 13:35:43, 81147 ms |
| R1b | before | 0 of 39 | |
| R2 | after | 2 of 37 | 19:29:18, 81247 ms; 19:31:33, 86179 ms |
| R2b | after | 3 of 30 | 20:22:47, 80887 ms; 20:27:21, 81852 ms; 20:29:05, 84468 ms |

**Cause.** The erase loop of `completedRemovingDelete` (`Pool/CasRefCatalog.cpp:495-556` at `2f89c5db182`; the
branch does not change it):

- It sends a conditional write of the catalog with the ETag of its last read, then re-reads the catalog.
- On a conflict it sleeps `Retry::backoff(attempt + 1)`: full jitter over 200 ms, 400 ms, ... capped at 5 s.
- The next iteration sends the write with the ETag read before the sleep. Churn writes the catalog about 100 times
  per second, so the write conflicts again and the sleep grows.
- The retry policy is frozen once at 90 s. When it is used up, the next write is refused before it is sent.

A normal drain erases 530-640 rows per round at about 0.7 ms each and rarely loses the first race.

**Effect on queries (R2b).**

- The aborted round leaves its `Removing` rows in the catalog. The next drain erases 2961 rows in 4.4 s (20:23:39)
  and 4937 rows in 11.0 s (20:30:10). `CREATE` and `DROP` stall for 4.6 s and 11 s at the same moments.
- CPU per `CREATE` is 5.7-6.2 ms in the minutes without a failed round, the same as the base (5.8-6.5 ms in R1b before
  its one 38 s round). It rises to 10-26 ms in the minutes of the failed rounds (20:22-20:23, 20:26-20:29), and memory per `CREATE` from
  2.6 MiB to 5-9.5 MiB: every catalog write encodes and decodes the enlarged catalog.
- Whole run: server CPU 475.9 s against 388.9 s in R1b; `CREATE` CPU 207.1 s against 146.7 s.

**Is it more frequent on the branch?** The sample cannot tell.

- 5 failed rounds of 67 on the branch, 1 of 73 at the base; one-sided Fisher p is about 0.086.
- The failures cluster: one abort enlarges the catalog and the next drain. Independent clusters: 3 on the branch, 1
  at the base.
- The branch changes neither the erase loop nor the number of catalog writes per table life.
- The same abort is recorded at the base without the branch under heavier churn: CAS-463, 12 aborted rounds in a
  row with two servers and 16 threads each.

**Where it goes.** CAS-463 (GC makes no progress under table churn) and CAS-321.3 (route the drain erase through
the hot-key lane). The mechanism above and a candidate minimal fix (re-read the catalog after the sleep) are in the
notes of CAS-463. The branch does not fix it.

## Limits {#limits}

- Two churn runs per build and one control run per build. Run-to-run spread is visible: `_log` objects at load end
  7592 and 21144 on the branch.
- Ambient load from another measurement session varied between runs (loadavg1 mean 4.6 to 12.1).
- Local object storage: no network latency, no request cost. A remote store widens every race window.
- One server. Replicated tables and several mounts are not covered.

## Scripts and raw files {#files}

All paths are in the master worktree under `tmp/prbundle/measure/`.

| Path | Content |
|---|---|
| `run.sh`, `start.sh` | One run: server start, load, sampling every 60 s, idle tail, dumps |
| `summarize.py`, `mdtable.py` | The tables above |
| `postcpu.sh`, `postq.sh` | CPU and `system.query_log` dumps from the kept state |
| `runs/<label>/samples.tsv` | Samples every 60 s |
| `runs/<label>/gc_rounds.tsv`, `gc_phase.tsv`, `gc_all_phases.tsv` | `system.cas_gc_log` dumps |
| `runs/<label>/events_final.tsv` | `system.events` at the end |
| `runs/<label>/cpu.tsv`, `cpu_by_minute.tsv`, `query_by_minute.tsv`, `slow_queries.tsv` | CPU and query latency (R1b, R2b) |

Run labels: `R1_before_churn`, `R1b_before_churn`, `R2_after_churn`, `R2b_after_churn`, `R3_before_nochurn`,
`R4_after_nochurn`. Binaries: `tmp/prbundle/bin/clickhouse-baseline-2f89c5db182`,
`tmp/prbundle/bin/clickhouse-pr1-be74be9fa7b`.
