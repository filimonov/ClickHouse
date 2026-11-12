---
description: 'Before/after measurements for the CAS late-request audit bundle on antalya-26.8: table churn, the no-churn control, the two relink integration modules, and the verdict on each prediction of the design.'
sidebar_label: 'CAS: late-request audit bundle measurements'
sidebar_position: 113
slug: /superpowers/reports/cas-pr2-late-request-audit-bundle-measurements
title: 'CAS: late-request audit bundle, measurements before and after'
doc_type: 'reference'
---

# CAS: late-request audit bundle, measurements before and after {#cas-pr2-measurements}

- Date: 2026-11-12.
- Before: `be74be9fa7b` (branch `fix/antalya-26.8/cas-ref-cleanup-per-life-licence`, called PR1 below).
- After: `fa1b89d1529` (branch `fix/antalya-26.8/cas-late-request-audit-bundle`, called the branch below).
- Design and predictions: `docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md`, section 12.

## Result {#result}

- **No measurable cost on table create and drop, ref cleanup or relink fetch.** On the same host state the branch
  and PR1 agree on churn throughput, `DROP` latency, ref objects deleted and GC round durations.
- **P4 to P7 hold as written.** No slot refusal in 181 relink confirms, no unknown-outcome warning, no new kind of
  failed GC round, module wall time 1.0 % and 4.7 % lower.
- **P1, P2 and the `_log` half of P3 miss their bands against R2, the baseline the design names.** The cause is the
  baseline, not the branch. See [Predictions](#predictions).
  - P1, P2: R2 ran at loadavg1 12; the branch ran on an idle host. Against R2c, the PR1 binary on the idle host,
    both are inside their bands.
  - P3: `_log` at load end under churn is a point sample that depends on whether a long GC round is in flight. It is
    7592, 21144 and 14706 in three runs of the PR1 binary and 25792 on the branch. Outside long rounds the median
    is 6634 (R2c) and 6901 (branch).
- **Known defect, unchanged.** GC rounds abort after about 80 s in the ref catalog erase under churn: 1 to 3 rounds
  per run on both binaries. Tracked as CAS-463.

## Setup {#setup}

- One standalone server at a time on port 19781, fresh state directory, CAS disk on local object storage,
  `cas_gc_interval_sec` 20, `cas_gc_log` on. Both binaries come from the same build directory and flags.
- Workload, 15 min, then 2 min idle:
  - Steady part: four `MergeTree` tables, one `INSERT` of 1,000 rows into each, every 0.5 s.
  - Churn part: two loops of `CREATE TABLE`, one `INSERT` of 100 rows, `DROP TABLE ... SYNC`.
- Host: 32 cores, 91 GiB RAM.

| Run | Binary | Churn | Load window, UTC | Host during the run |
|---|---|---|---|---|
| R2 | PR1 | yes | 2026-11-11 18:24:40 - 18:39:47 | another measurement session, loadavg1 12.05 |
| R4 | PR1 | no | 2026-11-11 18:41:55 - 18:56:58 | another measurement session, loadavg1 6.29 |
| R2b | PR1 | yes | 2026-11-11 19:16:45 - 19:31:52 | another measurement session, loadavg1 6.22 |
| S2 | branch | yes | 2026-11-12 03:44:12 - 03:59:20 | idle, loadavg1 3.49 |
| S4 | branch | no | 2026-11-12 04:01:27 - 04:16:29 | idle, loadavg1 1.13 |
| R2c | PR1 | yes | 2026-11-12 04:18:41 - 04:33:47 | idle, loadavg1 3.38 |
| R4c | PR1 | no | 2026-11-12 04:35:58 - 04:51:00 | idle, loadavg1 0.97 |

R2, R4 and R2b are the "after" runs of the PR1 report
(`docs/superpowers/reports/2026-11-11-cas-pr1-ref-cleanup-bundle-measurements.md`). R2c and R4c repeat them on the
idle host, back to back with S2 and S4. S2 against R2c and S4 against R4c are the like-for-like pairs. Times in the
sections below are server local time, UTC+1.

## Churn runs {#churn}

| Metric | R2 (PR1) | R2b (PR1) | R2c (PR1) | S2 (branch) |
|---|---:|---:|---:|---:|
| Ref objects deleted during load | 23968 | 22936 | 23968 | 21904 |
| Ref objects deleted, final | 25258 | 25258 | 25258 | 25258 |
| `_log` objects at load end | 7592 | 21144 | 14706 | 25792 |
| `_log` objects, peak | 30621 | 33811 | 19258 | 34192 |
| `_log` objects, final | 2502 | 2741 | 3053 | 3078 |
| Largest `_log` count per life at load end | 343 | 674 | 493 | 1010 |
| Largest `_log` count per life, peak | 887 | 1687 | 551 | 1304 |
| Lives with `_log` objects at load end | 880 | 3336 | 2194 | 3997 |
| Churn tables created and dropped | 18938 | 20871 | 24099 | 22596 |
| Batch flushes at load end | 121855 | 131764 | 148204 | 140704 |
| loadavg1 during load, mean | 12.05 | 6.22 | 3.38 | 3.49 |
| loadavg1 during load, max | 15.02 | 8.01 | 4.46 | 4.37 |
| Refusal lines in the server log | 0 | 0 | 0 | 0 |
| `CASRefCleanupChunksUnderCatalogChange` | 77 | 64 | 85 | 73 |
| `CASRefCleanupNamespacesSkipped` | 0 | 0 | 0 | 0 |
| `CASRefCleanupStoppedLeaseMoved` | 0 | 0 | 0 | 0 |
| `CASRefCleanupStoppedAuthorityUnverified` | 0 | 0 | 0 | 0 |
| GC rounds | 37 | 30 | 40 | 32 |
| GC round duration p50, ms | 2116 | 2157 | 2207 | 2301 |
| GC round duration p95, ms | 18390 | 81852 | 10958 | 80342 |
| GC round duration max, ms | 86179 | 84468 | 80956 | 83272 |
| GC rounds that failed | 2 | 3 | 1 | 3 |
| `ref_object_cleanup` phase rows | 35 | 27 | 39 | 29 |
| `ref_object_cleanup` rounds that deleted | 21 | 16 | 24 | 17 |
| `ref_object_cleanup` mean, ms | 69.9 | 76.0 | 51.4 | 68.8 |
| `ref_object_cleanup` median of deleting rounds, ms | 103.6 | 80.0 | 79.3 | 84.0 |
| `ref_object_cleanup` total, ms | 2445 | 2052 | 2005 | 1996 |
| `CREATE` queries | 18943 | 20876 | 24104 | 22601 |
| `CREATE` p50/p90/p99, ms | 16/27/71 | 16/26/58 | 14/19/42 | 15/25/37 |
| `CREATE` max, ms | 5389 | 11163 | 4560 | 2235 |
| `DROP` queries | 18938 | 20871 | 24099 | 22596 |
| `DROP` p50/p90/p99, ms | 6/14/42 | 6/15/31 | 5/10/30 | 5/16/26 |
| `DROP` max, ms | 5241 | 11009 | 715 | 11260 |
| `INSERT` queries | 25266 | 27283 | 30592 | 29092 |
| `INSERT` p50/p90/p99, ms | 7/14/47 | 6/11/22 | 5/8/15 | 5/10/18 |
| `INSERT` max, ms | 391 | 1092 | 272 | 375 |

- **Churn throughput follows host load, not the binary.** Tables per run: 18938 at loadavg1 12.05, 20871 at 6.22,
  24099 and 22596 at 3.4-3.5. The branch is 6.2 % below R2c.
- **`DROP` latency.** p50 is 5 ms on both binaries on the idle host; p99 is 26 ms on the branch and 30 ms on PR1.
  The 11 s maximum in S2 is two `DROP` queries at 04:49, during the drain after the second failed GC round. R2b has
  the same stall (11.0 s); R2c has a 4.6 s `CREATE` stall after its failed round.
- **Requests per table life.** Every `system.events` counter with a value of 1,000 or more agrees between S2 and R2c
  within 9 % per churn table, except two queue-wait timers. `CASRefQueueWaitMicroseconds` is 179.9 s in S2 against
  153.8 s, 183.0 s and 189.0 s in the three PR1 runs. `CASHotKeyQueueWaitMicroseconds` is 8.5 s against 10.5 s in R2c.
  S2 has no counter that R2c lacks apart from one `CASBlobCompareSwapConflict`.
- **`_log` at load end.** The count grows by 7,800-9,600 per minute while a long GC round runs and falls back after
  the next good round. The table below splits the 16 load samples of each run. A sample is "inside" from the start of
  a round of 20 s or more until 45 s after its end.

| Run | Rounds of 20 s or more | Seconds in them | Samples outside | Median `_log` outside | Load end inside |
|---|---:|---:|---:|---:|---|
| R2 (PR1) | 2 | 167 | 12 | 5503 | no |
| R2b (PR1) | 5 | 352 | 7 | 6895 | yes |
| R2c (PR1) | 2 | 127 | 12 | 6634 | yes |
| S2 (branch) | 5 | 304 | 8 | 6901 | yes |

  Outside long rounds the branch is 4.0 % above R2c and equal to R2b. The load end of S2 comes 37 s after its third
  failed round. The time spent in long rounds varies from 127 s to 352 s on the PR1 binary alone.

## Control runs without churn {#control}

| Metric | R4 (PR1) | R4c (PR1) | S4 (branch) |
|---|---:|---:|---:|
| Ref objects deleted during load | 25000 | 25000 | 25000 |
| Ref objects deleted, final | 25258 | 25258 | 25258 |
| `_log` objects at load end | 2253 | 2356 | 2352 |
| `_log` objects, peak | 3310 | 3189 | 3192 |
| `_log` objects, final | 2509 | 2601 | 2588 |
| Largest `_log` count per life at load end | 205 | 205 | 205 |
| Largest `_log` count per life, peak | 373 | 385 | 385 |
| Lives with `_log` objects at load end | 15 | 15 | 15 |
| Churn tables created and dropped | 0 | 0 | 0 |
| Batch flushes at load end | 27169 | 27272 | 27268 |
| loadavg1 during load, mean | 6.29 | 0.97 | 1.13 |
| loadavg1 during load, max | 13.73 | 1.93 | 2.26 |
| Refusal lines in the server log | 0 | 0 | 0 |
| `CASRefCleanupChunksUnderCatalogChange` | 0 | 0 | 0 |
| `CASRefCleanupNamespacesSkipped` | 0 | 0 | 0 |
| `CASRefCleanupStoppedLeaseMoved` | 0 | 0 | 0 |
| `CASRefCleanupStoppedAuthorityUnverified` | 0 | 0 | 0 |
| GC rounds | 50 | 50 | 50 |
| GC round duration p50, ms | 276 | 223 | 227 |
| GC round duration p95, ms | 456 | 425 | 448 |
| GC round duration max, ms | 534 | 616 | 519 |
| GC rounds that failed | 0 | 0 | 0 |
| `ref_object_cleanup` phase rows | 50 | 50 | 50 |
| `ref_object_cleanup` rounds that deleted | 26 | 25 | 25 |
| `ref_object_cleanup` mean, ms | 41.6 | 38.6 | 38.0 |
| `ref_object_cleanup` median of deleting rounds, ms | 80.7 | 74.9 | 74.6 |
| `ref_object_cleanup` total, ms | 2082 | 1932 | 1899 |
| `CREATE` queries | 5 | 5 | 5 |
| `CREATE` p50/p90/p99, ms | 9/11/11 | 8/12/14 | 8/14/18 |
| `CREATE` max, ms | 11 | 14 | 18 |
| `INSERT` queries | 6383 | 6412 | 6412 |
| `INSERT` p50/p90/p99, ms | 6/13/29 | 14/15/18 | 14/15/17 |
| `INSERT` max, ms | 165 | 195 | 173 |

- **S4 and R4c agree on every row.** Deleted counts are equal, `_log` at load end is 2352 against 2356, the round
  median 227 ms against 223 ms.
- **`INSERT` p50 is 14 ms on the idle host on both binaries, against 6 ms under ambient load in R4.** The ref write
  queue moves the same way: `CASRefQueueWaitMicroseconds` is 94.1 s over 6412 inserts in R4c and 91.9 s in S4
  (14.7 ms and 14.3 ms per insert) against 57.5 s over 6383 inserts in R4 (9.0 ms). The branch does not change it.
  Why ambient load shortens the queue was not investigated.

## Relink integration modules {#integration}

Both modules ran on both binaries: `pytest` started from `tests/integration` of the branch worktree, with the binary
under test in `CLICKHOUSE_TESTS_SERVER_BIN_PATH`.

| Module | PR1 | Branch | Time, PR1 | Time, branch | Confirm requests | Refused: slot moved or unreadable |
|---|---|---|---:|---:|---:|---:|
| `test_cas_replicated_relink` | 19 passed | 19 passed | 60.4 s | 57.5 s | 21 | 0 |
| `test_cas_gcs_relink_liveness` | 1 passed | 1 passed | 507.1 s | 501.9 s | 160 | 0 |

- Times are `pytest` time minus container setup.
- Confirm requests are `cas_confirm=` lines in the sender logs. In `test_cas_replicated_relink` two confirms answer
  `no` and none answers `unknown`; a slot refusal answers `unknown`.
- `test_cas_gcs_relink_liveness` asserts `CASRelinkConfirmRefusedSlotUnreadable` is 0.
- Harness note: `test_cas_gcs_relink_liveness` fails at setup on both binaries with
  `server.py.tmp: Permission denied` when the runner is not root. The `fakegcs` image has no `WORKDIR` and the
  instance runs as the host uid. The runs above used a `docker` wrapper that adds `-u root` to `exec` on that
  container (`tmp/prbundle/pr2/bin/shim/docker`).

## Predictions {#predictions}

| # | Prediction | Verdict |
|---|---|---|
| P1 | Churn tables created in S2 are within 10 % of R2. | Misses against R2: 22596 against 18938, 19.3 % more. Cause: R2 ran at loadavg1 12.05, S2 at 3.49. Against R2c on the same host state: 6.2 % fewer, inside the band. |
| P2 | `DROP TABLE` p50 and p99 in S2 are within 15 % of R2. | Misses against R2 in the faster direction: p50 5 against 6 ms, p99 26 against 42 ms. Same cause as P1. Against R2c: p50 5 against 5 ms, p99 26 against 30 ms, inside the band. |
| P3 | Ref objects deleted during load and `_log` objects at load end are within 10 % of R2 and R4. | Deleted during load holds: 21904 against 23968 (8.6 % fewer) and 25000 against 25000. `_log` without churn holds: 2352 against 2253 (R4) and 2356 (R4c). `_log` under churn misses: 25792 against 7592 (R2) and 14706 (R2c). Cause: the sample at load end falls inside a long GC round in S2 and R2c and outside in R2. Outside long rounds the median is 6901 against 6634 (R2c). |
| P4 | `CASRelinkConfirmRefusedSlotUnreadable` is 0 everywhere. `CASRelinkConfirmRefusedSlotMoved` is 0 in the standalone runs and reported next to the number of confirms in the modules. | Holds: neither counter appears in `system.events` of S2 or S4. Modules: 0 refusals of 21 and of 160 confirms. |
| P5 | No "outcome of its Live -> Removing catalog write is unknown" warning on local object storage. | Holds: 0 lines in S2 and S4. |
| P6 | S2 and S4 have no `CA GC round failed` line that R2 and R4 lack. | Holds: S2 has 3 lines, all the `Code: 210` catalog erase deadline that R2, R2b and R2c also have. S4 has none. |
| P7 | Wall time of each integration module is within 15 %. | Holds: 4.7 % and 1.0 % lower on the branch. |

P1 and P2 are inside their bands against R2c; the design named R2 before its host load was known to differ. The
`_log` sample under churn has no stable baseline: it spreads from 7592 to 21144 over three runs of the PR1 binary.

## Failed GC rounds under churn {#failed-rounds}

| Run | Binary | Failed rounds | Round end, duration |
|---|---|---:|---|
| R2 | PR1 | 2 of 37 | 19:29:18, 81247 ms; 19:31:33, 86179 ms |
| R2b | PR1 | 3 of 30 | 20:22:47, 80887 ms; 20:27:21, 81852 ms; 20:29:05, 84468 ms |
| R2c | PR1 | 1 of 40 | 05:25:06, 80956 ms |
| S2 | branch | 3 of 32 | 04:46:37, 83272 ms; 04:48:17, 80342 ms; 04:58:43, 81805 ms |

- Every failure is `Code: 210 ... CAS ref catalog erase for namespace '...': gave up at the policy deadline`.
- The mechanism is in the PR1 report, section "Failed GC rounds under churn". The branch does not change the erase
  loop of `completedRemovingDelete`.
- The first two failures of S2 hit the same namespace in consecutive rounds.
- 6 failed rounds of 107 on PR1 and 3 of 32 on the branch. The failures cluster, so the sample cannot tell the rates
  apart.
- Request counters per churn table agree between S2 and R2c ([Churn runs](#churn)): the branch adds no catalog write
  for the erase to lose to.
- Tracked as CAS-463 (GC makes no progress under table churn).

## Limits {#limits}

- One churn run and one control run of the branch. The PR1 binary alone spreads from 1 to 3 failed rounds and from
  127 s to 352 s in long rounds per run.
- The standalone runs do no relink. Relink cost is covered by the two modules only: 181 confirms in total, on local
  RustFS and a GCS emulator. No refusal occurred, so the refusal rate under renewal load is not measured (CAS-461).
- Local object storage: no network latency, no request cost. A remote store widens every race window and makes the
  one extra `GET` per confirmed relink visible.
- One server in the standalone runs. Several mounts are not covered.

## Scripts and raw files {#files}

Standalone runs: master worktree, `tmp/prbundle/measure/`.

| Path | Content |
|---|---|
| `run.sh`, `start.sh` | One run: server start, load, sampling every 60 s, idle tail, dumps |
| `run_pr2.sh`, `run_ctl.sh`, `run_ctl2.sh` | Queues for S2 and S4, R2c, R4c |
| `summarize.py`, `mdtable.py` | The metric tables above |
| `logstats.py` | The `_log` split by long GC rounds |
| `runs/<label>/samples.tsv` | Samples every 60 s |
| `runs/<label>/gc_rounds.tsv`, `gc_phase.tsv` | `system.cas_gc_log` dumps |
| `runs/<label>/events_final.tsv` | `system.events` at the end |
| `runs/<label>/drop_latency.tsv` | `CREATE`, `DROP`, `INSERT` latency from `system.query_log` |
| `runs/<label>/slow_queries.tsv` | Queries of 1 s or more (S2, R2c), dumped with `postq.sh` |

Run labels: `R2_after_churn`, `R2b_after_churn`, `R2c_pr1_churn`, `S2_pr2_churn`, `R4_after_nochurn`,
`R4c_pr1_nochurn`, `S4_pr2_nochurn`. Binaries: `tmp/prbundle/bin/clickhouse-pr1-be74be9fa7b`,
`tmp/prbundle/bin/clickhouse-pr2-fa1b89d1529`.

Integration modules: `tmp/prbundle/pr2/bin/integ.sh`, `integ_gcs.sh`; logs in `tmp/prbundle/pr2/logs/`.
