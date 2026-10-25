---
description: 'CAS GC rounds and server starts against the number of tables on one pool: hold-outs at 1, 4 and 16 tables, the per-table fit, and its N = 8 hold-out.'
sidebar_label: 'Storage cost CAS tables'
sidebar_position: 57
slug: /superpowers/reports/storage-cost-cas-tables
title: 'Storage cost calculator — CAS cost by table count'
doc_type: 'reference'
---

# Storage cost calculator — CAS cost by table count {#cas-tables}

A GC fold round and a server start each cost a fixed amount per table on the CAS disk. The `gc` and `recovery`
cells of `docs/superpowers/reports/2026-10-24-storage-cost-cas/formulas.json` price neither. Per table, a fold reads
**12 GETs** beyond the table's tail records. A start costs **GET 14 (3 of them 404), PUT 2 and LIST 5**. Deferred
rounds and the cleanup listing do not depend on the table count. Fitted at 1, 4 and 16 tables with residual 0;
the fit held exactly at 8 tables (§[Hold-out](#holdout)).

- **Run:** `cost-20261025T060213Z`, rig commit `a2004c7dd62`, `rig_dirty` false, RustFS 1.0.0-beta.9 (sha256
  `bf4aca66af21…`), one pass per case, `cas` only.
- **Binary:** `tmp/bin/26.8/clickhouse`, sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668`,
  source `09d0772a842c5d73cf3ef35b4c8b017d838215a0`.
- **Cards:** `GcTables` in `utils/cas-yardstick/yardstick/cost/cards/cas.py`; `Startup` in `cards/ops.py`.
- **Coefficients:** `coefficients.json` (`basis: fitted`, `cas_tables` 1–16).

## Design {#design}

- **`gc-tables.n<N>`:** N tables on one pool, one Compact part (5 columns, 20 000 rows, distinct content per table)
  per table per minute for 12 minutes. Under `SYSTEM CAS GC STOP` the card runs 6 synchronous rounds a minute, 10 s
  apart, the first after the inserts. Inserts and rounds never overlap. A cleanup listing that lands in a round is
  set apart. 12 minutes hold 2 cleanup periods (305 s).
- **`startup.cas-tables-<N>`:** N tables of 2 Compact parts each; stop, start and `count()` per table in the window.

## First hold-outs {#first-holdouts}

Predicted from the formulas of `049fbdc4a6c`, committed before the run. One uncited smoke pass of a 2-table,
2-minute variant ran before it (`cost-20261025T060047Z`). Every hold-out above one table failed.

| Case | N | Predicted | Measured | Deviation |
|---|---|---|---|---|
| `gc-tables`, 12 fold rounds, GET | 1 | 462 | 616 | +154 (+33 %) |
| | 4 | 1 200 | 1 762 | +562 (+47 %) |
| | 16 | 4 152 | 6 346 | +2 194 (+53 %) |
| `gc-tables`, 12 fold rounds, PUT / LIST / DELETE | 1, 4, 16 | 72 / 48 / 0 | 72 / 57 / 18 | LIST +9, DELETE +18 |
| `gc-tables`, 60 Deferred rounds, GET / PUT / LIST | 1, 4, 16 | 780 / 120 / 180 | 780 / 120 / 180 | 0 |
| `gc-tables`, cleanup listings per table | 1, 4, 16 | 1–12 | 2 | inside |
| `gc-tables`, foreground (inserts and listings) | 1, 4, 16 | PUT 9, HEAD 1, GET 7 per insert + 1 per table | as predicted | 0 |
| `startup.cas-tables-1`, GET / PUT / LIST | 1 | 40 / 11–12 / 7 | 40 / 11 / 7 | 0 (ok) |
| `startup.cas-tables-4`, GET / PUT / LIST | 4 | 64 / 11–12 / 7 | 106 / 17 / 22 | +42 / +5 / +15 |
| `startup.cas-tables-16`, GET / PUT / LIST | 16 | 160 / 11–12 / 7 | 370 / 41 / 82 | +210 / +29 / +75 |

HEAD 6 and DELETE 3 per start held at every N.

## Fit {#fit}

Residual 0 for every fold after the first of a process and for every start:

| Coefficient | Value |
|---|---|
| `gc_round_requests_steady` | fold GET 19 + 1 per new manifest + 1 per tail log record + **12 per table**, PUT 5, LIST 5, DELETE 2; Deferred GET 12, PUT 1, LIST 3 |
| `cas_startup_requests_base` | answered 2xx: GET 17, PUT 7, HEAD 3, DELETE 2, LIST 2 |
| `cas_startup_error_requests_base` | answered 4xx: GET 1, PUT 2, HEAD 3, DELETE 1 |
| `cas_startup_requests_per_table` | answered 2xx per table: GET 11, PUT 2, LIST 5 |
| `cas_startup_error_requests_per_table` | answered 404 per table: GET 3 |

- **Fold, per table:** 7 `ref_stream` GETs beyond the tail (2xx), 2 `_ckpt` GETs, 3 GETs of the table's mount objects
  under `gc/server-roots/<srid>/store/…/mount` (2 of them 404).
- **Generation pruning:** from the fourth fold of a process GC lists an old `gc/gen/<n>/` and deletes its 2 objects.
  Folds 2–3 have LIST 4 and DELETE 0; the steady state is LIST 5 and DELETE 2.
- **First fold of a process:** GET 13 + per table 1 `_ckpt` and 3 mount objects, plus its new manifests and records.
- **Start:** part loads and log replay add 1 GET per Compact part and 1 GET per log record as before.
- **Fold duration** (folds after the first): 28–52 ms at N 1, 53–115 ms at N 4, 125–339 ms at N 16.
- **Not measured:** every table took inserts each minute; a fold over idle tables may cost less. `cas_cached` starts
  were measured at one table only.

## Hold-out {#holdout}

Run `cost-20261025T070618Z` (start 07:06:18Z), predictions committed in `bd19b0524f1` before it. The run used a
detached worktree at rig commit `a2004c7dd62` (the rig code is unchanged since), so all eight records share one rig
commit, binary, source and store. **Every fitted term held exactly at 8 tables.**

| Case | Predicted from the fit | Measured | Deviation |
|---|---|---|---|
| `gc-tables.n8`, 12 fold rounds, GET / PUT / LIST / DELETE | 3 290 / 72 / 57 / 18 | 3 290 / 72 / 57 / 18 | 0 |
| `gc-tables.n8`, 60 Deferred rounds, GET / PUT / LIST | 780 / 120 / 180 | 780 / 120 / 180 | 0 |
| `gc-tables.n8`, largest per-round residual | 70 (the first fold) | 70 | 0 |
| `startup.cas-tables-8`, GET / PUT / HEAD / DELETE / LIST | 194 / 25–26 / 6 / 3 / 42 | 194 / 25 / 6 / 3 / 42 | 0 (ok) |
| `gc-tables.n8`, foreground GET | 680–704 | 760 | +56 |

The `gc-tables.n8` record is `FAILED` on its foreground GET bound only, an insert-side term outside the fit. Every
`gc-tables` pass has 10 foreground GETs per table in 12 minutes beyond the insert graph (`ref_catalog`, `_ckpt` and
manifest reads): 10, 40, 80 and 160 at 1, 4, 8 and 16 tables, 2 cleanup runs per table. That is about 5 GETs per
cleanup run of a table with parts, against 1 on the empty table of `lease.idle-10min`. The `lease` cell prices 1. This
term is measured, not attributed to source, and not fitted.

## How to re-run {#rerun}

```bash
utils/cas-yardstick/rustfs-bin.sh ensure
python3 utils/cas-yardstick/yardstick.py cost run \
  --binary tmp/bin/26.8/clickhouse --manifests docs/superpowers/reports/2026-10-25-storage-cost-cas-tables \
  --cases <experiment ids> --policies cas --runs-dir tmp/cas-yardstick/cost-runs
```
