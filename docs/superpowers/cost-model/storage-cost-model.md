---
description: 'Formulas, inputs and evidence of the storage cost calculator model, rendered from model-contract.json.'
sidebar_label: 'Storage cost model'
sidebar_position: 55
slug: /superpowers/cost-model/storage-cost-model
title: 'Storage cost model'
doc_type: 'reference'
---

# Storage cost model {#storage-cost-model}

The calculator's formulas and inputs, rendered from `model-contract.json`. The page evaluates the same contract, so a
number on the page and a formula here come from one expression.

The `Formulas`, `Inputs` and `Break-even` sections are generated. Edit the reports and the registry, then regenerate:

```bash
python3 utils/storage-cost-calculator/tools/generate_contract.py generate
python3 utils/storage-cost-calculator/tools/generate_contract.py check
```

## Evidence policy {#evidence-policy}

Formulas come from the pinned source; measurements verify them. Rulings D14–D25 in
[LEDGER.md](../reports/2026-10-23-storage-cost-calculator/LEDGER.md#decisions) record the decisions behind this policy.

Every coefficient and input carries one evidence kind:

| Kind | Meaning |
|---|---|
| `source` | A code path at commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, cited as `src/<path>:<line>` |
| `measured` | Measured on the rig; a P1 coefficient has a fit and a separate hold-out |
| `provider` | A dated provider document, such as an AWS price page |
| `assumed` | A preset value, a standard deployment profile value, or an explicit assumption row |

Commit `09d0772a842` is the source of the measured binary `26.8.27.20001.altinityantalya`: `antalya-26.8` merged into
the CAS disk-transactions branch.

Every coefficient also has a basis. It decides what happens outside the verified range:

| Basis | Meaning | Outside the verified range |
|---|---|---|
| `derived` | A formula over user-visible quantities (columns, rows, bytes, settings such as `min_bytes_for_wide_part`) taken from the source; rig cases confirm it | The formula applies unchanged. The cell gets the informational mark "formula from source; verified at …" |
| `fitted` | An empirical term with no source derivation, such as EBS I/O per byte, merge fan-in or GC round duration | The column is `extrapolated` |
| none | Not yet classified; the page treats it as `fitted` | The column is `extrapolated` |

In the committed reports the bases split as follows:

- **Derived:** object counts per part on every object-store variant, multipart sizing, `s3` and `s3plain` request
  counts per part, file removal and directory requests, `GET`s per stream read (cold, warm, cache miss) on `s3` and
  CAS, and CAS startup `GET`s per part and per log record.
- **Fitted:** EBS I/O demand, lifecycle merge shape, replication acquisition and catch-up, `s3` object sizes, bytes
  per `GET`, local metadata and cache I/O, merge and mutation source reads, CAS branch population, GC round requests
  and duration, reclaim delays, and CAS startup requests.
- **Assumed:** EBS deletes, restarts, reads and capacity headroom (D17), `peak_factor` (D16), CAS interserver bytes
  per fetched part (`cas_relink_bytes_per_part`), the default `cas_marks_blob_fraction` = 1, and every preset
  default.

Further rules:

- **Store.** Measured counts are validated on RustFS behind the ledger proxy, not on AWS. No AWS bill was reconciled.
- **EBS I/O.** Demand is measured ClickHouse I/O converted with AWS accounting rules. IOPS use the provider's
  merged-stream rate, a lower bound (D16).
- **Verified range.** The range the rig covered. The page shows it in the (i) dialog only.
- **Applicability** is per variant × operation × family. A family that no report lists for a variant is
  `not_applicable`: it needs no evidence and adds nothing to the column. Only `supported` families need evidence.

## What the page shows {#page}

`utils/storage-cost-calculator/index.html` evaluates this contract in the browser. It prices one workload on four
columns: EBS, S3, S3 plain rewritable and CAS.

### Input tiers {#input-tiers}

Every registry input has a page tier (D21, D23). The `Form placement` table at the end of [Inputs](#inputs) lists
each input's tier.

| Tier | Where on the page | Rule |
|---|---|---|
| `workload` | Always visible: data size, ingest rate, retention, replicas, shards, replica placement, tables | Describes the workload |
| `setting` | Default form, at most 10, named by the ClickHouse setting in code font where one exists | A product manager sets it after one sentence, and it moves money for a typical workload |
| `advanced` | The collapsed "Low-level assumptions" section at the bottom of the page | Settable, but rarely matters |
| `hidden` | Never an input; value and evidence appear in the (i) dialog | Needs more than one sentence, or is a table key |

Data size, ingest rate and retention are linked: data = ingest × retention. Two of the three can be locked, and the
third is derived. Every other input defaults to a standard deployment profile, so the default page prices all four
columns (D22).

**Tables.** Tables on the CAS disk, tables created per month and tables dropped per month share one workload line
with one (i). On every replica, each table adds 5 GETs and a LIST per `cas_table_cleanup_period_s` (the cleanup
thread's run, `lease` cell). The 5 GETs are the rig's measured value for a table with parts, not fitted; every table
is assumed to hold parts. Each server start reads per table GET 14 (3 of them 404), PUT 2 and LIST 5,
and replays its ref log tail (`recovery`). A GC fold reads 4 objects per table with new data. Tables with new data
per round are at most the inserts per GC interval. The orphan-manifest sweep re-reads the whole tail plus 5 objects
of at most 20 tables per round (`gc`). The paged `cas/ns/stream/` listing of Deferred rounds is not modeled. The GC
terms are fitted at 1–500 tables and held out at 40. The start terms are fitted at 1–16 and held out at 8, 150 and
500 (`docs/superpowers/reports/2026-10-25-storage-cost-cas-tables`). A CREATE TABLE sends 21 requests
per replica (`cas_table_create_requests`). A DROP TABLE SYNC sends 9 plus 4 per part per replica (`deletes` cell).
Both are formulas from source, verified at 1 table. With the filesystem cache the start was measured at one table
only. Its per-table terms repeat the cache-off fit as a stated assumption, shown on the CAS Recovery cell.

### The (i) dialog {#info-dialog}

Each input, cell and column has an (i) button. All open one dialog (D26); Esc closes it and returns focus. It shows:

- **Facts:** ClickHouse name, kind, current value, the derivation rule, the measured (verified) range, the default
  and the evidence: report, measurement ids, and source cites as `src/<path>:<line> @ 09d0772a842`.
- **Why or Details:** the cell's reasons and notes, or a column's sizing and notes.
- **Reports:** links to the report directories behind the value.
- **Fill from your cluster:** a SQL query and its column mapping, when the registry has one.

Cell notes sit on their cell as an (i) marker. Column notes sit in the table's Notes row under that column.

### States {#states}

Page states:

| State | Meaning |
|---|---|
| `ok` | Every price is present; columns show their own states |
| `prices_missing` | An edited price preset has a missing or invalid price. Quantities are shown, money is not |
| `cannot_reproduce` | The link was made with another contract or code version, or names a price preset this contract lacks. No number is shown, only the values the user entered |

Column states and marks:

| State or mark | Card | Meaning |
|---|---|---|
| `ok` | Total, deltas, rank | Every cell priced inside its verified range |
| `extrapolated` | Total with an "extrapolated: N cells" badge | A fitted term outside its verified range, a lookup that used the nearest measured key, or an assumption row |
| informational mark | Total, no badge; the note is in the (i) dialog | A derived formula outside its verified range: "formula from source; verified at …". It never lowers the state |
| `not_supported` | One sentence, no total | The product cannot run the workload, such as `s3plain` with mutations > 0 |
| `backlog_grows` | One sentence, no total | CAS GC cannot keep up with retirement, so no steady-state month exists |
| `unavailable`, `unsupported_cut` | "not available", no total | A missing cell or measurement. D22 makes both internal errors that no preset may produce |

The "cheapest of N priced" badge needs at least two columns with a total. It names extrapolation when the winner is
`extrapolated`.

### EBS sizing {#ebs-sizing}

The EBS card shows one line: volume size per replica, the IOPS and MiB/s it needs, and a verdict (D25). The column's
(i) dialog expands it:

| Line | Source |
|---|---|
| Volume size per replica | Stored data × `ebs_capacity_headroom` (assumed, default 1.3) |
| Peak demand per replica | Mean demand × `peak_factor` (assumed, default 1) |
| Baseline per volume | Included IOPS and MiB/s of the price preset's block volume: 3,000 and 125 for gp3 |
| Layout | The cheapest feasible gp3 layout, or the manual one: volumes × size, IOPS, MiB/s, replicas |
| Verdict | Baseline sufficient; provision N IOPS and M MiB/s at the stated monthly extra; several volumes; or no feasible layout |
| Demand by operation | IOPS and MiB/s of each operation cell |

The same block covers the local disk of the `s3`, `s3plain` and `cas` columns (cache and metadata). A column that
keeps nothing locally pays the minimum gp3 volume per replica.

### Price presets {#price-presets}

Two presets carry list prices, each with a source URL, a check date and the quoted page wording per rule
(`docs/superpowers/cost-model/billing-contract.json`). `custom` is an editable copy of `aws-us-east-1`. `azure` has
no prices and is not offered: ClickHouse on Azure Blob Storage is not supported yet.

| Preset | Object store | Block volume | VM classes |
|---|---|---|---|
| `aws-us-east-1` | S3 Standard; PUT/COPY/POST/LIST, GET, DELETE free | gp3 | m6i, m7i |
| `gcs` | Cloud Storage Standard, us-central1, XML API; Class A, Class B, free | Hyperdisk Balanced | n4, c3 |

- **Same rule ids.** Both presets use the AWS ids for routes and the block volume (`ec2_ec2_cross_az`, `block.gp3`);
  the page shows each provider's own words.
- **Request mapping.** Each S3 call ClickHouse makes is priced as its GCS XML API counterpart. `DeleteObjects` is a
  `POST` and so Class A; `ListMultipartUploads` and `ListParts` are Class B.
- **Errors.** GCS does not bill 307, 4xx or 5xx responses.
- **Cross-zone transfer.** GCS charges the sending VM, $0.01 per GiB; AWS charges each direction.

### Formulas from source, measurements verify {#formulas-from-source}

A coefficient is a formula over quantities a user can read from a cluster (D24). Rig table shapes are verification
points, not lookup keys:

| Shape | Columns | Streams per column | Skip-index files | Projections | Empty files per Wide part | Cases (`s3` report) |
|---|---|---|---|---|---|---|
| `narrow` | 5 (`UInt64`) | 1 | 0 | 0 | 0 | `objects-per-part.narrow-compact`, `.narrow-wide`, `.narrow-compact-1k` |
| `wide` | 40 | 1 | 0 | 0 | 0 | `objects-per-part.wide-compact`, `.wide-wide`, `.wide-compact-2` |
| `types` | 21 (typed) | 3.19 | 1 | 1 | 12 | `objects-per-part.types-fit`, `.types-fit-compact`, `.types-fit-wide` |
| hold-outs | 20 (`mid-wide`); 21 at 3.29 streams (`types-holdout`) | | | | | `objects-per-part.mid-wide`, `.types-holdout` |

The `s3`, `s3plain` and CAS object-count formulas reproduce every point above exactly. CAS needs the generators'
share of distinct mark files (`cas_marks_blob_fraction`), which the `shape` presets carry. The `columns` input offers narrow
and wide as quick picks. Outside 5–40 columns or 1–3.29 streams the formulas still apply and carry the informational
mark.

## Tiered storage {#tiered-storage}

With `hot_tier_days` above 0, the page prices two tiers. New parts stay on local disk (`ebs`) for
`hot_tier_days`, then a `TTL ... TO VOLUME` move takes them to object storage (`s3`, `s3plain` or `cas`) for the rest
of the retention. A "Tiered" block under the four columns shows, per cold variant, the hot `ebs` total plus the cold
total, beside the single-tier total of that variant. The preset "Logs, tiered (7 days hot)" sets 7 days.

Each tier is an ordinary pipeline run on its own inputs (`pipeline.tiered`). Every cell, state and badge is the
single-tier one; only the `tiering` registry rows below differ.

### Inputs {#tiered-inputs}

| Input | Default | Form | Meaning |
|---|---|---|---|
| `hot_tier_days` | 0 | Tiering row | Days on local disk; 0 keeps one tier. A value not below the retention is refused with the reason. |
| `cold_gc_interval_s` | 3600 | Low-level, and in the Tiered block | `cas_gc_interval_sec` of the cold CAS disk. |
| `cold_mutations_per_month` | 0 | Low-level | ALTER UPDATE/DELETE that rewrite a cold part. |
| `cold_select_part_reads_per_day` | 0 | Low-level | Cold parts read by SELECT per day. |

### Derivations {#tiered-derivations}

| Run input | Hot tier | Cold tier |
|---|---|---|
| `retention_days` | `hot_tier_days` | `cold_retention_days` = `retention_days − hot_tier_days` |
| `data_bytes` | `hot_data_bytes` = `ingest_rate × hot_tier_days` | `cold_data_bytes` = `data_bytes − hot_data_bytes` |
| `insert_rate` | unchanged | `cold_insert_rate` = `insert_rate × parts_per_insert / merge_size_ratio` |
| `insert_rows` | unchanged | `cold_insert_rows` = `part_rows × merge_size_ratio` |
| `insert_bytes_uncompressed` | unchanged | `cold_insert_bytes_uncompressed` = `part_bytes_uncompressed × merge_size_ratio` |
| `parts_per_insert` | derived as usual | `cold_parts_per_insert` = 1: the mover clones a whole part (`MergeTreePartsMover::clonePart`) |
| `merge_size_ratio` | unchanged | `cold_merge_size_ratio` = 1 |
| `async_insert` | unchanged | 0: a move never goes through the async insert queue |
| `cas_gc_interval_s` | unchanged | `cold_gc_interval_s` |
| `mutations_per_month` | unchanged | `cold_mutations_per_month`, one moved part each, wide or compact by `min_bytes_for_wide_part` |
| `select_part_reads_per_day` | unchanged | `cold_select_part_reads_per_day` |

Everything else (`ingest_rate`, `replicas`, `cas_tables`, restarts, schema) is the same in both runs. By
construction the cold tier takes in, per month, the parts the hot tier expires (`expired_parts_per_month`), and the
two data sizes add up to the single-tier `data_bytes`.

### Assumptions {#tiered-assumptions}

- **A part moves once, at its final size.** The final size is `part_rows × merge_size_ratio`, the size
  `expired_parts_per_month` and `mutated_part_rows` already use. `merge_result_rows` and `merge_result_bytes` are
  one merge's result (`× merge_effective_fan_in`), not the final size, so they are not used here.
- **No merges in the cold tier.** Moved parts are final-sized, so the merge selector gains nothing.
  `cold_merge_size_ratio` = 1 makes `merge_results_per_month` 0, so the `s3` and `s3plain` merges cells price 0.
  The `cas` merges cell does not read `merge_size_ratio`: it prices
  `insert_rate × parts_per_insert / (merge_effective_fan_in − 1)` merges, as if parts kept merging. In the cold tier
  that is $1.27 a month on Logs, more than the tier runs; its cell note says so.
- **A TTL move is priced as the write of the moved part.** The cold intake goes through the existing insert cells.
  Each cold inserts cell carries the note "TTL move priced as the write of the moved part (assumption)".
- **One moved part per cold mutation.** A mutation over N cold parts is N mutations here.

The cold columns are `extrapolated`; the hot `ebs` run is not. On Logs a moved part has 25.6 million rows, above
every measured insert size. `cold_merge_size_ratio` = 1 lies below the measured 18.21–472.95, and a 3,600 s GC
interval above the measured 10–60 s.

### What the numbers show {#tiered-numbers}

Logs, tiered (7 days hot, 23 cold), `aws-us-east-1`, per month:

| Cold variant | Hot `ebs` | Cold | Tiered | Single tier, same variant |
|---|---|---|---|---|
| `s3` | $728.00 | $540.38 | $1,268.38 | $3,689.81 |
| `s3plain` | $728.00 | $552.72 | $1,280.72 | $6,803.81 |
| `cas` | $728.00 | $279.56 | $1,007.56 | $2,847.13 |

Single-tier `ebs` is $3,120.00.

- **Hot tier.** The 7 hot days take all inserts and merges. On `ebs` their I/O fits the gp3 baseline, so the hot
  total is capacity only. On object storage the same 7 days cost $3,160.74 on `s3`: inserts $2,441.94, merges
  $555.49. On `cas` they cost $2,582.52: inserts $1,229.03, GC at 60 s $943.09, merges $324.74.
- **Cold tier.** The cold tier takes 337.5 final-sized parts a day. Its total is mostly stored bytes: $529.00 on `s3`
  and `s3plain`, $264.64 on `cas`, which stores one copy for both replicas. Hourly GC costs $4.04.
- **Result.** With these inputs, local disk as the first tier and `cas` as the second costs $1,007.56. That is the
  lowest of the seven options on the page.

## Known inaccuracies {#known-inaccuracies}

### Model limits {#model-limits}

States below match the presets and the [Break-even](#break-even) block.

- **Coverage.** Every preset prices all four columns, except `s3plain` on `analytics_batch`. That preset has
  mutations, and a plain rewritable disk refuses every mutation, so the column is `not_supported`.
- **Metrics preset.** The EBS column is `extrapolated`. Its 1,000-row inserts lie outside the 30k–400k-row range of
  `op_write_bytes`, so the mutations, deletes and recovery cells extrapolate (D18).
- **Cross-AZ CAS.** CAS interserver bytes do not scale with part bytes, so `acquire_bytes` has no CAS key. With
  `topology = cross_az` the transfer row prices `cas_relink_bytes_per_part`: 19 428.8 B per fetched part, measured
  at 100 000–300 000 rows per insert. It is an upper bound for smaller parts; 1 000-row parts sent 33 % less.
- **EBS assumptions.** Deletes use the largest measured write per dropped part. Restarts cost 0 because they write
  less than an idle server. Reads are a full-scan derivation without page-cache hits, priced as IOPS only (D17).
- **EBS IOPS.** The merged-stream rate is a lower bound. Syscall-level counts on the rig were 1.3–6.5× higher, so
  small-I/O workloads may be underpriced. The gp3 3,000-IOPS baseline usually dominates (D16).
- **Cut P2 cards.** `async-inserts`, `amplification-run`, `detach-attach`, `cache-eviction`, `replacement` and
  `fault-fetch` were not run. A 36-hour deadline cut them (D14). A non-zero replacement rate is not modelled.
- **CAS on wide tables.** On the Logs preset the CAS column is `backlog_grows` from 67 columns. A Wide part
  uploads a blob per stream plus its distinct mark files, so retired blobs per GC round grow with the column count.
  From 67 columns they exceed the per-round graduation and redelete budgets (5,000 blobs each). The threshold uses
  the `wide` preset's share of distinct mark files, 1/30. With the default share of 1 it drops to 35 columns. A
  30 s `cas_gc_interval_s` raises it to 133. The round duration (276.2 ms + 6.962 ms per new part) adds under 1 s
  to the 60 s interval, so it barely moves the threshold.
- **No rates or latency.** The model prices request counts and bytes. S3 rate limits, `503 Slow Down` and commit
  latency are not modelled.
- **Card cells.** `card_cells` reconcile single rig cards; the page does not evaluate them.

### Blog claims against the model {#blog-claims}

Source: `docs/superpowers/blog/2026-cas-storage-model/part4-cheap-self-sufficient.md` on branch `cas-blog-article`
(`git show cas-blog-article:<path>`). The table lists each quantitative or categorical claim about storage cost.
Request prices come from the billing preset `aws-us-east-1` (AWS price list, checked 2026-10-24). Counts cite the
CAS report, `docs/superpowers/reports/2026-10-24-storage-cost-cas`.

| Line | Blog claim | Model's current answer |
|---|---|---|
| 75-76 | `GET` or `HEAD` costs about $0.0004 per thousand | Agrees. `get_and_other` is $0.0004 per 1,000 (`GetObject`, `HeadObject`, `HeadBucket`). |
| 76-77 | `PUT`, `COPY` or `LIST` costs about $0.005 per thousand | Agrees. `put_copy_post_list` is $0.005 per 1,000. It also bills multipart calls and `POST`, and treats `ListParts` as a `LIST`. |
| 77-78 | The write tier is about 12.5× the read tier | Agrees. 0.005 / 0.0004 = 12.5. |
| 78 | `DELETE` is free | Agrees. `delete_cancel` is $0 and also covers `DeleteObjects` and `AbortMultipartUpload`. |
| 79-81 | Writes scale to at least about 3,500 requests per second per prefix; bursts past that get `503 Slow Down` | Not modelled. The calculator prices request counts and has no rate limits. |
| 88-93 | The collector never `LIST`s the pool on the normal path | Agrees at the measured scale. `gc_round_requests` has no per-blob-object term: a round lists a fixed number of times, fitted at 1k–10k objects. The `cas` / `gc` cell prices `ListObjectsV2` per round. |
| 95-99 | A naive commit takes four to six sequential round trips and adds over a second to every insert | Not modelled. The calculator has no latency. |
| 121-123 | An early configuration issued about 5,100 `HEAD` per second, two-thirds of reads failing | Not modelled. This is a soak observation, not a steady-state rate. |
| 163-172 | A commit costs the new blob `PUT`s, one tree `PUT` and exactly one manifest CAS `PUT`; at most two control writes under contention | Disagrees. The `cas` / `inserts` cell prices a fixed set of control `PutObject` and `GetObject` per published part beyond the blob writes (7 each at this contract), measured exactly on the uncontended path. `probe_cas_expect` (phase-0 report) recorded 24 conditional writes for 3 inserts. |
| 169-172 | No Keeper operation at all | Not modelled. The ledger counts object-store requests only. |
| 198-199 | A beta RustFS build leaked about 74 GB across 18 keys in an hour | Not modelled. This is a backend defect on the test store. |
| 268-275 | The read path issues no `HEAD` and no `LIST` | Agrees. The `cas` / `reads` cell has `GetObject` rows only: 2 per stream on a first read (marks blob and `.bin` range), 1 when warm. A server start (`cas` / `recovery`) does send `HeadObject` and `ListObjectsV2`. |
| 293-294 | A part marked in round R is deleted in round R+1 | Not checked per round. `reclaim_delay_periods` measures delays from retirement: condemn after about 0.8 GC periods, blob delete after about 2.75. The garbage storage row uses that delay. |
| 305-306 | About 80,000 audit events in twenty minutes at two writers | Not modelled. This is a soak observation. |

## Review {#review}

Review findings on the calculator code and the measurement contract, with dispositions:
[REVIEW-HISTORY.md](REVIEW-HISTORY.md).

## Reproduce {#reproduce}

Pinned inputs, from `docs/superpowers/reports/2026-10-24-storage-cost-phase0/README.md`:

| Item | Value |
|---|---|
| Binary, downloaded | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` |
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (the download decompresses itself on first start) |
| Source commit | `09d0772a842c5d73cf3ef35b4c8b017d838215a0`; `--source-dir` defaults to a checkout of it at `tmp/src-26.8` |
| Store | RustFS 1.0.0-beta.9 behind the ledger proxy, bucket `yardstick-cost`. `rustfs-bin.sh` runs only that build, checked by sha256. The committed runs were identified as this build after the fact ([measurement contract](measurement-contract.md#identities)) |

`cost run` and `cost probe` take `tmp/yardstick.lock` themselves for the whole session. A second session exits 3 and
names the holder. Exit codes of `cost run`, `cost probe` and `cost estimate`:

| Exit | Meaning |
|---|---|
| 0 | ok |
| 1 | measurement failure: a `FAILED` record, a failed cleanup, a failed probe, or a run with no `ok` record |
| 2 | usage: bad flags or selection, unknown card, missing source commit, missing or empty run directory |
| 3 | environment: lock held, dirty rig, store not the pinned build, services not answering, run directory exists |

1. Run the rig gate. It runs the six acceptance probes, then the drill and render checks:

   ```bash
   utils/cas-yardstick/rustfs-bin.sh ensure
   python3 utils/cas-yardstick/yardstick.py cost probe \
       --binary tmp/bin/26.8/clickhouse --runs-dir tmp/cas-yardstick/cost-runs
   ```

2. Run the experiments of a research report:

   ```bash
   python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
       --runs-dir tmp/cas-yardstick/cost-runs --manifests <report directory> --cases all
   ```

   `--manifests` takes the report directory and reads its `experiments/*.json`. Each report README names its
   policies and cases. The run writes `<runs-dir>/<run_id>/ledger.jsonl`, `measurements.jsonl` and one directory per
   pass; it writes nothing into the report. `--runs-dir` defaults to `tmp/cas-yardstick/runs`. Reports:

   | Report | Content |
   |---|---|
   | `docs/superpowers/reports/2026-10-24-storage-cost-phase0` | Six acceptance probes and the runtime estimate |
   | `docs/superpowers/reports/2026-10-24-storage-cost-lifecycle` | Part types, multipart sizing, parts per insert, merges |
   | `docs/superpowers/reports/2026-10-24-storage-cost-s3` | `s3` operation graphs |
   | `docs/superpowers/reports/2026-10-24-storage-cost-s3plain` | `s3_plain_rewritable` graphs and capability matrix |
   | `docs/superpowers/reports/2026-10-24-storage-cost-cas` | CAS graphs, GC, lease, cold reads and startup |
   | `docs/superpowers/reports/2026-10-24-storage-cost-replication` | Replication, recovery and fault graphs |
   | `docs/superpowers/reports/2026-10-24-storage-cost-ebs` | EBS demand and conversion |
   | `docs/superpowers/reports/2026-10-24-storage-cost-validation` | Fresh hold-outs and the hold-out ledger |

3. Export a run into its report. No rig command does this; the committed reports were exported by hand with scripts
   that are not in the repository. The steps were:

   - Append the run's `measurements.jsonl` records to the report's `measurements.jsonl`.
   - For each cited record, copy the ledger rows from its `setup_start` mark to its `teardown_end` mark (labels
     `<mark>@<measurement_id>`) into `raw/<card>/<run_id>/ledger.jsonl`. Gzip it when it exceeds 64 KiB.
   - Gzip the pass's relay logs and strace output beside it.
   - Regenerate the checksums as `docs/superpowers/cost-model/schemas/report-directory.md` states:

     ```bash
     cd raw && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
     ```

   `closure_check.py` replays every cited record from its slice and checks `raw/SHA256SUMS`.

4. Regenerate the contract, the page copy, the measured fixtures and the `Formulas` and `Inputs` blocks. Sweep the
   new contract, then generate again so the manifest hashes the sweep. The sweep does not change the contract version:

   ```bash
   python3 utils/storage-cost-calculator/tools/generate_contract.py generate
   node utils/storage-cost-calculator/tools/sweeps.mjs \
       --contract utils/storage-cost-calculator/model-contract.json \
       --out docs/superpowers/cost-model/breakeven.json
   python3 utils/storage-cost-calculator/tools/generate_contract.py generate
   ```

5. Check that every generated file matches a fresh render, then run the closure checks:

   ```bash
   python3 utils/storage-cost-calculator/tools/generate_contract.py check --mode final
   python3 utils/storage-cost-calculator/tools/closure_check.py --mode final
   ```

6. Run the tests:

   ```bash
   SCC_DEV=1 node --test 'utils/storage-cost-calculator/test/*.test.js'
   cd utils/storage-cost-calculator/tools && python3 -m unittest discover -p 'test_*.py'
   ```

The contract's `manifest` names what a page URL reproduces: the sha256 of every code file, the registry, coverage and
billing versions, the sha256 of `breakeven.json`, and per report the source commit, rig commit, binary and the sha256
of `raw/SHA256SUMS`.

## Formulas {#formulas}

<!-- formulas:begin -->
Generated by `tools/generate_contract.py` from `model-contract.json` version `0aba74da6565e52c`. Do not edit.

| Variant | Operation | Cell owner | Applicability |
|---|---|---|---|
| `ebs` | `storage` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease; supported: local_volumes |
| `ebs` | `inserts` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `ebs` | `merges` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `ebs` | `mutations` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `ebs` | `deletes` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `ebs` | `reads` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `ebs` | `recovery` | `2026-10-24-storage-cost-ebs` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: startup_recovery |
| `ebs` | `local_disk` | `2026-10-24-storage-cost-ebs` | not_applicable |
| `ebs` | `gc` | no cell | not_applicable |
| `ebs` | `lease` | no cell | not_applicable |
| `s3` | `storage` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `s3` | `inserts` | `2026-10-24-storage-cost-s3` | not_applicable: lifecycle, mutation_deletes, reads_cache, startup_recovery, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape, thresholds, multipart_graph, replication |
| `s3` | `merges` | `2026-10-24-storage-cost-s3` | not_applicable: mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle |
| `s3` | `mutations` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: multipart_graph, mutation_deletes |
| `s3` | `deletes` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `s3` | `reads` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: reads_cache |
| `s3` | `recovery` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: startup_recovery |
| `s3` | `local_disk` | `2026-10-24-storage-cost-s3` | not_applicable: thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape |
| `s3` | `gc` | no cell | not_applicable |
| `s3` | `lease` | no cell | not_applicable |
| `s3plain` | `storage` | `2026-10-24-storage-cost-s3plain` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `s3plain` | `inserts` | `2026-10-24-storage-cost-s3plain` | not_applicable: lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape, thresholds, multipart_graph |
| `s3plain` | `merges` | `2026-10-24-storage-cost-s3plain` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `s3plain` | `mutations` | `2026-10-24-storage-cost-s3plain` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `s3plain` | `deletes` | `2026-10-24-storage-cost-s3plain` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `s3plain` | `reads` | `2026-10-24-storage-cost-s3plain` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: reads_cache |
| `s3plain` | `recovery` | `2026-10-24-storage-cost-s3plain` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: startup_recovery |
| `s3plain` | `local_disk` | `2026-10-24-storage-cost-s3plain` | not_applicable: thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape |
| `s3plain` | `gc` | no cell | not_applicable |
| `s3plain` | `lease` | no cell | not_applicable |
| `cas` | `storage` | `2026-10-24-storage-cost-cas` | not_applicable: thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_lease, local_volumes; supported: objects_per_part_by_shape, lifecycle, cas_gc |
| `cas` | `inserts` | `2026-10-24-storage-cost-cas` | not_applicable: thresholds, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape, multipart_graph, cas_branches |
| `cas` | `merges` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: thresholds, multipart_graph, lifecycle, cas_branches |
| `cas` | `mutations` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: cas_branches |
| `cas` | `deletes` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: cas_branches |
| `cas` | `reads` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: reads_cache |
| `cas` | `recovery` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: startup_recovery |
| `cas` | `local_disk` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: cas_branches |
| `cas` | `gc` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_lease, local_volumes; supported: lifecycle, cas_gc |
| `cas` | `lease` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, local_volumes; supported: cas_lease |

### `ebs` / `storage` {#formula-ebs-storage}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `capacity` | `all` | `volume` |  | `data_bytes × ebs_capacity_headroom` |

### `ebs` / `inserts` {#formula-ebs-inserts}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_io` | `mibps` | `all` | `volume` |  | `ingest_rate × 1024 / 86400 × peak_factor × (lookup(io_bytes_per_op)[inserts, write] + lookup(io_bytes_per_op)[inserts, read] + insert_write_overhead_rows / insert_rows)` |
| `block_io` | `iops` | `all` | `volume` |  | `ingest_rate × 1024 / 86400 × peak_factor × (lookup(io_bytes_per_op)[inserts, write] + lookup(io_bytes_per_op)[inserts, read] + insert_write_overhead_rows / insert_rows) × ebs_sequential_ios_per_mib` |
| `transfer` | `interserver` | `all` | `variant` |  | `if topology == cross_az then ingest_rate / 86400 × 2628000 × (replicas − 1) × lookup(acquire_bytes)[ebs, insert] else 0` |

Notes:

- `IOPS at the AWS floor of 4 I/Os per MiB (a merged sequential stream): a lower bound; the rig syscall proxy counted 1.3-6.5x more; the gp3 3000-IOPS baseline usually dominates. Throughput x peak_factor (assumed, default 1 = average rate)`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `acquire_bytes` | `table over variant_id, result_type` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"part_rows": [50000, 200000]}` | 2026-10-24-storage-cost-replication |
| `insert_write_overhead_rows` | `1258.1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [1000, 300000]}` | 2026-10-24-storage-cost-ebs |
| `io_bytes_per_op` | `table over io_op, io_direction` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [1000, 300000]}` | 2026-10-24-storage-cost-ebs |

### `ebs` / `merges` {#formula-ebs-merges}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_io` | `mibps` | `all` | `volume` |  | `ingest_rate × (write_amplification_rule − 1) × 1024 / 86400 × peak_factor × (lookup(io_bytes_per_op)[merges, write] + lookup(io_bytes_per_op)[merges, read])` |
| `block_io` | `iops` | `all` | `volume` |  | `ingest_rate × (write_amplification_rule − 1) × 1024 / 86400 × peak_factor × (lookup(io_bytes_per_op)[merges, write] + lookup(io_bytes_per_op)[merges, read]) × ebs_sequential_ios_per_mib` |

Notes:

- `IOPS at the AWS floor of 4 I/Os per MiB (a merged sequential stream): a lower bound; the rig syscall proxy counted 1.3-6.5x more; the gp3 3000-IOPS baseline usually dominates. Throughput x peak_factor (assumed, default 1 = average rate)`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `io_bytes_per_op` | `table over io_op, io_direction` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [1000, 300000]}` | 2026-10-24-storage-cost-ebs |

### `ebs` / `mutations` {#formula-ebs-mutations}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_io` | `mibps` | `all` | `volume` |  | `(mutated_compact_parts_per_month + mutated_wide_parts_per_month) × mutated_part_rows × lookup(op_write_bytes)[mutations] / 2755657728000 × peak_factor` |
| `block_io` | `iops` | `all` | `volume` |  | `(mutated_compact_parts_per_month + mutated_wide_parts_per_month) × mutated_part_rows × lookup(op_write_bytes)[mutations] / 2755657728000 × peak_factor × ebs_sequential_ios_per_mib` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `op_write_bytes` | `table over io_op` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [30000, 400000]}` | 2026-10-24-storage-cost-ebs |

### `ebs` / `deletes` {#formula-ebs-deletes}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_io` | `mibps` | `all` | `volume` |  | `expired_parts_per_month × lookup(op_write_bytes)[deletes] / 2755657728000 × peak_factor` |
| `block_io` | `iops` | `all` | `volume` |  | `expired_parts_per_month × lookup(op_write_bytes)[deletes] / 2755657728000 × peak_factor × ebs_sequential_ios_per_mib` |

Notes:

- `Drops: assumption, 273 kB written per dropped part (the largest measured record; the per-part unit did not carry between the fit and hold-out layouts, 125 kB); an upper bound pending a model keyed on part size`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `op_write_bytes` | `table over io_op` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [30000, 400000]}` | 2026-10-24-storage-cost-ebs |

### `ebs` / `reads` {#formula-ebs-reads}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_io` | `mibps` | `all` | `volume` |  | `select_part_reads_per_day × 30.416666666666668 × data_bytes × 1073741824 / active_parts × lookup(io_bytes_per_op)[reads, read] / 2755657728000 × peak_factor` |
| `block_io` | `iops` | `all` | `volume` |  | `select_part_reads_per_day × 30.416666666666668 × data_bytes × 1073741824 / active_parts × lookup(io_bytes_per_op)[reads, read] / 2755657728000 × peak_factor × ebs_sequential_ios_per_mib` |

Notes:

- `Reads: a full scan per part read, assumed; page-cache hits not modelled; EBS reads priced as IOPS only, read throughput not added (assumption)`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `io_bytes_per_op` | `table over io_op, io_direction` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [1000, 300000]}` | 2026-10-24-storage-cost-ebs |

### `ebs` / `recovery` {#formula-ebs-recovery}

Owner: `2026-10-24-storage-cost-ebs`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_io` | `mibps` | `all` | `volume` |  | `restarts_per_month × lookup(op_write_bytes)[recovery] / 2755657728000 × peak_factor` |
| `block_io` | `iops` | `all` | `volume` |  | `restarts_per_month × lookup(op_write_bytes)[recovery] / 2755657728000 × peak_factor × ebs_sequential_ios_per_mib` |

Notes:

- `Restarts: writes below the idle baseline on the rig; priced 0 (assumption)`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `op_write_bytes` | `table over io_op` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-ebs | 0.2.0 | `{"insert_rows": [30000, 400000]}` | 2026-10-24-storage-cost-ebs |

### `ebs` / `local_disk` {#formula-ebs-local-disk}

Owner: `2026-10-24-storage-cost-ebs`.

No ledger rows.

### `s3` / `storage` {#formula-s3-storage}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `storage` | `active` | `all` | `variant` |  | `data_bytes × replicas × 1` |
| `storage` | `outdated` | `all` | `variant` |  | `ingest_rate × (write_amplification_rule − 1) / 86400 × old_parts_lifetime × replicas × 1` |

### `s3` / `inserts` {#formula-s3-inserts}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × (objects_per_part − large_objects_per_part)` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part` |
| `request` | `UploadPart` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part × multipart_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part` |
| `request` | `HeadObject` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × insert_tx_checks` |
| `request` | `GetObject` | `ok` | `variant` |  | `parts_inserted_per_month × (replicas − 1) × objects_per_part` |
| `request` | `GetObject` | `ok` | `variant` |  | `parts_inserted_per_month × (replicas − 1) × lookup(acquire_requests)[s3, insert, receiver, GetObject]` |
| `transfer` | `interserver` | `all` | `variant` |  | `if topology == cross_az then ingest_rate / 86400 × 2628000 × (replicas − 1) × lookup(acquire_bytes)[s3, insert] else 0` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `acquire_bytes` | `table over variant_id, result_type` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"part_rows": [50000, 200000]}` | 2026-10-24-storage-cost-replication |
| `acquire_requests` | `table over variant_id, result_type, replication_role, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"insert_rows": [1000, 200000]}` | 2026-10-24-storage-cost-replication |
| `insert_tx_checks` | `0` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"insert_rows": [1000, 600000]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `merges` {#formula-s3-merges}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `merge_results_per_month × replicas × ((if merge_results_per_month == 0 then 0 else merge_result_objects_per_part) − large_objects_per_merge_result)` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `merge_results_per_month × replicas × large_objects_per_merge_result` |
| `request` | `UploadPart` | `ok` | `variant` |  | `merge_results_per_month × replicas × large_objects_per_merge_result × multipart_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `merge_results_per_month × replicas × large_objects_per_merge_result` |
| `request` | `GetObject` | `ok` | `variant` |  | `if merge_sources_per_month == 0 then 0 else merge_sources_per_month × replicas × (if cache_enabled == 1 then merge_gets_per_source_part_cached else merge_gets_per_source_part)` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `merge_removals_per_month × replicas × 1` |

### `s3` / `mutations` {#formula-s3-mutations}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `mutated_compact_parts_per_month × replicas × objects_per_compact_part + mutated_wide_parts_per_month × replicas × mutation_objects_written + mutations_per_month × (if replicas == 1 then 1 else 0) × mutation_entry_objects − mutated_wide_parts_per_month × replicas × large_objects_per_mutated_part` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `mutated_wide_parts_per_month × replicas × large_objects_per_mutated_part` |
| `request` | `UploadPart` | `ok` | `variant` |  | `mutated_wide_parts_per_month × replicas × large_objects_per_mutated_part × multipart_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `mutated_wide_parts_per_month × replicas × large_objects_per_mutated_part` |
| `request` | `GetObject` | `ok` | `variant` |  | `(if mutated_compact_parts_per_month == 0 then 0 else mutated_compact_parts_per_month × replicas × (lookup(mutation_gets_per_part)[cache_enabled, mutation_kind, compact] + mutation_compact_gets_per_full_granule × max(1, floor_to(mutated_part_rows + index_granularity / 2, index_granularity) / index_granularity))) + mutated_wide_parts_per_month × replicas × lookup(mutation_gets_per_part)[cache_enabled, mutation_kind, wide]` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `(mutated_compact_parts_per_month + mutated_wide_parts_per_month) × replicas × delete_calls_per_removed_part` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `mutation_entries_removed_per_month × (if replicas == 1 then 1 else 0) × mutation_entry_objects` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `delete_calls_per_removed_part` | `1` | `count` | source | src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:599 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `mutation_compact_gets_per_full_granule` | `2` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-s3 |
| `mutation_entry_objects` | `1` | `count` | source | src/Storages/MergeTree/MergeTreeMutationEntry.cpp:73 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `mutation_gets_per_part` | `table over cache_enabled, mutation_kind, part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"insert_rows": [1000, 400000]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `deletes` {#formula-s3-deletes}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `expiry_operations_per_month × replicas × empty_part_objects` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `(expired_parts_per_month + expiry_operations_per_month) × replicas × delete_calls_per_removed_part` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `delete_calls_per_removed_part` | `1` | `count` | source | src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:599 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `empty_part_objects` | `8` | `count` | source | src/Storages/StorageMergeTree.cpp:2742 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |

### `s3` / `reads` {#formula-s3-reads}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `(if cache_enabled == 1 then select_part_reads_per_day × 30.416666666666668 × cache_miss_fraction × cached_miss_gets_per_part_read else select_part_reads_per_day × 30.416666666666668 × warm_gets_per_stream_read × read_streams_per_part + (parts_first_read_per_month × replicas + restarts_per_month × replicas × active_parts) × (stats_gets_per_column × shape_columns + (gets_per_stream_read − warm_gets_per_stream_read) × read_streams_per_part)) + (if cache_enabled == 1 then 0 else statistics_refreshes_per_month × replicas × active_parts × stats_gets_per_column × shape_columns)` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `gets_per_stream_read` | `2` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"read_streams_per_part": [3, 4]}` | 2026-10-24-storage-cost-s3 |
| `stats_gets_per_column` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"read_streams_per_part": [3, 4]}` | 2026-10-24-storage-cost-s3 |
| `warm_gets_per_stream_read` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"read_streams_per_part": [3, 4]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `recovery` {#formula-s3-recovery}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (active_parts + outdated_parts) × startup_gets_per_part + restarts_per_month × replicas × lookup(startup_requests)[cache_enabled, GetObject] + restarts_per_month × (if replicas == 1 then 1 else 0) × retained_mutation_entries` |
| `request` | `PutObject` | `ok` | `variant` |  | `restarts_per_month × replicas × lookup(startup_requests)[cache_enabled, PutObject]` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `restarts_per_month × replicas × lookup(startup_requests)[cache_enabled, DeleteObject]` |
| `request` | `GetObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (if replicas > 1 then 1 else 0) × missed_parts_per_restart × lookup(catchup_requests_per_missed_part)[s3, GetObject]` |
| `request` | `PutObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (if replicas > 1 then 1 else 0) × missed_parts_per_restart × lookup(catchup_requests_per_missed_part)[s3, PutObject]` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `catchup_requests_per_missed_part` | `table over variant_id, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"missed_parts_per_restart": [0, 5]}` | 2026-10-24-storage-cost-replication |
| `startup_requests` | `table over cache_enabled, startup_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `local_disk` {#formula-s3-local-disk}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `capacity` | `all` | `replica` |  | `(active_parts × objects_per_wide_part + merge_removals_per_month / 2628000 × old_parts_lifetime × objects_per_part) × metadata_bytes_per_object / 1073741824 + (if cache_enabled == 1 then cache_size_gib else 0)` |
- Excluded op `PutObject`: object writes are priced by the inserts and merges cells

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `metadata_bytes_per_object` | `4096` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"insert_rows": [1000, 600000]}` | 2026-10-24-storage-cost-s3 |

### `s3plain` / `storage` {#formula-s3plain-storage}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `storage` | `active` | `all` | `variant` |  | `data_bytes × replicas × 1` |
| `storage` | `outdated` | `all` | `variant` |  | `ingest_rate × (write_amplification_rule − 1) / 86400 × old_parts_lifetime × replicas × 1` |

### `s3plain` / `inserts` {#formula-s3plain-inserts}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × (plain_objects_per_part − large_objects_per_part) + parts_inserted_per_month × replicas × plain_meta_objects_per_part × (lookup(plain_dir_create_requests)[PutObject] + plain_rename_requests)` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part` |
| `request` | `UploadPart` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part × multipart_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part` |
| `request` | `HeadObject` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × insert_tx_checks + parts_inserted_per_month × replicas × plain_meta_objects_per_part × lookup(plain_dir_create_requests)[HeadObject]` |
| `request` | `GetObject` | `ok` | `variant` |  | `parts_inserted_per_month × (replicas − 1) × plain_objects_per_part` |
| `request` | `GetObject` | `ok` | `variant` |  | `parts_inserted_per_month × (replicas − 1) × (if replicas == 1 then 0 else lookup(acquire_requests)[s3plain, insert, receiver, GetObject])` |
| `transfer` | `interserver` | `all` | `variant` |  | `if topology == cross_az then ingest_rate / 86400 × 2628000 × (replicas − 1) × lookup(acquire_bytes)[s3plain, insert] else 0` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `acquire_bytes` | `table over variant_id, result_type` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"part_rows": [50000, 200000]}` | 2026-10-24-storage-cost-replication |
| `acquire_requests` | `table over variant_id, result_type, replication_role, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"insert_rows": [1000, 200000]}` | 2026-10-24-storage-cost-replication |
| `insert_tx_checks` | `0` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"insert_rows": [1000, 600000]}` | 2026-10-24-storage-cost-s3 |
| `plain_dir_create_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:125 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_rename_requests` | `1` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:239 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |

### `s3plain` / `merges` {#formula-s3plain-merges}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `merge_results_per_month × replicas × ((if merge_results_per_month == 0 then 0 else if merge_result_part_type == compact then objects_per_compact_part else plain_objects_per_wide_part) − large_objects_per_merge_result) + merge_results_per_month × replicas × plain_meta_objects_per_part × (lookup(plain_dir_create_requests)[PutObject] + plain_rename_requests) + merge_removals_per_month × replicas × (plain_objects_per_part × lookup(plain_unlink_requests)[PutObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[PutObject])` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `merge_results_per_month × replicas × large_objects_per_merge_result + merge_removals_per_month × replicas × large_objects_per_part` |
| `request` | `UploadPart` | `ok` | `variant` |  | `merge_results_per_month × replicas × large_objects_per_merge_result × multipart_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `merge_results_per_month × replicas × large_objects_per_merge_result + merge_removals_per_month × replicas × large_objects_per_part` |
| `request` | `GetObject` | `ok` | `variant` |  | `if merge_sources_per_month == 0 then 0 else merge_sources_per_month × replicas × (if cache_enabled == 1 then merge_gets_per_source_part_cached else merge_gets_per_source_part)` |
| `request` | `HeadObject` | `ok` | `variant` |  | `merge_results_per_month × replicas × plain_meta_objects_per_part × lookup(plain_dir_create_requests)[HeadObject] + merge_removals_per_month × replicas × (plain_objects_per_part × lookup(plain_unlink_requests)[HeadObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[HeadObject])` |
| `request` | `CopyObject` | `ok` | `variant` |  | `merge_removals_per_month × replicas × (plain_objects_per_part × lookup(plain_unlink_requests)[CopyObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[CopyObject]) − merge_removals_per_month × replicas × large_objects_per_part` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `merge_removals_per_month × replicas × (plain_objects_per_part × lookup(plain_unlink_requests)[DeleteObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[DeleteObject])` |
| `request` | `UploadPartCopy` | `ok` | `variant` |  | `merge_removals_per_month × replicas × large_objects_per_part × multipart_copy_parts` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `plain_dir_create_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:125 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_dir_remove_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:297 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_rename_requests` | `1` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:239 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_unlink_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:390 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |

### `s3plain` / `mutations` {#formula-s3plain-mutations}

Owner: `2026-10-24-storage-cost-s3plain`.

No ledger rows.

| State | Condition | Reason |
|---|---|---|
| `not_supported` | `if mutations_per_month + mutated_compact_parts_per_month + mutated_wide_parts_per_month ≠ 0 then 1 else 0` | s3_plain_rewritable cannot perform mutations (ALTER UPDATE, ALTER DELETE and lightweight DELETE; a lightweight UPDATE (patch parts) is possible only with enable_block_number_column and enable_block_offset_column): a plain metadata storage reports no hard links (src/Disks/DiskObjectStorage/DiskObjectStorage.cpp:805 @ 09d0772a842); checkMutationIsPossible refuses every mutation on such a disk (src/Storages/MergeTree/MergeTreeData.cpp:6240 @ 09d0772a842); a lightweight DELETE runs as ALTER UPDATE _row_exists, the same check (src/Interpreters/InterpreterDeleteQuery.cpp:200 @ 09d0772a842); that is the default lightweight_delete_mode (src/Core/Settings.cpp:5070 @ 09d0772a842); a lightweight UPDATE (patch parts) needs enable_block_number_column (src/Storages/MergeTree/MergeTreeData.cpp:12630 @ 09d0772a842); and enable_block_offset_column (src/Storages/MergeTree/MergeTreeData.cpp:12635 @ 09d0772a842); which is off by default (src/Storages/MergeTree/MergeTreeSettings.cpp:2013 @ 09d0772a842) |

### `s3plain` / `deletes` {#formula-s3plain-deletes}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `expired_parts_per_month × replicas × plain_empty_part_objects + expired_parts_per_month × replicas × plain_meta_objects_per_part × (lookup(plain_dir_create_requests)[PutObject] + plain_rename_requests) + expired_parts_per_month × replicas × ((plain_objects_per_part + plain_expired_part_extra_files) × lookup(plain_unlink_requests)[PutObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[PutObject]) + expired_parts_per_month × replicas × (plain_empty_part_objects × lookup(plain_unlink_requests)[PutObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[PutObject])` |
| `request` | `HeadObject` | `ok` | `variant` |  | `expired_parts_per_month × replicas × plain_meta_objects_per_part × lookup(plain_dir_create_requests)[HeadObject] + expired_parts_per_month × replicas × ((plain_objects_per_part + plain_expired_part_extra_files) × lookup(plain_unlink_requests)[HeadObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[HeadObject]) + expired_parts_per_month × replicas × (plain_empty_part_objects × lookup(plain_unlink_requests)[HeadObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[HeadObject])` |
| `request` | `CopyObject` | `ok` | `variant` |  | `expired_parts_per_month × replicas × ((plain_objects_per_part + plain_expired_part_extra_files) × lookup(plain_unlink_requests)[CopyObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[CopyObject]) + expired_parts_per_month × replicas × (plain_empty_part_objects × lookup(plain_unlink_requests)[CopyObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[CopyObject]) − expired_parts_per_month × replicas × large_objects_per_merge_result` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `expired_parts_per_month × replicas × ((plain_objects_per_part + plain_expired_part_extra_files) × lookup(plain_unlink_requests)[DeleteObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[DeleteObject]) + expired_parts_per_month × replicas × (plain_empty_part_objects × lookup(plain_unlink_requests)[DeleteObject] + plain_meta_objects_per_part × lookup(plain_dir_remove_requests)[DeleteObject])` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `expired_parts_per_month × replicas × large_objects_per_merge_result` |
| `request` | `UploadPartCopy` | `ok` | `variant` |  | `expired_parts_per_month × replicas × large_objects_per_merge_result × multipart_copy_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `expired_parts_per_month × replicas × large_objects_per_merge_result` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `plain_dir_create_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:125 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_dir_remove_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:297 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_empty_part_objects` | `11` | `count` | source | src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_expired_part_extra_files` | `2` | `count` | source | src/Storages/MergeTree/MergeTreePartition.cpp:470 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_rename_requests` | `1` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:239 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |
| `plain_unlink_requests` | `table over plain_op` | `count` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:390 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |

### `s3plain` / `reads` {#formula-s3plain-reads}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `(if cache_enabled == 1 then select_part_reads_per_day × 30.416666666666668 × cache_miss_fraction × cached_miss_gets_per_part_read else select_part_reads_per_day × 30.416666666666668 × warm_gets_per_stream_read × read_streams_per_part + (parts_first_read_per_month × replicas + restarts_per_month × replicas × active_parts) × (stats_gets_per_column × shape_columns + (gets_per_stream_read − warm_gets_per_stream_read) × read_streams_per_part)) + (if cache_enabled == 1 then 0 else statistics_refreshes_per_month × replicas × active_parts × stats_gets_per_column × shape_columns)` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `gets_per_stream_read` | `2` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"read_streams_per_part": [3, 4]}` | 2026-10-24-storage-cost-s3 |
| `stats_gets_per_column` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"read_streams_per_part": [3, 4]}` | 2026-10-24-storage-cost-s3 |
| `warm_gets_per_stream_read` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"read_streams_per_part": [3, 4]}` | 2026-10-24-storage-cost-s3 |

### `s3plain` / `recovery` {#formula-s3plain-recovery}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(plain_startup_requests)[cache_enabled, fixed, PutObject] + (active_parts + outdated_parts) × plain_meta_objects_per_part × lookup(plain_startup_requests)[cache_enabled, per_dir, PutObject])` |
| `request` | `HeadObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(plain_startup_requests)[cache_enabled, fixed, HeadObject] + (active_parts + outdated_parts) × plain_meta_objects_per_part × lookup(plain_startup_requests)[cache_enabled, per_dir, HeadObject])` |
| `request` | `GetObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (active_parts + outdated_parts) × startup_gets_per_part + restarts_per_month × replicas × (lookup(plain_startup_requests)[cache_enabled, fixed, GetObject] + (active_parts + outdated_parts) × plain_meta_objects_per_part × lookup(plain_startup_requests)[cache_enabled, per_dir, GetObject])` |
| `request` | `CopyObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(plain_startup_requests)[cache_enabled, fixed, CopyObject] + (active_parts + outdated_parts) × plain_meta_objects_per_part × lookup(plain_startup_requests)[cache_enabled, per_dir, CopyObject])` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(plain_startup_requests)[cache_enabled, fixed, DeleteObject] + (active_parts + outdated_parts) × plain_meta_objects_per_part × lookup(plain_startup_requests)[cache_enabled, per_dir, DeleteObject])` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(plain_startup_requests)[cache_enabled, fixed, ListObjectsV2] + (active_parts + outdated_parts) × plain_meta_objects_per_part × lookup(plain_startup_requests)[cache_enabled, per_dir, ListObjectsV2])` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `plain_startup_requests` | `table over cache_enabled, plain_startup_kind, plain_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3plain | 0.3.0 | `{"replicas": [1, 3]}` | 2026-10-24-storage-cost-s3plain |

### `s3plain` / `local_disk` {#formula-s3plain-local-disk}

Owner: `2026-10-24-storage-cost-s3plain`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `capacity` | `all` | `replica` |  | `active_parts × plain_objects_per_wide_part × plain_local_metadata_bytes_per_object / 1073741824 + (if cache_enabled == 1 then cache_size_gib else 0)` |
- Excluded op `PutObject`: object writes are priced by the inserts and merges cells
- Excluded op `HeadObject`: directory reads are priced by the inserts and merges cells

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `plain_local_metadata_bytes_per_object` | `0` | `B` | source | src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h:87 @ 09d0772a842 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3plain |

### `cas` / `storage` {#formula-cas-storage}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `storage` | `active` | `all` | `variant` |  | `data_bytes × 1` |
| `storage` | `garbage` | `all` | `variant` |  | `(insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × cas_blob_bytes / 1073741824 × reclaim_periods_blob_delete × (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × 1` |
| `storage` | `metadata` | `all` | `variant` |  | `data_bytes × 1073741824 / (cas_blob_uploads_per_part × cas_blob_bytes) × (cas_blob_uploads_per_part × 86 + cas_manifest_bytes) / 1073741824 × 1` |
- Excluded category `temporary`: local scratch staging is priced in local_disk

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `merge_effective_fan_in` | `6.1437` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-lifecycle | 0.2.0 | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-lifecycle |

### `cas` / `inserts` {#formula-cas-inserts}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert × (cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × (2 − multipart_upload) + 7) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert × (cas_blob_uploads_per_part × cas_hash_hit_fraction + 7) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert × cas_blob_uploads_per_part × 2628000` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `insert_rate × parts_per_insert × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × multipart_upload × 2628000` |
| `request` | `UploadPart` | `ok` | `variant` |  | `insert_rate × parts_per_insert × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × multipart_upload × multipart_parts × 2628000` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `insert_rate × parts_per_insert × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × multipart_upload × 2628000` |
| `transfer` | `interserver` | `all` | `variant` |  | `if topology == cross_az then insert_rate × parts_per_insert × 2628000 × (replicas − 1) × cas_relink_bytes_per_part / 1073741824 else 0` |

Notes:

- `cas_blob_uploads_per_part`
- `cas_hash_hit_fraction`
- `cas_relink_bytes_per_part`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `cas_relink_bytes_per_part` | `19428.8` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"insert_rows": [100000, 300000]}` | 2026-10-24-storage-cost-replication |

### `cas` / `merges` {#formula-cas-merges}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × (cas_merge_result_blob_uploads × (1 − cas_hash_hit_fraction) × (2 − multipart_upload) + 7 + merge_effective_fan_in × 2) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × (merge_effective_fan_in × (cas_blob_uploads_per_part + 2) + 7 + cas_merge_result_blob_uploads × cas_hash_hit_fraction) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × cas_merge_result_blob_uploads × 2628000` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × cas_merge_result_blob_uploads × (1 − cas_hash_hit_fraction) × multipart_upload × 2628000` |
| `request` | `UploadPart` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × cas_merge_result_blob_uploads × (1 − cas_hash_hit_fraction) × multipart_upload × multipart_parts × 2628000` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × cas_merge_result_blob_uploads × (1 − cas_hash_hit_fraction) × multipart_upload × 2628000` |

Notes:

- `merge_effective_fan_in`
- `cas_merge_result_blob_uploads`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `merge_effective_fan_in` | `6.1437` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-lifecycle | 0.2.0 | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-lifecycle |

### `cas` / `mutations` {#formula-cas-mutations}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `(cas_mutations_per_s × 2 + cas_mutations_per_s × cas_parts_per_mutation × (cas_mutated_columns × (2 + 2 × (1 − cas_mutation_marks_hit_fraction)) + 9)) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `(cas_mutations_per_s × 1 + cas_mutations_per_s × cas_parts_per_mutation × (cas_mutated_columns × (1 + cas_mutation_marks_hit_fraction) + 9)) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `(cas_mutations_per_s × 3 + cas_mutations_per_s × cas_parts_per_mutation × cas_mutated_columns × 2) × 2628000` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `cas_mutations_per_s × 1 × 2628000` |

Notes:

- `cas_branch_requests`

### `cas` / `deletes` {#formula-cas-deletes}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `cas_table_drops_per_s × (cas_parts_per_dropped_table × 2 + 3) × 2628000 × replicas` |
| `request` | `GetObject` | `ok` | `variant` |  | `cas_table_drops_per_s × (cas_parts_per_dropped_table × 2 + 2) × 2628000 × replicas` |
| `request` | `HeadObject` | `ok` | `variant` |  | `cas_table_drops_per_s × 1 × 2628000 × replicas` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `cas_table_drops_per_s × 1 × 2628000 × replicas` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `cas_table_drops_per_s × 2 × 2628000 × replicas` |
| `request` | `GetObject` | `ok` | `variant` |  | `cas_table_creates_per_month × replicas × 9` |
| `request` | `GetObject` | `client_error` | `variant` |  | `cas_table_creates_per_month × replicas × 2` |
| `request` | `HeadObject` | `client_error` | `variant` |  | `cas_table_creates_per_month × replicas × 1` |
| `request` | `PutObject` | `ok` | `variant` |  | `cas_table_creates_per_month × replicas × 4` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `cas_table_creates_per_month × replicas × 5` |

Notes:

- `cas_foreground_ref_removal_requests`
- `cas_table_create_requests`

### `cas` / `reads` {#formula-cas-reads}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `if cache_enabled == 1 then select_part_reads_per_day × 30.416666666666668 × cache_miss_fraction × cas_cached_miss_gets_per_stream_read × read_streams_per_part else select_part_reads_per_day × 30.416666666666668 × cas_warm_gets_per_stream_read × read_streams_per_part + (parts_first_read_per_month × replicas + restarts_per_month × replicas × active_parts) × (cas_gets_per_stream_read − cas_warm_gets_per_stream_read) × read_streams_per_part` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `cas_cached_miss_gets_per_stream_read` | `2` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-cas | 0.3.0 | `{"read_streams_per_part": [2, 3]}` | 2026-10-24-storage-cost-cas |
| `cas_gets_per_stream_read` | `2` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-cas | 0.3.0 | `{"read_streams_per_part": [2, 3]}` | 2026-10-24-storage-cost-cas |
| `cas_warm_gets_per_stream_read` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-cas | 0.3.0 | `{"read_streams_per_part": [2, 3]}` | 2026-10-24-storage-cost-cas |

### `cas` / `recovery` {#formula-cas-recovery}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(cas_startup_requests_base)[cache_enabled, GetObject] + cas_tables × lookup(cas_startup_requests_per_table)[cache_enabled, GetObject]) + restarts_per_month × replicas × ((active_parts + outdated_parts) × lookup(cas_startup_gets_per_part)[part_type] + cas_tables × cas_ref_log_tail_records × cas_startup_gets_per_log_record) + restarts_per_month × (if replicas == 1 then 1 else 0) × retained_mutation_entries + restarts_per_month × replicas × (if replicas > 1 then 1 else 0) × missed_parts_per_restart × lookup(catchup_requests_per_missed_part)[cas, GetObject]` |
| `request` | `PutObject` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(cas_startup_requests_base)[cache_enabled, PutObject] + cas_tables × lookup(cas_startup_requests_per_table)[cache_enabled, PutObject]) + restarts_per_month × replicas × (if replicas > 1 then 1 else 0) × missed_parts_per_restart × lookup(catchup_requests_per_missed_part)[cas, PutObject]` |
| `request` | `HeadObject` | `ok` | `variant` |  | `restarts_per_month × replicas × lookup(cas_startup_requests_base)[cache_enabled, HeadObject]` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `restarts_per_month × replicas × lookup(cas_startup_requests_base)[cache_enabled, DeleteObject]` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `restarts_per_month × replicas × (lookup(cas_startup_requests_base)[cache_enabled, ListObjectsV2] + cas_tables × lookup(cas_startup_requests_per_table)[cache_enabled, ListObjectsV2])` |
| `request` | `GetObject` | `client_error` | `variant` |  | `restarts_per_month × replicas × (lookup(cas_startup_error_requests_base)[cache_enabled, GetObject] + cas_tables × lookup(cas_startup_error_requests_per_table)[cache_enabled, GetObject])` |
| `request` | `HeadObject` | `client_error` | `variant` |  | `restarts_per_month × replicas × lookup(cas_startup_error_requests_base)[cache_enabled, HeadObject]` |
| `request` | `PutObject` | `client_error` | `variant` |  | `restarts_per_month × replicas × lookup(cas_startup_error_requests_base)[cache_enabled, PutObject]` |
| `request` | `DeleteObject` | `client_error` | `variant` |  | `restarts_per_month × replicas × lookup(cas_startup_error_requests_base)[cache_enabled, DeleteObject]` |

Notes:

- `Start with the filesystem cache: ASSUMPTION, the per-table terms repeat the cache-off fit; the cached start was measured at one table only`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `cas_startup_error_requests_base` | `table over cache_enabled, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-25-storage-cost-cas-tables | 0.1.0 | `{"cas_tables": [1, 500]}` | 2026-10-25-storage-cost-cas-tables |
| `cas_startup_error_requests_per_table` | `table over cache_enabled, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-25-storage-cost-cas-tables | 0.1.0 | `{"cas_tables": [1, 500]}` | 2026-10-25-storage-cost-cas-tables |
| `cas_startup_gets_per_log_record` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-cas | 0.3.0 | `{"cas_tables": [1, 1]}` | 2026-10-24-storage-cost-cas |
| `cas_startup_gets_per_part` | `table over part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-cas | 0.3.0 | `{"insert_rows": [100, 100000]}` | 2026-10-24-storage-cost-cas |
| `cas_startup_requests_base` | `table over cache_enabled, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-25-storage-cost-cas-tables | 0.1.0 | `{"cas_tables": [1, 500]}` | 2026-10-25-storage-cost-cas-tables |
| `cas_startup_requests_per_table` | `table over cache_enabled, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-25-storage-cost-cas-tables | 0.1.0 | `{"cas_tables": [1, 500]}` | 2026-10-25-storage-cost-cas-tables |
| `catchup_requests_per_missed_part` | `table over variant_id, replication_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-replication | 0.3.0 | `{"missed_parts_per_restart": [0, 5]}` | 2026-10-24-storage-cost-replication |

### `cas` / `local_disk` {#formula-cas-local-disk}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `capacity` | `all` | `replica` |  | `cas_blob_uploads_per_part × cas_blob_bytes × (insert_clients + 1) / 1073741824` |

### `cas` / `gc` {#formula-cas-gc}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `(1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × (if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation > 0 then gc_round_requests_fold_get + (if cas_tables > insert_rate × cas_gc_interval_s then insert_rate × cas_gc_interval_s else cas_tables) × gc_round_requests_fold_get_per_changed_table + (if cas_tables > gc_round_sweep_namespace_budget then gc_round_sweep_namespace_budget else cas_tables) × (cas_ref_log_tail_records × gc_round_requests_fold_get_per_swept_tail_log_record + gc_round_requests_fold_get_per_swept_table) else (8 × gc_round_requests_deferred_get + gc_round_requests_fold_get + (if cas_tables > gc_round_sweep_namespace_budget then gc_round_sweep_namespace_budget else cas_tables) × (cas_ref_log_tail_records × gc_round_requests_fold_get_per_swept_tail_log_record + gc_round_requests_fold_get_per_swept_table)) / 9) + (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × gc_round_requests_fold_get_per_manifest + (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_log_records_per_part × gc_round_requests_fold_get_per_new_log_record) × 2628000` |
| `request` | `PutObject` | `ok` | `variant` |  | `1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × (if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation > 0 then gc_round_requests_fold_put else (8 × gc_round_requests_deferred_put + gc_round_requests_fold_put) / 9) × 2628000` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × (if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation > 0 then gc_round_requests_fold_delete else (8 × 0 + gc_round_requests_fold_delete) / 9) × 2628000` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × (if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation > 0 then gc_round_requests_fold_list else (8 × gc_round_requests_deferred_list + gc_round_requests_fold_list) / 9) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `(gc_heartbeat_requests_per_s / 2 + 1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000)) × 2628000` |
| `request` | `PutObject` | `ok` | `variant` |  | `(gc_heartbeat_requests_per_s / 2 + 1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000)) × 2628000` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `((insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × 2 + cas_table_drops_per_s) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `(insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × 2 × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `((insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × 3 + (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × 2) × 2628000` |
| `request` | `PutObject` | `ok` | `variant` |  | `((insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × 1 + cas_table_drops_per_s) × 2628000` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `(if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table > 0 then 1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) + (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) / 1000 else 0) × 2628000` |

| State | Condition | Reason |
|---|---|---|
| `backlog_grows` | `(if gc_round_graduation_budget > 0 then if (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) > gc_round_graduation_budget then 1 else 0 else 0) + (if gc_round_redelete_budget > 0 then if (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) > gc_round_redelete_budget then 1 else 0 else 0)` | retired blobs per GC round exceed the per-round graduation or redelete budget; the garbage backlog grows and steady-state garbage is unavailable |

Notes:

- `1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000)`
- `(insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction)`
- `cas_tables × cas_ref_log_tail_records`
- `(insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation + cas_table_drops_per_s × cas_parts_per_dropped_table) × cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000)`
- `GC fold: tables with new data per round = min(tables, inserts per GC interval), assuming each insert of a round lands in a different table until every table has one`
- `GC fold: the orphan-manifest sweep reads cas_ref_log_tail_records per swept table, min(tables, gc_round_sweep_namespace_budget) tables per round`
- `GC: the paged cas/ns/stream/ listing of Deferred rounds is not modeled (up to 51 pages at 150-500 tables)`

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `merge_effective_fan_in` | `6.1437` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-lifecycle | 0.2.0 | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-lifecycle |

### `cas` / `lease` {#formula-cas-lease}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `lease_requests_per_s × replicas × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `cas_tables × replicas × cas_cleanup_gets_per_table / cas_table_cleanup_period_s × 2628000` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `cas_tables × replicas / cas_table_cleanup_period_s × 2628000` |

Notes:

- `Cleanup: each run reads 5 GETs for a table with parts (measured on the gc-tables passes, not fitted); every table on the disk is assumed to hold parts`

### `cas` / card cell `probe_cas_expect` {#formula-cas-card-probe-cas-expect}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `probe_put_requests` |
| `request` | `HeadObject` | `ok` | `variant` |  | `probe_head_requests` |
| `request` | `HeadObject` | `client_error` | `variant` |  | `probe_head_error_requests` |
| `request` | `GetObject` | `ok` | `variant` |  | `probe_get_requests` |
| `request` | `GetObject` | `client_error` | `variant` |  | `probe_get_error_requests` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `probe_list_requests` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `probe_delete_requests` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `probe_batch_delete_requests` |
| `storage` | `active` | `all` | `variant` |  | `probe_active_gib_months` |
| `storage` | `metadata` | `all` | `variant` |  | `probe_metadata_gib_months` |

### `cas` / card cell `probe_cas_gc_cohort` {#formula-cas-card-probe-cas-gc-cohort}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `probe_put_requests` |
| `request` | `HeadObject` | `ok` | `variant` |  | `probe_head_requests` |
| `request` | `HeadObject` | `client_error` | `variant` |  | `probe_head_error_requests` |
| `request` | `GetObject` | `ok` | `variant` |  | `probe_get_requests` |
| `request` | `GetObject` | `client_error` | `variant` |  | `probe_get_error_requests` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `probe_list_requests` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `probe_delete_requests` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `probe_batch_delete_requests` |
| `storage` | `active` | `all` | `variant` |  | `probe_active_gib_months` |
| `storage` | `garbage` | `all` | `variant` |  | `probe_garbage_gib_months` |
| `storage` | `metadata` | `all` | `variant` |  | `probe_metadata_gib_months` |

### `ebs` / card cell `probe_local_volume` {#formula-ebs-card-probe-local-volume}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `capacity` | `all` | `volume` |  | `probe_block_capacity_gib` |
| `block_io` | `iops` | `all` | `volume` |  | `probe_block_iops` |
| `block_io` | `mibps` | `all` | `volume` |  | `probe_block_mibps` |

### `s3` / card cell `probe_multipart_fault` {#formula-s3-card-probe-multipart-fault}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `1` |
| `request` | `UploadPart` | `ok` | `variant` |  | `2` |
| `request` | `UploadPart` | `accepted_dropped` | `variant` |  | `1` |
| `request` | `CompleteMultipartUpload` | `not_sent` | `variant` |  | `4` |
| `request` | `AbortMultipartUpload` | `not_sent` | `variant` |  | `4` |
| `request` | `PutObject` | `ok` | `variant` |  | `probe_put_requests` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `probe_delete_requests` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `probe_batch_delete_requests` |
| `storage` | `incomplete` | `all` | `variant` |  | `probe_incomplete_gib_months` |
| `storage` | `active` | `all` | `variant` |  | `probe_active_gib_months` |

### `s3` / card cell `probe_replicated_fetch.inserts` {#formula-s3-card-probe-replicated-fetch-inserts}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` | `r1` | `11` |
| `storage` | `active` | `all` | `variant` |  | `probe_r1_active_gib_months` |

### `s3` / card cell `probe_replicated_fetch.recovery` {#formula-s3-card-probe-replicated-fetch-recovery}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` | `r1` | `11` |
| `request` | `PutObject` | `ok` | `variant` | `r2` | `11` |
| `request` | `GetObject` | `ok` | `variant` | `r2` | `probe_r2_get_requests` |
| `storage` | `active` | `all` | `variant` |  | `probe_r2_active_gib_months` |
| `transfer` | `interserver_from_target` | `all` | `variant` |  | `probe_interserver_gib` |
| `transfer` | `interserver_to_target` | `all` | `variant` |  | `probe_interserver_to_gib` |

### `s3` / card cell `probe_s3_insert` {#formula-s3-card-probe-s3-insert}

Owner: `2026-10-24-storage-cost-phase0`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` | `r1` | `11` |
| `storage` | `active` | `all` | `variant` |  | `probe_active_gib_months` |
<!-- formulas:end -->

## Inputs {#inputs}

<!-- inputs:begin -->
Registry version `0.3.0`. Generated; do not edit.

| Id | Block | Label | Kind | Scope | Unit | Default | Validity | Rule | Evidence | Status |
|---|---|---|---|---|---|---|---|---|---|---|
| `data_bytes` | `simple` | Data size | `input` | `cluster` | `GiB` | `20480` |  |  | assumed: preset default (0.1.0) | `ok` |
| `ingest_rate` | `simple` | Ingest rate | `input` | `cluster` | `GiB/day` | `500` |  |  | assumed: preset default (0.1.0) | `ok` |
| `retention_days` | `simple` | Retention | `input` | `cluster` | `day` | `40` |  |  | assumed: preset default (0.1.0) | `ok` |
| `replicas` | `simple` | Replicas | `input` | `cluster` | `count` | `3` | `{"replicas": [1, 6]}` |  | assumed: preset default (0.1.0) | `ok` |
| `shards` | `simple` | Shards: one; multi-shard is out of scope | `pinned` | `cluster` | `count` | `1` | `{"shards": [1, 1]}` |  | assumed: the page models one shard of at most 200 TiB (0.3.0) | `ok` |
| `topology` | `simple` | Replica placement: same_az (replicas of a shard in one availability zone; interserver traffic not charged) or cross_az (each fetched part crosses zones; charged per GB in each direction) | `input` | `cluster` |  | `"same_az"` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cache_enabled` | `reads_cache` | Filesystem cache enabled | `input` | `replica` |  | `"1"` |  |  | assumed: preset default (0.1.0) | `ok` |
| `shape` | `schema` | Table preset: narrow = 5 UInt64 columns, wide = 40, types = the 21-column typed table of the s3 report | `input` | `cluster` |  | `"narrow"` |  |  | assumed: preset default (0.1.0) | `ok` |
| `columns` | `schema` | Columns of the table: a Wide part writes files per column stream, statistics are read once per column | `input` | `cluster` | `count` | `40` | `{"columns": [5, 40]}` |  | assumed: standard deployment profile (0.4.0) | `ok` |
| `streams_per_column` | `schema` | Serialization streams per column, on average: 1 for numbers, 2 for String, Nullable, Array and LowCardinality, 4 for Map, more for JSON | `input` | `cluster` | `count` | `1` | `{"streams_per_column": [1, 3.29]}` |  | assumed: standard deployment profile (0.4.0) | `ok` |
| `skip_index_files_per_part` | `schema` | Skip-index files per part: 1 when every skip index fits packed_skip_index_max_bytes (packed into skp_idx.packed), plus 2 per larger index | `input` | `cluster` | `count` | `0` | `{"skip_index_files_per_part": [0, 1]}` |  | assumed: standard deployment profile (0.4.0) | `ok` |
| `projections` | `schema` | Projections of the table: each is a Compact part of its own inside every part | `input` | `cluster` | `count` | `0` | `{"projections": [0, 1]}` |  | assumed: standard deployment profile (0.4.0) | `ok` |
| `empty_files_per_wide_part` | `schema` | Empty files of a Wide part (JSON streams with no data): s3 stores no object for them, s3_plain_rewritable stores one | `input` | `cluster` | `count` | `0` | `{"empty_files_per_wide_part": [0, 12]}` |  | assumed: standard deployment profile (0.4.0) | `ok` |
| `wide_part_streams` | `schema` | Serialization streams of a Wide part: columns x streams_per_column | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `columns × streams_per_column` | source: src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:140 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `objects_per_compact_part` | `schema` | Objects of a Compact part: 9 metadata files, data.bin and its marks, skip-index files, 10 per projection | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `part_metadata_files + compact_part_data_files + skip_index_files_per_part + projections × projection_part_objects` | source: src/Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp:53 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `objects_per_wide_part` | `schema` | Objects of a Wide part on s3: 9 metadata files, a .bin and a marks file per stream, skip-index files, 10 per projection | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `part_metadata_files + files_per_wide_stream × wide_part_streams + skip_index_files_per_part + projections × projection_part_objects` | source: src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:140 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `merge_result_objects_per_part` | `schema` | Objects of a merge result part on s3: an inserted part's count when the result has the inserted part type, else the count of the result's type | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `if merge_result_part_type == compact then if part_type == compact then objects_per_part else objects_per_compact_part else if part_type == wide then objects_per_part else objects_per_wide_part` | source: src/Storages/MergeTree/MergeTask.cpp:586 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_copy_parts` | `inserts` | UploadPartCopy requests to copy one file past the first upload buffer: object_bytes / s3_min_upload_part_size, rounded up | `derivation` | `cluster` | `count` |  |  | `ceil_to(object_bytes / multipart_next_part_bytes, 1)` | source: src/IO/S3/copyS3File.cpp:347 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_objects_per_wide_part` | `schema` | Objects of a Wide part on s3_plain_rewritable: the s3 objects plus one per empty file | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `objects_per_wide_part + empty_files_per_wide_part` | source: src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `part_metadata_files` | `schema` | Metadata files of every part: primary.cidx, count.txt, checksums.txt and 6 more | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/MergedBlockOutputStream.cpp:407 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `compact_part_data_files` | `schema` | Data files of a Compact part: data.bin and its marks | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp:53 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `files_per_wide_stream` | `schema` | Files per serialization stream of a Wide part: a .bin and a marks file | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:140 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `projection_part_objects` | `schema` | Objects of one projection part (Compact) inside a part | `measured` | `cluster` | `count` |  |  |  | source: docs/superpowers/reports/2026-10-24-storage-cost-s3/README.md:70 (26.8@09d0772a842) | `ok` |
| `part_load_files` | `schema` | Small files read per part when a server loads it | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/IMergeTreeDataPart.cpp:1504 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `merge_compact_source_extra_gets` | `schema` | GETs per Compact merge source beyond one seek per column | `measured` | `cluster` | `count` |  |  |  | source: docs/superpowers/reports/2026-10-24-storage-cost-s3/README.md:94 (26.8@09d0772a842) | `ok` |
| `merge_cached_compact_source_gets` | `schema` | GETs per Compact merge source through the cache | `measured` | `cluster` | `count` |  |  |  | source: docs/superpowers/reports/2026-10-24-storage-cost-s3/README.md:94 (26.8@09d0772a842) | `ok` |
| `merge_wide_source_extra_gets` | `schema` | GETs per Wide merge source beyond the per-stream reads | `measured` | `cluster` | `count` |  |  |  | source: docs/superpowers/reports/2026-10-24-storage-cost-s3/README.md:96 (26.8@09d0772a842) | `ok` |
| `mutation_rewritten_metadata_files` | `schema` | Metadata files a mutation rewrites in every Wide part | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/MutateTask.cpp:1162 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `type_mix` | `schema` | Column type mix | `input` | `cluster` |  | `"plain"` |  |  | assumed: preset default (0.1.0) | `ok` |
| `probe_incomplete_gib_months` | `reliability` | Probe: incomplete multipart storage | `measured` | `pool` | `GiB*month` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_garbage_gib_months` | `cas` | Probe: CAS garbage awaiting reclamation | `measured` | `pool` | `GiB*month` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_active_gib_months` | `cas` | Probe: active CAS objects | `measured` | `pool` | `GiB*month` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_block_capacity_gib` | `ebs` | Probe: local volume capacity peak | `measured` | `volume` | `GiB` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_block_iops` | `ebs` | Probe: local volume IOPS demand | `measured` | `volume` | `iops` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_block_mibps` | `ebs` | Probe: local volume throughput demand | `measured` | `volume` | `mibps` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_put_requests` | `reliability` | Probe: PutObject requests the store answered ok (reply delivered) | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_get_requests` | `reliability` | Probe: GetObject requests the store answered ok (reply delivered) | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_head_requests` | `reliability` | Probe: HeadObject requests the store answered ok (reply delivered) | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_list_requests` | `reliability` | Probe: ListObjectsV2 requests the store answered ok (reply delivered) | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_delete_requests` | `reliability` | Probe: DeleteObject requests the store answered ok (reply delivered) | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_batch_delete_requests` | `reliability` | Probe: DeleteObjects requests the store answered ok (reply delivered) | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_r2_get_requests` | `reliability` | Probe: fetch receiver GetObject requests the store answered ok | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_interserver_gib` | `reliability` | Probe: interserver bytes the replicas' interserver servers sent (relay from_target) | `measured` | `cluster` | `GiB` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_head_error_requests` | `reliability` | Probe: HeadObject requests answered with a 4XX | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_get_error_requests` | `reliability` | Probe: GetObject requests answered with a 4XX | `measured` | `pool` | `count` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_metadata_gib_months` | `cas` | Probe: metadata objects (CAS control state) | `measured` | `pool` | `GiB*month` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_r1_active_gib_months` | `reliability` | Probe: active objects replica 1 created | `measured` | `pool` | `GiB*month` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_r2_active_gib_months` | `reliability` | Probe: active objects replica 2 created | `measured` | `pool` | `GiB*month` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `probe_interserver_to_gib` | `reliability` | Probe: interserver bytes the replicas' interserver servers received (relay to_target) | `measured` | `cluster` | `GiB` |  |  |  | measured: phase-0 probe observation: docs/superpowers/reports/2026-10-24-storage-cost-phase0 (0.1.0) | `ok` |
| `min_bytes_for_wide_part` | `schema` | Minimum uncompressed bytes for a Wide part | `pinned` | `cluster` | `B` | `10485760` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:78 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `min_rows_for_wide_part` | `schema` | Minimum rows for a Wide part | `pinned` | `cluster` | `count` | `0` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:85 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `min_level_for_wide_part` | `schema` | Minimum merge level for a Wide part | `pinned` | `cluster` | `count` | `0` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:82 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `part_type_predicate` | `schema` | Part type rule: a part is Compact when its uncompressed bytes &lt; min_bytes_for_wide_part, its rows &lt; min_rows_for_wide_part or its level &lt; min_level_for_wide_part, else Wide. An insert measures the block in memory (Block::bytes); a merge sums the inputs' on-disk uncompressed column bytes, uses level max(input levels) + 1, and is Wide when any input is Wide. A mutation keeps the source type. Overrides: no adaptive granularity, or a Quantize('pq') column, forces Wide/Full. | `pinned` | `cluster` |  | `"compact_if_any_below_minimum"` |  |  | source: src/Storages/MergeTree/MergeTreeData.cpp:6399 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `part_level` | `schema` | Level of the part (0 for an inserted part) | `input` | `cluster` | `count` | `0` |  |  | source: src/Storages/MergeTree/MergeTreeDataWriter.cpp:735 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `part_bytes_uncompressed` | `schema` | Uncompressed bytes of one inserted part | `derivation` | `cluster` | `B` |  |  | `insert_bytes_uncompressed / parts_per_insert` | source: src/Storages/MergeTree/MergeTreeDataWriter.cpp:848 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `part_rows` | `schema` | Rows of one inserted part | `derivation` | `cluster` | `count` |  |  | `insert_rows / parts_per_insert` | source: src/Storages/MergeTree/MergeTreeDataWriter.cpp:915 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `adaptive_granularity` | `schema` | The table can write adaptive-granularity parts (index_granularity_bytes > 0, and mixed granularity allowed or no old non-adaptive part); without it every part is Wide | `input` | `cluster` |  | `"1"` |  |  | source: src/Storages/MergeTree/MergeTreeData.cpp:11680 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `quantize_pq_codec` | `schema` | A column of the part uses CODEC(Quantize('pq', ...)): the part is forced Wide/Full | `input` | `cluster` |  | `"0"` |  |  | source: src/Storages/MergeTree/MergeTreeData.cpp:6428 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `part_type` | `schema` | Part type of an inserted part: Wide without adaptive granularity or with a Quantize('pq') column, else the size, rows and level predicate | `derivation` | `cluster` |  |  |  | `if adaptive_granularity == 0 then wide else if quantize_pq_codec == 1 then wide else if part_bytes_uncompressed < min_bytes_for_wide_part then compact else if part_rows < min_rows_for_wide_part then compact else if part_level < min_level_for_wide_part then compact else wide` | source: src/Storages/MergeTree/MergeTreeData.cpp:6402 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_first_part_bytes` | `inserts` | First upload buffer: an object up to this size is one PutObject, a larger one starts a multipart upload with a first part of this size | `pinned` | `cluster` | `B` | `33554432` |  |  | source: src/Common/BufferAllocationPolicy.cpp:51 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_next_part_bytes` | `inserts` | Size of the second and later upload parts | `pinned` | `cluster` | `B` | `16777216` |  |  | source: src/Common/BufferAllocationPolicy.cpp:52 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_growth_parts` | `inserts` | Upload parts between part-size growth steps | `pinned` | `cluster` | `count` | `500` |  |  | source: src/IO/S3Defines.h:30 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_growth_factor` | `inserts` | Part-size growth factor every multipart_growth_parts parts | `pinned` | `cluster` | `1` | `2` |  |  | source: src/IO/S3Defines.h:29 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `strict_upload_part_size` | `inserts` | Fixed upload part size (0 = off): when set, every buffer is this size | `pinned` | `cluster` | `B` | `0` |  |  | source: src/Common/BufferAllocationPolicy.cpp:98 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `max_single_part_upload_size` | `inserts` | Largest object one buffer may send as a single PutObject | `pinned` | `cluster` | `B` | `33554432` |  |  | source: src/IO/S3Defines.h:26 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_sizing_rule` | `inserts` | Upload sizing: with strict_upload_part_size = 0 WriteBufferFromS3 fills a first buffer of clamp(max_single_part_upload_size, min_upload_part_size, max_upload_part_size), then buffers of min_upload_part_size that grow by multipart_growth_factor every multipart_growth_parts buffers (up to 5 GiB); with a strict size every buffer is that size. A second filled buffer starts the multipart upload. At finalize one buffer of at most max_single_part_upload_size is one PutObject (an object of exactly 32 MiB by default); one larger buffer, possible only when the first buffer or the strict size exceeds max_single_part_upload_size, is a multipart upload of one part. The last part holds the remainder. | `pinned` | `cluster` |  | `"exp_buffers_single_put_up_to_first"` |  |  | source: src/IO/WriteBufferFromS3.cpp:180 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `object_bytes` | `inserts` | Size of one object written to S3 (one part file) | `input` | `cluster` | `B` | `1048576` |  |  | assumed: preset default (0.2.0) | `ok` |
| `multipart_parts` | `inserts` | Data-carrying upload requests for one object: with a strict size ceil(size / strict); else 1 up to the first buffer and 1 + ceil((size - first) / next) beyond it (no growth step: valid below 499 later parts) | `derivation` | `cluster` | `count` |  | `{"object_bytes": [0, 8405385216]}` | `if strict_upload_part_size > 0 then if object_bytes ≤ strict_upload_part_size then 1 else ceil_to(object_bytes / strict_upload_part_size, 1) else if object_bytes ≤ multipart_first_part_bytes then 1 else 1 + ceil_to((object_bytes − multipart_first_part_bytes) / multipart_next_part_bytes, 1)` | source: src/Common/BufferAllocationPolicy.cpp:72 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `multipart_upload` | `inserts` | The object goes up as a multipart upload (1) or one PutObject (0): multipart when it exceeds the first buffer (the strict size, else the clamped first buffer) or max_single_part_upload_size | `derivation` | `cluster` | `count` |  |  | `if object_bytes > min(if strict_upload_part_size > 0 then strict_upload_part_size else multipart_first_part_bytes, max_single_part_upload_size) then 1 else 0` | source: src/IO/WriteBufferFromS3.cpp:180 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `min_insert_block_size_rows` | `inserts` | Insert squashing emits a block once it holds this many rows | `pinned` | `cluster` | `count` | `1048449` |  |  | source: src/Core/Settings.cpp:209 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `min_insert_block_size_bytes` | `inserts` | Insert squashing emits a block once it holds this many bytes | `pinned` | `cluster` | `B` | `268402944` |  |  | source: src/Core/Settings.cpp:232 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `use_strict_insert_block_limits` | `inserts` | Strict insert block limits (0 = off): when on, max limits split blocks and the parts formula does not apply | `pinned` | `cluster` | `count` | `0` |  |  | source: src/Core/Settings.cpp:162 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `max_block_size` | `inserts` | Rows per block a SELECT reads, so per source block of an INSERT SELECT (optimize_trivial_insert_select is off) | `pinned` | `cluster` | `count` | `65409` |  |  | source: src/Core/Settings.cpp:155 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `max_insert_block_size` | `inserts` | Rows per block the server parses formatted INSERT data into | `pinned` | `cluster` | `count` | `1048449` |  |  | source: src/Core/Settings.cpp:178 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `insert_kind` | `inserts` | INSERT with data (inline, formatted or native blocks) or INSERT SELECT; only INSERTs with data can go asynchronous | `input` | `cluster` |  | `"data"` |  |  | source: src/Interpreters/executeQuery.cpp:2894 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `insert_data_block_rows` | `inserts` | Rows per block an INSERT with data delivers: formatted input is parsed into max_insert_block_size rows; a native client sends its own blocks | `input` | `cluster` | `count` | `1048449` |  |  | source: src/Core/Settings.cpp:178 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `insert_source_block_rows` | `inserts` | Rows per block entering the insert squash: max_block_size for INSERT SELECT, insert_data_block_rows for data | `derivation` | `cluster` | `count` |  |  | `if insert_kind == select then max_block_size else insert_data_block_rows` | source: src/Core/Settings.cpp:5609 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `insert_protocol` | `inserts` | How an INSERT with data arrives: native protocol blocks or inline (HTTP body, query text); the async queue measures native data as decoded block bytes and inline data as raw bytes | `input` | `cluster` |  | `"native"` |  |  | source: src/Server/TCPHandler.cpp:1414 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `insert_payload_bytes` | `inserts` | Raw bytes of an inline INSERT's data (the async buffer counts these for inline inserts, not table bytes) | `input` | `cluster` | `B` | `1048576` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_clients` | `inserts` | Clients inserting concurrently; with wait_for_async_insert = 1 each has at most one insert in a buffer | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_rows` | `inserts` | Rows per INSERT | `input` | `cluster` | `count` | `100000` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_bytes_uncompressed` | `inserts` | Uncompressed bytes per INSERT | `input` | `cluster` | `B` | `10485760` |  |  | assumed: preset default (0.2.0) | `ok` |
| `partitions_per_insert` | `inserts` | Partitions one INSERT touches | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_rate` | `inserts` | INSERT queries per second, all clients together | `input` | `cluster` | `1/s` | `1` |  |  | assumed: preset default (0.2.0) | `ok` |
| `async_insert` | `inserts` | Async inserts on: the query setting async_insert (on by default in this build) or the table's async_insert setting | `input` | `cluster` |  | `"1"` |  |  | source: src/Core/Settings.cpp:7225 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `wait_for_async_insert` | `inserts` | An async INSERT waits for its flush (1 = on) | `pinned` | `cluster` | `count` | `1` |  |  | source: src/Core/Settings.cpp:7228 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_deduplicate` | `inserts` | Legacy async insert deduplication (0 = off); consulted only when deduplicate_insert = backward_compatible_choice | `pinned` | `cluster` | `count` | `0` |  |  | source: src/Core/Settings.cpp:2393 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `deduplicate_insert` | `inserts` | Insert deduplication mode; supersedes async_insert_deduplicate. When deduplication is on, async_insert_max_query_number inserts flush a buffer | `input` | `cluster` |  | `"enable"` |  |  | source: src/Core/Settings.cpp:2357 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `insert_deduplicate` | `inserts` | Legacy synchronous insert deduplication (1 = on); with deduplicate_insert = backward_compatible_choice it also decides for an INSERT made async by the table setting alone | `pinned` | `cluster` | `count` | `1` |  |  | source: src/Core/Settings.cpp:2379 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_source` | `inserts` | What makes an INSERT async: the query setting async_insert, or only the table's async_insert setting (the query setting 0) | `input` | `cluster` |  | `"query"` |  |  | source: src/Server/TCPHandler.cpp:1497 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_dedup_effective` | `inserts` | Deduplication on for async inserts (1) or off (0) | `derivation` | `cluster` | `count` |  |  | `if deduplicate_insert == enable then 1 else if deduplicate_insert == disable then 0 else if async_insert_source == table then insert_deduplicate else async_insert_deduplicate` | source: src/Core/DeduplicateInsert.cpp:26 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_max_query_number` | `inserts` | Inserts that flush a buffer when deduplication is on | `pinned` | `cluster` | `count` | `450` |  |  | source: src/Core/Settings.cpp:7239 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_busy_timeout_max_ms` | `inserts` | Longest wait before an async insert buffer is flushed | `pinned` | `cluster` | `ms` | `200` |  |  | source: src/Core/Settings.cpp:7252 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_busy_timeout_min_ms` | `inserts` | Shortest adaptive wait before an async insert buffer is flushed | `pinned` | `cluster` | `ms` | `50` |  |  | source: src/Core/Settings.cpp:7249 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_max_data_size` | `inserts` | Unparsed bytes that flush an async insert buffer; an INSERT with more data runs synchronously | `pinned` | `cluster` | `B` | `10485760` |  |  | source: src/Core/Settings.cpp:7234 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `async_insert_flush_rule` | `inserts` | Async flush: an INSERT with data of at most async_insert_max_data_size bytes is queued (INSERT SELECT and larger data run synchronously); inserts with the same user, roles, query shape, settings and data kind share a buffer; it is flushed into one part per partition when its unparsed bytes reach async_insert_max_data_size, when async_insert_max_query_number inserts accumulate with deduplication on, or when its deadline passes. The deadline is the first insert time plus an adaptive timeout between async_insert_busy_timeout_min_ms and _max_ms that grows while inserts are frequent. With wait_for_async_insert = 1 a client waits for the flush, so only concurrent inserts share one. | `pinned` | `cluster` |  | `"size_count_or_deadline"` |  |  | source: src/Interpreters/AsynchronousInsertQueue.cpp:680 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `inserts_per_async_flush` | `inserts` | Async inserts sharing one flush, an estimate for evenly spaced arrivals: the first insert plus those arriving within the longest busy timeout, at most ceil(buffer / entry bytes) entries (decoded bytes for native, raw for inline), at most one per waiting client, at most async_insert_max_query_number with deduplication on (the default). Bursts share more, a shorter adaptive timeout less. | `derivation` | `cluster` | `1` |  |  | `max(1, min(1 + insert_rate × async_insert_busy_timeout_max_ms / 1000, ceil_to(async_insert_max_data_size / (if insert_protocol == native then insert_bytes_uncompressed else insert_payload_bytes), 1), if wait_for_async_insert == 1 then insert_clients else 1000000000000, if async_insert_dedup_effective == 1 then async_insert_max_query_number else 1000000000000))` | assumed: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle#parts-per-insert (0.2.0) | `ok` |
| `parts_per_insert` | `inserts` | Parts one INSERT creates, per partition: synchronously one per squashed block (whole source blocks joined until either minimum is reached; strict limits off); an async INSERT with data below the size limit (decoded bytes &lt; limit for native, raw bytes &lt;= limit for inline), a share of a flush of inserts_per_async_flush inserts. INSERT SELECT is always synchronous. Not covered: a native async INSERT over the limit, which the server forwards as one combined block of the first limit's worth before the rest. | `derivation` | `cluster` | `1` |  |  | `partitions_per_insert × (if insert_kind == select then max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)) else if async_insert == 1 then if insert_protocol == native then if insert_bytes_uncompressed < async_insert_max_data_size then 1 / inserts_per_async_flush else max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)) else if insert_payload_bytes ≤ async_insert_max_data_size then 1 / inserts_per_async_flush else max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)) else max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)))` | source: src/Interpreters/Squashing.cpp:395 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `old_parts_lifetime` | `merges` | Minimum age before an outdated part may be removed; removal also waits for references and transactions and runs on the cleanup schedule | `pinned` | `cluster` | `s` | `480` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:556 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `max_bytes_to_merge_at_max_space_in_pool` | `merges` | Largest merge result the selector plans with free space | `pinned` | `cluster` | `B` | `161061273600` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:474 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `merge_selector_base` | `merges` | SimpleMergeSelector base (write amplification vs part count) | `pinned` | `cluster` | `1` | `5` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:885 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `max_parts_to_merge_at_once` | `merges` | Most parts one simple-selector merge takes; TTL drop merges read it too, a whole-partition force merge (min_age_to_force_merge_seconds) does not | `pinned` | `cluster` | `count` | `100` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:636 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `merge_size_ratio` | `merges` | Size of the part an inserted row ends up in, in inserted parts, row-weighted over the final parts: sum(r^2) / sum(r) | `input` | `cluster` | `1` | `64` |  |  | assumed: preset default (0.2.0) | `ok` |
| `merge_effective_fan_in` | `merges` | Effective merge fan-in of the write amplification rule | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `write_amplification_rule` | `merges` | Write amplification: bytes written by inserts and merges per inserted byte: 1 + rewrite levels at merge_size_ratio with effective fan-in merge_effective_fan_in (levels capped at 12) | `derivation` | `cluster` | `1` |  |  | `1 + (if merge_size_ratio < 1 then 0 else if merge_size_ratio < merge_effective_fan_in then (merge_size_ratio − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in then 1 + (merge_size_ratio / merge_effective_fan_in − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 2 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 3 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 4 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 5 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 6 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 7 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 8 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 9 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 10 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 11 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else 12)` | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `merge_fan_in_bucket` | `merges` | Fan-in bucket of a merge | `input` | `cluster` |  | `"5"` |  |  | assumed: bucket key of merge_fan_in_distribution (0.2.0) | `ok` |
| `merge_fan_in_distribution` | `merges` | Fraction of natural merges per fan-in bucket | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `merge_result_size_bucket` | `merges` | Merge result size bucket, in inserted parts | `input` | `cluster` |  | `"5-8"` |  |  | assumed: bucket key of merge_result_size_distribution (0.2.0) | `ok` |
| `merge_result_size_distribution` | `merges` | Fraction of natural merges per result size bucket (result rows / inserted part rows) | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `acquisition_rule` | `replication` | Fetch vs local execution of a replicated merge, in order: skip when the part or a covering one is already here; fetch when always_fetch_merged_part; try to fetch a TTL-recompress merge another replica was assigned, quietly retrying while no replica has it, until try_fetch_recompressed_part_timeout; fetch when execute_merges_on_single_replica_time_threshold > 0, the entry is younger than it and another replica is picked; fetch when a source or patch part is missing or covered; while a source part has newer metadata, try to fetch, quietly retrying; fetch when the entry is at least prefer_fetch_merged_part_time_threshold old, the sources hold at least prefer_fetch_merged_part_size_threshold bytes on disk and a replica has the result; with zero-copy, fetch or wait on the covering part and the exclusive lock; else merge locally. A mutation: fetch when always_fetch_mutated_part, the source is missing or covered, or the entry is that old and 1.1 x the source bytes on disk (expired TTL parts excluded) reach the size threshold with a replica holding the result; the single-replica branch never fires (merge entries only); zero-copy as for merges; else mutate locally, keeping the source part type. | `pinned` | `cluster` |  | `"local_unless_fetch_condition"` |  |  | source: src/Storages/MergeTree/MergeFromLogEntryTask.cpp:224 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `prefer_fetch_merged_part_time_threshold` | `replication` | Age of a merge or mutation entry from which (inclusive) a replica prefers fetching a large result | `pinned` | `cluster` | `s` | `3600` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:1284 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `prefer_fetch_merged_part_size_threshold` | `replication` | Source bytes on disk above which an old merge entry is fetched | `pinned` | `cluster` | `B` | `10737418240` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:1294 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `execute_merges_on_single_replica_time_threshold` | `replication` | Seconds the other replicas wait for the chosen replica's merge result (0 = off) | `pinned` | `cluster` | `s` | `0` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:1304 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `always_fetch_merged_part` | `replication` | Replica never merges and fetches every merge result (0 = off) | `pinned` | `cluster` | `count` | `0` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:1341 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `always_fetch_mutated_part` | `replication` | Replica never mutates and fetches every mutation result (0 = off) | `pinned` | `cluster` | `count` | `0` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:1348 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_key_families` | `cas` | CAS key families: blob, blob_meta, manifest, ref_stream, ref_ckpt, ns_files, ref_catalog (foreground state); lease, gc_hb, gc, pool_meta, roots (maintenance only) | `pinned` | `pool` |  | `"cas_layout_26_8"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h:397 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_gc_interval_s` | `cas` | GC round interval (cas_gc_interval_sec) | `input` | `pool` | `s` | `60` | `{"cas_gc_interval_s": [10, 60]}` |  | assumed: default src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:58 @ 09d0772a842 (0.3.0) | `ok` |
| `gc_scheduler_rule` | `cas` | GC scheduling: a round starts cas_gc_interval_sec after the previous round finished; only the GC leader pulses the heartbeat, every interval / 4 and once per round on acquiring the lease | `pinned` | `pool` |  | `"wait_interval_after_round"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.cpp:349 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `gc_heartbeat_requests_per_s` | `cas` | GC heartbeat requests per second (GET and PUT of gc/hb per pulse; per-round pulses add 2 per round) | `derivation` | `pool` | `1/s` |  |  | `2 / (cas_gc_interval_s / 4)` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.cpp:81 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `gc_round_redelete_budget` | `cas` | Blob deletes a GC round may issue (cas_gc_round_redelete_budget; 0 = unbounded) | `pinned` | `pool` | `count` | `5000` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:66 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `staging_rule` | `cas` | Staging: staging_backend = local (default) writes every blob file of a part to the scratch directory while hashing; the files are uploaded at commit and removed after it | `pinned` | `pool` |  | `"local_scratch_until_commit"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:984 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `staging_peak_bytes_rule` | `cas` | Staging peak: the blob file bytes of the parts committing at the same time (each insert or merge stages its whole part) | `pinned` | `pool` |  | `"sum_of_committing_part_blob_bytes"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:593 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_branch_requests` | `cas` | Requests per blob file by entry branch: fresh HEAD + PUT body + PUT .meta; hash hit HEAD + GET .meta; adoption and inline none | `pinned` | `pool` |  | `"head_then_publish_or_observe"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp:397 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_publish_requests` | `cas` | Requests to publish a part: manifest PUT and GET, then precommit and promote ref transactions (catalog GET, _log PUT, _ckpt GET, _ckpt PUT each) | `pinned` | `pool` | `count` | `10` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp:646 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_ref_requests_per_rename` | `cas` | Requests of the rename ref transaction of a new part (catalog GET, _log PUT, _ckpt GET, _ckpt PUT) | `pinned` | `pool` | `count` | `4` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp:3320 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_foreground_ref_removal_requests` | `cas` | Requests of DROP TABLE SYNC: per part up to one ref transaction (4 requests); per table the terminal transaction, a catalog PUT, 3 table-file requests and 2 LISTs (9 more) | `pinned` | `pool` | `count` | `9` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp:3428 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_hash_hit_fraction` | `cas` | Fraction of a part's blob files whose bytes the pool already holds (hash hits) | `input` | `pool` | `1` | `0` | `{"cas_hash_hit_fraction": [0, 1]}` |  | assumed: preset default: no repeated content (0.3.0) | `ok` |
| `cas_blob_bytes` | `cas` | Mean bytes of a blob file | `input` | `pool` | `B` | `1048576` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_mutations_per_s` | `cas` | Mutations per second: ALTER UPDATE/DELETE per month / 2 628 000 s | `derivation` | `cluster` | `1/s` |  |  | `mutations_per_month / 2628000` | assumed: a month is 730 h (2 628 000 s), the month of every per-month input (0.3.0) | `ok` |
| `cas_parts_per_mutation` | `cas` | Parts one mutation rewrites: parts mutated per month / mutations per month (0 without mutations) | `derivation` | `cluster` | `count` |  |  | `if mutations_per_month == 0 then 0 else (mutated_compact_parts_per_month + mutated_wide_parts_per_month) / mutations_per_month` | assumed: parts mutated per month spread evenly over the month's mutations (0.3.0) | `ok` |
| `cas_mutated_columns` | `cas` | Columns one mutation rewrites: every column for ALTER DELETE, one for UPDATE of one column and lightweight DELETE | `derivation` | `cluster` | `count` |  |  | `if mutation_kind == delete then columns else 1` | source: src/Storages/MergeTree/MutateTask.cpp:1247 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_table_drops_per_s` | `cas` | DROP TABLE per second: tables dropped per month / 2 628 000 s | `derivation` | `cluster` | `1/s` |  |  | `cas_table_drops_per_month / 2628000` | assumed: a month is 730 h (2 628 000 s), the month of every per-month input (0.3.0) | `ok` |
| `cas_table_create_requests` | `cas` | Requests of one CREATE TABLE on a CAS disk: GET 9, GET 404 2, HEAD 404 1, PUT 4, LIST 5 (21); catalog birth, first _ckpt, ref-log recovery, format_version.txt, table-start listings | `derivation` | `replica` | `count` |  |  | `9 + 2 + 1 + 4 + 5` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp:1297 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_parts_per_dropped_table` | `cas` | Active parts of a dropped table | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_mount_renew_period_s` | `cas` | Mount lease renewal period (cas_mount_renew_period_ms / 1000) | `pinned` | `pool` | `s` | `10` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:74 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_pool_objects` | `cas` | Objects in the CAS pool (blobs, .meta siblings, manifests) | `input` | `pool` | `count` | `5000` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_write` | `cas` | Kind of part write (key of cas_branch_population) | `input` | `pool` |  | `"insert_compact"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_branch` | `cas` | Entry branch of a part file | `input` | `pool` |  | `"fresh"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_marks_blob_fraction` | `cas` | Share of a Wide part's mark files that are distinct blobs; equal mark files (streams with equal compressed block sizes) are one upload | `input` | `cluster` | `1` | `1` | `{"cas_marks_blob_fraction": [0.033333, 1]}` |  | assumed: docs/superpowers/reports/2026-10-24-storage-cost-validation: generator tables 0.6, 0.033, 0.104; distinct marks reach 1 (0.4.0) | `ok` |
| `gc_round_term` | `cas` | Term of the GC round request and duration tables | `input` | `pool` |  | `"deferred_round"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_mutation_marks_hit_fraction` | `cas` | Fraction of a mutated column's marks files whose bytes are unchanged (a hash hit; fixed-width CODEC(NONE) columns) | `input` | `cluster` | `1` | `0` | `{"cas_mutation_marks_hit_fraction": [0, 1]}` |  | assumed: preset default: compressed marks change with the data (0.3.0) | `ok` |
| `cas_manifest_bytes` | `cas` | Bytes of one part manifest (inline small files included) | `input` | `pool` | `B` | `13000` |  |  | assumed: measured 13 019 B (5-column Compact) and 13 751 B (5-column Wide) in cas-branches (0.3.0) | `ok` |
| `gc_round_graduation_budget` | `cas` | Blobs a GC round may graduate to delete_pending (cas_gc_round_graduation_budget; 0 = unbounded) | `pinned` | `pool` | `count` | `5000` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:65 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_table_cleanup_period_s` | `cas` | Seconds between two cleanup-thread listings of one table: idle, max_cleanup_delay_period 300 s + U(0, 10 s); under load runs every cleanup_delay_period (30 s) but the listing waits merge_tree_clear_old_temporary_directories_interval_seconds (60 s) | `input` | `cluster` | `s` | `305` | `{"cas_table_cleanup_period_s": [60, 310]}` |  | assumed: idle value from src/Storages/MergeTree/IMergeTreeCleanupThread.cpp:134-149 @ 09d0772a842, MergeTreeCleanupThread.cpp:43-47, MergeTreeData.cpp:4028, MergeTreeSettings.cpp:1682,1685; the 60 s floor under load is an assumption (0.3.0) | `ok` |
| `cas_cleanup_gets_per_table` | `cas` | GETs one cleanup run reads for a table with parts, besides its LIST; every table on the disk is assumed to hold parts (an empty table reads 1) | `pinned` | `pool` | `count` | `5` |  |  | assumed: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables/README.md: 10 foreground GETs per table in 12 minutes beyond the insert graph at 1, 4, 8 and 16 tables, 2 cleanup runs per table; measured, not fitted, not traced to source (0.1.0) | `ok` |
| `cas_tables` | `cas` | MergeTree tables on the CAS disk | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_table_creates_per_month` | `cas` | CREATE TABLE statements per month for MergeTree tables on the CAS disk | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_table_drops_per_month` | `cas` | DROP TABLE statements per month for MergeTree tables on the CAS disk | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_ref_log_tail_records` | `cas` | Ref log records of one table since its last snapshot that a folding GC round reads | `input` | `pool` | `count` | `128` | `{"cas_ref_log_tail_records": [0, 256]}` |  | assumed: half the snapshot threshold of 256 records (src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h:296 @ 09d0772a842): the tail grows from 0 to 256 between snapshots (0.3.0) | `ok` |
| `cas_log_records_per_part` | `cas` | Ref log records one part adds: the precommit, promote and rename transactions (Pool/CasPartWriteTxn.cpp:697, :810; ContentAddressedTransaction.cpp:1306) | `pinned` | `pool` | `count` | `3` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp:697 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `gc_stage` | `cas` | GC reclamation stage | `input` | `pool` |  | `"blob_delete"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_objects_per_compact_part` | `cas` | Blob files of a Compact part on CAS: data.bin and one per projection (marks, primary.cidx and the small files are inline) | `derivation` | `cluster` | `count` |  | `{"projections": [0, 1]}` | `1 + projections` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:67 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_objects_per_wide_part` | `cas` | Blob files of a Wide part on CAS: a .bin per stream, the distinct mark files, one per projection | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29], "cas_marks_blob_fraction": [0.033333, 1]}` | `wide_part_streams × (1 + cas_marks_blob_fraction) + projections` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:67 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_objects_per_part` | `cas` | Blob files of an inserted part on CAS, by its part type (the rest of the part is inline in its manifest) | `derivation` | `pool` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29], "cas_marks_blob_fraction": [0.033333, 1]}` | `if part_type == compact then cas_objects_per_compact_part else cas_objects_per_wide_part` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:67 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_branch_population` | `cas` | Part files per entry branch for one part write | `measured` | `pool` | `count` |  | `{"insert_rows": [100, 4000000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `gc_round_requests` | `cas` | Requests of one GC round (excluding heartbeat pulses outside the round) | `measured` | `pool` | `count` |  | `{"cas_pool_objects": [1017, 9949]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `gc_round_requests_steady` | `cas` | Requests of one steady-state GC round: fold base, per new manifest, per tail record, per table; Deferred round | `measured` | `pool` | `count` |  | `{"cas_tables": [1, 500]}` |  | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables (0.1.0) | `ok` |
| `gc_round_duration` | `cas` | Duration of one GC round | `measured` | `pool` | `ms` |  | `{"cas_pool_objects": [1017, 9949]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `lease_requests_per_s` | `cas` | Mount lease renewals per second per server: one If-Match PUT per renew period, start to start | `derivation` | `replica` | `1/s` |  |  | `1 / cas_mount_renew_period_s` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp:690 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `reclaim_delay_periods` | `cas` | GC round periods from retirement to each reclamation stage (seconds = periods x (interval + round duration)) | `measured` | `pool` | `1` |  | `{"cas_gc_interval_s": [10, 60]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_blob_uploads_per_part` | `cas` | Distinct blob uploads of an inserted part (cas_objects_per_part) | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29], "cas_marks_blob_fraction": [0.033333, 1]}` | `cas_objects_per_part` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:67 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_merge_result_blob_uploads` | `cas` | Distinct blob uploads of a level-1 merge result: Wide once fan-in x part bytes reach min_bytes_for_wide_part (FutureMergedMutatedPart.cpp:25-32) | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29], "cas_marks_blob_fraction": [0.033333, 1]}` | `if merge_effective_fan_in × part_bytes_uncompressed < min_bytes_for_wide_part then cas_objects_per_compact_part else cas_objects_per_wide_part` | source: src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:67 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `reclaim_periods_blob_delete` | `cas` | GC round periods from retirement to the blob delete | `derivation` | `pool` | `1` |  |  | `lookup(reclaim_delay_periods)[blob_delete]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient reclaim_delay_periods (0.3.0) | `ok` |
| `gc_round_requests_fold_get` | `cas` | GC round requests: fold get | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_get]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_put` | `cas` | GC round requests: fold put | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_put]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_list` | `cas` | GC round requests: fold list | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_list]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_get_per_manifest` | `cas` | GC round requests: fold get per manifest | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_get_per_manifest]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_delete` | `cas` | GC round requests: fold DELETEs (an old gc/gen generation pruned) | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_delete]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_sweep_namespace_budget` | `cas` | Tables the orphan-manifest sweep visits per GC round at most (gc_round_sweep_namespace_budget) | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[sweep_namespace_budget]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_get_per_swept_table` | `cas` | GC fold GETs per table the orphan-manifest sweep visits, besides its tail (4 stream objects, 1 _ckpt) | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_get_per_swept_table]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_get_per_swept_tail_log_record` | `cas` | GC fold GETs per tail record of a table the orphan-manifest sweep visits | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_get_per_swept_tail_log_record]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_get_per_new_log_record` | `cas` | GC fold GETs per new ref log record | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_get_per_new_log_record]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_fold_get_per_changed_table` | `cas` | GC fold GETs per table with new data: its _ckpt and up to 3 mount objects (2 answer 404) | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[fold_get_per_changed_table]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_deferred_get` | `cas` | GC round requests: deferred get | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[deferred_get]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_deferred_put` | `cas` | GC round requests: deferred put | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[deferred_put]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_requests_deferred_list` | `cas` | GC round requests: deferred list | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests_steady)[deferred_list]` | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables coefficient gc_round_requests_steady (0.1.0) | `ok` |
| `gc_round_duration_fold_base` | `cas` | GC fold round duration: fold base | `derivation` | `pool` | `ms` |  |  | `lookup(gc_round_duration)[fold_base]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_duration (0.3.0) | `ok` |
| `gc_round_duration_fold_per_part` | `cas` | GC fold round duration: fold per part | `derivation` | `pool` | `ms` |  |  | `lookup(gc_round_duration)[fold_per_part]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_duration (0.3.0) | `ok` |
| `result_type` | `replication` | Result type of a replicated part: inserted, merged or mutated | `pinned` | `cluster` |  | `"insert"` |  |  | source: src/Interpreters/PartLog.cpp:67 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `replication_role` | `replication` | Side of a part fetch a request belongs to: the replica serving it or the one fetching | `pinned` | `cluster` |  | `"sender"` |  |  | source: src/Storages/MergeTree/DataPartsExchange.cpp:469 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `replication_op` | `replication` | S3 operation of a replication, recovery or retry request row | `pinned` | `cluster` |  | `"PutObject"` |  |  | source: src/Storages/MergeTree/DataPartsExchange.cpp:567 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `fault_class` | `replication` | Failed request class: rejected before the store (a 5XX answer the store never saw, store_reached 0) or a response lost after the store accepted the request (store_reached 1); a 5XX the store itself returns is store_reached 1 and billed per the billing artifact | `pinned` | `cluster` |  | `"rejected_before_store"` |  |  | source: src/IO/S3/Client.cpp:116 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `attempt_kind` | `replication` | Which attempts a retry row counts: every attempt, or only the attempts the store saw (billable) | `pinned` | `cluster` |  | `"attempts"` |  |  | source: src/IO/S3/Client.cpp:852 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `acquire_requests` | `replication` | Requests per part a follower fetches, by result type and side (sender reads, receiver writes and load reads) | `measured` | `cluster` | `count` |  | `{"insert_rows": [1000, 200000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `acquire_bytes` | `replication` | Interserver bytes the sender ships per byte on disk of a fetched part | `measured` | `cluster` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `cas_relink_bytes_per_part` | `replication` | Interserver bytes a CAS relink sends per fetched part: the manifest, not the blobs | `measured` | `cluster` | `B` |  | `{"insert_rows": [100000, 300000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `local_exec_requests` | `replication` | Requests a follower sends per merge or mutation result it executes itself | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `fetch_fraction` | `replication` | Share of follower acquisitions that are fetches, per result type, with default settings | `measured` | `cluster` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `zero_copy_requests` | `replication` | Requests per part a follower fetches with zero-copy replication (s3), by result type and side | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `startup_requests_after_restart` | `topology_recovery` | Requests a restarted replica sends before its first catch-up fetch | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `retry_outcome_mix` | `reliability` | Extra request attempts per failed attempt, and how many of them reach the store, per fault class | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `failed_upload_residue` | `reliability` | Incomplete multipart bytes left after a failed upload the server aborts itself | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `large_objects_per_part` | `inserts` | Part files larger than the 32 MiB first upload buffer, per inserted part (each one multipart upload) | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutations_per_month` | `mutations_deletes` | ALTER UPDATE/DELETE and lightweight DELETE statements per month | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutated_compact_parts_per_month` | `mutations_deletes` | Compact parts rewritten by mutations per month | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutated_wide_parts_per_month` | `mutations_deletes` | Wide parts rewritten by mutations per month | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutation_kind` | `mutations_deletes` | Kind of the mutations | `input` | `cluster` |  | `"update_column"` |  |  | assumed: preset default (0.3.0) | `ok` |
| `select_part_reads_per_day` | `reads_cache` | Parts read by SELECT queries per day (sum of SelectedParts) | `input` | `cluster` | `count/day` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `read_streams_per_part` | `reads_cache` | Column streams a query reads per part (a Nullable column is 2, length(arr) is 1) | `input` | `cluster` | `count` | `3` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cache_miss_fraction` | `reads_cache` | Share of read bytes the filesystem cache downloads from S3 (1: every read is cold); cache misses are segment downloads, so their requests scale with the missed bytes | `input` | `cluster` | `1` | `1` | `{"cache_miss_fraction": [0, 1]}` |  | assumed: preset default (0.3.0) | `ok` |
| `cache_size_gib` | `local_disk` | Filesystem cache size of each replica | `input` | `cluster` | `GiB` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `restarts_per_month` | `topology_recovery` | Server restarts per replica per month (intact state) | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `missed_parts_per_restart` | `topology_recovery` | Parts a restarted replica fetches to catch up | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `variant_id` | `topology_recovery` | Model variant a per-variant coefficient table is indexed by | `pinned` | `cluster` |  | `"s3"` |  |  | source: src/Storages/MergeTree/MergeTreeData.cpp:6399 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `startup_op` | `topology_recovery` | S3 operation of a startup request row | `pinned` | `cluster` |  | `"GetObject"` |  |  | source: src/Disks/IDisk.cpp:254 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `object_size_bucket` | `inserts` | Object size bucket: small files, up to 1 MiB, up to the 32 MiB first upload buffer, beyond | `pinned` | `cluster` |  | `"le_4KiB"` |  |  | source: src/IO/WriteBufferFromS3.cpp:188 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `large_objects_per_merge_result` | `merges` | Files of a merge result larger than the 32 MiB first upload buffer (each one multipart upload) | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `large_objects_per_mutated_part` | `mutations_deletes` | Files a mutation writes per Wide part larger than the 32 MiB first upload buffer | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `retained_mutation_entries` | `mutations_deletes` | mutation_&lt;n>.txt files a plain MergeTree keeps (at most finished_mutations_to_keep plus unfinished ones) | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `finished_mutations_to_keep` | `mutations_deletes` | Finished mutation entries a table keeps | `pinned` | `cluster` | `count` | `100` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:1769 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `refresh_statistics_interval` | `reads_cache` | Seconds between statistics refreshes of a table | `pinned` | `cluster` | `s` | `300` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:2362 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `expiry_operations_per_month` | `mutations_deletes` | TTL expiry merges and DROP PARTITION statements per month (each writes one empty covering part) | `derivation` | `cluster` | `count` |  |  | `expired_parts_per_month` | assumed: preset default (0.3.0) | `ok` |
| `mutation_entries_removed_per_month` | `mutations_deletes` | mutation_&lt;n>.txt objects removed per month: once finished_mutations_to_keep are kept, one per new mutation | `derivation` | `cluster` | `count` |  |  | `mutations_per_month` | source: src/Storages/StorageMergeTree.cpp:2394 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `parts_first_read_per_month` | `reads_cache` | Parts read for the first time per month: every new part (insert or merge result) once | `derivation` | `cluster` | `count` |  |  | `parts_inserted_per_month + merge_results_per_month` | assumed: preset default (0.3.0) | `ok` |
| `statistics_refreshes_per_month` | `reads_cache` | Statistics refreshes per table per month that reload every active part: one per refresh_statistics_interval while parts change | `derivation` | `cluster` | `count` |  |  | `2628000 / refresh_statistics_interval` | source: src/Storages/MergeTree/MergeTreeData.cpp:3425 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `outdated_parts` | `topology_recovery` | Outdated parts a replica holds: merge removals per second x old_parts_lifetime | `derivation` | `cluster` | `count` |  |  | `merge_removals_per_month / 2628000 × old_parts_lifetime` | assumed: preset default (0.3.0) | `ok` |
| `parts_inserted_per_month` | `inserts` | Parts inserted per month: insert_rate x 2 628 000 s x parts_per_insert | `derivation` | `cluster` | `count` |  |  | `insert_rate × 2628000 × parts_per_insert` | source: src/Interpreters/Squashing.cpp:395 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `merge_results_per_month` | `merges` | Merge results per month: parts_inserted_per_month x (1 - 1 / merge_size_ratio) / (merge_effective_fan_in - 1) | `derivation` | `cluster` | `count` |  | `{"merge_size_ratio": [18.21, 472.95]}` | `parts_inserted_per_month × (1 − 1 / merge_size_ratio) / (merge_effective_fan_in − 1)` | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `merge_sources_per_month` | `merges` | Merge source parts per month: merge_results_per_month x merge_effective_fan_in | `derivation` | `cluster` | `count` |  |  | `merge_results_per_month × merge_effective_fan_in` | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `merge_removals_per_month` | `merges` | Merged-away parts removed per month (old_parts_lifetime after their merge): merge_sources_per_month in steady state | `derivation` | `cluster` | `count` |  |  | `merge_sources_per_month` | assumed: preset default (0.3.0) | `ok` |
| `expired_parts_per_month` | `mutations_deletes` | Parts leaving retention per month at their final size: parts_inserted_per_month / merge_size_ratio | `derivation` | `cluster` | `count` |  |  | `parts_inserted_per_month / merge_size_ratio` | assumed: preset default (0.3.0) | `ok` |
| `active_parts` | `topology_recovery` | Active parts per replica: data_bytes x insert_rate x 86 400 s/day x parts_per_insert / (ingest_rate x merge_size_ratio) | `derivation` | `cluster` | `count` |  |  | `data_bytes × insert_rate × 86400 × parts_per_insert / (ingest_rate × merge_size_ratio)` | assumed: preset default (0.3.0) | `ok` |
| `index_granularity` | `schema` | Rows per granule (MergeTree setting index_granularity) | `input` | `cluster` | `count` | `8192` |  |  | source: src/Storages/MergeTree/MergeTreeSettings.cpp:72 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `mutated_part_rows` | `mutations_deletes` | Rows of a part a mutation rewrites: active parts are at their final size (part_rows x merge_size_ratio) | `derivation` | `cluster` | `count` |  |  | `part_rows × merge_size_ratio` | assumed: preset default (0.3.0) | `ok` |
| `merge_result_rows` | `merges` | Rows of a merge result: part_rows x merge_effective_fan_in (a first-level merge, most merge results) | `derivation` | `cluster` | `count` |  |  | `part_rows × merge_effective_fan_in` | assumed: preset default (0.3.0) | `ok` |
| `merge_result_bytes` | `merges` | Uncompressed bytes of a merge result: part_bytes_uncompressed x merge_effective_fan_in | `derivation` | `cluster` | `B` |  |  | `part_bytes_uncompressed × merge_effective_fan_in` | assumed: preset default (0.3.0) | `ok` |
| `mutation_compact_gets_per_full_granule` | `mutations_deletes` | GetObject per granule (half-full rounding) when a mutation reads a Compact part: two data.bin seeks per granule | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `objects_per_part` | `schema` | Objects of an inserted part on s3, by its part type | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `if part_type == compact then objects_per_compact_part else objects_per_wide_part` | source: src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:274 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `object_size_distribution` | `schema` | Fraction of a part's objects per size bucket | `measured` | `cluster` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `insert_tx_checks` | `inserts` | Requests an insert transaction sends per part besides its object writes | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `bytes_per_get` | `reads_cache` | Bytes per GetObject of a cold read (statistics reads and column ranges together) | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `stats_gets_per_column` | `reads_cache` | GetObject per column per part read: statistics.packed is read once per column statistics file | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `warm_gets_per_stream_read` | `reads_cache` | GetObject per stream of a part read whose marks and statistics estimates are cached: the .bin range | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `gets_per_stream_read` | `reads_cache` | GetObject per stream read per part: the .bin range and its marks file | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `cas_gets_per_stream_read` | `reads_cache` | GetObject per stream of the first read of a part on CAS: the marks blob and the .bin range (statistics are inline) | `measured` | `cluster` | `count` |  | `{"read_streams_per_part": [2, 3]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_warm_gets_per_stream_read` | `reads_cache` | GetObject per stream of a repeated part read on CAS with the marks cached: the .bin range | `measured` | `cluster` | `count` |  | `{"read_streams_per_part": [2, 3]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `shape_columns` | `schema` | Statistics files read per part: one per column | `derivation` | `cluster` | `count` |  |  | `columns` | source: src/Storages/MergeTree/IMergeTreeDataPart.cpp:1386 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cached_miss_gets_per_part_read` | `reads_cache` | GETs per part read through the cache on a miss: 2 x read streams - 1 | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `2 × read_streams_per_part − 1` | source: docs/superpowers/reports/2026-10-24-storage-cost-s3/README.md:188 (26.8@09d0772a842) | `ok` |
| `cas_cached_miss_gets_per_stream_read` | `reads_cache` | GetObject per stream of a CAS part read that misses the filesystem cache: at most the marks blob and one .bin segment | `measured` | `cluster` | `count` |  | `{"read_streams_per_part": [2, 3]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cached_miss_bytes_per_part_read` | `reads_cache` | Bytes downloaded per part read that misses the filesystem cache | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `cache_fill_io_per_byte` | `local_disk` | Cache-volume bytes allocated per byte the cache downloads | `measured` | `cluster` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `metadata_bytes_per_object` | `local_disk` | Local metadata bytes allocated per S3 object (one small file each) | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `metadata_iops_per_object_write` | `local_disk` | Local write calls per object written (the part's metadata files) | `measured` | `cluster` | `req` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `metadata_iops_per_part_write` | `local_disk` | Local write calls per part written besides its objects | `measured` | `cluster` | `req` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `startup_gets_per_part` | `topology_recovery` | GETs per part at a server start: 9 small files, plus statistics once per column and the archive index without the cache (one segment with it) | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `if cache_enabled == 1 then part_load_files + 1 else part_load_files + shape_columns + 1` | source: src/Storages/MergeTree/IMergeTreeDataPart.cpp:1504 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `cas_startup_gets_per_part` | `topology_recovery` | GetObject per part a CAS server loads: the manifest, and the first marks blob of a Wide part | `measured` | `cluster` | `count` |  | `{"insert_rows": [100, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_startup_gets_per_log_record` | `topology_recovery` | GetObject per ref log record a CAS server replays per table at start (records since the last snapshot) | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 1]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `startup_requests` | `topology_recovery` | Requests a server start sends besides part loading (disk access checks, table files) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `cas_startup_requests` | `topology_recovery` | Requests a CAS server start sends besides part loads and log replay (access checks, mount claim, pool and catalog reads), all outcomes | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 1]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_startup_error_requests` | `topology_recovery` | Requests a CAS server start sends that are answered 4xx (404 reads of absent keys, 412 conditional writes) | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 1]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_startup_requests_per_table` | `topology_recovery` | Requests a CAS server start sends per table on the disk (catalog row, _ckpt, table files), answered 2xx | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 500]}` |  | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables (0.1.0) | `ok` |
| `cas_startup_error_requests_per_table` | `topology_recovery` | Requests a CAS server start sends per table on the disk that are answered 404 | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 500]}` |  | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables (0.1.0) | `ok` |
| `cas_startup_requests_base` | `topology_recovery` | Requests a CAS server start without the filesystem cache sends besides tables, parts and log replay, answered 2xx | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 500]}` |  | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables (0.1.0) | `ok` |
| `cas_startup_error_requests_base` | `topology_recovery` | Requests a CAS server start without the filesystem cache sends besides tables, answered 4xx | `measured` | `cluster` | `count` |  | `{"cas_tables": [1, 500]}` |  | measured: docs/superpowers/reports/2026-10-25-storage-cost-cas-tables (0.1.0) | `ok` |
| `merge_gets_per_source_part` | `merges` | GETs per merge source part without the cache: Compact one seek per column plus 3, Wide 2 per stream plus 1 | `derivation` | `cluster` | `count` |  |  | `if part_type == compact then columns + merge_compact_source_extra_gets else files_per_wide_stream × wide_part_streams + merge_wide_source_extra_gets` | source: src/Storages/MergeTree/MergeTreeReaderCompact.cpp:219 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `merge_gets_per_source_part_cached` | `merges` | GETs per merge source part through the cache: Compact 3, Wide 1 per stream plus 1 | `derivation` | `cluster` | `count` |  |  | `if part_type == compact then merge_cached_compact_source_gets else wide_part_streams + merge_wide_source_extra_gets` | source: src/Storages/MergeTree/MergeTreeReaderStream.cpp:270 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `delete_calls_per_removed_part` | `mutations_deletes` | DeleteObjects calls per removed part (one batch of up to 1000 keys) | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:609 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `empty_part_objects` | `mutations_deletes` | Objects of the empty part that covers a dropped or expired part | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/StorageMergeTree.cpp:2742 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `mutation_entry_objects` | `mutations_deletes` | Objects written per mutation statement besides parts (mutation_&lt;n>.txt) | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/MergeTreeMutationEntry.cpp:73 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `mutation_objects_written` | `mutations_deletes` | Objects a mutation writes per Wide part: ALTER DELETE rewrites the part, UPDATE of one column and lightweight DELETE write one stream and 7 metadata files | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `if mutation_kind == delete then objects_per_wide_part else files_per_wide_stream + mutation_rewritten_metadata_files` | source: src/Storages/MergeTree/MutateTask.cpp:1247 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `mutation_gets_per_part` | `mutations_deletes` | GetObject per mutated part (narrow shape) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `catchup_requests_per_missed_part` | `topology_recovery` | Requests per part a restarted replica fetches to catch up, per S3 operation (replication report) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `merge_result_part_type` | `merges` | Part type of a merge result: the part type predicate at the merge result size | `derivation` | `cluster` |  |  |  | `if adaptive_granularity == 0 then wide else if quantize_pq_codec == 1 then wide else if merge_result_bytes < min_bytes_for_wide_part then compact else if merge_result_rows < min_rows_for_wide_part then compact else if 1 < min_level_for_wide_part then compact else wide` | source: src/Storages/MergeTree/MergeTreeData.cpp:6402 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_op` | `schema` | S3 operation of an s3plain request row | `pinned` | `cluster` |  | `"PutObject"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:111 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_startup_kind` | `topology_recovery` | Part of the s3plain startup cost: fixed per start, or per loaded directory | `pinned` | `cluster` |  | `"fixed"` |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.cpp:138 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_meta_objects_per_part` | `schema` | Directories per part on s3_plain_rewritable: the part's and one per projection | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `1 + projections` | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:111 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_dir_create_requests` | `inserts` | Requests per directory an s3plain part creates: the prefix.path PutObject and a HeadObject of it | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:125 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_rename_requests` | `inserts` | PutObject per directory an s3plain rename rewrites (prefix.path); every new part is renamed from its tmp_ name | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:239 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_copy_on_hardlink` | `mutations_deletes` | Requests per hard link on s3plain: a server-side copy (HeadObject for the size, CopyObject); reached by ATTACH/REPLACE PARTITION FROM, never by mutations (refused) | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:456 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_unlink_requests` | `mutations_deletes` | Requests per file an s3plain part removal unlinks: copy aside (HeadObject, CopyObject), delete the file, delete the copy | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:390 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_dir_remove_requests` | `mutations_deletes` | Requests per directory of an s3plain part removal: the rename to delete_tmp_ (PutObject), the prefix.path delete | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp:297 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_objects_per_part` | `schema` | Objects of an inserted part on s3_plain_rewritable, by its part type | `derivation` | `cluster` | `count` |  | `{"columns": [5, 40], "streams_per_column": [1, 3.29]}` | `if part_type == compact then objects_per_compact_part else plain_objects_per_wide_part` | source: src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_empty_part_objects` | `mutations_deletes` | Objects of the empty part that covers a dropped or expired part on s3plain (a blob per empty file) | `pinned` | `cluster` | `count` |  |  |  | source: src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:313 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_expired_part_extra_files` | `mutations_deletes` | Files of an expired part besides its shape's files: partition.dat and minmax_&lt;key>.idx of one partition key column | `pinned` | `cluster` | `count` |  |  |  | source: src/Storages/MergeTree/MergeTreePartition.cpp:470 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `plain_startup_requests` | `topology_recovery` | Requests an s3plain server start sends besides part loads: layout listing and prefix.path reads per directory, the disk access checks | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3plain (0.3.0) | `ok` |
| `plain_local_metadata_bytes_per_object` | `local_disk` | Local metadata bytes per object on s3plain: none, the layout is held in memory | `pinned` | `cluster` | `B` |  |  |  | source: src/Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h:87 @ 09d0772a842 (26.8@09d0772a842) | `ok` |
| `provisioned_size_gib` | `local_disk` | Manual provisioning: size of each volume | `input` | `volume` | `GiB` |  |  |  | assumed: manual allocation; read only when volumes_per_replica is a number (0.2.0) | `ok` |
| `provisioned_iops` | `local_disk` | Manual provisioning: IOPS of each volume | `input` | `volume` | `iops` | `3000` |  |  | assumed: manual allocation; default is the gp3 baseline; read only when volumes_per_replica is a number (0.2.0) | `ok` |
| `provisioned_mibps` | `local_disk` | Manual provisioning: throughput of each volume | `input` | `volume` | `mibps` | `125` |  |  | assumed: manual allocation; default is the gp3 baseline; read only when volumes_per_replica is a number (0.2.0) | `ok` |
| `volumes_per_replica` | `local_disk` | Volumes per replica: auto (cheapest feasible layout) or a count for manual provisioning | `input` | `replica` |  | `"auto"` |  |  | assumed: auto picks the cheapest feasible layout of the billing contract (0.2.0) | `ok` |
| `ebs_capacity_headroom` | `ebs` | Capacity headroom over stored data (free space, temporary merge output) | `input` | `volume` | `1` | `1.3` |  |  | assumed: provisioned capacity = data_bytes x this factor; the rig shows the headroom exists (the probe_local_volume capacity peak and merge-natural allocated peaks exceed the final active bytes, docs/superpowers/reports/2026-10-24-storage-cost-ebs) but its small parts do not size it (0.2.0) | `ok` |
| `ebs_io_rule` | `ebs` | EBS I/O accounting on SSD volumes: an I/O counts up to 256 KiB, larger ones split, physically sequential small ones merge; demand is priced as a merged sequential stream (4 I/Os per MiB), the fewest I/Os its bytes can take | `pinned` | `volume` |  | `"ssd_256kib_merged_sequential"` |  |  | provider: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html (checked 2026-10-24) (2026-10-24) | `ok` |
| `ebs_max_io_bytes` | `ebs` | Largest I/O an SSD volume counts as one IOPS | `pinned` | `volume` | `B` | `262144` |  |  | provider: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html (checked 2026-10-24) (2026-10-24) | `ok` |
| `io_op` | `ebs` | Operation key of the EBS demand tables | `input` | `cluster` |  | `"inserts"` |  |  | assumed: table key (0.2.0) | `ok` |
| `io_direction` | `ebs` | Direction key of the EBS demand tables | `input` | `cluster` |  | `"write"` |  |  | assumed: table key (0.2.0) | `ok` |
| `io_size_bucket` | `ebs` | Request-size bucket (power-of-two upper bound) | `input` | `cluster` |  | `"1MiB"` |  |  | assumed: table key (0.2.0) | `ok` |
| `io_bytes_per_op` | `ebs` | Volume bytes per byte the operation produces (merge, insert) or scans (read), by direction | `measured` | `volume` | `1` |  | `{"insert_rows": [1000, 300000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05, warm page cache (not dropped, no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle rate (server background, mostly system logs, plus about 2.3 kB/s of the embedded Keeper on r1); read demand is the bytes traced syscalls return, with no bound direction against a cold device) (0.2.0) | `ok` |
| `op_write_bytes` | `ebs` | Volume write bytes per unit: per source row of a mutated part (measured); per dropped part (assumption: the largest measured record, 273 kB; the per-part unit did not carry, 125 kB on the hold-out); per restart (assumption 0: below the idle baseline). Reads of these operations are not modelled | `measured` | `volume` | `B` |  | `{"insert_rows": [30000, 400000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05, warm page cache (not dropped, no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle rate (server background, mostly system logs, plus about 2.3 kB/s of the embedded Keeper on r1); read demand is the bytes traced syscalls return, with no bound direction against a cold device) (0.2.0) | `ok` |
| `insert_write_overhead_rows` | `ebs` | Fixed per-part write overhead of an insert, in rows: inserts write io_bytes_per_op[inserts, write] + this / insert_rows bytes per byte (marks, checksums, columns, count, serialization and statistics files and page rounding do not grow with rows) | `measured` | `cluster` | `count` |  | `{"insert_rows": [1000, 300000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05, warm page cache (not dropped, no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle rate (server background, mostly system logs, plus about 2.3 kB/s of the embedded Keeper on r1); read demand is the bytes traced syscalls return, with no bound direction against a cold device) (0.2.0) | `ok` |
| `io_request_size_distribution` | `ebs` | Fraction of volume requests per size bucket, by operation and direction | `measured` | `volume` | `1` |  | `{"insert_rows": [1000, 300000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05, warm page cache (not dropped, no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle rate (server background, mostly system logs, plus about 2.3 kB/s of the embedded Keeper on r1); read demand is the bytes traced syscalls return, with no bound direction against a cold device) (0.2.0) | `ok` |
| `ebs_sequential_ios_per_mib` | `ebs` | EBS I/Os per MiB, LOWER BOUND: a merged sequential stream (1 MiB / 256 KiB); the rig syscall proxy counted 1.3-6.5x this (5.3-26.1 per MiB; insert reads, 1335-1608 per MiB on negligible bytes, excluded); the gp3 3000-IOPS baseline usually dominates | `pinned` | `volume` | `req/MiB` | `4` |  |  | provider: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html (checked 2026-10-24); measured syscall-proxy spread on docs/superpowers/reports/2026-10-24-storage-cost-ebs: 5.3-26.1 I/Os per MiB (insert writes 8.5-14.1 narrow, 15-16 mixed, 26 complex types; merge reads 10-20; scans 8-16) (2026-10-24) | `ok` |
| `peak_factor` | `ebs` | Burst factor for provisioned IOPS/throughput; 1 = size for the average rate | `input` | `cluster` | `1` | `1` |  |  | assumed: not measurable on this rig: merge-natural.default gave 1.0 (all merges within 1.2 s) and merge-natural.wide 15.0-15.9 (one late merge sets the window); see the EBS report (0.2.0) | `ok` |

Form placement. `workload` and `setting` inputs make up the default form (at most 10 settings); `advanced` inputs sit in the collapsed low-level section; `hidden` inputs are model constants or table keys, shown only in the info dialog. Rule: a product manager can set it after one sentence and it moves money for a typical workload -> `setting`; can set it but it rarely matters -> `advanced`; needs more than one sentence -> `hidden`.

| Tier | Id | ClickHouse name | Form label | Block |
|---|---|---|---|---|
| `workload` | `cas_table_creates_per_month` |  | Tables created per month | `cas` |
| `workload` | `cas_table_drops_per_month` |  | Tables dropped per month | `cas` |
| `workload` | `cas_tables` |  | Tables on the disk | `cas` |
| `workload` | `data_bytes` |  | Data size | `simple` |
| `workload` | `ingest_rate` |  | Ingest rate | `simple` |
| `workload` | `replicas` |  | Replicas | `simple` |
| `workload` | `retention_days` |  | Retention | `simple` |
| `workload` | `topology` |  | Replica placement | `simple` |
| `setting` | `cas_gc_interval_s` | `cas_gc_interval_sec` | CAS GC interval | `cas` |
| `setting` | `async_insert` | `async_insert` | Async inserts | `inserts` |
| `setting` | `insert_rate` |  | INSERTs per second | `inserts` |
| `setting` | `insert_rows` |  | Rows per INSERT | `inserts` |
| `setting` | `cache_size_gib` |  | Filesystem cache size | `local_disk` |
| `setting` | `mutations_per_month` |  | ALTER UPDATE/DELETE per month | `mutations_deletes` |
| `setting` | `cache_enabled` | `enable_filesystem_cache` | Filesystem cache | `reads_cache` |
| `setting` | `select_part_reads_per_day` |  | Cold part reads per day | `reads_cache` |
| `setting` | `columns` |  | Columns | `schema` |
| `setting` | `index_granularity` | `index_granularity` | Rows per granule | `schema` |
| `advanced` | `cas_hash_hit_fraction` |  | Share of data already in the pool | `cas` |
| `advanced` | `cas_marks_blob_fraction` |  | Distinct CAS mark files | `cas` |
| `advanced` | `cas_parts_per_dropped_table` |  | Parts per dropped table | `cas` |
| `advanced` | `ebs_capacity_headroom` |  | EBS free-space headroom | `ebs` |
| `advanced` | `peak_factor` |  | EBS peak-to-average factor | `ebs` |
| `advanced` | `deduplicate_insert` | `deduplicate_insert` | Insert deduplication | `inserts` |
| `advanced` | `insert_bytes_uncompressed` |  | Uncompressed bytes per INSERT | `inserts` |
| `advanced` | `insert_clients` |  | Concurrent insert clients | `inserts` |
| `advanced` | `insert_data_block_rows` | `max_insert_block_size` | Rows per insert block | `inserts` |
| `advanced` | `insert_kind` |  | INSERT with data or INSERT SELECT | `inserts` |
| `advanced` | `insert_protocol` |  | Insert protocol | `inserts` |
| `advanced` | `partitions_per_insert` |  | Partitions per INSERT | `inserts` |
| `advanced` | `provisioned_iops` |  | Manual EBS volume IOPS | `local_disk` |
| `advanced` | `provisioned_mibps` |  | Manual EBS volume throughput | `local_disk` |
| `advanced` | `provisioned_size_gib` |  | Manual EBS volume size | `local_disk` |
| `advanced` | `volumes_per_replica` |  | EBS volumes per replica | `local_disk` |
| `advanced` | `mutated_compact_parts_per_month` |  | Compact parts mutated per month | `mutations_deletes` |
| `advanced` | `mutated_wide_parts_per_month` |  | Wide parts mutated per month | `mutations_deletes` |
| `advanced` | `mutation_kind` |  | Mutation kind | `mutations_deletes` |
| `advanced` | `cache_miss_fraction` |  | Cache miss share | `reads_cache` |
| `advanced` | `read_streams_per_part` |  | Columns read per part | `reads_cache` |
| `advanced` | `projections` |  | Projections | `schema` |
| `advanced` | `skip_index_files_per_part` |  | Skip-index files per part | `schema` |
| `advanced` | `streams_per_column` |  | Streams per column | `schema` |
| `advanced` | `type_mix` |  | Column types | `schema` |
| `advanced` | `missed_parts_per_restart` |  | Parts fetched per restart | `topology_recovery` |
| `advanced` | `restarts_per_month` |  | Restarts per month | `topology_recovery` |
| `hidden` | `cas_blob_bytes` |  | CAS blob bytes | `cas` |
| `hidden` | `cas_branch` |  | CAS write branch | `cas` |
| `hidden` | `cas_manifest_bytes` |  | CAS manifest bytes | `cas` |
| `hidden` | `cas_mutation_marks_hit_fraction` |  | Unchanged marks share | `cas` |
| `hidden` | `cas_pool_objects` |  | CAS pool objects | `cas` |
| `hidden` | `cas_ref_log_tail_records` |  | Ref log tail records | `cas` |
| `hidden` | `cas_table_cleanup_period_s` |  | Table cleanup period | `cas` |
| `hidden` | `cas_write` |  | CAS write kind | `cas` |
| `hidden` | `gc_round_term` |  | GC round term | `cas` |
| `hidden` | `gc_stage` |  | GC stage | `cas` |
| `hidden` | `io_direction` |  | EBS demand direction | `ebs` |
| `hidden` | `io_op` |  | EBS demand operation | `ebs` |
| `hidden` | `io_size_bucket` |  | EBS request size bucket | `ebs` |
| `hidden` | `async_insert_source` |  | Async insert source | `inserts` |
| `hidden` | `insert_payload_bytes` |  | Inline INSERT payload bytes | `inserts` |
| `hidden` | `large_objects_per_part` |  | Multipart files per part | `inserts` |
| `hidden` | `object_bytes` |  | Bytes per object | `inserts` |
| `hidden` | `large_objects_per_merge_result` |  | Multipart files per merge | `merges` |
| `hidden` | `merge_fan_in_bucket` |  | Merge fan-in bucket | `merges` |
| `hidden` | `merge_result_size_bucket` |  | Merge result size bucket | `merges` |
| `hidden` | `merge_size_ratio` |  | Final part size in inserts | `merges` |
| `hidden` | `large_objects_per_mutated_part` |  | Multipart files per mutation | `mutations_deletes` |
| `hidden` | `retained_mutation_entries` |  | Retained mutation entries | `mutations_deletes` |
| `hidden` | `adaptive_granularity` | `index_granularity_bytes` | Adaptive granularity | `schema` |
| `hidden` | `empty_files_per_wide_part` |  | Empty files per Wide part | `schema` |
| `hidden` | `part_level` |  | Part level | `schema` |
| `hidden` | `quantize_pq_codec` |  | Quantize codec | `schema` |
| `hidden` | `shape` |  | Table width | `schema` |

| Precedence group | Locked by default | Rules |
|---|---|---|
| `data_bytes`, `ingest_rate`, `retention_days` | `ingest_rate`, `retention_days` | `data_bytes` = `ingest_rate × retention_days`; `ingest_rate` = `data_bytes / retention_days`; `retention_days` = `data_bytes / ingest_rate` |
<!-- inputs:end -->

## Break-even {#break-even}

<!-- breakeven:begin -->
Generated from `breakeven.json` (`tools/sweeps.mjs`), swept on contract version `0aba74da6565e52c` with price preset `aws-us-east-1`. Do not edit.

A crossing is a sign change of the difference of two totals between consecutive ladder values, so the break-even lies inside the interval, not at a point. A variant that is not `ok` at a value has no total there and no crossing is drawn through it.

Ladders: ×1/4, ×1/2, ×1, ×2, ×4, ×8 of each scenario's value, clamped to the input's measured range; whole numbers for counts; the scenario value is always a point.

### Scenario `logs` {#breakeven-logs}

Logs.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 3,750, 7,500, 15,000, 30,000, 60,000, 120,000 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `s3` between 15,000 and 30,000; `ebs` / `s3plain` between 30,000 and 60,000; `ebs` / `cas` between 7,500 and 15,000 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
| `ingest_rate` | 125, 250, 500, 1,000, 2,000, 4,000 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `s3` between 500 and 1,000; `ebs` / `s3plain` between 1,000 and 2,000; `ebs` / `cas` between 250 and 500 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
| `replicas` | 1, 2, 4, 6 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `cas` between 1 and 2; `s3` / `cas` between 1 and 2 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |

### Scenario `metrics` {#breakeven-metrics}

Metrics.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 450, 900, 1,800, 3,600, 7,200, 14,400 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `s3` between 7,200 and 14,400; `ebs` / `cas` between 3,600 and 7,200 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
| `ingest_rate` | 5, 10, 20, 40, 80, 160 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `s3` between 80 and 160; `ebs` / `cas` between 40 and 80 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
| `replicas` | 1, 2, 4, 6 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `cas` between 4 and 6; `s3` / `cas` between 1 and 2 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |

### Scenario `analytics_batch` {#breakeven-analytics-batch}

Analytics batch.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 2,560, 5,120, 10,240, 20,480, 40,960, 81,920 | `ebs`, `s3`, `cas` | none | `ebs`: ok; `s3`: ok; `s3plain`: not_supported; `cas`: ok |
| `ingest_rate` | 7.0137, 14.0274, 28.0548, 56.1096, 112.219, 224.438 | `ebs`, `s3`, `cas` | none | `ebs`: ok; `s3`: ok; `s3plain`: not_supported; `cas`: ok |
| `replicas` | 1, 2, 4, 6 | `ebs`, `s3`, `cas` | `s3` / `cas` between 1 and 2 | `ebs`: ok; `s3`: ok; `s3plain`: not_supported; `cas`: ok |

### Scenario `single_replica_archive` {#breakeven-single-replica-archive}

Single replica archive.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 12,800, 25,600, 51,200, 102,400, 204,800, 409,600 | `ebs`, `s3`, `s3plain`, `cas` | none | `ebs`: ok / unavailable; `s3`: ok / unavailable; `s3plain`: ok / unavailable; `cas`: ok / unavailable |
| `ingest_rate` | 17.5342, 35.0685, 70.137, 140.274, 280.548, 561.096 | `ebs`, `s3`, `s3plain`, `cas` | none | `ebs`: ok / unavailable; `s3`: ok / unavailable; `s3plain`: ok / unavailable; `cas`: ok / unavailable |
| `replicas` | 1, 2, 4, 6 | `ebs`, `s3`, `s3plain`, `cas` | `s3` / `cas` between 1 and 2 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |

### Scenario `custom` {#breakeven-custom}

Custom.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 5,000, 10,000, 20,000, 40,000, 80,000, 160,000 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `s3` between 10,000 and 20,000; `ebs` / `s3plain` between 20,000 and 40,000; `ebs` / `cas` between 5,000 and 10,000 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
| `ingest_rate` | 125, 250, 500, 1,000, 2,000, 4,000 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `s3` between 250 and 500; `ebs` / `s3plain` between 500 and 1,000; `ebs` / `cas` between 125 and 250 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
| `replicas` | 1, 2, 3, 6 | `ebs`, `s3`, `s3plain`, `cas` | `ebs` / `cas` between 1 and 2; `s3` / `cas` between 1 and 2 | `ebs`: ok; `s3`: ok; `s3plain`: ok; `cas`: ok |
<!-- breakeven:end -->
