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

Every coefficient and input carries one evidence kind:

| Kind | Meaning |
|---|---|
| `source` | A code path at the pinned commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, cited as `path:line` |
| `measured` | Fitted on rig measurements; a P1 coefficient also has a validity domain and a separate hold-out |
| `provider` | A dated provider document, such as an AWS price page |
| `assumed` | A preset or user value |

- **Measured counts** are validated on RustFS behind the ledger proxy, not on AWS. No AWS bill was reconciled.
- **EBS I/O** is derived from measured ClickHouse I/O with AWS accounting rules, not measured on EBS.
- **Outside a validity domain** a value is still shown, labelled "extrapolated".
- **Applicability** is per variant × operation × family. A family that no report lists for a variant is
  `not_applicable`: it needs no evidence and adds nothing to the column. Only `supported` families need evidence.
- **Unavailable columns** have no total, no savings delta and no "cheapest" mark. The states are `not_supported`,
  `backlog_grows`, `unsupported_cut` and `unavailable`.

## Known inaccuracies {#known-inaccuracies}

<!-- known-inaccuracies: refresh after Plan 2 -->

### Model limits {#model-limits}

- Operations without evidence: the closure check lists every variant × operation × family cell that has neither
  evidence nor a `not_supported` state. The page shows such a column as `unavailable`.
- Card and probe cells (`card_cells` in the contract) reconcile single cards. The page does not evaluate them.
- Break-even sweeps need priced cells. Until the research reports land, every sweep total is `unavailable` and
  every crossing is `none`.

### Blog claims against the model {#blog-claims}

Source: `docs/superpowers/blog/2026-cas-storage-model/part4-cheap-self-sufficient.md` on branch `cas-blog-article`
(`git show cas-blog-article:<path>`). The table lists each quantitative or categorical claim about storage cost.
"Model: no priced cell yet (Plan 2 pending)" means the contract has no formulas cell for it, so the model gives no
number. Request prices come from the billing preset `aws-us-east-1` (AWS price list, checked 2026-10-24).

| Line | Blog claim | Model's current answer |
|---|---|---|
| 75-76 | `GET` or `HEAD` costs about $0.0004 per thousand | Agrees. `get_and_other` is $0.0004 per 1,000 (`GetObject`, `HeadObject`, `HeadBucket`). |
| 76-77 | `PUT`, `COPY` or `LIST` costs about $0.005 per thousand | Agrees. `put_copy_post_list` is $0.005 per 1,000. It also bills multipart calls and `POST`, and treats `ListParts` as a `LIST`. |
| 77-78 | The write tier is about 12.5× the read tier | Agrees. 0.005 / 0.0004 = 12.5. |
| 78 | `DELETE` is free | Agrees. `delete_cancel` is $0 and also covers `DeleteObjects` and `AbortMultipartUpload`. |
| 79-81 | Writes scale to at least about 3,500 requests per second per prefix; bursts past that get `503 Slow Down` | Not modelled. The calculator prices request counts and has no rate limits. |
| 88-93 | The collector never `LIST`s the pool on the normal path | Not checked. `probe_cas_gc_cohort` recorded 14 `LIST` requests in the priced cell (20 inserts, `DROP TABLE`, 3 GC rounds). The probe does not show whether they scan the pool. Model: no priced cell yet (Plan 2 pending). |
| 95-99 | A naive commit takes four to six sequential round trips and adds over a second to every insert | Not modelled. The calculator has no latency. |
| 121-123 | An early configuration issued about 5,100 `HEAD` per second, two-thirds of reads failing | Not modelled. This is a soak observation, not a steady-state rate. |
| 163-172 | A commit costs the new blob `PUT`s, one tree `PUT` and exactly one manifest CAS `PUT`; at most two control writes under contention | Model: no priced cell yet (Plan 2 pending). `probe_cas_expect` (phase-0 report) recorded 27 foreground `PUT` for 3 inserts: 3 blobs and 24 conditional writes, which is 8 per insert, and 3 `HEAD` (one per blob). It does not split the conditional writes into tree, manifest and control. |
| 169-172 | No Keeper operation at all | Not modelled. The ledger counts object-store requests only. |
| 198-199 | A beta RustFS build leaked about 74 GB across 18 keys in an hour | Not modelled. This is a backend defect on the test store. |
| 268-275 | The read path issues no `HEAD` and no `LIST` | Model: no priced cell yet (Plan 2 pending). No `reads` cell exists for `cas`. |
| 293-294 | A part marked in round R is deleted in round R+1 | Not checked. `probe_cas_gc_cohort` reclaimed all 40 cohort objects 14.3 s after `DROP TABLE` with rounds every 5 s; it does not record the round of each mark and delete. |
| 305-306 | About 80,000 audit events in twenty minutes at two writers | Not modelled. This is a soak observation. |

## Reproduce {#reproduce}

Pinned inputs, from `docs/superpowers/reports/2026-10-24-storage-cost-phase0/README.md`:

| Item | Value |
|---|---|
| Binary, downloaded | `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46` |
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (the download decompresses itself on first start) |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Store | RustFS behind the ledger proxy, bucket `yardstick-cost` |

1. Run the rig gate. It runs the six acceptance probes, then the drill and render checks, and exits 2 on a failure:

   ```bash
   utils/cas-yardstick/rustfs-bin.sh ensure
   YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost probe \
       --binary tmp/bin/26.8/clickhouse --runs-dir tmp/cas-yardstick/cost-runs
   ```

2. Run the experiments of a research report. `measurements.jsonl` and `raw/` are written to the report directory:

   ```bash
   python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
       --runs-dir tmp/cas-yardstick/cost-runs --manifests <report directory> --cases all
   ```

   `--manifests` takes the report directory itself; the rig reads its `experiments/*.json`. Reports: `docs/superpowers/reports/2026-10-24-storage-cost-phase0`
   and `docs/superpowers/reports/2026-10-24-storage-cost-lifecycle`.

3. Regenerate the contract, the page copy, the measured fixtures and the `Formulas` and `Inputs` blocks. Sweep the
   new contract, then generate again so the manifest hashes the sweep. The sweep does not change the contract version:

   ```bash
   python3 utils/storage-cost-calculator/tools/generate_contract.py generate
   node utils/storage-cost-calculator/tools/sweeps.mjs \
       --contract utils/storage-cost-calculator/model-contract.json \
       --out docs/superpowers/cost-model/breakeven.json
   python3 utils/storage-cost-calculator/tools/generate_contract.py generate
   ```

4. Check that every generated file matches a fresh render, then run the closure checks:

   ```bash
   python3 utils/storage-cost-calculator/tools/generate_contract.py check --mode final
   python3 utils/storage-cost-calculator/tools/closure_check.py --mode final
   ```

5. Run the tests:

   ```bash
   SCC_DEV=1 node --test 'utils/storage-cost-calculator/test/*.test.js'
   cd utils/storage-cost-calculator/tools && python3 -m unittest discover -p 'test_*.py'
   ```

The contract's `manifest` names what a page URL reproduces: the sha256 of every code file, the registry, coverage and
billing versions, the sha256 of `breakeven.json`, and per report the source commit, rig commit, binary and the sha256
of `raw/SHA256SUMS`.

## Formulas {#formulas}

<!-- formulas:begin -->
Generated by `tools/generate_contract.py` from `model-contract.json` version `0f7f5344c110222a`. Do not edit.

| Variant | Operation | Cell owner | Applicability |
|---|---|---|---|
| `ebs` | `storage` | no cell | not_applicable |
| `ebs` | `inserts` | no cell | not_applicable |
| `ebs` | `merges` | no cell | not_applicable |
| `ebs` | `mutations` | no cell | not_applicable |
| `ebs` | `deletes` | no cell | not_applicable |
| `ebs` | `reads` | no cell | not_applicable |
| `ebs` | `recovery` | no cell | not_applicable |
| `ebs` | `local_disk` | no cell | not_applicable |
| `s3` | `storage` | no cell | not_applicable |
| `s3` | `inserts` | no cell | not_applicable |
| `s3` | `merges` | no cell | not_applicable |
| `s3` | `mutations` | no cell | not_applicable |
| `s3` | `deletes` | no cell | not_applicable |
| `s3` | `reads` | no cell | not_applicable |
| `s3` | `recovery` | no cell | not_applicable |
| `s3` | `local_disk` | no cell | not_applicable |
| `s3plain` | `storage` | no cell | not_applicable |
| `s3plain` | `inserts` | no cell | not_applicable |
| `s3plain` | `merges` | no cell | not_applicable |
| `s3plain` | `mutations` | no cell | not_applicable |
| `s3plain` | `deletes` | no cell | not_applicable |
| `s3plain` | `reads` | no cell | not_applicable |
| `s3plain` | `recovery` | no cell | not_applicable |
| `s3plain` | `local_disk` | no cell | not_applicable |
| `cas` | `storage` | no cell | not_applicable |
| `cas` | `inserts` | no cell | not_applicable |
| `cas` | `merges` | no cell | not_applicable |
| `cas` | `mutations` | no cell | not_applicable |
| `cas` | `deletes` | no cell | not_applicable |
| `cas` | `reads` | no cell | not_applicable |
| `cas` | `recovery` | no cell | not_applicable |
| `cas` | `local_disk` | no cell | not_applicable |

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
Registry version `0.2.0`. Generated; do not edit.

| Id | Block | Label | Kind | Scope | Unit | Default | Validity | Rule | Evidence | Status |
|---|---|---|---|---|---|---|---|---|---|---|
| `data_bytes` | `simple` | Data size | `input` | `cluster` | `GiB` | `20480` |  |  | assumed: preset default (0.1.0) | `ok` |
| `ingest_rate` | `simple` | Ingest rate | `input` | `cluster` | `GiB/day` | `500` |  |  | assumed: preset default (0.1.0) | `ok` |
| `retention_days` | `simple` | Retention | `input` | `cluster` | `day` | `40` |  |  | assumed: preset default (0.1.0) | `ok` |
| `replicas` | `simple` | Replicas | `input` | `cluster` | `count` | `3` | `{"replicas": [1, 6]}` |  | assumed: preset default (0.1.0) | `ok` |
| `shards` | `simple` | Shards | `input` | `cluster` | `count` | `1` | `{"shards": [1, 1]}` |  | assumed: preset default (0.1.0) | `ok` |
| `cache_enabled` | `reads_cache` | Filesystem cache enabled | `input` | `replica` |  | `"1"` |  |  | assumed: preset default (0.1.0) | `ok` |
| `shape` | `schema` | Table shape | `input` | `cluster` |  | `"narrow"` |  |  | assumed: preset default (0.1.0) | `ok` |
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
| `min_bytes_for_wide_part` | `schema` | Minimum uncompressed bytes for a Wide part | `pinned` | `cluster` | `B` | `10485760` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:78 (26.8@09d0772a842) | `ok` |
| `min_rows_for_wide_part` | `schema` | Minimum rows for a Wide part | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:85 (26.8@09d0772a842) | `ok` |
| `min_level_for_wide_part` | `schema` | Minimum merge level for a Wide part | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:82 (26.8@09d0772a842) | `ok` |
| `part_type_predicate` | `schema` | Part type rule: a part is Compact when its uncompressed bytes &lt; min_bytes_for_wide_part, its rows &lt; min_rows_for_wide_part or its level &lt; min_level_for_wide_part, else Wide. An insert measures the block in memory (Block::bytes); a merge sums the inputs' on-disk uncompressed column bytes, uses level max(input levels) + 1, and is Wide when any input is Wide. A mutation keeps the source type. Overrides: no adaptive granularity, or a Quantize('pq') column, forces Wide/Full. | `pinned` | `cluster` |  | `"compact_if_any_below_minimum"` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeData.cpp:6399 (26.8@09d0772a842) | `ok` |
| `part_level` | `schema` | Level of the part (0 for an inserted part) | `input` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeDataWriter.cpp:735 (26.8@09d0772a842) | `ok` |
| `part_bytes_uncompressed` | `schema` | Uncompressed bytes of one inserted part | `derivation` | `cluster` | `B` |  |  | `insert_bytes_uncompressed / parts_per_insert` | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeDataWriter.cpp:848 (26.8@09d0772a842) | `ok` |
| `part_rows` | `schema` | Rows of one inserted part | `derivation` | `cluster` | `count` |  |  | `insert_rows / parts_per_insert` | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeDataWriter.cpp:915 (26.8@09d0772a842) | `ok` |
| `adaptive_granularity` | `schema` | The table can write adaptive-granularity parts (index_granularity_bytes > 0, and mixed granularity allowed or no old non-adaptive part); without it every part is Wide | `input` | `cluster` |  | `"1"` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeData.cpp:11680 (26.8@09d0772a842) | `ok` |
| `quantize_pq_codec` | `schema` | A column of the part uses CODEC(Quantize('pq', ...)): the part is forced Wide/Full | `input` | `cluster` |  | `"0"` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeData.cpp:6428 (26.8@09d0772a842) | `ok` |
| `part_type` | `schema` | Part type of an inserted part: Wide without adaptive granularity or with a Quantize('pq') column, else the size, rows and level predicate | `derivation` | `cluster` |  |  |  | `if adaptive_granularity == 0 then wide else if quantize_pq_codec == 1 then wide else if part_bytes_uncompressed < min_bytes_for_wide_part then compact else if part_rows < min_rows_for_wide_part then compact else if part_level < min_level_for_wide_part then compact else wide` | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeData.cpp:6402 (26.8@09d0772a842) | `ok` |
| `multipart_first_part_bytes` | `inserts` | First upload buffer: an object up to this size is one PutObject, a larger one starts a multipart upload with a first part of this size | `pinned` | `cluster` | `B` | `33554432` |  |  | source: tmp/src-26.8/src/Common/BufferAllocationPolicy.cpp:51 (26.8@09d0772a842) | `ok` |
| `multipart_next_part_bytes` | `inserts` | Size of the second and later upload parts | `pinned` | `cluster` | `B` | `16777216` |  |  | source: tmp/src-26.8/src/Common/BufferAllocationPolicy.cpp:52 (26.8@09d0772a842) | `ok` |
| `multipart_growth_parts` | `inserts` | Upload parts between part-size growth steps | `pinned` | `cluster` | `count` | `500` |  |  | source: tmp/src-26.8/src/IO/S3Defines.h:30 (26.8@09d0772a842) | `ok` |
| `multipart_growth_factor` | `inserts` | Part-size growth factor every multipart_growth_parts parts | `pinned` | `cluster` | `1` | `2` |  |  | source: tmp/src-26.8/src/IO/S3Defines.h:29 (26.8@09d0772a842) | `ok` |
| `strict_upload_part_size` | `inserts` | Fixed upload part size (0 = off): when set, every buffer is this size | `pinned` | `cluster` | `B` | `0` |  |  | source: tmp/src-26.8/src/Common/BufferAllocationPolicy.cpp:98 (26.8@09d0772a842) | `ok` |
| `max_single_part_upload_size` | `inserts` | Largest object one buffer may send as a single PutObject | `pinned` | `cluster` | `B` | `33554432` |  |  | source: tmp/src-26.8/src/IO/S3Defines.h:26 (26.8@09d0772a842) | `ok` |
| `multipart_sizing_rule` | `inserts` | Upload sizing: with strict_upload_part_size = 0 WriteBufferFromS3 fills a first buffer of clamp(max_single_part_upload_size, min_upload_part_size, max_upload_part_size), then buffers of min_upload_part_size that grow by multipart_growth_factor every multipart_growth_parts buffers (up to 5 GiB); with a strict size every buffer is that size. A second filled buffer starts the multipart upload. At finalize one buffer of at most max_single_part_upload_size is one PutObject (an object of exactly 32 MiB by default); one larger buffer, possible only when the first buffer or the strict size exceeds max_single_part_upload_size, is a multipart upload of one part. The last part holds the remainder. | `pinned` | `cluster` |  | `"exp_buffers_single_put_up_to_first"` |  |  | source: tmp/src-26.8/src/IO/WriteBufferFromS3.cpp:180 (26.8@09d0772a842) | `ok` |
| `object_bytes` | `inserts` | Size of one object written to S3 (one part file) | `input` | `cluster` | `B` | `1048576` |  |  | assumed: preset default (0.2.0) | `ok` |
| `multipart_parts` | `inserts` | Data-carrying upload requests for one object: with a strict size ceil(size / strict); else 1 up to the first buffer and 1 + ceil((size - first) / next) beyond it (no growth step: valid below 499 later parts) | `derivation` | `cluster` | `count` |  | `{"object_bytes": [0, 8405385216]}` | `if strict_upload_part_size > 0 then if object_bytes ≤ strict_upload_part_size then 1 else ceil_to(object_bytes / strict_upload_part_size, 1) else if object_bytes ≤ multipart_first_part_bytes then 1 else 1 + ceil_to((object_bytes − multipart_first_part_bytes) / multipart_next_part_bytes, 1)` | source: tmp/src-26.8/src/Common/BufferAllocationPolicy.cpp:72 (26.8@09d0772a842) | `ok` |
| `multipart_upload` | `inserts` | The object goes up as a multipart upload (1) or one PutObject (0): multipart when it exceeds the first buffer (the strict size, else the clamped first buffer) or max_single_part_upload_size | `derivation` | `cluster` | `count` |  |  | `if object_bytes > min(if strict_upload_part_size > 0 then strict_upload_part_size else multipart_first_part_bytes, max_single_part_upload_size) then 1 else 0` | source: tmp/src-26.8/src/IO/WriteBufferFromS3.cpp:180 (26.8@09d0772a842) | `ok` |
| `min_insert_block_size_rows` | `inserts` | Insert squashing emits a block once it holds this many rows | `pinned` | `cluster` | `count` | `1048449` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:209 (26.8@09d0772a842) | `ok` |
| `min_insert_block_size_bytes` | `inserts` | Insert squashing emits a block once it holds this many bytes | `pinned` | `cluster` | `B` | `268402944` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:232 (26.8@09d0772a842) | `ok` |
| `use_strict_insert_block_limits` | `inserts` | Strict insert block limits (0 = off): when on, max limits split blocks and the parts formula does not apply | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:162 (26.8@09d0772a842) | `ok` |
| `max_block_size` | `inserts` | Rows per block a SELECT reads, so per source block of an INSERT SELECT (optimize_trivial_insert_select is off) | `pinned` | `cluster` | `count` | `65409` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:155 (26.8@09d0772a842) | `ok` |
| `max_insert_block_size` | `inserts` | Rows per block the server parses formatted INSERT data into | `pinned` | `cluster` | `count` | `1048449` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:178 (26.8@09d0772a842) | `ok` |
| `insert_kind` | `inserts` | INSERT with data (inline, formatted or native blocks) or INSERT SELECT; only INSERTs with data can go asynchronous | `input` | `cluster` |  | `"data"` |  |  | source: tmp/src-26.8/src/Interpreters/executeQuery.cpp:2894 (26.8@09d0772a842) | `ok` |
| `insert_data_block_rows` | `inserts` | Rows per block an INSERT with data delivers: formatted input is parsed into max_insert_block_size rows; a native client sends its own blocks | `input` | `cluster` | `count` | `1048449` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:178 (26.8@09d0772a842) | `ok` |
| `insert_source_block_rows` | `inserts` | Rows per block entering the insert squash: max_block_size for INSERT SELECT, insert_data_block_rows for data | `derivation` | `cluster` | `count` |  |  | `if insert_kind == select then max_block_size else insert_data_block_rows` | source: tmp/src-26.8/src/Core/Settings.cpp:5609 (26.8@09d0772a842) | `ok` |
| `insert_protocol` | `inserts` | How an INSERT with data arrives: native protocol blocks or inline (HTTP body, query text); the async queue measures native data as decoded block bytes and inline data as raw bytes | `input` | `cluster` |  | `"native"` |  |  | source: tmp/src-26.8/src/Server/TCPHandler.cpp:1414 (26.8@09d0772a842) | `ok` |
| `insert_payload_bytes` | `inserts` | Raw bytes of an inline INSERT's data (the async buffer counts these for inline inserts, not table bytes) | `input` | `cluster` | `B` | `1048576` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_clients` | `inserts` | Clients inserting concurrently; with wait_for_async_insert = 1 each has at most one insert in a buffer | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_rows` | `inserts` | Rows per INSERT | `input` | `cluster` | `count` | `100000` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_bytes_uncompressed` | `inserts` | Uncompressed bytes per INSERT | `input` | `cluster` | `B` | `10485760` |  |  | assumed: preset default (0.2.0) | `ok` |
| `partitions_per_insert` | `inserts` | Partitions one INSERT touches | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.2.0) | `ok` |
| `insert_rate` | `inserts` | INSERT queries per second, all clients together | `input` | `cluster` | `1/s` | `1` |  |  | assumed: preset default (0.2.0) | `ok` |
| `async_insert` | `inserts` | Async inserts on: the query setting async_insert (on by default in this build) or the table's async_insert setting | `input` | `cluster` |  | `"1"` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:7225 (26.8@09d0772a842) | `ok` |
| `wait_for_async_insert` | `inserts` | An async INSERT waits for its flush (1 = on) | `pinned` | `cluster` | `count` | `1` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:7228 (26.8@09d0772a842) | `ok` |
| `async_insert_deduplicate` | `inserts` | Legacy async insert deduplication (0 = off); consulted only when deduplicate_insert = backward_compatible_choice | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:2393 (26.8@09d0772a842) | `ok` |
| `deduplicate_insert` | `inserts` | Insert deduplication mode; supersedes async_insert_deduplicate. When deduplication is on, async_insert_max_query_number inserts flush a buffer | `input` | `cluster` |  | `"enable"` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:2357 (26.8@09d0772a842) | `ok` |
| `insert_deduplicate` | `inserts` | Legacy synchronous insert deduplication (1 = on); with deduplicate_insert = backward_compatible_choice it also decides for an INSERT made async by the table setting alone | `pinned` | `cluster` | `count` | `1` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:2379 (26.8@09d0772a842) | `ok` |
| `async_insert_source` | `inserts` | What makes an INSERT async: the query setting async_insert, or only the table's async_insert setting (the query setting 0) | `input` | `cluster` |  | `"query"` |  |  | source: tmp/src-26.8/src/Server/TCPHandler.cpp:1497 (26.8@09d0772a842) | `ok` |
| `async_insert_dedup_effective` | `inserts` | Deduplication on for async inserts (1) or off (0) | `derivation` | `cluster` | `count` |  |  | `if deduplicate_insert == enable then 1 else if deduplicate_insert == disable then 0 else if async_insert_source == table then insert_deduplicate else async_insert_deduplicate` | source: tmp/src-26.8/src/Core/DeduplicateInsert.cpp:26 (26.8@09d0772a842) | `ok` |
| `async_insert_max_query_number` | `inserts` | Inserts that flush a buffer when deduplication is on | `pinned` | `cluster` | `count` | `450` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:7239 (26.8@09d0772a842) | `ok` |
| `async_insert_busy_timeout_max_ms` | `inserts` | Longest wait before an async insert buffer is flushed | `pinned` | `cluster` | `ms` | `200` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:7252 (26.8@09d0772a842) | `ok` |
| `async_insert_busy_timeout_min_ms` | `inserts` | Shortest adaptive wait before an async insert buffer is flushed | `pinned` | `cluster` | `ms` | `50` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:7249 (26.8@09d0772a842) | `ok` |
| `async_insert_max_data_size` | `inserts` | Unparsed bytes that flush an async insert buffer; an INSERT with more data runs synchronously | `pinned` | `cluster` | `B` | `10485760` |  |  | source: tmp/src-26.8/src/Core/Settings.cpp:7234 (26.8@09d0772a842) | `ok` |
| `async_insert_flush_rule` | `inserts` | Async flush: an INSERT with data of at most async_insert_max_data_size bytes is queued (INSERT SELECT and larger data run synchronously); inserts with the same user, roles, query shape, settings and data kind share a buffer; it is flushed into one part per partition when its unparsed bytes reach async_insert_max_data_size, when async_insert_max_query_number inserts accumulate with deduplication on, or when its deadline passes. The deadline is the first insert time plus an adaptive timeout between async_insert_busy_timeout_min_ms and _max_ms that grows while inserts are frequent. With wait_for_async_insert = 1 a client waits for the flush, so only concurrent inserts share one. | `pinned` | `cluster` |  | `"size_count_or_deadline"` |  |  | source: tmp/src-26.8/src/Interpreters/AsynchronousInsertQueue.cpp:680 (26.8@09d0772a842) | `ok` |
| `inserts_per_async_flush` | `inserts` | Async inserts sharing one flush, an estimate for evenly spaced arrivals: the first insert plus those arriving within the longest busy timeout, at most ceil(buffer / entry bytes) entries (decoded bytes for native, raw for inline), at most one per waiting client, at most async_insert_max_query_number with deduplication on (the default). Bursts share more, a shorter adaptive timeout less. | `derivation` | `cluster` | `1` |  |  | `max(1, min(1 + insert_rate × async_insert_busy_timeout_max_ms / 1000, ceil_to(async_insert_max_data_size / (if insert_protocol == native then insert_bytes_uncompressed else insert_payload_bytes), 1), if wait_for_async_insert == 1 then insert_clients else 1000000000000, if async_insert_dedup_effective == 1 then async_insert_max_query_number else 1000000000000))` | assumed: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle#parts-per-insert (0.2.0) | `ok` |
| `parts_per_insert` | `inserts` | Parts one INSERT creates, per partition: synchronously one per squashed block (whole source blocks joined until either minimum is reached; strict limits off); an async INSERT with data below the size limit (decoded bytes &lt; limit for native, raw bytes &lt;= limit for inline), a share of a flush of inserts_per_async_flush inserts. INSERT SELECT is always synchronous. Not covered: a native async INSERT over the limit, which the server forwards as one combined block of the first limit's worth before the rest. | `derivation` | `cluster` | `1` |  |  | `partitions_per_insert × (if insert_kind == select then max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)) else if async_insert == 1 then if insert_protocol == native then if insert_bytes_uncompressed < async_insert_max_data_size then 1 / inserts_per_async_flush else max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)) else if insert_payload_bytes ≤ async_insert_max_data_size then 1 / inserts_per_async_flush else max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)) else max(1, ceil_to(insert_rows / (if min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) < 1000000000000000 then min(if min_insert_block_size_rows > 0 then ceil_to(min_insert_block_size_rows, insert_source_block_rows) else 1000000000000000, if min_insert_block_size_bytes > 0 then ceil_to(min_insert_block_size_bytes / (insert_bytes_uncompressed / insert_rows), insert_source_block_rows) else 1000000000000000) else insert_source_block_rows), 1)))` | source: tmp/src-26.8/src/Interpreters/Squashing.cpp:395 (26.8@09d0772a842) | `ok` |
| `old_parts_lifetime` | `merges` | Minimum age before an outdated part may be removed; removal also waits for references and transactions and runs on the cleanup schedule | `pinned` | `cluster` | `s` | `480` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:556 (26.8@09d0772a842) | `ok` |
| `max_bytes_to_merge_at_max_space_in_pool` | `merges` | Largest merge result the selector plans with free space | `pinned` | `cluster` | `B` | `161061273600` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:474 (26.8@09d0772a842) | `ok` |
| `merge_selector_base` | `merges` | SimpleMergeSelector base (write amplification vs part count) | `pinned` | `cluster` | `1` | `5` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:885 (26.8@09d0772a842) | `ok` |
| `max_parts_to_merge_at_once` | `merges` | Most parts one simple-selector merge takes; TTL drop merges read it too, a whole-partition force merge (min_age_to_force_merge_seconds) does not | `pinned` | `cluster` | `count` | `100` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:636 (26.8@09d0772a842) | `ok` |
| `merge_size_ratio` | `merges` | Size of the part an inserted row ends up in, in inserted parts, row-weighted over the final parts: sum(r^2) / sum(r) | `input` | `cluster` | `1` | `64` |  |  | assumed: preset default (0.2.0) | `ok` |
| `merge_effective_fan_in` | `merges` | Effective merge fan-in of the write amplification rule | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.22, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `write_amplification_rule` | `merges` | Write amplification: bytes written by inserts and merges per inserted byte: 1 + rewrite levels at merge_size_ratio with effective fan-in merge_effective_fan_in (levels capped at 12) | `derivation` | `cluster` | `1` |  |  | `1 + (if merge_size_ratio < 1 then 0 else if merge_size_ratio < merge_effective_fan_in then (merge_size_ratio − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in then 1 + (merge_size_ratio / merge_effective_fan_in − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 2 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 3 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 4 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 5 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 6 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 7 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 8 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 9 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 10 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 11 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else 12)` | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `merge_fan_in_bucket` | `merges` | Fan-in bucket of a merge | `input` | `cluster` |  | `"5"` |  |  | assumed: bucket key of merge_fan_in_distribution (0.2.0) | `ok` |
| `merge_fan_in_distribution` | `merges` | Fraction of natural merges per fan-in bucket | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.22, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `merge_result_size_bucket` | `merges` | Merge result size bucket, in inserted parts | `input` | `cluster` |  | `"5-8"` |  |  | assumed: bucket key of merge_result_size_distribution (0.2.0) | `ok` |
| `merge_result_size_distribution` | `merges` | Fraction of natural merges per result size bucket (result rows / inserted part rows) | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.22, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `acquisition_rule` | `replication` | Fetch vs local execution of a replicated merge, in order: skip when the part or a covering one is already here; fetch when always_fetch_merged_part; try to fetch a TTL-recompress merge another replica was assigned, quietly retrying while no replica has it, until try_fetch_recompressed_part_timeout; fetch when execute_merges_on_single_replica_time_threshold > 0, the entry is younger than it and another replica is picked; fetch when a source or patch part is missing or covered; while a source part has newer metadata, try to fetch, quietly retrying; fetch when the entry is at least prefer_fetch_merged_part_time_threshold old, the sources hold at least prefer_fetch_merged_part_size_threshold bytes on disk and a replica has the result; with zero-copy, fetch or wait on the covering part and the exclusive lock; else merge locally. A mutation: fetch when always_fetch_mutated_part, the source is missing or covered, or the entry is that old and 1.1 x the source bytes on disk (expired TTL parts excluded) reach the size threshold with a replica holding the result; the single-replica branch never fires (merge entries only); zero-copy as for merges; else mutate locally, keeping the source part type. | `pinned` | `cluster` |  | `"local_unless_fetch_condition"` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeFromLogEntryTask.cpp:224 (26.8@09d0772a842) | `ok` |
| `prefer_fetch_merged_part_time_threshold` | `replication` | Age of a merge or mutation entry from which (inclusive) a replica prefers fetching a large result | `pinned` | `cluster` | `s` | `3600` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1284 (26.8@09d0772a842) | `ok` |
| `prefer_fetch_merged_part_size_threshold` | `replication` | Source bytes on disk above which an old merge entry is fetched | `pinned` | `cluster` | `B` | `10737418240` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1294 (26.8@09d0772a842) | `ok` |
| `execute_merges_on_single_replica_time_threshold` | `replication` | Seconds the other replicas wait for the chosen replica's merge result (0 = off) | `pinned` | `cluster` | `s` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1304 (26.8@09d0772a842) | `ok` |
| `always_fetch_merged_part` | `replication` | Replica never merges and fetches every merge result (0 = off) | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1341 (26.8@09d0772a842) | `ok` |
| `always_fetch_mutated_part` | `replication` | Replica never mutates and fetches every mutation result (0 = off) | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1348 (26.8@09d0772a842) | `ok` |

| Precedence group | Locked by default | Rules |
|---|---|---|
| `data_bytes`, `ingest_rate`, `retention_days` | `ingest_rate`, `retention_days` | `data_bytes` = `ingest_rate × retention_days`; `ingest_rate` = `data_bytes / retention_days`; `retention_days` = `data_bytes / ingest_rate` |
<!-- inputs:end -->

## Break-even {#break-even}

<!-- breakeven:begin -->
Stale: swept on contract version `1e443600bf11eb3a`, not `0f7f5344c110222a`. Run `tools/sweeps.mjs`.
<!-- breakeven:end -->
