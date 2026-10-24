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

## Review {#review}

Codex (`gpt-6-astra`, high effort, one round) reviewed the calculator code against the contract-only rule and the state and closure rules. It reported 5 MAJOR and 2 MINOR findings. Each MAJOR was reproduced with a failing test first, except the first, which follows the spec.

| Finding | Severity | Disposition |
|---|---|---|
| Rate unit conversion in `pipeline.js`, delta subtraction, bar-segment sums in `viewmodel.js` are arithmetic outside the contract | MAJOR | Not reproduced as a violation. Rate conversion parses an input (§4.2: rate with unit selector, month = 730 h). A delta is a difference of two contract totals (§4.5). A bar segment groups billed lines by unit class and adds no new number. No change. |
| `allocate` re-bills each cell with the default outcome mix, so cell money disagrees with the billed total | MAJOR | Closed. `allocate` takes the outcome mix of the aggregate bill. Test `test_allocation_keeps_the_outcome_mix`. |
| An operation whose families are all `not_applicable` is still evaluated and billed | MAJOR | Closed. `evaluateVariant` skips such an operation. Test `test_all_not_applicable_operation_contributes_nothing`. |
| Extrapolation is lost through registry derivations and table coefficients | MAJOR | Closed. Derived values carry the flag into every `input` node that reads them. Table coefficients keep `validity` and lookups apply it. Test `test_extrapolation_survives_derivations_and_table_validity`. |
| Support closure credits a card's evidence to every variant in `model_variants`, though no case policy measured it | MAJOR | Closed. `support` rejects a card whose `model_variants` lists a variant that no case policy measures. Test `test_card_variant_without_a_measuring_policy_is_flagged`. |
| Registry and precedence expressions get no generic dimension check | MINOR | Parked. Needs a dimension checker over registry and precedence expressions, well over 10 lines. No registry expression has a wrong dimension today. |
| Pinned registry defaults are missing from the evaluation context | MINOR | Closed (3 lines). `buildCtx` reads a pinned quantity's default. Test `test_pinned_quantity_default_reaches_expressions`. |

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
Generated by `tools/generate_contract.py` from `model-contract.json` version `3f57c696fe5e5ff4`. Do not edit.

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
| `ebs` | `gc` | no cell | not_applicable |
| `ebs` | `lease` | no cell | not_applicable |
| `s3` | `storage` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `s3` | `inserts` | `2026-10-24-storage-cost-s3` | not_applicable: lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape, thresholds, multipart_graph |
| `s3` | `merges` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: lifecycle |
| `s3` | `mutations` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `s3` | `deletes` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: mutation_deletes |
| `s3` | `reads` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: reads_cache |
| `s3` | `recovery` | `2026-10-24-storage-cost-s3` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: startup_recovery |
| `s3` | `local_disk` | `2026-10-24-storage-cost-s3` | not_applicable: thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape |
| `s3` | `gc` | no cell | not_applicable |
| `s3` | `lease` | no cell | not_applicable |
| `s3plain` | `storage` | no cell | not_applicable |
| `s3plain` | `inserts` | no cell | not_applicable |
| `s3plain` | `merges` | no cell | not_applicable |
| `s3plain` | `mutations` | no cell | not_applicable |
| `s3plain` | `deletes` | no cell | not_applicable |
| `s3plain` | `reads` | no cell | not_applicable |
| `s3plain` | `recovery` | no cell | not_applicable |
| `s3plain` | `local_disk` | no cell | not_applicable |
| `s3plain` | `gc` | no cell | not_applicable |
| `s3plain` | `lease` | no cell | not_applicable |
| `cas` | `storage` | `2026-10-24-storage-cost-cas` | not_applicable: thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_lease, local_volumes; supported: objects_per_part_by_shape, lifecycle, cas_gc |
| `cas` | `inserts` | `2026-10-24-storage-cost-cas` | not_applicable: thresholds, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: objects_per_part_by_shape, cas_branches; unsupported_cut: multipart_graph |
| `cas` | `merges` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: thresholds, lifecycle, cas_branches |
| `cas` | `mutations` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: cas_branches |
| `cas` | `deletes` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: cas_branches |
| `cas` | `reads` | no cell | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, startup_recovery, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; unsupported_cut: reads_cache |
| `cas` | `recovery` | no cell | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, replication, faults, cas_branches, cas_gc, cas_lease, local_volumes; unsupported_cut: startup_recovery |
| `cas` | `local_disk` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_gc, cas_lease, local_volumes; supported: cas_branches |
| `cas` | `gc` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_lease, local_volumes; supported: lifecycle, cas_gc |
| `cas` | `lease` | `2026-10-24-storage-cost-cas` | not_applicable: objects_per_part_by_shape, thresholds, multipart_graph, lifecycle, mutation_deletes, reads_cache, startup_recovery, replication, faults, cas_branches, cas_gc, local_volumes; supported: cas_lease |

### `s3` / `storage` {#formula-s3-storage}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `storage` | `active` | `all` | `variant` |  | `data_bytes × replicas × 1` |
| `storage` | `outdated` | `all` | `variant` |  | `ingest_rate × merge_effective_fan_in / (merge_effective_fan_in − 1) × (1 − 1 / merge_size_ratio) / 86400 × old_parts_lifetime × replicas × 1` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `merge_effective_fan_in` | `6.1437` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-lifecycle | 0.2.0 | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-lifecycle |

### `s3` / `inserts` {#formula-s3-inserts}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × (lookup(objects_per_part)[shape, part_type] − large_objects_per_part)` |
| `request` | `CreateMultipartUpload` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part` |
| `request` | `UploadPart` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part × multipart_parts` |
| `request` | `CompleteMultipartUpload` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × large_objects_per_part` |
| `request` | `HeadObject` | `ok` | `variant` |  | `parts_inserted_per_month × replicas × insert_tx_checks` |
| `request` | `GetObject` | `ok` | `variant` |  | `parts_inserted_per_month × (replicas − 1) × lookup(objects_per_part)[shape, part_type]` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `insert_tx_checks` | `0` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide", "types"]}` | 2026-10-24-storage-cost-s3 |
| `objects_per_part` | `table over shape, part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide", "types"]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `merges` {#formula-s3-merges}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `merge_results_per_month × replicas × lookup(objects_per_part)[shape, wide]` |
| `request` | `GetObject` | `ok` | `variant` |  | `merge_sources_per_month × replicas × (if cache_enabled == 1 then lookup(merge_gets_per_source_part_cached)[shape, part_type] else lookup(merge_gets_per_source_part)[shape, part_type])` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `merge_sources_per_month × replicas × 1` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `merge_gets_per_source_part` | `table over shape, part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide"]}` | 2026-10-24-storage-cost-s3 |
| `merge_gets_per_source_part_cached` | `table over shape, part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide"]}` | 2026-10-24-storage-cost-s3 |
| `objects_per_part` | `table over shape, part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide", "types"]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `mutations` {#formula-s3-mutations}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `mutated_parts_per_month × replicas × lookup(mutation_objects_written)[mutation_kind, part_type] + mutations_per_month × replicas × mutation_entry_objects` |
| `request` | `GetObject` | `ok` | `variant` |  | `mutated_parts_per_month × replicas × lookup(mutation_gets_per_part)[cache_enabled, mutation_kind, part_type]` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `mutated_parts_per_month × replicas × delete_calls_per_removed_part` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `delete_calls_per_removed_part` | `1` | `count` | source | tmp/src-26.8/src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:599 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `mutation_entry_objects` | `1` | `count` | source | tmp/src-26.8/src/Storages/MergeTree/MergeTreeMutationEntry.cpp:73 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `mutation_gets_per_part` | `table over cache_enabled, mutation_kind, part_type` | `count` | source | tmp/src-26.8/src/Storages/MergeTree/MutateTask.cpp:1247 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `mutation_objects_written` | `table over mutation_kind, part_type` | `count` | source | tmp/src-26.8/src/Storages/MergeTree/MutateTask.cpp:908 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |

### `s3` / `deletes` {#formula-s3-deletes}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `expired_parts_per_month × replicas × empty_part_objects` |
| `request` | `DeleteObjects` | `ok` | `variant` |  | `expired_parts_per_month × replicas × delete_calls_per_removed_part × 2` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `delete_calls_per_removed_part` | `1` | `count` | source | tmp/src-26.8/src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:599 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `empty_part_objects` | `8` | `count` | source | tmp/src-26.8/src/Storages/StorageMergeTree.cpp:2742 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |

### `s3` / `reads` {#formula-s3-reads}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `select_part_reads_per_day × 30.416666666666668 × (if cache_enabled == 1 then cache_miss_fraction × lookup(cached_miss_gets_per_part_read)[shape] else stats_gets_per_column × lookup(shape_columns)[shape] + gets_per_stream_read × read_streams_per_part)` |
| `block_io` | `mibps` | `all` | `replica` |  | `if cache_enabled == 1 then select_part_reads_per_day × 30.416666666666668 × cache_miss_fraction × lookup(cached_miss_bytes_per_part_read)[shape] × cache_fill_io_per_byte / (2628000 × 1048576) / replicas else 0` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `cache_fill_io_per_byte` | `1.0159` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["wide", "types"]}` | 2026-10-24-storage-cost-s3 |
| `cached_miss_bytes_per_part_read` | `table over shape` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["wide", "types"]}` | 2026-10-24-storage-cost-s3 |
| `cached_miss_gets_per_part_read` | `table over shape` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["wide", "types"]}` | 2026-10-24-storage-cost-s3 |
| `gets_per_stream_read` | `2` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["wide", "types"]}` | 2026-10-24-storage-cost-s3 |
| `shape_columns` | `table over shape` | `count` | source | tmp/src-26.8/src/Storages/Statistics/Statistics.cpp:933 | 26.8@09d0772a842 |  | 2026-10-24-storage-cost-s3 |
| `stats_gets_per_column` | `1` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["wide", "types"]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `recovery` {#formula-s3-recovery}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `restarts_per_month × replicas × active_parts × lookup(startup_gets_per_part)[cache_enabled, shape] + restarts_per_month × replicas × lookup(startup_requests)[cache_enabled, GetObject]` |
| `request` | `PutObject` | `ok` | `variant` |  | `restarts_per_month × replicas × lookup(startup_requests)[cache_enabled, PutObject]` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `restarts_per_month × replicas × lookup(startup_requests)[cache_enabled, DeleteObject]` |

| State | Condition | Reason |
|---|---|---|
| `not_supported` | `if missed_parts_per_restart > 0 then 1 else 0` | pending_owner: replication. Catch-up after a restart is missed_parts_per_restart x catchup_requests_per_missed_part[s3], and startup_requests_after_restart[s3] replaces the startup rows; the replication report has not published them yet |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `startup_gets_per_part` | `table over cache_enabled, shape` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide"], "active_parts": [2, 3]}` | 2026-10-24-storage-cost-s3 |
| `startup_requests` | `table over cache_enabled, startup_op` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide"], "active_parts": [2, 3]}` | 2026-10-24-storage-cost-s3 |

### `s3` / `local_disk` {#formula-s3-local-disk}

Owner: `2026-10-24-storage-cost-s3`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `capacity` | `all` | `replica` |  | `active_parts × lookup(objects_per_part)[shape, wide] × metadata_bytes_per_object / 1073741824 + (if cache_enabled == 1 then cache_size_gib else 0)` |
| `block_io` | `iops` | `all` | `replica` |  | `((parts_inserted_per_month × lookup(objects_per_part)[shape, part_type] + merge_results_per_month × lookup(objects_per_part)[shape, wide]) × metadata_iops_per_object_write + (parts_inserted_per_month + merge_results_per_month) × metadata_iops_per_part_write) / 2628000` |

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `metadata_bytes_per_object` | `4096` | `B` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide", "types"]}` | 2026-10-24-storage-cost-s3 |
| `metadata_iops_per_object_write` | `1.1767` | `req` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"objects_per_part": [11, 158]}` | 2026-10-24-storage-cost-s3 |
| `metadata_iops_per_part_write` | `25.979` | `req` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"objects_per_part": [11, 158]}` | 2026-10-24-storage-cost-s3 |
| `objects_per_part` | `table over shape, part_type` | `count` | measured | docs/superpowers/reports/2026-10-24-storage-cost-s3 | 0.3.0 | `{"shape": ["narrow", "wide", "types"]}` | 2026-10-24-storage-cost-s3 |

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
| `request` | `PutObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert × (cas_blob_uploads_per_part × (1 − cas_hash_hit_fraction) × 2 + 7) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert × (cas_blob_uploads_per_part × cas_hash_hit_fraction + 7) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert × cas_blob_uploads_per_part × 2628000` |

Notes:

- `cas_blob_uploads_per_part`
- `cas_hash_hit_fraction`

### `cas` / `merges` {#formula-cas-merges}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × (cas_merge_result_blob_uploads × (1 − cas_hash_hit_fraction) × 2 + 7 + merge_effective_fan_in × 2) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × (merge_effective_fan_in × (cas_blob_uploads_per_part + 2) + 7 + cas_merge_result_blob_uploads × cas_hash_hit_fraction) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `insert_rate × parts_per_insert / (merge_effective_fan_in − 1) × cas_merge_result_blob_uploads × 2628000` |

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
| `request` | `PutObject` | `ok` | `variant` |  | `cas_table_drops_per_s × (cas_parts_per_dropped_table × 2 + 3) × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `cas_table_drops_per_s × (cas_parts_per_dropped_table × 2 + 2) × 2628000` |
| `request` | `HeadObject` | `ok` | `variant` |  | `cas_table_drops_per_s × 1 × 2628000` |
| `request` | `DeleteObject` | `ok` | `variant` |  | `cas_table_drops_per_s × 1 × 2628000` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `cas_table_drops_per_s × 2 × 2628000` |

Notes:

- `cas_foreground_ref_removal_requests`

### `cas` / `local_disk` {#formula-cas-local-disk}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `block_capacity` | `staging_peak` | `all` | `replica` |  | `cas_blob_uploads_per_part × cas_blob_bytes × (insert_clients + 1) / 1073741824` |

### `cas` / `gc` {#formula-cas-gc}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `GetObject` | `ok` | `variant` |  | `(1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × (if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation > 0 then gc_round_requests_fold_get + cas_tables × cas_ref_log_tail_records else (8 × gc_round_requests_deferred_get + gc_round_requests_fold_get + cas_tables × cas_ref_log_tail_records) / 9) + (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × gc_round_requests_fold_get_per_manifest) × 2628000` |
| `request` | `PutObject` | `ok` | `variant` |  | `1 / (cas_gc_interval_s + (gc_round_duration_fold_base + gc_round_duration_fold_per_part × (insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation) × cas_gc_interval_s) / 1000) × (if insert_rate × parts_per_insert + insert_rate × parts_per_insert / (merge_effective_fan_in − 1) + cas_mutations_per_s × cas_parts_per_mutation > 0 then gc_round_requests_fold_put else (8 × gc_round_requests_deferred_put + gc_round_requests_fold_put) / 9) × 2628000` |
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

| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |
|---|---|---|---|---|---|---|---|
| `merge_effective_fan_in` | `6.1437` | `1` | measured | docs/superpowers/reports/2026-10-24-storage-cost-lifecycle | 0.2.0 | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` | 2026-10-24-storage-cost-lifecycle |

### `cas` / `lease` {#formula-cas-lease}

Owner: `2026-10-24-storage-cost-cas`.

| Row | Op | Outcome | Scope | Caller | Expression |
|---|---|---|---|---|---|
| `request` | `PutObject` | `ok` | `variant` |  | `lease_requests_per_s × replicas × 2628000` |
| `request` | `GetObject` | `ok` | `variant` |  | `cas_tables × replicas / cas_table_cleanup_period_s × 2628000` |
| `request` | `ListObjectsV2` | `ok` | `variant` |  | `cas_tables × replicas / cas_table_cleanup_period_s × 2628000` |

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
| `shards` | `simple` | Shards | `input` | `cluster` | `count` | `1` | `{"shards": [1, 1]}` |  | assumed: preset default (0.1.0) | `ok` |
| `cache_enabled` | `reads_cache` | Filesystem cache enabled | `input` | `replica` |  | `"1"` |  |  | assumed: preset default (0.1.0) | `ok` |
| `shape` | `schema` | Table shape: narrow = 5 UInt64 columns, wide = 40 UInt64 columns, types = the 21-column complex-type mix of objects-per-part.types-fit | `input` | `cluster` |  | `"narrow"` |  |  | assumed: preset default (0.1.0) | `ok` |
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
| `merge_effective_fan_in` | `merges` | Effective merge fan-in of the write amplification rule | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `write_amplification_rule` | `merges` | Write amplification: bytes written by inserts and merges per inserted byte: 1 + rewrite levels at merge_size_ratio with effective fan-in merge_effective_fan_in (levels capped at 12) | `derivation` | `cluster` | `1` |  |  | `1 + (if merge_size_ratio < 1 then 0 else if merge_size_ratio < merge_effective_fan_in then (merge_size_ratio − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in then 1 + (merge_size_ratio / merge_effective_fan_in − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 2 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 3 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 4 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 5 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 6 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 7 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 8 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 9 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 10 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else if merge_size_ratio < merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in then 11 + (merge_size_ratio / (merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in × merge_effective_fan_in) − 1) / (merge_effective_fan_in − 1) else 12)` | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `merge_fan_in_bucket` | `merges` | Fan-in bucket of a merge | `input` | `cluster` |  | `"5"` |  |  | assumed: bucket key of merge_fan_in_distribution (0.2.0) | `ok` |
| `merge_fan_in_distribution` | `merges` | Fraction of natural merges per fan-in bucket | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `merge_result_size_bucket` | `merges` | Merge result size bucket, in inserted parts | `input` | `cluster` |  | `"5-8"` |  |  | assumed: bucket key of merge_result_size_distribution (0.2.0) | `ok` |
| `merge_result_size_distribution` | `merges` | Fraction of natural merges per result size bucket (result rows / inserted part rows) | `measured` | `cluster` | `1` |  | `{"merge_size_ratio": [18.21, 472.95], "insert_rows": [1000, 100000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-lifecycle (0.2.0) | `ok` |
| `acquisition_rule` | `replication` | Fetch vs local execution of a replicated merge, in order: skip when the part or a covering one is already here; fetch when always_fetch_merged_part; try to fetch a TTL-recompress merge another replica was assigned, quietly retrying while no replica has it, until try_fetch_recompressed_part_timeout; fetch when execute_merges_on_single_replica_time_threshold > 0, the entry is younger than it and another replica is picked; fetch when a source or patch part is missing or covered; while a source part has newer metadata, try to fetch, quietly retrying; fetch when the entry is at least prefer_fetch_merged_part_time_threshold old, the sources hold at least prefer_fetch_merged_part_size_threshold bytes on disk and a replica has the result; with zero-copy, fetch or wait on the covering part and the exclusive lock; else merge locally. A mutation: fetch when always_fetch_mutated_part, the source is missing or covered, or the entry is that old and 1.1 x the source bytes on disk (expired TTL parts excluded) reach the size threshold with a replica holding the result; the single-replica branch never fires (merge entries only); zero-copy as for merges; else mutate locally, keeping the source part type. | `pinned` | `cluster` |  | `"local_unless_fetch_condition"` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeFromLogEntryTask.cpp:224 (26.8@09d0772a842) | `ok` |
| `prefer_fetch_merged_part_time_threshold` | `replication` | Age of a merge or mutation entry from which (inclusive) a replica prefers fetching a large result | `pinned` | `cluster` | `s` | `3600` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1284 (26.8@09d0772a842) | `ok` |
| `prefer_fetch_merged_part_size_threshold` | `replication` | Source bytes on disk above which an old merge entry is fetched | `pinned` | `cluster` | `B` | `10737418240` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1294 (26.8@09d0772a842) | `ok` |
| `execute_merges_on_single_replica_time_threshold` | `replication` | Seconds the other replicas wait for the chosen replica's merge result (0 = off) | `pinned` | `cluster` | `s` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1304 (26.8@09d0772a842) | `ok` |
| `always_fetch_merged_part` | `replication` | Replica never merges and fetches every merge result (0 = off) | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1341 (26.8@09d0772a842) | `ok` |
| `always_fetch_mutated_part` | `replication` | Replica never mutates and fetches every mutation result (0 = off) | `pinned` | `cluster` | `count` | `0` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeSettings.cpp:1348 (26.8@09d0772a842) | `ok` |
| `provisioned_size_gib` | `local_disk` | Manual provisioning: size of each volume | `input` | `volume` | `GiB` |  |  |  | assumed: manual allocation; read only when volumes_per_replica is a number (0.2.0) | `ok` |
| `provisioned_iops` | `local_disk` | Manual provisioning: IOPS of each volume | `input` | `volume` | `iops` | `3000` |  |  | assumed: manual allocation; default is the gp3 baseline; read only when volumes_per_replica is a number (0.2.0) | `ok` |
| `provisioned_mibps` | `local_disk` | Manual provisioning: throughput of each volume | `input` | `volume` | `mibps` | `125` |  |  | assumed: manual allocation; default is the gp3 baseline; read only when volumes_per_replica is a number (0.2.0) | `ok` |
| `volumes_per_replica` | `local_disk` | Volumes per replica: auto (cheapest feasible layout) or a count for manual provisioning | `input` | `replica` |  | `"auto"` |  |  | assumed: auto picks the cheapest feasible layout of the billing contract (0.2.0) | `ok` |
| `ebs_headroom` | `ebs` | Free space provisioned above the active data, as a fraction of it (outdated parts until old_parts_lifetime, merge results before their sources go, metadata, growth) | `input` | `volume` | `1` | `0.2` |  |  | assumed: not measured: rig parts are small, so the rig peak over active bytes does not carry to production sizes; 0.2 is an operator choice (0.2.0) | `ok` |
| `ebs_io_rule` | `ebs` | EBS I/O accounting on SSD volumes: an I/O counts up to 256 KiB, larger ones split; each request is counted on its own (no credit for merged sequential I/O), an upper bound of IOPS | `pinned` | `volume` |  | `"ssd_256kib_split_no_merge_credit"` |  |  | provider: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html (checked 2026-10-24) (2026-10-24) | `ok` |
| `ebs_max_io_bytes` | `ebs` | Largest I/O an SSD volume counts as one IOPS | `pinned` | `volume` | `B` | `262144` |  |  | provider: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html (checked 2026-10-24) (2026-10-24) | `ok` |
| `io_op` | `ebs` | Operation key of the EBS demand tables | `input` | `cluster` |  | `"inserts"` |  |  | assumed: table key (0.2.0) | `ok` |
| `io_direction` | `ebs` | Direction key of the EBS demand tables | `input` | `cluster` |  | `"write"` |  |  | assumed: table key (0.2.0) | `ok` |
| `io_size_bucket` | `ebs` | Request-size bucket (power-of-two upper bound) | `input` | `cluster` |  | `"1MiB"` |  |  | assumed: table key (0.2.0) | `ok` |
| `io_bytes_per_op` | `ebs` | Volume bytes per byte the operation produces (merge, insert) or scans (read), by direction | `measured` | `volume` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05; page cache not dropped (no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle baseline (embedded Keeper on r1), read demand is traced syscall bytes, an upper bound of cold device reads) (0.2.0) | `ok` |
| `io_request_size_distribution` | `ebs` | Fraction of volume requests per size bucket, by operation and direction | `measured` | `volume` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05; page cache not dropped (no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle baseline (embedded Keeper on r1), read demand is traced syscall bytes, an upper bound of cold device reads) (0.2.0) | `ok` |
| `ebs_ios_per_mib` | `ebs` | EBS I/Os per MiB of volume demand under ebs_io_rule, by operation and direction | `measured` | `volume` | `io/MiB` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05; page cache not dropped (no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle baseline (embedded Keeper on r1), read demand is traced syscall bytes, an upper bound of cold device reads) (0.2.0) | `ok` |
| `peak_factor` | `ebs` | Peak over mean of the 1-s write rate during natural merges | `measured` | `replica` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-ebs (local policy, scale 0.05; page cache not dropped (no root): write demand is /proc/&lt;pid>/io write_bytes net of the idle baseline (embedded Keeper on r1), read demand is traced syscall bytes, an upper bound of cold device reads) (0.2.0) | `ok` |
| `large_objects_per_part` | `inserts` | Part files larger than the 32 MiB first upload buffer, per inserted part (each one multipart upload) | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutations_per_month` | `mutations_deletes` | ALTER UPDATE/DELETE and lightweight DELETE statements per month | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutated_parts_per_month` | `mutations_deletes` | Parts rewritten by mutations per month | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `mutation_kind` | `mutations_deletes` | Kind of the mutations | `input` | `cluster` |  | `"update_column"` |  |  | assumed: preset default (0.3.0) | `ok` |
| `select_part_reads_per_day` | `reads_cache` | Parts read by SELECT queries per day (sum of SelectedParts) | `input` | `cluster` | `count/day` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `read_streams_per_part` | `reads_cache` | Column streams a query reads per part (a Nullable column is 2, length(arr) is 1) | `input` | `cluster` | `count` | `3` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cache_miss_fraction` | `reads_cache` | Share of part reads the filesystem cache misses (1: every read is cold) | `input` | `cluster` | `1` | `1` | `{"cache_miss_fraction": [0, 1]}` |  | assumed: preset default (0.3.0) | `ok` |
| `cache_size_gib` | `local_disk` | Filesystem cache size of each replica | `input` | `cluster` | `GiB` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `restarts_per_month` | `topology_recovery` | Server restarts per replica per month (intact state) | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `missed_parts_per_restart` | `topology_recovery` | Parts a restarted replica fetches to catch up | `input` | `cluster` | `count` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `variant_id` | `topology_recovery` | Model variant a per-variant coefficient table is indexed by | `pinned` | `cluster` |  | `"s3"` |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeData.cpp:6399 (26.8@09d0772a842) | `ok` |
| `startup_op` | `topology_recovery` | S3 operation of a startup request row | `pinned` | `cluster` |  | `"GetObject"` |  |  | source: tmp/src-26.8/src/Disks/IDisk.cpp:254 (26.8@09d0772a842) | `ok` |
| `object_size_bucket` | `inserts` | Object size bucket: small files, up to 1 MiB, up to the 32 MiB first upload buffer, beyond | `pinned` | `cluster` |  | `"le_4KiB"` |  |  | source: tmp/src-26.8/src/IO/WriteBufferFromS3.cpp:188 (26.8@09d0772a842) | `ok` |
| `parts_inserted_per_month` | `inserts` | Parts inserted per month: insert_rate x 2 628 000 s x parts_per_insert | `derivation` | `cluster` | `count` |  |  | `insert_rate × 2628000 × parts_per_insert` | source: tmp/src-26.8/src/Interpreters/Squashing.cpp:395 (26.8@09d0772a842) | `ok` |
| `merge_results_per_month` | `merges` | Merge results per month: parts_inserted_per_month x (1 - 1 / merge_size_ratio) / (merge_effective_fan_in - 1) | `derivation` | `cluster` | `count` |  | `{"merge_size_ratio": [18.21, 472.95]}` | `parts_inserted_per_month × (1 − 1 / merge_size_ratio) / (merge_effective_fan_in − 1)` | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `merge_sources_per_month` | `merges` | Merge source parts per month: merge_results_per_month x merge_effective_fan_in | `derivation` | `cluster` | `count` |  |  | `merge_results_per_month × merge_effective_fan_in` | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `expired_parts_per_month` | `mutations_deletes` | Parts leaving retention per month at their final size: parts_inserted_per_month / merge_size_ratio | `derivation` | `cluster` | `count` |  |  | `parts_inserted_per_month / merge_size_ratio` | assumed: preset default (0.3.0) | `ok` |
| `active_parts` | `topology_recovery` | Active parts per replica: data_bytes x insert_rate x 86 400 s/day x parts_per_insert / (ingest_rate x merge_size_ratio) | `derivation` | `cluster` | `count` |  |  | `data_bytes × insert_rate × 86400 × parts_per_insert / (ingest_rate × merge_size_ratio)` | assumed: preset default (0.3.0) | `ok` |
| `objects_per_part` | `schema` | S3 objects per part: one per part file (9 metadata files, data files per column stream, skp_idx.packed, projection parts) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `object_size_distribution` | `schema` | Fraction of a part's objects per size bucket | `measured` | `cluster` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `insert_tx_checks` | `inserts` | Requests an insert transaction sends per part besides its object writes | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `bytes_per_get` | `reads_cache` | Bytes per GetObject of a cold read (statistics reads and column ranges together) | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `stats_gets_per_column` | `reads_cache` | GetObject per column per part read: statistics.packed is read once per column statistics file | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `gets_per_stream_read` | `reads_cache` | GetObject per stream read per part: the .bin range and its marks file | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `shape_columns` | `schema` | Columns of a table shape (the statistics files a part read loads) | `pinned` | `cluster` | `count` |  |  |  | source: tmp/src-26.8/src/Storages/Statistics/Statistics.cpp:933 (26.8@09d0772a842) | `ok` |
| `cached_miss_gets_per_part_read` | `reads_cache` | GetObject per part read that misses the filesystem cache (segments) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `cached_miss_bytes_per_part_read` | `reads_cache` | Bytes downloaded per part read that misses the filesystem cache | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `cache_fill_io_per_byte` | `local_disk` | Cache-volume bytes allocated per byte the cache downloads | `measured` | `cluster` | `1` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `metadata_bytes_per_object` | `local_disk` | Local metadata bytes allocated per S3 object (one small file each) | `measured` | `cluster` | `B` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `metadata_iops_per_object_write` | `local_disk` | Local write calls per object written (the part's metadata files) | `measured` | `cluster` | `req` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `metadata_iops_per_part_write` | `local_disk` | Local write calls per part written besides its objects | `measured` | `cluster` | `req` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `startup_gets_per_part` | `topology_recovery` | GetObject per active part when a server loads its tables | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `startup_requests` | `topology_recovery` | Requests a server start sends besides part loading (disk access checks, table files) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `merge_gets_per_source_part` | `merges` | GetObject per merged source part without the cache: Compact columns + 3, Wide 2 x columns + 1 | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `merge_gets_per_source_part_cached` | `merges` | GetObject per merged source part through the filesystem cache | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-s3 (0.3.0) | `ok` |
| `delete_calls_per_removed_part` | `mutations_deletes` | DeleteObjects calls per removed part (one batch of up to 1000 keys) | `pinned` | `cluster` | `count` |  |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:609 (26.8@09d0772a842) | `ok` |
| `empty_part_objects` | `mutations_deletes` | Objects of the empty part that covers a dropped or expired part | `pinned` | `cluster` | `count` |  |  |  | source: tmp/src-26.8/src/Storages/StorageMergeTree.cpp:2742 (26.8@09d0772a842) | `ok` |
| `mutation_entry_objects` | `mutations_deletes` | Objects written per mutation statement besides parts (mutation_&lt;n>.txt) | `pinned` | `cluster` | `count` |  |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MergeTreeMutationEntry.cpp:73 (26.8@09d0772a842) | `ok` |
| `mutation_objects_written` | `mutations_deletes` | Objects written per mutated part (narrow shape): Compact rewrites all, Wide the changed streams and 7 metadata files | `pinned` | `cluster` | `count` |  |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MutateTask.cpp:908 (26.8@09d0772a842) | `ok` |
| `mutation_gets_per_part` | `mutations_deletes` | GetObject per mutated part (narrow shape) | `pinned` | `cluster` | `count` |  |  |  | source: tmp/src-26.8/src/Storages/MergeTree/MutateTask.cpp:1247 (26.8@09d0772a842) | `ok` |
| `catchup_requests_per_missed_part` | `topology_recovery` | GetObject per part a restarted replica fetches to catch up (replication report) | `measured` | `cluster` | `count` |  |  |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-replication (0.3.0) | `ok` |
| `cas_key_families` | `cas` | CAS key families: blob, blob_meta, manifest, ref_stream, ref_ckpt, ns_files, ref_catalog (foreground state); lease, gc_hb, gc, pool_meta, roots (maintenance only) | `pinned` | `pool` |  | `"cas_layout_26_8"` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h:397 (26.8@09d0772a842) | `ok` |
| `cas_gc_interval_s` | `cas` | GC round interval (cas_gc_interval_sec) | `input` | `pool` | `s` | `60` | `{"cas_gc_interval_s": [10, 60]}` |  | assumed: default tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:58 (0.3.0) | `ok` |
| `gc_scheduler_rule` | `cas` | GC scheduling: a round starts cas_gc_interval_sec after the previous round finished; only the GC leader pulses the heartbeat, every interval / 4 and once per round on acquiring the lease | `pinned` | `pool` |  | `"wait_interval_after_round"` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.cpp:349 (26.8@09d0772a842) | `ok` |
| `gc_heartbeat_requests_per_s` | `cas` | GC heartbeat requests per second (GET and PUT of gc/hb per pulse; per-round pulses add 2 per round) | `derivation` | `pool` | `1/s` |  |  | `2 / (cas_gc_interval_s / 4)` | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.cpp:81 (26.8@09d0772a842) | `ok` |
| `gc_round_redelete_budget` | `cas` | Blob deletes a GC round may issue (cas_gc_round_redelete_budget; 0 = unbounded) | `pinned` | `pool` | `count` | `5000` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:66 (26.8@09d0772a842) | `ok` |
| `staging_rule` | `cas` | Staging: staging_backend = local (default) writes every blob file of a part to the scratch directory while hashing; the files are uploaded at commit and removed after it | `pinned` | `pool` |  | `"local_scratch_until_commit"` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:984 (26.8@09d0772a842) | `ok` |
| `staging_peak_bytes_rule` | `cas` | Staging peak: the blob file bytes of the parts committing at the same time (each insert or merge stages its whole part) | `pinned` | `pool` |  | `"sum_of_committing_part_blob_bytes"` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:593 (26.8@09d0772a842) | `ok` |
| `cas_branch_requests` | `cas` | Requests per blob file by entry branch: fresh HEAD + PUT body + PUT .meta; hash hit HEAD + GET .meta; adoption and inline none | `pinned` | `pool` |  | `"head_then_publish_or_observe"` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp:397 (26.8@09d0772a842) | `ok` |
| `cas_publish_requests` | `cas` | Requests to publish a part: manifest PUT and GET, then precommit and promote ref transactions (catalog GET, _log PUT, _ckpt GET, _ckpt PUT each) | `pinned` | `pool` | `count` | `10` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp:646 (26.8@09d0772a842) | `ok` |
| `cas_ref_requests_per_rename` | `cas` | Requests of the rename ref transaction of a new part (catalog GET, _log PUT, _ckpt GET, _ckpt PUT) | `pinned` | `pool` | `count` | `4` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp:3320 (26.8@09d0772a842) | `ok` |
| `cas_foreground_ref_removal_requests` | `cas` | Requests of DROP TABLE SYNC: per part up to one ref transaction (4 requests); per table the terminal transaction, a catalog PUT, 3 table-file requests and 2 LISTs (9 more) | `pinned` | `pool` | `count` | `9` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp:3428 (26.8@09d0772a842) | `ok` |
| `cas_hash_hit_fraction` | `cas` | Fraction of a part's blob files whose bytes the pool already holds (hash hits) | `input` | `pool` | `1` | `0` | `{"cas_hash_hit_fraction": [0, 1]}` |  | assumed: preset default: no repeated content (0.3.0) | `ok` |
| `cas_blob_bytes` | `cas` | Mean bytes of a blob file | `input` | `pool` | `B` | `1048576` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_mutations_per_s` | `cas` | Mutations (ALTER UPDATE/DELETE) per second | `input` | `cluster` | `1/s` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_parts_per_mutation` | `cas` | Parts one mutation rewrites | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_mutated_columns` | `cas` | Columns one mutation rewrites | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_table_drops_per_s` | `cas` | DROP TABLE per second | `input` | `cluster` | `1/s` | `0` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_parts_per_dropped_table` | `cas` | Active parts of a dropped table | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_mount_renew_period_s` | `cas` | Mount lease renewal period (cas_mount_renew_period_ms / 1000) | `pinned` | `pool` | `s` | `10` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:74 (26.8@09d0772a842) | `ok` |
| `cas_pool_objects` | `cas` | Objects in the CAS pool (blobs, .meta siblings, manifests) | `input` | `pool` | `count` | `5000` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_write` | `cas` | Kind of part write (key of cas_branch_population) | `input` | `pool` |  | `"insert_compact"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_branch` | `cas` | Entry branch of a part file | `input` | `pool` |  | `"fresh"` |  |  | assumed: table key (0.3.0) | `ok` |
| `gc_round_term` | `cas` | Term of the GC round request and duration tables | `input` | `pool` |  | `"deferred_round"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_mutation_marks_hit_fraction` | `cas` | Fraction of a mutated column's marks files whose bytes are unchanged (a hash hit; fixed-width CODEC(NONE) columns) | `input` | `cluster` | `1` | `0` | `{"cas_mutation_marks_hit_fraction": [0, 1]}` |  | assumed: preset default: compressed marks change with the data (0.3.0) | `ok` |
| `cas_manifest_bytes` | `cas` | Bytes of one part manifest (inline small files included) | `input` | `pool` | `B` | `13000` |  |  | assumed: measured 13 019 B (5-column Compact) and 13 751 B (5-column Wide) in cas-branches (0.3.0) | `ok` |
| `gc_round_graduation_budget` | `cas` | Blobs a GC round may graduate to delete_pending (cas_gc_round_graduation_budget; 0 = unbounded) | `pinned` | `pool` | `count` | `5000` |  |  | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp:65 (26.8@09d0772a842) | `ok` |
| `cas_table_cleanup_period_s` | `cas` | Seconds between two cleanup-thread listings of one table: idle, max_cleanup_delay_period 300 s + U(0, 10 s); under load runs every cleanup_delay_period (30 s) but the listing waits merge_tree_clear_old_temporary_directories_interval_seconds (60 s) | `input` | `cluster` | `s` | `305` | `{"cas_table_cleanup_period_s": [60, 310]}` |  | assumed: idle value from src/Storages/MergeTree/IMergeTreeCleanupThread.cpp:134-149, MergeTreeCleanupThread.cpp:43-47, MergeTreeData.cpp:4028, MergeTreeSettings.cpp:1682,1685; the 60 s floor under load is an assumption (0.3.0) | `ok` |
| `cas_tables` | `cas` | Tables on the CAS disk with inserts | `input` | `cluster` | `count` | `1` |  |  | assumed: preset default (0.3.0) | `ok` |
| `cas_ref_log_tail_records` | `cas` | Ref log records of one table since its last snapshot that a folding GC round reads | `input` | `pool` | `count` | `128` | `{"cas_ref_log_tail_records": [0, 256]}` |  | assumed: half the snapshot threshold of 256 records (tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h:296): the tail grows from 0 to 256 between snapshots (0.3.0) | `ok` |
| `gc_stage` | `cas` | GC reclamation stage | `input` | `pool` |  | `"blob_delete"` |  |  | assumed: table key (0.3.0) | `ok` |
| `cas_objects_per_part` | `cas` | Blob files per part (the rest of the part is inline in its manifest) | `measured` | `pool` | `count` |  | `{"insert_rows": [100, 4000000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_branch_population` | `cas` | Part files per entry branch for one part write | `measured` | `pool` | `count` |  | `{"insert_rows": [100, 4000000]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `gc_round_requests` | `cas` | Requests of one GC round (excluding heartbeat pulses outside the round) | `measured` | `pool` | `count` |  | `{"cas_pool_objects": [1017, 9949]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `gc_round_duration` | `cas` | Duration of one GC round | `measured` | `pool` | `ms` |  | `{"cas_pool_objects": [1017, 9949]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `lease_requests_per_s` | `cas` | Mount lease renewals per second per server: one If-Match PUT per renew period, start to start | `derivation` | `replica` | `1/s` |  |  | `1 / cas_mount_renew_period_s` | source: tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp:690 (26.8@09d0772a842) | `ok` |
| `reclaim_delay_periods` | `cas` | GC round periods from retirement to each reclamation stage (seconds = periods x (interval + round duration)) | `measured` | `pool` | `1` |  | `{"cas_gc_interval_s": [10, 60]}` |  | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas (0.3.0) | `ok` |
| `cas_blob_uploads_per_part` | `cas` | Distinct blob uploads of an inserted part (cas_objects_per_part at the shape and part type) | `derivation` | `cluster` | `count` |  |  | `lookup(cas_objects_per_part)[shape, part_type]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient cas_objects_per_part (0.3.0) | `ok` |
| `cas_merge_result_blob_uploads` | `cas` | Distinct blob uploads of a level-1 merge result: Wide once fan-in x part bytes reach min_bytes_for_wide_part (FutureMergedMutatedPart.cpp:25-32) | `derivation` | `cluster` | `count` |  |  | `lookup(cas_objects_per_part)[shape, if merge_effective_fan_in × part_bytes_uncompressed < min_bytes_for_wide_part then compact else wide]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient cas_objects_per_part (0.3.0) | `ok` |
| `reclaim_periods_blob_delete` | `cas` | GC round periods from retirement to the blob delete | `derivation` | `pool` | `1` |  |  | `lookup(reclaim_delay_periods)[blob_delete]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient reclaim_delay_periods (0.3.0) | `ok` |
| `gc_round_requests_fold_get` | `cas` | GC round requests: fold get | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[fold_get]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_requests_fold_put` | `cas` | GC round requests: fold put | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[fold_put]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_requests_fold_list` | `cas` | GC round requests: fold list | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[fold_list]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_requests_fold_get_per_manifest` | `cas` | GC round requests: fold get per manifest | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[fold_get_per_manifest]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_requests_deferred_get` | `cas` | GC round requests: deferred get | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[deferred_get]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_requests_deferred_put` | `cas` | GC round requests: deferred put | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[deferred_put]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_requests_deferred_list` | `cas` | GC round requests: deferred list | `derivation` | `pool` | `count` |  |  | `lookup(gc_round_requests)[deferred_list]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_requests (0.3.0) | `ok` |
| `gc_round_duration_fold_base` | `cas` | GC fold round duration: fold base | `derivation` | `pool` | `ms` |  |  | `lookup(gc_round_duration)[fold_base]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_duration (0.3.0) | `ok` |
| `gc_round_duration_fold_per_part` | `cas` | GC fold round duration: fold per part | `derivation` | `pool` | `ms` |  |  | `lookup(gc_round_duration)[fold_per_part]` | measured: docs/superpowers/reports/2026-10-24-storage-cost-cas coefficient gc_round_duration (0.3.0) | `ok` |

| Precedence group | Locked by default | Rules |
|---|---|---|
| `data_bytes`, `ingest_rate`, `retention_days` | `ingest_rate`, `retention_days` | `data_bytes` = `ingest_rate × retention_days`; `ingest_rate` = `data_bytes / retention_days`; `retention_days` = `data_bytes / ingest_rate` |
<!-- inputs:end -->

## Break-even {#break-even}

<!-- breakeven:begin -->
Generated from `breakeven.json` (`tools/sweeps.mjs`), swept on contract version `3f57c696fe5e5ff4` with price preset `aws-us-east-1`. Do not edit.

A crossing is a sign change of the difference of two totals between consecutive ladder values, so the break-even lies inside the interval, not at a point. A variant that is not `ok` at a value has no total there and no crossing is drawn through it.

Ladders: ×1/4, ×1/2, ×1, ×2, ×4, ×8 of each scenario's value, clamped to the input's measured range; whole numbers for counts; the scenario value is always a point.

### Scenario `logs` {#breakeven-logs}

Logs.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 3,750, 7,500, 15,000, 30,000, 60,000, 120,000 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |
| `ingest_rate` | 125, 250, 500, 1,000, 2,000, 4,000 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |
| `replicas` | 1, 2, 4, 6 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |

### Scenario `metrics` {#breakeven-metrics}

Metrics.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 450, 900, 1,800, 3,600, 7,200, 14,400 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable; `s3plain`: unavailable; `cas`: unsupported_cut |
| `ingest_rate` | 5, 10, 20, 40, 80, 160 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable; `s3plain`: unavailable; `cas`: unsupported_cut |
| `replicas` | 1, 2, 4, 6 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable; `s3plain`: unavailable; `cas`: unsupported_cut |

### Scenario `analytics_batch` {#breakeven-analytics-batch}

Analytics batch.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 2,560, 5,120, 10,240, 20,480, 40,960, 81,920 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |
| `ingest_rate` | 7.0137, 14.0274, 28.0548, 56.1096, 112.219, 224.438 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |
| `replicas` | 1, 2, 4, 6 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |

### Scenario `single_replica_archive` {#breakeven-single-replica-archive}

Single replica archive.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 12,800, 25,600, 51,200, 102,400, 204,800, 409,600 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |
| `ingest_rate` | 17.5342, 35.0685, 70.137, 140.274, 280.548, 561.096 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |
| `replicas` | 1, 2, 4, 6 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: ok; `s3plain`: unavailable; `cas`: unsupported_cut |

### Scenario `custom` {#breakeven-custom}

Custom.

| Input | Values | Variants compared | Crossing | States |
|---|---|---|---|---|
| `data_bytes` | 5,000, 10,000, 20,000, 40,000, 80,000, 160,000 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable; `s3plain`: unavailable; `cas`: unsupported_cut |
| `ingest_rate` | 125, 250, 500, 1,000, 2,000, 4,000 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable; `s3plain`: unavailable; `cas`: unsupported_cut |
| `replicas` | 1, 2, 3, 6 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable; `s3plain`: unavailable; `cas`: unsupported_cut |
<!-- breakeven:end -->
