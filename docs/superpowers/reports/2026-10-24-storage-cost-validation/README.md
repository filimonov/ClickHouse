---
description: 'Validation of the storage cost calculator research: fresh hold-outs for consumed edges, CAS shape and threshold cases, natural-merge and acquisition-split validation on every remote variant, recovery wiring, the hold-out ledger and the open items.'
sidebar_label: 'Storage cost validation'
sidebar_position: 60
slug: /superpowers/reports/storage-cost-validation
title: 'Storage cost calculator — validation and hold-out ledger'
doc_type: 'reference'
---

# Storage cost calculator — validation and hold-out ledger {#validation}

Every hold-out the research reports consumed now has a done replacement on its variant, except `acquire_bytes` on CAS and `peak_factor` on EBS (both explained below). The CAS shape and threshold cases close the two pending CAS support entries. Natural merges reproduce the lifecycle fit on `s3` and `s3plain`: 11 merges, amplification 2.55 against the model's 2.38, inside the 0.25 tolerance. On CAS the three passes FAILED (0/3 ok, edges `consumed`): the card's own snapshot reads reach the store after the cutoff; the merge history in those FAILED records is identical. The acquisition split is inserts 1.0, merges 0.0, mutations 0.0 on every variant. The report holds 129 records, 117 ok. The 12 FAILED records are two falsified hold-outs and the CAS natural-merge passes, which fail on the card's own snapshot reads; they stay as findings.

| Item | Value |
|---|---|
| Binary | ClickHouse 26.8 build: download sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46`, executed sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` |
| Source commit | `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, the source of the measured binary: `antalya-26.8` merged into the CAS disk-transactions branch. Every `path:line` cite is at this commit |
| Report runs | `cost-20261024T154049Z`, `cost-20261024T162519Z` (the CAS point hold-outs) and `cost-20261024T164439Z` (the 1 000-row fetch); `measurements.jsonl` holds their 129 records, 117 ok |
| Rig | all runs execute the cards from a detached worktree at `9ed2efcd450`, so every record names one rig commit; manifests and predictions come from this directory and, for `faults.multipart-abort` and `faults.multipart-abort-part3c`, from the replication report |
| Earlier runs | `cost-20261024T145603Z` (calibration) and `cost-20261024T151251Z` (first report run); §[Cases](#cases) says what each found. Neither is in `measurements.jsonl` |
| Predictions | at `849c483095f`, before any run of their case; `types-holdout-2`, `merge-30k` and the revised `cas_cached` fit counts at `03f8c0489a2`, after the first run; the CAS point hold-outs at `168f71aea78`; the replication report's fault predictions at `2c006846dbc`, `0bc1e557850` and `bac037b336d` |
| Store | RustFS behind the ledger proxy; every ok record replays from `raw/<card>/<run_id>/ledger.jsonl.gz` |
| Cuts | `gc-under-load.default` and `amplification-run.2h`, cut to meet the campaign deadline (D14 in [LEDGER.md](../2026-10-23-storage-cost-calculator/LEDGER.md#decisions)): no run; coverage `status: cut` |

## Plan of runs {#plan}

Inventory at the start (`coverage.json` at `1f5326cdc6d`, `closure_check.py --mode final`):

| Item | Edges | Placement in this run |
|---|---|---|
| `acquire_requests`, `acquire_bytes` hold-out `replication-fetch.insert-wide` consumed on `s3plain`, `cas` | 4 | fresh `replication-fetch.insert-3` |
| `local_exec_requests` hold-out `replication-local.merge-8` consumed on `s3plain`, `cas` | 2 | fresh `replication-local.merge-40k` |
| `retry_outcome_mix` hold-out `faults.put-5xx-nth7` consumed on `s3plain`, `cas` | 2 | fresh `faults.put-5xx-nth5` |
| `failed_upload_residue` hold-outs consumed: `part3` on `s3plain`, `part3b` on `cas` | 2 | fresh `faults.multipart-abort-75k` (`s3plain`), `faults.multipart-abort-part3c` (`cas`, predicted in the replication report) |
| `failed_upload_residue` fit `faults.multipart-abort` on `cas`: planned | 1 | rerun with the current card |
| `gc_round_requests`, `gc_round_duration` validation `gc-under-load.default`: planned | 2 | cut (D14) |
| Pending support: `cas` `storage` and `inserts` × `objects_per_part_by_shape` | 2 | `objects-per-part.*` on `cas`, `cas_cached` |
| Natural lifecycle validation on every remote variant | — | `merge-natural.validation`, `replication-natural-fit.validation` on `s3`, `s3plain`, `cas` |

Cases (K = 3 each, one invocation per run):

| Case | Policies | Role | Coefficient |
|---|---|---|---|
| `objects-per-part.narrow-compact`, `.narrow-wide`, `.wide-compact`, `.wide-wide`, `.types-fit-compact`, `.types-fit-wide` | `cas`, `cas_cached` | fit | `cas_objects_per_part` |
| `objects-per-part.types-holdout` | `cas`, `cas_cached` | hold-out (consumed in the first run) | `cas_objects_per_part` |
| `objects-per-part.types-holdout-2` | `cas`, `cas_cached` | bound check (validation) | `cas_objects_per_part` |
| `objects-per-part.narrow-wide-2`, `.wide-wide-2`, `.types-fit-wide-2` | `cas` | hold-out (second run) | `cas_objects_per_part` |
| `boundaries.compact-below-bytes`, `.wide-above-bytes` | `cas` | fit | `part_type_predicate` |
| `boundaries.wide-equal-bytes` | `cas` | hold-out | `part_type_predicate` |
| `replication-fetch.insert-3` | `s3plain`, `cas` | hold-out | `acquire_requests`, `acquire_bytes` |
| `replication-local.merge-40k` | `s3plain`, `cas` | hold-out (`s3plain` consumed in the first run) | `local_exec_requests` |
| `replication-local.merge-30k` | `s3plain` | hold-out (fresh) | `local_exec_requests` |
| `replication-fetch.insert-1k` | `s3`, `s3plain`, `cas` | validation (fourth run) | `acquire_requests` |
| `faults.put-5xx-nth5` | `s3plain`, `cas` | hold-out | `retry_outcome_mix` |
| `faults.multipart-abort-75k` | `s3plain` | hold-out | `failed_upload_residue` |
| `faults.multipart-abort` | `s3`, `s3plain`, `cas` | fit on `cas`; validation on `s3`, `s3plain` | `failed_upload_residue` |
| `faults.multipart-abort-part3c` | `cas` | hold-out | `failed_upload_residue` |
| `merge-natural.validation` | `s3`, `s3plain`, `cas` | validation | `merge_effective_fan_in`, `merge_fan_in_distribution`, `merge_result_size_distribution` |
| `replication-natural-fit.validation` | `s3`, `s3plain`, `cas` | validation | `fetch_fraction` |

The validation cases repeat the fit workloads with seed 20261025; every other new case changes a parameter of a
consumed one (rows, parts, source rows, the faulted request).

## Cases and verdicts {#cases}

Runs `cost-20261024T154049Z`, `cost-20261024T162519Z` and `cost-20261024T164439Z`, K = 3 per case and policy. Role is the coverage edge; `consumed` marks a falsified
hold-out or a failed validation.

| Case | Policy | ok / passes | Role |
|---|---|---|---|
| `boundaries.compact-below-bytes` | `cas` | 3 / 3 | fit |
| `boundaries.wide-above-bytes` | `cas` | 3 / 3 | fit |
| `boundaries.wide-equal-bytes` | `cas` | 3 / 3 | holdout |
| `faults.multipart-abort` | `cas` | 3 / 3 | fit |
| `faults.multipart-abort` | `s3` | 3 / 3 | validation |
| `faults.multipart-abort` | `s3plain` | 3 / 3 | validation |
| `faults.multipart-abort-75k` | `s3plain` | 3 / 3 | holdout |
| `faults.multipart-abort-part3c` | `cas` | 3 / 3 | holdout |
| `faults.put-5xx-nth5` | `cas` | 3 / 3 | holdout |
| `faults.put-5xx-nth5` | `s3plain` | 3 / 3 | holdout |
| `merge-natural.validation` | `cas` | 0 / 3 | validation (consumed) |
| `merge-natural.validation` | `s3` | 3 / 3 | validation |
| `merge-natural.validation` | `s3plain` | 3 / 3 | validation |
| `objects-per-part.narrow-compact` | `cas` | 3 / 3 | fit |
| `objects-per-part.narrow-compact` | `cas_cached` | 3 / 3 | fit |
| `objects-per-part.narrow-wide` | `cas` | 3 / 3 | fit |
| `objects-per-part.narrow-wide` | `cas_cached` | 3 / 3 | fit |
| `objects-per-part.types-fit-compact` | `cas` | 3 / 3 | fit |
| `objects-per-part.types-fit-compact` | `cas_cached` | 3 / 3 | fit |
| `objects-per-part.types-fit-wide` | `cas` | 3 / 3 | fit |
| `objects-per-part.types-fit-wide` | `cas_cached` | 3 / 3 | fit |
| `objects-per-part.types-holdout` | `cas` | 0 / 3 | holdout (consumed) |
| `objects-per-part.types-holdout` | `cas_cached` | 0 / 3 | holdout (consumed) |
| `objects-per-part.types-holdout-2` | `cas` | 3 / 3 | validation |
| `objects-per-part.types-holdout-2` | `cas_cached` | 3 / 3 | validation |
| `objects-per-part.narrow-wide-2` | `cas` | 3 / 3 | holdout |
| `objects-per-part.wide-wide-2` | `cas` | 3 / 3 | holdout |
| `objects-per-part.types-fit-wide-2` | `cas` | 3 / 3 | holdout |
| `objects-per-part.wide-compact` | `cas` | 3 / 3 | fit |
| `objects-per-part.wide-compact` | `cas_cached` | 3 / 3 | fit |
| `objects-per-part.wide-wide` | `cas` | 3 / 3 | fit |
| `objects-per-part.wide-wide` | `cas_cached` | 3 / 3 | fit |
| `replication-fetch.insert-3` | `cas` | 3 / 3 | holdout, holdout (consumed) |
| `replication-fetch.insert-3` | `s3plain` | 3 / 3 | holdout |
| `replication-fetch.insert-1k` | `cas` | 3 / 3 | validation |
| `replication-fetch.insert-1k` | `s3` | 3 / 3 | validation |
| `replication-fetch.insert-1k` | `s3plain` | 3 / 3 | validation |
| `replication-local.merge-30k` | `s3plain` | 3 / 3 | holdout |
| `replication-local.merge-40k` | `cas` | 3 / 3 | holdout |
| `replication-local.merge-40k` | `s3plain` | 0 / 3 | holdout (consumed) |
| `replication-natural-fit.validation` | `cas` | 3 / 3 | validation |
| `replication-natural-fit.validation` | `s3` | 3 / 3 | validation |
| `replication-natural-fit.validation` | `s3plain` | 3 / 3 | validation |

The 12 FAILED records stay in `measurements.jsonl` as findings:

- **`objects-per-part.types-holdout`** (`cas`, `cas_cached`): `PutObject` 244, predicted 298..570. That is 57.5
  distinct uploads per part of 139 blob candidates (0.41), below the band's floor (0.52, the smallest fitted Wide
  ratio). Falsified in the first run, consumed. Its replacement `types-holdout-2` is a bound check; the point
  hold-outs of the value are `narrow-wide-2`, `wide-wide-2` and `types-fit-wide-2` (§[CAS shapes](#cas-shapes)).
- **`replication-local.merge-40k`** on `s3plain`: the prediction listed `r2` only, and the card enforces exact
  counts on `s3plain`, so `r1`'s own merge requests (`GetObject` 32, `HeadObject` 1, `PutObject` 13) were
  unpredicted. A prediction defect found in the first run; consumed; fresh hold-out `merge-30k` predicts both replicas.
- **`merge-natural.validation`** on `cas`: `window.no_workload_after_cutoff` lists 171 requests after the cutoff.
  All are observer reads (`GetObject` of part manifests and the ref catalog, `HeadObject` and `ListObjectsV2` of
  the roots) of the card's own `system.remote_data_paths` snapshot, which reaches the store on a CAS disk; none is a
  `PutObject`. The merge history in the records' partial observations equals `s3` and `s3plain` (§[Per-variant
  validation](#per-variant)). The edges are `consumed`; the gap is in §[Gaps](#gaps).

Earlier runs, not in `measurements.jsonl`:

| Run | Rig | What it found |
|---|---|---|
| `cost-20261024T145603Z` | working tree at `8bda2351d65` | calibration, K = 1, `cas` fit cases: the CAS shape counts below; every pass ok |
| `cost-20261024T151251Z` | `42f76e40755` | the two falsified hold-outs above; `cas_cached` snapshots read both disk names (fit predictions revised); `merge-natural` raised on CAS inline files (`system.remote_data_paths` lists them with an empty remote path; fixed in `03f8c0489a2`, test `test_layout_skips_files_without_an_object`). Every other case matched run 2 |

## CAS shapes and thresholds {#cas-shapes}

Distinct blob uploads per part = (`PutObject` − 7 × parts) / 2 (publish `PutObject` 7 per part; a fresh blob is
`HeadObject`, `PutObject` of the body and of its `.meta`; `Pool/CasPartWriteTxn.cpp:397`). Blob candidates are the
part's files ending in `.bin`, `.mrk`, `.mrk2`, `.mrk3`, `.cmrk`, `.cmrk2` or `.cmrk3`, plus `primary.idx` and
`data.packed` (`ContentAddressedTransaction.cpp:77`), counted from the `s3` layout of the same manifest. Every other
file (Compact `data.cmrk4` and `primary.cidx` included) is inline unless it outgrows the inline cap, in which case it
spills to a blob (`:998`); no file of these cases did.

| Case | Parts | Candidates per part | Uploads per part | Ratio | `cas` HEAD / GET / LIST | `cas_cached` HEAD / GET / LIST |
|---|---|---|---|---|---|---|
| `narrow-compact` | 3 | 1 | 1 | 1.00 | 9 / 40 / 6 | 15 / 55 / 12 |
| `wide-compact` | 3 | 1 | 1 | 1.00 | 9 / 40 / 6 | 15 / 55 / 12 |
| `types-fit-compact` | 1 | 2 | 2 | 1.00 | 8 / 20 / 6 | 14 / 31 / 12 |
| `narrow-wide` | 3 | 10 | 8 | 0.80 | 36 / 46 / 6 | 42 / 61 / 12 |
| `wide-wide` | 3 | 80 | 41.33 | 0.52 | 132 / 42 / 6 | 138 / 57 / 12 |
| `types-fit-wide` | 1 | 135 | 75 | 0.56 | 81 / 20 / 6 | 87 / 31 / 12 |
| `types-holdout` (consumed) | 2 | 139 | 57.5 | 0.41 | 158 / 67 / 6 | 164 / 80 / 12 |
| `types-holdout-2` (bound check) | 2 | 139 | 57.5 | 0.41 | 158 / 67 / 6 | 164 / 80 / 12 |
| `narrow-wide-2` (hold-out, 300 000 rows) | 3 | 10 | 8 | 0.80 | 36 / 46 / 6 | — |
| `wide-wide-2` (hold-out, 40 000 rows) | 3 | 80 | 41.33 | 0.52 | 132 / 42 / 6 | — |
| `types-fit-wide-2` (hold-out, 90 000 rows) | 1 | 135 | 75 | 0.56 | 81 / 20 / 6 | — |

- **Collapse.** A blob whose bytes equal an earlier blob of the pool is not uploaded again. Mark files of streams
  whose compressed blocks have equal sizes are byte-equal: random `UInt64` columns compress to equal block sizes,
  so 40 of them share a few mark files. The count therefore depends on the data, not only on the shape.
- **Value.** `cas_objects_per_part` is a formula over `columns`, `streams_per_column`, `projections` and
  `cas_marks_blob_fraction` (CAS report, [Coefficients](../2026-10-24-storage-cost-cas/README.md#coefficients)).
  These cases verify it: with the generators' mark fractions it gives narrow Compact 1, narrow Wide 8, wide
  Compact 1, wide Wide 41.3333, types Compact 2, types Wide 75, every measured count. Data whose mark files never
  coincide reaches the candidate count (narrow Wide 10 in `cas-branches.fresh` and `.adoption`; wide 80, types
  135): that is `cas_marks_blob_fraction` = 1, the default.
- **Hold-outs.** `narrow-wide-2`, `wide-wide-2` and `types-fit-wide-2` run the fit generators at other row counts,
  predicted with the fit counts at `168f71aea78` before their first run (`cost-20261024T162519Z`): all nine passes
  are exact. `types-holdout-2` (a different schema, predicted at `03f8c0489a2`) checks only that uploads lie in
  [42, 139] (measured 57.5); its edges are `validation`, not hold-out.
- **Observer reads.** The `objects-per-part` window contains the card's `system.remote_data_paths` snapshot. On a
  CAS disk it sends `HeadObject` 6, `ListObjectsV2` 6 and `GetObject` 9 per table plus `GetObject` 2 per part
  (the `GetObject` 10 per table and per part above include them); `cas_cached` lists both disk names and doubles
  them. No cell prices these records' `GetObject`, `HeadObject` or `ListObjectsV2`; only the uploads feed the value.
- **Thresholds.** `boundaries.compact-below-bytes` (1 blob: `HeadObject` 1, `PutObject` 9, `GetObject` 8),
  `wide-above-bytes` and the hold-out `wide-equal-bytes` (10 blobs: 10, 27, 8) are exact on `cas` in all repeats;
  the part types follow `part_type_predicate` as on `s3`.

## Per-variant validation {#per-variant}

### Write amplification and fan-in {#amplification}

`merge-natural.validation`: 64 inserts of 100 000 rows, seed 20261025. The model is `rewrite_levels` at the
lifecycle fit `merge_effective_fan_in` = 6.1437 (abs 0.2502) over each record's final part sizes.

| Variant | Records ok | Merges | Fan-in mean | Amplification measured | Model | Residual | TV fan-in / result size |
|---|---|---|---|---|---|---|---|
| `s3` | 3 / 3 | 11 | 6.364, 6.273, 6.364 | 2.5522, 2.6143, 2.5522 | 2.3755, 2.4336, 2.3755 | +0.177, +0.181, +0.177 | 0.067 / 0.087 |
| `s3plain` | 3 / 3 | 11 | 6.364 | 2.5522 | 2.3755 | +0.177 | 0.067 / 0.087 |
| `cas` | 0 / 3 | 11 | 6.364 | 2.5522 | 2.3755 | +0.177 | 0.067 / 0.087 |

- **Same on every variant.** The merge selector does not read the disk: all nine records merged 64 parts in 11
  merges, and eight ended with the final sizes 23, 23, 6, 6, 6 (inserted parts). `s3` repeat 2 ended with 27, 23, 6,
  6, 1, 1. Residuals are inside the fit tolerance 0.2502; distributions are inside their total-variation limits
  (0.3426 and 0.3852).
- **Difference.** `cas` is measured but not `ok`: the observer reads after the cutoff fail the window assertion.
  Its values come from the FAILED records' partial observations; they are not evidence edges.
- **R4-2.** Every `ok` record carries `quiet_cutoff_us` and `cutoff_us`. Replaying
  `window.no_workload_after_cutoff` from `raw/merge-natural/cost-20261024T154049Z/ledger.jsonl.gz`
  (`replay_cutoff.py` in this directory) gives 0 workload attempts after the cutoff in all six `s3` and `s3plain` records,
  equal to the recorded assertion; the cutoff lies 32–39 ms before the window end.

### Acquisition split {#acquisition-split}

`replication-natural-fit.validation`: 24 inserts of 100 000 rows, one `ALTER UPDATE`, natural merges on two
replicas, seed 20261025.

| Variant | Records ok | Fetch fraction: inserts | merges | mutations | Follower merges / mutated parts |
|---|---|---|---|---|---|
| `s3` | 3 / 3 | 1.0 | 0.0 | 0.0 | 4 / 2 |
| `s3plain` | 3 / 3 | 1.0 | 0.0 | — (mutations refused) | 4 / 0 |
| `cas` | 3 / 3 | 1.0 | 0.0 | 0.0 | 4 / 2 |

The split equals `fetch_fraction` (inserts 1.0, merges and mutations 0.0) on every variant. The only difference is
`s3plain`, which refuses mutations (`cards/s3plain.py` `GATES['mutations']`).

## Fresh hold-outs {#holdouts-fresh}

Verdicts follow the replication report: the K = 3 mean against coefficient × units with the coefficient's
tolerance; sender rows subtract the originator's own reads of the matching `replication-local` case (CAS: 3
`GetObject` per result).

| Coefficient | Case | Policy | Cell | Predicted | Measured | Tolerance | Holds |
|---|---|---|---|---|---|---|---|
| `acquire_requests` | `replication-fetch.insert-3` | `cas` | receiver `GetObject` per part | 8.83 | 8.44 | 1.0 | yes |
| `acquire_requests` | `replication-fetch.insert-3` | `cas` | receiver `PutObject` per part | 7.0 | 7.0 | 1.0 | yes |
| `acquire_requests` | `replication-fetch.insert-3` | `cas` | receiver `ListObjectsV2` per part | 0.5 | 0.33 | 1.0 | yes |
| `acquire_requests` | `replication-fetch.insert-3` | `cas` | sender `GetObject` per part | 2.5 | 2.33 | 1.0 | yes |
| `acquire_bytes` | `replication-fetch.insert-3` | `cas` | relay bytes / bytes on disk | 0.00881 | 0.00591 | rel 0.05 | **no** |
| `acquire_requests` | `replication-fetch.insert-3` | `s3plain` | receiver `GetObject`, `PutObject`, `HeadObject` per part | 9, 13, 1 | 9, 13, 1 | 1.0 | yes |
| `acquire_requests` | `replication-fetch.insert-3` | `s3plain` | sender `GetObject` per part | 11 | 11 | 1.0 | yes |
| `acquire_bytes` | `replication-fetch.insert-3` | `s3plain` | relay bytes / bytes on disk | 1.0008 | 1.0005 | rel 0.05 | yes |
| `local_exec_requests` | `replication-local.merge-40k` | `cas` | `GetObject`, `PutObject`, `HeadObject` per merge | 9.67, 7, 1 | 9.67, 7, 1 | 1.0 | yes |
| `local_exec_requests` | `replication-local.merge-30k` | `s3plain` | `GetObject`, `PutObject`, `HeadObject` per merge, both replicas | 32, 13, 1 | 32, 13, 1 | 1.0 | yes |
| `retry_outcome_mix` | `faults.put-5xx-nth5` | `cas` | rejected `PutObject` attempts | +1 over `faults.none` (9) | 10 | 0 | yes |
| `retry_outcome_mix` | `faults.put-5xx-nth5` | `s3plain` | rejected `PutObject` attempts | +1 over `faults.none` (13) | 14 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3c` | `cas` | incomplete bytes after the window | 0 | 0 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-75k` | `s3plain` | incomplete bytes after the window | 0 | 0 | 0 | yes |

- **Small parts.** `replication-fetch.insert-1k` (three 1 000-row parts, predicted at `c69414e314a`, run
  `cost-20261024T164439Z`) gives the same per-part requests: `s3` sender 11, receiver `PutObject` 11 and
  `GetObject` 9; `s3plain` 11, 13, 9 and `HeadObject` 1; `cas` receiver `GetObject` 8.44, `PutObject` 7, sender 2.33.
  `acquire_requests` validity becomes `insert_rows` 1 000–200 000 (it was `part_rows` 50 000–200 000, a value the
  presets do not resolve).
- **CAS relink bytes are per part, not per byte.** The relay carried 19 152 B per part at 100 000 rows,
  19 234 B at 150 000 rows and 20 158 B for one 300 000-row Wide part: the relink ships the part manifest, whose
  size follows the file count. `acquire_bytes[cas]` (a ratio) falsifies at 150 000 rows; the edge is `consumed`
  with no replacement (§[Gaps](#gaps)).
- **CAS multipart faults.** `faults.multipart-abort` (fit, 2 upload parts) and `-part3c` (hold-out, 4 parts) ran
  with the current card: every repeat aborted all 8 republished uploads (`UploadPart` 40 and 56, as predicted), and
  `ListParts` answers `NoSuchUpload` for each (`aborted_uploads_gone_in_store`). `failed_upload_residue[cas]` = 0;
  validity `upload_parts` [2, 4]. The `s3` and `s3plain` reruns of `faults.multipart-abort` agree with the
  replication report (validation edges).

## Wiring {#wiring}

| Cell | Change | Commit |
|---|---|---|
| `s3` `recovery` | the `not_supported` state for missed parts is gone; `GetObject` and `PutObject` rows add restarts × replicas × `missed_parts_per_restart` × `catchup_requests_per_missed_part[s3][op]` for replicas > 1 | `766a9e5b7eb` |
| `s3` `recovery` startup rows | kept: at 3 active narrow parts they give `GetObject` 50, `PutObject` 2, `DeleteObject` 2; the replicated restart measured 48, 1, 1 (`startup_requests_after_restart[s3]`, tolerance 2) | `766a9e5b7eb` |
| registry `catchup_requests_per_missed_part` | table index `[variant_id, replication_op]`, as the coefficient | `766a9e5b7eb` |
| `s3` `inserts` | the follower's 9 load reads per fetched part (`acquire_requests[s3][insert][receiver][GetObject]`, validity `insert_rows` 1 000–200 000 after `replication-fetch.insert-1k`) | `5f51532da31`, withdrawn in `feca627456f`, restored after review |
| `catchup_requests_per_missed_part` validity | `missed_parts_per_restart` [0, 5]: the rows are exactly 0 at 0 missed parts, and both restart cases fetched 5 | review finding 3 ([REVIEW-HISTORY.md](REVIEW-HISTORY.md)) |

Not wired, because the page could not check its validity against the presets (`presets.js` `coverageProblems`
checks every reached coefficient):

- **`cas` `recovery`**: `startup_requests_after_restart[cas]` was measured at 3 active parts (`active_parts`, which
  the presets do not resolve). Wired at `766a9e5b7eb` and removed again. The CAS report later measured server starts on 2–5 parts
  (`cas_startup_requests`, `cas_startup_gets_per_part`), so the cell is priced at `058646a31af`.

## Hold-out ledger {#ledger}

Every coefficient whose hold-out the research reports consumed, with its replacement (`coverage.json` at the report commit). A `_cached` policy measures the same variant, so `cas_cached` rows fold into `cas` (and `s3_cached`, `s3plain_cached` into theirs):

| Coefficient | Variant | Consumed | Replacement (status) |
|---|---|---|---|
| `acquire_bytes` | `cas` | `replication-fetch.insert-3`, `replication-fetch.insert-wide` | none on this variant |
| `acquire_bytes` | `s3plain` | `replication-fetch.insert-wide` | `replication-fetch.insert-3` (done) |
| `acquire_requests` | `cas` | `replication-fetch.insert-wide` | `replication-fetch.insert-3` (done) |
| `acquire_requests` | `s3plain` | `replication-fetch.insert-wide` | `replication-fetch.insert-3` (done) |
| `cas_branch_population` | `cas` | `cas-branches.mix-holdout` | `cas-branches.mix-holdout-2` (done) |
| `cas_objects_per_part` | `cas` | `cas-branches.mix-holdout`, `objects-per-part.types-holdout` | `cas-branches.inline`, `cas-branches.mix-holdout-2`, `objects-per-part.narrow-wide-2`, `objects-per-part.types-fit-wide-2`, `objects-per-part.wide-wide-2` (done) |
| `delete_calls_per_removed_part` | `s3` | `drop.partition-3` | `drop.partition-2` (done) |
| `empty_part_objects` | `s3` | `drop.partition-3` | `drop.partition-2` (done) |
| `failed_upload_residue` | `cas` | `faults.multipart-abort-part3b` | `faults.multipart-abort-part3c` (done) |
| `failed_upload_residue` | `s3plain` | `faults.multipart-abort-part3` | `faults.multipart-abort-75k` (done) |
| `gc_round_requests` | `cas` | `gc-scale.objects-30k` | `gc-scale.objects-3k`, `gc-scale.objects-5k` (done) |
| `io_bytes_per_op` | `ebs` | `io-sizes.mixed-holdout`, `io-sizes.mixed-holdout-2`, `io-sizes.types-holdout` | `io-sizes.mixed-holdout-3` (done) |
| `io_request_size_distribution` | `ebs` | `io-sizes.mixed-holdout`, `io-sizes.mixed-holdout-2`, `io-sizes.types-holdout` | `io-sizes.mixed-holdout-3` (done) |
| `local_exec_requests` | `cas` | `replication-local.merge-8` | `replication-local.merge-40k` (done) |
| `local_exec_requests` | `s3plain` | `replication-local.merge-40k`, `replication-local.merge-8` | `replication-local.merge-30k` (done) |
| `mutation_compact_gets_per_full_granule` | `s3` | `mutation.compact-30k` | `mutation.compact-20k`, `mutation.compact-45k` (done) |
| `mutation_gets_per_part` | `s3` | `mutation.update-mixed`, `mutation.update-mixed-2`, `mutation.update-mixed-3` | `mutation.compact-20k`, `mutation.compact-45k`, `mutation.update-mixed-4` (done) |
| `mutation_objects_written` | `s3` | `mutation.update-mixed`, `mutation.update-mixed-2`, `mutation.update-mixed-3` | `mutation.compact-20k`, `mutation.compact-45k`, `mutation.update-mixed-4` (done) |
| `peak_factor` | `ebs` | `merge-natural.wide` | none on this variant |
| `plain_dir_remove_requests` | `s3plain` | `drop.partition-3` | `drop.partition-2`, `merge-only.wide-wide-removal` (done) |
| `plain_empty_part_objects` | `s3plain` | `drop.partition-3` | `drop.partition-2` (done) |
| `plain_meta_objects_per_part` | `s3plain` | `objects-per-part.types-holdout` | `drop.partition-2`, `merge-only.narrow-compact-removal`, `merge-only.wide-wide-removal`, `merge-only.wide-wide-sources-3`, `objects-per-part.wide-wide`, `startup.wide-parts` (done) |
| `plain_objects_per_part` | `s3plain` | `objects-per-part.types-holdout` | `objects-per-part.wide-wide` (done) |
| `plain_unlink_requests` | `s3plain` | `drop.partition-3` | `drop.partition-2`, `merge-only.wide-wide-removal` (done) |
| `reclaim_delay_periods` | `cas` | `gc-lifecycle.drop-60s` | `gc-lifecycle.drop-60s-12`, `gc-lifecycle.merge` (done) |
| `retry_outcome_mix` | `cas` | `faults.put-5xx-nth7` | `faults.put-5xx-nth5` (done) |
| `retry_outcome_mix` | `s3plain` | `faults.put-5xx-nth7` | `faults.put-5xx-nth5` (done) |

- **No replacement on two variants.** `acquire_bytes` on `cas`: the ratio model is wrong for a relink
  (§[Gaps](#gaps)). `peak_factor` on `ebs`: the EBS report made it an assumed input
  (`docs/superpowers/reports/2026-10-24-storage-cost-ebs/README.md`).

## Gaps per coefficient {#gaps}

| Coefficient | Gap |
|---|---|
| `cas_objects_per_part` | data-dependent through `cas_marks_blob_fraction`: no rule predicts which mark files coincide. The default 1 prices the candidate count (narrow Wide 10, wide 80, types 135: 1.25x, 1.94x, 1.8x the generators'), an upper bound; a user sets the share from `system.remote_data_paths` |
| `acquire_bytes[cas]` | the ratio model does not hold: relay bytes are about 19–20 KB per part whatever the part's bytes; the `cas` keys are removed from the table. CAS prices interserver bytes per part with `cas_relink_bytes_per_part` (replication report) |
| `merge_effective_fan_in`, `merge_fan_in_distribution`, `merge_result_size_distribution` | validated on `s3` and `s3plain`; on `cas` the records fail on the card's own snapshot reads, with identical merge history |
| `failed_upload_residue` | 0 on every variant within `upload_parts` 2–4; a part answered after the abort started stays in the replay's upper bound (replication report) |
| `local_exec_requests` | per merge of 4 Compact sources; `GetObject` scales with sources (8 per source on `s3`, `s3plain`), so the per-merge value holds at 4 sources only |
| EBS `io_bytes_per_op` for `mutations`, `deletes`, `reads`, `recovery` | not measured: the `S3Card` operation cards do not trace syscalls (`cards/base.py:62`), so a pass on `local` sees `write_bytes` only. Placed on the EBS report (Tier 1), D17: write bytes from `/proc` in one untraced ops run on `local`, reads as a derivation from the s3 cold-read bytes and the D16 floor |

## Carried items {#carried}

| Item | Disposition |
|---|---|
| closure: test classes after `unittest.main()` | moved above the guard (`8bda2351d65`) |
| closure: a coefficient with no evidence row | a measured coefficient with no fit or hold-out row leaves every supported entry of its cells pending (`8bda2351d65`, `677b6caee15`) |
| closure: `pinned_ok` coarse | documented: one validation row of a quantity or coefficient the cell reads backs the entry; validation rows no longer add families (`677b6caee15`) |
| runner: ports at lock hand-over | a pass polls its endpoints for up to 120 s before failing (`8bda2351d65`, tests in `test_cost_cluster.py`) |
| lifecycle R4-2 | closed by the replay in §[Amplification](#amplification) |
| phase-0 README wording | `amplification-run` is 30 h of P2 |
| D14 cuts | `gc-under-load.default`: case and edges `cut`; `amplification-run.2h` was already cut |
| leftovers | removed: the two detached worktrees left by the s3 report's earlier runs, and the untracked `raw/*/cost-20261024T07*` and `raw/merge-forced/` slices of the s3 report |
| s3 `inserts` follower reads (reviewer question) | priced (§[Wiring](#wiring)) |

## Open items {#open-items}

| Item | Placement |
|---|---|
| EBS `io_bytes_per_op` for `mutations`, `deletes`, `reads`, `recovery` | EBS report (Tier 1), D17 |
| CAS natural-merge validation | Tier 2: the lifecycle card's snapshot reads must leave the window, or be excluded as observer traffic, before the cutoff assertion can hold on CAS |
| CAS uploads per part for other data | Tier 2: a mark-collapse rule from compressed block sizes, or a measured count per data profile |
| `acquire_bytes[cas]` as bytes per part | Tier 2: refit as bytes per part (manifest size per file) |
| snapshot reads inside `S3Card` windows on CAS | Tier 2: snapshot after the window end; today no cell prices them |
| `cas` `recovery` | done: the CAS report's `startup.*` cases (`058646a31af`) |

## Review {#review}

Review findings and their dispositions: [REVIEW-HISTORY.md](REVIEW-HISTORY.md).

## Reproduce {#reproduce}

Run from a sparse worktree at the rig commit `9ed2efcd450`. `$BINARY` is the 26.8 binary with the sha256 above;
`$SOURCE` is a ClickHouse checkout at `09d0772a842`.

```bash
git worktree add --detach ../rig-9ed2efcd450 9ed2efcd450
utils/cas-yardstick/rustfs-bin.sh ensure
cd ../rig-9ed2efcd450 && python3 utils/cas-yardstick/yardstick.py cost run \
  --binary "$BINARY" --source-dir "$SOURCE" \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-validation \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-replication \
  --cases "$(ls docs/superpowers/reports/2026-10-24-storage-cost-validation/experiments | sed 's/\.json$//' | paste -sd,),faults.multipart-abort,faults.multipart-abort-part3c" \
  --policies s3,s3plain,cas,cas_cached --replicas 2 --repeats 3 --runs-dir "$RUNS_DIR"
```

`cost run` takes `tmp/yardstick.lock` itself. Exporting a run into this directory is a manual step: [Reproduce](../../cost-model/storage-cost-model.md#reproduce), step 3.
