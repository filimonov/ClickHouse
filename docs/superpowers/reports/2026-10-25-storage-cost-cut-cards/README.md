---
description: 'Cards cut from the first measurement pass and run later: paced natural merges (amplification and the EBS peak factor), each predicted before its run.'
sidebar_label: 'Storage cost cut cards'
sidebar_position: 58
slug: /superpowers/reports/storage-cost-cut-cards
title: 'Storage cost calculator — cards cut from the first pass'
doc_type: 'reference'
---

# Storage cost calculator — cards cut from the first pass {#cut-cards}

Cards that the first pass cut for time (D14 in [LEDGER.md](../2026-10-23-storage-cost-calculator/LEDGER.md#decisions)),
each predicted and committed before its run. Results:

| Item | Verdict | Model change |
|---|---|---|
| Paced natural merges (§[Amplification](#amplification-result)) | rule fails: amplification 8.8 against 4.6 | `merge_effective_fan_in` valid for 50 000–100 000-row inserts only |
| Chain-regime amplification law (§[Hold-out](#amplification-holdout)) | fails its hold-out: 8.258 against 7.754 ± 0.4465 | none; the law is evidence only |
| Chain amplification constant (§[Constant](#amplification-constant)) | fails its hold-out at 8/s: 9.228 against 8.258–8.816 | none; the registry row notes the measured 1.8–2.0× |
| EBS `peak_factor` (§[Peak bound](#peak-bound)) | constant fails: 28.2, 15.5 and 19.8; the peak is the largest merge | stays assumed, default 1; `ebs_peak_mibps_bound` shown as a note on the EBS merges cell |
| `cas_relink_bytes_per_part` (§[Relink](#relink-derivation)) | constant fails below 30 000 rows; revised form passes | registry derivation from part type, granules and rows (basis fitted); the constant is retired |
| `inserts_per_async_flush` (§[Async](#async-revised)) | rule fails below 20/s; revised rule passes | registry rule returns 1 when 1 / rate ≥ `async_insert_busy_timeout_min_ms` |
| `gc-under-load` (§[GC](#gc-under-load)) | nothing reclaimed within `old_parts_lifetime`, as predicted | none |
| s3plain big-file removal (§[Big file](#big-file)) | exact | none; the `plain_unlink_requests` edge is done |

- **Rig:** commit `df8d6526e3e` (a detached checkout; `rig_dirty` false), RustFS 1.0.0-beta.9, binary
  `tmp/bin/26.8/clickhouse`.
- **Not run:** `gc-scale.mounts-2`, `gc-scale.refgrowth-*`, `detach-attach`, `replacement`, `faults.fetch` and
  `cache-eviction` have no card or need card changes, which would change the rig commit of this report.

## Paced natural merges {#amplification}

`amplification-run.2h` (one 1 000-row insert a second for 2 hours) is shortened to 10 minutes at a faster tempo:

- **`amplification-run.2hz-10m`:** 1 200 inserts, 0.5 s apart, on `local`.
- **`amplification-run.4hz-10m`:** 2 400 inserts, 0.25 s apart, on `local` and `s3`.

Predictions:

- **Amplification:** the registry rule `rewrite_levels_at_weighted_ratio` at fan-in 6.1437 over each record's final
  parts, ± 0.2502, the tolerance of the `amplification-run.2h` validation (`b28a65478d1`).
- **Peak factor** (`local` only, checked offline with `ebs_fit.peak_factors`): the registry constant 1 ± 0.105 is
  expected to fail, because paced inserts set the mean and one merge sets the 1-s peak. The alternative states that
  the peak interval holds the largest merge: between half its write and 1.2 × (the two largest merges + one second
  of inserts).

### Result {#amplification-result}

Run `cost-20261025T130354Z` (rig commit `df8d6526e3e`, a detached checkout; binary `tmp/bin/26.8/clickhouse`, sha256
`b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668`, source
`09d0772a842c5d73cf3ef35b4c8b017d838215a0`; RustFS 1.0.0-beta.9). All three records are `FAILED` on two assertions: amplification and the
result-size distribution. Fan-in, part counts, rows and quiescence held.

| Record | Merges | Max level | Amplification, bytes | Rule ± 0.2502 | Rows |
|---|---|---|---|---|---|
| `local`, 2/s, 1 200 inserts | 232 | 25 | 8.751 | 4.592 | 12.38 |
| `local`, 4/s, 2 400 inserts | 466 | 25 | 8.807 | 4.597 | 12.46 |
| `s3`, 4/s, 2 400 inserts | 466 | 25 | 8.807 | 4.597 | 12.46 |

- **Amplification under paced inserts is about 1.9× the rule.** The selector keeps folding a few new small parts into
  the largest part: level 25 for a part of 1 053 inserts, where log₆ 1 053 ≈ 3.9 levels. Results of 33–128 inserts
  make 60 % of the merges (fit: 5–8 inserts, 83 %). The tempo does not matter: 2/s and 4/s give 8.75 and 8.81.
- **The merge sequence is a chain.** Result sizes in inserts run 6, 11, 16, … 66, then 72, 78, … 114: each merge
  folds the next 5 or 6 single inserts into the growing part, about 22 merges to reach 114–125 inserts. Rows of a
  chain of C inserts are rewritten about C / (2 × 5) times, 11–12 at C ≈ 120; chains then merge at 125, 191 and
  1 053 inserts. This is an observation of these runs (5 `UInt64` columns, 1 000-row inserts), not a fitted rule.
- **`local` and `s3` merge identically** (same merges, levels and bytes), so the result holds for every variant
  that prices merges from `write_amplification_rule`.
- **`merge_effective_fan_in` is right; the level rule is not.** Mean fan-in 6.14–6.16 sits in the fit range, but
  rows are rewritten 11.4 times, not 3.6. The 1-hour-scale evidence is now two tempos; the rule stays as it is and
  the coverage edges are `consumed`.

Peak factor (r1 `write_bytes`, window start to the last merge, net of the idle 66 492 B/s):

| Record | Peak, B/s | Mean, B/s | Peak factor | Constant 1 ± 0.105 | Largest merge, B | Largest-merge bound |
|---|---|---|---|---|---|---|
| `local`, 2/s | 23 195 673 | 823 177 | 28.2 | fail | 22 747 333 | 11.4–38.1 MB: pass |
| `local`, 4/s | 23 998 120 | 1 548 902 | 15.5 | fail | 22 749 547 | 11.4–55.4 MB: pass |

- **The 1-s peak is one merge, not a property of the rate.** Doubling the tempo doubled the mean and left the peak at
  the largest merge, so the factor halved. `peak_factor` is not a constant; it stays an assumed input with default 1
  (provision for the mean rate, and let a merge run longer within the provisioned throughput).
- **What a reader can use:** a volume provisioned for peaks needs the bytes of the largest merge per second, about
  the largest part size of the table; the calculator has no input for it.

### Chain-regime law and its hold-out {#amplification-holdout}

A refit of the ladder to the chain records fails its hold-out by 0.06 beyond its slack. `write_amplification_rule`
keeps its form; `merge_effective_fan_in` is now valid for inserts of 50 000–100 000 rows only, so presets with
smaller inserts show amplification as extrapolated.

**Regime.** On local disk the selector folds 32 KB parts (1 000 rows) into one growing part 5–6 at a time; 2 MB
parts (100 000 rows) merge as a tree of about 6. The same 1 000-row burst on `s3` also built a tree (amplification
3.98–4.09, which the rule fits). The chain therefore needs small parts and merges that finish before the next few
inserts arrive.

**Fit.** The ladder `1 + rewrite_levels(r, f)` at the row-weighted final part size r, least squares over three chain
records:

| Record | r, inserts | Measured | Law, f = 2.3348 | Residual | Rule, f = 6.1437 |
|---|---|---|---|---|---|
| `small-inserts.default`, `local` ([EBS](/superpowers/reports/storage-cost-ebs)), 16/s burst | 353.4 | 8.332 | 7.885 | +0.4465 | 4.102 |
| `amplification-run.2hz-10m`, `local` | 937.4 | 8.751 | 9.046 | −0.2950 | 4.592 |
| `amplification-run.4hz-10m`, `local` | 943.5 | 8.807 | 9.051 | −0.2439 | 4.597 |

**Hold-out** (`amplification-run.1hz-10m`, `local`, 600 inserts of 1 000 rows, one per second; prediction
`aedc61d87ad`, slack = the largest fit residual, 0.4465):

| Run | r, inserts | Merges | Measured | Law | Verdict | Rule |
|---|---|---|---|---|---|---|
| `cost-20261025T144517Z` | 325.0 | 116 | 8.258 | 7.754 | fail by 0.058 | 4.078 |

Every other card assertion passed: fan-in mean 6.15, the fan-in and result-size distributions within the pooled chain
tables.

- **Chain amplification hardly depends on r.** Measured 8.26, 8.33, 8.75 and 8.81 at r = 325, 353, 937 and 944.
  The ladder adds a level for every factor f of r, so a single f cannot match both ends. The flat part is the chain
  itself: it stops near 120 inserts (a 3–4 MB part), where the selector's fixed cost per merge stops favouring the
  growing part, and its rewrite count (about C / (2 × 5)) is set by part bytes, not by r.
- **What a reader can use:** for steady inserts of about 1 000 rows, expect write amplification 8.3–8.8 on local
  disk and at RustFS speed, about twice `write_amplification_rule`. A law for other part sizes needs runs at
  other part sizes; none were run.

### Chain constant and its hold-out at 8/s {#amplification-constant}

A second law, a constant for 1 000-row inserts, also fails its hold-out. `write_amplification_rule` keeps its form;
its registry row now notes the measured gap.

**Law.** Amplification of 1 000-row inserts that merges keep up with is a constant: 8.5371, the mean of the four chain
records (16/s local burst, 1/s, 2/s, 4/s). Residuals −0.2791…+0.2703; tolerance = the largest, 0.2791.

**Hold-out** (`amplification-run.8hz-10m`, 4 800 inserts at 8/s, `local` and `s3`; prediction `ff164ba5339`):

| Run | Policy | Merges | Largest final part, inserts | Measured | Predicted | Verdict | Rule |
|---|---|---|---|---|---|---|---|
| `cost-20261025T160242Z` | `local` | 934 | 3 823 | 9.228 | 8.258–8.816 | fail | 5.233 |
| `cost-20261025T160242Z` | `s3` | 934 | 3 823 | 9.228 | 8.258–8.816 | fail | 5.233 |

The fan-in distribution also exceeded its bound (TV 0.0173 against 0.0156). The `s3` record has 31 ledger rows past
the window cutoff.

- **Amplification is not constant: it rises 0.4–0.5 each time the largest part grows about threefold.** Largest
  final part and amplification: 398 inserts and 8.26 (600 inserts), 1 053 and 8.75–8.81 (1 200–2 400), 3 823 and
  9.23 (4 800). The chain sets a floor near 8.3; merges of chain results add to it as the table grows. A law needs
  the largest part as a function of the inserts per partition, which these runs do not cover past 4 800 inserts.
- **Measured against the rule:** 8.26–9.23 against 4.08–5.23 at 1–8 inserts/s, 1.8–2.0× on `local` and `s3`
  alike. Presets with 1 000-row inserts price merge writes at about half the measured value; the
  `merge_effective_fan_in` range narrowing marks them extrapolated.

## CAS relink bytes per part {#relink}

A CAS fetch by relink sends the part manifest, and the manifest carries every part file except `.bin`, marks and
`primary.idx` inline (`partFileMustStayBlob`, `ContentAddressedTransaction.cpp:67` @ `09d0772a842`). The largest
inline file is `statistics.packed`, whose `uniq` sketches grow with the rows of a small part: 9 189 B at 1 000 rows,
15 087 B at 100 000 rows. That explains the 1 000-row hold-out (13 030 B against the constant 19 428.8 B).

Form, fitted on the 15 records of 100 000–300 000 rows (`insert`, `insert-3`, `insert-wide`, `merge`, `mutation`;
residuals within 0.02 %):

```
relink bytes per part = 2547 + 16.65 x granules + 23 x (files - 11) + inline bytes
```

Inline bytes come from a `clickhouse local` reproduction of the same inserts with the same binary; its parts have
the records' `bytes_on_disk` exactly. The 1 000-row record, not in the fit, sits within 0.11 %.

Hold-outs, `relay0.from_target` for 3 fetched parts, one pass each:

| Case | Constant 19 428.8 B ± 5 % | Form ± 1 % |
|---|---|---|
| `replication-fetch.relink-1k` (seen before: tests the constant) | 55 372–61 201 | 39 134 |
| `replication-fetch.relink-3k` | 55 372–61 201 | 49 013 |
| `replication-fetch.relink-10k` | 55 372–61 201 | 55 429 |
| `replication-fetch.relink-30k-wide` (Wide, 19 files) | 55 372–61 201 | 58 382 |

### Result {#relink-result}

Run `cost-20261025T133848Z`, all four records `ok`:

| Case | Measured B | Per part | Constant | Form |
|---|---|---|---|---|
| `relink-1k` | 39 091 | 13 030 | −32.9 %: fail | −0.11 %: pass |
| `relink-3k` | 48 977 | 16 326 | −16.0 %: fail | −0.07 %: pass |
| `relink-10k` | 55 357 | 18 452 | −5.03 %: fail | −0.13 %: pass |
| `relink-30k-wide` | 59 991 | 19 997 | +2.9 %: pass | +2.76 %: fail |

- **The constant fails below 30 000 rows.** `cas_relink_bytes_per_part` stays `assumed`; its edges are `consumed`.
- **Compact parts follow the form.** Bytes above the inline files are 2 549–2 556 at 1–2 granules and grow about
  17 B per granule.
- **Wide parts do not.** Bytes above the inline files are 3 334 at 30 000 rows (4 granules) and 3 347 at 300 000
  rows (37 granules): no granule term. The single Wide fit record had put it into `23 × (files − 11)`.

### Revised form and its hold-out {#relink-revised}

Refit on all 10 relink cases (residuals within 0.06 %), predicted in `07a862e8e08` before run
`cost-20261025T135134Z`:

```
Compact: relink bytes per part = 2530.9 + 17.49 x granules + inline bytes
Wide:    relink bytes per part = 3340.5 + inline bytes
```

| Case | Predicted B | Measured B | Deviation (± 1 %) |
|---|---|---|---|
| `relink-50k` (Compact, 7 granules) | 56 918 | 57 004 | +0.15 %: pass |
| `relink-10k-wide` (Wide, 2 granules) | 58 756 | 58 725 | −0.05 %: pass |

- **The revised form has a passed hold-out** from 1 000 to 300 000 rows, Compact and Wide, 5 columns. Wide parts of
  other column counts are not measured; the Wide constant may carry a per-file term.
- **Inline bytes are mostly `statistics.packed`:** 9.2 kB at 1 000 rows, 15.1 kB at 100 000 rows, for 5
  `UInt64` columns. The calculator has no input for them, so `cas_relink_bytes_per_part` stays the assumed scalar,
  an upper bound for 5-column parts above 30 000 rows and too high by up to a third below.

### Registry derivation {#relink-derivation}

`cas_relink_bytes_per_part` is now a registry derivation (basis fitted, valid for 1 000–300 000-row inserts). The
calculator has every input it needs: `part_type`, `part_rows` and `index_granularity`. The inline file bytes become a
saturating curve of rows, and the constants are refitted with the curve in place of the measured inline bytes:

```
relink(rows) = base + inline(rows)
base         = Compact: 2571.3 + 13.85 × ceil(rows / index_granularity);  Wide: 3798.0
inline(rows) = 16516.5 − 17470.6 × 530 / (rows + 530)
```

- **The curve.** Least squares over the mean inline bytes of the 8 Compact cases (1 000–200 000 rows); residuals
  −119…+259 B. Inline bytes are 63 % of the ceiling at 1 000 rows and within 2 % of it from 30 000 rows. Wide
  parts read 325–608 B above the curve; the Wide base absorbs it.
- **The constants.** Least squares of the measured bytes per part minus the curve: the granule term over the 8
  Compact cases, the Wide base as the mean of 3 Wide cases. The old constant 19 428.8 B is the value near the
  ceiling (19 176–19 388 B at 100 000–200 000 rows) and is no longer a coefficient.
- **Not a fresh hold-out.** The form passed its hold-out with measured inline bytes (§[Revised form](#relink-revised));
  the curve is fitted on all 11 cases, including those two.

| Case | Part | Rows | Measured B per part | Derivation | Residual |
|---|---|---|---|---|---|
| `relink-1k` | Compact | 1 000 | 13 030 | 13 050 | −0.15 % |
| `relink-3k` | Compact | 3 000 | 16 326 | 16 479 | −0.93 % |
| `relink-10k` | Compact | 10 000 | 18 452 | 18 236 | +1.19 % |
| `relink-50k` | Compact | 50 000 | 19 001 | 19 002 | +0.00 % |
| replication `insert` | Compact | 100 000 | 19 153 | 19 176 | −0.12 % |
| replication `mutation` | Compact | 100 000 | 19 158 | 19 176 | −0.09 % |
| validation `insert-3` | Compact | 150 000 | 19 233 | 19 289 | −0.29 % |
| replication `merge` | Compact | 200 000 | 19 441 | 19 388 | +0.28 % |
| `relink-10k-wide` | Wide | 10 000 | 19 575 | 19 435 | +0.72 % |
| `relink-30k-wide` | Wide | 30 000 | 19 997 | 20 011 | −0.07 % |
| replication `insert-wide` | Wide | 300 000 | 20 158 | 20 284 | −0.62 % |

`coefficients.json` holds the six constants (`cas_relink_*`, tolerance rel 1.25 %). The CAS `inserts` transfer row
reads the derivation in place of the retired coefficient.

## EBS 1-s peak bound {#peak-bound}

The 1-s write peak of a paced run is one merge: 1.02–1.06 × the on-disk bytes of the largest merge, or 0.55–0.57 ×
its uncompressed bytes, in all three local runs. `peak_factor` stays an assumed input with default 1. The EBS
`merges` cell shows `ebs_peak_mibps_bound` as a note: 0.57 × `ebs_largest_merge_bytes` per second, where
`ebs_largest_merge_bytes` is the final part size (`part_bytes_uncompressed` × `merge_size_ratio`), at most
`max_bytes_to_merge_at_max_space_in_pool`. The bound is not priced.

- **Valid only for merges that finish within a second.** The verified range is the measured largest merges,
  15.9–42.1 MB uncompressed; outside it the note is marked extrapolated. A larger merge on EBS is bounded by the
  volume throughput and runs longer, so its size does not set the peak.

| Record | Peak, B/s | Mean, B/s | Peak factor | Largest merge, B on disk | uncompressed | Peak / on disk | Peak / uncompressed |
|---|---|---|---|---|---|---|---|
| `local`, 2/s | 23 195 673 | 823 177 | 28.2 | 22 747 333 | 42 120 000 | 1.020 | 0.551 |
| `local`, 4/s | 23 998 120 | 1 548 902 | 15.5 | 22 749 547 | 42 120 000 | 1.055 | 0.570 |
| `local`, 1/s | 8 798 937 | 444 462 | 19.8 | 8 607 829 | 15 920 000 | 1.022 | 0.553 |

The rule multiplies by 0.57, the largest uncompressed ratio, as a constant: the three records failed on their
amplification assertion, so they cannot back a coefficient. The 1/s record was not predicted for the peak; it adds a third point.

## Async inserts {#async}

Registry `inserts_per_async_flush` (evidence `assumed`) estimates inserts per flush as max(1, min(1 + rate ×
`async_insert_busy_timeout_max_ms`, ceil(buffer / entry bytes), waiting clients, 450)). Its form was adjusted after
the cut `async-inserts.steady` observation, so it has no hold-out. Two cases, each on `s3` and `cas`, K = 2:

| Case | Rate, clients | Binding term | Inserts per flush | Parts for 1 000 inserts (± 20 %) |
|---|---|---|---|---|
| `async-inserts.steady-50hz-c4` | 50/s, 4 | waiting clients | 4 | 208–313 |
| `async-inserts.steady-10hz-c16` | 10/s, 16 | 1 + rate × 0.2 s | 3 | 277–417 |

### Result {#async-result}

Run `cost-20261025T133848Z`. Six of eight records are `FAILED` on the part count:

| Case | `s3` parts, k1 / k2 | `cas` parts, k1 / k2 | Inserts per flush | Rule |
|---|---|---|---|---|
| `steady-50hz-c4` | 321 / 286 | 355 / 279 | 2.8–3.6 | 4 |
| `steady-10hz-c16` | 1 000 / 1 000 | 1 000 / 1 000 | 1.0 | 3 |

- **Below one insert per 50 ms nothing is shared.** The adaptive timeout grows only when an insert arrives within
  `async_insert_busy_timeout_min_ms` (50 ms) of the previous one; otherwise it stays at the minimum
  (`getBusyWaitTimeoutMs`, `AsynchronousInsertQueue.cpp:759`–`:775` @ `09d0772a842`). At 10/s every insert is its
  own part. The rule's `1 + rate × timeout_max` holds only above 20/s (the cut `steady` case: 4.8–5.0 at 20/s).
- **The client cap is an upper bound.** Four waiting clients shared 2.8–3.6 inserts per flush, not 4: a client
  resubmits after its flush returns, and some miss the next buffer.
- **`s3` and `cas` behave alike;** the disk does not change the batching.

### Revised rule and its hold-out {#async-revised}

Revised rule, predicted in `07a862e8e08` before run `cost-20261025T135134Z`: inserts per flush are 1 when the gap
1 / rate is at least `async_insert_busy_timeout_min_ms`, else min(1 + rate × `async_insert_busy_timeout_max_ms`,
waiting clients, …) as before.

| Case | Gap | Predicted parts | `s3` k1 / k2 | `cas` k1 / k2 | Inserts per flush |
|---|---|---|---|---|---|
| `steady-15hz-c16` | 67 ms | 900–1 000 (1 per flush) | 1 000 / 1 000 | 1 000 / 1 000 | 1.00: pass |
| `steady-40hz-c16` | 25 ms | 92–139 (9 per flush) | 114 / 114 | 114 / 114 | 8.77: pass |

All eight records are `ok`. The client cap stays an upper bound (`steady-50hz-c4`: 70–90 % of it).

## GC under load {#gc-under-load}

`gc-under-load` (1 000 inserts of 1 000 rows with natural merges on `cas`, GC every 20 s) compares retired objects
with the objects GC deletes inside the window. Predicted in `e59a1d32ed9`, run `cost-20261025T141616Z`:

| Case | Prediction | Result |
|---|---|---|
| `gc-under-load.short` (`old_parts_lifetime` 480 s) | nothing reclaimed in the window | 1 187 objects and 1 187 manifests retired, 0 reclaimed, 3 GC rounds in 81 s: as predicted |
| `gc-under-load.olt-10` (`old_parts_lifetime` 10 s) | some sources reclaimed | not measured: the card failed |

- **Reclamation starts after `old_parts_lifetime`.** A merged-away part keeps its blobs until the server removes it
  (8 minutes by default), then GC needs 2.75 ± 1 periods (`reclaim_delay_periods`). A window shorter than that
  reclaims nothing however many rounds run.
- **The `short` record is `FAILED`** on `ledger.unpredicted`: the prediction left the foreground empty, as the cut
  `gc-under-load.default` prediction did, and the rig rejects unpredicted foreground operations. The observations
  above are from the record's `partial` section.
- **`olt-10` cannot run on this rig commit.** The card lists part files every 60 s (`LAYOUT_EVERY_S`) and fails
  when a merged-away part is gone before a listing (`lifecycle.py:333`). A 10 s lifetime removes parts first.
- **Not run:** `gc-scale.mounts-2.10k` and `gc-scale.refgrowth-*` need card changes (a second mount on one pool;
  a ref log past the 256-record snapshot), so the `mounts` and `ref_growth` terms of `gc_round_requests` stay
  unsupported.

## s3plain removal of a file past 32 MiB {#big-file}

Part removal on `s3_plain_rewritable` copies each file aside before deleting it. A file past
`max_single_operation_copy_size` (32 MiB, `IO/S3Defines.h:34`) is copied by multipart copy in `min_upload_part_size`
pieces (16 MiB, `IO/S3/copyS3File.cpp:347`, `:673`). `drop.big-file` drops one Wide part of 15 files whose `s.bin` is
40 003 075 B (1 000 000 rows of `FixedString(40) CODEC(NONE)`). Predicted in `e36fce272d8` from the drop graph of
`drop.partition-2`/`-3`, run `cost-20261025T142635Z`, two repeats, both `ok`:

| Operation | Predicted | Measured, k1 / k2 |
|---|---|---|
| `PutObject` | 15 | 15 / 15 |
| `HeadObject` | 27 | 27 / 27 |
| `CopyObject` | 25 | 25 / 25 |
| `CreateMultipartUpload` | 1 | 1 / 1 |
| `UploadPartCopy` | 3 | 3 / 3 |
| `CompleteMultipartUpload` | 1 | 1 / 1 |
| `DeleteObject` | 54 | 54 / 54 |

A file past 32 MiB costs ceil(size / 16 MiB) `UploadPartCopy` plus the create and the complete (5 requests here) in
place of one `CopyObject`. The edge of `plain_unlink_requests` is `done`.

## How to re-run {#rerun}

```bash
flock tmp/yardstick.lock python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
    --manifests docs/superpowers/reports/2026-10-25-storage-cost-cut-cards --cases <experiment ids> \
    --policies local,s3,s3plain,cas --replicas 2 --runs-dir tmp/cas-yardstick/cost-runs
```

Inline bytes of the relink cases come from `clickhouse local` with the same inserts and table settings; the sizes are
in each `predictions/replication-fetch.relink-*.cas.json` (`inputs.form.parts`).
