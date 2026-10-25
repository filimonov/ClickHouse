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
each predicted and committed before its run.

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

## Async inserts {#async}

Registry `inserts_per_async_flush` (evidence `assumed`) estimates inserts per flush as max(1, min(1 + rate ×
`async_insert_busy_timeout_max_ms`, ceil(buffer / entry bytes), waiting clients, 450)). Its form was adjusted after
the cut `async-inserts.steady` observation, so it has no hold-out. Two cases, each on `s3` and `cas`, K = 2:

| Case | Rate, clients | Binding term | Inserts per flush | Parts for 1 000 inserts (± 20 %) |
|---|---|---|---|---|
| `async-inserts.steady-50hz-c4` | 50/s, 4 | waiting clients | 4 | 208–313 |
| `async-inserts.steady-10hz-c16` | 10/s, 16 | 1 + rate × 0.2 s | 3 | 277–417 |
