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

Results follow the run.

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
