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
