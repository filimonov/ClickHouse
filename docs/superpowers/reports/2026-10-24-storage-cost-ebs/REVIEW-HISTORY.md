---
description: 'Review findings and dispositions of the storage cost EBS report.'
sidebar_label: 'Storage cost EBS: review'
sidebar_position: 60
slug: /superpowers/reports/2026-10-24-storage-cost-ebs/review-history
title: 'Storage cost EBS report: review history'
doc_type: 'reference'
---

# Storage cost EBS report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

One Codex round (`gpt-6-astra`, reasoning high, read-only; review log not retained in the repository): 6 MAJOR, 3 MINOR.

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| F1 | MAJOR | `/proc` and `/sys` reads counted as volume I/O | Fixed test-first (`71399e1090e`): only the replica's run directory and local disk count. Re-fit and fresh hold-out after the fix. |
| F2 | MAJOR | Syscall `ceil(size / 256 KiB)` is not a device-IOPS upper bound | Fixed: IOPS are priced at the pinned provider floor (`ebs_sequential_ios_per_mib`, lower bound); the syscall counts are evidence only and the README calls them a proxy. |
| F3 | MAJOR | Returned read bytes do not bound cold-device reads | Fixed in prose, the registry `evidence.ref` and the `io_sizes.py` docstring: read demand is the bytes the server issues, with no bound direction. |
| F4 | MAJOR | Baseline subtraction removes server background demand | Fixed in prose and the registry `evidence.ref`: the subtracted idle rate is server background (mostly system logs, about 0.06 MiB/s) plus about 2.3 kB/s of Keeper (r1 − r2); it is stated as excluded from EBS demand. |
| F5 | MAJOR | `ebs_fit` cannot reproduce the published coefficients from consumed FAILED records | Fixed: the report run has `ok` records of every fit case; `ebs_fit` reproduces `coefficients.json` from `measurements.jsonl` (README §Reproduce). Consumed FAILED records stay as evidence only. |
| F6 | MAJOR | The start-timeout test and `server.sh` take a zombie for a live server | Fixed test-first (`71399e1090e`): liveness ignores state `Z`. |
| F7 | MINOR | Source references: 128 KiB local read buffer, partition loop, `columns_substreams.txt` writer, buffer-size wording | Fixed (`cea0c37c6d3`). |
| F8 | MINOR | Calibration hint omits the vector and no-wait calls | Fixed in the registry hint. |
| F9 | MINOR | README presented the failed peak hold-out as confirmation | Fixed: [Peak factor](README.md#peak) states both values and the scheduling cause; `peak_factor` is an assumed input. |


A second review of `108c7853103` added: registry refs and the card docstring still claimed an upper bound and a
Keeper-only baseline (fixed with F3/F4); the boundary was not in `validity` (stated in `evidence.ref` instead, see
[Measurement boundary](README.md#boundary)); `SYSTEM FLUSH LOGS` ran inside the insert and merge windows (fixed in
`c2a9eed0b85` before the final report run); the test file's `__main__` guard, the lower-bound label and hint, and
cell notes (fixed).
