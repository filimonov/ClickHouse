---
description: 'Directory convention for storage cost calculator reports: where measurements, predictions, experiments, coefficients, formulas and raw evidence live.'
sidebar_label: 'Report directory'
sidebar_position: 52
slug: /superpowers/cost-model/schemas/report-directory
title: 'Report directory convention'
doc_type: 'reference'
---

# Report directory convention {#report-directory}

Each rig report lives in `docs/superpowers/reports/2026-10-<dd>-storage-cost-<name>/`.

| Path | Content |
|---|---|
| `README.md` | What was run, binary sha256, commit, headline results, known gaps |
| `measurements.jsonl` | One `measurement/1` per line |
| `predictions/<experiment_id>.<policy>.json` | `prediction/1` files, committed before the measurement they predict |
| `experiments/` | `experiment/1` manifests |
| `coefficients.json` | Fitted coefficients, each citing `measurement_ids`, `binary_sha256` and `commit` |
| `formulas.json` | `formulas/1` file |
| `raw/` | Ledger slices, relay logs and strace output for every cited measurement |
| `raw/SHA256SUMS` | Checksums of everything under `raw/` |

## Rules {#rules}

- A `measurement_id` cited in `coverage.evidence` or `coefficients.json` must have its raw slice under `raw/`.
- `raw/SHA256SUMS` is regenerated with `cd raw && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS`.
