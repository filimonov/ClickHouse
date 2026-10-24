---
description: 'Frozen JSON shapes of the storage cost calculator rig and model: ledger row, measurement, prediction, experiment, coverage, registry and the formulas grammar v1, with hand-written validators.'
sidebar_label: 'Cost model schemas'
sidebar_position: 51
slug: /superpowers/cost-model/schemas
title: 'Cost model schemas'
doc_type: 'reference'
---

# Cost model schemas {#cost-model-schemas}

Seven artifact shapes, each versioned by its `schema` field (`<name>/1`). The validators in
`utils/storage-cost-calculator/tools/artifacts.py` are the source of truth. The `*.schema.json` files (JSON Schema
2020-12) document the same shapes for readers and are not generated from, or interpreted by, the validators.

```bash
python3 utils/storage-cost-calculator/tools/test_artifacts.py
```

## Artifacts {#artifacts}

| Artifact | Schema doc | Validator | Example |
|---|---|---|---|
| Ledger row (`ledger-row/1`, JSONL) | `ledger-row.schema.json` | `validate_ledger_row` | `ledger-row-attempt.json`, `ledger-row-mark.json` |
| Measurement (`measurement/1`) | `measurement.schema.json` | `validate_measurement` | `measurement-ok.json`, `-unsupported.json`, `-failed.json` |
| Prediction (`prediction/1`) | `prediction.schema.json` | `validate_prediction` | `prediction.json` |
| Experiment (`experiment/1`) | `experiment.schema.json` | `validate_experiment` | `experiment.json` |
| Coverage (`coverage/1`) | `coverage.schema.json` | `validate_coverage` | `coverage.json` |
| Registry (`registry/1`) | `registry.schema.json` | `validate_registry` | `registry.json` |
| Formulas (`formulas/1`) | `formulas.schema.json` | `validate_formulas`, `validate_node` | `formulas.json`, `example-*.json` |

Every validator returns a list of `"<path>: <message>"` strings. An empty list means valid.

## Rules beyond field presence {#rules}

- **Strict keys.** Unknown keys are problems, except inside free-form objects (`effective_counts`, `observations`, `params`, settings, `inputs`, `tolerance` maps).
- **Measurement id.** `measurement_id` must equal `<run_id>/<policy>/<card>/<case_id>/k<k>`. `experiment_id` must equal `<card>.<case_id>`. The same holds for experiments.
- **Measurement status.** `ok` carries all result sections. `UNSUPPORTED` carries only `reason`. `FAILED` carries `reason` and `diagnostics`, and may carry `partial` with any subset of the `ok` sections. Each present section is validated.
- **Prediction.** Each expected count is an integer or `{"min","max"}`. Callers and ops come from the ledger enums.
- **Coverage.** Evidence rows with role `holdout` or `validation` need a non-null `prediction_commit`. Only `fit` rows may carry null.
- **Registry.** Ids are unique. `enum` is required for `value_type` `enum`, `table` for `table`. A `rule` exists exactly for `derivation`. Precedence groups never share members. `default_locked` and `rules` keys are group members. Set members, group members and table index ids must be registered quantities.

## Formulas grammar v1 {#formulas-grammar}

A node is `{"kind": K, "dim": str, ...}`. `K` is one of `input`, `coef`, `lookup`, `var`, `const`, `add`, `sub`, `mul`, `div`, `min`, `max`, `ceil_to`, `floor_to`, `branch`, `sum_over`, `price`. Values are numbers or the string `unavailable`.

- `var` is legal only inside the `body` of a `sum_over` that binds the same name.
- `div` requires `on_zero` (`zero` or `unavailable`).
- `lookup(table_id, keys)` needs at least one key. When the caller passes `tables` (id to index arity), the table must exist and `len(keys)` must equal its arity. Build `tables` with `table_arities_from_registry` or `table_arities_from_coefficients`.
- Enums are `const` nodes with `dim` `enum:<name>`.
- `branch.cond` is `{"op": "<"|"<="|">"|">="|"=="|"!=", "a": node, "b": node}`.

Worked examples, each a standalone node validated with `validate_node(node, "$", False, tables={"objects_per_part": 2})`:

| File | Shows |
|---|---|
| `example-part-type.json` | `branch` on `input` vs `coef`, enum-valued `const` leaves |
| `example-objects-per-part.json` | `lookup` keyed by an `input` and a `var`, wrapped in the `sum_over` that binds the var |
| `example-multipart-parts.json` | 1 part up to the first buffer, else `1 + ceil_to((size - first_buffer) / part_size, 1)` |

`coefficients.json` entries (`{id, kind: scalar|table, dim, value, validity, tolerance, evidence, status}`) are produced per report and have no validator here. A table value is `{"index": [...], "values": {"<k1>|<k2>": number}}`.

## Billing contract `units` {#billing-units}

Each full price preset in `billing-contract.json` (`billing/1`) declares `units`, the meaning of "GB" in its prices.

| Value | Meaning | Requirement |
|---|---|---|
| `decimal` | 1 GB = 10^9 bytes | none |
| `binary_gb` | the price list's GB is 2^30 bytes | `units_evidence: {source_url, checked, note}` |

The billing module converts GiB to the preset's unit and must implement both. `aws-us-east-1` uses `binary_gb`: the S3 price list's tier boundaries 51200 GB and 512000 GB are exact multiples of 1024, and no explicit AWS statement was found. `closure_check.py` rejects any other value and a `binary_gb` preset without `units_evidence`.

## Layout of a report {#report-layout}

See [report-directory.md](report-directory.md).
