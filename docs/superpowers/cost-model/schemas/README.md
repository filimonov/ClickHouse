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
2020-12) document the same shapes for readers; they are not generated from the validators.
`test_committed_artifacts.py` runs every committed registry, coverage, formulas, measurement, prediction and experiment
file, and a sample of ledger rows, through both, so the two cannot drift apart. It needs the `jsonschema` package.

```bash
cd utils/storage-cost-calculator/tools && python3 -m unittest test_artifacts test_committed_artifacts
```

## Versioning {#versioning}

- **Additive change, same version.** A new optional field, a new enum value a reader may ignore, or a new optional
  section stays under `<name>/1`. Update the validator, the schema document and this README in the same commit.
- **Breaking change, next version.** Removing or renaming a field, making a field required, changing a field's type or
  meaning, or narrowing an enum moves to `<name>/2`. Readers keep accepting `/1` until every committed artifact is
  rewritten.
- `billing/1` has no schema document; `closure_check.billing` is its only shape validator. `coefficients.json` has
  neither and is checked through the formulas that read it.

## Artifacts {#artifacts}

| Artifact | Schema doc | Validator | Example |
|---|---|---|---|
| Ledger row (`ledger-row/1`, JSONL) | `ledger-row.schema.json` | `validate_ledger_row` | `ledger-row-attempt.json`, `ledger-row-batch-delete.json`, `ledger-row-mark.json` |
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
- **Ledger row.** `error` and `delete_keys` are optional. `delete_keys` lists the keys a `DeleteObjects` request names; the proxy parses it before forwarding. `wire_bytes_bound` is `exact`, or `upper` when `expect_negotiated` and the store did not answer `ok`: the proxy answered `100 Continue` itself and streamed a body that a store handling `Expect` would have refused, so `req_wire_bytes` only bounds the real bytes.
- **Measurement identity.** `binary_sha256` is the executed binary, `binary_download_sha256` the downloaded one (null when no `SHA256SUMS` lists it); a self-extracting `clickhouse` differs between the two. `commit` is the binary's source commit, `rig_commit` the rig's.
- **Measurement sections.** `window.open_attempts` lists workload attempts that had no row at the window end, and `ledger.complete` is false when it is non-empty. Counts include `accepted_dropped` (upstream ok, reply lost) and `upstream_4xx`. `inventory.note` (optional) says why `initial` is `declared`; `inventory.reclassified` lists the `reclassify` calls. A clipped `local.*.trace` carries `window`, `outside`, `files` and `crossing`.
- **Prediction.** Each expected count is an integer or `{"min","max"}`; a range may add `basis` (`derived` or `assumption`) with the `reason`, so an assumed bound is never read as a source derivation. Callers and ops come from the ledger enums.
- **Coverage.** Evidence rows with role `holdout` or `validation` need a non-null `prediction_commit`. Only `fit` rows may carry null. A row's `measurement_ids` name its own `policy`; a row that applies another policy's records to its variant names that policy in `borrowed_from`. Each case declares `duration_s`, its expected pass duration (`0..timeout_s`); `cost estimate` budgets every pass at `max(measured median, duration_s)`.
- **Registry.** Ids are unique. `enum` is required for `value_type` `enum`, `table` for `table`. A `rule` exists exactly for `derivation`. Precedence groups never share members. `default_locked` and `rules` keys are group members. Set members, group members and table index ids must be registered quantities.

## Formulas grammar v1 {#formulas-grammar}

A node is `{"kind": K, "dim": str, ...}`. `K` is one of `input`, `coef`, `lookup`, `var`, `const`, `add`, `sub`, `mul`, `div`, `min`, `max`, `ceil_to`, `floor_to`, `branch`, `sum_over`, `price`. Values are numbers or the string `unavailable`.

- `var` is legal only inside the `body` of a `sum_over` that binds the same name.
- `div` requires `on_zero` (`zero` or `unavailable`).
- `lookup(table_id, keys)` needs at least one key. When the caller passes `tables` (id to index arity), the table must exist and `len(keys)` must equal its arity. Build `tables` with `table_arities_from_registry` or `table_arities_from_coefficients`.
- Enums are `const` nodes with `dim` `enum:<name>`.
- **Dims** are products of base units with integer exponents joined by `*` and `/` (`GiB*h`, `req/s`); `1` is the neutral unit. The evaluator expands these aliases first: `GiB_h` = `GiB*h`, `GiB_xfer` = `GiB`, `count` = `1`, `iops` = `req/s`, `mibps` = `MiB/s`. `checkDims` requires `add`/`sub`/`min`/`max`/`branch` operands to equal the declared dim, `mul` and `div` declared dims to equal the product or quotient of the operand dims, and `price` to declare `USD`.
- `branch.cond` is `{"op": "<"|"<="|">"|">="|"=="|"!=", "a": node, "b": node}`.
- `min` and `max` take `args[]` (one or more nodes); `add`, `sub`, `mul` and `div` take `a` and `b`.
- **Basis.** Every row declares `basis`: `measured` when its expression reads a `measured` registry quantity (the record's own number, filled from `observations.inputs`), else `predicted`. The drill gives an exactness verdict only to `predicted` rows and shows `measured` rows as measured; it rejects a row whose label contradicts its expression.
- **`all` rows.** A cell may not hold an `all` row and another outcome row for the same row type, op and caller (a null caller covers every caller): the `all` row already bills every outcome. `validate_formulas` and `contract.load` both reject it.
- **Rows.** `outcome_class` is `ok`, `accepted_dropped`, `client_error` (upstream 4XX), `server_error` (other upstream errors), `dropped` (outcome unknown), `not_sent` (the store never saw it) or `all`. Request rows may carry `status` (integer or class such as `4XX`), `s3_code` and `condition`, which the billing preset's free list matches on, and `caller`, which the drill reconciles with.
- **Applicability.** A formulas file may carry the optional `applicability` field, added to `formulas/1` after the
  grammar froze: `{operation: {family: supported | not_supported | unsupported_cut | not_applicable}}` for the
  families that occur on its variant. `tools/generate_contract.py` sets every unlisted family to `not_applicable`,
  which needs no evidence and adds nothing to a column. Only `supported` families need evidence.
- **Exclusions.** A cell may list `excluded: [{kind: op|category, id, reason}]`: record usage it deliberately does not price. The drill reports any other usage the cells do not cover.

Worked examples, each a standalone node validated with `validate_node(node, "$", False, tables={"objects_per_part": 2})`:

| File | Shows |
|---|---|
| `example-part-type.json` | `branch` on `input` vs `coef`, enum-valued `const` leaves |
| `example-objects-per-part.json` | `lookup` keyed by an `input` and a `var`, wrapped in the `sum_over` that binds the var |
| `example-multipart-parts.json` | 1 part up to the first buffer, else `1 + ceil_to((size - first_buffer) / part_size, 1)` |
| `example-min-max.json` | `max(1, min(parts_inserted, 10000, 500))`; also evaluated by `model.test.js` |

`coefficients.json` entries (`{id, kind: scalar|table, dim, value, validity, tolerance, evidence, status}`) are produced per report and have no validator here. A table value is `{"index": [...], "values": {"<k1>|<k2>": number}}`.

## Billing contract free list {#billing-free}

`outcome_billing.free` lists what a preset does not bill. An entry with `outcome_class` (`server_error`, `dropped`, `not_sent`) frees that whole class; an entry without one is a client error matched by `status` (a code or a class such as `4XX`), `s3_code` and `condition`. Every outcome the list does not free is billed, `ok` and `accepted_dropped` always.

## Billing contract `units` {#billing-units}

Each full price preset in `billing-contract.json` (`billing/1`) declares `units`, the meaning of "GB" in its prices.

| Value | Meaning | Requirement |
|---|---|---|
| `decimal` | 1 GB = 10^9 bytes | none |
| `binary_gb` | the price list's GB is 2^30 bytes | `units_evidence: {source_url, checked, note}` |

The billing module converts GiB to the preset's unit and must implement both. `aws-us-east-1` uses `binary_gb`: the S3 price list's tier boundaries 51200 GB and 512000 GB are exact multiples of 1024, and no explicit AWS statement was found. `closure_check.py` rejects any other value and a `binary_gb` preset without `units_evidence`.

## Layout of a report {#report-layout}

See [report-directory.md](report-directory.md).
