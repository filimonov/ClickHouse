---
description: 'Index of the storage cost calculator cost-model artifacts and the advanced input blocks of the inputs registry.'
sidebar_label: 'Cost model'
sidebar_position: 50
slug: /superpowers/cost-model
title: 'Cost model artifacts'
doc_type: 'reference'
---

# Cost model artifacts {#cost-model}

Seeds owned by phase 0 of the storage cost calculator. Research tasks add quantities, cards and evidence.

```bash
python3 utils/storage-cost-calculator/tools/closure_check.py --mode dev
```

`--mode final` fails on a missing artifact. Tests: `cd utils/storage-cost-calculator/tools && python3 -m unittest discover -p 'test_*.py'`.

## Artifacts {#artifacts}

| File | Schema | Content |
|---|---|---|
| `measurement-contract.md` | n/a | What the rig records and how to read it |
| `inputs-registry.json` | `registry/1` | Every quantity an expression may reference, with scope, unit, default and validity |
| `coverage.json` | `coverage/1` | Domain, cards with cases, evidence plan, excluded branches |
| `billing-contract.json` | `billing/1` | Dated AWS `us-east-1` rules and allocation algorithm; `gcs` and `azure` are structure only |
| `schemas/` | n/a | Frozen JSON shapes and the formulas grammar |

## Advanced input blocks {#blocks}

Every registry quantity carries a `block` from this list. `simple` is always visible.

| Block id | Label |
|---|---|
| `simple` | Simple inputs |
| `schema` | Schema |
| `inserts` | Inserts |
| `merges` | Merges |
| `replication` | Replication |
| `mutations_deletes` | Mutations and deletes |
| `reads_cache` | Reads and cache |
| `topology_recovery` | Topology and recovery |
| `ebs` | EBS |
| `local_disk` | Local disk |
| `cas` | CAS |
| `reliability` | Reliability |
| `prices` | Prices |
