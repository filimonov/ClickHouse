#!/usr/bin/env python3
"""Builds the measured fixtures the Node suite checks the model against (test_measured_fixtures_within_tolerance).

For every coverage evidence edge with status `done` and role `fit` or `holdout`, joins the report's prediction
(`predictions/<experiment_id>.<policy>.json`), each cited measurement and the report's formulas cells for the
measured variant (named after the card, or a domain-operation cell that references the edge's coefficient), and
writes one fixture per (edge, measurement). An edge whose report has no cell for the card on
the measured variant is listed in `_coefficient_only.json` instead: the card reconciled its request rows itself and
the record's status carries that verdict. The output directory is rebuilt from scratch so a fixture never outlives its
evidence. Exit 1 with every problem listed when an edge does not resolve.

    python3 tools/build_fixtures.py --reports 'docs/superpowers/reports/2026-10-24-storage-cost-*/' \\
        --coverage docs/superpowers/cost-model/coverage.json --out utils/storage-cost-calculator/test/fixtures/measured/
"""

import argparse
import glob
import json
import re
import sys
from pathlib import Path

import artifacts

ROLES = ("fit", "holdout")
REGISTRY = Path(__file__).resolve().parents[3] / "docs/superpowers/cost-model/inputs-registry.json"
COEF_ONLY = "_coefficient_only.json"


def _load(path):
    with open(path) as f:
        return json.load(f)


def _measurements(report):
    path = report / "measurements.jsonl"
    if not path.exists():
        return {}
    out = {}
    with open(path) as f:
        for n, line in enumerate(f, 1):
            if line.strip():
                m = json.loads(line)
                out[m.get("measurement_id")] = (m, f"{path}:{n}")
    return out


def _cells(report, variant, card, coefficient, operations):
    """The report's formulas cells for `variant` that belong to `card` (named `card` or `card.<operation>`), plus
    its domain-operation cells that reference `coefficient`: a variant report keys its cells by operation."""
    path = report / "formulas.json"
    if not path.exists():
        return None, f"{report}: no formulas.json"
    doc = _load(path)
    files = doc if isinstance(doc, list) else [doc]
    cells = {}
    for f in files:
        if f.get("variant") == variant:
            cells.update({k: v for k, v in f.get("cells", {}).items()
                          if k == card or k.startswith(card + ".") or (k in operations and _references(v, coefficient))})
    return cells, None


def _references(node, coefficient, rules=None, seen=None):
    """True when `node` reads `coefficient` as a coef, a lookup table or a registry quantity of that id; with `rules`
    (derivation id -> rule), also through the registry derivations its `input` nodes name.

    Cells are selected by direct reads only: a cell that reaches a coefficient through a derivation was not what the
    card measured. The coefficients a selected cell needs are found through the derivations."""
    rules = rules or {}
    seen = set() if seen is None else seen
    if isinstance(node, dict):
        kind, ref = node.get("kind"), node.get("id")
        if (kind in ("coef", "input") and ref == coefficient) or (kind == "lookup" and node.get("table_id") == coefficient):
            return True
        if kind == "input" and ref in rules and ref not in seen:
            seen.add(ref)
            if _references(rules[ref], coefficient, rules, seen):
                return True
        return any(_references(v, coefficient, rules, seen) for v in node.values())
    if isinstance(node, list):
        return any(_references(v, coefficient, rules, seen) for v in node)
    return False


def registry_rules(registry):
    """id -> rule of every registry derivation."""
    return {q["id"]: q["rule"] for q in (registry or {}).get("quantities", []) if q.get("kind") == "derivation" and q.get("rule")}


def _coefficients(report):
    path = report / "coefficients.json"
    if not path.exists():
        return None, f"{report}: no coefficients.json"
    doc = _load(path)
    return (doc if isinstance(doc, list) else doc.get("coefficients", [])), None


def _flatten(foreground, value):
    return [{"caller": caller, "op_kind": op, "quantity": value(q)}
            for caller, ops in sorted(foreground.items()) for op, q in sorted(ops.items())]


def _safe(s):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)


def build(report_dirs, coverage, registry=None):
    """Returns (fixtures {file name: fixture}, coefficient-only edges, problems).

    `registry` lets a cell reach a coefficient through the registry derivations it reads.
    """
    rules = registry_rules(registry)
    problems = artifacts.validate_coverage(coverage)
    if problems:
        return {}, [], [f"coverage: {p}" for p in problems]
    measurements = {}
    for r in report_dirs:
        for mid, entry in _measurements(r).items():
            if mid in measurements:
                problems.append(f"measurement {mid} appears in {measurements[mid][1]} and {entry[1]}")
            measurements[mid] = (entry[0], entry[1], r)
    foreign = {}
    for r in report_dirs:
        found, _ = _coefficients(r) if (r / "coefficients.json").exists() else ([], None)
        for c in found or []:
            if isinstance(c, dict) and c.get("id"):
                foreign.setdefault(c["id"], c)
    fixtures = {}
    coefficient_only = []
    # A borrowed row reuses another policy's edge, whose own fixture already replays those records.
    edges = [(f"evidence[{i}]", e) for i, e in enumerate(coverage["evidence"])
             if e["status"] == "done" and e["role"] in ROLES and "borrowed_from" not in e]
    for edge_id, e in edges:
        where = f"edge {e['coefficient']} {e['role']} {e['experiment_id']}.{e['policy']}"
        name = f"{e['experiment_id']}.{e['policy']}.json"
        preds = [r / "predictions" / name for r in report_dirs if (r / "predictions" / name).exists()]
        if len(preds) != 1:
            problems.append(f"{where}: expected one prediction {name} under the reports, found {len(preds)}")
            continue
        pred = _load(preds[0])
        bad = artifacts.validate_prediction(pred)
        if bad:
            problems += [f"{preds[0]}: {p}" for p in bad]
            continue
        if not e["measurement_ids"]:
            problems.append(f"{where}: done edge cites no measurement")
        without_cells = produced = False
        for k, mid in enumerate(e["measurement_ids"]):
            if mid not in measurements:
                problems.append(f"{where}: measurement {mid} is in no report's measurements.jsonl")
                continue
            m, at, report = measurements[mid]
            bad = artifacts.validate_measurement(m)
            if bad:
                problems += [f"{at}: {p}" for p in bad]
                continue
            if m["status"] != "ok":
                problems.append(f"{where}: measurement {mid} has status {m['status']}")
                continue
            if (m["experiment_id"], m["policy"]) != (e["experiment_id"], e["policy"]):
                problems.append(f"{where}: measurement {mid} is {m['experiment_id']}.{m['policy']}")
                continue
            cells, problem = _cells(report, m["variant"], m["card"], e["coefficient"], coverage["domain"]["operations"])
            if problem:
                problems.append(f"{where}: {problem}")
                continue
            if not cells:
                without_cells = True
                continue
            if not any(_references(c, e["coefficient"]) for c in cells.values()):
                problems.append(f"{where}: no {m['variant']} cell of card {m['card']} references {e['coefficient']}")
                continue
            coefficients, problem = _coefficients(report)
            if problem:
                problems.append(f"{where}: {problem}")
                continue
            # Cells may read coefficients other reports own (a variant reusing another's counts).
            own = {c.get("id") for c in coefficients}
            coefficients = coefficients + [c for cid, c in sorted(foreign.items())
                                           if cid not in own and any(_references(x, cid, rules) for x in cells.values())]
            fixtures[_safe(f"{e['experiment_id']}.{e['policy']}.{e['coefficient']}.{e['role']}.k{k}") + ".json"] = {
                "experiment_id": e["experiment_id"], "policy": e["policy"], "coefficient": e["coefficient"],
                "role": e["role"], "edge_id": edge_id, "inputs": pred["inputs"],
                "expected_rows": _flatten(pred["expected"]["foreground"], lambda q: q),
                "measured_rows": _flatten(m["ledger"]["foreground"], lambda c: c["attempts"]),
                "tolerance": pred["tolerance"], "measurement_id": mid,
                "variant": m["variant"], "mode": m["mode"], "card": m["card"], "report": str(report),
                "cells": cells, "coefficients": coefficients,
            }
            produced = True
        # One fixture reconciles the edge; a further measurement without cells does not make it coefficient-only.
        if without_cells and not produced:
            coefficient_only.append({"edge_id": edge_id, "coefficient": e["coefficient"], "role": e["role"],
                                     "experiment_id": e["experiment_id"], "policy": e["policy"],
                                     "measurement_ids": e["measurement_ids"], "request_rows_reconciled_by": "card"})
    return fixtures, coefficient_only, problems


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reports", required=True, help="glob of report directories")
    ap.add_argument("--coverage", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--registry", default=str(REGISTRY), help="inputs registry whose derivations cells may read")
    a = ap.parse_args(argv)
    reports = sorted(Path(p) for p in glob.glob(a.reports) if Path(p).is_dir())
    fixtures, coefficient_only, problems = build(reports, _load(a.coverage), _load(a.registry))
    if problems:
        print("build_fixtures: " + "\n  ".join([f"{len(problems)} problem(s):", *problems]), file=sys.stderr)
        return 1
    write(Path(a.out), fixtures, coefficient_only, a.coverage)
    print(f"build_fixtures: {len(fixtures)} fixture(s) and {len(coefficient_only)} coefficient-only edge(s) "
          f"from {len(reports)} report(s) into {a.out}")
    return 0


def write(out, fixtures, coefficient_only, coverage_path):
    """Replaces every fixture under `out` with the given ones."""
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.json"):
        old.unlink()
    for name, fx in sorted(fixtures.items()):
        (out / name).write_text(json.dumps(fx, indent=1, sort_keys=True) + "\n")
    # The Node check compares this list with the coverage edges that have no fixture.
    only = {"coverage": str(Path(coverage_path).resolve()), "edges": coefficient_only}
    (out / COEF_ONLY).write_text(json.dumps(only, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    sys.exit(main())
