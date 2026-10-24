#!/usr/bin/env python3
"""Builds the measured fixtures the Node suite checks the model against (test_measured_fixtures_within_tolerance).

For every coverage evidence edge with status `done` and role `fit` or `holdout`, joins the report's prediction
(`predictions/<experiment_id>.<policy>.json`), each cited measurement and the report's formulas cells for the
measured variant, and writes one fixture per (edge, measurement). The output directory is rebuilt from scratch so
a fixture never outlives its evidence. Exit 1 with every problem listed when an edge does not resolve.

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


def _cells(report, variant, card):
    """The report's formulas cells for `variant` that belong to `card` (named `card` or `card.<operation>`)."""
    path = report / "formulas.json"
    if not path.exists():
        return None, f"{report}: no formulas.json"
    doc = _load(path)
    files = doc if isinstance(doc, list) else [doc]
    cells = {}
    for f in files:
        if f.get("variant") == variant:
            cells.update({k: v for k, v in f.get("cells", {}).items() if k == card or k.startswith(card + ".")})
    if not cells:
        return None, f"{path}: no {variant} cell for card {card}"
    return cells, None


def _coefficients(report):
    path = report / "coefficients.json"
    if not path.exists():
        return []
    doc = _load(path)
    return doc if isinstance(doc, list) else doc.get("coefficients", [])


def _flatten(foreground, value):
    return [{"caller": caller, "op_kind": op, "quantity": value(q)}
            for caller, ops in sorted(foreground.items()) for op, q in sorted(ops.items())]


def _safe(s):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)


def build(report_dirs, coverage):
    """Returns (fixtures {file name: fixture}, problems)."""
    problems = artifacts.validate_coverage(coverage)
    if problems:
        return {}, [f"coverage: {p}" for p in problems]
    measurements = {}
    for r in report_dirs:
        for mid, entry in _measurements(r).items():
            if mid in measurements:
                problems.append(f"measurement {mid} appears in {measurements[mid][1]} and {entry[1]}")
            measurements[mid] = (entry[0], entry[1], r)
    fixtures = {}
    edges = [e for e in coverage["evidence"] if e["status"] == "done" and e["role"] in ROLES]
    for e in edges:
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
            cells, problem = _cells(report, m["variant"], m["card"])
            if problem:
                problems.append(f"{where}: {problem}")
                continue
            fixtures[_safe(f"{e['experiment_id']}.{e['policy']}.{e['coefficient']}.{e['role']}.k{k}") + ".json"] = {
                "experiment_id": e["experiment_id"], "policy": e["policy"], "coefficient": e["coefficient"],
                "role": e["role"], "inputs": pred["inputs"],
                "expected_rows": _flatten(pred["expected"]["foreground"], lambda q: q),
                "measured_rows": _flatten(m["ledger"]["foreground"], lambda c: c["attempts"]),
                "tolerance": pred["tolerance"], "measurement_id": mid,
                "variant": m["variant"], "mode": m["mode"], "card": m["card"], "report": str(report),
                "cells": cells, "coefficients": _coefficients(report),
            }
    return fixtures, problems


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reports", required=True, help="glob of report directories")
    ap.add_argument("--coverage", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    reports = sorted(Path(p) for p in glob.glob(a.reports) if Path(p).is_dir())
    fixtures, problems = build(reports, _load(a.coverage))
    if problems:
        print("build_fixtures: " + "\n  ".join([f"{len(problems)} problem(s):", *problems]), file=sys.stderr)
        return 1
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.json"):
        old.unlink()
    for name, fx in sorted(fixtures.items()):
        (out / name).write_text(json.dumps(fx, indent=1, sort_keys=True) + "\n")
    print(f"build_fixtures: {len(fixtures)} fixture(s) from {len(reports)} report(s) into {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
