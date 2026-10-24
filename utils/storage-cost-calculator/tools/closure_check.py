"""Closure checks of the storage cost calculator (design §5.2) over the cost-model artifacts.

Each check returns a list of problem strings; empty means closed. `main` loads the artifacts that exist and
exits 1 on any problem. `--mode dev` checks only what is present; `--mode final` also fails on a missing artifact.
Run: python3 utils/storage-cost-calculator/tools/closure_check.py --mode dev
"""
import argparse
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import artifacts

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_COST_MODEL = ROOT / "docs/superpowers/cost-model"
# Any dated storage-cost report directory, so a report written in a later month is never skipped silently.
DEFAULT_REPORTS = str(ROOT / "docs/superpowers/reports/*-storage-cost-*")
NODE_TESTS = ROOT / "utils/storage-cost-calculator/test"
YARDSTICK = ROOT / "utils/cas-yardstick"
RENDER_CHECK = ROOT / "utils/storage-cost-calculator/tools/render_check.mjs"
PRESETS = ROOT / "utils/storage-cost-calculator/presets.js"
IDENTITY = ("binary_sha256", "binary_download_sha256", "commit", "rig_commit")
REQUIRED = ("measurement-contract.md", "inputs-registry.json", "coverage.json", "billing-contract.json")
# Rig policy (cards/runner.py `VARIANTS`) to the model variant it measures.
POLICY_VARIANT = {"local": "ebs", "s3": "s3", "s3plain": "s3plain", "cas": "cas"}
REPLICAS = "replicas"
CONTRACT = "model-contract.json"


# --- 1. dependency closure ---------------------------------------------------------------------

def _walk(node, out):
    """Collect (kind, id) for every input, coef and lookup reference below `node`."""
    if isinstance(node, dict):
        kind = node.get("kind")
        if kind in ("input", "coef"):
            out.append((kind, node.get("id")))
        elif kind == "lookup":
            out.append(("table", node.get("table_id")))
        for value in node.values():
            _walk(value, out)
    elif isinstance(node, list):
        for value in node:
            _walk(value, out)
    return out


def _combine(scopes, path, problems):
    """Scope of an n-ary node whose operands must share one scope."""
    seen = {s for s in scopes if s}
    if {"replica", "cluster"} <= seen:
        problems.append(f"{path}: replica-scoped and cluster-scoped operands mixed without a mul by {REPLICAS}")
    return "replica" if "replica" in seen else (sorted(seen)[0] if seen else None)


def _scope(node, scopes, path, problems):
    """Scope of an expression. A replica-scoped factor reaches a cluster scope only through a mul by `replicas`."""
    if not isinstance(node, dict):
        return None
    kind = node.get("kind")
    sub = lambda key: _scope(node.get(key), scopes, f"{path}.{key}", problems)  # noqa: E731
    if kind in ("input", "coef"):
        return scopes.get(node.get("id"))
    if kind == "lookup":
        return scopes.get(node.get("table_id"))
    if kind == "price":
        return sub("quantity")
    if kind == "sum_over":
        return sub("body")
    if kind in ("ceil_to", "floor_to"):
        return sub("x")
    if kind == "mul":
        a, b = sub("a"), sub("b")
        if {a, b} == {"replica", "cluster"}:
            cluster_side = node["b"] if a == "replica" else node["a"]
            if not (cluster_side.get("kind") == "input" and cluster_side.get("id") == REPLICAS):
                problems.append(f"{path}: replica-scoped factor multiplied into a cluster scope needs an explicit mul by {REPLICAS}")
            return "cluster"
        return a or b
    if kind in ("add", "sub", "div"):
        return _combine([sub("a"), sub("b")], path, problems)
    if kind in ("min", "max"):
        return _combine([_scope(x, scopes, f"{path}.args[{i}]", problems) for i, x in enumerate(node.get("args", []))], path, problems)
    if kind == "branch":
        return _combine([sub("then"), sub("else")], path, problems)
    return None


def _cyclic(graph):
    """Members of `graph` (id -> referenced ids) that lie on or lead into a cycle, by repeated leaf removal."""
    rest = {k: set(v) & set(graph) for k, v in graph.items()}
    while True:
        leaves = [k for k, v in rest.items() if not v]
        if not leaves:
            return sorted(rest)
        for k in leaves:
            del rest[k]
        for v in rest.values():
            v.difference_update(leaves)


def dependency(registry, formulas_list):
    problems = [f"registry {p}" for p in artifacts.validate_registry(registry)]
    if problems:
        return problems
    quantities = registry["quantities"]
    ids = {q["id"] for q in quantities}
    coefs = ids | {q["coef"] for q in quantities if q["coef"]}
    scopes = {q["id"]: q["scope"] for q in quantities}
    scopes.update({q["coef"]: q["scope"] for q in quantities if q["coef"]})
    tables = artifacts.table_arities_from_registry(registry)

    def resolve(node, where):
        for kind, ref in _walk(node, []):
            if ref not in (ids if kind == "input" else coefs):
                problems.append(f"{where}: unknown {kind} {ref}")

    graph = {}
    for q in quantities:
        if q["rule"] is not None:
            resolve(q["rule"], f"quantity {q['id']}.rule")
            graph[q["id"]] = {r for _, r in _walk(q["rule"], []) if r in ids}
    if _cyclic(graph):
        problems.append("derivation cycle among: " + ", ".join(_cyclic(graph)))
    for i, group in enumerate(registry["precedence"]):
        for derived, rule in group["rules"].items():  # cyclic by design, resolved by locks, so not in the graph
            resolve(rule, f"precedence[{i}].rules.{derived}")
    defaults = {q["id"]: q["default"] for q in quantities}
    for q in quantities:
        if q["value_type"] == "enum" and q["default"] is not None and q["default"] not in q["enum"]:
            problems.append(f"quantity {q['id']}: default {q['default']!r} not in enum {q['enum']}")
        for key, (lo, hi) in (q["validity"] or {}).items():
            value = q["default"] if key == q["id"] else defaults.get(key)
            if isinstance(value, (int, float)) and not lo <= value <= hi:
                problems.append(f"quantity {q['id']}: default {value} outside validity {key} [{lo}, {hi}]")
    for n, f in enumerate(formulas_list):
        where = f"formulas[{n}] ({f.get('variant')})"
        problems += [f"{where} {p}" for p in artifacts.validate_formulas(f, tables)]
        resolve(f.get("cells"), where)
        for op, cell in (f.get("cells") or {}).items():
            for i, row in enumerate(cell.get("ledger_rows", []) if isinstance(cell, dict) else []):
                path = f"{where}.cells.{op}.ledger_rows[{i}].expr"
                found = []
                scope = _scope(row.get("expr"), scopes, path, found)
                problems += [f"{where}: {p}" for p in found]
                if row.get("scope") == "variant" and scope == "replica":
                    problems.append(f"{path}: replica-scoped expression in a variant-scoped row needs a mul by {REPLICAS}")
    return problems


# --- 2. support closure ------------------------------------------------------------------------

def unconditional_not_supported(cell):
    """The cell's operation cannot be performed by the variant at all, whatever the workload."""
    return isinstance(cell, dict) and any(isinstance(s, dict) and s.get("cond") is True and s.get("id") == "not_supported"
                                          for s in cell.get("states") or [])


def _cut_coefficients(coverage):
    cut_cases = {f"{c['name']}.{k['case_id']}" for c in coverage["cards"] for k in c["cases"] if k["status"] == "cut"}
    by_coef = {}
    for row in coverage["evidence"]:
        by_coef.setdefault(row["coefficient"], []).append(row)
    return {coef: sorted({r["experiment_id"] for r in rows})
            for coef, rows in by_coef.items()
            if all(r["status"] == "cut" or r["experiment_id"] in cut_cases for r in rows)}


def _pending_cells(coverage, formulas_list, applicability=None):
    """Cells of variants x operations x families with neither evidence nor an unconditional `not_supported` state.

    A cell has evidence when every coefficient its formulas use has a completed (`done` or `consumed`) evidence
    row from a card of that family whose `model_variants` include the variant. A planned row backs nothing.
    With the contract's `applicability`, only `supported` families need evidence; without it, every family does.
    """
    domain = coverage["domain"]
    card_of = {f"{c['name']}.{k['case_id']}": c for c in coverage["cards"] for k in c["cases"]}
    backed = {(row["coefficient"], card_of[row["experiment_id"]]["family"], v)
              for row in coverage["evidence"] if row["status"] in ("done", "consumed") and row["experiment_id"] in card_of
              for v in card_of[row["experiment_id"]]["model_variants"]}
    cells = {}
    for f in formulas_list:
        for op, cell in f.get("cells", {}).items():
            entry = cells.setdefault((f["variant"], op), {"unsupported": False, "coefs": set()})
            entry["unsupported"] |= unconditional_not_supported(cell)
            entry["coefs"] |= {ref for kind, ref in _walk(cell, []) if kind in ("coef", "table")}
    pending = []
    for v in domain["variants"]:
        for op in domain["operations"]:
            entry = cells.get((v, op), {"unsupported": False, "coefs": set()})
            app = (applicability or {}).get(v, {}).get(op, {})
            pending += [(v, op, fam) for fam in domain["families"]
                        if (applicability is None or app.get(fam) == "supported")
                        and not entry["unsupported"] and not (entry["coefs"] and all((c, fam, v) in backed for c in entry["coefs"]))]
    return pending


def _cells_on_cut(coverage, formulas_list):
    """A cell that uses a coefficient resting only on cut experiments must be unconditionally not_supported."""
    cut = _cut_coefficients(coverage)
    out = []
    for f in formulas_list:
        for op, cell in (f.get("cells") or {}).items():
            unsupported = unconditional_not_supported(cell)
            for kind, ref in sorted(set(_walk(cell, []))):
                if kind in ("coef", "table") and ref in cut and not unsupported:
                    out.append(f"cell {f['variant']}/{op} depends on cut coefficient {ref} and is not marked not_supported")
    return out


def support(registry, coverage, formulas_list, require_formulas=False, pending=None, applicability=None):
    problems = [f"coverage {p}" for p in artifacts.validate_coverage(coverage)]
    if problems:
        return problems
    domain = coverage["domain"]
    variants, families = set(domain["variants"]), set(domain["families"])
    for card in coverage["cards"]:
        if card["family"] not in families:
            problems.append(f"card {card['name']}: family {card['family']} not in domain.families")
        problems += [f"card {card['name']}: variant {v} not in domain.variants" for v in card["model_variants"] if v not in variants]
        for case in card["cases"]:
            for policy in case["policies"]:
                variant = POLICY_VARIANT.get(policy.removesuffix("_cached"))
                if variant not in card["model_variants"]:
                    problems.append(f"card {card['name']}.{case['case_id']}: policy {policy} measures {variant}, not in model_variants")
    for card in coverage["cards"]:
        measured = {POLICY_VARIANT.get(p.removesuffix("_cached")) for case in card["cases"] for p in case["policies"]}
        problems += [f"card {card['name']}: model variant {v} is not measured by any case policy"
                     for v in card["model_variants"] if v not in measured]
    problems += [f"family {f} has no card" for f in sorted(families - {c["family"] for c in coverage["cards"]})]
    problems += _cells_on_cut(coverage, formulas_list)
    if _cut_coefficients(coverage) and not PRESETS.exists():
        print(f"note: {PRESETS} absent; cut propagation to presets not checked", file=sys.stderr)
    pending_cells = _pending_cells(coverage, formulas_list, applicability)
    if require_formulas and applicability is not None:
        # A variant whose reports claim no family would price nothing; that is missing work, not a free column.
        problems += [f"variant {v}: no formulas file lists a supported family" for v in sorted(applicability)
                     if not any(x == "supported" for row in applicability[v].values() for x in row.values())]
    if require_formulas:
        problems += [f"cell {v}/{op}/{fam}: no evidence and no unconditional not_supported state" for v, op, fam in pending_cells]
    elif pending is not None:
        pending.extend(pending_cells)
    by_name = {}
    for q in registry.get("quantities", []):
        by_name[q["id"]] = q
        if q.get("coef"):
            by_name[q["coef"]] = q
    for coef, experiments in _cut_coefficients(coverage).items():
        q = by_name.get(coef)
        if q is None:
            continue
        if q["status"] != "unsupported":
            problems.append(f"{coef}: depends only on cut {experiments} and must be unsupported")
        if q["default"] not in (None, 0):
            problems.append(f"{coef}: non-zero default {q['default']} for a cut coefficient")
    return problems


# --- 3. evidence closure -----------------------------------------------------------------------

PROVENANCE = ("kind", "ref", "version", "measurement_ids", "binary_sha256", "commit")


def evidence(coverage, coefficients, measurement_ids=None, identity=None):
    """`measurement_ids`, when given, are the ids the reports hold; evidence must resolve to them.

    `identity` maps a coefficient id to its owner report's manifest entry; a measured coefficient must cite the
    commit and the binary that produced the report's records.
    """
    problems = [f"coverage {p}" for p in artifacts.validate_coverage(coverage)]
    if problems:
        return problems
    priority = {f"{c['name']}.{k['case_id']}": k["priority"] for c in coverage["cards"] for k in c["cases"]}
    rows_of = {}
    for row in coverage["evidence"]:
        rows_of.setdefault(row["coefficient"], []).append(row)
    for c in coefficients:
        cid, ev = c.get("id"), c.get("evidence") or {}
        if ev.get("kind") == "source" and not re.search(r":\d+$", ev.get("ref", "")):
            problems.append(f"{cid}: pinned constant ref {ev.get('ref')!r} needs path:line")
        if ev.get("kind") != "measured":
            continue
        problems += [f"{cid}: provenance missing {k}" for k in PROVENANCE if not ev.get(k)]
        owner = (identity or {}).get(cid)
        for key in ("commit", "binary_sha256") if owner else ():
            if owner[key] != "pending" and ev.get(key) != owner[key]:
                problems.append(f"{cid}: provenance {key} {ev.get(key)} differs from {owner['report']} records ({owner[key]})")
        rows = rows_of.get(cid, [])
        if not rows:
            problems.append(f"{cid}: measured coefficient has no evidence rows")
            continue
        listed = {m for r in rows for m in r["measurement_ids"]}
        problems += [f"{cid}: measurement {m} not in coverage evidence" for m in ev.get("measurement_ids", []) if m not in listed]
        if measurement_ids is not None:
            problems += [f"{cid}: measurement {m} does not resolve to a report record"
                         for m in sorted(listed | set(ev.get("measurement_ids", []))) if m not in measurement_ids]
        if not any(priority.get(r["experiment_id"]) == "P1" for r in rows):
            continue
        if c.get("validity") is None:
            problems.append(f"{cid}: P1 measured coefficient needs a validity domain")
        fits = [r for r in rows if r["role"] == "fit" and r["status"] in ("done", "consumed")]
        holdouts = [r for r in rows if r["role"] == "holdout" and r["status"] == "done"]
        if not fits:
            problems.append(f"{cid}: P1 coefficient has no done fit row")
        if not holdouts:
            problems.append(f"{cid}: P1 coefficient has no done holdout row")
        for h in holdouts:
            if fits and not all(_distinct(h, f) for f in fits):
                problems.append(f"{cid}: holdout {h['experiment_id']} overlaps the fit data (same experiment and span, "
                                "or shared measurements with some fit)")
    return problems


def _distinct(h, f):
    same_data = h["experiment_id"] == f["experiment_id"] and h["policy"] == f["policy"] and h["span"] == f["span"]
    return not same_data and not set(h["measurement_ids"]) & set(f["measurement_ids"])


# --- 4. accounting closure ---------------------------------------------------------------------

def _want_range(want):
    return (want, want) if isinstance(want, int) else (want["min"], want["max"])


def _compare(m, pred):
    mid, exact = m["measurement_id"], m["mode"] == "deterministic"
    problems = []
    foreground = m["ledger"]["foreground"]
    tol = pred["tolerance"]
    for caller, ops in pred["expected"]["foreground"].items():
        for op, want in ops.items():
            got = foreground.get(caller, {}).get(op, {}).get("attempts", 0)
            lo, hi = _want_range(want)
            slack = 0 if exact else tol["abs"].get(op, 0) + tol["rel"].get(op, 0) * max(abs(lo), abs(hi))
            if not lo - slack <= got <= hi + slack:
                problems.append(f"{mid}: {caller}.{op} attempts {got}, predicted {want}" + ("" if exact else f" within {slack}"))
    if exact:
        for caller, ops in foreground.items():
            problems += [f"{mid}: unpredicted {caller}.{op} ({c['attempts']} attempts)" for op, c in ops.items()
                         if c["attempts"] and op not in pred["expected"]["foreground"].get(caller, {})]
        problems += [f"{mid}: observation {k} is {m['observations'].get(k)!r}, predicted {v!r}"
                     for k, v in pred["expected"]["observations"].items() if m["observations"].get(k) != v]
    return problems


def _node_fixtures(cmd, mode):
    if cmd is None:
        tests = sorted(str(p) for p in NODE_TESTS.glob("*.test.js"))
        if shutil.which("node") and tests:
            cmd = ["node", "--test", *tests]
        elif mode == "final":
            return [f"node pricing tests missing: need node and {NODE_TESTS}/*.test.js"]
        else:
            print(f"note: node pricing tests skipped (node or {NODE_TESTS}/*.test.js absent)", file=sys.stderr)
            return []
    # Dev mode runs before the measured fixtures are built; final mode needs them (pipeline_eval.test.js).
    env = {k: v for k, v in os.environ.items() if k != "SCC_DEV"}
    if mode == "dev":
        env["SCC_DEV"] = "1"
    # closure.test.js runs this script; inside the suite it runs from, it would recurse.
    env["SCC_CLOSURE_CHILD"] = "1"
    done = subprocess.run(cmd, capture_output=True, text=True, env=env)
    tail = "\n".join((done.stdout + done.stderr).strip().splitlines()[-15:])
    return [] if done.returncode == 0 else [f"node fixtures failed ({' '.join(cmd)}):\n{tail}"]


def raw_slices(reports):
    """measurement_id -> raw ledger slice of every report record.

    `raw/<card>/<run_id>/ledger.jsonl[.gz]` wins over `raw/<card>/ledger.jsonl[.gz]`: every run's ledger restarts
    its seq numbers, so a card measured in several runs keeps one slice per run.
    """
    out = {}
    for r in reports:
        path = Path(r) / "measurements.jsonl"
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            try:
                m = json.loads(line)
            except ValueError:
                continue  # reported by the loader
            if not isinstance(m, dict):
                continue
            card = Path(r) / "raw" / str(m.get("card"))
            for p in [card / str(m.get("run_id")) / n for n in ("ledger.jsonl", "ledger.jsonl.gz")] + \
                     [card / n for n in ("ledger.jsonl", "ledger.jsonl.gz")]:
                if p.exists():
                    out[m.get("measurement_id")] = p
                    break
    return out


def replay_problems(record, raw_path):
    """Counts and byte-hour bounds recomputed from the raw rows by the rig's own replay must match the record."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(record, f)
    try:
        done = subprocess.run([sys.executable, "-m", "yardstick.cost.reproduce", f.name, str(Path(raw_path).resolve())],
                              cwd=YARDSTICK, capture_output=True, text=True)
    finally:
        Path(f.name).unlink()
    if done.returncode not in (0, 1):
        return [f"{record.get('measurement_id')}: replay failed: {done.stderr.strip()[-500:]}"]
    return [f"{record.get('measurement_id')}: {line}" for line in done.stdout.splitlines() if line.strip()]


def accounting(measurements, predictions, node_test_cmd=None, mode="dev", raw=None, contract=None, billing_contract=None,
               stale=None):
    """`raw` maps measurement ids to raw ledger slices; each record is replayed from its slice.

    The pricing fixtures verify `billing-contract.json`; the page prices with the contract's embedded copy, so the
    two must be the same document; a difference goes to `stale` (a stale contract). `node_test_cmd=False` skips
    the Node suite.
    """
    problems = [f"prediction {p}" for pr in predictions for p in artifacts.validate_prediction(pr)]
    if contract is not None and contract.get("billing") != billing_contract:
        (stale if stale is not None else problems).append(f"model-contract.json billing (version {(contract.get('billing') or {}).get('version')}) differs from "
                        f"billing-contract.json (version {(billing_contract or {}).get('version')}): regenerate the contract")
    preds = {(p["experiment_id"], p["policy"]): p for p in predictions if isinstance(p, dict) and "policy" in p}
    for m in measurements:
        found = artifacts.validate_measurement(m)
        problems += [f"measurement {m.get('measurement_id')}: {p}" for p in found]
        if found or m["status"] != "ok":
            continue
        mid = m["measurement_id"]
        for name, a in m["assertions"].items():
            if not a["ok"]:
                problems.append(f"{mid}: assertion {name} failed (expected {a['expected']!r}, actual {a['actual']!r})")
        for cat, b in m["inventory"]["byte_hours"].items():
            if b["lower"] < 0 or b["lower"] > b["upper"]:
                problems.append(f"{mid}: byte_hours.{cat} lower {b['lower']} not within 0 <= lower <= upper {b['upper']}")
        if raw is not None:
            if mid in raw:
                problems += replay_problems(m, raw[mid])
            elif mode == "final":
                problems.append(f"{mid}: no raw ledger slice to replay")
            else:
                print(f"note: {mid}: no raw ledger slice, replay skipped", file=sys.stderr)
        pred = preds.get((m["experiment_id"], m["policy"]))
        if pred is None:
            problems.append(f"{mid}: no prediction for {m['experiment_id']} / {m['policy']}")
        else:
            problems += _compare(m, pred)
    return problems + ([] if node_test_cmd is False else _node_fixtures(node_test_cmd, mode))


DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Required keys per section of a full price preset; a section with `checked` also needs the dated-rule form.
PRESET_KEYS = ("id", "as_of", "currency", "units", "assumptions", "request_classes", "outcome_billing", "storage",
               "transfer", "block", "allocation", "ec2_ebs_bandwidth", "multipart")
ENTRY_KEYS = {"request_classes": ("id", "ops", "price_per_1000", "checked"), "transfer": ("id", "price_per_gb", "checked"),
              "ec2_ebs_bandwidth": ("instance_class", "max_mibps", "checked")}


def _dated(node, path, problems):
    """Every rule with a `checked` key has a source URL and date, or is null with a note."""
    if isinstance(node, dict):
        if "checked" in node:
            if node["checked"] is None:
                if not node.get("note"):
                    problems.append(f"{path}: unchecked rule needs a note")
            elif not DATE.match(str(node["checked"])) or not node.get("source_url"):
                problems.append(f"{path}: needs source_url and a YYYY-MM-DD checked date")
        for k, v in node.items():
            _dated(v, f"{path}.{k}", problems)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            _dated(v, f"{path}[{i}]", problems)


def billing(contract):
    """Shape of billing-contract.json; the money itself is verified by the node pricing fixtures."""
    if not isinstance(contract, dict) or contract.get("schema") != "billing/1":
        return ["billing: schema must be billing/1"]
    presets = contract.get("presets")
    if not isinstance(presets, list) or "aws-us-east-1" not in [p.get("id") for p in presets]:
        return ["billing: presets must include aws-us-east-1"]
    problems = []
    for p in presets:
        path = f"billing.presets[{p.get('id')}]"
        if p.get("structure_only"):
            problems += [f"{path}: a structure-only preset carries no values"] if set(p) != {"id", "structure_only"} else []
            continue
        missing = [f"{path}: missing {k}" for k in PRESET_KEYS if k not in p]
        if missing:
            problems += missing
            continue
        _dated(p, path, problems)
        if p["units"] not in ("decimal", "binary_gb"):
            problems.append(f"{path}.units: expected decimal or binary_gb, got {p['units']!r}")
        elif p["units"] == "binary_gb" and not (p.get("units_evidence") or {}).get("source_url"):
            problems.append(f"{path}.units_evidence: binary_gb needs units_evidence with source_url and checked")
        if not p["assumptions"] or not all(isinstance(a, str) and a for a in p["assumptions"]):
            problems.append(f"{path}.assumptions: expected non-empty strings")
        for section, keys in ENTRY_KEYS.items():
            problems += [f"{path}.{section}[{i}]: needs {', '.join(keys)}" for i, e in enumerate(p[section]) if not all(k in e for k in keys)]
        problems += [f"{path}.{k}: needs checked" for k, node in (("outcome_billing", p["outcome_billing"]), ("storage", p["storage"]),
                                                              ("block.gp3", p["block"].get("gp3", {}))) if "checked" not in node]
        if not all(p["allocation"].get(k) for k in ("layouts", "objective", "tie_break")):
            problems.append(f"{path}.allocation: needs layouts, objective, tie_break")
        if not isinstance(p["multipart"].get("abort_days_after_initiation_default"), int) or not p["multipart"].get("residence_time_assumption"):
            problems.append(f"{path}.multipart: needs abort_days_after_initiation_default and residence_time_assumption")
    return problems


# --- 5. reproduction closure -------------------------------------------------------------------

def reproduction(copies=(), sums=()):
    """`copies` are (source, generated) path pairs that must be byte-identical; `sums` are SHA256SUMS files."""
    problems = []
    for src, gen in copies:
        try:
            if Path(src).read_bytes() != Path(gen).read_bytes():
                problems.append(f"stale copy: {gen} differs from {src}")
        except OSError as e:
            problems.append(f"copy {gen} vs {src}: {e}")
    for sums_path in sums:
        base = Path(sums_path).parent
        for line in Path(sums_path).read_text().splitlines():
            digest, _, name = line.partition("  ")
            target = base / name.removeprefix("./")
            if not line.strip():
                continue
            if not target.is_file():
                problems.append(f"{sums_path}: {name} is missing")
            elif hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                problems.append(f"{sums_path}: {name} sha256 mismatch")
    return problems


STALE_HINT = "run tools/generate_contract.py generate"
PENDING = "pending"
# What the page runs, in load order: the manifest hashes these. ui.js is included because it lays out the numbers.
CODE_FILES = ("model.js", "contract.js", "billing.js", "pipeline.js", "presets.js", "urlstate.js", "viewmodel.js", "ui.js")
INPUTS = (("registry", "inputs-registry.json"), ("coverage", "coverage.json"), ("billing", "billing-contract.json"))
REPORT_FILES = ("formulas.json", "coefficients.json", "measurements.jsonl")


def report_dirs(pattern):
    """(report directories, problems, notes) for the directories matching `pattern`.

    A report has README.md. A directory with report files but no README is a problem; one with neither (the
    calculator ledger) is noted and skipped.
    """
    dirs, problems, notes = [], [], []
    for p in sorted(glob.glob(str(pattern))):
        path = Path(p)
        if not path.is_dir():
            continue
        if (path / "README.md").exists():
            dirs.append(path)
            continue
        present = [n for n in REPORT_FILES if (path / n).exists()]
        if present:
            problems.append(f"{path}: has {', '.join(present)} but no README.md")
        else:
            notes.append(f"note: {path} has no README.md and no report files; not a report")
    return dirs, problems, notes


def report_records(report, problems):
    return _load_jsonl(report / "measurements.jsonl", problems) if (report / "measurements.jsonl").exists() else []


def report_identity(report, records, mode, problems, pending):
    """The report's source commit, rig commit and executed binary, each single-valued over its valid ok records."""
    ok = []
    for m in records:
        if m.get("status") != "ok":
            continue
        bad = artifacts.validate_measurement(m)
        if bad:
            problems.append(f"{report.name}: record {m.get('measurement_id')} is invalid: {bad[0]}")
        else:
            ok.append(m)
    keys = ("commit", "rig_commit", "binary_sha256")
    if not ok:
        (problems if mode == "final" else pending).append(f"{report.name}: no valid ok record in measurements.jsonl names its commit")
        return {k: PENDING for k in keys}
    out = {}
    for key in keys:
        seen = sorted({m[key] for m in ok})
        if len(seen) != 1:
            problems.append(f"{report.name}: records name several {key} values: {', '.join(seen)}")
        out[key] = seen[0]
    if out["commit"] not in (report / "README.md").read_text():
        problems.append(f"{report.name}: README.md does not name the records' source commit {out['commit']}")
    return out


def raw_digest(report, mode, problems, pending):
    sums = report / "raw/SHA256SUMS"
    if sums.exists():
        return hashlib.sha256(sums.read_bytes()).hexdigest()
    if (report / "measurements.jsonl").exists():
        (problems if mode == "final" else pending).append(f"{report.name}: raw/SHA256SUMS missing")
    return PENDING


def code_manifest(code_dir, mode, problems, pending):
    """(code_sha256, per-file sha256, missing files). The digest covers the sorted "<sha256>  <file>" lines."""
    files = {}
    for name in sorted(CODE_FILES):
        path = Path(code_dir) / name
        if path.exists():
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            (problems if mode == "final" else pending).append(f"code file {name} missing from {code_dir}")
    listing = "".join(f"{h}  {n}\n" for n, h in files.items())
    return hashlib.sha256(listing.encode()).hexdigest(), files, sorted(set(CODE_FILES) - set(files))


def manifest(contract, opts, mode, fresh=None):
    """Stale-contract problems: the manifest and the generated files against the code, inputs and reports now.

    `fresh` maps output paths to a fresh render (generate_contract.build); None renders one here.
    """
    stale = []
    cm = Path(opts["cost_model"])
    m = contract.get("manifest") or {}
    code_sha, files, _ = code_manifest(opts["code_dir"], mode, [], [])
    if (m.get("code_sha256"), m.get("code_files")) != (code_sha, files):
        listed = m.get("code_files") or {}
        changed = sorted(n for n in set(files) | set(listed) if files.get(n) != listed.get(n))
        stale.append(f"manifest code_sha256 does not match the code files (changed: {', '.join(changed) or 'none'})")
    for key, name in INPUTS:
        try:
            data = (cm / name).read_bytes()
            version = json.loads(data).get("version")
        except (OSError, ValueError) as e:
            stale.append(f"manifest {key}: cannot read {name}: {e}")
            continue
        if m.get(f"{key}_version") != version:
            stale.append(f"manifest {key}_version {m.get(f'{key}_version')} differs from {name} version {version}")
        if m.get(f"{key}_sha256") != hashlib.sha256(data).hexdigest():
            stale.append(f"manifest {key}_sha256 differs from the sha256 of {name}")
    reports, _, _ = report_dirs(opts["reports"])
    listed = {e.get("report"): e for e in m.get("evidence") or []}
    for r in reports:
        e = listed.pop(r.name, None)
        if e is None:
            stale.append(f"manifest evidence does not list report {r.name}")
            continue
        digest = raw_digest(r, mode, [], [])
        if e.get("raw_sha256sums") != digest:
            stale.append(f"{r.name}: raw/SHA256SUMS digest {digest} differs from the manifest ({e.get('raw_sha256sums')})")
        ident = report_identity(r, report_records(r, []), mode, [], [])
        stale += [f"{r.name}: records' {k} {ident[k]} differs from the manifest ({e.get(k)})" for k in ident if e.get(k) != ident[k]]
    stale += [f"manifest evidence lists {name}, which is not a report directory" for name in sorted(listed)]
    if fresh is None:
        import generate_contract  # imports this module; only the standalone run renders here

        fresh = generate_contract.build(opts, mode)[0]
    if fresh is None:
        stale.append("cannot render a fresh contract to compare with the generated files")
    else:
        stale += stale_outputs(fresh)
    return stale


BLOCKS = {"page": ("<!-- contract:begin -->", "<!-- contract:end -->"),
          "formulas": ("<!-- formulas:begin -->", "<!-- formulas:end -->"),
          "inputs": ("<!-- inputs:begin -->", "<!-- inputs:end -->"),
          "breakeven": ("<!-- breakeven:begin -->", "<!-- breakeven:end -->")}


def extract_block(text, name):
    begin, end = BLOCKS[name]
    if text.count(begin) != 1 or text.count(end) != 1:
        return None
    return text.split(begin, 1)[1].split(end, 1)[0]


def stale_outputs(outputs):
    """Every output file whose bytes differ from a fresh render, naming the block that differs."""
    out = []
    for path, fresh in outputs.items():
        try:
            current = Path(path).read_text()
        except OSError as e:
            out.append(f"{path}: {e}")
            continue
        if current == fresh:
            continue
        names = [n for n in BLOCKS if extract_block(fresh, n) is not None and extract_block(current, n) != extract_block(fresh, n)]
        out.append(f"{path} differs from a fresh render" + (f" (block {', '.join(names)})" if names else ""))
    return out


def classify_stale(stale, mode, problems, label):
    """A stale generated file is pending in dev mode and a failure in final mode."""
    for line in stale:
        msg = f"stale: {line}; {STALE_HINT}"
        if mode == "final":
            problems.append(msg)
        else:
            print(f"{label}: pending: {msg}", file=sys.stderr)


def identities(measurements):
    """Records of one report name one binary (downloaded and executed), one source commit and one rig commit."""
    problems = []
    for key in IDENTITY:
        seen = sorted({str(m.get(key)) for m in measurements if m.get("status") == "ok"})
        if len(seen) > 1:
            problems.append(f"identity {key} differs across the report's records: {', '.join(seen)}")
    return problems


def rerender(reports):
    """Every archived drill renders again with the page's own renderer, values and evidence included."""
    drills = [p for r in reports for p in sorted((Path(r) / "raw").glob("*/drill.json"))]
    if drills and not shutil.which("node"):
        return ["node missing: archived drills cannot be re-rendered"]
    problems = []
    for d in drills:
        probe = json.loads(d.read_text()).get("probe", "")
        done = subprocess.run(["node", str(RENDER_CHECK), str(d), "--probe", probe], capture_output=True, text=True)
        if done.returncode != 0:
            problems.append(f"{d}: render check failed: {done.stderr.strip()[-300:]}")
    return problems


# --- driver ------------------------------------------------------------------------------------

def _load(path, problems):
    """Parsed JSON, or None after reporting the failure with its path."""
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        problems.append(f"{path}: cannot load: {e}")


def _load_jsonl(path, problems):
    rows = []
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as e:
        return problems.append(f"{path}: cannot load: {e}") or rows
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as e:
            problems.append(f"{path}:{n}: bad JSON line: {e}")
            continue
        if isinstance(row, dict):
            rows.append(row)
        else:
            problems.append(f"{path}:{n}: expected a JSON object")
    return rows


def main(argv=None, fresh=None):
    """`fresh`: the outputs of a fresh render, when the caller (generate_contract.py check) already built them."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mode", choices=("dev", "final"), default="dev")
    ap.add_argument("--cost-model", default=str(DEFAULT_COST_MODEL))
    ap.add_argument("--reports", default=DEFAULT_REPORTS, help="glob of report directories")
    ap.add_argument("--copy", action="append", default=[], metavar="SRC=GENERATED")
    # The generated outputs the reproduction check renders afresh (tools/generate_contract.py).
    ap.add_argument("--page", default=str(ROOT / "utils/storage-cost-calculator/index.html"))
    ap.add_argument("--doc", default=str(DEFAULT_COST_MODEL / "storage-cost-model.md"))
    ap.add_argument("--contract-copy", default=str(ROOT / "utils/storage-cost-calculator/model-contract.json"))
    ap.add_argument("--code-dir", default=str(ROOT / "utils/storage-cost-calculator"))
    ap.add_argument("--skip-node", action="store_true", help="skip the Node suite (dev mode only)")
    args = ap.parse_args(argv)
    cm, problems = Path(args.cost_model), []
    if args.mode == "final":
        problems += [f"missing artifact: {cm / n}" for n in REQUIRED if not (cm / n).exists()]
        if args.skip_node:
            problems.append("--skip-node is not allowed in final mode")
    elif args.skip_node:
        print("note: Node suite skipped (--skip-node)", file=sys.stderr)
    contract = None
    if (cm / CONTRACT).exists():
        contract = _load(cm / CONTRACT, problems)
    elif args.mode == "final":
        problems.append(f"missing artifact: {cm / CONTRACT} (run tools/generate_contract.py generate)")
    else:
        print(f"note: pending: {cm / CONTRACT} not generated; evidence reads the reports' coefficients.json", file=sys.stderr)
    reports, found, notes = report_dirs(args.reports)
    problems += found
    for line in notes:
        print(line, file=sys.stderr)
    if args.mode == "final" and not reports:
        problems.append(f"missing artifact: no report directory matches {args.reports}")
    paths = sorted(cm.glob("formulas*.json")) + [r / "formulas.json" for r in reports if (r / "formulas.json").exists()]
    # A report's formulas.json may hold one formulas/1 object or an array of them (one per variant).
    formulas = [x for f in (_load(p, problems) for p in paths) if f is not None for x in (f if isinstance(f, list) else [f])]
    coefficients, measurements, predictions, per_report = [], [], [], []
    for r in reports:
        if (r / "coefficients.json").exists():
            loaded = _load(r / "coefficients.json", problems)
            if isinstance(loaded, list):
                coefficients += loaded
            elif loaded is not None:
                problems.append(f"{r / 'coefficients.json'}: expected a JSON array")
        if (r / "measurements.jsonl").exists():
            per_report.append(_load_jsonl(r / "measurements.jsonl", problems))
            measurements += per_report[-1]
        predictions += [x for x in (_load(p, problems) for p in sorted((r / "predictions").glob("*.json"))) if x is not None]
    reg = _load(cm / "inputs-registry.json", problems) if (cm / "inputs-registry.json").exists() else None
    cov = _load(cm / "coverage.json", problems) if (cm / "coverage.json").exists() else None
    bil = _load(cm / "billing-contract.json", problems) if (cm / "billing-contract.json").exists() else None
    if reg is not None:
        problems += dependency(reg, formulas)
    if cov is not None:
        pending = []
        # The generated contract says which families occur per variant; without it every family needs evidence.
        problems += support(reg or {"quantities": []}, cov, formulas, require_formulas=args.mode == "final", pending=pending,
                            applicability=(contract or {}).get("applicability"))
        if pending:
            print(f"note: {len(pending)} support cells pending (no evidence, no not_supported state)", file=sys.stderr)
        ids = {m.get("measurement_id") for m in measurements}
        if contract is not None:
            by_report = {e.get("report"): e for e in (contract.get("manifest") or {}).get("evidence") or []}
            owners = (contract.get("owners") or {}).get("coefficients") or {}
            identity = {c: by_report[r] for c, r in owners.items() if r in by_report}
            problems += evidence(cov, contract.get("coefficients") or [], ids, identity)
        else:
            problems += evidence(cov, coefficients, ids)
    if bil is not None:
        problems += billing(bil)
    stale = []
    problems += accounting(measurements, predictions, node_test_cmd=False if args.skip_node else None, mode=args.mode,
                           raw=raw_slices(reports), contract=contract, billing_contract=bil, stale=stale)
    if args.mode == "final":
        problems += [f"{r}: raw/SHA256SUMS missing" for r in reports if (r / "measurements.jsonl").exists() and not (r / "raw/SHA256SUMS").exists()]
    problems += reproduction([tuple(c.split("=", 1)) for c in args.copy], [r / "raw/SHA256SUMS" for r in reports if (r / "raw/SHA256SUMS").exists()])
    if contract is not None:
        opts = {"cost_model": str(cm), "reports": args.reports, "page": args.page, "doc": args.doc, "copy": args.contract_copy,
                "code_dir": args.code_dir, "fixtures_out": None}
        stale += manifest(contract, opts, args.mode, fresh)
    classify_stale(stale, args.mode, problems, "closure_check")
    for records in per_report:
        problems += identities(records)
    problems += rerender(reports)
    for p in problems:
        print(f"closure_check: {p}", file=sys.stderr)
    print(f"closure_check ({args.mode}): {'FAIL, %d problem(s)' % len(problems) if problems else 'ok'}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
