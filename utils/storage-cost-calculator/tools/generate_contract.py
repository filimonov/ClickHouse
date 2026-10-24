#!/usr/bin/env python3
"""Builds `model-contract.json` from the cost-model artifacts and the report directories (design §6).

`generate` merges every report's `formulas.json` and `coefficients.json` (one owner per cell and per coefficient),
embeds the registry and the billing contract, computes the reproduction manifest, rebuilds the measured fixtures,
and writes the contract to the standalone copies, the page block and the formula document blocks. It writes nothing
when a problem is found. `check` renders the same outputs in memory, fails on any byte that differs from the files,
then runs every closure check (tools/closure_check.py) in the same mode.

`--mode dev` reports a missing report, raw digest or code file, and a stale generated file, as a "pending" line;
`--mode final` fails on them.

    python3 utils/storage-cost-calculator/tools/generate_contract.py generate
    python3 utils/storage-cost-calculator/tools/generate_contract.py check --mode final
"""

import argparse
import glob
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import artifacts
import build_fixtures
import closure_check

ROOT = Path(__file__).resolve().parents[3]
CALC = ROOT / "utils/storage-cost-calculator"
DEFAULTS = {
    "cost_model": ROOT / "docs/superpowers/cost-model",
    "reports": closure_check.DEFAULT_REPORTS,
    "page": CALC / "index.html",
    "doc": ROOT / "docs/superpowers/cost-model/storage-cost-model.md",
    "copy": CALC / "model-contract.json",
    "code_dir": CALC,
    "fixtures_out": CALC / "test/fixtures/measured",
}
CONTRACT = closure_check.CONTRACT
CODE_FILES = closure_check.CODE_FILES
# Every script the page loads, in load order; the hashed CODE_FILES are a subset.
PAGE_SCRIPTS = CODE_FILES + ("viewmodel.js", "ui.js")
STATE_IDS = ("ok", "not_supported", "backlog_grows", "unsupported_cut", "unavailable")
# A formulas cell outside the domain operations is a card or probe cell: kept for the drill, never evaluated.
PROBE_PREFIX = "probe_"
BREAKEVEN = "breakeven.json"
BLOCKS = closure_check.BLOCKS
# The default form must fit one screen: workload knobs plus at most this many ClickHouse settings.
MAX_SETTING_INPUTS = 10
UI_SHORT_MAX = 40
SOURCE_PREFIX = "tmp/src-26.8/"
MEASURED_RELEASE = re.search(r"src-(\d+\.\d+)", SOURCE_PREFIX).group(1)
MEASURED_STORE = "RustFS"

# Validates the assembled contract with the page's own loader and renders every expression with the page's renderer,
# so the document shows exactly what "show calculation" shows.
NODE_RENDER = r"""
const dir = process.argv[1];
const model = require(dir + '/model.js');
const loader = require(dir + '/contract.js');
let input = '';
process.stdin.on('data', (d) => { input += d; });
process.stdin.on('end', () => {
  const { contract: c, files, card_cells: cardCells } = JSON.parse(input);
  const problems = [];
  const merged = loader.mergeFormulas(files);
  problems.push(...merged.problems);
  c.formulas = {};
  c.states = {};
  for (const v of c.variants) {
    c.formulas[v] = merged.formulas[v] || {};
    c.states[v] = merged.states[v] || [];
  }
  try { loader.load(c); } catch (e) { problems.push(...(e.problems || [String(e)])); }
  const registryArtifact = { quantities: c.registry, sets: c.sets, precedence: c.precedence };
  const render = (node, where) => {
    try { return model.render(node, c.registry); } catch (e) { problems.push(`${where}: cannot render: ${e.message}`); return null; }
  };
  const renderCell = (cell, where, dims) => ({
    rows: (cell.ledger_rows || []).map((r, i) => {
      if (dims) for (const p of model.checkDims(r.expr, registryArtifact)) problems.push(`${where}.ledger_rows[${i}].expr${p.slice(1)}`);
      return render(r.expr, `${where}.ledger_rows[${i}]`);
    }),
    shares: ((cell.allocation && cell.allocation.shares) || []).map((s, i) => render(s.weight, `${where}.allocation.shares[${i}]`)),
    states: (cell.states || []).map((s, i) => (s.cond === true ? null : render(s.cond, `${where}.states[${i}]`))),
    notes: (cell.notes || []).map((n, i) => render(n, `${where}.notes[${i}]`)),
  });
  const cells = {};
  for (const v of c.variants) for (const [op, cell] of Object.entries(c.formulas[v])) cells[`${v}/${op}`] = renderCell(cell, `formulas.${v}.${op}`, false);
  const cards = {};
  for (const [key, cell] of Object.entries(cardCells)) cards[key] = renderCell(cell, `card_cells.${key}`, true);
  const rules = {};
  for (const q of c.registry) if (q.rule) rules[q.id] = render(q.rule, `registry.${q.id}.rule`);
  const precedence = c.precedence.map((g, i) => Object.fromEntries(Object.entries(g.rules).map(([id, r]) => [id, render(r, `precedence[${i}].rules.${id}`)])));
  process.stdout.write(JSON.stringify({ problems, formulas: c.formulas, states: c.states, cells, cards, rules, precedence }));
});
"""


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def _json(path, problems):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        problems.append(f"{path}: cannot load: {e}")
        return None


def _families_on(coverage, variant):
    """family -> True when every case of every card of that family measured on `variant` is cut."""
    out = {}
    for card in coverage["cards"]:
        if variant in card["model_variants"]:
            cut = all(k["status"] == "cut" for k in card["cases"])
            out[card["family"]] = out.get(card["family"], True) and cut
    return out


def applicability(coverage, files_by_variant, cells, problems):
    """variant -> operation -> family -> supported | not_supported | unsupported_cut | not_applicable, whole domain.

    A cell that is unconditionally `not_supported` makes every family of its operation `not_supported`. Otherwise
    the variant's formulas files list the families that occur: a listed `supported` family that coverage measures on
    the variant only through cut cases becomes `unsupported_cut`. An unlisted family is `not_applicable`, never
    `supported`, so a family needs evidence only where a report claims it.
    """
    domain = coverage["domain"]
    out = {}
    for v in domain["variants"]:
        explicit = {}
        for owner, f in files_by_variant.get(v, []):
            for op, fams in (f.get("applicability") or {}).items():
                if op not in domain["operations"]:
                    problems.append(f"{owner}: applicability.{op}: {op} is not a domain operation")
                    continue
                for fam, value in fams.items():
                    if fam not in domain["families"]:
                        problems.append(f"{owner}: applicability.{op}.{fam}: {fam} is not a domain family")
                    elif (op, fam) in explicit:
                        problems.append(f"applicability {v}.{op}.{fam} is set by both {explicit[(op, fam)][1]} and {owner}")
                    else:
                        explicit[(op, fam)] = (value, owner)
        occurs = _families_on(coverage, v)
        out[v] = {}
        for op in domain["operations"]:
            cell = cells.get((v, op))
            never = cell is not None and closure_check.unconditional_not_supported(cell)
            row = {}
            for fam in domain["families"]:
                if (op, fam) in explicit:
                    value, owner = explicit[(op, fam)]
                    # `not_supported` is per operation: the variant cannot perform it at all (spec §4.4).
                    if value == "not_supported" and cell is not None and not never:
                        problems.append(f"{owner}: applicability {v}.{op}.{fam} is not_supported but the {v} cell "
                                        f"of {op} performs the operation; list only the families that occur")
                    if never and value != "not_supported":
                        problems.append(f"{owner}: applicability {v}.{op}.{fam} is {value} but the {v} cell of {op} "
                                        "is unconditionally not_supported")
                    row[fam] = "unsupported_cut" if value == "supported" and occurs.get(fam) else value
                elif never:
                    row[fam] = "not_supported"
                else:
                    row[fam] = "not_applicable"
            out[v][op] = row
    return out


def _canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _node(code_dir, payload):
    if not shutil.which("node"):
        return None, ["node missing: the contract cannot be validated or rendered"]
    done = subprocess.run(["node", "-e", NODE_RENDER, str(Path(code_dir).resolve())], input=json.dumps(payload),
                          capture_output=True, text=True)
    if done.returncode != 0:
        return None, [f"node render failed: {done.stderr.strip()[-800:]}"]
    return json.loads(done.stdout), []


def build(opts, mode="dev"):
    """Returns (outputs {path: text}, fixtures (fixtures, coefficient_only), problems, pending, notes)."""
    problems, pending = [], []
    cm = Path(opts["cost_model"])
    reports, found, notes = closure_check.report_dirs(opts["reports"])
    problems += found
    reg = _json(cm / "inputs-registry.json", problems)
    cov = _json(cm / "coverage.json", problems)
    bil = _json(cm / "billing-contract.json", problems)
    if reg is None or cov is None or bil is None:
        return None, None, problems, pending, notes
    for label, bad in (("registry", artifacts.validate_registry(reg)), ("coverage", artifacts.validate_coverage(cov))):
        problems += [f"{label} {p}" for p in bad]
    problems += registry_ui_problems(reg)
    if problems:
        return None, None, problems, pending, notes
    domain = cov["domain"]

    coefficients, coef_owner = [], {}
    files, files_by_variant, cells, cell_owner, card_cells = [], {}, {}, {}, {}
    formulas_list, measurement_ids, evidence = [], set(), []
    tables = artifacts.table_arities_from_registry(reg)
    for r in reports:
        for name in ("formulas.json", "coefficients.json"):
            if not (r / name).exists():
                (problems if mode == "final" else pending).append(f"{r.name}: {name} missing")
        loaded = _json(r / "coefficients.json", problems) if (r / "coefficients.json").exists() else []
        if not isinstance(loaded, list):
            problems.append(f"{r / 'coefficients.json'}: expected a JSON array")
            loaded = []
        for c in loaded:
            cid = c.get("id") if isinstance(c, dict) else None
            if isinstance(c, dict) and "verified_range" in c and "validity" in c:
                problems.append(f"coefficient {cid}: both verified_range and its old name validity")
            if cid in coef_owner:
                problems.append(f"coefficient {cid} is owned by both {coef_owner[cid]} and {r.name}")
                continue
            coef_owner[cid] = r.name
            coefficients.append(c)
        tables.update(artifacts.table_arities_from_coefficients([c for c in loaded if isinstance(c, dict)]))
        records = closure_check.report_records(r, problems)
        measurement_ids |= {m.get("measurement_id") for m in records}
        evidence.append({"report": r.name, **closure_check.report_identity(r, records, mode, problems, pending),
                         "raw_sha256sums": closure_check.raw_digest(r, mode, problems, pending)})
    for r in reports:
        doc = _json(r / "formulas.json", problems) if (r / "formulas.json").exists() else []
        for i, f in enumerate(doc if isinstance(doc, list) else [doc]):
            owner = f"{r.name}/formulas.json[{i}]"
            bad = artifacts.validate_formulas(f, tables)
            if bad:
                problems += [f"{owner} {p}" for p in bad]
                continue
            v = f["variant"]
            if v not in domain["variants"]:
                problems.append(f"{owner}: variant {v} is not in coverage domain.variants")
                continue
            formulas_list.append(f)
            files_by_variant.setdefault(v, []).append((owner, f))
            domain_cells = {}
            for op, cell in f["cells"].items():
                if op in domain["operations"]:
                    domain_cells[op] = cell
                    if (v, op) not in cells:
                        cells[(v, op)], cell_owner[f"{v}/{op}"] = cell, r.name
                    continue
                head = op.split(".", 1)[0]
                if not (head.startswith(PROBE_PREFIX) or head in {c["name"] for c in cov["cards"]}):
                    problems.append(f"{owner}: cell {op} is neither a domain operation nor a card or {PROBE_PREFIX}* cell")
                elif f"{v}/{op}" in card_cells:
                    problems.append(f"card cell {v}/{op} is defined by both {cell_owner[f'{v}/{op}']} and {r.name}")
                else:
                    card_cells[f"{v}/{op}"], cell_owner[f"{v}/{op}"] = cell, r.name
            # The page's merge names the owner in its duplicate-cell problem.
            files.append({"schema": f["schema"], "owner": f"{f['owner']} ({r.name})", "variant": v, "cells": domain_cells})

    if mode == "final":
        # A measured registry row is owned only through the report that holds its coefficient.
        owned = set(coef_owner) | {q["id"] for q in reg["quantities"] if q["kind"] != "measured"}
        priority = {f"{c['name']}.{k['case_id']}": k["priority"] for c in cov["cards"] for k in c["cases"]}
        for coef in sorted({e["coefficient"] for e in cov["evidence"] if priority.get(e["experiment_id"]) == "P1" and e["status"] != "cut"}):
            if coef not in owned:
                problems.append(f"missing report: P1 coefficient {coef} in coverage has no owner (coefficients.json or registry)")
        problems += [f"missing report: variant {v} has no formulas.json" for v in domain["variants"] if v not in files_by_variant]

    # Static closure checks over exactly what the contract is built from; the full set runs in `check`.
    app = applicability(cov, files_by_variant, cells, problems)
    if mode == "dev":  # final mode: the support closure fails on it
        pending += [f"variant {v}: no formulas file lists a supported family" for v in domain["variants"]
                    if not any(x == "supported" for row in app[v].values() for x in row.values())]
    problems += closure_check.dependency(reg, formulas_list)
    problems += closure_check.support(reg, cov, formulas_list, require_formulas=mode == "final", applicability=app,
                                      coefficients=coefficients)
    # Counted here, not taken from `support`, so the evidence statement is the same in both modes.
    assumptions = []
    support_pending = closure_check._pending_cells(cov, formulas_list, app, reg, assumed=closure_check.assumed_keys(coefficients),
                                                   assumptions=assumptions)
    problems += closure_check.evidence(cov, coefficients, measurement_ids, coefficient_identity(coefficients, coef_owner, evidence))
    problems += closure_check.billing(bil)
    if support_pending and mode == "dev":
        pending.append(f"{len(support_pending)} support cells have no evidence and no not_supported state")

    code_sha, code_files, code_missing = closure_check.code_manifest(opts["code_dir"], mode, problems, pending)
    manifest = {"code_sha256": code_sha, "code_files": code_files, "code_missing": code_missing, "contract_version": ""}
    for (key, name), doc in zip(closure_check.INPUTS, (reg, cov, bil)):
        manifest[f"{key}_version"] = doc["version"]
        manifest[f"{key}_sha256"] = sha256((cm / name).read_bytes())
    manifest["evidence"] = evidence
    contract = {
        "schema": "contract/1", "version": "", "manifest": manifest,
        "evidence_statement": evidence_statement(reports, coefficients, cov, reg, bil, len(support_pending), measured_build(evidence),
                                                 assumptions),
        "registry": with_verified_range(with_measured_by(reg["quantities"], coefficients, coef_owner, evidence)),
        "sets": reg["sets"], "precedence": reg["precedence"],
        "coefficients": with_verified_range(coefficients), "billing": bil,
        "variants": domain["variants"], "operations": domain["operations"], "families": domain["families"],
        "state_ids": list(STATE_IDS),
        "applicability": app,
        "formulas": {}, "states": {}, "card_cells": {k: card_cells[k] for k in sorted(card_cells)},
        "owners": {"coefficients": {k: coef_owner[k] for k in sorted(coef_owner, key=str)},
                   "cells": {k: cell_owner[k] for k in sorted(cell_owner)}},
    }
    rendered, bad = _node(opts["code_dir"], {"contract": contract, "files": files, "card_cells": contract["card_cells"]})
    problems += bad
    if rendered is None:
        return None, None, problems, pending, notes
    problems += rendered["problems"]
    contract["formulas"], contract["states"] = rendered["formulas"], rendered["states"]
    contract["evidence_by_variant"] = evidence_by_variant(contract["formulas"], coefficients, cov)
    # The version names the content: two builds from the same inputs carry the same version. The break-even file
    # records this version (it is swept on the contract), so its own hash joins the manifest after the version is
    # fixed; otherwise sweeping would change the version it must name.
    version = sha256(_canonical(contract).encode())[:16]
    contract["version"] = manifest["contract_version"] = version
    breakeven, breakeven_body = breakeven_input(cm / BREAKEVEN, version, mode, problems, pending)
    manifest["breakeven_sha256"] = sha256((cm / BREAKEVEN).read_bytes()) if breakeven is not None else None

    fixtures = build_fixtures.build(reports, cov, reg)
    problems += [f"measured fixtures: {p}" for p in fixtures[2]]
    standalone = json.dumps(contract, indent=1, ensure_ascii=False) + "\n"
    outputs = {Path(cm / CONTRACT): standalone, Path(opts["copy"]): standalone}
    page, doc = Path(opts["page"]), Path(opts["doc"])
    for path, blocks in ((page, {"page": embed(contract)}),
                         (doc, {"formulas": formulas_block(contract, rendered), "inputs": inputs_block(contract, rendered),
                                  "breakeven": breakeven_body})):
        try:
            text = path.read_text()
        except OSError as e:
            problems.append(f"{path}: cannot read the template: {e}")
            continue
        for name, body in blocks.items():
            text = replace_block(text, name, body, path, problems)
        outputs[path] = text
    if page in outputs:
        problems += page_scripts(outputs[page], page)
    return outputs, fixtures[:2], problems, pending, notes


def registry_ui_problems(reg):
    """Every input needs a page tier and a short label; the setting tier is capped so the default form stays short."""
    out, settings = [], []
    for q in reg["quantities"]:
        if q["kind"] != "input":
            continue
        ui = q.get("ui")
        if ui is None:
            out.append(f"registry {q['id']}: input has no ui tier")
            continue
        if len(ui["short"]) > UI_SHORT_MAX:
            out.append(f"registry {q['id']}: ui.short is {len(ui['short'])} characters, at most {UI_SHORT_MAX}")
        if ui["tier"] == "setting":
            settings.append(q["id"])
    if len(settings) > MAX_SETTING_INPUTS:
        out.append(f"registry: {len(settings)} inputs in the setting tier, at most {MAX_SETTING_INPUTS}: {', '.join(settings)}")
    return out


def cite(text, commit):
    """`tmp/src-26.8/src/X.cpp:12` -> `src/X.cpp:12 @ <commit>`: the reader can open the file at the measured build."""
    if not commit:
        return text.replace(SOURCE_PREFIX, "")
    return re.sub(r"(?:tmp/src-26\.8/)?(src/[A-Za-z0-9_./-]+:\d+(?:-\d+)?)(?![\d-]| @ )", rf"\1 @ {commit}", text)


def measured_build(evidence):
    """Short ClickHouse commit every report measured, or None when the reports disagree."""
    commits = {e.get("commit") for e in evidence if e.get("commit")}
    return next(iter(commits))[:11] if len(commits) == 1 else None


def measured_by(q, coefficients, coef_owner, commit):
    ev = q["evidence"]
    head = {"source": "Default from", "assumed": "Assumed:", "provider": "Provider rule:", "measured": "Measured:"}[ev["kind"]]
    parts = [f"{head} {cite(ev['ref'], commit)}."]
    for c in coefficients:
        r = (artifacts.verified_range(c) or {}).get(q["id"])
        if not r:
            continue
        ids = (c.get("evidence") or {}).get("measurement_ids") or []
        report = coef_owner.get(c["id"], "no report")
        parts.append(f"Measured from {r[0]:g} to {r[1]:g} by {c['id']} ({report}, {len(ids)} measurement(s)).")
    return " ".join(parts)


def with_verified_range(records):
    """Contract copies carry `verified_range`, and `validity` with the same value for readers not yet renamed."""
    out = []
    for x in records:
        if isinstance(x, dict) and ("verified_range" in x or "validity" in x):
            r = artifacts.verified_range(x)
            x = dict(x, verified_range=r, validity=r)
        out.append(x)
    return out


def with_measured_by(quantities, coefficients, coef_owner, evidence):
    """Registry rows for the contract; each input's `ui` gains the evidence sentence the info dialog shows."""
    commit = measured_build(evidence)
    out = []
    for q in quantities:
        if q["kind"] == "input" and q.get("ui"):
            q = dict(q, ui=dict(q["ui"], measured_by=measured_by(q, coefficients, coef_owner, commit)))
        out.append(q)
    return out


def coefficient_identity(coefficients, coef_owner, evidence):
    """coefficient id -> identity of the report that owns it, for the evidence closure."""
    by_report = {e["report"]: e for e in evidence}
    return {c["id"]: by_report[coef_owner[c["id"]]] for c in coefficients
            if isinstance(c, dict) and coef_owner.get(c.get("id")) in by_report}


def _coef_ids(node, out):
    if isinstance(node, dict):
        if node.get("kind") == "coef":
            out.add(node["id"])
        elif node.get("kind") == "lookup":
            out.add(node["table_id"])
        for v in node.values():
            _coef_ids(v, out)
    elif isinstance(node, list):
        for v in node:
            _coef_ids(v, out)
    return out


def evidence_by_variant(formulas, coefficients, cov):
    """Per variant, how many operation cells rest on coefficients with a done fit and hold-out, on a fit only, or on
    assumptions alone (no measured coefficient)."""
    measured = {c["id"] for c in coefficients if isinstance(c, dict) and (c.get("evidence") or {}).get("kind") == "measured"}
    fit = {e["coefficient"] for e in cov["evidence"] if e["role"] == "fit" and e["status"] in ("done", "consumed")}
    holdout = {e["coefficient"] for e in cov["evidence"] if e["role"] == "holdout" and e["status"] == "done"}
    out = {}
    for v in sorted(formulas):
        counts = {"fit_and_holdout": 0, "fit_only": 0, "assumption": 0}
        for cell in formulas[v].values():
            if not cell.get("ledger_rows"):
                continue
            used = _coef_ids(cell, set()) & measured
            if not used:
                counts["assumption"] += 1
            elif used <= (fit & holdout):
                counts["fit_and_holdout"] += 1
            else:
                counts["fit_only"] += 1
        out[v] = counts
    return out


def evidence_statement(reports, coefficients, cov, reg, bil, pending_cells, build=None, assumptions=()):
    kinds = [(c.get("evidence") or {}).get("kind") for c in coefficients if isinstance(c, dict)]
    p1 = {f"{c['name']}.{k['case_id']}" for c in cov["cards"] for k in c["cases"] if k["priority"] == "P1"}
    closed = sorted({e["coefficient"] for e in cov["evidence"] if e["experiment_id"] in p1 and e["role"] == "holdout" and e["status"] == "done"}
                    & {e["coefficient"] for e in cov["evidence"] if e["experiment_id"] in p1 and e["role"] == "fit" and e["status"] in ("done", "consumed")})
    names = ", ".join(r.name for r in reports) or "none"
    measured_on = (f"Measured on ClickHouse commit {build} (release {MEASURED_RELEASE}) against a {MEASURED_STORE} S3 store. "
                   if build else "")
    return (f"{measured_on}Built from {len(reports)} report(s): {names}. {len(coefficients)} coefficient(s): "
            f"{kinds.count('measured')} measured, {kinds.count('source')} pinned to source. "
            f"{len(closed)} P1 coefficient(s) have a done fit and a done hold-out. "
            f"Registry {reg['version']}, coverage {cov['version']}, billing {bil['version']}. "
            f"{pending_cells} variant x operation x family cell(s) have no evidence yet."
            + assumption_text(assumptions))


def assumption_text(assumptions):
    """One sentence per assumed coefficient key, naming the cells it backs and the owner's assumption."""
    cells = {}
    for v, op, fam, coef, key, note in assumptions:
        cells.setdefault((coef, key, note), []).append(f"{v}/{op}/{fam}")
    return "".join(f" Assumption {coef}[{key}] backs {', '.join(sorted(set(where)))}: {note}."
                   for (coef, key, note), where in sorted(cells.items()))


def embed(contract):
    # `<` only occurs inside JSON strings, so escaping it keeps the JSON valid and the script element unbroken.
    body = json.dumps(contract, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    return f'<script type="application/json" id="contract">{body}</script>'


def replace_block(text, name, body, path, problems):
    begin, end = BLOCKS[name]
    if text.count(begin) != 1 or text.count(end) != 1 or text.index(begin) > text.index(end):
        problems.append(f"{path}: needs exactly one {begin} ... {end} pair")
        return text
    head, rest = text.split(begin, 1)
    tail = rest.split(end, 1)[1]
    sep = "" if name == "page" else "\n"
    return f"{head}{begin}{sep}{body}{sep}{end}{tail}"


def page_scripts(text, path):
    found = re.findall(r'<script src="([^"]+)"></script>', text)
    if found != list(PAGE_SCRIPTS):
        return [f"{path}: script tags {found} must be exactly {list(PAGE_SCRIPTS)} in that order"]
    return []


# --- document rendering ------------------------------------------------------------------------

def _anchor(*parts):
    return re.sub(r"[^a-z0-9]+", "-", "-".join(parts).lower()).strip("-")


def _table_safe(s):
    return str(s).replace("|", "\\|").replace("\n", " ")


def _cell_text(s):
    # Plain text of a table cell; docs pages are MDX, where `<` and braces open JSX.
    return _table_safe(s).replace("<", "&lt;").replace("{", "\\{").replace("}", "\\}")


def _code(s):
    return "`" + _table_safe(s).replace("`", "'") + "`" if s not in (None, "") else ""


def _value(c):
    v = c.get("value")
    if c.get("kind") == "table" and isinstance(v, dict):
        return f"table over {', '.join(v.get('index', []))}"
    return json.dumps(v)


def _coefficient_rows(contract, refs):
    by_id = {c["id"]: c for c in contract["coefficients"] if isinstance(c, dict)}
    reg = {q["id"]: q for q in contract["registry"]}
    reg.update({q["coef"]: q for q in contract["registry"] if q["coef"]})
    rows = []
    for ref in sorted(refs):
        if ref in by_id:
            c, ev = by_id[ref], by_id[ref].get("evidence") or {}
            owner = contract["owners"]["coefficients"].get(ref, "")
            rows.append((ref, _value(c), c.get("dim"), ev.get("kind"), ev.get("ref"), ev.get("version"),
                         json.dumps(artifacts.verified_range(c)) if artifacts.verified_range(c) is not None else "", owner))
        elif ref in reg:
            q = reg[ref]
            rows.append((ref, json.dumps(q["default"]), q["unit"], q["evidence"]["kind"], q["evidence"]["ref"],
                         q["evidence"]["version"], json.dumps(artifacts.verified_range(q)) if artifacts.verified_range(q) else "", "registry"))
        else:
            rows.append((ref, "", "", "missing", "", "", "", ""))
    return rows


def _cell_section(contract, title, anchor, cell, rendered, owner):
    out = [f"### {title} {{#{anchor}}}", "", f"Owner: `{owner}`.", ""]
    if cell["ledger_rows"]:
        out += ["| Row | Op | Outcome | Scope | Caller | Expression |", "|---|---|---|---|---|---|"]
        for row, text in zip(cell["ledger_rows"], rendered["rows"]):
            out.append(f"| {_code(row['row_type'])} | {_code(row['op_kind'])} | {_code(row['outcome_class'])} | "
                       f"{_code(row['scope'])} | {_code(row.get('caller'))} | {_code(text)} |")
    else:
        out.append("No ledger rows.")
    if cell.get("allocation"):
        out += ["", "Allocation shares:", ""] + [f"- {_code(s['cell'])}: {_code(t)}" for s, t in zip(cell["allocation"]["shares"], rendered["shares"])]
    if cell.get("states"):
        out += ["", "| State | Condition | Reason |", "|---|---|---|"]
        out += [f"| {_code(s['id'])} | {_code(t) if t is not None else 'always'} | {_cell_text(s['reason'])} |"
                for s, t in zip(cell["states"], rendered["states"])]
    if cell.get("notes"):
        out += ["", "Notes:", ""] + [f"- {_code(t)}" for t in rendered["notes"]]
    for e in cell.get("excluded") or []:
        out.append(f"- Excluded {e['kind']} {_code(e['id'])}: {_cell_text(e['reason'])}")
    refs = {ref for kind, ref in closure_check._walk(cell, []) if kind in ("coef", "table")}
    if refs:
        out += ["", "| Coefficient | Value | Dim | Evidence | Ref | Version | Validity | Owner |", "|---|---|---|---|---|---|---|---|"]
        out += ["| " + " | ".join(_code(x) if i in (0, 1, 2, 6) else _cell_text(x or "") for i, x in enumerate(row)) + " |"
                for row in _coefficient_rows(contract, refs)]
    return out + [""]


def formulas_block(contract, rendered):
    out = ["Generated by `tools/generate_contract.py` from `model-contract.json` version "
           f"`{contract['version']}`. Do not edit.", "",
           "| Variant | Operation | Cell owner | Applicability |", "|---|---|---|---|"]
    for v in contract["variants"]:
        for op in contract["operations"]:
            app = contract["applicability"][v][op]
            values = sorted(set(app.values()))
            summary = values[0] if len(values) == 1 else "; ".join(f"{x}: {', '.join(f for f in app if app[f] == x)}" for x in values)
            owner = contract["owners"]["cells"].get(f"{v}/{op}") if op in contract["formulas"][v] else None
            out.append(f"| {_code(v)} | {_code(op)} | {_code(owner) if owner else 'no cell'} | {_cell_text(summary)} |")
    out.append("")
    for v in contract["variants"]:
        for op in contract["operations"]:
            cell = contract["formulas"][v].get(op)
            if cell is not None:
                out += _cell_section(contract, f"`{v}` / `{op}`", _anchor("formula", v, op), cell,
                                     rendered["cells"][f"{v}/{op}"], contract["owners"]["cells"][f"{v}/{op}"])
    for key, cell in contract["card_cells"].items():
        v, name = key.split("/", 1)
        out += _cell_section(contract, f"`{v}` / card cell `{name}`", _anchor("formula", v, "card", name), cell,
                             rendered["cards"][key], contract["owners"]["cells"][key])
    return "\n".join(out).rstrip("\n")


def inputs_block(contract, rendered):
    out = [f"Registry version `{contract['manifest']['registry_version']}`. Generated; do not edit.", "",
           "| Id | Block | Label | Kind | Scope | Unit | Default | Validity | Rule | Evidence | Status |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    for q in contract["registry"]:
        ev = q["evidence"]
        out.append(f"| {_code(q['id'])} | {_code(q['block'])} | {_cell_text(q['label'])} | {_code(q['kind'])} | "
                   f"{_code(q['scope'])} | {_code(q['unit'])} | {_code(json.dumps(q['default'])) if q['default'] is not None else ''} | "
                   f"{_code(json.dumps(artifacts.verified_range(q))) if artifacts.verified_range(q) else ''} | {_code(rendered['rules'].get(q['id']))} | "
                   f"{_cell_text(ev['kind'])}: {_cell_text(ev['ref'])} ({_cell_text(ev['version'])}) | {_code(q['status'])} |")
    out += ["", "Form placement. `workload` and `setting` inputs make up the default form (at most "
            f"{MAX_SETTING_INPUTS} settings); `advanced` inputs sit in the collapsed low-level section; `hidden` inputs are "
            "model constants or table keys, shown only in the info dialog. Rule: a product manager can set it after one "
            "sentence and it moves money for a typical workload -> `setting`; can set it but it rarely matters -> "
            "`advanced`; needs more than one sentence -> `hidden`.", "",
            "| Tier | Id | ClickHouse name | Form label | Block |", "|---|---|---|---|---|"]
    order = {t: i for i, t in enumerate(artifacts.REGISTRY_UI_TIERS)}
    inputs = [q for q in contract["registry"] if q["kind"] == "input" and q.get("ui")]
    for q in sorted(inputs, key=lambda q: (order[q["ui"]["tier"]], q["block"], q["id"])):
        ui = q["ui"]
        out.append(f"| {_code(ui['tier'])} | {_code(q['id'])} | {_code(ui['name']) if ui['name'] else ''} | "
                   f"{_cell_text(ui['short'])} | {_code(q['block'])} |")
    if contract["precedence"]:
        out += ["", "| Precedence group | Locked by default | Rules |", "|---|---|---|"]
        for g, rules in zip(contract["precedence"], rendered["precedence"]):
            out.append(f"| {', '.join(_code(m) for m in g['group'])} | {', '.join(_code(m) for m in g['default_locked'])} | "
                       + "; ".join(f"{_code(k)} = {_code(t)}" for k, t in rules.items()) + " |")
    return "\n".join(out)


def _money(v):
    return "none" if v is None else f"{v:,.2f}"


def _num(x):
    return f"{int(x):,}" if float(x).is_integer() else f"{x:g}"


def breakeven_block(doc):
    out = [f"Generated from `{BREAKEVEN}` (`tools/sweeps.mjs`), swept on contract version `{doc['contract_version']}` with price "
           f"preset `{doc['price_preset']}`. Do not edit.", "",
           "A crossing is a sign change of the difference of two totals between consecutive ladder values, so the "
           "break-even lies inside the interval, not at a point. A variant that is not `ok` at a value has no total there "
           "and no crossing is drawn through it.", ""]
    out += [f"Ladders: {doc['ladder_rule']}.", ""]
    for sid, sc in doc["scenarios"].items():
        out += [f"### Scenario `{sid}` {{#breakeven-{_anchor(sid)}}}", "",
                f"{_cell_text(sc['label'])}.", "",
                "| Input | Values | Variants compared | Crossing | States |", "|---|---|---|---|---|"]
        for inp, e in sc["sweeps"].items():
            priced = [v for v, st in e["states"].items() if "ok" in st]
            compared = ", ".join(_code(v) for v in priced) if len(priced) >= 2 else "none, fewer than two variants have a total"
            if e["crossing"] is None:
                crossing = "none"
            else:
                crossing = "; ".join(f"{_code(c['a'])} / {_code(c['b'])} between {_num(c['x_between'][0])} and {_num(c['x_between'][1])}"
                                     + (" (extrapolated)" if c["extrapolated"] else "") for c in e["crossing"])
            states = "; ".join(f"{_code(v)}: {' / '.join(st)}" for v, st in e["states"].items())
            values = ", ".join(_num(x) for x in e["values"]) if e["values"] else "none, no positive value to scale"
            out.append(f"| {_code(inp)} | {values} | {compared} | {crossing} | {states} |")
        out.append("")
    return "\n".join(out).rstrip("\n")


def breakeven_input(path, version, mode, problems, pending):
    """(parsed file or None, block body). A missing or stale file is pending in dev mode and a failure in final mode."""
    target = problems if mode == "final" else pending
    if not path.exists():
        target.append(f"{path} is missing; run node utils/storage-cost-calculator/tools/sweeps.mjs")
        return None, "Not generated: run `node utils/storage-cost-calculator/tools/sweeps.mjs`, then `generate`."
    doc = _json(path, problems)
    if not isinstance(doc, dict) or doc.get("schema") != "breakeven/1":
        problems.append(f"{path}: expected a breakeven/1 object")
        return None, ""
    if doc.get("contract_version") != version:
        target.append(f"{path} was swept on contract version {doc.get('contract_version')}, not {version}; run tools/sweeps.mjs")
        return None, f"Stale: swept on contract version `{doc.get('contract_version')}`, not `{version}`. Run `tools/sweeps.mjs`."
    return doc, breakeven_block(doc)


# --- commands ----------------------------------------------------------------------------------

def _report(cmd, mode, problems, pending, notes):
    for line in notes:
        print(line, file=sys.stderr)
    for line in pending:
        print(f"generate_contract: pending: {line}", file=sys.stderr)
    for line in problems:
        print(f"generate_contract: {line}", file=sys.stderr)
    print(f"generate_contract {cmd} ({mode}): {'FAIL, %d problem(s)' % len(problems) if problems else 'ok'}")
    return 1 if problems else 0


def options(args):
    return {k: getattr(args, k) for k in DEFAULTS}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=("generate", "check"))
    ap.add_argument("--mode", choices=("dev", "final"), default="dev")
    for key, value in DEFAULTS.items():
        ap.add_argument("--" + key.replace("_", "-"), dest=key, default=str(value))
    ap.add_argument("--skip-node", action="store_true", help="check: skip the Node suite (dev mode only)")
    args = ap.parse_args(argv)
    opts = options(args)
    outputs, fixtures, problems, pending, notes = build(opts, args.mode)
    if args.command == "generate":
        if not problems:
            for path, text in outputs.items():
                Path(path).write_text(text)
            build_fixtures.write(Path(opts["fixtures_out"]), *fixtures, Path(opts["cost_model"]) / "coverage.json")
        return _report("generate", args.mode, problems, pending, notes)
    if outputs is not None:
        closure_check.classify_stale(closure_check.stale_outputs(outputs), args.mode, problems, "generate_contract")
    code = _report("check", args.mode, problems, pending, notes)
    closure = ["--mode", args.mode, "--cost-model", opts["cost_model"], "--reports", opts["reports"],
               "--page", opts["page"], "--doc", opts["doc"], "--contract-copy", opts["copy"], "--code-dir", opts["code_dir"]]
    closure += ["--skip-node"] if args.skip_node else []
    # The closure check compares the files with this same render instead of building a second one.
    return max(code, closure_check.main(closure, fresh=outputs))


if __name__ == "__main__":
    sys.exit(main())
