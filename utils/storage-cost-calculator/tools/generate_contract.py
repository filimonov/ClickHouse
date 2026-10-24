#!/usr/bin/env python3
"""Builds `model-contract.json` from the cost-model artifacts and the report directories (design §6).

`generate` merges every report's `formulas.json` and `coefficients.json` (one owner per cell and per coefficient),
embeds the registry and the billing contract, computes the reproduction manifest, rebuilds the measured fixtures,
and writes the contract to the standalone copies, the page block and the formula document blocks. It writes nothing
when a problem is found. `check` renders the same outputs in memory, fails on any byte that differs from the files,
then runs every closure check (tools/closure_check.py) in the same mode.

`--mode dev` reports a missing report, raw digest or code file as a "pending" line; `--mode final` fails on it.

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
CONTRACT = "model-contract.json"
# What the page runs: the manifest hashes these, and the page must load them in this order before ui.js.
CODE_FILES = ("model.js", "contract.js", "billing.js", "pipeline.js", "presets.js", "urlstate.js", "viewmodel.js")
PAGE_SCRIPTS = CODE_FILES + ("ui.js",)
STATE_IDS = ("ok", "not_supported", "backlog_grows", "unsupported_cut", "unavailable")
PENDING = "pending"
# A formulas cell outside the domain operations is a card or probe cell: kept for the drill, never evaluated.
PROBE_PREFIX = "probe_"
BLOCKS = {"page": ("<!-- contract:begin -->", "<!-- contract:end -->"),
          "formulas": ("<!-- formulas:begin -->", "<!-- formulas:end -->"),
          "inputs": ("<!-- inputs:begin -->", "<!-- inputs:end -->")}

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


def report_dirs(pattern):
    """Report directories matching `pattern`; a matched directory without README.md is not a report (a ledger)."""
    out, notes = [], []
    for p in sorted(glob.glob(str(pattern))):
        path = Path(p)
        if not path.is_dir():
            continue
        if (path / "README.md").exists():
            out.append(path)
        else:
            notes.append(f"note: {path} has no README.md and is not read as a report")
    return out, notes


def _json(path, problems):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        problems.append(f"{path}: cannot load: {e}")
        return None


def _records(report, problems):
    path = report / "measurements.jsonl"
    rows = []
    if path.exists():
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except ValueError as e:
                    problems.append(f"{path}:{n}: bad JSON line: {e}")
    return rows


def _identity(report, records, mode, problems, pending):
    """The report's source commit, rig commit and executed binary, each single-valued over its ok records."""
    ok = [m for m in records if isinstance(m, dict) and m.get("status") == "ok"]
    if not ok:
        (problems if mode == "final" else pending).append(f"{report.name}: no ok record in measurements.jsonl names its commit")
        return {k: PENDING for k in ("commit", "rig_commit", "binary_sha256")}
    out = {}
    for key in ("commit", "rig_commit", "binary_sha256"):
        seen = sorted({str(m.get(key)) for m in ok})
        if len(seen) != 1:
            problems.append(f"{report.name}: records name several {key} values: {', '.join(seen)}")
        out[key] = seen[0]
    if out["commit"] not in (report / "README.md").read_text():
        problems.append(f"{report.name}: README.md does not name the records' source commit {out['commit']}")
    return out


def _raw_digest(report, mode, problems, pending):
    sums = report / "raw/SHA256SUMS"
    if sums.exists():
        return sha256(sums.read_bytes())
    if (report / "measurements.jsonl").exists():
        (problems if mode == "final" else pending).append(f"{report.name}: raw/SHA256SUMS missing")
    return PENDING


def code_manifest(code_dir, mode, problems, pending):
    """`code_sha256` is the digest of the sorted "<sha256>  <file>" lines of the code files that exist."""
    files = {}
    for name in sorted(CODE_FILES):
        path = Path(code_dir) / name
        if path.exists():
            files[name] = sha256(path.read_bytes())
        else:
            (problems if mode == "final" else pending).append(f"code file {name} missing from {code_dir}")
    listing = "".join(f"{h}  {n}\n" for n, h in files.items())
    return sha256(listing.encode()), files, sorted(set(CODE_FILES) - set(files))


def _families_on(coverage, variant):
    """family -> True when every case of every card of that family measured on `variant` is cut."""
    out = {}
    for card in coverage["cards"]:
        if variant in card["model_variants"]:
            cut = all(k["status"] == "cut" for k in card["cases"])
            out[card["family"]] = out.get(card["family"], True) and cut
    return out


def _unconditional_not_supported(cell):
    return any(s.get("cond") is True and s.get("id") == "not_supported" for s in cell.get("states", []))


def applicability(coverage, files_by_variant, cells, problems):
    """variant -> operation -> family -> supported | not_supported | unsupported_cut over the whole domain.

    A cell that is unconditionally `not_supported` makes every family of its operation `not_supported`. Explicit
    entries in a formulas file win next. Otherwise a family that occurs on the variant only through cut cases is
    `unsupported_cut`, and every other family, including one that does not occur on the variant, is `supported`.
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
            never = cell is not None and _unconditional_not_supported(cell)
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
                    row[fam] = value
                elif never:
                    row[fam] = "not_supported"
                else:
                    row[fam] = "unsupported_cut" if occurs.get(fam) else "supported"
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
    reports, notes = report_dirs(opts["reports"])
    reg = _json(cm / "inputs-registry.json", problems)
    cov = _json(cm / "coverage.json", problems)
    bil = _json(cm / "billing-contract.json", problems)
    if reg is None or cov is None or bil is None:
        return None, None, problems, pending, notes
    for label, bad in (("registry", artifacts.validate_registry(reg)), ("coverage", artifacts.validate_coverage(cov))):
        problems += [f"{label} {p}" for p in bad]
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
            if cid in coef_owner:
                problems.append(f"coefficient {cid} is owned by both {coef_owner[cid]} and {r.name}")
                continue
            coef_owner[cid] = r.name
            coefficients.append(c)
        tables.update(artifacts.table_arities_from_coefficients([c for c in loaded if isinstance(c, dict)]))
        records = _records(r, problems)
        measurement_ids |= {m.get("measurement_id") for m in records if isinstance(m, dict)}
        evidence.append({"report": r.name, **_identity(r, records, mode, problems, pending),
                         "raw_sha256sums": _raw_digest(r, mode, problems, pending)})
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
        owned = set(coef_owner) | {q["id"] for q in reg["quantities"]} | {q["coef"] for q in reg["quantities"] if q["coef"]}
        priority = {f"{c['name']}.{k['case_id']}": k["priority"] for c in cov["cards"] for k in c["cases"]}
        for coef in sorted({e["coefficient"] for e in cov["evidence"] if priority.get(e["experiment_id"]) == "P1" and e["status"] != "cut"}):
            if coef not in owned:
                problems.append(f"missing report: P1 coefficient {coef} in coverage has no owner (coefficients.json or registry)")
        problems += [f"missing report: variant {v} has no formulas.json" for v in domain["variants"] if v not in files_by_variant]

    # Static closure checks over exactly what the contract is built from; the full set runs in `check`.
    problems += closure_check.dependency(reg, formulas_list)
    problems += closure_check.support(reg, cov, formulas_list, require_formulas=mode == "final")
    # Counted here, not taken from `support`, so the evidence statement is the same in both modes.
    support_pending = closure_check._pending_cells(cov, formulas_list)
    problems += closure_check.evidence(cov, coefficients, measurement_ids, report_identity(coefficients, coef_owner, evidence))
    problems += closure_check.billing(bil)
    if support_pending and mode == "dev":
        pending.append(f"{len(support_pending)} support cells have no evidence and no not_supported state")

    code_sha, code_files, code_missing = code_manifest(opts["code_dir"], mode, problems, pending)
    manifest = {
        "code_sha256": code_sha, "code_files": code_files, "code_missing": code_missing,
        "contract_version": "", "registry_version": reg["version"], "coverage_version": cov["version"],
        "billing_version": bil["version"], "evidence": evidence,
    }
    contract = {
        "schema": "contract/1", "version": "", "manifest": manifest,
        "evidence_statement": evidence_statement(reports, coefficients, cov, reg, bil, len(support_pending)),
        "registry": reg["quantities"], "sets": reg["sets"], "precedence": reg["precedence"],
        "coefficients": coefficients, "billing": bil,
        "variants": domain["variants"], "operations": domain["operations"], "families": domain["families"],
        "state_ids": list(STATE_IDS),
        "applicability": applicability(cov, files_by_variant, cells, problems),
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
    # The version names the content: two builds from the same inputs carry the same version.
    version = sha256(_canonical(contract).encode())[:16]
    contract["version"] = manifest["contract_version"] = version

    fixtures = build_fixtures.build(reports, cov)
    problems += [f"measured fixtures: {p}" for p in fixtures[2]]
    standalone = json.dumps(contract, indent=1, ensure_ascii=False) + "\n"
    outputs = {Path(cm / CONTRACT): standalone, Path(opts["copy"]): standalone}
    page, doc = Path(opts["page"]), Path(opts["doc"])
    for path, blocks in ((page, {"page": embed(contract)}),
                         (doc, {"formulas": formulas_block(contract, rendered), "inputs": inputs_block(contract, rendered)})):
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


def report_identity(coefficients, coef_owner, evidence):
    """coefficient id -> identity of the report that owns it, for the evidence closure."""
    by_report = {e["report"]: e for e in evidence}
    return {c["id"]: by_report[coef_owner[c["id"]]] for c in coefficients
            if isinstance(c, dict) and coef_owner.get(c.get("id")) in by_report}


def evidence_statement(reports, coefficients, cov, reg, bil, pending_cells):
    kinds = [(c.get("evidence") or {}).get("kind") for c in coefficients if isinstance(c, dict)]
    p1 = {f"{c['name']}.{k['case_id']}" for c in cov["cards"] for k in c["cases"] if k["priority"] == "P1"}
    closed = sorted({e["coefficient"] for e in cov["evidence"] if e["experiment_id"] in p1 and e["role"] == "holdout" and e["status"] == "done"}
                    & {e["coefficient"] for e in cov["evidence"] if e["experiment_id"] in p1 and e["role"] == "fit" and e["status"] in ("done", "consumed")})
    names = ", ".join(r.name for r in reports) or "none"
    return (f"Built from {len(reports)} report(s): {names}. {len(coefficients)} coefficient(s): "
            f"{kinds.count('measured')} measured, {kinds.count('source')} pinned to source. "
            f"{len(closed)} P1 coefficient(s) have a done fit and a done hold-out. "
            f"Registry {reg['version']}, coverage {cov['version']}, billing {bil['version']}. "
            f"{pending_cells} variant x operation x family cell(s) have no evidence yet.")


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


def extract_block(text, name):
    begin, end = BLOCKS[name]
    if text.count(begin) != 1 or text.count(end) != 1:
        return None
    return text.split(begin, 1)[1].split(end, 1)[0]


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
                         json.dumps(c.get("validity")) if c.get("validity") is not None else "", owner))
        elif ref in reg:
            q = reg[ref]
            rows.append((ref, json.dumps(q["default"]), q["unit"], q["evidence"]["kind"], q["evidence"]["ref"],
                         q["evidence"]["version"], json.dumps(q["validity"]) if q["validity"] else "", "registry"))
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
                   f"{_code(json.dumps(q['validity'])) if q['validity'] else ''} | {_code(rendered['rules'].get(q['id']))} | "
                   f"{_cell_text(ev['kind'])}: {_cell_text(ev['ref'])} ({_cell_text(ev['version'])}) | {_code(q['status'])} |")
    if contract["precedence"]:
        out += ["", "| Precedence group | Locked by default | Rules |", "|---|---|---|"]
        for g, rules in zip(contract["precedence"], rendered["precedence"]):
            out.append(f"| {', '.join(_code(m) for m in g['group'])} | {', '.join(_code(m) for m in g['default_locked'])} | "
                       + "; ".join(f"{_code(k)} = {_code(t)}" for k, t in rules.items()) + " |")
    return "\n".join(out)


# --- commands ----------------------------------------------------------------------------------

def stale(outputs):
    """Problems for every output file whose bytes differ from a fresh render, naming the block that differs."""
    problems = []
    for path, fresh in outputs.items():
        try:
            current = Path(path).read_text()
        except OSError as e:
            problems.append(f"stale: {path}: {e}")
            continue
        if current == fresh:
            continue
        names = [n for n in BLOCKS if extract_block(fresh, n) is not None and extract_block(current, n) != extract_block(fresh, n)]
        problems.append(f"stale: {path} differs from a fresh render" + (f" (block {', '.join(names)})" if names else ""))
    return problems


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
        problems += stale(outputs)
    code = _report("check", args.mode, problems, pending, notes)
    closure = ["--mode", args.mode, "--cost-model", opts["cost_model"], "--reports", opts["reports"],
               "--page", opts["page"], "--doc", opts["doc"], "--contract-copy", opts["copy"], "--code-dir", opts["code_dir"]]
    closure += ["--skip-node"] if args.skip_node else []
    return max(code, closure_check.main(closure))


if __name__ == "__main__":
    sys.exit(main())
