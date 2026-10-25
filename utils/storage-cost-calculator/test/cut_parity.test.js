'use strict';
// The cut rule exists twice: closure_check.py classifies coefficients from coverage (experiments that are cut), the
// page reads `status: unsupported` from the contract. Both must name the same coefficients and the same cells. The
// page side is the code the page runs: `pipeline.unsupportedRefs` and `pipeline.rewriteCut`.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const pipeline = require('../pipeline.js');

const ROOT = path.join(__dirname, '..', '..', '..');
const CM = path.join(ROOT, 'docs', 'superpowers', 'cost-model');
const python = (dir) => JSON.parse(execFileSync('python3', [path.join(__dirname, '..', 'tools', 'closure_check.py'), '--dump-cuts', '--cost-model', dir], { encoding: 'utf8' }));
const readJson = (dir, name) => JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'));

// Per cell, the cut references `rewriteCut` leaves tainted when no input is known: every cut the cell can reach.
function javascript(c) {
  const cut = pipeline.unsupportedRefs(c);
  const ctx = { inputs: new Map(), coefs: new Map(), tables: new Map(), sets: new Map(), vars: new Map(), prices: null };
  const cells = {};
  for (const v of Object.keys(c.formulas).sort()) {
    for (const op of Object.keys(c.formulas[v]).sort()) {
      const refs = new Set();
      for (const row of c.formulas[v][op].ledger_rows) {
        const r = pipeline.rewriteCut(row.expr, ctx, cut, []);
        if (r.tainted) r.tainted.refs.forEach((x) => refs.add(x));
      }
      if (refs.size) cells[`${v}.${op}`] = [...refs].sort();
    }
  }
  return { cut_coefficients: [...cut.coefs].sort(), cells_on_cut: cells };
}

test('test_python_and_js_cut_classifiers_agree_on_the_live_contract', () => {
  assert.deepEqual(javascript(readJson(CM, 'model-contract.json')), python(CM));
});

// The live contract cuts nothing, so the live comparison alone would pass for any walk. Seed two cuts: a table a cell
// reads directly and a coefficient only registry derivations read.
test('test_python_and_js_cut_classifiers_agree_on_seeded_cuts', () => {
  const dir = fs.mkdtempSync(path.join(ROOT, 'tmp', 'cut-parity-'));
  try {
    const contract = readJson(CM, 'model-contract.json');
    const coverage = readJson(CM, 'coverage.json');
    const registry = readJson(CM, 'inputs-registry.json');
    // A coefficient that only registry derivations read, picked from the contract so a registry change that turns
    // one of them into a formula does not silently empty the seed.
    const direct = new Set();
    const viaRules = new Set();
    const refs = (node, into) => JSON.stringify(node, (k, v) => {
      if (v && v.kind === 'coef') into.add(v.id);
      if (v && v.kind === 'lookup') into.add(v.table_id);
      return v;
    });
    for (const ops of Object.values(contract.formulas)) for (const cell of Object.values(ops)) refs(cell.ledger_rows, direct);
    // Both sides must read it through a derivation: the page from the contract's registry, closure_check from the file.
    const viaFile = new Set();
    for (const q of contract.registry) if (q.kind === 'derivation' && q.rule) refs(q.rule, viaRules);
    for (const q of registry.quantities) if (q.kind === 'derivation' && q.rule) refs(q.rule, viaFile);
    const withEvidence = new Set(coverage.evidence.map((r) => r.coefficient));
    const derivationOnly = contract.coefficients.map((c) => c.id)
      .filter((id) => viaRules.has(id) && viaFile.has(id) && !direct.has(id) && withEvidence.has(id)).sort();
    assert.ok(derivationOnly.length, 'some coefficient is read only through a registry derivation');
    const seeds = ['acquire_bytes', derivationOnly[0]];
    for (const id of seeds) {
      const c = contract.coefficients.find((x) => x.id === id);
      assert.ok(c, `${id} is in the contract`);
      c.status = 'unsupported';
      const rows = coverage.evidence.filter((r) => r.coefficient === id);
      assert.ok(rows.length, `${id} has coverage evidence`);
      for (const r of rows) r.status = 'cut';
    }
    fs.writeFileSync(path.join(dir, 'model-contract.json'), JSON.stringify(contract));
    fs.writeFileSync(path.join(dir, 'coverage.json'), JSON.stringify(coverage));
    fs.writeFileSync(path.join(dir, 'inputs-registry.json'), JSON.stringify(registry));
    const py = python(dir);
    assert.deepEqual(py.cut_coefficients, seeds.slice().sort());
    // The derivation-only seed must reach a cell, or the seed does not test the derivation walk.
    assert.ok(Object.values(py.cells_on_cut).some((ids) => ids.includes(seeds[1])), `${seeds[1]}: ${JSON.stringify(py.cells_on_cut)}`);
    assert.deepEqual(javascript(contract), py);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
