'use strict';
// The cut rule exists twice: closure_check.py classifies coefficients from coverage (experiments that are cut), the
// page reads `status: unsupported` from the contract. Both must name the same coefficients and the same cells.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..', '..', '..');
const CM = path.join(ROOT, 'docs', 'superpowers', 'cost-model');
const python = () => JSON.parse(execFileSync('python3', [path.join(__dirname, '..', 'tools', 'closure_check.py'), '--dump-cuts', '--cost-model', CM], { encoding: 'utf8' }));

// The page side: coefficients the contract marks unsupported (directly or through their registry row), and per cell
// the ones it reads, following registry derivations like the Python walk.
function javascript(c) {
  const cut = new Set([...c.coefficients.filter((k) => k.status === 'unsupported').map((k) => k.id),
    ...c.registry.filter((q) => q.status === 'unsupported' && q.coef).map((q) => q.coef)]);
  const rules = new Map(c.registry.filter((q) => q.kind === 'derivation' && q.rule).map((q) => [q.id, q.rule]));
  const refs = (node) => {
    const out = new Set();
    const seen = new Set();
    const todo = [node];
    while (todo.length) {
      const visit = (n) => {
        if (!n || typeof n !== 'object') return;
        if (Array.isArray(n)) { n.forEach(visit); return; }
        if (n.kind === 'coef') out.add(n.id);
        if (n.kind === 'lookup') out.add(n.table_id);
        if (n.kind === 'input' && rules.has(n.id) && !seen.has(n.id)) { seen.add(n.id); todo.push(rules.get(n.id)); }
        Object.values(n).forEach(visit);
      };
      visit(todo.pop());
    }
    return out;
  };
  const cells = {};
  for (const v of Object.keys(c.formulas).sort()) {
    for (const op of Object.keys(c.formulas[v]).sort()) {
      const hit = [...refs(c.formulas[v][op].ledger_rows)].filter((id) => cut.has(id)).sort();
      if (hit.length) cells[`${v}.${op}`] = hit;
    }
  }
  return { cut_coefficients: [...cut].sort(), cells_on_cut: cells };
}

test('test_python_and_js_cut_classifiers_agree_on_the_live_contract', () => {
  const contract = JSON.parse(fs.readFileSync(path.join(CM, 'model-contract.json'), 'utf8'));
  assert.deepEqual(javascript(contract), python());
});
