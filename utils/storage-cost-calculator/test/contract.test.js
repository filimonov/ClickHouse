'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');

const CM = path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model');
const read = (...p) => JSON.parse(fs.readFileSync(path.join(CM, ...p), 'utf8'));
const registry = read('schemas', 'examples', 'registry.json');
const formulas = read('schemas', 'examples', 'formulas.json');
const billing = read('billing-contract.json');

const coef = (id, kind, extra) => Object.assign({
  id, kind, dim: 'count', value: 1, validity: null, tolerance: null,
  evidence: { ref: `report-${id}` }, status: 'ok',
}, extra || {});

function minimal() {
  const merged = contract.mergeFormulas([formulas]);
  return {
    schema: 'contract/1',
    version: '0.0.1',
    manifest: {
      code_sha256: 'x', contract_version: '1', registry_version: '1', coverage_version: '1', billing_version: '1',
      evidence: [{ report: 'r', commit: 'c', raw_sha256sums: 's' }],
    },
    evidence_statement: 'test',
    registry: registry.quantities,
    sets: registry.sets,
    precedence: registry.precedence,
    coefficients: [
      coef('min_bytes_for_wide_part', 'scalar', { dim: 'bytes', value: 10485760 }),
      coef('objects_per_part', 'table', {
        value: { index: ['shape', 'part_type'], values: { 'narrow|compact': 3 } },
      }),
    ],
    billing,
    variants: ['s3'],
    operations: ['insert'],
    applicability: { s3: { insert: { write_path: 'supported' } } },
    formulas: merged.formulas,
    states: merged.states,
  };
}

function problemsOf(c) {
  try {
    contract.load(c);
  } catch (e) {
    assert.ok(Array.isArray(e.problems));
    assert.equal(e.message, e.problems.join('\n'));
    return e.problems;
  }
  return [];
}

test('test_load_valid_minimal', () => {
  const c = contract.load(minimal());
  assert.deepEqual(c.tables.objects_per_part.index, ['shape', 'part_type']);
  assert.equal(c.tables.objects_per_part.values['narrow|compact'], 3);
  assert.equal(c.tables.objects_per_part.dim, 'count');
  assert.ok(Object.isFrozen(c));
  assert.equal(contract.CONTRACT_SCHEMA, 'contract/1');
});

test('test_duplicate_owner_rejected', () => {
  const c = minimal();
  c.coefficients.push(coef('min_bytes_for_wide_part', 'scalar', { evidence: { ref: 'report-other' } }));
  const p = problemsOf(c);
  assert.equal(p.length, 1);
  assert.match(p[0], /duplicate coefficient id min_bytes_for_wide_part/);
  assert.match(p[0], /report-min_bytes_for_wide_part/);
  assert.match(p[0], /report-other/);
});

test('test_unregistered_input_rejected', () => {
  const c = minimal();
  const f = JSON.parse(JSON.stringify(c.formulas));
  f.s3.insert.ledger_rows[0].expr.a.id = 'no_such_input';
  c.formulas = f;
  const p = problemsOf(c);
  assert.equal(p.length, 1);
  assert.match(p[0], /^formulas\.s3\.insert\.ledger_rows\[0\]\.expr\/a: .*unknown input id no_such_input/);
});

test('test_unknown_coef_and_table_rejected', () => {
  const c = minimal();
  const f = JSON.parse(JSON.stringify(c.formulas));
  f.s3.insert.ledger_rows[0].expr.b.table_id = 'missing_table';
  c.formulas = f;
  const p = problemsOf(c);
  assert.ok(p.some((l) => /formulas\.s3\.insert\.ledger_rows\[0\]\.expr\/b: .*missing_table/.test(l)), p.join('\n'));
});

test('test_merge_rejects_two_owners_of_cell', () => {
  const a = JSON.parse(JSON.stringify(formulas));
  a.owner = 'owner-a';
  const b = JSON.parse(JSON.stringify(formulas));
  b.owner = 'owner-b';
  b.cells = { insert: b.cells.insert, merge: JSON.parse(JSON.stringify(b.cells.insert)) };
  const m = contract.mergeFormulas([a, b]);
  assert.equal(m.problems.length, 1);
  assert.match(m.problems[0], /s3\.insert/);
  assert.match(m.problems[0], /owner-a/);
  assert.match(m.problems[0], /owner-b/);
  assert.ok(m.formulas.s3.merge, 'other cells still merge');
  assert.deepEqual(m.states.s3.map((s) => s.operation).sort(), ['insert', 'merge']);
  assert.equal(m.states.s3[0].id, 'not_supported');
});

test('test_merge_rejects_malformed_files', () => {
  const m = contract.mergeFormulas([{ schema: 'formulas/2', owner: 'o', variant: 'v', cells: {} },
    { schema: 'formulas/1', owner: 'o', cells: {} }, { schema: 'formulas/1', variant: 'v', cells: {} }]);
  assert.equal(m.problems.length, 3);
});

test('test_applicability_domain_complete', () => {
  const c = minimal();
  c.operations = ['insert', 'merge'];
  c.families = undefined;
  c.applicability = { s3: { insert: { write_path: 'supported' } } };
  const p = problemsOf(c);
  assert.equal(p.length, 1);
  assert.match(p[0], /applicability\.s3\.merge\.write_path/);

  const ok = minimal();
  ok.operations = ['insert', 'merge'];
  ok.applicability.s3.merge = { write_path: 'unsupported_cut' };
  assert.doesNotThrow(() => contract.load(ok));
});

test('test_formula_cell_without_applicability_rejected', () => {
  const c = minimal();
  c.applicability = { s3: {} };
  const p = problemsOf(c);
  assert.ok(p.some((l) => /applicability\.s3\.insert/.test(l)), p.join('\n'));
});

test('test_load_lists_every_problem_at_once', () => {
  const c = minimal();
  c.coefficients.push(coef('min_bytes_for_wide_part', 'scalar'));
  c.billing = Object.assign({}, billing, { schema: 'billing/0' });
  delete c.manifest.registry_version;
  const p = problemsOf(c);
  assert.equal(p.length, 3, p.join('\n'));
  assert.ok(p.some((l) => /duplicate coefficient/.test(l)));
  assert.ok(p.some((l) => /^billing\.schema/.test(l)));
  assert.ok(p.some((l) => /^manifest\.registry_version/.test(l)));
});

test('test_wrong_schema_and_missing_keys', () => {
  assert.match(problemsOf(Object.assign(minimal(), { schema: 'contract/2' }))[0], /^schema/);
  const c = minimal();
  delete c.sets;
  assert.ok(problemsOf(c).some((l) => /^sets/.test(l)));
});
