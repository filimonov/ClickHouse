'use strict';
// CAS prices mutations from the workload's own mutation inputs, not from a second set of CAS-only inputs.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const AWS = presets.resolvePrice(LIVE, 'aws-us-east-1');
const DERIVED = ['cas_mutations_per_s', 'cas_parts_per_mutation', 'cas_mutated_columns'];
const evalWith = (sc, values) => {
  const s = presets.SCENARIOS[sc];
  const r = pipeline.resolveInputs(LIVE, Object.assign({}, s.values, values || {}), s.locks, {});
  assert.deepEqual(r.problems, []);
  return { ev: pipeline.evaluationInputs(LIVE, r.inputs), res: pipeline.run(LIVE, r.inputs, AWS, {}) };
};
const close = (a, b, msg) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)), `${msg}: ${a} vs ${b}`);

test('test_cas_mutation_quantities_are_hidden_derivations', () => {
  for (const id of DERIVED) {
    const q = LIVE.registry.find((x) => x.id === id);
    assert.equal(q.kind, 'derivation', id);
    assert.equal(q.basis, 'derived', id);
    assert.equal(q.ui.tier, 'hidden', id);
  }
  for (const s of Object.values(presets.SCENARIOS)) for (const id of DERIVED) assert.equal(id in s.values, false, id);
});

test('test_derivations_reproduce_the_old_analytics_values', () => {
  // Analytics batch: 30 ALTER UPDATE a month over 300 wide parts, one column each.
  const { ev, res } = evalWith('analytics_batch');
  close(ev.get('cas_mutations_per_s'), 30 / 2628000, 'mutations per second');
  close(ev.get('cas_parts_per_mutation'), 10, 'parts per mutation');
  assert.equal(ev.get('cas_mutated_columns'), 1);
  // The CAS Mutations cell is unchanged in dollars (0.021288 before the derivations).
  close(res.variants.cas.cells.mutations.money, 0.021288, 'CAS mutations $');
});

test('test_workload_mutation_inputs_move_the_cas_cell', () => {
  const base = evalWith('analytics_batch').res.variants.cas.cells.mutations.money;
  const twice = evalWith('analytics_batch', { mutations_per_month: 60, mutated_wide_parts_per_month: 600 }).res.variants.cas.cells.mutations.money;
  close(twice, 2 * base, 'twice the mutations, twice the cost');
  // No mutations: zero parts per mutation and a $0 cell, not an unpriced one.
  const none = evalWith('analytics_batch', { mutations_per_month: 0, mutated_wide_parts_per_month: 0 });
  assert.equal(none.ev.get('cas_parts_per_mutation'), 0);
  assert.equal(none.res.variants.cas.state, 'ok');
  assert.equal(none.res.variants.cas.cells.mutations.money, 0);
  // ALTER DELETE rewrites every column of the part; UPDATE of one column and lightweight DELETE rewrite one.
  assert.equal(evalWith('analytics_batch', { mutation_kind: 'delete' }).ev.get('cas_mutated_columns'), 40);
  assert.equal(evalWith('analytics_batch', { mutation_kind: 'lightweight_delete' }).ev.get('cas_mutated_columns'), 1);
});
