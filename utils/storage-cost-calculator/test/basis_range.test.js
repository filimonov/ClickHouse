'use strict';
// Coefficient ranges and bases: `verified_range` (or the older `validity`), and `basis` deciding whether a value
// outside the range is extrapolated.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const model = require('../model.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');

const MINIMAL = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8'));
const load = (mutate) => { const c = JSON.parse(JSON.stringify(MINIMAL)); if (mutate) mutate(c); return contract.load(c); };
const s3At = (c, n) => {
  const r0 = pipeline.resolveInputs(c, { inserts_per_month: n }, [], {});
  return pipeline.run(c, r0.inputs, presets.resolvePrice(c, 'aws-us-east-1'), {}).variants.s3;
};
const coef = (x) => x.coefficients.find((k) => k.id === 'puts_per_insert_s3');

test('test_range_and_basis_readers', () => {
  assert.deepEqual(model.rangeOf({ verified_range: { a: [1, 2] }, validity: { a: [0, 9] } }), { a: [1, 2] });
  assert.deepEqual(model.rangeOf({ validity: { a: [0, 9] } }), { a: [0, 9] });
  assert.equal(model.rangeOf({}), null);
  assert.equal(model.basisOf({}), 'fitted');
  assert.equal(model.basisOf({ basis: 'derived' }), 'derived');
  assert.throws(() => model.basisOf({ id: 'k', basis: 'guessed' }), /k: unknown basis "guessed"/);
});

test('test_fitted_outside_range_extrapolates_derived_does_not', () => {
  // The old field name still works; the new one wins.
  assert.equal(s3At(load(), 60000).extrapolated, true);
  assert.equal(s3At(load((x) => { coef(x).verified_range = coef(x).validity; delete coef(x).validity; }), 60000).extrapolated, true);
  assert.equal(s3At(load((x) => { coef(x).basis = 'assumed'; }), 60000).extrapolated, true);
  const derived = s3At(load((x) => { coef(x).basis = 'derived'; }), 60000);
  assert.equal(derived.state, 'ok');
  assert.equal(derived.extrapolated, false);
  assert.equal(s3At(load(), 1000).extrapolated, false);
});
