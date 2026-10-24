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

test('test_column_badge_counts_extrapolated_cells_and_assumed_money', () => {
  const vmod = require('../viewmodel.js');
  const build = (c, raw) => {
    const r0 = pipeline.resolveInputs(c, raw, [], {});
    const aws = presets.resolvePrice(c, 'aws-us-east-1');
    return vmod.build(pipeline.run(c, r0.inputs, aws, {}), c, r0.fields, r0.lockOrder, { pricePreset: aws });
  };
  const card = (vm, v) => vm.cards.find((x) => x.variant === v);
  const out = build(load(), { inserts_per_month: 60000 });
  assert.equal(card(out, 's3').extrapolated, true);
  assert.match(card(out, 's3').extrapolated_text, /^extrapolated: \d+ cells?$/);
  // An assumed coefficient that prices money marks its cell and column; a fitted one inside its range does not.
  const plain = build(load(), { inserts_per_month: 1000 });
  assert.equal(card(plain, 'cas').extrapolated, false);
  const assumed = build(load((x) => { x.coefficients.find((k) => k.id === 'puts_per_insert_cas').basis = 'assumed'; }), { inserts_per_month: 1000 });
  assert.equal(card(assumed, 'cas').extrapolated, true);
  assert.equal(card(assumed, 'cas').extrapolated_text, 'extrapolated: 1 cell');
  assert.equal(assumed.table.rows.find((r) => r.operation === 'inserts').cells.find((c) => c.variant === 'cas').extrapolated, true);
});

test('test_derived_formula_outside_its_range_gets_an_information_note', () => {
  const c = load((x) => { coef(x).basis = 'derived'; });
  const s3 = s3At(c, 60000);
  assert.equal(s3.extrapolated, false);
  assert.ok(s3.notes.some((n) => n.text === 'puts_per_insert_s3: formula from source; verified at inserts_per_month 0 to 50000'), JSON.stringify(s3.notes));
  assert.equal(s3At(c, 1000).notes.some((n) => /formula from source/.test(n.text)), false);
});
