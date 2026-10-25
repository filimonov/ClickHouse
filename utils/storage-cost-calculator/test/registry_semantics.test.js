'use strict';
// Registry semantics the page applies at run time: nearest-key lookups only on enums that declare `ui.nearest`,
// `presets` of an enum input, formulas from source outside their verified range, fractions, the same-AZ note.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');
const vmod = require('../viewmodel.js');

const LIVE_JSON = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
const LIVE = contract.load(LIVE_JSON);
const AWS = presets.resolvePrice(LIVE, 'aws-us-east-1');
const CUSTOM = presets.SCENARIOS.custom;

function runWith(c, values, scenario) {
  const s = scenario || CUSTOM;
  const r = pipeline.resolveInputs(c, Object.assign({}, s.values, values), s.locks, {});
  return { r, res: pipeline.run(c, r.inputs, presets.resolvePrice(c, 'aws-us-east-1'), {}) };
}
const copyWith = (mutate) => {
  const j = JSON.parse(JSON.stringify(LIVE_JSON));
  mutate(j);
  return contract.load(j);
};

// ---- N1: nearest-key substitution -----------------------------------------------------------------

test('test_cas_cross_az_transfer_prices_relink_bytes_per_part', () => {
  const t = LIVE.coefficients.find((c) => c.id === 'acquire_bytes');
  assert.ok(!Object.prototype.hasOwnProperty.call(t.value.values, 'cas|insert'), 'the premise: no cas|insert ratio');
  const relink = LIVE.coefficients.find((c) => c.id === 'cas_relink_bytes_per_part');
  assert.equal(relink.basis, 'assumed');
  const values = { topology: 'cross_az' };
  const { r, res } = runWith(LIVE, values);
  assert.deepEqual(r.problems, []);
  const cas = res.variants.cas;
  assert.equal(cas.state, 'ok', cas.reason);
  // The manifest per fetched part, never s3plain's full-part bytes.
  assert.ok(!cas.notes.some((n) => /measured for s3plain/.test(n.text)), JSON.stringify(cas.notes));
  const row = cas.cells.inserts.rows.find((x) => x.row_type === 'transfer');
  assert.ok(row.quantity > 0, String(row.quantity));
  assert.ok(cas.cells.inserts.notes.some((n) => /^cas_relink_bytes_per_part: ASSUMPTION .*insert-1k.*Tier 2/.test(n.text)), JSON.stringify(cas.cells.inserts.notes));
  // In one zone the row is zero, so the assumption prices nothing and is not noted.
  const same = runWith(LIVE, { topology: 'same_az' }).res.variants.cas.cells.inserts.notes;
  assert.ok(!same.some((n) => /ASSUMPTION/.test(n.text)), JSON.stringify(same));
  // The row is linear in the relink bytes: doubling the coefficient doubles the transferred GiB.
  const doubled = copyWith((j) => { j.coefficients.find((c) => c.id === 'cas_relink_bytes_per_part').value *= 2; });
  const row2 = runWith(doubled, values).res.variants.cas.cells.inserts.rows.find((x) => x.row_type === 'transfer');
  assert.ok(Math.abs(row2.quantity - 2 * row.quantity) <= 1e-9 * row2.quantity, `${row2.quantity} != 2 x ${row.quantity}`);
  for (const v of ['ebs', 's3', 's3plain']) assert.equal(res.variants[v].state, 'ok', v);
});

// The live registry substitutes no variant; drop s3's measured key and opt variant_id in to see the mechanism.
const withoutS3Insert = (j) => { delete j.coefficients.find((c) => c.id === 'acquire_bytes').value.values['s3|insert']; };

test('test_nearest_applies_only_to_enums_that_declare_it', () => {
  const off = copyWith(withoutS3Insert);
  assert.equal(runWith(off, { topology: 'cross_az' }).res.variants.s3.state, 'unavailable');
  const c = copyWith((j) => { withoutS3Insert(j); j.registry.find((q) => q.id === 'variant_id').ui = { nearest: true }; });
  const s3 = runWith(c, { topology: 'cross_az' }).res.variants.s3;
  assert.equal(s3.state, 'ok');
  assert.ok(s3.notes.some((n) => /acquire_bytes: measured for (ebs|s3plain)\|insert; applied to s3\|insert/.test(n.text)), JSON.stringify(s3.notes));
});

test('test_coverage_requires_reached_substitutions_to_be_declared', () => {
  const c = copyWith((j) => { withoutS3Insert(j); j.registry.find((q) => q.id === 'variant_id').ui = { nearest: true }; });
  const scenario = { values: Object.assign({}, CUSTOM.values, { topology: 'cross_az' }), locks: [] };
  const rep = presets.coverageReport(c, scenario);
  assert.ok(rep.problems.some((p) => /s3\.inserts: acquire_bytes measured for (ebs|s3plain)\|insert, applied to s3\|insert: nearest-key substitution not declared/.test(p)), rep.problems.join('\n'));
  const declared = presets.coverageReport(c, Object.assign({}, scenario, { expected_extrapolated: { s3: ['acquire_bytes'] } }));
  assert.ok(!declared.problems.some((p) => /substitution/.test(p)), declared.problems.join('\n'));
  assert.ok(declared.declared.some((d) => /acquire_bytes measured for (ebs|s3plain)\|insert.*\(declared\)/.test(d)));
});

test('test_contract_rejects_a_malformed_nearest_or_preset', () => {
  const bad = (mutate) => assert.throws(() => copyWith(mutate), /registry/);
  bad((j) => { j.registry.find((q) => q.id === 'columns').ui.nearest = true; });
  bad((j) => { j.registry.find((q) => q.id === 'shape').ui.nearest = ['narrow']; });
  bad((j) => { j.registry.find((q) => q.id === 'shape').ui.nearest = { narrow: 'huge' }; });
  bad((j) => { delete j.registry.find((q) => q.id === 'shape').presets.types; });
  bad((j) => { j.registry.find((q) => q.id === 'shape').presets.narrow.no_such_input = 1; });
  copyWith((j) => { j.registry.find((q) => q.id === 'shape').ui.nearest = ['wide', 'narrow', 'types']; });
});

// ---- N2: registry presets -------------------------------------------------------------------------

test('test_metrics_evaluates_five_columns', () => {
  const s = presets.SCENARIOS.metrics;
  const r = pipeline.resolveInputs(LIVE, s.values, s.locks, {});
  assert.equal(r.inputs.get('columns'), 5);
  assert.equal(pipeline.evaluationInputs(LIVE, r.inputs).get('columns'), 5);
});

test('test_every_scenario_sets_columns_matching_its_shape', () => {
  const shape = LIVE.registry.find((q) => q.id === 'shape');
  for (const [id, s] of Object.entries(presets.SCENARIOS)) {
    assert.equal(typeof s.values.columns, 'number', `${id} sets columns`);
    if (s.values.shape) assert.equal(s.values.columns, shape.presets[s.values.shape].columns, id);
  }
});

test('test_shape_preset_fills_quantities_the_caller_did_not_set', () => {
  const narrow = pipeline.resolveInputs(LIVE, { shape: 'narrow' }, [], {});
  assert.deepEqual(narrow.problems, []);
  for (const [id, v] of Object.entries(LIVE.registry.find((q) => q.id === 'shape').presets.narrow)) {
    assert.equal(narrow.inputs.get(id), v, id);
    assert.equal(narrow.fields[id].preset_from, 'shape = narrow', id);
  }
  // A raw value or an override wins over the preset; an invalid raw value is not replaced by the preset's.
  assert.equal(pipeline.resolveInputs(LIVE, { shape: 'narrow', columns: 12 }, [], {}).inputs.get('columns'), 12);
  assert.equal(pipeline.resolveInputs(LIVE, { shape: 'narrow' }, [], { columns: 17 }).inputs.get('columns'), 17);
  const invalid = pipeline.resolveInputs(LIVE, { shape: 'narrow', columns: -3 }, [], {});
  assert.equal(invalid.inputs.has('columns'), false);
  assert.ok(invalid.problems.some((p) => /^columns:/.test(p)));
});

test('test_bad_preset_value_is_an_input_problem', () => {
  const c = copyWith((j) => { j.registry.find((q) => q.id === 'shape').presets.narrow.columns = -5; });
  const r = pipeline.resolveInputs(c, { shape: 'narrow' }, [], {});
  assert.ok(r.problems.some((p) => /shape preset narrow: columns: expected a whole number of 1 or more, got -5/.test(p)), r.problems.join('\n'));
  assert.equal(r.inputs.has('columns'), false);
});

// ---- N3: formulas from source outside their verified range ---------------------------------------

test('test_columns_outside_the_verified_range_is_a_note_not_an_extrapolation', () => {
  assert.ok(pipeline.derivedOnlyInputs(LIVE).has('columns'));
  // One CAS table keeps the start requests inside their measured range: columns is the only parameter out of range.
  const base = runWith(LIVE, { columns: 40, cas_tables: 1 }).res;
  const { r, res } = runWith(LIVE, { columns: 100, cas_tables: 1 });
  assert.equal(r.fields.columns.out_of_range, false);
  assert.equal(r.fields.columns.verified_only, true);
  const s3 = res.variants.s3;
  assert.equal(s3.state, 'ok');
  assert.equal(s3.extrapolated, base.variants.s3.extrapolated);
  assert.ok(s3.total > base.variants.s3.total, 'more columns, more objects per part');
  assert.ok(s3.notes.some((n) => /formula from source; verified at columns 5 to 40/.test(n.text)), JSON.stringify(s3.notes.map((n) => n.text)));
  for (const c of Object.values(s3.cells)) assert.equal(c.extrapolated, base.variants.s3.cells[Object.keys(s3.cells).find((k) => s3.cells[k] === c)].extrapolated);
  const vm = vmod.build(res, LIVE, r.fields, r.lockOrder, { pricePreset: AWS });
  assert.deepEqual(vm.notes.filter((n) => n.kind === 'out_of_range').map((n) => n.param), []);
  const inserts = vm.table.rows.find((x) => x.operation === 'inserts').cells.find((x) => x.variant === 's3');
  assert.equal(inserts.extrapolated, false);
  assert.ok(inserts.notes.some((n) => /formula from source; verified at columns 5 to 40/.test(n)));
  // Inside the range there is no note.
  assert.ok(!base.variants.s3.notes.some((n) => /formula from source/.test(n.text)));
});

// ---- N6: same-AZ note ----------------------------------------------------------------------------

test('test_same_az_note_is_text_on_the_transfer_cell_and_column', () => {
  const { r, res } = runWith(LIVE, { topology: 'same_az' });
  const vm = vmod.build(res, LIVE, r.fields, r.lockOrder, { pricePreset: AWS });
  const row = vm.table.rows.find((x) => x.operation === 'inserts');
  for (const v of LIVE.variants) {
    assert.ok(row.cells.find((x) => x.variant === v).notes.includes(pipeline.SAME_AZ_NOTE), v);
    assert.ok(vm.table.column_notes[v].some((n) => n.kind === 'text' && n.text === pipeline.SAME_AZ_NOTE), v);
  }
  const cross = runWith(LIVE, { topology: 'cross_az' });
  const vmc = vmod.build(cross.res, LIVE, cross.r.fields, cross.r.lockOrder, { pricePreset: AWS });
  assert.ok(!JSON.stringify(vmc.table).includes(pipeline.SAME_AZ_NOTE));
  // One replica sends nothing between replicas: no note.
  const one = runWith(LIVE, { topology: 'same_az', replicas: 1 });
  assert.ok(!Object.values(one.res.variants).some((x) => x.notes.some((n) => n.text === pipeline.SAME_AZ_NOTE)));
});

// ---- N10: fractions --------------------------------------------------------------------------------

test('test_fractions_outside_zero_to_one_are_input_problems', () => {
  const fractions = LIVE.registry.filter((q) => q.kind === 'input' && /_fraction$/.test(q.id)).map((q) => q.id);
  assert.ok(fractions.length >= 3, fractions.join());
  for (const id of fractions) {
    for (const bad of [-0.1, 1.5, 2]) {
      const r = pipeline.resolveInputs(LIVE, { [id]: bad }, [], {});
      assert.ok(r.problems.some((p) => p.startsWith(`${id}:`)), `${id} = ${bad}`);
      assert.equal(r.inputs.has(id), false);
    }
    for (const ok of [0, 0.5, 1]) assert.deepEqual(pipeline.resolveInputs(LIVE, { [id]: ok }, [], {}).problems, [], `${id} = ${ok}`);
  }
  assert.match(pipeline.resolveInputs(LIVE, { cas_hash_hit_fraction: 2 }, [], {}).problems[0], /expected a fraction from 0 to 1, got 2/);
});

// Table counts: a table has at least one column, and every column at least one stream. Streams per column is an
// average over columns (the `types` preset is 3.19), so it need not be whole.
test('test_columns_and_streams_per_column_are_at_least_one', () => {
  const problem = (values) => pipeline.resolveInputs(LIVE, values, [], {}).problems;
  for (const bad of [0, -1, 2.5]) assert.ok(problem({ columns: bad }).some((p) => p.startsWith('columns:')), `columns = ${bad}`);
  for (const bad of [0, 0.5, -1]) assert.ok(problem({ streams_per_column: bad }).some((p) => p.startsWith('streams_per_column:')), `streams_per_column = ${bad}`);
  assert.deepEqual(problem({ columns: 1, streams_per_column: 1 }), []);
  assert.deepEqual(problem({ columns: 21, streams_per_column: 3.19047619047619 }), []);
  assert.deepEqual(problem({ shape: 'types' }), []);
});
