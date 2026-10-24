'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');
const viewmodel = require('../viewmodel.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const MINIMAL = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8')));
const copy = (x) => JSON.parse(JSON.stringify(x));

test('test_every_scenario_within_coverage', () => {
  assert.deepEqual(Object.keys(presets.SCENARIOS), ['logs', 'metrics', 'analytics_batch', 'single_replica_archive', 'custom']);
  // The check is not vacuous: the live contract carries validity domains for the scenarios to fall into.
  const domains = presets.validityDomains(LIVE);
  assert.ok(domains.some((d) => d.owner.startsWith('coefficient ')), 'live contract has coefficient validity');
  for (const [id, s] of Object.entries(presets.SCENARIOS)) {
    assert.equal(typeof s.label, 'string', id);
    assert.equal(typeof s.description, 'string', id);
    assert.deepEqual(presets.coverageProblems(LIVE, s), [], id);
  }
  // A registry id that does not exist, a value outside a coefficient's validity, a value for a derived member and a
  // lock outside a precedence group are each reported.
  const bad = (mut) => { const s = copy(presets.SCENARIOS.logs); mut(s); return presets.coverageProblems(LIVE, s).join('\n'); };
  assert.match(bad((s) => { s.values.no_such_input = 1; }), /no_such_input is not a registry/);
  assert.match(bad((s) => { s.values.insert_rows = 1000000; }), /coefficient merge_effective_fan_in: insert_rows = 1000000 is outside \[1000, 100000\]/);
  assert.match(bad((s) => { s.values.data_bytes = 1; }), /value data_bytes is set but derived/);
  assert.match(bad((s) => { s.locks = ['no_such_lock', 'retention_days']; }), /no_such_lock/);
});

test('test_structure_only_prices_set_prices_missing', () => {
  // Preset ids are the billing contract's; `custom` is the page's editable copy.
  assert.deepEqual(Object.keys(presets.PRICES), [...LIVE.billing.presets.map((p) => p.id), 'custom']);
  const structureOnly = Object.entries(presets.PRICES).filter(([, d]) => d.structure_only).map(([id]) => id);
  assert.deepEqual(structureOnly, ['gcs', 'azure']);
  for (const id of structureOnly) {
    assert.equal(presets.resolvePrice(LIVE, id).structure_only, true, id);
    assert.equal(presets.priceLabel(LIVE, id), `${presets.PRICES[id].label} (values not provided)`);
    const preset = presets.resolvePrice(MINIMAL, id);
    const r0 = pipeline.resolveInputs(MINIMAL, {}, [], {});
    const res = pipeline.run(MINIMAL, r0.inputs, preset, {});
    assert.equal(res.page_state, 'prices_missing', id);
    const vm = viewmodel.build(res, MINIMAL, r0.fields, r0.lockOrder, { pricePreset: preset, openCell: { variant: 's3', operation: 'storage' } });
    // Money is suppressed everywhere; the structure (quantities with units) is shown.
    assert.equal(JSON.stringify(vm).includes('$'), false, `${id}: no money text`);
    for (const card of vm.cards) {
      assert.equal(card.total_text, null);
      assert.deepEqual(card.delta_texts, []);
      assert.deepEqual(card.bar_segments, []);
    }
    const s3 = vm.table.rows.find((r) => r.operation === 'storage').cells.find((c) => c.variant === 's3');
    assert.match(s3.text, /stored: 1,000 GiB/);
    assert.equal(vm.chart, null);
    assert.deepEqual(vm.drill.stages[3].lines, []);
    assert.match(vm.drill.stages[3].note, /provides no values/);
    assert.equal(vm.inputs.blocks.find((b) => b.block === 'prices').note, 'values not provided');
  }
  assert.match(presets.priceLabel(LIVE, 'aws-us-east-1'), /^AWS us-east-1 \(\d{4}-\d{2}-\d{2}\)$/);
  // The custom preset is an editable copy; the contract's presets are not editable and unknown ids throw.
  const aws = presets.resolvePrice(MINIMAL, 'aws-us-east-1');
  const custom = presets.resolvePrice(MINIMAL, 'custom', { 'storage.tiers.0.price_per_gb_month': 0.046 });
  assert.equal(custom.id, 'custom');
  assert.equal(custom.storage.tiers[0].price_per_gb_month, 0.046);
  assert.equal(aws.storage.tiers[0].price_per_gb_month, 0.023, 'base preset untouched');
  const inputs = pipeline.resolveInputs(MINIMAL, {}, [], {}).inputs;
  const a = pipeline.run(MINIMAL, inputs, aws, {}).variants.cas.total;
  const b = pipeline.run(MINIMAL, inputs, custom, {}).variants.cas.total;
  assert.ok(Math.abs(b - a - 23) < 1e-9, `${b} - ${a}`);
  assert.throws(() => presets.resolvePrice(MINIMAL, 'custom', { 'storage.tiers.9.price_per_gb_month': 1 }), /no such price field/);
  assert.throws(() => presets.resolvePrice(MINIMAL, 'custom', { 'block.gp3.size_gib': 1 }), /no such price field/);
  assert.throws(() => presets.resolvePrice(MINIMAL, 'aws-us-east-1', { 'block.gp3.iops_price': 1 }), /not editable/);
  assert.throws(() => presets.resolvePrice(MINIMAL, 'aws_us_east_1'), /unknown price preset/);
});
