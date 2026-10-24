'use strict';
// A billing preset whose price is missing, null or not a number never bills $0 in state ok.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const billing = require('../billing.js');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');

const LIVE_JSON = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
const aws = () => JSON.parse(JSON.stringify(LIVE_JSON.billing.presets.find((p) => p.id === 'aws-us-east-1')));

test('test_price_problems_for_null_missing_nan_and_tiers', () => {
  assert.deepEqual(billing.priceProblems(aws()), []);
  const cases = [
    [(p) => { p.request_classes[0].price_per_1000 = null; }, /request_classes\[0\] \(\w+\)\.price_per_1000: expected a finite number >= 0, got null/],
    [(p) => { delete p.request_classes[0].price_per_1000; }, /price_per_1000: expected a finite number >= 0, got null/],
    [(p) => { p.request_classes[0].price_per_1000 = NaN; }, /price_per_1000: expected a finite number >= 0, got null/],
    [(p) => { p.request_classes[0].price_per_1000 = -1; }, /got -1/],
    [(p) => { p.storage.tiers[0].price_per_gb_month = 'x'; }, /storage\.tiers\[0\]\.price_per_gb_month/],
    [(p) => { p.storage.tiers[p.storage.tiers.length - 1].up_to_gb = 1e12; }, /the last tier must be open \(null\)/],
    [(p) => { p.storage.tiers.reverse(); }, /up_to_gb/],
    [(p) => { p.block.gp3.iops_price = null; }, /block\.gp3\.iops_price/],
    [(p) => { const t = p.transfer.find((x) => x.id !== 'internet'); t.price_per_gb = null; }, /transfer\[\d\] \(\w+\)\.price_per_gb/],
  ];
  for (const [mutate, re] of cases) {
    const p = aws();
    mutate(p);
    assert.match(billing.priceProblems(p).join('\n'), re);
  }
  // The internet route may be unpriced; a structure-only preset has no prices to check.
  const p = aws();
  const net = p.transfer.find((x) => x.id === 'internet');
  if (net) { net.price_per_gb = null; assert.deepEqual(billing.priceProblems(p), []); }
  assert.deepEqual(billing.priceProblems({ id: 'gcs', structure_only: true }), []);
});

test('test_contract_load_rejects_a_shipped_preset_with_a_null_price', () => {
  const c = JSON.parse(JSON.stringify(LIVE_JSON));
  c.billing.presets.find((p) => p.id === 'aws-us-east-1').request_classes[0].price_per_1000 = null;
  assert.throws(() => contract.load(c), /billing\.presets: aws-us-east-1\.request_classes\[0\].*price_per_1000/);
});

test('test_run_with_an_invalid_price_shows_quantities_not_money', () => {
  const C = contract.load(LIVE_JSON);
  const p = aws();
  p.request_classes[0].price_per_1000 = null;
  const s = presets.SCENARIOS.logs;
  const r0 = pipeline.resolveInputs(C, s.values, s.locks, {});
  const res = pipeline.run(C, r0.inputs, p, {});
  assert.equal(res.page_state, 'prices_missing');
  assert.match(res.page_reason, /has invalid prices: aws-us-east-1\.request_classes\[0\]/);
  assert.equal(res.cheapest, null);
  for (const vr of Object.values(res.variants)) assert.equal(vr.total, null);
});
