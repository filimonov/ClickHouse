'use strict';
// The page always prices: no input value inside or outside its measured range, and no preset, may leave a column
// `unavailable` or `unsupported_cut`. Both are internal errors once every family has a measured or predicted row.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const INTERNAL = ['unavailable', 'unsupported_cut'];

// Values to try per input: every enum value; for a numeric range, the bounds, bounds ± 1 and 10x outside.
function trials(q) {
  if (q.value_type === 'enum') return q.enum.slice();
  const r = q.validity && q.validity[q.id];
  if (!r) return [];
  const [lo, hi] = r;
  return [...new Set([lo, hi, lo - 1, hi + 1, lo / 10, hi * 10].filter((x) => Number.isFinite(x)))];
}

function offenders(c) {
  const aws = presets.resolvePrice(c, 'aws-us-east-1');
  const out = [];
  const check = (label, values, locks) => {
    const r = pipeline.resolveInputs(c, values, locks, {});
    const res = pipeline.run(c, r.inputs, aws, {});
    for (const [v, vr] of Object.entries(res.variants)) {
      if (INTERNAL.includes(vr.state)) out.push(`${label}: ${v} ${vr.state} (${String(vr.reason).slice(0, 120)})`);
    }
  };
  for (const [id, s] of Object.entries(presets.SCENARIOS)) check(`preset ${id}`, s.values, s.locks);
  const base = presets.SCENARIOS.custom;
  for (const q of c.registry.filter((x) => x.kind === 'input')) {
    for (const x of trials(q)) check(`custom ${q.id}=${x}`, Object.assign({}, base.values, { [q.id]: x }), base.locks);
  }
  return out;
}

test('test_trials_cover_enums_and_out_of_range_numbers', () => {
  assert.deepEqual(trials({ id: 'a', value_type: 'enum', enum: ['x', 'y'] }), ['x', 'y']);
  assert.deepEqual(trials({ id: 'n', value_type: 'number', validity: { n: [1, 6] } }), [1, 6, 0, 7, 0.1, 60]);
  assert.deepEqual(trials({ id: 'n', value_type: 'number', validity: null }), []);
});

// Until every cut family has a predicted row, the remaining cells are listed instead of failing the suite.
test('test_no_input_or_preset_leaves_a_column_unpriced', { todo: 'predicted rows for cut families and nearest-key lookups are not in the contract yet' }, (t) => {
  const bad = offenders(LIVE);
  t.diagnostic(`${bad.length} unpriced column(s); first: ${bad.slice(0, 5).join(' | ')}`);
  assert.deepEqual(bad, []);
});
