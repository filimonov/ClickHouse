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

// A trial the pipeline rejects with a problem naming the input is the form saying "not a valid value", not an unpriced
// column. Presets must resolve without any problem.
function offenders(c) {
  const aws = presets.resolvePrice(c, 'aws-us-east-1');
  const out = [];
  const rejected = [];
  const check = (label, values, locks, inputId) => {
    const r = pipeline.resolveInputs(c, values, locks, {});
    if (inputId === undefined) assert.deepEqual(r.problems, [], label);
    else if (r.problems.length) {
      assert.ok(r.problems.every((p) => p.includes(inputId)), `${label}: ${r.problems.join('; ')}`);
      rejected.push(label);
      return;
    }
    const res = pipeline.run(c, r.inputs, aws, {});
    for (const [v, vr] of Object.entries(res.variants)) {
      if (INTERNAL.includes(vr.state)) out.push(`${label}: ${v} ${vr.state}`);
    }
  };
  for (const [id, s] of Object.entries(presets.SCENARIOS)) check(`preset ${id}`, s.values, s.locks);
  const base = presets.SCENARIOS.custom;
  for (const q of c.registry.filter((x) => x.kind === 'input')) {
    for (const x of trials(q)) check(`custom ${q.id}=${x}`, Object.assign({}, base.values, { [q.id]: x }), base.locks, q.id);
  }
  return { out, rejected };
}

// Columns the model cannot price by design, each with the reason; an exception that stops occurring fails the test.
const EXCEPTIONS = {};

test('test_trials_cover_enums_and_out_of_range_numbers', () => {
  assert.deepEqual(trials({ id: 'a', value_type: 'enum', enum: ['x', 'y'] }), ['x', 'y']);
  assert.deepEqual(trials({ id: 'n', value_type: 'number', validity: { n: [1, 6] } }), [1, 6, 0, 7, 0.1, 60]);
  assert.deepEqual(trials({ id: 'n', value_type: 'number', validity: null }), []);
});

test('test_no_input_or_preset_leaves_a_column_unpriced', (t) => {
  const { out, rejected } = offenders(LIVE);
  t.diagnostic(`${rejected.length} trial(s) rejected as input problems; ${out.length} unpriced column(s)`);
  assert.deepEqual(out, Object.keys(EXCEPTIONS));
  // Out-of-range fractions are rejected, not priced into negative request counts.
  assert.ok(rejected.includes('custom cas_hash_hit_fraction=10'), rejected.join('\n'));
});
