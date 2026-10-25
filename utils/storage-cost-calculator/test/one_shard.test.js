'use strict';
// The page models one shard: data size at most 200 TiB, ingest at most that per day, retention at most 100 years.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const ui = require('../ui.js');
const urlstate = require('../urlstate.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const resolve = (values, locks) => pipeline.resolveInputs(LIVE, values, locks, {});

test('test_data_size_is_at_most_200_tib_per_shard', () => {
  const locks = ['data_bytes', 'retention_days'];
  assert.deepEqual(resolve({ data_bytes: 204800, retention_days: 30 }, locks).problems, []);
  const over = resolve({ data_bytes: 204801, retention_days: 30 }, locks).problems;
  assert.equal(over.length, 1, over.join('; '));
  assert.match(over[0], /^data_bytes: 204,801 GiB is above 204,800 GiB: a single shard holds at most 200 TiB; multi-shard is out of scope$/);
  // A derived data size above the cap names the locked members that produced it.
  const derived = resolve({ ingest_rate: 10000, retention_days: 30 }, ['ingest_rate', 'retention_days']).problems;
  assert.equal(derived.length, 1, derived.join('; '));
  assert.match(derived[0], /^data_bytes: 300,000 GiB \(ingest_rate × retention_days\) is above 204,800 GiB/);
});

test('test_ingest_and_retention_bounds_follow_the_cap', () => {
  assert.deepEqual(resolve({ ingest_rate: 204800, retention_days: 1 }, ['ingest_rate', 'retention_days']).problems, []);
  assert.match(resolve({ ingest_rate: 204801, retention_days: 1 }, ['ingest_rate', 'retention_days']).problems.join('; '),
    /ingest_rate: 204,801 GiB\/day is above 204,800 GiB\/day/);
  // The slider case that reached 1,368,670 days: retention derived from a large data size and a small ingest.
  const p = resolve({ data_bytes: 200000, ingest_rate: 1 }, ['data_bytes', 'ingest_rate']).problems;
  assert.equal(p.length, 1, p.join('; '));
  assert.match(p[0], /^retention_days: 200,000 days \(data_bytes \/ ingest_rate\) is above 36,500 days: retention is at most 100 years$/);
  assert.deepEqual(resolve({ data_bytes: 36500, ingest_rate: 1 }, ['data_bytes', 'ingest_rate']).problems, []);
});

test('test_shards_is_fixed_at_one_and_off_the_form', () => {
  const q = LIVE.registry.find((x) => x.id === 'shards');
  assert.equal(q.kind, 'pinned');
  assert.equal(q.default, 1);
  for (const sc of ['logs', 'custom']) {
    const s = ui.defaultState(LIVE, sc);
    assert.equal('shards' in s.inputs, false, sc);
    const out = ui.compute(LIVE, s, {});
    assert.equal(ui.renderParts(out.vm, out.page).inputs.includes('data-field="shards"'), false, sc);
    // The URL carries no shard count.
    const back = urlstate.decode(urlstate.encode(Object.assign({}, s, { manifest: LIVE.manifest })), LIVE);
    assert.equal('shards' in back.state.inputs, false, sc);
  }
  // A link that sets a shard count is refused, not silently priced as one shard.
  assert.match(resolve({ shards: 2 }, []).problems.join('; '), /shards/);
});

test('test_data_and_ingest_sliders_are_logarithmic', () => {
  const out = ui.compute(LIVE, ui.defaultState(LIVE, 'logs'), {});
  const html = ui.renderParts(out.vm, out.page).inputs;
  for (const id of ['data_bytes', 'ingest_rate']) {
    const m = new RegExp(`<input type="range" min="0" max="1000" step="1" data-action="slider" data-id="${id}" data-scale="log" data-lo="([\\d.]+)" data-hi="([\\d.]+)" value="(\\d+)"`).exec(html);
    assert.ok(m, id);
    assert.equal(Number(m[2]), 204800, id);
  }
  // Both ends map to the bounds; positions in between give three significant digits.
  const s = ui.defaultState(LIVE, 'logs');
  const at = (pos) => {
    const r = ui.update(LIVE, s, {}, { type: 'slider', data: { action: 'slider', id: 'data_bytes', scale: 'log', lo: '10', hi: '204800' }, value: String(pos) }, {});
    return r.state.inputs.data_bytes;
  };
  assert.equal(at(1000), 204800);
  assert.equal(at(0), 10);
  const mid = at(500);
  assert.equal(mid, Number(mid.toPrecision(3)));
  assert.ok(mid > 1000 && mid < 2000, String(mid));
});
