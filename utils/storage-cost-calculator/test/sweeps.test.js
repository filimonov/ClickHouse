'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contractLoader = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');

const ROOT = path.join(__dirname, '..');
const MINIMAL = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8'));
const AWS = 'aws-us-east-1';
const code = { pipeline };
const close = (a, b, msg) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)), `${msg || ''} ${a} != ${b}`);
const fixture = () => contractLoader.load(JSON.parse(JSON.stringify(MINIMAL)));
const scenario = (inserts) => ({ a: { values: { inserts_per_month: inserts }, locks: ['ingest_rate', 'retention_days'] } });

test('test_breakeven_matches_independent_cases', async () => {
  const { buildBreakeven } = await import('../tools/sweeps.mjs');
  const c = fixture();
  // Case 1, by hand: x GiB on gp3 costs 0.08 x; CAS costs 0.023 x + 1.3 (130 000 inserts x 2 PUTs at 0.005 per 1 000).
  // Equal at x = 1.3 / 0.057 = 22.8, so the order flips between 20 and 30. S3 stays far above EBS.
  const one = buildBreakeven(code, c, scenario(130000), { data_bytes: [10, 20, 30, 40] }, AWS).a.sweeps.data_bytes;
  const at = (x) => one.points.find((p) => p.x === x).totals;
  close(at(20).ebs.value, 1.6);
  close(at(20).cas.value, 0.023 * 20 + 1.3);
  close(at(30).ebs.value, 2.4);
  close(at(30).cas.value, 0.023 * 30 + 1.3);
  assert.deepEqual(one.crossing.filter((x) => x.a === 'ebs' && x.b === 'cas').map((x) => x.x_between), [[20, 30]]);
  assert.equal(one.crossing.filter((x) => x.b === 's3' || x.a === 's3').length, 0);
  assert.deepEqual(one.states.cas, ['ok']);

  // Case 2, by hand: no inserts, so CAS is 0.023 x and EBS 0.08 x. EBS is dearer at every x: no crossing at all.
  const none = buildBreakeven(code, c, scenario(0), { data_bytes: [10, 20, 30, 40] }, AWS).a.sweeps.data_bytes;
  for (const x of [10, 20, 30, 40]) {
    close(none.points.find((p) => p.x === x).totals.cas.value, 0.023 * x);
    close(none.points.find((p) => p.x === x).totals.ebs.value, 0.08 * x);
  }
  assert.equal(none.crossing, null);
});

test('test_breakeven_entry_for_unavailable_totals_records_states', async () => {
  const { buildBreakeven } = await import('../tools/sweeps.mjs');
  // A contract without the prices of the preset cannot total: states are recorded, there is no crossing.
  const c = fixture();
  const r = buildBreakeven(code, c, scenario(130000), { data_bytes: [10, 20] }, 'gcs').a.sweeps.data_bytes;
  assert.equal(r.crossing, null);
  for (const v of c.variants) assert.equal(r.points[0].totals[v].value, null);
});

test('test_live_contract_breakeven_shape', async () => {
  const { buildBreakeven, LADDERS, PRICE_PRESET } = await import('../tools/sweeps.mjs');
  const c = contractLoader.load(JSON.parse(fs.readFileSync(path.join(ROOT, 'model-contract.json'), 'utf8')));
  const all = buildBreakeven({ pipeline, presets }, c, presets.SCENARIOS, LADDERS, PRICE_PRESET);
  assert.deepEqual(Object.keys(all), Object.keys(presets.SCENARIOS));
  assert.deepEqual(Object.keys(LADDERS), ['data_bytes', 'ingest_rate', 'replicas']);
  for (const [sid, sc] of Object.entries(all)) {
    for (const [input, ladder] of Object.entries(LADDERS)) {
      const e = sc.sweeps[input];
      assert.ok(e, `${sid} x ${input}`);
      assert.deepEqual(e.points.map((p) => p.x), ladder);
      for (const p of e.points) {
        for (const v of c.variants) {
          const t = p.totals[v];
          assert.ok(t && typeof t.state === 'string' && 'value' in t, `${sid} ${input} ${v}`);
          assert.equal(t.value === null, t.state !== 'ok');
        }
      }
      for (const v of c.variants) assert.ok(e.states[v].length > 0);
      if (e.crossing !== null) {
        assert.ok(e.crossing.length > 0);
        for (const x of e.crossing) assert.ok(Array.isArray(x.x_between) && x.x_between[0] < x.x_between[1]);
      }
    }
  }
});

test('test_ladders_are_geometric', async () => {
  const { LADDERS } = await import('../tools/sweeps.mjs');
  for (const [id, xs] of Object.entries(LADDERS)) {
    for (let i = 2; i < xs.length; i++) close(xs[i] / xs[i - 1], xs[1] / xs[0], id);
  }
});
