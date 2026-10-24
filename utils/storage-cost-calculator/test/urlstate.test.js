'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const urlstate = require('../urlstate.js');

const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
const clone = (x) => JSON.parse(JSON.stringify(x));

const fullState = () => ({
  inputs: { data_bytes: 1.5e12, ingest_rate: 50, retention_days: 30, label: 'Ünïcode ✓ данные' },
  locks: ['ingest_rate', 'retention_days'],
  overrides: { 'cell.x': 0 },
  scenario: 'aws-us-east-1',
  price_preset: 'custom',
  custom_prices: { 'ebs.gp3_gb_month': 0.0825, 's3.put_per_1000': 0.005 },
  units: 'binary_gb',
  openCell: null,
  sweepInput: 'retention_days',
  manifest: clone(contract.manifest),
});

test('test_roundtrip_full_state_including_custom_prices_and_units', () => {
  const s = fullState();
  const r = urlstate.decode(urlstate.encode(s), contract);
  assert.equal(r.problem, null);
  assert.deepEqual(r.state, s);
  assert.equal(r.state.units, 'binary_gb');
  assert.deepEqual(r.state.custom_prices, s.custom_prices);
});

test('test_url_with_unknown_versions_sets_cannot_reproduce', () => {
  const s = fullState();
  s.manifest.registry_version = '0.1.0';
  s.manifest.evidence[0].commit = 'deadbeef';
  const r = urlstate.decode(urlstate.encode(s), contract);
  assert.equal(r.problem.kind, 'cannot_reproduce');
  const reg = r.problem.missing.find((m) => m.artifact === 'registry' && m.field === 'registry_version');
  assert.equal(reg.version, '0.1.0');
  assert.equal(reg.loaded_value, contract.manifest.registry_version);
  const ev = r.problem.missing.find((m) => m.artifact === 'evidence:' + contract.manifest.evidence[0].report);
  assert.equal(ev.version, 'deadbeef');

  // Entries absent on either side are mismatches too.
  const t = fullState();
  delete t.manifest.billing_sha256;
  t.manifest.evidence.pop();
  const m = urlstate.decode(urlstate.encode(t), contract).problem.missing;
  assert.ok(m.some((x) => x.artifact === 'billing' && x.field === 'billing_sha256'));
  assert.ok(m.some((x) => x.artifact.startsWith('evidence:') && x.field === 'report'));
});

test('test_hash_is_url_safe', () => {
  const h = urlstate.encode(fullState());
  assert.match(h, /^#v1\.[A-Za-z0-9_-]+$/);
  // Keys are sorted, so key order in the input does not change the hash.
  const a = { z: 1, a: { y: 2, b: 3 } };
  const b = { a: { b: 3, y: 2 }, z: 1 };
  assert.equal(urlstate.encode(a), urlstate.encode(b));
  // Lengths 1..4 bytes exercise every padding-free tail.
  for (const v of ['a', 'ab', 'abc', 'abcd']) assert.deepEqual(urlstate.decode(urlstate.encode({ manifest: clone(contract.manifest), v }), contract).state.v, v);
});

test('test_malformed_hash_is_a_problem_not_partial_state', () => {
  const good = urlstate.encode(fullState());
  const cases = [
    '', '#', '#v2.abc', 'v1.abc', '#v1.!!!', '#v1.a', '#v1.' + good.slice(4, -3),
    '#v1.' + Buffer.from('[1,2]').toString('base64url'),
    '#v1.' + Buffer.from('{"inputs":{}}').toString('base64url'),
    '#v1.' + Buffer.from([0xff, 0xfe, 0xfd]).toString('base64url'),
    '#v1.' + Buffer.from('{"manifest":').toString('base64url'),
  ];
  for (const h of cases) {
    const r = urlstate.decode(h, contract);
    assert.equal(r.state, null, h);
    assert.equal(r.problem.kind, 'url_format', h);
    assert.equal(r.problem.missing[0].artifact, 'url_format', h);
  }
});
