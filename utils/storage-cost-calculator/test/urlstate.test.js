'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const urlstate = require('../urlstate.js');

const contract = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
const ALPHA = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
const clone = (x) => JSON.parse(JSON.stringify(x));

const fullState = () => ({
  inputs: { data_bytes: 1.5e12, ingest_rate: { value: 50, per: 'hour' }, retention_days: 30, type_mix: 'plain' },
  locks: ['ingest_rate', 'retention_days'],
  overrides: { replicas: 4 },
  scenario: 'logs',
  price_preset: 'custom',
  custom_prices: { 'block.gp3.price_per_gib_month': 0.0825, 'request_classes.put_copy_post_list.price_per_1000': 0.005 },
  units: 'hour',
  openCell: 'ebs.storage',
  sweepInput: 'data_bytes',
  manifest: clone(contract.manifest),
});

test('test_roundtrip_full_state_including_custom_prices_and_units', () => {
  const s = fullState();
  const r = urlstate.decode(urlstate.encode(s), contract);
  assert.equal(r.problem, null);
  assert.deepEqual(r.state, s);
  assert.equal(r.state.units, 'hour');
  assert.deepEqual(r.state.custom_prices, s.custom_prices);
});

test('test_url_with_unknown_versions_sets_cannot_reproduce', () => {
  const s = fullState();
  s.manifest.registry_version = '0.1.0';
  s.manifest.evidence[0].commit = 'deadbeef';
  const r = urlstate.decode(urlstate.encode(s), contract);
  assert.equal(r.problem.kind, 'cannot_reproduce');
  assert.equal(r.state, null);
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
  // Payload lengths of 1 to 4 bytes exercise every padding-free tail length (mod 3).
  for (const v of ['a', 'ab', 'abc', 'abcd']) assert.deepEqual(urlstate.decode(urlstate.encode(Object.assign(fullState(), { v })), contract).state.v, v);
});

test('test_every_manifest_entry_is_compared', () => {
  const mutate = (f) => { const s = fullState(); f(s.manifest); return urlstate.decode(urlstate.encode(s), contract).problem; };
  const field = (p, f) => p.missing.some((m) => m.field === f);
  assert.ok(field(mutate((m) => { m.evidence[0].binary_sha256 = '00'; }), 'binary_sha256'));
  assert.ok(field(mutate((m) => { m.evidence[1].rig_commit = '00'; }), 'rig_commit'));
  assert.ok(field(mutate((m) => { m.code_files['model.js'] = '00'; }), 'code_files.model.js'));
  // Adds an entry: once every code file exists the loaded list is empty, and `[]` would not differ from it.
  assert.ok(field(mutate((m) => { m.code_missing = [...m.code_missing, 'not-a-file.js']; }), 'code_missing'));
  assert.ok(field(mutate((m) => { delete m.code_files['billing.js']; }), 'code_files.billing.js'));
  // Loaded side lacking an entry.
  const loaded = clone(contract);
  delete loaded.manifest.coverage_sha256;
  loaded.manifest.evidence.pop();
  const p = urlstate.decode(urlstate.encode(fullState()), loaded).problem;
  assert.ok(field(p, 'coverage_sha256'));
  assert.ok(p.missing.some((m) => m.field === 'report' && m.loaded_value === null));
  assert.throws(() => urlstate.decode(urlstate.encode(fullState()), {}), TypeError);
});

test('test_hash_has_no_standard_base64_characters', () => {
  // 0xfb 0xff 0xbf encodes to "+/+/" in standard base64 and 0xff pads to "/w==".
  const s = fullState();
  s.inputs.bytes = '\u00fb\u00ff\u00bf\u00ff~~?>>>';
  const h = urlstate.encode(s);
  assert.doesNotMatch(h, /[+/=]/);
  // The bytes survive the encoding; the decoder then refuses the unknown input by name.
  assert.deepEqual(JSON.parse(Buffer.from(h.slice(4), 'base64url').toString('utf8')).inputs.bytes, s.inputs.bytes);
  assert.match(urlstate.decode(h, contract).problem.reason, /inputs: "bytes" is not a registry input/);
  assert.match(Buffer.from('\xfb\xff\xbf', 'latin1').toString('base64'), /[+/]/);
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
  const noInputs = fullState();
  delete noInputs.inputs;
  cases.push(urlstate.encode(noInputs));
  const badType = fullState();
  badType.locks = 'x';
  cases.push(urlstate.encode(badType));
  // Non-canonical tail: same bytes as `good` but with nonzero pad bits in the last character.
  const g = good.slice(4);
  const alt = ALPHA[(ALPHA.indexOf(g.slice(-1)) + 1) % 64];
  if (g.length % 4 !== 0) cases.push('#v1.' + g.slice(0, -1) + alt);
  for (const h of cases) {
    const r = urlstate.decode(h, contract);
    assert.equal(r.state, null, h);
    assert.equal(r.problem.kind, 'url_format', h);
    assert.equal(r.problem.missing[0].artifact, 'url_format', h);
  }
});

test('test_out_of_domain_values_are_a_url_format_problem', () => {
  const cases = [
    ['sweepInput', (s) => { s.sweepInput = 'foo'; }, /unknown sweep input "foo"/],
    ['units', (s) => { s.units = 'week'; }, /unknown rate unit "week"/],
    ['openCell', (s) => { s.openCell = 'foo'; }, /open cell "foo" is not a variant\.operation/],
    ['openCell operation', (s) => { s.openCell = 'ebs.foo'; }, /open cell "ebs\.foo"/],
    ['scenario', (s) => { s.scenario = 'aws-us-east-1'; }, /unknown scenario/],
    ['price preset', (s) => { s.price_preset = 'cheap'; }, /unknown price preset/],
    ['price path', (s) => { s.custom_prices = { 'x.y': 1 }; }, /price x\.y: no such price field/],
    ['price NaN', (s) => { s.custom_prices = { 'block.gp3.iops_price': 'NaN' }; }, /custom price block\.gp3\.iops_price: "NaN" is not a finite number/],
    ['price negative', (s) => { s.custom_prices = { 'block.gp3.iops_price': -1 }; }, /is not a finite number of 0 or more/],
    ['unknown input', (s) => { s.inputs.label = 'x'; }, /inputs: "label" is not a registry input/],
    ['enum value', (s) => { s.inputs.type_mix = 'round'; }, /inputs\.type_mix: "round" is not one of/],
    ['number as word', (s) => { s.overrides.replicas = 'many'; }, /overrides\.replicas: "many" is not a number/],
    ['null input', (s) => { s.inputs.replicas = null; }, /inputs\.replicas: null is not a finite number or a word/],
    ['rate shape', (s) => { s.inputs.ingest_rate = { value: 1, per: 'fortnight' }; }, /inputs\.ingest_rate: expected a number, a word or \{value, per\}/],
    ['lock', (s) => { s.locks = ['replicas']; }, /locks: "replicas" is not a member of a precedence group/],
  ];
  for (const [name, mutate, re] of cases) {
    const s = fullState();
    mutate(s);
    const r = urlstate.decode(urlstate.encode(s), contract);
    assert.equal(r.state, null, name);
    assert.equal(r.problem.kind, 'url_format', name);
    assert.match(r.problem.reason, re, name);
  }
});

test('test_proto_keys_are_rejected_without_pollution', () => {
  for (const field of ['inputs', 'overrides', 'custom_prices']) {
    const s = fullState();
    const text = JSON.stringify(s).replace(`"${field}":{`, `"${field}":{"__proto__":{"polluted":1},`);
    const enc = new TextEncoder().encode(text);
    let b = '';
    for (let i = 0; i < enc.length; i += 3) {
      const n = (enc[i] << 16) | ((enc[i + 1] || 0) << 8) | (enc[i + 2] || 0);
      b += ALPHA[(n >> 18) & 63] + ALPHA[(n >> 12) & 63] + (i + 1 < enc.length ? ALPHA[(n >> 6) & 63] : '') + (i + 2 < enc.length ? ALPHA[n & 63] : '');
    }
    const r = urlstate.decode(`#v1.${b}`, contract);
    assert.equal(r.state, null, field);
    assert.equal(r.problem.kind, 'url_format', field);
    assert.equal({}.polluted, undefined);
  }
});

test('test_hidden_inputs_only_from_the_preset', () => {
  const c = clone(contract);
  const q = c.registry.find((x) => x.id === 'merge_size_ratio');
  q.ui = { tier: 'hidden', name: null, short: 'Final part size in inserts' };
  const s = fullState();
  // The logs preset sets merge_size_ratio = 256; the same value is the preset's, another is a user input.
  s.inputs.merge_size_ratio = 256;
  assert.equal(urlstate.decode(urlstate.encode(s), c).problem, null);
  s.inputs.merge_size_ratio = 64;
  assert.match(urlstate.decode(urlstate.encode(s), c).problem.reason, /inputs: merge_size_ratio is not a user input/);
  const o = fullState();
  o.overrides.merge_size_ratio = 256;
  assert.match(urlstate.decode(urlstate.encode(o), c).problem.reason, /overrides: merge_size_ratio is not a user input/);
});

test('test_cannot_reproduce_names_the_link_contract_version', () => {
  const s = fullState();
  s.manifest.code_files = Object.assign({}, s.manifest.code_files, { 'ui.js': 'other' });
  const r = urlstate.decode(urlstate.encode(s), contract);
  assert.equal(r.problem.kind, 'cannot_reproduce');
  assert.equal(r.problem.url_contract_version, contract.manifest.contract_version);
  const ui = require('../ui.js');
  const res = ui.cannotReproduceResult(contract, r.problem);
  assert.match(res.page_reason, new RegExp(`The link was made with contract version ${contract.manifest.contract_version}\\.$`));
});
