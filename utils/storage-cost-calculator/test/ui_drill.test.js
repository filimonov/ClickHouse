'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const ui = require('../ui.js');

const TOOLS = path.join(__dirname, '..', 'tools');
const FX = path.join(__dirname, 'fixtures', 'drill');
const BILLING = path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'billing-contract.json');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'scc-drill-'));
test.after(() => fs.rmSync(tmp, { recursive: true, force: true }));

const node = (script, args) => spawnSync(process.execPath, [path.join(TOOLS, script), ...args], { encoding: 'utf8' });
const drillArgs = (jsonl, extra) => [path.join(FX, jsonl), '--probe', 'probe_fixture',
  '--prediction', path.join(FX, 'prediction.json'), '--formulas', path.join(FX, 'formulas.json'),
  '--registry', path.join(FX, 'registry.json'), '--billing', BILLING, ...(extra || [])];

function runDrill(name, jsonl, extra) {
  const out = path.join(tmp, `${name}.json`);
  const r = node('drill.mjs', drillArgs(jsonl, ['--out', out, ...(extra || [])]));
  return { r, out, json: fs.existsSync(out) ? JSON.parse(fs.readFileSync(out, 'utf8')) : null };
}

test('test_drill_json_and_exit_code', () => {
  const { r, json } = runDrill('ok', 'measurements.jsonl');
  assert.equal(r.status, 0, r.stderr);
  assert.equal(json.probe, 'probe_fixture');
  assert.equal(json.measurement_id, 'fx/s3_default/probe_fixture/case/k0');
  assert.deepEqual(json.mismatches, []);
  const c = json.cells.insert;
  const put = c.stage2_rows.find((x) => x.op_kind === 'PutObject');
  assert.deepEqual([put.predicted, put.measured, put.exact, put.branch_taken], [3, 3, true, ['then']]);
  assert.equal(c.stage1_inputs.inputs.parts_inserted, 1);
  assert.equal(c.stage1_inputs.coefs.min_bytes_for_wide_part, 10485760);
  assert.equal(c.stage3_ledger_summary.by_row_type.request.rows, 2);
  const money = Object.fromEntries(c.stage4_billing_lines.map((l) => [l.rule_id, l.money]));
  assert.equal(money.storage, 23); // 1000 GB * 0.023
  assert.equal(money.ec2_ec2_cross_az, 1); // 100 GB * 0.01
  assert.equal(money.block_capacity, 4); // stripe2: 2 x 25 GiB * 0.08
  assert.ok(Math.abs(c.billing_total - 28.000019) < 1e-9);
  assert.equal(c.stage5_allocation.layout, 'stripe2');
  assert.equal(c.stage5_allocation.monthly_cost, 4);
  assert.match(r.stdout, /PutObject ok \| then \| 3 \| 3/);

  const only = runDrill('cell', 'measurements.jsonl', ['--cell', 's3/insert']);
  assert.equal(only.r.status, 0);
  assert.deepEqual(Object.keys(only.json.cells), ['insert']);
  const none = runDrill('nocell', 'measurements.jsonl', ['--cell', 's3/nope']);
  assert.equal(none.r.status, 1);
});

test('test_drill_exit_code_on_mismatch_and_missing_input', () => {
  const bad = runDrill('bad', 'measurements-mismatch.jsonl');
  assert.equal(bad.r.status, 1);
  assert.match(bad.r.stderr, /PutObject ok" predicted 3 measured 4/);
  assert.equal(bad.json.cells.insert.stage2_rows[0].exact, false);
  assert.equal(bad.json.mismatches.length, 2); // not equal, and outside the prediction

  const pred = JSON.parse(fs.readFileSync(path.join(FX, 'prediction.json'), 'utf8'));
  pred.inputs = {};
  const p = path.join(tmp, 'pred-missing.json');
  fs.writeFileSync(p, JSON.stringify(pred));
  const args = drillArgs('measurements.jsonl').map((a) => (a === path.join(FX, 'prediction.json') ? p : a));
  const r = node('drill.mjs', args);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /missing input parts_inserted/);

  assert.equal(node('drill.mjs', [path.join(FX, 'measurements.jsonl'), '--probe', 'nobody', '--formulas', path.join(FX, 'formulas.json')]).status, 1);
  assert.equal(node('drill.mjs', []).status, 1);
});

test('test_render_drill_contains_substituted_values_and_money', () => {
  const { r, out, json } = runDrill('render', 'measurements.jsonl');
  assert.equal(r.status, 0, r.stderr);
  const html = ui.renderDrillHtml(json);
  const row = html.split('<tr ').find((t) => t.startsWith('data-row="insert#0"'));
  assert.match(row, /PutObject ok/);
  assert.equal((row.match(/data-value="3"/g) || []).length, 3); // predicted, measured and expected
  assert.match(row, /data-exact="true"/);
  for (const m of ['23', '1', '4', '0.000015', '28.000019']) assert.ok(html.includes(`data-value="${m}" data-kind="money"`), `money ${m}`);
  assert.ok(html.includes('stripe2: <span data-value="2">2</span> x <span data-value="25">25</span> GiB'));
  for (const n of [1, 2, 3, 4, 5]) assert.ok(html.includes(`data-stage="${n}"`));
  assert.equal(html.includes('data-exact="false"'), false);

  const ok = node('render_check.mjs', [out, '--probe', 'probe_fixture']);
  assert.equal(ok.status, 0, ok.stderr);
  assert.match(ok.stdout, /render_check OK probe_fixture: 9 values, 7 money lines/);
  assert.equal(node('render_check.mjs', [out, '--probe', 'other']).status, 1);

  const doctored = JSON.parse(JSON.stringify(json));
  doctored.cells.insert.stage2_rows[0].exact = false;
  const dpath = path.join(tmp, 'doctored.json');
  fs.writeFileSync(dpath, JSON.stringify(doctored));
  assert.ok(ui.renderDrillHtml(doctored).includes('data-exact="false"'));
  const bad = node('render_check.mjs', [dpath, '--probe', 'probe_fixture']);
  assert.equal(bad.status, 1);
  assert.match(bad.stderr, /data-exact="false"/);
});

test('test_rendered_html_is_balanced', () => {
  const { json } = runDrill('balanced', 'measurements.jsonl');
  const html = ui.renderDrillHtml(json);
  const stack = [];
  const voidTags = new Set(['br', 'hr', 'img', 'input', 'meta', 'link']);
  for (const m of html.matchAll(/<(\/?)([a-z][a-z0-9]*)[^>]*?>/g)) {
    if (voidTags.has(m[2])) continue;
    if (m[1]) assert.equal(stack.pop(), m[2], `unbalanced </${m[2]}>`);
    else stack.push(m[2]);
  }
  assert.deepEqual(stack, []);
  const hostile = ui.renderDrillHtml({ probe: '<script>x</script>', cells: {} });
  assert.equal(hostile.includes('<script>'), false);
});

test('test_index_page_template', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  assert.match(html, /^<!doctype html>/i);
  assert.match(html, /<title>Storage Cost Calculator<\/title>/);
  for (const id of ['presets', 'inputs', 'cards', 'table', 'drill', 'notes', 'evidence', 'chart']) assert.ok(html.includes(`id="${id}"`), id);
  assert.match(html, /<!-- contract:begin --><script type="application\/json" id="contract">\{\}<\/script><!-- contract:end -->/);
  assert.deepEqual([...html.matchAll(/<script src="([^"]+)"/g)].map((m) => m[1]), ['model.js', 'contract.js', 'billing.js', 'ui.js']);
  const css = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
  for (const t of ['--bg', '--card', '--ink', '--mute', '--line', '--accent', '--ok', '--warn', '--cls-bytes', '--cls-transfer', '--cls-put', '--cls-get', '--cls-block']) assert.ok(css.includes(`${t}:`), t);
  assert.ok(css.includes(':root:not([data-theme="light"])') && css.includes(':root[data-theme="dark"]'));
});
