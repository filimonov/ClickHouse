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
  assert.equal(c.stage3_ledger_summary.by_row['request PutObject'].rows, 1);
  // IOPS and MiB/s stay apart: no summary entry adds quantities of different units.
  assert.deepEqual([c.stage3_ledger_summary.by_row['block_io iops'].quantity, c.stage3_ledger_summary.by_row['block_io mibps'].quantity], [6000, 200]);
  const money = Object.fromEntries(c.stage4_billing_lines.map((l) => [l.rule_id, l.money]));
  assert.equal(money.storage, 23); // 1000 GB * 0.023
  assert.equal(money.ec2_ec2_cross_az, 1); // 100 GB * 0.01
  assert.equal(money.block_capacity, 4); // stripe2: 2 x 25 GiB * 0.08
  assert.ok(Math.abs(c.billing_total - 28.000019) < 1e-9);
  assert.equal(c.stage5_allocation.provisioning.layout, 'stripe2');
  assert.equal(c.stage5_allocation.provisioning.monthly_cost, 4);
  assert.equal(c.evidence.inventory_initial, 'declared');
  assert.deepEqual(c.evidence.limitations, ['fixture limitation: one workstation']);
  assert.equal(json.contract.registry_version, '1');
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
  assert.match(bad.r.stderr, /request PutObject ok" predicted 3 measured 4/);
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
  assert.match(ok.stdout, /render_check OK probe_fixture: 11 values, 14 money lines/);
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
  // tools/generate_contract.py writes the block; it holds the same contract as the standalone copy.
  const block = html.match(/<!-- contract:begin --><script type="application\/json" id="contract">(.*?)<\/script><!-- contract:end -->/s);
  assert.ok(block, 'contract block');
  const standalone = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
  assert.deepEqual(JSON.parse(block[1]), standalone);
  assert.equal(standalone.schema, 'contract/1');
  assert.deepEqual([...html.matchAll(/<script src="([^"]+)"/g)].map((m) => m[1]),
    ['model.js', 'contract.js', 'billing.js', 'pipeline.js', 'presets.js', 'urlstate.js', 'viewmodel.js', 'ui.js']);
  const css = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
  for (const t of ['--bg', '--card', '--ink', '--mute', '--line', '--accent', '--ok', '--warn', '--cls-bytes', '--cls-transfer', '--cls-put', '--cls-get', '--cls-block']) assert.ok(css.includes(`${t}:`), t);
  assert.ok(css.includes(':root:not([data-theme="light"])') && css.includes(':root[data-theme="dark"]'));
});

test('test_render_escapes_injected_strings', () => {
  const evil = '<img src=x onerror=alert(1)>';
  const html = ui.renderDrillHtml({
    probe: evil, measurement_id: evil, mismatches: [evil],
    cells: { [evil]: {
      stage1_inputs: { inputs: { [evil]: evil }, coefs: {}, observations: { k: evil } },
      stage2_rows: [{ row: evil, branch_taken: [evil], predicted: evil, measured: 1, expected: null, exact: evil }],
      stage3_ledger_summary: { by_row_type: { [evil]: { rows: 1, quantity: 1 } }, states: [{ id: evil, active: true, reason: evil }] },
      stage4_billing_lines: [{ rule_id: evil, label: evil, unit: evil, quantity: 1, money: 1, share: 0.5, share_text: evil, aggregate_money: 2, note: evil }], billing_total: 1,
      stage5_allocation: { layout: evil, volumes: 1, size_gib: 1, iops: 1, mibps: 1, monthly_cost: 1, alternatives: [{ layout: evil, volumes: 1, size_gib: 1, iops: 1, mibps: 1, monthly_cost: 1 }] },
    } },
  });
  assert.equal(html.includes('<img'), false);
  assert.ok(html.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.equal(ui.renderDrillHtml({ probe: "a'b", cells: {} }).includes("a'b"), false);
});

test('test_mixed_outcomes_compare_attempts_without_false_mismatch', () => {
  const args = drillArgs('measurements-mixed.jsonl').map((a) => (a.endsWith('formulas.json') ? path.join(FX, 'formulas-mixed.json')
    : a.endsWith('prediction.json') ? path.join(FX, 'prediction-mixed.json') : a));
  const out = path.join(tmp, 'mixed.json');
  const r = node('drill.mjs', [...args, '--out', out]);
  assert.equal(r.status, 0, r.stderr);
  const rows = JSON.parse(fs.readFileSync(out, 'utf8')).cells.insert.stage2_rows;
  // Each outcome class reconciles with its own ledger outcome: 3 upstream ok, 2 upstream errors.
  const ok = rows.find((x) => x.op_kind === 'PutObject' && x.outcome_class === 'ok');
  assert.deepEqual([ok.predicted, ok.measured, ok.exact], [3, 3, true]);
  const err = rows.find((x) => x.outcome_class === 'client_error');
  assert.deepEqual([err.predicted, err.measured, err.exact], [2, 2, true]);
});

test('test_cell_selecting_nothing_fails_and_non_ok_records_are_reported', () => {
  for (const cell of ['ebs/insert', 's3/nope', 'nope']) {
    const r = node('drill.mjs', drillArgs('measurements.jsonl', ['--cell', cell]));
    assert.equal(r.status, 1, cell);
    assert.match(r.stderr, /selected no cell across 1 record/);
  }
  const f = path.join(tmp, 'with-failed.jsonl');
  const lines = fs.readFileSync(path.join(FX, 'measurements.jsonl'), 'utf8').trim().split('\n');
  fs.writeFileSync(f, `${JSON.stringify({ card: 'probe_fixture', measurement_id: 'bad1', status: 'failed' })}\n${lines[1]}\n`);
  const r = node('drill.mjs', [f, ...drillArgs('measurements.jsonl').slice(1)]);
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /skipped 1 non-ok record\(s\): bad1 \(failed\)/);
});

// ---- review fixes: contract, states, allocation, reconciliation, evidence ---------------------

const readFx = (f) => JSON.parse(fs.readFileSync(path.join(FX, f), 'utf8'));
const recordFx = () => JSON.parse(fs.readFileSync(path.join(FX, 'measurements.jsonl'), 'utf8').trim().split('\n')[1]);
function drillWith(name, { record, formulas, prediction, extra } = {}) {
  const files = {};
  const put = (n, v) => { files[n] = path.join(tmp, `${name}-${n}`); fs.writeFileSync(files[n], typeof v === 'string' ? v : JSON.stringify(v)); };
  put('m.jsonl', `${JSON.stringify(record || recordFx())}\n`);
  put('f.json', formulas || readFx('formulas.json'));
  put('p.json', prediction || readFx('prediction.json'));
  const out = path.join(tmp, `${name}.json`);
  const r = node('drill.mjs', [files['m.jsonl'], '--probe', 'probe_fixture', '--prediction', files['p.json'], '--formulas', files['f.json'],
    '--registry', path.join(FX, 'registry.json'), '--billing', BILLING, '--out', out, ...(extra || [])]);
  return { r, json: fs.existsSync(out) ? JSON.parse(fs.readFileSync(out, 'utf8')) : null, out };
}

test('test_contract_validation_rejects_bad_dims_and_unregistered_observations', () => {
  const f = readFx('formulas.json');
  f.cells.insert.ledger_rows[0].expr.dim = 'GiB';
  const bad = drillWith('dims', { formulas: f });
  assert.equal(bad.r.status, 1);
  assert.match(bad.r.stderr, /mul of 1 and 1 is 1, node declares GiB/);
  const rec = recordFx();
  rec.observations.inputs = { parts_inserted: 1, not_in_registry: 5 };
  const obs = drillWith('obs', { record: rec });
  assert.equal(obs.r.status, 1);
  assert.match(obs.r.stderr, /observation not_in_registry is not a registered quantity/);
  assert.match(obs.r.stderr, /observation parts_inserted names a input quantity/);
});

test('test_unavailable_states_bill_nothing_and_render_the_state', () => {
  const f = readFx('formulas.json');
  f.cells.insert.states = [{ id: 'not_supported', cond: true, reason: 'fixture cannot do this' }];
  const ns = drillWith('notsup', { formulas: f });
  const c = ns.json.cells.insert;
  assert.equal(c.state, 'unavailable');
  assert.deepEqual([c.stage4_billing_lines, c.billing_total], [null, null]);
  assert.match(c.unavailable_reasons[0], /not supported: fixture cannot do this/);
  const html = ui.renderDrillHtml(ns.json);
  assert.ok(html.includes('data-state="unavailable"'));
  assert.equal(html.includes('data-kind="money"'), false);
  const rc = node('render_check.mjs', [ns.out, '--probe', 'probe_fixture']);
  assert.equal(rc.status, 0, rc.stderr);

  const rec = recordFx();
  rec.ledger.complete = false;
  rec.window.open_attempts = [{ seq: 9, caller: 'r1', op: 'PutObject', key: 'k' }];
  const inc = drillWith('incomplete', { record: rec });
  assert.equal(inc.json.cells.insert.state, 'unavailable');
  assert.match(inc.json.cells.insert.unavailable_reasons.join(';'), /incomplete ledger/);
  assert.match(inc.r.stdout, /incomplete ledger/);
});

test('test_two_cells_share_one_bill_and_get_allocated_money', () => {
  // The fixture cell split in two: storage tiers and the block allocation see the aggregate.
  const f = readFx('formulas.json');
  const rows = f.cells.insert.ledger_rows;
  const half = (r) => Object.assign({}, r, { expr: { kind: 'const', value: r.expr.value / 2, dim: r.expr.dim } });
  f.cells = {
    'probe_fixture.inserts': { ledger_rows: [rows[0], half(rows[2]), half(rows[4])], allocation: null, states: [], notes: [] },
    'probe_fixture.recovery': { ledger_rows: [rows[1], half(rows[2]), rows[3], half(rows[4]), rows[5], rows[6]], allocation: null, states: [], notes: [] },
  };
  const { r, json } = drillWith('split', { formulas: f, extra: ['--cell', 'probe_fixture'] });
  assert.equal(r.status, 0, r.stderr);
  const g = json.groups.s3;
  assert.deepEqual(g.cells, ['probe_fixture.inserts', 'probe_fixture.recovery']);
  const storage = g.allocation.find((a) => a.rule_id === 'storage');
  assert.equal(storage.money, 23);
  assert.deepEqual(Object.values(storage.shares).map((x) => x.money), [11.5, 11.5]);
  const cap = g.allocation.find((a) => a.rule_id === 'block_capacity');
  assert.equal(cap.money, 4); // one stripe2 allocation for 50 GiB, not two for 25
  const totals = g.cells.map((k) => json.cells[k].billing_total);
  assert.ok(Math.abs(totals[0] + totals[1] - g.total) < 1e-9);
  const html = ui.renderDrillHtml(json);
  assert.ok(html.includes('Allocation across probe_fixture.inserts, probe_fixture.recovery'));
});

test('test_reconciliation_catches_class_swaps_residuals_and_stochastic_tolerance', () => {
  // Moving successful requests into a free error row keeps the op total but not the outcome split.
  const f = readFx('formulas.json');
  f.cells.insert.ledger_rows[0].expr = { kind: 'const', value: 2, dim: 'count' };
  f.cells.insert.ledger_rows.push(Object.assign({}, f.cells.insert.ledger_rows[0], {
    outcome_class: 'client_error', status: 403, s3_code: 'SignatureDoesNotMatch', expr: { kind: 'const', value: 1, dim: 'count' } }));
  const swap = drillWith('swap', { formulas: f });
  assert.equal(swap.r.status, 1);
  assert.match(swap.r.stderr, /request PutObject ok" predicted 2 measured 3/);
  assert.match(swap.r.stderr, /request PutObject client_error" predicted 1 measured 0/);

  const rec = recordFx();
  rec.ledger.maintenance = { r2: { HeadObject: Object.assign({}, rec.ledger.foreground.r1.GetObject, { attempts: 4, upstream_ok: 4 }) } };
  const res = drillWith('residual', { record: rec });
  assert.equal(res.r.status, 1);
  assert.match(res.r.stderr, /unmodelled residual: r2 HeadObject ok x4/);
  const ex = readFx('formulas.json');
  ex.cells.insert.excluded = [{ kind: 'op', id: 'HeadObject', reason: 'fixture: not priced here' }];
  assert.equal(drillWith('excluded', { record: rec, formulas: ex }).r.status, 0);

  const stoch = recordFx();
  stoch.mode = 'stochastic';
  stoch.ledger.foreground.r1.GetObject.attempts = 13;
  stoch.ledger.foreground.r1.GetObject.upstream_ok = 13;
  const sf = readFx('formulas.json');
  sf.cells.insert.ledger_rows[1].expr = { kind: 'const', value: 13, dim: 'count' };
  const pred = readFx('prediction.json');
  pred.tolerance = { abs: { GetObject: 1 }, rel: {} };
  const out = drillWith('stoch', { record: stoch, formulas: sf, prediction: pred });
  assert.equal(out.r.status, 1);
  assert.match(out.r.stderr, /r1 GetObject measured 13 is outside the prediction 9..11 \+- 1/);
  pred.tolerance = { abs: { GetObject: 2 }, rel: {} };
  assert.equal(drillWith('stoch2', { record: stoch, formulas: sf, prediction: pred }).r.status, 0);
});

test('test_storage_must_match_the_record_and_evidence_is_required', () => {
  const rec = recordFx();
  rec.inventory.byte_hours.metadata = { lower: 1, upper: 2 };
  const r = drillWith('meta', { record: rec });
  assert.equal(r.r.status, 1);
  assert.match(r.r.stderr, /unmodelled residual: storage category metadata/);
  const ok = drillWith('evidence');
  const doctored = JSON.parse(JSON.stringify(ok.json));
  doctored.cells.insert.evidence.limitations = [];
  const dpath = path.join(tmp, 'no-evidence.json');
  fs.writeFileSync(dpath, JSON.stringify(doctored));
  const rc = node('render_check.mjs', [dpath, '--probe', 'probe_fixture']);
  assert.equal(rc.status, 1);
  assert.match(rc.stderr, /evidence missing or empty/);
});

test('test_not_sent_attempts_are_their_own_class', () => {
  // Attempts the store never saw (injected without forwarding) are neither 5XX answers nor billable.
  const rec = recordFx();
  rec.ledger.foreground.r1.PutObject = Object.assign({}, rec.ledger.foreground.r1.PutObject,
    { attempts: 5, upstream_ok: 3, upstream_error: 0, upstream_unknown: 0, upstream_4xx: 0, injected: 2 });
  const f = readFx('formulas.json');
  const notSent = Object.assign({}, f.cells.insert.ledger_rows[0], { outcome_class: 'not_sent', expr: { kind: 'const', value: 2, dim: 'count' } });
  f.cells.insert.ledger_rows.push(notSent);
  const pred = readFx('prediction.json');
  pred.expected.foreground.r1.PutObject = 5;
  const ok = drillWith('notsent', { record: rec, formulas: f, prediction: pred });
  assert.equal(ok.r.status, 0, ok.r.stderr);
  const asServer = JSON.parse(JSON.stringify(f));
  asServer.cells.insert.ledger_rows[asServer.cells.insert.ledger_rows.length - 1].outcome_class = 'server_error';
  const bad = drillWith('notsent-as-5xx', { record: rec, formulas: asServer, prediction: pred });
  assert.equal(bad.r.status, 1);
  assert.match(bad.r.stderr, /request PutObject server_error" predicted 2 measured 0/);
  assert.match(bad.r.stderr, /unmodelled residual: r1 PutObject not_sent x2/);
});

test('test_measured_rows_carry_no_exactness_claim', () => {
  // A row filled from the record's own observation reconciles the ledger with itself: it is shown as measured.
  const f = readFx('formulas.json');
  f.cells.insert.ledger_rows[1].basis = 'measured';
  f.cells.insert.ledger_rows[1].expr = { kind: 'input', id: 'observed_get_requests', dim: 'count' };
  const rec = recordFx();
  rec.observations.inputs = { observed_get_requests: 10 };
  const { r, json, out } = drillWith('measured', { record: rec, formulas: f });
  assert.equal(r.status, 0, r.stderr);
  const rows = json.cells.insert.stage2_rows;
  const get = rows.find((x) => x.op_kind === 'GetObject');
  const put = rows.find((x) => x.op_kind === 'PutObject');
  assert.deepEqual([get.basis, get.measured, get.exact], ['measured', 10, null]);
  assert.deepEqual([put.basis, put.exact], ['predicted', true]);
  assert.match(r.stdout, /GetObject ok \| - \| 10 \| 10 \| .* \| measured/);
  const html = ui.renderDrillHtml(json);
  assert.match(html, /data-basis="measured"[^>]*><td>request GetObject ok/);
  assert.equal(node('render_check.mjs', [out, '--probe', 'probe_fixture']).status, 0);

  // The label must match the expression: a predicted row may not read a measured quantity, and every row has one.
  const lie = JSON.parse(JSON.stringify(f));
  lie.cells.insert.ledger_rows[1].basis = 'predicted';
  const bad = drillWith('measured-as-predicted', { record: rec, formulas: lie });
  assert.equal(bad.r.status, 1);
  assert.match(bad.r.stderr, /basis predicted but reads measured quantity observed_get_requests/);
  const none = JSON.parse(JSON.stringify(f));
  delete none.cells.insert.ledger_rows[0].basis;
  const missing = drillWith('no-basis', { record: rec, formulas: none });
  assert.equal(missing.r.status, 1);
  assert.match(missing.r.stderr, /row "request PutObject ok" has no basis/);
});

test('test_unknown_object_sizes_make_storage_unavailable', () => {
  const rec = recordFx();
  rec.inventory.uncertain_identities = 2;
  const { json, r } = drillWith('uncertain', { record: rec });
  const c = json.cells.insert;
  assert.equal(c.state, 'unavailable');
  assert.ok(c.unavailable_reasons.includes('unknown object sizes (2)'), c.unavailable_reasons.join('; '));
  assert.equal(c.evidence.uncertain_identities, 2);
  assert.match(r.stdout, /unknown object sizes \(2\)/);
  assert.equal(drillWith('certain').json.cells.insert.evidence.uncertain_identities, 0);
});
