'use strict';
// Tables on the CAS disk are workload: their count, creations and drops drive per-table GC, cleanup and DDL requests.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');
const urlstate = require('../urlstate.js');
const ui = require('../ui.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const TABLE_INPUTS = ['cas_tables', 'cas_table_creates_per_month', 'cas_table_drops_per_month'];
const reg = (id) => LIVE.registry.find((q) => q.id === id);
const AWS = presets.resolvePrice(LIVE, 'aws-us-east-1');
const run = (sc, values) => {
  const s = presets.SCENARIOS[sc];
  const r = pipeline.resolveInputs(LIVE, Object.assign({}, s.values, values || {}), s.locks, {});
  assert.deepEqual(r.problems, []);
  return pipeline.run(LIVE, r.inputs, AWS, {});
};
const rowsOf = (res, op) => res.variants.cas.cells[op].rows;
const qty = (rows, opKind, outcome) => rows.filter((r) => r.op_kind === opKind && r.outcome_class === (outcome || 'ok')).reduce((s, r) => s + r.quantity, 0);
const page = (sc, inputs, uiState) => {
  const s = ui.defaultState(LIVE, sc);
  Object.assign(s.inputs, inputs || {});
  const out = ui.compute(LIVE, s, uiState || {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};

test('test_table_inputs_are_workload_and_settings_stay_at_most_ten', () => {
  for (const id of TABLE_INPUTS) {
    const q = reg(id);
    assert.ok(q, id);
    assert.equal(q.kind, 'input', id);
    assert.equal(q.ui.tier, 'workload', id);
    assert.equal(q.unit, 'count', id);
    assert.ok(q.ui.short.length <= 40, id);
    assert.ok(q.hint && /system\.(tables|query_log)/.test(q.hint.sql), `${id}: SQL to fill it from a cluster`);
  }
  assert.equal(reg('cas_tables').ui.short, 'Tables on the disk');
  assert.equal(reg('cas_table_drops_per_month').ui.short, 'Tables dropped per month');
  assert.equal(reg('cas_table_creates_per_month').ui.short, 'Tables created per month');
  // Per-second DROP rate is a hidden derivation of the per-month input, as restarts are entered per month.
  const s = reg('cas_table_drops_per_s');
  assert.equal(s.kind, 'derivation');
  assert.equal(s.ui.tier, 'hidden');
  const r = pipeline.resolveInputs(LIVE, { cas_table_drops_per_month: 4 }, [], {});
  assert.equal(pipeline.evaluationInputs(LIVE, r.inputs).get('cas_table_drops_per_s'), 4 / 2628000);
  assert.equal(reg('cas_parts_per_dropped_table').ui.tier, 'advanced');
  assert.equal(reg('cas_table_cleanup_period_s').ui.tier, 'hidden');
  assert.ok(LIVE.registry.filter((q) => q.kind === 'input' && q.ui && q.ui.tier === 'setting').length <= 10);
});

test('test_presets_carry_table_counts', () => {
  const want = {
    logs: [10, 0, 0], logs_tiered: [10, 0, 0], metrics: [20, 4, 4], analytics_batch: [5, 0, 0], single_replica_archive: [3, 0, 0], custom: [10, 0, 0],
  };
  assert.deepEqual(Object.keys(want), Object.keys(presets.SCENARIOS));
  for (const [id, [n, c, d]] of Object.entries(want)) {
    const v = presets.SCENARIOS[id].values;
    assert.deepEqual([v.cas_tables, v.cas_table_creates_per_month, v.cas_table_drops_per_month], [n, c, d], id);
    // The start terms hold to 500 tables; the cached ones are stated assumptions, not extrapolations.
    assert.deepEqual(((presets.SCENARIOS[id].expected_extrapolated || {}).cas || []).filter((x) => /^cas_startup/.test(x)), [], id);
  }
});

test('test_per_table_cas_requests_scale_with_the_table_count', () => {
  const one = run('logs', { cas_tables: 1 });
  const ten = run('logs', { cas_tables: 10 });
  // The cleanup thread runs on each table every cleanup period: 5 GETs and a LIST per table and replica.
  for (const op of ['GetObject', 'ListObjectsV2']) {
    const a = qty(rowsOf(one, 'lease'), op);
    const b = qty(rowsOf(ten, 'lease'), op);
    assert.ok(a > 0, op);
    assert.ok(Math.abs(b - 10 * a) < 1e-6 * b, `${op}: ${a} -> ${b}`);
  }
  // A restart reads each table's catalog row, checkpoint and files and replays its ref-log tail: per start, 9 more
  // tables cost 9 x (tail GETs + the measured per-table GETs), with and without the cache.
  const tail = pipeline.evaluationInputs(LIVE, pipeline.resolveInputs(LIVE, presets.SCENARIOS.logs.values, presets.SCENARIOS.logs.locks, {}).inputs);
  const coef = (id) => LIVE.coefficients.find((c) => c.id === id).value;
  const starts = tail.get('restarts_per_month') * tail.get('replicas');
  for (const cache of ['0', '1']) {
    const perTable = tail.get('cas_ref_log_tail_records') * coef('cas_startup_gets_per_log_record') + coef('cas_startup_requests_per_table').values[`${cache}|GetObject`];
    const a = run('logs', { cas_tables: 1, cache_enabled: cache });
    const b = run('logs', { cas_tables: 10, cache_enabled: cache });
    assert.ok(Math.abs(qty(rowsOf(b, 'recovery'), 'GetObject') - qty(rowsOf(a, 'recovery'), 'GetObject') - 9 * starts * perTable) < 1e-6, cache);
    const e404 = qty(rowsOf(b, 'recovery'), 'GetObject', 'client_error') - qty(rowsOf(a, 'recovery'), 'GetObject', 'client_error');
    assert.ok(Math.abs(e404 - 9 * starts * coef('cas_startup_error_requests_per_table').values[`${cache}|GetObject`]) < 1e-6, cache);
  }
  assert.ok(qty(rowsOf(ten, 'gc'), 'GetObject') > qty(rowsOf(one, 'gc'), 'GetObject'));
  assert.ok(ten.variants.cas.total > one.variants.cas.total);
});

test('test_create_table_requests_reproduce_the_source_count', () => {
  const d = reg('cas_table_create_requests');
  assert.equal(d.kind, 'derivation');
  assert.equal(d.basis, 'derived');
  assert.equal(d.evidence.kind, 'source');
  const r = pipeline.resolveInputs(LIVE, {}, [], {});
  // 7 catalog GETs, 2 catalog PUTs, 3 _ckpt GETs (1 absent) and 1 PUT, 1 absent _log GET, format_version.txt HEAD
  // (absent) and PUT, 5 LISTs of the table files: 21 requests.
  assert.equal(pipeline.evaluationInputs(LIVE, r.inputs).get('cas_table_create_requests'), 21);
  const base = run('logs', { cas_table_creates_per_month: 0, cas_table_drops_per_month: 0, replicas: 1 });
  const one = run('logs', { cas_table_creates_per_month: 1, cas_table_drops_per_month: 0, replicas: 1 });
  const diff = (op, oc) => qty(rowsOf(one, 'deletes'), op, oc) - qty(rowsOf(base, 'deletes'), op, oc);
  assert.deepEqual([diff('GetObject'), diff('GetObject', 'client_error'), diff('HeadObject', 'client_error'), diff('PutObject'), diff('ListObjectsV2')],
    [9, 2, 1, 4, 5]);
  // Each replica holds its own namespace of the table, so the statement runs once per replica.
  const two = run('logs', { cas_table_creates_per_month: 1, cas_table_drops_per_month: 1, replicas: 2 });
  const twoBase = run('logs', { cas_table_creates_per_month: 0, cas_table_drops_per_month: 0, replicas: 2 });
  const all = (res) => rowsOf(res, 'deletes').reduce((s, x) => s + x.quantity, 0);
  // DROP TABLE SYNC of a one-part table: PUT 5, GET 4, HEAD 1, DELETE 1, LIST 2 = 13 per replica.
  assert.equal(all(two) - all(twoBase), 2 * (21 + 13));
});

test('test_table_inputs_survive_the_url', () => {
  const s = ui.defaultState(LIVE, 'metrics');
  Object.assign(s.inputs, { cas_tables: 37, cas_table_creates_per_month: 6, cas_table_drops_per_month: 5 });
  const r = urlstate.decode(urlstate.encode(Object.assign({}, s, { manifest: LIVE.manifest })), LIVE);
  assert.equal(r.problem, null);
  for (const id of TABLE_INPUTS) assert.equal(r.state.inputs[id], s.inputs[id], id);
});

test('test_tables_render_as_one_compact_workload_row', () => {
  const { parts } = page('logs');
  const html = parts.inputs;
  const head = html.slice(0, html.indexOf('<section class="panel-block settings"'));
  const at = head.indexOf('data-row="tables"');
  assert.ok(at >= 0, 'one tables row in the workload block');
  assert.equal(head.split('data-row="tables"').length, 2);
  const rowHtml = head.slice(head.lastIndexOf('<div', at), head.indexOf('</div></div>', at));
  for (const id of TABLE_INPUTS) assert.match(rowHtml, new RegExp(`data-action="input" data-id="${id}"`), id);
  assert.equal((rowHtml.match(/data-info-kind="input"/g) || []).length, 1, 'one (i) for the row');
  assert.match(rowHtml.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' '), /Tables .*created\/month .*dropped\/month/);
  for (const id of TABLE_INPUTS) assert.equal(html.split(`data-field="${id}"`).length, 1, `${id} has no field of its own`);
  assert.equal(html.includes('data-field="cas_parts_per_dropped_table"'), true, 'parts per dropped table stays low-level');
});

test('test_tables_modal_explains_how_tables_enter_the_cas_cost', () => {
  const info = page('logs', {}, { info: { kind: 'input', id: 'cas_tables' } }).vm.info;
  const all = [info.description, ...info.lines, info.range_text].join(' ');
  assert.match(all, /cleanup/i);
  assert.match(all, /ref log/i);
  assert.match(all, /DROP TABLE SYNC/);
  assert.match(all, /CREATE TABLE/);
  assert.match(all, /parts per dropped table/i);
  assert.equal(info.range_text, 'fitted at 1–500 tables and held out; listing and DDL from source, verified at 1 table');
  for (const id of TABLE_INPUTS) {
    const i = page('logs', {}, { info: { kind: 'input', id } }).vm.info;
    assert.ok(i.description && i.description.split(/\.\s/).length === 1, `${id}: one sentence, got ${i.description}`);
    assert.doesNotMatch(i.range_text, /extrapolated/);
  }
});

test('test_cached_start_per_table_terms_are_stated_assumptions', () => {
  // With the cache the start was measured at one table; its per-table terms repeat the cache-off fit as stated
  // assumptions (the coefficient's assumed keys), so more tables extrapolate nothing and the Recovery cell says so.
  for (const sc of Object.keys(presets.SCENARIOS)) {
    for (const cache of ['0', '1']) {
      const res = page(sc, { cache_enabled: cache });
      const flagged = res.vm.table.rows.filter((r) => (r.cells.find((c) => c.variant === 'cas') || {}).extrapolated).map((r) => r.operation);
      assert.deepEqual(flagged, [], `${sc} cache ${cache}`);
    }
    const cell = run(sc, { cache_enabled: '1' }).variants.cas.cells.recovery;
    assert.ok(cell.notes.some((n) => /^Start with the filesystem cache: ASSUMPTION/.test(n.text)), sc);
  }
});

test('test_gc_fold_follows_the_capped_sweep_law', () => {
  // Per fold of a busy pool: per table with new data `fold_get_per_changed_table` GETs, and the orphan-manifest sweep
  // reads the tail plus `fold_get_per_swept_table` objects of at most `sweep_namespace_budget` tables.
  const v = LIVE.coefficients.find((c) => c.id === 'gc_round_requests_steady').value.values;
  const tail = pipeline.evaluationInputs(LIVE, pipeline.resolveInputs(LIVE, presets.SCENARIOS.logs.values, presets.SCENARIOS.logs.locks, {}).inputs).get('cas_ref_log_tail_records');
  const swept = tail * v.fold_get_per_swept_tail_log_record + v.fold_get_per_swept_table;
  const gets = (n) => qty(rowsOf(run('logs', { cas_tables: n }), 'gc'), 'GetObject');
  const d = (n) => gets(n) - gets(1);
  const B = v.sweep_namespace_budget;
  const close = (a, b, msg) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.abs(b), `${msg}: ${a} vs ${b}`);
  // Below the budget every table costs both terms: d(10) / d(5) = 9 / 4.
  close(d(10) / d(5), 9 / 4, 'linear below the budget');
  // Above it only the changed-table term grows.
  const perRound = d(B) / ((B - 1) * (v.fold_get_per_changed_table + swept));
  close(d(B + 10) - d(B), perRound * 10 * v.fold_get_per_changed_table, 'capped above the budget');
  // A fold prunes an old generation: DELETE 2 per fold round, priced as a DeleteObject row.
  assert.ok(rowsOf(run('logs', {}), 'gc').some((r) => r.op_kind === 'DeleteObject' && r.quantity > 0));
});

test('test_table_fit_text_says_when_a_hold_out_is_pending', () => {
  // The page states a pending hold-out from the coefficient's own record, not from a fixed sentence.
  const raw = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
  delete raw.coefficients.find((c) => c.id === 'gc_round_requests_steady').fit.holdout;
  const s = ui.defaultState(LIVE, 'logs');
  const info = ui.compute(contract.load(raw), s, { info: { kind: 'input', id: 'cas_tables' } }).vm.info;
  assert.equal(info.range_text, 'fitted at 1–500 tables, hold-out pending; listing and DDL from source, verified at 1 table');
});

test('test_gc_changed_tables_cap_at_inserts_per_round', () => {
  // A table changes within a GC round only when an insert lands in it: at most one new table per insert of the round.
  const v = LIVE.coefficients.find((c) => c.id === 'gc_round_requests_steady').value.values;
  const s = presets.SCENARIOS.analytics_batch;
  const ev = pipeline.evaluationInputs(LIVE, pipeline.resolveInputs(LIVE, s.values, s.locks, {}).inputs);
  const k = ev.get('insert_rate') * ev.get('cas_gc_interval_s');
  assert.equal(k, 3, 'analytics_batch: 3 inserts per GC round');
  const swept = ev.get('cas_ref_log_tail_records') * v.fold_get_per_swept_tail_log_record + v.fold_get_per_swept_table;
  const gets = (n) => qty(rowsOf(run('analytics_batch', { cas_tables: n }), 'gc'), 'GetObject');
  // From 1 to 3 tables both the changed-table and the sweep terms grow; from 3 to 5 only the sweep does.
  const ratio = (gets(5) - gets(3)) / (gets(3) - gets(1));
  const want = swept / (v.fold_get_per_changed_table + swept);
  assert.ok(Math.abs(ratio - want) <= 1e-9, `${ratio} vs ${want}`);
});

test('test_cleanup_reads_five_gets_per_table_with_parts', () => {
  // Each cleanup run of a table with parts reads about 5 GETs besides its one LIST (measured, not fitted).
  const q = reg('cas_cleanup_gets_per_table');
  assert.equal(q.kind, 'pinned');
  assert.equal(q.default, 5);
  assert.equal(q.evidence.kind, 'assumed');
  const rows = rowsOf(run('logs', { cas_tables: 10 }), 'lease');
  const list = qty(rows, 'ListObjectsV2');
  assert.ok(list > 0);
  assert.ok(Math.abs(qty(rows, 'GetObject') - 5 * list) <= 1e-9 * list);
});
