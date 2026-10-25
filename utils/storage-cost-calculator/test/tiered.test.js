'use strict';
// Tiered storage: the first hot_tier_days on local disk (ebs), the rest moved by TTL to object storage. Each tier is
// priced by the same pipeline run on its own inputs; the page sums the two totals.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');
const vmod = require('../viewmodel.js');
const ui = require('../ui.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const AWS = presets.resolvePrice(LIVE, 'aws-us-east-1');
const LOGS = presets.SCENARIOS.logs;
const close = (a, b, what) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b)), `${what}: ${a} != ${b}`);

function tieredOf(values, locks, overrides) {
  const r0 = pipeline.resolveInputs(LIVE, values, locks, overrides || {});
  const single = pipeline.run(LIVE, r0.inputs, AWS, {});
  return { r0, single, t: pipeline.tiered(LIVE, r0.inputs, AWS, { single }) };
}
const tieredLogs = (extra) => tieredOf(Object.assign({}, LOGS.values, { hot_tier_days: 7 }, extra || {}), LOGS.locks);

test('test_tiering_rows_in_registry', () => {
  const q = (id) => LIVE.registry.find((x) => x.id === id);
  assert.equal(q('hot_tier_days').ui.tier, 'workload');
  assert.equal(q('hot_tier_days').default, 0);
  for (const id of ['cold_gc_interval_s', 'cold_mutations_per_month', 'cold_select_part_reads_per_day']) assert.equal(q(id).ui.tier, 'advanced', id);
  assert.equal(q('cold_gc_interval_s').default, 3600);
  // Every cold input of `run` comes from a registry row of the tiering block with a stated basis.
  for (const from of [...Object.values(pipeline.HOT_INPUTS), ...Object.values(pipeline.COLD_INPUTS)]) {
    assert.ok(q(from), from);
    assert.equal(q(from).block, 'tiering', from);
    if (q(from).kind === 'derivation') assert.ok(q(from).basis && q(from).evidence.ref, from);
  }
});

test('test_hot_and_cold_split_sums_to_single_tier', () => {
  for (const [values, locks] of [[Object.assign({}, LOGS.values, { hot_tier_days: 7 }), LOGS.locks],
    [Object.assign({}, presets.SCENARIOS.single_replica_archive.values, { hot_tier_days: 30 }), presets.SCENARIOS.single_replica_archive.locks]]) {
    const { t } = tieredOf(values, locks);
    const all = pipeline.evaluationInputs(LIVE, pipeline.resolveInputs(LIVE, values, locks, {}).inputs);
    close(t.hot_inputs.get('data_bytes') + t.cold_inputs.get('data_bytes'), all.get('data_bytes'), 'data');
    close(t.hot_inputs.get('retention_days') + t.cold_inputs.get('retention_days'), all.get('retention_days'), 'retention');
    assert.equal(t.hot_inputs.get('ingest_rate'), all.get('ingest_rate'));
    assert.equal(t.cold_inputs.get('ingest_rate'), all.get('ingest_rate'));
  }
});

test('test_cold_intake_is_hot_final_parts', () => {
  const { t } = tieredLogs();
  const hot = pipeline.evaluationInputs(LIVE, t.hot_inputs);
  const cold = pipeline.evaluationInputs(LIVE, t.cold_inputs);
  // Parts leaving the hot tier per month (at their final size) are the parts the cold tier takes in.
  close(cold.get('parts_inserted_per_month'), hot.get('expired_parts_per_month'), 'parts per month');
  close(cold.get('part_rows'), hot.get('part_rows') * hot.get('merge_size_ratio'), 'rows of a moved part');
  close(cold.get('part_bytes_uncompressed'), hot.get('part_bytes_uncompressed') * hot.get('merge_size_ratio'), 'bytes of a moved part');
  assert.equal(cold.get('parts_per_insert'), 1);
  assert.equal(cold.get('merge_results_per_month'), 0, 'no merges in the cold tier');
  assert.equal(cold.get('async_insert'), '0');
  assert.equal(cold.get('cas_gc_interval_s'), 3600);
  assert.equal(cold.get('select_part_reads_per_day'), 0);
});

test('test_hot_tier_not_below_retention_is_refused', () => {
  for (const days of [30, 45]) {
    const { r0, t } = tieredLogs({ hot_tier_days: days });
    assert.equal(r0.fields.hot_tier_days.refused.text, 'hot tier must be shorter than the retention (30 days)');
    assert.ok(r0.problems.some((p) => /^hot_tier_days: .* is not below the retention of 30 days/.test(p)), r0.problems.join('\n'));
    assert.equal(t, null, 'a refused hot tier prices one tier');
  }
  assert.equal(tieredLogs({ hot_tier_days: 0 }).t, null);
  assert.equal(tieredOf(LOGS.values, LOGS.locks).t, null, 'presets without a hot tier stay single-tier');
  const neg = pipeline.resolveInputs(LIVE, Object.assign({}, LOGS.values, { hot_tier_days: -1 }), LOGS.locks, {});
  assert.ok(neg.problems.some((p) => /^hot_tier_days: expected a number of 0 or more/.test(p)));
  const gc = pipeline.resolveInputs(LIVE, Object.assign({}, LOGS.values, { cold_gc_interval_s: 0 }), LOGS.locks, {});
  assert.ok(gc.problems.some((p) => /^cold_gc_interval_s: expected a number above 0/.test(p)));
});

test('test_tiered_runs_the_pipeline_twice_and_sums_totals', () => {
  const { single, t } = tieredLogs();
  assert.deepEqual(t.cold_variants, ['s3', 's3plain', 'cas']);
  // Each tier is an ordinary run on its own inputs.
  assert.equal(t.hot.variants.ebs.total, pipeline.run(LIVE, t.hot_inputs, AWS, {}).variants.ebs.total);
  for (const r of t.rows) {
    assert.equal(r.state, 'ok', r.variant);
    assert.equal(r.hot_total, t.hot.variants.ebs.total);
    assert.equal(r.cold_total, t.cold.variants[r.variant].total);
    assert.equal(r.total, r.hot_total + r.cold_total);
    assert.equal(r.single_total, single.variants[r.variant].total);
  }
  const min = Math.min(...t.rows.map((r) => r.total));
  assert.equal(t.rows.find((r) => r.variant === t.cheapest).total, min);
  // Logs: 7 days hot on EBS plus 23 days on any object storage costs less than every single tier.
  for (const r of t.rows) assert.ok(r.total < Math.min(...Object.values(single.variants).map((v) => v.total)), r.variant);
});

test('test_cold_cells_carry_the_tiering_assumptions', () => {
  const { t } = tieredLogs();
  const notes = (v, op) => t.cold.variants[v].cells[op].notes.map((n) => n.text);
  for (const v of t.cold_variants) assert.ok(notes(v, 'inserts').includes(pipeline.TTL_MOVE_NOTE), v);
  // Cells that read merge_size_ratio price no cold merges; a cell that does not says so instead of claiming zero.
  for (const v of ['s3', 's3plain']) {
    assert.ok(notes(v, 'merges').includes(pipeline.COLD_MERGE_NOTE), v);
    assert.equal(t.cold.variants[v].cells.merges.money, 0, v);
  }
  assert.ok(notes('cas', 'merges').includes(pipeline.COLD_MERGE_IGNORED_NOTE));
  assert.ok(t.cold.variants.cas.cells.merges.money > 0);
});

test('test_cold_mutations_and_reads_reach_the_cold_cells', () => {
  const base = tieredLogs().t;
  const { t } = tieredLogs({ cold_mutations_per_month: 30, cold_select_part_reads_per_day: 500 });
  const cold = pipeline.evaluationInputs(LIVE, t.cold_inputs);
  assert.equal(cold.get('mutations_per_month'), 30);
  assert.equal(cold.get('mutated_wide_parts_per_month'), 30, 'a moved Logs part is wide');
  assert.equal(cold.get('mutated_compact_parts_per_month'), 0);
  for (const v of ['s3', 'cas']) {
    assert.ok(t.cold.variants[v].cells.mutations.money > base.cold.variants[v].cells.mutations.money, `${v} mutations`);
    assert.ok(t.cold.variants[v].cells.reads.money > base.cold.variants[v].cells.reads.money, `${v} reads`);
  }
  // A plain rewritable disk refuses mutations: the cold tier on it cannot run this workload, and the row says so.
  const plain = t.rows.find((r) => r.variant === 's3plain');
  assert.equal(plain.state, 'not_supported');
  assert.equal(plain.total, null);
  assert.match(plain.reason, /^cold s3plain: s3plain\.mutations: /);
  // The hot tier keeps the workload's own mutations and reads.
  assert.equal(t.hot.variants.ebs.total, base.hot.variants.ebs.total);
});

test('test_logs_tiered_preset', () => {
  const s = presets.SCENARIOS.logs_tiered;
  assert.deepEqual(s.values, Object.assign({}, LOGS.values, { hot_tier_days: 7, cold_gc_interval_s: 3600 }));
  assert.deepEqual(s.locks, LOGS.locks);
  for (const [id, sc] of Object.entries(presets.SCENARIOS)) if (id !== 'logs_tiered') assert.equal(sc.values.hot_tier_days, undefined, id);
});

test('test_tiered_view_model_and_render', () => {
  const s = ui.defaultState(LIVE, 'logs_tiered');
  const out = ui.compute(LIVE, s, {});
  const t = out.vm.tiered;
  assert.equal(t.title, 'Tiered: 7 days on local disk, then 23 days on object storage');
  const raw = pipeline.tiered(LIVE, pipeline.resolveInputs(LIVE, s.inputs, s.locks, {}).inputs, AWS, {});
  const m = (x) => vmod.fmtMoney(x, 'USD');
  for (const r of t.rows) {
    const x = raw.rows.find((y) => y.variant === r.variant);
    assert.equal(r.sum_text, `Hot: local disk (EBS) ${m(x.hot_total)} + Cold: ${r.label} ${m(x.cold_total)} = ${m(x.total)}`);
    assert.ok(r.single_text.startsWith(`single tier ${r.label} `), r.single_text);
  }
  assert.deepEqual(t.rows.filter((r) => r.cheapest).map((r) => r.variant), [raw.cheapest]);
  assert.equal(t.gc_field.id, 'cold_gc_interval_s');
  const parts = ui.renderParts(out.vm, out.page);
  assert.match(parts.cards, /<section class="panel-block tiered" data-block="tiered"/);
  assert.equal((parts.cards.match(/<li class="tier-row/g) || []).length, 3);
  assert.match(parts.cards, /data-field="cold_gc_interval_s"[\s\S]*?data-action="input" data-id="cold_gc_interval_s" value="3600"/);
  assert.match(parts.cards, /data-info-kind="input" data-id="cold_gc_interval_s"/);
  assert.ok(parts.cards.includes(pipeline.TTL_MOVE_NOTE));
  assert.ok(parts.cards.includes(`CAS merges: ${pipeline.COLD_MERGE_IGNORED_NOTE}`));
  // The Tiering row is one line of the workload form.
  assert.match(parts.inputs, /<div class="field field-set" data-row="tiering">/);
  // A single-tier preset shows no tiered block.
  const one = ui.compute(LIVE, ui.defaultState(LIVE, 'logs'), {});
  assert.equal(one.vm.tiered, null);
  assert.doesNotMatch(ui.renderParts(one.vm, one.page).cards, /data-block="tiered"/);
});

test('test_tiered_without_prices_shows_no_money', () => {
  const s = ui.defaultState(LIVE, 'logs_tiered');
  const r0 = pipeline.resolveInputs(LIVE, s.inputs, s.locks, {});
  const preset = Object.assign({}, AWS, { structure_only: true });
  const res = pipeline.run(LIVE, r0.inputs, preset, {});
  res.tiered = pipeline.tiered(LIVE, r0.inputs, preset, { single: res });
  for (const r of res.tiered.rows) assert.equal(r.total, null);
  const vm = vmod.build(res, LIVE, r0.fields, r0.lockOrder, { pricePreset: preset });
  for (const r of vm.tiered.rows) assert.match(r.sum_text, /quantities only, no prices$/);
  assert.equal(vm.tiered.rows.some((r) => r.cheapest), false);
});

test('test_refused_hot_tier_shows_its_reason_on_the_tiering_row', () => {
  const s = ui.defaultState(LIVE, 'logs_tiered');
  s.inputs.hot_tier_days = 30;
  const out = ui.compute(LIVE, s, {});
  assert.equal(out.vm.tiered, null);
  const html = ui.renderParts(out.vm, out.page).inputs;
  assert.match(html, /data-row="tiering">[\s\S]*?data-id="hot_tier_days" value="30"[\s\S]*?<p class="field-note field-error" role="alert">hot tier must be shorter than the retention \(30 days\)<\/p><\/div>/);
  // The single-tier columns still price the workload.
  assert.ok(out.vm.cards.every((c) => c.state === 'ok'));
});

test('test_cold_intake_follows_the_ttl_move_rule', () => {
  const { t } = tieredLogs();
  // No fetch: the follower GETs and the interserver row of the insert cells are gone.
  assert.deepEqual(t.move_dropped_rows, {
    s3: ['request GetObject', 'request GetObject', 'transfer interserver'],
    s3plain: ['request GetObject', 'request GetObject', 'transfer interserver'],
    cas: ['transfer interserver'],
  });
  const cold = pipeline.evaluationInputs(LIVE, t.cold_inputs);
  const moved = cold.get('parts_inserted_per_month');
  const replicas = cold.get('replicas');
  assert.equal(replicas, 2);
  const q = (v) => t.cold.variants[v].cells.inserts.quantities;
  // One part load per moved part on every replica: 9 part files + ttl.txt on s3 and s3plain, a manifest and the
  // first marks blob of a Wide part on CAS.
  assert.equal(cold.get('move_load_gets_per_part'), 10);
  assert.equal(cold.get('cas_move_load_gets_per_part'), 2);
  for (const v of ['s3', 's3plain', 'cas']) {
    const gets = q(v).filter((r) => r.op_kind === 'GetObject');
    close(gets[gets.length - 1].quantity, moved * replicas * (v === 'cas' ? 2 : 10), `${v} load GETs`);
    assert.equal(q(v).some((r) => r.row_type === 'transfer'), false, v);
  }
  // Every replica moves its own copy. On CAS the first replica publishes; a follower finds every blob published and
  // takes the hash-hit branch (HEAD and .meta GET per blob, no blob PUT).
  const leader = pipeline.run(LIVE, t.cold_inputs, AWS, {}).variants.cas.cells.inserts.quantities;
  const hit = pipeline.run(LIVE, new Map([...t.cold_inputs, ['cas_hash_hit_fraction', 1]]), AWS, {}).variants.cas.cells.inserts.quantities;
  assert.equal(cold.get('cas_move_follower_hash_hit_fraction'), 1);
  const sum = (rows, op) => rows.filter((r) => r.op_kind === op).reduce((a, r) => a + r.quantity, 0);
  const blobs = cold.get('cas_blob_uploads_per_part') * moved;
  close(sum(leader, 'PutObject') - sum(hit, 'PutObject'), 2 * blobs, 'a hit saves the blob and its .meta PUT');
  close(sum(hit, 'GetObject') - sum(leader, 'GetObject'), blobs, 'a hit reads the .meta');
  for (const op of ['PutObject', 'HeadObject', 'CreateMultipartUpload', 'UploadPart', 'CompleteMultipartUpload']) {
    close(sum(q('cas'), op), sum(leader, op) + (replicas - 1) * sum(hit, op), `cas ${op}`);
  }
  close(sum(q('cas'), 'GetObject'), sum(leader, 'GetObject') + (replicas - 1) * sum(hit, 'GetObject') + moved * replicas * 2, 'cas GET');
  const notes = t.cold.variants.cas.cells.inserts.notes.map((x) => x.text);
  assert.ok(notes.some((x) => /^a follower replica finds every blob already published/.test(x)));
  assert.ok(notes.some((x) => /^not measured: moves of the same part that overlap on two replicas can both miss/.test(x)));
  // s3 insert PUTs are already per replica.
  close(q('s3').find((r) => r.op_kind === 'PutObject').quantity, pipeline.run(LIVE, t.cold_inputs, AWS, {}).variants.s3.cells.inserts.quantities.find((r) => r.op_kind === 'PutObject').quantity, 's3 PUT');
  // The single-tier columns keep the insert cells.
  assert.equal(pipeline.run(LIVE, t.cold_inputs, AWS, {}).variants.cas.cells.inserts.quantities.some((r) => r.row_type === 'transfer'), true);
});
