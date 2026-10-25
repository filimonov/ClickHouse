'use strict';
// Tier roles of a TTL ... TO VOLUME policy. `cold`: every variant is the second tier, fed by TTL moves of final-size
// parts. `hot`: the ordinary workload kept for the hot days, whose parts then leave by TTL move.
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
const COLD = presets.SCENARIOS.cold_tier;
const HOT = presets.SCENARIOS.hot_tier;
const close = (a, b, what) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b)), `${what}: ${a} != ${b}`);

function runOf(values, locks, preset, opts) {
  const r0 = pipeline.resolveInputs(LIVE, values, locks, {});
  return { r0, r: pipeline.run(LIVE, r0.inputs, preset || AWS, opts || {}) };
}
const coldRun = (extra) => runOf(Object.assign({}, COLD.values, extra || {}), COLD.locks);
// The run inputs of a cold tier priced as an ordinary single-tier workload.
const asSingle = (inputs) => new Map([...inputs, ['tier_role', 'single']]);

test('test_tier_role_rows_in_registry', () => {
  const q = (id) => LIVE.registry.find((x) => x.id === id);
  assert.deepEqual(q('tier_role').enum, ['single', 'hot', 'cold']);
  assert.equal(q('tier_role').default, 'single');
  assert.equal(q('tier_role').ui.tier, 'workload');
  assert.equal(q('hot_tier_days').default, 0);
  for (const id of ['cold_gc_interval_s', 'cold_mutations_per_month', 'cold_select_part_reads_per_day']) assert.equal(q(id).ui.tier, 'advanced', id);
  // Every cold input of `run` comes from a registry row of the tiering block with a stated basis.
  for (const from of Object.values(pipeline.COLD_INPUTS)) {
    assert.ok(q(from), from);
    assert.equal(q(from).block, 'tiering', from);
    if (q(from).kind === 'derivation') assert.ok(q(from).basis && q(from).evidence.ref, from);
  }
  // Presets without a tier role stay single-tier.
  for (const [id, sc] of Object.entries(presets.SCENARIOS)) if (!['hot_tier', 'cold_tier'].includes(id)) assert.equal(sc.values.tier_role, undefined, id);
});

test('test_tier_presets', () => {
  assert.deepEqual(COLD.values, Object.assign({}, LOGS.values, { tier_role: 'cold', hot_tier_days: 7, cold_gc_interval_s: 3600 }));
  assert.deepEqual(HOT.values, Object.assign({}, LOGS.values, { tier_role: 'hot', retention_days: 7 }));
  assert.deepEqual(COLD.locks, LOGS.locks);
  assert.deepEqual(HOT.locks, LOGS.locks);
  assert.equal(COLD.label, 'Cold tier (fed by TTL moves)');
  assert.equal(HOT.label, 'Hot tier (7 days, then TTL to cold)');
});

test('test_cold_tier_prices_all_four_variants', () => {
  const { r } = coldRun();
  assert.deepEqual(r.problems, []);
  for (const v of LIVE.variants) {
    assert.equal(r.variants[v].state, 'ok', v);
    assert.ok(r.variants[v].total > 0, v);
  }
  assert.equal(r.tier.role, 'cold');
  // No fetch: the follower GETs and the interserver row of every inserts cell are gone, EBS included.
  assert.deepEqual(r.tier.dropped, {
    ebs: ['transfer interserver'],
    s3: ['request GetObject', 'request GetObject', 'transfer interserver'],
    s3plain: ['request GetObject', 'request GetObject', 'transfer interserver'],
    cas: ['transfer interserver'],
  });
  // Each column is an ordinary run of the cold contract on the cold inputs.
  const again = pipeline.run(r.tier.contract, asSingle(r.tier.inputs), AWS, {});
  for (const v of LIVE.variants) assert.equal(r.variants[v].total, again.variants[v].total, v);
  const min = Math.min(...LIVE.variants.map((v) => r.variants[v].total));
  assert.equal(r.variants[r.cheapest].total, min);
  // Cold storage on EBS keeps a provisioned gp3 copy per replica: no request cost for the moves.
  assert.equal(r.variants.ebs.cells.inserts.money, 0);
  const notes = r.variants.ebs.cells.inserts.notes.map((n) => n.text);
  assert.ok(notes.includes(pipeline.TTL_MOVE_NOTE));
  assert.ok(notes.includes('part load after a move reads the part files from the local disk: no request price'));
});

test('test_cold_intake_is_logs_final_parts', () => {
  const { r } = coldRun();
  const logs = pipeline.evaluationInputs(LIVE, pipeline.resolveInputs(LIVE, LOGS.values, LOGS.locks, {}).inputs);
  const cold = r.tier.values;
  // The parts the hot tier expires per month, at their final size, are the parts the cold tier takes in.
  close(cold.get('parts_inserted_per_month'), logs.get('expired_parts_per_month'), 'parts per month');
  close(cold.get('parts_inserted_per_month') / pipeline.DAYS_PER_MONTH, 337.5, 'parts per day');
  close(cold.get('part_rows'), logs.get('part_rows') * logs.get('merge_size_ratio'), 'rows of a moved part');
  assert.equal(cold.get('part_rows'), 25600000);
  assert.equal(cold.get('parts_per_insert'), 1);
  assert.equal(cold.get('merge_results_per_month'), 0, 'no merges in the cold tier');
  assert.equal(cold.get('async_insert'), '0');
  assert.equal(cold.get('cas_gc_interval_s'), 3600);
  assert.equal(cold.get('select_part_reads_per_day'), 0);
  assert.equal(cold.get('retention_days'), 23);
  assert.equal(cold.get('data_bytes'), 11500);
  assert.equal(cold.get('ingest_rate'), logs.get('ingest_rate'));
  // No merge cost where the cell reads merge_size_ratio; the CAS cell does not, and says so.
  for (const v of ['ebs', 's3', 's3plain']) assert.equal(r.variants[v].cells.merges.money, 0, v);
  for (const v of ['s3', 's3plain']) assert.ok(r.variants[v].cells.merges.notes.some((n) => n.text === pipeline.COLD_MERGE_NOTE), v);
  assert.ok(r.variants.cas.cells.merges.notes.some((n) => n.text === pipeline.COLD_MERGE_IGNORED_NOTE));
});

test('test_cold_intake_follows_the_ttl_move_rule', () => {
  const { r } = coldRun();
  const cold = r.tier.values;
  const moved = cold.get('parts_inserted_per_month');
  const replicas = cold.get('replicas');
  assert.equal(replicas, 2);
  const q = (v) => r.variants[v].cells.inserts.quantities;
  // One part load per moved part on every replica: 9 part files + ttl.txt on s3 and s3plain, a manifest and the
  // first marks blob of a Wide part on CAS. Local disk has no load request.
  assert.equal(cold.get('move_load_gets_per_part'), 10);
  assert.equal(cold.get('cas_move_load_gets_per_part'), 2);
  for (const v of ['s3', 's3plain', 'cas']) {
    const gets = q(v).filter((x) => x.op_kind === 'GetObject');
    close(gets[gets.length - 1].quantity, moved * replicas * (v === 'cas' ? 2 : 10), `${v} load GETs`);
  }
  for (const v of LIVE.variants) assert.equal(q(v).some((x) => x.row_type === 'transfer'), false, v);
  assert.equal(q('ebs').some((x) => x.row_type === 'request'), false);
  // Every replica moves its own copy. On CAS the first replica publishes; a follower finds every blob published and
  // takes the hash-hit branch (HEAD and .meta GET per blob, no blob PUT).
  const leader = pipeline.run(LIVE, asSingle(r.tier.inputs), AWS, {}).variants.cas.cells.inserts.quantities;
  const hit = pipeline.run(LIVE, asSingle(r.tier.inputs).set('cas_hash_hit_fraction', 1), AWS, {}).variants.cas.cells.inserts.quantities;
  assert.equal(cold.get('cas_move_follower_hash_hit_fraction'), 1);
  const sum = (rows, op) => rows.filter((x) => x.op_kind === op).reduce((a, x) => a + x.quantity, 0);
  const blobs = cold.get('cas_blob_uploads_per_part') * moved;
  close(sum(leader, 'PutObject') - sum(hit, 'PutObject'), 2 * blobs, 'a hit saves the blob and its .meta PUT');
  close(sum(hit, 'GetObject') - sum(leader, 'GetObject'), blobs, 'a hit reads the .meta');
  for (const op of ['PutObject', 'HeadObject', 'CreateMultipartUpload', 'UploadPart', 'CompleteMultipartUpload']) {
    close(sum(q('cas'), op), sum(leader, op) + (replicas - 1) * sum(hit, op), `cas ${op}`);
  }
  close(sum(q('cas'), 'GetObject'), sum(leader, 'GetObject') + (replicas - 1) * sum(hit, 'GetObject') + moved * replicas * 2, 'cas GET');
  const notes = r.variants.cas.cells.inserts.notes.map((x) => x.text);
  assert.ok(notes.some((x) => /^a follower replica finds every blob already published/.test(x)));
  assert.ok(notes.some((x) => /^not measured: moves of the same part that overlap on two replicas can both miss/.test(x)));
  assert.ok(notes.includes('once per table, not priced: the first move into a table costs 3 PUT and 13 GET'));
  // The relink bytes of a fetch describe a dropped row.
  assert.equal(notes.some((x) => /relink/i.test(x)), false);
  // s3 insert PUTs are already per replica; EBS block rows are per volume.
  const single = pipeline.run(LIVE, asSingle(r.tier.inputs), AWS, {}).variants;
  close(sum(q('s3'), 'PutObject'), sum(single.s3.cells.inserts.quantities, 'PutObject'), 's3 PUT');
  close(sum(q('ebs'), 'iops'), sum(single.ebs.cells.inserts.quantities, 'iops'), 'ebs IOPS');
});

test('test_cold_tier_discloses_extrapolation', () => {
  const { r } = coldRun();
  for (const v of ['s3', 's3plain', 'cas']) assert.equal(r.variants[v].extrapolated, true, v);
  const report = presets.coverageReport(LIVE, COLD);
  assert.deepEqual(report.problems, []);
  assert.ok(report.declared.some((x) => /insert_rows = 25600000 is outside/.test(x)), report.declared.join('\n'));
  assert.ok(report.declared.some((x) => /cas_gc_interval_s = 3600 is outside \[10, 60\]/.test(x)));
  // Without the declaration the cold values are outside coverage.
  assert.ok(presets.coverageProblems(LIVE, Object.assign({}, COLD, { expected_extrapolated: {} })).length > 0);
  const out = ui.compute(LIVE, ui.defaultState(LIVE, 'cold_tier'), {});
  const ranges = out.vm.notes.filter((n) => n.kind === 'out_of_range').map((n) => n.text);
  assert.ok(ranges.some((x) => /^insert_rows = 25,600,000 in the cold tier is outside the measured range/.test(x)), ranges.join('\n'));
  assert.ok(ranges.some((x) => /^merge_size_ratio = 1 in the cold tier is outside/.test(x)));
  for (const v of ['s3', 's3plain', 'cas']) assert.ok(out.vm.cards.find((c) => c.variant === v).extrapolated, v);
});

test('test_cold_mutations_and_reads_reach_the_cold_cells', () => {
  const base = coldRun().r;
  const { r } = coldRun({ cold_mutations_per_month: 30, cold_select_part_reads_per_day: 500 });
  assert.equal(r.tier.values.get('mutations_per_month'), 30);
  assert.equal(r.tier.values.get('mutated_wide_parts_per_month'), 30, 'a moved Logs part is wide');
  assert.equal(r.tier.values.get('mutated_compact_parts_per_month'), 0);
  for (const v of ['s3', 'cas']) {
    assert.ok(r.variants[v].cells.mutations.money > base.variants[v].cells.mutations.money, `${v} mutations`);
    assert.ok(r.variants[v].cells.reads.money > base.variants[v].cells.reads.money, `${v} reads`);
  }
  // A plain rewritable disk refuses mutations: its cold column cannot run this workload.
  assert.equal(r.variants.s3plain.state, 'not_supported');
  assert.match(r.variants.s3plain.reason, /^s3plain\.mutations: /);
});

test('test_cold_tier_needs_hot_days_below_the_retention', () => {
  for (const days of [0, 30, 45]) {
    const { r0, r } = coldRun({ hot_tier_days: days });
    for (const v of LIVE.variants) {
      assert.equal(r.variants[v].state, 'unavailable', `${days}: ${v}`);
      assert.equal(r.variants[v].total, null);
      assert.match(r.variants[v].reason, /tier_role cold needs hot_tier_days above 0/);
    }
    assert.equal(r.cheapest, null);
    assert.equal(r0.fields.hot_tier_days.refused.text, days ? 'hot tier must be shorter than the retention (30 days)' : 'the cold tier needs hot tier days above 0');
    assert.equal(r0.inputs.get('tier_role'), 'cold', 'the role is kept, not replaced by a single tier');
  }
  // Outside the cold role the hot days are not read, so nothing is refused.
  const one = pipeline.resolveInputs(LIVE, Object.assign({}, LOGS.values, { hot_tier_days: 45 }), LOGS.locks, {});
  assert.equal(one.fields.hot_tier_days.refused, null);
  const neg = pipeline.resolveInputs(LIVE, Object.assign({}, COLD.values, { hot_tier_days: -1 }), COLD.locks, {});
  assert.ok(neg.problems.some((p) => /^hot_tier_days: expected a number of 0 or more/.test(p)));
  const gc = pipeline.resolveInputs(LIVE, Object.assign({}, COLD.values, { cold_gc_interval_s: 0 }), COLD.locks, {});
  assert.ok(gc.problems.some((p) => /^cold_gc_interval_s: expected a number above 0/.test(p)));
  const role = pipeline.resolveInputs(LIVE, Object.assign({}, COLD.values, { tier_role: 'warm' }), COLD.locks, {});
  assert.ok(role.problems.some((p) => /^tier_role: "warm" is not one of single, hot, cold/.test(p)));
});

test('test_cold_tier_sweep_points_are_cold_runs', () => {
  const { r0, r } = runOf(COLD.values, COLD.locks, AWS, { sweep: { inputId: 'replicas', values: [1, 3] }, lockOrder: pipeline.resolveInputs(LIVE, COLD.values, COLD.locks, {}).lockOrder });
  for (const p of r.sweep.points) {
    const at = pipeline.run(LIVE, new Map(r0.inputs).set('replicas', p.x), AWS, {});
    for (const v of LIVE.variants) assert.equal(p.totals[v].value, at.variants[v].total, `${v} at ${p.x}`);
  }
});

test('test_hot_tier_is_logs_at_7_days_with_a_move_out_note', () => {
  const { r } = runOf(HOT.values, HOT.locks);
  const plain = runOf(Object.assign({}, LOGS.values, { retention_days: 7 }), LOGS.locks).r;
  for (const v of LIVE.variants) {
    assert.equal(r.variants[v].total, plain.variants[v].total, v);
    const notes = r.variants[v].cells.inserts.notes.map((n) => n.text);
    assert.ok(notes.includes(v === 'ebs' ? 'parts leave by TTL move after 7 days: free on local disk'
      : "parts leave by TTL move after 7 days: on object storage the move reads the part's files (not measured, not priced)"), `${v}: ${notes.join('; ')}`);
  }
  assert.equal(r.cheapest, plain.cheapest);
  assert.equal(r.tier.role, 'hot');
});

test('test_tier_presets_render_four_cards', () => {
  for (const [id, line] of [['cold_tier', 'Cold tier: 337.5 final-size parts/day/replica, 25.6 M rows each, TTL-moved; no merges; kept 23 days'],
    ['hot_tier', 'Hot tier: parts leave by TTL move after 7 days']]) {
    const out = ui.compute(LIVE, ui.defaultState(LIVE, id), {});
    assert.deepEqual(out.vm.problems, [], id);
    assert.equal(out.vm.tier_text, line);
    assert.deepEqual(out.vm.cards.map((c) => [c.variant, c.state]), LIVE.variants.map((v) => [v, 'ok']), id);
    const parts = ui.renderParts(out.vm, out.page);
    assert.equal((parts.cards.match(/<article class="card/g) || []).length, 4, id);
    assert.ok(parts.cards.includes(`<p class="tier-line" data-tier="${id.split('_')[0]}">${line}</p>`), id);
    assert.doesNotMatch(parts.cards, /data-block="tiered"/);
    assert.match(parts.inputs, /<div class="field field-set" data-row="tiering">[\s\S]*?data-id="tier_role"/);
    // The (i) of the Inserts cell states the tier's rule.
    for (const v of LIVE.variants) {
      const i = ui.compute(LIVE, ui.defaultState(LIVE, id), { info: { kind: 'cell', variant: v, operation: 'inserts' } }).vm.info;
      const want = id === 'cold_tier' ? pipeline.TTL_MOVE_NOTE : 'parts leave by TTL move after 7 days: ';
      assert.ok(i.lines.some((l) => l.startsWith(want)), `${id} ${v}: ${i.lines.join(' | ')}`);
    }
  }
  const one = ui.compute(LIVE, ui.defaultState(LIVE, 'logs'), {});
  assert.equal(one.vm.tier_text, null);
  assert.doesNotMatch(ui.renderParts(one.vm, one.page).cards, /tier-line/);
});

test('test_cold_tier_without_prices_shows_no_money', () => {
  const preset = Object.assign({}, AWS, { structure_only: true });
  const { r0, r } = runOf(COLD.values, COLD.locks, preset);
  assert.equal(r.page_state, 'prices_missing');
  for (const v of LIVE.variants) assert.equal(r.variants[v].total, null, v);
  assert.equal(r.cheapest, null);
  const vm = vmod.build(r, LIVE, r0.fields, r0.lockOrder, { pricePreset: preset });
  assert.equal(vm.cards.some((c) => c.cheapest), false);
});
