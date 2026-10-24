'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const model = require('../model.js');
const contract = require('../contract.js');
const billing = require('../billing.js');
const pipeline = require('../pipeline.js');

const FIX = path.join(__dirname, 'fixtures');
const MINIMAL = JSON.parse(fs.readFileSync(path.join(FIX, 'contracts', 'minimal.json'), 'utf8'));
const load = (mutate) => {
  const c = JSON.parse(JSON.stringify(MINIMAL));
  if (mutate) mutate(c);
  return contract.load(c);
};
const C = load();
const AWS = 'aws-us-east-1';
const inputs = (over) => new Map([...C.registry.map((q) => [q.id, q.default]), ...Object.entries(over || {})]);
const close = (a, b, msg) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)), `${msg || ''} ${a} != ${b}`);
const run = (over, c, preset, opts) => pipeline.run(c || C, inputs(over), preset || AWS, opts || {});

test('test_not_supported_has_no_total_no_delta_not_cheapest', () => {
  // The fixture's s3plain cannot replace a replica: a non-zero replacement rate makes it not comparable.
  const r = run({ replacement_rate: 0.1 });
  const sp = r.variants.s3plain;
  assert.equal(sp.state, 'not_supported');
  assert.match(sp.reason, /cannot replace a replica/);
  assert.equal(sp.total, null);
  assert.deepEqual(r.deltas.s3plain, { vs_ebs: null, vs_s3: null });
  assert.notEqual(r.cheapest, 's3plain');
  // Without replacements the same column is comparable.
  assert.equal(run().variants.s3plain.state, 'ok');
  // Applicability `not_supported` disables the column whatever the workload, naming the family.
  const c = load((x) => { x.applicability.cas.replacement.lifecycle = 'not_supported'; });
  const rc = run({}, c);
  assert.equal(rc.variants.cas.state, 'not_supported');
  assert.match(rc.variants.cas.reason, /cas\.replacement: not supported for lifecycle/);
  assert.equal(rc.variants.cas.total, null);
  assert.deepEqual(rc.deltas.cas, { vs_ebs: null, vs_s3: null });
  assert.equal(run().cheapest, 'cas');
  assert.notEqual(rc.cheapest, 'cas');
});

test('test_backlog_grows_suppresses_total_deltas_cheapest', () => {
  assert.equal(run().cheapest, 'cas');
  const r = run({ inserts_per_month: 2000000 });
  const cas = r.variants.cas;
  assert.equal(cas.state, 'backlog_grows');
  assert.match(cas.reason, /GC reclaims fewer objects/);
  assert.equal(cas.total, null);
  for (const cell of Object.values(cas.cells)) assert.equal(cell.money, null);
  assert.deepEqual(r.deltas.cas, { vs_ebs: null, vs_s3: null });
  assert.equal(r.cheapest, 'ebs');
  // Quantities stay visible for the column.
  assert.equal(cas.cells.inserts.quantities[0].quantity, 4000000);
});

test('test_delta_null_when_reference_unavailable', () => {
  // A replacement needs the cut ebs card: ebs is not comparable while s3 is.
  const r = run({ replacement_rate: 0.1 });
  assert.equal(r.variants.ebs.state, 'unsupported_cut');
  assert.equal(r.variants.s3.state, 'ok');
  for (const v of C.variants) assert.equal(r.deltas[v].vs_ebs, null, `${v}.vs_ebs`);
  assert.equal(r.deltas.cas.vs_s3.value, r.variants.cas.total - r.variants.s3.total);
  assert.equal(r.deltas.s3.vs_s3.value, 0);
  // The other column itself not ok: its delta against an ok reference is null too.
  assert.equal(r.deltas.s3plain.vs_s3, null);
  assert.equal(r.deltas.ebs.vs_s3, null);
});

test('test_nonzero_override_of_cut_term_makes_column_unavailable', () => {
  // Zero dependent input: the cut term contributes nothing and the column stays ok.
  const zero = run();
  assert.equal(zero.variants.ebs.state, 'ok');
  assert.deepEqual(zero.variants.ebs.cells.replacement.zeroed_cut_terms, [{ refs: ['ebs_replacement_gib'], by: ['replacement_rate'] }]);
  assert.equal(zero.variants.ebs.cells.replacement.quantities[0].quantity, 0);
  close(zero.variants.ebs.total, 80);
  // Overriding the dependent input makes the column unavailable with the reason, never a number.
  const r = run({ replacement_rate: 0.1 });
  const ebs = r.variants.ebs;
  assert.equal(ebs.state, 'unsupported_cut');
  assert.match(ebs.reason, /ebs_replacement_gib is unsupported/);
  assert.match(ebs.reason, /replacement_rate = 0\.1 is non-zero/);
  assert.equal(ebs.total, null);
  for (const cell of Object.values(ebs.cells)) assert.equal(cell.money, null);
  // A registry row marked unsupported is a cut term too: overriding it to non-zero cuts every column that reads it.
  const c = load((x) => { x.registry.find((q) => q.id === 'replacement_rate').status = 'unsupported'; });
  const rc = run({ replacement_rate: 0.1 }, c);
  for (const v of ['s3', 'cas', 'ebs']) {
    assert.equal(rc.variants[v].state, 'unsupported_cut', v);
    assert.match(rc.variants[v].reason, /replacement_rate/);
  }
  assert.equal(run({}, c).variants.s3.state, 'ok');
});

test('test_extrapolated_flag_propagates_to_totals_deltas_and_chart', () => {
  const inside = run({ inserts_per_month: 50000 });
  assert.equal(inside.variants.s3.extrapolated, false);
  // puts_per_insert_s3 was measured for inserts_per_month in [0, 50000].
  const r = run({ inserts_per_month: 60000 }, C, AWS, { sweep: { inputId: 'inserts_per_month', values: [1000, 60000] } });
  const s3 = r.variants.s3;
  assert.equal(s3.state, 'ok');
  assert.equal(s3.cells.inserts.extrapolated, true);
  assert.equal(s3.cells.storage.extrapolated, false);
  assert.equal(s3.extrapolated, true);
  assert.equal(typeof s3.total, 'number');
  assert.equal(r.deltas.s3.vs_ebs.extrapolated, true);
  // The reference column is extrapolated, so every delta against it is.
  assert.equal(r.deltas.cas.vs_s3.extrapolated, true);
  assert.equal(r.deltas.cas.vs_ebs.extrapolated, false);
  assert.equal(r.variants.cas.extrapolated, false);
  const [p1, p2] = r.sweep.points;
  assert.equal(p1.totals.s3.extrapolated, false);
  assert.equal(p2.totals.s3.extrapolated, true);
  assert.equal(typeof p2.totals.s3.value, 'number');
  assert.equal(p2.totals.cas.extrapolated, false);
});

test('test_cheapest_only_among_ok', () => {
  assert.equal(run().cheapest, 'cas');
  // cas is not comparable: the cheapest ok column is marked instead, even though cas's quantities are smaller.
  const noCas = load((x) => { x.applicability.cas.storage.lifecycle = 'not_supported'; });
  assert.equal(run({}, noCas).cheapest, 's3plain');
  // No ok column: nothing is marked.
  const none = load((x) => {
    for (const v of x.variants) x.applicability[v].storage.lifecycle = 'not_supported';
  });
  const r = run({}, none);
  assert.equal(r.cheapest, null);
  for (const v of C.variants) assert.equal(r.variants[v].state, 'not_supported');
  // A structure-only preset has no money, so no column is cheapest.
  assert.equal(run({}, C, 'gcs').cheapest, null);
});

test('test_aggregate_once_then_allocate_back_sums_to_total', () => {
  // 51 000 GiB active + 400 GiB temporary crosses the 51 200 GB first tier only in aggregate.
  const r = run({ data_bytes: 51000, inserts_per_month: 40000 });
  const s3 = r.variants.s3;
  assert.equal(s3.state, 'ok');
  const storage = s3.lines.find((l) => l.rule_id === 'storage');
  close(storage.quantity, 51400);
  assert.equal(storage.tiers.length, 2);
  const cellMoney = Object.values(s3.cells).reduce((s, c) => s + c.money, 0);
  close(cellMoney, s3.total, 'cells sum to the total');
  // Billing each cell on its own would keep all 51 400 GB in the first tier: 200 GB x (0.023 - 0.022) more.
  const alone = Object.values(s3.cells).reduce((s, c) => s + billing.bill(c.rows, MINIMAL.billing.presets[0], 'variant').total, 0);
  close(alone - s3.total, 0.2, 'separate billing minus aggregate');
  const share = s3.cells.storage.lines.find((l) => l.rule_id === 'storage');
  close(share.share, 51000 / 51400);
  close(share.money, storage.money * 51000 / 51400);
  // The EBS column notes its allocation and the instance class that carries its throughput.
  const notes = r.variants.ebs.notes.map((n) => n.text);
  assert.ok(notes.some((t) => /^single: 1 x \(51000 GiB/.test(t)), notes.join(' | '));
  assert.ok(notes.some((t) => /instance class for 125 MiB\/s .*: m6i\.large/.test(t)), notes.join(' | '));
});

test('test_fractional_monthly_event_expands_without_rounding', () => {
  const r = run({ replacement_rate: 0.1 });
  const cell = r.variants.s3.cells.replacement;
  close(cell.quantities[0].quantity, 123.4);
  const line = cell.lines.find((l) => l.rule_id === 'get_and_other');
  close(line.quantity, 123.4);
  close(line.money, 123.4 / 1000 * 0.0004);
});

test('test_sweep_detects_numeric_crossing', () => {
  // x GiB on one gp3 volume (at least 6 GiB for the 3000 IOPS floor): ebs = 0.08 x; cas = 0.023 x + 1.3 for
  // 130 000 inserts x 2 PUTs. They cross at x = 22.8: ebs is cheaper at 20, cas at 30.
  const s = pipeline.sweep(C, inputs({ inserts_per_month: 130000 }), AWS, 'data_bytes', [10, 20, 30, 40]);
  const at = (x) => s.points.find((p) => p.x === x).totals;
  close(at(20).ebs.value, 1.6);
  close(at(20).cas.value, 1.76);
  close(at(30).ebs.value, 2.4);
  close(at(30).cas.value, 1.99);
  const ebsCas = s.breakEven.filter((b) => b.a === 'ebs' && b.b === 'cas');
  assert.deepEqual(ebsCas.map((b) => b.x_between), [[20, 30]]);
  // s3 (1 300 GiB of temporary parts) stays dearer than ebs over the whole range: no crossing for that pair.
  assert.ok(s.points.every((p) => p.totals.s3.value > p.totals.ebs.value));
  assert.equal(s.breakEven.filter((b) => b.a === 'ebs' && b.b === 's3').length, 0);
  // A non-ok point breaks the chain: no crossing is drawn through it.
  const c = load((x) => {
    x.formulas.cas.storage.states.push({ id: 'not_supported', cond: { kind: 'branch', dim: 'count', cond: { op: '==', a: { kind: 'input', id: 'data_bytes', dim: 'GiB' }, b: { kind: 'const', value: 30, dim: 'GiB' } }, then: { kind: 'const', value: 1, dim: 'count' }, else: { kind: 'const', value: 0, dim: 'count' } }, reason: 'fixture gap' });
  });
  const gap = pipeline.sweep(c, inputs({ inserts_per_month: 130000 }), AWS, 'data_bytes', [10, 20, 30, 40]);
  assert.equal(gap.points[2].totals.cas.state, 'not_supported');
  assert.equal(gap.points[2].totals.cas.value, null);
  assert.deepEqual(gap.breakEven.filter((b) => b.a === 'ebs' && b.b === 'cas'), []);
});

test('test_structure_only_preset_sets_prices_missing', () => {
  const r = run({}, C, 'gcs');
  assert.equal(r.page_state, 'prices_missing');
  for (const v of C.variants) {
    assert.equal(r.variants[v].total, null);
    assert.deepEqual(r.deltas[v], { vs_ebs: null, vs_s3: null });
  }
  assert.equal(r.variants.s3.cells.inserts.quantities[0].quantity, 5000);
  assert.equal(r.variants.s3.cells.inserts.money, null);
});

test('test_unknown_preset_cannot_reproduce', () => {
  const r = run({}, C, 'aws-eu-west-1');
  assert.equal(r.page_state, 'cannot_reproduce');
  assert.match(r.page_reason, /aws-eu-west-1.*not present/);
  assert.deepEqual(r.variants, {});
});

test('test_missing_input_is_unavailable_not_zero', () => {
  const ins = inputs();
  ins.delete('inserts_per_month');
  const r = pipeline.run(C, ins, AWS, {});
  assert.equal(r.variants.s3.state, 'unavailable');
  assert.match(r.variants.s3.reason, /input inserts_per_month/);
  assert.equal(r.variants.s3.total, null);
});

test('test_sweep_forwards_outcome_mix', () => {
  // Half of the PUTs are 5XX, which the preset does not bill.
  const opts = { outcome_mix: { PutObject: { ok: 0.5, server_error: 0.5 } } };
  const ins = inputs({ inserts_per_month: 100000 });
  const s = pipeline.sweep(C, ins, AWS, 'inserts_per_month', [100000], opts);
  const direct = pipeline.run(C, ins, AWS, opts).variants.cas.total;
  close(s.points[0].totals.cas.value, direct);
  assert.ok(direct < pipeline.run(C, ins, AWS, {}).variants.cas.total);
});

test('test_sweep_reresolves_precedence_groups', () => {
  // retention_days stays locked; sweeping data_bytes locks it as the newest member, so ingest_rate is derived.
  const lockOrder = ['ingest_rate', 'retention_days'];
  const s = pipeline.sweep(C, inputs(), AWS, 'data_bytes', [500, 2000], { lockOrder });
  for (const p of s.points) {
    const r = pipeline.resolveInputs(C, { data_bytes: p.x, ingest_rate: 40, retention_days: 25 }, ['retention_days', 'data_bytes'], {});
    assert.deepEqual(r.problems, []);
    close(r.inputs.get('ingest_rate'), p.x / 25);
    close(p.totals.s3.value, pipeline.run(C, r.inputs, AWS, {}).variants.s3.total, `x=${p.x}`);
  }
  // The stale ingest_rate (40 GiB/day) would price 2000 GiB differently: replication follows ingest.
  const stale = pipeline.run(C, inputs({ data_bytes: 2000 }), AWS, {}).variants.s3.total;
  assert.ok(Math.abs(stale - s.points[1].totals.s3.value) > 1);
});

test('test_manual_provisioning_priced_and_missing_input_unavailable', () => {
  const manual = { volumes_per_replica: 2, provisioned_size_gib: 600, provisioned_iops: 3000, provisioned_mibps: 125 };
  const r = run(manual);
  const ebs = r.variants.ebs;
  assert.equal(ebs.state, 'ok');
  assert.equal(ebs.provisioning.manual, true);
  // 2 x 600 GiB x 0.08; baseline IOPS and throughput add nothing.
  close(ebs.total, 96);
  assert.ok(ebs.notes.some((n) => n.text === 'manual: 2 x (600 GiB, 3000 IOPS, 125 MiB/s)'));
  assert.ok(ebs.notes.some((n) => /instance class for 250 MiB\/s/.test(n.text)));
  const ins = inputs(manual);
  ins.delete('provisioned_size_gib');
  const m = pipeline.run(C, ins, AWS, {});
  assert.equal(m.variants.ebs.state, 'unavailable');
  assert.match(m.variants.ebs.reason, /volumes_per_replica = 2 needs provisioned_size_gib/);
  assert.equal(m.variants.ebs.total, null);
  // Columns without block rows are priced as usual.
  assert.equal(m.variants.s3.state, 'ok');
});

test('test_applicability_unsupported_cut_with_nonzero_workload', () => {
  assert.equal(run().variants.cas.state, 'ok');
  const r = run({ mutations_per_month: 5 });
  assert.equal(r.variants.cas.state, 'unsupported_cut');
  assert.match(r.variants.cas.reason, /cas\.mutations: cut for lifecycle and the workload needs request PutObject/);
  assert.equal(r.variants.cas.total, null);
});

test('test_declared_shares_apply_only_to_lines_the_sharing_cell_produced', () => {
  // s3plain: storage declares a 3:1 split with inserts; inserts declares nothing.
  const sp = run().variants.s3plain;
  assert.equal(sp.state, 'ok');
  const storageLine = sp.allocation.find((a) => a.rule_id === 'storage');
  assert.equal(storageLine.basis, 'allocation.shares');
  close(storageLine.shares.storage.fraction, 0.75);
  close(storageLine.shares.inserts.fraction, 0.25);
  // The PUTs come from the inserts cell only, so they stay there.
  const put = sp.allocation.find((a) => a.rule_id === 'put_copy_post_list');
  assert.equal(put.basis, 'quantity');
  close(put.shares.inserts.fraction, 1);
  assert.equal(put.shares.storage.fraction, 0);
  assert.equal(sp.cells.storage.lines.find((l) => l.rule_id === 'put_copy_post_list'), undefined);
  close(sp.cells.storage.money + sp.cells.inserts.money, sp.total);
  close(sp.cells.storage.money, storageLine.money * 0.75);
});

test('test_cut_reason_names_only_nonzero_inputs', () => {
  const c = load((x) => {
    const r = x.formulas.ebs.replacement.ledger_rows[0];
    r.expr.a = { kind: 'add', dim: 'count', a: { kind: 'input', id: 'replacement_rate', dim: 'count' }, b: { kind: 'input', id: 'mutations_per_month', dim: 'count' } };
  });
  const r = run({ replacement_rate: 0.1 }, c);
  assert.equal(r.variants.ebs.state, 'unsupported_cut');
  assert.match(r.variants.ebs.reason, /replacement_rate = 0\.1 is non-zero/);
  assert.doesNotMatch(r.variants.ebs.reason, /mutations_per_month/);
});

test('test_cut_lookup_table_zero_and_nonzero', () => {
  const zero = run();
  assert.equal(zero.variants.ebs.state, 'ok');
  assert.deepEqual(zero.variants.ebs.cells.mutations.zeroed_cut_terms, [{ refs: ['ebs_mutation_iops'], by: ['mutations_per_month'] }]);
  const r = run({ mutations_per_month: 5 });
  assert.equal(r.variants.ebs.state, 'unsupported_cut');
  assert.match(r.variants.ebs.reason, /ebs_mutation_iops is unsupported .*mutations_per_month = 5 is non-zero/);
});

test('test_infeasible_allocation_is_unavailable', () => {
  // Four 65 536 GiB gp3 volumes hold at most 262 144 GiB.
  const r = run({ data_bytes: 300000 });
  assert.equal(r.variants.ebs.state, 'unavailable');
  assert.match(r.variants.ebs.reason, /no feasible volume allocation/);
  assert.equal(r.variants.ebs.total, null);
  assert.equal(r.variants.s3.state, 'ok');
  assert.notEqual(r.cheapest, 'ebs');
  assert.equal(r.deltas.s3.vs_ebs, null);
});

// ---- measured fixtures (tools/build_fixtures.py) --------------------------------------------------------------

const MEASURED = process.env.SCC_MEASURED_FIXTURES || path.join(FIX, 'measured');
const COEF_ONLY = '_coefficient_only.json';
const REGISTRY = path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'inputs-registry.json');

// Evaluates the fixture's cells for its inputs; returns predicted request attempts per "caller|op" ("*" = any caller
// without a row of its own) and the problems found.
function predictedAttempts(fx) {
  const reg = JSON.parse(fs.readFileSync(REGISTRY, 'utf8')).quantities;
  const ins = new Map(reg.filter((q) => q.kind !== 'derivation' && q.default !== null && q.default !== undefined).map((q) => [q.id, q.default]));
  for (const [k, v] of Object.entries(fx.inputs)) ins.set(k, v);
  const coefs = new Map(fx.coefficients.filter((c) => c.kind !== 'table' && c.value !== null).map((c) => [c.id, { value: c.value, validity: c.validity || null }]));
  const tables = new Map(fx.coefficients.filter((c) => c.kind === 'table').map((c) => [c.id, c.value]));
  const ctx = { inputs: ins, coefs, tables, sets: new Map(), vars: new Map(), prices: null };
  const problems = [];
  const groups = new Map();
  for (const [name, cell] of Object.entries(fx.cells)) {
    const ev = model.evaluateCell(cell, ctx);
    ev.rows.forEach((r) => {
      if (r.row_type !== 'request' || r.basis !== 'predicted') return;
      if (r.state !== 'ok') { problems.push(`${name}: ${r.op_kind} row unavailable`); return; }
      const k = `${r.caller || '*'}|${r.op_kind}`;
      groups.set(k, (groups.get(k) || 0) + r.quantity);
    });
  }
  const excluded = new Set(Object.values(fx.cells).flatMap((c) => (c.excluded || []).filter((e) => e.kind === 'op').map((e) => e.id)));
  return { groups, problems, excluded };
}

function compareFixture(fx) {
  const { groups, problems, excluded } = predictedAttempts(fx);
  const measured = new Map(fx.measured_rows.map((r) => [`${r.caller}|${r.op_kind}`, r.quantity]));
  const explicit = (op) => new Set([...groups.keys()].filter((k) => k.endsWith(`|${op}`) && !k.startsWith('*|')).map((k) => k.split('|')[0]));
  for (const [k, predicted] of groups) {
    const [caller, op] = k.split('|');
    const callers = caller === '*' ? fx.measured_rows.filter((r) => r.op_kind === op && !explicit(op).has(r.caller)).map((r) => r.caller) : [caller];
    const got = callers.reduce((s, c) => s + (measured.get(`${c}|${op}`) || 0), 0);
    const slack = ((fx.tolerance.abs || {})[op] || 0) + ((fx.tolerance.rel || {})[op] || 0) * Math.abs(predicted);
    if (Math.abs(got - predicted) > slack) problems.push(`${caller} ${op}: measured ${got}, model ${predicted}` + (slack ? ` within ${slack}` : ' exactly'));
  }
  for (const r of fx.measured_rows) {
    if (r.quantity === 0 || excluded.has(r.op_kind)) continue;
    if (!groups.has(`${r.caller}|${r.op_kind}`) && !(groups.has(`*|${r.op_kind}`) && !explicit(r.op_kind).has(r.caller))) {
      problems.push(`${r.caller} ${r.op_kind}: measured ${r.quantity}, no model row`);
    }
  }
  return problems;
}

test('test_measured_fixtures_within_tolerance', (t) => {
  const files = fs.existsSync(MEASURED) ? fs.readdirSync(MEASURED).filter((f) => f.endsWith('.json')).sort() : [];
  if (!files.length) {
    const msg = `no measured fixtures in ${MEASURED}: run tools/build_fixtures.py (generate_contract.py generate does); SCC_DEV=1 skips this check`;
    if (process.env.SCC_DEV === '1') {
      t.skip(msg);
      return;
    }
    assert.fail(msg);
  }
  const read = (f) => JSON.parse(fs.readFileSync(path.join(MEASURED, f), 'utf8'));
  assert.ok(files.includes(COEF_ONLY), `${MEASURED} has fixtures but no ${COEF_ONLY}: rebuild it with tools/build_fixtures.py`);
  const fixtures = files.filter((f) => f !== COEF_ONLY).map((f) => [f, read(f)]);
  const failures = [];
  for (const [f, fx] of fixtures) for (const p of compareFixture(fx)) failures.push(`${f} (${fx.measurement_id}): ${p}`);
  assert.deepEqual(failures, []);
  // Every done fit or hold-out edge has a fixture, or is coefficient-only: its report has no cells and its
  // prediction no request rows.
  const only = read(COEF_ONLY);
  const coverage = JSON.parse(fs.readFileSync(only.coverage, 'utf8'));
  const withCells = new Set(fixtures.map(([, fx]) => fx.edge_id));
  const without = coverage.evidence.map((e, i) => [`evidence[${i}]`, e])
    .filter(([id, e]) => e.status === 'done' && ['fit', 'holdout'].includes(e.role) && !withCells.has(id)).map(([id]) => id);
  assert.deepEqual(only.edges.map((e) => e.edge_id).sort(), without.sort());
});
