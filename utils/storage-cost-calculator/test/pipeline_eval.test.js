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

test('test_not_applicable_family_contributes_nothing', () => {
  const base = run();
  // A family that does not occur on the variant leaves the column as it is.
  const c = load((x) => { x.applicability.cas.replacement.lifecycle = 'not_applicable'; });
  const r = run({}, c);
  assert.equal(r.variants.cas.state, 'ok');
  close(r.variants.cas.total, base.variants.cas.total);
  // An operation with no cell needs one only when a family of it is supported.
  const none = (value) => load((x) => {
    delete x.formulas.cas.replacement;
    for (const f of x.families) x.applicability.cas.replacement[f] = value;
  });
  assert.equal(run({}, none('not_applicable')).variants.cas.state, 'ok');
  const missing = run({}, none('supported')).variants.cas;
  assert.equal(missing.state, 'unavailable');
  assert.match(missing.reason, /cas\.replacement: the contract has no cell for a supported operation/);
  // A variant where no family occurs on any operation has nothing to price: unavailable, never a total of 0.
  const empty = run({}, load((x) => {
    for (const op of x.operations) for (const f of x.families) x.applicability.cas[op][f] = 'not_applicable';
  })).variants.cas;
  assert.equal(empty.state, 'unavailable');
  assert.equal(empty.total, null);
  assert.match(empty.reason, /cas: no operation has a supported family/);
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
  assert.ok(notes.some((t) => /^single: 1 replica × \(1 x 51000 GiB/.test(t)), notes.join(' | '));
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
  // An all-outcomes PUT row; half of the PUTs are 5XX, which the preset does not bill.
  const A = load((x) => { x.formulas.cas.inserts.ledger_rows[0].outcome_class = 'all'; });
  const opts = { outcome_mix: { PutObject: { ok: 0.5, server_error: 0.5 } } };
  const ins = inputs({ inserts_per_month: 100000 });
  const s = pipeline.sweep(A, ins, AWS, 'inserts_per_month', [100000], opts);
  const direct = pipeline.run(A, ins, AWS, opts).variants.cas.total;
  close(s.points[0].totals.cas.value, direct);
  assert.ok(direct < pipeline.run(A, ins, AWS, { outcome_mix: { PutObject: { ok: 1 } } }).variants.cas.total);
  // Without a mix the column cannot say what is billed: unavailable, never "all ok".
  const none = pipeline.run(A, ins, AWS, {}).variants.cas;
  assert.equal(none.state, 'unavailable');
  assert.match(none.reason, /an all-outcomes row needs an outcome mix for PutObject/);
});

test('test_inputs_reject_negative_and_fractional_counts', () => {
  for (const [id, v, re] of [['replicas', 0, /replicas: expected a whole number of 1 or more, got 0/], ['replicas', -1, /got -1/], ['replicas', 1.5, /got 1\.5/],
    ['inserts_per_month', -5, /inserts_per_month: expected a number of 0 or more, got -5/], ['replicas', NaN, /expected a finite number/]]) {
    const r = pipeline.resolveInputs(C, { [id]: v }, [], {});
    assert.match(r.problems.join('\n'), re, `${id}=${v}`);
    assert.equal(r.inputs.has(id), false, `${id}=${v} leaves no value`);
  }
  assert.match(pipeline.resolveInputs(C, { ingest_rate: { value: -1, per: 'hour' } }, ['ingest_rate', 'retention_days'], {}).problems.join('\n'), /ingest_rate: expected a rate of 0 or more/);
});

test('test_registry_range_marks_dependent_totals_extrapolated', () => {
  const R = load((x) => { x.registry.find((q) => q.id === 'replicas').validity = { replicas: [1, 6] }; });
  const at = (n) => pipeline.run(R, inputs({ replicas: n }), AWS, {});
  const out = at(7);
  const flagged = Object.values(out.variants).filter((v) => v.state === 'ok' && v.extrapolated);
  assert.ok(flagged.length > 0, 'a column that reads replicas is extrapolated at 7');
  assert.ok(Object.values(at(3).variants).every((v) => !v.extrapolated));
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
  assert.ok(ebs.notes.some((n) => n.text === 'manual: 1 replica × (2 x 600 GiB, 3000 IOPS, 125 MiB/s)'));
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
  // A record names its table by a preset (shape = narrow); the preset's quantities fill what the prediction omits.
  for (const q of reg) {
    const preset = q.presets && q.presets[fx.inputs[q.id]];
    for (const [k, v] of Object.entries(preset || {})) if (!(k in fx.inputs)) ins.set(k, v);
  }
  const coefs = new Map(fx.coefficients.filter((c) => c.kind !== 'table' && c.value !== null).map((c) => [c.id, { value: c.value, validity: c.verified_range || c.validity || null }]));
  const tables = new Map(fx.coefficients.filter((c) => c.kind === 'table').map((c) => [c.id, c.value]));
  const ctx = { inputs: ins, coefs, tables, sets: new Map(), vars: new Map(), prices: null };
  // Registry derivations the prediction does not state come from their rules, as in pipeline.buildCtx.
  const pending = reg.filter((q) => q.kind === 'derivation' && q.rule && !ins.has(q.id));
  for (let changed = true; changed;) {
    changed = false;
    for (const q of pending) {
      if (ins.has(q.id)) continue;
      const r = model.evaluate(q.rule, ctx);
      if (r.state === 'ok') { ins.set(q.id, r.value); changed = true; }
    }
  }
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

// Zero-tolerance comparisons still allow 1e-9 relative: the model sums JS doubles over contract nodes
// (11.999999999999998 for 12), while measured counts are integers.
const EXACT_REL_SLACK = 1e-9;
function toleranceSlack(predicted, tolerance, op) {
  const slack = ((tolerance.abs || {})[op] || 0) + ((tolerance.rel || {})[op] || 0) * Math.abs(predicted);
  return slack || EXACT_REL_SLACK * Math.abs(predicted);
}

function compareFixture(fx) {
  const { groups, problems, excluded } = predictedAttempts(fx);
  const measured = new Map(fx.measured_rows.map((r) => [`${r.caller}|${r.op_kind}`, r.quantity]));
  const explicit = (op) => new Set([...groups.keys()].filter((k) => k.endsWith(`|${op}`) && !k.startsWith('*|')).map((k) => k.split('|')[0]));
  for (const [k, predicted] of groups) {
    const [caller, op] = k.split('|');
    const callers = caller === '*' ? fx.measured_rows.filter((r) => r.op_kind === op && !explicit(op).has(r.caller)).map((r) => r.caller) : [caller];
    const got = callers.reduce((s, c) => s + (measured.get(`${c}|${op}`) || 0), 0);
    const slack = toleranceSlack(predicted, fx.tolerance, op);
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

test('test_exact_comparison_allows_float_residue_only', () => {
  const slack = toleranceSlack(12, {}, 'GET');
  assert.ok(Math.abs(11.999999999999998 - 12) <= slack);
  assert.ok(Math.abs(11.9 - 12) > slack);
  assert.equal(toleranceSlack(12, { abs: { GET: 2 } }, 'GET'), 2);
});

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
    // A borrowed row reuses its source edge's records, so it has no fixture of its own (tools/build_fixtures.py).
    .filter(([id, e]) => e.status === 'done' && ['fit', 'holdout'].includes(e.role) && !e.borrowed_from && !withCells.has(id)).map(([id]) => id);
  assert.deepEqual(only.edges.map((e) => e.edge_id).sort(), without.sort());
});

test('test_block_lines_follow_replicas_and_s3_lines_follow_their_formulas', () => {
  const one = run({ replicas: 1 });
  const two = run({ replicas: 2 });
  const block = new Set(['block_capacity', 'block_iops', 'block_mibps']);
  const lines = (r, v) => r.variants[v].lines;
  for (const l of lines(one, 'ebs')) {
    const d = lines(two, 'ebs').find((x) => x.rule_id === l.rule_id);
    assert.ok(block.has(l.rule_id), l.rule_id);
    close(d.money, 2 * l.money, l.rule_id);
    close(d.quantity, 2 * l.quantity, l.rule_id);
  }
  close(two.variants.ebs.total, 2 * one.variants.ebs.total);
  assert.equal(two.variants.ebs.provisioning.replicas, 2);
  assert.ok(two.variants.ebs.notes.some((n) => /^single: 2 replicas × \(1 x 1000 GiB/.test(n.text)));
  assert.ok(two.variants.ebs.notes.some((n) => /^per replica: smallest instance class/.test(n.text)));
  // The fixture's S3 formulas do not read replicas, so those lines are unchanged.
  for (const l of lines(one, 's3')) close(lines(two, 's3').find((x) => x.rule_id === l.rule_id).money, l.money, l.rule_id);
  const ins = inputs();
  ins.delete('replicas');
  assert.equal(pipeline.run(C, ins, AWS, {}).variants.ebs.state, 'unavailable');
});

test('test_extrapolation_survives_derivations_and_table_validity', () => {
  // A registry derivation over a coefficient used outside its range is extrapolated at every use.
  const viaDerivation = load((x) => {
    x.registry.push(Object.assign({}, x.registry.find((q) => q.id === 'data_bytes'), {
      id: 'puts_derived', kind: 'derivation', unit: 'count', default: null,
      rule: { kind: 'mul', dim: 'count', a: { kind: 'input', id: 'inserts_per_month', dim: 'count' }, b: { kind: 'coef', id: 'puts_per_insert_s3', dim: 'count' } },
    }));
    x.formulas.s3.inserts.ledger_rows[0].expr = { kind: 'input', id: 'puts_derived', dim: 'count' };
  });
  const d = run({ inserts_per_month: 60000 }, viaDerivation).variants.s3;
  assert.equal(d.cells.inserts.extrapolated, true);
  assert.equal(d.extrapolated, true);
  assert.equal(run({ inserts_per_month: 1000 }, viaDerivation).variants.s3.extrapolated, false);
  // A table coefficient's validity domain applies to its lookups.
  const viaTable = load((x) => {
    const t = x.coefficients.find((k) => k.id === 'ebs_mutation_iops');
    t.status = 'ok';
    t.validity = { mutations_per_month: [0, 10] };
  });
  const out = run({ mutations_per_month: 20 }, viaTable).variants.ebs;
  assert.equal(out.cells.mutations.extrapolated, true);
  assert.equal(out.extrapolated, true);
  assert.equal(run({ mutations_per_month: 5 }, viaTable).variants.ebs.extrapolated, false);
});

test('test_all_not_applicable_operation_contributes_nothing', () => {
  const base = run().variants.cas;
  const c = load((x) => { for (const f of x.families) x.applicability.cas.storage[f] = 'not_applicable'; });
  const r = run({}, c).variants.cas;
  assert.equal(r.state, 'ok');
  assert.ok(r.total < base.total, `storage must not be billed: ${r.total} vs ${base.total}`);
  assert.equal(r.cells.storage, undefined);
});

test('test_pinned_quantity_default_reaches_expressions', () => {
  const c = load((x) => { x.registry.find((q) => q.id === 'inserts_per_month').kind = 'pinned'; });
  const withoutIt = new Map(inputs());
  withoutIt.delete('inserts_per_month');
  const r = pipeline.run(c, withoutIt, AWS, {});
  assert.equal(r.variants.s3.state, 'ok');
  close(r.variants.s3.total, run().variants.s3.total);
});

test('test_cut_cell_without_rows_never_prices_zero', () => {
  // s3.mutations has no ledger rows; marking its family cut must not let a non-zero mutation workload price as $0.
  const cut = (x) => { x.applicability.s3.mutations.lifecycle = 'unsupported_cut'; };
  const c = load(cut);
  assert.deepEqual(c.formulas.s3.mutations.ledger_rows, []);
  const r = run({ mutations_per_month: 5 }, c);
  assert.equal(r.variants.s3.state, 'unsupported_cut');
  assert.match(r.variants.s3.reason, /s3\.mutations: cut for lifecycle.*mutations_per_month = 5/);
  assert.equal(r.variants.s3.total, null);
  // Zero workload on every input the operation reads elsewhere: the empty cell is a true zero.
  assert.equal(run({}, c).variants.s3.state, 'ok');
  // No sibling cell names a workload input: nothing shows the workload is zero, so the column is unavailable.
  const blind = load((x) => { cut(x); for (const v of ['ebs', 'cas', 's3plain']) x.formulas[v].mutations = { ledger_rows: [], allocation: null, states: [], notes: [] }; });
  const rb = run({}, blind);
  assert.equal(rb.variants.s3.state, 'unavailable');
  assert.equal(rb.variants.s3.total, null);
});

// A `not_supported` state may read the workload: the column is unsupported only when it runs the operation.
const mutationState = (ids) => (x) => {
  const sum = ids.map((id) => ({ kind: 'input', id, dim: 'count' })).reduce((a, b) => ({ kind: 'add', dim: 'count', a, b }));
  x.formulas.s3plain.mutations.states = [{
    id: 'not_supported',
    cond: { kind: 'branch', dim: 'count', cond: { op: '!=', a: sum, b: { kind: 'const', value: 0, dim: 'count' } }, then: { kind: 'const', value: 1, dim: 'count' }, else: { kind: 'const', value: 0, dim: 'count' } },
    reason: 'plain storage cannot mutate',
  }];
};

test('test_conditional_not_supported_follows_mutation_inputs', () => {
  const c = load(mutationState(['mutations_per_month']));
  const zero = run({ mutations_per_month: 0 }, c).variants.s3plain;
  assert.equal(zero.state, 'ok');
  assert.notEqual(zero.total, null);
  const some = run({ mutations_per_month: 3 }, c);
  assert.equal(some.variants.s3plain.state, 'not_supported');
  assert.match(some.variants.s3plain.reason, /plain storage cannot mutate/);
  assert.equal(some.variants.s3plain.total, null);
  assert.notEqual(some.cheapest, 's3plain');
  // The other columns are unaffected.
  assert.equal(some.variants.s3.state, 'ok');
});

test('test_conditional_not_supported_unresolvable_cond_is_unavailable', () => {
  const c = load(mutationState(['mutations_per_month']));
  const m = inputs({ mutations_per_month: 0 });
  m.delete('mutations_per_month');
  const sp = pipeline.run(c, m, AWS, {}).variants.s3plain;
  assert.equal(sp.state, 'unavailable');
  assert.notEqual(sp.state, 'ok');
  assert.equal(sp.total, null);
});
