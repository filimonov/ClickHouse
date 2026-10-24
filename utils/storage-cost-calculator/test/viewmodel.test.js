'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');
const vmod = require('../viewmodel.js');

const MINIMAL = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8'));
const LIVE_JSON = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8'));
const load = (mutate) => {
  const c = JSON.parse(JSON.stringify(MINIMAL));
  // The fixture registry sits in a test-only block; the page places rows by their registry block.
  for (const q of c.registry) q.block = 'simple';
  if (mutate) mutate(c);
  return contract.load(c);
};
const C = load();
const AWS = presets.resolvePrice(C, 'aws-us-east-1');

function view(raw, opts) {
  const o = opts || {};
  const c = o.contract || C;
  const preset = o.preset || AWS;
  const r0 = pipeline.resolveInputs(c, raw || {}, o.locks || [], o.overrides || {});
  const res = pipeline.run(c, r0.inputs, preset, Object.assign({ lockOrder: r0.lockOrder }, o.run || {}));
  const vm = vmod.build(res, c, r0.fields, r0.lockOrder, Object.assign({ pricePreset: preset }, o.ui || {}));
  return { res, vm };
}
const card = (vm, v) => vm.cards.find((c) => c.variant === v);
const cell = (vm, op, v) => vm.table.rows.find((r) => r.operation === op).cells.find((c) => c.variant === v);

test('test_card_text_for_each_state', () => {
  const ok = view().vm;
  assert.equal(card(ok, 'cas').state_text, 'cheapest');
  assert.equal(card(ok, 'cas').total_text, '$23.01');
  assert.equal(card(ok, 'ebs').state_text, 'comparable');
  assert.deepEqual(card(ok, 'cas').delta_texts, ['vs EBS: −$56.99', 'vs S3: −$24.58']);
  // A reference column has no delta against itself.
  assert.deepEqual(card(ok, 'ebs').delta_texts, ['vs S3: +$32.41']);
  assert.equal(cell(ok, 'storage', 'cas').text, '$23.00');
  assert.equal(cell(ok, 'storage', 'cas').clickable, true);

  const cases = [
    ['not_supported', 's3plain', { replacement_rate: 0.1 }, /cannot replace a replica/],
    ['unsupported_cut', 'ebs', { replacement_rate: 0.1 }, /card cut/],
    ['backlog_grows', 'cas', { inserts_per_month: 2000000 }, /GC reclaims fewer objects/],
    ['unsupported_cut', 'cas', { mutations_per_month: 5 }, /cut for lifecycle/],
  ];
  const unavailable = load((x) => { for (const op of x.operations) for (const f of x.families) x.applicability.cas[op][f] = 'not_applicable'; });
  const all = [...cases.map(([st, v, raw, re]) => [st, v, view(raw).vm, re]), ['unavailable', 'cas', view({}, { contract: unavailable }).vm, /^No measured model for this column yet$/]];
  for (const [state, v, vm, re] of all) {
    const k = card(vm, v);
    assert.equal(k.state, state, v);
    assert.equal(k.state_text, vmod.STATE_TEXT[state]);
    assert.match(k.reason, re);
    assert.equal(k.total_text, null, `${state}: no total`);
    assert.deepEqual(k.delta_texts, [], `${state}: no delta`);
    assert.equal(k.cheapest, false, `${state}: never cheapest`);
    assert.deepEqual(k.bar_segments, []);
    assert.equal(vm.table.columns.find((c) => c.variant === v).cheapest, false);
    for (const row of vm.table.rows) {
      const c = row.cells.find((x) => x.variant === v);
      assert.equal(c.clickable, false);
      assert.equal(/\$/.test(c.text), false, `${state}: no money in ${row.operation}`);
    }
    // The card says why; the notes do not repeat it.
    assert.equal(vm.notes.some((n) => n.kind === state && n.variant === v), false, `${state} note repeats the card`);
    assert.equal(/^[a-z0-9]+\.[a-z_]+: /.test(k.reason), false, k.reason);
  }
  assert.equal(vmod.STATE_TEXT.backlog_grows, 'backlog grows; steady-state monthly cost unavailable');
  assert.equal(vmod.STATE_TEXT.not_supported, 'not supported for this workload');
  assert.equal(vmod.STATE_TEXT.unsupported_cut, 'not modelled yet: a required measurement is missing');
  // Another column's delta against a reference that is not comparable says so instead of a number.
  const cut = view({ replacement_rate: 0.1 }).vm;
  assert.ok(card(cut, 'cas').delta_texts.includes('vs EBS: not comparable (EBS unsupported)'));
  // An extrapolated total keeps its number and says "extrapolated" on the card, its deltas and its cells.
  const ex = view({ inserts_per_month: 60000 }).vm;
  // The flag is carried by `extrapolated` (the page's badge), not baked into the texts.
  assert.equal(card(ex, 's3').state_text, 'comparable');
  assert.equal(card(ex, 's3').extrapolated, true);
  assert.ok(card(ex, 's3').total_text.startsWith('$'));
  const vsS3 = card(ex, 'cas').deltas.find((d) => d.text.startsWith('vs S3'));
  assert.match(vsS3.text, /^vs S3: [−+]\$[\d,.]+$/);
  assert.equal(vsS3.extrapolated, true, 'a delta against an extrapolated reference is flagged');
  assert.match(cell(ex, 'inserts', 's3').text, /^\$[\d,.]+$/);
  assert.equal(cell(ex, 'inserts', 's3').extrapolated, true);
  assert.ok(ex.notes.some((n) => n.kind === 'out_of_range' && /inserts_per_month = 60,000 is outside the measured range \[0, 50,000\]/.test(n.text)));
  // A state the view model does not know is an error, not a default text.
  const { res } = view();
  const r0 = pipeline.resolveInputs(C, {}, [], {});
  const broken = JSON.parse(JSON.stringify(res));
  broken.variants.cas.state = 'mystery';
  broken.cheapest = 'ebs';
  assert.throws(() => vmod.build(broken, C, r0.fields, r0.lockOrder, { pricePreset: AWS }), /unknown variant state "mystery"/);
});

test('test_bar_segments_sum_to_one_or_empty_when_unavailable', () => {
  const classes = new Set(['bytes', 'transfer', 'put', 'get', 'block']);
  for (const raw of [{}, { replacement_rate: 0.1 }, { inserts_per_month: 2000000 }]) {
    const { vm, res } = view(raw);
    for (const k of vm.cards) {
      if (k.state !== 'ok') { assert.deepEqual(k.bar_segments, [], k.variant); continue; }
      const sum = k.bar_segments.reduce((s, b) => s + b.fraction, 0);
      assert.ok(Math.abs(sum - 1) < 1e-12, `${k.variant}: ${sum}`);
      for (const b of k.bar_segments) {
        assert.ok(classes.has(b.class), b.class);
        assert.ok(b.fraction > 0);
      }
      // Each class's money is the sum of its billing lines.
      const m = k.bar_segments.reduce((s, b) => s + b.money, 0);
      assert.ok(Math.abs(m - res.variants[k.variant].total) < 1e-9);
    }
  }
  const s3 = card(view().vm, 's3').bar_segments.map((b) => b.class);
  assert.deepEqual(s3, ['put', 'bytes', 'transfer']);
  assert.deepEqual(card(view().vm, 'ebs').bar_segments.map((b) => b.class), ['block']);
  assert.throws(() => vmod.unitClassOf('no_such_rule', AWS), /not a rule of price preset aws-us-east-1/);
});

test('test_drill_has_five_stages_with_branch_and_allocation', () => {
  // A branching row: CAS inserts pay PUTs only above 500 inserts a month.
  const c = load((x) => {
    const row = x.formulas.cas.inserts.ledger_rows[0];
    row.expr = { kind: 'branch', dim: 'count', cond: { op: '>', a: { kind: 'input', id: 'inserts_per_month', dim: 'count' }, b: { kind: 'const', value: 500, dim: 'count' } }, then: row.expr, else: { kind: 'const', value: 0, dim: 'count' } };
    row.source_refs = [{ claim: 'one PUT per object', path: 'src/Disks/Example.cpp', line: 42 }];
  });
  const { vm } = view({}, { contract: c, ui: { openCell: { variant: 'cas', operation: 'inserts' } } });
  const d = vm.drill;
  assert.deepEqual(d.stages.map((s) => s.name), ['Inputs used', 'Branch taken per row', 'Ledger rows', 'Billing lines', 'Allocation']);
  const [inputs, branch, ledger, billingStage, allocation] = d.stages;
  assert.ok(inputs.lines.some((l) => l.id === 'inserts_per_month' && l.value === 1000 && l.text === 'inserts_per_month = 1,000'));
  assert.ok(inputs.lines.some((l) => l.id === 'puts_per_insert_cas' && l.kind === 'coef' && l.value === 2));
  assert.deepEqual(branch.lines[0].branch_taken, ['then']);
  assert.equal(branch.lines[0].conditions[0].text, 'inserts_per_month > 500: true, then taken');
  assert.equal(branch.lines[0].row, 'request PutObject all');
  assert.equal(ledger.lines[0].basis, 'predicted');
  assert.equal(ledger.lines[0].text, 'request PutObject all: 2,000 (predicted)');
  assert.equal(billingStage.lines[0].rule_id, 'put_copy_post_list');
  assert.equal(billingStage.lines[0].text, 'PUT, COPY, POST and LIST requests: 2,000 requests = $0.01 (share 100% of 2,000 requests = $0.01)');
  assert.equal(allocation.lines[0].basis, 'quantity');
  assert.deepEqual(d.composition.map((x) => [x.rule_id, x.op_kind, x.unit_class, x.fraction]), [['put_copy_post_list', 'PutObject', 'put', 1]]);
  assert.ok(d.sources.some((s) => s.kind === 'measured' && s.ref === 'fixture:puts_per_insert_cas'));
  assert.ok(d.sources.some((s) => s.kind === 'prediction' && s.ref === 'src/Disks/Example.cpp:42'));
  assert.ok(d.sources.some((s) => s.kind === 'billing' && /^https:\/\//.test(s.ref)));
  // The else side: the condition is false and the row is zero.
  const off = view({ inserts_per_month: 100 }, { contract: c, ui: { openCell: { variant: 'cas', operation: 'inserts' } } }).vm.drill;
  assert.deepEqual(off.stages[1].lines[0].branch_taken, ['else']);
  // Declared allocation shares move storage money to inserts: the allocation stage names the basis.
  const sp = view({}, { ui: { openCell: { variant: 's3plain', operation: 'inserts' } } }).vm.drill;
  const st = sp.stages[4].lines.find((l) => l.rule_id === 'storage');
  assert.equal(st.basis, 'allocation.shares');
  assert.ok(Math.abs(st.fraction - 0.25) < 1e-12);
  assert.match(st.text, /^S3 storage: 25% of \$23\.00 by allocation\.shares = \$5\.75$/);
  // EBS block lines carry the allocator's layout and the instance class.
  const ebs = view({}, { ui: { openCell: { variant: 'ebs', operation: 'storage' } } }).vm.drill;
  assert.ok(ebs.stages[4].lines.some((l) => l.kind === 'allocation' && /single: 1 replica × \(1 x 1000 GiB, 3000 IOPS, 125 MiB\/s\)/.test(l.text)));
  assert.ok(ebs.stages[4].lines.some((l) => l.kind === 'instance_class'));
  const ebs2 = view({ replicas: 2 }, { ui: { openCell: { variant: 'ebs', operation: 'storage' } } }).vm.drill;
  assert.ok(ebs2.stages[4].lines.some((l) => l.kind === 'allocation' && /single: 2 replicas × \(1 x 1000 GiB, 3000 IOPS, 125 MiB\/s\)/.test(l.text)));
  assert.ok(ebs2.stages[4].lines.some((l) => l.kind === 'instance_class' && /^per replica: /.test(l.text)));
  // A cell of a column that is not comparable does not open; neither does a cell that does not exist.
  const ns = view({ replacement_rate: 0.1 }, { ui: { openCell: { variant: 's3plain', operation: 'storage' } } }).vm;
  assert.equal(ns.drill, null);
  assert.match(ns.problems.join('\n'), /s3plain\.storage cannot be opened: not supported/);
});

test('test_inputs_blocks_from_registry_with_hint_and_lock_state', () => {
  const live = contract.load(LIVE_JSON);
  const s = presets.SCENARIOS.analytics_batch;
  const r0 = pipeline.resolveInputs(live, s.values, s.locks, { replicas: 9 });
  const preset = presets.resolvePrice(live, 'custom');
  const aws0 = presets.resolvePrice(live, 'aws-us-east-1');
  const res = pipeline.run(live, r0.inputs, preset, {});
  const vm = vmod.build(res, live, r0.fields, r0.lockOrder, { pricePreset: preset, priceEditing: true });
  const blocks = vm.inputs.blocks.map((b) => b.block);
  const order = vmod.BLOCKS.map(([id]) => id);
  assert.equal(blocks[0], 'simple');
  assert.deepEqual(blocks, order.filter((id) => blocks.includes(id)), 'blocks in spec order');
  // Every registry row except probe observations is a field of its block.
  const shown = vm.inputs.blocks.flatMap((b) => b.fields.map((f) => f.id)).filter((id) => !id.startsWith('price:'));
  assert.deepEqual(shown.sort(), live.registry.filter((q) => !(q.kind === 'measured' && !q.coef)).map((q) => q.id).sort());
  const simple = vm.inputs.blocks[0];
  assert.equal(simple.advanced, false);
  const f = (id) => vm.inputs.blocks.flatMap((b) => b.fields).find((x) => x.id === id);
  assert.equal(f('data_bytes').locked, true);
  assert.equal(f('data_bytes').value, 10240);
  assert.equal(f('ingest_rate').derived, true);
  assert.equal(f('ingest_rate').locked, false);
  assert.equal(f('ingest_rate').rate, true);
  assert.equal(f('ingest_rate').rule_text, 'data_bytes / retention_days');
  assert.equal(f('replicas').overridden, true);
  assert.equal(f('replicas').out_of_range, true);
  assert.deepEqual(f('replicas').range, [1, 6]);
  assert.ok(vm.notes.some((n) => n.kind === 'out_of_range' && n.param === 'replicas'));
  assert.equal(f('min_bytes_for_wide_part').kind, 'pinned');
  assert.equal(f('min_bytes_for_wide_part').editable, false);
  assert.match(f('min_bytes_for_wide_part').hint.sql, /system\.merge_tree_settings/);
  assert.equal(f('merge_effective_fan_in').value, live.coefficients.find((c) => c.id === 'merge_effective_fan_in').value);
  // `range` is always [lo, hi] of the field itself; a validity map over other inputs is `validity`.
  assert.equal(f('merge_effective_fan_in').range, null);
  assert.deepEqual(f('merge_effective_fan_in').validity.insert_rows, [1000, 100000]);
  assert.equal(f('replicas').validity, null);
  assert.deepEqual(f('multipart_parts').validity.object_bytes, [0, 8405385216]);
  assert.equal(f('multipart_parts').range, null);
  for (const b of vm.inputs.blocks) for (const x of b.fields) assert.ok(x.range === null || (Array.isArray(x.range) && x.range.length === 2), x.id);
  const schema = vm.inputs.blocks.find((b) => b.block === 'schema');
  assert.ok(schema.hints.some((h) => /system\.merge_tree_settings/.test(h.sql) && 'min_bytes_for_wide_part' in h.mapping));
  assert.deepEqual(vm.inputs.groups[0].locked, ['data_bytes', 'retention_days']);
  assert.deepEqual(vm.inputs.groups[0].derived, ['ingest_rate']);
  assert.match(vm.inputs.groups[0].label, /steady-state assumption/);
  // Prices are fields with their date; editable only for the custom preset while editing.
  const prices = vm.inputs.blocks.find((b) => b.block === 'prices');
  assert.ok(prices.fields.length > 0);
  for (const p of prices.fields) {
    assert.equal(p.as_of, preset.as_of);
    assert.equal(p.editable, true);
    // Labels are words, not billing rule ids; values are money in the preset's currency.
    assert.equal(/[a-z0-9]_[a-z0-9]/.test(p.label), false, p.label);
    if (p.value === null) assert.equal(p.value_text, 'not priced by this preset');
    else assert.match(p.value_text, /^\$\d[\d,]*\.\d{2,6} per /, p.value_text);
  }
  const price = (path) => prices.fields.find((p) => p.path === path);
  assert.equal(price('transfer.ec2_ec2_cross_az.price_per_gb').label, 'Transfer EC2 to EC2 across availability zones');
  assert.equal(price('request_classes.get_and_other.price_per_1000').value_text, '$0.0004 per 1,000 requests');
  assert.equal(price('storage.tiers.0.price_per_gb_month').value_text, '$0.023 per GB-month');
  assert.equal(price('transfer.internet.price_per_gb').value_text, 'not priced by this preset');
  assert.throws(() => presets.priceFields(Object.assign(JSON.parse(JSON.stringify(aws0)), { transfer: [{ id: 'mystery_route', price_per_gb: 0 }] })), /transfer rule mystery_route has no label/);
  const aws = presets.resolvePrice(live, 'aws-us-east-1');
  const vmAws = vmod.build(pipeline.run(live, r0.inputs, aws, {}), live, r0.fields, r0.lockOrder, { pricePreset: aws, priceEditing: true });
  assert.ok(vmAws.inputs.blocks.find((b) => b.block === 'prices').fields.every((p) => !p.editable));
  // A registry block the page does not know is reported, not dropped.
  const odd = load((x) => { x.registry[0].block = 'mystery'; });
  const v2 = view({}, { contract: odd }).vm;
  assert.match(v2.problems.join('\n'), /registry block mystery \(first row data_bytes\) has no place on the page/);
  assert.equal(v2.inputs.blocks.find((b) => b.block === 'mystery').fields[0].id, 'data_bytes');
});

test('test_chart_marks_extrapolated_points', () => {
  // puts_per_insert_s3 was measured over inserts_per_month in [0, 50000].
  const { vm } = view({}, { run: { sweep: { inputId: 'inserts_per_month', values: [1000, 40000, 100000, 2000000] } }, ui: { sweepInput: 'inserts_per_month' } });
  const ch = vm.chart;
  assert.equal(ch.labels.x, 'inserts_per_month (count)');
  assert.equal(ch.labels.y, 'Monthly total (USD)');
  assert.deepEqual(ch.labels.series.map((s) => s.variant), C.variants);
  const pt = (v, x) => ch.points.find((p) => p.variant === v && p.x === x);
  assert.equal(pt('s3', 1000).extrapolated, false);
  assert.equal(pt('s3', 100000).extrapolated, true);
  assert.match(pt('s3', 100000).label, /^S3 at 100,000: \$[\d,.]+ \(extrapolated\)$/);
  assert.ok(ch.extrapolated_points.some((p) => p.variant === 's3' && p.x === 100000));
  assert.ok(!ch.extrapolated_points.some((p) => p.x === 1000));
  // A point whose column is not comparable has no y and says why.
  assert.equal(pt('cas', 2000000).y, null);
  assert.equal(pt('cas', 2000000).label, 'CAS at 2,000,000: backlog grows');
  for (const b of ch.breakEven) assert.equal(b.text.endsWith('(extrapolated)'), b.extrapolated);
  // A point state the view model does not know is an error, not a raw id in the label.
  const r0 = pipeline.resolveInputs(C, {}, [], {});
  const res = pipeline.run(C, r0.inputs, AWS, { sweep: { inputId: 'inserts_per_month', values: [1000] }, lockOrder: r0.lockOrder });
  res.sweep.points[0].totals.cas = { value: null, state: 'mystery', extrapolated: false };
  assert.throws(() => vmod.build(res, C, r0.fields, r0.lockOrder, { pricePreset: AWS }), /unknown variant state "mystery"/);
  // A sweep over another input than the page asked for is reported.
  const other = view({}, { run: { sweep: { inputId: 'inserts_per_month', values: [1000] } }, ui: { sweepInput: 'data_bytes' } }).vm;
  assert.match(other.problems.join('\n'), /the sweep is over inserts_per_month, the page asked for data_bytes/);
});

test('test_fmt_money_thousands_and_currency', () => {
  assert.equal(vmod.fmtMoney(1234567.891, 'USD'), '$1,234,567.89');
  assert.equal(vmod.fmtMoney(1234.5, 'EUR'), '€1,234.50');
  assert.equal(vmod.fmtMoney(0, 'USD'), '$0.00');
  assert.equal(vmod.fmtMoney(1234, 'USD', { sign: true }), '+$1,234.00');
  assert.equal(vmod.fmtMoney(-5, 'USD', { sign: true }), '−$5.00');
  assert.equal(vmod.fmtMoney(-5, 'USD'), '−$5.00');
  // A non-zero amount below a cent is not shown as zero.
  assert.equal(vmod.fmtMoney(0.0004, 'USD'), '< $0.01');
  assert.equal(vmod.fmtMoney(-0.0004, 'USD', { sign: true }), '−< $0.01');
  assert.throws(() => vmod.fmtMoney(1, undefined), /no currency/);
  assert.throws(() => vmod.fmtMoney(NaN, 'USD'), /finite number/);
  assert.throws(() => vmod.fmtMoney(1, 'NOT-A-CURRENCY'), RangeError);
  assert.equal(vmod.fmtQuantity(1234567.4, 'GiB'), '1,234,567 GiB');
  assert.equal(vmod.fmtQuantity(12.3456, 'GiB/day'), '12.35 GiB/day');
  assert.equal(vmod.fmtQuantity(5000, 'count'), '5,000');
  // The page uses the preset's currency.
  const eur = Object.assign(JSON.parse(JSON.stringify(AWS)), { currency: 'EUR' });
  const { vm } = view({}, { preset: eur });
  assert.equal(card(vm, 'cas').total_text, '€23.01');
  assert.equal(cell(vm, 'storage', 'ebs').text, '€80.00');
  assert.equal(vm.currency, 'EUR');
  // Money is rounded only for display: the Result keeps the exact total.
  const { res } = view();
  assert.equal(res.variants.s3.total, 47.58833333333334);
  assert.equal(card(view().vm, 's3').total_text, '$47.59');
});

test('test_out_of_range_note_once_per_param', () => {
  // insert_rows is bounded by many coefficients with different ranges: one note per param names each violated owner with its range.
  const live = contract.load(LIVE_JSON);
  const s = presets.SCENARIOS.logs;
  const preset = presets.resolvePrice(live, 'aws-us-east-1');
  const notesFor = (rows) => {
    const r0 = pipeline.resolveInputs(live, Object.assign({}, s.values, { insert_rows: rows }), s.locks, {});
    const vm = vmod.build(pipeline.run(live, r0.inputs, preset, {}), live, r0.fields, r0.lockOrder, { pricePreset: preset });
    return vm.notes.filter((n) => n.kind === 'out_of_range');
  };
  // Coefficients no cell reads raise no note.
  const reached = presets.reachedCoefficients(live);
  const coefIds = new Set(live.coefficients.map((c) => c.id));
  const domains = presets.validityDomains(live).filter((d) => d.param === 'insert_rows' && (!coefIds.has(d.id) || reached.has(d.id)));
  const fmt = (n) => n.toLocaleString('en-US');
  const range = (d) => `[${fmt(d.range[0])}, ${fmt(d.range[1])}]`;
  // 2,000,000 is above the 1,000-100,000, 1,000-400,000 and 1,000-600,000 maxima: several owners with different ranges, one note.
  const low = notesFor(2000000);
  assert.equal(low.length, 1, JSON.stringify(low));
  const hit = domains.filter((d) => 2000000 < d.range[0] || 2000000 > d.range[1]);
  assert.ok(new Set(hit.map((d) => d.range.join())).size >= 2);
  assert.deepEqual(low[0].owners, hit.map((d) => d.owner));
  assert.equal(low[0].text, `insert_rows = 2,000,000 is outside the measured range of ${hit.map((d) => `${d.owner} ${range(d)}`).join(', ')}; dependent numbers are extrapolated`);
  // Outside every range: still one note, one shared-range-free list naming all owners.
  const all = notesFor(10000000);
  assert.equal(all.length, 1);
  assert.deepEqual(all[0].owners, domains.map((d) => d.owner));
});

test('test_field_display_values_ranges_and_steps', () => {
  const live = contract.load(LIVE_JSON);
  const s = presets.SCENARIOS.logs;
  const r0 = pipeline.resolveInputs(live, s.values, s.locks, {});
  const aws = presets.resolvePrice(live, 'aws-us-east-1');
  const build = (units) => vmod.build(pipeline.run(live, r0.inputs, aws, {}), live, r0.fields, r0.lockOrder, { pricePreset: aws, units });
  const f = (vm, id) => vm.inputs.blocks.flatMap((b) => b.fields).find((x) => x.id === id);
  const day = build(undefined);
  const hour = build('hour');
  // Labels come from the registry, never the id.
  assert.equal(f(day, 'data_bytes').label, 'Data size');
  // A rate is shown in the chosen unit, rounded; the exact per-day value stays in `value`.
  assert.equal(f(day, 'ingest_rate').display_value, 500);
  assert.equal(f(day, 'ingest_rate').display_unit, 'GiB/day');
  assert.equal(f(hour, 'ingest_rate').display_value, 20.8333);
  assert.equal(f(hour, 'ingest_rate').display_unit, 'GiB/hour');
  assert.equal(f(hour, 'ingest_rate').value, 500);
  // Counts step by one; a one-value range is fixed.
  assert.equal(f(day, 'replicas').step, 1);
  assert.equal(f(day, 'replicas').fixed, false);
  assert.equal(f(day, 'replicas').range_text, 'measured for 1 to 6');
  assert.equal(f(day, 'shards').fixed, true);
  assert.equal(f(day, 'shards').range_text, 'measured at 1 only');
  assert.equal(f(day, 'data_bytes').step, 'any');
  const fan = f(day, 'merge_effective_fan_in');
  assert.match(fan.range_text, /measured for .*1,000 to 100,000/);
  // A row without a label is reported, and its field has no label instead of the id.
  const { vm } = view({}, { contract: load((x) => { delete x.registry[0].label; }) });
  assert.equal(vm.inputs.blocks[0].fields.find((x) => x.id === 'data_bytes').label, null);
  assert.match(vm.problems.join('\n'), /registry data_bytes has no label/);
});

test('test_chart_y_ticks_are_round_money', () => {
  const { vm } = view({}, { run: { sweep: { inputId: 'data_bytes', values: [250, 1000, 8000] } }, ui: { sweepInput: 'data_bytes' } });
  const t = vm.chart.y_ticks;
  assert.ok(t.length >= 3 && t.length <= 6, JSON.stringify(t));
  assert.equal(t[0].value, 0);
  const top = Math.max(...vm.chart.points.filter((p) => p.y !== null).map((p) => p.y));
  assert.ok(t[t.length - 1].value >= top && t[t.length - 2].value < top);
  for (const x of t) assert.match(x.text, /^\$[\d,]+$/, 'whole-dollar ticks');
  assert.deepEqual(vm.chart.x_ticks.map((x) => x.text), ['250', '1,000', '8,000']);
});

test('test_plain_copy_for_reasons_quantities_and_derivations', () => {
  assert.equal(vmod.plainReason('ebs: no operation has a supported family in this contract'), 'No measured model for this column yet');
  assert.equal(vmod.plainReason('price preset gcs has no prices (structure only)'), 'This price list has no prices yet; quantities are shown without money');
  assert.equal(vmod.plainReason('s3plain.replacement: cannot replace a replica; s3plain.storage: cannot replace a replica'), 'cannot replace a replica');
  const gcs = presets.resolvePrice(C, 'gcs');
  const { vm } = view({}, { preset: gcs });
  assert.equal(vm.page_reason, 'This price list has no prices yet; quantities are shown without money');
  assert.equal(vm.notes.some((n) => n.kind === 'prices_missing'), false, 'the banner says it once');
  assert.equal(cell(vm, 'storage', 's3').text, 'Stored data 1,000 GiB; Replica transfer 1,217 GiB');
  assert.equal(cell(vm, 'storage', 'ebs').text, 'Volume capacity 1,000 GiB; IOPS 3,000; Throughput 125 MiB/s');
  assert.equal(cell(vm, 'inserts', 's3').text, 'PUT requests 5,000; Stored data 10 GiB');
  // Derivations read with registry labels.
  const live = contract.load(LIVE_JSON);
  const r0 = pipeline.resolveInputs(live, presets.SCENARIOS.logs.values, presets.SCENARIOS.logs.locks, {});
  const aws = presets.resolvePrice(live, 'aws-us-east-1');
  const lv = vmod.build(pipeline.run(live, r0.inputs, aws, {}), live, r0.fields, r0.lockOrder, { pricePreset: aws, units: 'hour' });
  const f = (id) => lv.inputs.blocks.flatMap((b) => b.fields).find((x) => x.id === id);
  assert.equal(f('data_bytes').derived_text, 'Derived from ingest rate × retention');
  // EBS and s3plain have measured models; the logs scenario mutates, which s3_plain_rewritable refuses. EBS drops and
  // restarts are priced by assumption rows, so the column stays available.
  assert.match(lv.cards.find((c) => c.variant === 's3plain').reason, /s3_plain_rewritable cannot perform mutations/);
  assert.equal(lv.cards.find((c) => c.variant === 'ebs').state, 'ok');
  // Slider bounds in the shown unit: the ladder for a rate without a measured range, the range for replicas.
  // 500 GiB/day ×1/4 … ×8, shown per hour.
  assert.deepEqual(f('ingest_rate').slider, { min: 5.20833, max: 166.667 });
  assert.deepEqual(f('replicas').slider, { min: 1, max: 6 });
  assert.equal(f('shards').slider, null);
  // Drill composition and card bars carry words, not rule ids.
  const d = view({}, { ui: { openCell: { variant: 'cas', operation: 'storage' } } }).vm.drill;
  assert.deepEqual(d.composition.map((x) => x.label), ['Stored data']);
});

test('test_every_domain_operation_has_a_label', () => {
  // The live contract and the coverage domain (which may list operations before any contract cell exists).
  const coverage = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'coverage.json'), 'utf8'));
  const ops = new Set([...LIVE_JSON.operations, ...coverage.domain.operations, 'gc', 'lease']);
  for (const op of ops) assert.ok(Object.prototype.hasOwnProperty.call(vmod.OPERATION_LABELS, op), `operation ${op} has no label`);
  assert.equal(vmod.OPERATION_LABELS.gc, 'Garbage collection');
  assert.equal(vmod.OPERATION_LABELS.lease, 'Mount lease');
  // An operation without a label is still reported, not silently shown by its id.
  const odd = load((x) => {
    x.operations.push('mystery');
    for (const v of x.variants) x.applicability[v].mystery = Object.fromEntries(x.families.map((f) => [f, 'supported']));
  });
  assert.match(view({}, { contract: odd }).vm.problems.join('\n'), /operation mystery has no label in viewmodel\.js/);
});

test('test_out_of_range_notes_skip_coefficients_no_cell_reads', () => {
  const withCoef = (id) => load((c) => {
    c.coefficients.push({ id, kind: 'scalar', value: 1, dim: 'count', validity: { inserts_per_month: [0, 100] }, status: 'ok', evidence: { kind: 'measured', ref: 'fixture:extra' } });
  });
  const notes = (c) => view({ inserts_per_month: 500 }, { contract: c }).vm.notes.filter((n) => n.kind === 'out_of_range' && n.owners.some((o) => /extra_coef/.test(o)));
  assert.deepEqual(notes(withCoef('extra_coef')), [], 'no cell reads it');
  // The same coefficient in a cell does warn.
  const read = load((c) => {
    c.coefficients.push({ id: 'extra_coef', kind: 'scalar', value: 1, dim: 'count', validity: { inserts_per_month: [0, 100] }, status: 'ok', evidence: { kind: 'measured', ref: 'fixture:extra' } });
    const swap = (n) => { if (n && typeof n === 'object') { if (n.kind === 'coef' && n.id === 'puts_per_insert_cas') n.id = 'extra_coef'; Object.values(n).forEach(swap); } };
    swap(c.formulas);
  });
  assert.equal(notes(read).length, 1);
});
