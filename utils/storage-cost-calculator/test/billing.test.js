'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const billing = require('../billing.js');

const CONTRACT = path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'billing-contract.json');
const FIX = path.join(__dirname, 'fixtures', 'pricing');
const presetOf = (id, over) => {
  const p = JSON.parse(fs.readFileSync(CONTRACT, 'utf8')).presets.find((x) => x.id === id);
  return Object.assign({}, p, over || {});
};
const close = (a, b, msg) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)), `${msg || ''} ${a} != ${b}`);
const row = (row_type, op_kind, outcome_class, quantity, extra) =>
  Object.assign({ row_type, op_kind, outcome_class, category: null, route: null, scope: 'variant', quantity }, extra);
const line = (res, id) => res.lines.find((l) => l.rule_id === id);
const P = () => presetOf('aws-us-east-1');

test('test_request_classes', () => {
  const r = billing.bill([row('request', 'PutObject', 'ok', 2000), row('request', 'GetObject', 'ok', 10000),
    row('request', 'DeleteObject', 'ok', 5000)], P());
  close(line(r, 'put_copy_post_list').money, 0.01);
  close(line(r, 'get_and_other').money, 0.004);
  assert.equal(line(r, 'delete_cancel').money, 0);
  close(r.total, 0.014);
  assert.match(line(r, 'put_copy_post_list').source_url, /AmazonS3/);
  assert.throws(() => billing.bill([row('request', 'FrobnicateObject', 'ok', 1)], P()), /FrobnicateObject/);
});

test('test_outcome_mix_split_and_free_list', () => {
  const mix = { GetObject: { ok: 0.5, client_error: 0.25, server_error: 0.125, dropped: 0.125 } };
  const r = billing.bill([row('request', 'GetObject', 'all', 8000000)], P(), 'variant', mix);
  assert.equal(line(r, 'get_and_other').quantity, 6000000);
  close(line(r, 'get_and_other').money, 2.4);
  // No mix: the billed share is unknown, so billing fails instead of assuming everything is ok.
  assert.throws(() => billing.bill([row('request', 'GetObject', 'all', 1000)], P()), /an all-outcomes row needs an outcome mix for GetObject, and none is given/);
  assert.throws(() => billing.bill([row('request', 'GetObject', 'all', 1000)], P(), 'variant', { PutObject: { ok: 1 } }), /outcome mix for GetObject/);
  // A free-listed client error is not billed; an unlisted one is; 5XX never.
  const rows = [row('request', 'GetObject', 'client_error', 100, { status: 403, s3_code: 'SignatureDoesNotMatch' }),
    row('request', 'GetObject', 'client_error', 40, { status: 404, s3_code: 'NoSuchKey' }),
    row('request', 'GetObject', 'client_error', 7),
    row('request', 'GetObject', 'client_error', 3, { status: 403, s3_code: 'AccessDenied' }),
    row('request', 'GetObject', 'client_error', 5, { status: 403, s3_code: 'AccessDenied', condition: "request initiated outside the bucket owner's AWS account or organization" }),
    row('request', 'GetObject', 'server_error', 900), row('request', 'GetObject', 'dropped', 900)];
  assert.equal(line(billing.bill(rows, P()), 'get_and_other').quantity, 50);
  assert.throws(() => billing.bill([row('request', 'GetObject', 'all', 1)], P(), 'variant', { GetObject: { ok: 0.5 } }), /sum/);
});

test('test_accepted_dropped_is_billed_and_dropped_is_not', () => {
  // The store accepted the request and its reply was lost: billed like ok. `dropped` means the store's outcome is
  // unknown, so the row carries no billable fact.
  const r = billing.bill([row('request', 'UploadPart', 'ok', 2000), row('request', 'UploadPart', 'accepted_dropped', 1000),
    row('request', 'UploadPart', 'dropped', 7000)], P());
  assert.equal(line(r, 'put_copy_post_list').quantity, 3000);
  assert.equal(line(billing.bill([row('request', 'UploadPart', 'not_sent', 5)], P()), 'put_copy_post_list').quantity, 0);
  assert.match(line(r, 'put_copy_post_list').note, /7000 dropped/);
  const mixed = billing.bill([row('request', 'GetObject', 'all', 1000)], P(), 'variant',
    { GetObject: { ok: 0.5, accepted_dropped: 0.25, dropped: 0.25 } });
  assert.equal(line(mixed, 'get_and_other').quantity, 750);
});

test('test_validated_cell_rows_keep_their_outcome_facts', () => {
  // A cell row that passed validation keeps status, s3_code and condition through evaluateCell into billing.
  const model = require('../model.js');
  const cell = { ledger_rows: [
    Object.assign(row('request', 'GetObject', 'client_error', null, { status: 403, s3_code: 'SignatureDoesNotMatch', caller: 'r1' }),
      { expr: { kind: 'const', value: 100, dim: 'count' } }),
    Object.assign(row('request', 'GetObject', 'client_error', null, { status: 403, s3_code: 'AccessDenied' }),
      { expr: { kind: 'const', value: 3, dim: 'count' } }),
  ], allocation: null, states: [], notes: [] };
  const ev = model.evaluateCell(cell, { inputs: new Map(), coefs: new Map(), tables: new Map(), sets: new Map(), vars: new Map() });
  assert.equal(ev.rows[0].s3_code, 'SignatureDoesNotMatch');
  assert.equal(line(billing.bill(ev.rows, P()), 'get_and_other').quantity, 3);
});

test('test_storage_tiers_once', () => {
  const r = billing.bill([row('storage', 'stored', 'ok', 51200, { category: 'active' }),
    row('storage', 'stored', 'ok', 1, { category: 'outdated' })], P());
  const s = line(r, 'storage');
  assert.equal(s.tiers.length, 2);
  close(s.money, 51200 * 0.023 + 1 * 0.022);
  // Two categories of 30000 each must not each get tier 1.
  const two = billing.bill([row('storage', 'stored', 'ok', 30000, { category: 'active' }),
    row('storage', 'stored', 'ok', 30000, { category: 'garbage' })], P());
  close(line(two, 'storage').money, 51200 * 0.023 + 8800 * 0.022);
  assert.throws(() => billing.bill([row('storage', 'stored', 'ok', 1)], P()), /category/);
});

test('test_incomplete_as_storage', () => {
  const r = billing.bill([row('storage', 'stored', 'ok', 100, { category: 'incomplete' })], P());
  close(line(r, 'storage').money, 2.3);
  assert.match(line(r, 'storage').note, /incomplete/);
  const bad = Object.assign({}, P(), { storage: Object.assign({}, P().storage, { incomplete_multipart: 'free' }) });
  assert.throws(() => billing.bill([row('storage', 'stored', 'ok', 100, { category: 'incomplete' })], bad), /incomplete_multipart/);
});

test('test_transfer_routes', () => {
  const r = billing.bill([row('transfer', 'x', 'ok', 10, { route: 'ec2_s3_same_region' }),
    row('transfer', 'x', 'ok', 10, { route: 'ec2_ec2_cross_az' }), row('transfer', 'x', 'ok', 10, { route: 'internet' })], P());
  assert.equal(line(r, 'ec2_s3_same_region').money, 0);
  // Cross-AZ is charged on the sending and on the receiving side: 10 GB moved bill 2 x 10 GB.
  close(line(r, 'ec2_ec2_cross_az').money, 0.2);
  assert.match(line(r, 'ec2_ec2_cross_az').note, /each direction/);
  assert.equal(line(r, 'internet').money, 0);
  assert.match(line(r, 'internet').note, /not priced|egress/);
  assert.throws(() => billing.bill([row('transfer', 'x', 'ok', 1, { route: 'carrier_pigeon' })], P()), /carrier_pigeon/);
  assert.throws(() => billing.bill([row('transfer', 'x', 'ok', 1)], P()), /route/);
  const noPrice = presetOf('aws-us-east-1');
  noPrice.transfer = noPrice.transfer.map((t) => (t.id === 'ec2_ec2_cross_az' ? Object.assign({}, t, { price_per_gb: null }) : t));
  assert.throws(() => billing.bill([row('transfer', 'x', 'ok', 1, { route: 'ec2_ec2_cross_az' })], noPrice), /price/);
  const noDirection = presetOf('aws-us-east-1');
  noDirection.transfer = noDirection.transfer.map((t) => (t.id === 'ec2_ec2_cross_az' ? Object.assign({}, t, { direction: 'sideways' }) : t));
  assert.throws(() => billing.bill([row('transfer', 'x', 'ok', 1, { route: 'ec2_ec2_cross_az' })], noDirection), /direction/);
});

test('test_interserver_transfer_of_the_logs_preset', () => {
  // 500 GiB/day fetched by one follower over a 2 628 000 s month, times 1.0008 framing: about $304 across AZs.
  const gib = (500 / 86400) * 2628000 * (2 - 1) * 1.000804700184805;
  const r = billing.bill([row('transfer', 'interserver', 'all', gib, { route: 'ec2_ec2_cross_az' })], presetOf('aws-us-east-1'));
  assert.ok(Math.abs(line(r, 'ec2_ec2_cross_az').quantity - 15220.57) < 0.01, line(r, 'ec2_ec2_cross_az').quantity);
  assert.ok(Math.abs(line(r, 'ec2_ec2_cross_az').money - 304.41) < 0.01, line(r, 'ec2_ec2_cross_az').money);
  const zero = billing.bill([row('transfer', 'interserver', 'all', 0, { route: 'ec2_ec2_cross_az' })], presetOf('aws-us-east-1'));
  assert.equal(line(zero, 'ec2_ec2_cross_az').money, 0);
});

test('test_decimal_conversion', () => {
  assert.equal(billing.toPresetGb(100, 'binary_gb'), 100);
  close(billing.toPresetGb(100, 'decimal'), 107.3741824);
  assert.throws(() => billing.toPresetGb(1, 'furlongs'), /furlongs/);
  assert.deepEqual(billing.UNITS, ['decimal', 'binary_gb']);
  const dec = presetOf('aws-us-east-1', { units: 'decimal' });
  close(line(billing.bill([row('storage', 'stored', 'ok', 1000, { category: 'active' })], dec), 'storage').money, 1073.741824 * 0.023);
});

test('test_scope_filter_reports_skipped', () => {
  const rows = [row('request', 'GetObject', 'ok', 1000), row('request', 'GetObject', 'ok', 5000, { scope: 'replica' })];
  const r = billing.bill(rows, P(), 'variant');
  assert.equal(line(r, 'get_and_other').quantity, 1000);
  assert.equal(r.skipped, 1);
  assert.throws(() => billing.bill([row('request', 'GetObject', 'ok', null)], P()), /quantity/);
});

const D = (capacity_gib, peak_iops, peak_mibps) => ({ capacity_gib, peak_iops, peak_mibps });

test('test_allocate_increases_size_for_iops_cap_instead_of_rejecting', () => {
  const a = billing.allocateVolumes(D(10, 20000, 0), P(), [{ id: 'single', volumes: 1 }]);
  assert.equal(a.feasible, true);
  assert.equal(a.size_gib, 40); // ceil(20000 / 500), not the requested 10
  close(a.monthly_cost, 40 * 0.08 + 17000 * 0.005);
  // A bound that growing the volume cannot fix is rejected: 80000 IOPS max.
  const b = billing.allocateVolumes(D(10, 90000, 0), P(), [{ id: 'single', volumes: 1 }]);
  assert.equal(b.feasible, false);
  assert.match(b.notes.join(' '), /iops/);
  assert.equal(b.monthly_cost, undefined);
});

test('test_allocate_min_cost_tiebreak_alternatives', () => {
  const free = presetOf('aws-us-east-1');
  free.block = { gp3: Object.assign({}, free.block.gp3, { price_per_gib_month: 0, iops_price: 0, mibps_price: 0 }) };
  const rev = [...free.allocation.layouts].reverse();
  const t = billing.allocateVolumes(D(100, 9000, 0), free, rev);
  assert.equal(t.layout, 'single'); // all cost 0; fewest volumes wins whatever the order
  assert.equal(t.alternatives.length, 3);
  const e = billing.allocateVolumes(D(100, 60000, 500), P());
  assert.equal(e.layout, 'stripe4');
  close(e.monthly_cost, 249.6);
  assert.deepEqual(e.alternatives.map((x) => x.layout).sort(), ['single', 'stripe2', 'stripe4']);
});

test('test_mibps_iops_coupling', () => {
  const a = billing.allocateVolumes(D(10, 100, 2000), P(), [{ id: 'single', volumes: 1 }]);
  assert.equal(a.iops, 8000); // 2000 MiB/s needs 2000 / 0.25 IOPS although demand is 100
  assert.equal(a.size_gib, 16);
  close(a.monthly_cost, 16 * 0.08 + 5000 * 0.005 + 1875 * 0.04);
  const all = billing.allocateVolumes(D(10, 100, 2000), P());
  assert.equal(all.layout, 'stripe4');
  close(all.monthly_cost, 61.92);
  const over = billing.allocateVolumes(D(10, 100, 3000), P(), [{ id: 'single', volumes: 1 }]);
  assert.equal(over.feasible, false);
  assert.match(over.notes.join(' '), /mibps/);
});

test('test_manual_invalid_rejected_vs_below_demand_noted', () => {
  const d = D(100, 6000, 200);
  const bad = billing.allocateVolumes(d, P(), undefined, { volumes: 1, size_gib: 10, iops: 6000, mibps: 200 });
  assert.equal(bad.feasible, false);
  assert.equal(bad.manual, true);
  assert.equal(bad.monthly_cost, undefined);
  assert.match(bad.notes.join(' '), /iops_per_gib_max/);
  const coupling = billing.allocateVolumes(d, P(), undefined, { volumes: 1, size_gib: 100, iops: 3000, mibps: 1000 });
  assert.match(coupling.notes.join(' '), /mibps_per_iops_max/);
  const bounds = billing.allocateVolumes(d, P(), undefined, { volumes: 1, size_gib: 100, iops: 2999, mibps: 125 });
  assert.match(bounds.notes.join(' '), /iops range/);
  const low = billing.allocateVolumes(d, P(), undefined, { volumes: 1, size_gib: 50, iops: 3000, mibps: 125 });
  assert.equal(low.feasible, true);
  close(low.monthly_cost, 4);
  assert.match(low.notes.join(' '), /below demand/);
  const full = billing.allocateVolumes(d, P(), undefined, { volumes: 2, size_gib: 50, iops: 3000, mibps: 125 });
  assert.equal(full.notes.some((n) => /below demand/.test(n)), false); // 2 x 50 GiB, 6000 IOPS, 250 MiB/s cover the demand
});

test('test_zero_demand', () => {
  const a = billing.allocateVolumes(D(0, 0, 0), P());
  assert.equal(a.layout, 'single');
  assert.equal(a.iops, 3000);
  assert.equal(a.mibps, 125);
  assert.equal(a.size_gib, 6); // 3000 baseline IOPS / 500 per GiB
  close(a.monthly_cost, 6 * 0.08);
  assert.throws(() => billing.allocateVolumes(D(-1, 0, 0), P()), /capacity_gib/);
});

test('test_pricing_fixtures_exact', () => {
  const files = fs.readdirSync(FIX).filter((f) => f.endsWith('.json')).sort();
  assert.ok(files.length >= 6);
  for (const f of files) {
    const fx = JSON.parse(fs.readFileSync(path.join(FIX, f), 'utf8'));
    assert.ok(fx.derivation && fx.derivation.length > 40, `${fx.name}: derivation`);
    const preset = presetOf(fx.preset_id, fx.preset_overrides);
    const layouts = fx.layouts ? fx.layouts.map((id) => preset.allocation.layouts.find((l) => l.id === id)) : undefined;
    const ex = fx.expected;
    if (fx.rows) {
      const r = billing.bill(fx.rows, preset, 'variant', fx.outcome_mix || null, null, { replicas: fx.replicas || 1 });
      close(r.total, ex.total, `${fx.name} total`);
      for (const [id, want] of Object.entries(ex.lines)) {
        const l = line(r, id);
        assert.ok(l, `${fx.name}: line ${id}`);
        close(l.quantity, want.quantity, `${fx.name} ${id} quantity`);
        close(l.money, want.money, `${fx.name} ${id} money`);
      }
    } else {
      const a = billing.allocateVolumes(fx.demand, preset, layouts, fx.manual || null);
      assert.equal(a.feasible, ex.feasible === undefined ? true : ex.feasible, `${fx.name} feasible`);
      if (ex.monthly_cost !== undefined) close(a.monthly_cost, ex.monthly_cost, `${fx.name} cost`);
      if (ex.layout) assert.equal(a.layout, ex.layout);
      if (ex.note_contains) assert.match(a.notes.join(' '), new RegExp(ex.note_contains));
      for (const [id, cost] of Object.entries(ex.alternatives || {})) {
        close(a.alternatives.find((x) => x.layout === id).monthly_cost, cost, `${fx.name} alt ${id}`);
      }
    }
  }
});

test('test_outcome_mix_validation_and_unknown_outcome', () => {
  assert.throws(() => billing.bill([row('request', 'GetObject', 'all', 1)], P(), 'variant', { GetObject: { ok: 1.5, client_error: -0.5 } }), /GetObject/);
  assert.throws(() => billing.bill([row('request', 'GetObject', 'all', 1)], P(), 'variant', { GetObject: { ok: 'x' } }), /number/);
  assert.throws(() => billing.bill([row('request', 'GetObject', 'all', 1)], P(), 'variant', { GetObject: { okay: 1 } }), /one of/);
  assert.throws(() => billing.bill([row('request', 'GetObject', 'weird', 1)], P()), /unknown outcome_class/);
});

test('test_unbilled_outcome_classes_come_from_the_preset_free_list', () => {
  // Server errors, unknown outcomes and requests the store never saw are free because the preset says so, with a
  // source; take an entry away and that class is billed.
  const free = P().outcome_billing.free;
  for (const cls of ['server_error', 'dropped', 'not_sent']) {
    const entry = free.find((e) => e.outcome_class === cls);
    assert.ok(entry, `preset lists ${cls}`);
    assert.ok(entry.source_url || P().outcome_billing.source_url, cls);
    assert.equal(line(billing.bill([row('request', 'GetObject', cls, 10)], P()), 'get_and_other').quantity, 0);
    const without = P();
    without.outcome_billing = Object.assign({}, without.outcome_billing, { free: free.filter((e) => e.outcome_class !== cls) });
    assert.equal(line(billing.bill([row('request', 'GetObject', cls, 10)], without), 'get_and_other').quantity, 10, cls);
    const mixed = billing.bill([row('request', 'GetObject', 'all', 100)], without, 'variant', { GetObject: { ok: 0.5, [cls]: 0.5 } });
    assert.equal(line(mixed, 'get_and_other').quantity, 100, cls);
  }
});

test('test_block_lines_scale_with_replicas', () => {
  const rows = [row('block_capacity', 'capacity', 'ok', 100), row('block_io', 'iops', 'ok', 60000), row('block_io', 'mibps', 'ok', 500)];
  const one = billing.billVariant(rows, P(), null, null, { replicas: 1 });
  const two = billing.billVariant(rows, P(), null, null, { replicas: 2 });
  for (const l of one.lines) {
    const d = two.lines.find((x) => x.rule_id === l.rule_id);
    close(d.quantity, 2 * l.quantity, `${l.rule_id} quantity`);
    close(d.money, 2 * l.money, `${l.rule_id} money`);
  }
  close(two.total, 2 * one.total);
  const a1 = one.provisioning;
  const a2 = two.provisioning;
  assert.deepEqual([a2.volumes, a2.size_gib, a2.iops, a2.mibps], [a1.volumes, a1.size_gib, a1.iops, a1.mibps]);
  assert.equal(a2.replicas, 2);
  assert.equal(a2.per_replica.volumes, a1.volumes);
  assert.match(a1.note, /^[a-z0-9_]+: 1 replica × \(\d+ x \d+ GiB, \d+ IOPS, \d+ MiB\/s\)$/);
  assert.match(a2.note, /: 2 replicas × \(/);
  // Non-block rows are not scaled.
  const req = row('request', 'GetObject', 'ok', 1000);
  close(billing.billVariant([req, ...rows], P(), null, null, { replicas: 2 }).lines.find((l) => l.rule_id === 'get_and_other').money,
    billing.billVariant([req], P(), null, null, { replicas: 1 }).lines.find((l) => l.rule_id === 'get_and_other').money);
});

test('test_block_rows_need_explicit_replicas', () => {
  const rows = [row('block_capacity', 'capacity', 'ok', 100)];
  for (const o of [undefined, {}, { replicas: 0 }, { replicas: 1.5 }, { replicas: '2' }, { replicas: null }]) {
    assert.throws(() => billing.billVariant(rows, P(), null, null, o), /replicas/, JSON.stringify(o));
  }
  // Without block rows replicas is not needed.
  assert.doesNotThrow(() => billing.billVariant([row('request', 'GetObject', 'ok', 1)], P(), null, null));
});

test('test_allocation_keeps_the_outcome_mix', () => {
  const mix = { PutObject: { server_error: 1 } };
  const rows = [row('request', 'PutObject', 'all', 1000), row('request', 'PutObject', 'ok', 1000)];
  const b = billing.bill(rows, P(), 'variant', mix);
  close(line(b, 'put_copy_post_list').quantity, 1000);
  const cells = [{ key: 'a', name: 'a', rows: [rows[0]] }, { key: 'b', name: 'b', rows: [rows[1]] }];
  const a = billing.allocate(b.lines, cells, P(), mix);
  close(a.byCell.a.money, 0);
  close(a.byCell.b.money, b.total);
});

test('test_minimum_volume_goes_to_the_cells_that_declared_block_rows_even_at_zero_demand', () => {
  // A volume has a minimum size, so zero demand still costs money. It goes to the cells that hold block rows.
  const empty = row('block_capacity', 'capacity', 'all', 0, { scope: 'replica' });
  const other = row('request', 'GetObject', 'ok', 1000, { scope: 'replica' });
  const b = billing.bill([empty, other], P(), 'replica', null, null, { replicas: 1 });
  const cap = line(b, 'block_capacity');
  assert.ok(cap.money > 0);
  const a = billing.allocate(b.lines, [{ key: 'disk', name: 'disk', rows: [empty] }, { key: 'reads', name: 'reads', rows: [other] }], P(), null);
  assert.deepEqual(a.problems, []);
  const capOf = (res, key) => res.byCell[key].lines.filter((l) => l.rule_id === 'block_capacity').reduce((t, l) => t + l.money, 0);
  close(capOf(a, 'disk'), cap.money);
  close(capOf(a, 'reads'), 0);
  // Two such cells split it equally; a cell without block rows gets none.
  const two = billing.allocate(b.lines, [{ key: 'x', name: 'x', rows: [empty] }, { key: 'y', name: 'y', rows: [empty] }, { key: 'reads', name: 'reads', rows: [other] }], P(), null);
  close(capOf(two, 'x'), cap.money / 2);
  close(capOf(two, 'reads'), 0);
  // With no block row anywhere the money still has no cell.
  const none = billing.allocate(b.lines, [{ key: 'reads', name: 'reads', rows: [other] }], P(), null);
  assert.match(none.problems.join(';'), /block_capacity money .* has no cell to allocate to/);
});

test('test_zero_block_demand_bills_the_volume_floor_only', () => {
  const rows = [row('block_capacity', 'capacity', 'ok', 0), row('block_io', 'iops', 'ok', 0), row('block_io', 'mibps', 'ok', 0)];
  const b = billing.billVariant(rows, P(), null, null, { replicas: 2 });
  assert.equal(line(b, 'block_iops').money, 0);
  assert.equal(line(b, 'block_mibps').money, 0);
  const cap = line(b, 'block_capacity');
  assert.ok(cap.money > 0, 'a volume has a minimum size');
  close(cap.money, 2 * b.provisioning.per_replica.volumes * b.provisioning.per_replica.size_gib * P().block.gp3.price_per_gib_month);
});
