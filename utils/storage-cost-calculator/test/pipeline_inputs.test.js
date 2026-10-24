'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const pipeline = require('../pipeline.js');
const billing = require('../billing.js');

const COST = path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model');
const reg = JSON.parse(fs.readFileSync(path.join(COST, 'inputs-registry.json'), 'utf8'));
// resolveInputs reads only `registry` and `precedence`, so the real registry stands in for a full contract.
const REAL = { registry: reg.quantities, precedence: reg.precedence };
const close = (a, b, msg) => assert.ok(Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b)), `${msg || ''} ${a} != ${b}`);

const q = (id, extra) => Object.assign({
  id, kind: 'input', unit: 'count', value_type: 'number', enum: null, default: 1, rule: null, validity: null,
}, extra);
const inp = (id, dim) => ({ kind: 'input', id, dim });
const bin = (kind, dim, a, b) => ({ kind, dim, a, b, on_zero: 'unavailable' });

test('test_stock_flow_retention_any_two_locked', () => {
  const raw = { data_bytes: 1000, ingest_rate: 50, retention_days: 10 };
  // Locked {ingest, retention}: data derived.
  let r = pipeline.resolveInputs(REAL, { data_bytes: 1, ingest_rate: 50, retention_days: 10 }, ['ingest_rate', 'retention_days'], {});
  assert.deepEqual(r.problems, []);
  close(r.inputs.get('data_bytes'), 500);
  assert.equal(r.fields.data_bytes.derived, true);
  assert.equal(r.fields.data_bytes.locked, false);
  assert.match(r.fields.data_bytes.rule_text, /ingest_rate/);
  // Locked {data, retention}: ingest derived.
  r = pipeline.resolveInputs(REAL, raw, ['data_bytes', 'retention_days'], {});
  close(r.inputs.get('ingest_rate'), 100);
  assert.equal(r.fields.ingest_rate.derived, true);
  // Locked {data, ingest}: retention derived.
  r = pipeline.resolveInputs(REAL, raw, ['data_bytes', 'ingest_rate'], {});
  close(r.inputs.get('retention_days'), 20);
  assert.equal(r.fields.retention_days.derived, true);
  assert.equal(r.fields.data_bytes.derived, false);
});

test('test_lock_change_unlocks_oldest', () => {
  const next = pipeline.lockMember(REAL, ['ingest_rate', 'retention_days'], 'data_bytes');
  assert.deepEqual(next, ['retention_days', 'data_bytes']);
  const r = pipeline.resolveInputs(REAL, { data_bytes: 1000, ingest_rate: 50, retention_days: 10 }, next, {});
  assert.equal(r.fields.ingest_rate.derived, true);
  close(r.inputs.get('ingest_rate'), 100);
  // Locking an already locked member makes it the newest lock.
  assert.deepEqual(pipeline.lockMember(REAL, ['ingest_rate', 'retention_days'], 'ingest_rate'), ['retention_days', 'ingest_rate']);
  assert.throws(() => pipeline.lockMember(REAL, ['ingest_rate', 'retention_days'], 'replicas'), /not in a precedence group/);
});

test('test_override_locks_member_and_group_equation_holds', () => {
  const raw = { data_bytes: 1000, ingest_rate: 50, retention_days: 10 };
  const r = pipeline.resolveInputs(REAL, raw, ['ingest_rate', 'retention_days'], { data_bytes: 2000 });
  assert.deepEqual(r.problems, []);
  assert.equal(r.inputs.get('data_bytes'), 2000);
  assert.equal(r.fields.data_bytes.locked, true);
  assert.equal(r.fields.data_bytes.overridden, true);
  assert.equal(r.fields.data_bytes.derived, false);
  // The oldest other lock (ingest_rate) is released and derived from the equation.
  assert.equal(r.fields.ingest_rate.locked, false);
  assert.equal(r.fields.ingest_rate.derived, true);
  close(r.inputs.get('ingest_rate'), 200);
  close(r.inputs.get('data_bytes'), r.inputs.get('ingest_rate') * r.inputs.get('retention_days'));
  // Overriding a second member keeps both; the third is derived.
  const r2 = pipeline.resolveInputs(REAL, raw, ['ingest_rate', 'retention_days'], { data_bytes: 2000, retention_days: 5 });
  assert.equal(r2.fields.retention_days.overridden, true);
  assert.equal(r2.fields.ingest_rate.derived, true);
  close(r2.inputs.get('ingest_rate'), 400);
  // Three overrides over-determine the group. Overrides apply in member order, so the last member is rejected
  // whatever the key order.
  for (const over of [{ data_bytes: 2000, ingest_rate: 7, retention_days: 5 }, { retention_days: 5, ingest_rate: 7, data_bytes: 2000 }]) {
    const r3 = pipeline.resolveInputs(REAL, raw, ['ingest_rate', 'retention_days'], over);
    assert.equal(r3.problems.length, 1);
    assert.match(r3.problems[0], /override retention_days/);
    assert.equal(r3.fields.retention_days.overridden, false);
    assert.equal(r3.fields.ingest_rate.overridden, true);
    close(r3.inputs.get('retention_days'), 2000 / 7);
  }
});

test('test_override_survives_preset_change_and_is_marked', () => {
  const locks = ['ingest_rate', 'retention_days'];
  const over = { data_bytes: 2000, replicas: 4 };
  const a = pipeline.resolveInputs(REAL, { ingest_rate: 50, retention_days: 10, replicas: 3 }, locks, over);
  // A preset change replaces the raw values only.
  const b = pipeline.resolveInputs(REAL, { ingest_rate: 80, retention_days: 20, replicas: 2 }, locks, over);
  for (const r of [a, b]) {
    assert.equal(r.inputs.get('data_bytes'), 2000);
    assert.equal(r.inputs.get('replicas'), 4);
    assert.equal(r.fields.replicas.overridden, true);
    assert.equal(r.fields.replicas.derived, false);
  }
  close(a.inputs.get('ingest_rate'), 200);
  // retention_days stays locked at the raw 20, so ingest is derived from the override.
  close(b.inputs.get('ingest_rate'), 100);
  assert.equal(b.fields.ingest_rate.overridden, false);
  assert.deepEqual(b.lockOrder, locks);
});

test('test_reset_override_restores_default_locks', () => {
  const group = REAL.precedence[0];
  const raw = { data_bytes: 1000, ingest_rate: 50, retention_days: 10 };
  const withOverride = pipeline.resolveInputs(REAL, raw, group.default_locked, { data_bytes: 2000 });
  assert.equal(withOverride.fields.ingest_rate.derived, true);
  const reset = pipeline.resolveInputs(REAL, raw, withOverride.lockOrder, {});
  assert.equal(reset.fields.data_bytes.derived, true);
  assert.equal(reset.fields.data_bytes.overridden, false);
  for (const id of group.group) assert.equal(reset.fields[id].locked, group.default_locked.includes(id), id);
  close(reset.inputs.get('data_bytes'), 500);
  // An empty lock order means the defaults.
  assert.deepEqual(pipeline.resolveInputs(REAL, raw, [], {}).lockOrder, group.default_locked);
  // A partial order keeps its entry as the newest lock and fills the older slot from the defaults.
  assert.deepEqual(pipeline.resolveInputs(REAL, raw, ['data_bytes'], {}).lockOrder, ['ingest_rate', 'data_bytes']);
});

test('test_lock_order_roundtrips_as_state', () => {
  const raw = { data_bytes: 1000, ingest_rate: 50, retention_days: 10 };
  const order = pipeline.lockMember(REAL, pipeline.lockMember(REAL, ['ingest_rate', 'retention_days'], 'data_bytes'), 'ingest_rate');
  assert.deepEqual(order, ['data_bytes', 'ingest_rate']);
  const r = pipeline.resolveInputs(REAL, raw, order, {});
  assert.deepEqual(r.lockOrder, order);
  assert.deepEqual(pipeline.resolveInputs(REAL, raw, JSON.parse(JSON.stringify(r.lockOrder)), {}).lockOrder, order);
  // Overrides do not leak into the persisted order.
  assert.deepEqual(pipeline.resolveInputs(REAL, raw, order, { retention_days: 3 }).lockOrder, order);
  // Bad state is reported, never defaulted silently.
  const bad = pipeline.resolveInputs(REAL, raw, ['nope', 'data_bytes', 'ingest_rate', 'retention_days'], {});
  assert.ok(bad.problems.some((p) => /nope/.test(p)));
  assert.ok(bad.problems.some((p) => /precedence group|more than/.test(p)));
  assert.equal(bad.lockOrder.length, 2);
});

test('test_enum_input_validated', () => {
  const c = { registry: [q('shape', { value_type: 'enum', enum: ['narrow', 'wide'], default: 'narrow', unit: '' }), q('n')], precedence: [] };
  const ok = pipeline.resolveInputs(c, { shape: 'wide' }, [], {});
  assert.deepEqual(ok.problems, []);
  assert.equal(ok.inputs.get('shape'), 'wide');
  const bad = pipeline.resolveInputs(c, { shape: 'medium', n: 'x', ghost: 1 }, [], {});
  assert.equal(bad.problems.length, 3);
  assert.ok(bad.problems.some((p) => /shape.*medium/.test(p)));
  assert.ok(bad.problems.some((p) => /n.*finite number/.test(p)));
  assert.ok(bad.problems.some((p) => /ghost/.test(p)));
  assert.equal(bad.inputs.has('shape'), false);
  const badOver = pipeline.resolveInputs(c, {}, [], { shape: 'huge' });
  assert.ok(badOver.problems.some((p) => /shape.*huge/.test(p)));
  assert.equal(badOver.fields.shape.overridden, false);
});

test('test_normalise_rate_units', () => {
  close(pipeline.normaliseRate(1, 'day'), 1);
  close(pipeline.normaliseRate(1, 'second'), 86400);
  close(pipeline.normaliseRate(1, 'minute'), 1440);
  close(pipeline.normaliseRate(1, 'hour'), 24);
  // 730 h / 24 = 30.41666 days.
  close(pipeline.normaliseRate(730 / 24, 'month'), 1);
  assert.ok(Math.abs(pipeline.normaliseRate(304.167, 'month') - 10) < 1e-3);
  assert.throws(() => pipeline.normaliseRate(1, 'week'), /unknown rate unit week/);
  assert.throws(() => pipeline.normaliseRate(NaN, 'day'), /finite/);
  // A raw rate carries its unit; the resolved input is per day.
  const r = pipeline.resolveInputs(REAL, { ingest_rate: { value: 24, per: 'hour' }, retention_days: 10, data_bytes: 1 }, ['ingest_rate', 'retention_days'], {});
  close(r.inputs.get('ingest_rate'), 576);
  close(r.inputs.get('data_bytes'), 5760);
  const bad = pipeline.resolveInputs(REAL, { ingest_rate: { value: 1, per: 'week' } }, ['ingest_rate', 'retention_days'], {});
  assert.ok(bad.problems.some((p) => /week/.test(p)));
  const notRate = pipeline.resolveInputs(REAL, { replicas: { value: 1, per: 'day' } }, [], {});
  assert.ok(notRate.problems.some((p) => /replicas.*not a rate/.test(p)));
});

test('test_out_of_range_flag_on_field', () => {
  const r = pipeline.resolveInputs(REAL, { replicas: 9 }, [], {});
  assert.equal(r.fields.replicas.out_of_range, true);
  assert.deepEqual(r.fields.replicas.validity, [1, 6]);
  assert.deepEqual(r.problems, []);
  assert.equal(pipeline.resolveInputs(REAL, { replicas: 3 }, [], {}).fields.replicas.out_of_range, false);
  assert.equal(pipeline.resolveInputs(REAL, {}, [], { replicas: 7 }).fields.replicas.out_of_range, true);
  assert.equal(r.fields.retention_days.out_of_range, false);
  assert.equal(r.fields.retention_days.validity, null);
});

test('test_manual_provisioning_inputs_reach_allocator', () => {
  const c = {
    registry: [
      q('provisioned_iops', { unit: 'iops', default: 3000 }),
      q('provisioned_mibps', { unit: 'mibps', default: 125 }),
      q('volumes_per_replica', { value_type: 'enum', enum: ['auto'], default: 'auto', unit: '' }),
    ],
    precedence: [],
  };
  const auto = pipeline.resolveInputs(c, {}, [], {});
  assert.equal(auto.inputs.get('volumes_per_replica'), 'auto');
  assert.equal(auto.fields.volumes_per_replica.overridden, false);
  const r = pipeline.resolveInputs(c, { provisioned_iops: 3000, provisioned_mibps: 125, volumes_per_replica: 1 }, [], {});
  assert.deepEqual(r.problems, []);
  const manual = { volumes: r.inputs.get('volumes_per_replica'), size_gib: 50, iops: r.inputs.get('provisioned_iops'), mibps: r.inputs.get('provisioned_mibps') };
  const preset = JSON.parse(fs.readFileSync(path.join(COST, 'billing-contract.json'), 'utf8')).presets.find((x) => x.id === 'aws-us-east-1');
  const alloc = billing.allocateVolumes({ capacity_gib: 100, peak_iops: 6000, peak_mibps: 200 }, preset, undefined, manual);
  assert.equal(alloc.manual, true);
  assert.equal(alloc.feasible, true);
  // 50 GiB x 0.08 per GiB-month; 3000 IOPS and 125 MiB/s are the gp3 baselines, so they add nothing.
  assert.equal(alloc.monthly_cost, 4);
  // A bad volume count is rejected as input, before it reaches the allocator.
  const bad = pipeline.resolveInputs(c, { volumes_per_replica: 'many' }, [], {});
  assert.ok(bad.problems.some((p) => /volumes_per_replica.*many/.test(p)));
  assert.ok(pipeline.resolveInputs(c, { volumes_per_replica: 1.5 }, [], {}).problems.some((p) => /volumes_per_replica.*1\.5/.test(p)));
});

test('test_denormalise_rate_inverts_normalise_rate', () => {
  for (const unit of ['second', 'minute', 'hour', 'day', 'month']) {
    const perDay = pipeline.normaliseRate(3, unit);
    assert.ok(Math.abs(pipeline.denormaliseRate(perDay, unit) - 3) < 1e-12, unit);
  }
  assert.equal(pipeline.denormaliseRate(48, 'hour'), 2);
  assert.throws(() => pipeline.denormaliseRate(1, 'week'), /unknown rate unit week/);
});
