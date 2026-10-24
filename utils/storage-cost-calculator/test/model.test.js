'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const model = require('../model.js');

const EX = path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'schemas', 'examples');
const load = (f) => JSON.parse(fs.readFileSync(path.join(EX, f), 'utf8'));

const k = (value, dim = 'count') => ({ kind: 'const', value, dim });
const inp = (id, dim = 'count') => ({ kind: 'input', id, dim });
const bin = (kind, a, b, dim = 'count', extra = {}) => Object.assign({ kind, dim, a, b }, extra);

function ctx(over = {}) {
  return Object.assign({
    inputs: new Map(), coefs: new Map(), tables: new Map(), sets: new Map(), vars: new Map(), prices: null,
  }, over);
}

test('test_const_input_coef_lookup', () => {
  const c = ctx({
    inputs: new Map([['n', 7], ['shape', 'narrow']]),
    coefs: new Map([['c', { kind: 'scalar', value: 2.5, validity: null, dim: 'count' }]]),
    tables: new Map([['t', { index: ['shape'], values: { narrow: 11 }, dim: 'count' }]]),
  });
  assert.equal(model.evaluate(k(3), c).value, 3);
  assert.equal(model.evaluate(inp('n'), c).value, 7);
  assert.equal(model.evaluate({ kind: 'coef', id: 'c', dim: 'count' }, c).value, 2.5);
  const lk = { kind: 'lookup', dim: 'count', table_id: 't', keys: [inp('shape', 'enum:shape')] };
  const r = model.evaluate(lk, c);
  assert.equal(r.value, 11);
  assert.equal(r.trace.lookup_key, 'narrow');
  assert.equal(model.evaluate(inp('missing'), c).state, 'unavailable');
  const e = model.evaluate(k('compact', 'enum:part_type'), c);
  assert.equal(e.value, 'compact');
});

test('test_dims_combine_and_mismatch', () => {
  const registry = load('registry.json');
  const b = (id) => inp(id, 'bytes');
  assert.deepEqual(model.checkDims(bin('add', b('uncompressed_bytes_per_part'), k(1, 'bytes'), 'bytes'), registry), []);
  assert.deepEqual(model.checkDims(bin('mul', b('uncompressed_bytes_per_part'), k(2, '1'), 'bytes'), registry), []);
  const mismatch = model.checkDims(bin('add', b('uncompressed_bytes_per_part'), k(1, 'count'), 'bytes'), registry);
  assert.equal(mismatch.length, 1);
  assert.match(mismatch[0], /^\$: add operands bytes and 1 must both equal bytes/);
  const mulBad = model.checkDims(bin('mul', b('uncompressed_bytes_per_part'), k(2, '1'), 'count'), registry);
  assert.match(mulBad[0], /^\$: mul of bytes and 1 is bytes, node declares 1/);
  const noDim = model.checkDims({ kind: 'add', a: k(1), b: k(1) }, registry);
  assert.deepEqual(noDim, ['$: missing dim']);
  const unknown = model.checkDims(bin('add', k(1), inp('nope'), 'count'), registry);
  assert.deepEqual(unknown, ['$/b: unknown input id nope']);
  assert.deepEqual(model.checkDims({ kind: 'var', name: 'x', dim: 'count' }, registry), ['$: unbound var x']);
  assert.deepEqual(model.checkDims({ kind: 'zap', dim: 'count' }, registry), ['$: unknown kind zap']);
  assert.deepEqual(model.checkDims({ kind: 'price', dim: 'count', unit: 'u', rule_id: 'r', quantity: k(1) }, registry),
    ['$: price must have dim USD, got 1']);
  assert.deepEqual(model.checkDims(load('example-part-type.json'), registry), []);
  assert.deepEqual(model.checkDims(load('example-objects-per-part.json'), registry), []);
});

test('test_div_on_zero_policies', () => {
  const zero = model.evaluate(bin('div', k(5), k(0), 'count', { on_zero: 'zero' }), ctx());
  assert.equal(zero.state, 'ok');
  assert.equal(zero.value, 0);
  const un = model.evaluate(bin('div', k(5), k(0), 'count', { on_zero: 'unavailable' }), ctx());
  assert.equal(un.state, 'unavailable');
  assert.equal(un.value, null);
  assert.equal(model.evaluate(bin('div', k(5), k(2), 'count', { on_zero: 'zero' }), ctx()).value, 2.5);
});

test('test_branch_records_branch_taken', () => {
  const lt = (a, b) => ({ kind: 'branch', dim: 'count', cond: { op: '<', a: k(a), b: k(b) }, then: k(10),
    else: { kind: 'input', id: 'absent', dim: 'count' } });
  const t = model.evaluate(lt(1, 2), ctx());
  assert.equal(t.trace.branch_taken, 'then');
  assert.equal(t.value, 10);
  // The untaken side would be unavailable; taking 'then' must not be affected.
  assert.equal(t.state, 'ok');
  const e = model.evaluate(lt(2, 1), ctx());
  assert.equal(e.trace.branch_taken, 'else');
  assert.equal(e.state, 'unavailable');
  assert.equal(e.trace.children.length, 3);
  assert.equal(t.trace.children.length, 3);
  const en = model.evaluate({ kind: 'branch', dim: 'count', cond: { op: '==', a: k('a', 'enum:x'), b: k('a', 'enum:x') },
    then: k(1), else: k(2) }, ctx());
  assert.equal(en.value, 1);
});

test('test_sum_over_binds_var', () => {
  const c = ctx({
    inputs: new Map([['shape', 'narrow']]),
    tables: new Map([['objects_per_part', { index: ['shape', 'part_type'], dim: 'count', values: { 'narrow|compact': 3, 'narrow|wide': 8 } }]]),
    sets: new Map([['part_types', ['compact', 'wide']]]),
  });
  const r = model.evaluate(load('example-objects-per-part.json'), c);
  assert.equal(r.value, 11);
  assert.deepEqual(r.trace.children.map((t) => t.bound), [
    { var: 'part_type', member: 'compact' }, { var: 'part_type', member: 'wide' }]);
  assert.equal(c.vars.size, 0);
});

test('test_lookup_missing_key_unavailable', () => {
  const c = ctx({
    inputs: new Map([['shape', 'wide_columns']]),
    tables: new Map([['t', { index: ['shape'], dim: 'count', values: { narrow: 1 } }]]),
  });
  const lk = { kind: 'lookup', dim: 'count', table_id: 't', keys: [inp('shape', 'enum:shape')] };
  const r = model.evaluate(lk, c);
  assert.equal(r.state, 'unavailable');
  assert.equal(r.trace.lookup_key, 'wide_columns');
  assert.equal(model.evaluate({ ...lk, table_id: 'nope' }, c).state, 'unavailable');
});

test('test_coef_outside_validity_extrapolated', () => {
  const coefs = new Map([['c', { kind: 'scalar', value: 4, dim: 'count', validity: { size: [10, 100] } }]]);
  const node = bin('add', { kind: 'coef', id: 'c', dim: 'count' }, k(1));
  const inside = model.evaluate(node, ctx({ coefs, inputs: new Map([['size', 50]]) }));
  assert.equal(inside.extrapolated, false);
  const outside = model.evaluate(node, ctx({ coefs, inputs: new Map([['size', 101]]) }));
  assert.equal(outside.extrapolated, true);
  assert.equal(outside.value, 5);
  assert.equal(outside.trace.children[0].extrapolated, true);
  const lowEdge = model.evaluate(node, ctx({ coefs, inputs: new Map([['size', 10]]) }));
  assert.equal(lowEdge.extrapolated, false);
});

test('test_unavailable_propagates', () => {
  const un = inp('absent');
  const c = ctx({ sets: new Map([['s', ['a', 'b']]]) });
  for (const kind of ['add', 'sub', 'mul', 'min', 'max', 'div']) {
    const extra = kind === 'div' ? { on_zero: 'zero' } : {};
    assert.equal(model.evaluate(bin(kind, k(1), un, 'count', extra), c).state, 'unavailable', kind);
    assert.equal(model.evaluate(bin(kind, un, k(1), 'count', extra), c).state, 'unavailable', kind);
  }
  const lk = { kind: 'lookup', dim: 'count', table_id: 't', keys: [un] };
  assert.equal(model.evaluate(lk, ctx({ tables: new Map([['t', { index: ['a'], dim: 'count', values: {} }]]) })).state, 'unavailable');
  const br = { kind: 'branch', dim: 'count', cond: { op: '<', a: un, b: k(1) }, then: k(1), else: k(2) };
  assert.equal(model.evaluate(br, c).state, 'unavailable');
  assert.equal(model.evaluate({ kind: 'var', name: 'v', dim: 'count' }, ctx({ vars: new Map([['v', null]]) })).state, 'unavailable');
  assert.equal(model.evaluate({ kind: 'ceil_to', dim: 'count', x: un, step: k(1) }, c).state, 'unavailable');
  assert.equal(model.evaluate({ kind: 'floor_to', dim: 'count', x: k(1), step: un }, c).state, 'unavailable');
  assert.equal(model.evaluate({ kind: 'sum_over', dim: 'count', set: 's', var: 'v', body: un }, c).state, 'unavailable');
  assert.equal(model.evaluate(k('unavailable'), c).state, 'unavailable');
  const pr = model.evaluate({ kind: 'price', dim: 'USD', unit: 'u', rule_id: 'r', quantity: un }, c);
  assert.equal(pr.state, 'unavailable');
  assert.equal(pr.value, null);
});

test('test_render_symbolic_fixture', () => {
  const registry = load('registry.json');
  assert.equal(model.render(load('example-part-type.json'), registry),
    'if uncompressed_bytes_per_part < min_bytes_for_wide_part then compact else wide');
  assert.equal(model.render(load('example-objects-per-part.json'), registry),
    'Σ_{part_type ∈ part_types} lookup(objects_per_part)[shape, part_type]');
  assert.equal(model.render(load('example-multipart-parts.json'), registry),
    'if object_size_bytes ≤ first_buffer_bytes then 1 else 1 + ceil_to((object_size_bytes − first_buffer_bytes) / part_size_bytes, 1)');
  const mul = bin('mul', inp('PUT_per_part'), inp('parts_per_day'));
  assert.equal(model.render(mul, registry), 'PUT_per_part × parts_per_day');
  assert.equal(model.render(bin('mul', bin('add', inp('a'), inp('b')), inp('c')), registry), '(a + b) × c');
  assert.equal(model.render(bin('sub', inp('a'), bin('sub', inp('b'), inp('c'))), registry), 'a − (b − c)');
  assert.equal(model.render(bin('sub', bin('sub', inp('a'), inp('b')), inp('c')), registry), 'a − b − c');
  assert.equal(model.render({ kind: 'price', dim: 'USD', unit: 'GiB_month', rule_id: 'storage', quantity: inp('gib') }, registry),
    'price(unit=GiB_month, rule=storage, gib)');
  const reordered = { ...registry, quantities: [...registry.quantities].reverse() };
  const fx = load('example-multipart-parts.json');
  assert.equal(model.render(fx, reordered), model.render(fx, registry));
});

test('test_fractional_values_not_rounded_without_node', () => {
  const r = model.evaluate(bin('mul', k(0.1), k(1234)), ctx());
  assert.equal(r.value, 0.1 * 1234);
  assert.equal(r.value, 123.4);
  const d = model.evaluate(bin('div', k(1), k(3), 'count', { on_zero: 'zero' }), ctx());
  assert.equal(d.value, 1 / 3);
});

test('test_three_worked_examples_from_schemas', () => {
  // part type: compact below min_bytes_for_wide_part, wide at and above.
  const coefs = new Map([['min_bytes_for_wide_part', { kind: 'scalar', value: 10485760, dim: 'bytes', validity: null }]]);
  const partType = (bytes) => model.evaluate(load('example-part-type.json'),
    ctx({ coefs, inputs: new Map([['uncompressed_bytes_per_part', bytes]]) })).value;
  assert.equal(partType(10485759), 'compact');
  assert.equal(partType(10485760), 'wide');

  // objects per part: lookup keyed by shape and part_type.
  const tables = new Map([['objects_per_part', { index: ['shape', 'part_type'], dim: 'count',
    values: { 'narrow|compact': 3, 'narrow|wide': 8 } }]]);
  const sets = new Map([['part_types', ['compact', 'wide']]]);
  assert.equal(model.evaluate(load('example-objects-per-part.json'),
    ctx({ tables, sets, inputs: new Map([['shape', 'narrow']]) })).value, 11);

  // multipart parts: 1 up to the first buffer, else 1 + ceil((size - first_buffer) / part_size).
  const mp = new Map([
    ['first_buffer_bytes', { kind: 'scalar', value: 100, dim: 'bytes', validity: null }],
    ['part_size_bytes', { kind: 'scalar', value: 50, dim: 'bytes', validity: null }],
  ]);
  const parts = (size) => model.evaluate(load('example-multipart-parts.json'),
    ctx({ coefs: mp, inputs: new Map([['object_size_bytes', size]]) }));
  assert.equal(parts(99).value, 1);
  assert.equal(parts(100).value, 1);
  assert.equal(parts(101).value, 2);
  assert.equal(parts(150).value, 2);
  assert.equal(parts(151).value, 3);
  assert.equal(parts(101).trace.branch_taken, 'else');
});

test('test_evaluate_cell_formulas_example', () => {
  const cell = load('formulas.json').cells.insert;
  const c = ctx({
    inputs: new Map([['parts_inserted', 4], ['shape', 'narrow']]),
    tables: new Map([['objects_per_part', { index: ['shape', 'part_type'], dim: 'count', values: { 'narrow|compact': 3 } }]]),
  });
  const r = model.evaluateCell(cell, c);
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].quantity, 12);
  assert.equal(r.rows[0].op_kind, 'PutObject');
  assert.equal(r.rows[0].expr, undefined);
  assert.deepEqual(r.allocation, { insert: 1 });
  assert.deepEqual(r.states, [{ id: 'not_supported', active: false, reason: 'wide_columns shape is out of the supported domain' }]);
  assert.equal(r.extrapolated, false);
  const w = model.evaluateCell(cell, ctx({ ...c, inputs: new Map([['parts_inserted', 4], ['shape', 'wide_columns']]) }));
  assert.equal(w.rows[0].state, 'unavailable');
  assert.equal(w.rows[0].quantity, null);
  assert.equal(w.states[0].active, true);
});

test('test_non_finite_and_invalid_values_unavailable', () => {
  const run = (node, c) => model.evaluate(node, ctx(c));
  const nan = run(inp('x'), { inputs: new Map([['x', NaN]]) });
  assert.equal(nan.state, 'unavailable');
  assert.equal(nan.value, null);
  assert.match(nan.trace.reason, /input x: non-finite value NaN/);
  assert.equal(run(inp('x'), { inputs: new Map([['x', null]]) }).state, 'unavailable');
  assert.match(run(inp('x'), { inputs: new Map([['x', null]]) }).trace.reason, /input x: invalid value null/);
  assert.equal(run(inp('x'), { inputs: new Map([['x', 'str']]) }).state, 'unavailable');
  const coef = { kind: 'coef', id: 'c', dim: 'count' };
  const inf = run(coef, { coefs: new Map([['c', { kind: 'scalar', value: Infinity, validity: null }]]) });
  assert.equal(inf.state, 'unavailable');
  assert.match(inf.trace.reason, /coef c: non-finite value Infinity/);
  const tbl = (v) => ({ tables: new Map([['t', { index: ['a'], dim: 'count', values: { k: v } }]]) });
  const lk = { kind: 'lookup', dim: 'count', table_id: 't', keys: [k('k', 'enum:a')] };
  assert.equal(run(lk, tbl(null)).state, 'unavailable');
  assert.match(run(lk, tbl(null)).trace.reason, /table t\[k\]/);
  assert.equal(run(lk, tbl('x')).state, 'unavailable');
  assert.equal(run(lk, tbl(5)).value, 5);
  const z = run(bin('mul', k(0), k(Infinity)), {});
  assert.equal(z.state, 'unavailable');
  const zi = run(bin('mul', k(0), inp('i')), { inputs: new Map([['i', Infinity]]) });
  assert.equal(zi.state, 'unavailable');
  const big = run(bin('mul', k(1e308), k(10)), {});
  assert.equal(big.state, 'unavailable');
  assert.match(big.trace.reason, /non-finite result/);
});

test('test_lookup_arity', () => {
  const c = ctx({ tables: new Map([['t', { index: ['a', 'b'], dim: 'count', values: { 'x|y': 1, x: 2 } }]]) });
  const one = { kind: 'lookup', dim: 'count', table_id: 't', keys: [k('x', 'enum:a')] };
  const r = model.evaluate(one, c);
  assert.equal(r.state, 'unavailable');
  assert.equal(r.trace.reason, 'lookup arity');
  const two = { ...one, keys: [k('x', 'enum:a'), k('y', 'enum:b')] };
  assert.equal(model.evaluate(two, c).value, 1);
  const registry = load('registry.json');
  const bad = { kind: 'lookup', dim: 'count', table_id: 'objects_per_part', keys: [inp('shape', 'enum:shape')] };
  assert.deepEqual(model.checkDims(bad, registry),
    ['$: lookup objects_per_part has 1 keys, table index has 2']);
  assert.deepEqual(model.checkDims(load('example-objects-per-part.json'), registry), []);
});

test('test_dim_algebra', () => {
  const registry = { quantities: [] };
  const n = (kind, a, b, dim) => ({ kind, dim, a, b });
  const c = (dim) => ({ kind: 'const', value: 1, dim });
  assert.equal(model.checkDims(n('mul', c('GiB'), c('req'), 'count'), registry).length, 1);
  assert.equal(model.checkDims(n('div', c('1'), c('GiB'), 'GiB'), registry).length, 1);
  assert.deepEqual(model.checkDims(n('mul', c('GiB'), c('h'), 'GiB_h'), registry), []);
  assert.deepEqual(model.checkDims(n('div', c('req'), c('s'), 'iops'), registry), []);
  assert.deepEqual(model.checkDims(n('mul', c('iops'), c('s'), 'req'), registry), []);
  assert.deepEqual(model.checkDims(n('add', c('count'), c('1'), 'count'), registry), []);
  assert.deepEqual(model.checkDims(n('div', c('GiB'), c('GiB'), 'count'), registry), []);
  const cl = (x, step, dim) => ({ kind: 'ceil_to', dim, x, step });
  assert.deepEqual(model.checkDims(cl(c('GiB'), c('GiB'), 'GiB'), registry), []);
  assert.deepEqual(model.checkDims(cl(c('GiB'), c('1'), 'GiB'), registry), []);
  assert.equal(model.checkDims(cl(c('GiB'), c('h'), 'GiB'), registry).length, 1);
  const reg = { quantities: [
    { id: 'p', unit: 'GiB', value_type: 'number', coef: 'p' },
    { id: 'tab', value_type: 'table', unit: 'objects', table: { index: ['a'], dim: 'count' }, coef: 'tab' }] };
  assert.equal(model.checkDims({ kind: 'coef', id: 'p', dim: 'req' }, reg).length, 1);
  assert.deepEqual(model.checkDims({ kind: 'coef', id: 'p', dim: 'GiB' }, reg), []);
  assert.deepEqual(model.checkDims({ kind: 'lookup', dim: 'count', table_id: 'tab', keys: [c('enum:a')] }, reg), []);
  assert.equal(model.checkDims({ kind: 'lookup', dim: 'GiB', table_id: 'tab', keys: [c('enum:a')] }, reg).length, 1);
});

test('test_check_dims_formulas_example_clean', () => {
  const registry = load('registry.json');
  const cell = load('formulas.json').cells.insert;
  const nodes = [...cell.ledger_rows.map((r) => r.expr), ...cell.allocation.shares.map((x) => x.weight),
    ...cell.states.map((x) => x.cond), ...cell.notes];
  for (const node of nodes) assert.deepEqual(model.checkDims(node, registry), []);
});

test('test_extrapolated_propagates_and_empty_sum', () => {
  const coefs = new Map([['c', { kind: 'scalar', value: 4, dim: 'count', validity: { size: [10, 100] } }]]);
  const c = ctx({ coefs, inputs: new Map([['size', 500]]), sets: new Map([['s', ['a']], ['e', []]]) });
  const cf = { kind: 'coef', id: 'c', dim: 'count' };
  const br = { kind: 'branch', dim: 'count', cond: { op: '<', a: cf, b: k(100) }, then: k(1), else: k(2) };
  assert.equal(model.evaluate(br, c).extrapolated, true);
  const brThen = { kind: 'branch', dim: 'count', cond: { op: '<', a: k(1), b: k(2) }, then: cf, else: k(2) };
  assert.equal(model.evaluate(brThen, c).extrapolated, true);
  assert.equal(model.evaluate({ kind: 'sum_over', dim: 'count', set: 's', var: 'v', body: cf }, c).extrapolated, true);
  assert.equal(model.evaluate({ kind: 'ceil_to', dim: 'count', x: cf, step: k(1) }, c).extrapolated, true);
  assert.equal(model.evaluate({ kind: 'price', dim: 'USD', unit: 'u', rule_id: 'r', quantity: cf }, c).extrapolated, true);
  const empty = model.evaluate({ kind: 'sum_over', dim: 'count', set: 'e', var: 'v', body: cf }, c);
  assert.equal(empty.state, 'ok');
  assert.equal(empty.value, 0);
});

test('test_evaluate_cell_fails_closed', () => {
  const un = inp('absent');
  const row = { row_type: 'request', op_kind: 'PutObject', outcome_class: 'ok', category: null, route: null, scope: 'variant', expr: k(1) };
  const a = model.evaluateCell({ ledger_rows: [row], allocation: null,
    states: [{ id: 's', cond: bin('add', un, k(1)), reason: 'r' }], notes: [] }, ctx());
  assert.equal(a.states[0].active, null);
  assert.equal(a.state, 'unavailable');
  assert.match(a.reason, /state s condition unavailable/);
  const b = model.evaluateCell({ ledger_rows: [row],
    allocation: { shares: [{ cell: 'x', weight: k(1) }, { cell: 'y', weight: un }] }, states: [], notes: [] }, ctx());
  assert.equal(b.allocation, null);
  assert.equal(b.state, 'unavailable');
  assert.match(b.reason, /allocation weight for y unavailable/);
  const d = model.evaluateCell({ ledger_rows: [row], allocation: null }, ctx());
  assert.deepEqual(d.states, []);
  assert.equal(d.state, 'ok');
});

test('test_evaluate_cell_notes', () => {
  const row = { row_type: 'request', op_kind: 'PutObject', outcome_class: 'ok', category: null, route: null, scope: 'variant', expr: k(1) };
  const r = model.evaluateCell({ ledger_rows: [row], allocation: null, states: [],
    notes: [k(2), inp('absent'), k('unavailable')] }, ctx());
  assert.deepEqual(r.notes.map((n) => [n.value, n.state]), [[2, 'ok'], [null, 'unavailable'], [null, 'unavailable']]);
  assert.equal(r.notes[0].trace.kind, 'const');
  assert.equal(r.state, 'ok');
  assert.deepEqual(model.evaluateCell({ ledger_rows: [row], allocation: null }, ctx()).notes, []);
});
