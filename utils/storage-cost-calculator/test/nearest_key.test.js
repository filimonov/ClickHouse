'use strict';
// A lookup miss on an enum key uses the nearest measured key, marks the number extrapolated and says so.
const test = require('node:test');
const assert = require('node:assert/strict');
const model = require('../model.js');

const table = { index: ['shape', 'bucket'], values: { 'narrow|8KiB': 1, 'wide|8KiB': 2, 'wide|1MiB': 3 }, validity: null };
const order = (xs) => ({ order: xs });
const nearest = new Map([['shape', order(['narrow', 'wide', 'types'])], ['bucket', order(['4KiB', '8KiB', '64KiB', '1MiB'])]]);
const key = (id, value) => ({ kind: 'const', value, dim: `enum:${id}` });
const lk = (a, b) => ({ kind: 'lookup', dim: 'count', table_id: 't', keys: [key('shape', a), key('bucket', b)] });
const ctx = (n) => ({ inputs: new Map(), coefs: new Map(), tables: new Map([['t', table]]), sets: new Map(), vars: new Map(), prices: null, nearest: n });

test('test_nearest_key_by_enum_order_and_map', () => {
  assert.equal(model.nearestKey(['types', '8KiB'], ['enum:shape', 'enum:bucket'], table, nearest), 'wide|8KiB');
  assert.equal(model.nearestKey(['narrow', '64KiB'], ['enum:shape', 'enum:bucket'], table, nearest), 'narrow|8KiB');
  assert.equal(model.nearestKey(['wide', '4KiB'], ['enum:shape', 'enum:bucket'], table, nearest), 'wide|8KiB');
  // A map names the stand-in explicitly; a part with no order or map must match.
  assert.equal(model.nearestKey(['types', '1MiB'], ['enum:shape', 'enum:bucket'], table, new Map([['shape', { map: { types: 'wide' } }]])), 'wide|1MiB');
  assert.equal(model.nearestKey(['types', '1MiB'], ['enum:shape', 'count'], table, new Map()), null);
});

test('test_substituted_lookup_is_extrapolated_and_traced', () => {
  const r = model.evaluate(lk('types', '1MiB'), ctx(nearest));
  assert.equal(r.state, 'ok');
  assert.equal(r.value, 3);
  assert.equal(r.extrapolated, true);
  assert.equal(r.trace.substituted_key, 'wide|1MiB');
  assert.equal(r.trace.lookup_key, 'types|1MiB');
  // Without a nearest map (tools, tests) a miss stays unavailable; an exact hit is not extrapolated.
  assert.equal(model.evaluate(lk('types', '1MiB'), ctx(undefined)).state, 'unavailable');
  assert.equal(model.evaluate(lk('wide', '1MiB'), ctx(nearest)).extrapolated, false);
});
