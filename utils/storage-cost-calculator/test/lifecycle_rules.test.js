'use strict';
// The lifecycle (R5) derivations of inputs-registry.json, evaluated with model.js at their boundaries and branches.
// Expected values come from tmp/src-26.8 (docs/superpowers/reports/2026-10-24-storage-cost-lifecycle/README.md).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const model = require('../model.js');

const REGISTRY = process.env.SCC_REGISTRY ||
  path.join(__dirname, '..', '..', '..', 'docs', 'superpowers', 'cost-model', 'inputs-registry.json');
const reg = JSON.parse(fs.readFileSync(REGISTRY, 'utf8'));
const MIB = 1 << 20;

// Evaluates `id` with the registry defaults, `over` replacing inputs, derivations evaluated on demand.
function evaluate(id, over = {}, fanIn = 6.1) {
  const byId = new Map(reg.quantities.map((q) => [q.id, q]));
  const inputs = new Map(reg.quantities.filter((q) => q.kind !== 'derivation').map((q) => [q.id, q.default]));
  Object.entries(over).forEach(([k, v]) => inputs.set(k, v));
  const coefs = new Map([['merge_effective_fan_in', { kind: 'scalar', value: fanIn, validity: null, dim: '1' }]]);
  const ctx = { inputs, coefs, tables: new Map(), sets: new Map(), vars: new Map(), prices: null };
  const need = (q) => {
    const out = [];
    const walk = (n) => {
      if (n && typeof n === 'object') {
        if (n.kind === 'input' && byId.get(n.id) && byId.get(n.id).kind === 'derivation' && !inputs.has(n.id)) out.push(n.id);
        Object.values(n).forEach(walk);
      }
    };
    walk(q.rule);
    return out;
  };
  const solve = (qid) => {
    if (inputs.has(qid)) return inputs.get(qid);
    const q = byId.get(qid);
    need(q).forEach(solve);
    assert.deepEqual(model.checkDims(q.rule, reg), [], qid);
    const r = model.evaluate(q.rule, ctx);
    inputs.set(qid, r.value);
    return r.value;
  };
  return solve(id);
}

test('part_type: strict byte threshold, level, and the Wide overrides', () => {
  assert.equal(evaluate('part_type', { insert_bytes_uncompressed: 10 * MIB - 40, insert_rows: 262143 }), 'compact');
  assert.equal(evaluate('part_type', { insert_bytes_uncompressed: 10 * MIB, insert_rows: 262144 }), 'wide');
  assert.equal(evaluate('part_type', { insert_bytes_uncompressed: 11 * MIB, min_level_for_wide_part: 1 }), 'compact');
  assert.equal(evaluate('part_type', { insert_bytes_uncompressed: 1000, adaptive_granularity: '0' }), 'wide');
  assert.equal(evaluate('part_type', { insert_bytes_uncompressed: 1000, quantize_pq_codec: '1' }), 'wide');
});

test('multipart: exactly the first buffer is one PutObject; the strict size splits smaller objects', () => {
  assert.equal(evaluate('multipart_parts', { object_bytes: 32 * MIB }), 1);
  assert.equal(evaluate('multipart_upload', { object_bytes: 32 * MIB }), 0);
  assert.equal(evaluate('multipart_parts', { object_bytes: 32 * MIB + 8 }), 2);
  assert.equal(evaluate('multipart_upload', { object_bytes: 32 * MIB + 8 }), 1);
  assert.equal(evaluate('multipart_parts', { object_bytes: 48 * MIB + 8 }), 3);
  assert.equal(evaluate('multipart_parts', { object_bytes: 20 * MIB, strict_upload_part_size: 16 * MIB }), 2);
  assert.equal(evaluate('multipart_upload', { object_bytes: 20 * MIB, strict_upload_part_size: 16 * MIB }), 1);
});

test('parts_per_insert: whole source blocks per squashed part; INSERT SELECT is never async', () => {
  const select = { insert_kind: 'select', insert_rows: 111195300, insert_bytes_uncompressed: 111195300 * 40 };
  assert.equal(evaluate('parts_per_insert', select), 100);
  assert.equal(evaluate('parts_per_insert', { insert_kind: 'select', insert_rows: 2621122,
                                             insert_bytes_uncompressed: 2621122 * 40 }), 3);
  assert.equal(evaluate('parts_per_insert', { insert_kind: 'data', insert_rows: 2621122,
                                             insert_bytes_uncompressed: 2621122 * 40, async_insert: '0' }), 3);
  // The default single waiting client never shares a flush.
  assert.equal(evaluate('parts_per_insert', { insert_kind: 'data' }), 1);
});

test('async flush: first insert plus the timeout window, whole entries, one per waiting client', () => {
  assert.equal(evaluate('inserts_per_async_flush', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 3000 }), 5);
  assert.equal(evaluate('inserts_per_async_flush', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 6 * MIB }), 2);
  assert.equal(evaluate('inserts_per_async_flush', { insert_rate: 20, insert_clients: 1, insert_payload_bytes: 3000 }), 1);
  assert.equal(evaluate('parts_per_insert', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 3000 }), 0.2);
  assert.equal(evaluate('parts_per_insert', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 11 * MIB,
                                             insert_rows: 1000, insert_bytes_uncompressed: 40000 }), 1);
});

test('write amplification: continuous ladder, capped at 12 levels', () => {
  const f = 5;
  assert.equal(evaluate('write_amplification_rule', { merge_size_ratio: 1 }, f), 1);
  assert.ok(Math.abs(evaluate('write_amplification_rule', { merge_size_ratio: 25 }, f) - 3) < 1e-9);
  assert.ok(Math.abs(evaluate('write_amplification_rule', { merge_size_ratio: 64 }, f) - (3 + (64 / 25 - 1) / 4)) < 1e-9);
  assert.equal(evaluate('write_amplification_rule', { merge_size_ratio: f ** 13 }, f), 13);
});
