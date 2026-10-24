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

test('round 2: clamped first buffer, native admission, client blocks, disabled minimums, dedup cap', () => {
  // A first buffer clamped to 16 MiB makes a 20 MiB object multipart although it is below the single-PUT limit.
  const clamped = { object_bytes: 20 * MIB, multipart_first_part_bytes: 16 * MIB };
  assert.equal(evaluate('multipart_upload', clamped), 1);
  assert.equal(evaluate('multipart_parts', clamped), 2);
  // Native INSERTs measure decoded block bytes and fall back to sync once they reach the limit (>=).
  const native = { insert_kind: 'data', insert_protocol: 'native', insert_rows: 1000, insert_clients: 16,
                   insert_rate: 20 };
  assert.equal(evaluate('parts_per_insert', { ...native, insert_bytes_uncompressed: 10 * MIB }), 1);
  assert.equal(evaluate('parts_per_insert', { ...native, insert_bytes_uncompressed: 10 * MIB - 1 }), 0.5);
  // Inline data at exactly the limit is still queued (<=).
  assert.equal(evaluate('parts_per_insert', { ...native, insert_protocol: 'inline', insert_payload_bytes: 10 * MIB,
                                             insert_bytes_uncompressed: 40000 }), 1);
  // A synchronous client block larger than the minimum is one part.
  assert.equal(evaluate('parts_per_insert', { insert_kind: 'data', async_insert: '0', insert_rows: 2000000,
                                             insert_bytes_uncompressed: 16000000, insert_data_block_rows: 2000000 }), 1);
  // Disabled minimums: every source block is its own part.
  assert.equal(evaluate('parts_per_insert', { insert_kind: 'select', insert_rows: 200000, insert_bytes_uncompressed: 1600000,
                                             min_insert_block_size_rows: 0, min_insert_block_size_bytes: 0 }), 4);
  assert.equal(evaluate('parts_per_insert', { insert_kind: 'select', insert_rows: 2621122, insert_bytes_uncompressed: 2621122 * 40,
                                             min_insert_block_size_bytes: 0 }), 3);
  // deduplicate_insert = enable (the default) caps a flush at async_insert_max_query_number entries.
  const many = { insert_rate: 10000, insert_clients: 1000, insert_payload_bytes: 1000, insert_protocol: 'inline' };
  assert.equal(evaluate('inserts_per_async_flush', many), 450);
  assert.equal(evaluate('inserts_per_async_flush', { ...many, deduplicate_insert: 'disable' }), 1000);
  assert.equal(evaluate('inserts_per_async_flush', { ...many, deduplicate_insert: 'backward_compatible_choice' }), 1000);
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
  assert.equal(evaluate('inserts_per_async_flush', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 3000,
                                                     insert_protocol: 'inline' }), 5);
  assert.equal(evaluate('inserts_per_async_flush', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 6 * MIB,
                                                     insert_protocol: 'inline' }), 2);
  assert.equal(evaluate('inserts_per_async_flush', { insert_rate: 20, insert_clients: 1, insert_payload_bytes: 3000,
                                                     insert_protocol: 'inline' }), 1);
  assert.equal(evaluate('parts_per_insert', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 3000,
                                             insert_protocol: 'inline' }), 0.2);
  assert.equal(evaluate('parts_per_insert', { insert_rate: 20, insert_clients: 16, insert_payload_bytes: 11 * MIB,
                                             insert_protocol: 'inline', insert_rows: 1000,
                                             insert_bytes_uncompressed: 40000 }), 1);
});

test('write amplification: continuous ladder, capped at 12 levels', () => {
  const f = 5;
  assert.equal(evaluate('write_amplification_rule', { merge_size_ratio: 1 }, f), 1);
  assert.ok(Math.abs(evaluate('write_amplification_rule', { merge_size_ratio: 25 }, f) - 3) < 1e-9);
  assert.ok(Math.abs(evaluate('write_amplification_rule', { merge_size_ratio: 64 }, f) - (3 + (64 / 25 - 1) / 4)) < 1e-9);
  assert.equal(evaluate('write_amplification_rule', { merge_size_ratio: f ** 13 }, f), 13);
});
