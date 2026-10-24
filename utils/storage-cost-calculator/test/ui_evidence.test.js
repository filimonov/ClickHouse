'use strict';
// Breakdown hints for work the workload does not do, the storage unit note, and the evidence section.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');

const MINIMAL = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8'));
const load = (mutate) => {
  const c = JSON.parse(JSON.stringify(MINIMAL));
  if (mutate) mutate(c);
  return contract.load(c);
};
const page = (c, inputs) => {
  const s = Object.assign(ui.defaultState(c, 'custom'), { inputs: inputs || {} });
  const out = ui.compute(c, s, {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};
const row = (vm, op) => vm.table.rows.find((r) => r.operation === op);

test('test_zero_cells_name_the_missing_work', () => {
  const C = load();
  const { vm, parts } = page(C);
  for (const c of row(vm, 'mutations').cells) assert.equal(c.hint, 'no ALTER UPDATE/DELETE in this workload', c.variant);
  assert.match(parts.table, /\$0\.00<\/button><span class="zero-hint">no ALTER UPDATE\/DELETE in this workload<\/span>/);
  // A $0 that comes from the model, not from a zero input, carries no hint.
  assert.equal(row(vm, 'inserts').cells.find((c) => c.variant === 'ebs').hint, null);
  // Once the workload mutates, no hint remains on the row.
  const m = page(C, { mutations_per_month: 5 }).vm;
  assert.ok(row(m, 'mutations').cells.every((c) => !c.hint));
});

test('test_storage_row_states_the_gib_assumption', () => {
  const { vm, parts } = page(load());
  assert.equal(row(vm, 'storage').note, 'GB = binary GiB (assumption, no AWS statement found)');
  assert.match(parts.table, /<th scope="row">Storage<span class="row-note">GB = binary GiB \(assumption, no AWS statement found\)<\/span><\/th>/);
  assert.equal(row(vm, 'inserts').note, null);
});

test('test_evidence_section_names_reports_and_counts_per_column', () => {
  const c = load((x) => {
    x.evidence_statement = 'Measured on ClickHouse commit 09d0772a842 (release 26.8) against a RustFS S3 store.';
    x.manifest.evidence = [{ report: '2026-10-24-storage-cost-s3', commit: '09d0772a842c5d73', raw_sha256sums: 'x' }];
    x.evidence_by_variant = { ebs: { fit_and_holdout: 2, fit_only: 1, assumption: 3 }, s3: { fit_and_holdout: 4, fit_only: 0, assumption: 0 } };
  });
  const { vm, parts } = page(c);
  assert.equal(vm.evidence.build, '09d0772a842');
  assert.deepEqual(vm.evidence.columns.map((x) => x.variant), ['ebs', 's3']);
  assert.match(parts.evidence, /Measured on ClickHouse commit 09d0772a842 \(release 26\.8\) against a RustFS S3 store\./);
  assert.match(parts.evidence, /<tr data-variant="ebs"><th scope="row">EBS<\/th><td><span data-value="2">2<\/span><\/td><td><span data-value="1">1<\/span><\/td><td><span data-value="3">3<\/span><\/td><\/tr>/);
  assert.match(parts.evidence, /<a href="\.\.\/\.\.\/docs\/superpowers\/reports\/2026-10-24-storage-cost-s3\/README\.md">2026-10-24-storage-cost-s3<\/a>/);
  // A contract without per-column counts shows the statement only.
  assert.equal(page(load()).parts.evidence.includes('evidence-columns'), false);
});
