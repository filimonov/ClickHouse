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
  // The hint belongs to the row: said once beside its label, never per cell.
  assert.equal(row(vm, 'mutations').note, 'no ALTER UPDATE/DELETE in this workload');
  assert.match(parts.table, /<th scope="row">Mutations<span class="row-note">no ALTER UPDATE\/DELETE in this workload<\/span><\/th>/);
  // A $0 that comes from the model, not from a zero input, carries no hint.
  assert.equal(row(vm, 'inserts').note, null);
  // Once the workload mutates, no hint remains on the row.
  const m = page(C, { mutations_per_month: 5 }).vm;
  assert.equal(row(m, 'mutations').note, null);
});

test('test_notes_live_in_the_table', () => {
  const { vm, parts } = page(load());
  // Column notes sit in the last row of the breakdown; the storage row itself carries no note.
  assert.equal(row(vm, 'storage').note, null);
  const s3 = vm.table.column_notes.s3.map((n) => n.kind);
  assert.ok(s3.includes('gib'), JSON.stringify(vm.table.column_notes.s3));
  assert.ok(vm.table.column_notes.s3.find((n) => n.kind === 'gib').text === 'GB = binary GiB (assumption, no AWS statement found)');
  assert.match(parts.table, /<tr class="notes-row" data-operation="notes"><th scope="row">Notes<\/th>/);
  assert.match(parts.table, /data-info-kind="column" data-variant="s3"/);
  // No flat notes panel: with nothing typed wrong the part is empty.
  assert.equal(parts.notes, '');
});

test('test_evidence_line_and_dialog', () => {
  const c = load((x) => {
    x.evidence_statement = 'Measured on ClickHouse commit 09d0772a842 (release 26.8) against a RustFS S3 store. Built from 1 report(s).';
    x.manifest.evidence = [{ report: '2026-10-24-storage-cost-s3', commit: '09d0772a842c5d73', raw_sha256sums: 'x' }];
    x.evidence_by_variant = { ebs: { fit_and_holdout: 2, fit_only: 1, assumption: 3 }, s3: { fit_and_holdout: 4, fit_only: 0, assumption: 0 } };
  });
  const { vm, parts } = page(c);
  assert.equal(vm.evidence.build, '09d0772a842');
  assert.match(parts.evidence, /<p>Measured on ClickHouse 09d0772a842, 1 report\.<button type="button" class="info-btn" data-action="info" data-info-kind="evidence"/);
  assert.ok(vm.table.column_notes.ebs.some((n) => n.text === 'Evidence: 2 cells fit and hold-out, 1 fit only, 3 assumption only'));
  const s = Object.assign(ui.defaultState(c, 'custom'), { inputs: {} });
  const out = ui.compute(c, s, { info: { kind: 'evidence', id: 'evidence' } });
  const modal = ui.renderParts(out.vm, out.page).modal;
  assert.match(modal, /Measured on ClickHouse commit 09d0772a842 \(release 26\.8\) against a RustFS S3 store\./);
  assert.match(modal, /<a href="https:\/\/github\.com\/filimonov\/ClickHouse\/blob\/docs\/storage-cost-calculator\/docs\/superpowers\/reports\/2026-10-24-storage-cost-s3\/README\.md" rel="noopener" target="_blank">2026-10-24-storage-cost-s3<\/a>/);
  assert.match(modal, /EBS: 2 cells fit and hold-out, 1 fit only, 3 assumption only/);
});
