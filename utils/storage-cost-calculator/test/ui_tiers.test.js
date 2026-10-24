'use strict';
// Form placement by registry tier and the one info dialog shared by inputs, coefficients, cells and columns.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');

const MINIMAL = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8'));
const TIERS = {
  data_bytes: ['simple', 'workload', null, 'Data size'], ingest_rate: ['simple', 'workload', null, 'Ingest rate'],
  retention_days: ['simple', 'workload', null, 'Retention'], replicas: ['simple', 'workload', null, 'Replicas'],
  inserts_per_month: ['inserts', 'setting', 'async_insert', 'Async inserts'], mutations_per_month: ['mutations_deletes', 'setting', null, 'Mutations per month'],
  replacement_rate: ['topology_recovery', 'advanced', null, 'Replica replacements'], volumes_per_replica: ['ebs', 'advanced', null, 'EBS volumes'],
  provisioned_size_gib: ['ebs', 'hidden', null, 'Volume size'], provisioned_iops: ['ebs', 'hidden', null, 'Volume IOPS'], provisioned_mibps: ['ebs', 'hidden', null, 'Volume throughput'],
};
function load(mutate) {
  const c = JSON.parse(JSON.stringify(MINIMAL));
  for (const q of c.registry) {
    const [block, tier, name, short] = TIERS[q.id];
    Object.assign(q, { block, ui: { tier, name, short, measured_by: `Assumed: preset default for ${q.id}.` } });
  }
  c.registry.find((q) => q.id === 'inserts_per_month').hint = { sql: 'SELECT count() FROM system.query_log', mapping: { 'count()': 'inserts per month' } };
  if (mutate) mutate(c);
  return contract.load(c);
}
const C = load();
const page = (uiState, c) => {
  const k = c || C;
  const out = ui.compute(k, ui.defaultState(k, 'custom'), uiState || {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};
const sectionOf = (html, marker) => {
  const at = html.indexOf(marker);
  assert.ok(at >= 0, marker);
  return html.slice(at, html.indexOf('</details>', at) >>> 0 || undefined);
};

test('test_form_places_fields_by_tier', () => {
  const { parts, vm } = page();
  const html = parts.inputs;
  const head = html.slice(0, html.indexOf('<details'));
  for (const id of ['data_bytes', 'ingest_rate', 'retention_days', 'replicas']) assert.ok(head.includes(`data-field="${id}"`), id);
  assert.match(head, /All other settings describe a typical deployment/);
  // A ClickHouse setting is labelled by its name in code font, inside its block's collapsed disclosure.
  const inserts = sectionOf(html, '<details class="block" data-block="inserts">');
  assert.match(inserts, /data-field="inserts_per_month"><div class="field-head"><span class="field-label"><code>async_insert<\/code><\/span><button type="button" class="info-btn"/);
  assert.match(sectionOf(html, 'data-block="mutations_deletes"'), /<span class="field-label">Mutations per month<\/span>/);
  // Advanced fields live only in the one low-level section, which comes last and is closed.
  const low = sectionOf(html, '<details class="block" data-block="advanced">');
  assert.ok(html.trimEnd().endsWith('</details></div>'), 'low-level section is last');
  assert.match(low, /<summary>Low-level assumptions<\/summary>/);
  assert.ok(low.includes('data-field="replacement_rate"') && low.includes('data-field="volumes_per_replica"'));
  assert.ok(low.includes('<h3>Topology and recovery</h3>') && low.includes('<h3>EBS</h3>'));
  assert.equal(html.split('data-field="replacement_rate"').length, 2, 'rendered once');
  // Hidden rows never render; no per-block advanced disclosure exists.
  for (const id of ['provisioned_size_gib', 'provisioned_iops', 'provisioned_mibps']) assert.equal(html.includes(`data-field="${id}"`), false, id);
  assert.equal(/<details[^>]* open/.test(html), false, 'every disclosure starts closed');
  assert.deepEqual(vm.problems.filter((p) => /ui tier/.test(p)), []);
  // Every non-price field has its (i) button.
  const fields = html.match(/data-field="[a-z_]+"/g).length;
  assert.equal(html.match(/data-action="info" data-info-kind="input"/g).length, fields);
});

test('test_contract_without_tiers_reports_one_problem_and_keeps_fields', () => {
  const c = load((x) => { for (const q of x.registry) delete q.ui; });
  const { vm, parts } = page({}, c);
  assert.equal(vm.problems.filter((p) => /no ui tier/.test(p)).length, 1);
  assert.ok(parts.inputs.includes('data-field="replacement_rate"'));
});

test('test_info_dialog_for_an_input', () => {
  const { vm, parts } = page({ info: { kind: 'input', id: 'inserts_per_month' } });
  assert.equal(vm.info.title, 'Async inserts');
  assert.equal(vm.info.name, 'async_insert');
  assert.equal(vm.info.measured_by, 'Assumed: preset default for inserts_per_month.');
  const m = parts.modal;
  assert.match(m, /<h2 id="modal-title">Async inserts<\/h2>/);
  assert.match(m, /<th scope="row">ClickHouse name<\/th><td><code>async_insert<\/code><\/td>/);
  assert.match(m, /<th scope="row">Evidence<\/th><td>Assumed: preset default for inserts_per_month\.<\/td>/);
  assert.match(m, /<th scope="row">Default<\/th>/);
  assert.match(m, /<h4>Fill from your cluster<\/h4><pre><code>SELECT count\(\) FROM system\.query_log<\/code><\/pre>/);
  assert.match(m, /data-action="close-info"/);
  // A hidden row opens the same dialog; nothing is open by default.
  assert.match(page({ info: { kind: 'input', id: 'provisioned_iops' } }).parts.modal, /<h2 id="modal-title">Volume IOPS<\/h2>/);
  assert.equal(page().parts.modal, '');
});

test('test_info_dialog_for_a_cell_and_a_column', () => {
  const { vm, parts } = page({ info: { kind: 'cell', variant: 'cas', operation: 'storage' } });
  assert.equal(vm.info.title, 'CAS · Storage');
  assert.match(parts.modal, /data-info="cell:cas\.storage"/);
  const s = Object.assign(ui.defaultState(C, 'custom'), { inputs: { replacement_rate: 0.1 } });
  const out = ui.compute(C, s, { info: { kind: 'variant', variant: 's3plain' } });
  assert.equal(out.vm.info.description, 'S3 plain rewritable cannot replace a replica');
  assert.ok(out.vm.info.lines.length >= 1);
  // The card shows one sentence and a Why button for the dialog.
  const card = ui.renderParts(out.vm, out.page).cards;
  assert.match(card, /<p class="card-reason">S3 plain rewritable cannot replace a replica\.<\/p><button type="button" class="btn btn-small" data-action="info" data-info-kind="variant" data-variant="s3plain">Why<\/button>/);
});

test('test_info_actions_open_and_close_the_dialog', () => {
  const s = ui.defaultState(C, 'custom');
  const a = ui.update(C, s, {}, { type: 'info', data: { infoKind: 'input', id: 'replicas' } }, {});
  assert.deepEqual(a.ui.info, { kind: 'input', id: 'replicas' });
  const b = ui.update(C, a.state, a.ui, { type: 'close-info', data: {} }, {});
  assert.equal(b.ui.info, null);
  assert.throws(() => ui.update(C, s, {}, { type: 'info', data: { infoKind: 'bogus' } }, {}), /unknown info kind/);
});

test('test_cites_point_at_the_measured_build', () => {
  const vm = require('../viewmodel.js');
  assert.equal(vm.cite('see tmp/src-26.8/src/Core/Settings.cpp:178', '09d0772a842'), 'see src/Core/Settings.cpp:178 @ 09d0772a842');
  assert.equal(vm.cite('tmp/src-26.8/src/A.cpp:1-3', null), 'src/A.cpp:1-3');
});
