'use strict';
// What a product manager sees on the first screen: one hint per row, plain units, short column notes,
// sizes in sensible units, and the sizing verdict when one gp3 volume is not enough.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');
const vmod = require('../viewmodel.js');
const presets = require('../presets.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const page = (sc, inputs, uiState) => {
  const s = ui.defaultState(LIVE, sc);
  Object.assign(s.inputs, inputs || {});
  const out = ui.compute(LIVE, s, uiState || {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};
const row = (vm, op) => vm.table.rows.find((r) => r.operation === op);
const card = (vm, v) => vm.cards.find((c) => c.variant === v);
const count = (s, sub) => s.split(sub).length - 1;

test('test_zero_work_hint_is_shown_once_per_row', () => {
  const { vm, parts } = page('logs');
  const r = row(vm, 'mutations');
  assert.equal(r.note, 'no ALTER UPDATE/DELETE in this workload');
  assert.ok(r.cells.every((c) => !c.hint), 'no cell repeats the row hint');
  assert.equal(count(parts.table, 'no ALTER UPDATE/DELETE in this workload'), 1);
  assert.match(parts.table, /<th scope="row">Mutations<span class="row-note">no ALTER UPDATE\/DELETE in this workload<\/span><\/th>/);
  // A workload that mutates has no hint on the row.
  assert.equal(row(page('logs', { mutations_per_month: 5 }).vm, 'mutations').note, null);
});

test('test_scenarios_read_and_restart', () => {
  for (const [id, s] of Object.entries(presets.SCENARIOS)) {
    assert.ok(s.values.select_part_reads_per_day > 0, `${id}: cold part reads per day`);
    assert.equal(s.values.restarts_per_month, 1, `${id}: one restart per replica per month`);
    assert.deepEqual(presets.coverageProblems(LIVE, s), [], id);
  }
  assert.equal(presets.SCENARIOS.logs.values.select_part_reads_per_day, 2000);
  assert.equal(presets.SCENARIOS.metrics.values.select_part_reads_per_day, 5000);
  const { vm } = page('logs');
  for (const op of ['reads', 'recovery']) {
    assert.equal(row(vm, op).note, null, op);
    // The object-storage columns price reads and recovery from measured cells.
    for (const v of ['s3', 'cas']) {
      const c = row(vm, op).cells.find((x) => x.variant === v);
      assert.equal(c.state, 'ok', `${v}.${op}`);
      assert.equal(c.extrapolated, false, `${v}.${op}`);
      assert.notEqual(c.text, '$0.00', `${v}.${op}`);
    }
  }
});

test('test_units_are_plain_words', () => {
  assert.equal(vmod.fmtQuantity(1, '1/s'), '1 per second');
  assert.equal(vmod.fmtQuantity(2000, 'count/day'), '2,000 per day');
  assert.equal(vmod.plainUnit('count/hour'), 'per hour');
  assert.equal(vmod.plainUnit('GiB/day'), 'GiB/day');
  const { vm, parts } = page('logs');
  const f = Object.fromEntries(vm.inputs.blocks.flatMap((b) => b.fields).map((x) => [x.id, x]));
  assert.equal(f.insert_rate.display_unit, 'per second');
  assert.equal(f.select_part_reads_per_day.display_unit, 'per day');
  assert.equal(/count\/|>1\/s</.test(parts.inputs), false, 'no count/day or 1/s on the form');
  assert.match(parts.inputs, /<option value="day" selected>per day<\/option>/);
  assert.match(parts.inputs, /<span class="unit">per second<\/span>/);
});

test('test_settings_labels_wrap_instead_of_truncating', () => {
  const css = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
  const rules = [...css.matchAll(/([^{}]*\.settings-grid[^{}]*\.field-label[^{}]*)\{([^}]*)\}/g)];
  assert.ok(rules.length > 0);
  for (const [, , body] of rules) assert.equal(/ellipsis|nowrap/.test(body), false, body);
});

test('test_sizes_use_three_significant_digits', () => {
  assert.equal(vmod.fmtSize(0.201823), '207 MiB');
  assert.equal(vmod.fmtSize(3.6345), '3.63 GiB');
  assert.equal(vmod.fmtSize(19500), '19,500 GiB');
  assert.equal(vmod.fmtSig(168.76), '169');
  assert.equal(vmod.fmtSig(42.19), '42.2');
  const { vm } = page('logs');
  const ebs = card(vm, 'ebs').sizing;
  assert.equal(ebs.short, `19,500 GiB per replica, needs ${vmod.fmtSig(ebs.peak_iops)} IOPS, ${vmod.fmtSig(ebs.peak_mibps)} MiB/s: gp3 baseline sufficient`);
  assert.match(ebs.short, /needs \d{1,3} IOPS, \d{1,3}(\.\d)? MiB\/s/);
  assert.match(card(vm, 'cas').sizing.short, /^207 MiB per replica/);
});

test('test_notes_row_is_one_short_line_per_column', () => {
  const { vm, parts } = page('logs');
  const ebs = vm.cards.find((c) => c.variant === 'ebs').sizing;
  // Size and verdict only; the IOPS and MiB/s figures are in the column dialog.
  assert.equal(vm.table.column_lines.ebs, '19,500 GiB/replica · gp3 ok');
  assert.ok(ebs.lines.some((l) => l.includes(`${vmod.fmtSig(ebs.peak_iops)} IOPS, ${vmod.fmtSig(ebs.peak_mibps)} MiB/s`)));
  assert.match(vm.table.column_lines.cas, /^207 MiB\/replica · /);
  assert.match(vm.table.column_lines.s3plain, /^6 GiB gp3, no local data$/);
  const notes = parts.table.slice(parts.table.indexOf('<tr class="notes-row"'));
  assert.equal(/<ul|<li/.test(notes), false, 'no paragraph lists in the notes row');
  for (const v of ['ebs', 's3', 's3plain', 'cas']) {
    assert.ok(notes.includes(`>${vm.table.column_lines[v]}<button type="button" class="info-btn" data-action="info" data-info-kind="column" data-variant="${v}"`), v);
  }
  // The modal keeps everything the row no longer shows.
  const info = page('logs', {}, { info: { kind: 'column', variant: 's3' } }).vm.info;
  assert.ok(info.lines.includes(vmod.GIB_NOTE));
  assert.ok(info.lines.some((l) => /^Instance: /.test(l)));
  assert.ok(info.lines.some((l) => /^Evidence: /.test(l)));
});

test('test_sizing_verdict_when_one_volume_is_not_enough', () => {
  // Ten times the ingest: one gp3 volume's throughput baseline is below the peak; striping covers it.
  const striped = card(page('logs', { ingest_rate: 5000 }).vm, 'ebs').sizing;
  assert.ok(striped.volumes > 1);
  assert.match(striped.verdict, /^\d+ striped gp3 volumes of [\d,]+ GiB; one volume's baseline \(3,000 IOPS, 125 MiB\/s\) is below the peak; (no charge above the baseline|\+\$[\d,.]+\/month per replica above the baseline)$/);
  // One volume by hand: the demand is provisioned above the baseline, with its monthly cost.
  const manual = { ingest_rate: 5000, retention_days: 3, volumes_per_replica: 1, provisioned_size_gib: 19500, provisioned_iops: 4000, provisioned_mibps: 900 };
  const single = card(page('logs', manual).vm, 'ebs');
  assert.equal(single.state, 'ok', single.reason);
  // A manual layout without its size names the missing input, not "not measured".
  assert.equal(card(page('logs', { volumes_per_replica: 1 }).vm, 'ebs').reason, 'EBS: the manual volume layout needs a value for '
    + LIVE.registry.find((q) => q.id === 'provisioned_size_gib').label);
  assert.match(single.sizing.verdict, /^provision [\d,]+ IOPS, [\d,]+ MiB\/s \(\+\$[\d,.]+\/month per replica\)$/);
  // Ten times the archive: no gp3 layout holds the data; the card says so with the numbers.
  const { vm } = page('single_replica_archive', { data_bytes: 512000 });
  const ebs = card(vm, 'ebs');
  assert.notEqual(ebs.state, 'ok');
  assert.equal(ebs.reason, 'EBS needs 666,000 GiB per replica; the largest gp3 layout is 4 volumes of 65,536 GiB (262,144 GiB)');
  assert.equal(vm.table.column_lines.ebs, 'no gp3 layout fits 666,000 GiB/replica');
});

test('test_input_dialog_has_no_kind_or_generic_evidence', () => {
  const { parts } = page('logs', {}, { info: { kind: 'input', id: 'data_bytes' } });
  const m = parts.modal;
  assert.equal(/<th scope="row">Kind<\/th>/.test(m), false);
  assert.equal(/<th scope="row">Evidence<\/th>/.test(m), false);
  for (const k of ['Current value', 'Derived', 'Default']) assert.ok(m.includes(`<th scope="row">${k}</th>`), k);
});

test('test_extrapolated_cheapest_badge_counts_the_priced_columns', () => {
  // Across zones the Logs winner, CAS, prices its replica transfer from an assumption.
  const { vm } = page('logs', { topology: 'cross_az' });
  const best = vm.cards.find((c) => c.cheapest);
  assert.ok(best && best.extrapolated, 'cross-AZ logs: the cheapest column is extrapolated');
  assert.equal(best.cheapest_text, 'cheapest of 4, extrapolated');
  // The table header shows the short form with a separate extrapolated pill.
  const col = vm.table.columns.find((c) => c.cheapest);
  assert.equal(col.cheapest_text, 'cheapest of 4');
  assert.equal(col.extrapolated, true);
});
