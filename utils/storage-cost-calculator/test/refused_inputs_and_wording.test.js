'use strict';
// Refused inputs stay visible with their reason, the block device is named by the price list, the modal lead
// counts assumptions, and the phone layout stays inside the gutter.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const CSS = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
const page = (sc, inputs, uiState, edit) => {
  const s = ui.defaultState(LIVE, sc);
  Object.assign(s.inputs, inputs || {});
  if (edit) edit(s);
  const out = ui.compute(LIVE, s, uiState || {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};
const field = (vm, id) => vm.inputs.blocks.flatMap((b) => b.fields).find((f) => f.id === id);
const text = (html) => html.replace(/<[^>]*>/g, ' ').replace(/&#39;/g, "'").replace(/\s+/g, ' ');

const CASES = [
  ['retention_days', 36501, 'retention above 36,500 days is not supported', 'logs'],
  ['data_bytes', 204801, 'data size above 204,800 GiB is not supported', 'custom'],
  ['ingest_rate', { value: 204801, per: 'day' }, 'ingest above 204,800 GiB/day is not supported', 'custom'],
];
for (const [id, value, reason, sc] of CASES) {
  test(`test_refused_${id}_keeps_its_value_and_says_why`, () => {
    // Lock the typed member so the refusal comes from the typed value, not from a derivation.
    const { vm, parts } = page(sc, { [id]: value }, {}, (s) => { s.overrides = { [id]: value }; });
    const f = field(vm, id);
    assert.equal(f.refused, reason);
    const shown = typeof value === 'object' ? value.value : value;
    assert.equal(f.display_value, shown, 'the input shows what was typed');
    assert.match(parts.inputs, new RegExp(`data-field="${id}"[\\s\\S]*?class="field-note field-error" role="alert">${reason}<`));
    assert.match(parts.inputs, new RegExp(`data-id="${id}" value="${shown}"`));
    const off = vm.cards.filter((c) => c.state === 'unavailable');
    assert.equal(off.length, vm.cards.length, 'no column can price a refused input');
    for (const c of off) assert.ok(c.state_text.includes(reason), c.state_text);
    assert.ok(text(parts.cards).includes(reason));
  });
}

test('test_inputs_at_the_limit_are_not_refused', () => {
  const { vm } = page('logs', { retention_days: 36500 }, {}, (s) => { s.overrides = { retention_days: 36500 }; });
  assert.equal(field(vm, 'retention_days').refused, null);
});

test('test_gcs_ebs_card_names_hyperdisk_not_gp3', () => {
  const aws = page('logs', {}, {}, (s) => { s.price_preset = 'aws-us-east-1'; });
  const gcs = page('logs', {}, {}, (s) => { s.price_preset = 'gcs'; });
  const all = (p) => JSON.stringify([p.vm.cards.map((c) => c.sizing), p.vm.table.notes || null, p.parts.table, p.parts.cards]);
  assert.match(all(aws), /gp3/);
  assert.doesNotMatch(all(gcs), /gp3/i);
  assert.match(all(gcs), /Hyperdisk Balanced/);
  for (const scenario of ['logs', 'metrics', 'single_replica_archive']) {
    const p = page(scenario, {}, {}, (s) => { s.price_preset = 'gcs'; });
    assert.doesNotMatch(JSON.stringify(p.vm.cards.map((c) => c.sizing)), /gp3/i, scenario);
  }
});

test('test_gcs_column_dialog_and_infeasible_wording_follow_the_price_list', () => {
  const p = page('logs', {}, { info: { kind: 'column', variant: 'ebs' } }, (s) => { s.price_preset = 'gcs'; });
  assert.doesNotMatch(JSON.stringify(p.vm.info), /gp3/i);
  assert.match(p.vm.info.lines.join('\n'), /Hyperdisk Balanced baseline per volume/);
});

test('test_lead_counts_assumption_lines_in_the_source', () => {
  const info = page('logs', {}, { info: { kind: 'cell', variant: 'cas', operation: 'recovery' } }).vm.info;
  assert.ok(info.source.some((l) => /\bASSUMPTION\b/.test(l)), JSON.stringify(info.source));
  assert.equal(info.description, 'Priced from measured coefficients and one assumption.');
  const plain = page('logs', { topology: 'cross_az' }, { info: { kind: 'cell', variant: 'cas', operation: 'inserts' } }).vm.info;
  assert.ok(!plain.source.some((l) => /\bASSUMPTION\b/.test(l)));
  assert.equal(plain.description, 'Priced from measured coefficients.');
});

test('test_phone_layout_stretches_panels_and_lets_the_price_select_shrink', () => {
  const small = /@media \(max-width: 900px\) \{([\s\S]*?)\n\}/.exec(CSS);
  assert.ok(small);
  assert.match(small[1], /\.layout \{[^}]*align-items: stretch/);
  assert.match(small[1], /#presets[^{]*\{ min-width: 0; \}/);
  assert.match(CSS, /\.price-pick select \{[^}]*width: 0; min-width: 0/);
});
