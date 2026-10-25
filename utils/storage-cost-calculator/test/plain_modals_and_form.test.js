'use strict';
// The compact form, the badge wording, the replica transfer row, the one-line Notes row and the plain-words modals.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');
const vmod = require('../viewmodel.js');
const presets = require('../presets.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const CSS = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
const page = (sc, inputs, uiState) => {
  const s = ui.defaultState(LIVE, sc);
  Object.assign(s.inputs, inputs || {});
  const out = ui.compute(LIVE, s, uiState || {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};
const row = (vm, op) => vm.table.rows.find((r) => r.operation === op);
const cell = (vm, op, v) => row(vm, op).cells.find((c) => c.variant === v);
const text = (html) => html.replace(/<[^>]*>/g, ' ').replace(/&#39;/g, "'").replace(/&amp;/g, '&').replace(/\s+/g, ' ');
const money = (s) => Number(String(s).replace(/[$,]/g, ''));
const CROSS = { topology: 'cross_az' };

test('test_form_has_no_inner_scroll_and_prices_sit_with_the_presets', () => {
  const rule = /\n\.panel-inputs \{([^}]*)\}/.exec(CSS);
  assert.ok(rule, 'the .panel-inputs rule exists');
  assert.doesNotMatch(rule[1], /overflow|max-height/, 'the inputs panel never clips its own content');
  const { parts } = page('logs');
  // The price list is a control in the preset row, not a row of its own.
  assert.match(parts.presets, /<div class="pills"[^>]*>(?:(?!<\/div>).)*<select[^>]*data-action="price"/s);
  // The data = ingest × retention rule is behind the (i) of its derived member, not a paragraph on the form.
  assert.doesNotMatch(parts.inputs, /group-note/);
  const info = page('logs', {}, { info: { kind: 'input', id: 'data_bytes' } }).vm.info;
  assert.ok(info.lines.some((l) => /lock two; the third is derived/i.test(l)), JSON.stringify(info.lines));
});

test('test_cheapest_badge_is_short_and_wraps', () => {
  const { vm } = page('logs', CROSS);
  const best = vm.cards.find((c) => c.cheapest);
  assert.match(best.cheapest_text, /^cheapest of \d(, extrapolated)?$/);
  assert.match(CSS, /\.card-head \.badge \{[^}]*white-space: normal/);
});

test('test_replica_transfer_row_reconciles_with_the_cards', () => {
  const { vm, parts } = page('logs', CROSS);
  const tr = row(vm, 'transfer');
  assert.ok(tr, 'cross-AZ replicas pay for transfer: the table has the row');
  assert.equal(tr.label, 'Replica transfer');
  assert.ok(money(cell(vm, 'transfer', 'ebs').text) > 0);
  // EBS inserts write only to the local volume: the transfer is no longer inside the Inserts cell.
  assert.equal(cell(vm, 'inserts', 'ebs').text, '$0.00');
  for (const c of vm.cards.filter((x) => x.state === 'ok')) {
    const sum = vm.table.rows.map((r) => r.cells.find((x) => x.variant === c.variant)).filter((x) => x.state === 'ok').reduce((s, x) => s + money(x.text), 0);
    assert.ok(Math.abs(sum - money(c.total_text)) < 0.05 * vm.table.rows.length, `${c.variant}: rows ${sum} vs card ${c.total_text}`);
    const seg = c.bar_segments.find((s) => s.class === 'transfer');
    assert.equal(cell(vm, 'transfer', c.variant).text, seg ? seg.money_text : '$0.00', c.variant);
  }
  assert.match(parts.table, /<tr data-operation="transfer"><th scope="row">Replica transfer/);
  // One zone: nothing to transfer, no row.
  assert.equal(row(page('logs').vm, 'transfer'), undefined);
});

test('test_assumption_modal_says_assumption_in_plain_words', () => {
  const { vm } = page('logs', CROSS);
  // The assumed relink bytes price only the transfer, so the badge sits on that row, not on Inserts.
  assert.equal(cell(vm, 'transfer', 'cas').extrapolated, true);
  assert.equal(cell(vm, 'inserts', 'cas').extrapolated, false);
  const t = page('logs', CROSS, { info: { kind: 'transfer', variant: 'cas' } });
  assert.equal(t.vm.info.description, 'Priced from a stated assumption.');
  assert.ok(t.vm.info.lines.includes('Each fetched part sends about 19 KB of manifest data regardless of its size. Parts of 1,000 rows sent a third less, so this is an upper bound.'), JSON.stringify(t.vm.info.lines));
  const shown = text(t.parts.modal);
  assert.doesNotMatch(shown, /\b[a-z0-9]+_[a-z0-9_]+\b/, 'no coefficient ids');
  assert.doesNotMatch(shown, /Tier 2|replication-fetch|cost-2026|ASSUMPTION/);
  assert.equal(page('logs', CROSS, { info: { kind: 'cell', variant: 'cas', operation: 'inserts' } }).vm.info.description, 'Priced from measured coefficients.');
  // In one zone the assumed row moves no bytes: nothing rests on the assumption (one table: start requests measured).
  const same = page('logs', { cas_tables: 1 });
  assert.equal(cell(same.vm, 'inserts', 'cas').extrapolated, false);
  assert.equal(same.vm.cards.find((c) => c.variant === 'cas').extrapolated_cells, 0);
});

test('test_every_assumed_coefficient_has_a_plain_sentence', () => {
  for (const c of LIVE.coefficients.filter((x) => x.basis === 'assumed')) {
    const s = vmod.assumptionSentence(c);
    assert.ok(typeof s === 'string' && /^[A-Z].*\.$/.test(s), `${c.id}: ${s}`);
    assert.doesNotMatch(s, /_|Tier|ASSUMPTION/, c.id);
  }
});

test('test_every_cell_modal_leads_with_a_plain_sentence', () => {
  for (const sc of Object.keys(presets.SCENARIOS)) {
    for (const inputs of [{}, CROSS]) {
      const { vm } = page(sc, inputs);
      for (const r of vm.table.rows) {
        for (const c of r.cells) {
          if (!c.clickable) continue;
          const info = page(sc, inputs, { info: { kind: 'cell', variant: c.variant, operation: c.operation } }).vm.info;
          const first = info.lines[0];
          assert.ok(first && /^[A-Z].*\.$/.test(first), `${sc} ${c.variant}.${c.operation}: ${first}`);
          assert.doesNotMatch(first, /_|=|proxy|syscall|peak_factor/, `${sc} ${c.variant}.${c.operation}: ${first}`);
        }
      }
    }
  }
  // Engineering detail stays, under Source.
  const ebs = page('logs', {}, { info: { kind: 'cell', variant: 'ebs', operation: 'inserts' } });
  assert.ok(ebs.vm.info.source.some((l) => /syscall proxy/.test(l)));
  assert.match(ebs.parts.modal, /<h4>Source<\/h4>/);
  assert.doesNotMatch(ebs.vm.info.lines.join(' '), /syscall proxy/);
});

test('test_input_modal_subtitle_default_and_verified_range', () => {
  for (const q of LIVE.registry) {
    if (q.kind === 'measured' && !q.coef) continue;
    const info = page('logs', {}, { info: { kind: 'input', id: q.id } }).vm.info;
    if (info.description) assert.notEqual(info.description.toLowerCase(), info.title.toLowerCase(), q.id);
  }
  const d = page('logs', {}, { info: { kind: 'input', id: 'data_bytes' } });
  assert.notEqual(d.vm.info.description, 'Data size');
  assert.match(d.vm.info.default_text, /^15,000 GiB \(Logs preset\); 20,480 GiB without a preset/);
  assert.equal(d.vm.info.range_text, 'no verified range');
  assert.match(d.parts.modal, /<th scope="row">Verified range<\/th><td>no verified range<\/td>/);
  // Same value in the preset and the registry: one value, labelled as the preset's.
  const g = page('logs', {}, { info: { kind: 'input', id: 'index_granularity' } }).vm.info;
  assert.match(g.default_text, /^8,192 \(Logs preset/);
  assert.equal((g.default_text.match(/8,192/g) || []).length, 1, g.default_text);
  // A formula from source is verified, not bounded, by the measurements.
  const derived = LIVE.coefficients.find((c) => c.basis === 'derived' && c.validity && Object.keys(c.validity).length);
  const info = page('logs', {}, { info: { kind: 'input', id: derived.id } }).vm.info;
  assert.match(info.range_text, /^formula from source; verified at /, derived.id);
  for (const q of LIVE.registry) {
    const i = page('logs', {}, { info: { kind: 'input', id: q.id } }).vm.info;
    if (i) assert.doesNotMatch(i.range_text || '', /no measured range/, q.id);
  }
});

test('test_notes_cell_is_at_most_sixty_characters', () => {
  for (const sc of Object.keys(presets.SCENARIOS)) {
    for (const inputs of [{}, CROSS]) {
      const { vm } = page(sc, inputs);
      for (const [v, line] of Object.entries(vm.table.column_lines)) {
        if (line === null) continue;
        assert.ok(line.length <= 60, `${sc} ${v}: ${line.length} chars: ${line}`);
        assert.doesNotMatch(line, /IOPS|MiB\/s/, `${sc} ${v}: figures belong in the (i): ${line}`);
      }
    }
  }
  const { vm } = page('logs');
  assert.equal(vm.table.column_lines.ebs, '19,500 GiB/replica · gp3 ok');
  // The figures moved into the column dialog.
  const info = page('logs', {}, { info: { kind: 'column', variant: 'ebs' } }).vm.info;
  assert.ok(info.lines.some((l) => /IOPS/.test(l) && /MiB\/s/.test(l)));
});

test('test_phone_puts_the_form_before_the_cards', () => {
  const phone = /@media \(max-width: 600px\) \{((?:[^{}]|\{[^}]*\})*)\}/g;
  const blocks = [...CSS.matchAll(phone)].map((m) => m[1]).join('\n');
  const order = (id) => {
    const m = new RegExp(`#${id}[^{]*\\{[^}]*order: (-?\\d+)`).exec(blocks);
    return m ? Number(m[1]) : null;
  };
  assert.ok(order('presets') !== null && order('inputs') !== null && order('cards') !== null, blocks);
  assert.ok(order('presets') < order('inputs') && order('inputs') < order('cards'));
  // The table keeps order 0: after the cards.
  assert.ok(order('cards') < 0);
});

test('test_cas_is_extrapolated_only_where_the_assumption_prices_bytes', () => {
  // One table: the CAS start requests stay inside their measured range (more tables: see tables_workload.test.js).
  const cas = (sc, inputs) => page(sc, Object.assign({ cas_tables: 1 }, inputs)).vm.cards.find((c) => c.variant === 'cas');
  // One zone, or one replica: the relink row moves nothing, so nothing rests on the assumed relink bytes.
  for (const [sc, inputs] of [['logs', {}], ['metrics', {}], ['custom', {}], ['single_replica_archive', {}], ['single_replica_archive', CROSS]]) {
    const c = cas(sc, inputs);
    assert.equal(c.extrapolated, false, `${sc} ${JSON.stringify(inputs)}: ${c.extrapolated_text}`);
  }
  assert.equal(page('logs', { cas_tables: 1 }).vm.cards.find((c) => c.cheapest).cheapest_text, 'cheapest of 4');
  // Across zones the relink bytes are priced: the transfer cell carries the badge and the assumption.
  for (const sc of ['logs', 'metrics']) {
    const { vm } = page(sc, Object.assign({ cas_tables: 1 }, CROSS));
    assert.equal(cell(vm, 'transfer', 'cas').extrapolated, true, sc);
    const info = page(sc, CROSS, { info: { kind: 'transfer', variant: 'cas' } }).vm.info;
    assert.equal(info.description, 'Priced from a stated assumption.', sc);
  }
});

test('test_no_internal_tier_label_on_the_page', () => {
  for (const inputs of [{}, CROSS]) {
    const { vm, parts } = page('logs', inputs);
    const html = [Object.values(parts).join('')];
    for (const v of ['ebs', 's3', 's3plain', 'cas']) {
      for (const info of [{ kind: 'column', variant: v }, { kind: 'variant', variant: v }, { kind: 'transfer', variant: v }]) html.push(page('logs', inputs, { info }).parts.modal);
      for (const r of vm.table.rows) if (r.operation !== 'transfer') html.push(page('logs', inputs, { info: { kind: 'cell', variant: v, operation: r.operation } }).parts.modal);
    }
    for (const q of LIVE.registry) html.push(page('logs', inputs, { info: { kind: 'input', id: q.id } }).parts.modal);
    assert.doesNotMatch(text(html.join(' ')), /Tier \d/);
  }
});
