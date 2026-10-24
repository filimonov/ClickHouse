'use strict';
// The page's string renderer on view models built from the fixture contract through the real pipeline and view model.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const urlstate = require('../urlstate.js');
const ui = require('../ui.js');

const MINIMAL = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'contracts', 'minimal.json'), 'utf8'));
const load = (mutate) => {
  const c = JSON.parse(JSON.stringify(MINIMAL));
  // The fixture registry sits in a test-only block; the page places rows by their registry block.
  for (const q of c.registry) q.block = 'simple';
  if (mutate) mutate(c);
  return contract.load(c);
};
const C = load();

const esc = (t) => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const base = (over) => Object.assign(ui.defaultState(C, 'custom'), over || {});
function page(state, uiState, c) {
  const out = ui.compute(c || C, state, uiState || {});
  return Object.assign(out, { html: ui.render(out.vm, out.page), parts: ui.renderParts(out.vm, out.page) });
}
// The markup of one card, cut at the next card.
const cardHtml = (html, v) => {
  const at = html.indexOf(`data-variant="${v}" data-state=`);
  assert.ok(at >= 0, `card ${v}`);
  const start = html.lastIndexOf('<article', at);
  return html.slice(start, html.indexOf('</article>', start));
};

test('test_render_contains_substituted_values_for_fixture_vm', () => {
  // A branching row: CAS inserts pay PUTs only above 500 inserts a month.
  const c = load((x) => {
    const row = x.formulas.cas.inserts.ledger_rows[0];
    row.expr = { kind: 'branch', dim: 'count', cond: { op: '>', a: { kind: 'input', id: 'inserts_per_month', dim: 'count' }, b: { kind: 'const', value: 500, dim: 'count' } }, then: row.expr, else: { kind: 'const', value: 0, dim: 'count' } };
  });
  const { html, parts, vm } = page(base({ openCell: 'cas.inserts' }), { showCalc: true }, c);
  // Totals, state texts and deltas of the cards.
  const cas = cardHtml(parts.cards, 'cas');
  assert.ok(cas.includes('<p class="card-total" data-kind="money">$23.01</p>'), cas);
  assert.ok(cas.includes('<p class="card-state">cheapest</p>'));
  assert.ok(cas.includes('badge-ok">cheapest'));
  assert.ok(cas.includes('<li>vs EBS: −$56.99</li>') && cas.includes('<li>vs S3: −$12.41</li>'));
  const ebs = cardHtml(parts.cards, 'ebs');
  assert.ok(ebs.includes('<p class="card-state">comparable</p>'));
  assert.ok(ebs.includes('<li>vs S3: +$44.58</li>'));
  assert.equal(ebs.includes('cheapest'), false);
  // Table cell money, and the cell is a drill button.
  assert.match(parts.table, /data-variant="cas" data-operation="storage"[^>]*>\$23\.00<\/button>/);
  // The five stages with their substituted values.
  const stages = [...parts.drill.matchAll(/<li class="stage" data-stage="(\d)"><h4>([^<]+)<\/h4>/g)].map((m) => [m[1], m[2]]);
  assert.deepEqual(stages, [['1', 'Inputs used'], ['2', 'Branch taken per row'], ['3', 'Ledger rows'], ['4', 'Billing lines'], ['5', 'Allocation']]);
  const stage = (n) => parts.drill.slice(parts.drill.indexOf(`data-stage="${n}"`), parts.drill.indexOf(`data-stage="${n + 1}"`) > 0 ? parts.drill.indexOf(`data-stage="${n + 1}"`) : undefined);
  assert.ok(stage(1).includes('<li>inserts_per_month = 1,000'), stage(1));
  assert.ok(stage(2).includes(esc('request PutObject all: inserts_per_month > 500: true, then taken')), stage(2));
  assert.ok(stage(3).includes('<li>request PutObject all: 2,000 (predicted)'), stage(3));
  assert.ok(stage(4).includes('<li>put_copy_post_list: 2,000 requests = $0.01 (share 100% of 2,000 requests = $0.01)'), stage(4));
  const alloc = vm.drill.stages[4].lines;
  assert.ok(alloc.length > 0);
  for (const l of alloc) assert.ok(stage(5).includes(`<li>${esc(l.text)}`), l.text);
  // The formula of the row is rendered as code.
  assert.ok(stage(2).includes('<div class="formula"><code>'));
  // Every container has its part, in page order.
  assert.deepEqual([...html.matchAll(/<div data-part="(\w+)">/g)].map((m) => m[1]), ui.PARTS);
  // Simple inputs: a slider and an editable number per numeric input, the derived member greyed with its rule.
  assert.match(parts.inputs, /type="range"[^>]*data-id="inserts_per_month" value="1000"/);
  assert.match(parts.inputs, /class="field is-derived" data-field="data_bytes"/);
  assert.ok(parts.inputs.includes(`Derived: <code>${esc(page(base()).fields.data_bytes.rule_text)}</code>`));
  assert.ok(parts.evidence.includes(esc(MINIMAL.evidence_statement)));
  // An unknown state is an error, not a default rendering.
  const broken = JSON.parse(JSON.stringify(vm));
  broken.cards[0].state = 'mystery';
  assert.throws(() => ui.render(broken, page(base()).page), /unknown variant state "mystery"/);
  assert.throws(() => ui.render(Object.assign({}, vm, { page_state: 'odd' }), page(base()).page), /unknown page state "odd"/);
});

test('test_render_not_supported_card_has_no_money', () => {
  const { parts } = page(base({ inputs: { replacement_rate: 0.1 } }));
  const s3plain = cardHtml(parts.cards, 's3plain');
  assert.ok(s3plain.includes('data-state="not_supported"'));
  assert.ok(s3plain.includes('<p class="card-state">not supported for this workload</p>'));
  assert.match(s3plain, /<p class="card-reason">[^<]*cannot replace a replica/);
  assert.equal(/\$|data-kind="money"|card-total|card-deltas|class="bar"/.test(s3plain), false, s3plain);
  assert.equal(s3plain.includes('cheapest'), false);
  // Its table cells say why and do not open a drill.
  const cells = [...parts.table.matchAll(/<td class="cell-off" data-state="not_supported">([^<]*)/g)].map((m) => m[1]);
  assert.ok(cells.length > 0 && cells.every((t) => t === 'not supported'), cells.join('|'));
  assert.equal(/data-variant="s3plain" data-operation=/.test(parts.table), false);
});

test('test_cell_click_handler_sets_open_cell', () => {
  const s0 = base();
  const before = page(s0);
  assert.equal(before.vm.drill, null);
  assert.equal(before.parts.drill, '');
  const { state, ui: u } = ui.update(C, s0, {}, { type: 'cell', data: { action: 'cell', variant: 's3', operation: 'storage' } }, { fields: before.fields });
  assert.equal(state.openCell, 's3.storage');
  assert.equal(s0.openCell, null, 'the reducer does not mutate its input');
  const after = page(state, u);
  assert.equal(after.vm.drill.variant, 's3');
  assert.match(after.parts.drill, /<section class="section drill-view" data-cell="s3\.storage">/);
  assert.match(after.parts.table, /data-variant="s3" data-operation="storage" aria-pressed="true"/);
  // "Show calculation" reveals the five stages; closing clears the open cell.
  const shown = ui.update(C, state, u, { type: 'show-calc', data: {} }, { fields: after.fields });
  assert.equal((page(shown.state, shown.ui).parts.drill.match(/<li class="stage"/g) || []).length, 5);
  const closed = ui.update(C, shown.state, shown.ui, { type: 'close-drill', data: {} }, {});
  assert.equal(closed.state.openCell, null);
  assert.throws(() => ui.update(C, s0, {}, { type: 'nope', data: {} }, {}), /unknown action "nope"/);
});

test('test_render_cannot_reproduce_shows_mismatch_and_no_numbers', () => {
  const s = base({ inputs: { inserts_per_month: 1234 }, openCell: 'cas.storage' });
  const hash = urlstate.encode(Object.assign({}, s, { manifest: Object.assign({}, C.manifest, { billing_version: '9.9.9-gone' }) }));
  const dec = urlstate.decode(hash, C);
  assert.equal(dec.state, null);
  const out = ui.compute(C, s, {}, dec.problem);
  const parts = ui.renderParts(out.vm, out.page);
  assert.equal(out.vm.page_state, 'cannot_reproduce');
  assert.match(parts.cards, /<h2>Cannot reproduce this link<\/h2>/);
  assert.ok(parts.cards.includes('cannot reproduce: billing version 9.9.9-gone not present'), parts.cards);
  const items = [...parts.cards.matchAll(/<ul class="mismatches">(.*?)<\/ul>/gs)][0][1];
  assert.ok(items.includes(`billing billing_version: the link has 9.9.9-gone, this page has ${C.manifest.billing_version}`), items);
  // No number anywhere: only the mismatch list and its reason name versions; every other part is digit-free.
  const html = ui.render(out.vm, out.page);
  const outside = html.replace(/<ul class="mismatches">.*?<\/ul>/s, '').replace(/<p>cannot reproduce: [^<]*<\/p>/, '');
  // Text content only: tag names such as h2 carry digits of their own.
  const text = outside.replace(/<[^>]*>/g, ' ');
  assert.equal(/\d/.test(text), false, text);
  assert.equal(/data-value=/.test(html), false);
  assert.equal(/\$|data-kind="money"/.test(html), false);
  for (const id of ['inputs', 'table', 'drill', 'notes', 'evidence', 'chart']) assert.equal(parts[id], '', id);
  assert.match(parts.presets, /data-action="start-over"/);
  // A malformed link is reported the same way.
  const bad = ui.compute(C, s, {}, urlstate.decode('#v1.@@', C).problem);
  const badHtml = ui.render(bad.vm, bad.page);
  assert.match(badHtml, /the link is not a calculator link/);
  assert.equal(/\$|data-kind="money"/.test(badHtml), false);
  // Start over leaves the state; the page then renders numbers again.
  const next = ui.update(C, s, {}, { type: 'start-over', data: {} }, {});
  assert.equal(next.problem, null);
  assert.equal(next.state.scenario, 'logs');
});

test('test_render_prices_missing_shows_quantities_without_money', () => {
  const { vm, parts, html } = page(base({ price_preset: 'gcs' }));
  assert.equal(vm.page_state, 'prices_missing');
  assert.equal(/\$|data-kind="money"|card-total/.test(html), false);
  assert.ok(parts.cards.includes('price preset gcs has no prices (structure only). Quantities are shown without money.'));
  for (const c of vm.cards.filter((k) => k.state === 'ok')) assert.ok(cardHtml(parts.cards, c.variant).includes(`<p class="card-state">${esc(c.state_text)}</p>`));
  // The table shows the quantities each cell bills, e.g. S3 storage in GiB-months.
  const qty = vm.table.rows.flatMap((r) => r.cells).filter((c) => c.state === 'ok');
  assert.ok(qty.length > 0);
  for (const c of qty) assert.ok(parts.table.includes(`>${esc(c.text)}</button>`), c.text);
  assert.ok(qty.some((c) => /\d/.test(c.text)), 'quantities are numbers');
  assert.match(parts.chart, /this price preset has no prices/);
  assert.equal(/<svg/.test(parts.chart), false);
});

test('test_rate_units_change_display_not_totals', () => {
  const s = base();
  const day = page(s);
  const hour = page(Object.assign({}, s, { units: 'hour' }));
  assert.deepEqual(hour.vm.cards.map((k) => k.total_text), day.vm.cards.map((k) => k.total_text));
  assert.match(day.parts.inputs, /type="number"[^>]*data-id="ingest_rate" value="40"/);
  // 40 GiB/day is 1.666... GiB/hour.
  assert.match(hour.parts.inputs, /type="number"[^>]*data-id="ingest_rate" value="1\.666/);
  // A value entered per hour is normalised by the pipeline: 2 GiB/hour is 48 GiB/day.
  const edited = ui.update(C, Object.assign({}, s, { units: 'hour' }), {}, { type: 'input', data: { id: 'ingest_rate' }, value: '2' }, { fields: hour.fields });
  assert.deepEqual(edited.state.inputs.ingest_rate, { value: 2, per: 'hour' });
  assert.equal(page(edited.state).fields.ingest_rate.value, 48);
});

test('test_override_of_derived_field_survives_preset_switch_and_resets', () => {
  const s = base();
  const p0 = page(s);
  assert.equal(p0.fields.data_bytes.derived, true);
  const o = ui.update(C, s, {}, { type: 'input', data: { id: 'data_bytes' }, value: '2000' }, { fields: p0.fields });
  assert.deepEqual(o.state.overrides, { data_bytes: 2000 });
  const p1 = page(o.state);
  assert.match(p1.parts.inputs, /data-field="data_bytes"><div class="field-head"><span class="field-label">[^<]*<\/span>.*?badge-accent">overridden/s);
  assert.match(p1.parts.inputs, /data-action="reset" data-id="data_bytes"/);
  const sw = ui.update(C, o.state, {}, { type: 'scenario', data: { id: 'custom' } }, { fields: p1.fields });
  assert.deepEqual(sw.state.overrides, { data_bytes: 2000 });
  const r = ui.update(C, sw.state, {}, { type: 'reset', data: { id: 'data_bytes' } }, {});
  assert.deepEqual(r.state.overrides, {});
  // Locking data_bytes and retention_days derives ingest from them.
  let st = base();
  for (const id of ['data_bytes', 'retention_days']) st = ui.update(C, st, {}, { type: 'lock', data: { id } }, { fields: page(st).fields }).state;
  const locked = page(st).fields;
  assert.equal(locked.ingest_rate.derived, true);
  assert.equal(locked.data_bytes.locked, true);
});
