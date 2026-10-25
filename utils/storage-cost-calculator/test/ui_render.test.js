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
// Registry defaults of the fixture, not the custom preset's standard profile (ids the fixture lacks).
const base = (over) => Object.assign(ui.defaultState(C, 'custom'), { inputs: {} }, over || {});
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
  // A priced card carries badges and deltas; "comparable"/"cheapest" state lines would say nothing more.
  assert.equal(cas.includes('class="card-state"'), false);
  assert.ok(cas.includes('badge-ok">cheapest'));
  assert.ok(cas.includes('title="Stored data: $23.00"'), cas);
  assert.ok(cas.includes('<li>vs EBS: −$56.99</li>') && cas.includes('<li>vs S3: −$24.58</li>'));
  const ebs = cardHtml(parts.cards, 'ebs');
  assert.equal(ebs.includes('comparable'), false);
  assert.ok(ebs.includes('<li>vs S3: +$32.41</li>'));
  assert.equal(ebs.includes('cheapest'), false);
  // Table cell money, and the cell is a drill button.
  assert.match(parts.table, /data-variant="cas" data-operation="storage"[^>]*>\$23\.00<\/button>/);
  // The five stages with their substituted values.
  const stages = [...parts.drill.matchAll(/<li class="stage" data-stage="(\d)"><h4>([^<]+)<\/h4>/g)].map((m) => [m[1], m[2]]);
  assert.deepEqual(stages, [['1', 'Inputs used'], ['2', 'Branch taken per row'], ['3', 'Ledger rows'], ['4', 'Billing lines'], ['5', 'Allocation']]);
  const stage = (n) => parts.drill.slice(parts.drill.indexOf(`data-stage="${n}"`), parts.drill.indexOf(`data-stage="${n + 1}"`) > 0 ? parts.drill.indexOf(`data-stage="${n + 1}"`) : undefined);
  assert.ok(stage(1).includes('<li>inserts_per_month = 1,000'), stage(1));
  assert.ok(stage(2).includes(esc('request PutObject ok: inserts_per_month > 500: true, then taken')), stage(2));
  assert.ok(stage(3).includes('<li>request PutObject ok: 2,000 (predicted)'), stage(3));
  assert.ok(stage(4).includes('<li>PUT, COPY, POST and LIST requests: 2,000 requests = $0.01 (share 100% of 2,000 requests = $0.01)'), stage(4));
  const alloc = vm.drill.stages[4].lines;
  assert.ok(alloc.length > 0);
  for (const l of alloc) assert.ok(stage(5).includes(`<li>${esc(l.text)}`), l.text);
  // The formula of the row is rendered as code.
  assert.ok(stage(2).includes('<div class="formula"><code>'));
  // Every container has its part, in page order.
  assert.deepEqual([...html.matchAll(/<div data-part="(\w+)">/g)].map((m) => m[1]), ui.PARTS);
  // Simple inputs: a slider and an editable number per numeric input, the derived member greyed with its rule.
  // Sliders span the view model's bounds: the sweep ladder for data_bytes; no ladder and no range means no slider.
  assert.match(parts.inputs, /type="range" min="250" max="8000" step="any" data-action="slider" data-id="data_bytes" value="1000"/);
  assert.equal(/type="range"[^>]*data-id="inserts_per_month"/.test(parts.inputs), false);
  assert.match(parts.inputs, /type="number" step="1" data-action="input" data-id="inserts_per_month" value="1000"/);
  assert.match(parts.inputs, /class="field is-derived" data-field="data_bytes"/);
  // The derivation rule sits behind (i), not under the slider.
  assert.equal(parts.inputs.includes(esc('Derived from ingest_rate × retention_days')), false);
  assert.match(page(base(), { info: { kind: 'input', id: 'data_bytes' } }, c).parts.modal, new RegExp(`<th scope="row">Derived</th><td>${esc('Derived from ingest_rate × retention_days')}</td>`));
  // The evidence is one line with an (i); the statement is in the dialog.
  assert.match(parts.evidence, /data-action="info" data-info-kind="evidence"/);
  assert.equal(page(base(), { info: { kind: 'evidence', id: 'evidence' } }).vm.info.description, MINIMAL.evidence_statement);
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
  const cells = [...parts.table.matchAll(/<td class="cell-off" data-label="S3 plain rewritable" data-state="not_supported">([^<]*)/g)].map((m) => m[1]);
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
  // Said once: one sentence with the versions, then each differing entry without repeating the artifact word.
  assert.ok(parts.cards.includes(`<p class="reason">This link was built with billing 9.9.9-gone; this page has billing ${C.manifest.billing_version}. The link was made with contract version ${C.manifest.contract_version}.</p>`), parts.cards);
  const items = [...parts.cards.matchAll(/<ul class="mismatches">(.*?)<\/ul>/gs)][0][1];
  assert.equal(items, `<li>billing: version 9.9.9-gone in the link, ${C.manifest.billing_version} here</li>`);
  // No number anywhere: only the mismatch list and its reason name versions; every other part is digit-free.
  const html = ui.render(out.vm, out.page);
  const outside = html.replace(/<ul class="mismatches">.*?<\/ul>/s, '').replace(/<p class="reason">[^<]*<\/p>/, '');
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
  assert.ok(parts.cards.includes('<p class="banner">This price list has no prices yet; quantities are shown without money.</p>'));
  assert.equal(/note-prices_missing/.test(parts.notes), false, 'said once, in the banner');
  for (const c of vm.cards.filter((k) => k.state === 'ok')) assert.ok(cardHtml(parts.cards, c.variant).includes(`<p class="card-state">${esc(c.state_text)}</p>`));
  // The table shows the quantities each cell bills, e.g. S3 storage in GiB-months.
  const qty = vm.table.rows.flatMap((r) => r.cells).filter((c) => c.state === 'ok');
  assert.ok(qty.length > 0);
  for (const c of qty) assert.ok(parts.table.includes(`>${esc(c.text)}</button>`), c.text);
  assert.ok(qty.some((c) => /\d/.test(c.text)), 'quantities are numbers');
  assert.match(parts.chart, /<select data-action="sweep" disabled>.*this price list has no prices/s);
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

test('test_render_custom_prices_editable_and_unpriced_rule_empty', () => {
  const c = load((x) => { x.billing.presets[0].transfer.push({ id: 'internet', price_per_gb: null, source_url: 'https://example.com/internet' }); });
  const { parts } = page(base({ price_preset: 'custom' }), {}, c);
  assert.match(parts.inputs, /data-path="request_classes\.put_copy_post_list\.price_per_1000" value="0\.005"/);
  assert.match(parts.inputs, /data-path="transfer\.internet\.price_per_gb" value="" placeholder="not priced by this preset"/);
  assert.equal(parts.inputs.includes('value="null"'), false);
  // Another preset shows prices as text, not inputs.
  assert.equal(/data-action="price-field"/.test(page(base()).parts.inputs), false);
});

test('test_inputs_show_labels_fixed_values_and_view_model_ranges', () => {
  const c = load((x) => {
    const q = x.registry.find((r) => r.id === 'replacement_rate');
    q.label = 'Replica replacements';
    q.validity = { replacement_rate: [0, 0] };
    x.registry.find((r) => r.id === 'inserts_per_month').label = 'Inserts per month';
    x.registry.find((r) => r.id === 'inserts_per_month').validity = { inserts_per_month: [0, 50000] };
    delete x.registry.find((r) => r.id === 'mutations_per_month').label;
  });
  const { parts, vm } = page(base(), {}, c);
  assert.ok(parts.inputs.includes('<span class="field-label">Inserts per month</span>'));
  // The measured range of a form field is in its (i) dialog.
  assert.equal(parts.inputs.includes('measured for 0 to 50,000'), false);
  assert.match(page(base(), { info: { kind: 'input', id: 'inserts_per_month' } }, c).parts.modal, /<th scope="row">Verified range<\/th><td>verified at 0 to 50,000<\/td>/);
  // A one-value range is a fixed value, shown as text without a slider or a box.
  const fixed = parts.inputs.slice(parts.inputs.indexOf('data-field="replacement_rate"'));
  const fixedField = fixed.slice(0, fixed.indexOf('data-field=', 1));
  assert.ok(fixedField.includes('<span class="value">0</span>'), fixedField);
  assert.equal(/type="range"|type="number"/.test(fixedField), false, fixedField);
  // A row without a label shows no id in its place and is reported.
  assert.ok(parts.inputs.includes('data-field="mutations_per_month"><div class="field-head"><span class="field-label"></span>'));
  // A contract defect is a view-model problem; the page does not render diagnostics.
  assert.ok(vm.problems.some((p) => /registry mutations_per_month has no label/.test(p)));
  assert.equal(parts.notes.includes('has no label'), false);
  assert.ok(vm.problems.length > 0);
});

test('test_chart_has_y_ticks_and_gridlines', () => {
  const { parts, vm } = page(base({ sweepInput: 'data_bytes' }));
  const ticks = vm.chart.y_ticks;
  assert.ok(ticks.length >= 3);
  for (const t of ticks) assert.ok(parts.chart.includes(`text-anchor="end">${esc(t.text)}</text>`), t.text);
  assert.equal((parts.chart.match(/<line class="grid"/g) || []).length, ticks.length);
  for (const t of vm.chart.x_ticks) assert.ok(parts.chart.includes(`text-anchor="middle">${esc(t.text)}</text>`), t.text);
});

test('test_clearing_a_custom_price_returns_to_the_preset_value', () => {
  const s = base({ price_preset: 'custom', custom_prices: { 'block.gp3.price_per_gib_month': 1 } });
  const r = ui.update(C, s, {}, { type: 'price-field', data: { path: 'block.gp3.price_per_gib_month' }, value: '  ' }, {});
  assert.deepEqual(r.state.custom_prices, {});
  assert.throws(() => ui.update(C, s, {}, { type: 'price-field', data: { path: 'block.gp3.price_per_gib_month' }, value: '-1' }, {}), /enter a number of 0 or more/);
});

// A document with only what mount touches: containers, delegated listeners, the hash and history.
function fakeDocument(hash) {
  const boxes = Object.fromEntries(ui.PARTS.map((id) => [id, { id, innerHTML: '' }]));
  const dialogListeners = {};
  Object.assign(boxes.modal, {
    open: false, showModal() { this.open = true; }, close() { this.open = false; (dialogListeners.close || []).forEach((fn) => fn()); },
    addEventListener: (type, fn) => { (dialogListeners[type] = dialogListeners[type] || []).push(fn); },
  });
  const listeners = {};
  const attrs = {};
  const replaced = [];
  const win = {
    location: { hash },
    history: { replaceState: (a, b, url) => { replaced.push(url); win.location.hash = url; } },
    localStorage: { getItem: () => { throw new Error('blocked'); }, setItem: () => { throw new Error('blocked'); } },
    matchMedia: () => ({ matches: false, addEventListener: () => {} }),
  };
  const doc = {
    defaultView: win, activeElement: null,
    documentElement: { getAttribute: (k) => attrs[k] || null, setAttribute: (k, v) => { attrs[k] = v; } },
    body: { setAttribute: (k, v) => { attrs[`body:${k}`] = v; } },
    getElementById: (id) => boxes[id] || null,
    querySelector: () => null,
    addEventListener: (type, fn) => { (listeners[type] = listeners[type] || []).push(fn); },
  };
  const fire = (type, el) => (listeners[type] || []).forEach((fn) => fn({ target: { closest: (sel) => (sel.startsWith('button') === (el.tagName === 'BUTTON') || sel === '[data-action]' ? el : null) } }));
  return { doc, boxes, attrs, replaced, fire };
}

test('test_mount_delegated_click_opens_cell_and_rewrites_hash', () => {
  const s = base();
  const f = fakeDocument(urlstate.encode(Object.assign({}, s, { manifest: C.manifest })));
  ui.mount(f.doc, C);
  assert.ok(f.boxes.cards.innerHTML.includes('$23.01'));
  assert.equal(f.boxes.drill.innerHTML, '');
  assert.equal(f.attrs['body:data-page-state'], 'ok');
  f.fire('click', { tagName: 'BUTTON', dataset: { action: 'cell', variant: 'cas', operation: 'storage' } });
  assert.match(f.boxes.drill.innerHTML, /data-cell="cas\.storage"/);
  const last = urlstate.decode(f.replaced[f.replaced.length - 1], C);
  assert.equal(last.state.openCell, 'cas.storage');
  // A failing action shows its message as a page note instead of breaking the page.
  f.fire('change', { tagName: 'INPUT', dataset: { action: 'price-field', path: 'block.gp3.price_per_gib_month' }, value: 'abc' });
  assert.match(f.boxes.notes.innerHTML, /note-error[^>]*>price block\.gp3\.price_per_gib_month: enter a number/);
  // A missing container is an error.
  const g = fakeDocument('');
  g.doc.getElementById = (id) => (id === 'chart' ? null : { innerHTML: '' });
  assert.throws(() => ui.mount(g.doc, C), /container #chart is missing/);
});

test('test_page_and_breakeven_document_sweep_the_same_ladder', async () => {
  const presets = require('../presets.js');
  const { buildBreakeven, scenarioLadders } = await import('../tools/sweeps.mjs');
  const { vm, page: chrome, parts, fields } = page(base({ sweepInput: 'ingest_rate' }));
  // The page sweeps ×1/4 … ×8 around the current value with the same function as the document.
  const want = presets.ladderFor('ingest_rate', fields.ingest_rate.value, fields.ingest_rate.validity);
  assert.deepEqual([...new Set(vm.chart.points.map((p) => p.x))], want);
  const doc = buildBreakeven({ pipeline: require('../pipeline.js') }, C, { custom: presets.SCENARIOS.custom }, scenarioLadders, 'aws-us-east-1', ['ingest_rate']);
  assert.deepEqual(doc.custom.sweeps.ingest_rate.values, want);
  assert.deepEqual(doc.custom.sweeps.ingest_rate.crossing, vm.chart.breakEven.length ? vm.chart.breakEven.map(({ a, b, x_between, extrapolated }) => ({ a, b, x_between, extrapolated })) : null);
  // The current value is marked on the chart.
  assert.equal(vm.chart.current_x, 40);
  assert.match(parts.chart, /<line class="current"[^>]*><title>Current: 40 GiB\/day<\/title><\/line>/);
  // Only the shared sweep inputs are offered.
  assert.deepEqual(chrome.sweep_inputs.map((s) => s.id), Object.keys(presets.SWEEP_INPUTS).filter((id) => C.registry.some((q) => q.id === id)));
  // A zero value cannot be scaled: no chart, and the page says why.
  const zero = page(base({ sweepInput: 'data_bytes', overrides: { data_bytes: 0 } }));
  assert.equal(zero.vm.chart, null);
  assert.match(zero.parts.chart, /<select data-action="sweep" disabled>.*<\/select><span class="field-note chart-empty">The chart scales the current value, so it needs a value above zero\.<\/span>/s);
  // A drawable chart keeps the selector enabled.
  assert.match(parts.chart, /<select data-action="sweep">/);
  // The Lock button explains itself.
  assert.match(page(base()).parts.inputs, /data-action="lock" data-id="data_bytes" aria-pressed="false" title="Lock this value and derive another member">Lock</);
});

test('test_billing_rows_show_labels_and_given_percentages', () => {
  const presets = require('../presets.js');
  const aws = presets.resolvePrice(C, 'aws-us-east-1');
  assert.equal(presets.ruleLabel('storage', aws), 'S3 storage');
  assert.equal(presets.ruleLabel('put_copy_post_list', aws), 'PUT, COPY, POST and LIST requests');
  assert.equal(presets.ruleLabel('ec2_ec2_cross_az', aws), 'Transfer EC2 to EC2 across availability zones');
  assert.throws(() => presets.ruleLabel('mystery', aws), /not a rule of price preset aws-us-east-1/);
  // Phase-0 drill: the label in the cell, the id only in data-rule, the percentage as given (no arithmetic).
  const html = ui.renderDrillHtml({ probe: 'p', cells: { k: {
    stage1_inputs: {}, stage2_rows: [], stage3_ledger_summary: {},
    stage4_billing_lines: [{ rule_id: 'storage', label: 'S3 storage', unit: 'GB-month', quantity: 250, money: 5.75, share: 0.25, share_text: '25.0%', aggregate_money: 23, note: '' }],
    billing_total: 5.75 } } });
  assert.match(html, /<tr data-rule="storage"><td>S3 storage<\/td>/);
  assert.ok(html.includes('(25.0% of <span data-value="23" data-kind="money">$23</span>)'), html);
  assert.equal(/<td>storage<\/td>/.test(html), false);
});

test('test_pinned_rule_badge_names_the_source_of_the_rule', () => {
  const c = load((x) => {
    const pin = (id, kind, ref) => x.registry.push(Object.assign(JSON.parse(JSON.stringify(x.registry.find((r) => r.id === 'replicas'))), { id, label: id, kind: 'pinned', default: 4, evidence: { kind, ref, version: '1' } }));
    pin('ebs_pinned_by_provider', 'provider', 'https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html');
    pin('pinned_by_clickhouse', 'source', 'src/Core/Settings.cpp:1');
  });
  // Model constants are not on the form; the info dialog names where the rule comes from.
  const modal = (id) => page(base(), { info: { kind: 'input', id } }, c).parts;
  assert.equal(page(base(), {}, c).parts.inputs.includes('data-field="ebs_pinned_by_provider"'), false);
  assert.match(modal('ebs_pinned_by_provider').modal, /<th scope="row">Kind<\/th><td>provider rule<\/td>/);
  assert.match(modal('pinned_by_clickhouse').modal, /<th scope="row">Kind<\/th><td>fixed by ClickHouse<\/td>/);
});

test('test_a_state_that_throws_never_blanks_the_page', () => {
  // A link whose values pass decode but fail in compute shows the cannot-reproduce view.
  const real = globalThis.SCC.urlstate.decode;
  globalThis.SCC.urlstate.decode = () => ({ state: Object.assign(base(), { openCell: 'nodot' }), problem: null });
  try {
    const f = fakeDocument('#v1.x');
    ui.mount(f.doc, C);
    assert.equal(f.attrs['body:data-page-state'], 'cannot_reproduce');
    assert.match(f.boxes.cards.innerHTML, /Cannot reproduce this link/);
    assert.match(f.boxes.cards.innerHTML, /the state in this link cannot be computed: ui: open cell &quot;nodot&quot;/);
  } finally {
    globalThis.SCC.urlstate.decode = real;
  }
  // An action that fails in compute returns to the last state that rendered, with the error as a note.
  const g = fakeDocument('');
  ui.mount(g.doc, C);
  g.fire('change', { tagName: 'SELECT', dataset: { action: 'sweep' }, value: 'foo' });
  assert.equal(g.attrs['body:data-page-state'], 'ok');
  assert.match(g.boxes.notes.innerHTML, /note-error[^>]*>foo is not a sweep input/);
  assert.ok(g.boxes.cards.innerHTML.includes('card-total'));
});
