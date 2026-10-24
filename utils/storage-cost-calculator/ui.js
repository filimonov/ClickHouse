// Calculator page: the phase-0 measurement drill (renderDrillHtml) and the two-panel page (render, mount).
// The renderers are pure (data in, HTML string out) so Node can render and check the exact page markup.
(function (root) {
  'use strict';

  const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

  const fmt = (v) => {
    if (typeof v !== 'number') return esc(v);
    return Number.isInteger(v) ? String(v) : String(Number(v.toPrecision(10)));
  };

  // Every number carries its exact value in data-value so checks compare values, not rounded text.
  const num = (v) => (v === null || v === undefined ? '<span class="mute">n/a</span>' : `<span data-value="${esc(v)}">${fmt(v)}</span>`);
  const money = (v) => `<span data-value="${esc(v)}" data-kind="money">$${fmt(v)}</span>`;
  const kv = (obj) => Object.entries(obj || {}).map(([k, v]) => `<tr><th scope="row">${esc(k)}</th><td>${typeof v === 'number' ? num(v) : esc(JSON.stringify(v))}</td></tr>`).join('');
  const table = (head, body) => `<div class="tbl"><table><thead><tr>${head.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${body}</tbody></table></div>`;

  function expectedText(e) {
    if (e === null || e === undefined) return '<span class="mute">n/a</span>';
    return typeof e === 'number' ? num(e) : `${num(e.min)}..${num(e.max)}`;
  }

  function stage1(c) {
    const s = c.stage1_inputs || {};
    return `<section class="stage" data-stage="1"><h4>1. Inputs</h4><div class="tbl"><table><tbody>`
      + `${kv(s.inputs)}${kv(s.coefs)}${kv(s.observations)}</tbody></table></div></section>`;
  }

  function stage2(key, c) {
    const rows = (c.stage2_rows || []).map((r, i) => {
      const exact = r.exact === true ? 'true' : r.exact === false ? 'false' : 'n/a';
      // A measured row feeds the record's own number back: it is labelled, never called exact.
      const check = exact === 'false' ? 'MISMATCH' : r.basis === 'measured' ? 'measured' : exact === 'true' ? 'exact' : '-';
      return `<tr data-row="${esc(key)}#${i}" data-basis="${esc(r.basis || 'n/a')}" data-exact="${exact}"><td>${esc(r.row)}</td><td>${esc((r.branch_taken || []).join(', ') || '-')}</td>`
        + `<td>${num(r.predicted)}</td><td>${num(r.measured)}</td><td>${expectedText(r.expected)}</td><td>${check}</td></tr>`;
    }).join('');
    return `<section class="stage" data-stage="2"><h4>2. Rows: branch taken, predicted vs measured</h4>${table(['row', 'branch', 'predicted', 'measured', 'expected', 'check'], rows)}</section>`;
  }

  function stage3(c) {
    const s = c.stage3_ledger_summary || {};
    const types = Object.entries(s.by_row || {}).map(([t, v]) => `<tr><th scope="row">${esc(t)}</th><td>${num(v.rows)}</td><td>${num(v.quantity)}</td></tr>`).join('');
    const states = (s.states || []).map((x) => `<li>${esc(x.id)}: ${x.active === null ? 'unknown' : x.active ? 'active' : 'inactive'} ${esc(x.reason || '')}</li>`).join('');
    return `<section class="stage" data-stage="3"><h4>3. Ledger summary</h4>${table(['row type and op', 'rows', 'quantity'], types)}`
      + `${states ? `<ul>${states}</ul>` : ''}${s.extrapolated ? '<p class="warn">extrapolated outside a coefficient validity range</p>' : ''}</section>`;
  }

  function stage4(c) {
    if (c.state === 'unavailable') {
      const why = (c.unavailable_reasons || []).map((r) => `<li>${esc(r)}</li>`).join('');
      return `<section class="stage" data-stage="4" data-state="unavailable"><h4>4. Billing</h4><p class="warn">unavailable: no total</p><ul>${why}</ul></section>`;
    }
    const lines = c.stage4_billing_lines;
    if (!lines) return '<section class="stage" data-stage="4"><h4>4. Billing</h4><p class="mute">not billed: a row is unavailable</p></section>';
    const share = (l) => (typeof l.share === 'number' && l.share < 1 ? ` (${fmt(100 * l.share)}% of ${money(l.aggregate_money)})` : '');
    const body = lines.map((l) => `<tr data-rule="${esc(l.rule_id)}"><td>${esc(l.rule_id)}</td><td>${num(l.quantity)} ${esc(l.unit)}</td><td>${money(l.money)}${share(l)}</td><td>${esc(l.note || '')}</td></tr>`).join('');
    return `<section class="stage" data-stage="4"><h4>4. Billing lines</h4>${table(['rule', 'quantity', 'money', 'note'], body)}<p>Total ${money(c.billing_total)}</p></section>`;
  }

  function provisioning(a) {
    const alt = (a.alternatives || []).map((x) => `<tr><td>${esc(x.layout)}</td><td>${num(x.volumes)}</td><td>${num(x.size_gib)}</td><td>${num(x.iops)}</td><td>${num(x.mibps)}</td><td>${money(x.monthly_cost)}</td></tr>`).join('');
    return `<p>Provisioning ${esc(a.layout || 'manual')}: ${num(a.volumes)} x ${num(a.size_gib)} GiB, ${num(a.iops)} IOPS, ${num(a.mibps)} MiB/s, ${money(a.monthly_cost)} per month</p>`
      + `${table(['layout', 'volumes', 'GiB', 'IOPS', 'MiB/s', 'cost'], alt)}`;
  }

  // Stage 5: the record's lines are billed once for all its cells; this is how each line's money splits.
  function stage5(c, group) {
    const a = c.stage5_allocation;
    if (!a || !group) return '<section class="stage" data-stage="5"><h4>5. Allocation</h4><p class="mute">nothing billed</p></section>';
    const cellKeys = group.cells || [];
    const body = (group.allocation || []).map((x) => `<tr data-alloc="${esc(x.rule_id)}"><td>${esc(x.rule_id)}</td><td>${money(x.money)}</td><td>${esc(x.basis)}</td>`
      + cellKeys.map((k) => `<td>${x.shares[k] ? money(x.shares[k].money) : '<span class="mute">-</span>'}</td>`).join('') + '</tr>').join('');
    return `<section class="stage" data-stage="5"><h4>5. Allocation across ${esc(cellKeys.join(', '))}</h4>`
      + `${table(['rule', 'billed once', 'basis', ...cellKeys], body)}${a.provisioning ? provisioning(a.provisioning) : ''}</section>`;
  }

  // What qualifies the number: shown under the price, never separable from it.
  function evidence(c) {
    const e = c.evidence;
    if (!e) return '<section class="evidence" data-evidence="missing"><p class="warn">no evidence recorded</p></section>';
    const lim = (e.limitations || []).map((l) => `<li>${esc(l)}</li>`).join('');
    const bounds = Object.entries((e.bounds || {}).byte_hours || {}).map(([k, b]) =>
      `<tr><th scope="row">${esc(k)}</th><td>${num(b.lower)}..${num(b.upper)} byte-hours</td></tr>`).join('');
    return `<section class="evidence" data-evidence="present"><h4>Evidence</h4><p>${esc(e.statement || '')}</p>`
      + `<p>mode ${esc(e.mode || '')}; initial inventory ${esc(e.inventory_initial || 'n/a')}${e.inventory_note ? ` (${esc(e.inventory_note)})` : ''}; `
      + `ledger ${e.ledger_complete ? 'complete' : `incomplete, ${num(e.open_attempts)} open attempts`}</p>`
      + `${lim ? `<ul class="limitations">${lim}</ul>` : ''}${bounds ? `<div class="tbl"><table><tbody>${bounds}</tbody></table></div>` : ''}</section>`;
  }

  function renderDrillHtml(drill) {
    const groups = drill.groups || {};
    const cells = Object.entries(drill.cells || {}).map(([key, c]) =>
      `<article class="cell" data-cell="${esc(key)}" data-state="${esc(c.state || 'ok')}"><h3>${esc(key)}</h3>${stage1(c)}${stage2(key, c)}${stage3(c)}`
      + `${stage4(c)}${evidence(c)}${stage5(c, groups[c.group])}</article>`).join('');
    const bad = (drill.mismatches || []).map((m) => `<li>${esc(m)}</li>`).join('');
    return `<div class="drill" data-probe="${esc(drill.probe)}"><h2>${esc(drill.probe)} <small>${esc(drill.measurement_id || '')}</small></h2>`
      + `${bad ? `<ul class="mismatches">${bad}</ul>` : ''}${cells}</div>`;
  }

  function renderDrill(container, drill) {
    container.innerHTML = renderDrillHtml(drill);
  }


  // ---- the calculator page ----------------------------------------------------------------------
  // Every number on the page is a view-model value or text (viewmodel.js); this part only lays it out.

  const lib = (name, file) => () => (root.SCC && root.SCC[name]) || require(file);
  const pipeline = lib('pipeline', './pipeline.js');
  const presetsLib = lib('presets', './presets.js');
  const viewmodel = lib('viewmodel', './viewmodel.js');
  const urlstate = lib('urlstate', './urlstate.js');

  // Container ids in index.html, in page order: left panel first, then the results panel.
  const PARTS = ['presets', 'inputs', 'cards', 'table', 'drill', 'notes', 'evidence', 'chart'];
  const PAGE_STATES = ['ok', 'prices_missing', 'cannot_reproduce'];
  const VARIANT_STATES = ['ok', 'not_supported', 'unavailable', 'backlog_grows', 'unsupported_cut'];
  const CELL_STATES = [...VARIANT_STATES, 'not_applicable'];
  const UNITS = [['second', 'per second'], ['minute', 'per minute'], ['hour', 'per hour'], ['day', 'per day'], ['month', 'per month']];
  const CLASS_LABELS = { bytes: 'Stored data', put: 'Write requests', get: 'Read requests', transfer: 'Transfer', block: 'Block storage' };
  const DEFAULT_SCENARIO = 'logs';
  const DEFAULT_PRICE = 'aws-us-east-1';
  const THEME_KEY = 'scc-theme';

  const known = (list, v, what) => {
    if (!list.includes(v)) throw new Error(`ui: unknown ${what} ${JSON.stringify(v)}`);
    return v;
  };
  const badge = (text, cls) => `<span class="badge${cls ? ` badge-${cls}` : ''}">${esc(text)}</span>`;
  const extrapolatedBadge = (flag) => (flag ? badge('extrapolated', 'warn') : '');
  const attr = (name, v) => (v === null || v === undefined ? '' : ` ${name}="${esc(v)}"`);
  const list = (items, cls) => (items.length ? `<ul${attr('class', cls)}>${items.join('')}</ul>` : '');
  const link = (ref) => (/^https?:\/\//.test(ref) ? `<a href="${esc(ref)}" rel="noopener" target="_blank">${esc(ref)}</a>` : esc(ref));

  // Rates are stored per day; the unit selector only changes how they are shown and entered. The factor is the
  // pipeline's own (normaliseRate), so a value entered per hour and normalised back reproduces the same total.
  function rateIn(perDay, unit) {
    return perDay / pipeline().normaliseRate(1, unit);
  }
  const rateUnit = (unit, per) => unit.replace(/\/day$/, `/${per}`);

  // ---- left panel -------------------------------------------------------------------------------

  function renderPresets(vm, page) {
    if (vm.page_state === 'cannot_reproduce') {
      return '<section class="panel-block"><h2>Start again</h2><p class="mute">This link cannot be shown with this version of the page.</p>'
        + '<button type="button" class="btn btn-primary" data-action="start-over">Start over with a preset</button></section>';
    }
    const sc = page.scenarios.map((s) => `<button type="button" class="pill" data-action="scenario" data-id="${esc(s.id)}" aria-pressed="${s.id === page.scenario}">${esc(s.label)}</button>`).join('');
    const current = page.scenarios.find((s) => s.id === page.scenario);
    const pr = page.prices.map((p) => `<option value="${esc(p.id)}"${p.id === page.price_preset ? ' selected' : ''}>${esc(p.label)}</option>`).join('');
    return '<section class="panel-block"><h2>Workload</h2>'
      + `<div class="pills" role="group" aria-label="Scenario">${sc}</div>`
      + `${current ? `<p class="mute">${esc(current.description)}</p>` : ''}`
      + `<label class="field-row"><span class="field-label">Prices</span><select data-action="price">${pr}</select></label></section>`;
  }

  function rangeText(f) {
    if (f.range) return `measured for ${f.range[0]} to ${f.range[1]}`;
    if (f.validity) return Object.entries(f.validity).map(([p, r]) => `measured for ${p} ${r[0]} to ${r[1]}`).join('; ');
    return '';
  }

  function fieldTags(f) {
    const tags = [];
    if (f.derived && f.kind === 'input') tags.push(badge('derived'));
    if (f.overridden) tags.push(badge('overridden', 'accent'));
    if (f.out_of_range) tags.push(badge('outside measured range', 'warn'));
    if (f.kind === 'pinned') tags.push(badge('fixed by ClickHouse'));
    if (f.kind === 'measured') tags.push(badge('measured'));
    return tags.join('');
  }

  // Slider bounds are control geometry, not a displayed number: the measured range when there is one, otherwise
  // a span around the current value.
  function sliderBounds(f, v) {
    if (f.range && f.range[1] > f.range[0]) return [f.range[0], f.range[1]];
    const top = v > 0 ? v * 4 : 100;
    return [0, top];
  }

  function control(f, page, simple) {
    const busy = page.cannot_reproduce;
    if (f.kind === 'price') {
      if (!f.editable) return `<span class="value">${esc(f.value_text)}</span>`;
      // A rule the preset does not price (value null) starts empty; its value_text says so.
      const v = f.value === null ? '' : f.value;
      return `<span class="control"><input type="number" step="any" min="0" data-action="price-field" data-path="${esc(f.path)}" value="${esc(v)}"`
        + `${f.value === null ? ` placeholder="${esc(f.value_text)}"` : ''} aria-label="${esc(f.label)}"><span class="unit">${esc(f.per)}</span></span>`;
    }
    if (!f.editable || busy) return `<span class="value">${esc(f.value_text)}</span>`;
    if (f.value_type === 'enum') {
      if (f.enum.includes('auto')) {
        return `<input type="text" inputmode="numeric" data-action="input" data-id="${esc(f.id)}" value="${esc(f.value === null ? '' : f.value)}" aria-label="${esc(f.label)}" placeholder="auto or a number">`;
      }
      const opts = f.enum.map((e) => `<option value="${esc(e)}"${e === f.value ? ' selected' : ''}>${esc(e)}</option>`).join('');
      return `<select data-action="input" data-id="${esc(f.id)}" aria-label="${esc(f.label)}">${opts}</select>`;
    }
    const v = f.value === null ? null : f.rate ? rateIn(f.value, page.units) : f.value;
    const num = `<input type="number" step="any" data-action="input" data-id="${esc(f.id)}" value="${esc(v === null ? '' : v)}" aria-label="${esc(f.label)}">`;
    const unit = f.rate ? `<select class="unit" data-action="units" aria-label="Rate unit">${UNITS.map(([u, l]) => `<option value="${u}"${u === page.units ? ' selected' : ''}>${esc(rateUnit(f.unit, u))}</option>`).join('')}</select>`
      : f.unit && f.unit !== 'count' ? `<span class="unit">${esc(f.unit)}</span>` : '';
    if (!simple || v === null) return `<span class="control">${num}${unit}</span>`;
    const [lo, hi] = sliderBounds(f, v);
    return `<input type="range" min="${esc(lo)}" max="${esc(hi)}" step="any" data-action="slider" data-id="${esc(f.id)}" value="${esc(v)}" aria-label="${esc(f.label)}">`
      + `<span class="control">${num}${unit}</span>`;
  }

  function renderField(f, page, simple) {
    const lock = f.locked === null || f.locked === undefined || page.cannot_reproduce ? ''
      : `<button type="button" class="btn btn-small" data-action="lock" data-id="${esc(f.id)}" aria-pressed="${f.locked}">${f.locked ? 'Locked' : 'Lock'}</button>`;
    const reset = f.overridden ? `<button type="button" class="btn btn-small" data-action="reset" data-id="${esc(f.id)}">Reset</button>` : '';
    const derived = f.derived && f.rule_text ? `<p class="field-note">Derived: <code>${esc(f.rule_text)}</code></p>` : '';
    const range = rangeText(f);
    const price = f.kind === 'price' ? `<p class="field-note">as of ${esc(f.as_of || 'n/a')}${f.source_url ? ` · ${link(f.source_url)}` : ''}</p>` : '';
    return `<div class="field${f.derived ? ' is-derived' : ''}${f.overridden ? ' is-overridden' : ''}" data-field="${esc(f.id)}">`
      + `<div class="field-head"><span class="field-label">${esc(f.label)}</span>${fieldTags(f)}${lock}${reset}</div>`
      + `<div class="field-control">${control(f, page, simple)}</div>${derived}`
      + `${range ? `<p class="field-note">${esc(range)}</p>` : ''}${price}</div>`;
  }

  function renderHints(b) {
    return b.hints.map((h) => {
      const rows = Object.entries(h.mapping).map(([col, field]) => `<tr><td><code>${esc(col)}</code></td><td>${esc(field)}</td></tr>`).join('');
      return `<details class="hint"><summary>Fill these from your cluster</summary><pre><code>${esc(h.sql)}</code></pre>`
        + `${rows ? `<div class="tbl"><table><thead><tr><th>Column</th><th>Field</th></tr></thead><tbody>${rows}</tbody></table></div>` : ''}</details>`;
    }).join('');
  }

  function renderInputs(vm, page) {
    if (vm.page_state === 'cannot_reproduce') return '';
    const groups = vm.inputs.groups.map((g) => `<p class="group-note">${esc(g.label)}. Lock the two you know; the third is derived.</p>`).join('');
    const blocks = vm.inputs.blocks.map((b) => {
      const fields = b.fields.map((f) => renderField(f, page, !b.advanced)).join('');
      if (!b.advanced) return `<section class="panel-block" data-block="${esc(b.block)}"><h2>${esc(b.label)}</h2>${groups}${fields}${renderHints(b)}</section>`;
      const any = b.fields.some((f) => f.overridden) ? badge('overridden', 'accent') : '';
      const prices = b.block === 'prices'
        ? `${b.as_of ? `<p class="field-note">Prices as of ${esc(b.as_of)}</p>` : ''}${b.note ? `<p class="field-note">${esc(b.note)}</p>` : ''}`
          + `${page.price_preset !== 'custom' && !b.note ? '<button type="button" class="btn btn-small" data-action="edit-prices">Edit prices</button>' : ''}`
        : '';
      return `<details class="block" data-block="${esc(b.block)}"${page.open_blocks.includes(b.block) ? ' open' : ''}><summary>${esc(b.label)} ${any}</summary>`
        + `<div class="block-body">${prices}${fields || '<p class="mute">Nothing to set.</p>'}${renderHints(b)}</div></details>`;
    }).join('');
    return `<div class="inputs">${blocks}</div>`;
  }

  // ---- right panel ------------------------------------------------------------------------------

  function bar(segments, currency, cls) {
    if (!segments.length) return '';
    const segs = segments.map((s) => `<span class="seg cls-${esc(s.class)}" style="flex-grow:${esc(s.fraction)}" title="${esc(`${CLASS_LABELS[s.class] || s.class}: ${viewmodel().fmtMoney(s.money, currency)}`)}"></span>`).join('');
    const legend = segments.map((s) => `<li><span class="dot cls-${esc(s.class)}"></span>${esc(CLASS_LABELS[s.class] || s.class)} <span class="mute">${esc(viewmodel().fmtMoney(s.money, currency))}</span></li>`).join('');
    return `<div class="${cls}" role="img" aria-label="Cost by kind">${segs}</div><ul class="legend">${legend}</ul>`;
  }

  function renderCard(c, vm) {
    known(VARIANT_STATES, c.state, 'variant state');
    if (c.cheapest && c.state !== 'ok') throw new Error(`ui: ${c.variant} is marked cheapest in state ${c.state}`);
    const head = `<header class="card-head"><h3>${esc(c.label)}</h3>${c.cheapest ? badge('cheapest', 'ok') : ''}${extrapolatedBadge(c.extrapolated)}</header>`;
    if (c.state !== 'ok') {
      return `<article class="card is-off" data-variant="${esc(c.variant)}" data-state="${esc(c.state)}">${head}`
        + `<p class="card-state">${esc(c.state_text)}</p>${c.reason ? `<p class="card-reason">${esc(c.reason)}</p>` : ''}</article>`;
    }
    const total = c.total_text === null ? '' : `<p class="card-total" data-kind="money">${esc(c.total_text)}</p><p class="card-per">per month</p>`;
    const deltas = list(c.delta_texts.map((t) => `<li>${esc(t)}</li>`), 'card-deltas');
    return `<article class="card${c.cheapest ? ' is-cheapest' : ''}" data-variant="${esc(c.variant)}" data-state="ok">${head}${total}`
      + `<p class="card-state">${esc(c.state_text)}</p>${deltas}${bar(c.bar_segments, vm.currency, 'bar')}</article>`;
  }

  function renderCannotReproduce(vm) {
    const reason = vm.notes.find((n) => n.kind === 'cannot_reproduce');
    const items = vm.notes.filter((n) => n.kind === 'problem').map((n) => `<li>${esc(n.text)}</li>`);
    return '<section class="cannot-reproduce" role="alert"><h2>Cannot reproduce this link</h2>'
      + `<p>${esc(reason ? reason.text : vm.page_reason)}</p>`
      + '<p class="mute">The link was made with other model or price versions. Showing its numbers with this page would show different numbers, so none are shown.</p>'
      + `${list(items, 'mismatches')}</section>`;
  }

  function renderCards(vm) {
    if (vm.page_state === 'cannot_reproduce') return renderCannotReproduce(vm);
    const banner = vm.page_state === 'prices_missing' ? `<p class="banner">${esc(vm.page_reason)}. Quantities are shown without money.</p>` : '';
    return `${banner}<div class="cards">${vm.cards.map((c) => renderCard(c, vm)).join('')}</div>`;
  }

  function renderTable(vm, page) {
    if (vm.page_state === 'cannot_reproduce') return '';
    const t = vm.table;
    if (!t.rows.length) return '<section class="section"><h2>Breakdown</h2><p class="mute">No workload type has a model yet, so there is nothing to break down.</p></section>';
    const head = t.columns.map((c) => `<th scope="col" data-variant="${esc(c.variant)}">${esc(c.label)}${c.cheapest ? ` ${badge('cheapest', 'ok')}` : ''}</th>`).join('');
    const body = t.rows.map((r) => `<tr data-operation="${esc(r.operation)}"><th scope="row">${esc(r.label)}</th>${r.cells.map((c) => {
      known(CELL_STATES, c.state, 'cell state');
      const key = `${c.variant}.${c.operation}`;
      if (!c.clickable) return `<td class="cell-off" data-state="${esc(c.state)}">${esc(c.text)}${extrapolatedBadge(c.extrapolated)}</td>`;
      return `<td><button type="button" class="cell-btn" data-action="cell" data-variant="${esc(c.variant)}" data-operation="${esc(c.operation)}"`
        + ` aria-pressed="${page.open_cell === key}" title="Show how this is calculated">${esc(c.text)}</button>${extrapolatedBadge(c.extrapolated)}</td>`;
    }).join('')}</tr>`).join('');
    return `<section class="section"><h2>Breakdown by workload type</h2><p class="mute">Select a cell to see how it is calculated.</p>`
      + `<div class="tbl"><table class="breakdown"><thead><tr><th scope="col">Workload type</th>${head}</tr></thead><tbody>${body}</tbody></table></div></section>`;
  }

  function stageLine(l) {
    const formula = l.formula ? `<div class="formula"><code>${esc(l.formula)}</code></div>` : '';
    return `<li>${esc(l.text)}${extrapolatedBadge(l.extrapolated)}${formula}</li>`;
  }

  function renderDrillPart(vm, page) {
    if (vm.page_state === 'cannot_reproduce') return '';
    const d = vm.drill;
    if (!d) return '';
    const comp = d.composition.map((x) => `<li><span class="dot cls-${esc(x.unit_class)}"></span>${esc(CLASS_LABELS[x.unit_class] || x.unit_class)}`
      + ` <span class="mute">${esc(x.rule_id)}${x.op_kind ? `, ${esc(x.op_kind)}` : ''}</span> <span class="money">${esc(x.money_text)}</span></li>`);
    const compBar = d.composition.filter((x) => x.fraction !== null && x.fraction > 0)
      .map((x) => `<span class="seg cls-${esc(x.unit_class)}" style="flex-grow:${esc(x.fraction)}"></span>`).join('');
    const stages = page.show_calc ? `<ol class="stages">${d.stages.map((s, i) => `<li class="stage" data-stage="${i + 1}"><h4>${esc(s.name)}</h4>`
      + `${s.note ? `<p class="mute">${esc(s.note)}</p>` : ''}${s.lines.length ? `<ul>${s.lines.map(stageLine).join('')}</ul>` : '<p class="mute">nothing in this stage</p>'}</li>`).join('')}</ol>` : '';
    const sources = list(d.sources.map((s) => `<li>${esc(s.kind)}: ${link(s.ref)}${s.version ? ` <span class="mute">${esc(s.version)}</span>` : ''}</li>`), 'sources');
    return `<section class="section drill-view" data-cell="${esc(`${d.variant}.${d.operation}`)}"><header class="drill-head"><h2>${esc(d.label)}</h2>${extrapolatedBadge(d.extrapolated)}`
      + '<button type="button" class="btn btn-small" data-action="close-drill">Close</button></header>'
      + `${compBar ? `<div class="bar bar-wide">${compBar}</div>` : ''}${list(comp, 'legend legend-col')}`
      + `<button type="button" class="btn" data-action="show-calc" aria-expanded="${!!page.show_calc}">${page.show_calc ? 'Hide calculation' : 'Show calculation'}</button>`
      + `${stages}${sources ? `<h4>Sources</h4>${sources}` : ''}</section>`;
  }

  function renderNotes(vm, page) {
    if (vm.page_state === 'cannot_reproduce') return '';
    const items = vm.notes.map((n) => `<li class="note note-${esc(n.kind)}">${esc(n.text)}${n.details && n.details.length ? list(n.details.map((x) => `<li>${esc(typeof x === 'string' ? x : JSON.stringify(x))}</li>`)) : ''}</li>`);
    for (const p of vm.problems) items.push(`<li class="note note-problem">${esc(p)}</li>`);
    if (page.error) items.unshift(`<li class="note note-error" role="alert">${esc(page.error)}</li>`);
    if (!items.length) return '';
    return `<section class="section"><h2>Notes</h2><ul class="notes">${items.join('')}</ul></section>`;
  }

  function renderEvidence(vm) {
    if (vm.page_state === 'cannot_reproduce' || !vm.evidence_statement) return '';
    return `<section class="section evidence-statement"><h2>What these numbers rest on</h2><p>${esc(vm.evidence_statement)}</p></section>`;
  }

  // Inline SVG. Points are spaced by their order (the sweep values are geometric), heights are proportional to the
  // largest total; both are drawing geometry. Axis labels are view-model values formatted by the view model.
  function renderChartSvg(ch, currency) {
    const W = 640;
    const H = 260;
    const pad = { l: 16, r: 16, t: 16, b: 40 };
    const xs = [...new Set(ch.points.map((p) => p.x))];
    const ys = ch.points.filter((p) => p.y !== null).map((p) => p.y);
    const top = Math.max(...ys);
    const px = (x) => pad.l + (xs.length > 1 ? (xs.indexOf(x) * (W - pad.l - pad.r)) / (xs.length - 1) : (W - pad.l - pad.r) / 2);
    const py = (y) => H - pad.b - (top > 0 ? (y * (H - pad.t - pad.b)) / top : 0);
    const series = ch.labels.series.map((s) => {
      const pts = ch.points.filter((p) => p.variant === s.variant);
      const runs = [];
      let cur = [];
      for (const p of pts) {
        if (p.y === null) { if (cur.length) runs.push(cur); cur = []; } else cur.push(p);
      }
      if (cur.length) runs.push(cur);
      const lines = runs.filter((r) => r.length > 1).map((r) => `<polyline class="line v-${esc(s.variant)}" points="${r.map((p) => `${px(p.x)},${py(p.y)}`).join(' ')}"/>`).join('');
      const dots = pts.filter((p) => p.y !== null).map((p) => `<circle class="pt v-${esc(s.variant)}${p.extrapolated ? ' is-extrapolated' : ''}" cx="${px(p.x)}" cy="${py(p.y)}" r="4"><title>${esc(p.label)}</title></circle>`).join('');
      return `<g data-variant="${esc(s.variant)}">${lines}${dots}</g>`;
    }).join('');
    const ticks = xs.map((x) => `<text class="tick" x="${px(x)}" y="${H - pad.b + 18}" text-anchor="middle">${esc(viewmodel().fmtQuantity(x))}</text>`).join('');
    return `<svg class="chart-svg" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(`${ch.labels.y} against ${ch.labels.x}`)}">`
      + `<line class="axis" x1="${pad.l}" y1="${H - pad.b}" x2="${W - pad.r}" y2="${H - pad.b}"/>`
      + `<text class="tick" x="${pad.l}" y="${pad.t - 4}">${esc(viewmodel().fmtMoney(top, currency))}</text>${series}${ticks}`
      + `<text class="axis-label" x="${W / 2}" y="${H - 4}" text-anchor="middle">${esc(ch.labels.x)}</text></svg>`;
  }

  function renderChart(vm, page) {
    if (vm.page_state === 'cannot_reproduce') return '';
    const opts = page.sweep_inputs.map((s) => `<option value="${esc(s.id)}"${s.id === page.sweep_input ? ' selected' : ''}>${esc(s.label)}</option>`).join('');
    const pick = `<label class="field-row"><span class="field-label">Vary</span><select data-action="sweep">${opts}</select></label>`;
    const ch = vm.chart;
    let body;
    if (vm.page_state === 'prices_missing') body = '<p class="mute">The chart compares money; this price preset has no prices.</p>';
    else if (!ch) body = '<p class="mute">Choose an input to vary.</p>';
    else if (!ch.points.some((p) => p.y !== null)) body = '<p class="mute">No column has a monthly total over this range, so there is nothing to draw.</p>';
    else {
      const legend = ch.labels.series.map((s) => `<li><span class="dot v-${esc(s.variant)}"></span>${esc(s.label)}</li>`).join('');
      const ex = ch.extrapolated_points.length ? `<p class="field-note">${badge('extrapolated', 'warn')} Hollow points lie outside a measured range.</p>` : '';
      const be = list(ch.breakEven.map((b) => `<li>${esc(b.text)}${extrapolatedBadge(b.extrapolated)}</li>`), 'break-even');
      const pts = ch.points.map((p) => `<li>${esc(p.label)}${extrapolatedBadge(p.extrapolated)}</li>`).join('');
      body = `${renderChartSvg(ch, vm.currency)}<ul class="legend">${legend}</ul>${ex}${be ? `<h4>Break-even</h4>${be}` : ''}`
        + `<details class="hint"><summary>Chart values</summary><ul class="points">${pts}</ul></details>`;
    }
    return `<section class="section"><h2>How the totals move</h2>${pick}${body}</section>`;
  }

  // Pure: view model + page chrome (selections, labels) -> HTML per container.
  function renderParts(vm, page) {
    if (!vm || typeof vm !== 'object') throw new Error('ui.render: a view model is required');
    known(PAGE_STATES, vm.page_state, 'page state');
    if (!page || typeof page !== 'object') throw new Error('ui.render: page chrome is required');
    const p = Object.assign({}, page, { cannot_reproduce: vm.page_state === 'cannot_reproduce' });
    return {
      presets: renderPresets(vm, p), inputs: renderInputs(vm, p), cards: renderCards(vm), table: renderTable(vm, p),
      drill: renderDrillPart(vm, p), notes: renderNotes(vm, p), evidence: renderEvidence(vm), chart: renderChart(vm, p),
    };
  }

  function render(vm, page) {
    const parts = renderParts(vm, page);
    return PARTS.map((id) => `<div data-part="${id}">${parts[id]}</div>`).join('');
  }

  // ---- state ------------------------------------------------------------------------------------

  function defaultState(contract, scenarioId) {
    const id = scenarioId || DEFAULT_SCENARIO;
    const s = presetsLib().SCENARIOS[id];
    if (!s) throw new Error(`ui: unknown scenario ${JSON.stringify(id)}`);
    return {
      inputs: JSON.parse(JSON.stringify(s.values)), locks: s.locks.slice(), overrides: {}, scenario: id, price_preset: DEFAULT_PRICE,
      custom_prices: {}, units: 'day', openCell: null, sweepInput: null, manifest: contract.manifest,
    };
  }

  const parseCell = (key) => {
    if (!key) return null;
    const i = key.indexOf('.');
    if (i <= 0) throw new Error(`ui: open cell ${JSON.stringify(key)} is not "variant.operation"`);
    return { variant: key.slice(0, i), operation: key.slice(i + 1) };
  };

  const isSweepable = (q) => q.kind === 'input' && q.value_type === 'number';

  function sweepInputOf(contract, state) {
    if (state.sweepInput) return state.sweepInput;
    const q = contract.registry.find((x) => x.block === 'simple' && isSweepable(x)) || contract.registry.find(isSweepable);
    return q ? q.id : null;
  }

  // The swept x values are model inputs the page picks, as a user would: the measured range in six even steps,
  // otherwise multiples of the current value. The totals at each x come from the pipeline.
  function sweepValues(field, current) {
    const r = field && field.validity;
    if (r && r[1] > r[0]) return [0, 1, 2, 3, 4, 5].map((i) => r[0] + ((r[1] - r[0]) * i) / 5);
    if (typeof current === 'number' && current > 0) return [0.25, 0.5, 1, 2, 4, 8].map((k) => current * k);
    return [1, 10, 100, 1000, 10000];
  }

  function mismatchText(m) {
    if (m.artifact === 'url_format') return 'the link is not a calculator link';
    const val = (v) => (v === null || v === undefined ? 'nothing' : String(v));
    return `${m.artifact} ${m.field}: the link has ${val(m.url_value)}, this page has ${val(m.loaded_value)}`;
  }

  // A Result that renders no number: the link names versions this page does not carry (spec §4.5).
  function cannotReproduceResult(contract, problem) {
    const missing = problem.missing || [];
    const names = [...new Set(missing.filter((m) => m.artifact !== 'url_format').map((m) => (m.version ? `${m.artifact} version ${m.version}` : m.artifact)))];
    const reason = problem.kind === 'cannot_reproduce' ? `cannot reproduce: ${names.join(', ')} not present` : `cannot reproduce: ${problem.reason}`;
    return {
      page_state: 'cannot_reproduce', page_reason: reason, problems: missing.map(mismatchText), evidence_statement: contract.evidence_statement,
      manifest: contract.manifest, variants: {}, cheapest: null, deltas: {}, sweep: null,
    };
  }

  function chrome(contract, state, ui, sweepId) {
    const P = presetsLib();
    const byId = new Map(contract.registry.map((q) => [q.id, q]));
    const priceIds = Object.keys(P.PRICES).filter((id) => P.PRICES[id].source === 'custom' || (contract.billing.presets || []).some((p) => p.id === id));
    return {
      scenarios: Object.entries(P.SCENARIOS).map(([id, s]) => ({ id, label: s.label, description: s.description })),
      scenario: state.scenario,
      prices: priceIds.map((id) => ({ id, label: P.priceLabel(contract, id) })),
      price_preset: state.price_preset, units: state.units, open_cell: state.openCell,
      sweep_inputs: contract.registry.filter(isSweepable).map((q) => ({ id: q.id, label: q.label || q.id })),
      sweep_input: sweepId || (byId.has(state.sweepInput) ? state.sweepInput : null),
      show_calc: !!ui.showCalc, open_blocks: ui.openBlocks || [], error: ui.error || null,
    };
  }

  // State -> {vm, page, fields} through the real pipeline and view model.
  function compute(contract, state, ui, problem) {
    const u = ui || {};
    if (problem) {
      const fields = pipeline().resolveInputs(contract, {}, [], {}).fields;
      const vm = viewmodel().build(cannotReproduceResult(contract, problem), contract, fields, [], {});
      return { vm, page: chrome(contract, defaultState(contract), u, null), fields };
    }
    if (!presetsLib().PRICES[state.price_preset]) {
      return compute(contract, state, u, { kind: 'cannot_reproduce', missing: [{ artifact: 'price preset', version: state.price_preset, field: 'price_preset', url_value: state.price_preset, loaded_value: null }] });
    }
    const preset = presetsLib().resolvePrice(contract, state.price_preset, state.price_preset === 'custom' ? state.custom_prices : undefined);
    const r0 = pipeline().resolveInputs(contract, state.inputs, state.locks, state.overrides);
    const sweepId = sweepInputOf(contract, state);
    const opts = { lockOrder: r0.lockOrder };
    if (sweepId) opts.sweep = { inputId: sweepId, values: sweepValues(r0.fields[sweepId], r0.inputs.get(sweepId)) };
    const res = pipeline().run(contract, r0.inputs, preset, opts);
    // Input problems are page notes; none is dropped.
    res.problems = [...(res.problems || []), ...r0.problems];
    const vm = viewmodel().build(res, contract, r0.fields, r0.lockOrder, {
      pricePreset: preset, openCell: parseCell(state.openCell), sweepInput: sweepId, priceEditing: true, theme: u.theme || null,
    });
    return { vm, page: chrome(contract, state, u, sweepId), fields: r0.fields };
  }

  function parseInput(q, raw, units) {
    if (q.value_type === 'enum') {
      if (q.enum.includes(raw)) return raw;
      // volumes_per_replica takes `auto` or a volume count; the pipeline rejects anything else with a note.
      return /^\s*\d+\s*$/.test(raw) ? Number(raw) : raw;
    }
    const t = String(raw).trim();
    const n = t === '' ? raw : Number(t);
    const v = typeof n === 'number' && Number.isFinite(n) ? n : raw;
    return typeof q.unit === 'string' && q.unit.endsWith('/day') && typeof v === 'number' ? { value: v, per: units } : v;
  }

  // Pure reducer: (state, ui, action) -> {state, ui}. `ctx.fields` are the resolved fields the action was taken on.
  function update(contract, state, ui, action, ctx) {
    const s = JSON.parse(JSON.stringify(state));
    const u = Object.assign({}, ui, { error: null });
    const d = action.data || {};
    const fields = (ctx && ctx.fields) || {};
    const q = (id) => {
      const x = contract.registry.find((r) => r.id === id);
      if (!x) throw new Error(`ui: ${id} is not a registry row`);
      return x;
    };
    switch (action.type) {
      case 'scenario': {
        const sc = presetsLib().SCENARIOS[d.id];
        if (!sc) throw new Error(`ui: unknown scenario ${JSON.stringify(d.id)}`);
        // Overrides survive a preset switch, marked, with a reset (spec §4.2).
        Object.assign(s, { scenario: d.id, inputs: JSON.parse(JSON.stringify(sc.values)), locks: sc.locks.slice() });
        break;
      }
      case 'price': known(Object.keys(presetsLib().PRICES), action.value, 'price preset'); s.price_preset = action.value; break;
      case 'edit-prices': s.price_preset = 'custom'; break;
      case 'price-field': {
        const v = Number(action.value);
        if (String(action.value).trim() === '' || !Number.isFinite(v) || v < 0) throw new Error(`price ${d.path}: enter a number of 0 or more, got ${JSON.stringify(action.value)}`);
        s.custom_prices[d.path] = v;
        break;
      }
      case 'input':
      case 'slider': {
        const v = parseInput(q(d.id), action.value, s.units);
        const f = fields[d.id];
        if (f && (f.derived || f.overridden)) s.overrides[d.id] = v;
        else s.inputs[d.id] = v;
        break;
      }
      case 'lock': {
        const f = fields[d.id];
        // The newly locked member keeps the value it had while derived instead of jumping to a stale raw value.
        if (f && f.derived && typeof f.value === 'number') s.inputs[d.id] = f.value;
        s.locks = pipeline().lockMember(contract, s.locks, d.id);
        break;
      }
      case 'reset': delete s.overrides[d.id]; break;
      case 'units': known(UNITS.map(([x]) => x), action.value, 'rate unit'); s.units = action.value; break;
      case 'cell': s.openCell = `${d.variant}.${d.operation}`; break;
      case 'close-drill': s.openCell = null; break;
      case 'show-calc': u.showCalc = !u.showCalc; break;
      case 'sweep': s.sweepInput = action.value; break;
      case 'start-over': return { state: defaultState(contract), ui: Object.assign(u, { showCalc: false }), problem: null };
      default: throw new Error(`ui: unknown action ${JSON.stringify(action.type)}`);
    }
    return { state: s, ui: u };
  }

  // ---- mount ------------------------------------------------------------------------------------

  function readTheme(win) {
    try {
      const t = win.localStorage.getItem(THEME_KEY);
      return t === 'light' || t === 'dark' ? t : null;
    } catch (e) {
      return null;
    }
  }

  function writeTheme(win, t) {
    try {
      win.localStorage.setItem(THEME_KEY, t);
    } catch (e) {
      // Storage blocked (private window, file:// policy): the toggle still applies to this visit.
    }
  }

  function mount(doc, contract) {
    const win = doc.defaultView;
    const boxes = {};
    for (const id of PARTS) {
      boxes[id] = doc.getElementById(id);
      if (!boxes[id]) throw new Error(`ui.mount: container #${id} is missing from the page`);
    }
    const html = doc.documentElement;
    const saved = readTheme(win);
    if (saved) html.setAttribute('data-theme', saved);
    let problem = null;
    let state = null;
    const hash = win.location.hash;
    // The hash is read once; afterwards the page owns it.
    if (hash && hash !== '#') {
      const dec = urlstate().decode(hash, contract);
      if (dec.state) state = dec.state;
      else problem = dec.problem;
    }
    if (!state) state = defaultState(contract);
    let ui = { showCalc: false, openBlocks: [], error: null };
    let fields = {};

    const themeButton = doc.querySelector('[data-action="theme"]');
    const effectiveTheme = () => html.getAttribute('data-theme') || (win.matchMedia && win.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const labelTheme = () => {
      if (themeButton) themeButton.textContent = effectiveTheme() === 'dark' ? 'Light theme' : 'Dark theme';
    };

    function paint(skip) {
      const out = compute(contract, state, ui, problem);
      fields = out.fields;
      const parts = renderParts(out.vm, out.page);
      const active = doc.activeElement;
      const focusKey = active && active.dataset ? [active.dataset.action, active.dataset.id || active.dataset.path || ''] : null;
      for (const id of PARTS) if (id !== skip) boxes[id].innerHTML = parts[id];
      if (focusKey && focusKey[0] && skip !== 'inputs') {
        const sel = `[data-action="${focusKey[0]}"]${focusKey[1] ? `[data-${active.dataset.id ? 'id' : 'path'}="${focusKey[1]}"]` : ''}`;
        const again = doc.querySelector(sel);
        if (again && again !== active) again.focus();
      }
      doc.body.setAttribute('data-page-state', out.vm.page_state);
      // A link that cannot be reproduced keeps its hash, so it can be reported as received.
      if (!problem) win.history.replaceState(null, '', urlstate().encode(Object.assign({}, state, { manifest: contract.manifest })));
      labelTheme();
    }

    function dispatch(action, skip) {
      try {
        const next = update(contract, state, ui, action, { fields });
        state = next.state;
        ui = next.ui;
        if ('problem' in next) problem = next.problem;
      } catch (e) {
        ui = Object.assign({}, ui, { error: e.message });
      }
      paint(skip);
    }

    function actionOf(el, value) {
      return { type: el.dataset.action, data: Object.assign({}, el.dataset), value };
    }

    doc.addEventListener('click', (ev) => {
      const el = ev.target.closest('button[data-action]');
      if (!el) return;
      if (el.dataset.action === 'theme') {
        const next = effectiveTheme() === 'dark' ? 'light' : 'dark';
        html.setAttribute('data-theme', next);
        writeTheme(win, next);
        labelTheme();
        return;
      }
      dispatch(actionOf(el));
    });
    doc.addEventListener('change', (ev) => {
      const el = ev.target.closest('[data-action]');
      if (!el || el.tagName === 'BUTTON') return;
      dispatch(actionOf(el, el.value));
    });
    // While a slider moves, the results follow it; the inputs panel is redrawn on release so the drag survives.
    doc.addEventListener('input', (ev) => {
      const el = ev.target.closest('[data-action="slider"]');
      if (!el) return;
      const box = el.parentElement.querySelector(`input[type="number"][data-id="${el.dataset.id}"]`);
      if (box) box.value = el.value;
      dispatch(actionOf(el, el.value), 'inputs');
    });
    // Open advanced blocks stay open across redraws.
    doc.addEventListener('toggle', (ev) => {
      const el = ev.target;
      if (!el.matches || !el.matches('details.block')) return;
      const id = el.dataset.block;
      const open = new Set(ui.openBlocks);
      if (el.open) open.add(id); else open.delete(id);
      ui = Object.assign({}, ui, { openBlocks: [...open] });
    }, true);
    if (win.matchMedia) win.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', labelTheme);
    paint();
    return { get state() { return state; }, get problem() { return problem; } };
  }

  const ui = { renderDrill, renderDrillHtml, render, renderParts, compute, update, mount, defaultState, cannotReproduceResult, PARTS };
  root.SCC = root.SCC || {};
  root.SCC.ui = Object.assign(root.SCC.ui || {}, ui);
  if (typeof module !== 'undefined' && module.exports) module.exports = root.SCC.ui;
})(typeof window !== 'undefined' ? window : globalThis);
