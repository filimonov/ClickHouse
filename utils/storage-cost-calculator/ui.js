// Drill-down rendering for the calculator page. Plan 3 adds the rest of the UI to the same namespace.
// renderDrillHtml is pure (string in, string out) so Node can render and check the exact page markup.
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
      return `<tr data-row="${esc(key)}#${i}" data-exact="${exact}"><td>${esc(r.row)}</td><td>${esc((r.branch_taken || []).join(', ') || '-')}</td>`
        + `<td>${num(r.predicted)}</td><td>${num(r.measured)}</td><td>${expectedText(r.expected)}</td><td>${exact === 'false' ? 'MISMATCH' : exact === 'true' ? 'exact' : '-'}</td></tr>`;
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

  const ui = { renderDrill, renderDrillHtml };
  root.SCC = root.SCC || {};
  root.SCC.ui = Object.assign(root.SCC.ui || {}, ui);
  if (typeof module !== 'undefined' && module.exports) module.exports = root.SCC.ui;
})(typeof window !== 'undefined' ? window : globalThis);
