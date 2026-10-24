// Drill-down rendering for the calculator page. Plan 3 adds the rest of the UI to the same namespace.
// renderDrillHtml is pure (string in, string out) so Node can render and check the exact page markup.
(function (root) {
  'use strict';

  const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

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
      const exact = r.exact === null || r.exact === undefined ? 'n/a' : String(r.exact);
      return `<tr data-row="${esc(key)}#${i}" data-exact="${exact}"><td>${esc(r.row)}</td><td>${esc((r.branch_taken || []).join(', ') || '-')}</td>`
        + `<td>${num(r.predicted)}</td><td>${num(r.measured)}</td><td>${expectedText(r.expected)}</td><td>${exact === 'false' ? 'MISMATCH' : exact === 'true' ? 'exact' : '-'}</td></tr>`;
    }).join('');
    return `<section class="stage" data-stage="2"><h4>2. Rows: branch taken, predicted vs measured</h4>${table(['row', 'branch', 'predicted', 'measured', 'expected', 'check'], rows)}</section>`;
  }

  function stage3(c) {
    const s = c.stage3_ledger_summary || {};
    const types = Object.entries(s.by_row_type || {}).map(([t, v]) => `<tr><th scope="row">${esc(t)}</th><td>${num(v.rows)}</td><td>${num(v.quantity)}</td></tr>`).join('');
    const states = (s.states || []).map((x) => `<li>${esc(x.id)}: ${x.active === null ? 'unknown' : x.active ? 'active' : 'inactive'} ${esc(x.reason || '')}</li>`).join('');
    return `<section class="stage" data-stage="3"><h4>3. Ledger summary</h4>${table(['row type', 'rows', 'quantity'], types)}`
      + `${states ? `<ul>${states}</ul>` : ''}${s.extrapolated ? '<p class="warn">extrapolated outside a coefficient validity range</p>' : ''}</section>`;
  }

  function stage4(c) {
    const lines = c.stage4_billing_lines;
    if (!lines) return '<section class="stage" data-stage="4"><h4>4. Billing</h4><p class="mute">not billed: a row is unavailable</p></section>';
    const body = lines.map((l) => `<tr data-rule="${esc(l.rule_id)}"><td>${esc(l.rule_id)}</td><td>${num(l.quantity)} ${esc(l.unit)}</td><td>${money(l.money)}</td><td>${esc(l.note || '')}</td></tr>`).join('');
    return `<section class="stage" data-stage="4"><h4>4. Billing lines</h4>${table(['rule', 'quantity', 'money', 'note'], body)}<p>Total ${money(c.billing_total)}</p></section>`;
  }

  function stage5(c) {
    const a = c.stage5_allocation;
    if (!a) return '<section class="stage" data-stage="5"><h4>5. Allocation</h4><p class="mute">no block rows</p></section>';
    const alt = (a.alternatives || []).map((x) => `<tr><td>${esc(x.layout)}</td><td>${num(x.volumes)}</td><td>${num(x.size_gib)}</td><td>${num(x.iops)}</td><td>${num(x.mibps)}</td><td>${money(x.monthly_cost)}</td></tr>`).join('');
    return `<section class="stage" data-stage="5"><h4>5. Allocation</h4><p>${esc(a.layout || 'manual')}: ${num(a.volumes)} x ${num(a.size_gib)} GiB, ${num(a.iops)} IOPS, ${num(a.mibps)} MiB/s, ${money(a.monthly_cost)} per month</p>`
      + `${table(['layout', 'volumes', 'GiB', 'IOPS', 'MiB/s', 'cost'], alt)}</section>`;
  }

  function renderDrillHtml(drill) {
    const cells = Object.entries(drill.cells || {}).map(([key, c]) =>
      `<article class="cell" data-cell="${esc(key)}"><h3>${esc(key)}</h3>${stage1(c)}${stage2(key, c)}${stage3(c)}${stage4(c)}${stage5(c)}</article>`).join('');
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
