'use strict';
// The extrapolated flag marks numbers that rest on a value outside a measured range. A $0 cell prices nothing, so it
// extrapolates nothing; each card and header carries one badge.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const pipeline = require('../pipeline.js');
const presets = require('../presets.js');
const ui = require('../ui.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const CSS = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
const SCENARIOS = Object.keys(presets.SCENARIOS);
const TOPOLOGIES = ['same_az', 'cross_az'];
const page = (sc, inputs, uiState) => {
  const s = ui.defaultState(LIVE, sc);
  Object.assign(s.inputs, inputs || {});
  const out = ui.compute(LIVE, s, uiState || {});
  return Object.assign(out, { state: s, parts: ui.renderParts(out.vm, out.page) });
};
const run = (s) => {
  const r0 = pipeline.resolveInputs(LIVE, s.inputs, s.locks, s.overrides);
  return pipeline.run(LIVE, r0.inputs, presets.resolvePrice(LIVE, s.price_preset), { lockOrder: r0.lockOrder });
};
const isZero = (t) => /^\$0\.00$/.test(t);
const cellsOf = (vm, v) => vm.table.rows.map((r) => r.cells.find((c) => c.variant === v)).filter((c) => c && c.state === 'ok');
const cardHtml = (html, v) => new RegExp(`<article class="card[^"]*" data-variant="${v}"[^>]*>.*?</article>`, 's').exec(html)[0];

test('test_zero_cell_is_never_extrapolated', () => {
  for (const sc of SCENARIOS) {
    for (const topology of TOPOLOGIES) {
      const { vm, state } = page(sc, { topology });
      const res = run(state);
      const where = `${sc} ${topology}`;
      for (const r of vm.table.rows) {
        for (const c of r.cells) if (c.state === 'ok' && isZero(c.text)) assert.equal(c.extrapolated, false, `${where}: ${c.variant}.${r.operation} is $0.00`);
      }
      for (const [v, vr] of Object.entries(res.variants)) {
        if (vr.state !== 'ok') continue;
        for (const [op, c] of Object.entries(vr.cells)) if (c.money === 0) assert.equal(c.extrapolated, false, `${where}: pipeline ${v}.${op}`);
      }
      for (const k of vm.cards.filter((x) => x.state === 'ok')) {
        const flagged = cellsOf(vm, k.variant).filter((c) => c.extrapolated);
        assert.equal(k.extrapolated_cells, flagged.length, `${where}: ${k.variant} card count`);
        assert.ok(flagged.every((c) => !isZero(c.text)), `${where}: ${k.variant}`);
      }
    }
  }
});

test('test_metrics_ebs_rests_on_no_extrapolated_amount', () => {
  const { vm, state } = page('metrics');
  for (const op of ['mutations', 'deletes', 'recovery']) {
    const c = vm.table.rows.find((r) => r.operation === op).cells.find((x) => x.variant === 'ebs');
    assert.equal(c.text, '$0.00', op);
    assert.equal(c.extrapolated, false, op);
  }
  const ebs = vm.cards.find((c) => c.variant === 'ebs');
  assert.equal(ebs.cheapest, true);
  assert.equal(ebs.extrapolated, false);
  assert.equal(ebs.cheapest_text, 'cheapest of 4');
  assert.equal(run(state).variants.ebs.extrapolated, false, 'deltas, the chart and break-even follow the same flag');
});

test('test_delta_names_an_extrapolated_reference_in_words', () => {
  // A delta line says "(extrapolated)" exactly when its reference column is extrapolated, and carries no pill.
  for (const sc of SCENARIOS) {
    for (const topology of TOPOLOGIES) {
      const { vm, parts } = page(sc, { topology });
      const flagged = new Map(vm.cards.map((c) => [c.label, c.extrapolated]));
      for (const k of vm.cards) {
        for (const d of k.deltas) {
          const ref = /^vs ([^:(]+?)(?: \(extrapolated\))?:/.exec(d.text);
          assert.ok(ref, d.text);
          assert.equal(/\(extrapolated\)/.test(d.text), !!flagged.get(ref[1]) && !/not comparable/.test(d.text), `${sc} ${topology}: ${k.variant} ${d.text}`);
        }
      }
      assert.doesNotMatch(parts.cards, /<ul class="card-deltas">(?:(?!<\/ul>).)*class="badge/s, 'a delta line carries no pill');
    }
  }
  // 100-row inserts lie below the measured S3 range: the other columns name S3 as extrapolated, EBS as plain.
  const ex = page('logs', { insert_rows: 100 }).vm;
  assert.equal(ex.cards.find((c) => c.variant === 's3').extrapolated, true);
  assert.equal(ex.cards.find((c) => c.variant === 'ebs').extrapolated, false);
  assert.match(ex.cards.find((c) => c.variant === 'ebs').deltas[0].text, /^vs S3 \(extrapolated\): [−+]\$/);
  assert.match(ex.cards.find((c) => c.variant === 's3').deltas[0].text, /^vs EBS: [−+]\$/);
});

test('test_header_badge_is_short_and_one_per_column', () => {
  for (const sc of SCENARIOS) {
    for (const topology of TOPOLOGIES) {
      const { vm, parts } = page(sc, { topology });
      const head = /<thead>(.*?)<\/thead>/s.exec(parts.table)[1];
      const ths = head.match(/<th scope="col"[^>]*>.*?<\/th>/gs).filter((t) => /data-variant/.test(t));
      assert.equal(ths.length, vm.table.columns.length);
      for (const th of ths) {
        const v = /data-variant="([^"]+)"/.exec(th)[1];
        const col = vm.table.columns.find((c) => c.variant === v);
        const ok = th.match(/class="badge badge-ok"/g) || [];
        const warn = th.match(/class="badge badge-warn[^"]*"/g) || [];
        assert.equal(ok.length, col.cheapest ? 1 : 0, `${sc} ${topology} ${v}`);
        if (col.cheapest) assert.match(th, /<span class="badge badge-ok">cheapest of \d<\/span>/);
        assert.equal(warn.length, col.cheapest && col.extrapolated ? 1 : 0, `${sc} ${topology} ${v}: the extrapolated pill only beside the cheapest badge`);
      }
    }
  }
  // The pills sit on one line: the header row is as tall with the second pill as without it.
  assert.match(CSS, /\.breakdown thead \.th-badges \{[^}]*white-space: nowrap/);
  assert.doesNotMatch(CSS, /\.breakdown thead \.badge[^{]*\{[^}]*white-space: normal/);
});

test('test_one_badge_per_card', () => {
  let cheapestExtrapolated = 0;
  for (const sc of SCENARIOS) {
    for (const topology of TOPOLOGIES) {
      const { vm, parts, state } = page(sc, { topology });
      for (const k of vm.cards) {
        const head = /<header class="card-head">(.*?)<\/header>/s.exec(cardHtml(parts.cards, k.variant))[1];
        const badges = head.match(/class="badge(?: badge-\w+)?"/g) || [];
        assert.ok(badges.length <= 1, `${sc} ${topology} ${k.variant}: ${head}`);
        if (k.cheapest && k.extrapolated) {
          cheapestExtrapolated++;
          assert.match(head, /cheapest of \d, extrapolated<\/span><button[^>]*data-info-kind="column"/);
          assert.doesNotMatch(head, /extrapolated: \d/);
          // The count moves into the (i) of the badge.
          const info = page(sc, Object.assign({}, state.inputs), { info: { kind: 'column', variant: k.variant } }).vm.info;
          const n = k.extrapolated_cells;
          if (n) assert.ok(info.lines.some((l) => new RegExp(`^Extrapolated: ${n} ${n === 1 ? 'cell' : 'cells'} \\(`).test(l)), JSON.stringify(info.lines));
        }
      }
    }
  }
  assert.ok(cheapestExtrapolated > 0, 'some preset has an extrapolated cheapest column (cross-AZ Logs)');
});

test('test_notes_row_is_one_line_at_13px', () => {
  // 13 px is the largest size at which four one-line notes fit the table at 1440 px without a sideways scroll.
  assert.match(CSS, /\n\.col-note \{[^}]*font-size: 13px/);
  // One line from 1440 px, the reference width; below it a note may wrap, so the table never scrolls sideways.
  assert.match(CSS, /@media \(min-width: 1440px\) \{[^@]*\.col-note \{[^}]*white-space: nowrap/);
  assert.doesNotMatch(CSS, /@media \(min-width: 1400px\) \{[^@]*\.col-note \{[^}]*white-space: nowrap/);
});
