'use strict';
// EBS sizing per replica (from the billing allocation) and cell notes as (i) markers in the breakdown.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const logs = (uiState) => {
  const out = ui.compute(LIVE, ui.defaultState(LIVE, 'logs'), uiState || {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page) });
};

test('test_ebs_sizing_on_logs', () => {
  const { vm, parts } = logs();
  const ebs = vm.cards.find((c) => c.variant === 'ebs');
  const s = ebs.sizing;
  assert.equal(s.replicas, 2);
  assert.equal(s.capacity_gib, 19500);
  assert.ok(s.peak_iops < 3000 && s.peak_mibps < 125, 'demand under the gp3 baseline');
  assert.equal(s.verdict, 'gp3 baseline sufficient');
  assert.match(s.lines.join('\n'), /Volume size per replica: 19,500 GiB \(stored data × 1\.3 ebs_capacity_headroom\)/);
  assert.match(s.lines.join('\n'), /gp3 baseline per volume: 3,000 IOPS, 125 MiB\/s included/);
  assert.match(s.lines.join('\n'), /Cheapest feasible layout: 1 × 19,500 GiB, 3,000 IOPS, 125 MiB\/s, on 2 replicas/);
  assert.match(cardHtml(parts.cards, 'ebs'), /<p class="card-sizing">Volumes: 19,500 GiB per replica, needs .*: gp3 baseline sufficient<button/);
  assert.equal(s.instance_class, 'm6i.large');
  // The column notes and the column dialog repeat the same numbers.
  assert.ok(vm.table.column_notes.ebs.some((n) => n.kind === 'sizing' && n.text === `Volumes: ${s.short}`));
  const col = logs({ info: { kind: 'column', variant: 'ebs' } }).vm.info;
  assert.deepEqual(col.lines.slice(0, s.lines.length), s.lines);
});

test('test_cell_notes_are_info_markers_on_their_cell', () => {
  const { vm, parts } = logs();
  const withNotes = vm.table.rows.flatMap((r) => r.cells.filter((c) => c.notes.length).map((c) => [r.operation, c]));
  assert.ok(withNotes.length > 0, 'the EBS cells carry notes');
  const [op, c] = withNotes[0];
  assert.ok(parts.table.includes(`data-info-kind="cell" data-variant="${c.variant}" data-operation="${op}"`));
  const info = logs({ info: { kind: 'cell', variant: c.variant, operation: op } }).vm.info;
  for (const n of c.notes) assert.ok(info.lines.includes(n), n);
});

function cardHtml(html, v) {
  const at = html.indexOf(`data-variant="${v}" data-state=`);
  const start = html.lastIndexOf('<article', at);
  return html.slice(start, html.indexOf('</article>', start));
}

test('test_card_state_and_row_states_agree', () => {
  for (const sc of ['logs', 'metrics', 'analytics_batch', 'single_replica_archive', 'custom']) {
    const out = ui.compute(LIVE, ui.defaultState(LIVE, sc), {});
    for (const card of out.vm.cards) {
      if (card.state === 'ok') continue;
      for (const row of out.vm.table.rows) {
        const cell = row.cells.find((c) => c.variant === card.variant);
        // A column that is not priced never shows a row as priced; a row it shows a state for is the card's state.
        assert.equal(cell.clickable, false, `${sc} ${card.variant}.${row.operation}`);
        if (cell.state !== 'not_applicable') assert.equal(cell.state, card.state, `${sc} ${card.variant}.${row.operation}`);
      }
    }
  }
  const out = ui.compute(LIVE, ui.defaultState(LIVE, 'logs'), {});
  const cas = out.vm.cards.find((c) => c.variant === 'cas');
  if (cas.state === 'unavailable' && /reads/.test(cas.reason)) {
    assert.equal(out.vm.table.rows.find((r) => r.operation === 'reads').cells.find((c) => c.variant === 'cas').state, 'unavailable');
  }
});
