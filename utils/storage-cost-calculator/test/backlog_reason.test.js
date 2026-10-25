'use strict';
// The CAS backlog sentence states the per-round numbers and the GC interval that keeps both budgets; the
// budgets are inputs; an edited preset no longer shows the preset's description.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const contract = require('../contract.js');
const ui = require('../ui.js');

const LIVE = contract.load(JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'model-contract.json'), 'utf8')));
const page = (inputs, sc) => {
  const s = ui.defaultState(LIVE, sc || 'logs');
  Object.assign(s.inputs, inputs || {});
  const out = ui.compute(LIVE, s, {});
  return Object.assign(out, { parts: ui.renderParts(out.vm, out.page), state: s });
};
const cas = (p) => p.vm.cards.find((c) => c.variant === 'cas');
const field = (p, id) => p.vm.inputs.blocks.flatMap((b) => b.fields).find((f) => f.id === id);

test('test_backlog_reason_names_budgets_and_threshold_interval', () => {
  const p = page({ cas_gc_interval_s: 600 });
  assert.equal(cas(p).state, 'backlog_grows');
  const r = cas(p).reason;
  assert.match(r, /a GC round retires 29,881 blobs \(rate 49\.37 blobs\/s × \(interval 600 s \+ fold 5\.27 s\)\)/);
  assert.match(r, /per-round budget of 5,000 \(graduation\) \/ 5,000 \(redelete\)/);
  assert.match(r, /keep `cas_gc_interval_sec` at or below 100\.1 s for this workload/);
  assert.match(r, /raise `cas_gc_round_graduation_budget` and `cas_gc_round_redelete_budget` in the disk config \(0 = unbounded\)/);
});

test('test_threshold_interval_is_the_boundary', () => {
  assert.equal(cas(page({ cas_gc_interval_s: 100.1 })).state, 'ok');
  assert.equal(cas(page({ cas_gc_interval_s: 100.3 })).state, 'backlog_grows');
});

test('test_owner_scenario_threshold', () => {
  const p = page({ replicas: 5, columns: 40, insert_rows: 100000, mutations_per_month: 18000, select_part_reads_per_day: 2000, cas_gc_interval_s: 120 });
  assert.equal(cas(p).state, 'backlog_grows');
  assert.match(cas(p).reason, /retires 5,987 blobs/);
  assert.match(cas(p).reason, /at or below 100\.1 s/);
});

test('test_budgets_are_inputs_and_zero_disables_the_state', () => {
  const p = page({ cas_gc_interval_s: 600 });
  for (const id of ['gc_round_graduation_budget', 'gc_round_redelete_budget']) {
    const f = field(p, id);
    assert.ok(f, id);
    assert.equal(f.kind, 'input');
    assert.equal(f.tier, 'advanced');
    assert.equal(f.value, 5000);
    assert.match(f.label, /cas_gc_round_/);
  }
  const both = page({ cas_gc_interval_s: 600, gc_round_graduation_budget: 0, gc_round_redelete_budget: 0 });
  assert.equal(cas(both).state, 'ok');
  const raised = page({ cas_gc_interval_s: 600, gc_round_graduation_budget: 40000, gc_round_redelete_budget: 40000 });
  assert.equal(cas(raised).state, 'ok');
  const one = page({ cas_gc_interval_s: 600, gc_round_graduation_budget: 0 });
  assert.equal(cas(one).state, 'backlog_grows');
  assert.match(cas(one).reason, /unbounded \(graduation\) \/ 5,000 \(redelete\)/);
});

test('test_no_interval_keeps_up_says_so', () => {
  const p = page({ cas_gc_interval_s: 600, gc_round_graduation_budget: 1, gc_round_redelete_budget: 1 });
  assert.equal(cas(p).state, 'backlog_grows');
  assert.match(cas(p).reason, /no GC interval keeps up with this workload; raise `cas_gc_round_graduation_budget`/);
});

test('test_edited_preset_drops_the_description', () => {
  const pristine = page({});
  assert.equal(pristine.page.scenario_edited, false);
  assert.match(pristine.parts.presets, /500 GiB\/day of logs kept 30 days on 2 replicas/);
  const edited = page({ replicas: 5 });
  assert.equal(edited.page.scenario_edited, true);
  assert.doesNotMatch(edited.parts.presets, /on 2 replicas/);
  assert.match(edited.parts.presets, /Logs preset with your edits/);
  assert.match(edited.parts.presets, /aria-pressed="true"[^>]*>Logs</, 'the chip stays on the chosen preset');
});

test('test_settings_field_head_wraps_so_a_badge_never_covers_the_label', () => {
  const css = fs.readFileSync(path.join(__dirname, '..', 'styles.css'), 'utf8');
  const rule = /\.panel-inputs \.settings-grid \.field-head \{([^}]*)\}/.exec(css);
  assert.ok(rule);
  assert.match(rule[1], /flex-wrap:\s*wrap/);
  assert.match(css, /\.panel-inputs \.field-head \.field-label \{[^}]*min-width:\s*min-content/);
});
