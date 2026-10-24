#!/usr/bin/env node
// Drill-down for one probe: evaluates the formulas against its measurement and prints/writes the five stages
// (inputs, rows with branch taken, ledger summary, billing lines, allocation). Exit 1 on a deterministic
// mismatch, a missing input or any failure to load an artifact.
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const model = require('../model.js');
const contract = require('../contract.js');
const billing = require('../billing.js');

const HERE = path.dirname(fileURLToPath(import.meta.url));
const EPS = 1e-9;

function repoRoot() {
  for (let d = HERE; ; d = path.dirname(d)) {
    if (fs.existsSync(path.join(d, 'utils')) && fs.existsSync(path.join(d, 'docs'))) return d;
    if (path.dirname(d) === d) throw new Error('repository root (directory with utils/ and docs/) not found above ' + HERE);
  }
}

function parseArgs(argv) {
  const a = { positional: [] };
  const flags = ['probe', 'out', 'prediction', 'formulas', 'billing', 'registry', 'coefficients', 'cell', 'report-dir', 'preset'];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith('--')) {
      const k = argv[i].slice(2);
      if (!flags.includes(k)) throw new Error(`unknown option ${argv[i]}`);
      if (i + 1 >= argv.length) throw new Error(`option ${argv[i]} needs a value`);
      a[k] = argv[++i];
    } else a.positional.push(argv[i]);
  }
  return a;
}

const readJson = (p) => {
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { throw new Error(`cannot read ${p}: ${e.message}`); }
};

function findReportDir(root, explicit) {
  if (explicit) return path.resolve(explicit);
  const base = path.join(root, 'docs', 'superpowers', 'reports');
  const dirs = fs.readdirSync(base).filter((d) => /^\d{4}-\d{2}-\d{2}-storage-cost-phase0$/.test(d)).sort();
  if (!dirs.length) throw new Error(`no <date>-storage-cost-phase0 directory under ${base}; pass --report-dir`);
  return path.join(base, dirs[dirs.length - 1]);
}

function buildCtx(registry, coefFile, prediction, record) {
  const inputs = new Map();
  const coefs = new Map();
  const tables = new Map();
  for (const q of registry.quantities) {
    if (q.kind !== 'derivation' && q.value_type !== 'table' && q.default !== null && q.default !== undefined) inputs.set(q.id, q.default);
  }
  for (const [k, v] of Object.entries(prediction.inputs || {})) inputs.set(k, v);
  const observations = record.observations || {};
  for (const [k, v] of Object.entries(observations)) inputs.set(k, v);
  const sets = new Map(Object.entries(registry.sets || {}));
  const ctx = { inputs, coefs, tables, sets, vars: new Map(), prices: null };
  for (const q of registry.quantities) {
    if (q.coef && q.value_type !== 'table') {
      const value = inputs.has(q.id) ? inputs.get(q.id) : q.default;
      if (value !== null && value !== undefined) coefs.set(q.coef, { kind: 'scalar', value, validity: q.validity, dim: q.unit });
    }
  }
  for (const c of coefFile) {
    if (c.kind === 'table') tables.set(c.id, { index: c.value.index, values: c.value.values, dim: c.dim });
    else coefs.set(c.id, { kind: 'scalar', value: c.value, validity: c.validity || null, dim: c.dim });
  }
  // Derived quantities (for example part_type) fill in when neither the prediction nor the run set them.
  for (let pass = 0; pass < 5; pass++) {
    let changed = false;
    for (const q of registry.quantities) {
      if (q.kind !== 'derivation' || !q.rule || inputs.has(q.id)) continue;
      const r = model.evaluate(q.rule, ctx);
      if (r.state === 'ok') { inputs.set(q.id, r.value); changed = true; }
    }
    if (!changed) break;
  }
  return { ctx, observations };
}

function branches(trace, out = []) {
  if (trace.branch_taken) out.push(trace.branch_taken);
  (trace.children || []).forEach((c) => branches(c, out));
  return out;
}

function missingIn(node, ctx, out = new Set()) {
  if (!node || typeof node !== 'object') return out;
  if (node.kind === 'input' && !ctx.inputs.has(node.id)) out.add(`input ${node.id}`);
  if (node.kind === 'coef' && !ctx.coefs.has(node.id)) out.add(`coef ${node.id}`);
  if (node.kind === 'lookup' && !ctx.tables.has(node.table_id)) out.add(`table ${node.table_id}`);
  for (const v of Object.values(node)) {
    if (Array.isArray(v)) v.forEach((x) => missingIn(x, ctx, out));
    else if (v && typeof v === 'object') missingIn(v, ctx, out);
  }
  return out;
}

function sumCounts(foreground, op, field) {
  let s = 0;
  for (const caller of Object.values(foreground || {})) s += ((caller[op] || {})[field]) || 0;
  return s;
}

function expectedFor(prediction, op) {
  const fg = (prediction.expected || {}).foreground;
  if (!fg) return null;
  let min = 0;
  let max = 0;
  let seen = false;
  for (const caller of Object.values(fg)) {
    if (!(op in caller)) continue;
    const e = caller[op];
    seen = true;
    min += typeof e === 'number' ? e : e.min;
    max += typeof e === 'number' ? e : e.max;
  }
  if (!seen) return null;
  return min === max ? min : { min, max };
}

function drillCell(op, cell, ctx, record, prediction, preset, problems) {
  const fg = (record.ledger || {}).foreground || {};
  const deterministic = record.mode === 'deterministic';
  const ev = model.evaluateCell(cell, ctx);
  const rows = ev.rows;
  const stage2 = rows.map((r, i) => {
    const out = {
      row: `${r.row_type} ${r.op_kind} ${r.outcome_class}`, row_type: r.row_type, op_kind: r.op_kind, outcome_class: r.outcome_class,
      branch_taken: branches(r.trace), predicted: r.state === 'ok' ? r.quantity : null, measured: null, expected: null, exact: null,
    };
    if (r.state !== 'ok') {
      const miss = [...missingIn(cell.ledger_rows[i].expr, ctx)];
      problems.push(`${op}: row "${out.row}" unavailable${miss.length ? `; missing ${miss.join(', ')}` : ''}`);
      out.exact = false;
      return out;
    }
    if (r.row_type !== 'request') return out;
    // The prediction counts attempts per op, so ok and all rows compare the op's total predicted requests
    // (every request row of the op) with the measured attempts. upstream_ok is informational.
    if (r.outcome_class === 'all' || r.outcome_class === 'ok') {
      out.measured = sumCounts(fg, r.op_kind, 'attempts');
      out.upstream_ok = sumCounts(fg, r.op_kind, 'upstream_ok');
      out.op_predicted = rows.filter((x) => x.row_type === 'request' && x.op_kind === r.op_kind && x.state === 'ok').reduce((t, x) => t + x.quantity, 0);
      out.expected = expectedFor(prediction, r.op_kind);
    } else if (r.outcome_class === 'dropped') out.measured = sumCounts(fg, r.op_kind, 'dropped');
    if (out.measured !== null && deterministic) {
      const basis = out.op_predicted === undefined ? out.predicted : out.op_predicted;
      out.exact = Math.abs(basis - out.measured) <= EPS;
      if (!out.exact) problems.push(`${op}: row "${out.row}" predicted ${basis} measured ${out.measured}`);
      const e = out.expected;
      const inside = e === null || (typeof e === 'number' ? Math.abs(out.measured - e) <= EPS : out.measured >= e.min && out.measured <= e.max);
      if (!inside) problems.push(`${op}: row "${out.row}" measured ${out.measured} is outside the prediction ${JSON.stringify(e)}`);
    }
    return out;
  });

  const byType = {};
  for (const r of rows) {
    const t = (byType[r.row_type] = byType[r.row_type] || { rows: 0, quantity: 0 });
    t.rows += 1;
    if (typeof r.quantity === 'number') t.quantity += r.quantity;
  }
  for (const s of ev.states) if (s.id === 'not_supported' && s.active) problems.push(`${op}: not supported: ${s.reason}`);

  let lines = null;
  let total = null;
  let allocation = null;
  if (rows.every((r) => r.state === 'ok')) {
    try {
      lines = [];
      total = 0;
      for (const scope of [...new Set(rows.map((r) => r.scope))]) {
        const b = billing.bill(rows, preset, scope, null);
        lines.push(...b.lines);
        total += b.total;
        if (b.allocation) {
          if (allocation) throw new Error('block rows in more than one scope');
          allocation = Object.assign({ scope }, b.allocation);
        }
      }
    } catch (e) {
      problems.push(`${op}: billing failed: ${e.message}`);
      lines = null;
      total = null;
    }
  }
  const pick = (m) => Object.fromEntries([...m].map(([k, v]) => [k, v && typeof v === 'object' && 'value' in v ? v.value : v]));
  return {
    stage1_inputs: { inputs: pick(ctx.inputs), coefs: pick(ctx.coefs), observations: record.observations || {} },
    stage2_rows: stage2,
    stage3_ledger_summary: { by_row_type: byType, states: ev.states, extrapolated: ev.extrapolated, allocation: ev.allocation, notes: ev.notes.map((n) => n.value) },
    stage4_billing_lines: lines,
    billing_total: total,
    stage5_allocation: allocation,
  };
}

function printCell(key, c) {
  const out = [`== ${key}`, '1. inputs', ...Object.entries(c.stage1_inputs.inputs).map(([k, v]) => `   ${k} = ${v}`)];
  out.push('2. rows (branch | predicted | measured | expected | exact)');
  for (const r of c.stage2_rows) {
    out.push(`   ${r.row} | ${r.branch_taken.join(',') || '-'} | ${r.predicted} | ${r.measured} | ${JSON.stringify(r.expected)} | ${r.exact}`);
  }
  out.push('3. ledger summary', ...Object.entries(c.stage3_ledger_summary.by_row_type).map(([t, v]) => `   ${t}: ${v.rows} rows, quantity ${v.quantity}`));
  out.push('4. billing', ...(c.stage4_billing_lines || []).map((l) => `   ${l.rule_id}: ${l.quantity} ${l.unit} = $${l.money}`), `   total = $${c.billing_total}`);
  const a = c.stage5_allocation;
  out.push('5. allocation', a ? `   ${a.layout || 'manual'}: ${a.volumes} x ${a.size_gib} GiB, ${a.iops} IOPS, ${a.mibps} MiB/s = $${a.monthly_cost}/month` : '   none');
  return out.join('\n');
}

function main(argv) {
  const args = parseArgs(argv);
  const jsonl = args.positional[0];
  if (!jsonl || !args.probe) throw new Error('usage: drill.mjs <measurements.jsonl> --probe <card> [--out FILE] [--prediction P --formulas P --billing P --registry P --coefficients P --cell [variant/]op --report-dir D --preset ID]');
  const root = repoRoot();
  // Resolved only when a default path under the report directory is needed.
  const reportDir = () => findReportDir(root, args['report-dir']);
  const model_dir = path.join(root, 'docs', 'superpowers', 'cost-model');
  const all = fs.readFileSync(jsonl, 'utf8').split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l)).filter((r) => r.card === args.probe);
  const records = all.filter((r) => r.status === 'ok');
  if (records.length < all.length) console.log(`skipped ${all.length - records.length} non-ok record(s): ${all.filter((r) => r.status !== 'ok').map((r) => `${r.measurement_id} (${r.status})`).join(', ')}`);
  if (!records.length) throw new Error(`no ok record for probe ${args.probe} in ${jsonl}`);

  const billingDoc = readJson(args.billing || path.join(model_dir, 'billing-contract.json'));
  const preset = billingDoc.presets.find((p) => p.id === (args.preset || 'aws-us-east-1'));
  if (!preset || preset.structure_only) throw new Error(`no full billing preset ${args.preset || 'aws-us-east-1'}`);
  const registry = readJson(args.registry || path.join(model_dir, 'inputs-registry.json'));
  const fdoc = readJson(args.formulas || path.join(reportDir(), 'formulas.json'));
  const merged = contract.mergeFormulas(Array.isArray(fdoc) ? fdoc : [fdoc]);
  if (merged.problems.length) throw new Error(merged.problems.join('\n'));
  const coefPath = args.coefficients || (args['report-dir'] || !args.formulas ? path.join(reportDir(), 'coefficients.json') : null);
  let coefFile = coefPath && fs.existsSync(coefPath) ? readJson(coefPath) : [];
  if (!Array.isArray(coefFile)) coefFile = coefFile.coefficients || [];

  const problems = [];
  const cells = {};
  for (const rec of records) {
    const predPath = args.prediction || path.join(reportDir(), 'predictions', `${rec.experiment_id}.${rec.policy}.json`);
    const prediction = readJson(predPath);
    const variantCells = merged.formulas[rec.variant];
    if (!variantCells) { problems.push(`${rec.measurement_id}: no formulas for variant ${rec.variant}`); continue; }
    const want = args.cell ? args.cell.split('/') : null;
    if (want && want.length === 2 && want[0] !== rec.variant) continue;
    const ops = Object.keys(variantCells).filter((o) => !want || o === want[want.length - 1]);
    for (const op of ops) {
      const samePolicy = records.filter((x) => x.policy === rec.policy).length > 1;
      const key = records.length === 1 ? op : samePolicy ? `${rec.policy}/${rec.case_id}/${op}` : `${rec.policy}/${op}`;
      const { ctx } = buildCtx(registry, coefFile, prediction, rec);
      cells[key] = drillCell(key, variantCells[op], ctx, rec, prediction, preset, problems);
      cells[key].measurement_id = rec.measurement_id;
      console.log(printCell(key, cells[key]));
    }
  }
  if (args.cell && !Object.keys(cells).length && !problems.length) problems.push(`--cell ${args.cell} selected no cell across ${records.length} record(s)`);
  const result = { probe: args.probe, measurement_id: records.map((r) => r.measurement_id).join(','), cells, mismatches: problems };
  if (args.out) fs.writeFileSync(args.out, JSON.stringify(result, null, 2) + '\n');
  if (problems.length) console.error(`MISMATCH (${problems.length}):\n  ${problems.join('\n  ')}`);
  else console.log(`OK ${args.probe}: ${Object.keys(cells).length} cell(s)`);
  return problems.length ? 1 : 0;
}

try {
  process.exitCode = main(process.argv.slice(2));
} catch (e) {
  console.error(`drill.mjs: ${e.message}`);
  process.exitCode = 1;
}
