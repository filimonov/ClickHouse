#!/usr/bin/env node
// Drill-down for one probe: evaluates the probe's formulas cells inside a validated contract against its
// measurement and prints/writes the five stages (inputs, rows with branch taken, ledger summary, billing lines,
// allocation) with the evidence that qualifies them. Exit 1 on a reconciliation problem, a missing input, an
// unmodelled residual or any failure to load an artifact.
import { createRequire } from 'node:module';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const model = require('../model.js');
const contract = require('../contract.js');
const billing = require('../billing.js');

const HERE = path.dirname(fileURLToPath(import.meta.url));
const EPS = 1e-9;
const GIB = 2 ** 30;
const HOURS_PER_MONTH = 730; // the AWS price-list month; probes turn byte-hours into GiB-months with it
const WORKLOAD = ['r1', 'r2', 'r3'];
const EVIDENCE_STATEMENT = 'Measured on one workstation: a local RustFS store behind the ledger proxy and loopback '
  + 'interserver relays. Request counts are exact ledger counts; storage is a byte-hour bound; local I/O is syscall demand.';
// Row outcome classes reconcile with ledger outcomes: client_error is an upstream 4XX, server_error any other
// upstream error, not_sent an attempt the store never saw (injected without forwarding, rejected, no connection).
const GROUP = { ok: 'ok', accepted_dropped: 'accepted_dropped', dropped: 'dropped', client_error: 'client_error', server_error: 'server_error', not_sent: 'not_sent', all: 'all' };
const CLASSES = ['ok', 'accepted_dropped', 'dropped', 'client_error', 'server_error', 'not_sent'];
const TRANSFER = { interserver_from_target: 'from_target', interserver_to_target: 'to_target' };

function repoRoot() {
  for (let d = HERE; ; d = path.dirname(d)) {
    if (fs.existsSync(path.join(d, 'utils')) && fs.existsSync(path.join(d, 'docs'))) return d;
    if (path.dirname(d) === d) throw new Error('repository root (directory with utils/ and docs/) not found above ' + HERE);
  }
}

function parseArgs(argv) {
  const a = { positional: [] };
  const flags = ['probe', 'out', 'prediction', 'formulas', 'billing', 'registry', 'coefficients', 'coverage', 'cell', 'report-dir', 'preset'];
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
const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

function findReportDir(root, explicit) {
  if (explicit) return path.resolve(explicit);
  const base = path.join(root, 'docs', 'superpowers', 'reports');
  const dirs = fs.readdirSync(base).filter((d) => /^\d{4}-\d{2}-\d{2}-storage-cost-phase0$/.test(d)).sort();
  if (!dirs.length) throw new Error(`no <date>-storage-cost-phase0 directory under ${base}; pass --report-dir`);
  return path.join(base, dirs[dirs.length - 1]);
}

// ---- contract ---------------------------------------------------------------------------------

// Pinned registry coefficients become contract coefficients unless the report carries a measured or cited one.
function contractCoefficients(registry, coefFile) {
  const out = coefFile.map((c) => Object.assign({ evidence: { ref: 'coefficients.json' } }, c));
  const have = new Set(out.map((c) => c.id));
  for (const q of registry.quantities) {
    if (!q.coef || q.value_type === 'table' || have.has(q.coef)) continue;
    out.push({ id: q.coef, kind: 'scalar', dim: q.unit, value: q.default, validity: q.validity, evidence: { ref: q.evidence.ref } });
    have.add(q.coef);
  }
  return out;
}

function buildContract({ registry, coefFile, billingDoc, coverage, merged, evidence }) {
  const operations = [...new Set(Object.values(merged.formulas).flatMap((cells) => Object.keys(cells)))].sort();
  const applicability = {};
  for (const v of coverage.domain.variants) {
    applicability[v] = {};
    for (const op of operations) {
      applicability[v][op] = Object.fromEntries(coverage.domain.families.map((f) => [f, merged.formulas[v] && merged.formulas[v][op] ? 'supported' : 'not_supported']));
    }
  }
  const code = ['model.js', 'contract.js', 'billing.js', path.join('tools', 'drill.mjs')]
    .map((f) => fs.readFileSync(path.join(HERE, '..', f))).reduce((h, b) => h.update(b), crypto.createHash('sha256')).digest('hex');
  return contract.load({
    schema: contract.CONTRACT_SCHEMA,
    version: '0.1.0',
    manifest: { code_sha256: code, contract_version: '1', registry_version: String(registry.version), coverage_version: String(coverage.version),
      billing_version: String(billingDoc.version), evidence },
    evidence_statement: EVIDENCE_STATEMENT,
    registry: registry.quantities,
    sets: registry.sets || {},
    precedence: registry.precedence || [],
    coefficients: contractCoefficients(registry, coefFile),
    billing: billingDoc,
    variants: coverage.domain.variants,
    operations,
    families: coverage.domain.families,
    applicability,
    formulas: merged.formulas,
    states: merged.states,
  });
}

function typeProblem(q, v) {
  if (q.value_type === 'number') return typeof v === 'number' && Number.isFinite(v) ? null : `expected a finite number, got ${JSON.stringify(v)}`;
  if (q.value_type === 'enum') return (q.enum || []).includes(v) ? null : `expected one of ${JSON.stringify(q.enum)}, got ${JSON.stringify(v)}`;
  return `value_type ${q.value_type} cannot be an input`;
}

// Inputs come only from registered quantities: defaults, the prediction's registered inputs, and the record's
// `observations.inputs`, which must name registered `measured` quantities. Everything else is evidence, not input.
function buildCtx(c, prediction, record, problems, where) {
  const quantities = new Map(c.registry.map((q) => [q.id, q]));
  const inputs = new Map();
  for (const q of c.registry) {
    if (q.kind !== 'derivation' && q.value_type !== 'table' && q.default !== null && q.default !== undefined) inputs.set(q.id, q.default);
  }
  const ignored = {};
  for (const [k, v] of Object.entries(prediction.inputs || {})) {
    const q = quantities.get(k);
    if (!q) { ignored[k] = v; continue; }
    const bad = typeProblem(q, v);
    if (bad) problems.push(`${where}: prediction input ${k}: ${bad}`);
    else inputs.set(k, v);
  }
  const observed = (record.observations || {}).inputs || {};
  for (const [k, v] of Object.entries(observed)) {
    const q = quantities.get(k);
    if (!q) { problems.push(`${where}: observation ${k} is not a registered quantity`); continue; }
    if (q.kind !== 'measured') { problems.push(`${where}: observation ${k} names a ${q.kind} quantity, not a measured one`); continue; }
    const bad = typeProblem(q, v);
    if (bad) problems.push(`${where}: observation ${k}: ${bad}`);
    else inputs.set(k, v);
  }
  const coefs = new Map();
  for (const rec of c.coefficients) {
    if (rec.kind !== 'table' && rec.value !== null && rec.value !== undefined) coefs.set(rec.id, { kind: 'scalar', value: rec.value, validity: rec.validity || null, dim: rec.dim });
  }
  const tables = new Map(Object.entries(c.tables));
  const ctx = { inputs, coefs, tables, sets: new Map(Object.entries(c.sets || {})), vars: new Map(), prices: null };
  for (let pass = 0; pass < 5; pass++) {
    let changed = false;
    for (const q of c.registry) {
      if (q.kind !== 'derivation' || !q.rule || inputs.has(q.id)) continue;
      const r = model.evaluate(q.rule, ctx);
      if (r.state === 'ok') { inputs.set(q.id, r.value); changed = true; }
    }
    if (!changed) break;
  }
  return { ctx, ignored };
}

// ---- evaluation -------------------------------------------------------------------------------

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

function limitationsOf(record) {
  return [...new Set(Object.values(record.local || {}).flatMap((l) => l.limitations || []))];
}

function evidenceOf(record) {
  const inv = record.inventory || {};
  const bounds = Object.fromEntries(Object.entries(inv.byte_hours || {}).filter(([, b]) => b.upper > 0));
  return {
    statement: EVIDENCE_STATEMENT, limitations: limitationsOf(record), mode: record.mode,
    inventory_initial: inv.initial || null, inventory_note: inv.note || null,
    ledger_complete: (record.ledger || {}).complete !== false, open_attempts: ((record.window || {}).open_attempts || []).length,
    bounds: { byte_hours: bounds },
  };
}

// Why a cell has no comparable number: unavailable rows or weights, a state that is active or unknown, an
// incomplete ledger. Such a cell is shown with its reasons and billed nothing.
function unavailableReasons(ev, rows, record) {
  const reasons = [];
  if (ev.state === 'unavailable') reasons.push(ev.reason);
  if (rows.some((r) => r.state !== 'ok')) reasons.push('a row is unavailable');
  for (const s of ev.states) {
    if (s.active === null) reasons.push(`state ${s.id} unknown: ${s.reason}`);
    else if (s.active) reasons.push(`${s.id === 'not_supported' ? 'not supported' : s.id}: ${s.reason}`);
  }
  if ((record.ledger || {}).complete === false) {
    reasons.push(`incomplete ledger: ${record.window.open_attempts.length} attempt(s) open at the window end; request totals are lower bounds`);
  }
  return reasons;
}

function evalCell(key, cell, ctx, problems) {
  const ev = model.evaluateCell(cell, ctx);
  const rows = ev.rows.map((r, i) => {
    const out = {
      row: `${r.row_type} ${r.op_kind} ${r.outcome_class}${r.caller ? ` ${r.caller}` : ''}`, row_type: r.row_type, op_kind: r.op_kind,
      outcome_class: r.outcome_class, caller: r.caller || null, category: r.category, route: r.route, branch_taken: branches(r.trace), predicted: r.state === 'ok' ? r.quantity : null,
      measured: null, expected: null, exact: null, state: r.state,
    };
    if (r.state !== 'ok') {
      const miss = [...missingIn(cell.ledger_rows[i].expr, ctx)];
      problems.push(`${key}: row "${out.row}" unavailable${miss.length ? `; missing ${miss.join(', ')}` : ''}`);
      out.exact = false;
    }
    return out;
  });
  return { ev, rows };
}

// ---- reconciliation ---------------------------------------------------------------------------

function integrated(record) {
  // Workload usage the cells must cover: foreground plus maintenance, per caller and op.
  const out = {};
  for (const part of ['foreground', 'maintenance']) {
    for (const [caller, ops] of Object.entries((record.ledger || {})[part] || {})) {
      if (!WORKLOAD.includes(caller)) continue;
      for (const [op, c] of Object.entries(ops)) {
        const t = ((out[caller] = out[caller] || {})[op] = out[caller][op] || { attempts: 0, upstream_ok: 0, upstream_error: 0, upstream_unknown: 0, accepted_dropped: 0, upstream_4xx: 0 });
        for (const f of Object.keys(t)) t[f] += c[f] || 0;
      }
    }
  }
  return out;
}

function classCount(c, group) {
  const ok = c.upstream_ok - (c.accepted_dropped || 0);
  switch (group) {
    case 'ok': return ok;
    case 'accepted_dropped': return c.accepted_dropped || 0;
    case 'dropped': return c.upstream_unknown;
    case 'client_error': return c.upstream_4xx || 0;
    case 'server_error': return c.upstream_error - (c.upstream_4xx || 0);
    case 'not_sent': return c.attempts - c.upstream_ok - c.upstream_error - c.upstream_unknown;
    default: return c.attempts;
  }
}

function expectedRange(prediction, caller, op) {
  const want = (((prediction.expected || {}).foreground || {})[caller] || {})[op];
  if (want === undefined) return null;
  return typeof want === 'number' ? { min: want, max: want } : want;
}

// Every request row group (caller or all, op, outcome class) must equal the ledger; every ledger usage must be
// covered by a row or an explicit exclusion; prediction ranges hold within the prediction's tolerance.
function reconcile(where, record, prediction, cells, problems) {
  const usage = integrated(record);
  const deterministic = record.mode === 'deterministic';
  const reqRows = cells.flatMap((c) => c.rows.map((r, i) => ({ r, ev: c.ev.rows[i], key: c.key }))).filter((x) => x.r.row_type === 'request' && x.r.state === 'ok');
  const excluded = new Map(cells.flatMap((c) => (c.cell.excluded || []).map((e) => [`${e.kind}:${e.id}`, e.reason])));
  const explicit = (op) => new Set(reqRows.filter((x) => x.r.op_kind === op && x.r.caller).map((x) => x.r.caller));
  const groups = new Map();
  for (const x of reqRows) {
    const k = `${x.r.caller || '*'}|${x.r.op_kind}|${GROUP[x.r.outcome_class]}`;
    (groups.get(k) || groups.set(k, []).get(k)).push(x);
  }
  for (const [k, members] of groups) {
    const [callerKey, op, group] = k.split('|');
    const callers = callerKey === '*' ? Object.keys(usage).filter((c) => !explicit(op).has(c)) : [callerKey];
    const measured = callers.reduce((s, c) => s + (usage[c] && usage[c][op] ? classCount(usage[c][op], group) : 0), 0);
    const predicted = members.reduce((s, x) => s + x.r.predicted, 0);
    const exact = Math.abs(predicted - measured) <= EPS;
    for (const x of members) Object.assign(x.r, { measured, group_predicted: predicted, exact });
    if (!exact) problems.push(`${where}: rows "${members[0].r.row_type} ${op} ${group}${callerKey === '*' ? '' : ` ${callerKey}`}" predicted ${predicted} measured ${measured}`);
  }
  for (const [caller, ops] of Object.entries(usage)) {
    for (const [op, c] of Object.entries(ops)) {
      for (const group of CLASSES) {
        if (classCount(c, group) <= 0) continue;
        const covered = reqRows.some((x) => x.r.op_kind === op && (x.r.caller === caller || (!x.r.caller && !explicit(op).has(caller)))
          && (GROUP[x.r.outcome_class] === group || x.r.outcome_class === 'all'));
        if (!covered && !excluded.has(`op:${op}`)) problems.push(`${where}: unmodelled residual: ${caller} ${op} ${group} x${classCount(c, group)}`);
      }
    }
  }
  // Prediction ranges are about foreground requests per caller and op; stochastic cards get the stated tolerance.
  const fg = (record.ledger || {}).foreground || {};
  const tol = prediction.tolerance || { abs: {}, rel: {} };
  for (const [caller, ops] of Object.entries(((prediction.expected || {}).foreground) || {})) {
    for (const op of Object.keys(ops)) {
      const e = expectedRange(prediction, caller, op);
      const got = ((fg[caller] || {})[op] || {}).attempts || 0;
      const slack = deterministic ? 0 : ((tol.abs || {})[op] || 0) + ((tol.rel || {})[op] || 0) * e.max;
      const inside = got >= e.min - slack && got <= e.max + slack;
      if (!inside) problems.push(`${where}: ${caller} ${op} measured ${got} is outside the prediction ${e.min}..${e.max}${slack ? ` +- ${slack}` : ''}`);
      for (const x of reqRows.filter((y) => y.r.op_kind === op && (y.r.caller === caller || !y.r.caller))) {
        x.r.expected = e.min === e.max && !slack ? e.min : { min: e.min - slack, max: e.max + slack };
      }
    }
  }
  // Storage rows bill the upper byte-hour bound of each category, as GiB-months.
  const bh = (record.inventory || {}).byte_hours || {};
  const storage = cells.flatMap((c) => c.rows.filter((r) => r.row_type === 'storage' && r.state === 'ok'));
  const byCat = {};
  for (const r of storage) byCat[r.category] = (byCat[r.category] || 0) + r.predicted;
  for (const [cat, b] of Object.entries(bh)) {
    const want = b.upper / GIB / HOURS_PER_MONTH;
    if (want <= 0) continue;
    if (!(cat in byCat)) {
      if (!excluded.has(`category:${cat}`)) problems.push(`${where}: unmodelled residual: storage category ${cat} (${b.lower}..${b.upper} byte-hours)`);
      continue;
    }
    const exact = Math.abs(byCat[cat] - want) <= EPS * Math.max(1, want);
    for (const r of storage.filter((y) => y.category === cat)) Object.assign(r, { measured: want, exact });
    if (!exact) problems.push(`${where}: storage ${cat} rows ${byCat[cat]} GiB-months, record upper bound ${want}`);
  }
  for (const cat of Object.keys(byCat)) {
    if (!(bh[cat] && bh[cat].upper > 0) && byCat[cat] > EPS) problems.push(`${where}: storage ${cat} rows bill ${byCat[cat]} GiB-months the record does not hold`);
  }
  // Interserver bytes per direction; a direction a relay carried must be billed or excluded.
  for (const c of cells) {
    for (const r of c.rows.filter((x) => x.row_type === 'transfer' && !(x.op_kind in TRANSFER))) {
      problems.push(`${where}: transfer row "${r.row}" has no measured counterpart (op_kind must be one of ${Object.keys(TRANSFER).join(', ')})`);
    }
  }
  const relays = Object.values(record.interserver || {});
  for (const [opKind, field] of Object.entries(TRANSFER)) {
    const want = relays.reduce((s, r) => s + (r[field] || 0), 0) / GIB;
    const rows = cells.flatMap((c) => c.rows.filter((r) => r.row_type === 'transfer' && r.op_kind === opKind && r.state === 'ok'));
    const got = rows.reduce((s, r) => s + r.predicted, 0);
    if (!rows.length) {
      if (want > 0 && !excluded.has(`op:${opKind}`)) problems.push(`${where}: unmodelled residual: ${opKind} ${want} GiB`);
      continue;
    }
    const exact = Math.abs(got - want) <= EPS * Math.max(1, want);
    rows.forEach((r) => Object.assign(r, { measured: want, exact }));
    if (!exact) problems.push(`${where}: transfer ${opKind} rows ${got} GiB, relays ${want} GiB`);
  }
}

// ---- billing and allocation -------------------------------------------------------------------

// Quantity a cell contributes to one aggregate billing line; allocation is proportional to it.
function cellQuantity(line, rows, preset) {
  const mine = rows.filter((r) => r.scope === line.scope);
  if (line.rule_id === 'storage') return mine.filter((r) => r.row_type === 'storage').reduce((s, r) => s + r.quantity, 0);
  if (line.rule_id === 'block_capacity') return mine.filter((r) => r.op_kind === 'capacity').reduce((s, r) => s + r.quantity, 0);
  if (line.rule_id === 'block_iops') return mine.filter((r) => r.op_kind === 'iops').reduce((s, r) => s + r.quantity, 0);
  if (line.rule_id === 'block_mibps') return mine.filter((r) => r.op_kind === 'mibps').reduce((s, r) => s + r.quantity, 0);
  const transfer = mine.filter((r) => r.row_type === 'transfer' && r.route === line.rule_id);
  if (transfer.length) return transfer.reduce((s, r) => s + r.quantity, 0);
  const req = mine.filter((r) => r.row_type === 'request');
  if (!req.length) return 0;
  const own = billing.bill(req, preset, line.scope, null).lines.find((l) => l.rule_id === line.rule_id);
  return own ? own.quantity : 0;
}

// Bills the record's available cells once per scope (tiers and baselines see the aggregate), then allocates
// each line's money back to the cells: by the cells' `allocation.shares` when declared, else by quantity.
function billGroup(where, cells, preset, problems) {
  const billed = cells.filter((c) => !c.unavailable.length);
  const rows = billed.flatMap((c) => c.ev.rows.map((r) => Object.assign({}, r, { cell: c.key })));
  if (!rows.length) return { lines: null, total: null, provisioning: null, allocation: [] };
  let lines = [];
  let provisioning = null;
  try {
    for (const scope of [...new Set(rows.map((r) => r.scope))]) {
      const b = billing.bill(rows, preset, scope, null);
      lines.push(...b.lines);
      if (b.allocation) {
        if (provisioning) throw new Error('block rows in more than one scope');
        provisioning = Object.assign({ scope }, b.allocation);
      }
    }
  } catch (e) {
    problems.push(`${where}: billing failed: ${e.message}`);
    return { lines: null, total: null, provisioning: null, allocation: [] };
  }
  const declared = {};
  for (const c of billed) Object.assign(declared, c.ev.allocation || {});
  const shareKeys = Object.keys(declared);
  for (const k of shareKeys) if (!billed.some((c) => c.name === k)) problems.push(`${where}: allocation share for unknown cell ${k}`);
  const allocation = lines.map((l) => {
    const weights = Object.fromEntries(billed.map((c) => [c.key, shareKeys.length ? (declared[c.name] || 0) : cellQuantity(l, c.ev.rows, preset)]));
    const sum = Object.values(weights).reduce((s, w) => s + w, 0);
    if (sum <= 0 && l.money > EPS) problems.push(`${where}: ${l.rule_id} money ${l.money} has no cell to allocate to`);
    const shares = Object.fromEntries(Object.entries(weights).map(([k, w]) => [k, { weight: w, fraction: sum > 0 ? w / sum : 0, money: sum > 0 ? l.money * w / sum : 0 }]));
    return { rule_id: l.rule_id, scope: l.scope, money: l.money, basis: shareKeys.length ? 'allocation.shares' : 'quantity', shares };
  });
  return { lines, total: lines.reduce((s, l) => s + l.money, 0), provisioning, allocation };
}

function summarize(rows) {
  // One entry per row type and op kind: quantities of different units are never added together.
  const out = {};
  for (const r of rows) {
    const t = (out[`${r.row_type} ${r.op_kind}`] = out[`${r.row_type} ${r.op_kind}`] || { rows: 0, quantity: 0 });
    t.rows += 1;
    if (typeof r.quantity === 'number') t.quantity += r.quantity;
  }
  return out;
}

// ---- output -----------------------------------------------------------------------------------

function printCell(key, c) {
  const out = [`== ${key}`, '1. inputs', ...Object.entries(c.stage1_inputs.inputs).map(([k, v]) => `   ${k} = ${v}`)];
  out.push('2. rows (branch | predicted | measured | expected | exact)');
  for (const r of c.stage2_rows) {
    out.push(`   ${r.row} | ${r.branch_taken.join(',') || '-'} | ${r.predicted} | ${r.measured} | ${JSON.stringify(r.expected)} | ${r.exact}`);
  }
  out.push('3. ledger summary', ...Object.entries(c.stage3_ledger_summary.by_row).map(([t, v]) => `   ${t}: ${v.rows} rows, quantity ${v.quantity}`));
  if (c.state === 'unavailable') out.push(`4. billing: unavailable: ${c.unavailable_reasons.join('; ')}`);
  else out.push('4. billing (this cell\'s allocated share)', ...(c.stage4_billing_lines || []).map((l) => `   ${l.rule_id}: ${l.quantity} ${l.unit} = $${l.money}`), `   total = $${c.billing_total}`);
  const e = c.evidence;
  out.push(`   evidence: ${e.mode}, inventory ${e.inventory_initial}, ledger ${e.ledger_complete ? 'complete' : 'incomplete'}`, ...e.limitations.map((l) => `   - ${l}`));
  return out.join('\n');
}

function printGroup(key, g) {
  const out = [`== group ${key}: ${g.cells.join(', ')}`, '4. billing lines (billed once for the group)'];
  for (const l of g.lines || []) out.push(`   ${l.rule_id}: ${l.quantity} ${l.unit} = $${l.money}`);
  out.push(`   total = $${g.total}`, '5. allocation');
  for (const a of g.allocation) out.push(`   ${a.rule_id} $${a.money} by ${a.basis}: ${Object.entries(a.shares).map(([k, s]) => `${k} $${s.money}`).join(', ')}`);
  const p = g.provisioning;
  if (p) out.push(`   provisioning ${p.layout || 'manual'}: ${p.volumes} x ${p.size_gib} GiB, ${p.iops} IOPS, ${p.mibps} MiB/s = $${p.monthly_cost}/month`);
  return out.join('\n');
}

function selectCells(variantCells, want, probe) {
  const name = want ? want[want.length - 1] : null;
  return Object.keys(variantCells).filter((o) => !name || o === name || o.startsWith(`${name}.`) || (!want && probe && o.startsWith(`${probe}.`)));
}

function main(argv) {
  const args = parseArgs(argv);
  const jsonl = args.positional[0];
  if (!jsonl || !args.probe) throw new Error('usage: drill.mjs <measurements.jsonl> --probe <card> [--out FILE] [--prediction P --formulas P --billing P --registry P --coefficients P --coverage P --cell [variant/]op --report-dir D --preset ID]');
  const root = repoRoot();
  const reportDir = () => findReportDir(root, args['report-dir']);
  const modelDir = path.join(root, 'docs', 'superpowers', 'cost-model');
  const all = fs.readFileSync(jsonl, 'utf8').split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l)).filter((r) => r.card === args.probe);
  const records = all.filter((r) => r.status === 'ok');
  if (records.length < all.length) console.log(`skipped ${all.length - records.length} non-ok record(s): ${all.filter((r) => r.status !== 'ok').map((r) => `${r.measurement_id} (${r.status})`).join(', ')}`);
  if (!records.length) throw new Error(`no ok record for probe ${args.probe} in ${jsonl}`);

  const billingDoc = readJson(args.billing || path.join(modelDir, 'billing-contract.json'));
  const preset = billingDoc.presets.find((p) => p.id === (args.preset || 'aws-us-east-1'));
  if (!preset || preset.structure_only) throw new Error(`no full billing preset ${args.preset || 'aws-us-east-1'}`);
  const registry = readJson(args.registry || path.join(modelDir, 'inputs-registry.json'));
  const coverage = readJson(args.coverage || path.join(modelDir, 'coverage.json'));
  const fdoc = readJson(args.formulas || path.join(reportDir(), 'formulas.json'));
  const merged = contract.mergeFormulas(Array.isArray(fdoc) ? fdoc : [fdoc]);
  if (merged.problems.length) throw new Error(merged.problems.join('\n'));
  const coefPath = args.coefficients || (args['report-dir'] || !args.formulas ? path.join(reportDir(), 'coefficients.json') : null);
  let coefFile = coefPath && fs.existsSync(coefPath) ? readJson(coefPath) : [];
  if (!Array.isArray(coefFile)) coefFile = coefFile.coefficients || [];
  const sumsPath = args.formulas && !args['report-dir'] ? null : path.join(reportDir(), 'raw', 'SHA256SUMS');
  const evidence = [{ report: sumsPath ? path.relative(root, path.dirname(path.dirname(sumsPath))) : 'fixture',
    commit: records[0].rig_commit || records[0].commit || 'unknown',
    raw_sha256sums: sumsPath && fs.existsSync(sumsPath) ? sha256(fs.readFileSync(sumsPath)) : '' }];
  const c = buildContract({ registry, coefFile, billingDoc, coverage, merged, evidence });

  const problems = [];
  const cells = {};
  const groups = {};
  const want = args.cell ? args.cell.split('/') : null;
  for (const rec of records) {
    const predPath = args.prediction || path.join(reportDir(), 'predictions', `${rec.experiment_id}.${rec.policy}.json`);
    const prediction = readJson(predPath);
    const variantCells = c.formulas[rec.variant];
    if (!variantCells) { problems.push(`${rec.measurement_id}: no formulas for variant ${rec.variant}`); continue; }
    if (want && want.length === 2 && want[0] !== rec.variant) continue;
    const names = selectCells(variantCells, want, args.probe);
    if (!names.length) continue;
    const samePolicy = records.filter((x) => x.policy === rec.policy).length > 1;
    const prefix = records.length === 1 ? '' : samePolicy ? `${rec.policy}/${rec.case_id}/` : `${rec.policy}/`;
    const { ctx, ignored } = buildCtx(c, prediction, rec, problems, rec.measurement_id);
    const evaluated = names.map((name) => {
      const key = prefix + name;
      const { ev, rows } = evalCell(key, variantCells[name], ctx, problems);
      return { key, name, cell: variantCells[name], ev, rows, unavailable: unavailableReasons(ev, rows, rec) };
    });
    reconcile(rec.measurement_id, rec, prediction, evaluated, problems);
    const g = billGroup(rec.measurement_id, evaluated, preset, problems);
    const groupKey = prefix ? prefix.replace(/\/$/, '') : rec.variant;
    groups[groupKey] = { measurement_id: rec.measurement_id, cells: evaluated.map((x) => x.key), lines: g.lines, total: g.total, provisioning: g.provisioning, allocation: g.allocation };
    const pick = (m) => Object.fromEntries([...m].map(([k, v]) => [k, v && typeof v === 'object' && 'value' in v ? v.value : v]));
    for (const x of evaluated) {
      const unavailable = x.unavailable.length > 0;
      const mine = unavailable || !g.lines ? null : g.allocation.map((a) => {
        const line = g.lines.find((l) => l.rule_id === a.rule_id && l.scope === a.scope);
        const share = a.shares[x.key];
        return { rule_id: a.rule_id, scope: a.scope, unit: line.unit, quantity: line.quantity * share.fraction, money: share.money,
          share: share.fraction, aggregate_quantity: line.quantity, aggregate_money: a.money, basis: a.basis, note: line.note };
      }).filter((l) => l.share > 0);
      cells[x.key] = {
        group: groupKey, measurement_id: rec.measurement_id, state: unavailable ? 'unavailable' : 'ok', unavailable_reasons: x.unavailable,
        stage1_inputs: { inputs: pick(ctx.inputs), coefs: pick(ctx.coefs), observations: rec.observations || {}, prediction_parameters: ignored },
        stage2_rows: x.rows,
        stage3_ledger_summary: { by_row: summarize(x.ev.rows), states: x.ev.states, extrapolated: x.ev.extrapolated, allocation: x.ev.allocation, notes: x.ev.notes.map((n) => n.value) },
        stage4_billing_lines: mine,
        billing_total: mine ? mine.reduce((s, l) => s + l.money, 0) : null,
        stage5_allocation: unavailable ? null : { group: groupKey, provisioning: g.provisioning },
        evidence: evidenceOf(rec),
      };
      console.log(printCell(x.key, cells[x.key]));
    }
    console.log(printGroup(groupKey, groups[groupKey]));
  }
  if (args.cell && !Object.keys(cells).length && !problems.length) problems.push(`--cell ${args.cell} selected no cell across ${records.length} record(s)`);
  const result = { probe: args.probe, measurement_id: records.map((r) => r.measurement_id).join(','), contract: c.manifest, cells, groups, mismatches: problems };
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
