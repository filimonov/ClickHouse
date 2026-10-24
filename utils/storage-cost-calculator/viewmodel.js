// View model: turns a pipeline Result into render-ready text and structure for ui.js (spec §4.5, §7).
// Display only: the numbers come from the Result; this file formats them and computes display fractions.
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const SCC = root.SCC || (root.SCC = {});
  const model = () => root.SCC.model || require('./model.js');
  const presets = () => root.SCC.presets || require('./presets.js');
  const pipeline = () => root.SCC.pipeline || require('./pipeline.js');

  const VARIANT_LABELS = { ebs: 'EBS', s3: 'S3', s3plain: 'S3 plain rewritable', cas: 'CAS' };
  const OPERATION_LABELS = {
    storage: 'Storage', inserts: 'Inserts', merges: 'Merges', mutations: 'Mutations', deletes: 'Deletes',
    reads: 'Reads', recovery: 'Recovery', replacement: 'Replica replacement', local_disk: 'Local disk',
    gc: 'Garbage collection', lease: 'Mount lease',
  };
  // Advanced block order follows spec §4.2; `simple` is always visible.
  const BLOCKS = [
    ['simple', 'Inputs'], ['schema', 'Schema'], ['inserts', 'Inserts'], ['merges', 'Merges'], ['replication', 'Replication'],
    ['mutations_deletes', 'Mutations and deletes'], ['reads_cache', 'Reads and cache'], ['topology_recovery', 'Topology and recovery'],
    ['ebs', 'EBS'], ['local_disk', 'Local disk'], ['cas', 'CAS'], ['reliability', 'Reliability'], ['prices', 'Prices'],
  ];
  // Card and cell wording per variant state. `unavailable` is rendered like `not_supported`: no total, no delta.
  const STATE_TEXT = {
    not_supported: 'not supported for this workload',
    unavailable: 'not available for this workload',
    backlog_grows: 'backlog grows; steady-state monthly cost unavailable',
    unsupported_cut: 'not modelled yet: a required measurement is missing',
  };
  const CELL_TEXT = { not_supported: 'not supported', unavailable: 'not available', backlog_grows: 'backlog grows', unsupported_cut: 'unsupported' };
  const PAGE_STATES = ['ok', 'prices_missing', 'cannot_reproduce'];
  const EXTRAPOLATED = 'extrapolated';

  function stateText(state) {
    if (state === 'ok') throw new Error('stateText: ok has no state text');
    if (!Object.prototype.hasOwnProperty.call(STATE_TEXT, state)) throw new Error(`unknown variant state ${JSON.stringify(state)}`);
    return STATE_TEXT[state];
  }

  // Short state text for table cells, chart points and deltas.
  function cellText(state) {
    if (!Object.prototype.hasOwnProperty.call(CELL_TEXT, state)) throw new Error(`unknown variant state ${JSON.stringify(state)}`);
    return CELL_TEXT[state];
  }

  function variantLabel(v) {
    if (!Object.prototype.hasOwnProperty.call(VARIANT_LABELS, v)) throw new Error(`unknown variant ${JSON.stringify(v)}`);
    return VARIANT_LABELS[v];
  }

  // ---- formatting -----------------------------------------------------------------------------

  const fmtCache = new Map();
  function nf(key, opts) {
    if (!fmtCache.has(key)) fmtCache.set(key, new Intl.NumberFormat('en-US', opts));
    return fmtCache.get(key);
  }

  // Money is rounded here and nowhere else (spec §4.4). A non-zero amount below one cent is not shown as zero.
  // `sign: true` prefixes + or − for deltas.
  function fmtMoney(value, currency, opts) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`fmtMoney: expected a finite number, got ${JSON.stringify(value)}`);
    if (typeof currency !== 'string' || !currency) throw new Error(`fmtMoney: price preset has no currency (${JSON.stringify(currency)})`);
    const sign = !!(opts && opts.sign);
    const f = nf(`money:${currency}:${sign}`, { style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 2, signDisplay: sign ? 'exceptZero' : 'auto' });
    const abs = Math.abs(value);
    if (abs > 0 && abs < 0.005) {
      const cent = nf(`money:${currency}:false`, { style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(0.01);
      return `${value < 0 ? '−' : sign ? '+' : ''}< ${cent}`;
    }
    return f.format(value).replace('-', '−');
  }

  // Thousands separators; whole numbers above 1,000, two decimals above 1, six significant-ish below. The unit is
  // kept as the contract states it; dimensionless units ("1", "count") are omitted.
  function fmtQuantity(value, unit) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`fmtQuantity: expected a finite number, got ${JSON.stringify(value)}`);
    const abs = Math.abs(value);
    const digits = abs >= 1000 ? 0 : abs >= 1 ? 2 : 6;
    const s = nf(`q:${digits}`, { maximumFractionDigits: digits }).format(value).replace('-', '−');
    return unit && unit !== '1' && unit !== 'count' ? `${s} ${unit}` : s;
  }

  // Unit prices keep up to six decimals ($0.0004 per 1,000 requests); totals use fmtMoney.
  function fmtPrice(value, currency) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`fmtPrice: expected a finite number, got ${JSON.stringify(value)}`);
    if (typeof currency !== 'string' || !currency) throw new Error(`fmtPrice: price preset has no currency (${JSON.stringify(currency)})`);
    return nf(`price:${currency}`, { style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 6 }).format(value);
  }

  const withExtrapolated = (text, ex) => (ex ? `${text} (${EXTRAPOLATED})` : text);

  // Words for the reader. Pipeline reasons name ids and contract terms; the page says what they mean.
  const UNIT_CLASS_LABELS = { bytes: 'Stored data', put: 'Write requests', get: 'Read requests', transfer: 'Transfer', block: 'Block storage' };
  const OP_KIND_LABELS = {
    stored: 'Stored data', capacity: 'Volume capacity', iops: 'IOPS', mibps: 'Throughput', interserver_from_target: 'Replica transfer',
    PutObject: 'PUT requests', GetObject: 'GET requests', HeadObject: 'HEAD requests', DeleteObjects: 'Delete requests', ListObjectsV2: 'LIST requests',
    CopyObject: 'COPY requests', CreateMultipartUpload: 'Multipart starts', UploadPart: 'Multipart parts', CompleteMultipartUpload: 'Multipart completions',
  };
  const UNIT_WORDS = { mibps: 'MiB/s', iops: 'IOPS', count: '', 1: '' };
  const REASONS = [
    [/no operation has a supported family in this contract/, 'No measured model for this column yet'],
    [/^price preset \S+ has no prices \(structure only\)$/, 'This price list has no prices yet; quantities are shown without money'],
  ];
  // One reason per distinct cause; the "<variant>.<operation>: " prefix is dropped because the card names the column.
  function plainReason(text) {
    if (!text) return text;
    const parts = String(text).split('; ').map((t) => {
      const hit = REASONS.find(([re]) => re.test(t));
      return hit ? hit[1] : t.replace(/^[a-z0-9_]+(\.[a-z0-9_]+)?: /, '');
    });
    return [...new Set(parts)].join('; ');
  }

  // Unit class of a billing line, shared by card bars and composition bars (styles.css --cls-*).
  function unitClassOf(ruleId, preset) {
    if (ruleId === 'storage') return 'bytes';
    if (ruleId === 'block_capacity' || ruleId === 'block_iops' || ruleId === 'block_mibps') return 'block';
    if ((preset.transfer || []).some((t) => t.id === ruleId)) return 'transfer';
    const rc = (preset.request_classes || []).find((c) => c.id === ruleId);
    // AWS bills PUT, COPY, POST and LIST in one class and "GET and all other requests" in another.
    if (rc) return rc.ops.includes('PutObject') ? 'put' : 'get';
    throw new Error(`billing line ${ruleId} is not a rule of price preset ${preset.id}`);
  }

  // Op kinds of the rows that feed a billing line.
  function opKindsOf(ruleId, rows, preset) {
    const pick = (f) => [...new Set(rows.filter(f).map((r) => r.op_kind))];
    if (ruleId === 'storage') return pick((r) => r.row_type === 'storage');
    if (ruleId.startsWith('block_')) return pick((r) => r.row_type === 'block_io' || r.row_type === 'block_capacity');
    if ((preset.transfer || []).some((t) => t.id === ruleId)) return pick((r) => r.row_type === 'transfer' && r.route === ruleId);
    const rc = preset.request_classes.find((c) => c.id === ruleId);
    return pick((r) => r.row_type === 'request' && rc && rc.ops.includes(r.op_kind));
  }

  // ---- formula and trace walking ----------------------------------------------------------------

  // Walks a formula node and its evaluation trace together. A cut term the pipeline zeroed has a `const` trace
  // where the formula has a subtree; the walk stops there.
  function zip(node, trace, fn) {
    if (!node || !trace || typeof node !== 'object') return;
    if (node.kind !== trace.kind) { fn(node, trace, true); return; }
    fn(node, trace, false);
    const kids = trace.children || [];
    switch (node.kind) {
      case 'add': case 'sub': case 'mul': case 'div': zip(node.a, kids[0], fn); zip(node.b, kids[1], fn); break;
      case 'min': case 'max': node.args.forEach((a, i) => zip(a, kids[i], fn)); break;
      case 'ceil_to': case 'floor_to': zip(node.x, kids[0], fn); zip(node.step, kids[1], fn); break;
      case 'lookup': node.keys.forEach((k, i) => zip(k, kids[i], fn)); break;
      case 'price': zip(node.quantity, kids[0], fn); break;
      case 'branch':
        zip(node.cond.a, kids[0], fn);
        zip(node.cond.b, kids[1], fn);
        if (trace.branch_taken) zip(node[trace.branch_taken], kids[2], fn);
        break;
      case 'sum_over': kids.forEach((k) => zip(node.body, k, fn)); break;
      default: break;
    }
  }

  // ---- cards, table ---------------------------------------------------------------------------------

  function barSegments(vr, preset) {
    if (vr.state !== 'ok' || !vr.lines || !(vr.total > 0)) return [];
    const by = new Map();
    for (const l of vr.lines) {
      const cls = unitClassOf(l.rule_id, preset);
      by.set(cls, (by.get(cls) || 0) + l.money);
    }
    return [...by].filter(([, m]) => m > 0).map(([cls, m]) => ({ class: cls, money: m, money_text: fmtMoney(m, preset.currency), label: UNIT_CLASS_LABELS[cls], fraction: m / vr.total }));
  }

  function deltaTexts(result, v, currency) {
    const out = [];
    for (const [key, ref] of [['vs_ebs', 'ebs'], ['vs_s3', 's3']]) {
      if (v === ref || !result.variants[ref]) continue;
      const d = (result.deltas[v] || {})[key];
      const name = `vs ${variantLabel(ref)}`;
      // The flag travels beside the text for the page's badge; a delta is extrapolated when either total is.
      if (d) out.push({ text: `${name}: ${fmtMoney(d.value, currency, { sign: true })}`, extrapolated: !!d.extrapolated });
      else out.push({ text: `${name}: not comparable (${variantLabel(ref)} ${result.variants[ref].state === 'ok' ? 'has no total' : cellText(result.variants[ref].state)})`, extrapolated: false });
    }
    return out;
  }

  function buildCards(result, contract, preset, pageState) {
    return contract.variants.map((v) => {
      const vr = result.variants[v];
      if (!vr) throw new Error(`result has no variant ${v}`);
      const cheapest = result.cheapest === v;
      if (cheapest && vr.state !== 'ok') throw new Error(`result marks ${v} cheapest in state ${vr.state}`);
      const card = { variant: v, label: variantLabel(v), state: vr.state, reason: plainReason(vr.reason), extrapolated: !!vr.extrapolated, cheapest, total_text: null, state_text: null, delta_texts: [], deltas: [], bar_segments: [] };
      if (vr.state !== 'ok') {
        card.state_text = stateText(vr.state);
        return card;
      }
      if (pageState === 'prices_missing') {
        // The page banner says why; the card only says what it shows.
        card.state_text = 'quantities only, no prices';
        return card;
      }
      card.total_text = fmtMoney(vr.total, preset.currency);
      card.state_text = cheapest ? 'cheapest' : 'comparable';
      card.deltas = deltaTexts(result, v, preset.currency);
      card.delta_texts = card.deltas.map((d) => d.text);
      card.bar_segments = barSegments(vr, preset);
      return card;
    });
  }

  function rowUnits(contract, v, op) {
    const cell = (contract.formulas[v] || {})[op];
    return cell ? cell.ledger_rows.map((r) => r.expr.dim) : [];
  }

  // "Replica transfer 1,217 GiB; Throughput 125 MiB/s". An op kind without a label is reported and shown as is.
  function quantitySummary(cell, units, problems) {
    return cell.quantities.map((q, i) => {
      if (!OP_KIND_LABELS[q.op_kind] && !problems.includes(`op kind ${q.op_kind} has no label in viewmodel.js`)) problems.push(`op kind ${q.op_kind} has no label in viewmodel.js`);
      const label = OP_KIND_LABELS[q.op_kind] || q.op_kind;
      const unit = Object.prototype.hasOwnProperty.call(UNIT_WORDS, units[i]) ? UNIT_WORDS[units[i]] : units[i];
      // "IOPS 3,000", not "IOPS 3,000 IOPS": a unit that repeats the label is dropped.
      const u = unit === label ? '' : unit;
      return q.state === 'ok' ? `${label} ${fmtQuantity(q.quantity, u)}` : `${label} not available`;
    }).join('; ');
  }

  function operationShown(contract, op) {
    return contract.variants.some((v) => (contract.formulas[v] || {})[op]
      || contract.families.some((f) => (((contract.applicability[v] || {})[op] || {})[f] || 'not_applicable') !== 'not_applicable'));
  }

  function buildTable(result, contract, preset, pageState, problems) {
    const rows = [];
    for (const op of contract.operations) {
      if (!operationShown(contract, op)) continue;
      if (!OPERATION_LABELS[op]) problems.push(`operation ${op} has no label in viewmodel.js`);
      const cells = contract.variants.map((v) => {
        const vr = result.variants[v];
        const c = vr.cells[op];
        if (!c) return { variant: v, operation: op, text: 'not applicable', state: 'not_applicable', extrapolated: false, clickable: false };
        if (vr.state !== 'ok') return { variant: v, operation: op, text: cellText(vr.state), state: vr.state, extrapolated: !!c.extrapolated, clickable: false };
        const text = pageState === 'prices_missing' ? quantitySummary(c, rowUnits(contract, v, op), problems) : fmtMoney(c.money, preset.currency);
        return { variant: v, operation: op, text, state: 'ok', extrapolated: !!c.extrapolated, clickable: true };
      });
      rows.push({ operation: op, label: OPERATION_LABELS[op] || op, cells });
    }
    return { columns: contract.variants.map((v) => ({ variant: v, label: variantLabel(v), cheapest: result.cheapest === v })), rows };
  }

  // ---- drill ------------------------------------------------------------------------------------

  const rowLabel = (r) => `${r.row_type} ${r.op_kind} ${r.outcome_class}${r.caller ? ` ${r.caller}` : ''}`;

  function buildDrill(result, contract, preset, pageState, open, problems) {
    const { variant: v, operation: op } = open;
    const vr = result.variants[v];
    const cell = vr && vr.cells[op];
    const formula = (contract.formulas[v] || {})[op];
    if (!vr || !cell || !formula) { problems.push(`drill: ${v}.${op} has no cell`); return null; }
    if (vr.state !== 'ok') { problems.push(`drill: ${v}.${op} cannot be opened: ${cellText(vr.state)}`); return null; }
    const R = model();
    const byId = new Map(contract.registry.map((q) => [q.id, q]));
    const coefById = new Map(contract.coefficients.map((c) => [c.id, c]));
    const used = new Map();
    const coefs = new Map();
    const lookups = new Map();
    const sources = [];
    const addSource = (s) => { if (!sources.some((x) => x.kind === s.kind && x.ref === s.ref)) sources.push(s); };
    const branchLines = [];
    const ledger = [];
    cell.rows.forEach((row, i) => {
      const fr = formula.ledger_rows[i];
      const conds = [];
      zip(fr.expr, cell.trace[i], (n, t, zeroed) => {
        if (zeroed) { conds.push({ text: `${R.render(n, contract.registry)}: cut term, zero for this workload`, taken: null }); return; }
        if (n.kind === 'input') used.set(n.id, t.value);
        if (n.kind === 'coef') coefs.set(n.id, t.value);
        if (n.kind === 'lookup') lookups.set(`${n.table_id}[${t.lookup_key}]`, { table_id: n.table_id, key: t.lookup_key, value: t.value });
        if (n.kind === 'branch') {
          const cond = `${R.render(n.cond.a, contract.registry)} ${n.cond.op} ${R.render(n.cond.b, contract.registry)}`;
          conds.push({ text: t.branch_taken ? `${cond}: ${t.branch_taken === 'then' ? 'true' : 'false'}, ${t.branch_taken} taken` : `${cond}: unavailable`, taken: t.branch_taken || null });
        }
      });
      const label = rowLabel(row);
      const branchTaken = conds.map((c) => c.taken).filter(Boolean);
      branchLines.push({ row: label, branch_taken: branchTaken, conditions: conds, formula: R.render(fr.expr, contract.registry),
        text: `${label}: ${conds.length ? conds.map((c) => c.text).join('; ') : 'no branch'}` });
      const unit = fr.expr.dim;
      const q = row.state === 'ok' ? fmtQuantity(row.quantity, unit) : 'not available';
      ledger.push({ row: label, basis: fr.basis || null, quantity: row.state === 'ok' ? row.quantity : null, unit, state: row.state, extrapolated: !!row.trace && !!row.trace.extrapolated,
        text: withExtrapolated(`${label}: ${q} (${fr.basis || 'no basis'})`, row.trace && row.trace.extrapolated) });
      for (const ref of fr.source_refs || []) addSource({ kind: 'prediction', ref: `${ref.path}:${ref.line}`, claim: ref.claim });
    });
    const inputLines = [];
    for (const [id, value] of used) {
      const q = byId.get(id);
      const name = q && q.label && q.label !== id ? `${q.label} (${id})` : id;
      const text = `${name} = ${value === null || value === undefined ? 'not available' : typeof value === 'number' ? fmtQuantity(value, q && q.unit) : value}`;
      inputLines.push({ id, label: q ? q.label : id, value, unit: q ? q.unit : null, kind: q ? q.kind : 'unknown', evidence_kind: q && q.evidence ? q.evidence.kind : null, text });
      if (!q) problems.push(`drill: input ${id} is not in the registry`);
      else if (q.evidence && q.evidence.ref) addSource({ kind: q.evidence.kind, ref: q.evidence.ref, version: q.evidence.version || null, id });
    }
    for (const [id, value] of coefs) {
      const c = coefById.get(id);
      const validity = c && c.validity ? Object.entries(c.validity).map(([p, r]) => `${p} in [${r[0]}, ${r[1]}]`).join(', ') : null;
      inputLines.push({ id, label: id, value, unit: c ? c.dim : null, kind: 'coef', evidence_kind: c ? c.evidence.kind : null, validity: c ? c.validity || null : null,
        text: `${id} = ${typeof value === 'number' ? fmtQuantity(value, c && c.dim) : 'not available'}${validity ? `; measured over ${validity}` : ''}` });
      if (!c) problems.push(`drill: coefficient ${id} is not in the contract`);
      else addSource({ kind: c.evidence.kind, ref: c.evidence.ref, version: c.evidence.version || null, id });
    }
    for (const [k, l] of lookups) {
      inputLines.push({ id: l.table_id, label: k, value: l.value, unit: null, kind: 'lookup', text: `${k} = ${typeof l.value === 'number' ? fmtQuantity(l.value) : 'not available'}` });
      const c = coefById.get(l.table_id);
      if (c) addSource({ kind: c.evidence.kind, ref: c.evidence.ref, version: c.evidence.version || null, id: l.table_id });
    }
    let billingLines = [];
    let allocationLines = [];
    let composition = [];
    const priced = pageState === 'ok';
    if (priced) {
      const cur = preset.currency;
      billingLines = cell.lines.map((l) => ({
        rule_id: l.rule_id, scope: l.scope, unit: l.unit, quantity: l.quantity, money: l.money, share: l.share, aggregate_quantity: l.aggregate_quantity, aggregate_money: l.aggregate_money, note: l.note,
        label: presets().ruleLabel(l.rule_id, preset), share_text: `${fmtQuantity(l.share * 100)}%`,
        text: `${presets().ruleLabel(l.rule_id, preset)}: ${fmtQuantity(l.quantity, l.unit)} = ${fmtMoney(l.money, cur)} (share ${fmtQuantity(l.share * 100)}% of ${fmtQuantity(l.aggregate_quantity, l.unit)} = ${fmtMoney(l.aggregate_money, cur)})`,
      }));
      for (const l of cell.lines) {
        const src = (vr.lines || []).find((x) => x.rule_id === l.rule_id && x.scope === l.scope);
        if (src && src.source_url) addSource({ kind: 'billing', ref: src.source_url, rule_id: l.rule_id });
      }
      allocationLines = (vr.allocation || []).filter((a) => a.shares[op] && a.shares[op].fraction > 0).map((a) => {
        const s = a.shares[op];
        return { rule_id: a.rule_id, scope: a.scope, basis: a.basis, weight: s.weight, fraction: s.fraction, money: s.money,
          label: presets().ruleLabel(a.rule_id, preset),
          text: `${presets().ruleLabel(a.rule_id, preset)}: ${fmtQuantity(s.fraction * 100)}% of ${fmtMoney(a.money, cur)} by ${a.basis} = ${fmtMoney(s.money, cur)}` };
      });
      if (vr.provisioning && cell.rows.some((r) => r.row_type === 'block_io' || r.row_type === 'block_capacity')) {
        for (const n of vr.notes.filter((x) => x.kind === 'allocation' || x.kind === 'instance_class')) allocationLines.push({ kind: n.kind, text: n.text });
      }
      composition = cell.lines.map((l) => ({
        rule_id: l.rule_id, op_kind: opKindsOf(l.rule_id, cell.rows, preset).join(', '), unit_class: unitClassOf(l.rule_id, preset), label: UNIT_CLASS_LABELS[unitClassOf(l.rule_id, preset)], money: l.money,
        money_text: fmtMoney(l.money, cur), fraction: cell.money > 0 ? l.money / cell.money : null,
      }));
    }
    const unpriced = priced ? null : `price preset ${preset.id} provides no values`;
    return {
      variant: v, operation: op, label: `${variantLabel(v)} · ${OPERATION_LABELS[op] || op}`, extrapolated: !!cell.extrapolated,
      composition,
      stages: [
        { name: 'Inputs used', lines: inputLines },
        { name: 'Branch taken per row', lines: branchLines },
        { name: 'Ledger rows', lines: ledger },
        { name: 'Billing lines', lines: billingLines, note: unpriced },
        { name: 'Allocation', lines: allocationLines, note: unpriced },
      ],
      sources,
    };
  }

  // ---- inputs -----------------------------------------------------------------------------------

  function valueText(value, q) {
    if (value === null || value === undefined) return 'not available';
    return typeof value === 'number' ? fmtQuantity(value, q.unit) : String(value);
  }

  // Rounded for an input box: six significant digits keep 1.66667 GiB/hour readable; the exact value stays in `value`.
  // Whole numbers are exact and stay as they are.
  const roundDisplay = (v) => (typeof v === 'number' && Number.isFinite(v) && !Number.isInteger(v) ? Number(v.toPrecision(6)) : v);

  // "measured for 1 to 6" for an input's own range; a validity map names each parameter by its registry label.
  function rangeText(f, byId) {
    if (f.display_range && f.display_range[0] === f.display_range[1]) return `measured at ${fmtQuantity(f.display_range[0], f.display_unit)} only`;
    if (f.display_range) return `measured for ${fmtQuantity(f.display_range[0])} to ${fmtQuantity(f.display_range[1], f.display_unit)}`;
    if (!f.validity) return null;
    return Object.entries(f.validity).map(([p, r]) => {
      const q = byId.get(p);
      return `measured for ${q && q.label ? `${q.label.toLowerCase()} ` : ''}${fmtQuantity(r[0])} to ${fmtQuantity(r[1], q && q.unit)}`;
    }).join('; ');
  }

  function buildInputs(contract, fields, lockOrder, preset, ui, pageState, problems) {
    const R = model();
    const byId = new Map(contract.registry.map((q) => [q.id, q]));
    // Rates are stored per day; `ui.units` only changes how they are shown and entered.
    const per = ui.units || 'day';
    const shown = (q, v) => (typeof v === 'number' && typeof q.unit === 'string' && q.unit.endsWith('/day') ? pipeline().denormaliseRate(v, per) : v);
    // "ingest_rate × retention_days" read as "ingest rate × retention": registry ids become their labels.
    const ids = contract.registry.filter((q) => q.label).map((q) => q.id).sort((a, b) => b.length - a.length);
    const worded = (text) => (text ? ids.reduce((t, id) => t.replace(new RegExp(`\\b${id}\\b`, 'g'), byId.get(id).label.toLowerCase()), text) : text);
    const coefById = new Map(contract.coefficients.map((c) => [c.id, c]));
    const blocks = new Map(BLOCKS.map(([id, label]) => [id, { block: id, label, advanced: id !== 'simple', fields: [], hints: [] }]));
    for (const q of contract.registry) {
      // Probe observations (measured rows without a coefficient) are fed by measurement records, not by the page.
      if (q.kind === 'measured' && !q.coef) continue;
      if (!blocks.has(q.block)) {
        problems.push(`registry block ${q.block} (first row ${q.id}) has no place on the page; shown last under its id`);
        blocks.set(q.block, { block: q.block, label: q.block, advanced: true, fields: [], hints: [] });
      }
      const b = blocks.get(q.block);
      // A missing label is a contract defect: the page shows no raw id in its place.
      if (!q.label) problems.push(`registry ${q.id} has no label`);
      const f = { id: q.id, label: q.label || null, unit: q.unit || null, kind: q.kind, value_type: q.value_type, enum: q.enum || null, evidence_kind: q.evidence ? q.evidence.kind : null,
        rate: typeof q.unit === 'string' && q.unit.endsWith('/day'), editable: q.kind === 'input', locked: null, derived: false, overridden: false, out_of_range: false, range: null, validity: null, rule_text: null, hint: q.hint || null, value: null };
      if (q.kind === 'input') {
        const fl = fields[q.id];
        if (!fl) problems.push(`input ${q.id} is missing from the resolved fields`);
        else Object.assign(f, { value: fl.value, locked: fl.locked, derived: fl.derived, overridden: fl.overridden, out_of_range: fl.out_of_range, range: fl.validity, rule_text: fl.rule_text });
        // Under cannot_reproduce only what the user entered is shown; a derived value is a model result.
        if (pageState === 'cannot_reproduce' && f.derived) f.value = null;
      } else if (q.kind === 'pinned') {
        f.value = q.default;
      } else if (q.kind === 'derivation') {
        f.derived = true;
        f.rule_text = q.rule ? R.render(q.rule, contract.registry) : null;
        f.validity = q.validity || null;
      } else if (q.kind === 'measured') {
        const c = coefById.get(q.coef);
        if (!c) problems.push(`registry ${q.id}: coefficient ${q.coef} is not in the contract`);
        else {
          f.value = c.kind === 'table' ? null : c.value;
          f.validity = c.validity || null;
          if (c.kind === 'table') f.value_text = `table of ${Object.keys(c.value.values).length} entries by ${c.value.index.join(', ')}`;
        }
      }
      if (!f.value_text) f.value_text = f.kind === 'derivation' ? `derived: ${f.rule_text}` : valueText(f.value, q);
      f.display_unit = f.rate ? q.unit.replace(/\/day$/, `/${per}`) : f.unit;
      f.display_value = roundDisplay(shown(q, f.value));
      f.display_range = f.range ? f.range.map((x) => roundDisplay(shown(q, x))) : null;
      if (f.rule_text) f.derived_text = `Derived from ${worded(f.rule_text)}`;
      if (f.kind === 'derivation') f.value_text = f.derived_text;
      // Slider bounds in the shown unit: the measured range, else the sweep ladder's span, else no slider.
      const ladder = presets().SWEEP_INPUTS[q.id] ? presets().ladderFor(q.id, f.value, f.range) : null;
      f.slider = f.display_range && f.display_range[1] > f.display_range[0] ? { min: f.display_range[0], max: f.display_range[1] }
        : ladder ? { min: roundDisplay(shown(q, ladder[0])), max: roundDisplay(shown(q, ladder[ladder.length - 1])) } : null;
      // Counts move in whole steps; a range of one value is fixed, not a control.
      f.step = q.unit === 'count' ? 1 : 'any';
      f.fixed = !!f.range && f.range[0] === f.range[1];
      f.range_text = rangeText(f, byId);
      b.fields.push(f);
      if (q.hint && q.hint.sql) {
        const h = b.hints.find((x) => x.sql === q.hint.sql);
        if (h) Object.assign(h.mapping, q.hint.mapping || {});
        else b.hints.push({ sql: q.hint.sql, mapping: Object.assign({}, q.hint.mapping || {}) });
      }
    }
    const prices = blocks.get('prices');
    if (preset && pageState !== 'cannot_reproduce') {
      const editable = !!ui.priceEditing && preset.id === 'custom';
      prices.as_of = preset.as_of || null;
      prices.note = preset.structure_only ? 'values not provided' : null;
      for (const p of presets().priceFields(preset)) {
        prices.fields.push({ id: `price:${p.path}`, path: p.path, label: p.label, value: p.value, unit: p.unit, per: p.per, kind: 'price', editable, as_of: preset.as_of, source_url: p.source_url,
          locked: null, derived: false, overridden: false, out_of_range: false, range: null, validity: null, hint: null, value_text: p.value === null ? 'not priced by this preset' : `${fmtPrice(p.value, preset.currency)} ${p.per}` });
      }
    }
    const groups = contract.precedence.map((g) => ({
      members: g.group.slice(),
      locked: lockOrder.filter((id) => g.group.includes(id)),
      derived: g.group.filter((id) => fields[id] && fields[id].derived),
      // The only group today is data = ingest × retention, a steady-state assumption the page labels (spec §4.2).
      label: g.group.includes('data_bytes') ? 'data = ingest × retention (steady-state assumption)' : g.group.join(' / '),
    }));
    return { blocks: [...blocks.values()].filter((b) => b.fields.length || b.block === 'prices' || b.block === 'simple'), groups, lock_order: lockOrder.slice() };
  }

  // ---- notes, chart -------------------------------------------------------------------------------

  function buildNotes(result, contract, fields, pageState) {
    const notes = [];
    // prices_missing is said by the page banner and a column state by its card; neither repeats here.
    if (result.page_reason && pageState === 'cannot_reproduce') notes.push({ kind: pageState, text: result.page_reason });
    for (const p of result.problems || []) notes.push({ kind: 'problem', text: p });
    if (pageState === 'cannot_reproduce') return notes;
    for (const v of contract.variants) {
      const vr = result.variants[v];
      if (vr.state !== 'ok') continue;
      for (const n of vr.notes) {
        if (n.kind === 'cell') notes.push({ kind: 'cell', variant: v, operation: n.operation, text: `${variantLabel(v)} ${n.operation}: ${n.text} = ${typeof n.value === 'number' ? fmtQuantity(n.value) : 'not available'}` });
        else notes.push({ kind: n.kind, variant: v, text: `${variantLabel(v)}: ${n.text}`, details: n.notes || null });
      }
    }
    // One note per parameter: coefficients may bound it with different ranges, so the note lists each violated owner with its range.
    const byParam = new Map();
    // A coefficient no cell reads cannot extrapolate anything, so it raises no note; a registry row follows its coefficient.
    const reached = presets().reachedCoefficients(contract);
    const coefIds = new Set(contract.coefficients.map((c) => c.id));
    for (const d of presets().validityDomains(contract)) {
      if (coefIds.has(d.id) && !reached.has(d.id)) continue;
      if (!byParam.has(d.param)) byParam.set(d.param, []);
      byParam.get(d.param).push(d);
    }
    for (const [param, domains] of byParam) {
      const x = fields[param] ? fields[param].value : undefined;
      if (typeof x !== 'number') continue;
      const hit = domains.filter((d) => x < d.range[0] || x > d.range[1]);
      if (!hit.length) continue;
      const fmtRange = (r) => `[${fmtQuantity(r[0])}, ${fmtQuantity(r[1])}]`;
      const same = hit.every((d) => d.range[0] === hit[0].range[0] && d.range[1] === hit[0].range[1]);
      const where = same
        ? `${fmtRange(hit[0].range)} of ${hit.map((d) => d.owner).join(', ')}`
        : hit.map((d) => `${d.owner} ${fmtRange(d.range)}`).join(', ');
      notes.push({ kind: 'out_of_range', param, range: same ? hit[0].range : null, ranges: hit.map((d) => ({ owner: d.owner, range: d.range })), owners: hit.map((d) => d.owner),
        text: `${param} = ${fmtQuantity(x)} is outside the measured range ${same ? '' : 'of '}${where}; dependent numbers are ${EXTRAPOLATED}` });
    }
    return notes;
  }

  // Four or five round money ticks from zero to the first round value at or above the largest total.
  function yTicks(points, currency) {
    const top = Math.max(0, ...points.filter((p) => p.y !== null).map((p) => p.y));
    if (!(top > 0)) return [];
    const raw = top / 4;
    const mag = 10 ** Math.floor(Math.log10(raw));
    const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((x) => x >= raw);
    const n = Math.ceil(top / step - 1e-9);
    // Whole-currency ticks once the step is a unit or more: "$4,000", not "$4,000.00".
    const whole = step >= 1 ? nf(`tick:${currency}`, { style: 'currency', currency, maximumFractionDigits: 0 }) : null;
    return Array.from({ length: n + 1 }, (_, i) => ({ value: i * step, text: whole ? whole.format(i * step) : fmtMoney(i * step, currency) }));
  }

  function buildChart(result, contract, preset, ui, problems, fields) {
    const s = result.sweep;
    if (!s) return null;
    if (ui.sweepInput && ui.sweepInput !== s.input) problems.push(`chart: the sweep is over ${s.input}, the page asked for ${ui.sweepInput}`);
    const q = contract.registry.find((x) => x.id === s.input);
    if (!q) throw new Error(`sweep input ${s.input} is not in the registry`);
    const points = [];
    for (const p of s.points) {
      for (const v of contract.variants) {
        const t = p.totals[v];
        const y = t.state === 'ok' && t.value !== null ? t.value : null;
        points.push({ variant: v, x: p.x, y, state: t.state, extrapolated: !!t.extrapolated && y !== null,
          label: y === null ? `${variantLabel(v)} at ${fmtQuantity(p.x, q.unit)}: ${cellText(t.state)}` : withExtrapolated(`${variantLabel(v)} at ${fmtQuantity(p.x, q.unit)}: ${fmtMoney(y, preset.currency)}`, t.extrapolated) });
      }
    }
    const breakEven = s.breakEven.map((b) => Object.assign({}, b, {
      text: withExtrapolated(`${variantLabel(b.a)} and ${variantLabel(b.b)} cross between ${fmtQuantity(b.x_between[0], q.unit)} and ${fmtQuantity(b.x_between[1], q.unit)}`, b.extrapolated),
    }));
    return {
      input: s.input, points, breakEven,
      // Where the workload is now: the ladder always contains it, so the page can mark it.
      current_x: fields[s.input] && typeof fields[s.input].value === 'number' ? fields[s.input].value : null,
      current_text: fields[s.input] && typeof fields[s.input].value === 'number' ? `Current: ${fmtQuantity(fields[s.input].value, q.unit)}` : null,
      y_ticks: yTicks(points, preset.currency), x_ticks: [...new Set(points.map((p) => p.x))].map((x) => ({ x, text: fmtQuantity(x) })),
      labels: { x: `${q.label || q.id}${q.unit ? ` (${q.unit})` : ''}`, y: `Monthly total (${preset.currency})`, series: contract.variants.map((v) => ({ variant: v, label: variantLabel(v) })) },
      extrapolated_points: points.filter((p) => p.extrapolated).map((p) => ({ variant: p.variant, x: p.x })),
    };
  }

  // ---- build ----------------------------------------------------------------------------------

  // `ui.pricePreset` is the preset object the Result was priced with (SCC.presets.resolvePrice): the Result does
  // not carry the currency or the rule metadata that formatting needs.
  function build(result, contract, fields, lockOrder, ui) {
    const u = ui || {};
    if (!PAGE_STATES.includes(result.page_state)) throw new Error(`unknown page state ${JSON.stringify(result.page_state)}`);
    const pageState = result.page_state;
    const preset = u.pricePreset || null;
    if (pageState !== 'cannot_reproduce' && (!preset || typeof preset !== 'object')) throw new Error('viewmodel.build: ui.pricePreset (the preset object the result was priced with) is required');
    const problems = [];
    const vm = {
      page_state: pageState, page_reason: pageState === 'cannot_reproduce' ? result.page_reason || null : plainReason(result.page_reason) || null, evidence_statement: result.evidence_statement,
      currency: preset && !preset.structure_only ? preset.currency : null, theme: u.theme || null,
      cards: [], table: { columns: [], rows: [] }, drill: null, chart: null, notes: [], inputs: null, problems,
    };
    vm.inputs = buildInputs(contract, fields, lockOrder || [], preset, u, pageState, problems);
    vm.notes = buildNotes(result, contract, fields, pageState);
    if (pageState === 'cannot_reproduce') return vm;
    vm.cards = buildCards(result, contract, preset, pageState);
    vm.table = buildTable(result, contract, preset, pageState, problems);
    if (u.openCell) vm.drill = buildDrill(result, contract, preset, pageState, u.openCell, problems);
    // A chart of money has nothing to draw without prices.
    if (pageState === 'ok') vm.chart = buildChart(result, contract, preset, u, problems, fields);
    return vm;
  }

  SCC.viewmodel = { build, plainReason, UNIT_CLASS_LABELS, OP_KIND_LABELS, fmtMoney, fmtPrice, fmtQuantity, cellText, unitClassOf, stateText, VARIANT_LABELS, OPERATION_LABELS, BLOCKS, STATE_TEXT };
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.viewmodel;
})(typeof window !== 'undefined' ? window : globalThis);
