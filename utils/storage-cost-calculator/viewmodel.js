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

  // What a column cannot do, per operation, for the one-sentence card reason.
  const OP_CANNOT = {
    storage: 'store parts', inserts: 'insert', merges: 'merge parts', mutations: 'run ALTER UPDATE/DELETE', deletes: 'drop parts',
    reads: 'serve reads', recovery: 'recover a restarted replica', replacement: 'replace a replica', local_disk: 'use the local disk',
    gc: 'collect garbage', lease: 'hold the mount lease',
  };
  const opWord = (op) => (OPERATION_LABELS[op] || op).toLowerCase();
  const joinWords = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')} and ${xs[xs.length - 1]}`);

  // One plain sentence per column state; the full reason with families and source cites is in the info dialog.
  function cardSentence(v, vr, contract, inputs) {
    if (vr.state === 'ok') return null;
    const label = variantLabel(v);
    const hit = REASONS.find(([re]) => re.test(vr.reason || ''));
    if (hit) return hit[1];
    const ops = [...new Set(reasonSegments(vr.reason).map((s) => s.operation).filter(Boolean))].filter((op) => contract.operations.includes(op));
    const value = (id) => {
      const f = inputs && inputs.blocks.flatMap((b) => b.fields).find((x) => x.id === id);
      return f && typeof f.value === 'number' ? f.value : null;
    };
    switch (vr.state) {
      case 'not_supported': {
        if (!ops.length) return `${label} is not supported for this workload`;
        const mut = ops.includes('mutations') ? value('mutations_per_month') : null;
        return `${label} cannot ${joinWords(ops.map((op) => OP_CANNOT[op] || opWord(op)))}`
          + `${mut ? `; this workload runs ${fmtQuantity(mut)} ALTER UPDATE/DELETE per month` : ''}`;
      }
      case 'unavailable': return ops.length ? `${label}: ${joinWords(ops.map(opWord))} not measured` : `${label}: not measured for this workload`;
      case 'unsupported_cut': return `${label}: a measurement this workload needs for ${ops.length ? joinWords(ops.map(opWord)) : 'one operation'} is missing`;
      case 'backlog_grows': return `${label}: ${ops.length ? joinWords(ops.map(opWord)) : 'the background work'} cannot keep up with this workload; the backlog grows`;
      default: throw new Error(`unknown variant state ${JSON.stringify(vr.state)}`);
    }
  }

  // A note with a number ends in "= value"; a text note stands alone.
  const noteText = (n) => (typeof n.value === 'number' ? `${n.text} = ${fmtQuantity(n.value)}` : n.text);

  // The cheapest badge needs two priced columns to compare; an extrapolated winner says so.
  function cheapestOf(result) {
    const priced = Object.keys(result.variants).filter((v) => result.variants[v].state === 'ok' && typeof result.variants[v].total === 'number');
    const v = result.cheapest;
    if (!v || priced.length < 2) return { variant: null, text: null, priced: priced.length };
    return { variant: v, text: result.variants[v].extrapolated ? 'cheapest (extrapolated)' : `cheapest of ${priced.length} priced`, priced: priced.length };
  }

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

  function buildCards(result, contract, preset, pageState, inputs) {
    const best = cheapestOf(result);
    return contract.variants.map((v) => {
      const vr = result.variants[v];
      if (!vr) throw new Error(`result has no variant ${v}`);
      if (result.cheapest === v && vr.state !== 'ok') throw new Error(`result marks ${v} cheapest in state ${vr.state}`);
      const cheapest = best.variant === v && pageState === 'ok';
      const card = { variant: v, label: variantLabel(v), state: vr.state, reason: cardSentence(v, vr, contract, inputs), extrapolated: !!vr.extrapolated, cheapest,
        cheapest_text: cheapest ? best.text : null, total_text: null, state_text: null, delta_texts: [], deltas: [], bar_segments: [] };
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
      card.state_text = cheapest ? best.text : null;
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

  // Inputs that drive an operation: a $0 cell whose drivers are all zero is "no <work> in this workload", not free.
  const OP_DRIVERS = {
    mutations: [['mutations_per_month', 'cas_mutations_per_s', 'mutated_compact_parts_per_month', 'mutated_wide_parts_per_month'], 'ALTER UPDATE/DELETE'],
    reads: [['select_part_reads_per_day'], 'cold reads'], recovery: [['restarts_per_month'], 'restarts'], replacement: [['replacement_rate'], 'replica replacements'],
  };
  function zeroHint(op, fields) {
    const d = OP_DRIVERS[op];
    if (!d) return null;
    const vals = d[0].filter((id) => fields && fields[id]).map((id) => fields[id].value);
    return vals.length && vals.every((x) => x === 0) ? `no ${d[1]} in this workload` : null;
  }
  // AWS prices storage per GB; no AWS page says whether that GB is 10^9 or 2^30 bytes.
  const GIB_NOTE = 'GB = binary GiB (assumption, no AWS statement found)';

  function buildTable(result, contract, preset, pageState, problems, fields) {
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
        const hint = pageState === 'ok' && c.money === 0 ? zeroHint(op, fields) : null;
        return { variant: v, operation: op, text, state: 'ok', extrapolated: !!c.extrapolated, clickable: true, hint };
      });
      rows.push({ operation: op, label: OPERATION_LABELS[op] || op, cells, note: op === 'storage' && pageState === 'ok' ? GIB_NOTE : null });
    }
    const best = cheapestOf(result);
    return { columns: contract.variants.map((v) => ({ variant: v, label: variantLabel(v), cheapest: pageState === 'ok' && best.variant === v, cheapest_text: pageState === 'ok' && best.variant === v ? best.text : null })), rows };
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
      const validity = c && model().rangeOf(c) ? Object.entries(model().rangeOf(c)).map(([p, r]) => `${p} in [${r[0]}, ${r[1]}]`).join(', ') : null;
      inputLines.push({ id, label: id, value, unit: c ? c.dim : null, kind: 'coef', evidence_kind: c ? c.evidence.kind : null, validity: c ? model().rangeOf(c) : null,
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

  // Form placement (registry `ui.tier`). Rows that are not inputs are model constants: the info dialog shows them.
  const TIERS = ['workload', 'setting', 'advanced', 'hidden'];
  function tierOf(q, problems) {
    if (q.kind !== 'input') return 'hidden';
    if (q.ui && TIERS.includes(q.ui.tier)) return q.ui.tier;
    if (q.ui) throw new Error(`registry ${q.id}: unknown ui tier ${JSON.stringify(q.ui.tier)}`);
    // A contract built before the registry had tiers: the row stays on the form, under the low-level section.
    problems.push(`registry ${q.id} has no ui tier`);
    return q.block === 'simple' ? 'workload' : 'advanced';
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
      const ui = q.ui || null;
      const tier = tierOf(q, problems);
      const f = { id: q.id, tier, name: ui ? ui.name : null, short: ui ? ui.short : null, form_label: (ui && (ui.name || ui.short)) || q.label || null,
        label: q.label || null, unit: q.unit || null, kind: q.kind, value_type: q.value_type, enum: q.enum || null, evidence_kind: q.evidence ? q.evidence.kind : null,
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
        f.validity = model().rangeOf(q);
      } else if (q.kind === 'measured') {
        const c = coefById.get(q.coef);
        if (!c) problems.push(`registry ${q.id}: coefficient ${q.coef} is not in the contract`);
        else {
          f.value = c.kind === 'table' ? null : c.value;
          f.validity = model().rangeOf(c);
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
    const untiered = problems.filter((p) => / has no ui tier$/.test(p));
    if (untiered.length > 1) {
      const ids = untiered.map((p) => p.replace(/^registry (\S+) has no ui tier$/, '$1'));
      for (const p of untiered) problems.splice(problems.indexOf(p), 1);
      problems.push(`${ids.length} registry inputs have no ui tier: ${ids.join(', ')}`);
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

  // ---- evidence -----------------------------------------------------------------------------------

  // The measured build, the reports behind the numbers and, per column, how its cells are backed.
  function buildEvidence(contract) {
    const by = contract.evidence_by_variant || null;
    return {
      statement: contract.evidence_statement || null, build: measuredBuild(contract),
      reports: ((contract.manifest || {}).evidence || []).map((e) => ({ name: e.report, href: `../../docs/superpowers/reports/${e.report}/README.md` })),
      columns: by ? contract.variants.filter((v) => by[v]).map((v) => ({ variant: v, label: variantLabel(v), fit_and_holdout: by[v].fit_and_holdout, fit_only: by[v].fit_only, assumption: by[v].assumption })) : [],
    };
  }

  // ---- info dialog --------------------------------------------------------------------------------

  // The ClickHouse build every report measured, short form; null when the reports disagree or name none.
  function measuredBuild(contract) {
    const commits = new Set(((contract.manifest || {}).evidence || []).map((e) => e.commit).filter(Boolean));
    return commits.size === 1 ? [...commits][0].slice(0, 11) : null;
  }

  // `tmp/src-26.8/src/X.cpp:12` -> `src/X.cpp:12 @ 09d0772a842`: a path the reader can open at the measured build.
  function cite(text, commit) {
    if (typeof text !== 'string') return text;
    if (!commit) return text.replace(/tmp\/src-26\.8\//g, '');
    return text.replace(/(?:tmp\/src-26\.8\/)?(src\/[A-Za-z0-9_./-]+:\d+(?:-\d+)?)(?![\d-]| @ )/g, `$1 @ ${commit}`);
  }

  // Reason segments of one variant: "<variant>.<operation>: text" pieces, split where a new prefix starts.
  function reasonSegments(reason) {
    const out = [];
    for (const part of String(reason || '').split('; ')) {
      const m = /^([a-z0-9_]+)\.([a-z0-9_]+): (.*)$/s.exec(part);
      if (m) out.push({ operation: m[2], text: m[3] });
      else if (out.length && !/^[a-z0-9_]+: /.test(part)) out[out.length - 1].text += `; ${part}`;
      else out.push({ operation: null, text: part.replace(/^[a-z0-9_]+: /, '') });
    }
    return out.filter((s) => s.text);
  }

  // A pinned value is either a rule of the provider (evidence is a provider page) or fixed in the ClickHouse source.
  function kindText(q, c) {
    if (!q) return 'measured coefficient';
    if (q.kind === 'pinned') return q.evidence && q.evidence.kind === 'provider' ? 'provider rule' : 'fixed by ClickHouse';
    return { input: 'input', measured: 'measured', derivation: 'derived' }[q.kind] || q.kind;
  }

  function defaultText(q) {
    if (q.default === null || q.default === undefined) return 'no default';
    return typeof q.default === 'number' ? fmtQuantity(q.default, q.unit) : String(q.default);
  }

  // What the (i) dialog shows. `open` is {kind: 'input', id} (any registry row or coefficient id), {kind: 'cell',
  // variant, operation} or {kind: 'variant', variant}.
  function buildInfo(result, contract, inputs, open, problems) {
    const commit = measuredBuild(contract);
    if (open.kind === 'input') {
      const q = contract.registry.find((x) => x.id === open.id);
      const c = contract.coefficients.find((x) => x.id === open.id || (q && q.coef === x.id));
      if (!q && !c) { problems.push(`info: ${open.id} is neither a registry row nor a coefficient`); return null; }
      const f = q ? inputs.blocks.flatMap((b) => b.fields).find((x) => x.id === q.id) : null;
      const ui = (q && q.ui) || {};
      const ev = (q && q.evidence) || (c && c.evidence) || {};
      const ids = (c && c.evidence && c.evidence.measurement_ids) || [];
      const evidence = ui.measured_by ? cite(ui.measured_by, commit)
        : [ev.kind ? `${ev.kind}: ${cite(ev.ref, commit)}` : null, ids.length ? `${ids.length} measurement(s)` : null].filter(Boolean).join('; ');
      const validity = c && model().rangeOf(c) ? Object.entries(model().rangeOf(c)).map(([k, r]) => `${k} from ${fmtQuantity(r[0])} to ${fmtQuantity(r[1])}`).join('; ') : null;
      return {
        kind: 'input', id: open.id, title: ui.short || (q && q.label) || open.id, name: ui.name || null, tier: f ? f.tier : 'hidden', kind_text: kindText(q, c),
        description: q ? q.label : `Coefficient of the model (${c.dim})`, measured_by: evidence || null,
        range_text: (f && f.range_text) || (validity ? `measured for ${validity}` : 'no measured range: the value is used as entered'),
        value_text: f ? f.value_text : c && typeof c.value === 'number' ? fmtQuantity(c.value, c.dim) : null,
        default_text: q ? `${defaultText(q)} (${ev.kind === 'source' ? `ClickHouse default, ${cite(ev.ref, commit)}` : cite(ev.ref, commit)})` : null,
        sql: q && q.hint ? q.hint.sql : null, mapping: q && q.hint ? q.hint.mapping : null, lines: [],
      };
    }
    const vr = result.variants[open.variant];
    if (!vr) { problems.push(`info: no variant ${open.variant}`); return null; }
    const segs = reasonSegments(vr.reason);
    if (open.kind === 'variant') {
      return { kind: 'variant', id: open.variant, title: variantLabel(open.variant), name: null, description: cardSentence(open.variant, vr, contract, inputs),
        measured_by: commit ? `Measured on ClickHouse ${commit}.` : null, lines: segs.map((s) => `${s.operation ? `${OPERATION_LABELS[s.operation] || s.operation}: ` : ''}${cite(s.text, commit)}`) };
    }
    const op = open.operation;
    const cell = vr.cells[op];
    const lines = segs.filter((s) => s.operation === op).map((s) => cite(s.text, commit));
    if (cell && cell.rows) for (const r of cell.rows) if (r.trace && r.trace.extrapolated) lines.push(`${rowLabel(r)}: extrapolated outside the measured range`);
    for (const n of vr.notes || []) if (n.kind === 'cell' && n.operation === op) lines.push(cite(noteText(n), commit));
    return { kind: 'cell', id: `${open.variant}.${op}`, title: `${variantLabel(open.variant)} · ${OPERATION_LABELS[op] || op}`, name: null,
      description: vr.state === 'ok' ? (cell && cell.extrapolated ? 'Priced, extrapolated.' : 'Priced from measured coefficients.') : stateText(vr.state),
      measured_by: commit ? `Measured on ClickHouse ${commit}.` : null, lines };
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
        if (n.kind === 'cell') notes.push({ kind: 'cell', variant: v, operation: n.operation, text: `${variantLabel(v)} ${n.operation}: ${cite(noteText(n), measuredBuild(contract))}` });
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
      cards: [], table: { columns: [], rows: [] }, drill: null, chart: null, notes: [], inputs: null, info: null, evidence: null, problems,
    };
    vm.inputs = buildInputs(contract, fields, lockOrder || [], preset, u, pageState, problems);
    vm.notes = buildNotes(result, contract, fields, pageState);
    if (pageState === 'cannot_reproduce') return vm;
    vm.cards = buildCards(result, contract, preset, pageState, vm.inputs);
    vm.table = buildTable(result, contract, preset, pageState, problems, fields);
    vm.evidence = buildEvidence(contract);
    if (u.openCell) vm.drill = buildDrill(result, contract, preset, pageState, u.openCell, problems);
    if (u.info) vm.info = buildInfo(result, contract, vm.inputs, u.info, problems);
    // A chart of money has nothing to draw without prices.
    if (pageState === 'ok') vm.chart = buildChart(result, contract, preset, u, problems, fields);
    return vm;
  }

  SCC.viewmodel = { build, plainReason, cite, cardSentence, reasonSegments, TIERS, GIB_NOTE, UNIT_CLASS_LABELS, OP_KIND_LABELS, fmtMoney, fmtPrice, fmtQuantity, cellText, unitClassOf, stateText, VARIANT_LABELS, OPERATION_LABELS, BLOCKS, STATE_TEXT };
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.viewmodel;
})(typeof window !== 'undefined' ? window : globalThis);
