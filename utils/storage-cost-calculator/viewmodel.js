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
    gc: 'Garbage collection', lease: 'Mount lease', transfer: 'Replica transfer',
  };
  // Advanced block order follows spec §4.2; `simple` is always visible.
  const BLOCKS = [
    ['simple', 'Inputs'], ['schema', 'Schema'], ['inserts', 'Inserts'], ['merges', 'Merges'], ['replication', 'Replication'],
    ['mutations_deletes', 'Mutations and deletes'], ['reads_cache', 'Reads and cache'], ['topology_recovery', 'Topology and recovery'],
    ['ebs', 'EBS'], ['local_disk', 'Local disk'], ['cas', 'CAS'], ['reliability', 'Reliability'], ['tiering', 'Tiering'], ['prices', 'Prices'],
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

  // A count per period reads as "per day", not "count/day" or "1/s".
  const PERIOD_WORDS = { s: 'second', second: 'second', minute: 'minute', hour: 'hour', day: 'day', month: 'month' };
  function plainUnit(unit) {
    const m = /^(?:count|1)\/([a-z]+)$/.exec(unit || '');
    return m && PERIOD_WORDS[m[1]] ? `per ${PERIOD_WORDS[m[1]]}` : unit;
  }

  // Thousands separators; whole numbers above 1,000, two decimals above 1, six significant-ish below. The unit is
  // the contract's in plain words; dimensionless units ("1", "count") are omitted.
  function fmtQuantity(value, unit) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`fmtQuantity: expected a finite number, got ${JSON.stringify(value)}`);
    const abs = Math.abs(value);
    const digits = abs >= 1000 ? 0 : abs >= 1 ? 2 : 6;
    const s = nf(`q:${digits}`, { maximumFractionDigits: digits }).format(value).replace('-', '−');
    return unit && unit !== '1' && unit !== 'count' ? `${s} ${plainUnit(unit)}` : s;
  }

  // Three significant digits for sizing figures the reader compares, not reconciles: "169 IOPS", "42.2 MiB/s".
  function fmtSig(value) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`fmtSig: expected a finite number, got ${JSON.stringify(value)}`);
    return nf('sig3', { maximumSignificantDigits: 3 }).format(value).replace('-', '−');
  }

  // A size in GiB shown in MiB below 1 GiB: "207 MiB", "3.63 GiB", "19,500 GiB".
  function fmtSize(gib) {
    if (typeof gib !== 'number' || !Number.isFinite(gib)) throw new Error(`fmtSize: expected a finite number, got ${JSON.stringify(gib)}`);
    return gib > 0 && gib < 1 ? `${fmtSig(gib * 1024)} MiB` : `${fmtSig(gib)} GiB`;
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

  // The numbers behind a backlog state: what a round retires against what it may graduate and delete.
  function backlogSentence(d) {
    const budget = (b) => (b === 0 ? 'unbounded' : fmtQuantity(b));
    const b = d.budgets;
    const fix = d.max_interval !== null && d.max_interval > 0
      ? `keep \`cas_gc_interval_sec\` at or below ${(Math.floor(d.max_interval * 10) / 10).toFixed(1)} s for this workload, or raise`
      : 'no GC interval keeps up with this workload; raise';
    return `a GC round retires ${fmtQuantity(d.retired)} blobs (rate ${fmtQuantity(d.rate)} blobs/s × (interval ${fmtQuantity(d.interval)} s + fold ${fmtQuantity(d.fold)} s)) `
      + `against a per-round budget of ${budget(b.gc_round_graduation_budget)} (graduation) / ${budget(b.gc_round_redelete_budget)} (redelete); `
      + `${fix} \`cas_gc_round_graduation_budget\` and \`cas_gc_round_redelete_budget\` in the disk config (0 = unbounded)`;
  }

  // One plain sentence per column state; the full reason with families and source cites is in the info dialog.
  const blockOf = (preset) => (preset ? presets().blockName(preset) : 'gp3');
  function cardSentence(v, vr, contract, inputs, preset) {
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
      case 'unavailable': {
        const bad = infeasibleLayout(vr.reason);
        const blk = blockOf(preset);
        if (bad && bad.need_gib !== null) {
          return `${label} needs ${fmtSize(bad.need_gib)} per replica; the largest ${blk} layout is ${bad.volumes} volumes of ${fmtQuantity(bad.max_gib, 'GiB')} (${fmtQuantity(bad.volumes * bad.max_gib, 'GiB')})`;
        }
        if (bad) return `${label}: no ${blk} volume layout meets this workload's demand`;
        const manual = /manual provisioning with volumes_per_replica = \S+ needs (\w+)/.exec(vr.reason || '');
        if (manual) return `${label}: the manual volume layout needs a value for ${(contract.registry.find((q) => q.id === manual[1]) || {}).label || manual[1]}`;
        return ops.length ? `${label}: ${joinWords(ops.map(opWord))} not measured` : `${label}: not measured for this workload`;
      }
      case 'unsupported_cut': return `${label}: a measurement this workload needs for ${ops.length ? joinWords(ops.map(opWord)) : 'one operation'} is missing`;
      case 'backlog_grows': {
        const d = vr.backlog;
        if (d) return `${label}: ${backlogSentence(d)}`;
        return `${label}: ${ops.length ? joinWords(ops.map(opWord)) : 'the background work'} cannot keep up with this workload; the backlog grows`;
      }
      default: throw new Error(`unknown variant state ${JSON.stringify(vr.state)}`);
    }
  }

  // A note with a number ends in "= value"; a text note stands alone.
  const noteText = (n) => (typeof n.value === 'number' ? `${n.text} = ${fmtQuantity(n.value)}` : n.text);
  // A `text` note is a plain sentence about one operation of a column (replicas in one AZ); it shows like a cell note.
  const isCellNote = (n) => n.kind === 'cell' || n.kind === 'text';

  // A cell is extrapolated when the pipeline says so (a fitted or assumed coefficient outside its range, a substituted
  // lookup key) or when one of its rows reads a coefficient whose basis is `assumed` and moves a non-zero quantity:
  // the rule pipeline.js uses for the assumption note, so the badge and the note agree.
  function assumedCoefs(contract) {
    return new Set(contract.coefficients.filter((c) => model().basisOf(c) === 'assumed').map((c) => c.id));
  }
  function readsAny(node, ids) {
    if (!node || typeof node !== 'object') return false;
    if ((node.kind === 'coef' && ids.has(node.id)) || (node.kind === 'lookup' && ids.has(node.table_id))) return true;
    return Object.values(node).some((v) => (Array.isArray(v) ? v.some((x) => readsAny(x, ids)) : v && typeof v === 'object' && readsAny(v, ids)));
  }
  const isTransferRow = (r) => r.row_type === 'transfer';
  // Assumed coefficients that rows of a cell price with. `part`: 'transfer' rows, 'other' rows or 'all'.
  function assumedUsed(contract, v, op, c, assumed, part) {
    const f = (contract.formulas[v] || {})[op];
    if (!f || !c || !c.rows || !assumed.size) return [];
    const out = new Set();
    f.ledger_rows.forEach((fr, i) => {
      const r = c.rows[i];
      if (!r || r.state !== 'ok' || !(r.quantity > 0)) return;
      if ((part === 'transfer' && !isTransferRow(r)) || (part === 'other' && isTransferRow(r))) return;
      for (const id of assumed) if (readsAny(fr.expr, new Set([id]))) out.add(id);
    });
    return [...out];
  }
  // Registry rows the model fixes or derives on an assumption (pinned values and derivations, not the user's own
  // inputs). A row that prices a non-zero quantity with one rests on that assumption.
  function assumedRegistryIds(contract) {
    return new Set(contract.registry.filter((q) => q.kind !== 'input' && q.evidence && q.evidence.kind === 'assumed').map((q) => q.id));
  }
  function readsInput(node, ids) {
    if (!node || typeof node !== 'object') return [];
    const own = node.kind === 'input' && ids.has(node.id) ? [node.id] : [];
    return own.concat(...Object.values(node).map((v) => (Array.isArray(v) ? v.flatMap((x) => readsInput(x, ids)) : v && typeof v === 'object' ? readsInput(v, ids) : [])));
  }
  function assumedInputsUsed(contract, v, op, c, part) {
    const f = (contract.formulas[v] || {})[op];
    const ids = assumedRegistryIds(contract);
    if (!f || !c || !c.rows) return [];
    const out = new Set();
    f.ledger_rows.forEach((fr, i) => {
      const r = c.rows[i];
      if (!r || r.state !== 'ok' || !(r.quantity > 0)) return;
      if ((part === 'transfer' && !isTransferRow(r)) || (part === 'other' && isTransferRow(r))) return;
      for (const id of readsInput(fr.expr, ids)) out.add(id);
    });
    return [...out];
  }
  // With `split`, the transfer of a cell is shown in the Replica transfer row and is judged there. The pipeline flag is
  // per cell; when its rows say which row was extrapolated, only rows of the shown part count.
  const rowExtrapolated = (r) => !!(r && r.trace && r.trace.extrapolated);
  // A cell that shows $0.00 prices nothing, so nothing in it is extrapolated. Without prices, a non-zero quantity counts.
  const showsMoney = (m) => Math.round(Math.abs(m) * 100) !== 0;
  function shownAmount(c, split, preset) {
    if (typeof c.money !== 'number') return (c.rows || []).some((r) => r.state === 'ok' && r.quantity !== 0);
    return showsMoney(split ? c.money - transferMoney(c, preset) : c.money);
  }
  function cellExtrapolated(contract, v, op, c, assumed, split, preset) {
    if (!c || !shownAmount(c, split, preset)) return false;
    if (c.extrapolated) {
      const rows = c.rows || [];
      if (!split || !rows.some(rowExtrapolated)) return true;
      if (rows.some((r) => !isTransferRow(r) && rowExtrapolated(r))) return true;
    }
    return assumedUsed(contract, v, op, c, assumed, split ? 'other' : 'all').length > 0;
  }
  const transferExtrapolated = (vr) => Object.values(vr.cells || {}).some((c) => (c.rows || []).some((r) => isTransferRow(r) && r.quantity > 0 && rowExtrapolated(r)));
  const transferAssumed = (contract, v, vr, assumed) => [...new Set(Object.entries(vr.cells || {}).flatMap(([op, c]) => assumedUsed(contract, v, op, c, assumed, 'transfer')))];
  const transferRowExtrapolated = (contract, v, vr, assumed, preset) => showsMoney(Object.values(vr.cells || {}).reduce((s, c) => s + transferMoney(c, preset), 0))
    && (transferAssumed(contract, v, vr, assumed).length > 0 || transferExtrapolated(vr));
  // Table cells of a column that carry the extrapolated badge; 'transfer' is the Replica transfer row.
  function extrapolatedCells(contract, v, vr, split, preset) {
    const assumed = assumedCoefs(contract);
    const ops = Object.entries(vr.cells || {}).filter(([op, c]) => cellExtrapolated(contract, v, op, c, assumed, split, preset)).map(([op]) => op);
    return split && transferRowExtrapolated(contract, v, vr, assumed, preset) ? [...ops, 'transfer'] : ops;
  }
  const columnExtrapolated = (result, contract, v, split, preset) => {
    const vr = result.variants[v];
    return !!vr && vr.state === 'ok' && (!!vr.extrapolated || (!!contract && extrapolatedCells(contract, v, vr, split, preset).length > 0));
  };

  // Money of the transfer lines a cell was allocated (cross-AZ replication); 0 without prices.
  function transferMoney(c, preset) {
    return (c && c.lines ? c.lines : []).filter((l) => unitClassOf(l.rule_id, preset) === 'transfer').reduce((s, l) => s + l.money, 0);
  }
  // The table shows transfer in its own row when any column pays for it; the cards list it as Transfer already.
  function transferSplit(result, contract, preset, pageState) {
    if (pageState !== 'ok' || !preset) return false;
    return contract.variants.some((v) => result.variants[v] && result.variants[v].state === 'ok'
      && Object.values(result.variants[v].cells || {}).some((c) => transferMoney(c, preset) > 0));
  }

  // The cheapest badge needs two priced columns to compare; an extrapolated winner says so. `short` is the table
  // header's text, which shows the extrapolated pill beside it.
  function cheapestOf(result, contract, split, preset) {
    const priced = Object.keys(result.variants).filter((v) => result.variants[v].state === 'ok' && typeof result.variants[v].total === 'number');
    const v = result.cheapest;
    if (!v || priced.length < 2) return { variant: null, text: null, short: null, extrapolated: false, priced: priced.length };
    const ex = columnExtrapolated(result, contract, v, split, preset);
    const short = `cheapest of ${priced.length}`;
    return { variant: v, text: `${short}${ex ? ', extrapolated' : ''}`, short, extrapolated: ex, priced: priced.length };
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

  // `refExtrapolated(ref)`: the reference column's own flag. The card's badge already speaks for its own column, so
  // the line names only an extrapolated reference, in words.
  function deltaTexts(result, v, currency, refExtrapolated) {
    const out = [];
    for (const [key, ref] of [['vs_ebs', 'ebs'], ['vs_s3', 's3']]) {
      if (v === ref || !result.variants[ref]) continue;
      const d = (result.deltas[v] || {})[key];
      const ex = !!d && refExtrapolated(ref);
      const name = `vs ${variantLabel(ref)}${ex ? ' (extrapolated)' : ''}`;
      if (d) out.push({ text: `${name}: ${fmtMoney(d.value, currency, { sign: true })}`, extrapolated: ex });
      else out.push({ text: `${name}: not comparable (${variantLabel(ref)} ${result.variants[ref].state === 'ok' ? 'has no total' : cellText(result.variants[ref].state)})`, extrapolated: false });
    }
    return out;
  }

  function buildCards(result, contract, preset, pageState, inputs, split) {
    const best = cheapestOf(result, contract, split, preset);
    // An input the page refused is the reason a column has no result.
    const refusals = inputs ? inputs.blocks.flatMap((b) => b.fields).filter((f) => f.refused).map((f) => f.refused) : [];
    return contract.variants.map((v) => {
      const vr = result.variants[v];
      if (!vr) throw new Error(`result has no variant ${v}`);
      if (result.cheapest === v && vr.state !== 'ok') throw new Error(`result marks ${v} cheapest in state ${vr.state}`);
      const cheapest = best.variant === v && pageState === 'ok';
      const exCells = vr.state === 'ok' ? extrapolatedCells(contract, v, vr, split, preset) : [];
      const card = { variant: v, label: variantLabel(v), state: vr.state, reason: cardSentence(v, vr, contract, inputs, preset), extrapolated: !!vr.extrapolated || exCells.length > 0,
        extrapolated_cells: exCells.length, extrapolated_text: exCells.length ? `extrapolated: ${exCells.length} ${exCells.length === 1 ? 'cell' : 'cells'}` : vr.extrapolated ? 'extrapolated' : null, cheapest,
        cheapest_text: cheapest ? best.text : null, total_text: null, state_text: null, delta_texts: [], deltas: [], bar_segments: [] };
      if (vr.state !== 'ok') {
        card.state_text = stateText(vr.state);
        if (vr.state === 'unavailable' && refusals.length) card.state_text += `: ${refusals.join('; ')}`;
        return card;
      }
      if (pageState === 'prices_missing') {
        // The page banner says why; the card only says what it shows.
        card.state_text = 'quantities only, no prices';
        return card;
      }
      card.total_text = fmtMoney(vr.total, preset.currency);
      card.state_text = cheapest ? best.text : null;
      card.deltas = deltaTexts(result, v, preset.currency, (ref) => columnExtrapolated(result, contract, ref, split, preset));
      card.delta_texts = card.deltas.map((d) => d.text);
      card.bar_segments = barSegments(vr, preset);
      card.sizing = buildSizing(v, vr, preset, inputs ? Object.fromEntries(inputs.blocks.flatMap((b) => b.fields).map((f) => [f.id, f])) : null, pageState);
      return card;
    });
  }

  // Hot local disk plus each cold object-storage variant (pipeline.tiered), beside the single-tier total of the same
  // variant. Sums and differences of contract totals only (spec §4.5).
  function buildTiered(result, preset, pageState, inputs) {
    const t = result.tiered;
    if (!t) return null;
    const P = pipeline();
    const field = (id) => (inputs ? inputs.blocks.flatMap((b) => b.fields).find((f) => f.id === id) || null : null);
    const priced = pageState === 'ok';
    const cur = priced ? preset.currency : null;
    const hot = `local disk (${variantLabel(t.hot_variant)})`;
    const totals = t.rows.filter((r) => r.total !== null).length;
    const rows = t.rows.map((r) => {
      const label = variantLabel(r.variant);
      const row = { variant: r.variant, label, state: r.state, extrapolated: r.extrapolated, cheapest: false, cheapest_text: null, sum_text: null, single_text: null, reason: null };
      if (r.state !== 'ok') {
        row.sum_text = `Hot: ${hot} + Cold: ${label}: ${stateText(r.state)}`;
        row.reason = plainReason(r.reason);
        return row;
      }
      if (!priced || r.total === null) {
        row.sum_text = `Hot: ${hot} + Cold: ${label}: quantities only, no prices`;
        return row;
      }
      row.sum_text = `Hot: ${hot} ${fmtMoney(r.hot_total, cur)} + Cold: ${label} ${fmtMoney(r.cold_total, cur)} = ${fmtMoney(r.total, cur)}`;
      row.single_text = r.single_total === null ? `single tier ${label}: ${r.single_state && r.single_state !== 'ok' ? cellText(r.single_state) : 'no total'}`
        : `single tier ${label} ${fmtMoney(r.single_total, cur)}, tiered ${fmtMoney(r.total - r.single_total, cur, { sign: true })}`;
      if (t.cheapest === r.variant && totals > 1) {
        row.cheapest = true;
        row.cheapest_text = `cheapest of ${totals}${r.extrapolated ? ', extrapolated' : ''}`;
      }
      return row;
    });
    const n = (x) => fmtQuantity(x);
    return {
      title: `Tiered: ${n(t.hot_days)} days on local disk, then ${t.cold_days === null ? 'the rest' : `${n(t.cold_days)} days`} on object storage`,
      hot_days: t.hot_days, cold_days: t.cold_days, rows,
      single_hot_text: priced && result.variants[t.hot_variant] && result.variants[t.hot_variant].state === 'ok'
        ? `Single tier ${variantLabel(t.hot_variant)}: ${fmtMoney(result.variants[t.hot_variant].total, cur)}` : null,
      gc_field: field('cold_gc_interval_s'),
      notes: [P.TTL_MOVE_NOTE, P.COLD_MERGE_NOTE,
        ...t.cold_variants.filter((v) => ((((t.cold.variants[v] || {}).cells || {}).merges || {}).notes || []).some((x) => x.text === P.COLD_MERGE_IGNORED_NOTE))
          .map((v) => `${variantLabel(v)} merges: ${P.COLD_MERGE_IGNORED_NOTE}`),
        ...t.problems.map(plainReason)],
    };
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
    mutations: [['mutations_per_month', 'mutated_compact_parts_per_month', 'mutated_wide_parts_per_month'], 'ALTER UPDATE/DELETE'],
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

  function buildTable(result, contract, preset, pageState, problems, fields, split) {
    const rows = [];
    const assumed = assumedCoefs(contract);
    for (const op of contract.operations) {
      if (!operationShown(contract, op)) continue;
      if (!OPERATION_LABELS[op]) problems.push(`operation ${op} has no label in viewmodel.js`);
      const cells = contract.variants.map((v) => {
        const vr = result.variants[v];
        const c = vr.cells[op];
        // No cell: "not applicable", unless the column's own reason names this operation; then the row says what the card says.
        if (!c) {
          const named = vr.state !== 'ok' && reasonSegments(vr.reason).some((s) => s.operation === op);
          return named ? { variant: v, operation: op, text: cellText(vr.state), state: vr.state, extrapolated: false, clickable: false }
            : { variant: v, operation: op, text: 'not applicable', state: 'not_applicable', extrapolated: false, clickable: false };
        }
        if (vr.state !== 'ok') return { variant: v, operation: op, text: cellText(vr.state), state: vr.state, extrapolated: !!c.extrapolated, clickable: false };
        // Under `split` the cell shows its money without the transfer, which the Replica transfer row carries.
        const own = split ? c.lines.filter((l) => unitClassOf(l.rule_id, preset) !== 'transfer').reduce((s, l) => s + l.money, 0) : c.money;
        const text = pageState === 'prices_missing' ? quantitySummary(c, rowUnits(contract, v, op), problems) : fmtMoney(own, preset.currency);
        return { variant: v, operation: op, text, state: 'ok', extrapolated: cellExtrapolated(contract, v, op, c, assumed, split, preset), clickable: true, money: own };
      });
      // "no cold reads in this workload" applies to the whole row: said once, beside the row label.
      const zero = pageState === 'ok' && cells.some((c) => c.state === 'ok' && c.money === 0);
      for (const c of cells) delete c.money;
      for (const c of cells) {
        const vr = result.variants[c.variant];
        c.notes = c.state === 'ok' ? (vr.notes || []).filter((n) => isCellNote(n) && n.operation === op).map((n) => cite(noteText(n), measuredBuild(contract))) : [];
      }
      rows.push({ operation: op, label: OPERATION_LABELS[op] || op, cells, note: zero ? zeroHint(op, fields) : null });
    }
    if (split) {
      const cells = contract.variants.map((v) => {
        const vr = result.variants[v];
        if (vr.state !== 'ok') return { variant: v, operation: 'transfer', text: cellText(vr.state), state: vr.state, extrapolated: false, clickable: false, info: false, notes: [] };
        const m = Object.values(vr.cells || {}).reduce((s, c) => s + transferMoney(c, preset), 0);
        return { variant: v, operation: 'transfer', text: fmtMoney(m, preset.currency), state: 'ok', extrapolated: transferRowExtrapolated(contract, v, vr, assumed, preset), clickable: false, info: true, notes: [] };
      });
      rows.push({ operation: 'transfer', label: OPERATION_LABELS.transfer, cells, note: null });
    }
    const best = cheapestOf(result, contract, split, preset);
    const top = (v) => pageState === 'ok' && best.variant === v;
    return { columns: contract.variants.map((v) => ({ variant: v, label: variantLabel(v), cheapest: top(v), cheapest_text: top(v) ? best.short : null, extrapolated: top(v) && best.extrapolated })), rows,
      column_notes: pageState === 'cannot_reproduce' ? {} : columnNotes(result, contract, preset, fields, pageState),
      column_lines: pageState === 'cannot_reproduce' ? {} : columnLines(result, contract, preset, fields, pageState) };
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

  // Low end of the logarithmic sliders, in registry units: 10 GiB of data, 1 GiB a day. The high end is the input bound.
  const LOG_SLIDER_MIN = { data_bytes: 10, ingest_rate: 1 };

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

  // Registry `presets` that set `id` (shape narrow: columns 5, ...) as quick picks on that field. A pick sets every
  // quantity of its preset; it shows pressed when all of them hold the preset's values.
  function quickPicks(contract, id, fields) {
    const out = [];
    for (const src of contract.registry) {
      if (src.kind !== 'input' || !src.presets) continue;
      for (const key of src.enum || []) {
        const p = src.presets[key];
        if (!p || !Object.prototype.hasOwnProperty.call(p, id)) continue;
        out.push({ source: src.id, key, label: `${key} (${fmtQuantity(p[id])})`, set: Object.assign({}, p),
          pressed: Object.entries(p).every(([k, v]) => fields && fields[k] && fields[k].value === v) });
      }
    }
    return out;
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
        else Object.assign(f, { value: fl.value, locked: fl.locked, derived: fl.derived, overridden: fl.overridden, out_of_range: fl.out_of_range, range: fl.validity, rule_text: fl.rule_text, refused: fl.refused ? fl.refused.text : null, refused_value: fl.refused ? fl.refused.value : null });
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
      f.display_unit = plainUnit(f.rate ? q.unit.replace(/\/day$/, `/${per}`) : f.unit);
      f.display_value = roundDisplay(shown(q, f.value === null && f.refused_value !== undefined ? f.refused_value : f.value));
      f.display_range = f.range ? f.range.map((x) => roundDisplay(shown(q, x))) : null;
      if (f.rule_text) f.derived_text = `Derived from ${worded(f.rule_text)}`;
      if (f.kind === 'derivation') f.value_text = f.derived_text;
      // Slider bounds in the shown unit: a logarithmic span from a small value to the input's bound, else the measured
      // range, else the sweep ladder's span, else no slider.
      const ladder = presets().SWEEP_INPUTS[q.id] ? presets().ladderFor(q.id, f.value, f.range) : null;
      const lim = pipeline().INPUT_LIMITS[q.id];
      f.slider = lim && LOG_SLIDER_MIN[q.id] ? { min: roundDisplay(shown(q, LOG_SLIDER_MIN[q.id])), max: roundDisplay(shown(q, lim.max)), log: true }
        : f.display_range && f.display_range[1] > f.display_range[0] ? { min: f.display_range[0], max: f.display_range[1] }
        : ladder ? { min: roundDisplay(shown(q, ladder[0])), max: roundDisplay(shown(q, ladder[ladder.length - 1])) } : null;
      // Counts move in whole steps; a range of one value is fixed, not a control.
      f.step = q.unit === 'count' ? 1 : 'any';
      f.fixed = !!f.range && f.range[0] === f.range[1];
      f.range_text = rangeText(f, byId);
      f.quick_picks = q.kind === 'input' ? quickPicks(contract, q.id, fields) : [];
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

  // ---- sizing (block volumes) -------------------------------------------------------------------------

  // Per-replica volume sizing from the billing allocation: what the workload needs, what gp3 includes, what the
  // layout provisions, and a one-line verdict. Every number comes from the Result; none is computed here.
  function buildSizing(v, vr, preset, fields, pageState) {
    const p = vr.provisioning;
    if (vr.state !== 'ok' || !p || !p.per_replica) return null;
    const g = preset && preset.block && preset.block.gp3;
    const blk = blockOf(preset);
    const r = p.per_replica;
    const val = (id) => (fields && fields[id] && typeof fields[id].value === 'number' ? fields[id].value : null);
    const headroom = val('ebs_capacity_headroom');
    const peak = val('peak_factor');
    const d = p.demand || {};
    const priced = pageState === 'ok' && preset && !preset.structure_only;
    const above = (vr.lines || []).filter((l) => l.rule_id === 'block_iops' || l.rule_id === 'block_mibps').reduce((s, l) => s + l.money, 0);
    const extra = priced && above > 0 ? fmtMoney(above / p.replicas, preset.currency) : null;
    const iopsNeed = d.peak_iops || 0;
    const mibpsNeed = d.peak_mibps || 0;
    let verdict;
    let brief;
    let tag;
    if (r.feasible === false) {
      verdict = `no feasible ${blk} layout: ${(r.notes || []).join('; ')}`;
      brief = `no ${blk} layout fits`;
      tag = brief;
    } else if (r.volumes > 1) {
      // Why one volume is not enough decides what the reader should look at: the peak or the size.
      const why = g && (iopsNeed > g.iops_baseline || mibpsNeed > g.mibps_baseline)
        ? `one volume's baseline (${fmtQuantity(g.iops_baseline)} IOPS, ${fmtQuantity(g.mibps_baseline)} MiB/s) is below the peak`
        : g && g.size_gib && (d.capacity_gib || 0) > g.size_gib[1] ? `one volume holds at most ${fmtQuantity(g.size_gib[1], 'GiB')}`
          : r.layout;
      const cost = !priced ? null : extra ? `+${extra}/month per replica above the baseline` : 'no charge above the baseline';
      verdict = [`${r.volumes} striped ${blk} volumes of ${fmtQuantity(r.size_gib, 'GiB')}`, why, cost].filter(Boolean).join('; ');
      brief = `${r.volumes} striped volumes${extra ? `, +${extra}/month` : ''}`;
      tag = `${r.volumes} volumes`;
    } else if (g && r.iops <= g.iops_baseline && r.mibps <= g.mibps_baseline) {
      verdict = `${blk} baseline sufficient`;
      brief = `${blk} baseline ok`;
      tag = `${blk} ok`;
    } else {
      verdict = `provision ${fmtQuantity(r.iops)} IOPS, ${fmtQuantity(r.mibps)} MiB/s${priced ? ` (+${fmtMoney(above / p.replicas, preset.currency)}/month per replica)` : ''}`;
      brief = `provision ${fmtQuantity(r.iops)} IOPS, ${fmtQuantity(r.mibps)} MiB/s${extra ? `, +${extra}/month` : ''}`;
      tag = `${blk} above baseline`;
    }
    const components = [];
    for (const [op, c] of Object.entries(vr.cells || {})) {
      const q = (kind) => (c.rows || []).filter((x) => x.row_type === 'block_io' && x.op_kind === kind && x.state === 'ok').reduce((s, x) => s + x.quantity, 0);
      const iops = q('iops');
      const mibps = q('mibps');
      if (iops > 0 || mibps > 0) components.push(`${OPERATION_LABELS[op] || op}: ${fmtSig(iops)} IOPS, ${fmtSig(mibps)} MiB/s`);
    }
    const need = `${fmtSig(iopsNeed)} IOPS, ${fmtSig(mibpsNeed)} MiB/s`;
    // What the volume holds: equal data on every column, so a small local disk must not read as less data.
    const cache = fields && fields.cache_enabled && String(fields.cache_enabled.value) === '1' && fields.cache_size_gib && fields.cache_size_gib.value > 0;
    const holds = v === 'ebs' ? 'Data volume' : v === 'cas' ? 'Local disk, upload staging only' : cache ? 'Local disk, metadata and cache' : 'Local disk, metadata only';
    const word = v === 'ebs' ? 'data' : v === 'cas' ? 'staging' : cache ? 'meta+cache' : 'metadata';
    const stored = v === 'ebs' && headroom ? (d.capacity_gib || 0) / headroom : null;
    const lines = [
      `Volume size per replica: ${fmtSize(d.capacity_gib || 0)}${stored !== null ? ` (stored data ${fmtSize(stored)} × ${fmtQuantity(headroom)} ebs_capacity_headroom)` : ''}`,
      `Peak demand per replica: ${need}${peak !== null ? ` (average × ${fmtQuantity(peak)} peak_factor)` : ''}`,
      g ? `${blk} baseline per volume: ${fmtQuantity(g.iops_baseline)} IOPS, ${fmtQuantity(g.mibps_baseline)} MiB/s included` : null,
      `${r.manual ? 'Manual layout' : 'Cheapest feasible layout'}: ${r.volumes} × ${fmtQuantity(r.size_gib, 'GiB')}, ${fmtQuantity(r.iops)} IOPS, ${fmtQuantity(r.mibps)} MiB/s, on ${p.replicas} ${p.replicas === 1 ? 'replica' : 'replicas'}`,
      `Verdict: ${verdict}`,
      ...components.map((x) => `Demand from ${x}`),
    ].filter(Boolean);
    const instance = (vr.notes || []).find((n) => n.kind === 'instance_class');
    // A column that keeps nothing on the volume pays only the smallest one; "0 GiB, needs 0 IOPS" says less.
    const empty = !(d.capacity_gib > 0) && !(iopsNeed > 0) && !(mibpsNeed > 0);
    return {
      variant: v, replicas: p.replicas, capacity_gib: d.capacity_gib, peak_iops: d.peak_iops, peak_mibps: d.peak_mibps, headroom, peak_factor: peak,
      volumes: r.volumes, size_gib: r.size_gib, iops: r.iops, mibps: r.mibps, layout: r.layout, manual: !!r.manual, verdict, components, lines,
      short: empty ? `Local disk, nothing persistent: the minimum ${blk} volume (${fmtSize(r.size_gib)}) per replica`
        : `${holds}: ${fmtSize(d.capacity_gib || 0)} per replica${stored !== null ? ` (${fmtSize(stored)} + ${fmtQuantity(Math.round((headroom - 1) * 100))}% headroom)` : ''}`
          + ` · ${iopsNeed > 0 || mibpsNeed > 0 ? `needs ${need}` : '0 IOPS'}: ${verdict}`,
      // The notes row: what the volume holds, its size per replica and the verdict, in about 26 characters so four
      // notes stay on one line at 1440 px; per replica, IOPS and MiB/s are in the dialog.
      line: empty ? `nothing local · ${fmtSize(r.size_gib)} ${blk}` : `${word} ${fmtSize(d.capacity_gib || 0)} · ${tag}`,
      instance_class: instance ? instance.instance_class || null : null, instance_text: instance ? instance.text : null,
    };
  }

  // A block column that no block-volume layout can hold: the billing reason lists each layout and the limit it breaks.
  function infeasibleLayout(reason) {
    const m = /no feasible volume allocation: (.*)$/s.exec(String(reason || ''));
    if (!m) return null;
    const sizes = [...m[1].matchAll(/([a-z]+(\d*)): size_gib ([\d.]+) > ([\d.]+)/g)].map((x) => ({ volumes: Number(x[2] || 1), size: Number(x[3]), max: Number(x[4]) }));
    const one = sizes.find((x) => x.volumes === 1);
    if (!one) return { need_gib: null };
    const most = Math.max(...sizes.map((x) => x.volumes));
    return { need_gib: one.size, max_gib: one.max, volumes: most };
  }

  // Column-level notes: the column dialog lists them all.
  function columnNotes(result, contract, preset, fields, pageState) {
    const out = {};
    for (const v of contract.variants) {
      const vr = result.variants[v];
      const items = [];
      const sz = vr ? buildSizing(v, vr, preset, fields, pageState) : null;
      if (sz) items.push({ kind: 'sizing', text: sz.short });
      if (sz && sz.instance_class) items.push({ kind: 'instance_class', text: `Instance: ${sz.instance_class} or larger for EBS bandwidth` });
      if (vr && vr.state === 'ok' && (vr.lines || []).some((l) => l.rule_id === 'storage')) items.push({ kind: 'gib', text: GIB_NOTE });
      for (const n of (vr && vr.state === 'ok' ? vr.notes : []) || []) {
        if (n.kind === 'text' && !items.some((x) => x.kind === 'text' && x.text === n.text)) items.push({ kind: 'text', text: n.text });
      }
      const by = (contract.evidence_by_variant || {})[v];
      if (by) items.push({ kind: 'evidence', text: `Evidence: ${by.fit_and_holdout} cells fit and hold-out, ${by.fit_only} fit only, ${by.assumption} assumption only` });
      out[v] = items;
    }
    return out;
  }

  // The notes row of the breakdown: one short line per column (sizing, else why it is off, else its evidence).
  function columnLines(result, contract, preset, fields, pageState) {
    const out = {};
    for (const v of contract.variants) {
      const vr = result.variants[v];
      if (!vr) continue;
      const sz = buildSizing(v, vr, preset, fields, pageState);
      const bad = vr.state !== 'ok' ? infeasibleLayout(vr.reason) : null;
      const by = (contract.evidence_by_variant || {})[v];
      out[v] = sz ? sz.line
        : bad ? (bad.need_gib !== null ? `no ${blockOf(preset)} layout fits ${fmtSize(bad.need_gib)}/replica` : `no ${blockOf(preset)} layout fits`)
          // The card says why a column is off; the Notes row names the state and its (i) has the sentence.
          : vr.state !== 'ok' ? cellText(vr.state)
            : by ? `evidence: ${by.fit_and_holdout} fit and hold-out, ${by.fit_only} fit only, ${by.assumption} assumed`
              : (vr.lines || []).some((l) => l.rule_id === 'storage') ? 'storage GB read as GiB' : null;
    }
    return out;
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

  // One plain sentence for inputs whose registry label is only their short name: the dialog never repeats its title.
  const FIELD_SENTENCES = {
    data_bytes: 'Compressed size of the table on disk, counted once: replicas are not added.',
    ingest_rate: 'Compressed bytes the table receives per day.',
    retention_days: 'How long data is kept before it is dropped.',
    replicas: 'Servers per shard that hold and serve the same data.',
    shards: 'Number of shards the data is split across.',
    insert_rows: 'Rows in one INSERT query.',
    cache_enabled: 'Whether each replica keeps a filesystem cache on its local disk.',
    cas_tables: 'MergeTree tables stored on the CAS disk, each with its own background requests.',
    cas_table_creates_per_month: 'CREATE TABLE statements per month on the CAS disk; every replica runs each one.',
    cas_table_drops_per_month: 'DROP TABLE statements per month on the CAS disk; every replica runs each one.',
  };

  // How the table count, creations and drops enter the CAS column; one text for the three inputs of the tables row.
  const TABLE_INPUTS = ['cas_tables', 'cas_table_creates_per_month', 'cas_table_drops_per_month'];
  const constSum = (n) => (!n ? null : n.kind === 'const' ? n.value : n.kind === 'add' ? ((a, b) => (a === null || b === null ? null : a + b))(constSum(n.a), constSum(n.b)) : null);
  // The measured per-table GC and start terms (report 2026-10-25-storage-cost-cas-tables): their values and the table
  // counts they were fitted at, read from the contract; null when the contract predates them.
  function tableFit(contract) {
    const c = (id) => contract.coefficients.find((x) => x.id === id);
    const gc = c('gc_round_requests_steady');
    const start = c('cas_startup_requests_per_table');
    const err = c('cas_startup_error_requests_per_table');
    if (!gc || !start || !err) return null;
    const r = (model().rangeOf(gc) || {}).cas_tables;
    const v = gc.value.values;
    // A coefficient's record states a passed hold-out as `ok, ...` or `done: ...`.
    const held = [gc, start, err].every((x) => /^(ok|done)\b/.test(String((x.fit && x.fit.holdout) || '')));
    return { span: r ? `${fmtQuantity(r[0])}–${fmtQuantity(r[1])}` : null, held, changed: v.fold_get_per_changed_table, swept: v.fold_get_per_swept_table,
      budget: v.sweep_namespace_budget, get: (start.value.values['0|GetObject'] || 0) + (err.value.values['0|GetObject'] || 0),
      put: start.value.values['0|PutObject'] || 0, list: start.value.values['0|ListObjectsV2'] || 0 };
  }
  function tableLines(contract) {
    const q = (id) => contract.registry.find((x) => x.id === id) || {};
    const period = q('cas_table_cleanup_period_s').default;
    const cleanupGets = q('cas_cleanup_gets_per_table').default;
    const tail = q('cas_ref_log_tail_records').default;
    const creates = constSum(q('cas_table_create_requests').rule);
    const drop = q('cas_foreground_ref_removal_requests').default;
    const fit = tableFit(contract);
    return [
      `Every replica's MergeTree cleanup thread lists each table's files once per cleanup period (${cleanupGets ? `${fmtQuantity(cleanupGets)} GETs for a table with parts` : 'GETs'} and a LIST${period ? `, every ${fmtQuantity(period)} s when idle` : ''}).`,
      fit ? `A server start reads each table's catalog row, checkpoint and files (${fmtQuantity(fit.get)} GETs, ${fmtQuantity(fit.put)} PUTs, ${fmtQuantity(fit.list)} LISTs) and replays its ref log tail${tail ? ` (${fmtQuantity(tail)} records assumed)` : ''}.`
        : `A server start replays each table's ref log tail${tail ? ` (${fmtQuantity(tail)} records assumed)` : ''}.`,
      fit ? `A GC fold reads ${fmtQuantity(fit.changed)} objects per table with new data; its orphan-manifest sweep re-reads the whole tail plus ${fmtQuantity(fit.swept)} objects of at most ${fmtQuantity(fit.budget)} tables per round.`
        : 'A folding GC round reads the tail of every table.',
      `CREATE TABLE sends ${creates === null ? 'a fixed set of' : fmtQuantity(creates)} requests per replica: the catalog entry, the first checkpoint, ref log recovery, format_version.txt and the table-start listings.`,
      `DROP TABLE SYNC removes each part's ref in one transaction${drop ? ` and pays ${fmtQuantity(drop)} requests for the table` : ''}, per replica; Parts per dropped table (low-level) sets the part count, and the dropped parts become GC work.`,
      fit && fit.span ? `The GC and start requests per table are fitted at ${fit.span} tables${fit.held ? ' and held out' : ', hold-out pending'}; the cleanup listing and CREATE and DROP TABLE are formulas from source, verified at 1 table.`
        : 'The measurements ran one table; the per-table scaling is a formula from source.',
    ];
  }
  const tableVerified = (contract) => {
    const fit = tableFit(contract);
    return fit && fit.span ? `fitted at ${fit.span} tables${fit.held ? ' and held out' : ', hold-out pending'}; listing and DDL from source, verified at 1 table`
      : 'formula from source; verified at 1 table';
  };
  function describe(q, title) {
    if (FIELD_SENTENCES[q.id]) return FIELD_SENTENCES[q.id];
    return q.label && q.label.toLowerCase() !== String(title).toLowerCase() ? q.label : null;
  }

  // Plain sentences for coefficients whose basis is `assumed`; the contract states the assumption in engineering terms.
  // A sentence that no longer matches its numbers returns null, and the dialog reports the gap.
  // No coefficient is `assumed` in the current registry; add an entry here when one is.
  const ASSUMPTIONS = {};
  function assumptionSentence(c) {
    const f = ASSUMPTIONS[c.id];
    return f ? f(c) : null;
  }

  // Registry words for a parameter or coefficient id: its short name, else its label without a trailing id.
  function nameOf(contract, id) {
    const q = contract.registry.find((x) => x.id === id) || contract.registry.find((x) => x.coef === id);
    if (!q) return id;
    if (q.ui && q.ui.short) return q.ui.short;
    return q.label ? q.label.split(': ')[0].replace(/\s*\([^()]*_[^()]*\)\s*$/, '') : id;
  }

  // "rows per insert 100,000 to 300,000" for each parameter of a range map.
  function rangesText(contract, ranges) {
    return Object.entries(ranges).map(([p, r]) => {
      const q = contract.registry.find((x) => x.id === p);
      const span = r[0] === r[1] ? `${fmtQuantity(r[0], q && q.unit)} only` : `${fmtQuantity(r[0])} to ${fmtQuantity(r[1], q && q.unit)}`;
      return `${nameOf(contract, p).toLowerCase()} ${span}`;
    }).join('; ');
  }

  // Where the measurements checked a value. A formula from source holds outside it; a fitted value is extrapolated there.
  function verifiedText(contract, q, c, f) {
    if (q && TABLE_INPUTS.includes(q.id)) return tableVerified(contract);
    let span = null;
    if (c && model().rangeOf(c)) span = rangesText(contract, model().rangeOf(c));
    else if (f && f.display_range) {
      const [lo, hi] = f.display_range;
      span = lo === hi ? `${fmtQuantity(lo, f.display_unit)} only` : `${fmtQuantity(lo)} to ${fmtQuantity(hi, f.display_unit)}`;
    } else if (f && f.validity) span = rangesText(contract, f.validity);
    if (!span) return 'no verified range';
    const basis = c ? model().basisOf(c) : q && q.kind === 'derivation' ? 'derived' : null;
    if (basis === 'derived') return `formula from source; verified at ${span}`;
    if (basis === 'assumed') return `assumed; checked at ${span}`;
    if (basis === 'fitted') return `fitted at ${span}; extrapolated outside`;
    return `verified at ${span}`;
  }

  // The active preset's value of an input (a derived member resolved as the preset resolves it); null without one.
  function presetValue(contract, scenario, q) {
    const s = scenario ? presets().SCENARIOS[scenario] : null;
    if (!s || q.kind !== 'input') return null;
    const f = pipeline().resolveInputs(contract, s.values, s.locks, {}).fields[q.id];
    return f && f.value !== null && f.value !== undefined ? { value: f.value, label: s.label } : null;
  }

  function defaultLine(contract, q, ev, commit, scenario) {
    const where = ev.kind === 'source' ? `ClickHouse default, ${cite(ev.ref, commit)}` : ev.kind === 'assumed' ? 'assumed' : cite(ev.ref, commit);
    const p = presetValue(contract, scenario, q);
    // A measured or derived quantity has no default to show; the Current value row says what it is.
    if (!p && q.kind !== 'input' && (q.default === null || q.default === undefined)) return null;
    if (!p) return `${defaultText(q)} (${where})`;
    const shown = typeof p.value === 'number' ? fmtQuantity(p.value, q.unit) : String(p.value);
    if (String(p.value) === String(q.default)) return `${shown} (${p.label} preset${ev.kind === 'source' ? `; the ClickHouse default, ${cite(ev.ref, commit)}` : ''})`;
    return `${shown} (${p.label} preset); ${defaultText(q)} without a preset (${where})`;
  }

  // An engineering note in words: a bare id becomes its registry name.
  function sourceNote(contract, n) {
    if (/^[a-z0-9_]+$/.test(n.text)) return typeof n.value === 'number' ? `${nameOf(contract, n.text)} = ${fmtQuantity(n.value)}` : nameOf(contract, n.text);
    return noteText(n);
  }

  // The assumption of one coefficient: a plain line and its source.
  function assumptionLines(contract, id, problems) {
    const c = contract.coefficients.find((x) => x.id === id);
    const s = c ? assumptionSentence(c) : null;
    if (!s) problems.push(`assumed coefficient ${id} has no plain sentence in viewmodel.js`);
    const res = c && c.fit && c.fit.residuals ? Object.values(c.fit.residuals).filter((x) => typeof x === 'number') : [];
    const fit = res.length ? `, within ${fmtQuantity(Math.round(Math.max(...res.map(Math.abs)) * 1000) / 10)}%` : '';
    const report = c && c.evidence && c.evidence.ref ? String(c.evidence.ref).split('/').pop() : null;
    const range = c && model().rangeOf(c) ? ` at ${rangesText(contract, model().rangeOf(c))}` : '';
    return { line: s || `${nameOf(contract, id)} is an assumption, not a measurement.`, source: `Assumed from measurements${range}${fit}${report ? `; report ${report}` : ''}.` };
  }

  // What a cell pays for, in one sentence with its money.
  function cellSentence(v, op, c, preset, pageState, split, fields) {
    const what = `${OPERATION_LABELS[op] || op} on ${variantLabel(v)}`;
    if (pageState !== 'ok' || !c.lines) return `${what}: quantities only; this price list has no prices.`;
    const paid = c.lines.filter((l) => l.money > 0 && !(split && unitClassOf(l.rule_id, preset) === 'transfer')).sort((a, b) => b.money - a.money);
    if (!paid.length) {
      const z = zeroHint(op, fields);
      return `${what}: nothing to pay${z ? `, ${z}` : ' for this workload'}.`;
    }
    const total = fmtMoney(paid.reduce((s, l) => s + l.money, 0), preset.currency);
    const label = (l) => presets().ruleLabel(l.rule_id, preset).replace(/^Transfer/, 'transfer');
    if (paid.length === 1) return `${what}: ${total} a month for ${label(paid[0])}.`;
    return `${what}: ${total} a month, ${joinWords(paid.map((l) => `${fmtMoney(l.money, preset.currency)} for ${label(l)}`))}.`;
  }

  // Inputs of the cold run that lie outside the measured range of a fitted or assumed coefficient (or input) the
  // variant's cold cells read, as texts. The cold inserts cell is the TTL move cell, not the single-tier one.
  function coldOutsideRange(contract, t, v) {
    const P = pipeline();
    const all = P.evaluationInputs(contract, t.cold_inputs);
    const move = P.moveCell(contract, v);
    const cells = Object.assign({}, contract.formulas[v] || {}, move.cell ? { inserts: move.cell } : {});
    const derivedOnly = P.derivedOnlyInputs(contract);
    const seen = new Map();
    const check = (ranges) => {
      for (const [param, r] of Object.entries(ranges || {})) {
        const x = all.get(param);
        if (typeof x !== 'number' || (x >= r[0] && x <= r[1])) continue;
        const q = contract.registry.find((e) => e.id === param);
        seen.set(`${param}|${r[0]}|${r[1]}`, `${nameOf(contract, param)} is ${fmtQuantity(x, q && q.unit)} in the cold tier; measured at ${fmtQuantity(r[0])} to ${fmtQuantity(r[1], q && q.unit)}`);
      }
    };
    const derived = new Set();
    const visit = (n) => {
      if (!n || typeof n !== 'object') return;
      if (n.kind === 'input' && !derived.has(n.id)) {
        // A registry derivation reads coefficients itself; the cell is extrapolated through it.
        const d = contract.registry.find((e) => e.id === n.id && e.kind === 'derivation' && e.rule);
        if (d) { derived.add(n.id); visit(d.rule); }
      }
      if (n.kind === 'coef') {
        const c = contract.coefficients.find((e) => e.id === n.id);
        if (c && model().basisOf(c) !== 'derived') check(model().rangeOf(c));
      } else if (n.kind === 'input' && !derivedOnly.has(n.id)) {
        const q = contract.registry.find((e) => e.id === n.id);
        if (q && q.kind === 'input') check(model().rangeOf(q));
      }
      for (const c of Object.values(n)) visit(c);
    };
    visit(cells);
    return [...seen.values()];
  }

  // The (i) of a tiered row: the cold-tier cells of the variant with their notes (the TTL move rule is the cold
  // inserts note), and which cold-tier inputs lie outside their measured range.
  function tieredInfo(result, contract, v, problems, preset, x, commit) {
    const t = result.tiered;
    const row = t && t.rows.find((r) => r.variant === v);
    const cold = t && t.cold.variants[v];
    if (!row || !cold) { problems.push(`info: no tiered row ${v}`); return null; }
    const label = variantLabel(v);
    const lines = [];
    const source = [];
    if (cold.state !== 'ok') lines.push(`Cold ${label}: ${cold.reason ? plainReason(cold.reason) : stateText(cold.state)}`);
    for (const op of Object.keys(cold.cells || {})) {
      const c = cold.cells[op];
      lines.push(withExtrapolated(cellSentence(v, op, c, preset, result.page_state, false, x.fields), cellExtrapolated(contract, v, op, c, new Set(), false, preset)));
      // Assumption notes are sentences; the rest name ids and formulas and go under Source, as in a cell dialog.
      for (const n of c.notes || []) (n.assumption ? lines : source).push(`${OPERATION_LABELS[op] || op}: ${cite(sourceNote(contract, n), commit)}`);
    }
    const out = coldOutsideRange(contract, t, v);
    const why = !row.extrapolated ? ['Not extrapolated: every cold-tier input lies inside its measured range.']
      : out.length ? ['Extrapolated: these cold-tier inputs lie outside the measured range.', ...out]
        : ['Some inputs lie outside the range the model was measured at, so this number is extrapolated.'];
    return { kind: 'tiered', id: v, title: `Tiered ${label}: cold tier`, name: null,
      description: `After the TTL move the data sits on ${label}. These are the cells of that cold tier.`,
      measured_by: commit ? `Measured on ClickHouse ${commit}.` : null, lines: [...lines, ...why], source };
  }

  // What the (i) dialog shows. `open` is {kind: 'input', id} (any registry row or coefficient id), {kind: 'cell',
  // variant, operation}, {kind: 'transfer', variant} or {kind: 'variant', variant}. `ctx`: {split, scenario, fields}.
  function buildInfo(result, contract, inputs, open, problems, preset, ctx) {
    const x = ctx || {};
    const commit = measuredBuild(contract);
    if (open.kind === 'input') {
      const q = contract.registry.find((r) => r.id === open.id);
      const c = contract.coefficients.find((r) => r.id === open.id || (q && q.coef === r.id));
      if (!q && !c) { problems.push(`info: ${open.id} is neither a registry row nor a coefficient`); return null; }
      const f = q ? inputs.blocks.flatMap((b) => b.fields).find((r) => r.id === q.id) : null;
      const ui = (q && q.ui) || {};
      const ev = (q && q.evidence) || (c && c.evidence) || {};
      const ids = (c && c.evidence && c.evidence.measurement_ids) || [];
      const evidence = ui.measured_by ? cite(ui.measured_by, commit)
        : [ev.kind ? `${ev.kind}: ${cite(ev.ref, commit)}` : null, ids.length ? `${ids.length} measurement(s)` : null].filter(Boolean).join('; ');
      // A value the user types is an input by definition, and "preset default" or "Default from <file>" repeats
      // the Default row; a dialog for an input shows evidence only when it says more than that.
      const userInput = !!q && q.kind === 'input';
      const generic = userInput && (/^Assumed: (preset default|standard deployment profile|table key)\.$/.test(evidence) || /^Default from /.test(evidence));
      const title = ui.short || (q && q.label) || open.id;
      const group = q ? inputs.groups.find((g) => g.members.includes(q.id)) : null;
      return {
        kind: 'input', id: open.id, title, name: ui.name || null, tier: f ? f.tier : 'hidden', kind_text: userInput ? null : kindText(q, c),
        description: q ? describe(q, title) : `Coefficient of the model (${c.dim})`, measured_by: (!generic && evidence) || null,
        range_text: verifiedText(contract, q, c, f),
        value_text: f ? f.value_text : c && typeof c.value === 'number' ? fmtQuantity(c.value, c.dim) : null,
        derived_text: f && f.derived && f.derived_text ? f.derived_text : null,
        default_text: q ? defaultLine(contract, q, ev, commit, x.scenario) : null,
        sql: q && q.hint ? q.hint.sql : null, mapping: q && q.hint ? q.hint.mapping : null,
        lines: [...(group ? [`${group.label.charAt(0).toUpperCase()}${group.label.slice(1)}. Lock two; the third is derived.`] : []),
          ...(q && TABLE_INPUTS.includes(q.id) ? tableLines(contract) : [])], source: [],
      };
    }
    if (open.kind === 'evidence') {
      const e = buildEvidence(contract);
      return { kind: 'evidence', id: 'evidence', title: 'What these numbers rest on', name: null, description: contract.evidence_statement || null,
        measured_by: null, lines: e.columns.map((c) => `${c.label}: ${c.fit_and_holdout} cells fit and hold-out, ${c.fit_only} fit only, ${c.assumption} assumption only`), links: e.reports.map((r) => ({ text: r.name, href: r.href })) };
    }
    if (open.kind === 'scenario') {
      const s = presets().SCENARIOS[open.id];
      if (!s) { problems.push(`info: no scenario ${open.id}`); return null; }
      return { kind: 'scenario', id: open.id, title: s.label, name: null, description: s.description, measured_by: null, lines: [] };
    }
    if (open.kind === 'tiered') return tieredInfo(result, contract, open.variant, problems, preset, x, commit);
    const vr = result.variants[open.variant];
    if (!vr) { problems.push(`info: no variant ${open.variant}`); return null; }
    const segs = reasonSegments(vr.reason);
    if (open.kind === 'column') {
      const fields = Object.fromEntries(inputs.blocks.flatMap((b) => b.fields).map((f) => [f.id, f]));
      const sz = buildSizing(open.variant, vr, preset, fields, result.page_state);
      // The cheapest card's badge reads "extrapolated" without a count; its (i) opens this dialog, which names the cells.
      const exOps = vr.state === 'ok' && result.page_state === 'ok' ? extrapolatedCells(contract, open.variant, vr, !!x.split, preset) : [];
      const exLine = exOps.length ? [`Extrapolated: ${exOps.length} ${exOps.length === 1 ? 'cell' : 'cells'} (${exOps.map((o) => OPERATION_LABELS[o] || o).join(', ')}).`] : [];
      const lines = [...(sz ? [sz.short] : []), ...exLine, ...(sz ? sz.lines : []), ...(sz && sz.instance_text ? [sz.instance_text] : []),
        ...(columnNotes(result, contract, preset, fields, result.page_state)[open.variant] || []).filter((n) => n.kind !== 'sizing').map((n) => n.text),
        ...(vr.state === 'ok' ? [] : segs.map((s) => `${s.operation ? `${OPERATION_LABELS[s.operation] || s.operation}: ` : ''}${cite(s.text, commit)}`))];
      return { kind: 'column', id: open.variant, title: `${variantLabel(open.variant)} notes`, name: null, description: vr.state === 'ok' ? null : cardSentence(open.variant, vr, contract, inputs, preset),
        measured_by: commit ? `Measured on ClickHouse ${commit}.` : null, lines };
    }
    if (open.kind === 'variant') {
      return { kind: 'variant', id: open.variant, title: variantLabel(open.variant), name: null, description: cardSentence(open.variant, vr, contract, inputs, preset),
        measured_by: commit ? `Measured on ClickHouse ${commit}.` : null, lines: segs.map((s) => `${s.operation ? `${OPERATION_LABELS[s.operation] || s.operation}: ` : ''}${cite(s.text, commit)}`) };
    }
    const assumed = assumedCoefs(contract);
    // Structural: the assumed registry rows the priced rows read, plus assumption sentences the Source carries.
    const lead = (used, extrapolated, source, inputsUsed) => {
      if (vr.state !== 'ok') return stateText(vr.state);
      if (used.length) return 'Priced from a stated assumption.';
      const n = (inputsUsed || []).length + (source || []).filter((l) => /\bASSUMPTION\b/.test(l)).length;
      const more = n === 0 ? '' : n === 1 ? ' and one assumption' : ` and ${n} assumptions`;
      return extrapolated ? `Priced, extrapolated outside the measured range${more}.` : `Priced from measured coefficients${more}.`;
    };
    const measuredBy = commit ? `Measured on ClickHouse ${commit}.` : null;
    if (open.kind === 'transfer') {
      const per = Object.entries(vr.cells || {}).map(([op, c]) => [op, transferMoney(c, preset)]).filter(([, m]) => m > 0);
      const total = per.reduce((s, [, m]) => s + m, 0);
      const used = vr.state === 'ok' ? transferAssumed(contract, open.variant, vr, assumed) : [];
      const lines = ['Replicas in different availability zones send parts to each other; each GB that crosses a zone is billed.'];
      if (vr.state === 'ok' && preset && !preset.structure_only) {
        lines.push(per.length ? `${variantLabel(open.variant)} pays ${fmtMoney(total, preset.currency)} a month: ${joinWords(per.map(([op, m]) => `${fmtMoney(m, preset.currency)} from ${opWord(op)}`))}.`
          : `${variantLabel(open.variant)} sends nothing across zones for this workload.`);
      }
      const source = [];
      for (const id of used) {
        const a = assumptionLines(contract, id, problems);
        lines.push(a.line);
        source.push(a.source);
      }
      const ex = vr.state === 'ok' && transferExtrapolated(vr);
      if (ex) lines.push('Some inputs lie outside the range the model was measured at, so this number is extrapolated.');
      const usedInputs = vr.state === 'ok' ? [...new Set(Object.entries(vr.cells || {}).flatMap(([o, c]) => assumedInputsUsed(contract, open.variant, o, c, 'transfer')))] : [];
      return { kind: 'transfer', id: open.variant, title: `${variantLabel(open.variant)} · ${OPERATION_LABELS.transfer}`, name: null, description: lead(used, ex, source, usedInputs), measured_by: measuredBy, lines, source };
    }
    const op = open.operation;
    const cell = vr.cells[op];
    if (vr.state !== 'ok' || !cell) {
      return { kind: 'cell', id: `${open.variant}.${op}`, title: `${variantLabel(open.variant)} · ${OPERATION_LABELS[op] || op}`, name: null,
        description: vr.state === 'ok' ? null : stateText(vr.state), measured_by: measuredBy, lines: segs.filter((s) => s.operation === op).map((s) => cite(s.text, commit)), source: [] };
    }
    const split = !!x.split;
    const used = assumedUsed(contract, open.variant, op, cell, assumed, split ? 'other' : 'all');
    const lines = [cellSentence(open.variant, op, cell, preset, result.page_state, split, x.fields)];
    const moved = split ? transferMoney(cell, preset) : 0;
    if (moved > 0) lines.push(`Its replica transfer, ${fmtMoney(moved, preset.currency)}, is in the Replica transfer row.`);
    const source = [];
    for (const id of used) {
      const a = assumptionLines(contract, id, problems);
      lines.push(a.line);
      source.push(a.source);
    }
    // The pipeline's own flag, without assumptions (said above) and, under `split`, without the transfer rows.
    const ex = cellExtrapolated(contract, open.variant, op, cell, new Set(), split, preset);
    if (ex) lines.push('Some inputs lie outside the range the model was measured at, so this number is extrapolated.');
    for (const n of vr.notes || []) if (n.kind === 'text' && n.operation === op) lines.push(n.text);
    for (const s of segs.filter((g) => g.operation === op)) source.push(cite(s.text, commit));
    for (const r of cell.rows || []) if (rowExtrapolated(r) && !(split && isTransferRow(r))) source.push(`${OP_KIND_LABELS[r.op_kind] || r.op_kind}: outside the measured range`);
    // The assumption is said above in words; its contract sentence names ids and record names.
    for (const n of cell.notes || []) if (!n.assumption) source.push(cite(sourceNote(contract, n), commit));
    return { kind: 'cell', id: `${open.variant}.${op}`, title: `${variantLabel(open.variant)} · ${OPERATION_LABELS[op] || op}`, name: null,
      description: lead(used, ex, source, assumedInputsUsed(contract, open.variant, op, cell, split ? 'other' : 'all')), measured_by: measuredBy, lines, source };
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
      // A formula from source is exact outside its verified range: the cell note says so, not an extrapolation note.
      if (d.basis === 'derived') continue;
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
      cards: [], tiered: null, table: { columns: [], rows: [] }, drill: null, chart: null, notes: [], inputs: null, info: null, evidence: null, problems,
    };
    vm.inputs = buildInputs(contract, fields, lockOrder || [], preset, u, pageState, problems);
    vm.notes = buildNotes(result, contract, fields, pageState);
    if (pageState === 'cannot_reproduce') return vm;
    const split = transferSplit(result, contract, preset, pageState);
    vm.cards = buildCards(result, contract, preset, pageState, vm.inputs, split);
    vm.tiered = buildTiered(result, preset, pageState, vm.inputs);
    vm.table = buildTable(result, contract, preset, pageState, problems, fields, split);
    vm.evidence = buildEvidence(contract);
    if (u.openCell) vm.drill = buildDrill(result, contract, preset, pageState, u.openCell, problems);
    if (u.info) vm.info = buildInfo(result, contract, vm.inputs, u.info, problems, preset, { split, scenario: u.scenario || null, fields });
    // A chart of money has nothing to draw without prices.
    if (pageState === 'ok') vm.chart = buildChart(result, contract, preset, u, problems, fields);
    return vm;
  }

  SCC.viewmodel = { build, buildTiered, assumptionSentence, plainUnit, fmtSig, fmtSize, plainReason, cite, cardSentence, reasonSegments, buildSizing, TIERS, GIB_NOTE, UNIT_CLASS_LABELS, OP_KIND_LABELS, fmtMoney, fmtPrice, fmtQuantity, cellText, unitClassOf, stateText, VARIANT_LABELS, OPERATION_LABELS, BLOCKS, STATE_TEXT };
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.viewmodel;
})(typeof window !== 'undefined' ? window : globalThis);
