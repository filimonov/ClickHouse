// Prices ledger rows with a billing preset and chooses gp3 volume layouts (billing/1 contract).
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const UNITS = ['decimal', 'binary_gb'];
  const GIB_IN_DECIMAL_GB = 1.073741824; // 2^30 / 10^9
  // accepted_dropped: the store answered ok and the reply to the caller was lost, so the provider bills it.
  // Every other class is billed unless the preset's `outcome_billing.free` list says otherwise: an entry with
  // `outcome_class` frees that whole class (server errors, unknown outcomes, requests the store never saw); an
  // entry without one is a client error matched by status and S3 code.
  const OUTCOMES = ['ok', 'accepted_dropped', 'client_error', 'server_error', 'dropped', 'not_sent'];
  const BILLED = new Set(['ok', 'accepted_dropped']);
  const EPS = 1e-9;

  // The preset's "GB" is 2^30 bytes under binary_gb and 10^9 bytes under decimal.
  function toPresetGb(gib, units) {
    if (units === 'binary_gb') return gib;
    if (units === 'decimal') return gib * GIB_IN_DECIMAL_GB;
    throw new Error(`unknown units ${JSON.stringify(units)}; expected one of ${UNITS.join(', ')}`);
  }

  const rowName = (r) => `${r.row_type} ${r.op_kind} ${r.outcome_class}`;

  function need(r, what) {
    if (typeof r.quantity !== 'number' || !Number.isFinite(r.quantity) || r.quantity < 0) {
      throw new Error(`row ${rowName(r)}: ${what} quantity must be a finite number >= 0, got ${JSON.stringify(r.quantity)}`);
    }
    return r.quantity;
  }

  // ---- requests ------------------------------------------------------------------------------

  function statusMatches(want, got) {
    if (want === got) return true;
    return typeof want === 'string' && /^\dXX$/.test(want) && Math.floor(got / 100) === Number(want[0]);
  }

  // A client error is free only when the row states a (status, s3_code) on the preset's free list;
  // an entry with a condition applies only when the row states the same condition.
  function isFreeOutcome(r, free) {
    if (r.status === undefined || r.status === null) return false;
    return free.some((e) => (e.outcome_class === undefined || e.outcome_class === 'client_error') && statusMatches(e.status, r.status)
      && (e.s3_code === null || e.s3_code === r.s3_code)
      && (e.condition === null || e.condition === r.condition));
  }

  const classFree = (cls, free) => free.some((e) => e.outcome_class === cls);
  function isBilled(cls, r, free) {
    if (BILLED.has(cls)) return true;
    if (cls === 'client_error') return !isFreeOutcome(r, free) && !classFree(cls, free);
    return !classFree(cls, free);
  }

  function billedFractions(r, outcomeMix, free) {
    if (r.outcome_class !== 'all') {
      if (!OUTCOMES.includes(r.outcome_class)) throw new Error(`row ${rowName(r)}: unknown outcome_class`);
      const billed = isBilled(r.outcome_class, r, free);
      return { billed: billed ? 1 : 0, excluded: { [r.outcome_class]: billed ? 0 : 1 } };
    }
    // Without a measured mix the billed share is unknown; assuming "all ok" would price errors that may be free.
    const mix = outcomeMix && outcomeMix[r.op_kind];
    if (!mix) throw new Error(`row ${rowName(r)}: an all-outcomes row needs an outcome mix for ${r.op_kind}, and none is given`);
    for (const [k, v] of Object.entries(mix)) {
      if (!OUTCOMES.includes(k) || typeof v !== 'number' || !Number.isFinite(v) || v < 0) {
        throw new Error(`outcome mix for ${r.op_kind}: ${k} must be one of ${OUTCOMES.join(', ')} with a number >= 0, got ${JSON.stringify(v)}`);
      }
    }
    const sum = OUTCOMES.reduce((s, k) => s + (mix[k] || 0), 0);
    if (Math.abs(sum - 1) > 1e-6) throw new Error(`outcome mix for ${r.op_kind} must sum to 1, got ${sum}`);
    const billed = OUTCOMES.reduce((s, k) => s + (isBilled(k, r, free) ? (mix[k] || 0) : 0), 0);
    const excluded = Object.fromEntries(OUTCOMES.filter((k) => !isBilled(k, r, free)).map((k) => [k, mix[k] || 0]));
    return { billed, excluded };
  }

  function requestLines(rows, preset, outcomeMix) {
    const classOf = new Map();
    for (const c of preset.request_classes) for (const op of c.ops) classOf.set(op, c);
    const free = preset.outcome_billing.free;
    const acc = new Map();
    for (const r of rows) {
      const c = classOf.get(r.op_kind);
      if (!c) throw new Error(`row ${rowName(r)}: op ${r.op_kind} is not in any request class of preset ${preset.id}`);
      const q = need(r, 'request');
      const f = billedFractions(r, outcomeMix, free);
      const a = acc.get(c.id) || { c, qty: 0, excl: {} };
      a.qty += q * f.billed;
      for (const [k, v] of Object.entries(f.excluded)) a.excl[k] = (a.excl[k] || 0) + q * v;
      acc.set(c.id, a);
    }
    return [...acc.values()].map(({ c, qty, excl }) => {
      const parts = Object.entries(excl).filter(([, v]) => v > 0).map(([k, v]) => `${v} ${k}`);
      return {
        rule_id: c.id, unit: 'requests', quantity: qty, money: (qty / 1000) * c.price_per_1000, tiers: null,
        note: `${c.price_per_1000} per 1000` + (parts.length ? `; not billed: ${parts.join(', ')}` : ''), source_url: c.source_url,
      };
    });
  }

  // ---- storage and transfer ------------------------------------------------------------------

  function storageLines(rows, preset) {
    const st = preset.storage;
    const byCat = {};
    let gib = 0;
    for (const r of rows) {
      if (!r.category) throw new Error(`row ${rowName(r)}: storage row needs a category`);
      if (r.category === 'incomplete' && st.incomplete_multipart !== 'as_storage') {
        throw new Error(`preset ${preset.id}: incomplete_multipart ${JSON.stringify(st.incomplete_multipart)} is not supported; only as_storage`);
      }
      byCat[r.category] = (byCat[r.category] || 0) + need(r, 'storage');
      gib += r.quantity;
    }
    const gb = toPresetGb(gib, preset.units);
    const tiers = [];
    let from = 0;
    let money = 0;
    for (const t of st.tiers) {
      const upTo = t.up_to_gb === null ? Infinity : t.up_to_gb;
      const part = Math.max(0, Math.min(gb, upTo) - from);
      if (part > 0) tiers.push({ from_gb: from, up_to_gb: t.up_to_gb, gb: part, price_per_gb_month: t.price_per_gb_month, money: part * t.price_per_gb_month });
      money += part * t.price_per_gb_month;
      from = upTo;
    }
    const note = Object.entries(byCat).map(([k, v]) => `${k} ${v} GiB-months`).join(', ') + (byCat.incomplete ? '; incomplete billed as storage' : '');
    return [{ rule_id: 'storage', unit: 'GB-month', quantity: gb, money, tiers, note, source_url: st.source_url, by_category: byCat }];
  }

  // `each_direction`: the provider charges the sending side and the receiving side, so each GB moved is billed twice.
  const TRANSFER_LEGS = { both: 1, out: 1, in: 1, each_direction: 2 };

  function transferLines(rows, preset) {
    const acc = new Map();
    for (const r of rows) {
      const rule = preset.transfer.find((t) => t.id === r.route);
      if (!r.route) throw new Error(`row ${rowName(r)}: transfer row needs a route`);
      if (!rule) throw new Error(`row ${rowName(r)}: unknown route ${r.route}`);
      acc.set(r.route, (acc.get(r.route) || 0) + need(r, 'transfer'));
    }
    return [...acc].map(([id, gib]) => {
      const rule = preset.transfer.find((t) => t.id === id);
      const gb = toPresetGb(gib, preset.units);
      if (rule.price_per_gb === null) {
        if (id !== 'internet') throw new Error(`route ${id}: price_per_gb is null; only internet may be unpriced`);
        return { rule_id: id, unit: 'GB', quantity: gb, money: 0, tiers: null, note: `egress not priced: ${rule.note}`, source_url: rule.source_url };
      }
      const legs = TRANSFER_LEGS[rule.direction];
      if (legs === undefined) throw new Error(`route ${id}: unknown direction ${JSON.stringify(rule.direction)}; expected one of ${Object.keys(TRANSFER_LEGS).join(', ')}`);
      const note = legs === 2 ? `${rule.price_per_gb} per GB in each direction: ${gb} GB billed out and in` : `${rule.price_per_gb} per GB, ${rule.direction}`;
      return { rule_id: id, unit: 'GB', quantity: gb, money: gb * legs * rule.price_per_gb, tiers: null, note, source_url: rule.source_url };
    });
  }

  // ---- block volumes -------------------------------------------------------------------------

  const volumeCost = (g, v) => v.size_gib * g.price_per_gib_month + Math.max(0, v.iops - g.iops_baseline) * g.iops_price
    + Math.max(0, v.mibps - g.mibps_baseline) * g.mibps_price;

  function sizeLayout(demand, g, layout) {
    const n = layout.volumes;
    const iops = Math.max(g.iops_baseline, Math.ceil(demand.peak_iops / n), Math.ceil(demand.peak_mibps / n / g.mibps_per_iops_max));
    const mibps = Math.max(g.mibps_baseline, Math.ceil(demand.peak_mibps / n));
    // A volume may be grown to lift the IOPS-per-GiB cap, so size is not a constraint on iops.
    const size = Math.max(Math.ceil(demand.capacity_gib / n), Math.ceil(iops / g.iops_per_gib_max), g.size_gib[0]);
    const why = [];
    if (size > g.size_gib[1]) why.push(`size_gib ${size} > ${g.size_gib[1]}`);
    if (iops > g.iops[1]) why.push(`iops ${iops} > ${g.iops[1]}`);
    if (mibps > g.mibps[1]) why.push(`mibps ${mibps} > ${g.mibps[1]}`);
    return { layout: layout.id, volumes: n, size_gib: size, iops, mibps, why };
  }

  function checkManual(m, g) {
    const why = [];
    const inRange = (name, v, [lo, hi]) => { if (!(v >= lo && v <= hi)) why.push(`${name} range ${lo}..${hi}: ${v}`); };
    if (!Number.isInteger(m.volumes) || m.volumes < 1) why.push(`volumes must be an integer >= 1: ${m.volumes}`);
    inRange('size_gib', m.size_gib, g.size_gib);
    inRange('iops', m.iops, g.iops);
    inRange('mibps', m.mibps, g.mibps);
    if (m.iops > m.size_gib * g.iops_per_gib_max) why.push(`iops ${m.iops} > size_gib ${m.size_gib} x iops_per_gib_max ${g.iops_per_gib_max}`);
    if (m.mibps > m.iops * g.mibps_per_iops_max) why.push(`mibps ${m.mibps} > iops ${m.iops} x mibps_per_iops_max ${g.mibps_per_iops_max}`);
    return why;
  }

  function allocateVolumes(demand, preset, layouts, manual) {
    for (const k of ['capacity_gib', 'peak_iops', 'peak_mibps']) {
      if (typeof demand[k] !== 'number' || !Number.isFinite(demand[k]) || demand[k] < 0) throw new Error(`demand.${k} must be a finite number >= 0, got ${demand[k]}`);
    }
    const g = preset.block.gp3;
    const lays = layouts || preset.allocation.layouts;
    const sized = lays.map((l) => sizeLayout(demand, g, l));
    const feasible = sized.filter((s) => s.why.length === 0).map((s) => Object.assign(s, { monthly_cost: s.volumes * volumeCost(g, s) }));
    const rejected = sized.filter((s) => s.why.length > 0).map((s) => ({ layout: s.layout, reason: s.why.join('; ') }));
    const alternatives = feasible.map(({ layout, volumes, size_gib, iops, mibps, monthly_cost }) => ({ layout, volumes, size_gib, iops, mibps, monthly_cost }));
    const keys = { fewest_volumes: (s) => s.volumes, smallest_total_iops: (s) => s.volumes * s.iops };
    const order = [(s) => s.monthly_cost, ...preset.allocation.tie_break.map((t) => {
      if (!keys[t]) throw new Error(`unknown tie_break ${t}`);
      return keys[t];
    })];
    const cmp = (a, b) => {
      for (const f of order) { const d = f(a) - f(b); if (Math.abs(d) > EPS) return d; }
      return 0;
    };

    if (manual) {
      const why = checkManual(manual, g);
      if (why.length) return { layout: null, volumes: manual.volumes, feasible: false, manual: true, notes: why, alternatives, rejected };
      const n = manual.volumes;
      const below = [];
      if (n * manual.size_gib < demand.capacity_gib) below.push(`capacity ${n * manual.size_gib} GiB < ${demand.capacity_gib}`);
      if (n * manual.iops < demand.peak_iops) below.push(`iops ${n * manual.iops} < ${demand.peak_iops}`);
      if (n * manual.mibps < demand.peak_mibps) below.push(`mibps ${n * manual.mibps} < ${demand.peak_mibps}`);
      return Object.assign({ layout: null, feasible: true, manual: true, notes: below.length ? [`below demand: ${below.join(', ')}`] : [], alternatives, rejected },
        { volumes: n, size_gib: manual.size_gib, iops: manual.iops, mibps: manual.mibps, monthly_cost: n * volumeCost(g, manual) });
    }
    if (feasible.length === 0) {
      return { layout: null, volumes: null, feasible: false, manual: false, notes: rejected.map((r) => `${r.layout}: ${r.reason}`), alternatives, rejected };
    }
    const best = [...feasible].sort(cmp)[0];
    const { why, ...pick } = best;
    return Object.assign(pick, { feasible: true, manual: false, notes: [], alternatives, rejected });
  }

  function replicasOf(opts) {
    const n = opts && opts.replicas;
    if (!Number.isInteger(n) || n < 1) throw new Error(`block rows need opts.replicas, an integer >= 1, got ${JSON.stringify(n)}`);
    return n;
  }

  // Block rows describe one replica; every block line is that replica's bill times the replica count.
  function blockLines(rows, preset, manual, opts) {
    const replicas = replicasOf(opts);
    const demand = { capacity_gib: 0, peak_iops: 0, peak_mibps: 0 };
    const field = { capacity: 'capacity_gib', iops: 'peak_iops', mibps: 'peak_mibps' };
    for (const r of rows) {
      const f = field[r.op_kind];
      const typeOk = r.row_type === 'block_capacity' ? r.op_kind === 'capacity' : r.op_kind !== 'capacity';
      if (!f || !typeOk) throw new Error(`row ${rowName(r)}: block rows need op_kind capacity (block_capacity) or iops/mibps (block_io)`);
      demand[f] += need(r, 'block');
    }
    const alloc = allocateVolumes(demand, preset, undefined, manual);
    if (!alloc.feasible) throw new Error(`no feasible volume allocation: ${alloc.notes.join('; ')}`);
    const g = preset.block.gp3;
    const n = alloc.volumes;
    const base = { tiers: null, source_url: g.source_url };
    const label = alloc.manual ? 'manual' : alloc.layout;
    const perReplica = `${n} x ${alloc.size_gib} GiB, ${alloc.iops} IOPS, ${alloc.mibps} MiB/s`;
    const note = `${label}: ${replicas} ${replicas === 1 ? 'replica' : 'replicas'} × (${perReplica})`;
    const iopsAbove = replicas * n * Math.max(0, alloc.iops - g.iops_baseline);
    const mibpsAbove = replicas * n * Math.max(0, alloc.mibps - g.mibps_baseline);
    const capacity = replicas * n * alloc.size_gib;
    return {
      allocation: Object.assign({ demand }, alloc, { replicas, note, per_replica: Object.assign({ note: `${label}: ${perReplica}` }, alloc) }),
      lines: [
        Object.assign({ rule_id: 'block_capacity', unit: 'GiB-month', quantity: capacity, money: capacity * g.price_per_gib_month, note }, base),
        Object.assign({ rule_id: 'block_iops', unit: 'IOPS-month above baseline', quantity: iopsAbove, money: iopsAbove * g.iops_price, note: `baseline ${g.iops_baseline} per volume` }, base),
        Object.assign({ rule_id: 'block_mibps', unit: 'MiB/s-month above baseline', quantity: mibpsAbove, money: mibpsAbove * g.mibps_price, note: `baseline ${g.mibps_baseline} per volume` }, base),
      ],
    };
  }

  // Bills the rows whose scope equals `scope`; the count of other rows is returned as `skipped`.
  function bill(rows, preset, scope, outcomeMix, manual, opts) {
    const sc = scope || 'variant';
    const mine = rows.filter((r) => r.scope === sc);
    const by = (t) => mine.filter((r) => r.row_type === t);
    const known = ['request', 'storage', 'transfer', 'block_io', 'block_capacity'];
    const stray = mine.find((r) => !known.includes(r.row_type));
    if (stray) throw new Error(`row ${rowName(stray)}: unknown row_type`);
    const block = by('block_io').length + by('block_capacity').length > 0
      ? blockLines([...by('block_capacity'), ...by('block_io')], preset, manual, opts) : { lines: [], allocation: null };
    const lines = [...requestLines(by('request'), preset, outcomeMix || null), ...(by('storage').length ? storageLines(by('storage'), preset) : []),
      ...transferLines(by('transfer'), preset), ...block.lines].map((l) => Object.assign(l, { scope: sc }));
    return { lines, total: lines.reduce((s, l) => s + l.money, 0), allocation: block.allocation, skipped: rows.length - mine.length };
  }

  // Bills each scope present in `rows` once, so tiers, free allowances and baselines see the variant's aggregate.
  function billVariant(rows, preset, outcomeMix, manual, opts) {
    const lines = [];
    let provisioning = null;
    for (const scope of [...new Set(rows.map((r) => r.scope))]) {
      const b = bill(rows, preset, scope, outcomeMix, manual, opts);
      lines.push(...b.lines);
      if (b.allocation) {
        if (provisioning) throw new Error('block rows in more than one scope');
        provisioning = Object.assign({ scope }, b.allocation);
      }
    }
    return { lines, total: lines.reduce((s, l) => s + l.money, 0), provisioning };
  }

  // Every price a priced preset bills with must be a finite number >= 0; a missing price never bills as $0.
  // Storage tiers ascend and end with an open tier (`up_to_gb: null`). Only the internet route may be unpriced.
  function priceProblems(preset) {
    const out = [];
    if (!preset || typeof preset !== 'object') return ['price preset: expected an object'];
    if (preset.structure_only) return out;
    const price = (v, where) => { if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) out.push(`${where}: expected a finite number >= 0, got ${JSON.stringify(v === undefined ? null : v)}`); };
    const id = preset.id || '?';
    for (const [i, c] of (Array.isArray(preset.request_classes) ? preset.request_classes : []).entries()) price(c && c.price_per_1000, `${id}.request_classes[${i}]${c && c.id ? ` (${c.id})` : ''}.price_per_1000`);
    if (!Array.isArray(preset.request_classes)) out.push(`${id}.request_classes: expected a list`);
    const tiers = preset.storage && Array.isArray(preset.storage.tiers) ? preset.storage.tiers : null;
    if (!tiers || !tiers.length) out.push(`${id}.storage.tiers: expected a non-empty list`);
    else {
      let prev = 0;
      tiers.forEach((t, i) => {
        price(t && t.price_per_gb_month, `${id}.storage.tiers[${i}].price_per_gb_month`);
        const up = t ? t.up_to_gb : undefined;
        const last = i === tiers.length - 1;
        if (last && up !== null) out.push(`${id}.storage.tiers[${i}].up_to_gb: the last tier must be open (null), got ${JSON.stringify(up === undefined ? null : up)}`);
        if (!last && !(typeof up === 'number' && Number.isFinite(up) && up > prev)) out.push(`${id}.storage.tiers[${i}].up_to_gb: expected a number above ${prev}, got ${JSON.stringify(up === undefined ? null : up)}`);
        if (typeof up === 'number') prev = up;
      });
    }
    for (const [i, r] of (Array.isArray(preset.transfer) ? preset.transfer : []).entries()) {
      if (r && r.id === 'internet' && r.price_per_gb === null) continue;
      price(r && r.price_per_gb, `${id}.transfer[${i}]${r && r.id ? ` (${r.id})` : ''}.price_per_gb`);
    }
    const g = preset.block && preset.block.gp3;
    if (!g) out.push(`${id}.block.gp3: missing`);
    else for (const k of ['price_per_gib_month', 'iops_price', 'mibps_price', 'iops_baseline', 'mibps_baseline']) price(g[k], `${id}.block.gp3.${k}`);
    return out;
  }

  const blockRowOf = (ruleId, r) => (ruleId === 'block_capacity' ? r.row_type === 'block_capacity' : r.row_type === 'block_io');

  // Quantity a cell contributes to one aggregate billing line; allocation is proportional to it.
  function cellQuantity(line, rows, preset, outcomeMix) {
    const mine = rows.filter((r) => r.scope === line.scope);
    const sum = (xs) => xs.reduce((s, r) => s + r.quantity, 0);
    if (line.rule_id === 'storage') return sum(mine.filter((r) => r.row_type === 'storage'));
    if (line.rule_id === 'block_capacity') return sum(mine.filter((r) => r.op_kind === 'capacity'));
    if (line.rule_id === 'block_iops') return sum(mine.filter((r) => r.op_kind === 'iops'));
    if (line.rule_id === 'block_mibps') return sum(mine.filter((r) => r.op_kind === 'mibps'));
    const transfer = mine.filter((r) => r.row_type === 'transfer' && r.route === line.rule_id);
    if (transfer.length) return sum(transfer);
    const req = mine.filter((r) => r.row_type === 'request');
    if (!req.length) return 0;
    const own = bill(req, preset, line.scope, outcomeMix || null).lines.find((l) => l.rule_id === line.rule_id);
    return own ? own.quantity : 0;
  }

  // Allocates each aggregate line's money back to `cells` ([{key, name, rows, shares}]) by each cell's own quantity
  // of the line (spec §4.4). A cell that declares `shares` (cell name -> weight) passes its portion of the lines it
  // produced on to those cells by weight; lines it did not produce are untouched.
  function allocate(lines, cells, preset, outcomeMix) {
    const problems = [];
    const keyOf = new Map(cells.map((c) => [c.name, c.key]));
    for (const c of cells) {
      for (const name of Object.keys(c.shares || {})) if (!keyOf.has(name)) problems.push(`allocation share for unknown cell ${name}`);
    }
    const allocation = lines.map((l) => {
      let own = Object.fromEntries(cells.map((c) => [c.key, cellQuantity(l, c.rows, preset, outcomeMix)]));
      let sum = Object.values(own).reduce((s, w) => s + w, 0);
      // A volume has a minimum size, so a zero demand still costs money: the cells that declared block rows share it equally.
      if (sum <= 0 && l.rule_id.startsWith('block_')) {
        own = Object.fromEntries(cells.map((c) => [c.key, c.rows.filter((r) => r.scope === l.scope && blockRowOf(l.rule_id, r)).length]));
        sum = Object.values(own).reduce((s, w) => s + w, 0);
      }
      if (sum <= 0 && l.money > EPS) problems.push(`${l.rule_id} money ${l.money} has no cell to allocate to`);
      // Fractions come from quantities, so a zero-priced line still shows who used it.
      const frac = Object.fromEntries(cells.map((c) => [c.key, 0]));
      let redirected = false;
      for (const c of cells) {
        const part = sum > 0 ? own[c.key] / sum : 0;
        const declared = Object.entries(c.shares || {}).filter(([name]) => keyOf.has(name));
        if (own[c.key] <= 0 || declared.length === 0) { frac[c.key] += part; continue; }
        const total = declared.reduce((s, [, w]) => s + w, 0);
        if (!(total > 0)) { problems.push(`${c.name}: allocation shares sum to ${total}`); frac[c.key] += part; continue; }
        redirected = true;
        for (const [name, w] of declared) frac[keyOf.get(name)] += part * w / total;
      }
      const shares = Object.fromEntries(cells.map((c) => [c.key, { weight: own[c.key], fraction: frac[c.key], money: l.money * frac[c.key] }]));
      return { rule_id: l.rule_id, scope: l.scope, money: l.money, basis: redirected ? 'allocation.shares' : 'quantity', shares };
    });
    const byCell = {};
    for (const c of cells) {
      const mine = allocation.map((a, i) => {
        const line = lines[i];
        const share = a.shares[c.key];
        return { rule_id: a.rule_id, scope: a.scope, unit: line.unit, quantity: line.quantity * share.fraction, money: share.money,
          share: share.fraction, aggregate_quantity: line.quantity, aggregate_money: a.money, basis: a.basis, note: line.note };
      }).filter((l) => l.share > 0);
      byCell[c.key] = { lines: mine, money: mine.reduce((s, l) => s + l.money, 0) };
    }
    return { allocation, byCell, problems };
  }

  // The smallest instance class (lowest max_mibps, list order on ties) whose EBS bandwidth carries the allocated
  // throughput of all volumes; null class with a reason when none does or the preset lists none.
  function instanceClass(provisioning, preset) {
    const classes = preset.ec2_ebs_bandwidth;
    if (!Array.isArray(classes) || classes.length === 0) return { instance_class: null, throughput_mibps: null, reason: `preset ${preset.id} lists no ec2_ebs_bandwidth` };
    const throughput = provisioning.volumes * provisioning.mibps;
    const fit = classes.filter((c) => c.max_mibps >= throughput);
    if (!fit.length) {
      return { instance_class: null, throughput_mibps: throughput, reason: `no instance class in preset ${preset.id} carries ${throughput} MiB/s of EBS bandwidth` };
    }
    const best = fit.reduce((a, c) => (c.max_mibps < a.max_mibps ? c : a));
    return { instance_class: best.instance_class, max_mibps: best.max_mibps, throughput_mibps: throughput, source_url: best.source_url, reason: null };
  }

  const billing = { bill, billVariant, allocate, instanceClass, allocateVolumes, toPresetGb, priceProblems, UNITS };
  root.SCC = root.SCC || {};
  root.SCC.billing = billing;
  if (typeof module !== 'undefined' && module.exports) module.exports = billing;
})(typeof window !== 'undefined' ? window : globalThis);
