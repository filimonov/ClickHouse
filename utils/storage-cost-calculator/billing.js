// Prices ledger rows with a billing preset and chooses gp3 volume layouts (billing/1 contract).
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const UNITS = ['decimal', 'binary_gb'];
  const GIB_IN_DECIMAL_GB = 1.073741824; // 2^30 / 10^9
  // accepted_dropped: the store answered ok and the reply to the caller was lost, so the provider bills it.
  // dropped: the store's outcome is unknown, so there is no billable fact. not_sent: the store never saw it.
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
    return free.some((e) => statusMatches(e.status, r.status)
      && (e.s3_code === null || e.s3_code === r.s3_code)
      && (e.condition === null || e.condition === r.condition));
  }

  function billedFractions(r, outcomeMix, free) {
    if (r.outcome_class !== 'all') {
      if (!OUTCOMES.includes(r.outcome_class)) throw new Error(`row ${rowName(r)}: unknown outcome_class`);
      const billed = BILLED.has(r.outcome_class) || (r.outcome_class === 'client_error' && !isFreeOutcome(r, free));
      return { billed: billed ? 1 : 0, excluded: { [r.outcome_class]: billed ? 0 : 1 } };
    }
    const mix = (outcomeMix && outcomeMix[r.op_kind]) || { ok: 1 };
    for (const [k, v] of Object.entries(mix)) {
      if (!OUTCOMES.includes(k) || typeof v !== 'number' || !Number.isFinite(v) || v < 0) {
        throw new Error(`outcome mix for ${r.op_kind}: ${k} must be one of ${OUTCOMES.join(', ')} with a number >= 0, got ${JSON.stringify(v)}`);
      }
    }
    const sum = OUTCOMES.reduce((s, k) => s + (mix[k] || 0), 0);
    if (Math.abs(sum - 1) > 1e-6) throw new Error(`outcome mix for ${r.op_kind} must sum to 1, got ${sum}`);
    const billed = (mix.ok || 0) + (mix.accepted_dropped || 0) + (isFreeOutcome(r, free) ? 0 : (mix.client_error || 0));
    return { billed, excluded: { client_error: isFreeOutcome(r, free) ? (mix.client_error || 0) : 0, server_error: mix.server_error || 0, dropped: mix.dropped || 0 } };
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
      return { rule_id: id, unit: 'GB', quantity: gb, money: gb * rule.price_per_gb, tiers: null, note: `${rule.price_per_gb} per GB, ${rule.direction}`, source_url: rule.source_url };
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

  function blockLines(rows, preset, manual) {
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
    const note = `${alloc.manual ? 'manual' : alloc.layout}: ${n} x (${alloc.size_gib} GiB, ${alloc.iops} IOPS, ${alloc.mibps} MiB/s)`;
    const iopsAbove = n * Math.max(0, alloc.iops - g.iops_baseline);
    const mibpsAbove = n * Math.max(0, alloc.mibps - g.mibps_baseline);
    return {
      allocation: Object.assign({ demand }, alloc),
      lines: [
        Object.assign({ rule_id: 'block_capacity', unit: 'GiB-month', quantity: n * alloc.size_gib, money: n * alloc.size_gib * g.price_per_gib_month, note }, base),
        Object.assign({ rule_id: 'block_iops', unit: 'IOPS-month above baseline', quantity: iopsAbove, money: iopsAbove * g.iops_price, note: `baseline ${g.iops_baseline} per volume` }, base),
        Object.assign({ rule_id: 'block_mibps', unit: 'MiB/s-month above baseline', quantity: mibpsAbove, money: mibpsAbove * g.mibps_price, note: `baseline ${g.mibps_baseline} per volume` }, base),
      ],
    };
  }

  // Bills the rows whose scope equals `scope`; the count of other rows is returned as `skipped`.
  function bill(rows, preset, scope, outcomeMix, manual) {
    const sc = scope || 'variant';
    const mine = rows.filter((r) => r.scope === sc);
    const by = (t) => mine.filter((r) => r.row_type === t);
    const known = ['request', 'storage', 'transfer', 'block_io', 'block_capacity'];
    const stray = mine.find((r) => !known.includes(r.row_type));
    if (stray) throw new Error(`row ${rowName(stray)}: unknown row_type`);
    const block = by('block_io').length + by('block_capacity').length > 0
      ? blockLines([...by('block_capacity'), ...by('block_io')], preset, manual) : { lines: [], allocation: null };
    const lines = [...requestLines(by('request'), preset, outcomeMix || null), ...(by('storage').length ? storageLines(by('storage'), preset) : []),
      ...transferLines(by('transfer'), preset), ...block.lines].map((l) => Object.assign(l, { scope: sc }));
    return { lines, total: lines.reduce((s, l) => s + l.money, 0), allocation: block.allocation, skipped: rows.length - mine.length };
  }

  const billing = { bill, allocateVolumes, toPresetGb, UNITS };
  root.SCC = root.SCC || {};
  root.SCC.billing = billing;
  if (typeof module !== 'undefined' && module.exports) module.exports = billing;
})(typeof window !== 'undefined' ? window : globalThis);
