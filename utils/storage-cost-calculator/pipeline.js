// Input pipeline: raw values, precedence-group locks, overrides and derivations -> typed model inputs.
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const SCC = root.SCC || (root.SCC = {});
  const model = () => root.SCC.model || require('./model.js');

  // A month is 730 h everywhere in the calculator (spec §4.2), so 730 / 24 days, not the calendar 30.4375.
  const DAYS_PER_MONTH = 730 / 24;
  const PER_DAY = {
    second: 86400, minute: 1440, hour: 24, day: 1, month: 1 / DAYS_PER_MONTH,
  };

  // Converts a rate given per `unit` to the per-day rate the registry stores.
  function normaliseRate(value, unit) {
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`rate must be a finite number, got ${JSON.stringify(value)}`);
    if (!Object.prototype.hasOwnProperty.call(PER_DAY, unit)) {
      throw new Error(`unknown rate unit ${unit}; expected one of ${Object.keys(PER_DAY).join(', ')}`);
    }
    return value * PER_DAY[unit];
  }

  // The inverse of normaliseRate: a per-day rate shown per `unit`.
  function denormaliseRate(perDay, unit) {
    return perDay / normaliseRate(1, unit);
  }

  // Whole counts of at least one: servers, and the columns of a table.
  // An INSERT carries at least one row and touches at least one partition; a block holds at least one row.
  const COUNT_INPUTS = ['replicas', 'shards', 'columns', 'insert_rows', 'partitions_per_insert', 'insert_data_block_rows'];
  // Averages of at least one: every column writes at least one stream (an average over columns need not be whole), a
  // query reads at least one stream of a part, and a row ends up in a part at least as large as its inserted part.
  const AT_LEAST_ONE_INPUTS = ['streams_per_column', 'read_streams_per_part', 'merge_size_ratio'];
  // Sizes and periods of a setting: 0 has no meaning (a GC round every 0 s, a blob of 0 bytes), so it is not priced.
  const POSITIVE_INPUTS = ['insert_bytes_uncompressed', 'cas_gc_interval_s', 'cas_blob_bytes', 'cas_table_cleanup_period_s'];
  // The page models one shard: it holds at most 200 TiB, one day cannot add more than that, and data is kept at most
  // 100 years. Above a bound the input is refused, whether typed or derived from its group partners.
  const INPUT_LIMITS = {
    data_bytes: { max: 204800, why: 'a single shard holds at most 200 TiB; multi-shard is out of scope' },
    ingest_rate: { max: 204800, why: 'one day cannot add more than a 200 TiB shard holds' },
    retention_days: { max: 36500, why: 'retention is at most 100 years' },
  };
  // A `<name>_fraction` of unit 1 is a share of a whole: a value above 1 would price negative requests.
  const isFraction = (q) => q.unit === '1' && /_fraction$/.test(q.id);
  const isRate = (q) => typeof q.unit === 'string' && q.unit.endsWith('/day');
  const isFiniteNumber = (v) => typeof v === 'number' && Number.isFinite(v);

  // Returns {value} or {problem}. A rate may arrive as {value, per}. An enum that lists `auto` also takes an integer
  // >= 1 (volumes_per_replica); the allocator validates the number against the volume limits.
  function parseValue(q, v) {
    if (isRate(q) && v !== null && typeof v === 'object') {
      try {
        const x = normaliseRate(v.value, v.per);
        return x < 0 ? { problem: `${q.id}: expected a rate of 0 or more, got ${v.value} per ${v.per}` } : { value: x };
      } catch (e) {
        return { problem: `${q.id}: ${e.message}` };
      }
    }
    if (v !== null && typeof v === 'object') return { problem: `${q.id}: ${q.unit || 'value'} is not a rate; expected a plain value, got an object` };
    if (q.value_type === 'number') {
      if (!isFiniteNumber(v)) return { problem: `${q.id}: expected a finite number, got ${JSON.stringify(v)}` };
      // Counts are whole and at least one; no workload quantity is negative.
      if (COUNT_INPUTS.includes(q.id) && !(Number.isInteger(v) && v >= 1)) return { problem: `${q.id}: expected a whole number of 1 or more, got ${v}` };
      if (AT_LEAST_ONE_INPUTS.includes(q.id) && !(v >= 1)) return { problem: `${q.id}: expected a number of 1 or more, got ${v}` };
      if (POSITIVE_INPUTS.includes(q.id) && !(v > 0)) return { problem: `${q.id}: expected a number above 0, got ${v}` };
      if (v < 0) return { problem: `${q.id}: expected a number of 0 or more, got ${v}` };
      if (isFraction(q) && v > 1) return { problem: `${q.id}: expected a fraction from 0 to 1, got ${v}` };
      return { value: v };
    }
    if (q.value_type === 'enum') {
      if (typeof v === 'string' && q.enum.includes(v)) return { value: v };
      if (q.enum.includes('auto') && Number.isInteger(v) && v >= 1) return { value: v };
      return { problem: `${q.id}: ${JSON.stringify(v)} is not one of ${q.enum.join(', ')}` };
    }
    return { problem: `${q.id}: value_type ${q.value_type} cannot be set as an input` };
  }

  // Returns the lock order per group, newest last, in precedence-group order. A group with n members holds n-1 locks.
  function normaliseLocks(contract, lockOrder, problems) {
    const groups = contract.precedence;
    const owner = new Map();
    groups.forEach((g, i) => g.group.forEach((id) => owner.set(id, i)));
    const per = groups.map(() => []);
    for (const id of Array.isArray(lockOrder) ? lockOrder : []) {
      if (!owner.has(id)) { problems.push(`lockOrder: ${id} is not in a precedence group`); continue; }
      const list = per[owner.get(id)];
      if (list.includes(id)) { problems.push(`lockOrder: ${id} is listed twice`); continue; }
      list.push(id);
    }
    return groups.map((g, i) => {
      const want = g.group.length - 1;
      let list = per[i];
      if (list.length > want) {
        problems.push(`lockOrder: group ${g.group.join('/')} has more than ${want} locks; keeping the newest ${want}`);
        list = list.slice(list.length - want);
      }
      // A short list is an unset state: the older slots fall to the group's default locks.
      const fill = [...g.default_locked, ...g.group].filter((id, k, a) => a.indexOf(id) === k && !list.includes(id));
      return [...fill.slice(0, want - list.length), ...list];
    });
  }

  // Locks `id`, releasing the oldest lock of its group; an already locked member becomes the newest lock.
  function lockMember(contract, lockOrder, id) {
    const gi = contract.precedence.findIndex((g) => g.group.includes(id));
    if (gi < 0) throw new Error(`${id} is not in a precedence group`);
    const problems = [];
    const per = normaliseLocks(contract, lockOrder, problems);
    if (problems.length) throw new Error(problems.join('; '));
    const list = per[gi];
    if (list.includes(id)) list.splice(list.indexOf(id), 1);
    else list.shift();
    list.push(id);
    return per.flat();
  }

  function resolveInputs(contract, raw, lockOrder, overrides) {
    const problems = [];
    const byId = new Map(contract.registry.map((x) => [x.id, x]));
    const inputs = contract.registry.filter((x) => x.kind === 'input');
    const isInput = (id) => byId.has(id) && byId.get(id).kind === 'input';
    const groups = contract.precedence;
    for (const g of groups) {
      for (const id of g.group) if (!isInput(id)) problems.push(`precedence group member ${id} is not a registry input`);
    }
    const groupOf = new Map();
    groups.forEach((g, i) => g.group.forEach((id) => groupOf.set(id, i)));

    // Raw values (presets, simple inputs) over registry defaults. An invalid raw value leaves the input without a value.
    const values = new Map();
    for (const x of inputs) if (x.default !== null && x.default !== undefined) values.set(x.id, x.default);
    for (const [id, v] of Object.entries(raw || {})) {
      if (!isInput(id)) { problems.push(`raw input ${id} is not a registry input`); continue; }
      const p = parseValue(byId.get(id), v);
      if (p.problem) { problems.push(p.problem); values.delete(id); } else values.set(id, p.value);
    }

    // An enum input with registry `presets` (shape narrow: columns 5, ...) sets the quantities that neither the
    // caller's raw values nor its overrides set. A value the caller gave always wins, even an invalid one.
    const setByCaller = new Set([...Object.keys(raw || {}), ...Object.keys(overrides || {})]);
    const presetOf = new Map();
    for (const x of inputs) {
      if (!x.presets || !values.has(x.id)) continue;
      const v = values.get(x.id);
      const preset = x.presets[v];
      if (!preset || typeof preset !== 'object') { problems.push(`${x.id} = ${JSON.stringify(v)} has no preset`); continue; }
      for (const [id, pv] of Object.entries(preset)) {
        if (setByCaller.has(id)) continue;
        if (!isInput(id)) { problems.push(`${x.id} preset ${v}: ${id} is not a registry input`); continue; }
        if (presetOf.has(id)) { problems.push(`${id} is set by both ${presetOf.get(id)} and ${x.id} = ${v}`); continue; }
        const p = parseValue(byId.get(id), pv);
        if (p.problem) { problems.push(`${x.id} preset ${v}: ${p.problem}`); values.delete(id); continue; }
        values.set(id, p.value);
        presetOf.set(id, `${x.id} = ${v}`);
      }
    }

    const locks = normaliseLocks(contract, lockOrder, problems);
    const persisted = locks.flat();
    const eff = locks.map((l) => l.slice());
    const overridden = new Set();
    // An override locks its member and releases the oldest lock that is not itself an override.
    // Applied in group member order: URL round-trips do not preserve key order, and the result must not depend on it.
    const memberRank = new Map(groups.flatMap((g) => g.group).map((id, k) => [id, k]));
    const rank = (id) => (memberRank.has(id) ? memberRank.get(id) : Infinity);
    const ordered = Object.entries(overrides || {}).sort((a, b) => rank(a[0]) - rank(b[0]));
    for (const [id, v] of ordered) {
      if (!isInput(id)) { problems.push(`override ${id} is not a registry input`); continue; }
      const p = parseValue(byId.get(id), v);
      if (p.problem) { problems.push(`override ${p.problem}`); continue; }
      if (groupOf.has(id)) {
        const list = eff[groupOf.get(id)];
        if (!list.includes(id)) {
          const victim = list.find((m) => !overridden.has(m));
          if (victim === undefined) {
            problems.push(`override ${id}: every lock of group ${groups[groupOf.get(id)].group.join('/')} is already an override; ${id} is not applied`);
            continue;
          }
          list.splice(list.indexOf(victim), 1);
          list.push(id);
        }
      }
      values.set(id, p.value);
      overridden.add(id);
    }

    // Unlocked members are derived from the locked ones by the group's rule, evaluated by the model.
    const derivedIds = new Set();
    const ruleText = new Map();
    groups.forEach((g, i) => {
      const unlocked = g.group.filter((id) => !eff[i].includes(id));
      for (const id of g.group) if (g.rules && g.rules[id]) ruleText.set(id, model().render(g.rules[id], contract.registry));
      for (const id of eff[i]) {
        if (!values.has(id)) problems.push(`group member ${id} is locked but has no value`);
      }
      for (const id of unlocked) { derivedIds.add(id); values.delete(id); }
      for (const id of unlocked) {
        const rule = g.rules && g.rules[id];
        if (!rule) { problems.push(`group member ${id} has no rule in its precedence group`); continue; }
        // Mirrors the context shape `evaluate` reads; only inputs are populated.
        const ctx = { inputs: values, coefs: new Map(), tables: new Map(), sets: new Map(), vars: new Map(), prices: null };
        const r = model().evaluate(rule, ctx);
        if (r.state !== 'ok') problems.push(`${id}: derivation from ${eff[i].join(', ')} is unavailable`);
        else values.set(id, r.value);
      }
    });

    const plainUnit = (u) => (u === 'day' ? 'days' : u);
    for (const [id, lim] of Object.entries(INPUT_LIMITS)) {
      const v = values.get(id);
      if (typeof v !== 'number' || !(v > lim.max)) continue;
      const q = byId.get(id);
      const rule = derivedIds.has(id) && ruleText.has(id) ? ` (${ruleText.get(id)})` : '';
      const n = (x) => Number(x.toPrecision(6)).toLocaleString('en-US');
      problems.push(`${id}: ${n(v)} ${plainUnit(q.unit)}${rule} is above ${n(lim.max)} ${plainUnit(q.unit)}: ${lim.why}`);
      values.delete(id);
    }

    // Zero ingest is a valid workload; data that arrives without INSERTs is not. The rate is left without a value.
    if (values.get('insert_rate') === 0 && values.get('ingest_rate') > 0) {
      problems.push(`insert_rate: 0 INSERTs per second cannot carry an ingest rate of ${values.get('ingest_rate')} GiB/day; set the ingest rate to 0 too`);
      values.delete('insert_rate');
    }

    const fields = {};
    const resolved = new Map();
    for (const x of inputs) {
      const value = values.has(x.id) ? values.get(x.id) : null;
      const own = model().rangeOf(x);
      const range = own && own[x.id];
      const gi = groupOf.get(x.id);
      if (value !== null) resolved.set(x.id, value);
      fields[x.id] = {
        value,
        kind: x.kind,
        locked: gi === undefined ? null : eff[gi].includes(x.id),
        derived: derivedIds.has(x.id),
        overridden: overridden.has(x.id),
        rule_text: ruleText.get(x.id) || null,
        validity: range || null,
        out_of_range: !!range && typeof value === 'number' && (value < range[0] || value > range[1]),
        // Read only by formulas from source: outside the range is exact, so the form shows no warning.
        verified_only: derivedOnlyInputs(contract).has(x.id),
        preset_from: presetOf.get(x.id) || null,
      };
      if (fields[x.id].verified_only) fields[x.id].out_of_range = false;
    }
    return { inputs: resolved, fields, lockOrder: persisted, problems };
  }

  // ---- evaluation: cells -> variant aggregate -> billing -> allocation (spec §4.1, §4.4) -----------------------

  const billingMod = () => root.SCC.billing || require('./billing.js');
  const SAME_AZ_NOTE = 'replicas in one AZ: interserver traffic not charged';
  // Precedence when several states hold for one variant: a column that cannot run the workload says so first.
  const STATE_ORDER = ['not_supported', 'unsupported_cut', 'backlog_grows', 'unavailable'];

  // The basis of a registry derivation: its own `basis`, else `derived` when its evidence is ClickHouse source (a
  // formula read from the code), else `fitted`.
  function derivationBasis(q) {
    if (q.basis !== undefined && q.basis !== null) return model().basisOf(q);
    return q.evidence && q.evidence.kind === 'source' ? 'derived' : 'fitted';
  }

  function inputsOf(node) {
    const ids = new Set();
    walk(node, (n) => { if (n.kind === 'input') ids.add(n.id); });
    return ids;
  }

  // Inputs that only formulas from source read (registry derivations with basis `derived`): a value outside their
  // verified range is exact, so it extrapolates nothing. Cells, other derivations and precedence rules count as
  // fitted readers.
  const derivedOnlyCache = new WeakMap();
  function derivedOnlyInputs(contract) {
    if (derivedOnlyCache.has(contract)) return derivedOnlyCache.get(contract);
    const fitted = new Set();
    const derived = new Set();
    for (const ops of Object.values(contract.formulas || {})) for (const cell of Object.values(ops || {})) for (const id of inputsOf(cell)) fitted.add(id);
    for (const g of contract.precedence || []) for (const r of Object.values(g.rules || {})) for (const id of inputsOf(r)) fitted.add(id);
    for (const q of contract.registry) {
      if (q.kind !== 'derivation' || !q.rule) continue;
      const into = derivationBasis(q) === 'derived' ? derived : fitted;
      for (const id of inputsOf(q.rule)) into.add(id);
    }
    const out = new Set(contract.registry.filter((q) => q.kind === 'input' && derived.has(q.id) && !fitted.has(q.id)).map((q) => q.id));
    derivedOnlyCache.set(contract, out);
    return out;
  }

  // References a cut card or an unsupported registry row leaves without a usable value. `via` maps a registry
  // derivation to the cut references its rule reaches (through other derivations too): a cell that reads the
  // derivation reads the cut.
  function unsupportedRefs(contract) {
    const coefs = new Set();
    const inputs = new Set();
    for (const c of contract.coefficients) if (c && c.status === 'unsupported') coefs.add(c.id);
    for (const qr of contract.registry) {
      if (!qr || qr.status !== 'unsupported') continue;
      if (qr.coef) coefs.add(qr.coef);
      if (qr.kind === 'input') inputs.add(qr.id);
    }
    const rules = new Map(contract.registry.filter((q) => q && q.kind === 'derivation' && q.rule).map((q) => [q.id, q.rule]));
    const via = new Map();
    for (const id of rules.keys()) {
      const refs = new Set();
      const seen = new Set([id]);
      const todo = [rules.get(id)];
      while (todo.length) {
        walk(todo.pop(), (n) => {
          if (n.kind === 'coef' && coefs.has(n.id)) refs.add(n.id);
          else if (n.kind === 'lookup' && coefs.has(n.table_id)) refs.add(n.table_id);
          else if (n.kind === 'input' && inputs.has(n.id)) refs.add(n.id);
          else if (n.kind === 'input' && rules.has(n.id) && !seen.has(n.id)) { seen.add(n.id); todo.push(rules.get(n.id)); }
        });
      }
      if (refs.size) via.set(id, [...refs].sort());
    }
    return { coefs, inputs, via };
  }

  function buildCtx(contract, inputs, cut) {
    const values = new Map(inputs);
    // A pinned quantity is a registered constant: expressions read it like an input.
    for (const q of contract.registry) if (q.kind === 'pinned' && !values.has(q.id) && q.default !== null && q.default !== undefined) values.set(q.id, q.default);
    const coefs = new Map();
    for (const c of contract.coefficients) {
      if (c.kind === 'table' || cut.coefs.has(c.id) || c.value === null || c.value === undefined) continue;
      // A derived coefficient is exact outside its verified range, so it never marks a number extrapolated.
      coefs.set(c.id, { value: c.value, validity: model().basisOf(c) === 'derived' ? null : model().rangeOf(c), verified_range: model().rangeOf(c), basis: model().basisOf(c) });
    }
    const tables = new Map(Object.entries(contract.tables || {}).filter(([id]) => !cut.coefs.has(id)));
    // A lookup miss falls back to the nearest measured key only on an ordinal enum that declares `ui.nearest` (true:
    // the enum order; an order; a map). Any other miss is unavailable: a variant or operation key never borrows
    // another's number. A substitution is extrapolated and named in a cell note.
    const nearest = new Map();
    for (const q of contract.registry) {
      const n = q.ui && q.ui.nearest;
      if (q.value_type !== 'enum' || !Array.isArray(q.enum) || !n) continue;
      nearest.set(q.id, n === true ? { order: q.enum } : Array.isArray(n) ? { order: n } : { map: n });
    }
    const ctx = { inputs: values, coefs, tables, sets: new Map(Object.entries(contract.sets || {})), vars: new Map(), prices: null, extrapolatedInputs: new Set(), nearest };
    // An input outside its own measured range makes every number that reads it extrapolated, unless only formulas
    // from source read it (derivedOutside notes those).
    const derivedOnly = derivedOnlyInputs(contract);
    for (const q of contract.registry) {
      if (derivedOnly.has(q.id)) continue;
      const r = q.kind === 'input' ? (model().rangeOf(q) || {})[q.id] : null;
      const x = values.get(q.id);
      if (r && typeof x === 'number' && (x < r[0] || x > r[1])) ctx.extrapolatedInputs.add(q.id);
    }
    // Registry derivations may read each other; evaluate until no new value appears (they are acyclic, §4.2).
    const pending = contract.registry.filter((x) => x.kind === 'derivation' && x.rule && !values.has(x.id));
    for (let changed = true; changed;) {
      changed = false;
      for (const x of pending) {
        if (values.has(x.id)) continue;
        const r = model().evaluate(x.rule, ctx);
        if (r.state === 'ok') {
          values.set(x.id, r.value);
          if (r.extrapolated) ctx.extrapolatedInputs.add(x.id);
          changed = true;
        }
      }
    }
    return ctx;
  }

  // Derived coefficients and tables a cell reads whose verified range does not hold the current values: exact by
  // construction, so only an informational note.
  // A coefficient with `basis: assumed` states its assumption as `<id>: ASSUMPTION ...` in `fit.method`; a cell that
  // prices a non-zero row with it carries that sentence as a note.
  function assumedNotes(contract, cell, rows) {
    if (!cell) return [];
    const out = [];
    for (const c of contract.coefficients) {
      if (model().basisOf(c) !== 'assumed') continue;
      const m = String((c.fit && c.fit.method) || '').match(new RegExp(`^${c.id}: (ASSUMPTION\\b.*)$`));
      if (!m) continue;
      const used = cell.ledger_rows.some((r, i) => {
        let reads = false;
        walk(r.expr, (n) => { if (n.kind === 'coef' && n.id === c.id) reads = true; });
        return reads && rows[i] && rows[i].state === 'ok' && rows[i].quantity > 0;
      });
      if (used) out.push(`${c.id}: ${m[1]}`);
    }
    return out;
  }

  function derivedOutside(contract, cell, ctx) {
    if (!cell) return [];
    const ids = new Set();
    walk(cell.ledger_rows, (n) => { if (n.kind === 'coef') ids.add(n.id); if (n.kind === 'lookup') ids.add(n.table_id); });
    const outside = (range) => Object.entries(range || {}).filter(([p, r]) => typeof ctx.inputs.get(p) === 'number' && (ctx.inputs.get(p) < r[0] || ctx.inputs.get(p) > r[1]));
    const verifiedAt = (off) => off.map(([p, r]) => `${p} ${r[0]} to ${r[1]}`).join(', ');
    const out = [];
    for (const c of contract.coefficients) {
      if (!ids.has(c.id) || model().basisOf(c) !== 'derived') continue;
      const off = outside(model().rangeOf(c));
      if (off.length) out.push(`${c.id}: formula from source; verified at ${verifiedAt(off)}`);
    }
    // Registry derivations the cell reads, directly or through other derivations; one note per verified range.
    const rules = new Map(contract.registry.filter((q) => q.kind === 'derivation' && q.rule).map((q) => [q.id, q]));
    const seen = new Set();
    const todo = [...inputsOf(cell)];
    const byRange = new Map();
    while (todo.length) {
      const id = todo.pop();
      if (seen.has(id) || !rules.has(id)) continue;
      seen.add(id);
      const q = rules.get(id);
      todo.push(...inputsOf(q.rule));
      if (derivationBasis(q) !== 'derived') continue;
      const off = outside(model().rangeOf(q));
      if (!off.length) continue;
      const key = verifiedAt(off);
      if (!byRange.has(key)) byRange.set(key, []);
      byRange.get(key).push(id);
    }
    for (const [key, list] of byRange) out.push(`${list.sort().join(', ')}: formula from source; verified at ${key}`);
    return out;
  }

  // Lookups that used the nearest measured key, once per table and key pair.
  function substitutions(traces) {
    const out = new Map();
    const visit = (t) => {
      if (!t || typeof t !== 'object') return;
      if (t.kind === 'lookup' && t.substituted_key) out.set(`${t.table_id}|${t.lookup_key}`, { table_id: t.table_id, requested: t.lookup_key, measured: t.substituted_key });
      for (const k of t.children || []) visit(k);
    };
    (traces || []).forEach(visit);
    return [...out.values()];
  }

  function walk(node, fn) {
    if (!node || typeof node !== 'object') return;
    fn(node);
    for (const v of Object.values(node)) {
      if (Array.isArray(v)) v.forEach((x) => walk(x, fn));
      else if (v && typeof v === 'object') walk(v, fn);
    }
  }

  // The cut references `n` stands for, or null. A derivation that reaches a cut is cut unless the context holds its
  // value (its rule took a side without the cut).
  function refOf(n, cut, ctx) {
    if (n.kind === 'coef' && cut.coefs.has(n.id)) return [n.id];
    if (n.kind === 'lookup' && cut.coefs.has(n.table_id)) return [n.table_id];
    if (n.kind === 'input' && cut.inputs.has(n.id)) return [n.id];
    if (n.kind === 'input' && cut.via && cut.via.has(n.id) && !(ctx && ctx.inputs.has(n.id))) return cut.via.get(n.id);
    return null;
  }
  function hasCut(node, cut, ctx) {
    let found = false;
    walk(node, (n) => { if (refOf(n, cut, ctx)) found = true; });
    return found;
  }
  function inputIds(node) {
    const ids = new Set();
    walk(node, (n) => { if (n.kind === 'input') ids.add(n.id); });
    return [...ids];
  }
  function hasVar(node) {
    let found = false;
    walk(node, (n) => { if (n.kind === 'var') found = true; });
    return found;
  }
  // Only var-free subtrees are evaluated on their own: a var is bound by an enclosing sum_over.
  function isZero(node, ctx) {
    if (hasVar(node)) return false;
    const r = model().evaluate(node, ctx);
    return r.state === 'ok' && r.value === 0;
  }

  // A cut term contributes nothing when its dependent workload is zero (spec §5.5): rewrites such a term to a zero
  // const of the same dim. Returns {node, tainted: {refs, dependents} | null}; a tainted result is a cut that the
  // workload needs. Product structure decides the term: a zero factor (or numerator) zeroes the whole product.
  function rewriteCut(node, ctx, cut, zeroed) {
    if (!node || typeof node !== 'object' || !hasCut(node, cut, ctx)) return { node, tainted: null };
    const refs = refOf(node, cut, ctx);
    if (refs) {
      if (node.kind === 'input' && cut.inputs.has(node.id)) {
        const v = ctx.inputs.get(node.id);
        if (v === 0) { zeroed.push({ refs, by: [node.id] }); return { node: { kind: 'const', value: 0, dim: node.dim }, tainted: null }; }
        return { node, tainted: { refs, dependents: [node.id] } };
      }
      return { node, tainted: { refs, dependents: [] } };
    }
    const merge = (ts) => {
      const live = ts.filter(Boolean);
      if (!live.length) return null;
      return { refs: [...new Set(live.flatMap((t) => t.refs))], dependents: [...new Set(live.flatMap((t) => t.dependents))] };
    };
    const zero = (t, by) => {
      zeroed.push({ refs: t.refs, by });
      return { node: { kind: 'const', value: 0, dim: node.dim }, tainted: null };
    };
    if (node.kind === 'mul' || node.kind === 'div') {
      const a = rewriteCut(node.a, ctx, cut, zeroed);
      const b = rewriteCut(node.b, ctx, cut, zeroed);
      const t = merge([a.tainted, b.tainted]);
      if (!t) return { node: Object.assign({}, node, { a: a.node, b: b.node }), tainted: null };
      if (!a.tainted && isZero(a.node, ctx)) return zero(t, inputIds(a.node));
      if (node.kind === 'mul' && !b.tainted && isZero(b.node, ctx)) return zero(t, inputIds(b.node));
      const clean = [a, b].filter((x) => !x.tainted).map((x) => x.node);
      return { node, tainted: { refs: t.refs, dependents: [...new Set([...t.dependents, ...clean.flatMap(inputIds)])] } };
    }
    if (node.kind === 'branch' && !hasCut(node.cond, cut, ctx) && !hasVar(node.cond)) {
      // Only the taken side is evaluated, so only its cut terms matter. The probe evaluates the condition alone.
      const probe = model().evaluate(Object.assign({}, node, { then: { kind: 'const', value: 1, dim: '1' }, else: { kind: 'const', value: 0, dim: '1' } }), ctx);
      if (probe.state === 'ok') {
        const side = probe.value === 1 ? 'then' : 'else';
        const r = rewriteCut(node[side], ctx, cut, zeroed);
        return r.tainted ? { node, tainted: r.tainted } : { node: Object.assign({}, node, { [side]: r.node }), tainted: null };
      }
    }
    // Any other node needs every operand: rewrite each child; a remaining cut child taints the node.
    const outNode = {};
    const ts = [];
    for (const [k, v] of Object.entries(node)) {
      if (Array.isArray(v)) {
        outNode[k] = v.map((x) => { const r = rewriteCut(x, ctx, cut, zeroed); ts.push(r.tainted); return r.node; });
      } else if (v && typeof v === 'object') {
        const r = rewriteCut(v, ctx, cut, zeroed);
        ts.push(r.tainted);
        outNode[k] = r.node;
      } else outNode[k] = v;
    }
    const t = merge(ts);
    return t ? { node, tainted: t } : { node: outNode, tainted: null };
  }

  function cutReason(variant, op, t, ctx) {
    // A zero-valued dependent does not make the term non-zero, so it is not named.
    const deps = t.dependents.filter((id) => ctx.inputs.get(id) !== 0).map((id) => `${id} = ${JSON.stringify(ctx.inputs.has(id) ? ctx.inputs.get(id) : null)}`);
    return `${variant}.${op}: ${t.refs.join(', ')} is unsupported (card cut)` + (deps.length ? ` and its dependent input ${deps.join(', ')} is non-zero` : ' and the cell needs it unconditionally');
  }

  // Inputs, coefficients and tables a node reads that the context does not hold.
  function missingRefs(node, ctx) {
    const out = new Set();
    walk(node, (n) => {
      if (n.kind === 'input' && !ctx.inputs.has(n.id)) out.add(`input ${n.id}`);
      if (n.kind === 'coef' && !ctx.coefs.has(n.id)) out.add(`coef ${n.id}`);
      if (n.kind === 'lookup' && !ctx.tables.has(n.table_id)) out.add(`table ${n.table_id}`);
    });
    return [...out];
  }

  // Table keys a trace looked up and did not find (no `ui.nearest` stand-in either).
  function missingKeys(trace) {
    const out = new Set();
    const visit = (t) => {
      if (!t || typeof t !== 'object') return;
      if (t.kind === 'lookup' && t.state === 'unavailable' && typeof t.lookup_key === 'string' && !t.reason
        && (t.children || []).every((k) => k && k.state === 'ok')) out.add(`table ${t.table_id} has no key ${t.lookup_key}`);
      for (const k of t.children || []) visit(k);
    };
    visit(trace);
    return [...out];
  }

  // Evaluates one variant's cells. Returns {cells, states: [{id, reason}], extrapolated}.
  function evaluateVariant(contract, v, ctx, cut) {
    const states = [];
    const cells = {};
    let extrapolated = false;
    // With no family occurring anywhere the column would price nothing; a 0 total would read as "free".
    const occurs = contract.operations.some((op) => contract.families.some((f) => {
      const x = ((contract.applicability[v] || {})[op] || {})[f];
      return x === 'supported' || x === 'unsupported_cut';
    }));
    if (!occurs) states.push({ id: 'unavailable', reason: `${v}: no operation has a supported family in this contract` });
    for (const op of contract.operations) {
      const app = (contract.applicability[v] || {})[op] || {};
      if (contract.families.every((f) => app[f] === 'not_applicable')) continue;
      // A `not_applicable` family does not occur on this variant: it adds nothing and changes no state.
      const fams = contract.families.filter((f) => app[f] !== 'supported' && app[f] !== 'not_applicable');
      const notSup = fams.filter((f) => app[f] === 'not_supported');
      if (notSup.length) states.push({ id: 'not_supported', reason: `${v}.${op}: not supported for ${notSup.join(', ')}` });
      const cell = (contract.formulas[v] || {})[op];
      if (!cell) {
        const occurs = contract.families.some((f) => app[f] === 'supported' || app[f] === 'unsupported_cut');
        if (!notSup.length && occurs) states.push({ id: 'unavailable', reason: `${v}.${op}: the contract has no cell for a supported operation` });
        continue;
      }
      const zeroed = [];
      const taints = [];
      const rw = (e) => { const r = rewriteCut(e, ctx, cut, zeroed); if (r.tainted) taints.push(r.tainted); return r.node; };
      const effective = Object.assign({}, cell, {
        ledger_rows: cell.ledger_rows.map((r) => Object.assign({}, r, { expr: rw(r.expr) })),
        allocation: cell.allocation && { shares: cell.allocation.shares.map((sh) => Object.assign({}, sh, { weight: rw(sh.weight) })) },
        states: (cell.states || []).map((st) => (st.cond === true ? st : Object.assign({}, st, { cond: rw(st.cond) }))),
        notes: (cell.notes || []).map(rw),
      });
      for (const t of taints) states.push({ id: 'unsupported_cut', reason: cutReason(v, op, t, ctx) });
      const ev = model().evaluateCell(effective, ctx);
      extrapolated = extrapolated || ev.extrapolated;
      const cutFams = fams.filter((f) => app[f] === 'unsupported_cut');
      if (cutFams.length) {
        const live = ev.rows.filter((r) => r.state !== 'ok' || r.quantity !== 0);
        if (live.length) states.push({ id: 'unsupported_cut', reason: `${v}.${op}: cut for ${cutFams.join(', ')} and the workload needs ${live.map((r) => `${r.row_type} ${r.op_kind}`).join(', ')}` });
      }
      // A cut family with no rows prices nothing. That is a true zero only when every input the other columns read for
      // this operation is zero; with no such input to check, the column cannot tell.
      if (cutFams.length && !cell.ledger_rows.length) {
        const siblings = contract.variants.filter((x) => x !== v).flatMap((x) => (((contract.formulas[x] || {})[op] || {}).ledger_rows || []).flatMap((r) => inputIds(r.expr)));
        const deps = [...new Set(siblings)];
        if (!deps.length) states.push({ id: 'unavailable', reason: `${v}.${op}: cut for ${cutFams.join(', ')} and the cell has no rows to show the workload is zero` });
        else {
          const live = deps.filter((id) => ctx.inputs.get(id) !== 0).map((id) => `${id} = ${JSON.stringify(ctx.inputs.has(id) ? ctx.inputs.get(id) : null)}`);
          if (live.length) states.push({ id: 'unsupported_cut', reason: `${v}.${op}: cut for ${cutFams.join(', ')}, the cell has no rows and ${live.join(', ')} is non-zero` });
        }
      }
      for (const st of ev.states) if (st.active) states.push({ id: st.id, reason: `${v}.${op}: ${st.reason}` });
      const unavailableRows = ev.rows.map((r, i) => (r.state === 'ok' ? null : `${r.row_type} ${r.op_kind}: unavailable (${[...missingRefs(effective.ledger_rows[i].expr, ctx), ...missingKeys(r.trace)].join(', ') || 'see trace'})`)).filter(Boolean);
      if (!taints.length && (unavailableRows.length || ev.state !== 'ok')) {
        states.push({ id: 'unavailable', reason: `${v}.${op}: ${[...unavailableRows, ev.reason].filter(Boolean).join('; ')}` });
      }
      cells[op] = {
        ev, zeroed,
        rows: ev.rows,
        quantities: ev.rows.map(({ trace, ...rest }) => rest),
        extrapolated: ev.extrapolated,
        trace: ev.rows.map((r) => r.trace),
        notes: ev.notes.map((n, i) => ({ text: model().render(effective.notes[i], contract.registry), value: n.value, state: n.state })),
        money: null, lines: null,
      };
    }
    return { cells, states, extrapolated };
  }

  function lookupPreset(contract, pricePreset) {
    if (pricePreset && typeof pricePreset === 'object') return { preset: pricePreset };
    const p = ((contract.billing && contract.billing.presets) || []).find((x) => x.id === pricePreset);
    return p ? { preset: p } : { problem: `price preset ${JSON.stringify(pricePreset)} is not present in billing version ${contract.manifest.billing_version}` };
  }

  // Manual provisioning needs all of its inputs; `volumes_per_replica = auto` (or no such input) means automatic.
  function manualOf(inputs) {
    const n = inputs.get('volumes_per_replica');
    if (n === undefined || n === 'auto') return { manual: null };
    const m = { volumes: n, size_gib: inputs.get('provisioned_size_gib'), iops: inputs.get('provisioned_iops'), mibps: inputs.get('provisioned_mibps') };
    const missing = ['provisioned_size_gib', 'provisioned_iops', 'provisioned_mibps'].filter((id) => inputs.get(id) === undefined);
    return missing.length ? { problem: `manual provisioning with volumes_per_replica = ${n} needs ${missing.join(', ')}` } : { manual: m };
  }

  function billVariantResult(out, ev, preset, options, inputs) {
    const B = billingMod();
    const ops = Object.keys(ev.cells);
    const rows = ops.flatMap((op) => ev.cells[op].rows.map((r) => Object.assign({}, r, { cell: op })));
    // Provisioning inputs matter only to a column that provisions volumes.
    const m = rows.some((r) => r.row_type === 'block_io' || r.row_type === 'block_capacity') ? manualOf(inputs) : { manual: null };
    if (m.problem) return { problem: m.problem };
    let b;
    try {
      b = B.billVariant(rows, preset, options.outcome_mix || null, m.manual, { replicas: inputs.get('replicas') });
    } catch (e) {
      return { problem: `billing failed: ${e.message}` };
    }
    const a = B.allocate(b.lines, ops.map((op) => ({ key: op, name: op, rows: ev.cells[op].rows, shares: ev.cells[op].ev.allocation })), preset, options.outcome_mix || null);
    if (a.problems.length) return { problem: a.problems.join('; ') };
    for (const op of ops) Object.assign(out.cells[op], { money: a.byCell[op].money, lines: a.byCell[op].lines });
    out.total = b.total;
    out.lines = b.lines;
    out.allocation = a.allocation;
    out.provisioning = b.provisioning;
    if (b.provisioning) {
      const p = b.provisioning;
      out.notes.push({ kind: 'allocation', text: p.note, notes: p.notes });
      const ic = B.instanceClass(p, preset);
      out.notes.push({ kind: 'instance_class', text: ic.instance_class ? `per replica: smallest instance class for ${ic.throughput_mibps} MiB/s of EBS bandwidth: ${ic.instance_class}` : ic.reason, instance_class: ic.instance_class });
    }
    return {};
  }

  const pricesSomething = (c) => (typeof c.money === 'number' ? c.money !== 0 : (c.rows || []).some((r) => r.state === 'ok' && r.quantity !== 0));

  // Display arithmetic only (spec §4.5): a delta is the difference of two contract totals already computed.
  function delta(a, ref) {
    if (!a || !ref || a.state !== 'ok' || ref.state !== 'ok' || a.total === null || ref.total === null) return null;
    return { value: a.total - ref.total, extrapolated: a.extrapolated || ref.extrapolated };
  }

  function run(contract, inputs, pricePreset, options) {
    const opts = options || {};
    const base = { evidence_statement: contract.evidence_statement, manifest: contract.manifest, problems: [] };
    const found = lookupPreset(contract, pricePreset);
    if (found.problem) {
      // A preset the contract does not carry cannot be priced faithfully: show no number at all (spec §4.5).
      return Object.assign(base, { page_state: 'cannot_reproduce', page_reason: found.problem, variants: {}, cheapest: null, deltas: {}, sweep: null, problems: [found.problem] });
    }
    const preset = found.preset;
    // An edited preset with a missing or invalid price shows quantities without money, never a $0 line.
    const badPrices = billingMod().priceProblems(preset);
    const pricesMissing = !!preset.structure_only || badPrices.length > 0;
    const cut = unsupportedRefs(contract);
    const ctx = buildCtx(contract, inputs instanceof Map ? inputs : new Map(Object.entries(inputs || {})), cut);
    const variants = {};
    for (const v of contract.variants) {
      const ev = evaluateVariant(contract, v, ctx, cut);
      const out = { state: 'ok', reason: null, extrapolated: ev.extrapolated, cells: {}, total: null, lines: null, allocation: null, provisioning: null, notes: [] };
      for (const [op, c] of Object.entries(ev.cells)) {
        for (const s of substitutions(c.trace)) c.notes.push({ text: `${s.table_id}: measured for ${s.measured}; applied to ${s.requested}`, value: null, state: 'ok', substitution: s });
        for (const m of derivedOutside(contract, (contract.formulas[v] || {})[op], ctx)) c.notes.push({ text: m, value: null, state: 'ok', verified_only: true });
        for (const m of assumedNotes(contract, (contract.formulas[v] || {})[op], c.rows)) c.notes.push({ text: m, value: null, state: 'ok', assumption: true });
        // A zero interserver row in one zone is a priced fact, not a missing number: the page says why it is zero.
        if (ctx.inputs.get('topology') === 'same_az' && ctx.inputs.get('replicas') > 1 && c.rows.some((r) => r.row_type === 'transfer' && r.op_kind === 'interserver' && r.state === 'ok' && r.quantity === 0)) {
          out.notes.push({ kind: 'text', operation: op, text: SAME_AZ_NOTE });
        }
        out.cells[op] = { money: null, quantities: c.quantities, lines: null, rows: c.rows, extrapolated: c.extrapolated, trace: c.trace, notes: c.notes, zeroed_cut_terms: c.zeroed, ev: c.ev };
        for (const n of c.notes) out.notes.push({ kind: 'cell', operation: op, text: n.text, value: n.value });
      }
      const top = STATE_ORDER.find((id) => ev.states.some((s) => s.id === id));
      if (top) {
        out.state = top;
        out.reason = ev.states.filter((s) => s.id === top).map((s) => s.reason).join('; ');
      } else if (!pricesMissing) {
        const billed = billVariantResult(out, ev, preset, opts, ctx.inputs);
        if (billed.problem) { out.state = 'unavailable'; out.reason = `${v}: ${billed.problem}`; }
      }
      if (out.state === 'ok') {
        // A cell that prices nothing extrapolates nothing: the flag needs a non-zero amount, or without prices a
        // non-zero quantity. The column is extrapolated only through such cells.
        for (const c of Object.values(out.cells)) if (c.extrapolated && !pricesSomething(c)) c.extrapolated = false;
        // Block lines are multiplied by the replica count, so a count outside its measured range extrapolates them.
        out.extrapolated = Object.values(out.cells).some((c) => c.extrapolated) || (!!out.provisioning && ctx.extrapolatedInputs.has('replicas'));
      }
      for (const c of Object.values(out.cells)) delete c.ev;
      variants[v] = out;
    }
    let cheapest = null;
    if (!pricesMissing) {
      for (const v of contract.variants) {
        const x = variants[v];
        if (x.state === 'ok' && x.total !== null && (cheapest === null || x.total < variants[cheapest].total)) cheapest = v;
      }
    }
    const deltas = {};
    for (const v of contract.variants) deltas[v] = { vs_ebs: delta(variants[v], variants.ebs), vs_s3: delta(variants[v], variants.s3) };
    const result = Object.assign(base, { page_state: pricesMissing ? 'prices_missing' : 'ok', page_reason: !pricesMissing ? null
      : preset.structure_only ? `price preset ${preset.id} has no prices (structure only)` : `price preset ${preset.id} has invalid prices: ${badPrices.join('; ')}`, variants, cheapest, deltas, sweep: null });
    if (opts.sweep) result.sweep = sweep(contract, inputs, pricePreset, opts.sweep.inputId, opts.sweep.values, { outcome_mix: opts.outcome_mix, lockOrder: opts.lockOrder });
    return result;
  }

  // Totals per variant against one swept input. Each point re-resolves the precedence groups with the swept input
  // locked as the newest member (options.lockOrder is the persisted lock order), so derived partners follow it.
  function sweep(contract, inputs, preset, inputId, values, options) {
    const opts = options || {};
    const baseInputs = inputs instanceof Map ? inputs : new Map(Object.entries(inputs || {}));
    const isInput = new Set(contract.registry.filter((x) => x.kind === 'input').map((x) => x.id));
    const inGroup = contract.precedence.some((g) => g.group.includes(inputId));
    const locks = inGroup ? lockMember(contract, opts.lockOrder || [], inputId) : opts.lockOrder || [];
    const points = values.map((x) => {
      const raw = Object.fromEntries([...baseInputs].filter(([id]) => isInput.has(id)));
      raw[inputId] = x;
      const res = resolveInputs(contract, raw, locks, {});
      const point = new Map([...[...baseInputs].filter(([id]) => !isInput.has(id)), ...res.inputs]);
      const r = run(contract, point, preset, { outcome_mix: opts.outcome_mix });
      const totals = {};
      for (const v of contract.variants) {
        const vr = r.variants[v];
        totals[v] = vr ? { value: vr.state === 'ok' ? vr.total : null, state: vr.state, extrapolated: vr.extrapolated } : { value: null, state: r.page_state, extrapolated: false };
      }
      return { x, totals, page_state: r.page_state, problems: res.problems };
    });
    // Display arithmetic only (spec §4.5): a crossing is a sign change of total_a - total_b between consecutive ok
    // points; a point where the totals are equal carries no sign and does not end the run.
    const breakEven = [];
    const vs = contract.variants;
    for (let i = 0; i < vs.length; i++) {
      for (let j = i + 1; j < vs.length; j++) {
        let last = null;
        for (const p of points) {
          const a = p.totals[vs[i]];
          const b = p.totals[vs[j]];
          if (a.state !== 'ok' || b.state !== 'ok' || a.value === null || b.value === null) { last = null; continue; }
          const d = a.value - b.value;
          if (d === 0) continue;
          if (last && Math.sign(d) !== last.sign) breakEven.push({ a: vs[i], b: vs[j], x_between: [last.x, p.x], extrapolated: a.extrapolated || b.extrapolated || last.extrapolated });
          last = { x: p.x, sign: Math.sign(d), extrapolated: a.extrapolated || b.extrapolated };
        }
      }
    }
    return { input: inputId, points, breakEven };
  }

  // The evaluation context of `inputs`: pinned constants and registry derivations included (presets.coverageReport).
  const evaluationInputs = (contract, inputs) => buildCtx(contract, inputs instanceof Map ? inputs : new Map(Object.entries(inputs || {})), unsupportedRefs(contract)).inputs;

  SCC.pipeline = Object.assign(SCC.pipeline || {}, { resolveInputs, normaliseRate, denormaliseRate, lockMember, DAYS_PER_MONTH, run, sweep, evaluationInputs,
    unsupportedRefs, rewriteCut, derivedOnlyInputs, derivationBasis, substitutions, SAME_AZ_NOTE, INPUT_LIMITS });
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.pipeline;
})(typeof window !== 'undefined' ? window : globalThis);
