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

  const isRate = (q) => typeof q.unit === 'string' && q.unit.endsWith('/day');
  const isFiniteNumber = (v) => typeof v === 'number' && Number.isFinite(v);

  // Returns {value} or {problem}. A rate may arrive as {value, per}. An enum that lists `auto` also takes a number
  // >= 1 (volumes_per_replica); the allocator validates the number against the volume limits.
  function parseValue(q, v) {
    if (isRate(q) && v !== null && typeof v === 'object') {
      try {
        return { value: normaliseRate(v.value, v.per) };
      } catch (e) {
        return { problem: `${q.id}: ${e.message}` };
      }
    }
    if (v !== null && typeof v === 'object') return { problem: `${q.id}: ${q.unit || 'value'} is not a rate; expected a plain value, got an object` };
    if (q.value_type === 'number') {
      return isFiniteNumber(v) ? { value: v } : { problem: `${q.id}: expected a finite number, got ${JSON.stringify(v)}` };
    }
    if (q.value_type === 'enum') {
      if (typeof v === 'string' && q.enum.includes(v)) return { value: v };
      if (q.enum.includes('auto') && isFiniteNumber(v) && v >= 1) return { value: v };
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

    const locks = normaliseLocks(contract, lockOrder, problems);
    const persisted = locks.flat();
    const eff = locks.map((l) => l.slice());
    const overridden = new Set();
    // An override locks its member and releases the oldest lock that is not itself an override.
    for (const [id, v] of Object.entries(overrides || {})) {
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
        const ctx = { inputs: values, coefs: new Map(), tables: new Map(), sets: new Map(), vars: new Map(), prices: null };
        const r = model().evaluate(rule, ctx);
        if (r.state !== 'ok') problems.push(`${id}: derivation from ${eff[i].join(', ')} is unavailable`);
        else values.set(id, r.value);
      }
    });

    const fields = {};
    const resolved = new Map();
    for (const x of inputs) {
      const value = values.has(x.id) ? values.get(x.id) : null;
      const range = x.validity && x.validity[x.id];
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
      };
    }
    return { inputs: resolved, fields, lockOrder: persisted, problems };
  }

  SCC.pipeline = Object.assign(SCC.pipeline || {}, { resolveInputs, normaliseRate, lockMember, DAYS_PER_MONTH });
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.pipeline;
})(typeof window !== 'undefined' ? window : globalThis);
