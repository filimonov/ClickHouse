// Typed expression evaluator for formulas grammar v1 (docs/superpowers/cost-model/schemas/README.md).
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const ARITH = new Set(['add', 'sub', 'mul', 'div', 'min', 'max']);
  const CMP = {
    '<': (a, b) => a < b, '<=': (a, b) => a <= b, '>': (a, b) => a > b,
    '>=': (a, b) => a >= b, '==': (a, b) => a === b, '!=': (a, b) => a !== b,
  };

  function res(value, state, extrapolated, trace) {
    return { value, state, extrapolated, trace };
  }

  function mkTrace(node, r, children, extra) {
    return Object.assign({ kind: node.kind, dim: node.dim, value: r.value, state: r.state, children }, extra || {});
  }

  function unavailable(node, children, extrapolated, extra) {
    const r = res(null, 'unavailable', !!extrapolated, null);
    r.trace = mkTrace(node, r, children, extra);
    if (extrapolated) r.trace.extrapolated = true;
    return r;
  }

  function ok(node, value, children, extrapolated, extra) {
    const r = res(value, 'ok', !!extrapolated, null);
    r.trace = mkTrace(node, r, children, extra);
    if (extrapolated) r.trace.extrapolated = true;
    return r;
  }

  function anyUnavailable(rs) {
    return rs.some((r) => r.state === 'unavailable');
  }

  function anyExtrapolated(rs) {
    return rs.some((r) => r.extrapolated);
  }

  // A value is usable when it is a finite number, or a string where the node's dim is an enum.
  function badValue(node, v) {
    if (typeof v === 'number') return Number.isFinite(v) ? null : `non-finite value ${v}`;
    if (typeof v === 'string' && typeof node.dim === 'string' && node.dim.startsWith('enum:')) return null;
    return `invalid value ${v === undefined ? 'undefined' : JSON.stringify(v)}`;
  }

  function num(node, r) {
    if (typeof r.value !== 'number') {
      throw new Error(`${node.kind}: operand is not a number: ${JSON.stringify(r.value)}`);
    }
    return r.value;
  }

  function validityExtrapolated(validity, ctx) {
    if (!validity) return false;
    for (const [param, range] of Object.entries(validity)) {
      const v = ctx.inputs.get(param);
      if (typeof v === 'number' && (v < range[0] || v > range[1])) return true;
    }
    return false;
  }

  function evalArith(node, ctx) {
    const a = evaluate(node.a, ctx);
    const b = evaluate(node.b, ctx);
    const kids = [a.trace, b.trace];
    const ex = anyExtrapolated([a, b]);
    if (anyUnavailable([a, b])) return unavailable(node, kids, ex);
    const x = num(node, a);
    const y = num(node, b);
    let v;
    switch (node.kind) {
      case 'add': v = x + y; break;
      case 'sub': v = x - y; break;
      case 'mul': v = x * y; break;
      case 'min': v = Math.min(x, y); break;
      case 'max': v = Math.max(x, y); break;
      case 'div':
        if (y === 0) {
          if (node.on_zero === 'zero') return ok(node, 0, kids, ex);
          if (node.on_zero === 'unavailable') return unavailable(node, kids, ex);
          throw new Error(`div: bad on_zero ${JSON.stringify(node.on_zero)}`);
        }
        v = x / y;
        break;
      default: throw new Error(`unreachable arith kind ${node.kind}`);
    }
    if (!Number.isFinite(v)) return unavailable(node, kids, ex, { reason: `non-finite result ${v}` });
    return ok(node, v, kids, ex);
  }

  function evalRound(node, ctx) {
    const x = evaluate(node.x, ctx);
    const step = evaluate(node.step, ctx);
    const kids = [x.trace, step.trace];
    const ex = anyExtrapolated([x, step]);
    if (anyUnavailable([x, step])) return unavailable(node, kids, ex);
    const s = num(node, step);
    if (s === 0) return unavailable(node, kids, ex);
    const q = num(node, x) / s;
    const v = (node.kind === 'ceil_to' ? Math.ceil(q) : Math.floor(q)) * s;
    if (!Number.isFinite(v)) return unavailable(node, kids, ex, { reason: `non-finite result ${v}` });
    return ok(node, v, kids, ex);
  }

  function evalLookup(node, ctx) {
    const keys = node.keys.map((k) => evaluate(k, ctx));
    const kids = keys.map((k) => k.trace);
    const ex = anyExtrapolated(keys);
    if (anyUnavailable(keys)) return unavailable(node, kids, ex);
    const key = keys.map((k) => String(k.value)).join('|');
    const table = ctx.tables.get(node.table_id);
    const extra = { lookup_key: key };
    if (table && table.index.length !== node.keys.length) {
      return unavailable(node, kids, ex, { reason: 'lookup arity' });
    }
    if (!table || !Object.prototype.hasOwnProperty.call(table.values, key)) {
      return unavailable(node, kids, ex, extra);
    }
    const cell = table.values[key];
    const bad = typeof cell === 'number' ? badValue({ dim: 'count' }, cell) : `invalid value ${JSON.stringify(cell)}`;
    if (bad) return unavailable(node, kids, ex, Object.assign({ reason: `table ${node.table_id}[${key}]: ${bad}` }, extra));
    return ok(node, cell, kids, ex, extra);
  }

  function evalBranch(node, ctx) {
    const a = evaluate(node.cond.a, ctx);
    const b = evaluate(node.cond.b, ctx);
    const condKids = [a.trace, b.trace];
    const ex = anyExtrapolated([a, b]);
    const cmp = CMP[node.cond.op];
    if (!cmp) throw new Error(`branch: unknown op ${node.cond.op}`);
    if (anyUnavailable([a, b])) return unavailable(node, condKids, ex);
    const taken = cmp(a.value, b.value) ? 'then' : 'else';
    const side = evaluate(node[taken], ctx);
    const r = res(side.value, side.state, ex || side.extrapolated, null);
    r.trace = mkTrace(node, r, [...condKids, side.trace], { branch_taken: taken });
    if (r.extrapolated) r.trace.extrapolated = true;
    return r;
  }

  function evalSumOver(node, ctx) {
    const members = ctx.sets.get(node.set);
    if (!members) throw new Error(`sum_over: unknown set ${node.set}`);
    const parts = members.map((m) => {
      const vars = new Map(ctx.vars);
      vars.set(node.var, m);
      const r = evaluate(node.body, Object.assign({}, ctx, { vars }));
      r.trace.bound = { var: node.var, member: m };
      return r;
    });
    const kids = parts.map((p) => p.trace);
    const ex = anyExtrapolated(parts);
    if (anyUnavailable(parts)) return unavailable(node, kids, ex);
    return ok(node, parts.reduce((s, p) => s + num(node, p), 0), kids, ex);
  }

  function evaluate(node, ctx) {
    switch (node.kind) {
      case 'const':
        if (node.value === 'unavailable') return unavailable(node, []);
        if (typeof node.value === 'number' && !Number.isFinite(node.value)) {
          return unavailable(node, [], false, { reason: `const: non-finite value ${node.value}` });
        }
        if (typeof node.value !== 'number' && !(typeof node.dim === 'string' && node.dim.startsWith('enum:'))) {
          throw new Error(`const: non-numeric value ${JSON.stringify(node.value)} needs an enum dim`);
        }
        return ok(node, node.value, []);
      case 'input': {
        const v = ctx.inputs.get(node.id);
        if (v === undefined) return unavailable(node, []);
        const bad = badValue(node, v);
        return bad ? unavailable(node, [], false, { reason: `input ${node.id}: ${bad}` }) : ok(node, v, []);
      }
      case 'coef': {
        const c = ctx.coefs.get(node.id);
        if (!c || c.value === undefined || c.value === null) return unavailable(node, []);
        const bad = badValue(node, c.value);
        if (bad) return unavailable(node, [], false, { reason: `coef ${node.id}: ${bad}` });
        return ok(node, c.value, [], validityExtrapolated(c.validity, ctx));
      }
      case 'var':
        if (!ctx.vars.has(node.name)) throw new Error(`var: unbound ${node.name}`);
      {
        const v = ctx.vars.get(node.name);
        const bad = badValue(node, v);
        return bad ? unavailable(node, [], false, { reason: `var ${node.name}: ${bad}` }) : ok(node, v, []);
      }
      case 'lookup': return evalLookup(node, ctx);
      case 'ceil_to':
      case 'floor_to': return evalRound(node, ctx);
      case 'branch': return evalBranch(node, ctx);
      case 'sum_over': return evalSumOver(node, ctx);
      case 'price': {
        // Prices are applied by the billing module; here the quantity passes through unchanged.
        const q = evaluate(node.quantity, ctx);
        const r = res(q.value, q.state, q.extrapolated, null);
        r.trace = mkTrace(node, r, [q.trace], { unit: node.unit, rule_id: node.rule_id });
        if (q.extrapolated) r.trace.extrapolated = true;
        return r;
      }
      default:
        if (ARITH.has(node.kind)) return evalArith(node, ctx);
        throw new Error(`unknown node kind ${node.kind}`);
    }
  }

  // A cell is unavailable when a state condition or an allocation weight cannot be evaluated;
  // guessing "inactive" or dropping a share would silently change the prediction.
  function evaluateCell(cell, ctx) {
    let extrapolated = false;
    const reasons = [];
    const rows = cell.ledger_rows.map((row) => {
      const { expr, ...fields } = row;
      const r = evaluate(expr, ctx);
      extrapolated = extrapolated || r.extrapolated;
      return Object.assign(fields, { quantity: r.value, state: r.state, trace: r.trace });
    });
    let allocation = null;
    if (cell.allocation) {
      allocation = {};
      for (const share of cell.allocation.shares) {
        const r = evaluate(share.weight, ctx);
        extrapolated = extrapolated || r.extrapolated;
        if (r.state === 'unavailable') {
          reasons.push(`allocation weight for ${share.cell} unavailable`);
          allocation = null;
          break;
        }
        allocation[share.cell] = r.value;
      }
    }
    const states = (cell.states || []).map((s) => {
      if (s.cond === true) return { id: s.id, active: true, reason: s.reason };
      const r = evaluate(s.cond, ctx);
      extrapolated = extrapolated || r.extrapolated;
      if (r.state === 'unavailable') {
        reasons.push(`state ${s.id} condition unavailable`);
        return { id: s.id, active: null, reason: s.reason };
      }
      return { id: s.id, active: r.value !== 0 && r.value !== false, reason: s.reason };
    });
    const out = { rows, allocation, extrapolated, states, state: reasons.length ? 'unavailable' : 'ok' };
    if (reasons.length) out.reason = reasons.join('; ');
    return out;
  }

  // ---- static checks -------------------------------------------------------------------------

  // Dims are products of base units with integer exponents ("GiB*h", "req/s", "1" is neutral).
  // Aliases are expanded before parsing: GiB_h = GiB*h, GiB_xfer = GiB, count = 1, iops = req/s,
  // mibps = MiB/s.
  const DIM_ALIAS = { GiB_h: 'GiB*h', GiB_xfer: 'GiB', count: '1', iops: 'req/s', mibps: 'MiB/s' };

  function dimMap(str) {
    const m = new Map();
    const tokens = String(str).split(/([*/])/);
    let sign = 1;
    for (const tok of tokens) {
      if (tok === '*') { sign = 1; continue; }
      if (tok === '/') { sign = -1; continue; }
      const t = tok.trim();
      if (t === '' || t === '1') continue;
      const exp = Object.prototype.hasOwnProperty.call(DIM_ALIAS, t) ? dimMap(DIM_ALIAS[t]) : new Map([[t, 1]]);
      for (const [u, e] of exp) m.set(u, (m.get(u) || 0) + sign * e);
    }
    return m;
  }

  function dimCombine(a, b, sign) {
    const m = new Map(a);
    for (const [u, e] of b) m.set(u, (m.get(u) || 0) + sign * e);
    return m;
  }

  function dimStr(m) {
    const us = [...m].filter(([, e]) => e !== 0).sort((x, y) => (x[0] < y[0] ? -1 : 1));
    const fmt = (u, e) => (Math.abs(e) === 1 ? u : `${u}^${Math.abs(e)}`);
    const num = us.filter(([, e]) => e > 0).map(([u, e]) => fmt(u, e));
    const den = us.filter(([, e]) => e < 0).map(([u, e]) => `/${fmt(u, e)}`);
    return (num.length ? num.join('*') : '1') + den.join('');
  }

  const canon = (str) => dimStr(dimMap(str));

  function inputDim(q) {
    if (q.unit) return q.unit;
    return q.value_type === 'enum' ? `enum:${q.id}` : '';
  }

  function checkDims(node, registry) {
    const quantities = new Map((registry.quantities || []).map((q) => [q.id, q]));
    const problems = [];
    const bad = (path, msg) => problems.push(`${path}: ${msg}`);

    // Returns the canonical declared dim, or null when the node is malformed.
    function walk(n, path, bound) {
      if (!n || typeof n !== 'object' || typeof n.kind !== 'string') {
        bad(path, 'not a node');
        return null;
      }
      if (typeof n.dim !== 'string' || n.dim === '') {
        bad(path, 'missing dim');
        return null;
      }
      const declared = canon(n.dim);
      const sub = (key, extraBound) => walk(n[key], `${path}/${key}`, extraBound || bound);
      const same = (what, dims) => {
        if (dims.every((d) => d !== null) && dims.some((d) => d !== declared)) {
          bad(path, `${what} ${dims.join(' and ')} must all equal ${declared}`);
        }
      };
      switch (n.kind) {
        case 'const': return declared;
        case 'coef': {
          const q = quantities.get(n.id) || (registry.quantities || []).find((x) => x.coef === n.id && x.value_type !== 'table');
          if (q && q.unit && canon(q.unit) !== declared) bad(path, `coef ${n.id} has unit ${canon(q.unit)}, node declares ${declared}`);
          return declared;
        }
        case 'input': {
          const q = quantities.get(n.id);
          if (!q) { bad(path, `unknown input id ${n.id}`); return declared; }
          const d = canon(inputDim(q));
          if (d !== declared) bad(path, `input ${n.id} has dim ${d}, node declares ${declared}`);
          return declared;
        }
        case 'var':
          if (!bound.has(n.name)) bad(path, `unbound var ${n.name}`);
          return declared;
        case 'lookup': {
          const t = quantities.get(n.table_id);
          if (!Array.isArray(n.keys) || n.keys.length === 0) bad(path, 'lookup needs keys');
          else {
            if (t && t.table && t.table.index.length !== n.keys.length) {
              bad(path, `lookup ${n.table_id} has ${n.keys.length} keys, table index has ${t.table.index.length}`);
            }
            n.keys.forEach((k, i) => walk(k, `${path}/keys/${i}`, bound));
          }
          if (t && t.table && t.table.dim && canon(t.table.dim) !== declared) {
            bad(path, `table ${n.table_id} has dim ${canon(t.table.dim)}, node declares ${declared}`);
          }
          return declared;
        }
        case 'add': case 'sub': case 'min': case 'max': {
          const a = sub('a');
          const b = sub('b');
          if (a !== null && b !== null && (a !== b || a !== declared)) {
            bad(path, `${n.kind} operands ${a} and ${b} must both equal ${declared}`);
          }
          return declared;
        }
        case 'mul': case 'div': {
          const a = sub('a');
          const b = sub('b');
          if (a !== null && b !== null) {
            const d = dimStr(dimCombine(dimMap(a), dimMap(b), n.kind === 'mul' ? 1 : -1));
            if (d !== declared) bad(path, `${n.kind} of ${a} and ${b} is ${d}, node declares ${declared}`);
          }
          return declared;
        }
        case 'ceil_to': case 'floor_to': {
          const x = sub('x');
          const step = sub('step');
          if (x !== null && x !== declared) bad(path, `${n.kind} of ${x} declares ${declared}`);
          if (x !== null && step !== null && step !== '1' && step !== x) bad(path, `${n.kind} step ${step} must be 1 or ${x}`);
          return declared;
        }
        case 'branch': {
          const a = walk(n.cond && n.cond.a, `${path}/cond/a`, bound);
          const b = walk(n.cond && n.cond.b, `${path}/cond/b`, bound);
          if (a !== null && b !== null && a !== b) bad(`${path}/cond`, `compares ${a} with ${b}`);
          const t = sub('then');
          const e = sub('else');
          if (t !== null && e !== null && (t !== e || t !== declared)) bad(path, `branch sides ${t} and ${e} must both equal ${declared}`);
          return declared;
        }
        case 'sum_over': {
          const inner = new Set(bound);
          inner.add(n.var);
          const body = sub('body', inner);
          if (body !== null && body !== declared) bad(path, `sum_over body ${body} declares ${declared}`);
          return declared;
        }
        case 'price':
          sub('quantity');
          if (declared !== 'USD') bad(path, `price must have dim USD, got ${declared}`);
          return declared;
        default:
          bad(path, `unknown kind ${n.kind}`);
          return null;
      }
    }

    walk(node, '$', new Set());
    return problems;
  }

  // ---- symbolic rendering --------------------------------------------------------------------

  const PREC = { add: 1, sub: 1, mul: 2, div: 2 };
  const SYM = { add: '+', sub: '−', mul: '×', div: '/' };
  const CMP_SYM = { '<=': '≤', '>=': '≥', '!=': '≠' };

  function render(node, registry) {
    function r(n, minPrec) {
      let s;
      let prec = 4;
      switch (n.kind) {
        case 'const': s = String(n.value); break;
        case 'input': case 'coef': s = n.id; break;
        case 'var': s = n.name; break;
        case 'lookup': s = `lookup(${n.table_id})[${n.keys.map((k) => r(k, 0)).join(', ')}]`; break;
        case 'min': case 'max': s = `${n.kind}(${r(n.a, 0)}, ${r(n.b, 0)})`; break;
        case 'ceil_to': case 'floor_to': s = `${n.kind}(${r(n.x, 0)}, ${r(n.step, 0)})`; break;
        case 'price': s = `price(unit=${n.unit}, rule=${n.rule_id}, ${r(n.quantity, 0)})`; break;
        case 'add': case 'sub': case 'mul': case 'div': {
          prec = PREC[n.kind];
          // The right operand of a non-associative operator needs parentheses at equal precedence.
          const rightMin = n.kind === 'sub' || n.kind === 'div' ? prec + 1 : prec;
          s = `${r(n.a, prec)} ${SYM[n.kind]} ${r(n.b, rightMin)}`;
          break;
        }
        case 'branch': {
          prec = 0;
          const op = CMP_SYM[n.cond.op] || n.cond.op;
          s = `if ${r(n.cond.a, 1)} ${op} ${r(n.cond.b, 1)} then ${r(n.then, 0)} else ${r(n.else, 0)}`;
          break;
        }
        case 'sum_over':
          prec = 0;
          s = `Σ_{${n.var} ∈ ${n.set}} ${r(n.body, 0)}`;
          break;
        default: throw new Error(`unknown node kind ${n.kind}`);
      }
      return prec < minPrec ? `(${s})` : s;
    }
    return r(node, 0);
  }

  const model = { evaluate, evaluateCell, checkDims, render };
  root.SCC = root.SCC || {};
  root.SCC.model = model;
  if (typeof module !== 'undefined' && module.exports) module.exports = model;
})(typeof window !== 'undefined' ? window : globalThis);
