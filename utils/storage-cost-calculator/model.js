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
    return ok(node, (node.kind === 'ceil_to' ? Math.ceil(q) : Math.floor(q)) * s, kids, ex);
  }

  function evalLookup(node, ctx) {
    const keys = node.keys.map((k) => evaluate(k, ctx));
    const kids = keys.map((k) => k.trace);
    const ex = anyExtrapolated(keys);
    if (anyUnavailable(keys)) return unavailable(node, kids, ex);
    const key = keys.map((k) => String(k.value)).join('|');
    const table = ctx.tables.get(node.table_id);
    const extra = { lookup_key: key };
    if (!table || !Object.prototype.hasOwnProperty.call(table.values, key)) {
      return unavailable(node, kids, ex, extra);
    }
    return ok(node, table.values[key], kids, ex, extra);
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
        if (typeof node.value !== 'number' && !(typeof node.dim === 'string' && node.dim.startsWith('enum:'))) {
          throw new Error(`const: non-numeric value ${JSON.stringify(node.value)} needs an enum dim`);
        }
        return ok(node, node.value, []);
      case 'input': {
        const v = ctx.inputs.get(node.id);
        return v === undefined ? unavailable(node, []) : ok(node, v, []);
      }
      case 'coef': {
        const c = ctx.coefs.get(node.id);
        if (!c || c.value === undefined || c.value === null) return unavailable(node, []);
        return ok(node, c.value, [], validityExtrapolated(c.validity, ctx));
      }
      case 'var':
        if (!ctx.vars.has(node.name)) throw new Error(`var: unbound ${node.name}`);
        return ok(node, ctx.vars.get(node.name), []);
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

  function evaluateCell(cell, ctx) {
    let extrapolated = false;
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
        allocation[share.cell] = r.value;
      }
    }
    const states = cell.states.map((s) => {
      if (s.cond === true) return { id: s.id, active: true, reason: s.reason };
      const r = evaluate(s.cond, ctx);
      extrapolated = extrapolated || r.extrapolated;
      return { id: s.id, active: r.state === 'ok' && r.value !== 0 && r.value !== false, reason: s.reason };
    });
    return { rows, allocation, extrapolated, states };
  }

  // ---- static checks -------------------------------------------------------------------------

  function inputDim(q) {
    if (q.unit) return q.unit;
    return q.value_type === 'enum' ? `enum:${q.id}` : '';
  }

  function checkDims(node, registry) {
    const quantities = new Map((registry.quantities || []).map((q) => [q.id, q]));
    const problems = [];
    const bad = (path, msg) => problems.push(`${path}: ${msg}`);

    function walk(n, path, bound) {
      if (!n || typeof n !== 'object' || typeof n.kind !== 'string') {
        bad(path, 'not a node');
        return null;
      }
      if (typeof n.dim !== 'string' || n.dim === '') {
        bad(path, 'missing dim');
        return null;
      }
      const sub = (key, extraBound) => walk(n[key], `${path}/${key}`, extraBound || bound);
      switch (n.kind) {
        case 'const': case 'coef': return n.dim;
        case 'input': {
          const q = quantities.get(n.id);
          if (!q) { bad(path, `unknown input id ${n.id}`); return n.dim; }
          const d = inputDim(q);
          if (d !== n.dim) bad(path, `input ${n.id} has dim ${d}, node declares ${n.dim}`);
          return n.dim;
        }
        case 'var':
          if (!bound.has(n.name)) bad(path, `unbound var ${n.name}`);
          return n.dim;
        case 'lookup':
          if (!Array.isArray(n.keys) || n.keys.length === 0) bad(path, 'lookup needs keys');
          else n.keys.forEach((k, i) => walk(k, `${path}/keys/${i}`, bound));
          return n.dim;
        case 'add': case 'sub': case 'min': case 'max': {
          const a = sub('a');
          const b = sub('b');
          if (a !== null && b !== null && (a !== b || a !== n.dim)) {
            bad(path, `${n.kind} operands ${a} and ${b} must both equal ${n.dim}`);
          }
          return n.dim;
        }
        case 'mul': case 'div': {
          const a = sub('a');
          const b = sub('b');
          if (a !== null && b !== null) {
            if (a === '1' && b !== n.dim) bad(path, `${n.kind}: ${b} must equal ${n.dim} when the other operand is 1`);
            else if (b === '1' && a !== n.dim) bad(path, `${n.kind}: ${a} must equal ${n.dim} when the other operand is 1`);
          }
          return n.dim;
        }
        case 'ceil_to': case 'floor_to': {
          const x = sub('x');
          sub('step');
          if (x !== null && x !== n.dim) bad(path, `${n.kind} of ${x} declares ${n.dim}`);
          return n.dim;
        }
        case 'branch': {
          const a = walk(n.cond && n.cond.a, `${path}/cond/a`, bound);
          const b = walk(n.cond && n.cond.b, `${path}/cond/b`, bound);
          if (a !== null && b !== null && a !== b) bad(`${path}/cond`, `compares ${a} with ${b}`);
          const t = sub('then');
          const e = sub('else');
          if (t !== null && e !== null && (t !== e || t !== n.dim)) bad(path, `branch sides ${t} and ${e} must both equal ${n.dim}`);
          return n.dim;
        }
        case 'sum_over': {
          const inner = new Set(bound);
          inner.add(n.var);
          const body = sub('body', inner);
          if (body !== null && body !== n.dim) bad(path, `sum_over body ${body} declares ${n.dim}`);
          return n.dim;
        }
        case 'price':
          sub('quantity');
          if (n.dim !== 'USD') bad(path, `price must have dim USD, got ${n.dim}`);
          return n.dim;
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
