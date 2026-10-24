// Contract loader: validates a contract/1 document and merges per-owner formulas files.
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const SCC = root.SCC || (root.SCC = {});
  const CONTRACT_SCHEMA = 'contract/1';
  const TOP_TYPES = {
    version: 'string', manifest: 'object', evidence_statement: 'string', registry: 'array', sets: 'object',
    precedence: 'array', coefficients: 'array', billing: 'object', variants: 'array', operations: 'array', families: 'array',
    applicability: 'object', formulas: 'object', states: 'object',
  };
  const MANIFEST_STRINGS = ['code_sha256', 'contract_version', 'registry_version', 'coverage_version', 'billing_version'];
  const EVIDENCE_KEYS = ['report', 'commit', 'raw_sha256sums'];

  const typeOf = (v) => (Array.isArray(v) ? 'array' : v === null ? 'null' : typeof v);
  const model = () => root.SCC.model || require('./model.js');

  function mergeFormulas(files) {
    const formulas = {};
    const states = {};
    const owners = {};
    const problems = [];
    files.forEach((f, i) => {
      if (!f || f.schema !== 'formulas/1') { problems.push(`formulas file #${i}: schema must be formulas/1`); return; }
      if (typeof f.variant !== 'string' || !f.variant) { problems.push(`formulas file #${i}: missing variant`); return; }
      if (typeof f.owner !== 'string' || !f.owner) { problems.push(`formulas file #${i}: missing owner`); return; }
      formulas[f.variant] = formulas[f.variant] || {};
      states[f.variant] = states[f.variant] || [];
      owners[f.variant] = owners[f.variant] || {};
      for (const [op, cell] of Object.entries(f.cells || {})) {
        if (!cell || typeof cell !== 'object') { problems.push(`cells.${f.variant}.${op} (${f.owner}): expected object, got ${typeOf(cell)}`); continue; }
        if (formulas[f.variant][op]) {
          problems.push(`cell ${f.variant}.${op} is defined by both ${owners[f.variant][op]} and ${f.owner}`);
          continue;
        }
        formulas[f.variant][op] = cell;
        owners[f.variant][op] = f.owner;
        (Array.isArray(cell.states) ? cell.states : []).forEach((s, k) => {
          if (!s || typeof s !== 'object') problems.push(`cells.${f.variant}.${op}.states[${k}] (${f.owner}): expected object, got ${typeOf(s)}`);
          else states[f.variant].push(Object.assign({}, s, { operation: op }));
        });
      }
    });
    return { formulas, states, problems };
  }

  // Calls fn(node, path) for every expression node of a cell, with the path used in problem lines.
  function eachNode(cell, base, fn) {
    const list = (v) => (Array.isArray(v) ? v : []);
    const field = (o, k) => (o && typeof o === 'object' ? o[k] : undefined);
    list(cell.ledger_rows).forEach((r, i) => fn(field(r, 'expr'), `${base}.ledger_rows[${i}].expr`));
    list(field(cell.allocation, 'shares')).forEach((x, j) => fn(field(x, 'weight'), `${base}.allocation.shares[${j}].weight`));
    // `cond: true` is an unconditional state (formulas grammar v1), not an expression node.
    list(cell.states).forEach((x, k) => { if (field(x, 'cond') !== true) fn(field(x, 'cond'), `${base}.states[${k}].cond`); });
    list(cell.notes).forEach((n, m) => fn(n, `${base}.notes[${m}]`));
  }

  // Reports references to coefficients that the contract does not carry; checkDims covers inputs.
  function checkRefs(node, path, coefs, problems) {
    if (!node || typeof node !== 'object') return;
    if (node.kind === 'coef') {
      if (!coefs.has(node.id)) problems.push(`${path}: unknown coef ${node.id}`);
      else if (coefs.get(node.id) === 'table') problems.push(`${path}: coef ${node.id} is a table coefficient; use lookup`);
    }
    if (node.kind === 'lookup') {
      if (!coefs.has(node.table_id)) problems.push(`${path}: unknown table ${node.table_id}`);
      else if (coefs.get(node.table_id) !== 'table') problems.push(`${path}: ${node.table_id} is not a table coefficient`);
    }
    // Sub-paths follow checkDims: /a, /keys/0, /cond/a.
    for (const [key, v] of Object.entries(node)) {
      if (Array.isArray(v)) v.forEach((x, i) => checkRefs(x, `${path}/${key}/${i}`, coefs, problems));
      else if (v && typeof v === 'object') checkRefs(v, `${path}/${key}`, coefs, problems);
    }
  }

  function checkShape(c, problems) {
    if (c.schema !== CONTRACT_SCHEMA) problems.push(`schema: expected ${CONTRACT_SCHEMA}, got ${JSON.stringify(c.schema)}`);
    for (const [key, type] of Object.entries(TOP_TYPES)) {
      if (typeOf(c[key]) !== type) problems.push(`${key}: expected ${type}, got ${typeOf(c[key])}`);
    }
    if (typeOf(c.manifest) === 'object') {
      for (const k of MANIFEST_STRINGS) {
        if (typeof c.manifest[k] !== 'string') problems.push(`manifest.${k}: expected string, got ${typeOf(c.manifest[k])}`);
      }
      if (!Array.isArray(c.manifest.evidence)) problems.push('manifest.evidence: expected array');
      else {
        c.manifest.evidence.forEach((e, i) => EVIDENCE_KEYS.forEach((k) => {
          if (!e || typeof e[k] !== 'string') problems.push(`manifest.evidence[${i}].${k}: expected string`);
        }));
      }
    }
    if (typeOf(c.billing) === 'object' && c.billing.schema !== 'billing/1') {
      problems.push(`billing.schema: expected billing/1, got ${JSON.stringify(c.billing.schema)}`);
    }
  }

  const isObj = (v) => typeOf(v) === 'object';

  // Returns id -> kind for the well-formed coefficient records; malformed elements become problems.
  function checkUnique(c, problems, tables) {
    const seen = new Set();
    c.registry.forEach((q, i) => {
      if (!isObj(q)) { problems.push(`registry[${i}]: expected object, got ${typeOf(q)}`); return; }
      if (seen.has(q.id)) problems.push(`registry: duplicate quantity id ${q.id}`);
      seen.add(q.id);
    });
    const owner = new Map();
    c.coefficients.forEach((rec, i) => {
      if (!isObj(rec)) { problems.push(`coefficients[${i}]: expected object, got ${typeOf(rec)}`); return; }
      const ref = rec.evidence && rec.evidence.ref;
      if (owner.has(rec.id)) {
        problems.push(`coefficients: duplicate coefficient id ${rec.id} (owners ${owner.get(rec.id).ref} and ${ref})`);
        return;
      }
      owner.set(rec.id, { ref, kind: rec.kind });
      if (rec.kind === 'table') {
        if (!isObj(rec.value)) problems.push(`coefficients[${i}] (${rec.id}).value: table coefficient needs {index, values}, got ${typeOf(rec.value)}`);
        else tables[rec.id] = { index: rec.value.index, values: rec.value.values, dim: rec.dim, validity: rec.validity || null };
      }
    });
    return new Map([...owner].map(([id, o]) => [id, o.kind]));
  }

  // An `all` row bills every outcome of its op; another outcome row for the same row type, op and caller (a null
  // caller covers them all) would bill those requests twice.
  function checkAllRows(cell, base, problems) {
    const rows = (Array.isArray(cell.ledger_rows) ? cell.ledger_rows : []).filter(isObj);
    rows.forEach((a, i) => {
      if (a.outcome_class !== 'all') return;
      rows.forEach((b, j) => {
        if (j === i || b.row_type !== a.row_type || b.op_kind !== a.op_kind) return;
        if (a.caller && b.caller && a.caller !== b.caller) return;
        if (b.outcome_class === 'all' && j < i) return;
        problems.push(`${base}.ledger_rows[${j}]: ${a.row_type} ${a.op_kind} has both an all row (ledger_rows[${i}]) and a ${b.outcome_class} row`);
      });
    });
  }

  function checkFormulas(c, kinds, problems) {
    const registryArtifact = { quantities: c.registry.filter(isObj), sets: c.sets, precedence: c.precedence };
    for (const [variant, ops] of Object.entries(c.formulas)) {
      if (!isObj(ops)) { problems.push(`formulas.${variant}: expected object, got ${typeOf(ops)}`); continue; }
      for (const [op, cell] of Object.entries(ops)) {
        const base = `formulas.${variant}.${op}`;
        if (!isObj(cell)) { problems.push(`${base}: expected object, got ${typeOf(cell)}`); continue; }
        checkAllRows(cell, base, problems);
        eachNode(cell, base, (node, path) => {
          for (const p of model().checkDims(node, registryArtifact)) problems.push(path + p.slice(1));
          checkRefs(node, path, kinds, problems);
        });
        if (!c.applicability[variant] || !c.applicability[variant][op]) {
          problems.push(`applicability.${variant}.${op}: missing entry for a formulas cell`);
        }
      }
    }
  }

  // The domain comes from the contract's own variants, operations and families, never from applicability.
  function checkApplicability(c, problems) {
    if (c.families.length === 0) problems.push('families: must be non-empty');
    c.families.forEach((f, i) => { if (typeof f !== 'string') problems.push(`families[${i}]: expected string, got ${typeOf(f)}`); });
    const families = c.families.filter((f) => typeof f === 'string');
    const allowed = new Set(['supported', 'not_supported', 'unsupported_cut', 'not_applicable']);
    for (const v of c.variants) {
      for (const op of c.operations) {
        for (const f of families) {
          const val = c.applicability[v] && c.applicability[v][op] && c.applicability[v][op][f];
          if (val === undefined) problems.push(`applicability.${v}.${op}.${f}: missing from the domain`);
          else if (!allowed.has(val)) problems.push(`applicability.${v}.${op}.${f}: invalid value ${JSON.stringify(val)}`);
        }
      }
    }
  }

  // Returns a shallow-frozen copy; nested objects are shared with the input JSON.
  function load(json) {
    const problems = [];
    const c = json && typeof json === 'object' ? json : {};
    checkShape(c, problems);
    const tables = {};
    // Later checks need these arrays and objects; a malformed one is already reported above.
    const ok = (k) => typeOf(c[k]) === TOP_TYPES[k];
    if (ok('registry') && ok('coefficients')) {
      const kinds = checkUnique(c, problems, tables);
      if (ok('sets') && ok('precedence') && ok('formulas') && ok('applicability')) checkFormulas(c, kinds, problems);
    }
    if (ok('applicability') && ok('variants') && ok('operations') && ok('families')) checkApplicability(c, problems);
    if (problems.length > 0) {
      const err = new Error(problems.join('\n'));
      err.problems = problems;
      throw err;
    }
    return Object.freeze(Object.assign({}, c, { tables }));
  }

  SCC.contract = { load, mergeFormulas, CONTRACT_SCHEMA };
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.contract;
})(typeof window !== 'undefined' ? window : globalThis);
