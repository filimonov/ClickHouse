// Versioned URL state: the hash carries the full calculator state plus the manifest of the artifacts it was built with.
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const PREFIX = '#v1.';
  const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
  const STATE_KEYS = ['inputs', 'locks', 'overrides', 'scenario', 'price_preset', 'custom_prices', 'units', 'openCell', 'sweepInput', 'manifest'];
  // Manifest versions and digests compared against the loaded contract: [artifact, version key, digest keys].
  const VERSIONED = [
    ['registry', 'registry_version', ['registry_sha256']],
    ['coverage', 'coverage_version', ['coverage_sha256']],
    ['billing', 'billing_version', ['billing_sha256']],
  ];

  // btoa/atob are avoided: they are Latin-1 only and the state may hold any Unicode (custom labels).
  function b64urlEncode(bytes) {
    let out = '';
    for (let i = 0; i < bytes.length; i += 3) {
      const n = (bytes[i] << 16) | ((bytes[i + 1] || 0) << 8) | (bytes[i + 2] || 0);
      out += ALPHABET[(n >> 18) & 63] + ALPHABET[(n >> 12) & 63];
      if (i + 1 < bytes.length) out += ALPHABET[(n >> 6) & 63];
      if (i + 2 < bytes.length) out += ALPHABET[n & 63];
    }
    return out;
  }

  function b64urlDecode(s) {
    if (!/^[A-Za-z0-9_-]*$/.test(s) || s.length % 4 === 1) throw new Error('not base64url');
    const bytes = [];
    for (let i = 0; i < s.length; i += 4) {
      const chunk = s.slice(i, i + 4);
      let n = 0;
      for (let j = 0; j < 4; j++) n = (n << 6) | (j < chunk.length ? ALPHABET.indexOf(chunk[j]) : 0);
      bytes.push((n >> 16) & 255);
      if (chunk.length > 2) bytes.push((n >> 8) & 255);
      if (chunk.length > 3) bytes.push(n & 255);
    }
    const out = new Uint8Array(bytes);
    // Non-canonical tails (nonzero pad bits) would give several URLs for one state.
    if (b64urlEncode(out) !== s) throw new Error('non-canonical base64url');
    return out;
  }

  // Sorted keys make the hash a pure function of the state, so equal states give equal URLs.
  function canonical(v) {
    if (Array.isArray(v)) return v.map(canonical);
    if (v && typeof v === 'object') {
      const o = {};
      Object.keys(v).sort().forEach((k) => { o[k] = canonical(v[k]); });
      return o;
    }
    return v;
  }

  function encode(state) {
    return PREFIX + b64urlEncode(new TextEncoder().encode(JSON.stringify(canonical(state))));
  }

  function formatProblem(reason) {
    return { kind: 'url_format', reason, missing: [{ artifact: 'url_format', version: null }] };
  }

  const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

  // One mismatch per differing entry; an entry absent on either side counts as a mismatch.
  function compareManifests(url, loaded) {
    const missing = [];
    const diff = (artifact, version, field, a, b) => {
      if (a !== b) missing.push({ artifact, version: version === undefined ? null : version, field, url_value: a === undefined ? null : a, loaded_value: b === undefined ? null : b });
    };
    diff('evaluator', url.code_sha256, 'code_sha256', url.code_sha256, loaded.code_sha256);
    diff('contract', url.contract_version, 'version', url.contract_version, loaded.contract_version);
    for (const [artifact, vkey, dkeys] of VERSIONED) {
      diff(artifact, url[vkey], vkey, url[vkey], loaded[vkey]);
      dkeys.forEach((k) => diff(artifact, url[vkey], k, url[k], loaded[k]));
    }
    const files = (m) => (isObj(m.code_files) ? m.code_files : {});
    const uf = files(url);
    const lf = files(loaded);
    for (const f of new Set([...Object.keys(uf), ...Object.keys(lf)])) diff('evaluator', url.code_sha256, `code_files.${f}`, uf[f], lf[f]);
    const absent = (m) => (Array.isArray(m.code_missing) ? [...m.code_missing].sort().join(',') : undefined);
    diff('evaluator', url.code_sha256, 'code_missing', absent(url), absent(loaded));
    const byReport = (m) => new Map((Array.isArray(m.evidence) ? m.evidence : []).map((e) => [e.report, e]));
    const ue = byReport(url);
    const le = byReport(loaded);
    for (const report of new Set([...ue.keys(), ...le.keys()])) {
      const u = ue.get(report);
      const l = le.get(report);
      const name = `evidence:${report}`;
      if (!u || !l) { diff(name, u ? u.commit : null, 'report', u ? 'present' : undefined, l ? 'present' : undefined); continue; }
      for (const k of new Set([...Object.keys(u), ...Object.keys(l)])) if (k !== 'report') diff(name, u.commit, k, u[k], l[k]);
    }
    return missing;
  }

  const SHAPE = {
    inputs: isObj, locks: Array.isArray, overrides: isObj, scenario: (v) => typeof v === 'string', price_preset: (v) => typeof v === 'string',
    custom_prices: isObj, units: (v) => typeof v === 'string', openCell: (v) => v === null || typeof v === 'string',
    sweepInput: (v) => v === null || typeof v === 'string', manifest: isObj,
  };

  function decode(hash, contract) {
    // The loaded contract is the reference; without its manifest nothing can be checked, so fail loudly.
    if (!contract || !isObj(contract.manifest)) throw new TypeError('decode: contract.manifest is required');
    if (typeof hash !== 'string' || !hash.startsWith(PREFIX)) return { state: null, problem: formatProblem('unknown hash format; expected the "#v1." prefix') };
    let state;
    try {
      const text = new TextDecoder('utf-8', { fatal: true }).decode(b64urlDecode(hash.slice(PREFIX.length)));
      state = JSON.parse(text);
    } catch (e) {
      return { state: null, problem: formatProblem(`malformed hash: ${e.message}`) };
    }
    if (!isObj(state)) return { state: null, problem: formatProblem('hash payload is not an object') };
    for (const k of STATE_KEYS) if (!(k in state) || !SHAPE[k](state[k])) return { state: null, problem: formatProblem(`hash payload: missing or ill-typed "${k}"`) };
    const missing = compareManifests(state.manifest, contract.manifest);
    if (missing.length) {
      const v = state.manifest.contract_version;
      return { state: null, problem: { kind: 'cannot_reproduce', missing, url_contract_version: typeof v === 'string' ? v : null } };
    }
    const bad = valueProblem(state, contract);
    if (bad) return { state: null, problem: formatProblem(`hash payload: ${bad}`) };
    return { state, problem: null };
  }

  const presets = () => (root.SCC && root.SCC.presets) || require('./presets.js');
  const UNITS = ['second', 'minute', 'hour', 'day', 'month'];
  const own = (o, k) => Object.prototype.hasOwnProperty.call(o, k);

  // Values of a decoded state, checked against the loaded contract. Returns the first problem as text, or null.
  // Inputs outside a measured range are not problems: the page computes them and marks them extrapolated.
  function valueProblem(state, contract) {
    const P = presets();
    if (!own(P.SCENARIOS, state.scenario)) return `unknown scenario ${JSON.stringify(state.scenario)}`;
    if (!own(P.PRICES, state.price_preset)) return `unknown price preset ${JSON.stringify(state.price_preset)}`;
    if (!UNITS.includes(state.units)) return `unknown rate unit ${JSON.stringify(state.units)}`;
    if (state.sweepInput !== null && !own(P.SWEEP_INPUTS, state.sweepInput)) return `unknown sweep input ${JSON.stringify(state.sweepInput)}`;
    if (state.openCell !== null) {
      const i = state.openCell.indexOf('.');
      if (i <= 0 || !contract.variants.includes(state.openCell.slice(0, i)) || !contract.operations.includes(state.openCell.slice(i + 1))) {
        return `open cell ${JSON.stringify(state.openCell)} is not a variant.operation of this contract`;
      }
    }
    const byId = new Map(contract.registry.map((q) => [q.id, q]));
    const preset = P.SCENARIOS[state.scenario].values;
    for (const [where, map] of [['inputs', state.inputs], ['overrides', state.overrides]]) {
      for (const [id, v] of Object.entries(map)) {
        const q = byId.get(id);
        if (!q || q.kind !== 'input') return `${where}: ${JSON.stringify(id)} is not a registry input`;
        // Hidden rows are model constants: only a preset may set them, and only to its own value.
        if (q.ui && q.ui.tier === 'hidden' && !(where === 'inputs' && own(preset, id) && preset[id] === v)) return `${where}: ${id} is not a user input`;
        const x = isObj(v) && own(v, 'value') ? v.value : v;
        if (isObj(v) && !(Object.keys(v).length === 2 && UNITS.includes(v.per))) return `${where}.${id}: expected a number, a word or {value, per}`;
        if (typeof x === 'number' ? !Number.isFinite(x) : typeof x !== 'string') return `${where}.${id}: ${JSON.stringify(x)} is not a finite number or a word`;
        if (typeof x === 'string' && q.value_type === 'enum' && !q.enum.includes(x)) return `${where}.${id}: ${JSON.stringify(x)} is not one of ${q.enum.join(', ')}`;
        if (typeof x === 'string' && q.value_type === 'number') return `${where}.${id}: ${JSON.stringify(x)} is not a number`;
      }
    }
    const grouped = new Set((contract.precedence || []).flatMap((g) => g.group));
    for (const id of state.locks) if (typeof id !== 'string' || !grouped.has(id)) return `locks: ${JSON.stringify(id)} is not a member of a precedence group`;
    for (const [k, v] of Object.entries(state.custom_prices)) {
      if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) return `custom price ${k}: ${JSON.stringify(v)} is not a finite number of 0 or more`;
    }
    // Edits stay in the state when another price preset is picked, so they are checked whatever the preset is.
    if (Object.keys(state.custom_prices).length) {
      try {
        P.resolvePrice(contract, 'custom', state.custom_prices);
      } catch (e) {
        return e.message;
      }
    }
    return null;
  }

  const urlstate = { encode, decode, STATE_KEYS, UNITS };
  root.SCC = root.SCC || {};
  root.SCC.urlstate = urlstate;
  if (typeof module !== 'undefined' && module.exports) module.exports = urlstate;
})(typeof window !== 'undefined' ? window : globalThis);
