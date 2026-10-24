#!/usr/bin/env node
// Break-even sweeps (spec §4.5): for every scenario preset and sweep input, the totals of each variant along a ladder
// of values and the intervals where two variants swap order. Writes docs/superpowers/cost-model/breakeven.json.
//
//   node tools/sweeps.mjs --contract utils/storage-cost-calculator/model-contract.json \
//        --out docs/superpowers/cost-model/breakeven.json
//
// The output records the contract version it was swept on. generate_contract.py compares it with the contract and
// hashes the file into the manifest, so a stale file is reported. Run generate, then this, then generate again.
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// Geometric ladders, so a crossing is bracketed at any scale. Each is `start * ratio^k`.
//  data_bytes  64 GiB x 4^k, k = 0..8       64 GiB .. 4 PiB
//  ingest_rate 1 GiB/day x 4^k, k = 0..7    1 .. 16,384 GiB/day
//  replicas    1 x 2^k, k = 0..3            1, 2, 4, 8 (8 lies outside the measured replicas validity [1, 6])
const geometric = (start, ratio, n) => Array.from({ length: n }, (_, k) => start * ratio ** k);
const LADDERS = {
  data_bytes: geometric(64, 4, 9),
  ingest_rate: geometric(1, 4, 8),
  replicas: geometric(1, 2, 4),
};
const PRICE_PRESET = 'aws-us-east-1';

function uniq(list) {
  return [...new Set(list)];
}

// One entry per scenario x input. `scenarios`: {id: {label, values, locks}}; `ladders`: {inputId: [x...]}.
function buildBreakeven(code, contract, scenarios, ladders, pricePreset) {
  const { pipeline } = code;
  const out = {};
  for (const [sid, sc] of Object.entries(scenarios)) {
    const base = pipeline.resolveInputs(contract, sc.values, sc.locks, {});
    const sweeps = {};
    for (const [inputId, values] of Object.entries(ladders)) {
      const s = pipeline.sweep(contract, base.inputs, pricePreset, inputId, values, { lockOrder: base.lockOrder });
      const states = {};
      for (const v of contract.variants) states[v] = uniq(s.points.map((p) => p.totals[v].state));
      const problems = uniq([...base.problems, ...s.points.flatMap((p) => p.problems)]);
      sweeps[inputId] = {
        values,
        points: s.points.map((p) => ({ x: p.x, totals: p.totals })),
        states,
        crossing: s.breakEven.length ? s.breakEven : null,
        problems,
      };
    }
    out[sid] = { label: sc.label || sid, sweeps };
  }
  return out;
}

function main(argv) {
  const opt = { contract: null, out: null, 'code-dir': path.resolve(HERE, '..') };
  for (let i = 0; i < argv.length; i += 2) {
    const k = argv[i].replace(/^--/, '');
    if (!(k in opt) || argv[i + 1] === undefined) throw new Error(`usage: sweeps.mjs --contract FILE --out FILE [--code-dir DIR]; got ${argv[i]}`);
    opt[k] = argv[i + 1];
  }
  if (!opt.contract || !opt.out) throw new Error('usage: sweeps.mjs --contract FILE --out FILE [--code-dir DIR]');
  const dir = path.resolve(opt['code-dir']);
  const code = { pipeline: require(path.join(dir, 'pipeline.js')), presets: require(path.join(dir, 'presets.js')) };
  const loader = require(path.join(dir, 'contract.js'));
  const raw = JSON.parse(fs.readFileSync(opt.contract, 'utf8'));
  const contract = loader.load(JSON.parse(JSON.stringify(raw)));
  const doc = {
    schema: 'breakeven/1',
    contract_version: raw.version,
    price_preset: PRICE_PRESET,
    ladders: LADDERS,
    scenarios: buildBreakeven(code, contract, code.presets.SCENARIOS, LADDERS, PRICE_PRESET),
  };
  fs.writeFileSync(opt.out, JSON.stringify(doc, null, 1) + '\n');
}

export { buildBreakeven, LADDERS, PRICE_PRESET };
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main(process.argv.slice(2));
