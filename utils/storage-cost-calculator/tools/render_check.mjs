#!/usr/bin/env node
// Renders a drill JSON with the same function the page uses and checks that the substituted values appear.
import { createRequire } from 'node:module';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const ui = require('../ui.js');

function fail(msg) {
  console.error(`render_check.mjs: ${msg}`);
  process.exitCode = 1;
}

function main(argv) {
  const file = argv.find((a) => !a.startsWith('--'));
  const pi = argv.indexOf('--probe');
  const probe = pi >= 0 ? argv[pi + 1] : null;
  if (!file) return fail('usage: render_check.mjs <drill.json> --probe <name>');
  const drill = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (probe && drill.probe !== probe) return fail(`drill is for probe ${drill.probe}, expected ${probe}`);
  if (drill.mismatches && drill.mismatches.length) return fail(`drill JSON lists ${drill.mismatches.length} mismatch(es)`);
  const html = ui.renderDrillHtml(drill);
  const problems = [];
  if (/data-exact="false"/.test(html)) problems.push('a row is marked data-exact="false"');
  let values = 0;
  let moneyCount = 0;
  for (const [key, cell] of Object.entries(drill.cells)) {
    // Each row is looked up inside its own <tr>, so a number that happens to appear elsewhere does not count.
    const trs = html.split('<tr ').filter((t) => t.startsWith('data-row='));
    cell.stage2_rows.forEach((r, i) => {
      const tr = trs.find((t) => t.startsWith(`data-row="${key}#${i}"`)) || '';
      for (const f of ['predicted', 'measured']) {
        if (r[f] === null || r[f] === undefined) continue;
        values++;
        if (!tr.includes(`data-value="${r[f]}"`)) problems.push(`${key}: row "${r.row}" ${f} ${r[f]} not rendered`);
      }
    });
    for (const l of cell.stage4_billing_lines || []) {
      moneyCount++;
      if (!html.includes(`data-value="${l.money}" data-kind="money"`)) problems.push(`${key}: money ${l.money} of ${l.rule_id} not rendered`);
    }
  }
  if (!values) problems.push('no predicted/measured value to check');
  if (problems.length) return fail(problems.join('\n  '));
  console.log(`render_check OK ${drill.probe}: ${values} values, ${moneyCount} money lines`);
  return undefined;
}

try { main(process.argv.slice(2)); } catch (e) { fail(e.message); }
