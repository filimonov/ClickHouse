'use strict';
// The five closure checks (design §5.2) and the generated-output check, run as part of the page's suite.
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const TOOLS = path.join(__dirname, '..', 'tools');

function run(script, args) {
  const done = spawnSync('python3', [path.join(TOOLS, script), ...args], { encoding: 'utf8', env: process.env, maxBuffer: 64 << 20 });
  const tail = `${done.stdout || ''}${done.stderr || ''}`.trim().split('\n').slice(-30).join('\n');
  return { status: done.status, tail: done.error ? String(done.error) : tail };
}

test('test_closure_checks_and_generated_outputs_pass_in_final_mode', (t) => {
  if (process.env.SCC_CLOSURE_CHILD === '1') {
    t.skip('running inside closure_check.py, which runs these checks itself');
    return;
  }
  if (process.env.SCC_DEV === '1') {
    t.skip('SCC_DEV=1: final-mode closure checks need every report, cell and code file; run without SCC_DEV to enforce them');
    return;
  }
  for (const [script, args] of [['closure_check.py', ['--mode', 'final']], ['generate_contract.py', ['check', '--mode', 'final']]]) {
    const r = run(script, args);
    assert.equal(r.status, 0, `${script} ${args.join(' ')} failed:\n${r.tail}`);
  }
});
