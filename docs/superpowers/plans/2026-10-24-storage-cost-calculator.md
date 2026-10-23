# Storage Cost Calculator — Plan 3: Model contract, evaluator and page

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the calculator: a versioned model contract (typed expressions over the registry and coefficients), one evaluator that produces both numbers and "show calculation", a billing module with feasible cost-minimising provisioning, the single-page UI with presets, drill-down, sensitivity chart and versioned URLs, the `generate`/`check` script, the closure checks, and the formula document.

**Architecture:** `utils/storage-cost-calculator/` is vanilla JavaScript with no bundler. `model.js` holds the typed expression evaluator (`evaluate`, `explain`), `billing.js` the billing rules and the provisioning allocator, `pipeline.js` the five stages for a set of inputs, `viewmodel.js` turns results into a plain view model (tested in Node), `ui.js` renders it (exercised in the browser), `presets.js` holds scenario presets within measured coverage. `model-contract.json` is generated from the research artifacts by `tools/generate_contract.py` and embedded into `index.html` by the same script. Tests run with `node --test`.

**Tech Stack:** Node 22 (`node --test`, no npm packages), Python 3.13 stdlib for tools, inline SVG for the chart.

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§4 product contract, §4.6 billing, §4.8 output, §6 model contract, §7 page). Plans 1–2 for the artifacts consumed.

## Global Constraints

- No arithmetic that changes a displayed number outside the contract: `model.js` evaluates contract nodes only; `billing.js` applies contract rules only.
- Expression node kinds (exactly): `input`, `coef`, `const`, `add`, `sub`, `mul`, `div`, `min`, `max`, `ceil_to`, `floor_to`, `branch`, `sum_over`, `allocate`, `price`. Every node has `dim` (a unit string such as `req`, `GiB`, `GiB_h`, `USD`, `1`); the dimension check fails the build on a mismatch.
- Monthly expected quantities stay fractional through billing; only `ceil_to`/`floor_to` nodes round; money is rounded at display.
- Unavailable states (`not_supported`, `backlog_grows`, `cannot_reproduce`, `out_of_range` as a label) suppress total, deltas and "cheapest" exactly as spec §4.3/§4.5.
- Month = 730 h; sizes GiB/TiB in the model, GiB→GB in billing where the preset says `decimal`.
- English UI copy. Design tokens on `:root`; light and dark; phone width without horizontal scroll; thousands separators; the preset's currency.
- Tests: `node --test utils/storage-cost-calculator/test/`; Python tools tested with `unittest`.
- Commits per task, `-s`, exact files, `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; no push.

## Review Focus

1. A preset whose values fall outside a coefficient's validity range — every dependent cell, total, delta and chart point labelled extrapolated (Task 6 `test_out_of_range_propagates_to_totals_and_chart`).
2. A URL from an older contract version — "cannot reproduce" with the missing version named, never a recomputed number (Task 7 `test_url_with_unknown_versions_renders_cannot_reproduce`).
3. Manual provisioned IOPS below demand — a note, the manual price, and no silent bump (Task 3 `test_manual_allocation_below_demand_is_priced_as_entered_with_note`).
4. Division by a zero-valued input (zero inserts per day) — `div` node yields a defined result (0 or `unavailable`) per the contract's `on_zero` attribute, never `NaN` on the page (Task 1 `test_div_by_zero_follows_on_zero_policy`).
5. A cut card in `coverage.json` with a non-zero dependent default — build fails (Task 4 `test_generate_fails_on_cut_card_with_nonzero_default`).

---

### Task 1: Typed expression evaluator

**Files:**
- Create: `utils/storage-cost-calculator/model.js`, `utils/storage-cost-calculator/test/model.test.js`

**Interfaces:**
- `evaluate(node, ctx) -> {value: number|null, state: 'ok'|'unavailable', trace: Trace}` where `ctx = {inputs: Map<id, number>, coefs: Map<id, {value, range, evidence}>, sets: Map<id, string[]>, prices: PricePreset, onZero: 'zero'|'unavailable'}`; `Trace = {node, value, dim, children: Trace[], branch_taken?: 'then'|'else', allocation?: {shares}}`.
- `checkDims(node, registry) -> string[]` (problems).
- Node shapes: `{kind:'input', id}`, `{kind:'coef', id}`, `{kind:'const', value, dim}`, binary `{kind, a, b}`, `{kind:'min'|'max', args:[...]}`, `{kind:'ceil_to', x, step}`, `{kind:'branch', cond:{op:'<'|'<='|'>'|'>='|'=='|'!=', a, b}, then, else}`, `{kind:'sum_over', set, var, body}` (body may reference `{kind:'var', name}`), `{kind:'allocate', total, shares:[{cell, weight}]}` → per-cell values, `{kind:'price', unit, rule_id, quantity}`.

- [ ] **Step 1: Failing tests** — `test_const_and_input_evaluate`, `test_add_mul_dims_combine` (`GiB × 1 = GiB`; `GiB + req` fails `checkDims`), `test_branch_records_branch_taken`, `test_ceil_to_rounds_up_to_step`, `test_sum_over_set`, `test_allocate_proportional_shares_sum_to_total`, `test_div_by_zero_follows_on_zero_policy`, `test_coef_outside_range_marks_trace_extrapolated`, `test_unavailable_propagates`.
- [ ] **Step 2: Run** — `node --test utils/storage-cost-calculator/test/model.test.js` FAIL.
- [ ] **Step 3: Implement `model.js`** (ES module; pure functions).
- [ ] **Step 4: Run** — PASS. **Step 5: Commit** — `calculator: typed expression evaluator`.

### Task 2: Contract schema and loader

**Files:**
- Create: `utils/storage-cost-calculator/contract.js`, `docs/superpowers/cost-model/model-contract.schema.json`, `utils/storage-cost-calculator/test/contract.test.js`

**Interfaces:**
- Contract JSON: `{version, built_from: {registry_version, coverage_version, billing_version, reports:[{name, commit}]}, evidence_statement, registry: [...inputs-registry quantities...], precedence: [...], coefficients: [{id, value, dim, range, evidence:{kind, ref, run_id, binary_sha256, commit}, status:'ok'|'unsupported'}], variants: ['ebs','s3','s3plain','cas'], operations: ['storage','inserts','merges','mutations','deletes','reads','recovery','local_disk'], applicability: {variant: {operation: 'supported'|'not_supported'|'unsupported_cut', reason}}, formulas: {variant: {operation: {ledger_rows: [{op_kind, dim:'req'|'GiB'|'GiB_h'|'iops'|'mibps'|'GiB_xfer', expr}], notes: [expr]}}}, states: {variant: [{id:'backlog_grows', cond}]}, presets_ref: 'presets.js'}`.
- `loadContract(json) -> Contract` validates against the schema (in-house minimal validator: required keys, enums, node kinds) and runs `checkDims` over every formula; throws with all problems listed.

- [ ] **Step 1: Failing tests** — `test_load_valid_contract`, `test_missing_applicability_entry_fails`, `test_formula_referencing_unknown_input_fails`, `test_dim_mismatch_fails_with_path`.
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `calculator: contract schema and loader`.

### Task 3: Billing and provisioning

**Files:**
- Create: `utils/storage-cost-calculator/billing.js`, `utils/storage-cost-calculator/test/billing.test.js`, `utils/storage-cost-calculator/test/fixtures/pricing/*.json`

**Interfaces:**
- `bill(ledgerRows: LedgerRow[], preset: PricePreset, scope: 'variant') -> {lines: [{rule_id, unit, quantity, money, tier_breakdown}], total}` applying request classes, outcome billing (billed unless the preset lists the outcome as free; unknown outcomes billed), storage tiers once at scope, incomplete-multipart storage, transfer rules, GiB→GB when `preset.units === 'decimal'`.
- `allocateVolumes(demand: {capacity_gib, peak_iops, peak_mibps}, constraints: Gp3Constraints, manual?: {iops, mibps, volumes}) -> {volumes: [{size_gib, iops, mibps}], monthly_cost, feasible: boolean, notes: string[]}`: enumerates volume counts 1..N (N from max size), for each the minimal IOPS/throughput satisfying demand and the coupling (`mibps ≤ 0.25 × iops`, `iops ≤ 500 × size_gib`, baselines free), prices with the preset's EBS rules, picks the minimum with tie-break (fewest volumes, then smallest total IOPS); manual allocations priced as entered with `feasible` and notes.
- Pricing fixtures (`fixtures/pricing/*.json`): each `{preset, rows_or_demand, expected_money}` computed independently (by hand, written in the fixture's `derivation` string).

- [ ] **Step 1: Failing tests** — `test_request_classes_priced_per_1000`, `test_unknown_outcome_is_billed`, `test_listed_free_outcome_not_billed`, `test_storage_tiers_applied_once_at_scope`, `test_incomplete_multipart_storage_billed`, `test_decimal_units_conversion`, `test_allocate_picks_cheapest_feasible_with_tiebreak` (three candidates, two equal cost → fewer volumes wins), `test_allocate_respects_mibps_per_iops_coupling` (1000 MiB/s demand forces 4000 IOPS), `test_manual_allocation_below_demand_is_priced_as_entered_with_note`, `test_pricing_fixtures_match_exactly` (loads every fixture).
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `calculator: billing rules and cost-minimising provisioning`.

### Task 4: Contract generator and closure checks

**Files:**
- Create: `utils/storage-cost-calculator/tools/generate_contract.py`, `utils/storage-cost-calculator/tools/test_generate_contract.py`
- Modify: `utils/storage-cost-calculator/tools/closure_check.py` (add `check_evidence`, `check_reproduction`)

**Interfaces:**
- `generate_contract.py generate --registry … --coverage … --billing … --reports-dir docs/superpowers/reports --reports docs/superpowers/reports/2026-10-24-storage-cost-*/ --out docs/superpowers/cost-model/model-contract.json --page utils/storage-cost-calculator/index.html --doc docs/superpowers/cost-model/storage-cost-model.md`: builds the contract (coefficients from each report directory's `coefficients.json`; formulas merged from each report directory's `formulas.json`, Plan 2 Global Constraints), renders formula blocks into the doc between `<!-- formulas:begin -->`/`<!-- formulas:end -->` markers, embeds the JSON into the page between `<!-- contract:begin -->`/`<!-- contract:end -->` as `<script type="application/json" id="contract">`.
- `generate_contract.py check …`: re-renders in memory and fails if the standalone JSON, the embedded copy or the doc blocks differ.
- Generation fails when: a cut card's dependent coefficient has `status != 'unsupported'`; a registry default is non-zero for a quantity whose only evidence is a cut card; a formula references an id missing from the registry; a P1 coefficient lacks hold-out evidence (`check_evidence`).
- `render_formula(node, registry) -> str` symbolic rendering (`PUT_per_part × parts_per_day × 30.4 …`) used by both the doc and the page's symbolic view.

- [ ] **Step 1: Failing tests** — `test_generate_embeds_same_json_in_page_and_file`, `test_check_detects_stale_embedded_copy`, `test_generate_fails_on_cut_card_with_nonzero_default`, `test_generate_fails_on_missing_holdout_for_p1`, `test_render_formula_symbolic`.
- [ ] **Step 2–4: Run/implement/pass** (`python3 -m unittest utils/storage-cost-calculator/tools/test_generate_contract.py`).
- [ ] **Step 5: Commit** — `calculator: contract generator and closure checks`.

### Task 5: Pipeline and presets

**Files:**
- Create: `utils/storage-cost-calculator/pipeline.js`, `utils/storage-cost-calculator/presets.js`, `utils/storage-cost-calculator/test/pipeline.test.js`, `utils/storage-cost-calculator/test/presets.test.js`

**Interfaces:**
- `resolveInputs(contract, raw: Map<id, number|string>, locks: Set<id>) -> {inputs, derived: Map<id, {value, rule, overridden}>, problems}` applying precedence (spec §4.3) and derivations.
- `run(contract, inputs, preset) -> Result` with `Result = {variants: {id: {state: 'ok'|'not_supported'|'backlog_grows'|'unsupported_cut', reason, cells: {operation: {money, lines, extrapolated, trace}}, total, extrapolated, allocation, notes}}, cheapest: id|null, deltas: {id: {vs_ebs, vs_s3}|null}, evidence_statement, versions}` — stages 1–5 per cell; "cheapest" only among `ok` columns and never when the column is extrapolated beyond … (label stays, selection allowed; spec §4.5: cheapest describes model estimates); deltas null for non-ok columns.
- `sweep(contract, inputs, preset, inputId, values[]) -> {points: [{x, totals: {variant: number|null, extrapolated}}], breakEven: [{between:[a,b], x}]}`.
- `presets.js`: `SCENARIOS = {logs, metrics, analytics_batch, single_replica_archive, custom}` each `{label, description, values: {...registry ids...}}` and `PRICES = {aws_us_east_1: {...from billing-contract preset...}, gcs: {structure_only: true}, azure: {structure_only: true}, custom}`; a test asserts every scenario's values lie within every referenced coefficient's range (presets within measured coverage).

- [ ] **Step 1: Failing tests** — `test_precedence_lock_switches_authoritative_field`, `test_override_survives_preset_change_and_is_marked`, `test_not_supported_column_has_no_total_and_no_delta`, `test_backlog_grows_state_suppresses_total`, `test_cheapest_excludes_unavailable`, `test_sweep_finds_break_even`, `test_every_scenario_within_coverage`, `test_measured_fixtures_within_tolerance` (reads `measurements.json` of each report; for each fit/hold-out card builds the inputs the card implies and asserts the contract's predicted ledger rows match the measured ones within the recorded tolerance).
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `calculator: pipeline, sweeps and presets`.

### Task 6: View model and page

**Files:**
- Create: `utils/storage-cost-calculator/viewmodel.js`, `utils/storage-cost-calculator/ui.js`, `utils/storage-cost-calculator/index.html`, `utils/storage-cost-calculator/styles.css`, `utils/storage-cost-calculator/test/viewmodel.test.js`

**Interfaces:**
- `viewModel(result, contract, ui: {openCell, sweepInput, theme}) -> VM` with `cards[]` (`title, total_text, delta_texts, state, state_text, bar_segments[{class, fraction}]`), `table.rows[]` (`operation, cells[{variant, text, state, extrapolated, clickable}]`), `drill` (`composition[{op_kind, money, fraction}], stages[{name, lines[]}], sources[]`) for the open cell, `notes[]`, `evidence_statement`, `chart {points, breakEven, labels}`, `advanced_blocks[]` (from the registry: `block, fields[{id, label, value, unit, kind, locked, overridden, range, hint_sql}]`).
- `ui.js`: renders the VM into `index.html` containers, wires inputs (sliders + editable numbers, locks, reset), presets, cell click/hover, "show calculation" toggle, theme, URL state (Task 7). Formatting helpers `fmtMoney(n, currency)`, `fmtNum(n)`.
- Layout and look per spec §7 (two panels, left sticky, cards, table, drill-down, notes, chart); tokens on `:root`; `@media (prefers-color-scheme: dark)`; phone width via a single-column breakpoint.

- [ ] **Step 1: Failing tests** — `test_card_state_text_for_not_supported`, `test_bar_segments_sum_to_one`, `test_out_of_range_propagates_to_totals_and_chart`, `test_drill_shows_branch_taken_and_allocation`, `test_advanced_blocks_from_registry_with_hint_sql`, `test_fmt_money_thousands_and_currency`.
- [ ] **Step 2–4: Run/implement/pass.**
- [ ] **Step 5: Page smoke** — `python3 -m http.server -d utils/storage-cost-calculator 8765` and open; also `file://` open works (embedded contract). Record a screenshot path in LEDGER.md.
- [ ] **Step 6: Commit** — `calculator: view model and page`.

### Task 7: URL state, hints and generated documentation

**Files:**
- Modify: `ui.js`, `pipeline.js`; Create: `utils/storage-cost-calculator/urlstate.js`, `test/urlstate.test.js`, `docs/superpowers/cost-model/storage-cost-model.md` (hand-written prose + generated blocks), `docs/superpowers/cost-model/hints.json` (SQL per advanced block with column→field mapping)

**Interfaces:**
- `encodeState(inputs, locks, presetIds, versions) -> string` (URL hash, compact JSON base64url); `decodeState(hash, contract) -> {inputs, locks, presetIds, versions, problem: null | {missing: [...]}}`; a missing version yields `problem` and the page renders "cannot reproduce: <artifact> <version> not present".
- `storage-cost-model.md`: frontmatter; sections `Evidence policy`, `Inputs` (rendered table from the registry), `Formulas` (generated blocks per variant × operation with the symbolic rendering and the coefficient provenance), `Billing rules` (from the billing contract), `Known inaccuracies` (hand-written, includes the blog part-4 disagreements), `Break-even regions` (Task 8), `Reproduce` (commands).

- [ ] **Step 1: Failing tests** — `test_roundtrip_state`, `test_url_with_unknown_versions_renders_cannot_reproduce`, `test_hints_cover_every_advanced_block`.
- [ ] **Step 2–4: Run/implement/pass**; run `generate_contract.py generate` then `check`.
- [ ] **Step 5: Commit** — `calculator: URL state, hints and formula document`.

### Task 8: Break-even sweeps and the blog hand-off

**Files:**
- Create: `utils/storage-cost-calculator/tools/sweeps.mjs` (Node script using `pipeline.sweep` to produce the break-even table for each scenario preset over replicas, insert size, retention, read fraction), output appended to `storage-cost-model.md` `Break-even regions` by `generate_contract.py` (reads `docs/superpowers/cost-model/breakeven.json`).
- Modify: `docs/superpowers/cost-model/storage-cost-model.md` (`Known inaccuracies`: list of statements in `docs/superpowers/blog/2026-cas-storage-model/part4-cheap-self-sufficient.md` on the `cas-blog-article` branch that disagree with the model, read via `git show cas-blog-article:docs/superpowers/blog/2026-cas-storage-model/part4-cheap-self-sufficient.md`).

- [ ] **Step 1: Test** — `test_breakeven_json_has_every_scenario_and_sweep_input`.
- [ ] **Step 2–4: Implement/run/pass.** **Step 5: Commit** — `calculator: break-even sweeps and known inaccuracies`.

### Task 9: Whole-branch verification

- [ ] **Step 1:** `node --test utils/storage-cost-calculator/test/` PASS; `python3 -m unittest discover -s utils/cas-yardstick/tests -t utils/cas-yardstick` PASS; `python3 utils/storage-cost-calculator/tools/closure_check.py` exit 0; `python3 utils/storage-cost-calculator/tools/generate_contract.py check` exit 0.
- [ ] **Step 2:** Codex review of the contract + evaluator + billing (scope: can a displayed number come from outside the contract; are the five closure checks implemented as the spec states) → close MAJORs.
- [ ] **Step 3:** LEDGER.md: step 4 done; hand to the umbrella review (step 5 of the unattended run).
- [ ] **Step 4: Commit** — `calculator: verification and review dispositions`.
