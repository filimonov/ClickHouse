# Storage Cost Calculator — Plan 3: Pipeline, page, generator and documentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the calculator on top of the Plan 1 evaluator, contract loader and billing: the input pipeline (precedence, overrides, derivations), the per-variant evaluation with states, sweeps and break-even, the contract generator with `generate`/`check` and the five closure checks, the page (two panels, cards, table, drill-down, notes, chart, presets, hints, versioned URLs, light/dark, phone width), the formula document, and the browser checks.

**Architecture:** Classic browser scripts with a `window.SCC` namespace and Node export shims (Plan 1 Task 11 convention): `model.js`, `contract.js`, `billing.js` (Plan 1), `pipeline.js`, `viewmodel.js`, `urlstate.js`, `presets.js`, `ui.js`; `index.html` loads them with `<script src>` and embeds the contract JSON in a `<script type="application/json" id="contract">` block so `file://` works without module imports. `tools/generate_contract.py` builds `model-contract.json` from the research artifacts and writes the embedded copy, the standalone JSON and the formula blocks of the document; `check` fails on any drift.

**Tech Stack:** Node 22 (`node --test`), Python 3.13 stdlib tools, inline SVG chart, CSS custom properties.

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§4.2–§4.8, §5.2, §6, §7, §8). Plans 1–2 for the artifacts consumed; Plan 1 Task 0 schemas are normative.

## Global Constraints

- No arithmetic that changes a displayed number outside the contract: `pipeline.js` only calls `SCC.model` and `SCC.billing` with contract nodes and rules.
- Grammar `formulas/1` exactly as Plan 1 Task 0; monthly expected quantities stay fractional through billing; rounding only via `ceil_to`/`floor_to` nodes; money rounded at display.
- State model: `variant.state ∈ {ok, not_supported, backlog_grows, unsupported_cut}` suppresses total, deltas and "cheapest"; `extrapolated: boolean` is an orthogonal flag kept on cells, totals, deltas and chart points with the label "extrapolated"; `page_state ∈ {ok, prices_missing, cannot_reproduce}`: `prices_missing` (structure-only price preset) suppresses money but shows quantities; `cannot_reproduce` suppresses every number. Deltas are `{value, extrapolated} | null`, `null` when either the column or its reference column (EBS or `s3`) is not `ok`. Price preset ids are the billing contract's (`aws-us-east-1`, `gcs`, `azure`, `custom`) everywhere.
- Manual provisioning is a product feature: registry inputs `provisioned_iops`, `provisioned_mibps`, `volumes_per_replica` (enum `auto` or a number) flow from the EBS and Local-disk blocks through `pipeline` into `allocateVolumes(manual)`; the UI shows the allocator's notes.
- Billing aggregation: evaluate cell quantities → aggregate once per variant at provider scope → bill once → allocate back to cells (`allocate` nodes) → display.
- Month = 730 h; sizes GiB/TiB in the model, GiB→GB in billing where the preset says `decimal`.
- English copy; tokens on `:root`; light and dark; phone width without horizontal scroll; thousands separators; the preset's currency; "cheapest" never on a non-`ok` column.
- Tests: `node --test utils/storage-cost-calculator/test/`; tools with `unittest`. Browser checks are a checklist in Task 7 executed by the implementer and recorded in LEDGER.md with screenshots in `tmp/screens/`.
- Commits per task, `-s`, exact files, `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; no push.

## Review Focus

1. A preset value outside a coefficient's validity — dependent cells, totals, deltas and chart points labelled extrapolated, numbers still shown (Task 2 `test_extrapolated_flag_propagates_to_totals_deltas_and_chart`).
2. A URL from a contract version that is not present — `cannot_reproduce` with the missing artifact and version named; no number rendered (Task 5 `test_url_with_unknown_versions_sets_cannot_reproduce`).
3. EBS not comparable (e.g. cut) while `s3` is `ok` — `vs_ebs` is `null` for every column (Task 2 `test_delta_null_when_reference_unavailable`).
4. A user override of a term whose card was cut — the column becomes `unsupported_cut` with the reason, never a number (Task 2 `test_nonzero_override_of_cut_term_makes_column_unavailable`).
5. Locking `data_bytes` and `retention_days` — `ingest_rate` is derived and greyed; switching the lock recomputes (Task 1 `test_stock_flow_retention_any_two_locked`).

---

### Task 1: Input pipeline (precedence groups, overrides, derivations)

**Files:**
- Create: `utils/storage-cost-calculator/pipeline.js` (part 1: inputs), `utils/storage-cost-calculator/test/pipeline_inputs.test.js`

**Interfaces:**
- `SCC.pipeline.resolveInputs(contract, raw: {id: value}, lockOrder: [id] per group, overrides: {id: value}) -> {inputs: Map<id, number|string>, fields: {id: {value, kind, locked, derived, overridden, rule_text, validity, out_of_range: boolean}}, lockOrder, problems: [str]}`. Rules: (1) in every precedence group exactly `group.length − 1` members are locked; `lockOrder` (oldest first) is state, persisted in the URL; locking a member unlocks the oldest other lock; (2) an override of a group member **locks** that member (its value is the override) and therefore unlocks the oldest other lock — so the group's equation always holds and an unlocked member is never overridden; (3) groups never share members (schema rule) so no cross-group conflict exists; (4) reset of an override restores the member's derived role and the group's `default_locked`; (5) preset or simple-input changes recompute derived fields and keep overrides and lock order; (6) `enum` inputs validated; (7) rates arrive normalised to per day by `SCC.pipeline.normaliseRate(value, unit)`.

- [ ] **Step 1: Tests** — `test_stock_flow_retention_any_two_locked` (three lock combinations, each derives the third), `test_lock_change_unlocks_oldest`, `test_override_locks_member_and_group_equation_holds`, `test_override_survives_preset_change_and_is_marked`, `test_reset_override_restores_default_locks`, `test_lock_order_roundtrips_as_state`, `test_enum_input_validated`, `test_normalise_rate_units`, `test_out_of_range_flag_on_field`, `test_manual_provisioning_inputs_reach_allocator`.
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: input pipeline with precedence groups and overrides`.

### Task 2: Evaluation pipeline, states, sweeps

**Files:**
- Modify: `pipeline.js` (part 2); Create: `test/pipeline_eval.test.js`, `test/fixtures/contracts/minimal.json` (hand-written contract covering every state), `test/fixtures/measured/` (built from the report directories by `tools/build_fixtures.py`)

**Interfaces:**
- `SCC.pipeline.run(contract, inputs, pricePreset, options: {outcome_mix?, sweep?: {inputId, values}}) -> Result`: `Result = {page_state: 'ok'|'prices_missing'|'cannot_reproduce', variants: {id: {state, reason, extrapolated, cells: {operation: {money, quantities, lines, rows, extrapolated, trace}}, total, allocation, notes}}, cheapest: id|null, deltas: {id: {vs_ebs: {value, extrapolated}|null, vs_s3: {value, extrapolated}|null}}, sweep: SweepResult|null, evidence_statement, manifest}` following the aggregation order of Global Constraints; the instance-class note comes from `contract.billing.ec2_ebs_bandwidth` (smallest class whose `max_mibps` ≥ the allocated throughput); `states` evaluated per variant (`backlog_grows` cond; `not_supported` from applicability; `unsupported_cut` when any referenced coefficient has `status: unsupported` and the dependent input is non-zero).
- `SCC.pipeline.sweep(contract, inputs, preset, inputId, values[]) -> {points: [{x, totals: {variant: {value: number|null, state, extrapolated}}}], breakEven: [{a, b, x_between: [x1, x2]}]}` (crossings detected by sign change of `total_a − total_b` between consecutive points with both `ok`).
- `tools/build_fixtures.py`: for every `coverage.evidence` edge with status `done` and role `fit|holdout`, joins the prediction (`predictions/<experiment_id>.<policy>.json`) and the measurement (`measurement_id`) and writes `test/fixtures/measured/<experiment_id>.<policy>.json` `{experiment_id, policy, coefficient, role, inputs (prediction.inputs), expected_rows (from prediction.expected.foreground, flattened to [{caller, op_kind, quantity}]), measured_rows (from measurement.ledger.foreground, same shape), tolerance, measurement_id}`; the Node test evaluates the contract's cells for `inputs` and asserts measured rows within tolerance (exact when `tolerance` is zero). Running `build_fixtures.py` is a step of `generate_contract.py generate` (Task 3), so fixtures are never stale.

- [ ] **Step 1: Tests** — `test_not_supported_has_no_total_no_delta_not_cheapest`, `test_backlog_grows_suppresses_total_deltas_cheapest`, `test_delta_null_when_reference_unavailable`, `test_nonzero_override_of_cut_term_makes_column_unavailable`, `test_extrapolated_flag_propagates_to_totals_deltas_and_chart`, `test_cheapest_only_among_ok`, `test_aggregate_once_then_allocate_back_sums_to_total`, `test_fractional_monthly_event_expands_without_rounding` (0.1 replacements/month × 1 234 requests → 123.4 requests), `test_sweep_detects_numeric_crossing` (fixture whose totals cross between x=2 and x=3; assert `x_between`), `test_measured_fixtures_within_tolerance` (fails with a clear message when `test/fixtures/measured/` is empty unless `SCC_DEV=1`; Task 8 runs without it).
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: evaluation pipeline, states and sweeps`.

### Task 3: Contract generator and the five closure checks

**Files:**
- Create: `utils/storage-cost-calculator/tools/generate_contract.py`, `tools/test_generate_contract.py`, `tools/build_fixtures.py`, `docs/superpowers/cost-model/storage-cost-model.md` (template with markers), `utils/storage-cost-calculator/test/closure.test.js` (spawns `closure_check.py --mode final` and `generate_contract.py check`; fails on exit ≠ 0)
- Modify: `utils/storage-cost-calculator/index.html` (Plan 1 Task 13 template: add the contract markers)
- Modify: `tools/closure_check.py` (wire `evidence`, `accounting`, `reproduction` to the generated contract)

**Interfaces:**
- `generate_contract.py generate --cost-model docs/superpowers/cost-model --reports 'docs/superpowers/reports/2026-10-24-storage-cost-*/' --page utils/storage-cost-calculator/index.html --doc docs/superpowers/cost-model/storage-cost-model.md`: merges `formulas.json` and `coefficients.json` (one owner each), embeds the billing contract, computes the `manifest` (`code_sha256` over `model.js, contract.js, billing.js, pipeline.js, presets.js, urlstate.js, viewmodel.js`, versions of registry, coverage, billing, each report's commit and `raw/SHA256SUMS` digest), runs `build_fixtures.py`, writes the standalone contract to **both** `docs/superpowers/cost-model/model-contract.json` and `utils/storage-cost-calculator/model-contract.json`, the page block between `<!-- contract:begin -->`/`<!-- contract:end -->`, and the doc blocks between `<!-- formulas:begin -->`/`<!-- formulas:end -->` (symbolic rendering via `node -e` calling `SCC.model.render`, one block per variant × operation with coefficient provenance tables) and `<!-- inputs:begin -->`/`<!-- inputs:end -->` (registry table). `check` re-renders and fails on any difference among standalone JSON, embedded copy and doc blocks, and on any closure problem.
- Templates: `index.html` (from Plan 1) gains the contract markers, the `<script src>` tags in order (`model.js`, `contract.js`, `billing.js`, `pipeline.js`, `presets.js`, `urlstate.js`, `viewmodel.js`, `ui.js`) and empty containers with ids (`presets`, `inputs`, `cards`, `table`, `drill`, `notes`, `evidence`, `chart`); `storage-cost-model.md` contains frontmatter, hand-written sections (`Evidence policy`, `Known inaccuracies`, `Reproduce`) and the marker pairs. Tasks 4–7 modify these files; they never recreate them.

- [ ] **Step 1: Tests** — `test_generate_writes_three_identical_copies`, `test_check_detects_stale_embedded_copy_and_doc_block`, `test_duplicate_coefficient_owner_fails`, `test_cut_card_with_nonzero_default_fails`, `test_p1_without_holdout_fails`, `test_manifest_includes_evaluator_sha_and_raw_digests`, `test_closure_checks_fail_on_injected_defects` (five fixtures, one per check).
- [ ] **Step 2–4** (`python3 -m unittest utils/storage-cost-calculator/tools/test_generate_contract.py`). **Step 5: Commit** — `calculator: contract generator and closure checks`.

### Task 4: Presets and view model

**Files:**
- Create: `presets.js`, `viewmodel.js`, `test/presets.test.js`, `test/viewmodel.test.js`

**Interfaces:**
- `SCC.presets.SCENARIOS = {logs, metrics, analytics_batch, single_replica_archive, custom}` each `{label, description, values: {registry_id: value}, locks: [ids]}`; `SCC.presets.PRICES = {aws_us_east_1: <from contract.billing>, gcs: {structure_only: true, label}, azure: {structure_only: true}, custom}`; a test asserts every scenario's values lie within every referenced coefficient's validity (presets within measured coverage) and that structure-only price presets produce `page_state: 'prices_missing'` (added to the state model: suppresses money, shows the structure).
- `SCC.viewmodel.build(result (including `result.sweep`), contract, fields, lockOrder, ui: {openCell, sweepInput, theme, priceEditing}) -> VM` with `cards[]` (`label, total_text|null, state, state_text, delta_texts, extrapolated, bar_segments[{class, fraction}]`), `table.rows[]` (`operation, cells[{variant, text, state, extrapolated, clickable}]`), `drill` (`composition[{op_kind, money, fraction}], stages: [{name, lines[]}] (inputs used; branch taken per row; ledger rows; billing lines; allocation), sources[{kind, ref}]`), `inputs` (simple fields and advanced blocks from the registry: `block, fields[{id, label, value, unit, kind, locked, derived, overridden, out_of_range, range, hint}]`, price fields editable with `as_of`), `notes[]`, `evidence_statement`, `chart {points, breakEven, labels, extrapolated_points}`, `page_state`.

- [ ] **Step 1: Tests** — `test_every_scenario_within_coverage`, `test_structure_only_prices_set_prices_missing`, `test_card_text_for_each_state`, `test_bar_segments_sum_to_one_or_empty_when_unavailable`, `test_drill_has_five_stages_with_branch_and_allocation`, `test_inputs_blocks_from_registry_with_hint_and_lock_state`, `test_chart_marks_extrapolated_points`, `test_fmt_money_thousands_and_currency`.
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: presets and view model`.

### Task 5: URL state and reproduction

**Files:**
- Create: `urlstate.js`, `test/urlstate.test.js`

**Interfaces:**
- `SCC.urlstate.encode(state: {inputs, locks, overrides, scenario, price_preset, custom_prices, units, openCell, sweepInput, manifest}) -> string` (URL hash, base64url of compact JSON); `decode(hash, contract) -> {state, problem: null | {missing: [{artifact, version}]}}` comparing every manifest entry (evaluator, contract, registry, coverage, billing, evidence digests) with the loaded contract; any mismatch → `problem` → page `cannot_reproduce`.

- [ ] **Step 1: Tests** — `test_roundtrip_full_state_including_custom_prices_and_units`, `test_url_with_unknown_versions_sets_cannot_reproduce` (names the artifact and version), `test_hash_is_url_safe`.
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: versioned URL state`.

### Task 6: Page rendering

**Files:**
- Create: `ui.js`, `styles.css`; Modify: `index.html` (containers only; markers untouched)

**Interfaces:**
- `SCC.ui.mount(root: Document, contract)`: loads the embedded contract, decodes the URL, builds state, renders the VM into the containers, wires: sliders + editable numbers, lock toggles, override reset, unit selector, scenario and price presets, editable prices (custom preset), cell click/hover → drill, "show calculation" toggle, sweep input selector, theme toggle (defaults to `prefers-color-scheme`), URL update on every change (`history.replaceState`). `SCC.ui.render(vm)` is a pure DOM write from the VM.
- Layout per spec §7; `styles.css` tokens (`--bg, --card, --ink, --mute, --line, --accent, --ok, --warn, --cls-bytes, --cls-transfer, --cls-put, --cls-get, --cls-block`), dark via `@media (prefers-color-scheme: dark)` and `[data-theme]`; one-column breakpoint at 900 px.

- [ ] **Step 1: Node tests on the string renderer** — `test_render_contains_substituted_values_for_fixture_vm` (asserts specific totals, delta texts, state texts and the five stage values of a fixture VM; headings alone do not satisfy it), `test_render_not_supported_card_has_no_money`, `test_cell_click_handler_sets_open_cell`.
- [ ] **Step 2–4.** **Step 5: Browser checks** (record results and screenshot paths in LEDGER.md): open `index.html` via `file://` and via `python3 -m http.server`; change an input → totals change and URL updates; open a cell → five stages visible; override a derived field → marked, survives a preset switch, reset works; lock `data_bytes` + `retention_days` → ingest derived; switch ingest units → same monthly totals; structure-only GCS preset → no money, structure visible; `cannot_reproduce` URL → message, no numbers; dark theme; 390 px width without horizontal scroll.
- [ ] **Step 6: Commit** — `calculator: page rendering`.

### Task 7: Formula document, break-even section and blog hand-off

**Files:**
- Create: `tools/sweeps.mjs` (writes `docs/superpowers/cost-model/breakeven.json`: for each scenario preset and sweep input, the `sweep` output), Modify: `generate_contract.py` (renders `breakeven.json` into the doc between `<!-- breakeven:begin -->`/`<!-- breakeven:end -->`), `storage-cost-model.md` (hand-written `Known inaccuracies` listing the statements in `docs/superpowers/blog/2026-cas-storage-model/part4-cheap-self-sufficient.md` on branch `cas-blog-article` — read via `git show cas-blog-article:<path>` — that disagree with the model, with the model's number beside each), `Reproduce` (commands: rig run, generate, check, tests).

- [ ] **Step 1: Test** — `test_breakeven_matches_independent_cases` (two hand-calculated fixtures: one with a known crossing interval, one with none; the sweep output must reproduce both; plus every scenario × input entry has totals present and either a crossing with `x_between` or `"crossing": null`).
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: break-even sweeps, known inaccuracies and reproduce section`.

### Task 8: Whole-branch verification and hand-off to the umbrella review

- [ ] **Step 1:** `node --test utils/storage-cost-calculator/test/` PASS without `SCC_DEV` (includes `closure.test.js` → `closure_check.py --mode final` and `generate_contract.py check`); `python3 -m unittest discover -s utils/cas-yardstick/tests -t utils/cas-yardstick` PASS; `python3 -m unittest discover -s utils/storage-cost-calculator/tools -p 'test_*.py'` PASS.
- [ ] **Step 2:** Codex review of `pipeline.js`, `viewmodel.js`, `urlstate.js`, `generate_contract.py` (scope: can a displayed number come from outside the contract; are the state rules and the five closure checks implemented as the spec states) → close MAJORs.
- [ ] **Step 3:** LEDGER.md: step 4 done; hand to step 5 (umbrella review). **Step 4: Commit** — `calculator: verification and review dispositions`.
