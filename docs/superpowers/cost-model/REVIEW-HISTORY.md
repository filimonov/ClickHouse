---
description: 'Review findings and dispositions for the storage cost calculator code, the measurement contract and the phase-0 rig.'
sidebar_label: 'Cost model review history'
sidebar_position: 56
slug: /superpowers/cost-model/review-history
title: 'Cost model review history'
doc_type: 'reference'
---

# Cost model review history {#review-history}

Findings from the reviews of the calculator code ([storage-cost-model.md](storage-cost-model.md)) and of the
measurement contract and phase-0 rig ([measurement-contract.md](measurement-contract.md)), with their dispositions.

## Calculator code {#calculator-code}

Codex (`gpt-6-astra`, high effort, one round) reviewed the calculator code against the contract-only rule and the state and closure rules. It reported 5 MAJOR and 2 MINOR findings. The first MAJOR is a spec question; each other MAJOR is closed with a named test.

| Finding | Severity | Disposition |
|---|---|---|
| Rate unit conversion in `pipeline.js`, delta subtraction, bar-segment sums in `viewmodel.js` are arithmetic outside the contract | MAJOR | Not a violation. Rate conversion parses an input (§4.2, the unit selector: month = 730 h). A delta is a difference of two contract totals, an output of the model (§4.5, deltas as outputs). A bar segment is display grouping the contract does not evaluate. No change. |
| `allocate` re-bills each cell with the default outcome mix, so cell money disagrees with the billed total | MAJOR | Closed. `allocate` takes the outcome mix of the aggregate bill. Test `test_allocation_keeps_the_outcome_mix`. |
| An operation whose families are all `not_applicable` is still evaluated and billed | MAJOR | Closed. `evaluateVariant` skips such an operation. Test `test_all_not_applicable_operation_contributes_nothing`. |
| Extrapolation is lost through registry derivations and table coefficients | MAJOR | Closed. Derived values carry the flag into every `input` node that reads them. Table coefficients keep `validity` and lookups apply it. Test `test_extrapolation_survives_derivations_and_table_validity`. |
| Support closure credits a card's evidence to every variant in `model_variants`, though no case policy measured it | MAJOR | Closed. `support` rejects a card whose `model_variants` lists a variant that no case policy measures. Test `test_card_variant_without_a_measuring_policy_is_flagged`. |
| Registry and precedence expressions get no generic dimension check | MINOR | Parked. Needs a dimension checker over registry and precedence expressions, well over 10 lines. No registry expression has a wrong dimension today. |
| Pinned registry defaults are missing from the evaluation context | MINOR | Closed (3 lines). `buildCtx` reads a pinned quantity's default. Test `test_pinned_quantity_default_reaches_expressions`. |


## Measurement contract and phase-0 rig {#measurement-contract}

| Date | Reviewer | Scope | Findings | Closed |
|---|---|---|---|---|
| 2026-10-24 | Codex (review log not retained in the repository); rulings in [LEDGER.md](../reports/2026-10-23-storage-cost-calculator/LEDGER.md) | schemas, the measurement contract, `yardstick/cost/*.py`, `model.js`, `contract.js`, `billing.js`, `drill.mjs`, `ui.js`, the phase-0 report | 18 MAJOR (two of them under R18), 5 MINOR; verdict REVISE | all 18 MAJOR and 4 MINOR fixed; 1 MINOR and the presets part of R18 parked |
| 2026-10-24 | Whole-branch final review of the phase-0 rig (opus); rulings in [LEDGER.md](../reports/2026-10-23-storage-cost-calculator/LEDGER.md) | `78133cca87d..3c290a1c129` | 8 Important, 4 deferred items to fix before the research runs, cheap minors; no Critical | all 8 Important and 4 deferred items fixed; 7 minors fixed, 1 done by documentation |

### Dispositions, 2026-10-24 {#review-2026-10-24}

The gate was re-run on the fixed code (`cost-20261024T024945Z`, five PASS); every record replays from its raw ledger slice.

| Finding | What | Disposition | Commit | Tests or reason |
|---|---|---|---|---|
| R1 MAJOR | `ledger.py`: replay endpoints are not byte-hour bounds for size-changing overwrites | fixed (first fix pass: bounds over every feasible effect order, not seq order; pointwise possibility test instead of the ruled permutation enumeration, so no cluster cap and no fail-closed fallback are needed) | `8c167405454`, first fix pass | `ByteHourBounds` (203..401 case; `test_overlapping_overwrites_land_in_either_order`: the 42.2 byte-hour schedule and the 1 B final state; brute force over every schedule in any order) |
| R2 MAJOR | ambiguous `DeleteObjects` cannot reduce `lower` | fixed | `8c167405454` | `BatchDeleteKeys`, proxy `test_delete_keys_recorded_when_the_response_is_lost`, validator `test_ledger_row_batch_delete_keys_are_optional_strings` |
| R3 MAJOR | open attempts vanish from stochastic accounting | fixed | `8c167405454`, `5be403b1793` | `test_open_attempt_without_row_marks_the_ledger_incomplete`; drill `test_unavailable_states_bill_nothing_and_render_the_state` (incomplete ledger) |
| R4 MAJOR | shared observer client bypasses the proxy | fixed | `8c167405454` | `test_run_dispatch_and_exit_codes` (rig endpoint is the proxy's `rig` port); multipart probe aborts through `ctx.rig`, `rig_client` removed |
| R5 MAJOR | `empty_verified` does not cover incomplete uploads | fixed | `8c167405454` | `test_empty_inventory_verified_only_where_uploads_can_be_listed`; records say `declared` with a note |
| R6 MAJOR | CAS drill omits maintenance requests and metadata byte-hours | fixed | `8c167405454`, `5be403b1793` | CAS cell prices integrated requests and `active`, `garbage`, `metadata`; drill `test_reconciliation_catches_class_swaps_residuals_and_stochastic_tolerance`, `test_storage_must_match_the_record_and_evidence_is_required` |
| R7 MAJOR | key families cannot establish CAS request origin; `classify` misses key `""` | fixed | `8c167405454` | `test_request_origin_follows_the_callers_gc_rounds`, `test_gc_round_traffic_is_maintenance_and_priced_with_the_rest`, `test_classify_treats_empty_key_as_keyless` |
| R8 MAJOR | local demand divides lifetime trace counts by the window; trace file truncated on restart | fixed | `8c167405454` | `test_window_clips_calls_and_lists_crossing_ones`, `test_traced_card_counts_only_calls_inside_the_window`, `TracerTest.test_prefix`; gate: 3 800 writes, was 3 803 |
| R9 MAJOR | model rows cannot carry outcome rules; every `dropped` row is free | fixed | `5be403b1793`, `8c167405454` | `test_accepted_dropped_is_billed_and_dropped_is_not`, `test_validated_cell_rows_keep_their_outcome_facts`, `test_formulas_rows_carry_outcome_facts_and_cells_list_exclusions`, `AcceptedDropped` |
| R10 MAJOR | displayed values can bypass the contract | fixed | `5be403b1793` | `test_contract_validation_rejects_bad_dims_and_unregistered_observations`; found and fixed `contract.load` rejecting `cond: true` (`test_unconditional_state_is_not_an_expression`) |
| R11 MAJOR | unavailable states still priced | fixed | `5be403b1793` | `test_unavailable_states_bill_nothing_and_render_the_state` |
| R12 MAJOR | allocation stage not exercised | fixed | `5be403b1793` | `test_two_cells_share_one_bill_and_get_allocated_money`; gate: fetch probe as `inserts` + `recovery` sharing the storage tier |
| R13 MAJOR | reconciliation accepts wrong costs; IOPS plus MiB/s summed | fixed | `5be403b1793`, `8c167405454` | `test_reconciliation_catches_class_swaps_residuals_and_stochastic_tolerance`, `test_mixed_outcomes_compare_attempts_without_false_mismatch`; summary keyed by row type and op; counts gain `upstream_4xx` (`ClientErrors`) |
| R14 MAJOR | failed assertions and cleanup failures keep success | fixed | `8c167405454` | `test_failed_assertion_fails_the_record_and_keeps_the_numbers`, `test_cleanup_failure_makes_run_exit_non_zero` |
| R15 MAJOR | report identities disagree (download vs executed hash, rig vs source commit) | fixed | `8c167405454`, this commit | `test_download_hash_and_source_commit_lookup`; closure `test_identities_agree_within_a_report_and_drills_render`; report README and plan reconciled |
| R16 MAJOR | workstation qualifications absent beside the price | fixed | `5be403b1793` | evidence block in drill JSON and page; `render_check.mjs` requires it (`test_storage_must_match_the_record_and_evidence_is_required`) |
| R17 MINOR | `min`/`max` grammar mismatch | fixed | `5be403b1793` | `test_min_max_args_grammar_shared_with_the_validator` and `test_worked_formula_examples_valid` on `example-min-max.json` |
| R18 MAJOR | accounting and support/evidence closure weaker than §5.2 | fixed, presets part parked | `5be403b1793`, `8c167405454` | `AccountingReplayTest`, `test_planned_evidence_backs_nothing_and_cuts_reach_cells`, `test_holdout_disjoint_from_every_fit_validity_and_resolvable_ids`, `test_cost_reproduce`. Parked: cut propagation to presets, because `presets.js` does not exist yet; the check prints a note |
| MINOR | chunked response bytes lost when cut midway | fixed | `8c167405454` | `test_chunked_body_cut_midway_keeps_the_bytes_already_relayed` |
| MINOR | retry identity ignores `Range` and the query | fixed | `8c167405454` | `test_range_and_query_are_part_of_the_identity` |
| MINOR | fetch relay prices only one direction | fixed | `8c167405454`, `5be403b1793` | `test_inputs_split_storage_by_creator_and_count_both_directions`; contract wording narrowed (§Relay) |
| MINOR | acceptance ranges (`2000`, `8..11`) are not derived from source | labelled, derivation parked | first fix pass | The ranges carry `basis: assumption` with a reason in the prediction files (values unchanged). Deriving them, or replacing them with tolerances from independent repetitions, needs repeated runs: LEDGER.md phase-0 follow-ups |

| First fix pass | silent fallbacks in `cas_probe.py` (missing `gc_rounds`, Finish without Start) | fixed: both fail the pass | first fix pass | `test_classify_without_collected_rounds_fails`, `test_request_origin_follows_the_callers_gc_rounds` |
| First fix pass | drill's `server_error` remainder included attempts the store never saw | fixed: `server_error` = `upstream_error - upstream_4xx`; new class `not_sent` (unbilled) for injected, rejected or unconnected attempts; multipart Complete/Abort rows are `not_sent` | first fix pass | `test_not_sent_attempts_are_their_own_class`, billing `test_accepted_dropped_is_billed_and_dropped_is_not` |

### Dispositions, final review, 2026-10-24 {#review-final-2026-10-24}

The gate was re-run on the fixed code: `cost-20261024T035215Z`, six PASS (the sixth is `probe_cas_expect`), `rig_commit` `8a9806dd0b7`. Every record replays from its raw ledger slice. The first test named for each fixed item failed before its fix; the extended brute force and the sweep equivalence test are guards that also pass on the old code.

| Item | What | Disposition | Commit | Tests |
|---|---|---|---|---|
| I1 | CAS lease, GC and pool-meta traffic landed in `foreground` | fixed: `lease`, `gc`, `pool_meta` families are `maintenance` (cited in `cas_probe.py`); `attribute` = in one of the caller's GC rounds or on a control family | `24d9ca2adf3` | `test_control_families_are_maintenance_outside_gc_rounds` (key shapes from gate run `cost-20261024T030843Z`), `test_lease_traffic_in_window_does_not_change_foreground` (CAS families, `gc: off`, real key shapes) |
| I2 | idle replicas measured and priced | fixed: a pass starts exactly `card.replicas` servers and records that count; a request from another workload port fails with `unexpected_caller` | `24d9ca2adf3` | `test_starts_exactly_the_cards_replicas_and_records_them`, `test_traffic_from_a_caller_beyond_the_cards_replicas_fails`; gate records say `replicas: 1` for one-replica probes |
| I3 | estimate rested on probe-sized medians and a tenth of the timeout; wrong default registry | fixed: `duration_s` per case (schema, validator, example, README); a pass costs `max(median, duration_s)`; default `inputs-registry.json`, missing registry fails | `fae8a736118`, `24d9ca2adf3` | `test_estimate_math_fallbacks_and_cut_order`, `test_default_registry_exists_and_a_missing_one_fails`, `test_phase0_coverage_declares_duration_floors`, `test_coverage_duration_is_a_floor_below_the_timeout` |
| I4 | measured passthrough rows labelled `exact` | fixed: rows declare `basis` (schema, validator, README); exactness only for `predicted`; a label contradicting the expression is a problem | `31e5df2070c` | `test_measured_rows_carry_no_exactness_claim`, `test_formulas_row_basis_is_predicted_or_measured` |
| I5 | objects of unknown size priced at zero | fixed: `uncertain_identities > 0` makes storage cells unavailable (`unknown object sizes (N)`); the evidence block lists the count | `31e5df2070c` | `test_unknown_object_sizes_make_storage_unavailable` |
| I6 | `--scale` recorded, never applied | fixed: flag removed; the record carries the manifest's `scale`; `COUNTS` reach the card scaled as `ctx.counts` | `24d9ca2adf3` | `test_record_scale_is_the_manifests_and_counts_follow_it`, parser rejects `--scale` in `test_cost_subcommand_registered_in_yardstick_py` |
| I7 | `Expect` path unproven live; no wire-bytes bound | fixed: sixth card `probe_cas_expect` (`expect_continue_min_bytes` = 1, cited); rows carry `wire_bytes_bound` (schema, validator, README); live: 24 negotiated rows, each one attempt | `64dd8717f92`, `31e5df2070c` | `CasExpectTest` (three tests), `test_expect_rejected_upstream_flags_wire_bytes_as_upper_bound`, `test_ledger_row_wire_bytes_bound`; gate PASS |
| I8 | byte-hour envelope cubic per identity | fixed: sweep over starts and the landed floor `M(t)`; 1440 overwrites well under 2 s (n=400 took 3.9 s before) | `24d9ca2adf3` | `test_many_overwrites_of_one_key_are_fast`, `test_sweep_matches_the_pointwise_definition` (300 random cases), existing bound tests unchanged |
| D1 | report glob limited to `2026-10-*` | fixed: `*-storage-cost-*` | `31e5df2070c` | `test_default_reports_glob_takes_any_date` |
| D2 | zero-byte tie-break inverted presence | fixed: ties break toward absent in `lower`, present in `upper`; brute force extended with `unknown` transitions and end states | `24d9ca2adf3` | `test_zero_byte_tie_breaks_toward_absent_in_lower_and_present_in_upper`, `test_every_schedule_with_unknown_transitions_lies_inside_bounds_and_end_states` |
| D3 | `server.sh` fallbacks killed only the tracer | fixed: fallbacks signal the server's process group | `24d9ca2adf3` | `test_stop_fallback_kills_the_traced_server_too` (fake tracer and server) |
| D4 | an `all` row beside another outcome row double-bills | fixed: `contract.load` and `validate_formulas` reject it | `31e5df2070c` | `test_all_row_beside_another_outcome_of_the_same_op_rejected`, `test_formulas_reject_an_all_row_beside_another_outcome_of_the_same_op` |
| Minor | connect failure recorded as upstream `error`, drill comment said `not_sent` | documented: the drill comment now says a connect failure is a `server_error` (unbilled like any 5XX); recording it as `not_sent` would contradict `check_rows` (a proxy-written 502 is `delivered`) | `31e5df2070c` | none (comment only) |
| Minor | a failed row write was silent | fixed: later `flush` and `stop` raise | `24d9ca2adf3` | `test_finish_removes_the_open_entry_even_when_the_write_fails` |
| Minor | sampler thread errors lost | fixed: raised at `stop` | `24d9ca2adf3` | `test_dir_sampler_thread_failure_reraised_at_stop` |
| Minor | billing hard-coded the unbilled classes | fixed: `server_error`, `dropped`, `not_sent` are free only through preset entries with `outcome_class` and source URLs | `31e5df2070c` | `test_unbilled_outcome_classes_come_from_the_preset_free_list` |
| Minor | commit trailer deviation | recorded in LEDGER.md (attention table) | final docs commit | none |
| Minor | `active_by_creator` ignored `card.reclassify` | fixed | `24d9ca2adf3` | `test_split_follows_the_cards_reclassification` |
| Minor | report directory hard-coded | fixed: manifests set `params.report_dir` to their own report directory; `cost probe` passes `--report-dir` to the drill; phase-0 stays the default | `24d9ca2adf3` | `test_manifests_point_their_cards_at_their_own_report_directory`, `--report-dir` assertion in `test_probe_runs_six_with_drill_and_render_checks` |
| Minor | committed gate records stale | fixed: refreshed from `cost-20261024T035215Z`; README cites the run id | final docs commit | `closure_check.py --mode dev` (replay and SHA256SUMS) |
