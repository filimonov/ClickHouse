---
description: 'Review findings and dispositions of the storage cost lifecycle report.'
sidebar_label: 'Storage cost lifecycle: review'
sidebar_position: 54
slug: /superpowers/reports/storage-cost-lifecycle/review-history
title: 'Storage cost lifecycle report: review history'
doc_type: 'reference'
---

# Storage cost lifecycle report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

Codex (`gpt-6-astra`, reasoning high, read-only; review log not retained in the repository) round 1 found 12 MAJOR and 6 MINOR. It confirmed that all 45 records' request counts
reconcile with their raw windows, the fitted values reproduce, the hold-out predictions precede their runs, and the
wide hold-out tolerances reproduce from the pre-run data. Fixes are in `9dbe21126be`; the cards were re-run.

| # | Finding | Disposition |
|---|---|---|
| 1 | MAJOR: `part_type` omits the Wide overrides | Fixed: `adaptive_granularity` and `quantize_pq_codec` branches; `lifecycle_rules.test.js` |
| 2 | MAJOR: multipart omits the strict size and one-buffer multipart | Fixed: strict branch in `multipart_parts`, new `multipart_upload`, README; test |
| 3 | MAJOR: `parts_per_insert` makes `INSERT SELECT` async | Fixed: `insert_kind`, oversized data synchronous; test |
| 4 | MAJOR: squashing formula is not whole source blocks | Fixed: emitted rows rounded up to `insert_source_block_rows`; Codex's 111 195 300-row example gives 100; test |
| 5 | MAJOR: async sharing labelled source-derived | Fixed: evidence `assumed`, upper-bound estimate with payload, client and dedup caps; test |
| 6 | MAJOR: natural-merge bounds ignore TTL and force merges | Fixed: claims scoped to the simple selector; the cards' tables have no TTL and force merge off |
| 7 | MAJOR: acquisition rule misses branches, remote threshold misattributed | Fixed: ordered fetch, wait and skip branches; the remote threshold only gates the picker's refresh |
| 8 | MAJOR: mutation sizing and age boundary | Fixed: 1.1 × source bytes on disk without expired TTL parts; age inclusive |
| 9 | MAJOR: amplification fitted to another formula than the registry's | Fixed: fit to `write_amplification_rule` at the row-weighted size (f = 6.2271), hint corrected; test `test_amplification_rule_is_the_registry_formula_at_the_row_weighted_ratio`; committed prediction reported apart |
| 10 | MAJOR: hold-out TVs refer to the previous fit | Fixed: `coefficients.json` records both distances, against the committed reference and the current table |
| 11 | MAJOR: `quiesced` misses merges between polls | Fixed test-first (`test_quiet_period_counts_a_merge_finished_between_polls`): the part log's last merge end resets the quiet period; re-run |
| 12 | MAJOR: merged-away objects stay active | Fixed test-first (`test_merged_away_parts_become_outdated_at_their_merge`): `reclassify` marks them `outdated` at the merge; assertion `inventory.outdated_byte_hours>0`; re-run |
| 13 | MINOR: wrong citation anchors | Fixed in the predictions' `source_refs` and the README |
| 14 | MINOR: `old_parts_lifetime` is an eligibility age | Fixed: label and README say minimum age |
| 15 | MINOR: ladders differ past 12 levels | Fixed: the Python ladder caps at 12; test |
| 16 | MINOR: async bounds do not test batching | Closed by D14 cut: the async cases are cut, data only |
| 17 | MINOR: two coverage spans misdescribe their cases | Fixed: `level-cross` spans its merge result, `bursty` states burst size, pause and clients |
| 18 | MINOR: the stated domain excludes the hold-out | Fixed: README and the coefficients' `domain` separate fit and hold-out |

Round 2 (review log not retained in the repository) closed MAJORs 6, 8, 9, 10
and every MINOR, kept 8 MAJORs open and found one new MAJOR. It confirmed that the re-run records replay, the
checksums match, and no hold-out expected value changed after its prediction commit. Fixes are in `d53e470cdb5`; the
cards were re-run again.

| Finding | Round 2 | Disposition |
|---|---|---|
| 1 | hint can give the wrong `adaptive_granularity` | Fixed: the hint states the whole predicate (mixed granularity or no non-adaptive part) |
| 2 | a clamped first buffer below 32 MiB is missed | Fixed: `multipart_upload` compares with the smaller of the first buffer and the single-PUT limit; test |
| 3 | native admission measured as inline | Fixed: `insert_protocol`; native decoded bytes < limit, inline raw bytes ≤ limit; test |
| 4 | client blocks are not fixed; disabled minimums give unavailable | Fixed: `insert_data_block_rows` input; a disabled minimum is skipped, both off emit every block; tests |
| 5 | wrong dedup predicate; mean-rate bound; client hint | Fixed: `deduplicate_insert` (on by default) drives the 450 cap; the estimate states evenly spaced arrivals; the client hint reads `PendingAsyncInsert`; test |
| 7 | the "wait" branches fetch | Fixed: README and label say "try to fetch, quietly retrying" |
| 11 | a merge can slip past the last flush | Fixed test-first (`test_completion_waits_again_when_the_final_part_log_shows_a_later_merge`): the part log read after the final flush is the cutoff, a later merge or a running one restarts the wait |
| 12 | long runs lose part-to-object mappings | Fixed test-first (`test_layout_accumulates_across_snapshots`, `test_long_runs_snapshot_the_layout_while_inserting`): snapshots every 60 s accumulate |
| new | the 2 h prediction mixes fits | Fixed: every field recomputed from the report run's six fit records with the registry formula |

Round 3 (review log not retained in the repository) closed round-2 findings 2, 3, 7, 12
and the 2 h prediction, kept 1, 4, 5, 11 open and found one MINOR. It confirmed the re-run replays, the fit
reproduces (6.2845998), every 2 h prediction field reproduces, and no hold-out expected value changed after its
prediction commit. Fixes are in `474392fb9ad`; the cards were re-run.

| Finding | Round 3 | Disposition |
|---|---|---|
| 1 | the hint's creation-date shortcut | Fixed: removed; the hint says to check for non-adaptive marks files |
| 4 | native async overflow forwards one combined block | Domain: stated as not covered in the `parts_per_insert` label and the README |
| 5 | backward-compatible dedup with table-only async reads `insert_deduplicate` | Fixed: `async_insert_source` and `insert_deduplicate`; test |
| 11 | a merge can start after the final flush | Fixed test-first (`test_completion_waits_again_when_an_active_part_has_no_part_log_row`, `test_no_workload_request_after_the_observation_cutoff`): active parts must be explained by the part log, and a request after the cutoff fails the pass |
| new MINOR | "serialized sizes" of native entries | Fixed: decoded `Block::bytes` for native, raw length for inline |

Round 4 (review log not retained in the repository) closed every open finding (1, 4, 5, 11
and the wording MINOR) and re-checked 9, 12 and the 2 h prediction: **0 MAJOR open**. It confirmed all 45 records
replay, 647 retirement entries over 18 merge records replay, the nine raw checksums match, and the fit reproduces
(6.1436930). It found two MINORs:

| Finding | Round 4 | Disposition |
|---|---|---|
| R4-1 | rounded validity and span bounds excluded their own fit records (18.21875 < 18.22) | Fixed: bounds round outward (validity 18.21–472.95) |
| R4-2 | the observation cutoff is not in the records | Closed: the natural-merge records of the validation report (`cost-20261024T154049Z`, `s3` and `s3plain`) carry `quiet_cutoff_us` and `cutoff_us`; replaying `window.no_workload_after_cutoff` from their raw ledger finds 0 workload attempts after the cutoff, as recorded (`docs/superpowers/reports/2026-10-24-storage-cost-validation` §Amplification) |

MINOR totals over the four rounds: 7 fixed, 1 closed by the D14 cut (#16), 1 closed by the validation report (R4-2).
