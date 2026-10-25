---
description: 'Review findings and dispositions of the storage cost CAS report.'
sidebar_label: 'Storage cost CAS: review'
sidebar_position: 56
slug: /superpowers/reports/storage-cost-cas/review-history
title: 'Storage cost CAS report: review history'
doc_type: 'reference'
---

# Storage cost CAS report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

One Codex round (`gpt-6-astra`, reasoning high, read-only; review log not retained in the repository) on the interim report: 24 MAJOR, 2 MINOR. 23 MAJOR and both MINOR are closed; 1 MAJOR is
parked with its reason. Card fixes were written test-first (`tests/test_cost_cards_cas.py`) and every cited case
re-ran after them in the report run.

| # | Severity | Finding | Disposition |
|---|---|---|---|
| 1 | MAJOR | hash-hit graph omits `.meta` backfill and the condemned-body resurrect | closed: both branches in §[Branches](README.md#branches); not priced (content returning during its own reclamation) |
| 2 | MAJOR | publish counts ignore batching, empty `_ckpt` writes and retries | closed: §[Publishing](README.md#publish) states the uncontended path and the three conditions |
| 3 | MAJOR | idle rounds force a fold after 8 deferrals | closed: `gc` prices 8 deferred + 1 fold per 9 idle rounds |
| 4 | MAJOR | stages need not advance every round (budgets, clamp, spared, absent) | closed: conditions in §[Reclamation](README.md#reclaim); exhausted budgets are `backlog_grows` |
| 5 | MAJOR | lease rule text adds the latency twice | closed in §[Lease](README.md#lease); the committed predictions keep their text; their numeric band holds |
| 6 | MAJOR | time-window attribution is not causal | parked: the window method is the attribution rule of this report; §[Request origin](README.md#origin) states both directions, exact claims come from GC-off cases |
| 7 | MAJOR | observer slice can drop concurrent requests | closed: no slice; the placement read moved to the teardown, outside the window (`test_placement_is_read_in_the_teardown_outside_the_window`) |
| 8 | MAJOR | lease renewals inside a round counted as round requests | closed: excluded from round rows (`test_lease_renewal_inside_a_round_is_not_a_round_request`) |
| 9 | MAJOR | `families_disjoint` was tautological | closed: checks that outside every round only `gc_hb` and the lease run, on the rows; fails on a leaked GC request |
| 10 | MAJOR | inserts priced blob files, not distinct uploads | closed: `cas_objects_per_part` counts distinct uploads (default codecs); `staging-peak` is a branch fit only |
| 11 | MAJOR | mutation formula contradicted the measured marks hit | closed: `cas_mutation_marks_hit_fraction` (default 0; 1 reproduces `adoption`) |
| 12 | MAJOR | merge used the inserted part type for its result | closed: result type from fan-in × part bytes against `min_bytes_for_wide_part` |
| 13 | MAJOR | `gc` always charged a fold | closed: fold with new parts, else the deferred mix |
| 14 | MAJOR | round period dropped the per-part duration | closed |
| 15 | MAJOR | retirement ignored mutations and drops | closed |
| 16 | MAJOR | no batch deletes or drop cleanup in `gc` | closed: `DeleteObjects` per retiring round and per 1000 keys; drop's `_ckpt` DELETE and catalog PUT |
| 17 | MAJOR | `backlog_grows` used one budget and treated 0 as a limit | closed: both budgets, 0 unbounded |
| 18 | MAJOR | no metadata storage | closed: `.meta` and manifest byte-months |
| 19 | MAJOR | staging used uncompressed logical bytes | closed: blob bytes of a part × (insert clients + 1 merge) |
| 20 | MAJOR | `gc-under-load` prediction compared foreground exactly | closed: `exact = false`; the validation report compares reclaimed with retired |
| 21 | MAJOR | `gc-under-load` counted outdated parts as references | closed: active parts only (`test_retired_blobs_are_those_no_active_part_references`) |
| 22 | MAJOR | coverage generation failed on consumed cases | closed |
| 23 | MAJOR | coverage named the consumed `drop-60s` | closed: `drop-60s` consumed, `drop-60s-12` the hold-out |
| 24 | MAJOR | `objects-5k` predicted no duration | closed: `objects-3k` predicts the fold duration, committed before its run |
| 25 | MINOR | catalog GET cited the readable-runtime admission | closed: `Pool/CasRefLedger.cpp:3429` |
| 26 | MINOR | placeholder provenance | closed |

## Follow-up review {#review-fix-1}

| Finding | Fix |
|---|---|
| M1: lease edges backed by FAILED records | the two edges are `consumed`; `lease.idle-10min` predicted afresh (`aa7970d78d6`) and run as the `validation` edge |
| M2: idle-table cleanup listing unmodelled | priced in the `lease` cell per table (`cas_table_cleanup_period_s`), and in the fresh lease prediction |
| M3: cells read tables through derivations | `closure_check.derived_refs` follows `input` nodes into registry derivations, cycle-safe, with a test |
| M4: applicability missing families | `multipart_graph` (inserts, unsupported_cut), `lifecycle` (merges, gc, storage), `thresholds` (merges) |
| M3 ruling (round 2) | support backs each family by its own coefficients; `mutations` and `deletes` note the pinned rules their cases validate (`cas_branch_requests` ← `cas-branches.adoption`, `cas_foreground_ref_removal_requests` ← `cas-drop.table-sync`). Still pending in final mode: `inserts`/`storage` × `objects_per_part_by_shape` (no objects-per-part card ran on `cas`) and `merges` × `thresholds` (no thresholds case ran on `cas`) |
| m1–m5 | equality kept next to the Expect bound; always-true conjunct dropped; `roots` cites the write probe; fold split cited to source; cohort test adds a PUT after the retire |
