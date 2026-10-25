---
description: 'Review findings and dispositions of the storage cost s3 report.'
sidebar_label: 'Storage cost s3: review'
sidebar_position: 55
slug: /superpowers/reports/storage-cost-s3/review-history
title: 'Storage cost s3 report: review history'
doc_type: 'reference'
---

# Storage cost s3 report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

One Codex round (`gpt-6-astra`, reasoning high, read-only; review log not retained in the repository) on the revision built from `cost-20261024T075800Z`, a run later found invalid (it overlapped
other rigs): 18 MAJOR, 5 MINOR. 16 MAJOR are closed; M16 and M17 are closed in the formulas, and their measurement is
parked (§Open items). The card defect M2 is fixed with a regression test (`test_cost_cards_ops.WindowContentsTest`,
same commit `09e9319a11f`). Every number below comes from the report run above. No second round: one per report.

| # | Finding | Disposition |
|---|---|---|
| M1 | Mutation entries are removed past `finished_mutations_to_keep` | Closed: `mutations` prices one `DeleteObject` per new entry once 100 are kept (`mutation_entries_removed_per_month`) |
| M2 | A completion `count()` read `_row_exists` inside the lightweight-delete window (34 / 8 GETs) | Closed test-first: row counts run in the teardown, after the window; rerun |
| M3 | The types hold-out (158 objects) does not validate the table value 154 | Closed: the `types` row is the types-fit schema's, fitted by `types-fit-compact` and `types-fit-wide`; `types-holdout` is a validation of the per-stream rule; the table's hold-outs are `wide-wide` and `wide-compact-2` |
| M4 | Mutation GET counts were labelled source-derived | Closed: `mutation_objects_written` and `mutation_gets_per_part` are measured. The report run showed the counts vary between repeats (+1 `GetObject` per Compact part read, cleanup batching of `DeleteObjects`): the exact hold-outs `mutation.update-mixed` and `mutation.update-mixed-2` missed and are consumed; `mutation.update-mixed-3` stated the band as a stochastic tolerance the card does not apply and is consumed too; `mutation.update-mixed-4`, committed with ranges before its first run, is the hold-out |
| M5 | The cache-fill hold-out was reused after its failure drove a fix | Closed: that failure was in an invalid run, so no record of it is cited. `cold-select.cache-on-types` is a fit; `cold-select.cache-on-types-2` is the hold-out |
| M6 | Spans claimed 25 and 26 columns | Closed: spans count top-level columns from the manifests (`Map(String, UInt64)` is one) |
| M7 | Outdated bytes used a part-count factor | Closed: ingest × (`write_amplification_rule` − 1) held for `old_parts_lifetime` |
| M8 | Local capacity omitted outdated parts' metadata | Closed: outdated objects (removals per second × `old_parts_lifetime` × objects) are added; headroom stays with billing |
| M9 | Write syscalls and cache allocation were priced as provisioned IOPS and throughput | Closed: those rows are removed; the coefficients stay measured for the EBS report |
| M10 | Statistics refresh omitted; its rule misstated | Closed: `reads` prices the conditional refresh (columns per active part per 300 s while parts change); §Reads states the skip of unchanged sets |
| M11 | Replication marked supported in `recovery` without a row | Closed: `recovery` lists `startup_recovery` only and is `not_supported` while `missed_parts_per_restart` > 0; its state and note name `catchup_requests_per_missed_part[s3]` and `startup_requests_after_restart[s3]` with `pending_owner: replication` |
| M12 | `cache_miss_fraction` hint measured bytes, the formula used part reads | Closed: the input is a byte share; the cell multiplies segment downloads, which scale with missed bytes (assumption stated) |
| M13 | Statistics reads come from part pruning, cached per part | Closed: the graph and the `reads` cell separate first reads (statistics and marks) from warm reads, measured by `cold-select.warm-off` and its hold-out `warm-off-types` |
| M14 | Skip indexes over 1 MiB and full-text indexes are not packed | Closed: §Insert states the branch; the measured layouts are packed |
| M15 | Merge and mutation outputs can be multipart | Closed: both cells price large files as multipart uploads (inputs with `system.parts_columns` hints) |
| M16 | One TTL merge can expire several parts | Closed in the formula, measurement parked: `deletes` separates expiry operations from expired parts. The hold-out `drop.partition-3` misses (DROP PARTITION covers each part with its own empty part, `StorageMergeTree.cpp:2724`) and is consumed; `drop.partition-2` holds. A multi-part TTL merge is not measured: ordinary merges of the sources can run before expiry, so a case needs merges held off first |
| M17 | Startup omits outdated parts and mutation files | Closed in the formula, measurement parked: `recovery` loads `outdated_parts` like active ones and one GET per retained mutation entry, from the source; the startup cards have no history |
| M18 | Replicated tables keep mutation entries in Keeper | Closed: the entry rows apply only when `replicas` = 1 |
| m1 | `String` size-stream citation | Fixed: `SerializationString.cpp:692` (predictions too; no expected value changed) |
| m2 | Buffer growth after 500 parts | Fixed in §Multipart |
| m3 | Fit predictions followed the exploratory run | Stated in the header table |
| m4 | `shape_columns` evidence | Fixed: cites the manifest schema |
| m5 | "every op exactly" overstated fits | Fixed in §Reconciliation |
