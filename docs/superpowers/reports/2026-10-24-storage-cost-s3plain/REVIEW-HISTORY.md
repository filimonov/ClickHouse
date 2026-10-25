---
description: 'Review findings and dispositions of the storage cost s3plain report.'
sidebar_label: 'Storage cost s3plain: review'
sidebar_position: 56
slug: /superpowers/reports/storage-cost-s3plain/review-history
title: 'Storage cost s3plain report: review history'
doc_type: 'reference'
---

# Storage cost s3plain report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

One Codex round (`gpt-6-astra`, reasoning high, read-only; review log not retained in the repository) on the gate, the card
change, the predictions of `2f20606ffe6`, the draft cells and the builders: 6 MAJOR, 1 MINOR.

| # | Finding | Disposition |
|---|---|---|
| M1 | The mutation refusal is too broad: a lightweight `UPDATE` (patch parts) passes once both block columns are enabled (`MergeTreeData.cpp:12630`) | Closed by scope: the gate refuses the mutation kinds the cost model prices (`ALTER UPDATE`, `ALTER DELETE`, lightweight `DELETE`); its reason now names the patch-part exception and cites `:12630`, `:12635`. No variant's cells price patch parts |
| M2 | `types` predictions miss the empty-file blobs (348 / 340, not 324 / 316) | Confirmed by the run: `objects-per-part.types-holdout` falsified and consumed. Fixed: `plain_objects_per_part` (own counts); fresh hold-out `objects-per-part.types-holdout-single` |
| M3 | Delete cells use 11 files for partitioned and TTL parts (13 / 12 measured) | Fixed: `plain_expired_part_extra_files` (2, one partition key column); the TTL-only case (one extra file) is an assumption in §Assumptions |
| M4 | Removing a file past 32 MiB is a multipart copy (`copyS3File.cpp:673`), not one `CopyObject` | Fixed: the removal rows of `merges` and `deletes` price the multipart copy from source, not measured (§Open items) |
| M5 | `plain_empty_part_objects` is used but not registered | Fixed: registered (pinned, `DiskObjectStorageTransaction.cpp:313`) |
| M6 | Recovery loads files of active parts only; outdated parts are loaded too (`MergeTreeData.cpp:3656`) | Fixed in the `s3plain` cell (active + outdated); the same term of the `s3` cell is reported to its owner |
| m1 | Hard-link edges rest on unlink cases; the startup note claims three cached access checks | Fixed: no edges for `plain_copy_on_hardlink`; the note says one check, two with the cache |

The run of `2f20606ffe6` also falsified `drop.partition-3` (consumed): `DROP PARTITION` over three parts made three
empty covering parts, not one. Fresh hold-out: `drop.partition-2`.
