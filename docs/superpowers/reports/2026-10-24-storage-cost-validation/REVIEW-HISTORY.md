---
description: 'Review findings and dispositions of the storage cost validation report.'
sidebar_label: 'Storage cost validation: review'
sidebar_position: 60
slug: /superpowers/reports/storage-cost-validation/review-history
title: 'Storage cost validation report: review history'
doc_type: 'reference'
---

# Storage cost validation report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

One Codex round (`gpt-6-astra`, reasoning high, read-only; review log not retained in the repository): 3 MAJOR,
4 MINOR, verdict REVISE. All closed:

| # | Severity | Finding | Disposition |
|---|---|---|---|
| 1 | MAJOR | `cas_objects_per_part` priced its upper bound as an exact count (wide Wide 80 against 41.33 measured) | closed: the value is the measured count of the shape generators; the bound is stated as the data-dependent ceiling (§[CAS shapes](README.md#cas-shapes)) |
| 2 | MAJOR | `types-holdout-2` checks a band, not the coefficient | closed: its edges are `validation`; point hold-outs `narrow-wide-2`, `wide-wide-2`, `types-fit-wide-2` predicted at `168f71aea78`, exact in run `cost-20261024T162519Z` |
| 3 | MAJOR | catch-up validity `inserts_while_down` is no registry input, so the page cannot flag extrapolation | closed: validity is `missed_parts_per_restart` [0, 5] (exact at 0; both restart cases fetched 5 parts). The CAS recovery cell and the s3 follower load row are withdrawn: their validity parameters cannot be checked against the presets |
| 4 | MINOR | placement prose listed `.mrk*`/`.cmrk*` and omitted the inline spill | closed: exact suffixes (`ContentAddressedTransaction.cpp:77`) and the spill (`:998`) |
| 5 | MINOR | "same final sizes" in all nine merge records | closed: `s3` repeat 2 ended with 27, 23, 6, 6, 1, 1 |
| 6 | MINOR | `s3plain` byte ratio 1.0008 | closed: 1.0005 |
| 7 | MINOR | EBS placement named evidence that does not exist | closed: placement is the EBS report (Tier 1), D17, with the method it uses |
