---
description: 'Review findings and dispositions of the storage cost replication report.'
sidebar_label: 'Storage cost replication: review'
sidebar_position: 58
slug: /superpowers/reports/storage-cost-replication/review-history
title: 'Storage cost replication report: review history'
doc_type: 'reference'
---

# Storage cost replication report: review history {#review-history}

Review findings on [README.md](README.md) and their dispositions.

One Codex round (`gpt-6-astra`, reasoning high, read-only), run while the natural and restart runs were in
progress: 9 MAJOR, 1 MINOR, verdict REVISE. Dispositions (rows 1–9 MAJOR, row 10 the MINOR):

| # | Finding | Disposition |
|---|---|---|
| 1 | Failed repeats are discarded before fitting | Closed by record: failed records stay in `measurements.jsonl` and are open items in §[Gaps](README.md#gaps); CAS residue has no value; the s3 sender re-read sets `abs` 1 |
| 2 | "Exact" s3plain checks accept broad ranges | Closed: exactness is claimed for `s3` and `ebs` only; s3plain and CAS are stated as checked against assumed ranges (§[Measured graphs](README.md#graphs-measured)) |
| 3 | Consumed hold-outs reused as independent evidence | Closed: every hold-out whose prediction changed in `0bc1e557850` is `consumed`; done hold-outs are on `s3` and `ebs` |
| 4 | Hold-out edges done without a residual check | Closed: each hold-out edge is checked against the fit (table in §[Cases](README.md#reconciliation)); a failing check makes the edge `consumed`. Object-proportional fetch ops scale with objects per part; CAS `acquire_bytes` is valid for the Compact shape only |
| 5 | Stochastic tolerances not from K=3 | Closed for CAS: `abs` = half the widest K=3 range. Restart stays K=1 with an assumed `rel` 0.25, labelled |
| 6 | CAS relink "no blob reads" is false | Closed: the receiver's load reads blobs after promotion (`DataPartsExchange.cpp:1705`); text corrected |
| 7 | Acquisition branches missing from graphs and coverage | Closed by exclusion: clone-instead-of-fetch, CAS byte-fetch fallbacks and zero-copy fallback are listed in §[Branches not exercised](README.md#branches-excluded) with the reason each is outside the measured topology |
| 8 | Restart accounting mixes acquisition, local execution and maintenance | Closed: startup and catch-up are recomputed from the raw ledger with maintenance excluded; follower local merges are reported per record (0 means the catch-up is fetches only) |
| 9 | Retry outcomes discarded | Closed in the coefficient: `retry_outcome_mix` has an `attempt_kind` key (`attempts`, `store_reached`). The card-level assertion of the split is parked: it needs a new rig commit and a rerun of every fault case |
| 10 | Retry formula stated too generally | Closed: scoped to the retryable writes; `GetObject` bypasses the outer loop |

The MINOR (row 10) is closed; no finding is parked except the card assertion of #9.
