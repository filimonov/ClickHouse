---
description: 'Ledger of the unattended implementation of the storage cost calculator design: status per step, decisions taken without the owner, defects and items needing attention.'
sidebar_label: 'Storage cost calculator ledger'
sidebar_position: 51
slug: /superpowers/reports/storage-cost-calculator-ledger
title: 'Storage cost calculator — implementation ledger'
doc_type: 'reference'
---

# Storage cost calculator — implementation ledger {#ledger}

Spec: `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md`. Branch `docs/storage-cost-calculator`,
worktree `~/workspace/ClickHouse/storage-cost-calc`. Unattended run started 2026-10-23.

## Status {#status}

| Step | State | Notes |
|---|---|---|
| 1. Ledger | done | this file |
| 2. Spec review rounds (Codex) until no MAJOR | in progress | r1 done (REVISE, 25 findings, all applied); r2 started |
| 3. Implementation plan (`writing-plans`) | pending | |
| 4. Execute plan (`subagent-driven-development`) | pending | |
| 5. Umbrella review of the branch | pending | |
| 6. Look & feel in a browser at several resolutions | pending | |
| 7. Fix and repeat 5–7 | pending | |

## Decisions taken without the owner {#decisions}

| # | Date | Decision | Reason |
|---|---|---|---|
| D1 | 2026-10-23 | CAS card defaults to the measured 26.8 binary; "next release" is a labelled forecast toggle | two reviews: an unmeasured combination cannot be the headline number |
| D2 | 2026-10-23 | EBS demand measured on a dedicated local device, priced against published AWS accounting; no EC2 run | cost and time; results labelled derived |
| D3 | 2026-10-23 | Two-replica mode and completion barriers added to the yardstick | the replica multiplier is the central comparison and must be measured |
| D4 | 2026-10-23 | GCS preset ships with structure and values labelled "not validated against a bill" | AWS is the validated use case; owner asked for other presets to exist |

## Defects, risks and items needing attention {#attention}

| # | Date | Item | State |
|---|---|---|---|

## Watchdog log {#watchdog}

| Time | Observation | Action |
|---|---|---|
