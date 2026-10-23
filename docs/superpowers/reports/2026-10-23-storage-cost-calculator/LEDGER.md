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
| 2. Spec review rounds (Codex) until no MAJOR | in progress | r1 REVISE (25, applied); r2 REVISE (18, applied as revision 3 with Tier 1/2 scope freeze); r3 started |
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
| D4 | 2026-10-23 | GCS/Azure presets ship as structure only in Tier 1 ("values not provided"); values are Tier 2 | AWS is the validated use case; owner asked for other presets to exist; Codex r2-5 asked to bound the deliverable |
| D5 | 2026-10-23 | "Next release (forecast)" CAS toggle moved to Tier 2; Tier 1 shows the measured 26.8 only | owner preferred the forecast as headline; two reviews showed an unmeasured combination cannot be the default and the 26.6 measurements double the matrix |
| D6 | 2026-10-23 | Scope frozen to Tier 1 (one binary, AWS Standard + gp3, coverage table §5.4 with P1/P2) with a phase-0 exit gate and a 3 workstation-day budget | Codex r2-5: the programme had no bounded deliverable |

## Defects, risks and items needing attention {#attention}

| # | Date | Item | State |
|---|---|---|---|
| A1 | 2026-10-23 | My gp3 I/O accounting claim in revision 2 (16 KiB units) was wrong; SSD volumes count I/O up to 256 KiB. Caught by Codex r2-12. | fixed in revision 3 |
| A2 | 2026-10-23 | My AWS error-billing claim (403/404 free) in revision 2 was wrong and would have discounted CAS probes. Caught by Codex r2-13. | fixed in revision 3: dated outcome mapping, unknown outcomes billed |

## Watchdog log {#watchdog}

| Time | Observation | Action |
|---|---|---|
