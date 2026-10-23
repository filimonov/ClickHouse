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
| 2. Spec review rounds (Codex) until no MAJOR | done | r1 REVISE (25, applied); r2 REVISE (18, applied as revision 3 with Tier 1/2 scope freeze); r3 REVISE (12 MAJOR, 3 MINOR). Non-convergence after 3 rounds; gpt-6.1-sol consult chose option C; revision 4 written (product contract + artifacts with closure checks + acceptance slice); r4: 3 MAJOR, 3 MINOR (applied, rev 4.1); r5: 0 MAJOR, 2 MINOR, READY (applied, rev 4.2). Findings per round 25 → 18 → 15 → 6 → 2 |
| 3. Implementation plan (`writing-plans`) | done | three plans: `docs/superpowers/plans/2026-10-24-storage-cost-{phase0-rig,research,calculator}.md`; Codex plan review r1: 3 CRITICAL, ~30 MAJOR, 2 MINOR (`tmp/codex_plan_r1_findings.md`) → plans rewritten (frozen schemas in Plan 1 Task 0, experiment manifests, coverage evidence edges, evaluator+billing+drill pulled into phase 0, classic-script distribution, proxy framing, Keeper config, window semantics); r2: 4 CRITICAL, ~24 MAJOR (`tmp/codex_plan_r2_findings.md`) → plans revision 3 (Expect negotiation, traced writes, process generations, bounded replay, table coefficients + lookup, EBS formulas owner, CardContext, cleanup order, manual provisioning path, lock rules, closure in the Node suite). Stop rule: two rounds done; remaining issues are for per-task reviewers |
| 4. Execute plan (`subagent-driven-development`) | in progress | Plan 1 first |
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
| D8 | 2026-10-24 | Plan 1 absorbs the evaluator, contract loader and billing (Plan 3 Tasks 1–3 moved to Plan 1 Tasks 11–13) so the phase-0 gate proves ledger → model → billing → drill-down; calculator scripts are classic `<script src>` files with a `window.SCC` namespace and Node export shims, not ES modules (`file://` must work) | Codex plan review r1 findings 8 and 19 |
| D9 | 2026-10-24 | Maintenance control for merges uses the table setting `max_parts_to_merge_at_once = 1` (existing yardstick `NO_MERGES`), never `SYSTEM STOP MERGES`; GC via `cas_gc_enabled`; lease traffic classified, not stopped | `SYSTEM STOP MERGES` blocks mutations and is lost on restart (plan review r1-2) |
| D10 | 2026-10-24 | Interserver receiver attribution is by construction (two-replica cards have one receiver; the three-replica `replacement` card keeps r1/r2 idle, asserted); sender attribution is exact per relay | loopback peers cannot distinguish r2 from r3 (plan review r1-33) |
| D11 | 2026-10-24 | Proxy answers `100 Continue` itself and strips `Expect` upstream (`expect_negotiated` flag; wire bytes upper-bound for rejected conditional writes) | plan review r2-27: the pinned CAS single-attempt client may send `Expect` above a 1 MiB threshold (to be verified in source by Task 14) |
| D12 | 2026-10-24 | Plan reviews stop after two rounds (35 → 29 findings); revision 3 folds round 2 and execution starts; remaining issues go to the per-task reviewers of subagent-driven development | memory: plan loops surface code-slip-class items from round 2 on |
| D7 | 2026-10-23 | Revision 4 reformulates the design per the gpt-6.1-sol consult (`tmp/codex_consult_convergence.md`): the design makes no claim about the code; implementation facts become versioned research artifacts (measurement contract, registry, coverage, graph reports, billing contract, model contract) with five closure checks and an executable acceptance slice at the phase-0 gate. Weakened: universal source-verification claim → evidence policy; accuracy promise scoped to validity domains; Tier 2 "no formula changes" → versioned extensibility; fixed review count → review as budget | three Codex rounds (25 → 18 → 15 findings) each found errors in the code-level detail the previous revision added; the loop had no stopping condition |
| D6 | 2026-10-23 | Scope frozen to Tier 1 (one binary, AWS Standard + gp3, coverage table §5.4 with P1/P2) with a phase-0 exit gate and a 3 workstation-day budget | Codex r2-5: the programme had no bounded deliverable |

## Defects, risks and items needing attention {#attention}

| # | Date | Item | State |
|---|---|---|---|
| A1 | 2026-10-23 | My gp3 I/O accounting claim in revision 2 (16 KiB units) was wrong; SSD volumes count I/O up to 256 KiB. Caught by Codex r2-12. | fixed in revision 3 |
| A3 | 2026-10-23 | Revision 3 code-facing claims found wrong by Codex r3 and verified by me against `tmp/src-26.8`: part type is compact if ANY of bytes/rows/level below threshold (`MergeTreeData.cpp` `satisfies`), `min_rows_for_wide_part` default 0; multipart part count follows `BufferAllocationPolicy` (32 MiB first buffer, 16 MiB then growth), not `ceil(size/min_upload_part_size)`; CAS replicas also re-merge locally (relink is the fetch transport); `always_fetch_mutated_part` exists; CAS namespaces are per server root × table (`liveNamespace`), so "namespaces pinned to 1" excluded the default deployment; `NetworkSendBytes` cannot isolate interserver traffic; gp3 couples throughput to IOPS (0.25 MiB/s per IOPS); versioned buckets break the lifecycle equations. Settings: `gc_interval_sec=60`, `mount_renew_period_ms=10000`, `staging_backend=local`. | revision 4: design-level items fixed in §4.3–§4.6/§5.2/§5.5; code-facing items are obligations of the research artifacts (§5.1) |
| A2 | 2026-10-23 | My AWS error-billing claim (403/404 free) in revision 2 was wrong and would have discounted CAS probes. Caught by Codex r2-13. | fixed in revision 3: dated outcome mapping, unknown outcomes billed |

## Watchdog log {#watchdog}

| Time | Observation | Action |
|---|---|---|
| 2026-10-23 23:26 | Codex r3 running (pid 3911839, log growing, reading `MergeFromLogEntryTask`); 26.8 binary downloaded, 26.6 at 289 MB; a foreign `clickhouse server` from worktree `antalya-26.8-pr-2159` listens on 19000–19003 and 19181 (not ours; yardstick uses 19700+) | none |
| A4 | 2026-10-23 | `utils/cas-yardstick/rustfs.sh` runs RustFS via docker image `rustfs/rustfs:1.0.0-rc.3`; docker availability on this host checked at plan time (see watchdog log); the CI lane runs the bare binary (`ci/tmp/rustfs`, copied into the worktree). The plan must pick one and the ledger proxy must sit in front of it. | open, for the plan |
| 2026-10-24 00:00 | RustFS binary starts standalone and answers 403 on 19121; loopback alias bind 127.0.1.x works; strace available; `/proc/sys/vm/drop_caches` not writable (no root) → EBS cold reads are an upper bound, host-specific label applies | recorded for Plan 1 Tasks 1, 4, 7 and Plan 2 Task 6 |
| 2026-10-24 00:00 | Step 3 (plans): Codex plan review r1 running (pid 3928638, log 315 KB and growing, reading yardstick tests); no yardstick or own clickhouse processes; no subagents | none |
