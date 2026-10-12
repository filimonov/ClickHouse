# CAS mount-lease series: state on 2026-10-12 {#lease-series-state}

The series is **paused**. The late-request audit (`../2026-10-12-late-request-audit/README.md`) found data-loss defects outside the lease, so those go first. This file records where every branch, PR, plan and open decision stands, so the series can resume without the session that built it.

Spec: `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md` (r8, `0cb9107ed87`). Plans: `docs/superpowers/plans/2026-10-0{8,9}-cas-lease-pr*.md`. Campaign report and journal: [campaign-report.md](campaign-report.md), [campaign-journal.md](campaign-journal.md).

## PRs and branches {#prs-and-branches}

Repo `Altinity/ClickHouse`, target `antalya-26.6-20261008`. Branches are `fix/antalya-26.6/<name>` in the worktrees `cas-95-1` and `cas-janitor-budget` (same object store). Rows without a remote exist **only locally**.

| PR | Branch | Head | Remote | State |
|---|---|---|---|---|
| 1 GC sighting after read | `cas-gc-sighting-after-read` | `bc7f453aa4a` | yes | merged #2509 |
| 5 probe sentinel boundary | `cas-probe-sentinel-boundary` | `58debc8b370` | yes | merged #2510 |
| 3 renewal memory guard | `cas-renewal-memory-guard` | `949578b2d73` | yes | merged #2511 |
| 7 claim monotonicity | `cas-mount-claim-monotonicity` | `17c7f28b660` | yes | open #2512 |
| 4 renewal progress | `cas-mount-renewal-progress` | `0a8317178b0` | yes | open #2513 (CI rerun pending) |
| 2 v2 observers wait declared lease | `cas-observers-wait-declared-lease` | `732155f15e7` | yes | open #2516 (stacked on PR 1, now merged) |
| 8 renew until definitive | `cas-mount-renewal-until-definitive` | `2f650d9efe9` | yes | open #2517 |
| 10 v2 one ref-lane fault | `cas-ref-lane-fault-latch-squash` | `b9a51702188` | **no** | ready, independent |
| CAS-349 unit-test temp roots | `cas-unit-test-temp-roots` | `458b49619ee` | **no** | ready, independent |
| CAS-351 recovery seal fault | `cas-ref-lane-recovery-seal-fault` | `1556359b7ac` | **no** | on 10 v2, needs review round B |
| 9a `system.cas_mounts`, `is_local` | `cas-mounts-table-local-path` | `07458806831` | **no** | ready after 8 |
| 9b operator budget contract | `cas-operator-budget-contract-7` | `674c618ae18` | **no** | ready after 9a; docs fix before opening |
| 11a ownership primitives | `cas-ownership-primitives-5` | `ef8a1c57bad` | **no** | ready after 9a |
| 11b lossless claim | `cas-lossless-claim-4` | `dabbfabfb28` | **no** | ready |
| 12 terminal losses + stop | `cas-terminal-losses-stop-10` | `a470f914557` | **no** | ready |
| 13 farewell rules | `cas-farewell-rules-5` | `470e1eb1f6d` | **no** | ready |
| 14 reacquire in lease thread | `cas-reacquire-in-lease-thread-6` | `903e03ae516` | **no** | ready |
| 15 remove remount machine | `cas-remove-remount-machine-9` | `b81092d3e85` | **no** | ready (on merge of 14 + PR 3) |
| 16 remove observability stack | `cas-remove-observability-stack-7` | `3b4037ae265` | **no** | ready after 15 |

PR bodies written so far: [pr-bodies/](pr-bodies/) (PR 1–5, 7, 8). The PR 16 soak/Grafana retarget for the master branch is held as [pr16-master-retarget.diff](pr16-master-retarget.diff) (CAS-356). Per-PR SDD ledgers live in the git-ignored `.superpowers/sdd/2026-10-0*-cas-lease-pr*/` of the master worktree.

## Direction after the audit {#direction}

The owner judged the series too complex for its goal (exclusive writable mount of one server root). Findings that shape the next step:

- **Session model, as in ZooKeeper.** Live → Suspended (requests fail retryable) → Live again on a same-epoch renewal; Lost (another actor changed the slot) → terminate. Safe because every actor that acts on "holder dead" first changes the slot by a conditional write (verified, `../2026-10-12-late-request-audit/objects/mount.md`). The code already has the states (`Ok`, `NoBudget`, `LostOrRearmed`).
- **Transport deadline (CAS-343)** instead of the `2E+M` reserve: check `mount_fence.deadline_boot_ms` before every send in `src/IO` (~170 lines, no `base/poco`), then drop `E` as the reserve unit and the second `validateCasRequestBudget` inequality; writes run to ~27 s instead of 14 s.
- **Likely kept:** PR 8 (pause ≠ loss), 2 v2 (#2516, also fixes mixed-lease premature fencing, CAS-374), 7 (#2512, epoch monotonicity, CAS-364), 4, 15, 16.
- **Likely dropped or shrunk:** 14 (reacquire → terminate), most of 9b (budget contract), 12 (loss table R8–R18 → Suspended/Lost).
- Nothing removes the time residue for `_files/` and `beginRemoving` without a format change (CAS-370, CAS-367).

## Open owner decisions {#open-decisions}

1. Simplification direction (above) — to be decided after the data-loss fixes.
2. Open 10 v2 and CAS-349 (independent).
3. Fate of 9a…16 under the chosen direction; regrouping proposal (9a+9b, 11a+11b, 12→12a/b/c, 14+15).
4. Rerun failed CI on #2513/#2510.
5. Defaults 60/15 (lease 60 s, period 15 s) — proposed, not decided.
6. SIGTERM/`std::terminate` on losses — marked provisional.
7. Back up local-only branches to the fork (needs an explicit push request).
8. Owner's local skill `analyzing-cas-health` reads counter names PR 16 removes.

## Deferred items {#deferred}

Prose fixes to apply before opening the matching PR: [deferred-docs-fixes.md](deferred-docs-fixes.md). Deferred by the owner: 17a/17b/18 (transport deadline, now CAS-343), reads during a pause, CAS-149.1/156.1/185/289.4, removal of external test prefixes (AWS/GCS), soak S39 leg B redesign.
