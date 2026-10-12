# CAS mount-lease series: state on 2026-10-12 {#lease-series-state}

The series is **paused**. The late-request audit (`../2026-10-12-late-request-audit/README.md`) found data-loss defects outside the lease, so those go first. This file records where every branch, PR, plan and open decision stands, so the series can resume without the session that built it.

Spec: `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md` (r8, `0cb9107ed87`). Plans: `docs/superpowers/plans/2026-10-0{8,9}-cas-lease-pr*.md`. Campaign report and journal: [campaign-report.md](campaign-report.md), [campaign-journal.md](campaign-journal.md).

## PRs and branches {#prs-and-branches}

Repo `Altinity/ClickHouse`, target `antalya-26.6-20261008`. Branches are `fix/antalya-26.6/<name>` in the worktrees `cas-95-1` and `cas-janitor-budget` (same object store). "yes" = `altinity`; "fork" = backup push to `filimonov` on 2026-10-12, no PR.

| PR | Branch | Head | Remote | State |
|---|---|---|---|---|
| 1 GC sighting after read | `cas-gc-sighting-after-read` | `bc7f453aa4a` | yes | merged #2509 |
| 5 probe sentinel boundary | `cas-probe-sentinel-boundary` | `58debc8b370` | yes | merged #2510 |
| 3 renewal memory guard | `cas-renewal-memory-guard` | `949578b2d73` | yes | merged #2511 |
| 7 claim monotonicity | `cas-mount-claim-monotonicity` | `17c7f28b660` | yes | open #2512 |
| 4 renewal progress | `cas-mount-renewal-progress` | `0a8317178b0` | yes | open #2513 (CI rerun pending) |
| 2 v2 observers wait declared lease | `cas-observers-wait-declared-lease` | `732155f15e7` | yes | open #2516 (stacked on PR 1, now merged) |
| 8 renew until definitive | `cas-mount-renewal-until-definitive` | `2f650d9efe9` | yes | open #2517 |
| 10 v2 one ref-lane fault | `cas-ref-lane-fault-latch-squash` | `b9a51702188` | fork | ready, independent |
| CAS-349 unit-test temp roots | `cas-unit-test-temp-roots` | `458b49619ee` | fork | ready, independent |
| CAS-351 recovery seal fault | `cas-ref-lane-recovery-seal-fault` | `1556359b7ac` | fork | on 10 v2, needs review round B |
| 9a `system.cas_mounts`, `is_local` | `cas-mounts-table-local-path` | `07458806831` | fork | ready after 8 |
| 9b operator budget contract | `cas-operator-budget-contract-7` | `674c618ae18` | fork | ready after 9a; docs fix before opening |
| 11a ownership primitives | `cas-ownership-primitives-5` | `ef8a1c57bad` | fork | ready after 9a |
| 11b lossless claim | `cas-lossless-claim-4` | `dabbfabfb28` | fork | ready |
| 12 terminal losses + stop | `cas-terminal-losses-stop-10` | `a470f914557` | fork | ready |
| 13 farewell rules | `cas-farewell-rules-5` | `470e1eb1f6d` | fork | ready |
| 14 reacquire in lease thread | `cas-reacquire-in-lease-thread-6` | `903e03ae516` | fork | ready |
| 15 remove remount machine | `cas-remove-remount-machine-9` | `b81092d3e85` | fork | ready (on merge of 14 + PR 3) |
| 16 remove observability stack | `cas-remove-observability-stack-7` | `3b4037ae265` | fork | ready after 15 |

PR bodies written so far: [pr-bodies/](pr-bodies/) (PR 1–5, 7, 8). The PR 16 soak/Grafana retarget for the master branch is held as [pr16-master-retarget.diff](pr16-master-retarget.diff) (CAS-356). Per-PR SDD ledgers live in the git-ignored `.superpowers/sdd/2026-10-0*-cas-lease-pr*/` of the master worktree.

## Models follow the release line {#models}

On `cas-gc-rebuild` the mount-plane TLA+ models describe the release line `antalya-26.6-20261008` at `2a034cdb374` (PRs 1, 3, 5, 7 merged as #2509, #2511, #2510, #2512). Every model commit of an unmerged series PR is reverted by its own commit, newest first. The series state of the models stays on the local branch `models/mount-lease-series` (`29ae2193a2d`).

Restored models and the revision their files now equal:

| Model (files incl. cfgs, `_RESULTS.md`, run script) | Equals | Why |
|---|---|---|
| `CaCasMountCore`, `CaCasMountCoreLateRenew`, `run_mount.sh` | `3dbf425edd5` | last PR 7 commit |
| `CaMountObserveCore`, `run_mountobserve.sh` | `11a0675f8d3` | PR 1 (CAS-342) model |
| `CaDiskLifecycle`, `run_disklifecycle.sh` | `44520adb5c2` | before the 2026-10-01 one-thread design |
| `CaMountRenewRetryCore`, `run_mountrenewretry.sh` | `5451b2513ac^` (`fc3a4c378c9` for the runner) | before the 2026-10-01 no-deadline design |
| `CaRefLaneCore`, `run_reflane.sh` | `89a60b1b7f8` | before PR 10 |

`CaCasMountCoreStraggler.tla` exists only in the series. `CaGcLeaseCore` was not touched. PRs 3 and 5 changed no model.

### Classification {#models-classification}

Model commits since 2026-09-20 on the models above. "Revert" is the commit on `cas-gc-rebuild` that undoes the series commit.

| Commit | Models | Change modelled | PR | Merged | Release line | Revert |
|---|---|---|---|---|---|---|
| `44520adb5c2` | `run_disklifecycle.sh` | runner reads the singular temporal-violation message | — | — | kept | — |
| `9a5bbedbc41` | `CaDiskLifecycle` | one lease thread renews and reclaims (2026-10-01 design) | 14/15 | no | series-only | `2417b567344` |
| `265f827fe82` | `CaDiskLifecycle` | readiness arms the fence on the renewal (same design) | 14/15 | no | series-only | `81a088b17c9` |
| `5451b2513ac` | `CaMountRenewRetryCore` | renewal without a lease deadline (2026-10-01 design) | 8 | no | series-only | `2894594c906` |
| `137d9faf2fa` | `CaDiskLifecycle` | one arming rule for reclaim and renewal (same design) | 14/15 | no | series-only | `d53c6d476fe` |
| `11a0675f8d3` | `CaMountObserveCore` (new), `CaCasMountCore` late-renew witness | observer dates a sighting after the read (CAS-342); same-epoch renewal after expiry | 1 | #2509 | kept | — |
| `eb2e40e5eab` | `CaMountRenewRetryCore` | `Refused` and `Conflict{NotObserved}` continue the same tuple | 8 | no | series-only | `fda253256f2` |
| `5f15430aeed` | `CaMountRenewRetryCore` | RESULTS names PR 8 | 8 | no | series-only | `b223af813c5` |
| `a67369873eb` | `CaDiskLifecycle` | absorbing `Stopping` and `Closed` (step 1) | 12 | no | series-only | `a069508cbea` |
| `49d9c01242f` | `CaCasMountCore`, `CaCasMountCoreLateRenew` | incarnations, claim monotonicity (CAS-178) | 7 | #2512 | kept | — |
| `3dbf425edd5` | `CaCasMountCore` | strict claim monotonicity, equal-epoch refresh sabotage | 7 | #2512 | kept | — |
| `62d81168e23` | `CaRefLaneCore` | occupant outcomes latch before any exit | 10 | no | series-only | `5b5d143560a` |
| `f5bdfa89cea` | `CaRefLaneCore` | leave-faulted sabotage | 10 | no | series-only | `53c906b64be` |
| `0b912ab2da8` | `CaRefLaneCore` | occupied slot is our bytes or one fault | 10 v2 | no | series-only | `5679369dcbc` |
| `0c0203ed0f7` | `CaMountObserveCore` | observer waits the longer of its own and the declared lease | 2 v2 (#2516) | no | series-only | `85d64c8dc7e` |
| `781506a4af9` | `CaCasMountCore` | epoch allocation as one immutable write (step 2a) | 11a | no | series-only | `b978f29b4f0` |
| `3357e54496f` | `CaCasMountCore` | straggler adopt during allocation, fence latched | 11a | no | series-only | `fcf85489e79` |
| `021829a925d` | `CaCasMountCore` | fence latch replaces the adopt block | 11a | no | series-only | `e6f04080f97` |
| `3032a4ed15b` | `CaCasMountCore`, `CaCasMountCoreStraggler` (new) | straggler-landed witness | 11a | no | series-only | `63efada1424` |
| `1d5b639d000` | `CaCasMountCore` | mount claim as one immutable operation (step 2b) | 11b | no | series-only | `d6b753dde37` |
| `f41f88acf62` | `run_mount.sh` | drop duplicated step-2b rows | 11b | no | series-only | `cf3b7820bdd` |
| `d05be03d197` | `CaCasMountCore`, `CaDiskLifecycle` | two-step slot absence; no farewell on `Stopping` | 12 | no | series-only | `74699c4129c` |
| `1250c34a796` | `CaCasMountCore` | fence latched for good on an absence stop | 12 | no | series-only | `ce5cf0fc9b5` |
| `0fd85e5a2e8` | `CaDiskLifecycle` | reacquire in the lease thread | 14 | no | series-only | `e171de1af4a` |
| `ab20c78bc06` | `CaCasMountCore`, `CaCasMountCoreStraggler` | reacquire by certificate, resumable claim | 14 | no | series-only | `ebbcdd671dc` |
| `9cd8ace02ce` | `CaCasMountCore` | RESULTS: soundness of `ViewNoHistory` | 14 | no | series-only | `9c97f61e4cb` |
| `4973c7e722a` | `CaDiskLifecycle` | farewell waits for publishes | 13 | no | series-only | `b9f8923f100` |
| `13efdc37e8d` | `CaDiskLifecycle` | one lease thread, no remount | 15 | no | series-only | `1e5bf352105` |
| `1ed1864e8e9` | `CaDiskLifecycle` | RESULTS: step-3 state counts | 15 | no | series-only | `1978f33901a` |

Not series, left as they are: `CaGcCondemnMarkerGate` (`c9c4fa54bf0` … `b483e4ea41c`), `CaBlobPublishCore` (`0acdf9c7939`), `CaGcRootLocalPartManifestCore` (`a385db56107`, `eae836fd008`, `d864fe01b7c`), `CaEpochAllocCore`.

### Batteries after the reverts {#models-batteries}

All five restored models pass their full batteries on `cas-gc-rebuild` after the reverts (4 workers, `-Xmx8g`, `nice 10`, 1200 s per row): `run_disklifecycle.sh` 7/7, `run_reflane.sh` 28/28, `run_mountrenewretry.sh` 17/17, `run_mountobserve.sh` 20/20, `run_mount.sh` all rows (1342 s in total) — every row meets its recorded expectation.

### Re-applying a series PR's models when it merges {#models-reapply}

Revert that PR's revert commits from the table, oldest series commit first (bottom of the revert order up):

```bash
git revert --no-edit 2894594c906 fda253256f2 b223af813c5   # PR 8: CaMountRenewRetryCore
```

- Later commits of one model build on earlier ones. `CaCasMountCore`: 11a, then 11b, 12, 14. `CaDiskLifecycle`: the 2026-10-01 rows (14/15) come before PR 12's `a67369873eb`. A PR that merges out of this order needs its model commits ported by hand from `models/mount-lease-series`.
- The `models/README.md` hunk of `9a5bbedbc41` sits next to the `CaMountObserveCore` paragraph from `11a0675f8d3`; its revert `2417b567344` resolved that conflict by hand.
- Run the model's `run_*.sh` after re-applying; the expectations in its `_RESULTS.md` come back with the files.

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
7. ~~Back up local-only branches~~ — pushed to `filimonov` on 2026-10-12.
8. Owner's local skill `analyzing-cas-health` reads counter names PR 16 removes.

## Deferred items {#deferred}

Prose fixes to apply before opening the matching PR: [deferred-docs-fixes.md](deferred-docs-fixes.md). Deferred by the owner: 17a/17b/18 (transport deadline, now CAS-343), reads during a pause, CAS-149.1/156.1/185/289.4, removal of external test prefixes (AWS/GCS), soak S39 leg B redesign.
