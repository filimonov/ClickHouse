# CAS open items, 2026-10-18 {#cas-open-items-2026-10-18}

State on 2026-10-18 after the member-lifecycle (PR A/B/C), lease (#2517), blob-publication (#2555) and draft-prep work.
Base: `antalya-26.6-20261008` @ `8fefc27bd62`. Session history (Russian): [2026-10-19-session-history-ru.md](2026-10-19-session-history-ru.md).

## Branch chain {#branch-chain}

Every change, open or local, in merge order. A local branch opens as a PR only when the owner says so and its
predecessor has merged (no stacked PRs); after each merge, merge the base into the branches that overlap it.
Base `antalya-26.6-20261008` @ `8fefc27bd62`. Branch names drop the `fix/antalya-26.6/` prefix.

**Wave 0: open PRs, waiting for review or merge**

| PR | Branch | Change | Depends on | State |
|---|---|---|---|---|
| #2517 | `cas-mount-renewal-until-definitive` | expired lease = pause; renewal until a definitive answer | — | merged 2026-10-19 at `54d125835906` (merge `9e540342644`); CI triage: https://github.com/Altinity/ClickHouse/pull/2517#issuecomment-6269177830 |
| #2528 | external author | CAS-36: no manifest GET on promote | — | review by its author; TLA+ check and C′ defense-in-depth note sent |
| #2594 | `cas-blob-attempt-log-level` | first failed blob attempt below Warning | — | review |
| #2577 | `cas-commit-outcome-texts` | honest commit-outcome texts | — | review |
| #2573 | `cas-392-parallel-stale-mount-wait` | custom disks outside `storage_policies_mutex` | — | review |
| #2570 | `cas-378-retired-life-republish` | no republish of a life retired during cold admission | — | review |
| #2566 | `cas-member-lifecycle-pr-a` | PR A: no `/` in server root id; staging cleanup | — | merged 2026-10-19 (merge `af662dc591c`); CI triage: https://github.com/Altinity/ClickHouse/pull/2566#issuecomment-6269521695 |
| #2567 | `cas-363-member-lifecycle` | PR B: owner life, epoch nonce, fenced decommission | A | review |
| #2574 | `cas-member-lifecycle-pr-c` | PR C: rejoin, `retirement_verified` | B | review; S47 rerun after A and C |

**Wave 1: local, independent of wave 0 (open in any order)**

| Branch | Head | Change | Overlaps |
|---|---|---|---|
| `cas-404-cas-mounts-server-uuid` | `42985cb11ac` | `system.cas_mounts.server_uuid` halves swapped | — |
| `cas-354-ifmatch-404-not-error` | `0a51702489a` | `If-Match` 404 not logged as Error | — |
| `cas-unit-test-temp-roots-2` | `c3e92a4c3b4` | CAS-349 test temp roots (gated) | — |
| `cas-ref-lane-fault-latch-2` | `d0480a86184` | 10 v2 ref-lane fault latch + CAS-351 (gated; model `models/reflane-fault-latch-port`) | — |
| `cas-373-late-request-robustness` | `0f285d4e01d` | audit small fixes (SK17, PM-7, versioning bound) | — |
| `cas-366-superseded-ckpt-contribution` | `2d2182b3b19` | refuse an append into a reclaimed seal slot | ref-lane latch (same files) |
| `cas-367-keep-lane-closed-on-ambiguous-removing` | `c7a242d05bf` | ambiguous `beginRemoving` keeps the lane closed | ref-lane latch |
| `cas-368-fresh-incarnation-per-step1-retry` | `d42febbcc9b` | fresh incarnation per namespace-create retry | CAS-405.2 (`CasRetry.h`) |
| `cas-405-1-fetch-cleanup-busy-loop` | `2b031d5490f` | fetch backs off while its old part is `Outdated` | upstream code: owner decision |

**Wave 2: after #2517 (and PR B/C where noted)**

| Branch | Change | Depends on |
|---|---|---|
| `cas-403-install-renewer-during-stop` (`84c1b5f4398`) | no `LOGICAL_ERROR` when a stop races a remount | #2517, PR B |
| `cas-365-confirm-reads-mount-slot` (`cfc79da94a9`) | relink-confirm `Yes` checks the mount slot | #2517 |
| `cas-364-fence-greater-epoch` (`0c57ca5a337`) | fence certificate only for a greater epoch | PR B, PR C |
| `cas-405-2-pause-is-a-wait` (`c84405e77cb`) | a write pause is a wait; open: waits under `DataPartsLock` | #2517 |
| CAS-405.3 | defaults lease 120 s / renew 40 s | CAS-405.2 |
| shrunk 9a | `system.cas_mounts` local row without store I/O | CAS-405.2 |
| CAS-406 | renewal mode table, review renames | #2517 |
| counter sweep from 16 | remove dead counters + `analyzing-cas-health` | #2517 |
| CAS-179 from 13 | no farewell while a snapshot publish may land | PR B, PR C |

## Decisions pending {#decisions-pending}

- Open PRs for the six prepared drafts: CAS-364, 373, 366, 365, 367, 368 (branches `fix/antalya-26.6/cas-36x-…`,
  reports in `tmp/noclock/drafts/prep/<task>/`).
- Backport https://github.com/ClickHouse/ClickHouse/pull/124167 to `antalya-26.6`: a concurrent lightweight `DELETE`
  is lost when mutation N+1 registers before N (`cas_lightweight_delete_4` on #2517, 50000 rows left). Upstream code;
  the patch conflicts with the base.
- `cas_selects` on #2517: one client `Code: 210` from errno 99 port exhaustion on `clickhouse1`; no lease loss.
- CAS-405.2: a query can wait up to 180 s holding `DataPartsLock` (the `preparePartForCommit` rename reaches the entry
  gate). Proposed: wait only at query entry, before MergeTree locks; then an umbrella review of the branch.
- CAS-405.2: a query inherits a background leader's fail-fast in a batched ref append (the one `503` error on the
  stand). Proposed: a separate task.
- CAS-405.1: upstream change in `StorageReplicatedMergeTree::fetchPart`; accept it, and send it upstream too?
- Land the ref-lane model: `git merge --ff-only models/reflane-fault-latch-port` on `cas-gc-rebuild` (re-applies
  three reverted series model commits plus the CAS-351 model).
- CAS-393: six design questions with proposed answers (`tmp/cas393/RESEARCH.md`); spec after the answers.
- CAS-36: the own spec `docs/superpowers/specs/2026-10-12-cas-orphan-sweep-floor-for-rowless-manifests-design.md` is
  untracked since #2528 took the fix; keep it as a record or delete.
- Delete local branches after a look: `wip/pr10-port`, the pre-squash `fix/antalya-26.6/cas-ref-lane-fault-latch`
  (`1556359b7ac`), `fix/antalya-26.6/cas-mount-renewal-until-definitive-stale-local`.
- Delete external test prefixes: `s3://test-altinity-support-team/ca_lat_20261002_aws1/`,
  `gs://content-adressable-test-mfilimonov/ca_lat_20261002_gcs1/`, AWS `ca_lease_pr06_20261008_aws`, GCS
  `ca_lease_pr06_20261008_gcs/` and `ca_lease_pr06_20261008_gcsbase/`.
- #2343: add a link to #2577 (the honest `_ckpt` error text promised there).
- Delete the local branch `fix/antalya-26.6/cas-ckpt-giveup-reason` (superseded by #2577).
- Disk: docker build cache and dangling images (~70 GB), `tmp/cas358` (5.7 GB).

## Small fixes proposed, not started {#small-fixes}

- CAS-354 / CAS-402: `Expect404ResponseScope` for an `If-Match` PUT in `ObjectStorageBackend::nativeConditionalPut`;
  removes the flaky `<Error> AWSClient: Response status: 404` stateless reds (RCA:
  `docs/superpowers/reports/2026-10-16-cas-aws404-conditional-put-rca.md`).
- CAS-404 (High): `system.cas_mounts.server_uuid` shows the uuid with its 64-bit halves swapped.
- CAS-403: `installRenewer` throws `LOGICAL_ERROR` when a stop races a remount; PR B's fenced tombstone added a
  production trigger.
- Cherry-pick the renewer stop-race fix (cause 1, `1a9f1fe5bcd`) to the release line.
- CAS-132 (High): `outcome` column in the `SYSTEM CAS GC RUN` result, so a non-leader run is visible.

## Merge order {#merge-order}

- PR A #2566 (merged) → PR B #2567 → PR C #2574. Next: merge the base into B and C, then gates and CI.
- Rerun soak S47 after A and C: its only red is PR A's `NoWait` refusal text.
- #2570 (CAS-378), #2573 (CAS-392), #2577 (commit outcome texts), #2594 (blob attempt log level) are open.

## Default settings: survive a ~2 minute storage outage {#defaults-two-minute-outage}

Goal: a loss of connectivity to the object store for about 2 minutes passes without a lost mount, without a
remount and without client-visible write errors beyond a pause.

- Today: lease `cas_mount_lease_ttl_ms` 30 s, writes give up at the 90 s policy deadline, an attempt is bounded by
  the disk `request_timeout_ms` (30 s); RustFS read stalls of 90–100 s already fail CI (#2343).
- With #2516 (observers wait for the declared lease, holder cap 10 min) a longer lease is safe; with #2517 an
  expired lease is a pause, not a loss.
- To decide: lease TTL and renew period, the write policy deadline (90 s), attempt timeouts; cost: slower detection
  of a dead member and a longer GC floor hold. Verify on the fault-proxy stand with the store unreachable for 120 s
  (inserts, merges, selects; no `CASMountLeaseLost`, no remount, no client error).
- Measured (20 stand runs): #2517 alone removes lease loss and remount for full outages of 60–180 s; clients
  still get immediate `NETWORK_ERROR` (210). Work split into CAS-405.1 (fetch/cleanup busy loop), CAS-405.2
  (pause = wait), CAS-405.3 (lease 120 s / renew 40 s), CAS-405.4 (fast reclaim after a crash), CAS-405.5
  (`_ckpt` give-up text); CAS-256 reproduced.

## Data correctness {#data-correctness}

- CAS-393: idempotent `INSERT` retry — record the block id in the ref-log promote entry; recovery rebuilds the
  window; check how 26.6.4 reads unknown ref-log keys first. Today a retry after an unknown outcome can duplicate,
  and with `non_replicated_deduplication_window > 0` a retried insert can be lost (`tmp/dupcheck/REPORT.md`).
- CAS-394: do not roll back a part whose promote PUT is durable (model first). CAS-395: the same check for merges,
  mutations, `REPLACE`/`ATTACH PARTITION`, fetch.
- CAS-368 AC#2: model the late landing of a namespace-create step-1 write.
- Residuals that need a format change (CAS-367/368): catalog revision and `removal_by`. No task yet.

## CI and infrastructure {#ci-infra}

- #2343: collect RustFS logs and host metrics in CAS regression jobs; try RustFS 1.0 (`cas_alter_attach_3` is red
  in ~40% of runs).
- #2555 stress: the third server start under ASan did not finish in 80 s while replaying hundreds of `_log`
  objects per namespace. Not root-caused.
- Retry `Connection refused` inside the S3 client for single-attempt creation (nothing was sent, so a resend cannot
  duplicate). No task yet.
- CAS-396…402: follow-ups from the #2555 review.

## Paused mount-lease series (PR 9a–16) {#paused-lease-series}

State: `docs/superpowers/cas/2026-10-12-mount-lease-series-state/README.md`. Paused on 2026-10-12 because the
late-request audit found data-loss defects outside the lease.

- Merged: PR 1, 3, 5, 7, 4, 2 v2 (#2509–#2513, #2516). Open: PR 8 (#2517).
- Local only, backed up to the `filimonov` fork: 10 v2, CAS-349, CAS-351, 9a, 9b, 11a, 11b, 12, 13, 14, 15, 16.
- Their models were reverted on `cas-gc-rebuild` (25 revert commits) so the models match the release line;
  the series models live on `models/mount-lease-series` (#2517's on `models/mount-lease-series-v2`).
- To decide: the direction (ZooKeeper-like session: Live → Suspended → Live, Lost → terminate), then the fate of
  each branch. CAS-405 (pause = wait, longer lease, transport deadline CAS-343) is that direction in practice.
- Independent and ready: 10 v2 (one ref-lane fault latch), CAS-349 (unit-test temp roots).

## Lease (#2517) follow-ups {#lease-follow-ups}

- CAS-406: #2517 review refactorings (one `MountRenewMode` table for plane and retry policy, renames,
  exhaustive `std::visit`).
- CAS-407: move the renewal loop into the request engine (review simplification 6); check on the models
  whether the startup renewal can leave the fenced plane.
- CAS-256: merges restart once a second during a write pause.
- CAS-298 AC#3/#4 (S39 on all replicas, CAS-344).
- CAS-389 (Low): stop-vs-renewal race, cause 2.

## Prose and housekeeping {#prose-housekeeping}

- Run the deferred prose batch `tmp/lease-series/deferred-docs-fixes.md`.
- Shrink the memory index `MEMORY.md` (over its 24.4 KB limit).
- S47 coverage: ASan/TSan builds, more seeds, the `cas-drop-member` CLI sweep (CAS-213.1).
