---
description: 'Consolidated live backlog of all still-pending CAS MergeTree work items. Index of the topic files under BACKLOG/, plus the Inbox append target for quick adds. Issue IDs preserved (never renumbered).'
sidebar_label: 'CAS Backlog (live)'
sidebar_position: 9
slug: /superpowers/cas/backlog
title: 'CAS MergeTree — Live Backlog (pending issues)'
doc_type: 'guide'
---

# CAS MergeTree — Live Backlog {#cas-backlog}

Live backlog: only open work. History and removed entries live in git; verification record in
`consolidation-2026-08/`.

This file is the **index**. The backlog itself lives in per-topic files under
[`BACKLOG/`](BACKLOG/), split 2026-08-04 so a reader (or grep) can go straight to one subsystem
instead of scanning one flat multi-hundred-item file. Item format is uniform:
`- **[id] Title** — PRIORITY — 1-3 lines: current status / the open ask / evidence pointer` (a few
genuinely-live long designs keep structured detail below their header line instead of being
compressed). Issue IDs are never renumbered.

**Blob-publication baseline since 2026-08-23.** Every blob decision begins with `HEAD`; an absent or
`Condemned` body is published unconditionally, while metadata/control objects retain native
conditional operations for create-if-absent and conditional replacement. The [real-storage gate](/superpowers/cas/unconditional-blob-publication-live-results)
and [performance gate](/superpowers/cas/unconditional-blob-publication-performance) are both still
blocked for the external/evidence reasons recorded in those reports. Backlog text below marked
historical or closed must not be read as the current body-publication API.

## Topic files {#topics}

| File | Items | Covers |
|---|---|---|
| [`BACKLOG/ref-protocol.md`](BACKLOG/ref-protocol.md) | 11 | Rev.6 lease-boundary exclusivity, the ref-lane state machine, ref-ledger internals. Top items: `[Late Predecessor PUT]`, `[PART-WRITE-RELEASE-SEAM]`, `[MOUNT-CLAIM-EPOCH-REGRESSION]`. |
| [`BACKLOG/gc.md`](BACKLOG/gc.md) | 46 | GC scalability & byte cost, correctness/observability follow-ups, throughput-collapse and fsck-vs-GC RCAs. Top items: `[gc-frontier-one-list]`, `[GC-DEFER-DECISION-LIST-COST]`, `[gc-rebuild-lease-interlock]`. |
| [`BACKLOG/mounts-and-lifecycle.md`](BACKLOG/mounts-and-lifecycle.md) | 10 items + 3 prose sections | Mount-lease/fence recovery, CA disk lifecycle (rev.8 residuals), pool bootstrap, operator recovery. Top items: `[POOL-REFUSAL-NODE-FATAL]`, `[operator-replica-readd-uuid-trap]` (2026-09-04 field report: remove/re-add a replica under a k8s operator has no supported recovery), `[decommission-successor-mount-race]`, disk-lifecycle-leak (deferred, prose section). |
| [`BACKLOG/gcs.md`](BACKLOG/gcs.md) | 9 items + fix order | CAS on Google Cloud Storage: the live gate's unrun arms, the relink-confirm livelock, the hot-control-key 429 class, environment findings of 2026-09-02, observability/docs items, the recommended fix order. Top items: `[relink-confirm-lane-livelock]`, `[gcs-hot-control-keys-429]`, `[gcs-live-gate-oauth-and-ambiguity]`. |
| [`BACKLOG/formats-and-storage.md`](BACKLOG/formats-and-storage.md) | 23 | Staging/adoption, real-store backends (S3/GCS/Azure), the local/emulated backend, codec/format items. Top items: `[GATE #1: Azure]`, `[B66a]` atomic ordinary emulated writes, `[sec4-decoder-size-bounds]`. |
| [`BACKLOG/replication.md`](BACKLOG/replication.md) | 7 | `MOVE PART`/`PARTITION` onto CA disks, merge/insert retry vs. the mount-lease fence, cross-replica relink. Top items: `[move-part-to-ca-architecturally-unimplemented]`, `[merge-progress-reset-mount-fence]`, `[RPL-5 slice]`. |
| [`BACKLOG/testing-and-ci.md`](BACKLOG/testing-and-ci.md) | 40 | Test coverage & harness, gate-filter/gate-suite gaps, soak/chaos hygiene, standing testing-methodology rules. Top items: `[unconditional-blob-publication-live-gate]`, `[gate-filter-gap-3-backend-contract]`, `[4h continuous chaos soak]`. |
| [`BACKLOG/operability-and-introspection.md`](BACKLOG/operability-and-introspection.md) | 24 | Operability & release gates, disk-error audit follow-ups, fsck/introspection surfaces, the `lazy_load_tables` decision. Top items: `[B197]` SYSTEM control surface, `lazy_load_tables` USER DECISION, `[fsck-partial-degrade-false-consistency]`. |
| [`BACKLOG/performance.md`](BACKLOG/performance.md) | 26 | Read/write path, write-path optimization candidates, stage 2 (postponed), scalability findings from the full-scale campaign. Top items: `[ckpt-read-policy]`, `[ref-catalog-write-hotspot]`, stage-2 concurrent commitPart (postponed). |
| [`BACKLOG/docs-and-cleanup.md`](BACKLOG/docs-and-cleanup.md) | 25 | Architecture/refactoring (no behavior change), minor/polish, source-layout residue, standing hygiene checklist items. Top items: `[refactor: CasGc split]`, `[Group G]` upstream carve-outs, `[phase4-blob-uploader-descoped]`. |
| [`BACKLOG/issue-2310.md`](BACKLOG/issue-2310.md) | 2 | Issue CLOSED 2026-09-14 (gate passed on #2300 run 10, fix in 26.6.4.20001). Triage of Altinity/ClickHouse#2310 (`ATTACH PARTITION FROM` stalls on relink confirm): the verdict that it is `[relink-confirm-lane-livelock]` on a pre-fix package, and the two items that stayed open. Items: `[attach-partition-cas-relink-residency]`, `[s3-empty-file-multipart-retry]`. |

Priority legend: **GATE** = release gate; **HARD** = agreed-necessary, not yet done; **DESIRABLE** =
valuable, not committed; **DOC** = documentation debt; **TEST/INFRA** = validation/harness/CI;
**MINOR** = small concrete improvement; **VERIFY** = believed open, confirm before working. These
seven are the canonical set; individual items also carry more specific free-form qualifiers where an
item's author wanted to say something the seven don't capture (`QUESTION`, `DESIGN QUESTION`, `WATCH`,
`GAP`, `INFRA`, `LOW`/`LOW-PRI`, `PARTIAL`, `MEASURED`, `IN PROGRESS`, `TRACKED, by design`, and
similar) — read those as elaborating one of the seven, not as a competing taxonomy.

**2026-08-04 orphaned-open triage merge:** 367 open-verdict clusters from the docs-consolidation
corpus were 4-way classified; 54 effective new/still-open findings (57 minus 3 rechecked and closed
by design) were merged into the topic files above, each marked with a `## New findings from the
2026-08-04 orphaned-open triage` heading; 35 duplicates were folded into their existing matching item
as a confirmation note rather than inserted separately. Full triage record:
`.superpowers/sdd/2026-08-03-cas-docs-map-reduce-consolidation/orphan-triage-final.md`.

## Inbox {#inbox}

### `[cas-txn-commit-inside-noexcept-aftercommit]` A CAS transaction over a committed part runs inside `noexcept` MergeTree-transaction callbacks; any throw there is a server abort (2026-09-10) {#cas-txn-commit-inside-noexcept-aftercommit}

**Observed.** PR #2300 CI run 10 (head f377ba3a499, attempt 2), Stateless amd_asan_ubsan cas-s3 2/2: "Server died",
signal 6, during `01169_old_alter_partition_isolation_stress` on the query `COMMIT`. `clickhouse-server.err.log`:
`Terminate called for uncaught exception: Code: 210. DB::Exception: CAS write could not be committed (CAS ref-log
append for namespace 'stateless-ca-s3/store/044/...' txn 1-804 was refused BEFORE any request was sent — the append
lane is NOT wedged ... and the txn id is not consumed); retrying later. (NETWORK_ERROR)`, thrown by
`makeCasWriteRetryLaterExceptionPtr` ← `CasRefLedger::commitRefChunk` ← `flushRefBatch` ← `runRefQueueLeader` ←
`appendRefOpsOnRuntime` ← `Pool::appendRefOps` ← `PartWriteTxn::abandon` ← `ContentAddressedTransaction::publishStaging`
(the scratch build's abandon right after a successful `repointRef`). The same namespace logged `refusing snapshot
publication while the append lane is not Ready (state 1)` in the same second: an ordinary transient refusal. The
reviewer's independent 2026-09-10 report on the PR reached the same root cause and named it the sole merge blocker.

**Why a transient became an abort.** `TransactionLog::finalizeCommittedTransaction` and
`MergeTreeTransaction::afterCommit` are `noexcept`. `afterCommit` calls `VersionMetadata::setAndStoreRemovalCSN` /
`setAndStoreCreationCSN` for every part the transaction touched → `updateInfoWithRefreshDataThenStoreAndSetMetadata`
→ `storeInfo` → `writeFile(txn_version.txt)` into the COMMITTED part directory; no try/catch anywhere on that path
(`src/Interpreters/MergeTreeTransaction.cpp`, `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp`).
`MergeTreeTransaction::rollback() noexcept` does the same with `RolledBackCSN`. On a content-addressed disk a file
write into a committed part is a transaction over a live ref, and `publishStaging` takes the repoint branch:
`checkOpAdmitted(Write)` → `getView(ForceFresh)` → `stageManifest` + `precommitAdd` → `fanOutBlobUploads` →
`repointRef` → hook → scratch-build `abandon`. Every one of those steps throws on a transient refusal or timeout;
the abandon is merely the one that fired. Upstream has the same contract hole for any object-storage disk
(`writeFile` can throw inside `noexcept afterCommit`); on local disks it is practically unreachable, on CAS a
lease-health refusal is routine, so it is reachable under sanitizer load.

**Landed (point fix, not the class).** fix-cicd 25051b967f0 + 8b8cb8a50d9 + be8666f8f06 (denoised 21249fab1ee;
cas-gc-rebuild 8d68db5e3c1..fb035142262): `ContentAddressedTransaction::abandonBuildBestEffort(ns, ref, st, what)
noexcept` — try { seam; `st.build->abandon()`; } catch(...) { log inside its own catch } `st.build.reset()`; used
after the repoint, after `dropRefIfPresent` on the removal path, and in the destructor (which already had this
tolerance inline). Nothing allocates outside a catch boundary (it runs from the destructor); the message is a
literal; the strings come by reference from the route. GC accounting is sound (codex verified against
`CasPartWriteTxn.cpp:152` and `CasPool.cpp:1677`): the reset runs `~PartWriteTxn`, which queues the writer cleanup
duty for the unsettled precommit without retiring its build sequence; a successor's recovery sweep is the backstop;
reclamation is delayed, never premature. Test seam `armAbandonFailureForTest` / `takeAbandonFailureForTest`
(one-shot, inert and allocation-free when unarmed, gated by `hasAbandonFailureForTest`), tests
`CASCommitRollback.AbandonRefused{AfterRepoint,AfterRefDrop,AfterAbsentRefDrop}DoesNotFailCommit`. Codex: 3 rounds
(MAJOR:2 → MAJOR:1 + MINOR → clean). Gates: release `CAS*` 2529, ASan 2533, 0 reports; fail-first verified.

**Why abandon is the ONLY point that may be swallowed.** After the repoint (or the ref drop) the durable outcome is
already recorded in `out_slot`; the abandon is bookkeeping with a documented fallback. Swallowing any of the other
five steps would silently drop the `txn_version.txt` write itself and tell the caller nothing.

**Options for the class (estimates 2026-09-10).**

| Option | Where | Size | Risk | Estimate |
|---|---|---|---|---|
| Catch at each remaining throw point of the repoint branch in CAS | CAS only | ~50 lines | high: hides a lost durable write from a caller that cannot react | do not do |
| Tolerate a failed CSN store in `afterCommit`/`rollback`: catch, log, keep the in-memory CSN, rely on the TID→CSN lookup `VersionMetadata::isVisible` already performs (`TransactionLog::getCSN(removal_tid)`), and re-persist at the next opportunity (`appendCSNToVersionMetadata` on part load) | `src/Interpreters/MergeTreeTransaction.cpp` (upstream file; fork patch, portable upstream) | ~30–50 lines + a failpoint test: inject a write failure during `COMMIT`, assert no abort, visibility correct, the Keeper transaction log still gets cleaned | medium: the upstream comment says the CSN write is what lets the ZK log be cleaned ("Write allocated CSN, so we will be able to cleanup log in ZK"); must prove a lost write is a bounded leak, not a stuck log | 1–2 days, mostly the semantic proof |
| Shrink the CAS surface: a single inline entry written into a committed part should not need a scratch build (no `stageManifest`/`precommitAdd`/`abandon`), one `repointRef` RMW instead | CAS | ~100–150 lines, restructure the repoint branch | medium; reduces six throw points to two, does not remove the class | 1 day |
| Retry these writes under the lease instead of the 90 s policy | CAS | small | turns the abort into a `COMMIT` that hangs for minutes | not a solution |

**Recommendation.** Option 2 as the class fix (it is the right contract for every object-storage disk, not only
CAS), option 3 as CAS-side surface reduction. Do not patch site by site again: if another `Terminate called` shows
`ContentAddressedTransaction::commit` under `afterCommit`, `rollback`, or a destructor, this entry is the plan.

**Related.** `[cas-transient-lease-fence-surfaces-to-clients]` (the same `retrying later` reaching synchronous
callers); `[ref-catalog-write-hotspot]` and Altinity/ClickHouse#2343 (the load that makes lanes not-Ready);
`reference_cas_ci_observability_gaps` (the ASan log kept only 12 frames of the exception stack, the terminate stack
was unsymbolized beyond `terminate_handler`; the chain above was established from the code, the query id and the
`Child process was terminated by signal 6` line). Evidence: lane-g `tmp/pr2300-cicd-watch/run10/asan_cas_2of2_att2.err.log`
(line 131680 ff.), `tmp/round9/review/abandon{,2,3}.md`.


### Real-GCS 15-minute soak (2026-09-04, binary 03ccdd795d9) — return items {#gcs-soak-2026-09-04-return-items}

Report: `docs/superpowers/cas/2026-09-04-gcs-soak-15min.md`. Correctness PASS: `dangling=0` on both
checkpoints, the aggregate oracle held (node1 = node2 = model, count 40883), GC's retired pipeline closed
(`CASGCRetiredCondemned = Graduated = Redeleted = 5458`), `GaveUp` 2 (leader) / 1, zero dangling/corrupt
rows in `cas_log`, RSS plateau 1.4–1.7 GB per node. The driver reported FAIL on its final converge
checkpoint ("unreachable never stabilized within 1092 s", history 4909 → 287 strictly monotone) and its
own post-failure fsck already showed `unreachable=0`: a timing miss, not non-convergence.

- **Harness: `fixpoint_timeout_s` assumes a GC round every ~2 s** (`gc_interval_s=2` default) and gave
  1092 s for ~98 rounds; real GCS rounds took 18.8 s to 1250 s (round 7: 1114 s of its 1125 s in
  `pending_deletes`, 5000 throttled deletes). Derive the bound from observed round durations, as
  `wait_for_pool_drain` already does; otherwise every real-GCS soak with a backlog fails falsely.
  **Reproduced on real AWS 2026-09-04** (`ca_live_20260904_aws_r1`, 8-minute no-chaos smoke; read-only
  investigation report in the run's `tmp/aws_unreachable_debug_report.md`): `history=[3517, 2779]`,
  bound 300 s, while the pool converged to `unreachable=0 dangling=0 unaccounted=0` on its own 38
  minutes after the checkpoint started; condemned = graduated = redeleted = 4305, spared/replaced/
  absent 0, every unreachable object a `blobs/` key in `delete_pending` or `awaiting_gc`. Two concrete
  defects in `soak/checker.py`: (1) `poll_unreachable_to_stable` needs `stable=3` samples and each sample
  is a full fsck (~325 s at 4-5k objects), so three samples cannot fit a 300 s bound regardless of GC
  health; (2) `initial` is read before the first condemning round, so `max(300, 0.2 * initial)` collapses
  to the floor while the backlog is still rising; the `wait_for_pool_drain` precondition never engaged
  because `soak.pool`'s physical probe returns `None` on a live store. Fix: bound ≥ `stable` × measured
  fsck seconds plus a drain estimate taken from `pending_reclaim` in `system.cas_mounts` and the observed
  `system.cas_gc_log` round durations. Also: the run JSON's `fsck_status: "skipped"` is misleading when
  three fscks ran and only the final one was not reached.
  Owner: `utils/ca-soak/soak/run.py`.
- **GC round cost on GCS** — the measurement behind `{#gc-manifests-immutable-cheap-reduce}`: five leader
  rounds 1827 s total, `fold_reduce` 1383 s of it; sequential per-object phases (`fold_reduce`,
  `ref_object_cleanup`, `manifest_deletes`, `pending_deletes`) are the whole budget.
- **429 `SlowDown` hot spot on one key per node: the mount-lease checkpoint object
  `cas/ns/state/<mount-id>/_ckpt`** (269 on ch1, 340 on ch2) — GCS's ~1 mutation/s per object; the
  renewer/checkpoint writer exceeds it. Question: can `_ckpt` writes be paced to the object budget or
  coalesced, and does the 429 backoff ever threaten the lease deadline? (Not in this run: no lease loss
  from 429.)
- **DNS resolution outage to `storage.googleapis.com` at 11:07:01–11:08:49Z** (repeated
  `Poco::TimeoutException`/DNS errors, zero 429s in the window) caused a simultaneous lease loss and a
  six-attempt remount storm on BOTH nodes (attempt 7 succeeded) and the 53.9 s lease phase on ch2's
  round 191. The fencing protocol did its job; the item is observability: the remount log should name
  the DNS failure class distinctly, and the host's Docker resolver flakiness is the suspect (unconfirmed
  against host DNS logs).
  Not a storm: the remount attempts were 1.5, 2.5, 4.5, 8.5, 16.5 s apart, then 72 s (attempt 7 succeeded
  once DNS returned) — `CasMountRuntime::remountLoop`'s deterministic doubling backoff 1 s → 30 s cap
  (`backoff_ms = min(backoff_ms * 2, 30000)`), with NO jitter: both nodes lost the lease within 1.4 s of
  each other and retried in lockstep (attempt k at the same second on ch1 and ch2). Harmless with two
  nodes; with N nodes it is the thundering herd jitter exists to break. Small change: draw the remount
  wait from the engine's full-jitter schedule (`Retry::backoff`) instead of the bare doubling.

### `[gc-manifests-are-immutable-so-reduce-and-deletes-can-be-cheap]` The orphan sweep re-reads every listed manifest and deletes manifests one conditional request at a time; manifest keys cannot be reborn, so neither is needed (2026-09-04, user) {#gc-manifests-immutable-cheap-reduce}

Measured on the 15-minute real-GCS soak (leader `ca-live-gcs-ch1-1`, `system.cas_gc_log`, phase rows
joined to `Finish` rows through `round_id`): rounds 1.2 s → 18.8 s → 128 s → 584 s → longer. Round 3 =
`fold_reduce` 380 s (3840 GETs: 2870 `CASGCGet`, 814 `CASRootGet`, 100 `CASManifestGet`; 1966 read
errors), `fold_ref_intake` 46 s, `manifest_deletes` 37 s (188 conditional deletes). Round 4 =
`fold_reduce` 300 s (3002 `CASGCGet`, 2006 errors), `ref_object_cleanup` 204 s (512 keys × HEAD + catalog
GET + gc/state GET + conditional DELETE). Round 5 = `fold_reduce` 587 s (3400 GET + 5383 HEAD, all HEADs
inline: `CASGCReadAheadMiss` 5382, hits 0), `manifest_deletes` 617 s (3250 conditional deletes at ~190 ms).
`orphan_sweep` nominated nothing in any round.

The reduce's `CASGCGet` volume is NOT manifest bodies (those are capped at 100 by
`manifest_sweep_delete_budget_keys`): it is `floorForNamespace` in `prefixEligibleOn`
(`Gc/CasOrphanManifestSweep.cpp:481`), called once per listed manifest because the eligibility memo is
keyed by build prefix and every `INSERT` is its own build; each call reads the mount key of every
`/`-prefix of the namespace (`…/store/465` 404, `…/store` 404, `<root>` hit). 956 × 3 = 2868 ≈ 2870
reads; 956 × 2 + 54 reissues = 1966 errors. Rounds 4 and 5: 1000 × 3 + 2 = 3002, 1000 × 2 + 6 = 2006.

Why the per-key conditional deletes are not needed (user, 2026-09-04): manifest keys, ref `_log` and
`_snap` keys are write-once by construction (`op.create`, epoch/sequence/ordinal monotone, life-qualified;
global manifest-key uniqueness rests on decommission being irreversible). Blobs are NOT write-once and
keep the exact-token delete. GCS accepts batch `DeleteObjects` (the live gate proves it; an earlier note
here claiming the XML API lacks it was wrong). Retirements cannot be taken from the source-edge runs:
`sourceEdgeId` (`Gc/CasBlobInDegree.cpp:164`) is an irreversible hash, so a nominated orphan's body is
still read, bounded by the candidate budget.

Design, `IMPLEMENTED` (2026-09-04) on branch `cas-gc-write-once-keys`:
`docs/superpowers/specs/2026-09-04-cas-gc-immutable-key-round-cost-design.md`
(A: one floor per namespace per page; B: late manifest reads and read-ahead in the sweep; C:
`removeManyWriteOnce` with `manifest_deletes` as consumer; D: ref-cleanup cohorts). Plan:
`docs/superpowers/plans/2026-09-04-cas-gc-immutable-key-round-cost.md`. Measured on two 2026-09-04
GCS soaks of the implemented binary (`HEAD 0247064fa17`); full figures and the placement/re-scope
notes are in the spec's `#implementation-record`:

| phase | before (morning, old binary) | after |
|---|---|---|
| `fold_reduce`, steady-state round | 300 to 380 s | 2.1 to 4.9 s |
| `manifest_deletes`, round with 1506 keys | 617 s / 3250 keys | 2.0 s, 2 `CASBulkDeleteRequests` |
| `ref_object_cleanup`, round with work | 200 s | 0.6 to 1.4 s |
| `fold_reduce`, mass-removal round | 587 s | 123.4 s (item E: inline `HEAD`s, not this design's target) |

**Investigation item, not part of the design (refined 2026-09-04 with run-2 evidence):** round 5's
5382 inline HEADs was the first sighting. The no-chaos GCS soak reproduced the same pattern from
ordinary workload backlog alone, without chaos and without a scripted cliff: rounds 23-25 show
`CASGCReadAheadMiss` 2534 / 795 / 222 against inline `HEAD` counts 2530 / 793 / 219, and each round's
`Finish` row shows `CASGCReadAheadWasted=64` — exactly one window pinned per round, independent of
round size. Unlike the original sighting, `epoch_crossings=0` on the intake row for all three rounds,
so the epoch-crossing hypothesis is not the only mechanism that pins the shared read-ahead window on
a mass-removal round; something else pins it too. Needs a local reproduction that isolates the
mechanism without a crossing; the fix shape, once found, is still an epoch-crossing-style discard
rule applied at the fold's own hinting site, `Gc/CasGc.cpp:2512`.
A sibling of the same class, found at the final review of the write-once-key branch: the shared
recovery walk (`Pool/CasRefProtocol.cpp`, `recoverRefTableDetailedFromAuthority`) discards hinted
ref-log ids only at a seal, so its `CORRUPTED_DATA` and decode throws leave up to one window pinned,
and `planManifestCursorPage` catches per-namespace throws and keeps using the same reader for the rest
of the page. Throughput only; the `OutstandingHintGuard` shape the sweep's tail walk uses closes it.

### `[gc-sweep-reads-the-committed-tail-twice]` The orphan sweep walks each namespace's committed tail twice per page (2026-09-04) {#gc-sweep-reads-the-committed-tail-twice}

`activeManifestKeys` (`Gc/CasOrphanManifestSweep.cpp`) first recovers the table through
`recoverRefTableDetailedFromAuthority`, which replays every log from the checkpoint base to the
committed frontier, then walks the same range again itself to collect tail-removal targets. On the
GCS soak both walks read each epoch-2 log once, so every ref log the sweep touches costs two GETs,
and an epoch crossing costs two windows of read-ahead waste instead of one (the `wasted=127`
rows). The recovery already decodes every transaction; collecting the `-1` manifest edges during that
replay would remove the second walk. Small, GC-internal, no protocol change; measure with the sweep's
`CASRootGet` on the reduce row (814 per round on the 2026-09-04 GCS soak before A to D, still two
per log after).


### `[soak-harness-needsrecovery-after-near-ttl-pause]` The soak harness treats a near-TTL chaos pause's designed fence-then-recover as a fatal violation (2026-09-04) {#soak-harness-needsrecovery-after-near-ttl-pause}

GCS soak run 1 (`ca_live_20260904_r2`, phase 3, chaos): chaos fault #4 was `both pause` for 29 s
against a 30 s mount-lease TTL. `ch2`'s renewer fenced (`external_lease_deadline`, 0 physical
attempts), its ref append lane went `CASRefNeedsRecovery` at the committed-frontier publication
fence, and the harness's recovery checkpoint treats any nonzero `CASRefNeedsRecovery` as a
"LATE-PUT FENCING VIOLATION" and aborts the driver. The data model matched byte-for-byte on both
nodes (count 62496, same `sum_fp`/`uniq_keys`/`sum_v`/`sum_version`) and fsck came back clean
(`dangling=0 unreachable=0 stale_edge=0`); `system.cas_log` shows `ch2` recovered normally
(`mount_remount ok`) once the pause ended. `CASRefNeedsRecovery` is a cumulative counter: one
genuine, expected recovery event during a designed near-TTL pause keeps it nonzero in every later
snapshot by construction, which is what "stays 1 across three ticks" actually means here, not a
stuck lane. Ruled not attributable to the branch under measurement (`cas-gc-write-once-keys`): no
file it touches sits on the mount renewer or the ref append lane. Fix options for the harness: (a)
classify a chaos pause whose duration is within 1 s of the lease TTL as `freeze_long` rather than
an ordinary pause, since a fence-then-recover is the designed outcome at that margin; or (b) drop
`CASRefNeedsRecovery` from the must-stay-zero counter set at the recovery checkpoint specifically
for the case where the lane has since remounted and the data model matches, since the counter
cannot distinguish "recovered once, correctly" from "still stuck" without also reading the mount
state.

### `[gc-blob-pending-deletes-now-dominant]` With A-D landed, serial exact-token blob deletes are the largest remaining GC phase on a mass-removal round (2026-09-04) {#gc-blob-pending-deletes-now-dominant}

GCS soak run 2 (`ca_live_20260904_r3`, phase 3, `--no-chaos`): round 25's `pending_deletes` phase
alone took 551.5 s for 2731 blobs — one `HEAD` plus one conditional `DELETE` per blob, serial
(about 100 ms each) — against `fold_reduce` 14.7 s, `manifest_deletes` 0.22 s and
`ref_object_cleanup` 0.22 s in the same round. This is now the largest single-phase cost this
design's rounds show, larger than any of the phases A-D target. Blobs stay exact-token by design
(I5: a condemned blob key can be re-uploaded by a writer, so the exact-token delete is what keeps
that resurrection safe; blobs are explicitly out of `removeManyWriteOnce`'s scope). The lever is
already on this list: `[gc-delete-concurrency-serial]`.

### `[cas-transient-lease-fence-surfaces-to-clients]` A transient mount-lease fence surfaces to synchronous client calls as `NETWORK_ERROR` (2026-09-04) {#cas-transient-lease-fence-surfaces-to-clients}

Local CA-s3 stateless lane, run 2 (`docs/superpowers/cas/2026-09-04-stateless-lane-triage.md`, 11137
tests): under the same S3-endpoint saturation that produces the PUT-timeout class, the renewer logged
`CAS mount renewal 'stateless-ca-s3' fenced after 3 physical attempts in 9068 ms
(classification=external_lease_deadline)`; 371 "mount lease not held" lines followed, almost all
background retries that self-healed (drop retries, merge-tree executor), but two tests surfaced the
window to a synchronous client call and failed with `Code: 210 … mount lease not held`:
`01128_generate_random_nested` (the INSERT path's ref-log append: "NEEDS RECOVERY at committed-frontier
publication fence") and `02581_share_big_sets_between_mutation_tasks` (`StorageMergeTree::waitForMutation`).
The fence itself is correct and fail-close (unit-tested at `CasServerRoot.cpp`, `gtest_cas_heartbeat.cpp`,
`gtest_cas_pool.cpp`). The open question is client-facing semantics: whether a query that hits a
CAS-transient fence should wait for the remount (bounded by the query's own timeout) rather than fail
with `NETWORK_ERROR`, and where that wait belongs (the ref-lane append, `waitForMutation`, or a
disk-level "mount recovering" retry). Needs a design call, not a mechanical fix; frequency 2/11137 on
this lane.

Measured frequencies from the same run for the recorded items: PUT-timeout Error log
(`{#cas-s3-lane-put-timeout-logged-at-error}`) 42/11137; ref-catalog starvation
(`{#ref-catalog-cas-starvation}`) 1/11137. Class B (404 on stderr) = 0 after 6830b73af27.

### `[snapshot-publish-error-hook-busy-loop]` An exception before any write-failure arm in the background snapshot publisher redispatches with no backoff (2026-09-04) {#snapshot-publish-error-hook-busy-loop}

**FIXED 2026-09-04 in b3ac09f1c6f:** the detached task's `catch (...)` in `dispatchSnapshotPublisher` now arms `advancePublishBackoff` under `state_mutex` before the error hook runs (the only site that knows an exception escaped; `settleSnapshotPublish` carries no outcome). Red-first: `CASDetachedWork.ThrowingPublishAttemptIsPacedByTheBackoff` — 6,921,818 attempts in 300 s on the old code, 513 ms green after. The remount-path wall clock at `Pool/CasPool.cpp:1258` that the fix report flagged is stamping-only and never enters the reclaim or fence decisions (those read the injectable clocks) — not a trap, nothing to do.

`CasRefLedger::dispatchSnapshotPublisher`'s detached task's outer `catch (...)` (`CasRefLedger.cpp`,
around the `dispatch_detached` call) never arms `advancePublishBackoff` — every OTHER exit from
`tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl` (a non-`Committed` write, an encode
failure, a checkpoint that fails to advance) does, before returning `false`. `settleSnapshotPublish`
re-evaluates `admitSnapshotPublishUnderStateLock` on every settlement, and its only pacing gate is
the backoff deadline (`now >= rt.publish_backoff_until_ms`) — so an exception reaching the outer
`catch` without one of those ordinary arms having run first (any exception thrown before the
write attempt itself — an encode setup bug, an assertion, anything unanticipated) causes the ledger
to redispatch at full speed, forever, since the tail count that made it over-threshold is never
cleared either. Measured directly: `1,263,317` redispatches in 30 real seconds, one core pegged. It
stops only because `Pool::tryRemountOnce`'s mount-lease check uses real `std::chrono::system_clock`
(not the injectable boot clock the rest of the engine reads), so the real default 30000ms
`mount_lease_ttl_ms` eventually elapses and fences the mount closed. A test that reproduces this
exact path deterministically, without a real 30s spin, is `CASDetachedWork
.SettlementSurvivesAThrowingErrorHandler` (`src/Disks/tests/gtest_cas_detached_work.cpp`) with its
`snapshot_after_capture_hook_for_test` re-armed persistently instead of self-disarming after one
throw. Fix shape: arm `advancePublishBackoff` in `dispatchSnapshotPublisher`'s outer `catch (...)`
(or in `settleSnapshotPublish` before a redispatch) so ANY exception paces the same as an ordinary
write failure. This is a lease/ledger production-path change and needs review before it is made, not
something to fix opportunistically alongside an unrelated task.

### `[stateless-lane-wall-time-is-drop-table]` Measured on the CA-s3 stateless lane (2026-09-04): DROP TABLE is the suite's largest wall-time sink; the server is not CPU-bound {#stateless-lane-wall-time-is-drop-table}

`system.trace_log` over ten minutes of the parallel suite (~10 jobs): ~280 CPU samples against 630k
Real samples — the server is waiting, not computing; 21% of the CPU samples are stack unwinding and
symbolisation for exceptions (every expected 412/404/timeout builds a full `StackTrace`). Query threads'
Real samples: 44% waiting in `InterpreterDropQuery::executeToTable` for the synchronous data drop, 25%
in `TaskTracker::waitAll` under `fanOutBlobUploads`/`commit`/`moveDirectory` (the part-commit round
trips), 14% in `finalizeConditionalWrite` (conditional PUT wait), 13% in `poll`. `system.query_log`,
same window: DROP TABLE n=1436 p50 2.4 s p90 11.9 s max 34.7 s; CREATE TABLE p50 184 ms p90 391 ms;
INSERT p50 253 ms p90 541 ms. S3 calls: GET 11 095, HEAD 2 966, PUT 1 597, LIST 206, DELETE 25.
The background drop workers themselves sit in a futex inside `DatabaseCatalog::dropTableDataTask`, not
in S3 — the drop chain is serialised somewhere (the namespace removal through the hot `ref_catalog`
door with the conflict backoff up to 5 s fits the p90/max shape). Placement: profile one DROP on the CA
disk (S3 operations and CAS conflicts per dropped table) as the first measurement of the
`{#ref-catalog-cas-starvation}` follow-up; second target = the part-commit round trips (upload fan-out
+ conditional PUTs); third = skip `StackTrace` capture for expected 412s.

Design for the first target: `docs/superpowers/specs/2026-09-04-cas-hot-key-write-lane-design.md`
(revision 34, phase A, 2026-09-04; accepted by the codex review of revision 33 with no CRITICAL or MAJOR), which supersedes the fix sketch below where they differ; the
deferred phase B is `{#hot-key-lane-phase-b}`.

### `[ref-catalog-cas-starvation-under-parallel-writers]` One process's CREATE/DROP writers starve each other on the ref-catalog compare-and-swap (2026-09-04) {#ref-catalog-cas-starvation}

Seen on the local CA-s3 stateless lane (run 2, ~10 parallel jobs): `01039_mergetree_exec_time`'s
`CREATE TABLE` failed after 78.9 s with "CAS ref catalog update: gave up at the policy deadline". RCA in
`docs/superpowers/cas/2026-09-04-ref-catalog-starvation-rca.md`: 113 `PreconditionFailed` (412) on
`cas_s3/cas/ref_catalog` in 80 s from 53 threads; the losing writer made 35 attempts spaced by the
engine's full-jitter backoff (up to the 5 s cap) while fresh writers, starting at attempt 1, kept
winning about once a second; successful `CREATE TABLE` in the window: p50 203 ms, p90 457 ms, max 10.4 s.
`readModifyWrite` routes a `Conflict` into the same `pauseAndReissue`/`Retry::backoff` schedule and the
same `reissues` counter as a transport fault; the spec prescribes exactly that ("conflicts spend the same
budget as errors", motivated by GCS's ~1 write/s per-object budget) but never analyses fairness among
many independent writers on one hot key. Pre-migration, the catalog loop retried a conflict at once,
capped at 100 attempts. Verdict: spec-conformant, unfair by construction under sustained arrivals.

DROP TABLE is the same starvation, measured on one 33.6 s drop (table `6f6ae69d…`, two parts): the
query waited 18 s in the drop queue behind other drops, `dropAllData` then spent 1.0 s on two
`delete_tmp` ref repoints (~0.5 s per part) and **15.4 s in "removing table directory recursive"**,
which is ONE namespace-removal conditional write on `ref_catalog` that lost eight races in a row
(`WriteBufferFromS3 was canceled` at +0.03, +0.21, +0.37, +0.71, +1.8, +4.5, +8.1, +12.3 s — the
growing conflict backoff) before landing; the worker thread then immediately took the next table.
Sixteen drop workers were active, all bottlenecked on that one key: drops complete in bursts of ~10 per
5 s with gaps. The query thread itself issues zero S3/CAS calls (all per-query counters are 0); it only
waits. So a DROP is, as designed, one catalog write plus one ref write per part — and the catalog
write is what costs seconds.

Fix, two halves, in this order:

1. **Serialize within a process and remember the last committed state (user's proposal, primary).**
   Compare-and-swap is needed only against other servers. Inside one server every catalog mutation
   takes one FIFO door per pool in `CasRefCatalog`: one write at a time, no combining. The door keeps
   the last COMMITTED snapshot and its etag from the previous successful write, applies the next change
   to that snapshot and issues the conditional PUT against the remembered etag — no read before the
   write. Only a lost race (another server's write, a 412 that the engine settles by its resolve read)
   refreshes the remembered state from `Conflict::seen` and retries; any outcome that does not prove
   what is durable (`GaveUp`, `Conflict{NotObserved}`, a fence loss or remount) invalidates the memory,
   so the next writer reads first — fail-close. GC, the reconciler and decommission take the same door,
   otherwise their writes stale the memory and cost one extra round trip each. Effect on this lane: N
   concurrent `CREATE`s become N sequential PUTs with no reads and no intra-process 412s. Tests: N
   threads on the in-memory backend under an injected clock — at most one CAS in flight, FIFO order,
   zero reads between consecutive committed writes, bounded maximum wait; and a second `Pool` on the
   same backend as the external writer whose win forces exactly one re-read and one retry.
2. **Decouple conflict pacing from transport-fault backoff (secondary, spec change).** A `Conflict`
   already carries the fresh object; pace its reissue with a flat small jitter (the RCA suggests
   uniform(0, 250 ms)) instead of the growing `reissues`-driven schedule, keeping the capped exponential
   backoff for real transport faults. Size the flat ceiling against the GCS object budget the spec cites;
   with half 1 in place, conflicts come only from other servers and are rare. Pin with an N-writer fake-clock
   test asserting the earliest writer's consecutive-loss streak stays bounded while new arrivals keep
   winning (red today). Update the spec sentences at "Conflicts spend the same budget as errors".

Superseded by the design `docs/superpowers/specs/2026-09-04-cas-hot-key-write-lane-design.md`
(revision 34, phase A, 2026-09-04; accepted by the codex review of revision 33 with no CRITICAL or MAJOR). Phase A is the fix sketch above minus nothing it needs: a per-pool
FIFO lane above the request engine for any hot compare-and-swap object, the catalog first; one hold at a
time per key from any of the pool's planes; an LRU of last known objects so the next write needs no read,
under one absolute rule (a verdict a `decide` renders on a cached base is never delivered; every write is
conditional on the cached etag); the engine's `WriteResult` returned unchanged; the flat conflict pause,
with the growing schedule kept for a conflict that settled a transport fault (`Conflict::any_ambiguous`);
the step-1 fence marker caught. Revisions 1 to 26 (sixteen codex and six opus rounds) also designed
combining, GCS spacing, a hold clamp and the GC erase inside the lane; that is phase B,
`{#hot-key-lane-phase-b}`, deferred by owner decision on 2026-09-04 after the review loop failed to
converge on its periphery (findings per round never below eight, the document tripled).



### `[hot-key-lane-phase-a-followups]` Phase A of the hot-key lane landed on `cas-hot-key-lane`; what its reviews deferred (2026-09-04) {#hot-key-lane-phase-a-followups}

Branch `cas-hot-key-lane` (12 commits over `e59fe7e8e4b`), `CAS*` gate 2406/2406, reviewed per task (opus/lite),
whole-branch (opus: mergeable) and end to end (codex `gpt-5.6-sol` high: three majors, folded). Not yet merged.

Before or right after merge:
- The new death-test twin `CASRefCatalogDeathTest.AStaleHintCasAdmitEntryAdmitsWhenTheStoreHasRoomAborts` has never
  executed: `lane-g` has no debug or sanitizer build. Run `CASRefCatalog*:CASHotKeys.*` in an ASan build once.
- The acceptance measurement of the design's Task 7 (ten minutes of the parallel stateless suite on the CA-s3 lane:
  `DROP TABLE`/`CREATE TABLE` percentiles, `PreconditionFailed` on `ref_catalog`, the `CASHotKey*` and
  `CASRequestConflictPause` deltas) is the gate for `{#hot-key-lane-phase-b}`.

Deferred by the reviews (all triaged as not blocking the merge; the first is the one the measurement needs):
- The catalog loop's own conflict pauses (`op.pause` in `casUpdateImpl`) have no `ProfileEvents` counter; the engine's
  `CASRequestConflictPause` counts only `readModifyWrite`'s. Add one so the hottest key's pacing is visible.
- Same-thread reentrant `submit` while holding self-waits until the deadline (the spec's callback rule is stated, not
  enforced); a thread-local in-hold flag throwing `LOGICAL_ERROR` would make it immediate. Note: a `LOGICAL_ERROR`
  aborts debug and sanitizer builds.
- `CasRequests.h` pulls `Common/CacheBase.h` into every includer; a forward declaration of `CasHotKeys` plus an
  out-of-line `CasRequests` destructor would confine it.
- `CASRequestConflictPause`'s description says "no transport fault preceded it"; accurate: "in the same inner write".
- `kWaitSlice` in `Backend/` vs the directory's `SCREAMING_SNAKE` constants.
- The `Leave` guard's comment says "allocates nothing"; true only under the lock (the log line after it allocates).
- Tests that discriminate weakly (each carries a deterministic sibling, so none is a false-pass risk today):
  `CleanConflictsBeforeAFault...` (probabilistic; assert the ConflictPause/Reissue deltas),
  `AConflictThatSettledAFault...` (sleep bound non-discriminating; the Reissue delta carries it),
  `ABaseReadThatFails...` (pins only `sent_any` absolutely), `ACachedStartPastTheDeadlineSendsNothing`
  (`decided == 1` is the discriminator, closed by the final review), the corruption test's "break again" arm,
  `AFailedEnqueue...` (only the `inserted == true` arm), the GC-erase test ("exactly" in prose, "at most" in code),
  the "nothing of ours landed" assertion (give the external a distinct `removal_started_round`).
- Unused `ProfileEvents` externs in `gtest_cas_ref_catalog.cpp` and `gtest_cas_pool.cpp` (plan-mandated).
- The two remaining items of `task-1-review.md` (the SDD workspace under `.superpowers/sdd/`, git-ignored; the
  review files there are the only record of them if the workspace is deleted).

### `[drop-path-head-of-line-and-repoint-ramp]` A synchronous DROP waits for an unrelated table's batch, and the per-part repoint grows 30-100x over an MSan shard (2026-09-15) {#drop-path-head-of-line-and-repoint-ramp}

Two findings from the T4 msan investigation (`docs/superpowers/cas/2026-09-16-msan-cas-s3-shard-budget-rca.md` §1.7, shard
`Stateless (amd_msan, cas s3 storage, parallel, 2/3)` of run 10, binary v26.6.4, no hot-key lane phase A). They
sit on top of `[PART-REMOVAL-REPOINT]` (`BACKLOG/gc.md`: elide the `delete_tmp_*` repoint; parallel removal via
`concurrent_part_removal_threshold_for_remote_disk=1`) and `[ref-catalog-cas-starvation]` (namespace removal on
`ref_catalog`, fixed by hot-key phase A), and neither of those two closes them.

**1. Head-of-line wait in the catalog drop task (upstream code, small portable fix).** With
`database_atomic_wait_for_drop_and_detach_synchronously = 1` (the stateless test users config) every `DROP TABLE`
blocks in `DatabaseCatalog: Waiting for table … to be finally dropped`. `dropTableDataTask`
(`src/Interpreters/DatabaseCatalog.cpp:1649`) takes the current batch, runs `dropTablesParallel`, waits for the
whole batch, and only then reschedules; a table enqueued while a batch runs waits for the batch's slowest drop
whatever `database_catalog_drop_table_concurrency` (256 in CI) allows. Measured: `tab_00718` enqueued 10:36:27,
reached `dropAllData` 10:42:29, exactly when the previous batch's `badFixedStringSort` (6 parts, 379 s) finished;
a `File` table with nothing to delete waited 5 min 27 s the same way. 13 of the shard's per-test timeouts had
`DROP TABLE` as the slowest statement (25-564 s) and none was stuck on a single query. The parallel-removal
threshold above shortens one table's drop; it does not stop the next table waiting for it. Fix shape: reschedule
the task while a batch is in flight (or drop per table from `enqueueDroppedTableCleanup` when `ignore_delay`), so
a synchronous drop only ever waits for its own table. Grade as an upstream patch: compact, motivated by the CAS
lane, useful outside it.

**2. The per-part repoint cost is not constant: 0.35 s early, 10-37 s late.** `[PART-REMOVAL-REPOINT]` measured
0.7-1.5 s per part as a flat cost. On the msan shard the drop-task thread's own timestamps give, per part removed
(`Removing N parts from filesystem (serially)` to the last `Repointed committed ref … delete_tmp_*`): hour 07
median 0.83 s, 08: 2.38 s, 09: 6.66 s, 10: 10.73 s (p90 23 s, max 37 s); a 7-part table took 3 min 5 s at 10:42.
What is known about where the time goes, and what is not:
- The ledger is one per pool (`CasPool.h:1245`, `CasRefLedger ref_ledger`) with one leader-flush queue
  (`appendRefOpsOnRuntime`); the periodic `system.stack_trace` samples of local runs 7 and 8 show removal threads
  waiting either in that queue (18 threads at once in one run-7 sample, `IMergeTreeDataPart::remove →
  moveDirectory → republishRef → precommitAdd → appendRefOps → appendRefOpsOnRuntime`) or as the leader inside
  the conditional PUT of the `_log` chunk / `_ckpt` (`commitRefChunk`, `publishCkpt` → `finalizeConditionalWrite`
  → `TaskTracker::waitAll`). Run 8 totals: `CASRefBatchFlushes` 262,468 for `CASRefBatchedMutations` 291,366,
  i.e. ~1.1 mutations per flush, the combiner almost never fires; `CASRefQueueWaitMicroseconds` 19,889 s.
- It is NOT raw PUT latency: in the CI log the sampled `WriteBufferFromS3` Create→Close for `_log` / `_ckpt` keys
  grows only from 22 ms to 81 ms median (p90 58 → 175 ms) between hours 07 and 10, max 12 s once. A 3.7x
  growth in PUT cannot make a 30-100x growth in repoint unless the queue in front of the PUT is deep or tail
  PUTs dominate. The CI log is silent inside the gap (no line between the `Removing N parts` and the first
  `Repointed`), and run 8's `query_log`/`cas_log` were empty, so the split between queue wait and PUT tail is
  unmeasured.
- Open: whether the ramp is the pool-wide serial lane saturating under the whole shard's ref traffic (inserts,
  removals, GC intake all through one queue at ~1 PUT per mutation), or the store's tail latency. This decides
  whether eliding the repoint halves the cost or removes it.

Next measurement (before any code): a run with `query_log`, `cas_log` and `system.stack_trace` sampling enabled on
a plain disk, then per part removal: `CASRefQueueWaitMicroseconds` of the removing thread vs the `_log` PUT
latency of the same flush, by hour. One number each answers the open question above. Then order the fixes:
(1) here, the catalog batch wait; (2) `[PART-REMOVAL-REPOINT]`'s elided repoint if the queue dominates, or the
store latency work of `{#gc-backlog-runaway}` / RustFS knobs if the tail dominates.

### `[hot-key-lane-phase-b]` Hot-key lane phase B: combining, GCS spacing, the hold clamp, the GC erase, `_ckpt` (2026-09-04) {#hot-key-lane-phase-b}

Designed to revision 26 of the hot-key lane spec (commit `26bde9f9604`, 13.5k words) and deferred from
the phase A landing (revision 27) by owner decision on 2026-09-04. Not started. Phase A fixed the seams
so that none of this reopens the callers: `submit`'s signature, `Decide = DecideOnObject`, the engine's
`WriteResult` returned unchanged, the caller's `Conflict` loop and pause rule, the queue of `Item`s with
the guard as the single remover, the hold as base, decide, write, settle. Each sub-item below has its
own gate; none is started on the documentation's say-so.

**0. Caller-side freeze in `dropNamespaceImpl` (do first, not phase B proper).** `cancelStalledCreating`'s
creator-fence read (`isCreatorFenceTerminal(cancel_op, ...)`, `CasRefLedger.cpp:5121`) runs under a
default `Retry::standard()` of its own while `resolveNamespaceLife` freezes once and passes its policy
down (`:1384`). Inside a hold that read can keep the key for up to its own 90 s after its caller's
window. Freeze once at `dropNamespaceImpl`'s entry and pass it to `cancelStalledCreating` and its read.
One-line class; makes the clamp (item 3) unnecessary for phase A.

**1. Combining.** Gate: the phase A acceptance run's `CASHotKeyQueueWaitMicroseconds` per submission
and `PUT` rate on `ref_catalog`; combining pays when the queue wait, not the write, dominates a
submission at p90. The design, with the rules twenty-two rounds established (each was lost once in a
rewrite and found again as a critical, so they are listed): the holder takes the items queued behind it
that were submitted on the same `CasRequests` (the leader's per-attempt fence gate then covers every
member: same atomics, and a re-arm between two admissions is caught by whichever generation is older),
are not under `single_attempt` (leader or member; `Retry::once` permits one attempt and a batch may
reissue), carry no `Liveness` closure (the engine consults one before every physical attempt and the
batch's write is gated on the leader alone; such an item holds alone), and whose bound deadline is not
earlier than the leader's; allocation (`BatchOutcome`, the members vector) before taking, `taken` set in
the same critical section, so a failed allocation takes nothing; "a leader takes this item" and "this
item's caller leaves" decided under one mutex against `taken`, exclusive (the use-after-free the flag
prevents was found by opus round 17). Chain: each member's `decide` on `Object{bytes = candidate, etag =
base etag}` (the etag is a placeholder no `decide` may read as an identity of the bytes), after the
member's own `gate(reservedFor(0, 2))` (`FenceLost`/`NoBudget` written as its `GaveUp` with `sent_any =
false`, `decide` skipped); a member's exception is held; a decline stops the chain so the landed object is
exactly its input; a taken item the chain did not reach is `Conflict{NotObserved}`. Settle: `Committed`
gives each contributor a three-way `gate(0)` as `postCommit` does (`FenceLost` → `GaveUp{FenceLost,
sent_any = true}`; `NoBudget` → `GaveUp{Deadline, Lease, sent_any = true}`; else `Committed{combined
etag, attempts_sent = 0}`), delivers held exceptions, and gives the declined member
`Declined{landed object}`; `Conflict` → every member `Conflict{seen, 0, any_ambiguous}`; `Refused` → the
same `Refused` to every member; `GaveUp` with `sent_any = false` → members `Conflict{NotObserved}`;
`GaveUp` with `sent_any = true` or an exception → members `GaveUp{Unresolved, sent_any = true}` with
their own `Source`. Held verdicts are delivered only from a landed batch (as-if-serial: a batch that does
not land is where serial execution would have landed a prefix). The leader's guard settles unsettled
taken members the same way, settle and erase in one critical section. The as-if-serial argument covers
the lane's key alone; a `decide`'s other reads must not observe a key a batch member may write. A
member's `decide` runs on the leader's thread while its caller is parked (disjoint parts of the member's
operation; the caller's step 1 touches `admitted_generation`, the fence and clock closures, the bound).
`written` is a prefix state the store never held once later members followed (only test callers read
it). Members carry `attempts_sent = 0`; the pool's attempt counters are the leader's. A member's reads
inside the hold need the clamp (item 3). Invariants HK4, HK7, HK8 and tests 1 to 4, 12 and 16 of
revision 26.

**2. `Generation` spacing.** Gate: 429/`SlowDown` count on the catalog key over the parallel stateless
suite on the `gcs` lane after phase A; the owner's position is "GCS will say if we are too fast". The
design: `write_spacing_ms` as a constructor argument (`Pool` passes 1000 on the `Generation` dialect, 0
elsewhere); `Lane::last_write_end_ms` optional, set by a no-throw guard around the engine's write on
return and unwind (the lane exists then, the leader's item is in its queue, nothing allocates); before
the write, `op.pause` the smaller of the remainder and the leader's remaining window, and let the verb's
own entry gate decide (a pause that consumed the window ends `GaveUp{Deadline, sent_any = false}`);
erasure becomes "erase a lane whose queue is empty and whose last write is older than the interval",
swept at enter and leave, with the residue after the pool's last activity bounded by the write rate.
INV-HK9 and tests 17, 18 of revision 26. Ten rounds of findings went into where this lives; above the
engine it is one place, inside `readModifyWrite` it was three call sites and two result families.

**3. The hold clamp.** One optional clamp deadline on `CasOperation`, honoured as a minimum at its
twelve `policy.bind` sites, set by the lane for the hold and by a leader for a member's `decide`,
cleared after; a read the clamp refuses gives up as at its own policy deadline, reported with
`GaveUp::Source::Policy`. Required by combining (a member's nested read runs under the leader's hold);
for phase A, item 0 is the caller-side answer.

**4. The GC erase into the lane.** `deleteCompletedRemovingAtSnapshot`'s body from "refresh authority"
through the `replace` becomes one `submit` whose `decide` refreshes authority, checks `op.admitted()`,
`throwIfAmbiguous` and the exact row, and returns the erased candidate; the mandatory resolution read
after `Committed` stays authoritative for the reconciler's next selection. Four prerequisites: the
refresh reads `gc/state` through the erase's own operation, not a private one, so the clamp reaches it;
the `decide` keeps `throwIfAmbiguous` and the absent-catalog refusal; `CompletedRemovingDeleteResult`
needs the catalog cut the call ends on for `FencedOut` and `EntryChanged`, which a `decide` exception
carries nowhere, so the erase captures the base it was shown or re-reads; the double refresh a
cached-base re-run causes is one extra `gc/state` `GET` inside the hold. Its operation carries a
`Liveness` closure (the cached `authority_held`), so it holds alone and never combines. The authority
gap across the engine's internal reissue (a TTL on the GC lease) stays its own item. Until then the
erase races the lane as it races everything today and costs the lane one stale cache entry per erase.

**5. `_ckpt` and `gc/state` through the lane.** One-call change per site (`readModifyWrite` to `submit`
in a `Conflict` loop with the pause rule). Gate: the audit of the `decide` contract's condition 1 for
`publishCkptContribution`, whose `decide` counts its runs and records its decline's reason in captured
locals that the caller's outcome reads, so a re-run on a fresh base doubles what it reports; move both
out of the closure first. Operations that carry a `Liveness` closure will not combine (item 1) but do
serialize and use the cache. Half 2's go/no-go for `_ckpt` left on `readModifyWrite`: 40·M requests per
second per contended key for M writers, and 429/`SlowDown` not above today's.

**How the design got here, for whoever picks this up.** Twenty-six revisions in one day; findings per
review round never fell below eight; the document tripled while the core (FIFO ticket, `taken`
handshake, cache as a hint the `PUT` validates, engine result unchanged) was judged sound from revision
18 on. About 60% of the MAJOR+ findings after revision 21 were specific to combining, 20% to spacing and
erasure. Rules for the next round of this work: a review revision only removes or tightens, never adds a
feature; a new feature goes in its own revision and costs at least one round; a reviewer verifies
against the checklist above and names the failing scenario; prose and pseudocode slips are MINOR; after
three rounds the remaining questions are answered by code and tests, not by another revision.

### `[cas-s3-lane-put-timeout-logged-at-error]` A stalled single-attempt PUT is logged at Error and fails a stateless test whose write succeeded (2026-09-04) {#cas-s3-lane-put-timeout-logged-at-error}

Seen on the local CA-s3 stateless lane (run 2 after the 404-scope fix): `<Error> WriteBufferFromS3:
S3Exception … Timeout … key cas_s3/cas/ref_catalog` reaches the client's stderr and fails the test
although stdout is right. Root cause (`docs/superpowers/cas/2026-09-04-put-timeout-error-log-rca.md`):
the request engine issues writes through the single-attempt client with `attempt_timeout_ms` = 5000
(`CasRequestBudget` default) and `WriteBufferFromS3` bounded to one attempt, so a stall past 5 s throws
once and `WriteBufferFromS3.cpp` logs it at Error once; the engine then classifies the timeout as
ambiguous, resolves by a read and reissues to the deadline — the write lands, the log line stays. This
predates the request-contract migration (same client, timeout and log site before it); it surfaces
locally under the full parallel suite hammering one S3 endpoint.

Recommended fix (not applied — it touches the upstream slice `src/IO/WriteBufferFromS3.cpp`, consult
first): a thread-local log-severity scope modelled on `Expect404ResponseScope`, opened by the request
engine's write loop around its single-attempt writes, that downgrades only the timeout/network-stall
class at that one `LOG_ERROR` site to Info while still counting `WriteBufferFromS3RequestsErrors`; an
ordinary non-CAS timeout keeps logging at Error. The engine's ambiguity handling is untouched by design.
Until then the class is attributable on the lane by its exact line and key.

### From the engine fix-round review (2026-09-04) {#engine-fix-round-review-2026-09-04}

- **Spec drift.** `docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md` (revision 13)
  still prescribes `op.publish(…, Retry::once())` and "never the shared `standard`" for
  `ensureBlobPresent`; since the loop-deadline fix the publish runs under the loop's frozen policy made
  single-attempt (one physical attempt, bounded by the loop's one deadline). Reword the spec sentence. Owner: the spec's next revision (revision 14), one editing pass for all three recorded drifts (this one, `{#spec-drift-ref-lane-once}`, and the `isAccessTokenExpiredError` sentence under `{#codex-prod-review-2026-09-03-residue}`).
- **`CasRefLedger::resolveNamespaceLife` is a third hand-written loop of the forbidden shape:** 32
  iterations, a fresh `Retry::standard()` window per verb, no pacing — the shape the spec's inventory
  rule forbids ("a hand-written loop captures one `Retry` before it starts, shares it across every call
  it makes"). Pre-existing, outside the fix round's brief. Placement: the same freeze-at-entry treatment
  as `deleteCompletedRemovingAtSnapshot` / `ensureBlobPresent` (commit 1effe101617), with a red-first
  test that a perpetual conflict ends within one window; owner = the final fix round of the request-contract plan (2026-09-04), which freezes one policy at entry and paces the re-read arms.

### Spec drift: the ref-lane inventory row says `standard`, the coverage-gate paragraph and the code say `once` (2026-09-04) {#spec-drift-ref-lane-once}

`docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md` (revision 13) lists "the ref lane
(`commitRefChunk`, the recovery walk, `resolveWedgeOnce`) — `create` under `standard`" in the inventory
table, while its coverage-gate paragraph states that "the `once` writes of the pulse and the wedge retry
are never a key's first request" and that the recovery walk's epoch seal at `T+1` is a `once` write.
The implementation follows the paragraph (`resolveWedgeOnce` and the recovery seal `create` under
`Retry::once`; the lane's own next flush is the retry), as chosen in the migration's ref-ledger unit and
approved by its review. Fix: reword the inventory row to say `commitRefChunk` under `standard`, the wedge
retry and the epoch seal under `once` with the reason. Found by the external test review (tests-02 #7/#8).

### Soak 2026-09-04 (phase 3, 30 min, seed 20260904, binary 6ddaefbcc9e) — return items {#soak-2026-09-04-return-items}

Verdict PASS (`SOAK_EXIT=0`, both checkpoints `dangling=0 stale_edge=0 dryrun_count=0`, `GaveUp` = 0 on both
nodes; ch1 self-fenced and remounted cleanly under the one `freeze_long` fault). Three items survive the
analysis (full evidence in the plan workspace's soak report at the time; the run row is appended to
`utils/ca-soak/scenarios/RUN_HISTORY.md`):

- **GC round cost at ~825k objects.** The `gc_checkpoint` stage's GC on ch1 cost 1934.9 s cumulative over
  50 rounds, single rounds up to 179.8 s in `manifest_deletes`; the driver's pool-drain wait blocked on it,
  so the "30-minute" run took 2590 s wall-clock (+44%). Profile `CasPool`'s `manifest_deletes` and
  `fold_ref_intake` phases at that object count before trusting any duration-budgeted phase-3 gate.
- **`B152/B185` warning text is wrong for this occurrence.** `wait_for_pool_consistent` in
  `utils/ca-soak/soak/run.py` reports "did not HOLD dangling==0 … after a fault window", but the flap
  happened in the routine `gc_checkpoint` stage before any chaos fault. Broaden or correct the message so
  a real future finding is not dismissed as the known post-restart flap.
- **Pool drain probe `None` under load.** One `pool_bytes=None` sample at 23:26:26Z inside the GC-drain
  window; plausibly a `docker exec`/`du` subprocess timeout under host I/O contention, unprovable from
  RustFS logs (known observability gap). `soak/pool.py` `pool_size` should log subprocess-timeout and
  empty-stdout as distinct causes.

Also noted, no item: `_ckpt checkpoint could not be advanced … persistent CAS contention` errors in the
steady/mutations stages traced to RustFS `PUT` timeouts on the same objects moments earlier, all
self-healed; the dominant `AWSClient 404` error-log volume is the expected `HEAD`-miss dedup probing.

### Codex production review (2026-09-03, adjudicated) — residue {#codex-prod-review-2026-09-03-residue}

Adjudication of the external reviewer's 10 findings against spec revision 13: 3 confirmed defects (fixed in
the plan's fix round: read classification narrowed to authoritative absence; one absolute bound per
hand-written loop in `deleteCompletedRemovingAtSnapshot` and `ensureBlobPresent`; `claimMount`'s raced
branches return the observed body), 3 design-accepted, 2 rejected, 2 prose. What remains here:

- **Spec drift.** The spec's retry section names `S3Exception::isAccessTokenExpiredError` as the
  refreshability predicate; the landed `isRefreshableCredentialError` is deliberately narrower (named codes
  only, never `S3Errors::UNKNOWN`), because the general predicate would turn every unmodelled store answer
  into a refusal. The spec sentence is stale; the ruling is recorded in
  `docs/superpowers/cas/2026-09-03-request-contract-rulings.md`. Fix: reword the spec sentence to name the
  CAS-local predicate and its reason.
- **Accepted request costs, recorded so nobody re-argues them without new evidence:** one extra `GET` per
  conflicting iteration of the catalog erase loop (the post-write read is also what convicts a false
  `Committed`; `Conflict::seen` may be `NotObserved`); one extra `GET` on the pool-meta union path and on its
  lost-create path (the steady-state open stays one `GET`); one extra `GET` on the lost marker-create path of
  blob publication.

Append new items here — quick adds and concurrent-agent findings land in this section, unformatted
is fine. They get triaged into the topic files above during the next grooming pass. Do not delete
from here without triaging; do not hand-sort into a topic file without checking the item's anchor
isn't referenced elsewhere first.

### Resolved 2026-09-03 — docs pass (Task 25) {#inbox-resolved-2026-09-03-docs}
Closed by the request-contract spec revision 13 and the operator-docs update
(`docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md`, `docs/en/antalya/cas/operations/{debugging,monitoring,troubleshooting}.md`, `docs/en/operations/system-tables/cas_log.md`); the rulings behind each are in
[the rulings doc](2026-09-03-request-contract-rulings.md). Source-comment items with the same substance stay in the sections below — that pass runs after the build.
- Operator note: `system.cas_log`'s `token` column now renders `<dialect>:<value>` uniformly across every event type (verified against the landed code, not only `BlobReuseAdopt`/`ManifestPut`); documented in `cas_log.md`.
- SPEC DRIFT: `CasFsck`'s retirement check only classifies, never removes; `CasOperation::stream` takes no size parameter; `allocateWriterEpoch`'s non-convergence at the deadline is retry-later via the shared `orThrow`, not its own `CORRUPTED_DATA` switch.
- SPEC DRIFT: `GaveUp` (and `Conflict`/`Refused`) now carry `attempts_sent`, `Declined` does not; `CasOperation::pause`/`CasRequests::pause` are in the spec; the `ensureBlobPresent` row states `publish` under `once` with a fresh envelope per attempt and create-first `reconcileMetaClean`; the generation-capture scope (one `txn_generation` at transaction start, every upload task via `resume`) is stated.
- Operator note: the mount renewal audit event's old→new key mapping (`retrying`/`unresolved_reason`/`deadline_source`/`stop_cause` → one `classification` value + `attempts_sent`) is documented, with the full `classification` value list.
- Engine note: `isDefinitelyRefusedWrite`'s `#if USE_AWS_S3` scoping (so `Refused` is unreachable on a non-S3 build) and `Declined` carrying no `attempts_sent` are both stated in the spec.
- SPEC DRIFT (engine fix round): the `Why::Unresolved` `last_seen` enumeration now includes the precondition-unchanged `Object`/`Meta` case; the resolve order for `replace` (unchanged precondition first, byte equality only once it has moved) is corrected; ambiguity is stated as scoped to one inner write of a `readModifyWrite`, with the other `WriteState` fields call-wide.
- CP4′ fixer calibration note: the engine reserves `Backend::attemptTimeoutMs()` per attempt, not `CasRequestBudget::attempt_timeout_ms`; stated in the spec's reservation paragraph (the `~Pool` destructor policy this bullet also carried is an architecture note, not a docs claim, and is recorded as-is in the rulings doc).

### Resolved 2026-09-03 — source-comment pass (Task 25) {#inbox-resolved-2026-09-03-source-comments}
Closed by the code-comment half of Task 25 (`26b9ba8a495`). Each item was verified against the current
code before being touched; comments now state the reason and drop plan/review provenance.
- `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/README.md`'s `Backend/` bullet named the
  deleted `CasRequestControl` and the deleted token-aware verbs; rewritten to name `CasBackend`'s
  transport primitives and `CasRequests`.
- `Backend/CasBackend.h`'s `ProbeOutcome` comment claimed ordinary `head`/`read` flatten container,
  permission and transport faults into absence; `isObjectNotFound` only flattens an authoritative key
  miss, so the comment is corrected to say every other fault propagates as an exception.
- `Backend/CasBackend.h`'s `checkConditionalWriteSingleAttemptSupport` comment said the capability probe
  checks it; `Pool::open` asks it directly through `backendForCapabilityPredicates()` before the probe
  operation is admitted — corrected (also closes U1-review.md's Prose P7).
- `src/Disks/tests/gtest_cas_pool.cpp`'s `WriteCountingBackend`/`ProbeWatchingBackend` instrumentation
  comments: already correct, no longer cite `probeSentinelRaw` — checked, no change.
- `src/Disks/tests/gtest_cas_forget.cpp`'s `ForgetRacingActiveRemountThreadCompletesBounded` mechanism
  comment: already correct — `ToggleableTransportFaultBackend`'s fault already sits on the `head`/`read`/
  `list` primitives now that U1 has landed, so "keeps every attempt at `StayTransient`" holds. Checked,
  no change.
- `Pool/CasRefProtocol.cpp`'s `crossEpochFromSeal` doc comment (the checkpoint-snapshot reader) still
  cited "review NEW-3" / "review C3" after its header twin (`CasRefProtocol.h`) was already cleaned — the
  citations are dropped, the reason kept.
- U1-review.md's Prose: P3 (`CasPool.h`'s `farewellRequests()` doc described only the lease-release half
  of a plane the mount-lease renewer's claim/adopt also shares) and P4
  (`ContentAddressedTransaction.cpp`'s staging write buffer comment claimed a live operation ties two
  generation reads together, when `admit().generation()` is a plain value off a temporary) are fixed; P7
  is the `checkConditionalWriteSingleAttemptSupport` fix above. P6 (`CasRequestBudget.cpp`'s logger tag)
  already reads `"CasRequestBudget"`, checked, no change. P8 names only a workspace report file, not
  applicable here.
- U10-review.md's P1–P6: none names a source comment worth changing — P1/P3/P4/P6 are about the U10 task
  report or an already-correct call site, P2 is already fixed (`CasMountRuntime.cpp` derives the renewal
  counters on every outcome, not only `Committed`), P5 is a ruling-doc item. P7 remains open below.
- U9-rereview.md's NEW-2: `gtest_cas_part_write.cpp`'s `HeadThenDeleteOnceBackend` inline comment still
  claimed the HEAD-to-GET window its class doc had already dropped; fixed. NEW-3 remains open below.
- U6-rereview.md's F2: `CatalogLifecycleReconciler::reconcile`'s header doc is already correct — the
  refresh runs unconditionally before the drain-complete verdict, including when there is no eligible
  row to erase. Checked, no change. F4: the ten internal-document-reference sites in
  `Pool/CasRefCatalog.h`, and the matching sites in `gtest_cas_ref_catalog_birth_wiring.cpp` and
  `gtest_cas_ref_ckpt.cpp`, are cleaned (kept the reason, dropped the `Task N` / review citations).
  `Pool/CasRefCatalog.cpp`'s two sites remain open below.

### From the CP3 (Task 7) review, 2026-09-03 — prose only {#inbox-cp3-review-prose}
- `task-7-report.md` (workspace only): "no caller reaches them yet" — legacy write verbs reach `write`/`remove` through the forwarders; the accurate claim is that no assertion needs a per-key write counter.
- RESOLVED 2026-09-04 (final review): the `PLANES` doc's `Liveness` IS passed — `CasMountRuntime::startRenewer` gives `[this]{ return !renewalCancelled(); }`; the note below was stale.
  - (was) U10 review prose batch (units/U10-review.md P7): the `PLANES` doc in `Pool/CasServerRoot.h` promising a `Liveness` no production caller passes — `CasMountRuntime::installRenewer` (renamed from `installKeeper`) supplies none either. `CasServerRoot.h` is a forbidden file for the source-comment pass; left for the engine-defect pass.
- U9 prose (units/U9-rereview.md NEW-3): `Pool/CasPartWriteTxn.cpp`'s `reconcileMetaClean` create-first gate comment over-states what an absent observation implies (an absent body does not imply an absent marker). Forbidden file for the source-comment pass; left for the engine-defect pass.
- RESOLVED: the callerless `stagingPutIfAbsentMutable`/`stagingConditionalOverwrite` LOCK TASK item above is stale — `grep -rn` over `src/` and `programs/` finds no such symbol anywhere in the tree; the deletion already happened.
- U6 re-review prose (units/U6-rereview.md F4, `Pool/CasRefCatalog.cpp`): its two internal-document-reference sites (the header and test-file sweep did not reach `.cpp`, a forbidden file for the source-comment pass); left for the engine-defect pass.
- Engine test seam (if the checkpoint shows jitter-dependent reds): `Retry::backoff` draws full jitter with no test seam, so a test driving an ambiguity under `untilLeaseSafe` with a short remaining lease is a coin flip; a `setBackoffFnForTest` on `CasRequests` (used by `pauseAndReissue`) would make it deterministic. `MountLeaseRenewer::renewOn`'s (renamed from `MountLeaseKeeper`) `catch (...)` arm reports `attempts_sent = 0` — an exception that escaped the engine carries no count; document at the field. `Pool/CasServerRoot.h` is a forbidden file for the source-comment pass; left for the engine-defect pass.
- Product question from the lock (2026-09-03): `CAS_WRITE_UNATTRIBUTED` is unreachable on the Native/S3 write path now — its only throw site was the deleted legacy minter, and the request engine settles a 2xx whose value fits no grammar by a resolve read (`GaveUp{Unresolved}` at the deadline). Decide whether a distinct unattributed-write signal is wanted (an event/counter) or the error code is retired; the three tests that pinned the throw now pin the give-up. (Spec revision 13 records this as an open product question rather than deciding it — see [the rulings doc](2026-09-03-request-contract-rulings.md).)

### From the engine fix round review, 2026-09-03 — prose and spec drift {#inbox-engine-fix-prose}
- `Backend/CasRequests.h`, doc on `isDefinitelyRefusedWrite`: still says the engine refuses when "there is no reissue left to sign with what it did install" — under `once` no refresh is invoked any more, so that disjunct is unreachable; drop it. (FALSE)
- `Backend/CasRequests.h`, `WriteState::any_ambiguous` comment: "an inner write that ended in `Conflict` saw the precondition move" is false of the `!any_ambiguous` arm, which returns `Conflict{NotObserved}` having proved nothing — scope the claim to the ambiguous arm.
- `Backend/CasRequests.cpp`, `writeLoop` reset comment: "sent DIFFERENT bytes" is not guaranteed (`decide` may repeat bytes); the proof is the observed precondition, not the byte difference.
- `Backend/CasRequests.h`, `admit`/`resume` thread-safety comment: the conclusion is right, the enumeration is not (neither reads the backend; `resume` reads no member).


