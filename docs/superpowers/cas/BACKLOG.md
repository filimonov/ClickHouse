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


## `[chunked-flush-publisher-gate-flake-aborts-gate]` `CASRefWriterChunkedFlush.SnapshotPublisherLatchedAcrossChunks` flakes on a circular wait and, because the assert leaves two joinable threads, terminates the whole `unit_tests_dbms` binary {#chunked-flush-publisher-gate-flake-aborts-gate}

Known since 2026-08-25 (memory only until now; recurred 2026-09-03 on the request-contract gate and
2026-09-04 during the single-attempt log-level task's combined run; on this host the discriminator
line appears in a noticeable fraction of full `CAS*` gate logs, e.g. `build/crit6_gate.log` 2026-08-30
after 1505 green tests). Two independent defects:

1. **Test-side circular wait** (`src/Disks/tests/gtest_cas_ref_chunked_flush.cpp:1017`). The carve
   hook parks the LEADER append thread in `ChunkFaultBackend::awaitBlockEntered` at the chunk-2
   boundary (`CarvePhaseForTest::ChunkReseed`), waiting for chunk 1's snapshot publisher to reach its
   armed `_snap/` PUT. The publisher, however, refuses to publish while `rt->lane_state != Ready`
   (`Pool/CasRefLedger.cpp:4491`, `refusing snapshot publication while the append lane is not Ready
   (state 1)`), and the lane is `Writing` exactly because that leader is mid-chunk. Whether the
   publisher captured its candidate before or after the lane went `Writing` is a race the test does
   not control; when it loses, the only exits are the 20 s budget inside `awaitBlockEntered` and the
   test's own `ASSERT_EQ(a.fut.wait_for(20s), ready)` at `:1048`, so the outer wait fails.
   Discriminator: the refusal line above appears in failing runs only.
2. **Failure mode kills the run**: the `ASSERT_EQ` returns from `TestBody` with `AppendCaller a`/`b`
   still holding joinable `std::thread`s; `std::thread::~thread` calls `std::terminate`
   (`libc++abi: terminating`), so one flake discards every test after it (1458 of 2170 in the first
   sighting). In CI this is a red unit-test job unrelated to the PR under test.

Fix order: (2) first and mechanically, so a flake costs one test, not the gate: join or detach the
callers before any `ASSERT`, e.g. an RAII joiner on `AppendCaller` or `EXPECT_` plus explicit joins.
Then (1): make the hook order deterministic, either by gating the carve hook on the publisher having
captured its candidate (the `snapshot_after_capture_hook_for_test` seam already exists at
`CasRefLedger.cpp:4497`) or by arming the block only after that capture; the assertion the test
proves (the dropped chunk-2 trigger re-fires on settlement) does not depend on which thread parks
first. Verify with 20 isolated repeats and two full `CAS*` gates with zero refusal lines.

## `[single-attempt-client-status-error-log-site]` A single-attempt client's HTTP-status failure (429, 409, 5xx) is still logged at Error by `PocoHTTPClient`; on GCS that is 170 lines per node per 8-minute smoke {#single-attempt-client-status-error-log-site}

Follow-up of `docs/superpowers/cas/2026-09-04-single-attempt-client-log-level-proposal.md`, which
demoted the thrown-exception path (`Client.cpp` network error, `WriteBufferFromS3` S3Exception) and
declared the status-code path out of scope until a lane showed it. The GCS smoke of 2026-09-05 showed
it at once: every 429 on `_ckpt` (see `BACKLOG/gcs.md`, failure class 2) is written as
`<Error> AWSClient: Response status: 429, Too Many Requests` at `src/IO/S3/PocoHTTPClient.cpp:740`,
the `else if (HTTP_NOT_FOUND != status_code || !Expect404ResponseScope::is404Expected())` branch,
while the engine resolves and reissues it. On AWS the same hot key produces 409
`ConditionalRequestConflict`, logged by the same line.

**Sizing (2026-09-05): the same size as the first two sites, about an hour with the run.**
`PocoHTTPClient` is built from `PocoHTTPClientConfiguration` (constructor at `PocoHTTPClient.cpp:222`),
which derives from `Aws::Client::ClientConfiguration` and therefore already carries `retryStrategy`;
the signal is in hand at construction and simply not copied.

1. A `bool single_attempt` member, filled in that constructor next to `s3_use_adaptive_timeouts` by
   `dynamic_cast<const SingleAttemptRetryStrategy *>(client_configuration.retryStrategy.get())`. The
   second constructor (from a bare `Aws::Client::ClientConfiguration`, `:240`) leaves it `false`.
2. In the log branch: `single_attempt` and a retryable status (429, 409, 5xx) → `LOG_DEBUG`; otherwise
   as today. Other 4xx on a single-attempt client stay Error, they are genuine request errors.

About ten lines of product code. Tests: the `PocoHTTPClient` gtests already drive `TestPocoHTTPServer`
with a chosen status, so the two cells (single-attempt → Debug, ordinary → Error) on a 429 use the
same log capture as the two earlier sites. Risk as before: the line's level only; retries and the
outcome are untouched. Verification: those gtests plus a GCS smoke with zero `Response status: 429`
at Error and the same 429 count at Debug.

**409 `ConditionalRequestConflict`, two parts; the second matters more and is NOT this item.**
- Log: 409 joins the retryable list above, so `Response status: 409` goes to Debug for the
  single-attempt client; the `WriteBufferFromS3: S3Exception name ConditionalRequestConflict` line is
  Debug already (08c2a2ec25e). Do this together with the site.
- Classification: the engine does not know the name, treats it as "outcome unknown" and spends a
  resolve read before reissuing. Safe but imprecise: AWS states the attempt did not apply ("conflicting
  operation in flight, retry"), so the outcome IS known. The right step is a predicate
  `isConditionalRequestConflictError` next to `isPreconditionFailedError` in `src/IO/S3` (by exception
  name, the SDK does not model it) and a "conflict in flight" class in `CasRequests::writeLoop`:
  reissue with backoff, no resolve read, and a log line that says what it is rather than reading as a
  refusal. That is a branch in the engine, so by the engine rule: a short spec paragraph, opus review,
  a gtest on `CasInMemoryBackend` injecting the name. Half a day, not an hour. Placement: under A1 in
  `BACKLOG/gcs.md` (`[gcs-hot-control-keys-429]`): the `_ckpt` conflicts disappear with the rewrite
  rate, and until then the engine copes.

## `[emulated-resurrect-should-spill-to-disk]` Emulated `publishBlob` should spill before atomic install {#emulated-resurrect-spill-to-disk}

**REFRAMED 2026-08-23; identifier and history preserved.** The separate resurrection API was deleted.
The remaining debt is `ObjectStorageBackend::publishBlob` in `EmulatedSingleProcess` mode: it
materializes the complete `[header][payload]` body before installing it. This retains complete-object
visibility and bounds concurrency, but peak memory is still the largest single blob even though the
backing store is a local disk.

Atomic install itself is already implemented by `emuPublishBlobAtomically`: the fully materialized
body is written to a sibling temporary object and renamed into place under `emu_mutex`. The separate
remaining follow-up is to stream directly to that temporary file (or the pool scratch path), validate
the declared size, and retain airtight cleanup on success, size mismatch, and mid-stream exception.
This is emulated-mode peak-memory work only; native S3/GCS `publishBlob` already streams or uses
native copy and is not affected.

## CAS-021 (issue #2207) adjudication follow-ups: controller-outcome honesty + condemn-memo staleness (2026-08-20) {#cas-021-followups}

Adjudication of https://github.com/Altinity/ClickHouse/issues/2207 (two read-only code sweeps against
HEAD `684161dcc03`; updated for the 2026-08-23 rewrite): the controller-outcome observation remains
relevant to mutable blob metadata, but the old conditional body-displacement branch was deleted. The
claimed integrity consequence remains neutralized — the delete path is guarded by the normative
delete-site in-degree re-read, exact-token deletion cannot remove a fresh retagged publication, and
the writer checks its admitted fence generation before publication;
the equality-resolved meta etag is consumed by NOBODY (`writeCondemnedMeta` reads only `.outcome`);
the ref-log lane adjudicates authorship by byte equality over a payload that carries txn identity
(`classifyRefLogOccupant`, `CasRefLedger.cpp:2240`); a false-`Occupied` → mount-fault path does not
exist. GC's gate predicate is "durable Condemned evidence exists" — which the equality-resolve GET
literally proves — same as the already-Condemned arm at `CasGc.cpp:137`.

Follow-ups, in recommended packaging:

- (1) **Honesty patch over the request controller** (one coherent change, NO durable-op/wire/behavior
  change): split the equality-resolved outcome out of `Committed` (e.g. `IntendedStateDurable`), stop
  returning the observed occupant token on that arm (it claims authorship no caller has; today unused
  — make that structural); rename `slotOccupy`'s misleading `NotUnresolved` label; add the
  "trust model" doc-block at the resolution ladder (what equality-resolve proves / does not prove,
  pointers to the three system invariants that make it safe) and the ownership-decidability table by
  key class (immutable content-addressed / mutable identity-in-payload / mutable identity-free /
  owner-anchor `claimOwnerOrThrow`); cross-reference sentences at `writeCondemnedMeta` ("a foreign
  `Condemned` marker satisfies the predicate by design") and `reconcileMetaClean` ("an
  equality-resolved desired `Clean` record is already durable");
  rename the pin tests to read as spec. ~150-250 line diff + test renames; controller = adversarial
  review mandatory. This addresses the CORE of CAS-021 at the type level: the external auditor's
  reading becomes impossible to write.
- (2) **Stale condemn-marker memoization — ACCEPTED RESIDUAL, do NOT fix with re-reads** (user
  decision 2026-08-20): the in-process `condemn_markers_confirmed` note survives a legitimate
  `Condemned -> Clean` transition (no `forgetCondemnMarker` on writer replacement without an intervening fold),
  so `confirm_condemned_marker` (`CasGc.cpp:1885`) can graduate an entry whose durable meta says
  Clean. Consequence when it fires (ultra-rare race): ONE spurious `deleteExact` — an S3 DELETE,
  which is FREE — self-healing at `CasGc.cpp:862-870` (TokenMismatch drops the confirmation, meta
  untouched). The re-read fix would cost +1 BILLABLE GET per graduating condemned entry on the
  COMMON path (P9 GET-budget class) to save free DELETEs in a rare race — worse than the disease.
  No zero-cost invalidation exists either (the window is by definition "nothing observed the fresh
  replacement"). Only sanctioned improvement: the observability LABEL at the self-heal site (counter/
  log as "spared by token rotation", not an anomaly) — zero extra requests; fold into (1) if done.
- (3) One trust-model paragraph for conditional writes in the numbered doc set
  (`03-writer-protocol.md`) — documentation only.

Issue response drafted (2026-08-20); post/adaptation is the user's call.

## Issue #2233 adjudication residue: soak-harness observability + Poco shared-pool risk (2026-08-20) {#issue-2233-followups}

Adjudication of https://github.com/Altinity/ClickHouse/issues/2233 ("replica HTTP dies on green-path soak
after relink NETWORK_ERROR storm"): the refusal storm is the known, designed
`{#relink-confirm-busy-lane}` behavior (all four remediations there still open — the per-ref rule-3
refinement is the availability fix); the claimed causality "storm -> HTTP death" is contradicted by our
own artifacts (a 90-minute phase-3 soak absorbed 112,598 refusals — peak 9,219/min — and ended
`PHASE3 OK` with both replicas alive; the reporter saw ~278 total), and no fd/socket/thread leak exists
on the abandoned-relink path (drain-then-throw + `SCOPE_EXIT` verified). Prime suspects for the
reporter's observation: VM-level OOM (28g `mem_limit` x2 on a 16 GiB Docker Desktop VM; their upstream-
compose symptom was "Connection refused after the peer exits") and the Poco shared-`server_pool`
silent-refusal upstream bug (below). Items:

- (1) **ca-soak compose: `ch2` has NO healthcheck** (`docker-compose.yml` — only `ch1` has the HTTP
  `/ping` probe, added for capability-probe serialization). "Container healthy while HTTP dead" on ch2
  is therefore vacuous. Add the same healthcheck to ch2. Trivial.
- (2) **soak driver: `TRANSPORT FAILURE` is an `else`-branch catch-all** (`soak/run.py:2020-2026`) that
  names a subsystem it never diagnosed — the same triage-misdirection failure mode #2219 complains
  about, one layer up. Phase 1 additionally does exactly one attempt (`transport_resilient=False`) and
  checkpoints do not gate on HTTP health (phase-2-only wait), so any transient `OSError` becomes the
  issue's exact headline. Split the label (name the errno/op) and consider a phase-1 HTTP-health gate
  at checkpoints. Small.
- (3) **Poco shared-`server_pool` silent connection refusal — assess exposure** (upstream bug, comment
  in `base/poco/Net/src/TCPServerDispatcher.cpp:154-180`): one `Poco::ThreadPool` capped at
  `max_connections` is shared by 8123/HTTPS/native/9009; `_currentThreads` is per-dispatcher, so
  saturation by long-lived interserver byte fetches can make the 8123 dispatcher drop accepted sockets
  with NO ClickHouse-level error (client sees RST; at most a Poco `Warning`). Relink-storm second-order
  effect: refusals suppress zero-byte relinks and FORCE long byte fetches, i.e. the storm converts
  cheap transfers into thread-holding ones. Candidate observability first: expose refused-connection
  counts / alarm on pool saturation before considering upstream surgery (upstream file = consult-first).
- (4) **Confirm-path observability gaps** (feeds `{#relink-confirm-busy-lane}` items (b)/(c)): no
  ProfileEvents pair for proven/refused confirms; refusal reason (which rule) logged only at Debug on
  both sides; the receiver collapses refusal vs transport failure vs timeout into one message — the
  reporter's logs could not distinguish them even in principle. This adjudication would have taken
  minutes with (b)+(c) implemented.

## Issue #2243 CONFIRMED: local port exhaustion fences out the mount lease (2026-08-20) {#issue-2243-port-exhaustion-lease}

https://github.com/Altinity/ClickHouse/issues/2243 — read-heavy concurrent `SELECT FINAL` on a CAS
default-policy disk exhausts the container's ephemeral ports (errno 99 `EADDRNOTAVAIL`), which kills
the mount-lease renewal → fence → `TransientNotLive` → ~42 s full-disk refusal → self-remount discards
in-flight `PartWriteTxn`s. CONFIRMED REAL (adjudicated from code; mechanism matches our own S07 finding
from 2026-07-06). The rev.8 fail-close chain itself worked exactly as designed; the defects are in the
trigger and its classification.

Mechanism (verified at file:line): the S3 client keep-alive default is 5 s (`S3Defines.h:12`); pooled
DISK connections expire at 0.8×5 s idle (0.1×5 s once the group crosses `disk_connections_soft_limit`),
`mustReconnect` resets any connection whose request STARTED > 4.5 s ago, and `http_keep_alive_max_requests`
rotates every 100 requests. With P pooled connections and R req/s, reuse survives only while P/R < 4 s —
concurrent FINAL prefetch puts P in the dead band, so nearly every request connects fresh and the CLIENT
closes on return → TIME_WAIT at request rate (reporter: ~430 GET/s × 60 s ≈ 26k of 28,232 ports). The
store honoring keep-alive is irrelevant. Retry amplification: `EADDRNOTAVAIL` is classified as remote
transient at EVERY layer (Poco → `CoreErrors::NETWORK_CONNECTION` → retryable; CAS controller →
`Unresolved`); reads ride `s3_retry_attempts=500`. Lease renewal = ONE bare single-attempt PUT per 10 s
through the same DISK pool (no reserve, no controller budget — WEAKER than any data write); outage length
is constructive: `mountObservationThresholdMs = ttl + 5% + cadence = 36.5 s` (field answer to
{#fence-window blast radius}'s "measure where remount time goes").

Fix directions, in value order:
- (1) **Classify local socket-resource errors as local** (`EADDRNOTAVAIL`, `EMFILE`, `ENFILE`): hard
  backoff instead of retry-at-rate. Template = the existing DNS sub-classification branch
  (`PocoHTTPClient.cpp:840`); CAS side maps it out of `Unresolved`'s full budget. Retry loops are the
  positive-feedback term — this cuts the amplifier.
- (2) **Mount-lease resilience**: in-period fast retry (today a failed renewal waits the FULL next
  period — `CasServerRoot.cpp:1437` `continue` re-enters `wait_for(period)`), and/or reserved
  connection / dedicated small budget for the renewal PUT. Note the margin arithmetic: ttl=30s /
  period=10s / margin=2s allows at most TWO ride-out warnings per lease generation, not three.
- (3) **Keep-alive defaults for CAS-over-S3 profiles**: 5 s TTL + 100-request rotation is the churn
  engine under sustained read concurrency; evaluate raising `http_keep_alive_timeout` (60 s) and
  `http_keep_alive_max_requests` in the CAS disk profile / docs. Caveat: the server-advertised
  `Keep-Alive: timeout=N` header mins the client value (`HTTPClientSession.cpp:377`) — capture what
  RustFS advertises first. Diagnostic BEFORE any change: `DiskConnectionsCreated/Reused/Expired/Reset/
  Preserved` + `DiskConnectionsTotal/Stored` (prediction: Created≈request rate, Expired>>Reset, Stored≈0;
  if Reset>>Expired the cause is the non-drained-body branch and the fix belongs in the read path).
- (4) NOT a fix: capping pool limits below the ephemeral range (reporter's #1) — limits gate KEEPING,
  not CREATING; TIME_WAIT is invisible to them (hence errno 99, never `HTTP_CONNECTION_LIMIT_REACHED`);
  lower caps flip the pool into the 0.5 s TTL regime sooner and worsen churn.

Housekeeping folded in:
- **S07 wide-part port-exhaustion finding re-rated**: was closed 2026-07-06 as "cost/latency only, not a
  data bug" (`performance.md:131`, DESIRABLE) — #2243 refutes that scope: the same condition takes the
  LEASE down and discards in-flight write txns. Availability class, not just cost.
- **[B196] is stale as written**: `s3_max_connections` is dead code for the disk path (an AWS-SDK
  `ClientConfiguration` field; `PocoHTTPClient` never reads `maxConnections`) — the disk path is governed
  by `disk_connections_*` + keep-alive, and NONE of those caps concurrent socket creation either.
- **Soak-harness blind spot**: `soak/cluster.py` classifies "mount lease not held" as retryable
  `NETWORK_ERROR` — our own soaks would ride through a #2243 event and score it recoverable; add a
  lease-loss detector (count `TransientNotLive` windows / `CasMountLeaseKeeper` errors) to checkpoints.

## GC falls behind without bound under sustained small-part churn (measured 2026-09-15) {#gc-backlog-runaway}

Local msan rig, CI msan binary v26.6.4, 6 passes of the CI shard 2/3 test list on one server (3 h 24 min), RustFS
rc.3 in its own cgroup (`tmp/investigation/t4/msan_local/analysis/REPORT_run7.md` in lane-g): GC round duration
24.3 s → 596.8 s (24x) against a 20 s interval, `CASGCPendingReclaim_cas_s3` 81 → 35,351 (436x), rounds per 10 min
23 → 1. Round length tracks the backlog (r = 0.92) more than the object store's delete latency (r = 0.57, RustFS
`delete` 0.4 → 44 ms with store size), so the loop is inside CAS: the bigger the backlog, the longer the round, the
more the backlog grows. GC is ~1/3 of all object-store operations in that run (run 3 events: ~890k GC reads, 290k
`CASGCMetaOps`, ~510k graduation HEADs of ~4.7 M S3 requests). On NVMe the tests do not feel it (iteration 6 / 1
median 1.17); on the CI runner's slow disk it is the likeliest amplifier of the msan/tsan CAS-S3 lane ramp
(issue #2298). The CI msan shard's own log shows round 27 at +61 min = ~135 s per round already in hour one.

**Which phases grow, and why** (live `system.cas_gc_log` of run 8, `cas_s3`, 5-minute windows; the `round` column
is 0 on `Phase` rows — use `round_id`):

| window | rounds | `defer_decision` | `fold_ref_intake` | `pending_deletes` | `fold_reduce` | `namespace_cleanup` |
|---|---|---|---|---|---|---|
| 0-5 min | 11 | 0.2 s | 0.5 s | 0.1 s | 0.6 s | 0.8 s |
| 15-20 min | 6 | 10.9 s | 9.7 s | 8.4 s | 3.9 s | 2.0 s |

- `defer_decision`: a full LIST of the ref-log prefix across every namespace each round, `ref_log_keys_listed`
  2,308 → 19,415, `namespaces_seen` 105 → 434 — of which `dead_life_debris` 51 → 403 (93-95 %): with one live test
  table on the server, almost everything the LIST walks is dropped tables' debris waiting for cleanup.
- `fold_ref_intake`: one GET per ref-log record (`logs_applied` 613 → 2,102 per round); the fold read-ahead (in `antalya-26.6` since `8f6cd74a3f5`, 2026-09-05, so ON in this run at its default `cas_gc_read_concurrency=16`)
  branch (2.4x intake) targets exactly this.
- `pending_deletes`: `deleted` 60 → 1,181 per round at ~7 ms each (exact-token DELETE + graduation HEAD); RustFS
  `delete` latency itself grows 0.4 → 44 ms with store size (run 7), which is where the loop closes.
- `fold_reduce`: one HEAD per zero-transition candidate.
The other 13 phases stay in the tens of milliseconds.

**Why the debris cleanup does not keep up** (`CasGc.cpp:352-386`, `CasNamespaceJanitor.cpp`): the janitor runs ONCE
per round over ONE page of 1,000 keys (page size hard-coded at `CasGc.cpp:363`), deleting dead-life objects one by one
(HEAD + exact `remove`; the bulk-delete path is not used), and it runs AFTER the LIST/intake phases, so what it removes
was already listed and read in the same round. Its throughput is `page × rounds/min`, and rounds/min collapses as the
listed debris grows (11 → 4 rounds per 5 min; namespace removals per window 417 → 10 while 403 dead lives waited):
a positive feedback loop. Namespace deaths themselves are folded (`new_removals`, ~35-40 per folding round). No
setting bounds this: `gc_round_ref_cleanup_budget` (5000) is not the limiter.

Asks, in order of size:
1. **Small, measurable first:** make the janitor's pages per round and page size settings (`gc_round_janitor_pages`,
   default 1; `gc_janitor_page_keys`, default 1000) and let a round run pages while dead candidates and a time budget
   remain; move `namespace_cleanup` BEFORE `defer_decision` so a round does not list what it is about to delete.
   A/B on the msan rig (`pages=10`) in one run.
2. **Medium:** batch the janitor's deletes through the existing bulk-delete path (`gc_bulk_delete_chunk_keys`; the
   LIST already carries etags, skip the per-key HEAD where the token is known), and delete the covered ref logs of
   dead lives together with them instead of waiting for retention prune.
3. **Structural, the real O(debris) fix:** replace the global hint enumeration (`enumerateRefPrefix`,
   `CasGc.cpp:3955`: one LIST of `<prefix>/cas/ns/stream/` over everything incl. dead lives) with the catalog cut
   (already one GET per round, `CasRefCatalog::read` in `listRefPrefix` `:4003`) plus one frontier probe per LIVE life
   (GET of `refLogKey(life, last_folded + 1)`, 404 = unchanged — the same probe fold intake already makes for
   `frontier_proven`/`absent_probes`), and a per-life LIST of `<prefix>/cas/ns/stream/<life>/_log/` only for the
   lives that fold. Run 8 at 20 min: 20 LIST pages / 19,415 keys (93 % debris) would become ~30-50 small requests
   with zero debris sensitivity. A cross-round index of dead lives does NOT help by itself: S3 still walks their
   keys, only the parsing is saved. Also note `gc_fold_threshold = 1` (`CasPool.h:160`) means every round with one
   new log anywhere folds (`deferred = 0` throughout the runs) and the defer verdict is computed AFTER the full LIST,
   so a deferred round pays the whole enumeration anyway.
4. Bound the work per round / make the drain rate independent of the backlog (batching deletes; merge the fold
   read-ahead is already in: `cas-gc-fold-read-ahead` was merged into `cas-gc-rebuild` as `e377741c725` and reached `antalya-26.6` as `8f6cd74a3f5` on 2026-09-05, so run 8 measured intake WITH it; the remaining lever is `[gc-intake-manifest-edge-serial-chain]`); back off rounds when the
   store's delete latency is high instead of stacking longer rounds.
5. Export `cas_gc_log` round and phase durations to the CI artifacts so this is visible per run.
Corrections from the run-8 `cas_gc_log` dump (brainstorm rev.2, `docs/superpowers/specs/2026-09-15-cas-gc-dead-namespace-debris-cleanup-design.md`):
- "deferred = 0 throughout" is wrong for run 8: 12 of 85 rounds were deferred.
- The janitor's limiter is REQUEST COUNT, not page count: over 85 rounds it visited 55,285 keys and deleted 22,888,
  a full 1000-key page costs 1.3-7.4 s (~7 ms per key: HEAD + exact DELETE, serial). Raising pages per round first
  would lengthen rounds and partly cancel itself. Revised order: (1) batch the janitor's write-once `_log`/`_snap`
  deletes through `removeChunkWriteOnceOrOneByOne` (~60 lines, no license change; a page becomes 1-2 batch requests),
  (2) page settings at the existing phase position (the reorder before `defer_decision` is NOT invariant-preserving:
  `suppress_destructive` comes from the fold verdict), (3) the targeted drain of retired lives.
- `gc_round_ref_cleanup_budget` IS the limiter on the rounds where covered-log cleanup is live: of 73
  `ref_object_cleanup` rows, 66 deleted nothing and 6 sat exactly on the 5,000 cap — bimodal, i.e. cleanup fires only
  for lives that hold a checkpoint-authorized snapshot (`planRefCleanup` returns early without a checkpoint,
  `CasRefProtocol.cpp:822-835`), and short-lived test tables never publish one (run 8: ~40 `_snap` keys for 434 lives).
- Safety notes from the codex review of the brainstorm: `retired_lives` are lives observed absent/replaced during
  reconciliation, not proof that this actor erased them; the janitor's liveness lambda is a per-page cached
  `authority_held` (`CasGc.cpp:364-367`, `4692-4703`) and cannot fence a concurrent new leader; `Removing` lives still
  have recovery readers (`namespaceStillLogicallyPresent`, `dropNamespaceImpl` retry), so deleting on the DROP path
  is rejected. Open question for the owner: may the round's drain phase perform physical deletes at all.
CI-side mitigation without GC changes: system logs of the CAS lanes on a plain disk (every log flush onto `cas_s3`
is new ref logs and objects), which also shrinks the RustFS object count.
Related: RustFS `delete`/`delete_version` latency growth with object count (RustFS side),
`[part-removal-repoint-waste]`.

## `[covered-log-cleanup-aborts-on-catalog-etag]` Covered-log cleanup aborts before its first delete whenever any other namespace changed since the fold (found by the janitor review, 2026-09-16) {#covered-log-cleanup-aborts-on-catalog-etag}

Found by the codex review of the janitor brainstorm (`docs/superpowers/reports/2026-09-15-cas-gc-dead-namespace-debris-cleanup-codex-reviews/review_r2.md` MAJOR 1,
`BRAINSTORM.md` rev.3a §1(d) and §7 question 5). Recorded here as its own item: it is outside the janitor's
scope and may be the larger lever.

**Mechanism.** `Gc::cleanupRefObjects` deletes the `_log` objects of **live** lives that are already covered
by their checkpoint. Before its first chunk it re-reads the pool catalog and, in `authorityHolds`
(`Gc/CasGc.cpp:3590-3605`), requires `current_catalog.etag == folded.catalog_cut->etag` — equality of the
**whole pool catalog** with the cut taken at fold time — in addition to the per-namespace checks (row
unchanged, life resolves to the same incarnation, `gc/state` present with the same owner/sequence). The
etag condition is the strict one: any `CREATE` or `DROP` of any other table between the fold and the cleanup
changes it, and the caller then stops the entire pass before the first delete (`:3690-3691`). The code
comment says the strictness is deliberate: "every irreversible delete is still licensed by the SAME complete
catalog observation and GC lease that adopted the fold".

**Measured (run 8 of the msan rig, `RIG/run8/samples/cas_gc_log.tsv`, 85 `ref_object_cleanup` rows, stage
read off each row's ProfileEvents: the catalog GET classifies as `Other`, `/cas/ns/` reads as `Root`):**

| stage reached | rows |
|---|---|
| no object read at all (no checkpoint / no `checkpoint_snapshot_id`; `planRefCleanup` returns early) | 37 |
| nonempty chunk built, then `authorityHolds` false at the catalog comparison | **40** |
| failed after the catalog comparison | 0 |
| deleted something | 8 (7 at the 5,000 `gc_round_ref_cleanup_budget` cap) |

Under a workload that creates and drops tables continuously, the etag rarely survives from the fold to the
cleanup; the phases in between (`pending_deletes`, `fold_reduce`) are exactly the ones that grow over the run
(`{#gc-backlog-runaway}`), so the window widens as the round lengthens.

**Why it matters beyond cleanup.** Covered logs the pass fails to delete stay under the live life's `_log/`,
are listed by every global `enumerateRefPrefix` LIST (`defer_decision` growth), and become dead-life debris
for the janitor when the table is dropped. So the aborts feed the very debris that S1/S2 make the janitor
remove faster: **longer round → cleanup aborts more → more objects → longer LIST → longer round.** The 8
successful rows deleted 23,840 objects; 40 aborted rounds at up to 5,000 each is the order of the backlog
this can produce per run.

**Plan, three steps, no step before the previous one's result:**

0. *Estimate from existing data, no code.* From run 8's Phase rows: the window between the fold's catalog cut
   and `ref_object_cleanup` per round, against the lane's CREATE/DROP rate. If the window is seconds and the
   rate is several per second, survival of the etag is structurally near zero and the question is not "how
   often" but "is whole-catalog equality required at all".
1. *Measurement with code, one local run.* Split the stop reason in `authorityHolds` into ProfileEvents:
   etag-only (row and life of THIS namespace unchanged), row/life changed, `gc/state` absent or owner/sequence
   changed; plus, per round, the number of objects planned and not deleted because of the abort. Release
   build in lane-g, local ca-s3 stateless lane, 60-90 min, dump `cas_gc_log` and events. Result: the share
   of aborts whose per-namespace licence was intact, and the undeleted volume per run. ca-impl, half a day.
2. *Brainstorm on the licence, only if etag-only dominates.* The narrow question: which observation must be
   unchanged to delete write-once `_log` keys of one life that its own checkpoint covers. Candidates:
   (a) narrow the licence to row + life + `gc/state`, dropping whole-catalog etag equality — the argument is
   that the keys are write-once, belong to one life, and the plan derives from that life's durable
   checkpoint, so other namespaces' churn changes neither the key set nor its coverage; the counter-argument
   to answer is the author's "same complete observation" comment, i.e. whether the fold seal or this life's
   coverage depends on the rest of the cut; (b) keep the licence, re-read the catalog and take a fresh cut
   immediately before the cleanup inside the same round, shrinking the window to milliseconds; (c) move the
   cleanup phase to right after the fold while the cut is fresh. Constraints in: the round stays one-pass,
   LIST trust is not reopened, no new object kinds. ca-arch with the code, then codex in a loop capped at
   three rounds.

**Not to do:** relax the etag check directly. It is a licence on irreversible deletes and the comment says
the strictness is intended. Numbers first, then the invariant, then code.

Related: `{#gc-backlog-runaway}`, `[targeted-drain-of-retired-lives]`, `[PART-REMOVAL-REPOINT]`
(`BACKLOG/gc.md`).

## `[targeted-drain-of-retired-lives]` FROZEN, not designed: draining a retired life's prefixes right after reconciliation (approach A of the janitor brainstorm, 2026-09-15) {#targeted-drain-of-retired-lives}

Source: `docs/superpowers/specs/2026-09-15-cas-gc-dead-namespace-debris-cleanup-design.md` rev.3 §A, and the three codex review rounds
(`docs/superpowers/reports/2026-09-15-cas-gc-dead-namespace-debris-cleanup-codex-reviews/`). Status by owner decision 2026-09-16: **frozen as "not
designed"**. It is not scheduled after S1/S2; whether it is worth building at all is decided by the S1/S2
measurements (`{#gc-backlog-runaway}`).

**The idea.** `pre_fold_ref_drain` erases every eligible `Removing` row and the reconciler returns
`retired_lives` (`Gc/CatalogLifecycleReconciler.h:32-34`); both per-life prefixes are constructible from the
incarnation (`Formats/CasLayout.h:134-143`). A would, after a drain that reports `Authoritative` and
`DrainComplete`, and only for a life that the unambiguous `final_catalog_cut` no longer resolves, delete that
life's `namespaceStreamPrefix` (batched write-once deletes) and `namespaceStatePrefix` (exact removes), leaving
the global janitor page as a backstop. Cost per dead life: 2 LISTs, 1 batch delete, a few exact removes.
~150 lines plus a budget setting. The attraction: today's janitor finds dead-life debris only by paging the
global `<prefix>/cas/ns/stream/` LIST, one 1,000-key page per round, and 93-95% of listed keys are that debris.

**Why it is frozen: five review findings, each a legitimate reader or writer of a retired prefix that the
design did not fence, and each answered in the brainstorm by one more precondition rather than an
invariant.**

1. `retired_lives` records lives *observed* absent or replaced, including on `FencedOut`
   (`Gc/CatalogLifecycleReconciler.cpp:99-118`, `Pool/CasRefCatalog.cpp:474-484`, `525-536`); it is not proof
   that this actor erased the row. Only the conjunction with an unambiguous final cut is a licence (r1 #1).
2. Two GC actors can overlap. The janitor's liveness callback is a flag refreshed once per page
   (`Gc/CasGc.cpp:364-367`, `4692-4703`); `CasOperation::gate` samples it without refreshing the lease
   (`Backend/CasRequests.cpp:413-425`) and cannot cancel an in-flight delete. A successor leader's janitor and
   a stale leader's drain can hit the same prefix. Per-chunk authority refresh improves stopping, it is not
   fencing (r1 #2).
3. `Removing` lives still have readers: `namespaceStillLogicallyPresent` and a retried DROP both call
   `ensureRefTableRecovered` first (`Pool/CasRefLedger.cpp:5063-5071`, `5146-5155`), and recovery over drained
   inputs **throws `CORRUPTED_DATA`**, non-transiently: `chooseRecoveryGrounding` throws for a `Live` or
   `Removing` namespace with no readable `_ckpt` (`Pool/CasRefCkpt.cpp:142-151`); partial drainage throws on
   missing committed logs under an unchanged checkpoint (`:1067-1081`); the outer retry treats corruption as
   final (`:1489-1494`). Rev.2's "a cold probe answers present conservatively" and "a DROP retry proceeds to a
   terminal append" were both wrong. The window exists today (the janitor deletes the same bytes after the row
   erase); A narrows it from many rounds to part of one round and so raises the hit rate (r1 #4, r2 #2).
4. A drained prefix is not guaranteed empty: a snapshot publisher captures live state and then creates at the
   captured life's `_snap` key (`Pool/CasRefLedger.cpp:4543-4544`); its admission checks mount generation and
   runtime flags (`:4445-4456`), which retirement invalidates but cannot un-send. A PUT in flight lands after
   the drain. "Nothing can write under a dead incarnation" is withdrawn; the true guarantee is only "a
   successor life uses a different prefix" (r2 #4).
5. Bulk deletion is accounting-atomic, not physically all-or-nothing (`removeManyWriteOnce` contract), so a
   partially drained prefix is a normal outcome, and `deletePrefixWholesale`'s `out_fully_drained` means
   "enumeration exhausted", not "prefix empty" (`Gc/CasGc.cpp:3733-3757`) (r2 #3).

The accretion is the signal: after two rounds A carried "publisher quiescence", "per-chunk authority
refresh", "own budget", "backstop stays forever", and two blocking open questions (may the drain phase perform
physical deletes at all; how the callers in 3 handle the exception). The system's recovery model treats a
missing checkpoint of a `Live`/`Removing` life as corruption, not as "nothing to recover", and there is no
durable marker "this prefix may be destroyed" that recovery, the publisher, a DROP retry and a second GC
actor all consult. Without that marker A is a race by construction and the backstop does the real work.

**What would unfreeze it.** A retirement-fence invariant designed first, on its own: who may read or write a
retired life's prefix and until which durable event; recovery over a fenced life refuses (not
`CORRUPTED_DATA`), publisher admission and DROP retry check the fence, and the fence is visible in the
catalog cut a successor leader derives. Only then is a targeted drain a licence instead of a bet. That is
architectural work, not a janitor optimisation, and it is justified only if S1/S2 leave the janitor unable to
keep up with debris production.

**Decision pending from the owner, no default:** whether a round's drain phase may perform physical deletes
at all. Today the drain erases catalog rows under the lease (`Pool/CasRefCatalog.cpp:514-517`) and performs
no physical cleanup; nothing in the code states either way.

## Issue #2244 (filed by us): lease/remount retry asymmetry — CI RCA of job 96307284077 (2026-08-20) {#issue-2244-lease-retry-asymmetry}

https://github.com/Altinity/ClickHouse/issues/2244 — full RCA in the issue. Before the minimum fix, the
two operations keeping a CAS mount alive were the only S3 operations without retries: renewal sent
one 5-second-timeout `PUT` per 10-second period, while remount claim used `SingleAttempt` inside a
roughly 15-operation sequential chain with a 36.5-second observation window. An intermittent-timeout
episode that the data plane rode out on `Attempt 2/501 succeeded` therefore tripped the fence and cost
roughly 15 minutes of full-disk write refusal. This was field evidence #2 for
{#fence-window blast radius} and {#fence-window observability}.

Fix directions (tracked in the issue, value order): (1) in-period renewal retries while
`now + margin < confirmed_deadline` — would have prevented this trip outright; (2) per-step retries in
the remount chain + investigate the own-ambiguous-claim observation-window reset (STID-3982 family);
(3) observability: log the trip reason + every remount attempt step at default level, ProfileEvents
for renewals/remounts; (4) rate-limit the snapshot-publication refusal loop (125,952 warnings/16 min
on one table — NEW defect, no backoff on that loop). CI-env extra: RustFS logs at ERROR-only — raise
its log level in the CA lanes (re-opens the [CAS CI observability gaps] rustfs item at the level
dimension).

The minimum pre-release fix is specified in
`docs/superpowers/specs/2026-08-23-cas-mount-renewal-retry-design.md`: ambiguity-aware in-period
renewal retries, renewal/remount observability, and the snapshot-refusal backoff hole. It deliberately
does not change the remount protocol. **Implemented 2026-08-24:** the minimum cut and its focused/full
TLA+, Release/Debug, proxy-integration, and 15-minute S39 gates are complete. The per-step remount
follow-up below landed afterwards (see its CLOSED note). **Issue closed 2026-09-14**; first release with
the whole fix: 26.6.4.20001.altinityantalya.

### Issue #2244 follow-up: per-step remount recovery — CLOSED 2026-09-14 {#issue-2244-remount-retry-follow-up}

Originally: replace whole-chain remount retry with an explicitly modeled state machine (per-step retry
classification, preserved owner/catalog/epoch results, own-ambiguous-claim handling, epoch burning,
teardown cancellation, deterministic fault injection), gated by its own TLA+ pass.

What landed in antalya-26.6 (7f932d31352 "classify mount-claim conflicts", 2026-08-25, and the
request-engine migration 37c9bd4356b, 2026-09-05), verified against the tree on 2026-09-14:
- no `SingleAttempt` is left on the mount path: `readOwnerObject`, `claimOwnerOrThrow`, the sentinel
  probe in `allocateWriterEpoch`, `claimMount` (read/create/replace), `claimMountAwaitingExpiry`,
  `computeHeartbeatFloor`, `probeNonTerminalMountSlots` all run under `Retry::standard()` (90 s budget,
  `CasRetry.h:41`); the only remaining "single attempt" is `checkConditionalWriteSingleAttemptSupport`,
  a backend capability probe;
- an own claim that landed after a client-side timeout is recognized as ours in `claimMount` ("own
  claim replayed — refreshed seq + expiry", same uuid + same epoch) instead of resetting the
  token-stability observation; unresolved conditional writes are settled by the engine's resolve read
  (`CASRequestResolveRead`);
- coverage: `gtest_cas_mount_claim_conflicts.cpp`, `gtest_cas_mount.cpp`, `gtest_cas_mount_runtime.cpp`,
  integration `test_cas_mount_renewal_retry`.

Not done, and no longer blocking anything: the explicit state-machine model with its own TLA+ gate.
The retry classification was folded into the request engine's policies rather than a mount-specific
model; the refinement audit against `CaCasMountCore` was not repeated for it. Reopen only if a remount
defect appears that the engine-level policies cannot explain.

Triage bonus recorded: the RustFS "Erasure decode failed ... downstream_closed" error class is a red
herring = ClickHouse's silent-by-design mid-body aborts (cancellations, LIMIT, abandoned prefetch);
do not chase it in future triage.

## CAS disk settings whitelist rejects valid S3 keys (found by #2243 mitigation attempt, 2026-08-20) {#cas-disk-s3-key-whitelist-gap}

Field report (Carlos, clickhouse-regression run 32408309167/job 96552561919): adding
`<http_keep_alive_timeout>60</http_keep_alive_timeout>` to a CAS disk block — the mitigation
suggested in #2243 — kills the server at startup with `Unknown setting 'http_keep_alive_timeout'
(UNKNOWN_SETTING)` during metadata loading.

Mechanism: on a CAS disk the S3 keys are direct children of the same block as the CAS settings, and
`ContentAddressedSettings.cpp` fail-closes on any key that is neither a CAS setting nor in the
`non_cas_keys` whitelist. The whitelist was built by enumerating in-repo configs, so it contains only
the S3 keys those configs happened to use — `http_keep_alive_timeout` and
`http_keep_alive_max_requests` (both genuinely consumed on the disk path,
`ObjectStorages/S3/diskSettings.cpp:169-170`) are missing, as is every other unused-in-repo
`S3AuthSettings` key (`connect_timeout_ms`, `max_connections`, `session_token`, ...). No workaround
exists: the disk path reads S3 auth settings only from the disk block
(`S3Settings::loadFromConfigForObjectStorage`), never from the global `<s3>` per-endpoint section.

Fix: instead of growing the enumerated whitelist, skip every key whose name is a builtin
`S3AuthSettings` or `S3RequestSettings` name (BaseSettings exposes the builtin-name enumeration),
keeping `non_cas_keys` only for the genuinely ad-hoc generic-disk-layer keys (`type`, `name`,
`use_fake_transaction`, ...). Update the four-way-scan comment accordingly. Release-relevant: this
blocks the #2243 CI mitigation on CAS disks.

## Issue #2211: `SYSTEM CAS GC RUN` on a follower silently does nothing (adjudicated 2026-08-21) {#issue-2211-gc-run-follower-noop}

https://github.com/Altinity/ClickHouse/issues/2211 — CONFIRMED as described; the report's code anchors
all check out on HEAD. History splits it in two:

1. **No-steal on manual runs is DELIBERATE** — commit `74d67b85021` (2026-07-13, "manual GC rounds
   never steal a lease"): the observation-window steal protocol's safety argument needs the two
   "incumbent frozen" observations spaced by real wall time (the loop's paced ticks); two manual
   calls can land microseconds apart and fake a frozen incumbent → two concurrent destructive GC
   actors. The pre-fix manual path also never heartbeat-protected an acquired lease. Keep as is;
   the issue itself agrees steal is the wrong fix.
2. **The silent success row was NOT a chosen contract** — commit `cb111510c1a` (2026-07-20) merely
   surfaced the already-computed `RoundReport` as a result set ("mirroring the DROP POOL MEMBER
   precedent", deferred-register item 11). No record anywhere (commits, specs, backlogs) weighs
   throw-vs-row for the follower case; the docs (`operations/debugging.md` `{#sql-gc-run}`) don't
   mention the follower no-op either. Genuine operator-contract gap.

Also confirmed: `GcLease` is `{owner: UInt128 random gc_id, seq}` — no host identity
(`CasGcStateFormat.h:17`), and `system.cas_mounts.is_leader` is populated only for the local mount,
so a follower cannot name the leader today.

Fix shape (DECIDED 2026-08-21, user call): keep the quiet idempotent OK — no exception. Rationale:
with default `distributed_ddl_output_mode=throw`, a throwing follower inverts the bug for
`ON CLUSTER` (leader ran the round, N-1 followers threw, the statement reports failure), and a node
inside the DDL fan-out cannot tell it is part of `ON CLUSTER`, so selective throwing is impossible.
A follower's "not my lease" is a valid outcome of "run a round here if this node may", and quiet OK
keeps scripts/harnesses that poke `RUN` on every node working. Instead, make the outcome
first-class and visible:
- add a `finish` column (`Success`/`NotALeader`/`Deferred` — already exists in `cas_gc_log`, the
  interpreter row just doesn't emit it) to the `RUN` result set, so the operator reads a word, not
  infers from `acquired_lease=0` + zeros;
- add advisory identity to `GcLease`, mirroring the existing `MountLease` precedent
  (`CasServerRootFormats.h` carries `hostname`/`pid`/`server_uuid` next to its protocol fields for
  exactly this purpose): `hostname` (+ `server_uuid`/`pid` for symmetry), written at acquire/steal.
  The protocol part stays untouched — `owner` MUST remain a random per-process-instance UInt128
  (a restarted server is a NEW GC actor and must not resume the old lease; hostname is neither
  unique nor per-instance), which is WHY host identity was never the owner: the advisory field was
  simply never needed until #2211 (YAGNI, not a considered rejection — no record deciding against
  it). Durable-format change, pre-release so no compat scaffolding
  ([[feedback_ca_no_compat_scaffolding_predev]]);
- follower `RUN` row then carries `leader_host` — `NotALeader, leader_host='replica-2'` in one read,
  no operator discovery query. Rejected as contract (user call): documenting a
  `clusterAllReplicas(system.cas_mounts) WHERE is_leader=1` discovery recipe as the way to find the
  leader — too strange a requirement once the row can name the holder;
- bonus: `system.cas_mounts.is_leader` can be populated for ALL rows (match `gc/state`
  hostname/server_uuid against mount slots), not local-only;
- docs `{#sql-gc-run}`: state the leadership model in one sentence.
No-steal on manual `RUN` stays untouched.

## Reviewer ask: full-word wire keys in all CAS persisted formats (2026-08-21, RESOLVED 2026-08-30) {#wire-keys-full-words}

Reviewer feedback: after the move to JSON text formats, the 2-5-char field keys (`su`, `eat`, `fen`,
`nwe`, ...) are illegible and force manual remapping when reading raw objects. Wanted: human-readable
identifiers everywhere, matching the struct/enum names, no mapping table needed.

Resolved by `docs/superpowers/specs/2026-08-28-cas-semantic-wire-keys-design.md`, shipped as the
semantic-wire-keys generation reset: every persisted key became a "sufficiently full" semantic word
(metadata written once per object gets a descriptive name; fields repeated once per record get a
short semantic word; enum values render as full words). Exact full C++ member names everywhere —
the reviewer's literal ask — were deliberately REJECTED for repeated records and for the fixed
`cas_blob` descriptor: the design's "Rejected alternatives" section records why, and points at the
Altinity PR #2288 implementation of the rejected alternative as a worked illustration of the cost
(the descriptor no longer fits the default payload offset, and the raw uncompressed formats grow
materially). `Formats/README.md` carries the naming rule the reviewer originally quoted.

## Issue #2219: relink refusals logged at Error with stack trace (adjudicated 2026-08-21, fix queued) {#issue-2219-relink-refusal-log-level}

https://github.com/Altinity/ClickHouse/issues/2219 — CONFIRMED, cosmetic but worth fixing (the issue
documents a multi-hour false triage chasing a network fault that wasn't there; up to 53% of relink
proofs refuse under small-part load, each printing `Error` + stack trace + `NETWORK_ERROR`).

Mechanism: the throw site is fine (`DataPartsExchange.cpp` taxonomy row 3, ~:1550 — deliberately
thrown so the byte-fallback does NOT run); the noise comes from the generic queue handler
`StorageReplicatedMergeTree::processQueueEntry` (:4224-4239), whose demotion list
(`NO_REPLICA_HAS_PART`/`ABORTED`/`PART_IS_TEMPORARILY_LOCKED` → `LOG_INFO`, no stack trace) does not
know this code. `NETWORK_ERROR` is NOT load-bearing on this path — no `e.code()` branch in the queue
or fetch path tests it; "retry-later" is the default for any stored exception.

FIXED 2026-08-26 (`081c473904e`; branch `cas-relink-refusal-classification` for the antalya-26.6
PR). The code chosen is `NO_REPLICA_HAS_PART`, not the `ABORTED` this record originally planned: both
are in the demotion lists of BOTH queue executors (`processQueueEntry` AND
`ReplicatedMergeMutateTaskBase::executeStep` — the second one matters for merge-entries executed as
fetch), but `ABORTED` also sets `need_to_save_exception = false`, the exact
"treated as not an error, no backoff engages" pathology `[merge-progress-reset-mount-fence]`
documents; `NO_REPLICA_HAS_PART` keeps the exception on the queue entry. It is also the one
fetch-transient code stateless `part_log` hygiene checks whitelist (`02265_column_ttl` broke in the
CAS lanes on exactly this: PR #2159 CI, 13/14 reruns under `prefer_fetch_merged_part_size_threshold=1`).
Verified: the only two `e.code() == NO_REPLICA_HAS_PART` branches in the tree are those demotions;
`enqueuePartForCheck` keys on `replica.empty()`, not the code. Original plan text follows for
provenance: reuse `ABORTED`, which is already in the `processQueueEntry` demotion list with the
comment "Interrupted merge or downloading a part is not an error" → `LOG_INFO`, no stack trace.
Change only the exception code at the two CA retry-later throw sites in `DataPartsExchange.cpp`
(taxonomy row 3 and the `CaRelinkPromote::Unresolved` promote); the message text stays
self-describing. Verified: nothing on the fetch/queue path branches on `ABORTED` except the
demotion itself (shutdown-`ABORTED` overload is theoretical there). Also removes the misdirecting
`(NETWORK_ERROR)` label the issue complains about. Rejected: a dedicated new error code + a fourth
demotion branch (touches upstream, vetoed); `PART_IS_TEMPORARILY_LOCKED` (foreign semantics + a
`cleanup_thread.wakeup()` side effect); demoting all `NETWORK_ERROR` (hides real network faults).
Sender side already logs at `Debug` — receiver only.

## Nested `server_root_id` + prefix-based decommission victim selection (2031-triage CAS-007) {#nested-srid-decommission}

`validateServerRootId` accepts slashes (`Pool/CasServerRoot.h:199-229`; `gtest_cas_mount.cpp:97` asserts
`shard-01/replica-a` valid, and multi-segment srid support was deliberately fixed in `b97847d32f9`),
while `CasDecommission` picks victims by path prefix (`Tools/CasDecommission.cpp:146,150` +
prefix-LIST drains of `cas/manifests/<srid>/`, `staging/`, `roots/`). So
`SYSTEM CAS DROP POOL MEMBER 'a'` destroys the namespaces and control objects of a LIVE member
`a/b`. Existing test covers only the sibling case (`gtest_cas_decommission.cpp:699-716`). Partial
mitigation exists in one direction only: mounting `a` when `a/b` already exists fails closed
(`CasServerRoot.cpp:145-149`); the reverse order is unguarded. Note the asymmetry — relink routing
compares srid for EXACT equality (`ContentAddressedMetadataStorage.cpp:2021`), i.e. the prefix rule
is decommission-local.

Fix options (decide at fix time): (1) make decommission victim selection exact-srid + an explicit
refusal when any other member's srid is prefixed by the victim's, or (2) forbid nesting at validation
(reject a srid that is a prefix of, or prefixed by, an existing member's) — cheaper but removes the
multi-segment layouts that were deliberately enabled. Either way add the nesting case to
`gtest_cas_decommission.cpp`. P2: destructive but operator-initiated and requires a nested-srid
layout.

## Empty conditional token passes as an unconditional write on the Native backend (2031-triage CAS-010) {#empty-token-unconditional-write-guard}

`CasObjectStorageBackend`'s conditional-write path validates only the token TYPE
(`mintingTypeMatches`, `CasObjectStorageBackend.cpp:918-930`), and `Token::type` defaults to `ETag`
(`Primitives/CasTypes.h:259`), so a default-constructed `Token{}` passes every check; the S3 writer
then omits `If-Match` for an empty string (`WriteBufferFromS3.cpp:656-657,746-747`) and the "fenced"
write becomes an unconditional clobber. Emulated/InMemory backends fail closed, so the gap is
Native-only and invisible to the emu doubles.

No call site intentionally passes an empty expected token: callers either guard `head.exists` or use
`optional<Token>`. Generation-token paths additionally validate the numeric generation value, but an
empty ETag can still pass the type-only guard in `putOverwrite`, `casPut`, or `deleteExact`. The one
path that can legitimately produce an empty token on a `Done` write is `tokenFromWriteResult`'s
`HEAD` fallback, whose value flows into
`MountLeaseKeeper::last_token` (`CasServerRoot.cpp:1551,1688,1747,1828`); reaching it needs two
simultaneous store anomalies. Fix: reject an empty ETag token at the conditional-write entry
(fail-closed `LOGICAL_ERROR`-class throw) so no future call site can turn a fence into a clobber,
plus a Native-backend unit test. P2 — missing guard, not a demonstrated data-loss path.

## FIXED 2026-08-31 (S06 verified PASS 10/10; S21 fix landed, run not yet taken) `[s06-column-subset-verdict-measures-an-untouched-counter]` S06's column-subset check counts `CASBlobGet`, which the data read path never increments {#s06-column-subset-verdict-inert}

**Found by rerunning S06 at `--scale full` (2026-08-31) after `dev` and `ci` both left it
`INCONCLUSIVE`.** Scale is not the blocker, and no scale setting can be.

The verdict asks whether a few-column `SELECT` fetches far fewer blobs than an all-column scan, and
measures it as a `CASBlobGet` delta around each scan. At `full` — 10,000 columns, 2,000-row blocks —
both windows measured **zero**, so the card took its `all_gets == 0` branch and declared itself
inconclusive. It did the honest thing; the premise it needs was never established.

Zero is not explained by the reads being cheap or cached, because the run demonstrably moved data:
`CASBlobPut` 61,650, 60,078 objects and 300 MB in the pool. The discriminator is which code path
increments the counter, and the data read path is worth spelling out because it looks at first like
it could not possibly avoid the backend — a blob carries a fixed-length envelope, so its payload
cannot be read as if it started at byte zero.

It does not. `CasManifestReader::locate` returns a `BlobLocation` whose `offset` is the pool's
**fixed** `blob_header_len`, so no per-object header read is needed. `readBlobPayload` then calls
`object_storage->readObject` — the generic `IObjectStorage`, not `Cas::Backend` — and wraps the
result in a `ReadBufferFromFileView` starting at that offset. So the envelope IS skipped, by the
view's start offset rather than by a backend call, and `CasInstrumentedBackend` is nowhere on the
path. `checkOpAdmitted(CasOpClass::ContentRead)` is the only CAS-side gate on it, and it counts
nothing.

`CASBlobGet` is raised only in `CasInstrumentedBackend::get`, on the metadata/object path GC and the
ref machinery use; the sibling `CASBlobGetStream` is the backend's streaming get and is equally off
this path, which is why it too stayed at zero. The cumulative counters show the split directly:
`S3GetObject` 50,146 against `CASBlobGet` 14,509.

So the check is inert by construction, at `dev`, `ci` and `full` alike, and the two prior
`INCONCLUSIVE` verdicts carried no information about the property under test.

**Fix:** measure the delta the read path actually moves — `DiskS3GetObject` (or `S3GetObject`) —
and keep `CASBlobGet` only where a CAS backend get is genuinely expected. **Confirming experiment
before changing the verdict:** run the two scans unchanged and record both counters; the fix is
warranted only if `DiskS3GetObject` moves while `CASBlobGet` stays flat. If neither moves, the reads
are being served locally and the card needs a cache-drop, which is a different defect.

**The sweep is done, and it found one more.** Four cards touch `CASBlobGet`:

- **S06** (`s06_s08_manifest_parts.py`) — the verdict above.
- **S21** (`s19_s22_clone_fetch.py`) — `column-subset fetches only required blobs`, gated on
  `1-column CASBlobGet << all-column CASBlobGet`. **Same defect, same inert branch**, and it too has
  been `INCONCLUSIVE` at every scale it has been run at. Fix both together.
- **S12/S14** (`s12_s14_faults.py`) — reads the counter into observations, but its verdict is about
  `CASRootList`/`CASRootGet` at startup, which genuinely do traverse the CAS backend. Correct as is.
- **S41** (`s41_wide_insert_baseline.py`) — observations only, and its own docstring states that
  "`CASBlobGet` is a metadata GET here". That is independent corroboration of the discriminator:
  the counter's metadata scope was already understood and written down in this tree, which is what
  makes S06's and S21's use of it to measure column-data reads wrong rather than merely unlucky.

## `[s23-idle-rss-growth-500mb]` 500 MB of RSS growth on an empty, idle pool over 15 minutes {#s23-idle-rss-growth}

**Found by rerunning S23 at `--scale full` (2026-08-31), which FAILED where `dev` and `ci` were
inconclusive.** The `dev` window is 4 minutes at 5 s per minute; `full` is 15 real minutes, and that
is what made the verdict discriminate.

`memory flat over idle window` failed with `idle_rss_growth` = **500,146,176 bytes**. Per node,
resident went 677 MB to 1,178 MB on `ch1` and 641 MB to 1,087 MB on `ch2`. It is not only allocator
noise: ClickHouse's own `mem_tracking` roughly doubled too, 337 MB to 666 MB.

What makes it worth a look is what the pool held while this happened: **nothing**. `pool_shape` is
zero across every class — no blobs, no manifests, no refs, no roots, no files — `leftover_ca_tables`
is empty, `max_s3_ops_per_round` is 0, and GC deleted nothing across 11 successful rounds.

**Not yet established as a leak, and the reason is the window.** Fifteen minutes cannot separate a
server settling into its steady state — thread pools, caches and arenas materializing after boot —
from growth that does not stop. Peak resident (1,158 MB) sits close to final (1,181 MB), which is
weak evidence for a plateau.

**There is no `trace_log` evidence for this, and three separate things must change before a rerun
can produce any.** Asked whether the trace told us where the 500 MB went, the answer is no:

1. **The artifact dump does not collect it.** `predown_dump.sh` loops `for tt in CPU Real` — the
   `Memory` trace type is never queried, so no allocation stacks reach the run directory.
2. **There would be nothing to collect.** `total_memory_profiler_step` — the server-level knob that
   samples background and idle allocations — defaults to **0**, meaning off, and `ca-soak`'s
   `profiling.xml` sets only the two query profilers. The per-query `memory_profiler_step` (4 MiB)
   cannot fire during an idle window because no query is running. This is why the earlier one-hour
   chaos soak did have ~25k `Memory` samples while an idle run would have none: those came from
   queries.
3. **What was collected is not time-scoped — but the capability exists and is simply unused.**
   `predown_dump.sh` already accepts `FROM_TS`/`TO_TS` and folds them into a `${WINDOW}` clause on
   every trace query; the scenario runner just never passes them. So S23's `Real` aggregate spans the
   whole server lifetime, and although it is full of CAS write frames — `commit` 380 samples,
   `publishStaging` 378, `publishBlob` 365, `fanOutBlobUploads` 368, and
   `CityHash128BlobHashingWriteBuffer::nextImpl` 172 — those most plausibly belong to the setup phase
   and cannot be attributed to the idle minutes either way. The pool was empty by the end. Fixing
   this is a matter of the card passing the window it already knows, not of adding a feature.

**Items 1 and 2 are now fixed and verified (2026-08-31).** `predown_dump.sh` dumps `Memory` alongside
`CPU` and `Real`, and `configs/memory.xml` sets `total_memory_profiler_step` to 4 MiB. The setting
had to go in `memory.xml` rather than `profiling.xml`, because the latter is mounted into `users.d`
where a server setting is ignored. Verified on a live cluster: the setting reports `changed = 1`, a
write workload produced **1,064 background** `Memory` samples where the count would previously have
been zero, and the dump wrote 1.27 MB of `Memory` stacks with no error output.

**Experiment that decides it,** with those three fixed first: set `total_memory_profiler_step` to a
few MiB on the server, have the dump query the `Memory` trace type, and scope the aggregate to the
idle window by `event_time`. Then run one idle scenario with a 60-plus-minute window, sampling RSS
and `mem_tracking` each minute, and read the shape of the curve rather than its endpoints. A plateau
means the verdict's threshold is wrong for a freshly booted server; continued linear growth on an
empty pool means a leak, and the `Memory` stacks will say whose. Do not file a leak against the
product on the 15-minute number alone.

## `[s10-patch-premise-blocked-by-a-hardcoded-workaround]` S10 forces the non-patch delete path, so its patch-part premise cannot hold {#s10-patch-premise-blocked}

**Supersedes an earlier entry of mine that named the wrong cause.** I first attributed
`max_patch_parts_observed = 0` to `apply_patches_on_merge` being set at session level. That setting
defect is real, but it is not why patch parts are absent.

The actual cause is one line: `lw_supported = False` is **hardcoded** in S10, so the `if
lw_supported:` branch never fires and the lightweight `DELETE FROM` beneath it is dead code. The card
falls through to `ALTER TABLE ... DELETE`, which produces mutations, not patch parts. No setting and
no scale can produce a patch part while that line stands.

**The claim justifying that line is stale, and there is no product defect. Tested 2026-08-31.**
The comment says lightweight `DELETE` is "unreliable on this build (CA storage path diverges)". It was
added 2026-07-01 in the commit that introduced the whole suite (`b3fa29f29266`), whose message never
mentions lightweight `DELETE` — so the assertion stood for two months with no recorded error text, no
issue and no reproduction behind it.

Measured directly against a CA-disk table with a row-count-and-checksum oracle: 50,000 rows, two
`DELETE FROM ... WHERE bucket = N` statements, expected survivors 40,000 with checksum
1441566520450331260. Result: **40,000 rows and exactly that checksum**, no error. Lightweight `DELETE`
is correct on the CA path.

**But removing the flag would still not produce a patch part, and that is the more useful finding.**
Lightweight `DELETE` creates none: the run left 0 patch parts and one `Wide` part. Patch parts come
from lightweight **UPDATE**, and reaching them needs three table settings, each of which the server
names in an error until supplied:

| requirement | how it announces itself |
|---|---|
| `enable_block_number_column = 1` | `Code: 48 ... supported only for tables with materialized _block_number column` |
| `enable_block_offset_column = 1` | the same error, now naming `_block_offset` |
| `apply_patches_on_merge = 1` | a **table** setting; the session `SET` S10 uses throws `Code: 115` |

With all three plus `allow_experimental_lightweight_update`, an `UPDATE ... WHERE bucket = 5` on a CA
table succeeds and yields `patch-c46b00d08453527290352a2e6e9ad36f-all_3_3_0_2`, 2,000 rows, oracle
matching exactly (2,000 patched of 20,000).

**S10's detector is already correct** and needs no change: it matches `part_type = 'Patch' OR name
LIKE 'patch-%'`, and the real part carries that prefix.

**One caution before removing the flag.** Its comment gives two reasons and only the first is refuted.
"Unreliable" is false; "force the ALTER TABLE DELETE fallback for correct oracle semantics" is a
separate claim about how the card counts rows. The oracle used here counts through `SELECT`, which
honours the delete mask — but S10's own oracle must be checked against masked rows before its delete
path is switched, not assumed equivalent.

**Secondary, and worth fixing regardless:** `_probe_patch_parts` issues `SET apply_patches_on_merge
= 1`, which throws `Code: 115` because the name is a **MergeTree table setting**
(`MergeTreeSettings.cpp:861`), not a session one. The probe then returns `True` anyway, because
`allow_experimental_lightweight_update` was accepted and any single acceptance satisfies it. So the
probe reports patch support on the strength of a setting that does not by itself produce patch parts,
and swallows the one genuine rejection into an observation. Apply it with `ALTER TABLE ... MODIFY
SETTING` or at `CREATE`, and make the probe require the setting that actually matters rather than any
of them.

## `[s10-one-unreachable-manifest-survives-gc-fixpoint]` A single unreachable manifest survives GC to fixpoint, three rounds running {#s10-manifest-residual}

**Found at `--scale full` (2026-08-31); the same run as the entry above, so read that first — the
patch-part premise did not hold, and this residual arises from the lightweight-delete workload
rather than from patch content.**

`no unbounded leftovers` and `obsolete patch content reclaimed` both failed on one object:
`fsck_final` reports `unreachable: 1, dangling: 0` against `reachable: 163`, and
`residual_classification` puts it in `leak` as `unreachable:_manifests: 1` — not `pipeline`, not
`bookkeeping`. `reclaimable_drain_check` agrees it is reclaimable. `gc_fixpoint_history` is
`[1, 1, 1]`: GC was driven to fixpoint three times and the residual did not move.

One object is small but the shape is not benign — a reclaimable, unreachable manifest that survives
repeated fixpoints is by definition not being reclaimed. Also worth a glance in the same run:
`graduation_drain_history` is a run of ones with a single **128** in it.

**Before calling it a product defect:** identify the object and what still points at it, and
re-derive whether the workload can legitimately leave it (a manifest published after the fold's
coverage seal is bookkeeping, not a leak). The classification is the card's, and the card's premise
was broken in the same run.

## `[ca-event-log-loses-gc-manifest-deletes]` GC deleted 517 manifests and the CA event log recorded one {#ca-event-log-loses-manifest-deletes}

**Found while auditing S10's leftover-manifest finding in `cas_log` (2026-08-31).** This is why that
audit could not be done.

In the full-scale S10 run, `ch1`'s `gc_log` sums to exactly **517** `manifests_deleted` across its
rounds — round 6 alone deletes 133 — while `ch1`'s `cas_log` holds exactly **one**
`manifest_delete` event. `ch2` deleted none and logged none, so the whole discrepancy sits on one
node.

It is not a semantics mismatch between the two numbers. `CasGc.cpp` PHASE 15/18 emits
`CasEventType::ManifestDelete` **unconditionally, once per attempt**, inside the `mf_cleanup_now`
loop, and increments `report.manifests_deleted` only when the outcome classifies as `Deleted`. So
attempts are greater than or equal to deletions, and the event count must be **at least** 517.
It is 1.

One event did land — round 1, for a namespace owned by the other server root — so the emitter is
wired and reachable. That argues for events being dropped or lost rather than never produced.
Candidates not yet distinguished: a bounded queue in the event sink discarding under burst (round 6's
133 deletions in one phase is exactly a burst), a per-call `EventEmitter{*store}` binding to a sink
that is not the disk's configured log on most calls, or rows buffered in `ca_event_log` and lost when
the scenario tears the cluster down.

**Consequence for anything that reads `cas_log`:** manifest reclaim is effectively invisible there,
so `cas_log` cannot support any claim about whether a manifest was deleted, retried or skipped. The
S10 residual finding must be re-derived from `gc_log` and `fsck` until this is fixed.

**First step:** count events against `attempted` rather than against `manifests_deleted` — the phase
already records `attempted` as a metric — and check whether `system.ca_event_log` shows drops of its
own before looking for a bug in the emitter.

## `[s15-month-name-timestamp]` FIXED: S15 failed every variant on a format specifier {#s15-month-name-timestamp}

**Diagnosed and fixed 2026-08-31.** S15 had been `INCONCLUSIVE` at every scale, and the cause was
neither scale, nor shard counts, nor measurement. All three of its compose variants ran; all three
raised.

The verdict `variant <name> workload` is declared inconclusive when `_run_variant` throws, and the
throw was `Code: 41 ... while converting '2026-08-31 07:August:34' to DateTime`. The minutes field
holds `August`, because the card built its since-timestamp with
`formatDateTime(now(),'%Y-%m-%d %H:%M:%S')` and in ClickHouse **`%M` is the month name** — minutes
are `%i`. Every later query scoped by that timestamp then threw, taking the whole variant with it, so
five of eight verdicts could never resolve.

Fixed by using `toString(now())`, which yields the canonical form directly, at both call sites.

**The part worth remembering is that this trap was already documented in this very tree.**
`scenarios/run.py` carries a helper whose comment reads: "Do NOT use `formatDateTime` with `'%M'` —
in ClickHouse `'%M'` is the MONTH NAME, not minutes, which silently corrupts the since-timestamp used
to scope every card's GC/event-log queries." The card fell into it anyway, twice. A warning in a
comment beside the correct helper did not stop a second implementation of the wrong thing; only a
grep for `formatDateTime` with `%M` across the tree would have, and that sweep now finds exactly
these two sites and nothing else. The other `%M` hits in `utils/ca-soak` are Python `strftime` and
shell `date`, where `%M` genuinely means minutes.

**Also worth noting for anyone reading a report:** the exception text was never hidden — it sat in
the verdict's `note` field all along. I first read `detail`/`evidence`, found them empty, and
concluded the verdicts carried no reason. The `Verdict` record's fields are `name`, `expected`,
`observed`, `note`. Read `note`.

## `[s23-idle-baseline-measures-the-telemetry]` An idle CAS pool's background allocator is system-log flushing, not CAS {#s23-idle-baseline-measures-telemetry}

**Measured 2026-08-31, using the `Memory` trace collection enabled the same day.** Fifteen idle
minutes on an empty pool, all tables dropped, the trace aggregate scoped to the window itself.

**RSS did not accumulate:** +19 MB drift inside an oscillation of roughly plus or minus 80 MB
(1,033 to 1,193 MB). **No CAS frame appears in the background allocation top at all.** What does
appear is an INSERT — `MergeTreeSink::consume` into `MergeTreeDataWriter::writeTempPart`, 1,705
samples — and `system.part_log` names the destinations: about 1,055 inserts across eight system log
tables in fifteen minutes, `metric_log` alone accounting for 43.98 MiB, `trace_log` for 88,215 rows.
An idle server is busy writing telemetry about itself, and part of that is self-inflicted: the 10 ms
query profiler in `configs/profiling.xml` plus the memory sampling produce the very `trace_log` rows
whose flush then allocates.

**This reframes S23's verdict rather than settling it.** A verdict named "memory flat over idle
window" is, at this profiling configuration, mostly measuring the cost of observing the server —
roughly 176 MB/hour of system-log data before any CAS work exists. Either the threshold accounts for
that churn, or the scenario quiets the profiler for the duration of the idle measurement. Measuring
CAS's idle cost through an amplified telemetry path measures the wrong thing.

**What this probe does NOT establish, and the reason is that it is not S23's experiment.** It began
at 1,102 MB, which is where S23 *ended* (1,178 MB), because a write workload had run before the
tables were dropped. S23 began at 677 MB on a freshly booted server and climbed. So the two runs
converge on the same plateau by different routes, and the plateau is stable — but that the climb from
677 MB stops there is plausible, not shown. A fresh boot idled for 60-plus minutes, read as a curve,
is what would show it. Until then nothing here may be recorded as a leak, and nothing may be recorded
as warm-up either.

## `[soak-retry-budget-turns-a-503-into-a-livelock]` A 500-attempt retry budget converts RustFS's read-concurrency limit into an indefinite stall {#soak-retry-budget-livelock}

**Observed live 2026-08-31 while verifying an S21 fix; the merge sat at progress 0.11 having read
20.67 MiB of 1.10 GiB, and advanced 0 bytes in 40 seconds.** Every link below is measured or quoted,
because two earlier explanations of mine were not.

- RustFS says exactly what is wrong, 3,884 times in twelve minutes:
  `SlowDown: disk read concurrency limit reached, please reduce your request rate`. It is a
  **concurrency** ceiling, not a volume problem — the store answers a direct probe in 1.2 ms while
  refusing the merge's reads.
- The client does not pace itself: `s3_max_get_rps` and `s3_max_get_burst` are both **0**.
- The retry budget is **500** (`s3_retry_attempts`), and the log shows `Attempt 19/501`.

So a persistent 503 becomes neither success nor failure. Every thread keeps retrying, concurrency
therefore never falls back under the ceiling, and the operation cannot finish or give up. 104 threads
were parked in `RetryRequestSleep` at once.

**The failure mode is already named in this repo, for a different trigger.** `configs/storage_conf.xml`
carries a B187 comment describing "the 500x5s retry storm that wedges the merge finalize", caused by
rustfs closing mid-body on a conditional PUT, and mitigates *that* path with
`expect_continue_min_bytes = 65536`. The read path has no equivalent mitigation, so the same wedge
returns through the concurrency limit. **Two workarounds interacting: a retry budget raised to survive
one rustfs defect turns a second rustfs behaviour into a livelock.**

**Fix, in the order the evidence supports it.** Set `s3_max_get_rps` so the client stays under the
store's ceiling — this is precisely what the error message asks for, and it is a rig configuration
gap, not a product defect. Then reconsider whether 500 attempts is right: a budget that large cannot
distinguish "retry until the transient clears" from "wait forever", and a persistent, self-caused 503
deserves to surface as an error long before attempt 500. Both are wider than S21 — every scenario
doing concurrent reads at scale is exposed.

**The ceiling IS configurable, and raising it fixes the stall.** `RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS`
is the knob — found by extracting `RUSTFS_*` names from the `1.0.0-rc.3` binary, alongside
`RUSTFS_OBJECT_DISK_PERMIT_WAIT_TIMEOUT`, `RUSTFS_OBJECT_DISK_DEGRADED_READ_CAP`,
`RUSTFS_OBJECT_DISK_READ_TIMEOUT` and `RUSTFS_OBJECT_DISK_WRITE_ABSOLUTE_CAP`. The binary also carries
the write-side twin of the message, `foreground write concurrency limit reached`.

Measured, not assumed:

| ceiling | outcome for the same 1.1 GiB seven-part merge |
|---|---|
| default | never completes: 20 MiB read, 0 bytes written across a 40 s window, 3,884 503s in 12 min |
| **256** | **completes**: `system.part_log` shows `MergeParts` at 1,211.7 s with `error = 0` |

`configs/rustfs.env` is set to 256 for that reason. A trial at 1024 was botched — rustfs was restarted
mid-merge, destroying the experiment — so 1024 carries no evidence and is not used.

**Two things this does not settle.** 503s still occur at 256 (579 in three minutes) while the host is
completely idle: load 0.82, iowait 0%, on NVMe. So the ceiling is reached for reasons other than
physical disk saturation, and what those are is unknown. And the client still does not pace itself at
all, so every refusal becomes a long wait rather than an error — `s3_max_get_rps` remains the other
half of the fix.

**A hypothesis I raised and then withdrew, recorded so nobody re-runs it:** I proposed a permit leak in
rustfs, on the grounds that progress came in a burst after each restart and then appeared to stop.
The merge's completion refutes it. What actually happened is that the merge was slow and *uneven*, and
I sampled a single 90-second window that fell in a pause, then concluded throughput was zero. Read a
curve, not one interval.

## `[renewal-gives-up-with-budget-left]` A renewal stopped with 1,969 ms of confirmed budget unspent, and one attempt burned 23.7 s {#renewal-gives-up-with-budget-left}

**Supersedes an earlier framing of mine that reported known, fixed behaviour as a new defect.** The
first version of this entry said "a single unresolved heartbeat write costs the mount lease, with no
retry" — which is exactly what
[Issue #2244](https://github.com/Altinity/ClickHouse/issues/2244) diagnosed on 2026-08-20 and whose
minimum fix landed **2026-08-24** (`docs/superpowers/specs/2026-08-23-cas-mount-renewal-retry-design.md`),
with focused and full TLA+, Release/Debug, proxy-integration and 15-minute S39 gates. Filing it again
as novel was a duplicate.

**Observed 2026-09-01, S03 at `--scale full`**, which then failed with `Code: 210 ... mount lease not
held`. Read off `system.ca_event_log`:

| field | ch1 | ch2 |
|---|---|---|
| `attempts_sent` | 1 | **1** |
| `elapsed_ms` | **23,755** | 18,031 |
| `remaining_confirmed_budget_ms` | 0 | **1,969** |
| `unresolved_reason` | `deadline_mid_way` | `deadline_mid_way` |
| `classification` | `external_lease_deadline` | `external_lease_deadline` |
| `stop_cause` | `continue` | `continue` |

**What is NOT a defect, on the current design.** The implemented protocol retries "within the time
still justified by its last confirmed lease". ch1 had **zero** budget left, so sending no second
attempt is the design working, not failing.

**What is worth investigating, and both are separate from #2244's original diagnosis.**

**ch2 stopped with 1,969 ms of confirmed budget remaining** and `stop_cause = continue`, meaning
nothing asked it to stop — precisely the window in which the 2026-08-24 fix is supposed to retry.
Either the budget arithmetic, the margin term (`now + margin < confirmed_deadline`), or the loop's
exit condition keeps a retry from being issued when one is still justified.

**A single attempt consumed 23.7 s against a 30 s TTL.** #2244 describes renewal as "one
5-second-timeout `PUT` per 10-second period". An attempt running 23.7 s is nearly five times that
bound, so either the per-attempt timeout is not being applied on this path or the attempt is not the
`PUT` alone. Whatever the answer, an attempt that can eat 79% of the TTL leaves no room for the retry
protocol to help — the budget is gone before the second attempt could be considered.

**Why the write did not resolve** is the storage saturation measured the same night: RustFS reporting
`permits_in_use: 256/256`, 100% queue utilization, answering `503` after ~5 s with its CPU at 0.13%.
A heartbeat is an ordinary write on that path and gets no privilege.

**Downstream, and correct.** The fenced epoch is terminal, so the self-remount re-claims with a fresh
`writer_epoch`, finds the previous epoch's slot un-fenced and not proven dead, and refuses under "no
wall-clock trust" — three `mount_conflict` events with `outcome = live_double_start`, five seconds
apart. That refusal prevents taking a lease from a possibly-live writer on a clock guess. Do not read
those conflicts as the fault. Related: `[issue-2244-remount-retry-follow-up]`, the still-open per-step
remount work.

**Reproduction:** S03 at `--scale full`; does not reproduce at `dev` or `ci`. The ten-second span of
the conflicts is an artifact of when `predown_dump` ran, not the duration of the event.
## `[s05-standalone-repoints-on-the-non-transactional-path]` 1,200 committed refs repointed outside a transaction during sparse-write GC {#s05-standalone-repoints}

**Found 2026-09-01, S05 at `--scale full`** (10,000 tables, one insert each). Sixteen verdicts, zero
anomalies — the card ran cleanly and caught product behaviour, not a harness fault.

`CASRefRepoint == 0 on the non-transactional path` observed **1,200**. The card's own note: "unexpected
standalone repoint of a committed ref during sparse-write GC — investigate which op took the
`repointRef` path".

It does NOT reproduce at `dev` or `ci`, so whatever takes that path needs either the object count or
the sparse-write shape that only `full` produces.

**First question to answer:** which operation calls `repointRef` outside a transaction. The count is
suspiciously close to a per-table figure for a 10,000-table pool, so start by checking whether it
scales with tables, with parts, or with GC rounds. Related: `[part-removal-repoint-waste]`, where
`delete_tmp_*` repoints were measured at ~22% of the writer PUT class — if the same call site is
responsible, these are one finding, not two.

## `[s01-rss-growth-scales-with-the-blob]` RSS growth during a large upload is a fraction of the blob, not a constant {#s01-rss-scales}

**Measured 2026-09-01 across two scales of S01.** At `ci` (512 MiB blob) RSS growth during the upload
was **exactly 0**. At `full` (8 GiB blob) it was **2.228 GiB — 28% of the blob**. Growth tracks the
blob rather than staying flat, so it is not query-pipeline noise.

The verdict passes either way, because its threshold is "growth < blob size" — which would admit 99%
just as happily. It catches full materialization and nothing short of it.

**Where the memory actually goes, from `Memory` trace samples taken over the same run:** 42% in
`SerializationString::deserializeBinaryBulkWithSizeStream` under `MergeTreeReaderWide::readData`, 6%
in `ColumnString::shrinkToFit` under `MergeTreeSequentialSource::generate` inside `MergeTask` — that
is the MERGE reading String columns. Only **7%** falls in the CAS write path (`publishBlob`,
`PartWriteTxn`). So the card's headline verdict, which exists to prove the write path streams,
is dominated by the read side of the merge that builds the part.

**Two things to settle before tightening anything.** Whether the 28% is buffering by design or the
same effect Altinity#2233 reports (RSS growing 0.98 GiB on a 0.50 GiB blob, i.e. ABOVE the blob) —
measuring growth at three blob sizes answers it. And whether the verdict should measure the write path
specifically rather than whole-server RSS, which is a different verdict and needs its own design.

**Caveat on the trace evidence:** 139 samples, with 43% landing in generic thread-pool frames that were
not decomposed. Enough to show where the bulk sits, not enough to apportion precisely.

## `[manifest-cache-by-id-prose-batch]` manifest-cache-by-id: prose and naming batch {#manifest-cache-by-id-prose-batch}

Found by the final whole-branch review of the manifest-cache-by-id work (branch `cas-gc-rebuild`,
`docs/superpowers/specs/2026-09-02-cas-manifest-cache-by-id-design.md`). All prose or a single
identifier rename, none blocking; batched here per the standing batch-prose directive rather than run
as their own fix rounds. Each item names its file, line, the current text, and the exact replacement.

- `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.cpp:450`,
  `CachedPartFolderAccess::prepareEntries`. Current: "No pool HEAD/GET is performed before precommit;
  the promote path re-proves each dependency fail-closed." False: `promote` re-checks no `Materialized`
  leaf and probes no `TrustedManifest` leaf. Replace with: "the promote gate requires a dependency
  proof for every blob leaf and a live precommit owner; it probes no blob, a missing adopted body is
  fsck's to report."
- `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:1210`,
  `createHardLink`'s carry-forward comment. Current: "record a TOKENLESS W-EVIDENCE dep for its blob
  (no HEAD before precommit; promote re-proves it)." Same false claim as the `prepareEntries` one
  above. Replace with: "record a TOKENLESS W-EVIDENCE dep for its blob (no HEAD before precommit; the
  promote gate requires a dependency proof for every blob leaf and a live precommit owner — it probes
  no blob, a missing body is fsck's to report)."
- `src/Disks/tests/gtest_cas_pool.cpp:131`, the `publishPartWithEntries` helper comment. Current: "Each
  Blob entry's body MUST be present at promote: the promote gate revalidates EVERY blob leaf with a
  HEAD and fails closed on an absent body." False: promote's `TrustedManifest` arm issues no probe.
  Replace with: "Each Blob entry's body MUST be present at promote: the promote gate requires a
  dependency proof for every blob leaf and fails closed if one is missing — it does not itself HEAD or
  GET the body."
- `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h:66`, the
  `Freshness::StrictValidate` enumerator comment. Current: "fsck/debug: bypass retained views entirely;
  fresh resolve + validated read." Overclaims: `StrictValidate` now does nothing beyond a `ForceFresh`
  resolve except skip the retained-view cache. Replace with: "fsck/debug: fresh resolve that bypasses
  the retained view cache entirely, populating nothing; otherwise identical to `ForceFresh`."
- `src/Disks/tests/gtest_cas_part_folder_access.cpp:230`,
  `HitPathJournalEmptyAndCheapWhenExplainDisabled`. Current: "Same request oracle as
  `RetainedHitCostsNoRequest` — one cold build, then retained hits." Stale since `5973676fbad`, which
  moved `RetainedHitCostsNoRequest` to five zero totals excluding the cold build; this test still
  asserts `getCount == 1` over the cold build plus hits. Replace with: "One body GET across the cold
  build and five hits; the full no-request oracle is `RetainedHitCostsNoRequest`."
- `docs/en/antalya/cas/operations/troubleshooting.md:28`, the "Stale-looking part metadata" row's cause
  cell. Current: "The part-folder view cache may be serving a retained (not re-validated) view." A
  retained view is validated by manifest id against a fresh resolve on every hit; the snapshot that can
  now outlive an out-of-band change is the manifest decode cache, which the row's fix cell already
  names. Replace with: "The part-folder view cache or the manifest decode cache may be serving a
  snapshot taken before the out-of-band change."
- `src/Common/ProfileEvents.cpp:929`, `CASPartFolderManifestGets`'s description. Current: "Number of
  part-manifest body GET requests used to build or validate folder views. High values indicate cache
  misses or validation work." No GET validates anything now; the counter increments once per manifest
  decode-cache miss. Replace with: "Number of part-manifest body GET requests, one per manifest
  decode-cache miss."
- `docs/superpowers/cas/BACKLOG/performance.md`, `{#hardlink-per-file-forcefresh-head}` (lines
  ~306-322). Under the `✅ CLOSED` banner the body still asserts, present tense, that `ForceFresh`
  "never serves a retained view" and the reader's `HEAD` "is mandatory even on a decode-cache hit" —
  both now false, and the sibling `{#dedup-cache-weight-constant-64}` keeps only its banner under a
  closed heading. Past-tense the first sentence, or trim the body to the banner, to match that sibling.
- `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:1619-1627`
  (`unlinkFile`'s `already_proven` memo) and
  `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h:166`
  (`force_fresh_validated_refs`). The surrounding comments were rewritten from "re-proven" to
  "resolved" while the identifiers still say proven/validated — the memo now saves a fresh RESOLVE, not
  a proof. Rename `force_fresh_validated_refs` to `force_fresh_resolved_refs` and `already_proven` to
  `already_resolved` (or fold into the memo's removal, if that happens first).

## `[cas-unit-test-mutation-battery]` Proposed: a dynamic mutation battery over the CAS unit tests (2026-09-03, deferred by the user) {#cas-unit-test-mutation-battery}

**Context.** The backend request-contract migration (spec
`docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md`, revision 13) rewrote most of
the 142 CAS unit-test files: doubles moved from the legacy verbs to `CasRequests`, one-shot faults were
re-modelled for an engine that retries, counts pinned to the old controller were re-derived. The user
asked whether the tests still test something useful or degenerated into `2 + 2 = 4`. The static half of
that audit (two reviewers, per-test "killing mutation" + verdict DISCRIMINATING / WEAK / TAUTOLOGICAL)
is recorded in `docs/superpowers/cas/2026-09-03-test-vacuity-audit.md`; its ranked mutant lists are the
input to this item. The dynamic half was NOT run — other priorities (user, 2026-09-03).

**Proposal (dynamic half).** Execute the mutants: a mutant nobody kills is a hole in the suite.

1. Merge the auditors' mutant lists into at most 20 distinct one-line production mutants (never a test
   line); one patch file each, with the tests the audit expects to go red.
2. Per mutant, in order: apply → `ninja unit_tests_dbms` in `build_debug` → run
   `build_debug/src/unit_tests_dbms --gtest_filter='CAS*'` (the gate filter is exactly `CAS*`) → record
   ran / passed / failed and the FAILED names → revert and verify `git status --porcelain -- src` is
   empty before the next mutant.
3. Verdict per mutant: KILLED (an expected test is red), KILLED-BY-OTHERS (red, but none of the expected
   tests — name what killed it), SURVIVED (zero reds: name the property no test pins). An abort in the
   debug build counts as killed by an assertion; say so.
4. Commit nothing from the mutants; finish with one clean `CAS*` gate to prove the tree is restored.
5. Output: the mutant table (patch, expected tests, actual reds, verdict), the SURVIVED list with the
   property each exposes, and for every test the static audit called TAUTOLOGICAL or WEAK whether the
   battery confirms or refutes it. Each SURVIVED mutant becomes a test to write, not a note.

**Cost.** ~20 incremental debug builds + ~20 `CAS*` runs, sequential (one builder); roughly 3–4 hours
of a cheap agent's time. Zero risk to the tree if step 2's revert check is honoured.

- [ ] **CAS mount protocols run serially on the startup thread** (found 2026-09-05, analysed 2026-09-06,
  `lane-g/tmp/followup/multidisk/REPORT.md`). One disk's token-stability observation (~TTL + TTL/20 + poll) blocks the
  startup thread while earlier-mounted disks' leases age; renewers are per-disk and start eagerly, so the exposure is the
  serial `DiskSelector::initialize` → `disk->startup()` → `Pool::open` chain. Trigger is gone on ordinary restarts since the
  farewell-window fix; a genuine hard kill of a multi-disk server still opens it. Fix direction: run `Pool::open` per disk on
  a background task and collect futures after the disk loop (touches `DiskSelector`, consult-first). Needs a hard-kill
  integration step (`stop_clickhouse(kill=True)` with two CAS disks) and a spec.

- [ ] **Mount-lease budget: fewer knobs, derived attempt timeout** (2026-09-07, from the PR #2300 run-3 triage). Today
  the operator sets TTL, renew period, `cas_attempt_timeout_ms` and margin, and the connect cap is `min(connect_timeout_ms,
  attempt)`; the mount refuses when `period + 2×(attempt + 2×cap) + margin ≥ TTL` (a disk with `connect_timeout_ms ≥ 2 s`
  under the old defaults). Proposed: (1) `cas_attempt_timeout_ms = 0` = auto = `(TTL − period − margin)/5 − 2×cap`, i.e.
  "five full attempts fit in the renewal window" as a design constant; the validation then becomes tautological; (2) clamp
  the connect cap to the lease budget instead of refusing the mount, WARN once at mount and show the effective cap in
  `system.cas_mounts` and the "budget in effect" log line; (3) document the fencing-latency trade-off (observation =
  TTL + TTL/20 + period/2; GC fence-out = TTL + TTL/20 + period). Interim fix applied: the `test_cas_s3` config keeps `connect_timeout_ms` at 1000 (user chose the smallest change; the default TTL stays 30 s).
- [ ] **GC per-disk thread pools → one server-wide pool** (2026-09-07). `Cas::Gc` owns `read_pool` (gc_read_concurrency
  16, max_free 16) and `GcMetaWriter`'s pool (gc_meta_pool_size 16) for the DISK's lifetime although both are used only inside
  a round: ≈17–32 idle threads per CAS disk, measured 523 threads at 30 inline disks. Do it like `CasBlobUploadPool`: one
  pool initialised once from server settings, per-disk settings keep bounding per-round in-flight work; or create the pools
  per round. Interim fix applied: `SYSTEM CAS FORGET` in every stateless test that creates an inline disk.
- [ ] **ASan memory ceiling on stateless lanes = thread count × fake stack** (2026-09-07). Resident set grows with live threads
  (fake stacks, `max_uar_stack_size_log` 20 → 11 MB/thread); `detect_stack_use_after_return=0` fixes it but weakens
  detection; `max_uar_stack_size_log=18` is the conservative alternative (E2 not finished). Prefer cutting threads first
  (FORGET, shared GC pool, review `background_schedule_pool_size=512` in the test config).
- [ ] **Sanitizer CI lanes: a smaller thread-pool profile for the CAS stateless stand** (2026-09-07, run-3 triage).
  Measured throughput of the CAS lane under MSan is 0.5 k tests/h against 2.0 k/h for the regular S3
  lane with the same sanitizer (TSan CAS < 0.9 k/h vs 2.0 k/h; ASan CAS 4.2 k/h, fine), so the msan
  and tsan CAS shards never fit the 6 h job budget. The thread census points at pools, not tests:
  ≈2400 threads at 30 inline disks (BgSchPool 508, IOWriter 165, CAS per-disk GC pools 523, …).
  `tests/config/install.sh` already has an `is_sanitizer_build` branch and a `--cas-s3-storage`
  branch; add one `config.d/cas_sanitizer_pools.yaml` linked only when both hold, with a short,
  explained key list: `threadpool_writer_pool_size` 500 → 64–100, `background_schedule_pool_size`
  512 → 128–256 (renewal and cleanup tasks live there; not below 128), and on the default CAS disk
  `cas_gc_read_concurrency` / `cas_gc_meta_pool_size` 16 → 4 (inline disks created by tests do not
  inherit these — there is no server-wide default for disk settings — so `SYSTEM CAS FORGET` stays
  the lever for them). `max_thread_pool_free_size` is not a lever: local pools built on the global
  pool count as busy (E1 with 50 changed nothing). Order: FORGET first (landed, run 4), this profile
  second, resharding (tsan ≥ 4, msan ≥ 6, plus the harness's own 4 h budget) only as insurance.
  Verify by the same `system.stack_trace` census per thread group, not by job wall time.
  Alternative idea, recorded for completeness: give idle pool threads a bounded lifetime so a pool
  shrinks between bursts and re-creates threads on demand. ClickHouse's `ThreadPool` never retires an
  idle worker unless `threads.size() > min(max_threads, scheduled + max_free_threads)`, and local
  pools pin their workers for the pool's lifetime, so this would be a change to the shared
  `ThreadPool` (idle timeout + shrink) outside the CAS tree, with wide blast radius and its own
  warm-up cost per burst; probably too much for the gain, and the per-disk pools would be better
  removed altogether (see the shared GC pool item above) than made shrinkable.
- [ ] **`deleteFilesFromS3` per-key error classification loses the error class** (2026-09-08, found while fixing the CAS batch delete).
  `src/IO/S3/deleteFileFromS3.cpp` classifies each per-key error of a `DeleteObjects` reply with
  `S3ErrorMapper::GetErrorForName` alone, which knows only S3-specific names (`NoSuchKey`, `NoSuchBucket`, …);
  a service-wide name such as `AccessDenied` comes back `UNKNOWN`. Message and code text stay right, only the
  `S3Errors` enum callers may match on is wrong. The CAS path got a two-step lookup (S3 mapper, then
  `Aws::Client::CoreErrorsMapper`, the same order `S3ErrorMarshaller::Marshall` uses) in 13bf6a92df0; the generic
  path is shared upstream code, out of the CAS PR's scope → separate small fix, upstream-worthy.
- [ ] **CAS gtests: ~92 more PoolConfig hooks capture test-frame locals by reference** (2026-09-08). ASan caught one
  (`gtest_cas_ref_writer.cpp:2828`, fixed in a726e933419: a Pool outlives its test frame through a background
  publish's `shared_from_this()`, deferred teardown calls `boot_ms_fn` on a dead local). Census of the same
  `_fn = [&` shape: gtest_cas_pool.cpp 51, gtest_cas_mount.cpp 23, gtest_cas_writer_duties.cpp 5,
  gtest_cas_ref_recovery_cas_walk.cpp 4, gtest_cas_observability.cpp 2, gtest_cas_retirement_sweep.cpp 2,
  gtest_cas_ref_snapshot_publish_ordering.cpp 2, gtest_cas_detached_work.cpp 1, gtest_cas_event_log.cpp 1,
  gtest_cas_gc_ack_floor.cpp 1. Each site needs a read (is the value mutated after the hook is installed, by whom);
  over half of the ref-writer sites needed shared state, not a by-value capture. One task per file; run the suite
  5× under ASan after each. Alternative that removes the class: make `Pool` teardown not call config hooks (snapshot
  the clock values it needs at construction), then the capture shape stops mattering.

## `[version-metadata-reload-fail-closed-when-record-vanished]` Reload of `txn_version.txt` synthesizes a non-transactional record when a previously stored file has vanished (2026-09-17, follow-up of the transaction metadata retry) {#version-metadata-reload-fail-closed}

**Where.** `VersionMetadataOnDisk::loadMetadata` (`VersionMetadataOnDisk.cpp:86-93`): when neither `txn_version.txt` nor its `.tmp` exists, the part gets non-transactional metadata (empty `removal_tid`, `NonTransactionalCSN`). That is correct for a part that never had the file and wrong for a part whose file was stored before and is gone.

**How it can vanish.** `ReplaceFileOperation` of the object-storage metadata backend (`MetadataStorageFromDiskTransactionOperations.cpp:428`) moves the destination aside, then the replacement in; if the second move and its undo both fail, the destination is lost. Today: the server terminates in the callback, restarts, and reloads the part as non-transactional (silently wrong, but every running snapshot died with the process). With the bounded retry from `2026-09-16-transaction-metadata-store-best-effort-design.md` (rev.3e, §5): the retried `setAndStoreRemovalTID(EmptyTID)` reloads the synthesized record, finds the value equal and returns; running snapshots survive, and a snapshot that predates the part's creation can see the part after a re-attach. Found by the codex xhigh review of the branch (`lane-g/tmp/txn_meta_store/branch_review.md` #1, still open in `fixwave_review.md`).

**Fix shape.** Fail closed on reload: if the in-memory info has `storing_version > 0` (a record was stored before) and no file is found, throw (`CORRUPTED_DATA`-class, not `LOGICAL_ERROR`, since it is input-reachable) instead of synthesizing; the retry helper then rethrows after its budget as for any other persistent error, and the load path surfaces the corruption. Needs a fault test for "replacement and undo both fail" on the metadata storage. Generic MergeTree code, upstream-portable; separate PR after the retry branch lands.

## `[otel-demo-s3-budget-audit-2026-09-25]` otel.demo CAS S3 budget audit: twelve ranked findings on repeated and unnecessary work {#otel-demo-s3-budget-audit-2026-09-25}

Report: `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md`. Spec that absorbs the GC-side items:
`docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md` (A0 = F3, A4 = F4, verification F6/F8, open
questions F5/F7/F2). Stand: 2.3M PUT, 6.4M GET, 384k LIST per day on one replica for 43 MB/day of user data; 86% of
parts are `system.*` log tables on the CAS disk (F1, deliberate on the stand, a docs note for production); `delete_tmp` repoints 191k/day (F2,
`[PART-REMOVAL-REPOINT]`, now with a full cost line); carried condemned rows never persist `marker_confirmed` (F3,
one-line GC fix); one manifest body GET per edge with no per-round reuse (F4, 29% of GETs); `_ckpt` PUT per flush (F5,
21% of PUTs, owner decision); ~3 S3 LIST requests per 1000-key page (F6, verify); six GC requests per garbage blob (F7).
