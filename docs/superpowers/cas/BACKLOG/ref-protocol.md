---
description: 'Live backlog — ref protocol: rev.6 lease-boundary exclusivity, the ref-lane state machine, and ref-ledger internals.'
sidebar_label: 'Ref protocol'
sidebar_position: 1
slug: /superpowers/cas/backlog/ref-protocol
title: 'CAS Backlog — Ref protocol'
doc_type: 'guide'
---

# CAS Backlog — Ref protocol {#ref-protocol}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for the ref-lane/ref-ledger
protocol: rev.6 lease-boundary exclusivity, ref-log recovery, and the ref-lane state machine.

## Rev.6 lease-boundary exclusivity (highest-priority open design) {#ref-protocol-rev6}

- **[MOUNT-CLAIM-EPOCH-REGRESSION] should `claimMount` permit epoch regression?** — QUESTION — `claimMount` (`CasServerRoot.cpp:787`) reclaims a same-uuid body that is `gc_fenced`/clean-marked/proven-dead by comparing NOTHING but the epoch's inequality, so a fenced twin holding a HIGHER `writer_epoch` is reclaimable while a fresh writer proceeds with a LOWER one — an epoch regression at the mount claim. `prev_epoch_seal` is required iff `writer_epoch > life_epoch`, so a regressed writer may skip the seal obligation; confirm that path cannot readmit a Late-Predecessor window. Decide: gate the claim on a fresh `writer_epoch` above the reclaimed body's (new `MountClaimResult` field), or accept the regression as benign under the seal grammar.
- **[refsnaplog Phase 2] measured ref-log/snapshot optimizations** — DESIRABLE, measurements-gated — inline zero-byte log keys; GC-side fallback compaction for never-mounted tables; indexed/chunked multi-object snapshots; lazy snapshot blocks + byte-bounded row cache; per-round ref index; streamed snapshot construction; adaptive thresholds; decoded-body reuse; chunked namespace removal. None of these have landed (checked `src/` and every `docs/superpowers/` topic file for each verbatim name — no hits). The cross-epoch fault-injection test this list used to ask for is done: `src/Disks/tests/gtest_cas_list_liar_end_to_end.cpp` (`CASListLiarEndToEnd` suite, 8 cases).
- **[timeout-retry RFC residuals] region-redirect retry can bypass the CAS budget** — PARTIAL — Every CAS subsystem, including the plain-object path (`CasPlainObjects::casPutObject`/`casRemoveObject`), now runs through `CasRequests`/`CasOperation` with `Retry::standard()` (90 s budget) — the old `CasRequestController` and the "still on the disk's default retry policy" residual are gone. What remains open: the AWS SDK's region-redirect retry can bypass `ShouldRetry` when a client is `aws-global` (CAS disks are not `aws-global` today — add a startup guard/probe if that ever changes; no such guard exists yet).

## Ref-ledger internals and lane state machine {#ref-protocol-ledger}

- **[ORPHANED-ADJUDICATION-COMMENT] a comment documents an adjudication its neighbouring code does not perform** — {#orphaned-adjudication-comment} — MINOR — The `mine | successor's seal | foreign` adjudication comment (its narrow `catch`, absorbing only `CORRUPTED_DATA`/`UNKNOWN_FORMAT_VERSION` so a transient decode failure cannot be laundered into a `Foreign`/mount-fencing verdict) sits above `chainLinkFor` (`CasRefLedger.cpp:123-141`), which does no such thing. It belongs above `classifyRefLogOccupant` (`:179-194`), which actually has that `catch`. Take it with the next sweep that touches the file; re-derive the comment from the code rather than deleting it blind. `chainLinkFor` stays in the anonymous namespace, asserted only through `prepareRefChunk`'s validator.
- **[PART-WRITE-RELEASE-SEAM] the `PartWriteTxn`/`PreparedPartWrite`/receiver-guard ownership seam needs its own contract spec** — HARD, user-directed extraction (2026-07-29) — Three layers each with their own abort/retry, an overloaded `isTerminal`, nine scattered proven-no-send exits erased into a generic `NETWORK_ERROR`, false ERROR/WARNING log lines on settled-late releases, no exactly-once emission contract for unproven releases. Extract a standalone spec (single-`attempted`-bit proof channel, destructor-owned last-word emission, severity ladder, marker-sync fix) before relink implementation touches this seam. Partial progress: the single-`attempted`-bit channel exists (`publication_attempted`, `CasPartWriteTxn.h:24-40`); no standalone spec exists yet.
- **[reftxnid-wraparound-guard-missing] `nextRefTxnId` lacks a `UINT64_MAX` wraparound guard** — MINOR — Found during the deep-verification batch (batch-021, cluster C-0514): the sibling counter at `CasRefProtocol.cpp:941` has an explicit wraparound guard; `nextRefTxnId` does not. Add the matching guard or document why it is provably unreachable. (Confirmed 2026-08-04: this is the same finding as orphaned-open cluster C-0514 from the docs-consolidation triage — already tracked, not a separate item.)

## Ref-ledger follow-ups from the two-model adversarial consult (2026-07-21) {#ref-ledger-consult-followups-2026-07-21}

Consult-flagged, controller-verified, deliberately deferred with measurement/design gates. Evidence:
`docs/superpowers/reports/2026-07-21-reftablestate-experiments.md` (deleted in `f5c01e88d01`), `tmp/consult-gpt56sol-answer.md`.

- **Post-durable-PUT allocation window in the ref-lane flush** — folded into the publish-confirm fetch-handoff work and tracked there, not here; this pointer stays only so the finding isn't rediscovered. Two nuances not to lose: the catch's "permanently unreplayable" framing over-claims (the covered region can throw via `MemoryTracker` limits on a durable+applied transaction); wedge resolution followed by flush is a path `BM_FlushInstall` does not model yet — measure before changing anything.
- **Recovery re-runs 3-4 codec passes per snapshot row** (measured, est. 2-3x recovery/GC-rebuild cut) — `stateFromSnapshot` (`CasRefProtocol.cpp:431-432`) still round-trips through `encodeRefTableSnapshot` + `decodeRefTableSnapshot` (hand-built defense) instead of accepting the validated-witness type `decodeRefTableSnapshot` already produces; per-row size helpers then re-encode a third time.
- **`precommits` is a plain `std::set`, deep-copied per state scratch copy** (`CasRefProtocol.h:254`) — bounded only by the ~64 MiB admission byte budget, not the 1,000-op cap; every shipped "O(1) ~58 ns copy" benchmark used a one-precommit fixture, and no P-sweep (1/100/10,000 precommits) exists in `benchmark_cas_ref_protocol.cpp` — only N-sweeps over row count. Do not build a third COW container without running that P-sweep first.
- **GC per-table recovery gate before ref-log fold** (defense-in-depth; mandatory before any multi-writer or rolling-upgrade-skew milestone) — refuted as a live defect today (a single lease-holder cannot mint the fabricated history this would catch), but still worth building before that changes: per-table `recoverRefTable(ns)` before folding new logs, `CORRUPTED_DATA` clamps the table (no cursor advance) rather than aborting the round. Not built yet.

## New findings from the 2026-08-04 orphaned-open triage {#orphan-triage-2026-08-04}

- **[recovery-seal-greatest-applied-gap] recovery can publish a seal without advancing `greatest_applied` to it** — DESIRABLE — Codex review finding: next epoch's first log could use a stale `prev`, letting GC accept a late void log below the cursor. Traced the code: `RefReplayBuilder::applyOne` → `RefTableState::applyTxnInPlace` (`CasRefProtocol.cpp:583,668-687`) advances `greatest_applied` unconditionally after every applied txn, seals included, and recovery's own seal-write arm (`CasRefLedger.cpp:1219-1220`) calls exactly that path. No commit closes the finding outright, so it stays open pending a reviewer's call on whether this trace is sufficient.

## No query-cancellation checks in the CA tree (2031-triage CAS-015) {#no-query-cancellation-checks}

Every CA wait (ref-lane single flight, leader election, namespace recovery, part-folder single flight) is bounded
by the I/O underneath it — the CAS request budget (16 attempts / 90 s), the 120 s `recovery_retry_budget_ms`, or a
mount-fence loss — and the shutdown drain is itself explicitly timed (`CasRefLedger.cpp:1996-2019`, `wait_until` on
a shared deadline, fail-closed) — so "hangs forever" is the wrong framing. What is genuinely missing: not one wait
in the CA tree polls query cancellation, so `KILL QUERY` and `max_execution_time` cannot interrupt a query parked
behind a slow leader, and several bounded operations in sequence can still add up to minutes. Still entirely
unaddressed (no `isCancelled`/`QueryStatus` reference anywhere in the CA tree).

Fix: thread a cancellation callback (the standard `isCancelled`/`QueryStatus` poll used elsewhere in the read path)
into the waits that run on a query thread — read/single-flight first, ledger waits after.

## Part-folder single flight keyed by ref only, no post-wait manifest check (2031-triage CAS-019) {#part-folder-single-flight-manifest-keying}

`PartFolderAccess`'s single-flight key is `ns+ref` (`cacheKey()`, `PartFolderAccess.h:34`) and a follower never
re-checks that the view it receives (`future.get()`, `.cpp:255-256`) is for the manifest id it itself resolved. Not
the mixed-manifest hazard an earlier audit claimed: single flight covers only the stale-tolerant `CachedForLoad`
mode, every view is an internally consistent single-manifest snapshot, and all read-after-write paths use
`ForceFresh`. The real consequence is a one-repoint skew: a follower straddling a repoint can get the neighbouring
manifest's view and surface a spurious `FILE_DOESNT_EXIST`.

Fix: either key the single flight by `ns+ref+manifest_id`, or add the cheap post-wait check (compare the served
view's manifest id against the resolved one, re-resolve on mismatch). The second is smaller and keeps the sharing
benefit. P2.

## Allocation in `noexcept`/destructor paths under a memory limit (2031-triage CAS-018) {#noexcept-allocation-hardening}

The audit's headline (leadership leaked out of the ref queue on a throw) was closed separately (release is a
single unconditional authority, `CasRefLedger.cpp:2019` area; the historic stranded-item bug was fixed in
`79c07d6cc3d` with regression tests in `gtest_cas_ref_lane_exception_safety.cpp`); its "renewal fences the mount"
sub-claim is false (the write is wrapped in `try`/`catch (...)`, `CasServerRoot.cpp:1613-1624`). What remains is
hardening nits: `GcPhaseTimer`'s destructor (`CasGcPhaseTimer.h:54-75`) still builds a `GcPhaseRecord`, moves a
`std::map`, and does per-counter `String`/`emplace` work unconditionally, outside its one `try`/`catch` (which
wraps only the sink call) — same class of issue in `CasProbe.cpp`'s cleanup lambdas, `CasMountRuntime.cpp`, and a
fail-closed branch in `CasRefLedger.cpp`. P3: wrap or pre-size, no behaviour change intended.

## Ref-lane residuals from 2031-triage CAS-017 {#lane-residuals-2031-cas-017}

The audit's "a transient backend error leaves the table permanently unusable" is refuted (the pinned reopen test
`PredurableCatalogReadFailureReopensExactLiveLane`, GC reclaim of `Removing` per {#cas-join-set-truncate}, and a
latch that lives only in memory so a restart clears it). Two genuine residuals remain:

- **Read path fabricates absence instead of retry-later** — inside the removal-latch window `acquireReadableRefTableRuntime` (`CasRefLedger.cpp:614-634`) returns `nullptr`, which `resolveRef` (`:285-298`) renders as "no such ref", while `appendRefOps` (`:2068`,`:2102`) throws the retry-later class for the identical state. A reader should not see "absent" for "temporarily not admitting".
- **One `Faulted` arm never fires the anomaly policy** — the "occupant unreadable" arm (`:3793-3823`, `ProfileEvents::CASRefAppendOccupantUnreadable`) sets `Faulted` without invoking `on_impossible_interference`, unlike the sibling `occupant != Occupant::Ours` arm (`:3849-3868`) a few lines below it. That lane alone has no automatic remount and needs an operator.

### [cas-join-set-truncate] `StorageJoin`/`StorageSet::truncate` throw retry-later, self-healing, on a CAS disk {#cas-join-set-truncate}

`StorageJoin::truncate` and `StorageSet::truncate` call `disk->removeRecursive(path)` then immediately
`disk->createDirectories(path)`. On a content-addressed disk `createDirectories` is a pure admission
no-op (`ContentAddressedTransaction::createDirectory` never touches the catalog), so the real re-mint
happens lazily on the first write after `TRUNCATE` returns — that write resolves the namespace through
`CasRefLedger::namespaceLife`.

**Verdict: TRANSIENT, not permanent.** A unit-level test
(`CASRefWriterNamespaceRemoval.FilesOnlyNamespaceTruncateThrowsRetryLaterUntilGcReclaimsThenRebirths` in
`src/Disks/tests/gtest_cas_ref_writer.cpp`) reproduces the exact sequence — birth a files-only
namespace life (the shape `StorageJoin`/`StorageSet` tables use, no MergeTree part ever published),
`dropNamespace` it (the `removeRecursive`-shaped call), then immediately call `namespaceLife` again on
the same name. It throws a typed `NETWORK_ERROR` ("CAS namespace … is Removing: creation waits for its
terminal fold and catalog removal to complete; retry later"), because the catalog row is still
`Removing` until a GC round actually deletes it. After draining GC (two rounds, same shape used
throughout this test file), the identical call mints a fresh incarnation and writes succeed normally —
self-healing, no operator action required.

Practically: `TRUNCATE` on a `StorageJoin`/`StorageSet` table backed by a CAS disk completes without
error (`removeRecursive`/`createDirectories` do not themselves touch `namespaceLife`), but the very next
write to that table (the next `INSERT`, or backup rewrite) throws a retry-later error until the
background GC round reclaims the just-removed row — a window bounded by GC round latency, not by
anything the client controls. A client without retry-on-`NETWORK_ERROR` will see the write it issues
right after `TRUNCATE` fail; retrying it (or simply waiting for the next GC round) succeeds.

**Before the `existsDirectory` fix** (the `DirShape::TableDir` cleanup-completeness probe), the same
`TRUNCATE` was silently a no-op on these engines: `existsDirectory` never reported the directory present
in the first place (it only answered "has at least one committed part", and these engines never publish
one), so `removeRecursive` was skipped entirely and the table kept its old contents. This is a change of
which wrong thing happens on `TRUNCATE`, not a newly introduced break: the old behavior silently ignored
the user's `TRUNCATE`; the new one executes it and imposes a bounded retry-later window on the following
write.

**Direction, not a fix here.** A real fix belongs in the CAS layer's rebirth semantics — either give
`namespaceLife` a fast, non-error path for "predecessor is provably terminal, just needs its row
folded" instead of forcing every caller through the GC-latency retry-later window, or have
`StorageJoin`/`StorageSet::truncate` itself wait for the removal to fully settle before returning
(mirroring `DROP TABLE ... SYNC`'s own synchronous-completion contract) rather than leaving the very next
write to discover the window. Out of scope for the fix-verify pass that found this; tracked here as a
usability rough edge, not a correctness defect.

## The debug/sanitizer body-counter cross-check restores the O(K·N) replay it was meant to avoid (2031-triage CAS-054) {#debug-body-counter-assert-on-replay}

`RefTableState::debugAssertBodyCounters` recomputes body-byte totals and `owned_manifests` membership from scratch
and still runs unconditionally under `DEBUG_OR_SANITIZER_BUILD` on every `applyTxnInPlace` (`CasRefProtocol.cpp:585`)
and every `admits` preview (`:730`) — two extra row re-encodes per committed ref plus two per precommit, on top of
the incremental O(1) counters it is cross-checking (`b5f448e9b41`, `13ab814869c`). Shipped builds are unaffected;
this is purely a debug/sanitizer-build cost, but it puts the in-place-apply comment's stated `O(K+N)` back to
`O(K*N)` for exactly the builds the soak and correctness runs execute. Cheapest honest fix: sample it (every apply
on `admits` previews; on `applyTxnInPlace` only on the first apply after an install, or under an explicit
test-only flag). Not correctness-affecting either way; the assert itself must not simply be deleted.

## `precommitAdd` has no guard against a second call on the same `PartWriteTxn` (2031-triage CAS-072) {#precommit-add-single-slot-guard}

`PartWriteTxn` carries exactly ONE precommit binding (`precommit_target_ns`/`precommit_final_ref`/
`precommit_manifest` + `precommit_state`, `CasPartWriteTxn.h:383-385`), and every consumer (`abandon`, the
destructor cleanup duty, `cleanupStagedManifestDebrisBestEffort`) assumes it. `precommitAdd`
(`CasPartWriteTxn.cpp:702`) still unconditionally overwrites the triple on every call with no guard — a second
call would silently orphan the first binding (its manifest body writer-deleted while its precommit stays live in
the ref log). No production path calls it twice today (`publishStaging`'s two call sites are mutually exclusive
branches, a failed commit refuses to retry, `prepareEntries` mints a fresh build per handle) — latent, not live.
Cheapest fix: reject a `precommitAdd` whose `precommit_state != NotAttempted` with `LOGICAL_ERROR`, or replace the
single triple with a container all three consumers iterate if a build ever legitimately needs two bindings. Also
worth deciding while in there: an idempotent re-add (the already-committed no-op arm, `:754-774`) still sets
`precommit_state == Durable` (`:801`) even though this build never became the precommit owner, so a subsequent
`abandon` (the `Durable`-strict removal at `:1124-1131`) appends a removal for a binding that never existed and
fails loudly — fail-closed, but noisy for a path the code deliberately supports.

## `noexcept` ref-drop helpers allocate outside their `try` (opus review NV-5) {#noexcept-ref-drop-allocates}

`dropRefBestEffort` (`PartFolderAccess.cpp:598-615`) and `dropRefIfMatches` (`:618-676`) are both `noexcept` and
both still call `eraseView` AFTER/outside their `try` blocks. `eraseView`'s first line builds
`key.cacheKey()` (`.h:34`, a string concatenation) unconditionally, so a `bad_alloc` there terminates the process —
exactly on the rollback path taken under memory pressure, where allocation is most likely to fail. Two-line fix:
move the `eraseView` calls inside the existing `try`. P2.

## Shutdown drain ignores pending snapshot publishes (opus review NV-9) {#shutdown-drain-misses-snapshot-publishes}

`drainRefLanesForShutdown` (`CasRefLedger.cpp:1979-2032`) still waits only on `pending`/`leader_active` and checks
`lane_state` — never on `pending_snapshot_publishes`. This is the mechanical confirmation of the B3+B4 chain
already queued pre-release ({#detached-pool-outlives-context}, `final-checks-todo.md` (deleted in `1dc22709a49`, item 10)): an undrained
publisher is what lets a detached task be the last `Pool` owner. The wait-loop pattern to transplant already
exists twice in the same file (`:1838`, `:4319`), so the fix is a transplant with a `wait_budget_ms` bound rather
than new machinery. P1 as part of that chain.

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

## Part publish leaves the common path with zero `GET`s (owner decision 2026-09-25) {#part-publish-zero-gets}

Owner decision, confirmed against the code: a namespace is node-owned, so the per-flush catalog `GET`
(`commitRefChunk`'s `positive_append` branch, `CasRefLedger.cpp:3425`) guards only a race this same process already
closes in-process, and can be dropped from the common path; the `_ckpt` `GET` (`publishCkpt`'s `readModifyWrite`)
races only this process's own snapshot publisher and can become an in-memory etag cache with re-read on 412 only;
`PartWriteTxn::promote`'s manifest re-read should run only when the staging PUT ended `Unresolved`; the post-commit
folder-view rebuild should seed from the manifest bytes already in memory. Not implemented yet — all four `GET`s
are still present at the cited sites. Result when done: a flush goes from four round trips to two, roughly halving
a part publish's 389 ms. Full analysis and the owner's verified-safe reasoning:
`docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f31`;
tracked in `docs/superpowers/cas/umbrella-roadmap.md` §2 ("Publish a part with zero GETs").

## `[cas-txn-commit-inside-noexcept-aftercommit]` A CAS transaction over a committed part runs inside `noexcept` MergeTree-transaction callbacks; any throw there is a server abort (2026-09-10) {#cas-txn-commit-inside-noexcept-aftercommit}

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

**Status (2026-09-26).** The class fix is tracked as open PR Altinity/ClickHouse#2396 (closes #2344), design
`docs/superpowers/specs/2026-09-16-transaction-metadata-store-best-effort-design.md` rev.3f.

**Related.** `[cas-transient-lease-fence-surfaces-to-clients]` (the same `retrying later` reaching synchronous
callers); `[ref-catalog-write-hotspot]` and Altinity/ClickHouse#2343 (the load that makes lanes not-Ready);
`reference_cas_ci_observability_gaps` (the ASan log kept only 12 frames of the exception stack, the terminate stack
was unsymbolized beyond `terminate_handler`; the chain above was established from the code, the query id and the
`Child process was terminated by signal 6` line). Evidence: lane-g `tmp/pr2300-cicd-watch/run10/asan_cas_2of2_att2.err.log`
(line 131680 ff.), `tmp/round9/review/abandon{,2,3}.md`.

## `[cas-transient-lease-fence-surfaces-to-clients]` A transient mount-lease fence surfaces to synchronous client calls as `NETWORK_ERROR` (2026-09-04) {#cas-transient-lease-fence-surfaces-to-clients}

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

Related: `[cas-txn-commit-inside-noexcept-aftercommit]`.

### `[pool-meta-algos-used-append-only]` `PoolMeta.algos_used` only ever grows, no removal path {#pool-meta-algos-used-append-only}

LOW-PRI. `PoolMeta.algos_used` only ever grows (CAS-union on new-algo admission,
`Pool/CasPoolMeta.cpp:77-99`), no removal path. Bounded in practice by `BlobHashAlgo`'s enum
cardinality, so likely not worth acting on; recorded so a future algo-proliferation doesn't reopen the
question unexamined.

Source: `docs/superpowers/cas/random/todo.md` item 14 (session TODO, Russian, file deleted by the u22
consolidation pass). Placement is a best-fit judgment call by the applier — `PoolMeta` is a
ref-protocol control object; the source proposal did not state a target file.

### `[entity-tag-grammar-compat-watch]` A compatibility review against entity-tag grammar was flagged but never filed {#entity-tag-grammar-compat-watch}

WATCH. 2026-09-02 session notes flagged "a compatibility review against entity-tag grammar" as
found-but-unfiled, with no further detail recorded. Re-derive scope before acting: likely concerns
whether CAS's exact-token/`ETag` comparisons assume RFC 7232 entity-tag quoting/weak-vs-strong syntax
uniformly across S3/GCS-HMAC/GCS-OAuth backends. Needs the original session's context
(docs-restructure.md/rebuild-branch.md session ids, both deleted, point at `claude --resume
e010c5f5-eaec-4c00-b787-277854921eb6` / `06c1752d-9324-4150-b975-5774418363c8` if those sessions are
still resumable) or a fresh audit.

Source: `docs/superpowers/cas/random/todo.md` item 14 (session TODO, Russian, file deleted by the u22
consolidation pass). Placement is a best-fit judgment call by the applier — token/ETag comparison
grammar; the source proposal did not state a target file.
