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

## Ref-ledger follow-ups from the two-model adversarial consult (2026-07-21) {#ref-ledger-consult-followups-2026-07-21}

Consult-flagged, controller-verified, deliberately deferred with measurement/design gates. Evidence:
`docs/superpowers/reports/2026-07-21-reftablestate-experiments.md`, `tmp/consult-gpt56sol-answer.md`.

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
already queued pre-release ({#detached-pool-outlives-context}, `final-checks-todo.md` item 10): an undrained
publisher is what lets a detached task be the last `Pool` owner. The wait-loop pattern to transplant already
exists twice in the same file (`:1838`, `:4319`), so the fix is a transplant with a `wait_budget_ms` bound rather
than new machinery. P1 as part of that chain.

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
