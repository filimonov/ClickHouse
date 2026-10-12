---
description: 'TLC evidence for the focused unconditional CAS blob-publication protocol, exact sabotages, and non-vacuity witnesses'
sidebar_label: 'CAS blob publication TLC results'
sidebar_position: 4
slug: /superpowers/models/CaBlobPublishCore-results
title: 'CAS blob publication TLC results'
doc_type: 'reference'
---

# `CaBlobPublishCore` results {#cablobpublishcore-results}

## Verdict {#verdict}

The focused safety gate passed twice from identical final inputs. Both complete runs printed
`ALL EXPECTATIONS MET`: every sabotage violated its exact mapped invariant, the safe configuration
exhausted its state graph without an error, and all three negated-reachability witnesses fired while
the safety invariants remained enabled.

## Checker and finite scope {#checker-and-finite-scope}

- TLC: `2026.07.18.145032`, revision `30cc360`.
- Pinned `tla2tools.jar` SHA-256:
  `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`.
- Workers: `1`.
- Writers: `w1` and `w2`; one GC actor.
- Bounds: two publication attempts per writer, two total landed writes, four metadata versions, and
  two fence generations. `MaxWrites` counts the optional seed as a landed write.
- The safe configuration explores both deterministic ETag tokens and allocation-based generation
  tokens. `sab_reuse_condemned_envelope` uses `Generation` and still fails by direct condemned-
  envelope comparison. The staged-copy sabotages use `ETag` so byte-identical reproduction is
  explicit.
- TLC metadata, traces, and row logs were stored below the repository `tmp` directory.
- The runner passes `-noGenerateSpecTE`, so TLC does not create source-adjacent trace modules.

The model separates durable precommit, `HEAD`, metadata observation, publication selection, backend
landing, response loss, recovery, metadata reconciliation, readiness, commit, fence loss, and exact
deletion. Each writer has a fixed staged envelope and monotonic `publicationAttempted` history.
ETags are a deterministic function of envelope plus payload; generation tokens consume `nextToken`
on every landed write. Freshness after `Condemned` compares the selected envelope directly, not the
provider token. A single queued exact-delete record remains available for retries, which is the
authorization needed by all three staged-copy regressions. A pending old backend attempt can land
in any current writer phase; the late witness requires the retry to land and reach `Ready` first.

The two-write accounting is concrete: the fresh-writer race, both recopy regressions, and the staged
retag witness use no seed and two landings; the corrected late witness uses the retry landing and
then the old late landing; condemned-envelope reuse and the first-condemned regression use one seed
plus one landing. Exact-delete actions do not consume a write. The omitted shape—one seed followed
by two post-seed racing replacements—is a cross-product not required by a named sabotage or witness;
its fresh-writer race and seeded condemnation/delete dimensions are checked separately.

## Safety invariants {#safety-invariants}

The safe run checks these stable names:

- `CommittedRefHasContent`
- `ReadyRequiresObservedMaterialization`
- `CondemnedNeedsFreshPublication`
- `FreshAfterCondemned`
- `PublicationAttemptIsMonotonic`
- `VerbatimCopyOnlyFirstAbsent`
- `ExactDeleteCannotRemoveFreshIncarnation`
- `PublicationRequiresDurablePrecommit`
- `ReadyRequiresCleanMeta`
- `FencedWriterCannotCommit`
- `KeyNamesPayload`

`CommittedRefHasContent` is intentionally logical: a committed reference requires a present body
with the key's payload, but does not require the current incarnation token to equal a writer's prior
observation.

## First complete run {#first-complete-run}

Command output: `build/task1_blobpublish_tla_fix_round1.log`. TLC row-log run ID:
`CaBlobPublishCore-11-1787384973424501810`.

| Configuration | Expected | Exact observed result | Generated | Distinct | Seconds |
|---|---|---|---:|---:|---:|
| `CaBlobPublishCore_sab_adopt_condemned.cfg` | violation | `CondemnedNeedsFreshPublication` | 83 | 49 | 0 |
| `CaBlobPublishCore_sab_reuse_condemned_envelope.cfg` | violation | `FreshAfterCondemned` | 296 | 159 | 1 |
| `CaBlobPublishCore_sab_recopy_after_condemned.cfg` | violation | `ExactDeleteCannotRemoveFreshIncarnation` | 31,563 | 11,762 | 0 |
| `CaBlobPublishCore_sab_recopy_after_absent.cfg` | violation | `ExactDeleteCannotRemoveFreshIncarnation` | 18,953 | 7,233 | 1 |
| `CaBlobPublishCore_sab_first_condemned_then_copy.cfg` | violation | `VerbatimCopyOnlyFirstAbsent` | 7,937 | 2,753 | 0 |
| `CaBlobPublishCore_sab_unconditional_delete.cfg` | violation | `ExactDeleteCannotRemoveFreshIncarnation` | 608 | 284 | 1 |
| `CaBlobPublishCore_sab_ready_without_reobserve.cfg` | violation | `ReadyRequiresObservedMaterialization` | 118 | 65 | 0 |
| `CaBlobPublishCore_sab_publish_before_precommit.cfg` | violation | `PublicationRequiresDurablePrecommit` | 58 | 39 | 0 |
| `CaBlobPublishCore_sab_skip_meta_clean.cfg` | violation | `ReadyRequiresCleanMeta` | 212 | 107 | 1 |
| `CaBlobPublishCore_sab_commit_after_fence.cfg` | violation | `FencedWriterCannotCommit` | 146 | 81 | 0 |
| `CaBlobPublishCore_sab_wrong_payload.cfg` | violation | `KeyNamesPayload` | 56 | 32 | 0 |
| `CaBlobPublishCore_safe.cfg` | green | `green` | 13,270,635 | 2,051,149 | 85 |
| `CaBlobPublishCore_witness_racing_publishers.cfg` | witness | `WitnessRacingPublishers` reached | 890 | 380 | 1 |
| `CaBlobPublishCore_witness_staged_retag.cfg` | witness | `WitnessStagedRetag` reached | 5,159 | 1,809 | 0 |
| `CaBlobPublishCore_witness_late_landing.cfg` | witness | `WitnessLateLanding` reached | 5,344 | 1,853 | 1 |

The safe graph completed at depth `34` with zero states left on the queue. The TLC log reports
`01min 24s`; the runner's whole-second measurement reports `85` seconds.

## Second complete run {#second-complete-run}

Command output: `build/task1_blobpublish_tla_fix_round1_rerun.log`. TLC row-log run ID:
`CaBlobPublishCore-11-1787385081646033514`.

| Configuration | Expected | Exact observed result | Generated | Distinct | Seconds |
|---|---|---|---:|---:|---:|
| `CaBlobPublishCore_sab_adopt_condemned.cfg` | violation | `CondemnedNeedsFreshPublication` | 83 | 49 | 0 |
| `CaBlobPublishCore_sab_reuse_condemned_envelope.cfg` | violation | `FreshAfterCondemned` | 296 | 159 | 1 |
| `CaBlobPublishCore_sab_recopy_after_condemned.cfg` | violation | `ExactDeleteCannotRemoveFreshIncarnation` | 31,563 | 11,762 | 1 |
| `CaBlobPublishCore_sab_recopy_after_absent.cfg` | violation | `ExactDeleteCannotRemoveFreshIncarnation` | 18,953 | 7,233 | 0 |
| `CaBlobPublishCore_sab_first_condemned_then_copy.cfg` | violation | `VerbatimCopyOnlyFirstAbsent` | 7,937 | 2,753 | 1 |
| `CaBlobPublishCore_sab_unconditional_delete.cfg` | violation | `ExactDeleteCannotRemoveFreshIncarnation` | 608 | 284 | 0 |
| `CaBlobPublishCore_sab_ready_without_reobserve.cfg` | violation | `ReadyRequiresObservedMaterialization` | 118 | 65 | 0 |
| `CaBlobPublishCore_sab_publish_before_precommit.cfg` | violation | `PublicationRequiresDurablePrecommit` | 58 | 39 | 1 |
| `CaBlobPublishCore_sab_skip_meta_clean.cfg` | violation | `ReadyRequiresCleanMeta` | 212 | 107 | 0 |
| `CaBlobPublishCore_sab_commit_after_fence.cfg` | violation | `FencedWriterCannotCommit` | 146 | 81 | 0 |
| `CaBlobPublishCore_sab_wrong_payload.cfg` | violation | `KeyNamesPayload` | 56 | 32 | 1 |
| `CaBlobPublishCore_safe.cfg` | green | `green` | 13,270,635 | 2,051,149 | 84 |
| `CaBlobPublishCore_witness_racing_publishers.cfg` | witness | `WitnessRacingPublishers` reached | 890 | 380 | 0 |
| `CaBlobPublishCore_witness_staged_retag.cfg` | witness | `WitnessStagedRetag` reached | 5,159 | 1,809 | 1 |
| `CaBlobPublishCore_witness_late_landing.cfg` | witness | `WitnessLateLanding` reached | 5,344 | 1,853 | 0 |

The second safe graph also completed at depth `34` with zero states left on the queue. The TLC log
reports `01min 24s`; the runner reports `84` seconds.

## Counterexample audit {#counterexample-audit}

The underlying TLC traces were inspected, not only the runner summary:

- `sab_recopy_after_condemned` performs an absent verbatim staged copy, backend landing, response
  loss, recovery `HEAD`, a `Condemned` metadata observation with its exact token queued, the forbidden
  identical copy, `Clean` reconciliation, and the old exact-delete attempt that removes the newer
  write serial.
- `sab_recopy_after_absent` performs the same ambiguous first copy, condemnation, an exact deletion
  that leaves its authorization queued, recovery `HEAD` of absence, identical copy, `Clean`
  reconciliation and readiness, then the queued exact-delete retry removes the newer write serial.
- `sab_first_condemned_then_copy` starts from a present body, observes `Condemned`, begins a retagged
  stream, loses the response before landing, enters recovery, lets the queued exact delete remove the
  old body, observes absence, and then lands the forbidden original staged copy. The seed, queued
  authorization, and replacement are all `ETag(1, Content)`, while the write serial changes from
  `1` to `2`; the invariant no longer fires merely at transport selection.

Every other sabotage trace ended at the mapped invariant shown in the tables. No timeout, parse
error, deadlock, wrong invariant, or unrelated TLC error was classified as a pass.

## Witness audit {#witness-audit}

- `WitnessRacingPublishers` records both writers observing the same absent write serial and then
  landing equivalent publications.
- `WitnessStagedRetag` records an absent verbatim staged copy, backend landing, response loss,
  recovery `HEAD`, `Condemned` re-observation, and a fresh retagged stream landing.
- `WitnessLateLanding` records response loss before backend landing, recovery `HEAD`, a retry
  publication landing and reconciling to `Ready`, and only then `LateBackendLand` from the old
  attempt. The old request changes the body after the newer request has become ready.

Each witness config lists all eleven safety invariants before its negated reachability check.
The safe config explores the same honest behavior with both token families to completion.

## Commands {#commands}

```bash
mkdir -p build tmp
docs/superpowers/models/run_blobpublish.sh > build/task1_blobpublish_tla_fix_round1.log 2>&1
grep -q '^ALL EXPECTATIONS MET$' build/task1_blobpublish_tla_fix_round1.log
docs/superpowers/models/run_blobpublish.sh > build/task1_blobpublish_tla_fix_round1_rerun.log 2>&1
grep -q '^ALL EXPECTATIONS MET$' build/task1_blobpublish_tla_fix_round1_rerun.log
```

Both `grep` commands exited `0`. The complete runner output for each run ended with
`ALL EXPECTATIONS MET`. A successful focused invocation instead prints
`SELECTED EXPECTATIONS MET`; it cannot satisfy the full-battery grep gate.

## SDK resend and split observation (CAS-361) {#sdk-resend-cas-361}

Verdict: the current protocol loses data on an ETag store. One SDK resend of an already landed blob
`PUT` or `CopyObject`, followed by a reissued exact-token `DELETE`, deletes a body that a committed
reference depends on. Fix A (no SDK resend for blob publication) closes it in the model with the
exact delete unchanged.

### What changed in the model {#cas-361-model-changes}

New switches; every original config sets them off and keeps its recorded state counts (`safe`:
13,270,635 generated, 2,051,149 distinct; one-worker reruns of `sab_adopt_condemned`,
`sab_recopy_after_condemned` and `witness_late_landing` match the tables above exactly).

| Constant / action | Code | Meaning |
|---|---|---|
| `SdkResend(w, s)`, `MaxResends`, `ResendTransports` | `S3::Client::doRequestWithRetryNetworkErrors` behind `ObjectStorageBackend::publish` (`WriteSettings{}`) and `S3ObjectStorage::copyObject` (`client.get()`) | An envelope that landed once lands again, same bytes, in any writer phase, during or after the call. `LateBackendLand` alone lands each attempt at most once. |
| `ResendFreshTransports` | none (fix B) | A resend on these transports carries a fresh envelope. |
| `DeleteMatchesSerial` | none (fix E) | The exact delete also matches the landing serial (`If-Match` + `x-amz-if-match-last-modified-time`). |
| `GraduationGate`, `GCGraduate` | `confirm_condemned_marker` | `GCDelete` needs graduation; graduation refuses while any non-aborted writer holds a durable precommit. A writer that precommits after graduation is not seen. |
| `SplitObserve`, `Backfill(w)` | `ensureBlobPresent`: `op.head`, then `loadMeta`, then `putMetaIfAbsent` | The marker `GET` no longer rechecks the body (CAS-360, S13). An absent marker is backfilled and adopted without a body check. |
| `RecheckHeadOnBackfill`, `RecheckHead(w)` | none (audit option B) | A second `HEAD` after an absent marker; adopt only on the same token. |

`GraduationGate` is needed for realistic traces. Without it any queued delete may land at any
time, so the writer that resends can also be the one that commits, and the re-`HEAD` fix fails on
a schedule graduation would forbid (a delete between the writer's re-`HEAD` and its backfill while
its precommit is live).

### Results {#cas-361-results}

Bounds for all rows: `ETag` unless noted, `MaxWrites = 3`, `MaxToken = 3`, `MaxEnvelope = 6` (7
for fix B), `MaxResends = 1`, `MaxMetaVersion = 4`, `GraduationGate = TRUE`. Green rows check all
eleven safety invariants; violation rows check `CommittedRefHasContent`. Four workers; the two
`cur_*` traces were re-run with one worker for the shortest trace.

| Configuration | Expected | Result | Generated | Distinct | Seconds |
|---|---|---|---:|---:|---:|
| `cur_resend` (1 worker) | violation | `CommittedRefHasContent`, 20 states | 603,566 | 175,505 | 6 |
| `cur_resend_generation` | green | green | 10,653,538 | 3,116,814 | 31 |
| `fix_A` | green | green | 4,820,529 | 1,275,294 | 14 |
| `sab_A_copy_resends` | violation | `CommittedRefHasContent` | 342,149 | 117,133 | 2 |
| `sab_A_put_resends` | violation | `CommittedRefHasContent` | 1,686,483 | 508,118 | 6 |
| `fix_B` (`MaxEnvelope = 7`) | green | green | 16,402,018 | 4,337,335 | 46 |
| `sab_B_copy_same_bytes` | violation | `CommittedRefHasContent` | 447,815 | 157,927 | 2 |
| `fix_E` | green | green | 12,265,979 | 3,189,844 | 35 |
| `cur_split` (1 worker, no resend) | violation | `CommittedRefHasContent`, 10 states | 1,020 | 490 | 1 |
| `fix_split_rehead` (split, re-`HEAD`, no resend) | green | green | 6,265,133 | 1,618,837 | 19 |
| `sab_split_rehead_resend` | violation | `CommittedRefHasContent` | 893,533 | 263,196 | 3 |

The full battery (original fifteen rows plus these eleven) printed `ALL EXPECTATIONS MET`.

### `cur_resend` counterexample {#cas-361-cur-resend-trace}

1. `w1` precommits, sees the key absent, starts the staged `CopyObject` of envelope 1; the response is lost.
2. `w1` recovers, sees the key still absent; the first copy then lands late (`ETag(1)`, serial 1).
3. `w1` loses its fence and aborts. Its precommit no longer counts.
4. GC condemns `ETag(1)` and graduates. `w2` precommits after graduation and `HEAD`s `ETag(1)`.
5. GC's exact delete `If-Match(ETag(1))` removes the body and the marker.
6. The SDK resends `w1`'s copy: the same bytes land again as `ETag(1)`, serial 2.
7. `w2` reads the marker as absent, reconciles it to `Clean`, becomes ready and commits.
8. GC reissues the same `If-Match(ETag(1))` (`readLoop` reissue, carried `delete_pending`, or a
   deposed leader); it matches and removes the body `w2` committed. Dangling reference.

`w2` never published: it adopted the resurrected body. `sab_A_put_resends` reaches the same end
through a streaming `PUT` (envelope 3, after an earlier copy attempt that never landed).

### `cur_split` counterexample (S13, CAS-360) {#cas-361-cur-split-trace}

GC condemns and graduates the seeded body. `w1` precommits after graduation and its `HEAD` sees the
body. GC deletes body and marker. `w1`'s marker `GET` finds nothing, `putMetaIfAbsent(Clean)`
succeeds, `w1` adopts and commits a reference to a deleted body. No resend is involved.

### Fix verdicts {#cas-361-fix-verdicts}

- A (single-attempt transport for blob `PUT` and `CopyObject`): green. Both halves are load-bearing:
  fixing only the streaming publish or only the copy still violates.
- B (fresh envelope per wire attempt): green, but not implementable. The SDK resends the buffered
  request, and the header is inside the body. A `CopyObject` resend copies the same staging bytes
  even if `PUT` could be re-headered (`sab_B_copy_same_bytes`).
- E (exact delete also matches last-modified time): green with resends left on. Store support for
  `x-amz-if-match-last-modified-time` on general-purpose buckets and RustFS is not verified.
- Generation stores: green with resends on (`cur_resend_generation`).
- Re-`HEAD` before backfill closes S13 only together with A: with resends on, the resurrected body
  carries the same token and passes the recheck (`sab_split_rehead_resend`).

### Not modelled {#cas-361-not-modelled}

- S7 (`NoSuchUpload` on `CompleteMultipartUpload` reported as success when any object exists at the
  key). The loss needs `reconcileMetaClean` to turn a `Condemned` marker `Clean` after the synthetic
  success. `ReconcileMetaClean` here still refuses `Condemned`, a stand-in for graduation that
  predates `GraduationGate`. Modelling S7 needs that guard replaced by the code's compare-swap and
  re-checked against the whole battery.
- Resends outside the SDK (proxy, load balancer, a store applying one request twice) behave like
  `SdkResend` and are not removed by fix A. Only E or a generation store closes them.

## Scope note {#scope-note}

This is a bounded safety proof, not a liveness proof or a provider wire-format test. The two-write
bound covers every named release-gate counterexample and witness. Multipart behavior, S3 copy bytes,
and GCS request semantics remain live-test obligations outside this focused model.
