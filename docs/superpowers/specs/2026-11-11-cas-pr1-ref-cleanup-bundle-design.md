---
description: 'Design for one pull request on antalya-26.8 that carries four related CAS stories as commit series: fresh incarnation per namespace-create attempt (CAS-368, CAS-449), `_ckpt` frontier refusal (CAS-366), `fsck` tail-walk revalidation (CAS-448) and the per-life ref cleanup licence with its counters (CAS-29.8, CAS-29.7).'
sidebar_label: 'CAS: ref cleanup bundle (PR1)'
sidebar_position: 111
slug: /superpowers/specs/cas-pr1-ref-cleanup-bundle-design
title: 'CAS: ref cleanup licence and its three prerequisites in one pull request'
doc_type: 'reference'
---

# CAS: ref cleanup licence and its three prerequisites in one pull request — design {#cas-pr1-ref-cleanup-bundle}

- Date: 2026-11-11. Status: draft, not approved.
- Tasks: CAS-368, CAS-449, CAS-366, CAS-448, CAS-29.8, CAS-29.7. Symptom task: CAS-447.
- Base: `2f89c5db182`, head of PR [#2805](https://github.com/Altinity/ClickHouse/pull/2805)
  (`port/antalya-26.8/cas-from-26.6`). Branch: `fix/antalya-26.8/cas-ref-cleanup-per-life-licence`.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Line numbers are at the base.
- Story D is specified in `docs/superpowers/specs/2026-11-11-cas-ref-cleanup-per-life-licence-design.md`
  ("the licence spec"). This document does not repeat it. It adds the three other stories, the commit
  layout, the gates and the measurements.

## 1. Decision {#decision}

- **One pull request, four stories, ten commits.** Each story is a commit series that reads on its own.
- **Order: A, B, C, D.** A, B and C each remove a hazard that story D makes more frequent
  ([§2](#order)).

| Story | Tasks | Area | What changes |
|---|---|---|---|
| A | CAS-368, CAS-449 | ref catalog | Namespace-create step 1 mints a new incarnation on every decision and never resends unanswered bytes |
| B | CAS-366 | `_ckpt` | `publishCkpt` refuses a frontier that a later writer epoch moved past; callers stop acknowledging it |
| C | CAS-448 | `fsck` | The tail walk revalidates `_ckpt` before it reports an absent log as a broken chain |
| D | CAS-29.8, CAS-29.7 | GC | Ref cleanup is licensed per life; five counters and six phase metrics say why a pass stopped |

- **Every commit builds and passes the `CAS*` gtest gate.** A test and the product change that turns it
  green share a commit. Red-first evidence is a log ([§8](#commits)).
- **No format change, no new setting, no port to `antalya-26.6`.**
- **Models are not re-run** unless a rule in [§4.2](#story-b-change) or in the licence spec §5.1 changes
  ([§11](#models)).

## 2. Why one pull request and why this order {#order}

Story D makes ref cleanup run under catalog churn. On PR 2794 shard 2/2 it deleted nothing for 3 h while the
ledger published about 180,000 `_log` objects (licence spec §2). After D it deletes in most rounds. Three
hazards scale with that frequency:

- **A before D.** The licence of D identifies a life by `(namespace, incarnation)`. It is sound only if an
  erased incarnation is never inserted again. Model config `CaRefCleanupLicenceCore_sab_reuseincarnation`
  shows the violation when it is. Today `createNamespace` can re-insert one (CAS-449).
- **B before D.** A deposed writer's `If-None-Match` create can land in a seal slot that cleanup already
  reclaimed. `publishCkpt` then accepts its frontier and the writer acknowledges a transaction nobody will
  read. Only the lease check stands in the way. More cleanup means more reclaimed slots.
- **C before D.** `fsck` reports a tail log that cleanup deleted between its `_ckpt` read and its tail read as
  a broken chain. More cleanup means more false verdicts.

A, B and C are correct without D. Each is one to three commits. Four pull requests would cost four CI runs
and four reviews for one connected change.

## 3. Story A: a fresh incarnation per namespace-create decision (CAS-368, CAS-449) {#story-a}

### 3.1 Problem {#story-a-problem}

`CasRefCatalog::createNamespace` (`CA/Pool/CasRefCatalog.cpp`) mints the incarnation once, before the
admission loop. `createNamespaceStep1` captures that entry by reference and inserts it on every decision of
`casUpdateImpl`. Two paths insert the same incarnation twice:

- **Resend (CAS-368).** An attempt's fate is unknown. `CasOperation::writeLoop` (`CA/Backend/CasRequests.cpp:928`)
  resends the same bytes: after a connect hint (`:1077`) and after an unresolved but repeatable attempt
  (after `:1123`). If the first `PUT` landed, was cancelled by a sibling (`cancelStalledCreating`) and the
  catalog body returned to its earlier value, the resend revives the cancelled life.
- **Re-decide (CAS-449).** The first `PUT` lands and the response is lost. The settle read sees a different
  body, so `writeLoop` returns `Conflict{any_ambiguous}`. `casUpdateImpl` pauses `Retry::backoff` and calls
  `decide` again. Meanwhile a sibling completes the `Creating` row, the table is dropped, the fold records
  cleanup evidence and the row is erased. The new decision finds the name absent and inserts the same
  incarnation.

### 3.2 Change {#story-a-change}

Port of draft `fix/antalya-26.6/cas-368-fresh-incarnation-per-step1-retry` (`2e0393480a6`, `d42febbcc9b`).
The draft diff applies to the base without conflicts.

- **`Retry` gains two fields** (`CA/Backend/CasRetry.h`):
  - `uint32_t attempts_already_sent = 0`: physical attempts that earlier submissions of the same logical
    operation sent. Only the write path's transport attempt number continues from it.
  - `bool resend_unresolved = true`: when false, `writeLoop` never resends bytes after an attempt whose fate
    is unknown. It returns `GaveUp::Why::Unresolved`. A clean conflict and a credential reissue are
    unaffected. Read paths ignore it.
- **`writeLoop` honours them** at four places: the attempt number passed to `withTransportAccess`, the
  first-attempt fuse test, the connect-hint resend (`:1077`) and the unresolved return (`:1123`).
  `attempt_spacing_ms` needs no change: it only shapes the pause length (`:955`, `:900`).
- **`CasOperation::pauseBeforeRedecide(ms, unresolved, policy)`** is new. It pauses through `gatedPause`, so
  the deadline and the fence are checked exactly as for a reissue. It returns a `WriteResult` when the
  operation must give up.
- **`casUpdateImpl` re-decides after `Unresolved`** when the policy has `resend_unresolved == false` and
  `single_attempt == false`: it adds `attempts_sent` to `attempts_already_sent`, pauses
  `Retry::backoff(++settled_faults)` and loops. The loop reads the catalog again and calls `mutate` again.
- **`createNamespaceStep1` mints inside `mutate`.** Its signature takes `ns` and `creator` instead of an
  entry and returns the entry of the catalog that landed (`*findEntry(landed, ns)`). It sets
  `resend_unresolved = false` on its policy. `createNamespace` no longer mints before the loop, and
  `completeCreation` receives the returned entry.

Minting inside `mutate` closes both paths: every decision, whether it follows `Unresolved` or `Conflict`,
builds a row with a new incarnation.

Adoption stays. If the settle read sees exactly the bytes the attempt sent, `writeLoop` commits by read and
step 1 returns that entry. No second incarnation is minted.

### 3.3 Tests {#story-a-tests}

All in `src/Disks/tests/gtest_cas_ref_catalog.cpp`, suite `CASRefCatalog`, after
`ATrueRefusalCreateNamespaceStep1SeesASuperseder`.

Fixtures (from the draft): `CatalogWriteRecorder` (a `CountingBackend` that records every conditional write
of the catalog key, its transport attempt number, and calls `on_write` before the write reaches the store)
and `Step1Fixture` (pool, external actor `other`, `createA`, `step1Incarnations`,
`expectDistinctIncarnationsAndTheLastOneLive`).

| Id | Test | Red at the base |
|---|---|---|
| S1 | `CreateNamespaceStep1ConflictRetryMintsAFreshIncarnation` | yes |
| S2 | `CreateNamespaceStep1RetryAfterALostResponseMintsAFreshIncarnation` | yes |
| S3 | `CreateNamespaceStep1AdoptsALostResponseThatLanded` | no (guard) |
| S4 | `CreateNamespaceStep1DecidesAgainAfterAConnectHintOrAFirstAttemptTimeout` (`USE_AWS_S3`) | yes |
| S5 | `CreateNamespaceStep1FailsAtOnceWhenItsCatalogReadIsRefused` (`USE_AWS_S3`) | no (guard) |
| S6 | `CreateNamespaceStep1DoesNotPausePastItsDeadline` | no (guard) |
| S7 | `CreateNamespaceStep1FenceLostAfterAnUnresolvedAttemptIsFencedOut` | no (guard) |
| S8 | `CreateNamespaceStep1NeverReinsertsAnErasedIncarnation` (new, CAS-449) | yes |

S8 drives the CAS-449 interleaving:

1. `CatalogWriteRecorder` gets a second callback, `after_write`. It runs once, after the base `write`
   returns or throws, so after a landed write and before the settle read.
2. The test arms `injectAmbiguousLandedWrite(catalog_key)` and calls `createA`.
3. `after_write`, acting as `other`: reads the catalog; takes the `Creating` row of `a` (incarnation `I1`);
   calls `completeCreation` and `beginRemoving` on it; erases the row with `deleteCompletedRemoving`.
   The fold seal with cleanup evidence for `I1` is built as in
   `CASRefCatalog.AStaleHintCreateNamespaceStep1AdmitsANewIncarnation` (`gtest_cas_ref_catalog.cpp:2255`).
   `casUpdate` cannot erase a row: `identityPreserving` rejects a changed row count.
4. Expect: `createA` succeeds; the final row of `a` is `Live` with an incarnation other than `I1`;
   `step1Incarnations` are pairwise distinct; `_ckpt` exists under the key of the second incarnation.
   The sibling already wrote the `_ckpt` of `I1`, so the assertion names the second life's key.

At the base the retry inserts `I1` again and expectation 4 fails.

Must stay green: `CASPool.ConcurrentNamespaceCreationsNeverRaceEachOtherOnTheCatalog`,
`CASRefCatalog.ATrueRefusalCreateNamespaceStep1SeesASuperseder`.

### 3.4 Not closed by story A {#story-a-residual}

- A proxy that duplicates a `PUT`, or an original request still in flight after a full cancel, can still land
  old bytes. Closing that needs a revision field in the catalog body, which is a format change.
- The fix is per server. A server on an older binary keeps the old behaviour.
- CAS-368 acceptance criterion #2 (a model that tracks the catalog value and a late apply) stays open.

## 4. Story B: refuse a `_ckpt` frontier that a later epoch moved past (CAS-366) {#story-b}

### 4.1 Problem {#story-b-problem}

A deposed writer can acknowledge a ref-log transaction that no recovery and no fold will read:

1. Writer epoch 1 is admitted and prepares an append at slot `N`.
2. Its successor (epoch 2) recovers, seals epoch 1 below `N` or at `N`, and moves the frontier on.
3. Ref cleanup reclaims the covered seal slot.
4. The epoch-1 `If-None-Match` create lands in the reclaimed slot.
5. `mergeCommittedThrough` compares frontiers numerically and accepts the epoch-1 contribution, or
   `publishCkpt` returns `IdenticalSkip`. The writer acknowledges.

Today only the lease check prevents step 5.

### 4.2 Change {#story-b-change}

Port of draft `fix/antalya-26.6/cas-366-superseded-ckpt-contribution` (`2d2182b3b19c`, 11 files). The draft
conflicts with the base only in `CA/Pool/CasRefLedger.cpp`, where PR 2577 reworked the failure messages.

- **`publishCkpt` refuses before the merge**, without consulting admission
  (`CA/Pool/CasRefCkpt.cpp`, new `refuseFrontierAgainstDurable(durable, contribution)`):

| Contribution frontier `f`, durable `_ckpt` `d` | Verdict |
|---|---|
| contribution or durable has no `committed_through` | no refusal |
| the contribution's own `last_epoch_seal` equals `f` (a seal publication) | no refusal |
| `d.last_epoch_seal == f` | `SuccessorSeal` |
| `f.writer_epoch < d.committed_through.writer_epoch`, and `f` is not below a durable seal of its own epoch | `Superseded` |
| otherwise | no refusal |

- **`CkptPublishOutcome` gains two values** (`CA/Pool/CasRefCkpt.h`): `SuccessorSeal` (conclusive: the
  contribution is not in history, and no further write follows the refusal) and `Superseded` (unresolved; callers handle it like `FencedOut`).
- **`ckptContributionRecorded(outcome)`** is true only for `Published` and `IdenticalSkip`. Callers that
  only need "did my contribution become durable" use it: two sites in `runRecoveryWalkOnce`, the birth
  contribution in `commitRefChunk`, `tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl`, and step 2 of
  `completeCreation`.
- **`resolveWedgeOnce`**: `SuccessorSeal` closes the wedge as the existing `Occupant::SuccessorSeal` path
  does. `Superseded` behaves like `FencedOut`: `StillWedged`, a retry-later error, `requireRecovery`.
- **`commitRefChunk`**, after frontier publication: `SuccessorSeal` rejects the append
  (`CASRefAppendSealRejected`, lane `Closed`, `INVALID_STATE`), exactly as the existing
  `Occupant::SuccessorSeal` arm (`:3895`). `Superseded` calls `requireRecovery` and fails the waiters with
  an unknown outcome.
- **New `ProfileEvent` `CASRefCheckpointFrontierRefused`**, incremented in `publishCkptContribution` for both
  new outcomes. One row in `docs/en/antalya/metrics_and_events.md`.

Port decisions, where the base differs from the draft's base:

- **Messages use the PR 2577 form, and `Superseded` never promises installation.** `Superseded` cannot
  tell whether the transaction entered history, so its messages use the unknown-outcome helpers, not the
  durable-unconfirmed ones (those say "the next ref-log recovery installs it").
  `resolveWedgeOnce` on `Superseded`: "... wedged txn {}-{} is durable but was not acknowledged (a later
  epoch moved past it before checkpoint-frontier publication; the lane needs recovery):
  {`outcome_unknown_tail`}". `commitRefChunk` on `Superseded`:
  `complete_error(chunk_survivors, outcome_unknown(ErrorCodes::NETWORK_ERROR, "a later epoch moved past it before the frontier was published"))`.
- **The `SuccessorSeal` lane-closing arm exists once.** A preceding commit moves the body of the existing
  arm into a local lambda in `commitRefChunk`. The fix commit calls it from the new arm.
- **A frontier two or more epochs behind is `Superseded`,** not `CORRUPTED_DATA` (draft decision, kept).

### 4.3 Tests {#story-b-tests}

| File | Tests | Red at the base |
|---|---|---|
| `gtest_cas_ref_ckpt.cpp`, suite `CASRefCheckpoint` | `PublishRefusesNonSealAtDurableSeal`, `PublishRefusesLowerEpochPastLaterSeal`, `SupersededAfterOwnAttemptLandedIsUnresolvedNotUnwritten`, `PublishRefusesAFrontierAboveItsEpochSeal`, `PublishRefusesAFrontierTwoEpochsBehind` | yes |
| same | `PublishStillSkipsAdoptedStraggler`, `PublishStillSkipsARepublishedSeal` | no (guards) |
| `gtest_cas_ref_ckpt_join.cpp`, suite `CASRefCheckpointJoin` | `AFrontierAtTheDurableSealFromAFencedOutWriterIsStillSuccessorSeal` | yes |
| `gtest_cas_ref_wedge_every_attempt.cpp`, suite `CASRefWedgeEveryAttempt` | `AppendLandingInAReclaimedSealSlotIsRejectedNotAcknowledged`, `AppendLandingPastALaterEpochSealNeedsRecovery`, `WedgeRetryLandingInAReclaimedSealSlotIsRejectedNotAdopted`, `WedgeRetryLandingPastALaterEpochSealNeedsRecovery`, `SnapshotPublishPastALaterEpochSealDoesNotAdvanceTheCheckpoint` | yes |

- The wedge tests run the successor and ref cleanup from a `before_create` hook, between admission and the
  landing of the create.
- Three existing `CASRefCheckpoint` fixtures seal epoch 1 above the contribution (`ID_1_3`), as in the draft.
- The exact count of red tests is recorded from the red run on this base. The draft's count on its own base
  was 13.

### 4.4 Not closed by story B {#story-b-residual}

- A deposed recoverer that keeps getting `Superseded` retries until its lease check fires. Nothing else
  bounds it. Task CAS-451 decides whether to latch the mount fault.
- A deposed writer on an older binary still acknowledges. The fix protects only writers that run it.
- Recovery after `Superseded` while the lease check itself is broken is not modelled.

## 5. Story C: `fsck` tail walk revalidates `_ckpt` (CAS-448) {#story-c}

`checkRefStream` (`CA/Tools/CasFsck.cpp:312`) walks ids above the checkpoint by exact key. When a key is
absent it records a broken chain at once. Ref cleanup may have deleted that log after `_ckpt` advanced past
it.

Change: an absent tail id gets the revalidation that the checkpoint-base path of the same function already
has.

| `_ckpt` re-read | Verdict |
|---|---|
| etag differs from `checkpoint_sample`, or `_ckpt` is absent | `Unchecked` |
| the re-read throws | `Unchecked` ("could not be revalidated") |
| etag unchanged | chain broken, as today |

`checkDeadline` is called before the re-read. `NsVerdicts` precedence is unchanged: a recorded broken chain
wins over `Unchecked`.

Tests, `src/Disks/tests/gtest_cas_fsck.cpp`, suite `CASFsckAuthority` (licence spec §8.1):

| Id | Test | Red at the base |
|---|---|---|
| F1 | `CoveredTailLogVanishingAfterAuthorityAdvanceIsUnchecked` | yes |
| F2 | `MissingTailLogUnderUnchangedCheckpointIsChainBroken` | no (guard); seen red under the mutation "always `Unchecked`" |

## 6. Story D: per-life ref cleanup licence and counters (CAS-29.8, CAS-29.7) {#story-d}

Built exactly as the licence spec says: §5 (behaviour), §6 (observability), §8.2 to §8.5 (test groups A, B,
G, C), §10 (documentation). One split across commits: every assertion on a new counter or phase metric
lands with the counters ([§8](#commits), commit 9), including the `CASRefCleanupStoppedAuthorityUnverified`
assertion that the licence spec states inside G2. Summary for orientation only:

- `authorityHolds` in `Gc::cleanupRefObjects` (`CA/Gc/CasGc.cpp:3942`) drops the whole-catalog etag term.
- A row or life mismatch skips that namespace and its remaining chunks. The pass continues.
- `gc/state` absent, lease moved, a failed read or an ambiguous catalog stop the pass.
- Five `ProfileEvents` and six `ref_object_cleanup` phase metrics are added.
- The comment at `CA/Gc/CasGc.cpp:2786` that says the pre-chunk revalidation "WIDENS" the licence is
  corrected in the licence commit.

Differences from the licence spec are packaging only. Its header, §1, §7, §11 and §13 are edited in the same
change as this document to say: one pull request, stories as commit series, base on top of PR 2805, gate by
`unit_tests_dbms --gtest_filter='CAS*'` (the 26.8 tree has no `utils/cas-gate`).

## 7. Fixed premises {#premises}

- One GC round commits with a single `gc/state` CAS. A cold `LIST` is trusted. `HEAD`-before-`PUT` stays.
- No on-S3 format change. Objects deleted outside the protocol are unsupported.
- No port to `antalya-26.6` for any of the four stories.
- CAS-29.8 does not depend on CAS-29.6.

## 8. Commit series {#commits}

| # | Story | Subject | Files | Behaviour |
|---|---|---|---|---|
| 1 | A | Let a CAS write return an unresolved attempt instead of resending it | `CasRetry.h`, `CasRequests.h`, `CasRequests.cpp` | none: `resend_unresolved` defaults to true |
| 2 | A | Mint a fresh incarnation on every namespace-create step-1 decision | `CasRefCatalog.cpp`, `gtest_cas_ref_catalog.cpp` | the fix; tests S1 to S7 |
| 3 | A | Test that namespace create never re-inserts an erased incarnation | `gtest_cas_ref_catalog.cpp` | test S8 only |
| 4 | B | Extract the successor-seal lane close in `commitRefChunk` | `CasRefLedger.cpp` | none |
| 5 | B | Ask `ckptContributionRecorded` where a caller only needs durability | `CasRefCkpt.h`, `CasRefLedger.cpp`, `CasRefCatalog.cpp` | none: with three outcomes it equals `!= FencedOut` |
| 6 | B | Refuse a `_ckpt` frontier that a later writer epoch moved past | `CasRefCkpt.*`, `CasRefLedger.*`, `CasRefCkptFormat.h`, `ProfileEvents.cpp`, `metrics_and_events.md`, three gtest files | the fix; tests of [§4.3](#story-b-tests) |
| 7 | C | Revalidate `_ckpt` before `fsck` reports an absent tail log as a broken chain | `CasFsck.cpp`, `gtest_cas_fsck.cpp` | the fix; F1, F2 |
| 8 | D | License ref cleanup per life instead of by the whole catalog | `CasGc.cpp`, `gtest_cas_ref_gc.cpp` | the licence and the refusal scope; key assertions of groups A, B, G; comment fix |
| 9 | D | Count why a ref cleanup pass stopped | `CasGc.cpp`, `ProfileEvents.cpp`, `metrics_and_events.md`, `gtest_cas_ref_gc.cpp`, GC log tests if they list phase metrics | counters and phase metrics; group C and the counter assertion of G2 |
| 10 | D | Document the per-life ref cleanup licence | `docs/en/antalya/cas/architecture/garbage-collection.md` | docs only |

Rules:

- **Bisectable.** After each commit: `ninja clickhouse unit_tests_dbms` and
  `unit_tests_dbms --gtest_filter='CAS*'` pass. The pass count is recorded per commit.
- **Red first.** For each commit with tests, the tests are added to the tree first, built and run. That log
  is kept as `master/tmp/prbundle/logs/red_<commit#>_<story>.log`. Then the product change is added. A test
  that is green before its product change is either a guard (listed as such above) or a defect in the test.
- **S8 red evidence** is taken on the tree of commit 1 plus the test fixtures, before commit 2's product
  hunks are applied.
- **A red run compiles.** Where a test names a symbol that its product change introduces (the two
  `CkptPublishOutcome` values and `CASRefCheckpointFrontierRefused` in commit 6, the new `Authority` values
  and counters in commits 8 and 9), the red tree carries the bare declaration and nothing that uses it. A
  compile failure is not red evidence.
- **Mutation checks** (F2 and the group G guards of the licence spec §8.4) are run uncommitted. Their logs go
  to the same directory.
- **`LOGICAL_ERROR` expectations** are death tests under `DEBUG_OR_SANITIZER_BUILD`.
- **Commits** are signed off and carry the co-author trailer. No amend, no rebase.

## 9. Gates {#gates}

| Gate | When | Command or tree |
|---|---|---|
| Release `CAS*` gtests | after every commit | `build/src/unit_tests_dbms --gtest_filter='CAS*'` |
| ASan `CAS*` gtests | on the final tree, and again after review fixes | `build_asan`, same filter |
| Fast-test compile | on the final tree | `build_fast` (`ENABLE_LIBRARIES=0`, `ENABLE_TESTS=0`), objects of every touched product TU |
| Stateless CAS tests | on the final tree | the 58 tests named `*_cas_*` under `tests/queries/0_stateless`, against a standalone server of the final binary |
| Style | on the final tree | the functions of `ci/jobs/check_style.py` that cover C++ and docs |

- The baseline `CAS*` pass count at `2f89c5db182` is recorded before the first commit.
- A red in any gate gets a root cause or a tracked task before the branch is called ready.
- The cas-s3 lane check of the licence spec §8.6 runs in CI after the pull request opens. It is not a local
  gate.

## 10. Measurements before and after {#measurements}

Question: does ref cleanup delete under catalog churn after the change, and does it cost anything where the
old licence already worked?

Setup:

- Binaries: `2f89c5db182` (before) and the final branch head (after). Same build directory and flags.
- One standalone server at a time, fresh state directory, CAS disk on local object storage as in
  `tests/config/config.d/cas_storage_policy_for_merge_tree_by_default.xml` (`cas_gc_interval_sec` 20,
  `cas_gc_log` on).
- Load is kept small. Another session on the same host measures settings; its containers and ports are not
  touched.

Workload, 15 min, then 2 min idle:

- **Steady part:** four `MergeTree` tables on the CAS policy. One `INSERT` of 1,000 rows into each, every
  0.5 s.
- **Churn part:** two loops of `CREATE TABLE`, one `INSERT`, `DROP TABLE ... SYNC` on throwaway tables.

| Run | Binary | Churn |
|---|---|---|
| R1 | before | yes |
| R2 | after | yes |
| R3 | before | no |
| R4 | after | no |

R3 and R4 are the control: without churn the catalog is still and the old licence already holds.

Sampled every 60 s and at the end of each part:

- `CASRefCleanupObjectsDeleted` and, in R2 and R4, the five new counters (`system.events`).
- Largest and total count of `_log` objects per live namespace (files under
  `<pool_prefix>/cas/ns/stream/<incarnation>/_log/` in the object storage directory; `CA/Formats/CasLayout.h:151`).
- `system.cas_gc_log`: round `duration_ms` (median, p95) and the `ref_object_cleanup` phase row
  (`phase_duration_microseconds`, `phase_metrics`).
- Count of "catalog observation/life moved" lines in the server log.

Predictions, written before the runs:

| Run | Prediction |
|---|---|
| R1 | `CASRefCleanupObjectsDeleted` stays near 0 during churn and rises only in the idle tail. The largest per-life `_log` count grows for the whole 15 min. |
| R2 | `CASRefCleanupObjectsDeleted` rises during churn. `CASRefCleanupChunksUnderCatalogChange` > 0. `CASRefCleanupStoppedAuthorityUnverified` = 0. The largest per-life `_log` count at 15 min is below R1's. |
| R3, R4 | Both delete. Deleted counts and round durations agree within run-to-run noise. `CASRefCleanupChunksUnderCatalogChange` = 0 in R4. |

A failed prediction is a finding. It gets a root cause before the branch is called ready.

Output: `docs/superpowers/reports/2026-11-11-cas-pr1-ref-cleanup-bundle-measurements.md` with the table of
results, the scripts' paths and the raw sample files.

## 11. Models {#models}

- Models and result notes live on the docs branch `cas-gc-rebuild` under `docs/superpowers/models/`. The
  26.8 tree carries no `.tla` files.
- **Story D:** `CaRefCleanupLicenceCore`, 12 configs, run on 2026-11-11 (licence spec §9).
- **Story B:** `CaRefTableSnapshotLogCore` with the refusal on every publisher, commit `02a19b97326`
  (`INV_ACKED_REACHABLE`, `INV_SUCCESSOR_NOT_REFUSED`, sabotage config `_sab_a2_le`).
- **Stories A and C:** no model. `CaRefCatalogCore` creates a life atomically with a fresh incarnation, which
  is the behaviour story A restores.
- No re-run is required. A re-run is required if the verdict table of [§4.2](#story-b-change) or the
  licence of the licence spec §5.1 changes during implementation or review.

## 12. Documentation {#documentation}

- `docs/en/antalya/metrics_and_events.md`: one row for `CASRefCheckpointFrontierRefused` (commit 6), five
  rows for the cleanup counters (commit 9).
- `docs/en/antalya/cas/architecture/garbage-collection.md`: phase 17, as the licence spec §10 says
  (commit 10).
- No change to `configuration.md`. No user-visible change for stories A and C beyond their error paths.
- The pull request body follows `.github/PULL_REQUEST_TEMPLATE.md` and ticks the CAS regression job. It is
  written when the pull request is opened, not in this change.

## 13. Acceptance mapping {#acceptance}

| Criterion | Met by |
|---|---|
| CAS-368 #1: a retried step 1 never re-sends identical catalog bytes | S1, S2, S4, for every retry after an attempt that may have landed. The reissue after a credential refresh (`CasRequests.cpp:1063`) keeps its bytes: the rejected attempt is proven unapplied |
| CAS-368 #2: model with a catalog value and a late apply | not in scope ([§14](#non-goals)) |
| CAS-449 #1: a gtest drives the interleaving | S8 |
| CAS-449 #2: no retry re-inserts an incarnation an earlier attempt wrote | S1, S2, S4, S8 |
| CAS-449 #3: `completeCreation` operates on the entry the successful admission wrote | `createNamespaceStep1` returns `*findEntry(landed, ns)`; S8 checks `_ckpt` under the landed life |
| CAS-366 #1: the model fails without the refusal and holds with it | `CaRefTableSnapshotLogCore`, `02a19b97326` |
| CAS-366 #2: a late append into a reclaimed seal slot is never acknowledged | `AppendLandingInAReclaimedSealSlotIsRejectedNotAcknowledged`, `WedgeRetryLandingInAReclaimedSealSlotIsRejectedNotAdopted` |
| CAS-448 #1, #2 | F1, F2 |
| CAS-448 #3: same release as CAS-29.8 | same pull request, commit 7 before commit 8 |
| CAS-29.8 #2, #3, #4 and CAS-29.7 #1 | licence spec §11 mapping (tests A1, B1, B2, G1, C1 to C6) |
| CAS-29.7 #2, #3 | [§10](#measurements) R1 against R2, plus the CI evidence in the licence spec §2 |
| CAS-447 #1 | [§10](#measurements) locally; the cas-s3 lane check in CI |
| CAS-447 #2, #3 | licence spec §2; group A |

## 14. Non-goals and where they go {#non-goals}

| Not done here | Placement |
|---|---|
| Whole-plan checkpoint cadence for ref cleanup | CAS-29.6 |
| Model for CAS-368 (#2) | stays open on CAS-368 |
| Catalog revision field against duplicated or late `PUT`s | stays open on CAS-368 (format change) |
| Bound on `Superseded` retries of a deposed recoverer | CAS-451 |
| End-to-end two-mount tests for story B | noted on CAS-366 when it closes |
| Port to `antalya-26.6` | none (owner decision, 2026-11-11) |
| cas-s3 lane check | CI of the pull request, after it opens |
| Audit drafts CAS-364, CAS-365, CAS-367, CAS-373 and CAS-69 | the second pull request |

## 15. Risks {#risks}

- **`CasRefLedger.cpp` port is by hand.** The draft's hunks there do not apply. The risk is a caller left on
  `== FencedOut`. Check: after commit 6, every `CkptPublishOutcome::FencedOut` comparison in `CA/` is listed
  and justified in the commit's review.
- **Commit 5 changes five call sites without a behaviour change.** Check: the `CAS*` pass count equals the
  count after commit 4.
- **Phase metrics may be listed in stateless references** (`05007_cas_gc_introspection` and similar). Check:
  the stateless gate of [§9](#gates).
- **S8 depends on the order "landed write, callback, settle read".** If `injectAmbiguousLandedWrite` settles
  inside the backend, the callback moves to a `read` override. The builder confirms on the first red run.
- **Base moves.** PR 2805 may get new commits before this branch is opened as a pull request. The branch
  then merges the new base. No rebase.

## 16. Left to the builder {#builder}

- The exact shape of `after_write` in `CatalogWriteRecorder`.
- The name and placement of the lane-close lambda in `commitRefChunk`.
- The wording of log lines not quoted in this document or in the licence spec.
- Everything the licence spec §15 leaves open.
