---
description: 'Design for CAS-29.8: the GC phase that deletes covered ref logs and snapshots stops requiring an unchanged ref-catalog etag and licenses each namespace by its own catalog row, its own life and the GC lease.'
sidebar_label: 'CAS GC: per-life ref cleanup licence'
sidebar_position: 110
slug: /superpowers/specs/cas-ref-cleanup-per-life-licence-design
title: 'CAS GC: ref cleanup is licensed per life, not by the whole catalog'
doc_type: 'reference'
---

# CAS GC: ref cleanup is licensed per life, not by the whole catalog — design {#cas-ref-cleanup-per-life-licence}

- Date: 2026-11-11. Status: draft, not approved. Tasks: CAS-29.8 (this change), CAS-29.7 (counters), CAS-448 (fsck), CAS-449 (prerequisite), CAS-447 (symptom).
- Source: `docs/superpowers/reports/2026-11-11-cas-29.8-ref-cleanup-licence-adjudication.md` (the safety argument; not repeated here).
- Amends: `docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md`, section B3.
- Code facts are at `3fa3a01203b` (head of PR [#2805](https://github.com/Altinity/ClickHouse/pull/2805)).
  `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
- Not part of PR #2805. Work starts after #2805 merges into `antalya-26.8`.

## 1. Decision {#decision}

- **Drop the whole-catalog etag condition** from the per-chunk licence of `Gc::cleanupRefObjects`
  (`CA/Gc/CasGc.cpp`, lambda `authorityHolds`). A chunk no longer requires
  `current_catalog.etag == folded.catalog_cut->etag`.
- **Keep the other three conditions unchanged:** the namespace's catalog row equals the fold's row by value;
  the life index resolves the incarnation to the same life and is unambiguous; `gc/state` is present and its
  `lease.owner` and `lease.seq` equal the adopted lease.
- **A row or life mismatch refuses that namespace only.** The pass skips the namespace's remaining chunks and
  continues with the next namespace. Today the first refusal ends the pass.
- **Whole-pass stops stay:** `gc/state` absent, lease owner or seq moved, catalog or `gc/state` read fails,
  catalog ambiguous.
- **The fold's catalog cut stays the reference observation.** The fresh read before a chunk is a comparison
  target only. The life, the cursor, the checkpoint and the listing still come from the fold.
- **Four stop-reason counters and a planned counter are added** ([§6](#observability)). This closes CAS-29.7.
- **`fsck` gets the `_ckpt` revalidation on its tail walk** (CAS-448, [§7](#fsck-tail-walk)), as a separate
  change that lands first.
- **No existing TLA+ model changes** ([§9](#tla-impact)). A new model of the licence,
  `CaRefCleanupLicenceCore`, is written and checked. Its result: dropping the whole-catalog condition removes
  no data protection.

## 2. Problem and evidence {#problem}

`cas/ref_catalog` is one object. Every CAS namespace create, drop and lifecycle step rewrites it and changes
its etag. `authorityHolds` re-reads it before each chunk and requires the etag of the round's catalog cut. Under
DDL churn the etag has always moved, so no chunk is ever deleted.

| Observation | Source |
|---|---|
| 46 rounds had a cleanup plan; 46 logged `catalog observation/life moved`; 0 chunks deleted | local cas-s3 stateless run, 2026-11-09, 30 min (CAS-447 notes) |
| `CASRefCleanupObjectsDeleted` = 0 for 3 h; ~180k `_log` objects published | PR [#2794](https://github.com/Altinity/ClickHouse/pull/2794), `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)`, `metric_log` |
| 971 to 1656 `_log` objects under each of the six largest lives after 30 min | same local run |

- The objects that accumulate belong to long-lived namespaces (the system log tables on the `cas_s3` policy).
  Dropped tables are reclaimed by the dead-life janitor and are not affected.
- Cost: every ledger recovery lists a growing `_log` prefix and every GC round enumerates a growing
  `cas/ns/stream/` (CAS-432, CAS-447).
- Lineage: the etag equality arrived with `53e2a135980` (per key), moved to per chunk in `fe0b909f011`.
  The same whole-catalog condition was removed from the ref writer in `684161dcc03`; the GC site kept it.

The adjudication report shows that no reader of a life's `_log` or `_snap` objects takes another namespace's
catalog row as input. The etag condition is a strict over-approximation of the row and life conditions.

Failure asymmetry, which sets the burden of proof:

| Option | How it fails | Cost |
|---|---|---|
| Keep the etag condition | Refuses every chunk under churn | Unbounded `_log` growth; no data loss |
| Drop it wrongly | A chunk deletes an object a reader still needs | `CORRUPTED_DATA` for that namespace; irreversible |

## 3. Goals, non-goals, anti-goals {#goals}

**Goals**

- Cleanup deletes covered objects of a namespace whose own row and life did not change, whatever happened to
  other namespaces between the fold and the chunk.
- A change of one namespace's row or life costs that namespace one round of cleanup and nothing else.
- Every refusal has a counted reason.
- The licence stays evaluable from durable state plus the round's lease pair, so a multi-node executor
  (CAS-90) can apply it without the fold's in-memory cut ([§12](#multi-node)).

**Non-goals**

- Cleanup listing its own range (parent spec B2, CAS-29.6). This change does not depend on it.
- A design of multi-node GC.
- A change to which keys a plan may delete (`planRefCleanup`), to the recovery-triple validation, to the budget,
  or to the phase's place after the round's `gc/state` CAS.
- A change to checkpoint cadence. After this change the per-life `_log` count is bounded by how often the
  writer publishes a checkpoint snapshot and GC folds past it. If the count is still too high, that is a
  separate cause under CAS-447.
- Any on-S3 format change or new object kind. The format is frozen since 26.6.4.

**Anti-goals**

- Resolving the life from a fresh catalog read. `folded.ref_tables` is keyed by name and transaction ids
  restart at `{E,1}` per life; a fresh resolution after a rebirth pairs the old life's ids with the new
  incarnation and can delete the new life's `_snap`. This is safe only after B2.
- Weakening the row, life or lease condition, or narrowing the ambiguity check to one life.
- A setting that restores the whole-catalog condition. It would be a second code path with no user.
- Continuing after a lease refusal or after an unverifiable catalog. "Not verified" must not be treated as
  "verified".

## 4. Fixed premises {#premises}

- One GC round commits with a single `gc/state` CAS; cleanup runs after it.
- A cold `LIST` is trusted; the `HEAD`-before-`PUT` protocol steps stay as they are.
- Objects deleted outside the protocol are unsupported.
- Keys of a life are `cas/ns/stream/<incarnation>/_log|_snap/<id>` (`CA/Formats/CasLayout.h`). The name is not
  part of a physical key.
- **An incarnation must never be reused.** Cleanup safety depends on it today and after this change: the
  whole-catalog condition is evaluated before the delete and does not cover the window between them
  ([§9](#tla-impact), `_old_reuseincarnation`).
- **The code mints one incarnation per `createNamespace` call, not per admission attempt.**
  `mintFreshIncarnation` (`CA/Pool/CasRefCatalog.cpp`) draws a random nonzero 128-bit value and has one call
  site, before the admission loop. A row moves `Creating -> Live -> Removing -> erased` and no write moves it
  back. `createNamespaceStep1` inserts the same entry on every retry that finds the name absent.
- **Gap (CAS-449).** The first `PUT` lands and its response is lost. The resolution read sees another body,
  so the admission pauses (`Retry::backoff`, at most 5 s) and retries. If a sibling opener of the same mount
  completes the row, the table is dropped and GC erases the row before that retry reads the catalog, the
  retry inserts the erased incarnation again. The erase needs at least one committed GC round after the drop.
  No happens-before excludes the overlap. Found by source review; not reproduced at runtime. The same code is
  on `antalya-26.6`. CAS-449 closes the path and is a prerequisite of step 3 ([§11](#delivery)).

## 5. Behaviour {#behaviour}

### 5.1 Licence per chunk {#licence}

For a chunk of namespace `X`, with `observed_row` from the fold's cut and `life = (X, observed_row.incarnation)`:

```
catalog = read(cas/ref_catalog)                 -- fails: stop the pass
catalog must be unambiguous                     -- ambiguous: stop the pass
row = catalog[X]
if row is absent, or row != observed_row, or catalog resolves life.incarnation to another life:
    skip X (its remaining chunks too); continue with the next namespace
state = read(gc/state)                          -- fails or absent: stop the pass
if state.lease.owner != adopted.owner or state.lease.seq != adopted.seq:
    stop the pass
delete the chunk
```

The catalog etag is still read and compared, for the counter in [§6](#observability) only. It gates nothing.

### 5.2 Refusal scope {#refusal-scope}

| Event before a chunk of `X` | Scope | Why |
|---|---|---|
| `X`'s row absent, changed by value, or its incarnation resolves to another life | `X` only | `X`'s plan was built from the old row. Other namespaces' licences do not take `X`'s row as input. |
| `gc/state` absent, or lease owner or seq moved | whole pass | The lease is the one shared licence. A deposed leader stops. |
| Catalog or `gc/state` read fails | whole pass | No row can be verified. |
| Catalog ambiguous (two rows resolve one incarnation) | whole pass | A corruption signal, not churn. |
| Per-round budget exhausted | pass returns, as today | |
| Recovery triple of `X` does not validate | `X` only, as today | |

- Within a skipped namespace nothing more is deleted in this round. Its plan is rebuilt next round from the
  next fold's row.
- A chunk already licensed completes even if the lease or the row moves during the delete request. This is
  today's behaviour (`LeaseMoveDuringAChunkLetsTheChunkCompleteAndNothingElse`,
  `RebirthDuringAChunkDeletesOnlyTheOldLifesKeys`): the keys carry the old incarnation and are covered.

### 5.3 What does not change {#unchanged}

- `suppress_destructive` returns from the phase at once.
- `planRefCleanup` (`CA/Pool/CasRefProtocol.cpp`): logs `L <= durable_cursor`, `L < checkpoint`,
  `L != retained_log_proof`; snapshots `S < checkpoint`.
- `readCheckpointSnapshotBase` validates the checkpoint-named triple per namespace.
- Chunk size `gc_bulk_delete_chunk_keys`, budget `cas_gc_round_ref_cleanup_budget`, write-once batch delete.
- The set of keys a round may delete. Only how often a licensed plan is carried out changes.

### 5.4 Why the row and life conditions stay {#why-row-and-life}

**Data safety does not rest on these conditions.** A chunk's keys carry the fold-time incarnation, the plan
covers only log ids below the life's checkpoint and at or below its cursor, and snapshot ids below the
checkpoint. Checkpoint and cursor move forward only. The argument also needs an incarnation that is never
reused, which holds once CAS-449 is fixed ([§4](#premises)). Under that premise the model holds its data
invariant with every licence term removed ([§9](#tla-impact), `_datafloor`).

The conditions do three other things:

1. **Bound work on a changed row.** After `Live -> Removing`, an erase or a rebirth, the pass deletes at most
   the chunk already licensed. Without the row comparison it keeps deleting the old life's covered keys
   (`_sab_norow`). Those deletes are harmless, and they belong to the namespace janitor.
2. **Detect an alias.** If a second row is admitted with this incarnation, both rows own the same keys and the
   plan's premise is void. The ambiguity check and the life resolution stop the pass. `684161dcc03` found this
   case on the writer side. The model does not represent it; test G2 covers it.
3. **Stay a second barrier.** If the life is resolved at delete time, or an incarnation is reused, the data
   invariant breaks (`_sab_rederive`, `_sab_reuseincarnation`). The row comparison narrows that window to
   one chunk when the returning row differs by value. It does not close it.

A create, a drop, a row erase or a `Creating -> Live` step of a different name changes neither this life's
`_ckpt`, nor its seal row, nor its stream.

### 5.5 Readers that sampled an older `_ckpt` {#stale-ckpt-readers}

A reader samples `_ckpt`, the writer publishes a later checkpoint, GC folds past it and cleanup deletes logs
the reader still plans to read. No catalog row is involved, so the dropped condition neither caused nor
prevented this. It was rare because cleanup almost never ran. It becomes routine.

| Reader | Outcome | Action |
|---|---|---|
| Writer recovery (`CA/Pool/CasRefLedger.cpp`, `runRecoveryWalkOnce`) | 404, re-reads `_ckpt`, sees a new etag, restarts | none |
| Orphan-sweep protection view (`CA/Gc/CasOrphanManifestSweep.cpp`) | view throws; that namespace's deletions are skipped | none |
| Decommission sweep (`CA/Tools/CasDecommission.cpp`) | same | none |
| GC rebuild | `CORRUPTED_DATA` only if the rebuilding leader was deposed; its `gc/state` CAS loses anyway | none |
| `fsck` recovery leg | recorded `Unchecked`, unless the stream check already recorded chain-broken | none |
| `fsck` tail walk (`CA/Tools/CasFsck.cpp`, `checkRefStream`) | **chain-broken on a healthy pool** | fix, [§7](#fsck-tail-walk) |
| `clickhouse-disks` inspect | 404 for the operator | none |

## 6. Observability {#observability}

New `ProfileEvents` (`src/Common/ProfileEvents.cpp`; names are a call of this spec, [§13](#calls)):

| `ProfileEvent` | Incremented when | Unit |
|---|---|---|
| `CASRefCleanupObjectsPlanned` | a namespace's plan is built | keys in the plan |
| `CASRefCleanupNamespacesSkipped` | the licence skips a namespace because its row or life changed | namespaces |
| `CASRefCleanupStoppedLeaseMoved` | the pass stops because `gc/state` is absent or the lease moved | passes |
| `CASRefCleanupStoppedAuthorityUnverified` | the pass stops because the catalog or `gc/state` read failed, or the catalog is ambiguous | passes |
| `CASRefCleanupChunksUnderCatalogChange` | a chunk is deleted while the catalog etag differs from the fold's cut | chunks |

- `CASRefCleanupObjectsDeleted` keeps its meaning.
- `CASRefCleanupNamespacesSkipped` counts licence refusals only. A namespace that never gets a plan (no row
  in the fold's cut, no checkpoint, invalid recovery triple) is not counted.
- `CASRefCleanupStoppedAuthorityUnverified` includes a read that failed after its retries. The existing
  warning line carries the cause.
- `CASRefCleanupChunksUnderCatalogChange` counts exactly the chunks the old licence refused. It is the
  before/after measure of this change and replaces the "etag only, row and life unchanged" refusal counter that
  the parent spec planned for CAS-29.7.
- A key left undeleted is planned again next round. Over a run, planned minus deleted counts it once per round.
- After a whole-pass stop the later namespaces have no plan and are not counted as planned.

Phase row `ref_object_cleanup` gains metrics with the same meaning, per round: `objects_planned`,
`objects_deleted`, `namespaces_skipped`, `stopped_lease_moved` (0/1), `stopped_authority_unverified` (0/1),
`chunks_under_catalog_change`. The existing `namespaces_planned`, `suppressed` and `trim_enabled` stay.

Log lines keep their level. The `catalog observation/life moved` line names the namespace and says that the
namespace is skipped, not that cleanup stopped.

## 7. Companion change: `fsck` tail walk (CAS-448) {#fsck-tail-walk}

- `checkRefStream` walks `base+1 .. committed_through` from a sampled `_ckpt`. An absent tail id is recorded
  as chain-broken with no revalidation. The base path in the same function already re-reads `_ckpt` and
  records `Unchecked` when its etag moved.
- **Change:** an absent tail id gets the same revalidation. `_ckpt` changed, absent or unreadable: `Unchecked`.
  `_ckpt` unchanged: chain-broken, as today.
- It is correct without the licence change, so it lands first, as its own pull request.
- It must be in every release that contains the licence change.

## 8. Tests {#tests}

All tests are gtests under the `CAS*` filter. The order below is the order of implementation: each group is
written and seen red before the production change that turns it green.

Fixtures, all existing in `src/Disks/tests/gtest_cas_ref_gc.cpp` unless noted:

- `seedTwoCoveredLogs(backend, layout, ns)`: one `Live` namespace, three committed transitions, a snapshot and
  a recoverable `_ckpt` at the third; returns the two covered `_log` keys. Called once per namespace for
  two-namespace tests.
- `RefCleanupAuthorityRaceBackend`: moves an authority at `Timing::AfterFirstDelete` or `Timing::DuringChunk`.
- `Gc::setPostHotScanCatalogReadHookForTest`: runs a callback right after the round's catalog cut. The
  namespace janitor runs between this point and cleanup and re-reads the catalog. A hook that erases a row
  lets the janitor delete that life's stream first. So the hook is used only for changes to other
  namespaces; a change to the cleaned namespace's own row is injected from the race backend during its
  first chunk.
- `gc_bulk_delete_chunk_keys = 1` wherever chunk timing matters.

### 8.1 Group F: `fsck` (CAS-448; `src/Disks/tests/gtest_cas_fsck.cpp`, suite `CASFsckAuthority`) {#tests-fsck}

Fixture (new; the existing base-path fixtures have no tail): a `Live` namespace; a valid checkpoint-named
triple at id `{1,1}` (snapshot and same-id log); `_ckpt.committed_through = {1,3}`; logs `{1,2}` and `{1,3}`
present. `fsck` then passes the base check and walks the tail `{1,2}`, `{1,3}`.

| # | Test | Trigger | Red before | Green after |
|---|---|---|---|---|
| F1 | `CoveredTailLogVanishingAfterAuthorityAdvanceIsUnchecked` | `MutateOnFirstGetBackend`: on `fsck`'s read of log `{1,2}` the test replaces `_ckpt` and the read finds the log absent | chain-broken | `Unchecked` |
| F2 | `MissingTailLogUnderUnchangedCheckpointIsChainBroken` | log `{1,2}` absent from the start; `_ckpt` untouched | green (guard) | green |

Both tests assert `ref_records_walked` or the reported key, so a verdict from the base path cannot satisfy them.

F2 is a guard. Check it once against the mutation "always record `Unchecked` for an absent tail id": it must
turn red.

### 8.2 Group A: unrelated catalog churn no longer refuses (suite `CASRefGcCleanupAuthority`) {#tests-churn}

| # | Test | Fixture and trigger | Red before | Green after |
|---|---|---|---|---|
| A1 | `UnrelatedNamespaceCreatedBetweenCutAndCleanupStillDeletesCoveredKeys` | `seedTwoCoveredLogs(A)`; hook admits a `Creating` row for another name (`CasRefCatalog::casAdmitEntry`) | 0 deletes | both keys deleted |
| A2 | `UnrelatedNamespaceDroppedBetweenCutAndCleanupStillDeletesCoveredKeys` | seed A and B; hook flips B to `Removing` | 0 deletes | A's keys deleted |
| A3 | `UnrelatedNamespaceErasedBetweenCutAndCleanupStillDeletesCoveredKeys` | seed A and B; hook erases B's row | 0 deletes | A's keys deleted |
| A4 | `CatalogTokenMoveBeforeFirstChunkStillDeletesEveryCoveredKey` (renamed from `...RefusesEveryRefObjectDelete`) | same-content catalog `replace` in the hook | both keys present | both absent, `deleteCount == 1` each |
| A5 | `CatalogTokenMoveBetweenChunksAllowsBothChunks` (renamed from `...AllowsFirstAndRefusesSecondDelete`) | race backend, `Authority::Catalog`, `Timing::AfterFirstDelete` | second key kept | both deleted |
| A6 | `RecoveryAfterCleanupUnderUnrelatedChurn` | A1, then `recoverRefTableDetailedFromAuthority` and `readCheckpointSnapshotBase` with `_ckpt` read after cleanup | red: keys not deleted | keys deleted, no throw, committed set unchanged |

In A2 and A3 the namespace janitor may act on B before cleanup. The assertions cover A's keys only.

Production change after group A is red: remove the etag term from the licence.

### 8.3 Group B: a row or life change refuses one namespace {#tests-scope}

| # | Test | Fixture and trigger | Red before | Green after |
|---|---|---|---|---|
| B1 | `OwnRowMoveRefusesOnlyThatNamespace` | seed A and B; race backend moves A's row `Live -> Removing` (same incarnation) at `Timing::DuringChunk` on A's first key | A1 deleted, A2 kept, B1 and B2 kept | A1 deleted, A2 kept, B1 and B2 deleted |
| B2 | `OwnRowErasedDuringCleanupSkipsThatNamespace` | seed A and B; race backend erases A's row at `Timing::DuringChunk` on A's first key | A1 deleted, A2 kept, B1 and B2 kept | A1 deleted, A2 kept, B1 and B2 deleted |

Both need new `Authority` values in the race backend. In both, the deleted A1 proves the round reached cleanup.

B1 also guards the row comparison. Its row keeps the incarnation, so the life still resolves and only the
value comparison refuses A2. Mutation that must turn it red once: remove the row comparison (A2 is deleted).

Production change after group B is red: a row or life mismatch skips the namespace and the pass continues.

### 8.4 Group G: guards that must stay green {#tests-guards}

These pass before and after. Each is trusted only after it was seen red under the named mutation. The mutation
is not committed.

| # | Test | Expectation | Mutation that must turn it red |
|---|---|---|---|
| G1 | `GcFenceMoveRefusesEveryNamespace` (new; seed A and B; `Authority::GcFence`, `Timing::DuringChunk` on A's first key) | A1 deleted; A2, B1, B2 kept | scope a lease refusal to the namespace |
| G2 | `AliasingIncarnationAdmittedDuringCleanupRefusesThePass` (new; race backend admits a `Creating` row for another name with A's incarnation at `Timing::DuringChunk` on A's first key) | A1 deleted, A2 kept; `CASRefCleanupStoppedAuthorityUnverified` = 1 | remove both the ambiguity check and the life resolution (`resolve` itself throws on an ambiguous incarnation, so removing one is not enough) |
| G3 | `GcFenceMoveBeforeFirstChunkRefusesEveryRefObjectDelete`, `GcFenceMoveBetweenChunksAllowsFirstAndRefusesSecondDelete`, `LeaseMoveDuringAChunkLetsTheChunkCompleteAndNothingElse` (existing) | unchanged | remove the lease comparison |
| G4 | `RebirthDuringAChunkDeletesOnlyTheOldLifesKeys` (existing) | first chunk deleted, second kept | none; a rebirth is refused by the row comparison and by the life resolution, so no single mutation exposes it. Regression only. |
| G5 | `CASRefGc.RefObjectCleanup*` (existing, six tests) | unchanged | none; plan-rule tests, run as regression |

G1, G2 and B1 need `A` to sort before `B`: the pass walks namespaces in name order.

G2 injects the alias from the race backend, not from the post-hot-scan hook. The namespace janitor re-reads
the catalog before cleanup and throws on ambiguity, so a hook-placed alias would pass with cleanup's own check
removed.

### 8.5 Group C: counters {#tests-counters}

Written red before the counters exist; asserted inside the tests above.

| # | Assertion | In test |
|---|---|---|
| C1 | `CASRefCleanupChunksUnderCatalogChange` = 2 (chunk size 1), `CASRefCleanupNamespacesSkipped` = 0 | A1 |
| C2 | `CASRefCleanupNamespacesSkipped` = 1 | B1, B2 |
| C3 | `CASRefCleanupStoppedLeaseMoved` = 1 | G1 |
| C4 | `CASRefCleanupStoppedAuthorityUnverified` = 1 | G2 |
| C5 | `CASRefCleanupObjectsPlanned` = 4, `CASRefCleanupObjectsDeleted` = 3 | B1 |
| C6 | phase row `ref_object_cleanup` carries the six new metrics with the same values | B1 |

Production change after group C is red: the counters and phase-row metrics of [§6](#observability).

### 8.6 Lane check {#tests-lane}

One `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)` run on the pull request. Read from
`metric_log` and the server log:

| Measure | Expected |
|---|---|
| `CASRefCleanupObjectsDeleted` | > 0 (was 0 for 3 h) |
| `CASRefCleanupChunksUnderCatalogChange` | > 0; this is the number of chunks the old licence refused |
| `CASRefCleanupStoppedAuthorityUnverified` | 0; a nonzero value is explained from the warning line |
| largest per-life `_log` count at the end of the run | recorded; compared with 971 to 1656 after 30 min before the change |
| `CORRUPTED_DATA` from ref recovery in the server log | none |

## 9. TLA+ impact {#tla-impact}

Models are in `docs/superpowers/models/`. **No existing model has to change.** None of them contains a
whole-catalog term in the licence of covered-log cleanup, so the code moves toward what the models already
assume.

| Model | What it covers of ref cleanup | Catalog or lease term in the cleanup guard | Change |
|---|---|---|---|
| `CaRefTableSnapshotLogCore` | the plan rule: `GcPlan`, `GcKeep`, `GcDelete`, `GcCleanupLog`, `GcCleanupSnap` gated by the checkpoint base; one namespace, one incarnation | none; the catalog is out of its scope | none; the plan rule is unchanged |
| `CaRefDeltaIntakeCore` | `Cleanup(t, i)` for two namespaces, gated by the namespace's own snapshot and cursor | none; already per namespace | none |
| `CaRefNsCleanupStaleLeaderCore` | the namespace janitor (dead-life debris, exact `life_id`) | catalog cut per page; another phase | none |
| `CaRefFoldClampRecoveryCore` | atomicity of owner-removed body cleanup | none | none |
| `CaRefCatalogCore` | `cleanupEvidence` for the catalog row erase | row erase, not log cleanup | none |
| `CaRefWriterCleanupCore` | writer-side precommit cleanup | none | none |
| `CaGcDestructiveGateCore` | the physical destructive gate versus the lifecycle-row erase gate | another phase; same direction (a per-row proof is not invalidated by unrelated state) | none |
| `CaRefPreFoldDrainCore`, `CaRefPreFoldDrainAllRowsCore` | `catalogToken` for the catalog CAS that erases a `Removing` row | a real whole-object CAS; not cleanup | none |
| `CaGcRootLocalPartManifestCore` | part-manifest cleanup and the orphan sweep's cut order | another phase | none |
| `CaGcLeaseCore`, `CaIncarnationCore`, `CaRefLaneCore`, `CaGcShardIncarnationCore`, `CaGcRoundDeferCore` | no ref-cleanup licence term | | none |

**New model: `CaRefCleanupLicenceCore`** (owner decision, 2026-11-11). No earlier model covered the licence
itself. Files in `docs/superpowers/models/`: `CaRefCleanupLicenceCore.tla`, 12 configurations
`CaRefCleanupLicenceCore_*.cfg`, runner `run_refcleanuplicence.sh`, results
`CaRefCleanupLicenceCore_RESULTS.md`.

- State: one name with up to two lives; a row that is absent, `Live` or `Removing`; per life log and snapshot
  ids restarting at 1, a checkpoint and a fold cursor; the lease; the cleaner's fold-time row and plan.
- Actions: append, snapshot publish, checkpoint CAS; drop, erase, rebirth; a catalog write that does not touch
  the row; fold; lease steal; a successor leader's fold and delete; cleaner `Check` and `Delete` as two steps
  with one key per chunk, so the check-to-delete window exists.
- Each licence term is a switch. Two sabotage switches: the delete resolves the name again; a rebirth reuses
  the erased incarnation.
- `Recreate` allocates a new incarnation unless the reuse switch is on. The green runs assume the premise
  of [§4](#premises). They do not cover the admission-retry path of CAS-449, and they do not compare the two
  licences on a reused incarnation whose row returns equal by value.

Results of the 2026-11-11 run (TLC `2026.07.18.145032`, `MaxInc = 2`, `MaxId = 3`; all 12 expectations met):

| Configuration | Licence terms and sabotage | Result | Meaning |
|---|---|---|---|
| `_safe` | row, life, lease | green, 8,364,961 distinct states | the specified licence is safe |
| `_datafloor` | none | green on the data invariant, 9,999,773 distinct states | data safety rests on the plan bounds, fold-time keys and fresh incarnations |
| `_nolife` | row, lease | green, same state graph as `_safe` | with one row per incarnation the row comparison implies the life term |
| `_old_churnblocked` | row, life, lease, whole catalog | green, including "no chunk is licensed after a catalog write" | the old condition stops cleanup after any catalog write (CAS-447) |
| `_witness_churnprogress` | row, life, lease | a chunk is licensed after an unrelated catalog write | the new licence makes progress there |
| `_sab_rederive` | row, life, lease; delete resolves the name again | data invariant violated | fold-time keys are load-bearing |
| `_sab_reuseincarnation` | row, life, lease; incarnation reused | data invariant violated | incarnation freshness is load-bearing |
| `_old_reuseincarnation` | row, life, lease, whole catalog; incarnation reused | data invariant violated | the old condition did not protect against reuse either |
| `_sab_nolease` | row, life | a deposed leader deletes a second chunk | the lease term bounds a deposed leader to one chunk |
| `_sab_norow` | life, lease | a second chunk is deleted after `Live -> Removing` | the row comparison bounds work on a changed row to one chunk |
| `_witness_rebirth`, `_witness_twokeyplan` | row, life, lease | reachable | the green runs are not vacuous |

**Not modelled**, and covered by tests instead: two rows with one incarnation (G2); the refusal scope, since
the model has one name (B1, B2, G1); `retained_log_proof` and the work budget (G5 and the existing budget
tests).

The same model is the starting point for the CAS-90 executor, which evaluates this predicate without the
fold's in-memory cut.

## 10. Documentation {#documentation}

- `docs/en/antalya/cas/architecture/garbage-collection.md`, phase 17 (`ref_object_cleanup`): the **Safety**
  bullet states the per-namespace check (same row and life) and the lease check, and drops "ref-catalog token
  still equals the fold's catalog cut"; "the first failure stops the whole pass" becomes the scope table of
  [§5.2](#refusal-scope) in words. **Fails the round if** and **Observability** list the new behaviour and
  metrics.
- `docs/en/antalya/metrics_and_events.md`: five new rows.
- `docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md`, B3: a note that the fresh-cut step
  and the sentence that forbids relaxing the etag comparison are superseded by this spec.
- No change to `docs/en/antalya/cas/configuration.md`: no setting is added or changed.

## 11. Delivery {#delivery}

| Step | Content | Base | Depends on |
|---|---|---|---|
| 0 | CAS-449: one incarnation per inserting admission attempt, with its gtest | `antalya-26.8` after #2805 merges | nothing |
| 1 | CAS-448: `fsck` tail walk, tests F1 and F2 | `antalya-26.8` after #2805 merges | nothing |
| 2 | `CaRefCleanupLicenceCore` model and its result note ([§9](#tla-impact)); written and run on 2026-11-11 | docs branch | nothing |
| 3 | CAS-29.8 with CAS-29.7: licence, refusal scope, counters, tests A, B, G, C, user docs | `antalya-26.8` after #2805 merges | step 0 merged; step 1 merged or in the same release |

- One pull request per step, each targeting `antalya-26.8`. None goes into #2805.
- No backport to `antalya-26.6` (owner decision, 2026-11-11). The condition exists there since `a840c03b425` and stays.
- Gates on the exact pushed tree: `CAS*` gtests through `utils/cas-gate`, the ASan lane for the touched
  suites, the lane check of [§8.6](#tests-lane) for step 3.
- Pull request bodies tick the CAS regression job.
- Compatibility: no format change. A mixed-version pool is safe: every version already deletes the same keys
  whenever the catalog is still. Downgrade restores the old refusal and nothing else.

Acceptance mapping:

| Criterion | Met by |
|---|---|
| CAS-29.8 #2: an unrelated `CREATE` between fold and cleanup no longer refuses the pass | A1 |
| CAS-29.8 #3: a changed row or life refuses that namespace only; a moved lease refuses everything | B1, B2, G1 |
| CAS-29.8 #4: on the cas-s3 lane, refusals with unchanged row and life are zero | true by construction; the lane check reports `CASRefCleanupChunksUnderCatalogChange` instead |
| CAS-29.7 #1: stop-reason counters and a planned counter on the phase row | [§6](#observability), C1 to C6 |
| CAS-29.7 #2 and #3: share of etag-only refusals, and whether the licence change is justified | the lane check; already answered by the evidence in [§2](#problem) |
| CAS-447 #1: per-life `_log` count bounded during a 3 h run | the lane check; subject to the checkpoint-cadence non-goal |
| CAS-448 #1 to #3 | F1, F2, step order |

## 12. Consequences for multi-node GC (CAS-90) {#multi-node}

Nothing is built for CAS-90 here. Two requirements are recorded so the licence stays usable there:

- **Round eligibility must travel with the work.** `suppress_destructive` is computed in memory by the fold
  and is not in the fold seal. A coordinator must either issue cleanup work only for eligible rounds or put
  the flag into the claim. An executor that only sees a matching lease pair cannot tell.
- **Barrier.** Cleanup of a life runs only after the round's `gc/state` CAS and after every protection walk of
  that round that starts at the parent cursor has finished. Single-node phase order gives this today.

The predicate of [§5.1](#licence) needs only: a catalog read, `gc/state`, the adopted fold seal's coverage row
for the incarnation, the life's `_ckpt`, and the round's `(lease.owner, lease.seq)`. `seq` is minted per round,
so equality pins the leader and the seal.

## 13. Risks, limits and calls made here {#calls}

**Risks**

- Cleanup goes from almost never to every eligible round. The stale-`_ckpt` outcomes of
  [§5.5](#stale-ckpt-readers) become routine. All but the `fsck` tail walk are handled today; that one is step 1.
- The reader enumeration in the report is a search over key constructors and their callers at `3fa3a01203b`,
  not a proof that no other reader exists. A new reader must take its positions from its own row, `_ckpt` and
  seal row.
- Write settlement by a deposed writer was not traced in the report. No catalog row is its input, so this
  change does not bear on it.
- No product code and no test of this spec was run. The model suite ran ([§9](#tla-impact)). The lane check
  is the first measurement.
- Incarnation reuse across admission retries (CAS-449) is open in the code this spec builds on. Step 3 does
  not start before step 0 merges.

**Calls made in this spec** (not decided by the owner; strike any of them):

| # | Call | Reason |
|---|---|---|
| K1 | Counters ship in the same pull request as the licence change, in post-change form | The pre-change measurement CAS-29.7 asked for exists ([§2](#problem)); a counter of refusals that no longer happen would be dead on arrival |
| K2 | `CASRefCleanupChunksUnderCatalogChange` exists | It is the only direct measure of what the change bought; one comparison per chunk |
| K3 | Two pass-stop counters, not one | A moved lease is routine on deposition; an unverifiable or ambiguous catalog is an alert |
| K4 | No setting to restore the old condition | It would be a fallback path with no user |
| K5 | The fresh catalog read stays per chunk, as today | It bounds work on a changed row to one chunk ([§5.4](#why-row-and-life)). It is not needed for data safety. Cost: one catalog `GET` per chunk, not measured |
| K6 | CAS-448 is its own pull request and lands first | It is correct on its own and removes the one false verdict before cleanup becomes frequent |
| K7 | An erased own row is a namespace skip, not a pass stop | Its keys are dead-life debris for the janitor; other namespaces are unaffected |

## 14. Decisions for the owner {#owner-decisions}

None open. Decided on 2026-11-11:

| Question | Decision |
|---|---|
| Write the `CaRefCleanupLicenceCore` model before step 3? | Yes |
| Backport to `antalya-26.6`? | No |

## 15. Left to the builder {#builder}

- How the licence returns three outcomes (delete, skip namespace, stop pass) and where the counters are bumped.
- The new `Authority` values of `RefCleanupAuthorityRaceBackend` (own-row move, own-row erase, alias admit),
  the `fsck` tail fixture helper, and a two-namespace seed helper if calling `seedTwoCoveredLogs` twice is
  awkward.
- The test seam `armGcFenceMoveOnAuthorityHoldsRevalidationForTest` relies on a catalog read followed by a
  `gc/state` read. Keep that order or move the seam.
- Exact wording of log lines and of the `ProfileEvents` descriptions.
