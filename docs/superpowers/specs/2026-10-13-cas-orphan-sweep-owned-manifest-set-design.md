---
description: 'CAS-358: the fold maintains a durable owned-manifest set per gc-shard; the orphan sweep decides from that set and the adopted seal alone and never reads a ref log, a `_snap` or a `_ckpt`.'
sidebar_label: 'CAS GC: owned-manifest set'
sidebar_position: 108
slug: /superpowers/specs/cas-orphan-sweep-owned-manifest-set-design
title: 'CAS GC: the orphan sweep reads the owned-manifest set, not the ref log'
doc_type: 'reference'
---

# CAS GC: the orphan sweep reads the owned-manifest set, not the ref log — design {#cas-orphan-sweep-owned-manifest-set}

- Date: 2026-10-13. Status: rev.1. Task: CAS-358. Depends on: CAS-36 (floor before the owner view), CAS-362/363 (unique writer epochs). Closes: CAS-87.
- Code facts verified against `45854cb0ee7` (`cas-gc-rebuild`). Numbers from https://github.com/Altinity/ClickHouse/pull/2542.

## 1. Decision {#decision}

- **The ref log is read once, by the fold, from the cursor forward.** The orphan sweep reads no ref log, no `_snap` and no `_ckpt`. `activeManifestKeys` and the committed-tail walk in `Gc/CasOrphanManifestSweep.cpp` are deleted, together with their recovery-op budget.
- **The fold keeps a durable owned-manifest set.** One `manifest_owned` run per gc-shard under the generation prefix, rows `(life_id, ManifestRef)`, merged by the same streaming prior-run-plus-deltas pass the blob source-edge runs use. `+1` adds a row, `-1` removes it. No counter: the ref protocol admits one owner per manifest (`Pool/CasRefProtocol.h:375`, `Promote` emits no net edge, `CasRefProtocol.cpp:765`).
- **The sweep's owner view is the set as of the fold cursor `C`.** A listed key of a catalog-named life is retained if its life has no coverage row, is held, is not covered by the set, or is in the set, or its build epoch is not strictly below `C.writer_epoch` (rule (1) of the §6 premise). Rule (2), the tail-removal test, is subsumed: a manifest removed above `C` was owned at `C` and is in the set.
- **A set is born at the cursor, never at the frontier.** A life whose parent cursor is `{0,0}` is covered from its first fold. A life older than the feature is seeded once from `_snap` plus the logs `(P, C]` with a stop position at `C`; while the `_snap` position `P` is above `C` the seed waits one round. No per-life `as_of` state exists.
- **Forward compatible with no operator action.** The fold seal does not change (its 26.6.4 reader is `Strict`). The runs and their pointer live in new objects under `gc/gen/<G>/attempt/<A>/`, which an older leader prunes with the generation without knowing them. An absent pointer or an absent run is "no set": the sweep retains, the fold re-seeds. No existing object changes bytes; no format version bump.

## 2. The defect {#defect}

### 2.1 Code facts {#code-facts}

- `planManifestCursorPage` (`CasOrphanManifestSweep.cpp:667`) builds, per namespace with a candidate on the page, `activeManifestKeys` (`:192`): `recoverRefTableDetailedFromAuthority` (`Pool/CasRefProtocol.cpp:1042`) replays `_snap` plus every log up to `committed_through`, then a second walk from the fold cursor to `committed_through` collects removal targets. Memoized within one page only (`manifest_sweep_list_budget_keys = 1000`).
- Without a `_snap` the replay starts at `{life_epoch, 1}` (`Pool/CasRefCkpt.cpp:176-180`). Every remount adds a seal-only epoch (INV-2), so the walk grows with table age.
- The fold already extracts the same edges (`manifestEdgesOfTxn`, `Gc/CasGc.cpp:2637`) and drops them after reading the body.
- `sweepNamespace` (`:560`, used by `Tools/CasDecommission.cpp:287`) builds the same view for one namespace.

### 2.2 The cost {#cost}

One writer, two readers, ~800 tables, 26.6.4 Antalya:

| metric | value |
|---|---|
| ref streams | 792 |
| streams with a `_snap` | 74 |
| epochs that are a single `epoch_seal` | 3,876 |
| GC leader successful `GET`s per day | ~956 k, mostly this replay |
| GC leader 404 `GET`s per day before PR 2542 | ~2.4 M, hint overshoot at epoch crossings |

Shortening the replay (a `_snap` threshold that counts seal-only epochs, PR 2542's LIST-bounded hints) keeps it. The fold has already consumed every record below `C`; reading them again is the defect.

## 3. Why the set as of `C` is a complete owner view {#why-complete}

Three positions per life: `C` the fold cursor (`RefCoverage.last_folded_ref_id`), `P` the writer's `_snap` position, `T` `committed_through`. `C <= T`, `P <= T`, no order between `C` and `P`.

The sweep's universe is builds the mount floor has ended (`prefixEligibleUnder`). For a listed key `K` of build epoch `E`:

- **`K` removed above `C`.** Its `-1` is unfolded and the fold still needs the body (`foldManifestEdges` reads it for `-1`). `K` was owned at `C`, so `K` is in the set: retained. Every merge in the last round is this case.
- **`K` created above `C`.** Grants do not cross epochs, so `E` is the live epoch, and `C.writer_epoch <= E`: rule (1) retains. CAS-362/363 make writer epochs unique and monotone per life, which rule (1) assumes.
- **`K` not in the set, `E < C.writer_epoch`.** Every record of epoch `E` is folded (an epoch is left only over its consumed seal, INV-2). `K` was unowned at `C`, and nothing below `C` can still name it. A `+1` for a build below the floor is impossible: the `+1` precedes the build's end, the floor advances only over ended builds. Deletable.

Observation order stays CAS-36's: floor, catalog cut, then the view. The set is older than the floor by construction (written last round), so a build that ended between `C` and the floor read falls under rule (1).

## 4. Design {#design}

### 4.1 Objects {#objects}

| object | key | writer | reader strictness |
|---|---|---|---|
| owned run | `gc/gen/<G>/attempt/<A>/manifest_owned/<shard>/<seq>` (`Layout::manifestOwnedRunKey`) | fold, `putDeterministicArtifact` | `cas_run` header with kind `manifest_owned`; unknown kind fails closed as today |
| owned seal | `gc/gen/<G>/attempt/<A>/manifest_owned_seal` (`Layout::manifestOwnedSealKey`) | fold, `putDeterministicArtifact`, before the fold seal PUT (`CasGc.cpp:3500`) | `Tolerant` |

**Owned run rows.** NDJSON `cas_run`, sorted by key, trailer `{"n"}` and whole-object CityHash128 as `source_edge` runs (`Formats/CasRecordStreamFormat.h`). Key = `life_id` (16 bytes) ++ `writer_epoch` (u64 BE) ++ `build_sequence` (u64 BE) ++ `manifest_ordinal` (u32 BE); one marker byte payload. `life_id` is the opaque physical life id the fold seal keys `ref_lives` by, so a reborn namespace never shares rows with its predecessor.

**Shard.** `manifestOwnedShard(life_id, gc_shards)`: high 64 bits of the life id modulo `gc_shards`, the `blobShard` rule (`Gc/CasGcShardPlan.h:30`). All rows of one life sit in one run, so a sweep page, sorted by `cas/manifests/<ns>/`, opens one or two runs.

**Owned seal.** Raw text, deterministic (sorted), line and object caps as the fold seal:

```
{"type":"cas_manifest_owned_seal","v":1}
{"generation":"G","parent_generation":"P"}
{"t":"run","key":"...","checksum":"<hex128>","shard":"s","key_generation":"g"}   one per shard, 0..gc_shards-1
{"t":"life","id":"<hex128>"}                                                    one per COVERED life
{"n":N}
```

A `run` record is the `RunRef` of `Formats/CasFoldSealFormat.h:25`. A `life` record is positive evidence: a life absent from the list is not covered. The reader skips unknown keys and unknown `t` words.

### 4.2 Fold {#fold}

Inside the per-log staging at `CasGc.cpp:2665-2680`: every `RefManifestEdge` that `foldManifestEdges` accepts also stages `ManifestOwnedDelta{life_id, ref, remove = change < 0}`. A log merged into the round buffers merges its owned deltas with it; a mid-log clamp discards them with the rest. A dead precommit skipped at `:2672` stages nothing. Precommit bindings are owners and are in the set.

Per shard, after the blob merge loop (`:3347`):

- **Pure carry**: no deltas and no seed for the shard this round: copy the parent `RunRef` verbatim.
- **Merge**: three sorted inputs, prior run rows, seed rows (§4.3), round deltas in transaction order; output the new run. A `remove` for an absent key is a no-op, counted in `CASGCManifestOwnedUnmatchedRemoves` with one `WARNING` per round carrying one example. Expected zero: seeds are exact at `C`, so there is no idempotency slack to absorb.
- A life whose seed is deferred this round has its deltas **dropped**; it stays uncovered and is seeded next round at the new cursor, which includes this round's transactions.

The seal's `covered` list = parent's covered lives, plus lives with parent cursor `{0,0}` in this round's walk plan, plus lives seeded this round, minus lives whose fold-state row carries `cleanup_evidence` (their rows are gone with the `remove_namespace` `-1`s).

The owned seal is written before the fold seal, both with `putDeterministicArtifact`. A deposed leader's pair is reclaimed with its attempt by the generation prune.

Retention: `referenced_generations` at `CasGc.cpp:971-979` and the post-CAS hand-off at `:1038-1065` include the owned runs' `key_generation`, adopted and parent.

### 4.3 Seed {#seed}

For a life in the walk plan with parent cursor `C > {0,0}` and not in the parent's covered list:

1. `chooseRecoveryGrounding(catalog_entry, ckpt)` gives `base` (the `_snap` position `P`, or none) and `walk_from`.
2. If `base > C`: defer, `CASGCManifestOwnedSeedsDeferred`. The state at `C` is not reconstructible: `planRefCleanup` deletes a log only when it is `< P` and `<= C`, so logs below `C` may be gone once `P` passed it. Next round `C' >= T >= P` unless the life clamped.
3. Else `recoverRefTableDetailedFromAuthority(..., stop_at = C)`: the walk loop bound becomes `id <= min(committed_through, stop_at)`; every log in `(P, C]` exists by the same cleanup rule. Rows = committed bindings plus precommits of the recovered state, into the shard's seed input. `CASGCManifestOwnedSeeds`.
4. Seeds are bounded per round by `GcRoundWorkBudget::max_seed_ops`, one unit per ref-log `GET`/decode of the seed walk, fed by the existing `gc_round_sweep_recovery_op_budget` (default 5000): the setting keeps its meaning, ref-log work GC spends on recovery per round, and moves from the sweep to the fold. Rounds have no time deadline in this tree (`Pool/CasPool.h:154`); when one lands, seeds fall under it. A seed cut by the budget is discarded whole, the life stays uncovered.

A fresh pool and the GC rebuild path (`CasGc.cpp:4384-4565`) recover every life to the position they seal as cursor and emit the set from the same recovered tables: every life covered, no seed.

### 4.4 Sweep {#sweep}

`planManifestCursorPage` per parsed key, in order:

1. floor (`floorForNamespace`, CAS-36), `prefixEligibleUnder`;
2. catalog entry; a rowless key passes on the floor alone, as CAS-36 decided;
3. coverage row of the life from the adopted fold seal (`coverageOf`): absent → `NoCoverage`, held or clamped → `Hold`;
4. owned seal (read once per page, with the fold seal): absent, or life not in `covered`, or the life's shard run absent → `OwnedSetMissing`, retain;
5. membership: the shard run is opened once per page and streamed into `std::set<(life_id, ManifestRef)>` for the lives on the page; present → skip;
6. `manifestDeletionPremise`, rule (1) only; then nomination as today (frozen `GET`, body decode, blob-source retirements).

`NamespaceFoldView` loses `tail_removal_targets`; `SweepRetainClass::TailRemoval` is replaced by `OwnedSetMissing`; `ManifestSweepResult.retained_tail_removal` by `retained_owned_set_missing`. `sweepNamespace` applies the same six steps for its one namespace. `GcRoundWorkBudget::max_sweep_recovery_ops` becomes `max_seed_ops`, charged by the fold's seed walk (§4.3); the sweep charges nothing to it.

### 4.5 Compatibility {#compatibility}

| situation | what happens |
|---|---|
| old leader folds | writes blob runs and a fold seal, no owned seal; prunes `gc/gen/<g>/` by prefix (`CasGc.cpp:3766`), which removes owned runs and seals of pruned generations |
| new leader adopts a generation without an owned seal | every life uncovered; sweep retains all catalog-named lives; fold seeds within the deadline, round after round; `CASGCManifestOwnedSealAbsent` |
| owned seal present, a referenced run returns 404 (an old leader pruned it pre-CAS and then lost its CAS) | the shard's lives are uncovered: sweep retains them, fold re-seeds; `CASGCManifestOwnedRunAbsent`, `WARNING` |
| run present, checksum mismatch | `CORRUPTED_DATA`, as blob runs |
| new leader, old data | fold seal bytes identical to a tree without this change for the same inputs (golden test) |

Absence is a recognized state for this family only because an older version is allowed to delete it. Nothing on an absence path deletes.

## 5. Change {#change}

- `Formats/CasLayout.h`: `manifestOwnedRunKey`, `manifestOwnedSealKey`, inverse parsers for `cas-inspect`.
- `Formats/CasRecordStreamFormat.h`: kind word `manifest_owned`; row codec `ManifestOwnedKeyCodec` (pack/parse, 36 bytes).
- `Formats/CasManifestOwnedSealFormat.{h,cpp}`: `CasManifestOwnedSeal{generation, parent_generation, runs, covered}`, `encode`/`decode` (`Tolerant`), caps shared with the fold seal.
- `Gc/CasGcShardPlan.{h,cpp}`: `manifestOwnedShard`.
- `Gc/CasManifestOwned.{h,cpp}`: `ManifestOwnedDelta`, `mergeManifestOwnedRun(prior, seed_rows, deltas, out RunRef)`, `openManifestOwnedRun`, `readAdoptedManifestOwnedSeal`.
- `Gc/CasGc.cpp`: staging at `:2651`; seeds and per-shard merge after `:3347`; owned seal PUT before `:3500`; retention at `:971` and `:1038`; rebuild path at `:4384-4565`; `CASGCManifestOwned*` events; `fold_reduce` metrics `manifest_owned_rows`, `manifest_owned_seeds`, `manifest_owned_seeds_deferred`.
- `Pool/CasRefProtocol.{h,cpp}`: `recoverRefTableDetailedFromAuthority` gains `std::optional<RefTxnId> stop_at`; `fsck` and the rebuild path pass none.
- `Gc/CasOrphanManifestSweep.{h,cpp}`: delete `activeManifestKeys`, `NamespaceProtection`, the tail walk, `OutstandingHintGuard`; `planManifestCursorPage` and `sweepNamespace` per §4.4; `manifestDeletionPremise` rule (1) only.
- `Gc/CasBlobInDegree.h`: `GcRoundWorkBudget` sweep recovery-op fields renamed to seed-op fields; `Pool/CasPool.h` doc string of `gc_round_sweep_recovery_op_budget` updated, name and default unchanged.
- `Tools/CasFsck.cpp`, `Tools/CasInspect`: list and verify the new family (checksum, row order, covered lives subset of `ref_lives`).

No on-S3 byte change to any existing object. No fallback path: an unreadable owned seal or run with a present body propagates; only an absent one is a state.

## 6. Tests, failing first {#tests}

`src/Disks/tests/gtest_cas_orphan_manifest_sweep.cpp` (T1-T5, T9), `gtest_cas_gc_fold.cpp` (T6-T8, T10), new `gtest_cas_manifest_owned.cpp` (T11-T13), `gtest_cas_fold_seal_format.cpp` (T14). Helpers: `writeManifestRaw`, `setWatermarkMinActive`, `sweepManifestCursorPageForTest`, the fold's seeded-ref-log helpers, `ProfileEvents` snapshots. Order: write all, see T1-T4, T6-T8, T11-T13 fail on `45854cb0ee7`, then apply §5.

| # | Test | Setup | Expect |
|---|---|---|---|
| T1 | `CursorPageReadsNoRefLog` | life with `_snap`, ten logs above `C`, one old-epoch orphan body | `deleted == 1`; `CASRefLogBodyGets` and snapshot `GET`s unchanged across the page; no `_ckpt` `GET` |
| T2 | `CursorPageRetainsManifestRemovedAboveCursor` | merge log above `C` removes `M` (old epoch, below floor) | `M` retained, class `None` (in set), no tail walk |
| T3 | `CursorPageRetainsUncoveredLife` | owned seal lists runs but not the life | `retained_owned_set_missing == 1`, body exists |
| T4 | `CursorPageRetainsWhenOwnedSealAbsent` | fold seal adopted, no owned seal | every catalog-named candidate retained; rowless candidate below floor still deleted |
| T5 | `CursorPageRetainsWhenRunAbsent` | owned seal present, shard run deleted | retained, `CASGCManifestOwnedRunAbsent == 1`, no throw |
| T6 | `FoldSeedsLifeAtCursor` | life with `C > {0,0}`, `_snap` at `P <= C`, logs to `T > C` | after the round: run rows == recovered state at `C` applied with deltas `(C, C']`; life covered; `CASGCManifestOwnedSeeds == 1` |
| T7 | `FoldDefersSeedWhenSnapAboveCursor` | `_snap` at `P > C` | round 1: uncovered, `CASGCManifestOwnedSeedsDeferred == 1`, this round's deltas dropped; round 2: covered, rows exact |
| T8 | `FoldOwnedSetIncremental` | covered life, add/remove/precommit/promote/remove-namespace logs over three rounds | rows after each round == owners of the recovered state at `C`; `UnmatchedRemoves == 0`; after `remove_namespace` folded, life not covered and no rows |
| T9 | `DecommissionSweepUsesOwnedSet` | `sweepNamespace` on a claimed slot | same decisions as the page; no ref-log `GET` |
| T10 | `FoldSealBytesUnchangedByOwnedSet` | golden fold seal captured on `45854cb0ee7` for a fixed fold input | the new tree produces byte-identical fold seal bytes for the same input, plus an owned seal |
| T11 | `OwnedSealTolerantReader` | encoded seal plus an unknown key and an unknown `t` record | decodes, unknown ignored; missing `n` or bad count → `CORRUPTED_DATA` |
| T12 | `OwnedRunMergeDeterministic` | prior + seed + deltas, two orders of equal deltas | byte-identical run; checksum verifies; removal of absent key counted |
| T13 | `OwnedRunKindGate` | `source_edge` header opened as owned run | fails closed |
| T14 | `PruneReclaimsOwnedFamilyWithGeneration` | generation with owned run and seal below `prune_floor`, not referenced | both deleted by `pruneSupersededGenerations`; a referenced owned run's generation retained |

T1 is the acceptance test of the decision: it pins the request classes the sweep page may issue.

Gate: `utils/cas-gate` (`CAS*` filter), ASan lane for the four suites.

## 7. TLA+ {#tla}

`docs/superpowers/models/CaGcRootLocalPartManifestCore.tla`, `GSwView` (`:1100`): today `swOwned'` is the current owner map, the replayed frontier. Add:

- variable `foldOwned`, set at the fold's adoption step to the owner map as folded (`owner[m]` restricted to edges the fold has applied);
- constant `ViewFromFoldState`: `GSwView` takes `swOwned' = foldOwned ∪ {m : buildEpoch(m) = liveEpoch}` and no tail term;
- sabotage `SabotageViewAtCutoverFrontier`: `swOwned'` from the current `owner` map while `foldOwned` lags, expected to violate the body-before-decrements property (`SabotageDeleteBodyBeforeDecrements`'s invariant);
- configs: `fix_ownedset` (positive, all witnesses green), `sab_viewatfrontier` (counterexample required).

Run via `run_gc_partmanifest.sh`; results appended to `CaGcRootLocalPartManifestCore_RESULTS.md`.

## 8. Documentation {#docs}

- `docs/en/antalya/cas/architecture/manifests-and-refs.md#orphan-sweep`: the flowchart's "protection view" node becomes "owned set as of the fold cursor"; the "same complete replay" paragraph is replaced by the three-case argument of §3.
- `docs/en/antalya/cas/architecture/garbage-collection.md#round-cost`: rows `GET`/`PUT` owned runs (1 per non-carry shard), `PUT` owned seal (1), sweep `GET` owned runs (≤ `gc_shards` per page); the sweep's ref-log and `_ckpt` terms removed.
- `docs/en/antalya/cas/architecture/storage-layout.md`: the two keys.
- `Formats/README.md`: the family, its reader strictness, and the absence rule of §4.5.

## 9. Out of scope {#out-of-scope}

- One sequence per namespace (CAS-305 d) turns the cursor into one integer; this design works on the pair.
- The fold's global `LIST cas/ns/stream/` (CAS-29.1) is untouched.
- A `_snap` threshold for seal-only epochs, and the `prev_epoch_seal` hint bound (CAS-359), are independent.
