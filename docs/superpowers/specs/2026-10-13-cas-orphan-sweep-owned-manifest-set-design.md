---
description: 'CAS-358: the fold maintains a durable owned-manifest set per gc-shard; the orphan sweep decides from that set and the adopted seal alone and never reads a ref log, a `_snap` or a `_ckpt`.'
sidebar_label: 'CAS GC: owned-manifest set'
sidebar_position: 108
slug: /superpowers/specs/cas-orphan-sweep-owned-manifest-set-design
title: 'CAS GC: the orphan sweep reads the owned-manifest set, not the ref log'
doc_type: 'reference'
---

# CAS GC: the orphan sweep reads the owned-manifest set, not the ref log — design {#cas-orphan-sweep-owned-manifest-set}

- Date: 2026-10-13. Status: rev.8 (rev.7 plus the §7 extension; codex `gpt-6-astra` high verdict APPROVE WITH MINORS on rev.6, minors applied in rev.7) (reviews: rev.2 eight findings `tmp/cas358/codex_spec_review_1.md`, rev.3 six `_2.md`, rev.4 four `_3.md`, rev.5 five `_4.md`, rev.6 three minors `_5.md`). Task: CAS-358. Depends on: CAS-36 (floor before the owner view), CAS-362/363 (unique, monotone writer epochs; today `allocateWriterEpoch` can restart at 1, `Pool/CasServerRoot.cpp:693-698`). Closes: CAS-87. Absorbs: CAS-86 (read-ahead window on a throwing recovery walk), CAS-310 (the model's unfolded-removal invariant).
- Code facts verified against `45854cb0ee7` (`cas-gc-rebuild`). Numbers from https://github.com/Altinity/ClickHouse/pull/2542.

## 1. Decision {#decision}

- **The ref log is read once, by the fold, from the cursor forward.** The orphan sweep reads no ref log, no `_snap` and no `_ckpt`. `activeManifestKeys` and the committed-tail walk in `Gc/CasOrphanManifestSweep.cpp` are deleted, together with their recovery-op budget.
- **The fold keeps a durable owned-manifest set.** One `manifest_owned` run per gc-shard under the generation prefix, rows `(life_id, ManifestRef)`, merged by the same streaming prior-run-plus-deltas pass the blob source-edge runs use. The set is exactly the ref table's binding set, committed rows plus precommits, as of the fold cursor; it depends on no manifest body. No counter: the ref protocol admits one owner per manifest (`Pool/CasRefProtocol.h:375`; `Promote` emits no net edge, `CasRefProtocol.cpp:765`).
- **The sweep's owner view is the set as of the fold cursor `C` plus the epoch gate.** A listed key of a catalog-named life is retained if its life has no coverage row, is held, is not covered, or is in the set, or its build epoch is not strictly below `C.writer_epoch` (rule (1) of the §6 premise, unchanged). Rule (2), the tail-removal test, is subsumed by the set.
- **A set is born at the cursor, never at the frontier.** A life whose parent cursor is `{0,0}` is covered from its first fold. A life older than the feature is seeded once from `_snap` plus the logs `(P, C]` with a stop position at `C`; while the `_snap` position `P` is above `C` the seed waits. No per-life `as_of` state exists.
- **Forward compatible with no operator action.** The fold seal does not change (its 26.6.4 reader is `Strict`). The runs and their pointer live in new objects under `gc/gen/<G>/attempt/<A>/`, which an older leader prunes with the generation without knowing them. An absent pointer or an absent run is "no set": the sweep retains, the fold re-seeds, as long as the adopted fold seal itself survives (§4.5). No existing object changes bytes; no format version bump.

## 2. The defect {#defect}

### 2.1 Code facts {#code-facts}

- `planManifestCursorPage` (`CasOrphanManifestSweep.cpp:667`) builds, per namespace with a candidate on the page, `activeManifestKeys` (`:192`): `recoverRefTableDetailedFromAuthority` (`Pool/CasRefProtocol.cpp:1042`) replays `_snap` plus every log up to `committed_through`, then a second walk from the fold cursor to `committed_through` collects removal targets. Memoized within one page only (`manifest_sweep_list_budget_keys = 1000`).
- Without a `_snap` the replay starts at `{life_epoch, 1}` (`Pool/CasRefCkpt.cpp:169-180`). Every remount adds a seal-only epoch (INV-2), so the walk grows with table age.
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

## 3. Why the set as of `C` plus the epoch gate is a complete owner view {#why-complete}

Three positions per life: `C` the fold cursor (`RefCoverage.last_folded_ref_id`), `P` the writer's `_snap` position, `T` `committed_through`. `C <= T`, `P <= T`, no order between `C` and `P`.

The sweep's universe is builds the mount floor has ended (`prefixEligibleUnder`). For a listed key `K` of build epoch `E`, judged against its life's sampled cursor `C`:

- **Epoch gate, `E >= C.writer_epoch`: retain.** Grants do not cross epochs, so every `+1` above `C` names a build of epoch `>= C.writer_epoch`. This covers a manifest created above `C`, and a manifest whose whole history, `+1` and `-1`, lies above `C`. Nothing about the set is needed here. With writer epochs unique and monotone per life (CAS-362/363) the comparison is a proof; without them it is not.
- **`K` in the set: retain.** The set is the binding set at `C`. A manifest owned at `C` whose `-1` is above `C` is in it, and the fold still needs its body to emit the blob `-1`s (`foldManifestEdges` reads it for `-1`). Every merge in the last round is this case.
- **Otherwise, `E < C.writer_epoch` and `K` not in the set: delete.** All records of epoch `E` are folded (an epoch is left only over its consumed seal, INV-2). `K` is unowned at `C`. No record above `C` can name it: a `+1` above `C` has epoch `>= C.writer_epoch > E`; a `-1` above `C` presupposes a `+1`, which is either folded, contradicting unowned at `C`, or above `C`, contradicting the epoch. A `+1` for a build below the floor is impossible in any case: the `+1` precedes the build's end, the floor advances only over ended builds.

Observation order stays CAS-36's: floor, catalog cut, then the view. The set is older than the floor by construction (written last round), so a build that ended between `C` and the floor read falls under the epoch gate. A life reborn under the same name has a new opaque life id, a new coverage row and no rows in the set; a leader change between fold and sweep hands the new leader the same adopted, verified bytes.

## 4. Design {#design}

### 4.1 Objects {#objects}

| object | key | writer | reader strictness |
|---|---|---|---|
| owned run | `gc/gen/<G>/attempt/<A>/manifest_owned/<shard>/<seq>` (`Layout::manifestOwnedRunKey`) | fold, `putDeterministicArtifact` | `cas_run` header with kind `manifest_owned`; unknown kind fails closed as today |
| owned seal | `gc/gen/<G>/attempt/<A>/manifest_owned_seal` (`Layout::manifestOwnedSealKey`) | fold, `putDeterministicArtifact`, before the fold seal PUT (`CasGc.cpp:3500`) | `Tolerant` |

**Owned run rows.** NDJSON `cas_run`, sorted by key, trailer `{"n"}` and whole-object CityHash128 as `source_edge` runs (`Formats/CasRecordStreamFormat.h`). Key = `life_id` (16 bytes) ++ `writer_epoch` (u64 BE) ++ `build_sequence` (u64 BE) ++ `manifest_ordinal` (u32 BE); one marker byte payload. `life_id` is the opaque physical life id the fold seal keys `ref_lives` by, so a reborn namespace never shares rows with its predecessor. Run cardinality is unbounded by the format (`CasRecordStreamFormat.h:54-58`); readers stream.

**Shard.** `manifestOwnedShard(life_id, gc_shards)`: high 64 bits of the life id modulo `gc_shards`, the `blobShard` rule (`Gc/CasGcShardPlan.h:30`). All rows of one life sit in one run. Life ids are opaque, so the shard of a namespace is unrelated to its position in a `cas/manifests/` listing; a page may need every shard.

**Owned seal.** Raw text, deterministic (sorted), line and object caps as the fold seal:

```
{"type":"cas_manifest_owned_seal","v":1}
{"generation":"G","parent_generation":"P"}
{"t":"run","key":"...","checksum":"<hex128>","shard":"s","key_generation":"g"}   one per shard that HAS a run
{"t":"life","id":"<hex128>"}                                                    one per COVERED life
{"t":"seed_after","id":"<hex128>"}                                              the last life whose seed was ATTEMPTED, optional
{"n":N}
```

A `run` record is the `RunRef` of `Formats/CasFoldSealFormat.h:25`. A shard without a `run` record has an empty set and no covered lives. A `life` record is positive evidence: a life absent from the list is not covered. `seed_after` is the fairness cursor of §4.3. The reader skips unknown keys and unknown `t` words.

### 4.2 Fold {#fold}

**Deltas mirror owner transitions, not bodies.** In the per-log staging at `CasGc.cpp:2665-2680`, before `foldManifestEdges` is consulted, every `RefManifestEdge` stages `ManifestOwnedDelta{life_id, ref, remove = change < 0}`: `AddPrecommit` adds, `RemovePrecommit` and `RemoveCommitted` remove, `Promote` stages nothing. The staging does not depend on whether the body exists or on the dead-precommit skip at `:2672-2690`, and a precommit removal whose body never existed (`:2674-2675`) still removes its row. This is what makes seeds (recovered committed rows plus precommits, no body check), the rebuild path and incremental maintenance agree on one definition. A log merged into the round buffers merges its owned deltas with it; a mid-log clamp discards them with the rest.

**Effective parent coverage.** Before the merge, the fold reads the parent owned seal (absent → no coverage) and `HEAD`s every carried run it names; a run that is absent invalidates its shard: its `RunRef` is dropped and every covered life of that shard becomes uncovered (`CASGCManifestOwnedRunAbsent`, `WARNING`). All later steps use this effective coverage, never the raw parent list.

**Per shard**, after the blob merge loop (`:3347`):

- **Pure carry**: no deltas and no seed for the shard this round, and the carried run answered the `HEAD`: copy the parent `RunRef` verbatim.
- **Merge**: a streaming pass producing at most one row per key. Baseline per key: the seed row if the key's life is seeded this round, else the prior-run row; a prior-run row for a life seeded this round is `CORRUPTED_DATA` (an uncovered life has no rows). Then this round's deltas for the key in `(transaction position, op_ordinal)` order, the order `manifestEdgesOfTxn` emits (`CasRefProtocol.cpp:744-768`), applied as set operations: `remove` on an absent key is a no-op; cases: prior or seed row plus remove → absent; no baseline, add then remove → absent; remove then add → present; prior row, no delta → present. The existing reducer's stable same-key ordering (`Gc/CasBlobInDegree.cpp:376-388`, `638-664`) is reused. A no-op remove is counted in `CASGCManifestOwnedUnmatchedRemoves` with one `WARNING` per round carrying one example; expected zero.
- **Delta admission.** Owned deltas enter the merge only for lives that are effectively covered by the parent, have parent cursor `{0,0}`, or were seeded successfully this round. Deltas of every other life are **dropped**: deferred, failed, skipped-held and not-admitted seeds alike. Such a life stays uncovered, keeps no rows, and is seeded later at a cursor that includes this round's transactions. A covered life that clamps mid-round keeps the deltas of its accepted logs; the clamp discards only the clamped log (`CasGc.cpp:2730-2736`).
- **Empty initialization.** A shard that gains covered lives (a `{0,0}`-cursor life whose logs carry only birth and seal operations, or a rebuild with no owners) and has no prior run writes a canonical empty run, checksum and trailer included, so that §4.1's rule "no `run` record means no covered lives" holds. The write counts toward `M`.
- **A merge `GET` that returns 404 after a successful `HEAD`** re-evaluates effective coverage for the shard: the parent-covered lives of the shard become uncovered and their deltas are dropped; seeded and `{0,0}` lives proceed against an empty baseline. `HEAD` is not a retention pin.

The seal's `covered` list = effective parent coverage, plus lives with parent cursor `{0,0}` in this round's walk plan, plus lives seeded this round, minus lives whose fold-state row carries `cleanup_evidence` (their rows went with the `remove_namespace` `-1`s).

The owned seal is written before the fold seal, both with `putDeterministicArtifact`. A deposed leader's pair is reclaimed with its attempt by the generation prune.

Retention: `referenced_generations` at `CasGc.cpp:971-979` and the post-CAS hand-off at `:1038-1065` include the owned runs' `key_generation`, adopted and parent.

### 4.3 Seed {#seed}

A seed candidate is a life in the walk plan with parent cursor `C > {0,0}`, not effectively covered, not held this round, and whose sampled `_ckpt` names a frontier `T >= C`. A life whose `_ckpt` is unreadable or whose grounding throws is already held by intake (`CasGc.cpp:2343-2359`) and is skipped (`CASGCManifestOwnedSeedsSkippedHeld`). An absent frontier or `T < C` is an intake anomaly that suppresses destruction but does not hold the life (`CasGc.cpp:2501-2511`, `2535-2544`); such a life is skipped too (`CASGCManifestOwnedSeedsSkippedAnomaly`), because a walk bounded by `min(T, C)` would stop below `C` and publish false coverage. No seed exception escapes to the round.

**Fair admission.** Candidates are attempted in life-id order starting after the parent owned seal's `seed_after`, wrapping around; every attempt, successful or failed, moves `seed_after` to that life; each candidate is visited at most once per round; a round that attempts no seed carries the parent's `seed_after` verbatim. The cursor lives in the attempt-scoped owned seal and becomes durable with the round's CAS, so an old leader's round loses it (the next feature-aware round starts from the smallest life id). A life whose seed fails every round (a persistently missing historical log) costs one attempt per cycle and cannot keep a later candidate from ever being admitted; over a stable candidate population and adopted feature-aware rounds, each candidate is attempted at least once in `N` rounds.

1. `chooseRecoveryGrounding(catalog_entry, ckpt)` gives `base` (the `_snap` position `P`, or none) and `walk_from`.
2. If `base > C`: defer, `CASGCManifestOwnedSeedsDeferred`. The state at `C` is not reconstructible: `planRefCleanup` deletes a log only when it is `< P` and `<= C` (`CasRefProtocol.cpp:837-845`), so logs below `C` may be gone once `P` passed it. Deferral repeats while `P > C`: a clamp that leaves `C' < P`, or a newer `_snap` above `C'`, defers again. Under normal operation the next round folds to `T >= P`.
3. Else `recoverRefTableDetailedFromAuthority(..., stop_at = C)`: both the walk loop bound and the read-ahead hint bound (`CasRefProtocol.cpp:1074` hints against `committed_through` today) become `min(committed_through, stop_at)`: no log request is issued above `C`. At an epoch crossing up to one window may extend past the earlier epoch's seal while staying below `C`; the moved `OutstandingHintGuard` discards it. The helper returns whatever position it reached (`CasRefProtocol.cpp:1067-1114`), so the caller checks it: the seed is successful only if the recovered state's greatest applied id equals `C`; anything else is a failed seed (step 4). Rows = committed bindings plus precommits of the recovered state (no body check), into the shard's seed input. `CASGCManifestOwnedSeeds`.
4. **Failure is a failed seed, not corruption.** The cleanup rule is a property of each leader's own cleanup plan, not a pin: a concurrent leader's post-CAS ref-object cleanup (`cleanupRefObjects`, `CasGc.cpp:1183`, after its own CAS at `:991-1006`), planned from newer adopted authority and a newer `_snap`, may delete a log in `(P, C]` that this stale attempt's walk still needs (`CasRefProtocol.cpp:1073-1077` throws on an absent log); the stale attempt cannot adopt against changed authority in any case. With `stop_at` set, an absent log, a decode error or a snapshot checksum mismatch ends the seed: the life stays uncovered, `CASGCManifestOwnedSeedFailed`, one `WARNING` with the key, the round continues; the next round re-samples grounding and repeats step 2. The writer-facing walk (no `stop_at`) keeps its `CORRUPTED_DATA` throw.
5. **Budget is an admission limit.** A seed starts only while `max_seed_ops == 0 || seed_ops_used < max_seed_ops` (zero is unlimited, the contract of every `GcRoundWorkBudget` bound, `Gc/CasBlobInDegree.h:252-259`); a started seed runs to its end, and every request it issues is charged when issued: snapshot body, grounding logs, walk logs, and speculative read-ahead requests that are later discarded without decoding (`Gc/CasGcReadAhead.cpp:92-105` issues hints as real reads and waits for them on discard); a `take` of an already-charged hint is not charged again. A life needing more than the budget therefore completes in one round and exhausts that round's seeds; it is never restarted from scratch. The bound is the life's log count, the price today's sweep pays per page. `max_seed_ops` is fed by the existing `gc_round_sweep_recovery_op_budget` (default 5000, `Pool/CasPool.h:115`): the setting keeps its meaning, ref-log work GC spends on recovery per round, and moves from the sweep to the fold. Rounds have no time deadline in this tree (`Pool/CasPool.h:154`); when one lands, seeds fall under it.

A fresh pool and the GC rebuild path (`CasGc.cpp:4384-4565`) recover every life to the position they seal as cursor and emit the set from the same recovered tables: every life covered, no seed.

### 4.4 Sweep {#sweep}

`planManifestCursorPage` in two passes over the page. Before pass 1, the floor of every distinct namespace on the page is sampled (`floorForNamespace`), and only then the shared catalog cut is read: the catalog read today sits at `:704`, before the floors at `:768`, and the unsafe order (rowless cut sampled, writer creates and promotes, floor sampled late) is what CAS-36's `FixFloorBeforeCatalog` forbids. A `catalogEntryOf` lookup after a per-key floor read is not the same thing.

**Pass 1, per parsed key**, in order:

1. `prefixEligibleUnder` against the pre-sampled floor;
2. catalog entry from the cut; a rowless key passes on the floor alone, as CAS-36 decided;
3. coverage row of the life from the adopted fold seal (`coverageOf`): absent → `NoCoverage`, held or clamped → `Hold`;
4. owned seal (read once per page, with the fold seal): absent, or life not in `covered`, or no `run` record for the life's shard → `OwnedSetMissing`, retain;
5. epoch gate (`manifestDeletionPremise`, rule (1) only): `E >= C.writer_epoch` → `UnconsumedSeal`, retain;
6. otherwise the key is a **candidate**, recorded under its `(shard, life)`.

**Pass 2, per shard with candidates**: open the run once, stream it to the end, verify the checksum, and mark each candidate row found. Negative membership is used only after a verified full drain; a run that fails to open or verify retains every candidate of its shard (`OwnedSetMissing` on 404, `CORRUPTED_DATA` on mismatch). Membership bookkeeping is bounded by the page's candidates plus decoder scratch; the backend stream buffer of an open run, the catalog cut and the seals are outside that bound (`Gc/CasBlobInDegree.h:137-140`; an in-memory backend copies the whole object, `Backend/CasInMemoryBackend.cpp:94-96`). Opens per page `<= min(gc_shards, distinct candidate lives)`.

Candidates not found are nominated as today (frozen `GET`, body decode, blob-source retirements, deletion after the round CAS).

`NamespaceFoldView` loses `tail_removal_targets`; `SweepRetainClass::TailRemoval` is replaced by `OwnedSetMissing`; `ManifestSweepResult.retained_tail_removal` by `retained_owned_set_missing`. `sweepNamespace` applies the same steps for its one namespace. `GcRoundWorkBudget::max_sweep_recovery_ops` becomes `max_seed_ops`, charged by the fold's seed walk (§4.3); the sweep charges nothing to it.

### 4.5 Compatibility {#compatibility}

| situation | what happens |
|---|---|
| old leader folds | writes blob runs and a fold seal, no owned seal; prunes `gc/gen/<g>/` by prefix (`CasGc.cpp:3766`), which removes owned runs and seals of pruned generations |
| new leader adopts a generation without an owned seal | every life uncovered; sweep retains all catalog-named lives; fold seeds within the budget, round after round; `CASGCManifestOwnedSealAbsent` |
| owned seal present, a referenced run in an OLDER generation returns 404 (an old leader pruned that carried generation pre-CAS and then lost its CAS) | the shard's lives are uncovered (effective coverage, §4.2): sweep retains them, fold re-seeds; `CASGCManifestOwnedRunAbsent`, `WARNING` |
| the ADOPTED generation `G` is pruned pre-CAS by a leader that then loses its CAS (`keep = 1`, `G` holds only seals and runs no blob `RunRef` points at) | the adopted fold seal is gone with it; the next fold throws at `CasGc.cpp:1791` and needs `SYSTEM CAS GC REBUILD`. Pre-existing retention defect, independent of this family, tracked as CAS-386; the automatic-recovery claim of this design holds whenever the adopted fold seal survives |
| run present, checksum mismatch | `CORRUPTED_DATA`, as blob runs |
| deposed new leader's attempt | not adopted, never reader-visible, reclaimed with the generation |
| new leader, old data | fold seal encoding of an identical `CasFoldSeal` value is byte-identical to the base tree (golden test); whole-round outputs may differ where retention decisions differ |

Absence is a recognized state for this family only because an older version is allowed to delete it. Nothing on an absence path deletes.

### 4.6 Cost {#cost-design}

Per folding round with `M` shards that merge (deltas or seeds) and `gc_shards - M` carried:

| operation | count |
|---|---|
| `GET` parent owned seal | 1 (fold) + 1 per sweep page |
| `HEAD` parent owned runs | `H <= gc_shards`, one per `run` record of the parent owned seal; a shard with no parent run needs none; a `HEAD`ed shard may still merge (seed after 404) |
| `GET` prior owned runs | `<= M` (where a prior run exists) |
| `PUT` owned runs | `M`, empty initializations included |
| `PUT` owned seal | 1 |
| seed reads | charged to `max_seed_ops` when issued: snapshot body, its same-position log and predecessor seal where the grounding needs them (`CasRefProtocol.cpp:971-1028`), one `GET` per log in `(P, C]`, plus discarded speculative reads: at most one read-ahead window per epoch crossing (positions past an earlier epoch's seal, still below `C`) and one window on an early failure; no log request above `C` is ever issued |
| sweep run reads | `<= min(gc_shards, candidate lives)` per page; no ref-log, `_snap` or `_ckpt` reads |

Deterministic-write conflicts and retries add requests as for blob runs. Bytes per rewritten run are one row per owned manifest of the shard, bodiless precommits and inline-only manifests included; on part-sized manifests that is fewer rows than the blob runs' one per file, but the comparison is workload-dependent, not a bound.

## 5. Change {#change}

- `Formats/CasLayout.h`: `manifestOwnedRunKey`, `manifestOwnedSealKey`, inverse parsers for `cas-inspect`.
- `Formats/CasRecordStreamFormat.h`: kind word `manifest_owned`; row codec `ManifestOwnedKeyCodec` (pack/parse, 36 bytes).
- `Formats/CasManifestOwnedSealFormat.{h,cpp}`: `CasManifestOwnedSeal{generation, parent_generation, runs, covered, std::optional<UInt128> seed_after}`, `encode`/`decode` (`Tolerant`), caps shared with the fold seal.
- `Gc/CasGcShardPlan.{h,cpp}`: `manifestOwnedShard`.
- `Gc/CasManifestOwned.{h,cpp}`: `ManifestOwnedDelta`, `mergeManifestOwnedRun(prior, seed_rows, deltas, out RunRef)`, `openManifestOwnedRun`, `readAdoptedManifestOwnedSeal`, `markCandidatesInRun`.
- `Gc/CasGc.cpp`: staging at `:2665`; effective coverage, seeds and per-shard merge after `:3347`; owned seal PUT before `:3500`; retention at `:971` and `:1038`; rebuild path at `:4384-4565`; `CASGCManifestOwned*` events; `fold_reduce` metrics `manifest_owned_rows`, `manifest_owned_seeds`, `manifest_owned_seeds_deferred`, `manifest_owned_seeds_failed`.
- `Pool/CasRefProtocol.{h,cpp}`: `recoverRefTableDetailedFromAuthority` gains `std::optional<RefTxnId> stop_at`; with it set, an absent log or decode failure is reported to the caller instead of thrown; `fsck` and the rebuild path pass none. The sweep's `OutstandingHintGuard` moves into this walk instead of being deleted: the seed runs on the fold's shared read-ahead reader, and a walk that ends early must free its hinted range (CAS-86).
- `Gc/CasOrphanManifestSweep.{h,cpp}`: delete `activeManifestKeys`, `NamespaceProtection`, the tail walk; `planManifestCursorPage` and `sweepNamespace` per §4.4; `manifestDeletionPremise` rule (1) only.
- `Gc/CasBlobInDegree.h`: `GcRoundWorkBudget` sweep recovery-op fields renamed to seed-op fields; `Pool/CasPool.h` doc string of `gc_round_sweep_recovery_op_budget` updated, name and default unchanged.
- `Tools/CasFsck.cpp`, `Tools/CasInspect`: list and verify the new family (checksum, row order, covered lives subset of `ref_lives`, no rows for uncovered lives).

No on-S3 byte change to any existing object. No fallback path: an unreadable owned seal or run with a present body propagates; only an absent one is a state.

## 6. Tests, failing first {#tests}

`src/Disks/tests/gtest_cas_orphan_manifest_sweep.cpp` and `gtest_cas_orphan_sweep_requests.cpp` (T1-T5, T9, T15-T18), `gtest_cas_gc_fold.cpp` (T6-T8, T10, T19-T20), new `gtest_cas_manifest_owned.cpp` (T11-T13), `gtest_cas_gc_round.cpp` (T14). Helpers: `writeManifestRaw`, `setWatermarkMinActive`, `sweepManifestCursorPageForTest`, the fold's seeded-ref-log helpers, the backend request log by key family that `CASOrphanSweepRequests` already uses. Order: write all, see T1-T4, T6-T8, T11-T13, T15-T20 fail on `45854cb0ee7`, then apply §5.

| # | Test | Setup | Expect |
|---|---|---|---|
| T1 | `CursorPageReadsNoRefLog` | life with `_snap`, ten logs above `C`, one old-epoch orphan body | `deleted == 1`; backend request log over the page shows zero `GET`s under `_log/`, `_snap/` and `_ckpt` keys; one owned-seal `GET`, one run `GET` |
| T2 | `CursorPageRetainsManifestRemovedAboveCursor` | merge log above `C` removes `M` (old epoch, below floor) | `M` retained, class `None` (in set) |
| T3 | `CursorPageRetainsUncoveredLife` | owned seal lists runs but not the life | `retained_owned_set_missing == 1`, body exists |
| T4 | `CursorPageRetainsWhenOwnedSealAbsent` | fold seal adopted, no owned seal | every catalog-named candidate retained; rowless candidate below floor still deleted |
| T5 | `CursorPageRetainsWhenRunAbsent` | owned seal present, shard run deleted; shard holds two lives | both lives' candidates retained, `CASGCManifestOwnedRunAbsent == 1`, no throw |
| T6 | `FoldSeedsLifeAtCursor` | life with `C > {0,0}`, `_snap` at `P <= C`, logs to `T > C`, one precommit whose body is absent | after the round: rows == committed plus precommit bindings of the recovered state at `C`, the bodiless precommit included, with deltas `(C, C']` applied; life covered; `CASGCManifestOwnedSeeds == 1` |
| T7 | `FoldDefersSeedWhenSnapAboveCursor` | fixture A: `_snap` at `P > C`, round 1; round 2 folds to `T >= P`. Fixture B: a life with ordinary fold coverage at `C1 > {0,0}` and no owned-set coverage (no rows), writer publishes `_snap` at `P2 > C1`, then a round that clamps below `P2`, then an unheld round. Fixture C: a no-snapshot life needing `max_seed_ops + 1` logs beside a second candidate | A: round 1 deferred, round 2 seeded exactly at the new cursor. B: the clamped round counts skipped-held; the next unheld round defers against its still-lagging cursor; no rows in either. C: the oversized life seeds in one round and exhausts the budget, the second candidate leaves no rows and seeds next round |
| T8 | `FoldOwnedSetIncremental` | covered life, add/remove/precommit/promote/remove-namespace over three rounds, a seeded bodiless precommit later removed, one mid-log clamp | rows after each round == bindings of the recovered state at `C`; the bodiless precommit's row is removed by its `-1`; a clamped log leaves no partial rows; after `remove_namespace` folded, life not covered and no rows; `UnmatchedRemoves == 0` |
| T9 | `DecommissionSweepUsesOwnedSet` | `sweepNamespace` on a claimed slot | same decisions as the page; no ref-log `GET` |
| T10 | `FoldSealEncodingUnchanged` | golden bytes captured on `45854cb0ee7` for a fixed `CasFoldSeal` value | `encodeFoldSeal` on the new tree is byte-identical |
| T11 | `OwnedSealTolerantReader` | encoded seal plus an unknown key and an unknown `t` record | decodes, unknown ignored; missing `n` or bad count → `CORRUPTED_DATA` |
| T12 | `OwnedRunMergeContract` | each case of §4.2 (prior+remove, seed+remove, add-then-remove, remove-then-add, prior-untouched), plus a permutation of deltas across independent keys | each case gives the stated row; the permutation gives byte-identical runs; a prior row for a seeded life → `CORRUPTED_DATA` |
| T13 | `OwnedRunKindGate` | `source_edge` header opened as owned run | fails closed |
| T14 | `PruneReclaimsOwnedFamilyWithGeneration` | generation with owned run and seal below `prune_floor`, not referenced; another referenced only by an owned run | first deleted by `pruneSupersededGenerations`; second retained |
| T15 | `LeaderAlternationReseeds` | new leader folds with an owned run carried from an older generation `H`; old-shaped leader prunes `H` pre-CAS, then loses its CAS; new leader resumes | adopted fold seal intact; new leader sees run 404, lives uncovered, no deletion; re-seeds; a deposed attempt's owned seal is never read |
| T16 | `PageMembershipIsBoundedByCandidates` | run with 100k rows of one life, page with 3 candidates spanning 3 shards | 3 run opens, membership container size 3 (not resident memory); decisions correct |
| T17 | `RebirthUnderSameNameStartsUncovered` | life A removed and folded; namespace recreated as life B; A's debris and a B body below floor | while B is uncovered every key of the namespace is retained (`OwnedSetMissing`, the catalog join names B); after B is covered and the epoch gate passes, A's debris is reclaimed and B's owned body kept |
| T18 | `BuildEndsBetweenCursorAndFloorRead` | commit of `M` in the live epoch lands after the adopted fold, before the sweep's floor read | `M` retained by the epoch gate |
| T19 | `SeedFailsOnMissingLogWithoutAbort` | life A with one log in `(P, C]` deleted, placed first in life-id order, needing more than `max_seed_ops` reads before the gap; healthy life B second; three rounds | A uncovered every round, `CASGCManifestOwnedSeedFailed` increments, rounds commit, no `CORRUPTED_DATA` in `system.errors`; B is attempted in round 2 (`seed_after` advanced past A) and covered; A is attempted again in round 3 |
| T20 | `CarriedRunAbsentDropsCoverage` | parent owned seal names a run that is deleted; shard has no deltas; once with `max_seed_ops` already spent, once with budget | fold `HEAD`s it, drops the `RunRef` and the shard's covered lives; without budget the lives stay uncovered this round, with budget they re-seed in the same round |
| T26 | `RunVanishesBetweenHeadAndMergeGet` | shard with a covered life X that accepted deltas this round and a `{0,0}` life Y; a hook deletes the run after the successful `HEAD`, before the merge `GET` | X uncovered and without rows, its deltas dropped; Y's rows exact; the round adopts; X re-seeds later; backend request log shows `H == 1` for the shard and one merge `GET` |
| T27 | `SeedIssuesNoRequestAboveCursor` | multi-epoch seed with read-ahead enabled, `C < T` with at least one window of logs between `C` and `T` in `C`'s epoch | success; the backend request log, filtered to the seed walk (intake legitimately reads `(C, T]`), holds no log key lexicographically above `C`; every issued request, speculative ones included, counted in `seed_ops_used` once |
| T27b | `SeedFailureChargesAndFreesHints` | seed failing before `C`, once in the same epoch as the failure, once right after a crossing | failed seed; discarded speculative reads charged; the shared reader has `pending() == 0` afterwards; no request above `C` |
| T28 | `SeedSkippedOnRegressedOrAbsentFrontier` | uncovered life, `M` added in epoch 2 and still owned, cursor `C` in epoch 3; `_ckpt` once with `T < C`, once with no frontier; then the correct `_ckpt` restored | no seed in the anomalous rounds (`CASGCManifestOwnedSeedsSkippedAnomaly`), no rows, `M` retained by the sweep; after restoration the seed succeeds at `C` and `M` is in the set |
| T21 | `UncoveredLifeKeepsNoRows` | uncovered life accepts one addition then clamps; a second uncovered life is not admitted by budget | neither life has rows after the round; both seed later without `CORRUPTED_DATA` |
| T22 | `ZeroBudgetSeedsEverything` | `gc_round_sweep_recovery_op_budget = 0`, three seed candidates | all three seeded in one round |
| T23 | `EmptyRunInitialization` | fresh pool, one life with birth and seal only; rebuild on a pool with no owners | a canonical empty run per covered shard, checksum verifies, the life covered |
| T24 | `PartialReseedOfMissingShard` | run of a shard with two lives deleted; budget admits one seed | the seeded life covered, the other uncovered although the replacement run exists |
| T25 | `RowlessRaceFloorBeforeCut` | page lists a staged body; catalog sampled rowless; writer creates the row, precommits, promotes, ends the build; floor then advances | body retained: floors are sampled before the cut |

T1 is the acceptance test of the decision: it pins the request classes the sweep page may issue.

T15 covers only the older-generation prune; the adopted-generation prune is CAS-386's test.

Gate: `utils/cas-gate` (`CAS*` filter), ASan lane for the five suites.

## 7. TLA+ {#tla}

`docs/superpowers/models/CaGcRootLocalPartManifestCore.tla`, `GSwView` (`:1100`): today `swOwned'` is the current owner map, the replayed frontier, and the nomination predicate has no cursor-epoch gate (`:1134-1139`). Seven questions, each with the sabotage that proves the model can see the answer; a green run without its counterexample is vacuous.

### 7.1 The view source {#tla-view}

- variable `foldOwned`, the historical binding set as of the fold cursor, maintained by applying journal owner transitions in the fold step up to the cursor; it is not a restriction of the current `owner` map, which cannot name an owner already removed above the cursor;
- the sampled per-life cursor epoch `foldCursorEpoch[life]`, and the gate `E >= foldCursorEpoch[life]` in the nomination predicate, the implementation's rule (1) (`CasOrphanManifestSweep.cpp:489-503`);
- constant `ViewFromFoldState`: `GSwView` takes `swOwned' = foldOwned ∪ {m : buildEpoch(m) >= foldCursorEpoch[lifeOf(m)]}` and no tail term;
- every existing honest cfg stays green on `INV_NO_LOSS` and `INV_NO_DANGLE` with `ViewFromFoldState`.

### 7.2 The unfolded-removal invariant (CAS-310) {#tla-unfolded-removal}

- the journal event records the removed owner kind; invariant `HasUnfoldedCommittedRemoval(m) => mBody[m]` (committed removals only: an abandoned precommit's body may legitimately be gone);
- sabotage `SabotageSweepUnfoldedRemoval`: `swOwned'` from the current `owner` map while `foldOwned` lags; counterexample required;
- sabotage `SabotageLiveEpochGateOnly`: the gate `buildEpoch(m) = liveEpoch` instead of `>= foldCursorEpoch`; the three-epoch schedule (fold in epoch 1; epoch 2 creates, commits and removes `m`; writer at epoch 3) must produce a counterexample; `witness_threeepochlag` (positive) retains `m`.

### 7.3 Coverage exactness {#tla-coverage}

The class of the rev.2, rev.3 and rev.5 findings (bodiless precommit, rows for an uncovered life, seed below `C` on a regressed frontier). Model the owned seal as `covered[life]` and the run as `rows[life]`, the `_ckpt` frontier as a value that may lag or regress relative to the cursor, and the seed as a step bounded by `min(T, C)` with an endpoint check.

- invariant `INV_OWNED_EXACT`: `covered[life] => rows[life] = bindingsAt(cursor[life])` and `~covered[life] => rows[life] = {}`;
- sabotage `SabotageDeltaWithoutAdmission`: an uncovered life's accepted deltas enter the run; must break `INV_OWNED_EXACT`;
- sabotage `SabotageSeedToFrontier`: the seed walks to `T` instead of `C`; must break it;
- sabotage `SabotageSeedBelowCursor`: with `T < C` the seed stops at `T` and publishes coverage; must break it, and under `ViewFromFoldState` must reach `INV_NO_LOSS`;
- sabotage `SabotageOwnedNeedsBody`: a transition stages its owned delta only when the body is readable; must break it.

### 7.4 Absence under leader alternation {#tla-absence}

Extends attempt scoping (`EnableAttemptScoping`, `SabotageDeposedLeaderWritesFinalGen`). A legacy-leader step folds without writing the owned family and may delete any owned seal or run of a non-adopted or pruned generation; a `HEAD`-success-then-`GET`-404 interleaving is a separate step.

- `INV_NO_LOSS` holds with owned seals and runs that may vanish; a vanished run uncovers its shard's lives before any delta is admitted;
- sabotage `SabotageAbsenceIsEmptyCoverage`: an absent owned seal or run is read as an empty, covered set; counterexample required;
- sabotage `SabotageHeadIsAPin`: a shard whose `HEAD` succeeded is merged against an empty baseline when the `GET` fails, keeping its covered lives; counterexample required.

### 7.5 Seed liveness under budget and fairness {#tla-liveness}

- property `EventuallyCovered`: every life whose history is reconstructible (`P <= C` at some point, logs present) is eventually covered, with weak fairness on the fold step, a finite seed budget per round and at least one life whose seed fails forever;
- sabotage `SabotageFixedSeedOrder`: admission always starts from the smallest life id (no `seed_after`); must violate `EventuallyCovered`;
- sabotage `SabotageDiscardOversizedSeed`: a seed over budget is discarded rather than run to completion; must violate it for a life longer than the budget.

### 7.6 Debris liveness {#tla-debris}

`OrphanManifestDebrisDrains` keeps holding under `ViewFromFoldState` with the new retain class: an uncovered life retains only until its seed lands, which 7.5 guarantees.

### 7.7 Prune against a lost CAS (CAS-386) {#tla-prune}

Separate small model or an extension of attempt scoping: generations, `keep`, `referenced_generations` built from run placement only, an all-carry round, the pre-CAS prune, a lost CAS.

- invariant `AdoptedSealReadable`: the adopted generation's fold seal exists in every state;
- the current code (`referenced_generations` from runs, prune floor `G + 1 - keep`) with `keep = 1` must produce a counterexample;
- the candidate fix (the adopted generation is always in `referenced_generations`) must be green; this run is CAS-386's AC.

Configs: `fix_ownedset` (positive, 7.1-7.6 green on every existing honest cfg), one `sab_*` per sabotage above (counterexample required), `witness_threeepochlag`, `prune_keep1` and `prune_keep1_fix` (7.7). Run via `run_gc_partmanifest.sh`; results appended to `CaGcRootLocalPartManifestCore_RESULTS.md`.

## 8. Documentation {#docs}

- `docs/en/antalya/cas/architecture/manifests-and-refs.md#orphan-sweep`: the flowchart's "protection view" node becomes "owned set as of the fold cursor plus the epoch gate"; the "same complete replay" paragraph is replaced by the three-case argument of §3.
- `docs/en/antalya/cas/architecture/garbage-collection.md#round-cost`: the rows of §4.6; the sweep's ref-log and `_ckpt` terms removed.
- `docs/en/antalya/cas/architecture/storage-layout.md`: the two keys.
- `Formats/README.md`: the family, its reader strictness, and the absence rule of §4.5.

## 9. Out of scope {#out-of-scope}

- One sequence per namespace (CAS-305 d) turns the cursor into one integer; this design works on the pair.
- The fold's global `LIST cas/ns/stream/` (CAS-29.1) is untouched.
- A `_snap` threshold for seal-only epochs, and the `prev_epoch_seal` hint bound (CAS-359), are independent.
- Owner-removed manifests whose post-CAS body delete failed (CAS-46) are outside the set, but the epoch gate retains them until their epoch closes; the sweep is their eventual reclaimer, not a timely retry. CAS-46's durable retry stays.
- The seed relies on `planRefCleanup`'s rule, a log is deletable only below both the `_snap` base and the fold cursor; CAS-29.6 keeps that rule when it changes the listing.
- Once CAS-29.10 adds the round deadline, the seed walk is one of the cut families, with `max_seed_ops` kept as the admission ceiling.
- CAS-315's sweep half (a view replay meeting an incoherent cut) disappears with the replay; its fsck and preview half stays.
- CAS-386: the pre-CAS prune can delete the adopted generation's fold seal when `keep = 1` and no blob `RunRef` points at that generation; a likely fix is to always include the adopted generation in `referenced_generations`. Independent of this family.
