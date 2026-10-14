---
description: 'CAS-358: the orphan-manifest sweep runs one full circuit per interval (default a day) instead of a page every round; later its owner view comes from the writer snapshot alone, with no ref-log walk.'
sidebar_label: 'CAS GC: sweep cadence and snapshot view'
sidebar_position: 109
slug: /superpowers/specs/cas-orphan-sweep-cadence-and-snapshot-view-design
title: 'CAS GC: the orphan sweep runs once a day and decides from the snapshot'
doc_type: 'reference'
---

# CAS GC: the orphan sweep runs once a day and decides from the snapshot — design {#cas-orphan-sweep-cadence-and-snapshot-view}

- Date: 2026-10-14. Status: rev.1. Task: CAS-358. Replaces `2026-10-13-cas-orphan-sweep-owned-manifest-set-design.md` (withdrawn, see §5). Depends on: CAS-36 (floor before the catalog cut; unchanged here). Related: CAS-359 (`prev_epoch_seal` hint bound, independent), CAS-387 (one sequence per life, independent), CAS-386 (prune vs adopted seal, independent).
- Code facts verified against the `cas-gc-rebuild` working tree at `aa5e67ed6fc`. Numbers from https://github.com/Altinity/ClickHouse/pull/2542 and from the otel.demo stand (7 days ending 2026-10-13).

## 1. Decision {#decision}

Two stages, each shippable alone. Neither adds on-store state, touches the fold, the writer, or any object format.

- **Stage 1, cadence.** The sweep runs as a *circuit*: pages from an empty cursor until the LIST wraps, then idles. A new circuit starts at most once per `manifest_sweep_interval_sec` (default 86 400). Per-page behaviour, budgets and the §6 deletion premise are unchanged. About twenty lines in `Gc/CasGc.cpp`, one setting.
- **Stage 2, snapshot view.** The owner view of a catalog-named namespace is the writer's checkpoint-named snapshot `snap(P)` read through `readCheckpointSnapshotBase` (`Pool/CasRefProtocol.cpp:954`). A manifest is deleted only if it is below the floor, absent from `snap(P)`, its build epoch is strictly below `P.writer_epoch`, and the adopted fold cursor `C` of that life is at or above `P`. The committed-tail walk and the genesis replay in `activeManifestKeys` (`Gc/CasOrphanManifestSweep.cpp:192`) are deleted. A namespace without a snapshot is retained and counted.
- **Rejected: an owned-manifest set maintained by the fold** (the withdrawn spec). Correct, but the backfill of pre-existing lives (seed at the cursor, admission whitelist, two-pass sweep, 28 tests) is a second reference-tracking machine for a defect that reclaims ~1 000 bodies a month. §5.

## 2. The defect and its cost {#defect}

- `planManifestCursorPage` (`CasOrphanManifestSweep.cpp:667`) runs every round on a 1 000-key page. For each namespace with a candidate on the page it builds `activeManifestKeys` (`:192`): a full table recovery (`_snap` or genesis `{life_epoch,1}` plus every later log up to `committed_through`, `Pool/CasRefCkpt.cpp:176-180`), then a second walk from the fold cursor to `committed_through` for removal targets. Memoized within one page only.
- A `_snap` exists only once the tail exceeds 256 logs or 1 MiB (`Pool/CasPool.h:290-291`). Every remount adds a seal-only epoch, so the genesis walk grows with table age.
- The fold reads each ref log exactly once (otel.demo: `_log` GET/PUT ratio 1.05). The sweep reads the same logs again, every round.

| metric | PR 2542 stand | otel.demo |
|---|---|---|
| ref streams / with a `_snap` | 792 / 74 | — |
| seal-only epochs | 3 876 | — |
| sweep ref-log GETs per day | ~956 k (after the PR's 404 cut) | ~0.98 M (0.29 M taken + 0.69 M read-ahead wasted) |
| bodies the sweep deleted | — | 0 in the week; 1 077 since 2026-09-14 |

Orphan manifests come from two sources only: a crash between the staged body PUT and the ref append, and a failed post-CAS delete. `PartWriteTxn::abandon` deletes its own staging (`Pool/CasPartWriteTxn.cpp:1078-1097`). The sweep is a janitor for rare debris and costs more than the fold.

## 3. Stage 1: one circuit per interval {#stage-1}

### 3.1 Behaviour {#stage-1-behaviour}

- A **circuit** is the sequence of pages from `manifest_sweep_cursor == ""` until a page reports `wrapped` (`ManifestSweepResult::wrapped`, `:985`). The round CAS still publishes the cursor after each page (`CasGc.cpp:967`); a wrap publishes `""`.
- The leader keeps one in-memory `next_circuit_not_before` (steady clock). It is zero when the leader starts, so the first destructive round starts a circuit. When a page wraps, it becomes `now + manifest_sweep_interval_sec`.
- A round whose cursor is empty and whose clock is before `next_circuit_not_before` skips the sweep: no LIST, no seal or catalog read, `orphan_sweep` phase row with `idle = 1`, cursor unchanged.
- A round whose cursor is non-empty runs its page as today, whatever the clock says. A circuit in progress is never cut.
- `manifest_sweep_interval_sec = 0` means a new circuit starts on the round after a wrap: today's behaviour. `manifest_sweep_list_budget_keys = 0` still disables the sweep.
- A leader change restarts the clock: the new leader starts a circuit at its first destructive round. One extra circuit per leader term is the price of keeping the stamp out of `gc/state` (format frozen since 26.6.4). A circuit after a restart is also when debris is most likely to exist.
- `suppress_destructive` rounds and REBUILD (`CasGc.cpp:4575`, cursor reset) are unchanged.

### 3.2 Setting {#stage-1-setting}

| setting | default | meaning |
|---|---|---|
| `manifest_sweep_interval_sec` | 86400 | Seconds between orphan-manifest sweep circuits; a circuit lists the whole `cas/manifests/` keyspace one page per round. 0 = circuits run back to back. |

Declared next to `manifest_sweep_list_budget_keys` (`ContentAddressedSettings.cpp:63`), carried in `PoolConfig`. The description says what happens, not what it bounds. A config that names the setting must not reach a 26.6.4 binary (unknown setting refuses the disk); roll the binary first.

### 3.3 Change {#stage-1-change}

- `Gc/CasGc.cpp:3259` and `:967`: the condition gains `&& sweepCircuitDue(state)`, where `sweepCircuitDue` is `!state.manifest_sweep_cursor.empty() || now >= next_circuit_not_before`. After `planManifestCursorPage` returns `wrapped`, set `next_circuit_not_before`.
- `:1194` phase row: add `idle` and `circuit_started` metrics.
- `Gc` gains one member, `next_sweep_circuit_ms`, read against `mono_ms_fn` (the injected monotonic observation clock, `Gc/CasGc.h:924`); reset to zero on lease acquire.
- No change to `CasOrphanManifestSweep.*`.

### 3.4 Cost {#stage-1-cost}

Per circuit, as today: `N / 1000` LIST pages, one `_ckpt` + replay per namespace per page, bodies for nominated candidates. Per day: one circuit instead of `86400 / gc_interval_sec` pages. On otel.demo (`gc_interval_sec` such that ~233 rounds a day) the sweep's ref-log reads drop by the ratio of a day to a circuit; with stage 2 they drop to zero.

### 3.5 Tests, failing first {#stage-1-tests}

`gtest_cas_orphan_manifest_sweep.cpp`, with the GC's test clock hook:

| # | test | setup | asserts |
|---|---|---|---|
| S1 | `CircuitIdlesUntilTheIntervalElapses` | interval 3600, one circuit wrapped | next round: no LIST request, `idle == 1`, cursor `""`; clock + 3600: a page runs |
| S2 | `FreshLeaderStartsACircuitAtOnce` | new leader, interval 3600 | first destructive round LISTs |
| S3 | `ACircuitInProgressIgnoresTheInterval` | cursor non-empty, clock before the deadline | page runs, cursor advances |
| S4 | `IntervalZeroRunsCircuitsBackToBack` | interval 0 | the round after a wrap LISTs |
| S5 | `SuppressedRoundFreezesTheClockToo` | `suppress_destructive` | no circuit starts, deadline unchanged |

## 4. Stage 2: the snapshot is the owner view {#stage-2}

### 4.1 Rule {#stage-2-rule}

For a listed key `K` under namespace `ns`, build epoch `E`, with the catalog row's life `L`, that life's `_ckpt`, `P = _ckpt.checkpoint_snapshot_id`, `snap(P)` = the manifests named by `committed` and `precommits` of the snapshot, and `C = ref_lives[L].coverage.last_folded_ref_id` from the adopted fold seal:

delete `K` iff

1. `prefixEligibleUnder(floor, prefix)` (CAS-36, step one, unchanged);
2. the row is `Live` or `Removing` (a `Creating` row retains, as today);
3. `P` exists and `K ∉ snap(P)`;
4. `E < P.writer_epoch`;
5. coverage is `Covered` with no hold, and `C ≥ P`.

Every other case retains, under a counted class: `NoSnapshot` (3 fails on `P`), `InSnapshot`, `SnapshotEpoch` (4), `CursorBelowSnapshot` (5), plus the existing `NoCoverage` and `Hold`. Two branches skip rules 2 to 5 and use the floor alone: no catalog row (CAS-36), and `E < _ckpt.life_epoch` (an older life of the same name, §4.3 item 3).

### 4.2 Why this is complete {#stage-2-proof}

- **Nothing in the writer needs `K` after `P`.** A grant (`AddPrecommit`, the only `+1`, `manifestEdgesOfTxn`) names a build of its own epoch; grants do not cross epochs (`Pool/CasRefProtocol.h:375`, INV-2). With `E < P.writer_epoch` every grant of `K` precedes `P`, and `snap(P)` is the whole binding set through `P`. `K ∉ snap(P)` therefore means `K` is not owned at `P` and can never be granted again (keys are write-once, the epoch is closed).
- **Nothing in the fold needs `K` after `C`.** The fold reads a body on `+1` and on `-1`. Every `+1` of `K` is in epoch `E < P.writer_epoch ≤ C.writer_epoch`, hence folded. If `K` was ever owned, its `-1` is the record that ended the ownership, at a position `≤ P ≤ C`, hence folded. If it was never owned, there is no `-1`. Rule 5 needs clean coverage: a hold or clamp means some position below `C` is unaccounted.
- **The floor is redundant but kept.** `E < P.writer_epoch` already proves the build's mount epoch has ended. Keeping the floor first costs one cached lease GET per namespace per page and leaves CAS-36 and its TLA order untouched.
- **A removal above `P` needs no tail walk.** A manifest removed after `P` is still in `snap(P)` and is retained by rule 3. That is the CAS-310 wedge shape, and it is the reason rule 5 compares `C` with `P` and not with the removal.

### 4.3 The four slippery points, as rules {#stage-2-slippery}

1. **Cursor below the snapshot.** A `-1` at a position in `(C, P]` is unfolded and its target is absent from `snap(P)`. Rule 5 retains the whole namespace this page. It resolves itself: the fold passes `P` within a few rounds.
2. **Builds of the snapshot's own epoch.** A build at `(P.writer_epoch, seq)` staged after `P` is live and absent from `snap(P)`. Rule 4 retains it. It resolves on the next epoch's snapshot.
3. **Keys of an older life of the same name.** After DROP + CREATE under the same name, the old life's manifests sit under the same `cas/manifests/<ns>/` prefix and are absent from the new life's snapshot; the old life's `-1`s live in the old stream, which the new life's `_ckpt` does not name. They need no snapshot: creation over a `Removing` row is refused until the old life's terminal removal has folded and GC has deleted the row (`Pool/CasRefLedger.cpp:1349`), so by the time a new life exists, every `-1` of the old life is folded and every owned body of the old life was deleted by the fold. Rule: a key whose `E` is below the row's `_ckpt.life_epoch` is decided by the floor alone (the rowless rule). A key with `E ≥ life_epoch` goes through rules 3 to 5 like any other; the proof in §4.2 holds for it because the old life's grants precede the new life's birth.
4. **No snapshot.** Tables below the snapshot threshold never publish a `_snap`; on the PR 2542 stand that is 718 of 792 streams. Rule 3 retains them under `NoSnapshot`. Their debris is at most a few bodies per hard restart and is visible in `cas-fsck`. Two optional follow-ups, neither needed for correctness: lower `snapshot_log_count_threshold` per pool, or have the writer publish a snapshot after a recovery that found a non-empty tail (`admitSnapshotPublishUnderStateLock`, `Pool/CasRefLedger.cpp:4140`, one condition).

### 4.4 Page flow {#stage-2-flow}

Keys are sorted by namespace, so a page is a few whole tables. Per page, in this order:

1. `LIST` one page (as today).
2. Adopted fold seal, then the catalog cut (as today, `:703-704`).
3. Per listed key: floor (cached per namespace), rowless branch (CAS-36), row state.
4. Per namespace, once per page: `readCkpt`, then `readCheckpointSnapshotBase` (the `_log` at `P`, its predecessor seal if any, the `_snap`): at most four GETs. Build `snap(P)` as manifest keys via `layout.manifestKey`. Read `C` from the seal. Any throw retains the namespace, as today.
5. Rules 3 to 5 per key; survivors are candidates.
6. Candidates: exact GET, decode, nominate with blob source retirements; post-CAS exact-token delete (all as today, `:923-985`, `CasGc.cpp:1194`).

Deleted: `activeManifestKeys`, `NamespaceProtection`, `OutstandingHintGuard`, `hintRefLogsWithinEpoch` use in the sweep, `tail_removal_targets` in `NamespaceFoldView`, the `TailRemoval` class, `gc_round_sweep_recovery_op_budget` (nothing left to budget; the four GETs per namespace are bounded by `gc_round_sweep_namespace_budget`). `sweepNamespace` (used by `Tools/CasDecommission.cpp`) takes the same view. `manifestDeletionPremise` keeps rule (1) as rule 4 restated on `P` and gains rules 3 and 5.

### 4.5 Cost {#stage-2-cost}

Per circuit: `N / 1000` LISTs, one lease GET and at most four ref-object GETs per namespace per page, bodies for nominated candidates. No ref-log walk, no read-ahead, no 404s. On the PR 2542 stand: 792 streams, ~3 200 GETs per circuit plus pages, once a day. Memory: one `snap(P)` at a time plus the page's floor and view maps, as today.

### 4.6 Compatibility {#stage-2-compat}

No object is written or changed; the sweep reads only existing objects (`_ckpt`, `_log`, `_snap`, the adopted seal, the catalog, mount leases). An older and a newer leader may alternate freely: each decides from the same objects under its own rule, and each rule is sound on its own. No setting is added by stage 2.

### 4.7 Tests, failing first {#stage-2-tests}

`gtest_cas_orphan_manifest_sweep.cpp` and `gtest_cas_sweep_deletion_premise.cpp`; request-count tests in `gtest_cas_orphan_sweep_requests.cpp`.

| # | test | setup | asserts |
|---|---|---|---|
| T1 | `SnapshotViewDeletesABuildAbsentFromTheSnapshot` | `K` at `E=3`, `P={5,10}`, `K ∉ snap(P)`, `C={5,12}` | deleted |
| T2 | `ABuildOfTheSnapshotEpochIsRetained` | `K` at `E=5`, `P={5,10}` | retained, class `SnapshotEpoch` |
| T3 | `ACursorBelowTheSnapshotRetainsTheNamespace` | `C={5,8}`, `P={5,10}`, `K` removed at `{5,9}` | retained, class `CursorBelowSnapshot` (the CAS-310 shape) |
| T4 | `ANamespaceWithoutASnapshotIsRetained` | `_ckpt` without `checkpoint_snapshot_id` | retained, class `NoSnapshot`, no `_log` GET |
| T5 | `OwnersInCommittedAndPrecommitsAreRetained` | `K` in `committed`; `K2` in `precommits` | both retained, class `InSnapshot` |
| T6 | `ARemovalAboveTheSnapshotNeedsNoTailWalk` | `K` removed at `{6,2} > P`, `C ≥ P` | retained (in `snap(P)`), zero `_log` GETs above `P` |
| T7 | `TheViewReadsAtMostFourRefObjectsPerNamespace` | three namespaces on a page | GET count per namespace ≤ 4, no `_log` GET outside `{P, prev seal}` |
| T8 | `OlderLifeDebrisIsDecidedByTheFloorAlone` | reborn `ns`, key at `E=2`, new life `life_epoch=4`, no snapshot for the new life | deleted when the floor admits `E=2`, with no `_snap` read for it |
| T9 | `CreationOverARemovingRowIsRefused` | `Removing` row without `cleanup_evidence` | `createNamespace` retries later (existing ledger test if present, else new) |
| T10 | `AHeldOrClampedCoverageRetains` | hold at `{5,3}`, otherwise T1 | retained, class `Hold` |
| T11 | `RowlessKeysKeepTheFloorRule` | CAS-36 T1 | unchanged |
| T12 | `TheCursorPagePathAndSweepNamespaceAgree` | T1 through both paths | same decision and class |
| T13 | `AnUndecodableSnapshotRetainsTheNamespace` | corrupt `_snap` | retained, warning, page continues |

Existing tests that assert the tail walk (`EpochSealFoldCursorCrossesTailByExactDecodedSuccessor`, `MissingImmediateEpochAfterCleanedCursorCannotBeSkippedByLaterBacklink`, `CleanedCursorCrossesOnlyThroughExactImmediateEpochHead`, `AnUnconsumedTailRemovalRetainsItsTarget`, `RecoveryWorkBudgetRetainsAndConvergesWithoutWedgingTheCursor`) are deleted with the walk; `PendingCommittedRemovalBodyIsSkipped` is rewritten as T3/T6.

### 4.8 TLA+ {#stage-2-tla}

`CaGcRootLocalPartManifestCore.tla`, the sweep steps at `GSwView`:

- Add a per-life snapshot position `snapPos[n]`, advanced by a writer step to any committed position (a snapshot is a prefix of the committed log).
- `GSwView` becomes: `swOwned' = { m : ownedAt(snapPos[life], m) \/ epoch(m) >= epochOf(snapPos[life]) \/ cursor[life] < snapPos[life] }` for catalog-named `m`.
- Properties: the existing unfolded-removal invariant (CAS-310: no deleted body is named by an unfolded `-1`) and no-live-build deletion. Sabotage: `sab_cursorbelowsnap` drops the `cursor >= snapPos` term and must reproduce the CAS-310 trace; `sab_snapepoch` drops the epoch term and must delete a staged build of the snapshot epoch; `fix_floorfirst` / `sab_floorfirst` (CAS-36) unchanged.
- Stage 1 needs no model change: idling is a stutter.

## 5. Rejected {#rejected}

- **Fold-maintained owned-manifest set** (`2026-10-13-cas-orphan-sweep-owned-manifest-set-design.md`, rev.8). New run family under the generation prefix, side-object seal, seed at the cursor with budget and fairness, delta admission whitelist, two-pass sweep, 28 tests, seven TLA questions. Five codex rounds found a new hole each time (bodiless precommits, unbounded membership set, seed starvation, cleanup pin, epoch gate). The backfill is the complexity, and it exists only to serve a janitor that reclaims ~1 000 bodies a month. The snapshot already is the owned set; the writer maintains it.
- **Blob in-degree as the owner set.** Sharded by blob hash; inline-only manifests have no edge rows.
- **`_snap` threshold that counts seal-only epochs.** Shortens the replay, keeps it.
- **A durable circuit stamp in `gc/state`.** Format frozen since 26.6.4; the in-memory clock costs one circuit per leader term.
- **Snapshot via `LIST _snap/`.** A `_snap` whose `_ckpt` CAS did not land is not a base; only `checkpoint_snapshot_id` names one.

## 6. Documentation {#docs}

- `docs/en/antalya/cas/architecture/garbage-collection.md`: a short "Orphan manifests" subsection: a daily circuit, the five-rule view, the retain classes, the no-snapshot case, the two settings.
- `docs/en/antalya/cas/architecture/manifests-and-refs.md`: the sweep flow note points at the snapshot rule instead of the tail walk.
- The withdrawn spec keeps its file with a status line pointing here.

## 7. Out of scope {#out-of-scope}

- Writer-side snapshot publish after recovery (§4.3 item 4): a separate small task if `NoSnapshot` counts matter on a real pool.
- CAS-387 (one sequence per life): orthogonal; rule 4 then reads the epoch from the seal payload rather than the key.
- CAS-359: the hint bound still helps the writer's recovery walk and the fold; the sweep no longer hints.
