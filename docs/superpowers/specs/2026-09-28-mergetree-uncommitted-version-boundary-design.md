---
description: 'Design for one MergeTree rule that closes the mutation-ordering races on plain MergeTree: an in-flight version (a mutation being registered, a lightweight update being published, a part being moved in) announces itself as a pending version under the mutex that every selector already holds, and the two version accessors treat it as a boundary.'
sidebar_label: 'MergeTree pending versions'
sidebar_position: 41
slug: /superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design
title: 'MergeTree: pending versions under the selection mutex — design'
doc_type: 'reference'
---

# MergeTree: pending versions under the selection mutex — design (rev.7) {#design}

Scope: plain `StorageMergeTree` only (`ReplicatedMergeTree` orders mutations through the queue and
`havePendingPatchPartsForMutation`). Target: one upstream pull request built on top of
https://github.com/ClickHouse/ClickHouse/pull/119155 and superseding it, plus the same commits on
`altinity/antalya-26.6`. Related issues: https://github.com/ClickHouse/ClickHouse/issues/122083,
https://github.com/ClickHouse/ClickHouse/issues/116686, https://github.com/Altinity/ClickHouse/issues/2331,
and the `MOVE PARTITION TO TABLE` loss drafted in `tmp/cas252/issue_move_partition_to_table_covered_by_merge.md`.

Revision history. rev.1 to rev.6 (design reviews rounds 1 to 4 and the audit of PR #119155, all in
`docs/superpowers/reports/2026-09-28-cas-252-codex-reviews/`) taught the *readers* about reservations held
in `committing_blocks`: a typed reservation snapshot consumed by mutation selection, the merge predicate and
patch selection, then a coherent cut for that snapshot (ordering, then a data-parts read lock across the
snapshot, then a publication epoch, then a boundary-handoff epoch), plus a split-phase `NewPart` case for
`MOVE`. Every round was sound in its findings and the scope kept growing, because the reservations live under
`committing_blocks_mutex`, a different mutex from the one the readers decide under, so every reader needed
its own coherence argument. rev.7 (owner decision 2026-09-28) restates the problem: **an in-flight version
must announce itself where the readers already look, under the mutex the readers already hold.** The
reservations stay where they are for their other users; the readers gain one ordered set of pending versions
guarded by `currently_processing_in_background_mutex`, and the two version accessors treat its members as
boundaries. No snapshot type, no epoch, no lock held longer, no change in the compaction framework.

## 0. The races, restated {#races}

`StorageMergeTree` decides what to merge, what to mutate and which patches to apply from three inputs read
under `currently_processing_in_background_mutex`: the registered mutations (`current_mutations_by_version`),
through `getCurrentMutationVersion` and `getNextMutationVersion`; the active parts; the patch parts. Three
writers create a version that is not yet in any of those inputs but must not be crossed:

| writer | version | invisible window | what crosses it today |
|---|---|---|---|
| `startMutation` | mutation N, allocated in `prepareMutationEntry` | file write → `addPreparedMutationEntry` | a mutate task applying a later registered N+1 alone (R1, #122083, Altinity #2331); a merge of a part below N with a part inserted above N (R3) |
| `updateLightweight` | lightweight update U, allocated before the patch is written | allocation → patch part active | a mutation above U mutating the part without the patch (R2, #116686, PR #119155) |
| `movePartitionToTable` | moved part with block q, `PreActive` in the destination | destination lock released → `dest_transaction.commit` | a merge of two active neighbours spanning q; the later commit outdates the moved part (the `MOVE` loss) |

R1 additionally has a plain bug in front of the window: `StorageMergeTree::mutate` declares its alter lock
in an `if` initializer, so the lock meant to serialize mutations with `ALTER`s is released before
`startMutation` runs; two mutations can therefore register out of order (upstream master has the same
lines). Reproduced from CI logs (two `DELETE`s 8 ms apart, `mutation_51` registered 1.1 ms before
`mutation_50`, one row of mutation 50 survived) and deterministically by test S1.

## 1. The rule {#rule}

**A version that is allocated but not yet visible to the selectors is a pending version. A writer inserts it
into `pending_versions` under `currently_processing_in_background_mutex` before anything that must not cross
it can be selected, and erases it under the same mutex after its state is visible. The version accessors
count pending versions as boundaries.**

```cpp
/// Versions that exist (a block number is allocated) but whose state the selectors cannot see yet: a
/// mutation between its file write and its registration, a lightweight update between block allocation
/// and patch commit, a part moved in between preactivation and commit. Guarded by
/// currently_processing_in_background_mutex, like current_mutations_by_version.
std::map<Int64, PendingVersionKind> pending_versions;
```

Readers (three sites, all under the mutex they already hold):

1. `getCurrentMutationVersion(dv)`: the highest version `<= dv` among registered mutations **and pending
   versions** (0 if none). `MergeTreeMergePredicate::canMergeParts` therefore refuses two parts with a pending
   version between their data versions, with the existing "different mutation version" message, and a merge
   can no longer cross an unregistered mutation, an unpublished patch or a moving part.
2. `getNextMutationVersion(dv)`: the lowest version `> dv` among registered mutations **and pending versions**
   (0 if none). `getPatchesToApplyOnMerge` therefore never materializes a patch from above a pending version;
   the bound is exclusive (a patch whose version equals the pending version is not applied).
3. `selectPartsToMutate`: the applicable range for a part ends at
   `current_mutations_by_version.lower_bound(first pending version > part data version)`; a part whose
   pending registered mutations all lie above a pending version is postponed with a reason that names the
   writer: `PENDING_LIGHTWEIGHT_UPDATE` (PR #119155's string, unchanged) for an update,
   `"Mutation with a lower block number is not registered yet"` for a mutation, `"Part with a lower block
   number is not published yet"` for a moved part; that is why the set carries the kind next to the version.

Writers (three sites):

- `startMutation`: `prepareMutationEntry` allocates the block and writes the file as today; between them,
  under the mutex, insert `{version, Mutation}`; `addPreparedMutationEntry` registers the entry and erases the
  pending version in its existing critical section. On any failure before registration an RAII guard (taking
  the mutex) erases it. The `alter` path, which calls `prepareMutationEntry` and `addPreparedMutationEntry`
  around its durable metadata commit, gets the same guard.
- `updateLightweight`: after `allocateBlockNumber(Op::Update)`, insert `{U, LightweightUpdate}`; erase after the
  patch sink has committed its parts, or on failure (RAII). `waitForCommittingInsertsAndMutations` keeps
  using `committing_blocks` as today.
- `movePartitionToTable`: after `fillNewPartName` for the moved parts, insert `{q, MovedPart}` for each;
  erase after `dest_transaction.commit()`, or on failure.

Why coherence is free: erasure and every selection happen under one mutex, so a selection sees either the
pending version or the already-visible state (registered entry, active patch, active moved part), never
neither. A version allocated after the selection took the mutex is above every part the selection captured
(block numbers come from one increment) and cannot affect its result. There is no snapshot to keep coherent
and no lock is held longer: the writers take the selection mutex for one `insert`/`erase`, which mutation
registration already does; lightweight updates and moves are rare relative to inserts, which do not touch
the set.

### The lock fix, separately {#lock}

`StorageMergeTree::mutate` keeps its alter lock until `startMutation` returns (two lines). This restores the
documented contract "serialize order of mutations with alter queries" and makes R1's inversion impossible
for every mutation created through `mutate`; the pending version then covers R1 as defence in depth. A
barrier `ALTER` reads `rbegin` of the registered map to pick the mutation to wait for; it now also considers
the highest pending **mutation** version, so an `ALTER` issued through `StorageAlias` (which forwards the
alias's lock instead of taking the target's) still waits for an in-flight mutation. `StorageAlias::alter` is
left as is.

### What stays as it is {#unchanged}

- `committing_blocks` and `CommittingBlock::Op` for `waitForCommittingInsertsAndMutations` and
  `canUsePartInMerges`'s `min_update_block` (kept as the conservative superset; its removal is a follow-up).
- `MergeTreePartsCollector`, `MergeTreeMergePredicate` (apart from the accessors it already calls),
  `PatchPartsUtils`, `MergeTreeData::Transaction`: untouched.
- `system.mutations`, `killMutation`, `getMutationsSnapshot`, `clearOldMutations`, `backupMutations`: untouched,
  because pending versions are not entries of `current_mutations_by_version`; the only status change is the
  postpone reason strings above.
- The on-disk format: untouched.

## 2. What supersedes what, and prerequisites {#supersedes}

Built on top of PR #119155 with its commits kept (authorship preserved), then:

| PR #119155 piece | in this design |
|---|---|
| `Op::Update` loop in `selectPartsToMutate`, guarded by `supportsLightweightUpdate()` | replaced by the pending version inserted in `updateLightweight` (the guard, which a settings `ALTER` can flip mid-flight, disappears) |
| `PostponeReasons::PENDING_LIGHTWEIGHT_UPDATE` | kept unchanged |
| `removeCommittingBlock` triggers on `Update` | kept; the pending erase triggers the assignee too, for the failure path of a mutation (registration already triggers on success) |
| failpoint `mt_lightweight_update_pause_after_block_allocation` | kept |
| test `05227_mutation_waits_for_lightweight_update.sh` | kept verbatim, plus a `timeout` around its `SYSTEM WAIT FAILPOINT` |

Added: `pending_versions`, the three writer sites with RAII guards, the two accessor changes, the mutate
end bound and its reasons, the lock fix, failpoints `mt_pause_once_before_register_mutation`
(`PAUSEABLE_ONCE`) and `mt_move_partition_pause_after_dest_preactivation` (`PAUSEABLE_ONCE`), and the tests
in §3. Commit order: (1) #119155's commits; (2) `pending_versions` with the accessors and the mutate bound,
wired for mutations and updates, replacing the loop; (3) the `MOVE` writer; (4) the lock fix and the barrier
change; (5) tests. Each commit builds and its tests pass on their own.

Prerequisite for `antalya-26.6` only: upstream's registration-time transaction-rollback recheck in
`startMutation` is absent on 26.6; it lands first as its own pull request with its upstream test, merges, and
this series is based on the merged result (no stacked pull requests). Upstream relies on the code in master.

## 3. Tests, failing first {#tests}

`05227` (PR #119155) stays as the R2 pin. New stateless tests (numbers from `add-test` at rebase time), tags
`no-parallel, no-replicated-database, no-shared-merge-tree`, deterministic (failpoints and polling, every
`SYSTEM WAIT FAILPOINT` under `timeout`, traps that disable every failpoint, reap clients and drop tables;
`KILL QUERY` on a thread parked in `pauseFailPoint` takes effect only after the failpoint is disabled):

- **S1 mutation vs mutation** (R1, lock fix): pause DELETE A at `mt_pause_once_before_register_mutation`;
  DELETE B with `lock_acquire_timeout = 1` fails with `TIMEOUT_EXCEEDED` and `system.mutations` is empty;
  release; both applied in order; count 0. Pre-fix arm: B completes while A is paused, A is then released
  and reported done with its rows surviving. (Ordering arm implemented as `05054_mutations_register_in_order.sh`,
  verified red/green; the loss arm is added in the plan.)
- **S2 update vs mutation, guard-independent** (R2): PR #119155's scenario with `ALTER TABLE ... MODIFY
  SETTING enable_block_offset_column = 0` issued while the update is paused; the mutation is still postponed
  with `PENDING_LIGHTWEIGHT_UPDATE` (red on #119155 as is). Cancellation arm: `KILL QUERY` the paused update,
  disable the failpoint, no patch is committed, the mutation completes.
- **S2c partial progress**: registered K < pending U < registered M on one part, K held applied-but-unfinished
  with `mt_mutate_task_pause_in_prepare`; K applies and M is postponed; after U commits, M applies.
- **S3 merge vs in-flight mutation** (R3): background selector paused with
  `mt_merge_selecting_task_pause_when_scheduled`; `INSERT (0)`; pause DELETE `WHERE id = 1` before
  registration; `INSERT (1)`; `OPTIMIZE ... FINAL SETTINGS optimize_throw_if_noop = 1` throws and two active
  parts remain; release; the mutation applies to the first part only; `groupArray(id)` is `[0,1]`. Red
  today (the merged part keeps the first part's data version and row 1 is deleted).
- **S3b patch across a pending update** (accessor 2): parts below a pending update U (paused after
  allocation), a committed later lightweight update V above U; `OPTIMIZE` must not materialize V's patch;
  after U commits both apply. Green today because of the `min_update_block` superset; kept as the pin for
  the exclusive bound together with a unit test on `getNextMutationVersion` (pending equal to a patch
  version).
- **S4 barrier `ALTER` vs in-flight mutation** (lock fix and the barrier change): pause DELETE before
  registration; `ALTER TABLE ... RENAME COLUMN` with `lock_acquire_timeout = 1` fails with `TIMEOUT_EXCEEDED`;
  the same through a `StorageAlias` waits for the pending version instead of overtaking it; release; both
  succeed afterwards. Red today.
- **S5 `MOVE PARTITION TO TABLE`**: pause the move at `mt_move_partition_pause_after_dest_preactivation`;
  insert into the destination above the moved block; `OPTIMIZE` the destination partition with
  `optimize_throw_if_noop = 1` throws (pending version between the neighbours); resume; the moved rows are
  active in the destination and absent from the source. Red today (the merge happens and the moved part is
  outdated at commit).
- **Unit test** for the accessors with a mixed registered/pending set: below, equal and above the data
  version, empty set, the mutate end bound, and the exclusive patch bound.

## 4. Why it is sound, and what it does not claim {#argument}

- Every selector runs under `currently_processing_in_background_mutex`; every insert into and erase from
  `pending_versions` runs under it. A pending version is inserted after allocation and before the writer's
  state can be crossed (before the file write for a mutation, before the patch write for an update, before
  the destination lock is released for a move), and erased after that state is visible under the same mutex
  in which registration, patch commit or move commit made it visible, or by RAII on failure. So at every
  selection the version is pending, or visible, or (after a failure) gone with nothing to protect.
- Registered mutations and pending versions share one ordering (block numbers from one increment), so the
  accessors' `max`/`min` over both sets are well defined and `lower_bound` on the map at the first pending
  version above a part is the correct end of the applicable range.
- A merge or a mutate task selected before a version was allocated produces a part whose data version is
  below it; the later mutation, patch or move applies to that part as usual.
- **Not covered, stated as scope:** (a) a mutation inside a transaction whose transaction rolls back between
  file write and registration: upstream's recheck handles it; 26.6 gets it as the prerequisite. (b) A
  nontransactional `mutation_N.txt` left behind when registration fails and the file removal fails:
  `loadMutations` treats every final file as registered after a restart; the plan reorders the locals and
  members of `PreparedMutationEntry` so the file removal is attempted before the block is released, and the
  restart limitation is stated. (c) `StorageAlias::alter` still forwards the alias lock; the barrier change
  makes it wait for pending mutations, but a metadata `ALTER` through an alias can still run concurrently
  with a mutation that has not allocated yet; follow-up issue.

## 5. Questions for review {#questions}

1. Is there a fourth writer that allocates a version the selectors cannot see (fetch/clone paths of plain
   `MergeTree`, `ATTACH PART`, `REPLACE PARTITION`, `restoreAndActivatePart`, `swapActivePart`)? For each,
   is allocation and publication one parts-lock critical section (no pending version needed) or not?
2. Is taking `currently_processing_in_background_mutex` in `updateLightweight` and `movePartitionToTable`
   free of lock-order inversions with the locks those paths already hold (parts lock, `committing_blocks_mutex`,
   the alter lock, `DatabaseCatalog`/`DatabaseAtomic` mutexes on the `alter` path)?
3. Does any reader of `getCurrentMutationVersion`/`getNextMutationVersion` rely on the value being a
   registered mutation (for example to look it up in the map) rather than a version number? Name each caller.
4. Is the barrier `ALTER` change (wait for the highest pending mutation version too) correct, and what does
   `waitForMutation` do with a version that is pending and not yet registered?
5. Anything simpler still?
