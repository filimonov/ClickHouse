---
description: 'Design for one MergeTree rule that closes the mutation-ordering races on plain MergeTree: an in-flight version (a mutation being registered, a lightweight update being published, a part being moved in) announces itself as a pending version under the mutex that every selector already holds, and the two version accessors treat it as a boundary.'
sidebar_label: 'MergeTree pending versions'
sidebar_position: 41
slug: /superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design
title: 'MergeTree: pending versions under the selection mutex — design'
doc_type: 'reference'
---

# MergeTree: pending versions under the selection mutex — design (rev.8) {#design}

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
boundaries. No snapshot type, no epoch, no lock held longer, no change in the compaction framework. rev.8
(after design review round 5, `design_review_r5.md`, which judged rev.7 "substantially simpler and more
maintainable" and the shape a MergeTree maintainer is much more likely to accept): allocation and
announcement are one atomic step under the selection mutex (a helper), so no selector can see an allocated
version before its announcement; `MOVE` announces before it takes the destination parts lock (never the
background mutex under a parts lock); `OPTIMIZE FINAL` rebuilds its predicate and collector after every wait
that released the mutex; a barrier `ALTER` waits for a pending mutation to become registered before
`waitForMutation`; the accessors are renamed to boundary terminology; the RAII guard has an explicit
"erase while locked and disarm" operation; PR #119155's `removeCommittingBlock` wake-up is dropped in favour
of the pending erase; pending insert/erase are logged at Debug.

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

**A version that is allocated but not yet visible to the selectors is a pending version. Allocation and
announcement are one atomic operation with respect to selection: a helper acquires
`currently_processing_in_background_mutex`, allocates the block number while holding it, inserts `{version,
kind}` into `pending_versions`, and only then releases the mutex, so no selector can observe an allocated
version before its announcement. The writer erases the version under the same mutex after its state is
visible. The version accessors count pending versions as boundaries.**

Lock order: `currently_processing_in_background_mutex` → `committing_blocks_mutex` (the order merge-predicate
construction already uses). A writer never takes the background mutex while holding a data-parts lock.
Pending cleanup erases under the background mutex, releases it, and only then triggers the assignee.

```cpp
/// Versions that exist (a block number is allocated) but whose state the selectors cannot see yet: a
/// mutation between its file write and its registration, a lightweight update between block allocation
/// and patch commit, a part moved in between preactivation and commit. Guarded by
/// currently_processing_in_background_mutex, like current_mutations_by_version.
std::map<Int64, PendingVersionKind> pending_versions;
```

Readers (three sites, all under the mutex they already hold):

1. `getCurrentVersionBoundary(dv)` (today `getCurrentMutationVersion`; renamed because a moved part is not a
   mutation): the highest version `<= dv` among registered mutations **and pending versions** (0 if none). `MergeTreeMergePredicate::canMergeParts` therefore refuses two parts with a pending
   version between their data versions, with the existing "different mutation version" message, and a merge
   can no longer cross an unregistered mutation, an unpublished patch or a moving part.
2. `getNextVersionBoundary(dv)` (today `getNextMutationVersion`): the lowest version `> dv` among registered
   mutations **and pending versions** (0 if none). `getPatchesToApplyOnMerge` therefore never materializes a
   patch from above a pending version. Equality: the utility skips only `max_patch_version > bound`, so a
   patch whose version equals a pending update's version is excluded today by the retained
   `min_update_block` filter, which holds as long as the update's committing-block holder outlives the pending
   erase; that ownership order is stated in the plan and pinned by a test, and the utility comparison is not
   changed in this pull request. These are the only three plain-`MergeTree` callers of the two accessors
   (`canMergeParts` twice, `getPatchesToApplyOnMerge` once); none looks the value up in the map.
3. `selectPartsToMutate`: the applicable range for a part ends at
   `current_mutations_by_version.lower_bound(first pending version > part data version)`; a part whose
   pending registered mutations all lie above a pending version is postponed with a reason that names the
   writer: `PENDING_LIGHTWEIGHT_UPDATE` (PR #119155's string, unchanged) for an update,
   `"Mutation with a lower block number is not registered yet"` for a mutation, `"Part with a lower block
   number is not published yet"` for a moved part; that is why the set carries the kind next to the version.

Writers (three sites):

- `startMutation`: `prepareMutationEntry` allocates through the helper (`allocatePendingVersion(Mutation)`),
  then writes the file as today; `addPreparedMutationEntry` registers the entry and erases the pending version
  in its existing critical section through the guard's "erase while already locked and disarm" operation
  (a destructor that locks the background mutex would self-deadlock while unwinding under it). On any failure
  before registration the guard's destructor erases it, taking the mutex only when the caller does not hold
  it. The `alter` path, which calls `prepareMutationEntry` and `addPreparedMutationEntry` around its durable
  metadata commit, gets the same guard.
- `updateLightweight`: `allocatePendingVersion(LightweightUpdate)` instead of `allocateBlockNumber(Op::Update)`;
  erase after the patch sink has committed its parts (some partitions' patches may be visible after a
  failure; the erase happens after the sink has finished either way), then release the committing-block
  holder. `waitForCommittingInsertsAndMutations` keeps using `committing_blocks` as today.
- `movePartitionToTable`: allocates and announces every destination version (`MovedPart`) **before** taking
  the destination data-parts lock; under that lock the announced versions are assigned to the moved parts'
  names (`fillNewPartName` takes the pre-allocated holder); erase after `dest_transaction.commit()` has
  returned and the destination parts lock is released, or, on failure, after the destination transaction has
  unwound.

Why coherence is free: allocation-with-announcement, erasure and every selection happen under one mutex, so
a selection sees either the pending version or the already-visible state (registered entry, active patch,
active moved part), never neither. One selector releases that mutex mid-decision: `OPTIMIZE FINAL` waits on
`currently_processing_in_background_condition` for running merges and retries. A predicate constructed under
one acquisition captures patch state and `min_update_block`; after the wait releases and reacquires the mutex,
`selectPartsToMerge` constructs a fresh `MergeTreeMergePredicate` and `MergeTreePartsCollector` before rereading
parts. No predicate is reused across an unlock. This is a construction change inside `selectPartsToMerge`
only. A version allocated after the selection took the mutex is above every part the selection captured
(block numbers come from one increment) and cannot affect its result. There is no snapshot to keep coherent
and no lock is held longer: the writers take the selection mutex for one `insert`/`erase`, which mutation
registration already does; lightweight updates and moves are rare relative to inserts, which do not touch
the set.

### The lock fix, separately {#lock}

`StorageMergeTree::mutate` keeps its alter lock until `startMutation` returns (two lines). This restores the
documented contract "serialize order of mutations with alter queries" and makes R1's inversion impossible
for every mutation created through `mutate`; the pending version then covers R1 as defence in depth. A
barrier `ALTER` reads `rbegin` of the registered map to pick the mutation to wait for; it now also considers
the highest pending **mutation** version. `waitForMutation` is map-only and treats an unregistered version as
killed, so the barrier first waits, under the background mutex on
`currently_processing_in_background_condition`, for the pending version to leave `pending_versions` (every
registration, failure removal and rollback removal notifies that condition); then, under the same mutex, it
checks whether the version was registered: if so it releases the mutex and calls `waitForMutation`; if not,
preparation failed or rolled back and there is nothing to wait for. This lets an `ALTER` issued through
`StorageAlias` (which forwards the alias's lock instead of taking the target's) wait for an in-flight
mutation. `StorageAlias::alter` is left as is.

### What stays as it is {#unchanged}

- `committing_blocks` and `CommittingBlock::Op` for `waitForCommittingInsertsAndMutations` and
  `canUsePartInMerges`'s `min_update_block` (kept as the conservative superset; its removal is a follow-up).
- `MergeTreePartsCollector`, `MergeTreeMergePredicate` (apart from the accessors it already calls),
  `PatchPartsUtils`, `MergeTreeData::Transaction`: untouched.
- `system.mutations`, `killMutation`, `getMutationsSnapshot`, `clearOldMutations`, `backupMutations`: untouched,
  because pending versions are not entries of `current_mutations_by_version`; the only status change is the
  postpone reason strings above. Pending insert and erase are logged at Debug with version and kind (a
  leaked entry would otherwise show only as postponed work or `OPTIMIZE` refusals).
- The on-disk format: untouched.

## 2. What supersedes what, and prerequisites {#supersedes}

Built on top of PR #119155 with its commits kept (authorship preserved), then:

| PR #119155 piece | in this design |
|---|---|
| `Op::Update` loop in `selectPartsToMutate`, guarded by `supportsLightweightUpdate()` | replaced by the pending version inserted in `updateLightweight` (the guard, which a settings `ALTER` can flip mid-flight, disappears) |
| `PostponeReasons::PENDING_LIGHTWEIGHT_UPDATE` | kept unchanged |
| `removeCommittingBlock` triggers on `Update` | dropped: the pending erase (after releasing the mutex) owns the wake-up for updates, moved parts and the failure path of mutations; registration already triggers on success |
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
- **S6 stale predicate after the `OPTIMIZE FINAL` wait**: `OPTIMIZE FINAL` waits for a running merge (held
  with the existing merge pause failpoint); meanwhile a lightweight update allocates, publishes and erases
  its pending version; the merge is released; the retry must materialize the now-visible patch (its
  predicate was rebuilt). Red with the reused predicate.
- **S7 failure cleanup**: mutation preparation failure after announcement (`mt_throw_after_mutation_commit`),
  and `MOVE` failure after destination preactivation: no pending entry remains, the assignee is woken, and a
  refused merge proceeds. Red if the guard leaks.
- **Unit test** for the boundary accessors with a mixed registered/pending set: below, equal and above the
  data version, empty set, the mutate end bound, the patch-equality case with the holder outliving the
  erase, and allocation-to-announcement atomicity for each kind (a selector-side observer must never see an
  allocated block without its pending entry).

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

## 5. Settled in review round 5 {#settled-r5}

- No fourth writer: every plain-`MergeTree` block allocation goes through `prepareMutationEntry`,
  `updateLightweight`, the `NewPart` helpers and the replace drop-range reservation; ordinary `INSERT`,
  `ATTACH`, `REPLACE PARTITION` and restored attachment allocate and activate under one data-parts write lock.
- Lock order: update insertion/erase adds `lightweight-update lock → background mutex`, corrected `MOVE` adds
  `operation_with_data_parts_mutex → destination background mutex`; no reverse edge found; taking the
  background mutex inside a destination parts lock is forbidden.
- The two accessors have exactly three plain-`MergeTree` callers, none of which looks the value up in the map.
- Contention note: `select_in_partition` polls memory pressure in one-second sleeps while holding the
  background mutex; that already delays mutation registration and now also delays update and `MOVE`
  announcement; recorded, not changed here.
- Remaining review items (S1's pre-fix loss arm, S2c's hold of K, a deterministic long-backoff hook if
  immediate wake-up is to be proven rather than eventual progress) are carried into the implementation plan.
