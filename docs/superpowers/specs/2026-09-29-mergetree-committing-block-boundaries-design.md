# MergeTree: committing blocks as version boundaries — design (rev.1) {#design}

Target: upstream `master` (13d023155c7) on top of the two commits of PR #119155, one pull request.
Supersedes the mechanism of `2026-09-28-mergetree-uncommitted-version-boundary-design.md` (rev.10, the
`PendingVersions` registry): same races, same tests where they still apply, no new registry. The reason for the
restart is recorded there and in the SDD ledger: rev.1–rev.6 of that document were this design and were abandoned
after design review round 3 found the counterexample in §3 below; the fix chosen then (an extra registry erased under
the selection mutex) closed it at the cost of ~460 lines, a lock coupling on every mutation and update, and an alter
recovery re-arm that protects nothing. This document closes the same counterexample with one rule (§3) instead.

## 0. The races {#races}

Plain `StorageMergeTree` decides what a mutation, a merge or `OPTIMIZE FINAL` may touch from the registered
mutations (`current_mutations_by_version`) and the parts, while a *version* exists earlier than either:

- a mutation has its block number from `allocateBlockNumber(Op::Mutation)` in `prepareMutationEntry` until
  `addPreparedMutationEntry` registers it (the file is written in between);
- a lightweight update has its block number from `allocateBlockNumber(Op::Update)` in `updateLightweight` until its
  patch parts are committed (the holder dies with `MergeTreeSinkPatch`).

R1. Two mutations register out of order: `mutate` released the alter lock before `startMutation` (issue #122083).
R2. `selectPartsToMutate` applies a mutation to a part while a lightweight update of that part is between allocation
    and patch commit (issue #116686; PR #119155 postpones it, but behind `supportsLightweightUpdate()`, which a
    concurrent `MODIFY SETTING` flips, and only for `Op::Update`).
R3. A merge spans a version that is allocated but not visible, or records a metadata mutation's version past it in
    `getMutationVersionForMergedPart`, so the update's patch is skipped for the merged part.
R4. `OPTIMIZE FINAL` retries after waiting for running merges with the predicate and the metadata snapshot it built
    before the wait (a pre-existing bug, found by test 05297 of the previous branch).

## 1. The rule {#rule}

**`committing_blocks` is the registry of allocated-but-invisible versions.** Every block number is inserted there by
`allocateBlockNumber` under `committing_blocks_mutex` and removed by the holder's destructor, which for a mutation
runs at the end of `addPreparedMutationEntry` (inside the registration critical section) and for an update runs with
the sink after the last patch part is committed. There is no instant at which a version is neither a committing block
nor visible. This is what `ReplicatedMergeTree` relies on already (`ReplicatedMergeTreeMergePredicate` refuses merges
across an uncommitted block; the queue postpones a mutation while a lower update is uncommitted).

A **version reservation** is a committing block with `op ∈ {Mutation, Update}`. `NewPart` blocks are not boundaries on
this target: an insert allocates, inserts and activates its part in one data-parts write-lock section, and
`MOVE PARTITION TO TABLE` commits its destination under the parts lock since PR #108922. (Their inclusion, as in
`ReplicatedMergeTree`, is a separate question and is out of scope.)

Selectors take a **snapshot** of the reservations under `currently_processing_in_background_mutex` (which they already
hold) and read it as follows. `firstReservationAfter(dv)` = the lowest reservation strictly above `dv`, or none.

1. `selectPartsToMutate`: one snapshot per selection pass, before the parts are read (as #119155 does), taken
   unconditionally (the `supportsLightweightUpdate()` guard is removed). For each part, the applicable range of
   registered mutations is `[upper_bound(dv), lower_bound(firstReservationAfter(dv)))`. If the range is empty because of
   an `Update` reservation, the part is postponed with `PostponeReasons::PENDING_LIGHTWEIGHT_UPDATE` (kept verbatim from
   #119155). A `Mutation` reservation cannot have a registered mutation above it (every registration path holds the
   alter lock, §4), so no second reason string exists.
2. `MergeTreeMergePredicate::canMergeParts`: refuse if a reservation lies in `(min(dv_l, dv_r), max(dv_l, dv_r)]`
   (`ReplicatedMergeTree`'s rule for uncommitted blocks, restricted to reservations). `canUsePartInMerges`'s
   `min_update_block` refusal stays.
3. `getMutationVersionForMergedPart`: the walk that records a materialized metadata mutation's version ends at
   `lower_bound(firstReservationAfter(sources_dv))`; the pre-check and the `RENAME` refusal loop stay unbounded (a bounded
   pre-check would skip the refusal, pinned by test 05296). The caller passes the bound from the predicate's snapshot.
4. `getPatchesToApplyOnMerge`: `next_version = min(getNextMutationVersion(dv), firstReservationAfter(dv))`, plus the
   watermark rule of §3.
5. `removeCommittingBlock` triggers `background_operations_assignee` for `Mutation` and `Update` (a released reservation
   removes a boundary), after `committing_blocks_mutex` is released, as #119155 does for `Update`.

Why a snapshot is enough (the monotonicity argument): block numbers come from one `SimpleIncrement`, so a version
allocated after the snapshot is above every part, every registered mutation and every reservation that existed at the
snapshot. A later mutation cannot register while the selector holds the background mutex; a later update's patch is
above every source part; a later insert's part is above every source part. None of them can be *between* two things
the selector compares, so the snapshot is exact for every decision in rules 1–3.

## 2. The lock fix and the two small fixes, separately {#separate}

- `mutate` keeps the alter lock through `startMutation` (R1; commit 6109419a064 of the previous branch, test 05291).
- `OPTIMIZE FINAL` rebuilds the predicate and the parts collector and re-reads `getInMemoryMetadataPtr()` on every retry
  after the wait released the mutex (R4; commits b9fbb41381a and b637b72b894 of the previous branch, test 05297).
- `alter`'s recovery path skips re-registering a moved-from entry (`held_entry && held_entry->commands`; the
  null-`commands` dereference is pre-existing; commit 1f86af3569e of the previous branch, guard only).

## 3. The counterexample and the watermark {#watermark}

The monotonicity argument covers parts and mutations. It does not cover **patch materialization**, because a merge that
applies a patch raises the merged part's version to the patch's version (`FutureMergedMutatedPart::assign`,
`FutureMergedMutatedPart.cpp:68-77`). Design review round 3 of the previous document:

1. part `P` with `dv < U`; update `U` allocated, paused before its patch commit;
2. update `V > U` (disjoint rows) commits its patch — updates do not wait for lower updates;
3. the merge predicate copies the reservations at `t1` and the patch parts at `t2 > t1` (`MergeTreeMergePredicate`'s
   constructor: initializer list, then body); `U` was allocated after `t1` and `V` committed before `t2`;
4. `U` is in no snapshot, patch `V` is; the merge materializes `V` and the result's version becomes `V`;
5. `U` commits its patch below the result's version: it never applies to the merged part. Data loss.

The window is the constructor's own duration, but it exists. Rule: **the predicate reads the reservations and a
watermark together, before it reads the patch parts, and never materializes a patch above the watermark.** The
watermark is the value of `increment` at the moment of the reservation copy, read under `committing_blocks_mutex`, the
same mutex `allocateBlockNumber` increments and inserts under, so "allocated at or before the watermark" and "present in
the snapshot or already released" are the same statement. Then, for the merge's patch selection:

- `U` allocated before the copy: `U` is in the snapshot (still uncommitted at `t1`) → `min_update_block ≤ U` → every
  patch with version `≥ U`, including `V`, is left for read time (existing rule);
- `U` allocated after the copy: `U > watermark` and `V > U > watermark` → `V` is left for read time (new rule).

A patch left for read time is applied by block number and offset, idempotently, exactly as it is applied to the source
parts today; only materialization on merge raises the version. `getPatchesToApplyOnMerge` therefore skips any patch with
`max data version > watermark`. The order "reservations and watermark, then patches" is load-bearing and is stated in the
constructor; the reverse order has the symmetric hole (`U` committing between the two copies).

## 4. Why it is sound, and what it does not claim {#argument}

- I1 (no gap): a version is a committing block from allocation until visibility (§1); the alter recovery interval
  un-registers a rename mutation whose block is long released, but during that interval no mutation can register above
  it (the failing `ALTER` holds the alter lock) and a merge below it records at most a lower version, so the re-registered
  rename still applies; a merge that started before the failure with the new metadata recorded the rename's version and
  wrote the new names. No re-arm is needed. (The value loss for rows *inserted* in that window is a pre-existing bug,
  `tmp/cas252/issue_alter_rollback_window_loses_renamed_column.md`, not changed by this design.)
- I2 (exact snapshot): the monotonicity argument of §1 for parts and mutations; §3 for patches.
- I3 (alter lock): every path that registers a mutation on plain `MergeTree` holds the alter lock: `mutate` (after the
  R1 fix) and `alter` (its own rename mutation, from the interpreter's `lockForAlter`). Hence no registered mutation
  above a `Mutation` reservation, and no barrier-`ALTER` fence.
- Lock order: unchanged. `currently_processing_in_background_mutex → committing_blocks_mutex` (predicate constructor,
  `selectPartsToMutate`); `removeCommittingBlock` releases its mutex before `trigger`; no new edge; nothing takes the
  background mutex on the allocation path (this is the contention the previous design added and this one does not).
- Not claimed: `NewPart` blocks as boundaries; `ReplicatedMergeTree`; `SharedMergeTree`; transactions beyond what 04516
  already pins.

## 5. Tests, failing first {#tests}

Reused from the previous branch where their expectations hold on this design (renumbered by `add-test`):
05291 (R1, red before the lock fix), 05292 (R2: settings-independent postponement, cancelled update wakes the mutation,
truncation: a lower registered mutation applies while a higher one waits), 05293 (barrier vs paused mutation, pin),
05294 (merge across a paused mutation refused; merge below a pending update keeps it applicable), 05295 (failed mutation
releases its block; `KILL MUTATION` ignores an unregistered one), 05296 (merge does not record a metadata mutation past
a pending update; `RENAME` refused), 05297 (`OPTIMIZE FINAL` retry with a held merge sees the `RENAME` published during
the wait), 05227 (from #119155, timeout wrapper), 04516, 04309, 04312.

New, red on this design without §3: **05298 the round-3 counterexample.** `PAUSEABLE_ONCE` failpoint
`mt_merge_predicate_pause_after_reservation_snapshot` between the reservation copy and the patch copy in the predicate
constructor; `SYSTEM STOP MERGES`; two parts of a table with block number/offset columns; `OPTIMIZE TABLE … FINAL` in the
background reaches the failpoint; while paused: `UPDATE U` parked at `mt_lightweight_update_pause_after_block_allocation`,
then `UPDATE V` on other rows runs to completion; release the predicate; release `U`; wait both; assert every row shows
both updates and one regular active part. Without the watermark the rows of `U` keep their old values.

Test rules as before: tags with reasons, `timeout 60` around every `SYSTEM WAIT FAILPOINT`, traps that disable every
failpoint and `wait`, `KILL … ASYNC` for parked queries, bounded polls, full test names on the command line.

## 6. Size {#size}

Source: `StorageMergeTree.{h,cpp}` (mutate lock scope; `selectPartsToMutate` end bound; `getMutationVersionForMergedPart`
bound parameter; `removeCommittingBlock` trigger; `OPTIMIZE FINAL` retry; recovery guard; a `CommittingBlocksSnapshot
{blocks, watermark}` accessor), `MergeTreeMergePredicate.{h,cpp}` (watermark member; `canMergeParts` reservation rule;
`getPatchesToApplyOnMerge` bounds; `firstReservationAfter`), `PatchPartsUtils.{h,cpp}` (watermark parameter or the caller
filters before the call), `FailPoint.cpp` (one failpoint). Expected: about 100–150 changed lines in `src`. No new header,
no gtest, no new object kind.
