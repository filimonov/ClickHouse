---
description: 'Design for one MergeTree rule that closes three mutation-ordering races on plain MergeTree: an uncommitted block reservation of a mutation or lightweight update is a provisional mutation version that no mutation, merge or patch selection may cross.'
sidebar_label: 'MergeTree uncommitted version boundary'
sidebar_position: 41
slug: /superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design
title: 'MergeTree: uncommitted block reservations as version boundaries — design'
doc_type: 'reference'
---

# MergeTree: uncommitted block reservations as version boundaries — design (rev.1) {#design}

Scope: plain `StorageMergeTree` only (`ReplicatedMergeTree` orders mutations through the queue and
`havePendingPatchPartsForMutation`). Target: one upstream pull request built on top of
https://github.com/ClickHouse/ClickHouse/pull/119155 and superseding it, plus the same commits on
`altinity/antalya-26.6`. Related issues: https://github.com/ClickHouse/ClickHouse/issues/122083,
https://github.com/ClickHouse/ClickHouse/issues/116686, https://github.com/Altinity/ClickHouse/issues/2331.

## 0. Three races, one cause {#cause}

`StorageMergeTree` reserves a block number in `committing_blocks` (`CommittingBlock{op, number}`, ordered by
number) before the durable state that the number stands for becomes visible:

| reservation | visible state it precedes | window |
|---|---|---|
| `Op::Mutation` | the entry in `current_mutations_by_version` | `prepareMutationEntry` (write `mutation_N.txt`) → `addPreparedMutationEntry` |
| `Op::Update` | the patch part of a lightweight `UPDATE` | `allocateBlockNumber` in `updateLightweight` → patch part commit |
| `Op::NewPart` | the inserted part | `allocateBlockNumber` → part commit |

The selection logic consults only the visible state: `selectPartsToMutate` walks
`current_mutations_by_version` from `upper_bound(part data version)` to `end()`; `MergeTreeMergePredicate::canMergeParts`
compares `getCurrentMutationVersion` of the two parts, which reads the same map; `getPatchesToApplyOnMerge`
uses `getNextMutationVersion` from the same map. Every race below is one selection that crosses a reservation:

- **R1, mutation vs mutation** (issue #122083, Altinity #2331). Two mutations register out of order (N+1 before
  N). A mutate task selected in between applies N+1 alone; the part's data version is now N+1, so N never
  applies to it (`upper_bound` skips it) and is reported done. Rows targeted by N survive. Root cause on top of
  the window: `StorageMergeTree::mutate` declares its alter lock in an `if` initializer, so the lock meant to
  serialize mutations with `ALTER`s is released before `startMutation` runs (upstream master has the same
  lines).
- **R2, lightweight update vs mutation** (issue #116686, PR #119155). A mutation above an uncommitted
  `Op::Update` block mutates the part without the patch and writes a higher data version the patch no longer
  applies to. PR #119155 postpones the part in `selectPartsToMutate` when an `Op::Update` block lies at or
  below the target version, and wakes the assignee when an `Update` block is released.
- **R3, merge vs in-flight mutation** (found in review of R1's fix). Part P below an unregistered mutation
  block N and part Q inserted above N have equal `getCurrentMutationVersion` (N is not in the map yet), so
  they may merge; the merged part takes P's data version; when N registers it applies to the merged part and
  deletes rows inserted after it. The mirror (merged part above N) skips N forever. `canUsePartInMerges`
  already guards the `Update` kind conservatively (`min_update_block`); nothing guards `Mutation`.

Fixing each race where it surfaces gives three loops over `committing_blocks` in three places, each with its
own postpone reason and wake-up. This design replaces them with one rule.

## 1. The rule {#rule}

**An uncommitted `Mutation` or `Update` reservation is a provisional mutation version.** For every decision that
compares or advances a part's data version, a reservation counts exactly like a registered mutation with that
version, until its holder releases it. Consequences:

1. `selectPartsToMutate` may apply to a part only the registered mutations below the first reservation above
   the part's data version. If the part has pending registered mutations but all of them lie above such a
   reservation, the part is postponed with one reason:
   `UNCOMMITTED_LOWER_VERSION = "Waiting for a mutation or lightweight update with a lower block number to commit"`.
   This subsumes PR #119155's `Op::Update` loop and the `Op::Mutation` bound of the first R1 fix.
2. `MergeTreeMergePredicate::canMergeParts` compares the two parts' current mutation versions **including
   reservations**: a reservation between the two data versions makes the versions differ, so the merge is
   refused. This closes R3 and makes `canUsePartInMerges`'s `min_update_block` refusal a special case (kept
   or removed per review, see §5).
3. `getPatchesToApplyOnMerge` bounds the patches by the next version **including reservations**, so a merge
   never applies a patch from above an uncommitted lower version.
4. Liveness: `removeCommittingBlock` triggers `background_operations_assignee` when a `Mutation` or `Update`
   reservation is released (PR #119155 does this for `Update`; registration of a mutation already triggers,
   but a mutation that fails between allocation and registration releases its block without registering).
5. `Op::NewPart` reservations are not version boundaries: an insert has no mutation semantics, and merges
   across an uncommitted insert are governed by block ranges and `getMaxLevelInBetween`, unchanged here.
   `Op::Unknown` is treated as a boundary, as `getMinUpdateBlockNumber` already does (conservative).

### Implementation shape {#shape}

One snapshot, three accessors, no new data structure:

```cpp
/// Uncommitted reservations that stand for a mutation version: mutations between file write and
/// registration, lightweight updates between block allocation and patch commit.
static bool isVersionReservation(const CommittingBlock & b)
{
    return b.op == CommittingBlock::Op::Mutation || b.op == CommittingBlock::Op::Update
        || b.op == CommittingBlock::Op::Unknown;
}

/// Highest committed or reserved mutation version <= data_version (0 if none).
UInt64 StorageMergeTree::getCurrentMutationVersion(UInt64 data_version, const CommittingBlocksSet & reservations, lock) const;
/// Lowest committed or reserved mutation version > data_version (0 if none).
UInt64 StorageMergeTree::getNextMutationVersion(UInt64 data_version, const CommittingBlocksSet & reservations, lock) const;
```

`MergeTreeMergePredicate` already holds `committing_blocks` (a snapshot taken in its constructor) and passes
it to both accessors. `selectPartsToMutate` takes one snapshot per call (as PR #119155 does) and uses
`getNextMutationVersion`-style logic for the end bound: `mutations_end_it = lower_bound(first reservation >
part data version)`. The snapshot is taken under `currently_processing_in_background_mutex`, which is also
held when a mutation registers and when its holder is destroyed, so a reservation cannot vanish between the
snapshot and the decision without the decision seeing the registered entry (see §4 for the argument).

The two-argument overloads of the accessors that read only the map remain for callers that hold no
snapshot (`getIncompleteMutationsStatus`, `clearOldMutations`), which are correct as long as no part can
cross a gap, which is exactly what the rule guarantees.

### The lock fix, separately {#lock}

`StorageMergeTree::mutate` keeps its alter lock until `startMutation` returns (two lines). This restores the
documented contract "serialize order of mutations with alter queries" and, as a side effect, makes R1's
inversion impossible for every mutation created through `mutate` (all user mutations, lightweight `DELETE`
included; the `ALTER` path already registers under the lock `InterpreterAlterQuery` took). It is a distinct
bug with a distinct commit; the rule above then covers R1 as defence in depth and covers any future creator
of mutations that does not take the lock.

## 2. What supersedes what {#supersedes}

Built on top of PR #119155 with its commits kept (authorship preserved):

| PR #119155 piece | in this design |
|---|---|
| `Op::Update` loop in `selectPartsToMutate` | replaced by the reservation-aware end bound (rule 1) |
| `PostponeReasons::PENDING_LIGHTWEIGHT_UPDATE` | replaced by `UNCOMMITTED_LOWER_VERSION` (one reason for both kinds); its test's expected string changes |
| `removeCommittingBlock` triggers on `Update` | extended to `Mutation` (rule 4) |
| failpoint `mt_lightweight_update_pause_after_block_allocation` | kept, used by the tests |
| test `05227_mutation_waits_for_lightweight_update.sh` | kept, expected reason string updated |

Added: failpoint `mt_pause_once_before_register_mutation` (`PAUSEABLE_ONCE`; the existing
`mt_pause_before_register_mutation` is `PAUSEABLE` and would hold every mutation, so it cannot hold one while a
second one is observed), the lock fix, the merge-predicate and patch-selection changes, and the tests below.

## 3. Tests, failing first {#tests}

One new stateless test, `NNNNN_mutations_do_not_cross_uncommitted_versions.sh` (number from `add-test` at
rebase time), tags `no-parallel, no-replicated-database, no-shared-merge-tree`, deterministic (failpoints and
polling on `system.mutations`, no sleeps as synchronisation, bounded `SYSTEM WAIT FAILPOINT`, cleanup in a
trap that drops the table and disables both failpoints):

- **S1 mutation vs mutation** (R1): pause DELETE A at `mt_pause_once_before_register_mutation`; DELETE B with
  `lock_acquire_timeout = 1` must fail with `TIMEOUT_EXCEEDED` (lock fix); release; both done in order; count 0.
  Red before the lock fix (B succeeds and A's rows survive).
- Rule 1's `Mutation` arm has no SQL-reachable scenario once the lock fix is in (every creator of a mutation
  holds the per-table alter lock), so it is covered by S3, by the argument in §4, and by a unit-level check
  if the reviewer asks for one.
- **S2 update vs mutation** (R2): PR #119155's test as is, with the new reason string.
- **S3 merge vs in-flight mutation** (R3): `INSERT (0)`; pause DELETE `WHERE id = 1` before registration;
  `INSERT (1)` (block above the mutation); `OPTIMIZE TABLE ... FINAL` must not merge the two parts (assert
  `system.parts` still has two active parts, or `optimize_throw_if_noop = 1` throws); release; the mutation
  applies to the first part only; `SELECT groupArray(id)` is `[0,1]`. Red before rule 2 (the merge happens
  and row 1 is deleted). A second arm: `INSERT (0)` in a part; pause DELETE `WHERE id = 0`; `INSERT (2)`
  above; `OPTIMIZE FINAL`; the mirror (merged part above N skips N) shows as `id = 0` surviving.
- **S4 barrier `ALTER` vs in-flight mutation** (lock fix): pause DELETE before registration; `ALTER TABLE
  ... RENAME COLUMN` with `lock_acquire_timeout = 1` must fail with `TIMEOUT_EXCEEDED`; release; rename
  succeeds afterwards. Red before the lock fix (the rename proceeds and the mutation is interpreted against
  the new metadata).

## 4. Why the rule is sound {#argument}

- Reservation lifetime: `PlainCommittingBlockHolder` lives from `allocateBlockNumber` until the holder's
  destructor removes the block. For a mutation the holder is destroyed after `addPreparedMutationEntry`
  registered the entry, both under `currently_processing_in_background_mutex`; for an update the block is
  released after the patch part is committed. So at every instant a version is either reserved, or
  registered/committed, or both; never neither while in flight.
- Every decision (mutation selection, merge predicate construction, patch selection) takes its snapshot of
  `committing_blocks` while holding `currently_processing_in_background_mutex`, and consults the map under
  the same lock. A reservation released before the snapshot has its entry visible in the map; a reservation
  released after the snapshot is still in the snapshot. Either way the version is counted.
- Block numbers come from one increment, so a reservation allocated after the snapshot is above every
  version the decision can see, and no decision needs to see it: a part it selects has a data version below
  it, and the later mutation or update applies to the result as usual.
- Failure between allocation and registration: the holder is destroyed, the reservation disappears, the
  gap is known never to register, and rule 4 wakes the assignee. Orphaned `mutation_N.txt` handling is
  unchanged (`MergeTreeMutationEntry` removes it when unregistered).

## 5. Open questions for review {#questions}

1. Is it cleaner to fold reservations into `getCurrentMutationVersion`/`getNextMutationVersion` (this
   design) or to keep them out of the accessors and add one explicit `firstReservationAbove(dv)` helper used
   by the three sites? Which reads better to a MergeTree maintainer?
2. With rule 2 in place, is `canUsePartInMerges`'s `min_update_block` refusal still needed (it refuses any part
   at or above the lowest uncommitted update, even when both parts of a candidate merge are above it)? If it
   is stricter for a reason (patch parts must not be applied on merge across an uncommitted update), say
   which case rule 2 plus rule 3 would miss.
3. Should `Op::NewPart` reservations be boundaries after all (a merge that spans an uncommitted insert's block
   number)? If block ranges already forbid that merge, cite where.
4. Transaction rollback in the mutation window (upstream master has a registration-time recheck; 26.6 does
   not): in scope for the antalya branch, out of scope upstream?
5. Is the single postpone reason acceptable to upstream, or should the two kinds keep distinct strings for
   `system.mutations` consumers?
