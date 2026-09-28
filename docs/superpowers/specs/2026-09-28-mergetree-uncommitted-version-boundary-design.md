---
description: 'Design for one MergeTree rule that closes three mutation-ordering races on plain MergeTree: an uncommitted block reservation of a mutation or lightweight update is a version boundary that no mutation, merge or patch selection may cross.'
sidebar_label: 'MergeTree uncommitted version boundary'
sidebar_position: 41
slug: /superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design
title: 'MergeTree: uncommitted block reservations as version boundaries — design'
doc_type: 'reference'
---

# MergeTree: uncommitted block reservations as version boundaries — design (rev.2) {#design}

Scope: plain `StorageMergeTree` only (`ReplicatedMergeTree` orders mutations through the queue and
`havePendingPatchPartsForMutation`). Target: one upstream pull request built on top of
https://github.com/ClickHouse/ClickHouse/pull/119155 and superseding it, plus the same commits on
`altinity/antalya-26.6`. Related issues: https://github.com/ClickHouse/ClickHouse/issues/122083,
https://github.com/ClickHouse/ClickHouse/issues/116686, https://github.com/Altinity/ClickHouse/issues/2331.

Revision history: rev.1 folded reservations into `getCurrentMutationVersion`/`getNextMutationVersion` and
used one postpone reason. Codex design review round 1 (`docs/superpowers/reports/2026-09-28-cas-252-codex-reviews/design_review_r1.md`)
accepted the invariant and asked for an explicit helper instead of overloaded accessors, distinct postpone
strings, corrections to the lifetime argument, an honest scope for transactions and orphan files, the
`StorageAlias` gap, and a test plan without the invalid "mirror" arm. A separate audit of PR #119155
(`audit_119155.md`) found its guard skipped when `supportsLightweightUpdate()` is false, which a settings
`ALTER` can make true while an update is in flight. rev.2 incorporates all of it.

## 0. Three races, one cause {#cause}

`StorageMergeTree` reserves a block number in `committing_blocks` (`CommittingBlock{op, number}`, ordered by
number) before the durable state that the number stands for becomes visible:

| reservation | visible state it precedes | window |
|---|---|---|
| `Op::Mutation` | the entry in `current_mutations_by_version` | `prepareMutationEntry` (write `mutation_N.txt`) → `addPreparedMutationEntry` |
| `Op::Update` | the patch part of a lightweight `UPDATE` | `allocateBlockNumber` in `updateLightweight` → patch part commit (the holder lives in `MergeTreeSinkPatch`) |
| `Op::NewPart` | the inserted part | `allocateBlockNumber` → part commit, both under `lockParts` |

The selection logic consults only the visible state: `selectPartsToMutate` walks
`current_mutations_by_version` from `upper_bound(part data version)` to `end()`; `MergeTreeMergePredicate::canMergeParts`
compares `getCurrentMutationVersion` of the two parts, which reads the same map; `getPatchesToApplyOnMerge`
uses `getNextMutationVersion` from the same map. Every race below is one selection that crosses a reservation:

- **R1, mutation vs mutation** (issue #122083, Altinity #2331). Two mutations register out of order (N+1 before
  N). A mutate task selected in between applies N+1 alone; the part's data version is now N+1, so N never
  applies to it (`upper_bound` skips it) and is reported done. Rows targeted by N survive. Root cause on top of
  the window: `StorageMergeTree::mutate` declares its alter lock in an `if` initializer, so the lock meant to
  serialize mutations with `ALTER`s is released before `startMutation` runs (upstream master has the same
  lines). Reproduced from CI logs: two `DELETE`s 8 ms apart, `mutation_51` registered 1.1 ms before
  `mutation_50`, one row of mutation 50 survived; and deterministically by the test in §3.
- **R2, lightweight update vs mutation** (issue #116686, PR #119155). A mutation above an uncommitted
  `Op::Update` block mutates the part without the patch and writes a higher data version the patch no longer
  applies to. PR #119155 postpones the part in `selectPartsToMutate` when an `Op::Update` block lies at or
  below the target version, and wakes the assignee when an `Update` block is released. Its guard is taken
  only `if (supportsLightweightUpdate())`, which reads two table settings that a settings-only `ALTER` can
  flip while an update still holds its reservation; the audit gives the interleaving that loses the update.
- **R3, merge vs in-flight mutation** (found in review of R1's fix). Part P below an unregistered mutation
  block N and part Q inserted above N have equal `getCurrentMutationVersion` (N is not in the map yet), so
  they may merge; the merged part takes P's data version (`mutation` is 0 for unmutated sources, so
  `getDataVersion` is `min_block`); when N registers it applies to the merged part and deletes rows inserted
  after it. `canUsePartInMerges` already guards the `Update` kind conservatively (`min_update_block`);
  nothing guards `Mutation`.

Fixing each race where it surfaces gives three loops over `committing_blocks` in three places, each with its
own postpone reason and wake-up. This design replaces them with one rule and one helper.

## 1. The rule {#rule}

**An uncommitted `Mutation` or `Update` reservation is a version boundary.** No mutation selection, merge or
patch selection may produce a part whose data version crosses it, until its holder releases it.

Consequences:

1. `selectPartsToMutate` may apply to a part only the registered mutations below the first reservation above
   the part's data version. A part whose pending registered mutations all lie above such a reservation is
   postponed. The postpone reason names the kind of the reservation: `PENDING_LIGHTWEIGHT_UPDATE` (PR
   #119155's string, unchanged) for `Update`, and a new `PENDING_MUTATION_REGISTRATION = "Waiting for a
   mutation with a lower block number to be registered"` for `Mutation` and `Unknown`. This subsumes PR
   #119155's `Op::Update` loop and drops its `supportsLightweightUpdate()` guard: the existence of a
   reservation is authoritative, not the table's current eligibility to start a new update.
2. `MergeTreeMergePredicate::canMergeParts` keeps its registered-version comparison and additionally refuses
   the merge when a reservation lies in `(min(dv_left, dv_right), max(dv_left, dv_right)]`. Closes R3.
3. `getPatchesToApplyOnMerge` bounds the patches by `min(getNextMutationVersion(dv), first reservation > dv)`,
   so a merge never materializes a patch from above an uncommitted lower version.
4. Liveness: `removeCommittingBlock` triggers `background_operations_assignee` when a `Mutation` or `Update`
   reservation is released, after `committing_blocks_mutex` is dropped (PR #119155 does this for `Update`;
   a mutation that fails between allocation and registration releases its block without registering, so
   registration's own trigger does not cover it).
5. `Op::NewPart` reservations are not boundaries: insert allocation, indexing into `data_parts_indexes` and
   commit happen under `lockParts` (`MergeTreeSink`, `MergeTreeData::renameTempPartAndReplace`), so a selector
   sees either no allocation yet or the committed part, and `getMaxLevelInBetween` guards indexed gaps.
   `Op::Unknown` is not produced by plain `StorageMergeTree` today; it is treated as a boundary, as
   `getMinUpdateBlockNumber` already does.
6. `canUsePartInMerges`'s `min_update_block` refusal is redundant once rules 2 and 3 hold (review, §3 answer 2)
   but stays in this change: removing it permits merges wholly above a pending update and needs its own
   below/above-boundary tests. Its removal is a follow-up, not part of the superseding pull request.

### Implementation shape {#shape}

One helper, one snapshot per decision, no new data structure, existing accessors untouched:

```cpp
/// Block reservations that stand for a mutation version not yet visible: a mutation between its
/// file write and its registration, a lightweight update between block allocation and patch commit.
using VersionReservations = std::vector<CommittingBlock>;   /// sorted by number, pre-filtered

VersionReservations StorageMergeTree::getVersionReservations() const;   /// snapshot under committing_blocks_mutex
/// The lowest reservation strictly above data_version, or nullptr.
static const CommittingBlock * firstVersionReservationAfter(Int64 data_version, const VersionReservations & r);
```

`selectPartsToMutate` takes the snapshot once per call under `currently_processing_in_background_mutex` and
sets `mutations_end_it = current_mutations_by_version.lower_bound(reservation->number)` per part (or `end()`).
`MergeTreeMergePredicate` takes it in its constructor (where it already copies `committing_blocks`) and uses
it in `canMergeParts` and `getPatchesToApplyOnMerge`. `getCurrentMutationVersion` and `getNextMutationVersion`
keep describing registered mutations only; their other callers (`getIncompleteMutationsStatus`,
`clearOldMutations`) stay correct because no part can cross a gap once the rule holds.

Pre-filtering matters: `committing_blocks` is mostly `NewPart` entries under insert load, and the helper runs
once per candidate part.

### The lock fix, separately {#lock}

`StorageMergeTree::mutate` keeps its alter lock until `startMutation` returns (two lines). This restores the
documented contract "serialize order of mutations with alter queries" and, as a side effect, makes R1's
inversion impossible for every mutation created through `mutate` (all user mutations, lightweight `DELETE`
included; the `ALTER` path already registers under the lock `InterpreterAlterQuery` took). It is a distinct
bug with a distinct commit; rule 1 then covers R1 as defence in depth. Known exception: `StorageAlias::alter`
forwards the alias's lock holder to the target instead of taking the target's alter lock, so a metadata
`ALTER` issued through an alias can still overtake a mutation on the target between its file write and its
registration. The pull request either takes the target's alter lock in `StorageAlias::alter` (preferred if
small) or names this as a remaining exception with a follow-up issue.

## 2. What supersedes what {#supersedes}

Built on top of PR #119155 with its commits kept (authorship preserved), then:

| PR #119155 piece | in this design |
|---|---|
| `Op::Update` loop in `selectPartsToMutate`, guarded by `supportsLightweightUpdate()` | replaced by the helper; guard dropped (audit finding 1) |
| `PostponeReasons::PENDING_LIGHTWEIGHT_UPDATE` | kept unchanged for `Update`; `PENDING_MUTATION_REGISTRATION` added |
| `removeCommittingBlock` triggers on `Update` | extended to `Mutation` |
| failpoint `mt_lightweight_update_pause_after_block_allocation` | kept |
| test `05227_mutation_waits_for_lightweight_update.sh` | kept verbatim, plus a `timeout` around its `SYSTEM WAIT FAILPOINT` (audit finding 2) |

Added: failpoint `mt_pause_once_before_register_mutation` (`PAUSEABLE_ONCE`; the existing
`mt_pause_before_register_mutation` is `PAUSEABLE` and would hold every mutation, so it cannot hold one while a
second one is observed), the lock fix, the merge-predicate and patch-selection changes, and the tests in §3.

Commit order in the superseding pull request: (1) #119155's commits; (2) the helper and rule 1 with the guard
drop and the new reason; (3) rule 2 and rule 3 in the merge predicate; (4) rule 4; (5) the lock fix; (6)
tests. Each commit builds and its tests pass on their own.

## 3. Tests, failing first {#tests}

`05227_mutation_waits_for_lightweight_update.sh` (PR #119155) stays as the R2 pin. One new stateless test,
`NNNNN_mutations_do_not_cross_uncommitted_versions.sh` (number from `add-test` at rebase time), tags
`no-parallel, no-replicated-database, no-shared-merge-tree`, deterministic (failpoints and polling on
`system.mutations`/`system.parts`, no sleeps as synchronisation, every `SYSTEM WAIT FAILPOINT` under `timeout`,
a trap that disables both failpoints, reaps background clients and drops the tables):

- **S1 mutation vs mutation** (R1, lock fix): pause DELETE A at `mt_pause_once_before_register_mutation`;
  DELETE B with `lock_acquire_timeout = 1` fails with `TIMEOUT_EXCEEDED` and `system.mutations` is empty;
  release A; wait for A done; run B again synchronously; count 0, two mutations done in id order. Red before
  the lock fix: B succeeds while A is paused, A's rows survive. (Implemented and verified red/green on
  `antalya-26.6` as `05054_mutations_register_in_order.sh`; it is this scenario.)
- **S2 update vs mutation, cancellation arm** (rule 4): pause a lightweight `UPDATE` after its block
  allocation (PR #119155's failpoint); start `ALTER ... UPDATE` and observe `PENDING_LIGHTWEIGHT_UPDATE`;
  `KILL QUERY` the paused update; no patch part is committed; the postponed mutation wakes and completes
  without a scheduler tick timeout. Red before rule 4 only if the wake-up is missing; otherwise it pins it.
- **S2b guard drop** (audit finding 1): pause the update after allocation; `ALTER TABLE ... MODIFY SETTING
  enable_block_offset_column = 0`; start the mutation; it must still be postponed with
  `PENDING_LIGHTWEIGHT_UPDATE`; re-enable the setting, release, both applied. Red on PR #119155 as is.
- **S3 merge vs in-flight mutation** (R3, rule 2): `INSERT (0)`; pause DELETE `WHERE id = 1` before
  registration; `INSERT (1)` (block above the mutation); `OPTIMIZE TABLE ... FINAL SETTINGS
  optimize_throw_if_noop = 1` must throw (two active parts remain in `system.parts`); release; the mutation
  applies to the first part only; `SELECT groupArray(id)` is `[0,1]`. Red before rule 2 (the merge happens,
  the merged part keeps the first part's data version, and row 1 is deleted). A plain merge of unmutated
  parts cannot advance above N, so there is no "mirror" arm; the above-boundary case is a patch case, below.
- **S3b patch across a pending update** (rule 3): parts below a pending update U (paused after allocation),
  a committed later lightweight update V above U; a merge of the parts must not materialize V's patch
  (assert on `_part` data and `system.parts` after the merge), and after U commits both patches apply. Red
  before rule 3.
- **S4 barrier `ALTER` vs in-flight mutation** (lock fix): pause DELETE before registration; `ALTER TABLE ...
  RENAME COLUMN` with `lock_acquire_timeout = 1` fails with `TIMEOUT_EXCEEDED`; release; the rename succeeds
  afterwards. Red before the lock fix. An alias variant is added only if `StorageAlias::alter` is fixed in the
  same pull request.
- **Unit test** for `firstVersionReservationAfter`: classification of `NewPart`/`Mutation`/`Update`/`Unknown`,
  reservations below and above the data version, several `NewPart`s in between, empty input.
- Rule 1's `Mutation` arm has no SQL-reachable scenario once the lock fix is in (every creator of a mutation
  holds the per-table alter lock); it is covered by the unit test and by S3, which exercises the same
  reservation from the merge side.

## 4. Why the rule is sound, and what it does not claim {#argument}

- Successful mutation registration transfers the entry into `current_mutations_by_version` and destroys its
  reservation while holding `currently_processing_in_background_mutex`; every decision takes its snapshot
  under the same mutex, so a decision sees the reservation, or the registered entry, or both, never neither.
- A lightweight update's reservation is not released under that mutex: its holder lives in the patch sink
  and outlives the patch part commit. A decision made before the release keeps the reservation in its
  snapshot; a decision made after it can observe the committed patch part. Either way the version is counted.
- Block numbers come from one increment, so a reservation allocated after a snapshot is above every version
  the decision can see, and the later mutation or update applies to the decision's result as usual.
- A failed operation releases its reservation without publishing state; a decision with an older snapshot
  is merely conservative, and rule 4 wakes the assignee.
- **Not covered by the rule, stated as scope:** (a) a mutation inside a transaction whose transaction rolls
  back while the mutation is between file write and registration. Upstream master rechecks the transaction
  after registration under the background mutex and removes the entry and its file; `antalya-26.6` lacks that
  code, so the Antalya series backports it as a prerequisite commit with its test; the upstream pull request
  relies on it. (b) A nontransactional `mutation_N.txt` left behind when registration fails and the file
  removal fails or the process dies first: `loadMutations` treats every final file as registered after a
  restart. The pull request reorders `PreparedMutationEntry` so the entry (and its file removal) is destroyed
  before the block holder, which narrows the same-process gap, and states the restart limitation; a durable
  distinction between "final file" and "registered mutation" is a separate change.

## 5. Open questions for review {#questions}

1. Is dropping `supportsLightweightUpdate()` from the guard (rule 1) enough, or does `updateLightweight`
   itself need to refuse a settings `ALTER` while it holds a reservation?
2. Is `PENDING_MUTATION_REGISTRATION` the right name and string for the new reason, given that after the
   lock fix it should never be observed through SQL?
3. Should the `StorageAlias::alter` fix be in this pull request (it touches a third storage) or a follow-up?
4. For the Antalya series: backport the upstream transaction recheck as the first commit, or land it as its
   own pull request first?
