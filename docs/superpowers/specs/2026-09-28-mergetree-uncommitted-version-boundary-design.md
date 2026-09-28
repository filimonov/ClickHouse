---
description: 'Design for one MergeTree rule that closes three mutation-ordering races on plain MergeTree: an uncommitted block reservation of a mutation or lightweight update is a version boundary that no mutation, merge or patch selection may cross, decided on a coherent cut of parts and reservations.'
sidebar_label: 'MergeTree uncommitted version boundary'
sidebar_position: 41
slug: /superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design
title: 'MergeTree: uncommitted block reservations as version boundaries — design'
doc_type: 'reference'
---

# MergeTree: uncommitted block reservations as version boundaries — design (rev.6) {#design}

Scope: plain `StorageMergeTree` only (`ReplicatedMergeTree` orders mutations through the queue and
`havePendingPatchPartsForMutation`). Target: one upstream pull request built on top of
https://github.com/ClickHouse/ClickHouse/pull/119155 and superseding it, plus the same commits on
`altinity/antalya-26.6`. Related issues: https://github.com/ClickHouse/ClickHouse/issues/122083,
https://github.com/ClickHouse/ClickHouse/issues/116686, https://github.com/Altinity/ClickHouse/issues/2331.

Revision history. rev.1: reservations folded into the mutation-version accessors, one postpone reason.
rev.2 (after design review round 1 and the audit of PR #119155, reports in
`docs/superpowers/reports/2026-09-28-cas-252-codex-reviews/`): explicit helper, distinct reason strings,
corrected lifetime argument, transactions and orphan files scoped, `StorageAlias` gap, guard drop. rev.3 (after
design review round 2, `design_review_r2.md`): the decision must be a **coherent cut**: parts and patches are
captured before the reservation snapshot, in merge selection and in mutation selection alike (round 2's
counterexamples: a mutation allocated after the predicate's snapshot and an insert above it merged with an
older part; a lightweight update allocated after the snapshot and a later disjoint update materialized
across it); rule 3 stated as a generalization of the existing `min_update_block` filter with the `0`
sentinel handled; `Unknown` wakes the assignee too; the two open decisions settled (fix `StorageAlias::alter`
here; land the Antalya transaction recheck first as its own pull request); S3b replaced by the two
late-reservation tests; S1 proves the loss before releasing A; S2 cancellation and S3 made deterministic.
rev.4 (after design review round 3, `design_review_r3.md`, the last design round): the cut is **atomic**, not
ordered: parts, patches and reservations are captured while the data-parts read lock is held, because a
lightweight update publishes its patch under the data-parts lock and releases its reservation right after,
so two non-overlapping snapshots can miss both; every merge-selection attempt, including each `OPTIMIZE
FINAL` retry, builds a fresh bundle; the failure statement, the lightweight-update proof and the cleanup
ordering claim corrected; S3e, a deterministic S3d and rule-3 unit coverage added. rev.5 (owner decision
2026-09-28): the cut is validated by a **publication epoch** instead of holding the data-parts read lock
across the reservation snapshot. Any lengthening of a lock's hold needs its own safety proof and must not
make the MVCC neighbours of the parts lock (inserts, part commits, patch commits) wait behind merge
selection; an epoch check keeps every lock exactly as short as today and detects the one thing the cut
needs to exclude, a publication between the two captures. The lock-overlap form is kept in §1 as the
rejected alternative with its cost. rev.6 (after design review round 4, `design_review_r4.md`): the epoch is a
**boundary-handoff epoch**, advanced only by publications that can retire a reservation predating the
captured parts (patch activation of an `Update`, activation of a split-phase `NewPart`), never by an ordinary
insert, so a hot insert stream cannot starve selection; publication means the `PreActive → Active` transition
in `MergeTreeData::Transaction::commit` (plus `restoreAndActivatePart` and `swapActivePart`), not the
temporary-part rename; `Op::NewPart` is a boundary when its allocation and activation are not one parts-lock
critical section, which today is `MOVE PARTITION TO TABLE` (the review gives a data-loss interleaving that
exists without any of our changes); validation exhaustion is `CANNOT_SELECT`, never `NOTHING_TO_MERGE`; the
rule-3 bound is exclusive; the test hooks (`S2c`, `S3e` retry arm, backoff hook) are named.

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
  lines). Reproduced from CI logs (two `DELETE`s 8 ms apart, `mutation_51` registered 1.1 ms before
  `mutation_50`, one row of mutation 50 survived) and deterministically by test S1, red without the lock fix,
  green with it (`05054_mutations_register_in_order.sh` on `antalya-26.6`).
- **R2, lightweight update vs mutation** (issue #116686, PR #119155). A mutation above an uncommitted
  `Op::Update` block mutates the part without the patch and writes a higher data version the patch no longer
  applies to. PR #119155 postpones the part in `selectPartsToMutate` when an `Op::Update` block lies at or
  below the target version, and wakes the assignee when an `Update` block is released. Its guard is taken
  only `if (supportsLightweightUpdate())`, which reads two table settings that a settings-only `ALTER` can
  flip while an update still holds its reservation (the audit gives the interleaving that loses the update);
  and its snapshot is taken before the parts are read, which is the cut problem of §1.
- **R3, merge vs in-flight mutation** (found in review of R1's fix). Part P below an unregistered mutation
  block N and part Q inserted above N have equal `getCurrentMutationVersion` (N is not in the map yet), so
  they may merge; the merged part takes P's data version (`mutation` is 0 for unmutated sources, so
  `getDataVersion` is `min_block`); when N registers it applies to the merged part and deletes rows inserted
  after it. `canUsePartInMerges` already guards the `Update` kind conservatively (`min_update_block`, computed
  from a snapshot taken in the predicate's constructor, before the parts are collected); nothing guards
  `Mutation`.

Fixing each race where it surfaces gives three loops over `committing_blocks` in three places, each with its
own postpone reason and wake-up, all taken at the wrong moment. This design replaces them with one rule, one
typed snapshot, and one ordering discipline.

## 1. The rule and the cut {#rule}

**An uncommitted `Mutation`, `Update` or `Unknown` reservation is a version boundary.** No mutation selection,
merge or patch selection may produce a part whose data version crosses it, until its holder releases it.

**A decision is made on a validated cut.** Two non-overlapping snapshots, in either order, are insufficient:
reservations first misses a reservation N allocated in between while seeing a part Q > N committed in between
(round 2); parts first misses a patch U published and released in between while having captured a later
patch V > U (round 3). What the cut must exclude is exactly one event: **a part or patch publication between
the parts capture and the reservation snapshot**. A reservation allocated in the gap is above every captured
part (block numbers come from one increment) and needs no consideration; a reservation released in the gap
without a publication is a failed operation, and an older snapshot is merely conservative.

So the cut is validated, not locked. Validation uses a `boundary_publication_epoch` (a 64-bit atomic in
`StorageMergeTree`), not an epoch advanced by every ordinary part publication. It is advanced only by
publication events that can retire a reservation which may predate the captured part set: activation of a
patch part for an `Update`, and activation of any split-phase `NewPart` retained as a boundary. An ordinary
insert whose allocation and activation occur in one parts-lock critical section does not advance it; if it
starts after the capture, its version is above every captured input and cannot invalidate the decision.
Publication means the transition that makes a part visible to the captured active-part set: the
`PreActive → Active` step in `MergeTreeData::Transaction::commit`, and the two direct activations
`restoreAndActivatePart` and `swapActivePart`; the temporary-part rename and `PreActive` insertion
(`renameTempPartAndReplaceImpl` → `preparePartForCommit`) are not publication points, because they happen
before the selector's first read. A selection attempt reads the epoch **inside** the data-parts read lock
together with the parts and patches, releases the lock exactly as today, snapshots `VersionReservations`
under `committing_blocks_mutex`, and re-reads the epoch. Equal epochs mean no relevant handoff happened
between the two captures and the bundle is coherent. A changed epoch means the attempt recaptures (three
in-call tries bound the CPU work; they do not claim progress under an adversarial stream of relevant
handoffs, which are rare by construction). Validation exhaustion is not reported as `NOTHING_TO_MERGE`:
background selection records the retry and requests an immediate later attempt without increasing the
normal no-work backoff; explicit `OPTIMIZE` returns `CANNOT_SELECT` with a transient "part set changed while
capturing merge inputs" explanation. Mutation selection uses the same validation with parts only.

Why this is simpler and safer than the alternative: no lock is held longer than today, so inserts, part
commits and patch commits (the MVCC neighbours of the parts lock) never wait behind merge selection; the
proof obligation shrinks to "the epoch is bumped at every publication under the lock", one site; and the
retry is the ordinary optimistic pattern. The rejected alternative, holding the data-parts read lock through
the reservation snapshot (lock order background mutex → parts lock → `committing_blocks_mutex`, an edge
insert commit already has), is also correct, but it lengthens a hot lock by the reservation copy on every
selection and would need an enumeration of every writer that takes `committing_blocks_mutex` under
`lockParts` and a bound on the wait it adds; the owner rejected lengthening any lock without that proof.
A universal epoch bumped by every insert was also rejected (review round 4): a continually ready insert
writer can publish inside every validation window, and no finite retry count or backoff proves progress.

Consequences:

1. `selectPartsToMutate` captures the parts vector and the reservations as one validated cut, and
   may apply to a part only the registered mutations below the first reservation above the part's data
   version: `mutations_end_it = current_mutations_by_version.lower_bound(reservation->number)` (or `end()`).
   A part whose pending registered mutations all lie above such a reservation is postponed with a reason
   that names the kind: `PENDING_LIGHTWEIGHT_UPDATE` (PR #119155's string, unchanged) for `Update`, and a new
   `PENDING_MUTATION_REGISTRATION = "Mutation with a lower block number is not registered yet"` for
   `Mutation`. `Unknown` is not produced by plain `StorageMergeTree` today; it is treated as a boundary and
   reported with the mutation string, with a comment saying it is conservative and currently unreachable.
   This subsumes PR #119155's loop and drops its `supportsLightweightUpdate()` guard: the existence of a
   reservation is authoritative, not the table's current eligibility to start a new update. The general
   bound is less conservative than #119155's in one way: with registered K < U < M it applies K now and
   postpones M, where #119155 postpones the whole part; this is safe (K is below every boundary) and gets
   its own test (S2c).
2. Merge selection (`selectPartsToMerge` and its collector/predicate pair) captures the active regular parts,
   the patch parts and the reservations as one validated cut per attempt and hands the bundle to
   `MergeTreePartsCollector` and `MergeTreeMergePredicate`; the predicate does not reread patch parts and the
   collector does not reread regular parts. Every collector invocation, including each retry after the
   `OPTIMIZE FINAL` wait for running merges, is a separate attempt with a fresh bundle; a reservation
   snapshot is never reused with a later parts read. `canMergeParts` keeps its registered-version comparison and additionally refuses the
   merge when a reservation lies in `(min(dv_left, dv_right), max(dv_left, dv_right)]`. Closes R3 and the
   round-2 late-reservation counterexamples.
3. `getPatchesToApplyOnMerge` bounds the patches by `min(next registered version above dv, first reservation
   above dv)`, where `getNextMutationVersion(dv) == 0` means infinity and is treated as such before the
   `min` (the raw `min` would yield 0, which `getPatchesToApplyOnMerge` reads as unbounded). The bound is
   **exclusive**: a patch whose data version equals the reservation is not applied (the existing helper
   skips only `max_patch_version > bound`, which for registered mutations never hits equality because block
   numbers are unique, but does when the bound is an in-flight update's own reservation and one partition's
   patch has already been published). This is the
   per-range generalization of the existing `min_update_block` filter (`getPatchPartsByPartition` already
   excludes patches at or above the lowest pending update); the two must not coexist as independent rules.
   `canUsePartInMerges`'s `min_update_block` refusal stays in this change as the conservative superset;
   removing it (merges wholly above a pending update) is a follow-up with its own tests.
4. Liveness: `removeCommittingBlock` triggers `background_operations_assignee` when a `Mutation`, `Update` or
   `Unknown` reservation is released, after `committing_blocks_mutex` is dropped (PR #119155 does this for
   `Update`). Successful registration already triggers, so the `Mutation` trigger matters for the failure
   path: a mutation that fails between allocation and registration releases its block without registering.
5. `Op::NewPart` is also a provisional boundary unless its allocation, insertion and `PreActive → Active`
   publication occur in one uninterrupted data-parts write-lock critical section. Ordinary `INSERT`
   (`MergeTreeSink::commitPart` holds the parts lock across `fillNewPartName`, `renameTempPartAndAdd` and
   `Transaction::commit`), `ATTACH`, `REPLACE PARTITION` and restored-part attachment satisfy that condition;
   `MOVE PARTITION TO TABLE` currently does not: the destination allocates `NewPart` reservations, inserts
   the moved parts as `PreActive`, releases the destination parts lock for the disk renames, and commits
   later. A merge of two active neighbours selected in that window spans the moved part's block; the
   later commit then finds the covering part and marks the moved part `Outdated`, and the rows are active
   in neither table (a loss that exists today, independent of this design; it gets its own issue and the
   regression test below). `VersionReservations` therefore retains split-phase `NewPart` reservations, with
   a precise part-publication postpone reason (`PENDING_PART_PUBLICATION = "Part with a lower block number
   is not published yet"`) rather than misreporting them as mutations; a lock-nested `NewPart` (the ordinary
   insert) is excluded from the snapshot only where the holder encodes that its allocation and activation
   were one critical section. The simplest robust first implementation retains every `NewPart` and lets
   `firstAfter` skip nothing; the optimized exclusion is a follow-up with its own proof per holder.

### Implementation shape {#shape}

One typed snapshot, existing accessors untouched:

```cpp
/// Block reservations that stand for a version not yet visible: a mutation between its file write
/// and its registration, a lightweight update between block allocation and patch commit.
struct VersionReservations
{
    std::vector<CommittingBlock> blocks;   /// sorted by number; only Mutation, Update, Unknown

    /// The lowest reservation strictly above data_version, or nullptr.
    const CommittingBlock * firstAfter(Int64 data_version) const;
};

VersionReservations StorageMergeTree::getVersionReservations() const;   /// filtered copy under committing_blocks_mutex
```

Capture discipline, stated once and asserted by construction: a `MergeSelectionInputs` bundle (regular
parts, patch parts, `VersionReservations`, the epoch pair) is built by one function: read parts, patches and
`publication_epoch` under the data-parts read lock; release; snapshot reservations; re-read the epoch;
validate or recapture. `selectPartsToMerge` builds one bundle per attempt and constructs the predicate and
the collector from it; the initial collection is hoisted out of `MergeTreePartsCollector` into the bundle.
The predicate's constructor no longer calls `getCommittingBlocks()` or `getPatchPartInfos()` itself.
`selectPartsToMutate` builds the same kind of bundle without patches.
`getCurrentMutationVersion` and `getNextMutationVersion` keep describing registered mutations only; their
other callers (`getIncompleteMutationsStatus`, `clearOldMutations`) stay correct because no part can cross a
gap once the rule holds. Pre-filtering matters: `committing_blocks` is mostly `NewPart` entries under insert
load, `firstAfter` is a binary search per candidate part.

Lock order is unchanged from today: `currently_processing_in_background_mutex` is held by both selectors;
the data-parts read lock is taken and released for the capture exactly as `collectInitial` does now; then
`committing_blocks_mutex` is taken briefly for the filtered copy. No lock is held longer, no lock is nested
that is not nested today. The epoch increment sits inside `Transaction::commit` (and the two direct
activations), under the data-parts write lock that publication already holds, and only for patch parts and
split-phase `NewPart`s. `removeCommittingBlock` releases `committing_blocks_mutex` before the trigger; the
trigger is load-bearing on the failure path only (registration already triggers on success).

### The lock fix, separately {#lock}

`StorageMergeTree::mutate` keeps its alter lock until `startMutation` returns (two lines). This restores the
documented contract "serialize order of mutations with alter queries" and, as a side effect, makes R1's
inversion impossible for every mutation created through `mutate` (all user mutations, lightweight `DELETE`
included; the `ALTER` path already registers under the lock `InterpreterAlterQuery` took). It is a distinct
bug with a distinct commit; rule 1 then covers R1 as defence in depth. `StorageAlias::alter` forwards the
alias's lock holder to the target instead of taking the target's alter lock; this pull request fixes it
(acquire the target table's alter lock in `StorageAlias::alter` and pass that holder to
`target_storage->alter`) and adds the alias variant of S4, because the pull request claims mutation/metadata
ordering and cannot leave a known path that bypasses it.

## 2. What supersedes what, and prerequisites {#supersedes}

Built on top of PR #119155 with its commits kept (authorship preserved), then:

| PR #119155 piece | in this design |
|---|---|
| `Op::Update` loop in `selectPartsToMutate`, snapshot before the parts read, guarded by `supportsLightweightUpdate()` | replaced by the typed snapshot taken after the parts read; guard dropped (audit finding 1); one final loop, not two |
| `PostponeReasons::PENDING_LIGHTWEIGHT_UPDATE` | kept unchanged for `Update`; `PENDING_MUTATION_REGISTRATION` added |
| `removeCommittingBlock` triggers on `Update` | extended to `Mutation` and `Unknown` |
| failpoint `mt_lightweight_update_pause_after_block_allocation` | kept |
| test `05227_mutation_waits_for_lightweight_update.sh` | kept verbatim, plus a `timeout` around its `SYSTEM WAIT FAILPOINT` (audit finding 2) |

Added: failpoint `mt_pause_once_before_register_mutation` (`PAUSEABLE_ONCE`; the existing
`mt_pause_before_register_mutation` is `PAUSEABLE` and would hold every mutation), a `PAUSEABLE` failpoint
`mt_merge_selecting_pause_after_reservation_snapshot` in merge selection for the cut tests, the lock fix,
the `StorageAlias::alter` fix, the merge-predicate and patch-selection changes, and the tests in §3.

Commit order in the superseding pull request: (1) #119155's commits; (2) `VersionReservations` and the
ordering in `selectPartsToMutate` with the guard drop and the new reason; (3) the coherent cut in merge
selection, rules 2 and 3; (4) rule 4; (5) the lock fix; (6) the `StorageAlias::alter` fix; (7) tests. Each
commit builds and its tests pass on their own.

Prerequisite for `antalya-26.6` only: upstream's registration-time transaction-rollback recheck in
`startMutation` (erase the just-registered entry and remove its file when the transaction rolled back
concurrently) is absent on 26.6. It lands first as its own pull request with its upstream test, merges, and
the boundary series is then based on the merged result (no stacked pull requests). The upstream pull request
relies on the code already in master.

## 3. Tests, failing first {#tests}

`05227_mutation_waits_for_lightweight_update.sh` (PR #119155) stays as the R2 pin. New stateless tests
(numbers from `add-test` at rebase time), tags `no-parallel, no-replicated-database, no-shared-merge-tree`,
deterministic (failpoints and polling on `system.mutations`/`system.parts`, no sleeps as synchronisation,
every `SYSTEM WAIT FAILPOINT` under `timeout`, traps that disable every failpoint, reap background clients
and drop the tables; `KILL QUERY` on a thread parked in `pauseFailPoint` takes effect only after the failpoint
is disabled, so cancellation arms disable it right after the kill):

- **S1 mutation vs mutation** (R1, lock fix): pause DELETE A at `mt_pause_once_before_register_mutation`;
  DELETE B with `lock_acquire_timeout = 1` fails with `TIMEOUT_EXCEEDED` and `system.mutations` is empty;
  release A; wait for A done; run B again synchronously; count 0, two mutations done in id order. Before the
  fix the test additionally waits for B to complete while A is still paused, then releases A, and shows A
  reported done with its rows surviving: the loss itself, not only the ordering. (The ordering arm is
  implemented as `05054_mutations_register_in_order.sh` and verified red/green; the loss arm is still to be
  added to that test in the implementation plan.)
- **S2 update vs mutation, cancellation arm** (rule 4): pause a lightweight `UPDATE` after its block
  allocation (PR #119155's failpoint); start `ALTER ... UPDATE` and observe `PENDING_LIGHTWEIGHT_UPDATE`;
  `KILL QUERY` the paused update, then disable the failpoint; no patch part is committed; the postponed
  mutation completes. The wake-up itself is pinned by forcing the assignee into a known long backoff first
  (`mt_merge_selecting_task_pause_when_scheduled` or the existing backoff failpoint), so a completion within
  the test's bound proves the trigger, not a lucky tick.
- **S2b guard drop** (audit finding 1): pause the update after allocation; `ALTER TABLE ... MODIFY SETTING
  enable_block_offset_column = 0`; start the mutation; it must be postponed with `PENDING_LIGHTWEIGHT_UPDATE`
  while the setting stays disabled; then re-enable, release, both applied. Red on PR #119155 as is.
- **S2c partial progress** (rule 1's bound): registered K < pending U < registered M on one part; K is applied
  and M postponed; after U commits, M applies. Pins the less conservative behaviour.
- **S3 merge vs in-flight mutation** (R3, rule 2): pause the background merge selector
  (`mt_merge_selecting_task_pause_when_scheduled`) so only explicit `OPTIMIZE` selects; `INSERT (0)`; pause
  DELETE `WHERE id = 1` before registration; `INSERT (1)`; `OPTIMIZE TABLE ... FINAL SETTINGS
  optimize_throw_if_noop = 1` throws and two active parts remain; release; the mutation applies to the first
  part only; `SELECT groupArray(id)` is `[0,1]`. Red before rule 2. A plain merge of unmutated parts cannot
  advance above N, so there is no mirror arm.
- **S3b late mutation reservation** (the cut, MAJOR 1 of round 2): pause merge selection at
  `mt_merge_selecting_pause_after_reservation_snapshot`; allocate and pause mutation N; commit insert Q > N;
  resume. Either Q is not in the captured parts, or the reservation snapshot taken after the parts contains
  N and refuses the merge; the merge must not combine P and Q. Red with reservation-first ordering.
- **S3c late update reservation** (rule 3 under the cut): pause merge selection at the same point; allocate
  and pause lightweight update U; commit a disjoint update V > U; resume; the merge must not materialize V
  across U (the captured patch set predates V, or the later reservation snapshot bounds it). Red with
  reservation-first ordering; this replaces rev.2's S3b, which was already green because of
  `min_update_block`.
- **S3d failure release wakes a refused merge** (rule 4, `Mutation` arm): a `PAUSEABLE_ONCE` failpoint
  immediately before the failure injection of `mt_throw_after_mutation_commit` holds the mutation with its
  reservation; the test waits until merge selection has refused the range because of it, releases the
  mutation into the injected failure, and, with a test-only deterministic long-backoff hook on the assignee
  (`BackgroundJobsAssignee` backoff can reach 600 s), shows the merge completes because `removeCommittingBlock`
  called `trigger`, not because of a tick.
- **S3e patch published between the captures** (round 3, MAJOR 1): pending update U (paused before
  commit) and a committed disjoint patch V > U; pause merge selection after the patch capture and before the
  reservation capture; let U commit. With the validated cut U's publication bumps the epoch, the attempt
  recaptures, sees U's patch (or, if U's release has not happened, its reservation), and the merge does not
  materialize V across U. Without it U publishes and releases before the reservation capture and U is lost.
  A second arm pins the bounded retry: keep publishing (a background insert loop) while selection is paused,
  and assert the attempt gives up after three recaptures with the Debug line, without a merge.
- **S5 split-phase `MOVE`** (consequence 5): pause `MOVE PARTITION TO TABLE` after destination
  preactivation (a new `PAUSEABLE_ONCE` failpoint at the point where the destination parts lock is released);
  commit a later destination insert; `OPTIMIZE` the destination partition; resume; the moved rows remain
  active in the destination and absent from the source. Red today (the merge spans the moved part's block
  and its commit marks it `Outdated`); green with `NewPart` retained as a boundary.
- **S6 validation exhaustion is not success:** with `mt_merge_selecting_pause_after_epoch_read` and three
  injected handoffs, explicit `OPTIMIZE` returns `CANNOT_SELECT` with the transient explanation, not a
  silent no-op, and the background assignee schedules an immediate retry.
- **S4 barrier `ALTER` vs in-flight mutation** (lock fix): pause DELETE before registration; `ALTER TABLE ...
  RENAME COLUMN` with `lock_acquire_timeout = 1` fails with `TIMEOUT_EXCEEDED`; release; the rename succeeds.
  Alias variant: the same `RENAME COLUMN` through a `StorageAlias` of the table. Both red before their fix.
- **Unit test** for `VersionReservations::firstAfter`: classification of `NewPart`/`Mutation`/`Update`/`Unknown`,
  reservations below and above the data version, several `NewPart`s in between, empty input, the map
  bound `lower_bound(firstAfter(dv)->number)` against a small `current_mutations_by_version`, and the rule-3
  upper bound with `getNextMutationVersion(dv) == 0` (infinity) and patches below, at and above the
  reservation.
- **Test hooks, named:** `mt_assignee_force_long_backoff` (`PAUSEABLE`): while enabled,
  `BackgroundJobsAssignee::postpone` uses the maximum backoff, so a completion inside the test bound can only
  come from an explicit `trigger`; S2's cancellation arm and S3d use it. `mt_merge_selecting_pause_after_epoch_read`
  (`PAUSEABLE`): pauses a selection attempt between the first epoch read and the reservation snapshot; S3b,
  S3c, S3e use it, and S3e's retry-exhaustion arm counts three injected relevant handoffs, one acknowledged per
  attempt, instead of a background insert loop (which does not hit the narrow window deterministically).
  `mt_mutate_task_pause_in_prepare` (existing) holds K applied-but-not-finished in S2c so K < U < M cannot
  collapse before the observation.

## 4. Why the rule is sound, and what it does not claim {#argument}

- Successful mutation registration transfers the entry into `current_mutations_by_version` and destroys its
  reservation while holding `currently_processing_in_background_mutex`; both selectors hold that mutex while
  they capture parts and snapshot reservations, so a decision sees the reservation, or the registered entry,
  or both, never neither.
- Patch publication requires the data-parts write lock, and the update reservation outlives publication
  (its holder lives in the patch sink). A publication between the parts capture and the reservation snapshot
  bumps `publication_epoch` under that write lock; the selection re-reads the epoch after the reservation
  snapshot and recaptures on a change. So a validated bundle observes either the committed patch (published
  before the parts capture) or the reservation (still held at the snapshot, since release follows
  publication, and publication after the capture invalidates the bundle). Ordering the two snapshots without
  validation is insufficient (round 3, MAJOR 1).
- Under the cut, a reservation allocated after the reservation snapshot is above every part and patch the
  decision captured, so the decision's result has a data version below it, and the later mutation or update
  applies to the result as usual.
- A failed mutation publishes no registered entry. A failed lightweight update may have published a subset
  of its patch parts (`MergeTreeSinkPatch` commits partitions one at a time). Until release, its reservation
  protects the missing or partially published state; after release, the committed patches are observed
  through the atomic cut. A decision with an older snapshot is merely conservative, and rule 4 wakes the
  assignee. Registration itself already triggers, so the `Mutation` trigger is load-bearing only for the
  failure path.
- **Not covered by the rule, stated as scope:** (a) a mutation inside a transaction whose transaction rolls
  back between file write and registration: upstream master's registration-time recheck handles it; 26.6
  gets it as the prerequisite pull request of §2. (b) A nontransactional `mutation_N.txt` left behind when
  registration fails and the file removal fails: `loadMutations` treats every final file as registered after
  a restart, and parts may have advanced above N after the reservation was released. The pull request
  reorders both the `PreparedMutationEntry` members and the locals in `prepareMutationEntry` (the local
  `entry` is currently declared before `block_holder`, so unwinding destroys the holder first) so the entry
  and its file removal are destroyed before the block holder on every path, and states the restart
  limitation; a durable distinction
  between "final file" and "registered mutation" is a separate change. Process termination while the
  reservation exists is different: no in-process crossing can follow, only the restart ambiguity.

## 5. Settled in round 3 {#settled}

- Lazy reservation capture on the predicate's first use is not acceptable without validation; the bundle is
  built by hoisting the initial collection out of `MergeTreePartsCollector`, a localized constructor/API
  change, not a compaction-framework redesign, and validated by the epoch (rev.5).
- No other path decides on parts plus `committing_blocks` in the wrong order: background selection and
  `OPTIMIZE` both go through `selectPartsToMerge`; `waitForCommittingInsertsAndMutations` inspects
  reservations only; the `OPTIMIZE FINAL` retry inside `selectPartsToMerge` is the one place that needs a
  fresh bundle per attempt.
- S3b and S3c are red with reservation-first ordering (the predicate snapshots reservations in its
  constructor, the collector reads parts later); S3e covers the publication-between-captures gap they miss.
- The remaining review items (S1's pre-fix loss check in the existing test, S2c holding K pending
  explicitly) are carried into the implementation plan and code review.

## 6. Settled in review round 4 {#settled-r4}

- Publication inventory: ordinary insert, patch insert, mutation commit, `ATTACH`, `REPLACE PARTITION` and
  restored clone use rename plus `Transaction::commit`; merge and `MOVE` separate preactivation from commit;
  `restoreAndActivatePart` and `swapActivePart` bypass rename. The common visible transition is activation in
  `Transaction::commit`; the two direct activations are the exceptions and get the increment too.
- A universal insert-bumped epoch can starve selection forever; the boundary-handoff epoch above is the
  answer, with `CANNOT_SELECT` on exhaustion.
- The epoch protocol is sufficient for `selectPartsToMutate` although it captures no patches: a pending
  update either remains a reservation and bounds the target, publishes in the gap and invalidates the
  capture, or published before the capture and is included later by `MutateTask::prepare`'s patch snapshot;
  updates allocated after validation are above every selected version.
- Separate upstream issue to file from this review: the `MOVE PARTITION TO TABLE` split-phase loss of §1
  consequence 5, with test S5.
